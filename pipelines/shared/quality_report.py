"""Local quality checks and visual diagnostics; bounded-memory reads, no SQL."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from .imputation import IMPUTED_COL, is_imputed_series
from .integrity import suspect_days


def _issue(issues, severity, dataset, check, detail):
    issues.append(dict(severity=severity, dataset=dataset, check=check, detail=detail))


def _hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _load_domain(path, domain, issues):
    if not path.exists():
        _issue(issues, "ERROR", domain, "missing_file", str(path))
        return None, {}
    parquet = pq.ParquetFile(path)
    names = parquet.schema_arrow.names
    total = domain.upper() + "__TOTAL"
    if not {"timestamp", total}.issubset(names):
        _issue(issues, "ERROR", domain, "required_columns", "Falten timestamp o el total global")
        return None, {}
    cols = [c for c in ("timestamp", total, IMPUTED_COL, "__quality_status") if c in names]
    data = pd.read_parquet(path, columns=cols)
    data["timestamp"] = pd.to_datetime(data.timestamp, errors="coerce")
    values = [c for c in names if c.startswith(domain.upper() + "__")]
    metrics = dict(rows=len(data), columns=len(names), invalid_dates=int(data.timestamp.isna().sum()),
                   duplicate_dates=int(data.timestamp.duplicated().sum()), negative_cells=0,
                   invalid_numeric_cells=0, partition_mismatch_rows=0)
    partitions = {}
    if domain == "demand":
        for variable in ("SERVEI_CODI", "TIPUS_CLASS", "TIPUS_VISITA_AGRUPAT", "VISI_LLOC_VISITA", "VISI_SITUACIO_VISITA"):
            partitions[variable] = [c for c in values if c.startswith(f"DEMAND__{variable}__") and len(c.split("__")) == 3]
    # Check all numerical features in batches, not only the plotted totals.
    for batch in parquet.iter_batches(batch_size=1024, columns=values):
        numbers = batch.to_pandas().apply(pd.to_numeric, errors="coerce")
        metrics["negative_cells"] += int((numbers < 0).sum().sum())
        metrics["invalid_numeric_cells"] += int((~np.isfinite(numbers.to_numpy(dtype=float, na_value=np.nan))).sum())
        for columns in partitions.values():
            if columns:
                delta = numbers[columns].sum(axis=1) - numbers[total]
                tolerance = np.maximum(numbers[total].abs() * 1e-6, 0.05)
                metrics["partition_mismatch_rows"] += int((delta.abs() > tolerance).sum())
    if data.empty:
        _issue(issues, "ERROR", domain, "empty_dataset", "El final no conte dies")
        return None, metrics
    for name in ("invalid_dates", "duplicate_dates", "negative_cells", "invalid_numeric_cells", "partition_mismatch_rows"):
        if metrics[name]:
            _issue(issues, "ERROR", domain, name, str(metrics[name]))
    if metrics["invalid_dates"] or metrics["duplicate_dates"]:
        return None, metrics
    data = data.sort_values("timestamp").reset_index(drop=True)
    gaps = pd.date_range(data.timestamp.min(), data.timestamp.max()).difference(data.timestamp)
    metrics["missing_calendar_dates"] = len(gaps)
    if len(gaps):
        _issue(issues, "REVIEW", domain, "calendar_gaps", f"{len(gaps)} dates absents a l'historic")
    data[total] = pd.to_numeric(data[total], errors="coerce")
    if IMPUTED_COL not in data:
        data[IMPUTED_COL] = pd.NA
    normalized = data[IMPUTED_COL].astype("string").str.lower().str.strip()
    known = normalized.isin(["true", "false", "1", "0", "1.0", "0.0", "yes", "no", "y", "n"])
    data["__estimated"] = is_imputed_series(data[IMPUTED_COL])
    data["__known_status"] = known
    if not known.all():
        _issue(issues, "REVIEW", domain, "unknown_provenance", f"{int((~known).sum())} dies sense estat d'imputacio valid")
    observed = known & ~data.__estimated
    future = (data.timestamp > pd.Timestamp.today().normalize()) & observed
    if future.any():
        _issue(issues, "ERROR", domain, "future_observations", str(int(future.sum())))
    fractional = ((data[total] - data[total].round()).abs() > 0.01) & observed
    if fractional.any():
        _issue(issues, "REVIEW", domain, "fractional_observed_counts", str(int(fractional.sum())))
    metrics["first_date"] = str(data.timestamp.min().date())
    metrics["last_date"] = str(data.timestamp.max().date())
    last_observed = data.loc[observed, "timestamp"].max()
    metrics["last_marked_observed"] = None if pd.isna(last_observed) else str(last_observed.date())
    metrics["imputed_rows_all_history"] = int(data.__estimated.sum())
    return data, metrics


def _check_join(data_root, issues):
    """Compare every numerical feature with finals and CSV in small batches."""
    path = data_root / "finals/demand_diagnosis_joined.parquet"
    if not path.exists():
        _issue(issues, "ERROR", "joined", "missing_file", str(path))
        return
    names = pq.ParquetFile(path).schema_arrow.names
    if "timestamp" not in names:
        _issue(issues, "ERROR", "joined", "missing_timestamp", str(path))
        return
    dates = pd.read_parquet(path, columns=["timestamp"]).timestamp
    if dates.isna().any() or dates.duplicated().any():
        _issue(issues, "ERROR", "joined", "invalid_or_duplicate_dates", "Clau temporal invalida")
        return
    for domain in ("demand", "diagnosis"):
        source = data_root / f"{domain}_pipeline/finals/{domain}_final.parquet"
        if not source.exists():
            continue
        source_names = pq.ParquetFile(source).schema_arrow.names
        if "timestamp" not in source_names:
            continue
        columns = [c for c in source_names if c.startswith(domain.upper() + "__")]
        if set(columns) - set(names):
            _issue(issues, "ERROR", "joined", domain + "_missing_columns", str(sorted(set(columns) - set(names))))
            continue
        # Only one domain in memory at a time.
        left = pd.read_parquet(source, columns=["timestamp", *columns]).set_index("timestamp")
        if left.index.has_duplicates:
            continue
        right = pd.read_parquet(path, columns=["timestamp", *columns]).set_index("timestamp")
        if not left.index.equals(right.index):
            if set(left.index) != set(right.index):
                _issue(issues, "ERROR", "joined", domain + "_date_mismatch", "El join i el final no cobreixen les mateixes dates")
            right = right.reindex(left.index)
        mismatches = 0
        for start in range(0, len(left), 1024):
            a = left.iloc[start:start + 1024].to_numpy(dtype=float, na_value=np.nan)
            b = right.iloc[start:start + 1024].to_numpy(dtype=float, na_value=np.nan)
            mismatches += int((~np.isclose(a, b, rtol=1e-6, atol=0.05, equal_nan=True)).sum())
        if mismatches:
            _issue(issues, "ERROR", "joined", domain + "_value_mismatch", f"{mismatches} cel·les diferents")
        flag = domain.upper() + "___is_imputed"
        if IMPUTED_COL in source_names and flag in names:
            a = pd.read_parquet(source, columns=["timestamp", IMPUTED_COL]).set_index("timestamp")[IMPUTED_COL]
            b = pd.read_parquet(path, columns=["timestamp", flag]).set_index("timestamp")[flag].reindex(a.index)
            same = a.astype("string").fillna("<missing>") == b.astype("string").fillna("<missing>")
            if not same.all():
                _issue(issues, "ERROR", "joined", domain + "_flag_mismatch", str(int((~same).sum())))
        elif IMPUTED_COL in source_names:
            _issue(issues, "ERROR", "joined", domain + "_missing_flags", "Falten les marques d'imputacio al join")
    csv = path.with_suffix(".csv")
    if not csv.exists():
        _issue(issues, "ERROR", "joined", "missing_csv", str(csv))
    else:
        offset = 0
        mismatches = 0
        column_error = list(pd.read_csv(csv, nrows=0).columns) != names
        for chunk in ([] if column_error else pd.read_csv(csv, usecols=["timestamp"], chunksize=1024)):
            expected = dates.iloc[offset:offset + len(chunk)]
            parsed = pd.to_datetime(chunk.timestamp, errors="coerce")
            if len(expected) != len(chunk) or not np.array_equal(parsed.to_numpy(), expected.to_numpy()):
                mismatches += len(chunk)
            offset += len(chunk)
        if column_error:
            _issue(issues, "ERROR", "joined", "csv_columns", "Les columnes del CSV no coincideixen amb el Parquet")
        elif mismatches or offset != len(dates):
            _issue(issues, "ERROR", "joined", "stale_csv", f"CSV: {offset} files; Parquet: {len(dates)}; dates inconsistents: {mismatches}")
        # Compare numerical values without loading the whole CSV.
        if not column_error and not mismatches and offset == len(dates):
            for domain in ("DEMAND", "DIAGNOSIS"):
                cols = [c for c in names if c.startswith(domain + "__") and not c.startswith(domain + "___")]
                reference = pd.read_parquet(path, columns=cols)
                offset = 0
                changed = 0
                for chunk in pd.read_csv(csv, usecols=cols, chunksize=1024):
                    chunk = chunk[cols].apply(pd.to_numeric, errors="coerce")
                    values = reference.iloc[offset:offset + len(chunk)].to_numpy(dtype=float, na_value=np.nan)
                    changed += int((~np.isclose(chunk.to_numpy(dtype=float, na_value=np.nan), values, rtol=1e-6, atol=0.05, equal_nan=True)).sum())
                    offset += len(chunk)
                if changed:
                    _issue(issues, "ERROR", "joined", "csv_" + domain.lower() + "_values", f"{changed} cel·les diferents")
    manifest_path = path.with_name(path.stem + "_manifest.json")
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if set(manifest.get("files", {})) != {path.name, csv.name} or manifest.get("rows") != len(dates):
            _issue(issues, "ERROR", "joined", "invalid_manifest", "Falten fitxers o el recompte de files no coincideix")
        for name, digest in manifest.get("files", {}).items():
            file = path.parent / name
            if file.resolve().parent != path.parent.resolve():
                raise ValueError("Unsafe file name in output manifest")
            if not file.exists() or _hash(file) != digest:
                _issue(issues, "ERROR", "joined", "manifest_hash", name)
    else:
        _issue(issues, "REVIEW", "joined", "missing_manifest", "Export antic sense manifest de versio")


def _render(data, domain, start, end, reasons, output):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter

    total = domain.upper() + "__TOTAL"
    calendar = pd.date_range(start, end)
    view = data.set_index("timestamp").reindex(calendar)
    estimated = view.__estimated.fillna(False).astype(bool)
    known = view.__known_status.fillna(False).astype(bool)
    observed = known & ~estimated
    suspicious = pd.Series([d in reasons and reasons[d] != ["imputed"] for d in calendar], index=calendar)
    previous = data.set_index("timestamp")
    previous = previous.loc[previous.__known_status & ~previous.__estimated, [total]].copy()
    previous.index = previous.index + pd.Timedelta(weeks=52)
    previous = previous.reindex(calendar)[total]
    colors = dict(observed="#1768d5", estimated="#eb8500", suspect="#d73535", previous="#aab4bf")
    with plt.rc_context({"font.family": "DejaVu Sans", "font.size": 10, "axes.spines.top": False, "axes.spines.right": False}):
        fig, (ax, weekly) = plt.subplots(2, 1, figsize=(13, 8), gridspec_kw={"height_ratios": [2.4, 1]}, layout="constrained")
        fig.suptitle(f"{'Demanda' if domain == 'demand' else 'Diagnostics'} · Catalunya\n{start.date()} — {end.date()}", fontsize=17, weight="bold")
        ax.plot(calendar, previous, color=colors["previous"], linewidth=1, alpha=.6, label="Observat fa 52 setmanes")
        ax.plot(calendar, view[total].where(observed), color=colors["observed"], linewidth=1.4, label="Observat (completesa no certificada)")
        ax.plot(calendar, view[total].where(estimated), color=colors["estimated"], linestyle="--", linewidth=1.8, marker=".", markersize=3, label="Imputat")
        flagged = observed & suspicious
        ax.scatter(calendar[flagged], view.loc[flagged, total], color=colors["suspect"], marker="x", s=38, linewidth=1.5, zorder=5, label="Sospitos: revisar")
        unknown = ~known & view[total].notna()
        if unknown.any():
            ax.scatter(calendar[unknown], view.loc[unknown, total], color="#66727c", s=12, label="Procedencia desconeguda")
        missing = view[total].isna()
        if missing.any():
            ax.scatter(calendar[missing], np.zeros(int(missing.sum())), color="black", marker="|", s=80, label="Dia/valor absent")
        ax.set_ylim(bottom=0)
        ax.set_ylabel("Recompte diari")
        ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x:,.0f}".replace(",", ".")))
        ax.grid(axis="y", alpha=.18)
        ax.legend(loc="upper left", fontsize=8, ncol=2)
        x = np.arange(7)
        for delta, mask, color, label in [(-.21, observed, colors["observed"], "Observat"), (.21, estimated, colors["estimated"], "Imputat")]:
            medians = view[total].where(mask).groupby(calendar.day_of_week).median().reindex(range(7))
            weekly.bar(x + delta, medians, .4, color=color, alpha=.85, label=label)
        weekly.set_xticks(x, ["Dl", "Dt", "Dc", "Dj", "Dv", "Ds", "Dg"])
        weekly.set_ylabel("Mediana de visites")
        weekly.set_title("Patro setmanal del periode", loc="left", fontsize=11)
        weekly.grid(axis="y", alpha=.18)
        weekly.legend(fontsize=8)
        fig.supxlabel(f"{int(observed.sum())} observats · {int(estimated.sum())} imputats · {int(flagged.sum())} observats sospitosos · {int(missing.sum())} absents\nUn grafic plausible no demostra que la font sigui completa.", fontsize=9)
        fig.savefig(output, dpi=150)
        plt.close(fig)


def create_quality_report(data_root, output_dir, start_date=None, end_date=None):
    data_root, output_dir = Path(data_root).resolve(), Path(output_dir).resolve()
    sources = [data_root / f"{d}_pipeline/finals/{d}_final.parquet" for d in ("demand", "diagnosis")]
    if any(output_dir == p.parent for p in sources) or output_dir == data_root / "finals":
        raise ValueError("Use a separate output directory, such as data/quality_report")
    start_arg = None if start_date is None else pd.Timestamp(start_date)
    end_arg = None if end_date is None else pd.Timestamp(end_date)
    if (start_arg is not None and pd.isna(start_arg)) or (end_arg is not None and pd.isna(end_arg)):
        raise ValueError("Dates must be valid YYYY-MM-DD dates")
    start_arg = start_arg.normalize() if start_arg is not None else None
    end_arg = end_arg.normalize() if end_arg is not None else None
    if start_arg is not None and end_arg is not None and start_arg > end_arg:
        raise ValueError("start-date must be on or before end-date")
    output_dir.mkdir(parents=True, exist_ok=True)
    # Never leave a PNG from an earlier successful run beside a failed report.
    for name in ("demand_quality.png", "diagnosis_quality.png"):
        old = output_dir / name
        if old.exists():
            old.unlink()
    issues, datasets, rows = [], {}, []
    for domain, path in zip(("demand", "diagnosis"), sources):
        try:
            data, metrics = _load_domain(path, domain, issues)
            datasets[domain] = metrics
            if data is None:
                continue
            end = data.timestamp.max() if end_arg is None else end_arg
            start = max(data.timestamp.min(), end - pd.Timedelta(days=364)) if start_arg is None else start_arg
            if end < data.timestamp.min() or start > data.timestamp.max():
                _issue(issues, "REVIEW", domain, "empty_period", "No hi ha dades al periode demanat")
            reasons = suspect_days(data, domain.upper() + "__TOTAL")
            period = {d: why for d, why in reasons.items() if start <= d <= end}
            missing = pd.date_range(start, end).difference(data.timestamp)
            for day in missing:
                period[day] = ["missing_day"]
            if "__quality_status" in data:
                for _, row in data.loc[data.__quality_status.isin(["suspect", "pending_rebuild"])].iterrows():
                    if start <= row.timestamp <= end:
                        period.setdefault(row.timestamp, []).append(str(row.__quality_status))
            metrics["period"] = {"start": str(start.date()), "end": str(end.date()), "flagged_days": len(period)}
            if period:
                _issue(issues, "REVIEW", domain, "flagged_period", f"{len(period)} dies imputats, absents o sospitosos")
            for day, why in sorted(period.items()):
                value = data.loc[data.timestamp == day, domain.upper() + "__TOTAL"]
                rows.append(dict(dataset=domain, timestamp=str(day.date()), total=None if value.empty else float(value.iloc[0]), reasons=";".join(why)))
            ledger = path.with_name(path.stem + "_integrity.json")
            if ledger.exists():
                state = json.loads(ledger.read_text(encoding="utf-8"))
                metrics["sql_review"] = {"reviewed_days": state.get("reviewed_days", 0), "deferred_days": state.get("deferred_days", 0), "pending_rebuild": len(state.get("pending_rebuild", []))}
                checks = state.get("checks", {})
                checked_times = pd.to_datetime([c.get("checked_at") for c in checks.values()], errors="coerce")
                last_check = checked_times.max() if len(checked_times) else pd.NaT
                metrics["sql_review"]["last_check"] = None if pd.isna(last_check) else last_check.isoformat()
                if pd.isna(last_check):
                    _issue(issues, "REVIEW", domain, "no_sql_checks", "El registre no conte comprovacions SQL datades")
                stale_checks = 0
                indexed = data.set_index("timestamp")
                for date, check in checks.items():
                    day = pd.Timestamp(date)
                    if start <= day <= end and day in indexed.index and "local_total" in check:
                        value = indexed.loc[day, domain.upper() + "__TOTAL"]
                        if pd.isna(value) or check["local_total"] != float(value):
                            stale_checks += 1
                if stale_checks:
                    _issue(issues, "REVIEW", domain, "stale_sql_checks", f"{stale_checks} totals locals han canviat des de la comprovacio SQL")
                if metrics["sql_review"]["pending_rebuild"] or metrics["sql_review"]["deferred_days"]:
                    _issue(issues, "REVIEW", domain, "pending_sql_review", str(metrics["sql_review"]))
            else:
                _issue(issues, "REVIEW", domain, "no_sql_review", "No consta una revisio SQL amb el pipeline nou")
            _render(data, domain, start, end, period, output_dir / f"{domain}_quality.png")
        except (ValueError, OSError, KeyError, TypeError) as exc:
            _issue(issues, "ERROR", domain, "unreadable_or_invalid", str(exc))
    try:
        _check_join(data_root, issues)
    except (ValueError, OSError, KeyError, TypeError) as exc:
        _issue(issues, "ERROR", "joined", "unreadable_or_invalid", str(exc))
    status = "ERROR" if any(i["severity"] == "ERROR" for i in issues) else "REVIEW" if issues else "OK"
    report = {"status": status, "scope": "local_files_only_not_source_completeness", "data_root": str(data_root),
              "datasets": datasets, "issues": issues}
    (output_dir / "summary.json").write_text(json.dumps(report, indent=2, ensure_ascii=False, allow_nan=False), encoding="utf-8")
    pd.DataFrame(rows, columns=["dataset", "timestamp", "total", "reasons"]).to_csv(output_dir / "flagged_days.csv", index=False, encoding="utf-8-sig")
    lines = [f"RESULTAT: {status}", "Comprovacio local; no certifica la completesa de SQL.", ""]
    for domain, metrics in datasets.items():
        lines.append(f"{domain}: {metrics.get('rows', 0)} files; ultim observat: {metrics.get('last_marked_observed', '?')}; dies assenyalats al periode: {metrics.get('period', {}).get('flagged_days', '?')}")
    lines.extend(f"[{i['severity']}] {i['dataset']}/{i['check']}: {i['detail']}" for i in issues)
    (output_dir / "summary.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report
