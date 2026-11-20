"""Reproduce historical integrity risks against an explicit source snapshot.

Use --source-root pointing to the audited, pre-fix checkout (for example commit
06ea62a). Assertions intentionally reproduce its incorrect behavior. Verify the
fixed implementation with pytest instead. Does not connect to SQL or modify
existing pipeline outputs.
"""
from __future__ import annotations

import contextlib
import argparse
import io
import inspect
import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--source-root", type=Path, default=ROOT, required=__name__ == "__main__",
                    help="Path to the historical pre-fix source checkout")
parser.add_argument("--data-root", type=Path, default=ROOT / "data")
parser.add_argument("--output", type=Path, default=ROOT / "data/audit/integrity_audit.json")
ARGS = parser.parse_args() if __name__ == "__main__" else parser.parse_args([])
sys.path.insert(0, str(ARGS.source_root.resolve()))

from pipelines.demand.aggregation_optimized import aggregate_final_optimized
from pipelines.diagnosis.aggregation_optimized import aggregate_diagnosis_final_optimized
from pipelines.shared.final_joiner import FinalDataJoiner
from pipelines.shared.imputation import (
    IMPUTED_COL,
    add_observed_imputation_metadata,
    impute_tail_to_date,
    is_imputed_series,
)
from pipelines.shared.parquet_storage import ParquetFinalStore, ParquetIncrementalManager
from pipelines.shared.utils import get_incremental_processing_window
from run_pipeline_optimized import check_parquet_imputation, join_final_outputs


def frame(dates, **values):
    return pd.DataFrame({"timestamp": pd.to_datetime(dates), **values})


def reproduce(root):
    findings = {}
    demand = root / "demand.parquet"
    diagnosis = root / "diagnosis.parquet"
    add_observed_imputation_metadata(frame(["2024-01-01"], DEMAND__TOTAL=[100])).to_parquet(demand)
    add_observed_imputation_metadata(frame(
        ["2024-01-01", "2024-01-02", "2024-01-03"], DIAGNOSIS__TOTAL=[10, 20, 30]
    )).to_parquet(diagnosis)
    joined = FinalDataJoiner(demand, diagnosis, root / "joined.parquet").join_columnwise()
    assert joined["DEMAND__TOTAL"].tolist() == [100, 100, 100]
    assert not joined["DEMAND___is_imputed"].any()
    findings["join_forward_fill_false_observed"] = {
        "demand_input_rows": 1, "joined_rows": 3,
        "values": joined["DEMAND__TOTAL"].tolist(),
        "imputed_flags": joined["DEMAND___is_imputed"].tolist(),
    }

    missing_join = join_final_outputs(
        demand_file=root / "absent.parquet", diagnosis_file=diagnosis,
        output_file=root / "never_created.parquet",
    )
    assert missing_join and not (root / "never_created.parquet").exists()
    findings["join_reports_success_without_output"] = True

    # Five years with strong weekly seasonality are averaged by month/day.
    dates = pd.date_range("2019-01-01", "2023-12-31")
    source = frame(dates, DEMAND__TOTAL=[10 if d.day_of_week >= 5 else 100 for d in dates])
    imputed = impute_tail_to_date(source, dates.max(), pd.Timestamp("2024-01-14"))
    january = imputed[imputed.timestamp >= "2024-01-01"]
    findings["weekday_pattern_smoothed"] = {
        "observed_weekday_value": 100, "observed_weekend_value": 10,
        "imputed_min": float(january.DEMAND__TOTAL.min()),
        "imputed_max": float(january.DEMAND__TOTAL.max()),
    }
    assert january.DEMAND__TOTAL.min() > 10
    assert january.DEMAND__TOTAL.max() < 100

    shorter = impute_tail_to_date(
        frame(["2024-01-01", "2024-01-02", "2024-01-03"], DEMAND__TOTAL=[10, 20, 30]),
        pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-02"),
    )
    assert len(shorter) == 2
    findings["bounded_refresh_drops_later_observed_rows"] = True

    for domain, aggregate in [
        ("DEMAND", aggregate_final_optimized),
        ("DIAGNOSIS", aggregate_diagnosis_final_optimized),
    ]:
        total = f"{domain}__TOTAL"
        geo = f"{domain}__TOTAL__UP__NEW"
        base = root / domain
        store = ParquetFinalStore(base / "final.parquet")
        manager = ParquetIncrementalManager(base / "incremental")
        duplicate = frame(["2024-01-01"], **{total: [100]})
        manager.add_data(duplicate.copy())
        manager.add_data(duplicate.copy())
        result = aggregate(manager, store)
        assert result[total].iloc[0] == 200
        findings[f"{domain}_retry_double_count"] = 200

        # Historical selection backfill has no global total; new days do.
        if "partial_column_update" in inspect.signature(aggregate).parameters:
            manager.add_data(frame(["2024-01-01"], **{geo: [20]}))
            manager.add_data(frame(["2024-01-02"], **{total: [120], geo: [30]}))
            result = aggregate(manager, store, partial_column_update=True)
            old = result.loc[result.timestamp == pd.Timestamp("2024-01-01"), total].iloc[0]
            assert old == 0
            findings[f"{domain}_selection_backfill_zeros_total"] = float(old)

        manager.add_data(frame(["2024-01-03"], **{total: [130]}))
        corrupt = manager.output_dir / "incremental_corrupt.parquet"
        corrupt.write_bytes(b"not a parquet file")
        aggregate(manager, store)
        assert not corrupt.exists()
        findings[f"{domain}_corrupt_chunk_deleted_after_skip"] = True

    # Metadata alone can suppress rebuilding an absent final file.
    from pipelines.demand import incremental_optimized as runner

    missing_base = root / "missing_final"
    manager = ParquetIncrementalManager(missing_base / "incremental")
    manager.add_data(frame(["2024-01-03"], DEMAND__TOTAL=[100]))
    manager.clear_incremental_files()
    conn = Mock()
    with (
        patch.object(runner, "get_connection", return_value=conn),
        patch.object(runner, "get_min_max_date", return_value=(pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-03"))),
        patch.object(runner, "_load_selected_rs", return_value=None),
        patch.object(runner, "_load_selected_up", return_value=None),
        patch.object(runner, "get_data_for_year") as query,
    ):
        runner.run_incremental_pipeline_optimized(
            "unused", "unused", "unused", "unused", "DATA_VISITA", pd.DataFrame(),
            manager.output_dir, missing_base / "final.parquet", end_date="2024-01-03",
        )
    assert not query.called and not (missing_base / "final.parquet").exists()
    findings["metadata_prevents_missing_final_rebuild"] = True

    if not hasattr(runner, "_missing_geo_sources"):
        frozen_base = root / "frozen_existing_day"
        frozen_manager = ParquetIncrementalManager(frozen_base / "incremental")
        frozen_final = frozen_base / "final.parquet"
        cached = frame(["2024-01-01", "2024-01-02", "2024-01-03"], DEMAND__TOTAL=[100, 1, 100])
        ParquetFinalStore(frozen_final).save_final(add_observed_imputation_metadata(cached))
        frozen_manager.add_data(cached.copy())
        frozen_manager.clear_incremental_files()
        with (
            patch.object(runner, "get_connection", return_value=Mock()),
            patch.object(runner, "get_min_max_date", return_value=(pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-03"))),
            patch.object(runner, "_load_selected_rs", return_value=None),
            patch.object(runner, "_load_selected_up", return_value={"00001": "NEW"}),
            patch.object(runner, "get_data_for_year", return_value=frame(["2024-01-02"], DEMAND__TOTAL=[200])) as query,
        ):
            runner.run_incremental_pipeline_optimized(
                "unused", "unused", "unused", "unused", "DATA_VISITA", pd.DataFrame(),
                frozen_manager.output_dir, frozen_final, end_date="2024-01-03",
            )
        result = pd.read_parquet(frozen_final)
        assert not query.called
        assert result.loc[result.timestamp == pd.Timestamp("2024-01-02"), "DEMAND__TOTAL"].iloc[0] == 1
        assert "DEMAND__TOTAL__UP__NEW" not in result.columns
        findings["cached_partial_day_not_revalidated"] = True
        findings["new_selection_not_backfilled_without_new_dates"] = True

    window = get_incremental_processing_window(
        pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-03"),
        last_processed_date=pd.Timestamp("2024-01-03"), today=pd.Timestamp("2024-01-03"),
    )
    assert window is None
    findings["existing_dates_never_requeried_by_default"] = True

    invalid = add_observed_imputation_metadata(frame(["2024-01-01"], DEMAND__TOTAL=[float("nan")]))
    invalid_path = root / "invalid_values.parquet"
    invalid.to_parquet(invalid_path)
    with contextlib.redirect_stdout(io.StringIO()):
        accepted = check_parquet_imputation(invalid_path)
    assert accepted
    findings["imputation_check_accepts_null_observed_total"] = True

    if hasattr(runner, "_missing_geo_sources"):
        alias_store = ParquetFinalStore(root / "alias.parquet")
        alias_store.save_final(frame(["2024-01-01"], DEMAND__TOTAL__UP__ABC=[10]))
        missing = runner._missing_geo_sources(alias_store, None, {"00001": "AB"})
        assert not missing
        findings["geo_alias_prefix_falsely_considered_present"] = True
    return findings


def inspect_local():
    summary = {}
    for domain in ("demand", "diagnosis"):
        path = ARGS.data_root / f"{domain}_pipeline/finals/{domain}_final.parquet"
        if not path.exists():
            continue
        total = f"{domain.upper()}__TOTAL"
        df = pd.read_parquet(path, columns=["timestamp", total, IMPUTED_COL])
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        mask = is_imputed_series(df[IMPUTED_COL])
        recent = df[df.timestamp >= "2026-01-01"].copy()
        recent["month"] = recent.timestamp.dt.strftime("%Y-%m")
        recent["imputed"] = mask.loc[recent.index]
        low = recent[(~recent.imputed) & (recent[total] < 100)]
        weekly = {}
        for year in (2025, 2026):
            june = df[df.timestamp.dt.strftime("%Y-%m") == f"{year}-06"]
            weekdays = june.loc[june.timestamp.dt.day_of_week < 5, total].mean()
            weekends = june.loc[june.timestamp.dt.day_of_week >= 5, total].mean()
            weekly[str(year)] = float(weekdays / weekends)
        summary[domain] = {
            "rows": len(df), "imputed_rows": int(mask.sum()),
            "last_stored": str(df.timestamp.max().date()),
            "last_marked_observed": str(df.loc[~mask, "timestamp"].max().date()),
            "last_contiguous_observed": str(ParquetFinalStore(path).get_last_contiguous_timestamp()),
            "first_imputed": str(df.loc[mask, "timestamp"].min().date()),
            "june_weekday_to_weekend_ratio": weekly,
            "recent_months": recent.groupby("month").agg(days=("timestamp", "size"), imputed=("imputed", "sum")).reset_index().to_dict("records"),
            "observed_totals_under_100": [
                {"date": str(row.timestamp.date()), "total": float(row[total])}
                for _, row in low.iterrows()
            ],
        }
    return summary


def main():
    with tempfile.TemporaryDirectory(prefix="integrity_audit_") as folder:
        findings = reproduce(Path(folder))
    report = {
        "source_root": str(ARGS.source_root.resolve()),
        "source_module": str(Path(sys.modules[aggregate_final_optimized.__module__].__file__).resolve()),
        "data_root": str(ARGS.data_root.resolve()),
        "synthetic_reproductions": findings, "local_summary": inspect_local(),
    }
    output = ARGS.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    print(f"Saved: {output}")


if __name__ == "__main__":
    main()
