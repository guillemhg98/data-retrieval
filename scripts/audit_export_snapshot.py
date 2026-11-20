"""Read-only consistency checks for an extracted data-retrieval export."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


def inspect_export(data_root: Path, source_root: Path) -> dict:
    results = {}
    finals = {}
    for domain in ("demand", "diagnosis"):
        name = domain.upper()
        path = data_root / f"{domain}_pipeline/finals/{domain}_final.parquet"
        df = pd.read_parquet(path)
        finals[domain] = df
        timestamps = pd.to_datetime(df.timestamp)
        value_cols = [c for c in df if c.startswith(name + "__")]
        numbers = df[value_cols].apply(pd.to_numeric, errors="coerce")
        total = numbers[name + "__TOTAL"]
        observed = ~df["__is_imputed"].astype(bool)
        expected = pd.date_range(timestamps.min(), timestamps.max())
        partitions = {}
        if domain == "demand":
            for variable in ("SERVEI_CODI", "TIPUS_CLASS", "TIPUS_VISITA_AGRUPAT", "VISI_LLOC_VISITA", "VISI_SITUACIO_VISITA"):
                cols = [c for c in value_cols if c.startswith(f"{name}__{variable}__") and len(c.split("__")) == 3]
                if cols:
                    delta = numbers[cols].sum(axis=1) - total
                    tolerance = np.maximum(total.abs() * 1e-6, 0.05)
                    partitions[variable] = {"columns": len(cols), "mismatch_rows": int((delta.abs() > tolerance).sum())}
        geos = {}
        selected = pd.read_csv(source_root / "selections/selected_up.csv", dtype=str)
        for alias in selected.geo_id:
            col = f"{name}__TOTAL__UP__{alias}"
            if col not in numbers:
                geos[alias] = {"missing": True}
            else:
                positive = timestamps[numbers[col] > 0]
                geos[alias] = {"first_positive_date": str(positive.min()), "last_positive_date": str(positive.max()), "zero_rows_2026": int(((numbers[col] == 0) & (timestamps.dt.year == 2026)).sum())}
        low = df.loc[observed & (total < 1000) & (timestamps >= "2026-09-01"), ["timestamp", name + "__TOTAL"]]
        results[domain] = {
            "rows": len(df), "columns": len(df.columns),
            "duplicate_dates": int(timestamps.duplicated().sum()),
            "invalid_dates": int(timestamps.isna().sum()),
            "missing_calendar_dates": [str(d.date()) for d in expected.difference(timestamps)],
            "null_numeric_cells": int(numbers.isna().sum().sum()),
            "negative_cells": int((numbers < 0).sum().sum()),
            "nonfinite_cells": int((~np.isfinite(numbers)).sum().sum()),
            "fractional_observed_totals": int(((total % 1 != 0) & observed).sum()),
            "imputed_rows": int((~observed).sum()),
            "global_category_partitions": partitions,
            "selected_up_coverage": geos,
            "suspicious_observed_totals_under_1000_since_september": [
                {"date": str(row.timestamp.date()), "total": float(row[name + "__TOTAL"])} for _, row in low.iterrows()
            ],
        }
    joined_path = data_root / "finals/demand_diagnosis_joined.parquet"
    joined = pd.read_parquet(joined_path)
    joined_results = {"rows": len(joined), "columns": len(joined.columns), "duplicate_dates": int(joined.timestamp.duplicated().sum())}
    for domain, df in finals.items():
        numeric_cols = [c for c in df if c.startswith(domain.upper() + "__")]
        a = df.set_index("timestamp")
        b = joined.set_index("timestamp")
        common_dates = a.index.intersection(b.index)
        missing_cols = sorted(set(numeric_cols) - set(b.columns))
        cols = [c for c in numeric_cols if c in b]
        x = a.loc[common_dates, cols].to_numpy(dtype=float)
        y = b.loc[common_dates, cols].to_numpy(dtype=float)
        joined_results[domain] = {
            "missing_columns": missing_cols,
            "missing_dates": len(a.index.difference(b.index)),
            "numeric_mismatch_cells": int((~np.isclose(x, y, rtol=1e-6, atol=0.05, equal_nan=True)).sum()),
            "imputation_flag_mismatches": int((a.loc[common_dates, "__is_imputed"] != b.loc[common_dates, domain.upper() + "___is_imputed"]).sum()),
        }
    csv_path = joined_path.with_suffix(".csv")
    csv = pd.read_csv(csv_path)
    csv["timestamp"] = pd.to_datetime(csv.timestamp)
    joined_results["csv_rows"] = len(csv)
    joined_results["csv_column_order_matches"] = list(csv.columns) == list(joined.columns)
    numeric = joined.select_dtypes(include="number").columns
    a = joined.set_index("timestamp")
    b = csv.set_index("timestamp").reindex(a.index)
    joined_results["csv_numeric_mismatch_cells"] = int((~np.isclose(a[numeric].to_numpy(dtype=float), b[numeric].to_numpy(dtype=float), rtol=1e-6, atol=0.05, equal_nan=True)).sum())
    results["joined"] = joined_results
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", required=True, type=Path)
    parser.add_argument("--source-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    report = inspect_export(args.data_root, args.source_root)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
