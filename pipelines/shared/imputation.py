"""Tail imputation helpers for asynchronous source refreshes."""
from __future__ import annotations

import json
from pathlib import Path
import pandas as pd
from typing import Optional

IMPUTED_COL = "__is_imputed"
IMPUTATION_METHOD_COL = "__imputation_method"
IMPUTATION_SOURCE_LAST_DATE_COL = "__imputation_source_last_date"
IMPUTATION_CREATED_AT_COL = "__imputation_created_at"

IMPUTATION_COLUMNS = {
    IMPUTED_COL,
    IMPUTATION_METHOD_COL,
    IMPUTATION_SOURCE_LAST_DATE_COL,
    IMPUTATION_CREATED_AT_COL,
}

SAME_MONTH_DAY_METHOD = "same_month_day_mean"


def is_imputed_series(values: pd.Series) -> pd.Series:
    """Return a boolean mask for rows marked as imputed."""
    if values.dtype == bool:
        return values.fillna(False)

    normalized = values.astype("string").str.strip().str.lower()
    return normalized.isin({"true", "1", "yes", "y"})


def drop_imputed_rows(
    df: pd.DataFrame,
    timestamp_col: str = "timestamp",
) -> pd.DataFrame:
    """Remove imputed rows so real values can replace them on later runs."""
    if df.empty or IMPUTED_COL not in df.columns:
        return df.copy()

    out = df.copy()
    out = out[~is_imputed_series(out[IMPUTED_COL])].copy()
    return out.drop(columns=list(IMPUTATION_COLUMNS), errors="ignore")


def add_observed_imputation_metadata(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Add tracking columns for observed rows."""
    out = df.copy()
    out[IMPUTED_COL] = False
    out[IMPUTATION_METHOD_COL] = ""
    out[IMPUTATION_SOURCE_LAST_DATE_COL] = ""
    out[IMPUTATION_CREATED_AT_COL] = ""
    return out


def impute_tail_to_date(
    df: pd.DataFrame,
    observed_until: Optional[pd.Timestamp],
    target_until: Optional[pd.Timestamp] = None,
    timestamp_col: str = "timestamp",
) -> pd.DataFrame:
    """
    Complete a final daily dataframe through target_until with tracked estimates.

    Real rows are kept as observed. Any missing calendar day from the first
    observed day through target_until is imputed. Values are column-wise averages
    from the same calendar month/day in previous observed years. If a column has
    no same-month/day history, the column's observed mean is used as a fallback.
    """
    if df.empty:
        return add_observed_imputation_metadata(df)

    target_day = (
        pd.Timestamp.today().normalize()
        if target_until is None
        else pd.to_datetime(target_until).normalize()
    )
    today = pd.Timestamp.today().normalize()
    target_day = min(target_day, today)

    real = drop_imputed_rows(df, timestamp_col=timestamp_col)
    if real.empty:
        return add_observed_imputation_metadata(real)

    real = real.copy()
    real[timestamp_col] = pd.to_datetime(real[timestamp_col], errors="coerce").dt.floor("D")
    real = real.dropna(subset=[timestamp_col]).sort_values(timestamp_col)

    if observed_until is None or pd.isna(observed_until):
        observed_day = real[timestamp_col].max()
    else:
        observed_day = pd.to_datetime(observed_until).normalize()

    observed_day = min(observed_day, target_day, real[timestamp_col].max())
    real = real[real[timestamp_col] <= observed_day].copy()
    if real.empty:
        return add_observed_imputation_metadata(real)

    numeric_cols = [
        col
        for col in real.columns
        if col != timestamp_col and col not in IMPUTATION_COLUMNS
    ]
    for col in numeric_cols:
        real[col] = pd.to_numeric(real[col], errors="coerce")

    real = add_observed_imputation_metadata(real)

    first_day = real[timestamp_col].min()
    existing_days = set(real[timestamp_col])
    candidate_dates = pd.date_range(first_day, target_day, freq="D")
    imputed_dates = [
        day
        for day in candidate_dates
        if day not in existing_days
    ]

    if not imputed_dates:
        return real.sort_values(timestamp_col).reset_index(drop=True)

    history = real.set_index(timestamp_col)
    fallback_means = history[numeric_cols].mean(skipna=True)
    imputed_rows = []
    created_at = pd.Timestamp.now().isoformat()
    source_last_date = observed_day.date().isoformat()

    for impute_day in imputed_dates:
        same_day_history = history[
            (history.index.month == impute_day.month)
            & (history.index.day == impute_day.day)
            & (history.index < impute_day)
        ]
        if same_day_history.empty:
            values = fallback_means.copy()
        else:
            values = same_day_history[numeric_cols].mean(skipna=True)
            values = values.fillna(fallback_means)

        row = values.fillna(0).to_dict()
        row[timestamp_col] = impute_day
        row[IMPUTED_COL] = True
        row[IMPUTATION_METHOD_COL] = SAME_MONTH_DAY_METHOD
        row[IMPUTATION_SOURCE_LAST_DATE_COL] = source_last_date
        row[IMPUTATION_CREATED_AT_COL] = created_at
        imputed_rows.append(row)

    imputed = pd.DataFrame(imputed_rows)
    combined = pd.concat([real, imputed], ignore_index=True, sort=False)
    return combined.sort_values(timestamp_col).reset_index(drop=True)


def get_imputation_metadata_paths(output_file: str | Path) -> tuple[Path, Path]:
    """Return JSON summary and CSV row-list paths for a Parquet output."""
    output_path = Path(output_file)
    summary_path = output_path.with_name(f"{output_path.stem}_imputation_metadata.json")
    rows_path = output_path.with_name(f"{output_path.stem}_imputed_rows.csv")
    return summary_path, rows_path


def write_imputation_metadata_files(
    df: pd.DataFrame,
    output_file: str | Path,
    timestamp_col: str = "timestamp",
) -> tuple[Path, Path]:
    """Write imputation counts and exact imputed dates next to a Parquet file."""
    summary_path, rows_path = get_imputation_metadata_paths(output_file)
    summary_path.parent.mkdir(parents=True, exist_ok=True)

    summary, rows = build_imputation_metadata(df, output_file, timestamp_col)

    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    rows.to_csv(rows_path, index=False, encoding="utf-8-sig")
    return summary_path, rows_path


def build_imputation_metadata(
    df: pd.DataFrame,
    output_file: str | Path,
    timestamp_col: str = "timestamp",
) -> tuple[dict, pd.DataFrame]:
    """Build a JSON-ready summary and row-level imputation table."""
    timestamps = _metadata_timestamp_series(df, timestamp_col)
    groups = []
    rows = []

    for imputed_col in _find_imputed_columns(df):
        prefix = imputed_col[: -len(IMPUTED_COL)]
        label = _metadata_group_label(prefix)
        method_col = f"{prefix}{IMPUTATION_METHOD_COL}"
        source_col = f"{prefix}{IMPUTATION_SOURCE_LAST_DATE_COL}"
        created_col = f"{prefix}{IMPUTATION_CREATED_AT_COL}"

        imputed_mask = is_imputed_series(df[imputed_col])
        imputed_timestamps = timestamps[imputed_mask & timestamps.notna()]
        imputed_dates = [
            ts.date().isoformat()
            for ts in imputed_timestamps.sort_values().drop_duplicates()
        ]

        groups.append(
            {
                "dataset": label,
                "imputed_column": imputed_col,
                "num_imputed_rows": int(imputed_mask.sum()),
                "num_observed_rows": int((~imputed_mask).sum()),
                "first_imputed_date": imputed_dates[0] if imputed_dates else None,
                "last_imputed_date": imputed_dates[-1] if imputed_dates else None,
                "imputed_dates": imputed_dates,
            }
        )

        for idx in df.index[imputed_mask]:
            ts = timestamps.loc[idx]
            if pd.isna(ts):
                date_value = ""
            else:
                date_value = ts.date().isoformat()

            rows.append(
                {
                    "dataset": label,
                    "timestamp": date_value,
                    "is_imputed": True,
                    "imputation_method": _metadata_value(df, idx, method_col),
                    "imputation_source_last_date": _metadata_value(df, idx, source_col),
                    "imputation_created_at": _metadata_value(df, idx, created_col),
                }
            )

    total_imputed = sum(group["num_imputed_rows"] for group in groups)
    summary = {
        "source_file": str(Path(output_file)),
        "generated_at": pd.Timestamp.now().isoformat(),
        "timestamp_column": timestamp_col,
        "total_rows": int(len(df)),
        "total_imputed_rows": int(total_imputed),
        "groups": groups,
    }

    rows_df = pd.DataFrame(
        rows,
        columns=[
            "dataset",
            "timestamp",
            "is_imputed",
            "imputation_method",
            "imputation_source_last_date",
            "imputation_created_at",
        ],
    )
    return summary, rows_df


def _find_imputed_columns(df: pd.DataFrame) -> list[str]:
    return [col for col in df.columns if col.endswith(IMPUTED_COL)]


def _metadata_group_label(prefix: str) -> str:
    if not prefix:
        return "base"
    return prefix.rstrip("_") or "base"


def _metadata_timestamp_series(
    df: pd.DataFrame,
    timestamp_col: str,
) -> pd.Series:
    if timestamp_col in df.columns:
        return pd.to_datetime(df[timestamp_col], errors="coerce")
    if isinstance(df.index, pd.DatetimeIndex):
        return pd.Series(pd.to_datetime(df.index, errors="coerce"), index=df.index)
    return pd.Series(pd.NaT, index=df.index)


def _metadata_value(df: pd.DataFrame, idx, col: str) -> str:
    if col not in df.columns:
        return ""
    value = df.at[idx, col]
    if pd.isna(value):
        return ""
    return str(value)
