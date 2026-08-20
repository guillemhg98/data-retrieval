"""Optimized diagnosis pipeline with Parquet storage and partial incremental files."""
import hashlib
import os
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional
import logging

from pipelines.shared.imputation import (
    IMPUTATION_COLUMNS,
    drop_imputed_rows,
    impute_tail_to_date,
)
from pipelines.shared.naming import (
    DOMAIN_DIAGNOSIS,
    GEO_RS,
    GEO_UP,
    VARIABLE_ICD10_3,
    canonicalize_feature_name,
    clean_geo_series,
    feature_code,
    total_code,
)

logger = logging.getLogger(__name__)
MAX_DIAGNOSIS_FEATURES = int(os.getenv("MAX_DIAGNOSIS_FEATURES", "200000"))


def build_daily_diagnosis_counts_optimized(
    df: pd.DataFrame,
    date_column: str = "timestamp",
    code_column: str = "DIAG_CODE",
    value_col: str = "n",
) -> pd.DataFrame:
    """
    Efficiently build daily diagnosis code counts using vectorized operations.

    Args:
        df: Input DataFrame
        date_column: Timestamp column
        code_column: Diagnosis code column
        value_col: Count column

    Returns:
        Daily diagnosis counts DataFrame
    """
    df = df.copy()

    # Ensure datetime
    df[date_column] = pd.to_datetime(df[date_column]).dt.floor("D")

    # Vectorized groupby
    result = (
        df.groupby([date_column, code_column], observed=True)[value_col]
        .sum()
        .reset_index()
    )
    result.columns = [date_column, f"DIAG_{code_column}", "DIAG_COUNT"]

    return result


def build_daily_diagnosis_by_group_optimized(
    df: pd.DataFrame,
    group_col: str,
    date_column: str = "timestamp",
    code_column: str = "DIAG_CODE",
    value_col: str = "n",
) -> pd.DataFrame:
    """
    Efficiently build daily diagnosis by group using vectorized operations.

    Args:
        df: Input DataFrame
        group_col: Group column (e.g., RS, UP)
        date_column: Timestamp column
        code_column: Diagnosis code column
        value_col: Count column

    Returns:
        Daily diagnosis by group DataFrame
    """
    df = df.copy()

    # Ensure datetime
    df[date_column] = pd.to_datetime(df[date_column]).dt.floor("D")

    # Select needed columns only
    cols_to_use = [date_column, group_col, code_column, value_col]
    df = df[[c for c in cols_to_use if c in df.columns]].copy()

    # Vectorized groupby
    result = (
        df.groupby([date_column, group_col, code_column], observed=True)[value_col]
        .sum()
        .reset_index()
    )

    # Rename for clarity
    result.columns = [date_column, f"DIAG_{group_col}", f"DIAG_{code_column}", "count"]

    return result


def build_daily_total_general_optimized(
    df: pd.DataFrame,
    date_column: str = "timestamp",
    value_col: str = "n",
) -> pd.DataFrame:
    """
    Efficiently build daily total diagnosis counts.

    Args:
        df: Input DataFrame
        date_column: Timestamp column
        value_col: Count column

    Returns:
        Daily totals DataFrame
    """
    df = df.copy()

    # Ensure datetime
    df[date_column] = pd.to_datetime(df[date_column]).dt.floor("D")

    # Vectorized groupby
    result = df.groupby(date_column, observed=True)[value_col].sum().to_frame()
    result.columns = [total_code(DOMAIN_DIAGNOSIS)]

    return result


def build_daily_total_by_group_optimized(
    df: pd.DataFrame,
    group_col: str,
    group_label: str,
    date_column: str = "timestamp",
    value_col: str = "n",
) -> pd.DataFrame:
    """
    Build daily diagnosis totals by group using all diagnosis rows.

    This is intentionally independent from selected diagnosis-code filters, so
    RS/UP totals remain true totals while code-specific features can stay small.
    """
    df = df.copy()
    df[date_column] = pd.to_datetime(df[date_column]).dt.floor("D")
    geo_level = GEO_UP if group_label.upper() == GEO_UP else GEO_RS
    df[group_col] = clean_geo_series(df[group_col], geo_level)

    grouped = (
        df.groupby([date_column, group_col], as_index=False, observed=True)[value_col]
        .sum()
    )
    grouped["feature"] = grouped[group_col].map(
        lambda geo: total_code(DOMAIN_DIAGNOSIS, geo_level, geo)
    )
    _validate_feature_count(
        grouped["feature"].nunique(),
        f"diagnosis total {group_label} features",
    )

    wide = grouped.pivot_table(
        index=date_column,
        columns="feature",
        values=value_col,
        aggfunc="sum",
        fill_value=0,
        observed=True,
    )
    wide.index.name = date_column
    return wide.reset_index()


def build_diagnosis_wide_format_optimized(
    df: pd.DataFrame,
    date_column: str = "timestamp",
    code_column: str = "DIAG_CODE",
    value_col: str = "n",
) -> pd.DataFrame:
    """
    Efficiently build wide format diagnosis matrix (codes as columns).

    Args:
        df: Input DataFrame
        date_column: Timestamp column
        code_column: Diagnosis code column
        value_col: Count column

    Returns:
        Wide DataFrame with diagnosis codes as columns
    """
    df = df.copy()

    # Ensure datetime
    df[date_column] = pd.to_datetime(df[date_column]).dt.floor("D")

    # Efficient pivot
    result = df.pivot_table(
        index=date_column,
        columns=code_column,
        values=value_col,
        aggfunc="sum",
        fill_value=0,
        observed=True,
    )

    # Rename columns
    result.columns = [
        feature_code(DOMAIN_DIAGNOSIS, VARIABLE_ICD10_3, col)
        for col in result.columns
    ]

    result.index = pd.to_datetime(result.index)

    # Add timestamp column
    result["timestamp"] = result.index

    return result.sort_index()


def add_incremental_diagnosis_optimized(
    new_df: pd.DataFrame,
    manager,
    timestamp_col: str = "timestamp",
    code_col: str = "DIAG_CODE",
) -> None:
    """
    Add new diagnosis data to incremental storage with deduplication.

    Args:
        new_df: New data to add
        manager: ParquetIncrementalManager instance
        timestamp_col: Timestamp column
        code_col: Diagnosis code column
    """
    if new_df.empty:
        return

    # Ensure timestamp column
    if timestamp_col not in new_df.columns:
        new_df[timestamp_col] = pd.Timestamp.now()

    new_df[timestamp_col] = pd.to_datetime(new_df[timestamp_col])

    id_parts = [new_df[timestamp_col].astype(str)]

    id_columns = [
        code_col,
        f"DIAG_{code_col}",
        "DIAG_DIAG_CODE",
        "UP",
        "RS",
        "DIAG_UP",
        "DIAG_up_c",
        "DIAG_RS",
    ]
    for col in id_columns:
        if col in new_df.columns:
            id_parts.append(new_df[col].astype(str))

    if len(id_parts) == 1:
        value_cols = sorted(
            col
            for col in new_df.columns
            if col not in {timestamp_col, "data_id"}
        )
        schema_signature = hashlib.md5("|".join(value_cols).encode()).hexdigest()[:8]
        id_parts.append(pd.Series(schema_signature, index=new_df.index))

    new_df["data_id"] = id_parts[0]
    for part in id_parts[1:]:
        new_df["data_id"] = new_df["data_id"] + "_" + part

    manager.add_data(new_df, timestamp_col=timestamp_col)


def aggregate_diagnosis_final_optimized(
    incremental_manager,
    final_store,
    timestamp_col: str = "timestamp",
    with_range: Optional[tuple] = None,
    clear_incremental: bool = True,
    observed_until: Optional[pd.Timestamp] = None,
    impute_until: Optional[pd.Timestamp] = None,
) -> pd.DataFrame:
    """
    Efficiently aggregate incremental diagnosis data to final output.

    Args:
        incremental_manager: ParquetIncrementalManager instance
        final_store: ParquetFinalStore instance
        timestamp_col: Timestamp column
        with_range: Optional (start_date, end_date) to filter

    Returns:
        Aggregated DataFrame
    """
    parquet_files = incremental_manager.iter_incremental_files()
    if not parquet_files:
        logger.warning("No diagnosis incremental data to aggregate")
        return pd.DataFrame()

    parts = []
    for parquet_file in parquet_files:
        try:
            df = pd.read_parquet(parquet_file)
        except Exception as e:
            logger.error(f"Error reading {parquet_file}: {e}")
            continue

        if df.empty:
            continue

        if "data_id" in df.columns:
            df = df.drop_duplicates(subset=["data_id"], keep="last")

        if with_range:
            start, end = with_range
            df[timestamp_col] = pd.to_datetime(df[timestamp_col])
            df = df[(df[timestamp_col] >= start) & (df[timestamp_col] <= end)]

        if not df.empty:
            parts.append(_build_diagnosis_wide_final(df, timestamp_col))

    if not parts:
        logger.warning("No diagnosis incremental data to aggregate")
        return pd.DataFrame()

    df = _combine_diagnosis_wide_parts(parts, timestamp_col)
    df = _merge_with_existing_diagnosis_final(df, final_store, timestamp_col)

    if impute_until is not None:
        df = impute_tail_to_date(
            df,
            observed_until=observed_until,
            target_until=impute_until,
            timestamp_col=timestamp_col,
        )

    # Save final
    final_store.save_final(df, index_col=timestamp_col)

    if clear_incremental:
        incremental_manager.clear_incremental_files()

    logger.info(f"Aggregated diagnosis final: {len(df)} rows")

    return df


def refresh_diagnosis_final_imputation(
    final_store,
    observed_until: Optional[pd.Timestamp],
    impute_until: Optional[pd.Timestamp],
    timestamp_col: str = "timestamp",
) -> pd.DataFrame:
    """Refresh tail imputations when there are no new source rows to aggregate."""
    existing_df = final_store.load_final()
    if existing_df.empty or impute_until is None:
        return existing_df

    df = impute_tail_to_date(
        existing_df,
        observed_until=observed_until,
        target_until=impute_until,
        timestamp_col=timestamp_col,
    )
    final_store.save_final(df, index_col=timestamp_col)
    return df


def _combine_diagnosis_wide_parts(
    frames: list[pd.DataFrame],
    timestamp_col: str,
) -> pd.DataFrame:
    """Combine per-file diagnosis final frames by summing matching timestamps."""
    indexed = [frame.set_index(timestamp_col) for frame in frames if not frame.empty]
    if not indexed:
        return pd.DataFrame()

    combined = pd.concat(indexed, axis=0).fillna(0)
    combined = combined.groupby(level=0, observed=True).sum(min_count=1).fillna(0)
    combined.index.name = timestamp_col
    return combined.sort_index().reset_index()


def _build_diagnosis_wide_final(
    df: pd.DataFrame,
    timestamp_col: str = "timestamp",
) -> pd.DataFrame:
    """Build one diagnosis final row per timestamp from mixed incremental rows."""
    out = df.copy().drop(columns=["index", "data_id"], errors="ignore")

    if timestamp_col not in out.columns:
        if isinstance(out.index, pd.DatetimeIndex):
            out[timestamp_col] = out.index
        else:
            raise ValueError(f"Missing timestamp column: {timestamp_col}")

    out[timestamp_col] = pd.to_datetime(out[timestamp_col]).dt.floor("D")
    out = out.rename(
        columns={
            col: canonicalize_feature_name(col, DOMAIN_DIAGNOSIS)
            for col in out.columns
            if col != timestamp_col
        }
    )

    frames = []
    wide_cols = [
        col
        for col in out.columns
        if (
                col not in IMPUTATION_COLUMNS
                and (
                col == "DIAG_TOTAL"
                or col.startswith("DIAG_TOTAL_")
                or col.startswith("DIAG_CODE_")
                or col.startswith(f"{DOMAIN_DIAGNOSIS}__")
            )
        )
    ]
    if wide_cols:
        frames.append(_sum_numeric_by_timestamp(out, wide_cols, timestamp_col))

    has_code_wide = any(
        col.startswith("DIAG_CODE_")
        or col.startswith(f"{DOMAIN_DIAGNOSIS}__{VARIABLE_ICD10_3}__")
        for col in out.columns
    )
    if not has_code_wide and {"DIAG_DIAG_CODE", "DIAG_COUNT"}.issubset(out.columns):
        frames.append(
            _pivot_diagnosis_long(
                out,
                code_col="DIAG_DIAG_CODE",
                value_col="DIAG_COUNT",
                prefix=VARIABLE_ICD10_3,
                timestamp_col=timestamp_col,
            )
        )

    group_specs = [
        ("DIAG_RS", "RS"),
        ("DIAG_UP", "UP"),
        ("DIAG_up_c", "UP"),
        ("RS", "RS"),
        ("UP", "UP"),
    ]
    for group_col, label in group_specs:
        required = {group_col, "DIAG_DIAG_CODE", "count"}
        if required.issubset(out.columns):
            frames.append(
                _pivot_diagnosis_group(
                    out,
                    group_col=group_col,
                    label=label,
                    timestamp_col=timestamp_col,
                )
            )

    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        return pd.DataFrame()

    indexed = [frame.set_index(timestamp_col) for frame in frames]
    combined = pd.concat(indexed, axis=1).fillna(0)
    combined = combined.T.groupby(level=0).sum().T
    combined.index.name = timestamp_col
    return combined.sort_index().reset_index()


def _sum_numeric_by_timestamp(
    df: pd.DataFrame,
    value_cols: list[str],
    timestamp_col: str,
) -> pd.DataFrame:
    _validate_feature_count(len(value_cols), "wide diagnosis columns")
    out = df[[timestamp_col, *value_cols]].copy()
    out[value_cols] = out[value_cols].apply(pd.to_numeric, errors="coerce")
    return (
        out.groupby(timestamp_col, as_index=False, observed=True)[value_cols]
        .sum(min_count=1)
        .fillna(0)
    )


def _pivot_diagnosis_long(
    df: pd.DataFrame,
    code_col: str,
    value_col: str,
    prefix: str,
    timestamp_col: str,
) -> pd.DataFrame:
    out = df[[timestamp_col, code_col, value_col]].dropna(subset=[code_col]).copy()
    out[value_col] = pd.to_numeric(out[value_col], errors="coerce").fillna(0)
    out["feature"] = out[code_col].map(
        lambda code: feature_code(DOMAIN_DIAGNOSIS, prefix, code)
    )
    _validate_feature_count(out["feature"].nunique(), "diagnosis code features")

    wide = out.pivot_table(
        index=timestamp_col,
        columns="feature",
        values=value_col,
        aggfunc="sum",
        fill_value=0,
        observed=True,
    )
    wide.index.name = timestamp_col
    return wide.reset_index()


def _pivot_diagnosis_group(
    df: pd.DataFrame,
    group_col: str,
    label: str,
    timestamp_col: str,
) -> pd.DataFrame:
    out = df[[timestamp_col, group_col, "DIAG_DIAG_CODE", "count"]].dropna(
        subset=[group_col, "DIAG_DIAG_CODE"]
    ).copy()
    out["count"] = pd.to_numeric(out["count"], errors="coerce").fillna(0)
    geo_level = GEO_UP if label.upper() == GEO_UP else GEO_RS
    out[group_col] = clean_geo_series(out[group_col], geo_level)
    out["feature"] = [
        feature_code(DOMAIN_DIAGNOSIS, VARIABLE_ICD10_3, code, geo_level, geo)
        for code, geo in zip(out["DIAG_DIAG_CODE"], out[group_col])
    ]
    _validate_feature_count(out["feature"].nunique(), f"diagnosis {label} features")

    wide = out.pivot_table(
        index=timestamp_col,
        columns="feature",
        values="count",
        aggfunc="sum",
        fill_value=0,
        observed=True,
    )
    wide.index.name = timestamp_col
    return wide.reset_index()


def _validate_feature_count(feature_count: int, context: str) -> None:
    """Fail before pandas tries to allocate an impossibly wide matrix."""
    if feature_count <= MAX_DIAGNOSIS_FEATURES:
        return

    raise ValueError(
        f"Refusing to build {feature_count:,} {context}. "
        f"This usually means the selected diagnosis-code filter was not loaded "
        f"or the diagnosis code column is not normalized. "
        f"Set MAX_DIAGNOSIS_FEATURES to override if this is intentional."
    )


def _merge_with_existing_diagnosis_final(
    new_df: pd.DataFrame,
    final_store,
    timestamp_col: str = "timestamp",
) -> pd.DataFrame:
    """Merge new diagnosis final rows with the existing final parquet."""
    existing_df = final_store.load_final()
    if existing_df.empty:
        return new_df

    existing_df = drop_imputed_rows(existing_df, timestamp_col=timestamp_col)
    if existing_df.empty:
        return new_df

    existing_df = _build_diagnosis_wide_final(existing_df, timestamp_col)
    if existing_df.empty:
        return new_df

    existing_idx = existing_df.set_index(timestamp_col)
    new_idx = new_df.set_index(timestamp_col)

    # Replace complete overlapping days with the new aggregate. This avoids
    # double-counting and prevents stale values from surviving in columns that
    # are absent from a recomputed day.
    existing_idx = existing_idx[~existing_idx.index.isin(new_idx.index)]
    combined = pd.concat([existing_idx, new_idx], axis=0, sort=False)
    combined = combined.sort_index().fillna(0)
    combined.index.name = timestamp_col
    return combined.reset_index()
