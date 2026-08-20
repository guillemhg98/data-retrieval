"""Optimized demand pipeline with Parquet storage and partial incremental files."""
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional
import logging
from datetime import datetime

from pipelines.shared.imputation import (
    IMPUTATION_COLUMNS,
    drop_imputed_rows,
    impute_tail_to_date,
)
from pipelines.shared.naming import (
    DOMAIN_DEMAND,
    GEO_RS,
    GEO_UP,
    canonicalize_feature_name,
    clean_geo_series,
    feature_code,
    total_code,
)

logger = logging.getLogger(__name__)


def build_daily_total_cat_optimized(
    df: pd.DataFrame,
    date_column: str = "timestamp",
    value_col: str = "counts",
) -> pd.DataFrame:
    """
    Efficiently build daily category totals using vectorized operations.

    Args:
        df: Input DataFrame
        date_column: Timestamp column
        value_col: Value column to aggregate

    Returns:
        Daily totals DataFrame
    """
    df = df.copy()

    # Ensure datetime
    df[date_column] = pd.to_datetime(df[date_column]).dt.floor("D")

    if value_col not in df.columns:
        df[value_col] = 1

    # Vectorized groupby - much faster than iterating
    result = df.groupby(date_column, observed=True)[value_col].sum().to_frame()
    result.columns = [total_code(DOMAIN_DEMAND)]

    return result


def build_daily_features_global_optimized(
    df: pd.DataFrame,
    date_column: str = "timestamp",
    value_col: str = "counts",
    prefix: str = DOMAIN_DEMAND,
) -> pd.DataFrame:
    """
    Build daily categorical totals without RS/UP grouping.

    For example: DEMAND__SERVEI_CODI__INF, DEMAND__TIPUS_CLASS__C9C.
    These columns represent Catalunya/global totals for each category value.
    """
    df = df.copy()

    if value_col not in df.columns:
        df[value_col] = 1

    df[date_column] = pd.to_datetime(df[date_column]).dt.floor("D")

    categorical_vars = [
        col
        for col in [
            "VISI_LLOC_VISITA",
            "VISI_SITUACIO_VISITA",
            "SERVEI_CODI",
            "TIPUS_CLASS",
            "TIPUS_VISITA_AGRUPAT",
        ]
        if col in df.columns
    ]

    pieces = []
    for var in categorical_vars:
        tmp = df[[date_column, var, value_col]].copy()
        tmp[var] = tmp[var].fillna("NA").astype(str).str.strip().replace("", "NA")
        tmp["feature"] = tmp[var].map(
            lambda category: feature_code(prefix, var, category)
        )
        tmp = (
            tmp.groupby([date_column, "feature"], as_index=False, observed=True)[
                value_col
            ]
            .sum()
        )
        pieces.append(tmp)

    if not pieces:
        return pd.DataFrame()

    long_df = pd.concat(pieces, ignore_index=True)
    wide = long_df.pivot_table(
        index=date_column,
        columns="feature",
        values=value_col,
        aggfunc="sum",
        fill_value=0,
        observed=True,
    )
    wide.index = pd.to_datetime(wide.index)
    wide["timestamp"] = wide.index
    return wide.sort_index()

def build_daily_features_by_group_optimized(
    df: pd.DataFrame,
    group_col: str,
    date_column: str = "timestamp",
    value_col: str = "counts",
    prefix: str = DOMAIN_DEMAND,
) -> pd.DataFrame:
    """
    Efficiently build daily features grouped by category using vectorized operations.

    Args:
        df: Input DataFrame
        group_col: Column to group by
        date_column: Timestamp column
        value_col: Value column to aggregate
        prefix: Prefix for column names

    Returns:
        Wide DataFrame with daily features
    """
    df = df.copy()

    if value_col not in df.columns:
        df[value_col] = 1

    if group_col not in df.columns:
        if "UP" in df.columns:
            df[group_col] = df["UP"]
        else:
            df[group_col] = "NA"

    # Ensure datetime
    df[date_column] = pd.to_datetime(df[date_column]).dt.floor("D")

    # Select and clean only needed columns
    cols_to_use = [
        date_column,
        group_col,
        "VISI_LLOC_VISITA",
        "VISI_SITUACIO_VISITA",
        "SERVEI_CODI",
        "TIPUS_CLASS",
        "TIPUS_VISITA_AGRUPAT",
        value_col,
    ]

    df = df[[c for c in cols_to_use if c in df.columns]].copy()

    pieces = []

    # Process each categorical variable efficiently
    categorical_vars = [
        col
        for col in [
            "VISI_LLOC_VISITA",
            "VISI_SITUACIO_VISITA",
            "SERVEI_CODI",
            "TIPUS_CLASS",
            "TIPUS_VISITA_AGRUPAT",
        ]
        if col in df.columns
    ]

    for var in categorical_vars:
        # Vectorized string operations
        tmp = df[[date_column, group_col, var, value_col]].copy()

        # Fast string cleaning
        geo_level = GEO_UP if group_col.upper() == GEO_UP else GEO_RS
        tmp[group_col] = clean_geo_series(tmp[group_col], geo_level)
        tmp[var] = (
            tmp[var].fillna("NA").astype(str).str.strip().replace("", "NA")
        )

        # Vectorized column creation
        tmp["feature"] = [
            feature_code(prefix, var, category, geo_level, geo)
            for category, geo in zip(tmp[var], tmp[group_col])
        ]

        # Efficient groupby
        tmp = (
            tmp.groupby([date_column, "feature"], as_index=False, observed=True)[
                value_col
            ]
            .sum()
        )

        pieces.append(tmp)

    # Total per group
    tmp_total = df[[date_column, group_col, value_col]].copy()
    geo_level = GEO_UP if group_col.upper() == GEO_UP else GEO_RS
    tmp_total[group_col] = clean_geo_series(tmp_total[group_col], geo_level)
    tmp_total["feature"] = tmp_total[group_col].map(
        lambda geo: total_code(prefix, geo_level, geo)
    )
    tmp_total = (
        tmp_total.groupby([date_column, "feature"], as_index=False, observed=True)[
            value_col
        ]
        .sum()
    )

    pieces.append(tmp_total)

    # Concatenate all pieces
    long_df = pd.concat(pieces, ignore_index=True)

    # Efficient pivot with memory optimization
    wide = long_df.pivot_table(
        index=date_column,
        columns="feature",
        values=value_col,
        aggfunc="sum",
        fill_value=0,
        observed=True,
    )

    wide.index = pd.to_datetime(wide.index)

    # Add timestamp column
    wide["timestamp"] = wide.index

    return wide.sort_index()


def add_incremental_optimized(
    new_df: pd.DataFrame,
    manager,
    timestamp_col: str = "timestamp",
) -> None:
    """
    Add new data to incremental storage with deduplication.

    Args:
        new_df: New data to add
        manager: ParquetIncrementalManager instance
        timestamp_col: Timestamp column
    """
    if new_df.empty:
        return

    # Ensure timestamp column
    if timestamp_col not in new_df.columns:
        new_df[timestamp_col] = pd.Timestamp.now()

    new_df[timestamp_col] = pd.to_datetime(new_df[timestamp_col])

    # Add to manager (handles deduplication and retention)
    manager.add_data(new_df, timestamp_col=timestamp_col)


def aggregate_final_optimized(
    incremental_manager,
    final_store,
    timestamp_col: str = "timestamp",
    with_range: Optional[tuple] = None,
    clear_incremental: bool = True,
    observed_until: Optional[pd.Timestamp] = None,
    impute_until: Optional[pd.Timestamp] = None,
) -> pd.DataFrame:
    """
    Efficiently aggregate incremental data to final output.

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
        logger.warning("No incremental data to aggregate")
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
            parts.append(_build_wide_final_by_timestamp(df, timestamp_col))

    if not parts:
        logger.warning("No incremental data to aggregate")
        return pd.DataFrame()

    df = _combine_wide_parts(parts, timestamp_col)
    df = _merge_with_existing_final(df, final_store, timestamp_col)

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

    return df


def refresh_final_imputation(
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


def _combine_wide_parts(
    frames: list[pd.DataFrame],
    timestamp_col: str,
) -> pd.DataFrame:
    """Combine already-aggregated wide frames without loading raw history."""
    indexed = [frame.set_index(timestamp_col) for frame in frames if not frame.empty]
    if not indexed:
        return pd.DataFrame()

    combined = pd.concat(indexed, axis=0).fillna(0)
    combined = combined.groupby(level=0, observed=True).sum(min_count=1).fillna(0)
    combined.index.name = timestamp_col
    return combined.sort_index().reset_index()


def _build_wide_final_by_timestamp(
    df: pd.DataFrame,
    timestamp_col: str = "timestamp",
) -> pd.DataFrame:
    """Combine partial wide incremental rows into one row per timestamp."""
    out = df.copy().drop(columns=["index"], errors="ignore")

    if timestamp_col not in out.columns:
        if isinstance(out.index, pd.DatetimeIndex):
            out[timestamp_col] = out.index
        else:
            raise ValueError(f"Missing timestamp column: {timestamp_col}")

    out[timestamp_col] = pd.to_datetime(out[timestamp_col]).dt.floor("D")
    out = out.rename(
        columns={
            col: canonicalize_feature_name(col, DOMAIN_DEMAND)
            for col in out.columns
            if col != timestamp_col
        }
    )

    value_cols = [
        col
        for col in out.columns
        if col != timestamp_col and col not in IMPUTATION_COLUMNS
    ]
    out[value_cols] = out[value_cols].apply(pd.to_numeric, errors="coerce")

    return (
        out.groupby(timestamp_col, as_index=False, observed=True)[value_cols]
        .sum(min_count=1)
        .fillna(0)
        .sort_values(timestamp_col)
    )


def _merge_with_existing_final(
    new_df: pd.DataFrame,
    final_store,
    timestamp_col: str = "timestamp",
) -> pd.DataFrame:
    """Merge new final rows with the existing final parquet, if present."""
    existing_df = final_store.load_final()
    if existing_df.empty:
        return new_df

    existing_df = drop_imputed_rows(existing_df, timestamp_col=timestamp_col)
    if existing_df.empty:
        return new_df

    existing_df = _build_wide_final_by_timestamp(existing_df, timestamp_col)

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
