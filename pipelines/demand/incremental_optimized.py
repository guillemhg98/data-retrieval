"""Optimized demand pipeline main runner with Parquet storage."""
import pandas as pd
import logging
import re
from pathlib import Path
from typing import Optional

from pipelines.shared import (
    get_connection,
    setup_logging,
    get_min_max_date,
    get_year_ranges,
    get_half_year_ranges,
    get_data_for_year,
    get_incremental_processing_window,
)
from pipelines.shared.parquet_storage import ParquetIncrementalManager, ParquetFinalStore
from .aggregation_optimized import (
    build_daily_total_cat_optimized,
    build_daily_features_global_optimized,
    build_daily_features_by_group_optimized,
    aggregate_final_optimized,
    refresh_final_imputation,
    _build_wide_final_by_timestamp,
)
from .transformations import prepare_visits_chunk

logger = setup_logging()


def _normalize_up_codes(values: pd.Series) -> pd.Series:
    """Normalize UP codes for matching selected UP files."""
    return values.astype("string").str.strip().str.zfill(5)


def _normalize_rs_values(values: pd.Series) -> pd.Series:
    """Normalize RS labels for matching selected RS files."""
    return values.astype("string").str.strip().str.upper()


def _normalize_geo_alias(value, fallback: str) -> str:
    """Normalize a configured territorial subset identifier for output names."""
    if pd.isna(value):
        return fallback

    token = str(value).strip()
    if not token:
        return fallback

    token = re.sub(r"[^0-9A-Za-z]+", "_", token).strip("_").upper()
    return token or fallback


def _selection_map_from_df(
    selection_df: pd.DataFrame,
    normalizer,
) -> dict[str, str]:
    """Build source-value -> output-geo-id mapping from a selection CSV."""
    if selection_df.empty:
        return {}

    columns = list(selection_df.columns)
    column_lookup = {str(col).strip().lower(): col for col in columns}
    source_col = (
        column_lookup.get("rs")
        or column_lookup.get("up")
        or column_lookup.get("value")
        or column_lookup.get("source_value")
        or (columns[1] if len(columns) > 1 else columns[0])
    )
    alias_col = (
        column_lookup.get("geo_id")
        or column_lookup.get("id")
        or column_lookup.get("subset_id")
        or (columns[0] if len(columns) > 1 else source_col)
    )

    mapping = {}
    for _, row in selection_df.iterrows():
        source_values = normalizer(pd.Series([row[source_col]])).dropna()
        if source_values.empty:
            continue

        source = str(source_values.iloc[0])
        if not source:
            continue

        alias = _normalize_geo_alias(row[alias_col], fallback=source)
        mapping[source] = alias
    return mapping


def _load_selection_values(
    selection_file: Optional[str | Path],
    filename: str,
    label: str,
    normalizer,
) -> Optional[dict[str, str]]:
    """Load a one-column shared selection CSV."""
    candidates = []
    if selection_file:
        active_path = Path(selection_file)
        candidates.append(active_path)
        try:
            base_dir = active_path.resolve().parents[1]
            candidates.append(base_dir / "selections" / filename)
        except IndexError:
            pass

    candidates.append(Path.cwd() / "selections" / filename)

    seen = set()
    found_file = False
    for path in candidates:
        path = Path(path)
        path_key = path.resolve()
        if path_key in seen:
            continue
        seen.add(path_key)

        if not path.exists():
            continue

        found_file = True
        selection_df = pd.read_csv(path, dtype=str)
        values = _selection_map_from_df(selection_df, normalizer)
        if not values:
            logger.warning(f"Selected demand {label} file is empty: {path}")
            continue

        logger.info(f"Loaded {len(values)} selected demand {label} from {path}")
        return values

    if found_file:
        logger.info(
            f"No selected demand {label} configured; all values will be included "
            "for this grouped output."
        )
    else:
        logger.warning(
            f"No selected demand {label} file found. Expected "
            f"selections/{filename}. "
            "All values will be included for this grouped output."
        )
    return None


def _load_selected_rs(selected_rs_file: Optional[str | Path]) -> Optional[dict[str, str]]:
    """Load selected RS labels for demand grouped outputs."""
    return _load_selection_values(
        selection_file=selected_rs_file,
        filename="selected_rs.csv",
        label="RS values",
        normalizer=_normalize_rs_values,
    )


def _load_selected_up(selected_up_file: Optional[str | Path]) -> Optional[dict[str, str]]:
    """Load selected UP codes for demand grouped outputs."""
    return _load_selection_values(
        selection_file=selected_up_file,
        filename="selected_up.csv",
        label="UP values",
        normalizer=_normalize_up_codes,
    )


def _filter_if_selected(
    df: pd.DataFrame,
    column: str,
    selected_values: Optional[dict[str, str]],
    normalizer,
) -> pd.DataFrame:
    """Filter a dataframe only when a selection file contains values."""
    if selected_values is None:
        return df

    normalized = normalizer(df[column])
    mask = normalized.isin(selected_values.keys())
    out = df[mask].copy()
    if out.empty:
        return out

    out[column] = normalized[mask].map(selected_values).astype(str)
    return out


def _expected_geo_aliases(
    selected_rs: Optional[dict[str, str]],
    selected_up: Optional[dict[str, str]],
) -> dict[str, set[str]]:
    expected = {}
    if selected_rs is not None:
        expected["RS"] = set(selected_rs.values())
    if selected_up is not None:
        expected["UP"] = set(selected_up.values())
    return expected


def _missing_geo_sources(
    final_store: ParquetFinalStore,
    selected_rs: Optional[dict[str, str]],
    selected_up: Optional[dict[str, str]],
) -> dict[str, dict[str, str]]:
    existing = final_store.load_final()
    if existing.empty:
        return {}

    existing_cols = {str(col) for col in existing.columns}
    missing: dict[str, dict[str, str]] = {}
    for geo_level, selected in (("RS", selected_rs), ("UP", selected_up)):
        if not selected:
            continue

        missing_level = {
            source: alias
            for source, alias in selected.items()
            if not any(f"__{geo_level}__{alias}" in col for col in existing_cols)
        }
        if missing_level:
            missing[geo_level] = missing_level

    return missing


def _up_filter_for_missing_geos(
    missing_geos: dict[str, dict[str, str]],
    up_rs: pd.DataFrame,
) -> Optional[list[str]]:
    values = set(missing_geos.get("UP", {}).keys())
    missing_rs = set(missing_geos.get("RS", {}).keys())
    if missing_rs:
        up_rs_map = up_rs[["Codi UP", "RS"]].copy()
        up_rs_map["Codi UP"] = _normalize_up_codes(up_rs_map["Codi UP"])
        up_rs_map["RS"] = _normalize_rs_values(up_rs_map["RS"])
        values.update(up_rs_map.loc[up_rs_map["RS"].isin(missing_rs), "Codi UP"])

    return sorted(values) if values else None


def _process_demand_range(
    conn,
    schema: str,
    table_name: str,
    date_column: str,
    range_label: str,
    range_start: pd.Timestamp,
    range_end: pd.Timestamp,
    end_exclusive: pd.Timestamp,
    up_rs: pd.DataFrame,
    incremental_mgr: ParquetIncrementalManager,
    selected_rs: Optional[dict[str, str]],
    selected_up: Optional[dict[str, str]],
    missing_geos: Optional[dict[str, dict[str, str]]] = None,
) -> Optional[pd.Timestamp]:
    where_in = None
    if missing_geos:
        up_filter = _up_filter_for_missing_geos(missing_geos, up_rs)
        if up_filter:
            where_in = {"UP": up_filter}

    df_chunk = get_data_for_year(
        conn=conn,
        schema=schema,
        table_name=table_name,
        date_column=date_column,
        year_start=range_start,
        year_end=range_end,
        last_loaded_date=None,
        selected_cols=[
            "DATA_VISITA",
            "UP",
            "VISI_LLOC_VISITA",
            "VISI_SITUACIO_VISITA",
            "SERVEI_CODI",
            "TIPUS_CLASS",
            "VISI_TIPUS_VISITA",
        ],
        where_in=where_in,
    )

    if df_chunk.empty:
        logger.info(f"No demand data for {range_label}")
        return None

    logger.info(f"Demand {range_label}: {len(df_chunk)} rows")
    df_chunk = prepare_visits_chunk(df_chunk, up_rs=up_rs)
    df_chunk = df_chunk[df_chunk["DATA_VISITA"] < end_exclusive].copy()
    if df_chunk.empty:
        logger.info(f"No demand rows on or before today for {range_label}")
        return None

    df_chunk["timestamp"] = df_chunk["DATA_VISITA"]
    pieces = []
    if not missing_geos:
        pieces.extend(
            [
                build_daily_total_cat_optimized(df_chunk).reset_index(),
                build_daily_features_global_optimized(df_chunk),
            ]
        )

    rs_selection = (
        missing_geos.get("RS")
        if missing_geos and "RS" in missing_geos
        else selected_rs
    )
    up_selection = (
        missing_geos.get("UP")
        if missing_geos and "UP" in missing_geos
        else selected_up
    )
    if not missing_geos or "RS" in missing_geos:
        pieces.append(
            build_daily_features_by_group_optimized(
                _filter_if_selected(df_chunk, "RS", rs_selection, _normalize_rs_values),
                group_col="RS",
            )
        )
    if not missing_geos or "UP" in missing_geos:
        pieces.append(
            build_daily_features_by_group_optimized(
                _filter_if_selected(df_chunk, "UP", up_selection, _normalize_up_codes),
                group_col="UP",
            )
        )

    yearly_daily = _build_wide_final_by_timestamp(
        pd.concat(pieces, ignore_index=True, sort=False)
    )
    incremental_mgr.add_data(yearly_daily, timestamp_col="timestamp")
    logger.info(
        f"Saved demand {range_label} daily aggregate ({len(yearly_daily)} rows)"
    )
    return df_chunk["timestamp"].max()


def _process_demand_range_with_fallback(
    conn,
    schema: str,
    table_name: str,
    date_column: str,
    range_label: str,
    range_start: pd.Timestamp,
    range_end: pd.Timestamp,
    end_exclusive: pd.Timestamp,
    up_rs: pd.DataFrame,
    incremental_mgr: ParquetIncrementalManager,
    selected_rs: Optional[dict[str, str]],
    selected_up: Optional[dict[str, str]],
    missing_geos: Optional[dict[str, dict[str, str]]] = None,
) -> Optional[pd.Timestamp]:
    try:
        return _process_demand_range(
            conn,
            schema,
            table_name,
            date_column,
            range_label,
            range_start,
            range_end,
            end_exclusive,
            up_rs,
            incremental_mgr,
            selected_rs,
            selected_up,
            missing_geos,
        )
    except Exception as exc:
        logger.warning(
            f"Demand {range_label} failed; retrying in 6-month chunks: {exc}",
            exc_info=True,
        )

    max_loaded = None
    for half_label, half_start, half_end in get_half_year_ranges(range_start, range_end):
        chunk_max = _process_demand_range(
            conn,
            schema,
            table_name,
            date_column,
            half_label,
            half_start,
            half_end,
            end_exclusive,
            up_rs,
            incremental_mgr,
            selected_rs,
            selected_up,
            missing_geos,
        )
        if pd.notna(chunk_max) and (max_loaded is None or chunk_max > max_loaded):
            max_loaded = chunk_max
    return max_loaded


def run_incremental_pipeline_optimized(
    db_server: str,
    db_database: str,
    schema: str,
    table_name: str,
    date_column: str,
    up_rs: pd.DataFrame,
    incremental_dir: str | Path,
    final_file: str | Path,
    selected_rs_file: Optional[str | Path] = None,
    selected_up_file: Optional[str | Path] = None,
    auth_mode: str = "ActiveDirectoryIntegrated",
    min_valid_date: str = "2008-01-01",
    retention_days: Optional[int] = None,
    start_date: Optional[str | pd.Timestamp] = None,
    end_date: Optional[str | pd.Timestamp] = None,
) -> None:
    """
    Run optimized incremental demand pipeline with Parquet storage.

    Args:
        db_server: Database server
        db_database: Database name
        schema: Schema name
        table_name: Table name
        date_column: Date column in table
        up_rs: UP-RS mapping DataFrame
        incremental_dir: Directory for incremental parquet files
        final_file: Final output parquet file
        selected_rs_file: Optional file with selected RS values for grouped outputs
        selected_up_file: Optional file with selected UP values for grouped outputs
        auth_mode: Database authentication mode
        min_valid_date: Minimum date to process
        retention_days: Days of incremental data to keep. None keeps all history,
            which is required when rebuilding final daily files from 2008 onward.
        start_date: Optional inclusive start day. If provided, overrides resume.
        end_date: Optional inclusive end day. Defaults to today when omitted.
    """
    logger.info("Starting optimized demand pipeline...")

    # Initialize storage managers
    incremental_mgr = ParquetIncrementalManager(
        incremental_dir,
        retention_days=retention_days,
        chunk_size=10000,
    )
    final_store = ParquetFinalStore(final_file)
    selected_rs = _load_selected_rs(selected_rs_file)
    selected_up = _load_selected_up(selected_up_file)

    # Get last processed day from metadata, with final parquet as a fallback.
    metadata_exists = incremental_mgr.metadata_file.exists()
    metadata_last_date = incremental_mgr.get_last_timestamp()
    final_last_date = final_store.get_last_contiguous_timestamp()
    if metadata_exists and metadata_last_date is not None and final_last_date is not None:
        last_loaded_date = min(metadata_last_date, final_last_date)
    elif metadata_exists:
        last_loaded_date = metadata_last_date
    else:
        last_loaded_date = final_last_date
    logger.info(
        f"Last processed day: {last_loaded_date} "
        f"(metadata={metadata_last_date}, final_contiguous={final_last_date}, "
        f"metadata_exists={metadata_exists})"
    )

    # Connect to database
    conn = get_connection(db_server, db_database, auth_mode=auth_mode)

    try:
        # Get data range
        min_date, max_date = get_min_max_date(
            conn=conn,
            schema=schema,
            table_name=table_name,
            date_column=date_column,
            min_valid_date=min_valid_date,
        )

        if min_date is None or max_date is None:
            logger.info("No valid data in source table")
            return

        today_day = pd.Timestamp.today().normalize()
        requested_end_day = (
            today_day
            if end_date is None
            else min(pd.to_datetime(end_date).normalize(), today_day)
        )
        source_observed_until = min(pd.to_datetime(max_date).normalize(), requested_end_day)
        expected_aliases = _expected_geo_aliases(selected_rs, selected_up)
        missing_geos = _missing_geo_sources(final_store, selected_rs, selected_up)
        has_selection_refresh = bool(missing_geos)
        if has_selection_refresh:
            logger.info(
                "Detected demand selection columns to backfill: %s",
                {
                    geo_level: sorted(values.values())
                    for geo_level, values in missing_geos.items()
                },
            )

        normal_window = get_incremental_processing_window(
            min_date=min_date,
            max_date=max_date,
            last_processed_date=last_loaded_date,
            requested_start_date=start_date,
            requested_end_date=end_date,
        )

        processing_jobs = []
        if has_selection_refresh:
            selection_end_date = end_date
            if last_loaded_date is not None and pd.notna(last_loaded_date):
                selection_end_date = min(
                    pd.to_datetime(last_loaded_date).normalize(),
                    requested_end_day,
                )
            selection_window = get_incremental_processing_window(
                min_date=min_date,
                max_date=max_date,
                last_processed_date=None,
                requested_start_date=start_date,
                requested_end_date=selection_end_date,
            )
            if selection_window is not None:
                processing_jobs.append(("selection backfill", selection_window, missing_geos))

        if normal_window is not None:
            processing_jobs.append(("new days", normal_window, None))

        if not processing_jobs:
            logger.info("No new demand days to process on or before today")
            refresh_final_imputation(
                final_store,
                observed_until=source_observed_until,
                impute_until=requested_end_day,
                expected_geo_aliases=expected_aliases,
            )
            return

        global_max_loaded = last_loaded_date
        observed_days = []

        for job_label, window, job_missing_geos in processing_jobs:
            job_start, end_exclusive, max_process_day = window
            observed_days.append(max_process_day)
            logger.info(
                f"Processing demand {job_label}: {job_start.date()} -> "
                f"{max_process_day.date()}"
            )

            # Process by year for memory efficiency. If a year is too large,
            # retry the same range in half-year chunks before the final join.
            year_ranges = get_year_ranges(job_start, max_process_day)
            for year, year_start, year_end in year_ranges:
                logger.info(f"Processing demand {job_label} year {year}")
                effective_year_start = max(year_start, job_start)
                effective_year_end = min(year_end, end_exclusive)
                if effective_year_end <= effective_year_start:
                    logger.info(f"No demand data on or before today for year {year}")
                    continue

                chunk_max = _process_demand_range_with_fallback(
                    conn=conn,
                    schema=schema,
                    table_name=table_name,
                    date_column=date_column,
                    range_label=f"{job_label} {year}",
                    range_start=effective_year_start,
                    range_end=effective_year_end,
                    end_exclusive=end_exclusive,
                    up_rs=up_rs,
                    incremental_mgr=incremental_mgr,
                    selected_rs=selected_rs,
                    selected_up=selected_up,
                    missing_geos=job_missing_geos,
                )
                if pd.notna(chunk_max):
                    if global_max_loaded is None or chunk_max > global_max_loaded:
                        global_max_loaded = chunk_max

        # Aggregate to final
        logger.info("Aggregating to final output...")
        aggregate_final_optimized(
            incremental_mgr,
            final_store,
            observed_until=max(observed_days) if observed_days else source_observed_until,
            impute_until=requested_end_day,
            expected_geo_aliases=expected_aliases,
            partial_column_update=has_selection_refresh,
        )

        logger.info("Demand pipeline completed successfully")

    finally:
        conn.close()


def run_demand_pipeline_main_optimized(
    config,
    start_date: Optional[str | pd.Timestamp] = None,
    end_date: Optional[str | pd.Timestamp] = None,
) -> None:
    """Main entry point for optimized demand pipeline."""
    run_incremental_pipeline_optimized(
        db_server=config.DB_SERVER,
        db_database=config.DB_DATABASE,
        schema=config.SCHEMA,
        table_name=config.TABLE_NAME,
        date_column=config.DATE_COLUMN,
        up_rs=pd.read_excel(config.resolve_up_rs_file(), sheet_name=config.UP_RS_SHEET),
        incremental_dir=config.PIPELINE_DATA_DIR / "incremental",
        final_file=config.PIPELINE_DATA_DIR / "finals" / "demand_final.parquet",
        selected_rs_file=config.SELECTED_RS_FILE,
        selected_up_file=config.SELECTED_UP_FILE,
        auth_mode=config.AUTH_MODE,
        min_valid_date=config.MIN_VALID_DATE,
        retention_days=None,
        start_date=start_date,
        end_date=end_date,
    )
