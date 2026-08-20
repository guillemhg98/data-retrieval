"""Optimized diagnosis pipeline main runner with Parquet storage."""
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
    get_incremental_processing_window,
)
from pipelines.shared.parquet_storage import ParquetIncrementalManager, ParquetFinalStore
from .aggregation_optimized import (
    build_daily_diagnosis_counts_optimized,
    build_daily_diagnosis_by_group_optimized,
    build_daily_total_by_group_optimized,
    build_daily_total_general_optimized,
    aggregate_diagnosis_final_optimized,
    refresh_diagnosis_final_imputation,
    _build_diagnosis_wide_final,
)

logger = setup_logging()


def validate_table_columns(
    conn,
    schema: str,
    table_name: str,
    required_columns: list[str],
) -> None:
    """Validate that the required columns exist in the target table."""
    query = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    """
    existing = pd.read_sql_query(query, conn, params=[schema, table_name])
    existing_columns = {str(c).upper() for c in existing["COLUMN_NAME"].tolist()}

    missing = [col for col in required_columns if str(col).upper() not in existing_columns]
    if missing:
        raise ValueError(
            f"Missing columns in {schema}.{table_name}: {missing}. "
            f"Available columns: {sorted(existing_columns)}"
        )


def get_diagnosis_data_for_year_optimized(
    conn,
    schema: str,
    table_name: str,
    date_column: str,
    up_column: str,
    diag_code_column: str,
    year_start: pd.Timestamp,
    year_end: pd.Timestamp,
    last_loaded_date: Optional[pd.Timestamp] = None,
) -> pd.DataFrame:
    """Query diagnosis data already aggregated by day, UP, and code prefix."""
    date_expr = f"CAST([{date_column}] AS date)"
    code_expr = (
        f"UPPER(LEFT(LTRIM(RTRIM(CAST([{diag_code_column}] AS VARCHAR(50)))), 3))"
    )

    if last_loaded_date is None:
        query = f"""
        SELECT
            {date_expr} AS [timestamp],
            [{up_column}] AS [{up_column}],
            {code_expr} AS [DIAG_CODE],
            COUNT_BIG(*) AS [n]
        FROM [{schema}].[{table_name}]
        WHERE [{date_column}] >= ?
            AND [{date_column}] < ?
            AND [{diag_code_column}] IS NOT NULL
        GROUP BY {date_expr}, [{up_column}], {code_expr}
        ORDER BY [timestamp] ASC
        """
        params = [year_start, year_end]
    else:
        query = f"""
        SELECT
            {date_expr} AS [timestamp],
            [{up_column}] AS [{up_column}],
            {code_expr} AS [DIAG_CODE],
            COUNT_BIG(*) AS [n]
        FROM [{schema}].[{table_name}]
        WHERE [{date_column}] >= ?
            AND [{date_column}] < ?
            AND [{date_column}] > ?
            AND [{diag_code_column}] IS NOT NULL
        GROUP BY {date_expr}, [{up_column}], {code_expr}
        ORDER BY [timestamp] ASC
        """
        params = [year_start, year_end, last_loaded_date]

    return pd.read_sql_query(query, conn, params=params)


def _normalize_diag_codes(values: pd.Series) -> pd.Series:
    """Normalize diagnosis values to the ICD10 3-character code used by filters."""
    return values.astype("string").str.strip().str.upper().str[:3]


def _expand_diagnosis_code_spec(value) -> list[str]:
    """Expand one ICD10_3 code or range specification into normalized codes."""
    if pd.isna(value):
        return []

    token = str(value).strip().upper()
    if not token:
        return []

    token = token.replace("–", "-").replace("—", "-")
    token = token.replace(" ", "")
    if "-" not in token:
        code = _normalize_diag_codes(pd.Series([token])).iloc[0]
        return [] if pd.isna(code) or not code else [str(code)]

    start_raw, end_raw = token.split("-", 1)
    start = _parse_icd10_3_bound(start_raw)
    end = _parse_icd10_3_bound(end_raw, start_letter=start[0] if start else None)
    if start is None or end is None:
        code = _normalize_diag_codes(pd.Series([token])).iloc[0]
        return [] if pd.isna(code) or not code else [str(code)]

    start_letter, start_num = start
    end_letter, end_num = end
    if (end_letter, end_num) < (start_letter, start_num):
        return []

    codes = []
    for letter_ord in range(ord(start_letter), ord(end_letter) + 1):
        letter = chr(letter_ord)
        first_num = start_num if letter == start_letter else 0
        last_num = end_num if letter == end_letter else 99
        codes.extend(f"{letter}{num:02d}" for num in range(first_num, last_num + 1))
    return codes


def _parse_icd10_3_bound(
    value: str,
    start_letter: Optional[str] = None,
) -> Optional[tuple[str, int]]:
    """Parse an ICD10_3 range bound, accepting incomplete upper bounds like F4."""
    match = re.match(r"^([A-Z]?)(\d{1,2})", value)
    if not match:
        return None

    letter = match.group(1) or start_letter
    if not letter:
        return None

    digits = match.group(2)
    number = int(digits)
    if len(digits) == 1:
        number = number * 10 + 9

    return letter, number


def _normalize_up_codes(values: pd.Series) -> pd.Series:
    """Normalize UP codes so they match the Excel mapping."""
    return values.astype("string").str.strip().str.zfill(5)


def _normalize_rs_values(values: pd.Series) -> pd.Series:
    """Normalize RS labels for stable matching against selected RS files."""
    return values.astype("string").str.strip().str.upper()


def _normalize_feature_name(value, fallback: str) -> str:
    """Build a stable column-name token from an optional human label."""
    if pd.isna(value):
        return fallback

    token = str(value).strip()
    if not token:
        return fallback

    token = re.sub(r"[^0-9A-Za-z]+", "_", token).strip("_").upper()
    return token or fallback


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
    legacy_filename: Optional[str] = None,
    legacy_subdir: Optional[str] = None,
) -> Optional[dict[str, str]]:
    """Load a one-column selection CSV from the shared selections folder."""
    candidates = []
    if selection_file:
        active_path = Path(selection_file)
        candidates.append(active_path)
        resolved = active_path.resolve()
        for parent_index in (1, 3):
            try:
                base_dir = resolved.parents[parent_index]
            except IndexError:
                continue
            candidates.append(base_dir / "selections" / filename)
            if legacy_subdir and legacy_filename:
                candidates.append(
                    base_dir / "diagnosis_pipeline" / legacy_subdir / legacy_filename
                )

    candidates.append(Path.cwd() / "selections" / filename)
    if legacy_subdir and legacy_filename:
        candidates.append(Path.cwd() / "diagnosis_pipeline" / legacy_subdir / legacy_filename)

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
            logger.warning(f"Selected {label} file is empty: {path}")
            continue

        logger.info(f"Loaded {len(values)} selected {label} from {path}")
        return values

    if found_file:
        logger.info(f"No selected {label} configured in selections/{filename}.")
    else:
        logger.warning(
            f"No selected {label} file found. Expected "
            f"selections/{filename}."
        )
    return None


def _load_selected_codes(
    selected_codes_file: Optional[str | Path],
) -> Optional[dict[str, list[str]]]:
    """Load selected diagnosis-code prefixes mapped to one or more aliases."""
    candidates = []
    if selected_codes_file:
        active_path = Path(selected_codes_file)
        candidates.append(active_path)
        resolved = active_path.resolve()
        for parent_index in (1, 3):
            try:
                base_dir = resolved.parents[parent_index]
            except IndexError:
                continue
            candidates.append(base_dir / "selections" / "selected_diagnosis_codes.csv")
            candidates.append(
                base_dir
                / "diagnosis_pipeline"
                / "selected_codes"
                / "selected_codes.csv"
            )

    candidates.append(Path.cwd() / "selections" / "selected_diagnosis_codes.csv")
    candidates.append(
        Path.cwd() / "diagnosis_pipeline" / "selected_codes" / "selected_codes.csv"
    )

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
        selected_df = pd.read_csv(path)
        if selected_df.empty:
            logger.warning(f"Selected diagnosis codes file is empty: {path}")
            continue

        code_aliases: dict[str, list[str]] = {}
        column_lookup = {str(col).strip().lower(): col for col in selected_df.columns}
        alias_col = column_lookup.get("feature_name")
        for _, row in selected_df.iterrows():
            codes = _expand_diagnosis_code_spec(row.iloc[0])
            if not codes:
                continue

            alias = (
                _normalize_feature_name(row[alias_col], fallback=codes[0])
                if alias_col is not None
                else None
            )
            for code in codes:
                output_alias = alias or code
                aliases = code_aliases.setdefault(code, [])
                if output_alias not in aliases:
                    aliases.append(output_alias)

        if not code_aliases:
            logger.warning(f"Selected diagnosis codes file has no usable codes: {path}")
            continue

        mapping_count = sum(len(aliases) for aliases in code_aliases.values())
        alias_count = len(
            {alias for aliases in code_aliases.values() for alias in aliases}
        )
        logger.info(
            f"Loaded {len(code_aliases)} selected diagnosis codes with "
            f"{mapping_count} code-to-output mappings as {alias_count} "
            f"output groups from {path}"
        )
        return code_aliases

    if found_file:
        logger.info(
            "No selected diagnosis codes configured in "
            "selections/selected_diagnosis_codes.csv."
        )
    else:
        logger.warning(
            "No selected diagnosis codes file found. Expected "
            "selections/selected_diagnosis_codes.csv."
        )
    return None


def _expand_selected_code_aliases(
    df: pd.DataFrame,
    selected_codes: dict[str, list[str]],
    code_col: str = "DIAG_CODE",
) -> pd.DataFrame:
    """Duplicate selected code rows once per requested output alias."""
    if not selected_codes:
        return df.iloc[0:0].copy()

    mapping = pd.DataFrame(
        [
            (source_code, alias)
            for source_code, aliases in selected_codes.items()
            for alias in aliases
        ],
        columns=[code_col, "_DIAG_OUTPUT_ALIAS"],
    )
    out = df.merge(mapping, on=code_col, how="inner")
    if out.empty:
        return out.drop(columns=["_DIAG_OUTPUT_ALIAS"], errors="ignore")

    out[code_col] = out["_DIAG_OUTPUT_ALIAS"]
    return out.drop(columns=["_DIAG_OUTPUT_ALIAS"])


def _load_selected_rs(selected_rs_file: Optional[str | Path]) -> Optional[dict[str, str]]:
    """Load selected RS labels."""
    return _load_selection_values(
        selection_file=selected_rs_file,
        filename="selected_rs.csv",
        label="RS values",
        normalizer=_normalize_rs_values,
    )


def _load_selected_up(selected_up_file: Optional[str | Path]) -> Optional[dict[str, str]]:
    """Load selected UP codes."""
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
) -> pd.DataFrame:
    """Filter a dataframe only when a selection file was provided."""
    if selected_values is None:
        return df

    if column.upper() == "UP" or column.lower() == "up_c":
        normalized = _normalize_up_codes(df[column])
    else:
        normalized = _normalize_rs_values(df[column])

    mask = normalized.isin(selected_values.keys())
    out = df[mask].copy()
    if out.empty:
        return out

    out[column] = normalized[mask].map(selected_values).astype(str)
    return out


def run_incremental_diagnosis_pipeline_optimized(
    db_server: str,
    db_database: str,
    schema: str,
    table_name: str,
    date_column: str,
    up_column: str,
    diag_code_column: str,
    up_rs: pd.DataFrame,
    incremental_dir: str | Path,
    final_file: str | Path,
    selected_codes_file: Optional[str | Path] = None,
    selected_rs_file: Optional[str | Path] = None,
    selected_up_file: Optional[str | Path] = None,
    auth_mode: str = "ActiveDirectoryIntegrated",
    min_valid_date: str = "2008-01-01",
    retention_days: Optional[int] = None,
    start_date: Optional[str | pd.Timestamp] = None,
    end_date: Optional[str | pd.Timestamp] = None,
) -> None:
    """
    Run optimized incremental diagnosis pipeline with Parquet storage.

    Args:
        db_server: Database server
        db_database: Database name
        schema: Schema name
        table_name: Table name
        date_column: Date column in table
        up_column: UP column name
        diag_code_column: Diagnosis code column
        up_rs: UP-RS mapping DataFrame
        incremental_dir: Directory for incremental parquet files
        final_file: Final output parquet file
        selected_codes_file: Optional file with selected diagnosis codes to filter
        selected_rs_file: Optional file with selected RS values for grouped outputs
        selected_up_file: Optional file with selected UP values for grouped outputs
        auth_mode: Database authentication mode
        min_valid_date: Minimum date to process
        retention_days: Days of incremental data to keep. None keeps all history,
            which is required when rebuilding final daily files from 2008 onward.
        start_date: Optional inclusive start day. If provided, overrides resume.
        end_date: Optional inclusive end day. Defaults to today when omitted.
    """
    logger.info("Starting optimized diagnosis pipeline...")

    # Initialize storage managers
    incremental_mgr = ParquetIncrementalManager(
        incremental_dir,
        retention_days=retention_days,
        chunk_size=10000,
    )
    final_store = ParquetFinalStore(final_file)

    selected_codes = _load_selected_codes(selected_codes_file)
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
        # Validate table schema before querying
        validate_table_columns(
            conn=conn,
            schema=schema,
            table_name=table_name,
            required_columns=[date_column, up_column, diag_code_column],
        )

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

        window = get_incremental_processing_window(
            min_date=min_date,
            max_date=max_date,
            last_processed_date=last_loaded_date,
            requested_start_date=start_date,
            requested_end_date=end_date,
        )
        if window is None:
            logger.info("No new diagnosis days to process on or before today")
            refresh_diagnosis_final_imputation(
                final_store,
                observed_until=source_observed_until,
                impute_until=requested_end_day,
            )
            return

        start_date, end_exclusive, max_process_day = window
        logger.info(
            f"Processing new diagnosis days: {start_date.date()} -> "
            f"{max_process_day.date()}"
        )

        # Process by year for memory efficiency
        year_ranges = get_year_ranges(start_date, max_process_day)
        global_max_loaded = last_loaded_date

        for year, year_start, year_end in year_ranges:
            logger.info(f"Processing diagnosis year {year}")
            effective_year_start = max(year_start, start_date)
            effective_year_end = min(year_end, end_exclusive)
            if effective_year_end <= effective_year_start:
                logger.info(f"No diagnosis data on or before today for year {year}")
                continue

            # Query data efficiently
            df_chunk = get_diagnosis_data_for_year_optimized(
                conn=conn,
                schema=schema,
                table_name=table_name,
                date_column=date_column,
                up_column=up_column,
                diag_code_column=diag_code_column,
                year_start=effective_year_start,
                year_end=effective_year_end,
                last_loaded_date=None,
            )

            if df_chunk.empty:
                logger.info(f"No data for year {year}")
                continue

            logger.info(f"Year {year}: {len(df_chunk)} rows")

            df_chunk["DIAG_CODE"] = _normalize_diag_codes(df_chunk["DIAG_CODE"])
            df_chunk = df_chunk.dropna(subset=["DIAG_CODE"])
            df_chunk = df_chunk[df_chunk["DIAG_CODE"] != ""]

            if df_chunk.empty:
                continue

            # Prepare data
            df_chunk["timestamp"] = pd.to_datetime(df_chunk["timestamp"]).dt.floor("D")
            df_chunk = df_chunk.dropna(subset=["timestamp"])
            df_chunk = df_chunk[df_chunk["timestamp"] < end_exclusive].copy()
            if df_chunk.empty:
                logger.info(f"No diagnosis rows on or before today for year {year}")
                continue

            df_chunk[up_column] = _normalize_up_codes(df_chunk[up_column])
            df_chunk["n"] = pd.to_numeric(df_chunk["n"], errors="coerce").fillna(0)

            # Add UP-RS mapping
            up_rs_map = up_rs[["Codi UP", "RS"]].copy()
            up_rs_map["Codi UP"] = _normalize_up_codes(up_rs_map["Codi UP"])
            up_rs_map["RS"] = _normalize_rs_values(up_rs_map["RS"])
            up_rs_map.columns = [up_column, "RS"]
            before_merge = len(df_chunk)
            df_chunk = df_chunk.merge(
                up_rs_map, on=up_column, how="left"
            ).fillna("UNKNOWN")
            unknown_count = (df_chunk["RS"] == "UNKNOWN").sum()
            if unknown_count > 0:
                logger.warning(f"Found {unknown_count} rows with unknown UP codes (out of {before_merge} total)")
                unknown_ups = df_chunk[df_chunk["RS"] == "UNKNOWN"][up_column].unique()
                logger.warning(f"Unknown UP codes: {list(unknown_ups)[:10]}...")  # Show first 10

            # Build aggregations efficiently
            general_total = build_daily_total_general_optimized(df_chunk).reset_index()
            rs_total = build_daily_total_by_group_optimized(
                _filter_if_selected(df_chunk, "RS", selected_rs),
                group_col="RS",
                group_label="RS",
            )
            up_total = build_daily_total_by_group_optimized(
                _filter_if_selected(df_chunk, up_column, selected_up),
                group_col=up_column,
                group_label="UP",
            )

            if selected_codes:
                code_df = _expand_selected_code_aliases(df_chunk, selected_codes)
                logger.info(
                    f"Selected diagnosis-code rows for year {year}: "
                    f"{len(code_df)} output rows from {len(df_chunk)} aggregated rows "
                    f"across {code_df['DIAG_CODE'].nunique()} output groups"
                )
            else:
                code_df = df_chunk.iloc[0:0].copy()
                logger.warning(
                    "Skipping code-specific diagnosis features because no selected "
                    "diagnosis-code file was found"
                )

            code_daily = build_daily_diagnosis_counts_optimized(code_df)
            rs_daily = build_daily_diagnosis_by_group_optimized(
                _filter_if_selected(code_df, "RS", selected_rs),
                group_col="RS",
            )
            up_daily = build_daily_diagnosis_by_group_optimized(
                _filter_if_selected(code_df, up_column, selected_up),
                group_col=up_column,
            )

            # Store one already-aggregated daily file per processed year.
            yearly_daily = _build_diagnosis_wide_final(
                pd.concat(
                    [
                        general_total,
                        rs_total,
                        up_total,
                        code_daily,
                        rs_daily,
                        up_daily,
                    ],
                    ignore_index=True,
                    sort=False,
                )
            )
            incremental_mgr.add_data(yearly_daily, timestamp_col="timestamp")
            logger.info(
                f"Saved diagnosis year {year} daily aggregate "
                f"({len(yearly_daily)} rows)"
            )

            # Track max date
            chunk_max = df_chunk["timestamp"].max()
            if pd.notna(chunk_max):
                if global_max_loaded is None or chunk_max > global_max_loaded:
                    global_max_loaded = chunk_max

            # Clean up
            del (
                df_chunk,
                general_total,
                rs_total,
                up_total,
                code_df,
                code_daily,
                rs_daily,
                up_daily,
                yearly_daily,
            )

        # Aggregate to final
        logger.info("Aggregating diagnosis to final output...")
        aggregate_diagnosis_final_optimized(
            incremental_mgr,
            final_store,
            observed_until=max_process_day,
            impute_until=requested_end_day,
        )

        logger.info("Diagnosis pipeline completed successfully")

    finally:
        conn.close()


def run_diagnosis_pipeline_main_optimized(
    config,
    start_date: Optional[str | pd.Timestamp] = None,
    end_date: Optional[str | pd.Timestamp] = None,
) -> None:
    """Main entry point for optimized diagnosis pipeline."""
    run_incremental_diagnosis_pipeline_optimized(
        db_server=config.DB_SERVER,
        db_database=config.DB_DATABASE,
        schema=config.SCHEMA,
        table_name=config.TABLE_NAME,
        date_column=config.DATE_COLUMN,
        up_column=config.UP_COLUMN,
        diag_code_column=config.DIAG_CODE_COLUMN,
        up_rs=pd.read_excel(config.resolve_up_rs_file(), sheet_name=config.UP_RS_SHEET),
        incremental_dir=config.PIPELINE_DATA_DIR / "incremental",
        final_file=config.PIPELINE_DATA_DIR / "finals" / "diagnosis_final.parquet",
        selected_codes_file=config.SELECTED_CODES_FILE,
        selected_rs_file=config.SELECTED_RS_FILE,
        selected_up_file=config.SELECTED_UP_FILE,
        auth_mode=config.AUTH_MODE,
        min_valid_date=config.MIN_VALID_DATE,
        retention_days=None,
        start_date=start_date,
        end_date=end_date,
    )
