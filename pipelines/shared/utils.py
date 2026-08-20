"""Shared utility functions for data processing and state management."""
from __future__ import annotations

import json
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import pyodbc


def ensure_daily_range(
    df: pd.DataFrame,
    start: Optional[str] = None,
    end: Optional[str] = None,
    fill_value: int | float = 0,
) -> pd.DataFrame:
    """
    Ensure dataframe has continuous daily index with no gaps.
    
    Args:
        df: DataFrame with datetime index
        start: Start date (optional)
        end: End date (optional)
        fill_value: Value to fill gaps with
        
    Returns:
        pd.DataFrame: DataFrame with continuous daily index
    """
    out = df.copy()
    out.index = pd.to_datetime(out.index, errors="coerce")
    out = out[~out.index.isna()].sort_index()

    if out.empty:
        return out

    idx_start = pd.to_datetime(start) if start is not None else out.index.min()
    idx_end = pd.to_datetime(end) if end is not None else out.index.max()

    full_idx = pd.date_range(idx_start, idx_end, freq="D")
    out = out.reindex(full_idx).fillna(fill_value)
    return out


def load_output_matrix(path: str | Path) -> pd.DataFrame:
    """Load output matrix from CSV file with datetime index."""
    path = Path(path)
    df = pd.read_csv(path, index_col="Timestamp")
    df.index = pd.to_datetime(df.index)
    return df.sort_index()


def save_output_matrix(df: pd.DataFrame, path: str | Path) -> None:
    """Save output matrix to CSV file with datetime index."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, encoding="utf-8-sig", index_label="Timestamp")


def load_state(state_file: str | Path) -> Optional[pd.Timestamp]:
    """
    Load pipeline state (last loaded date) from JSON file.
    
    Args:
        state_file: Path to state JSON file
        
    Returns:
        pd.Timestamp or None: Last loaded date from state
    """
    state_file = Path(state_file)

    if not state_file.exists():
        return None

    try:
        with open(state_file, "r", encoding="utf-8") as f:
            state = json.load(f)
    except Exception:
        return None

    value = state.get("last_loaded_date")
    if value is None:
        return None

    return pd.to_datetime(value, errors="coerce")


def save_state(state_file: str | Path, last_loaded_date: pd.Timestamp) -> None:
    """Save pipeline state (last loaded date) to JSON file."""
    state_file = Path(state_file)
    state_file.parent.mkdir(parents=True, exist_ok=True)

    with open(state_file, "w", encoding="utf-8") as f:
        json.dump(
            {
                "last_loaded_date": pd.to_datetime(last_loaded_date).isoformat(),
                "updated_at": datetime.now().isoformat()
            },
            f,
            ensure_ascii=False,
            indent=2
        )


def load_last_date_from_output(final_file: str | Path) -> Optional[pd.Timestamp]:
    """Get the maximum date from output file."""
    final_file = Path(final_file)

    if not final_file.exists():
        return None

    try:
        df = pd.read_csv(final_file, index_col="Timestamp")
    except Exception:
        return None

    if df.empty:
        return None

    idx = pd.to_datetime(df.index, errors="coerce")
    idx = idx[~idx.isna()]

    if len(idx) == 0:
        return None

    return idx.max()


def latest_timestamp(*values: Optional[pd.Timestamp]) -> Optional[pd.Timestamp]:
    """Return the latest valid timestamp from optional timestamp values."""
    valid = []
    for value in values:
        if value is None:
            continue
        ts = pd.to_datetime(value, errors="coerce")
        if pd.notna(ts):
            valid.append(ts)

    if not valid:
        return None

    return max(valid)


def get_incremental_processing_window(
    min_date: pd.Timestamp,
    max_date: pd.Timestamp,
    last_processed_date: Optional[pd.Timestamp] = None,
    requested_start_date: Optional[pd.Timestamp] = None,
    requested_end_date: Optional[pd.Timestamp] = None,
    today: Optional[pd.Timestamp] = None,
) -> Optional[tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp]]:
    """
    Resolve the daily incremental window.

    If requested_start_date is provided, the explicit requested range wins.
    Otherwise, the pipelines resume at the day after the latest processed day.
    This avoids re-reading part of the same day when the source timestamp column
    has hours. If nothing has been processed yet, start from min_date.

    Returns:
        (start_day, end_exclusive, max_process_day) or None when there is no
        new day to process.
    """
    min_day = pd.to_datetime(min_date).normalize()
    source_max_day = pd.to_datetime(max_date).normalize()
    today_day = (
        pd.Timestamp.today().normalize()
        if today is None
        else pd.to_datetime(today).normalize()
    )
    requested_end_day = (
        None
        if requested_end_date is None
        else pd.to_datetime(requested_end_date).normalize()
    )
    max_process_day = min(
        source_max_day,
        today_day,
        requested_end_day if requested_end_day is not None else today_day,
    )

    if requested_start_date is not None:
        start_day = pd.to_datetime(requested_start_date).normalize()
    elif last_processed_date is None or pd.isna(last_processed_date):
        start_day = min_day
    else:
        start_day = pd.to_datetime(last_processed_date).normalize() + pd.Timedelta(days=1)

    start_day = max(start_day, min_day)
    end_exclusive = max_process_day + pd.Timedelta(days=1)
    if start_day >= end_exclusive:
        return None

    return start_day, end_exclusive, max_process_day


def get_min_max_date(
    conn: pyodbc.Connection,
    schema: str,
    table_name: str,
    date_column: str,
    min_valid_date: str = "2000-01-01",
    max_valid_date: Optional[str | pd.Timestamp] = None,
) -> tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    """Get minimum and maximum dates from database table.

    The upper bound defaults to tomorrow so today's rows are included while
    obviously future/corrupt source timestamps are ignored before pandas parses
    the database MAX value.
    """
    if max_valid_date is None:
        max_valid_date = (
            pd.Timestamp.today().normalize() + pd.Timedelta(days=1)
        ).strftime("%Y-%m-%d")

    query = f"""
    SELECT
        MIN([{date_column}]) AS min_date,
        MAX([{date_column}]) AS max_date
    FROM [{schema}].[{table_name}]
    WHERE [{date_column}] IS NOT NULL
        AND [{date_column}] >= ?
        AND [{date_column}] < ?
    """
    df = pd.read_sql_query(query, conn, params=[min_valid_date, max_valid_date])

    if df.empty or pd.isna(df.loc[0, "min_date"]) or pd.isna(df.loc[0, "max_date"]):
        return None, None

    return pd.to_datetime(df.loc[0, "min_date"]), pd.to_datetime(df.loc[0, "max_date"])


def get_year_ranges(
    start_date: pd.Timestamp, end_date: pd.Timestamp
) -> list[tuple[int, pd.Timestamp, pd.Timestamp]]:
    """Generate year-based date ranges for batch processing."""
    ranges = []
    for year in range(start_date.year, end_date.year + 1):
        year_start = pd.Timestamp(f"{year}-01-01 00:00:00")
        year_end = pd.Timestamp(f"{year + 1}-01-01 00:00:00")
        ranges.append((year, year_start, year_end))
    return ranges


def get_data_for_year(
    conn: pyodbc.Connection,
    schema: str,
    table_name: str,
    date_column: str,
    year_start: pd.Timestamp,
    year_end: pd.Timestamp,
    last_loaded_date: Optional[pd.Timestamp] = None,
    selected_cols: Optional[list[str]] = None,
) -> pd.DataFrame:
    """Query data for a specific year, optionally filtering by last loaded date."""
    cols_sql = ", ".join(f"[{c}]" for c in selected_cols)

    if last_loaded_date is None:
        query = f"""
        SELECT {cols_sql}
        FROM [{schema}].[{table_name}]
        WHERE [{date_column}] >= ?
            AND [{date_column}] < ?
        ORDER BY [{date_column}] ASC
        """
        params = [year_start, year_end]
    else:
        query = f"""
        SELECT {cols_sql}
        FROM [{schema}].[{table_name}]
        WHERE [{date_column}] >= ?
            AND [{date_column}] < ?
            AND [{date_column}] > ?
        ORDER BY [{date_column}] ASC
        """
        params = [year_start, year_end, last_loaded_date]

    return pd.read_sql_query(query, conn, params=params)
