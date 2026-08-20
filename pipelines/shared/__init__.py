"""Shared utilities for pipelines."""
from .utils import (
    ensure_daily_range,
    load_output_matrix,
    save_output_matrix,
    load_state,
    save_state,
    load_last_date_from_output,
    latest_timestamp,
    get_incremental_processing_window,
    get_min_max_date,
    get_year_ranges,
    get_data_for_year,
)
from .logging_config import setup_logging
from .parquet_storage import (
    ParquetIncrementalManager,
    ParquetFinalStore,
    load_and_merge_final_outputs,
)
from .imputation import (
    IMPUTED_COL,
    IMPUTATION_COLUMNS,
    drop_imputed_rows,
    impute_tail_to_date,
)
from .final_joiner import FinalDataJoiner, IncrementalFinalJoiner


def get_connection(*args, **kwargs):
    """Create a database connection, importing pyodbc only when needed."""
    from .db import get_connection as _get_connection

    return _get_connection(*args, **kwargs)


__all__ = [
    "get_connection",
    "ensure_daily_range",
    "load_output_matrix",
    "save_output_matrix",
    "load_state",
    "save_state",
    "load_last_date_from_output",
    "latest_timestamp",
    "get_incremental_processing_window",
    "get_min_max_date",
    "get_year_ranges",
    "get_data_for_year",
    "setup_logging",
    "ParquetIncrementalManager",
    "ParquetFinalStore",
    "load_and_merge_final_outputs",
    "IMPUTED_COL",
    "IMPUTATION_COLUMNS",
    "drop_imputed_rows",
    "impute_tail_to_date",
    "FinalDataJoiner",
    "IncrementalFinalJoiner",
]
