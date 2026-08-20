"""
Optimized pipeline runner with Parquet storage.

This version supports two execution modes:
- Production mode: query SQL Server / Azure Synapse.
- Sample mode: use bundled synthetic CSV data and write sample Parquet outputs.
"""
import argparse
import shutil
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.config import DemandConfig, DiagnosisConfig, get_config
from pipelines.shared import FinalDataJoiner, setup_logging
from pipelines.shared.imputation import (
    IMPUTATION_CREATED_AT_COL,
    IMPUTATION_METHOD_COL,
    IMPUTATION_SOURCE_LAST_DATE_COL,
    IMPUTED_COL,
    is_imputed_series,
    write_imputation_metadata_files,
)

logger = setup_logging()


def convert_parquet_file(
    input_file: str | Path,
    output_format: str,
    output_file: Optional[str | Path] = None,
) -> Path:
    """Convert one Parquet file to CSV or Excel."""
    input_path = Path(input_file)
    if not input_path.exists():
        raise FileNotFoundError(f"Parquet file not found: {input_path}")

    output_format = output_format.lower()
    if output_format == "xlsx":
        output_format = "excel"
    if output_format not in {"csv", "excel"}:
        raise ValueError("output_format must be 'csv' or 'excel'")

    suffix = ".csv" if output_format == "csv" else ".xlsx"
    output_path = Path(output_file) if output_file else input_path.with_suffix(suffix)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Loading parquet file: {input_path}")
    df = pd.read_parquet(input_path)

    if output_format == "csv":
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
    else:
        df.to_excel(output_path, index=False)

    logger.info(
        f"Converted {input_path} to {output_path} "
        f"({len(df)} rows, {len(df.columns)} columns)"
    )
    return output_path


def print_parquet_rows(
    input_file: str | Path,
    start_date: Optional[pd.Timestamp] = None,
    end_date: Optional[pd.Timestamp] = None,
    date_column: str = "timestamp",
    columns: Optional[list[str]] = None,
    limit: int = 100,
) -> pd.DataFrame:
    """Print rows from a Parquet file filtered by an inclusive date range."""
    input_path = _require_existing_parquet(input_file)
    if limit < 0:
        raise ValueError("--parquet-limit must be 0 or a positive integer")

    df = pd.read_parquet(input_path)
    filtered = _filter_rows_by_date(df, date_column, start_date, end_date)
    filtered = _with_display_date_column(filtered, df, date_column)

    if columns:
        display_columns = _resolve_display_columns(filtered, date_column, columns)
        filtered = filtered[display_columns]

    if date_column in filtered.columns:
        filtered = filtered.sort_values(date_column)

    display_df = filtered if limit == 0 else filtered.head(limit)
    range_text = _format_range_text(start_date, end_date)
    print(f"File: {input_path}")
    print(f"Date column: {date_column}")
    print(f"Rows matched{range_text}: {len(filtered)} of {len(df)}")
    if limit and len(filtered) > len(display_df):
        print(f"Showing first {len(display_df)} rows. Use --parquet-limit 0 to print all.")

    if display_df.empty:
        print("(no rows matched)")
    else:
        with pd.option_context(
            "display.max_columns",
            None,
            "display.width",
            240,
            "display.max_colwidth",
            120,
        ):
            print(display_df.to_string(index=False))

    return filtered


def delete_parquet_rows(
    input_file: str | Path,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    date_column: str = "timestamp",
    dry_run: bool = False,
    create_backup: bool = True,
) -> tuple[int, int, Optional[Path]]:
    """
    Delete rows from a Parquet file by inclusive date range.

    Returns (deleted_rows, remaining_rows, backup_path).
    """
    input_path = _require_existing_parquet(input_file)
    if start_date is None or end_date is None:
        raise ValueError("--delete-parquet-rows requires --start-date and --end-date")
    if start_date > end_date:
        raise ValueError("--start-date cannot be after --end-date")

    df = pd.read_parquet(input_path)
    mask = _date_range_mask(df, date_column, start_date, end_date)
    deleted_rows = int(mask.sum())
    remaining = df.loc[~mask].copy()

    if dry_run:
        _preview_processing_metadata_after_delete(
            input_path,
            remaining,
            date_column=date_column,
            cursor_before=start_date,
        )
        return deleted_rows, len(remaining), None

    if deleted_rows == 0:
        return deleted_rows, len(remaining), None

    stamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    backup_path = input_path.with_name(f"{input_path.name}.bak_{stamp}")
    temp_path = input_path.with_name(f".{input_path.name}.tmp_{stamp}")

    if create_backup:
        shutil.copy2(input_path, backup_path)

    try:
        remaining.to_parquet(
            temp_path,
            compression="snappy",
            index=False,
            row_group_size=100000,
        )
        temp_path.replace(input_path)
    finally:
        if temp_path.exists():
            temp_path.unlink()

    _sync_processing_metadata_after_delete(
        input_path,
        remaining,
        date_column=date_column,
        create_backup=create_backup,
        cursor_before=start_date,
    )

    return deleted_rows, len(remaining), backup_path if create_backup else None


def check_parquet_imputation(
    input_file: str | Path,
    date_column: str = "timestamp",
) -> bool:
    """Validate imputation metadata columns in a final or joined Parquet file."""
    input_path = _require_existing_parquet(input_file)
    df = pd.read_parquet(input_path)
    imputed_cols = [col for col in df.columns if col.endswith(IMPUTED_COL)]

    print(f"File: {input_path}")
    print(f"Rows: {len(df)}, Columns: {len(df.columns)}")

    if not imputed_cols:
        print("No imputation metadata columns found.")
        return False

    timestamps = _timestamp_series(df, date_column)
    today = pd.Timestamp.today().normalize()
    ok = True

    duplicate_timestamps = int(timestamps[timestamps.notna()].duplicated().sum())
    future_rows = int((timestamps > today).sum())
    if duplicate_timestamps:
        ok = False
        print(f"WARNING: {duplicate_timestamps} duplicate timestamp rows found.")
    if future_rows:
        ok = False
        print(f"WARNING: {future_rows} rows are dated after today.")

    suffixes = [
        IMPUTED_COL,
        IMPUTATION_METHOD_COL,
        IMPUTATION_SOURCE_LAST_DATE_COL,
        IMPUTATION_CREATED_AT_COL,
    ]
    for imputed_col in imputed_cols:
        prefix = imputed_col[: -len(IMPUTED_COL)]
        label = prefix.rstrip("_") or "base"
        required = [f"{prefix}{suffix}" for suffix in suffixes]
        missing = [col for col in required if col not in df.columns]
        if missing:
            ok = False
            print(f"{label}: missing metadata columns: {', '.join(missing)}")
            continue

        imputed_mask = is_imputed_series(df[imputed_col])
        observed_count = int((~imputed_mask).sum())
        imputed_count = int(imputed_mask.sum())
        observed_dates = timestamps[(~imputed_mask) & timestamps.notna()]
        imputed_dates = timestamps[imputed_mask & timestamps.notna()]

        last_observed = observed_dates.max() if not observed_dates.empty else None
        imputed_range = (
            f"{imputed_dates.min().date()} -> {imputed_dates.max().date()}"
            if not imputed_dates.empty
            else "none"
        )
        last_observed_text = (
            last_observed.date().isoformat()
            if last_observed is not None and pd.notna(last_observed)
            else "none"
        )

        print(
            f"{label}: observed={observed_count}, imputed={imputed_count}, "
            f"last_observed={last_observed_text}, imputed_range={imputed_range}"
        )

        if imputed_count == 0:
            continue

        method_col = f"{prefix}{IMPUTATION_METHOD_COL}"
        source_col = f"{prefix}{IMPUTATION_SOURCE_LAST_DATE_COL}"
        created_col = f"{prefix}{IMPUTATION_CREATED_AT_COL}"
        for col in [method_col, source_col, created_col]:
            missing_values = df.loc[imputed_mask, col].isna() | (
                df.loc[imputed_mask, col].astype("string").str.strip() == ""
            )
            if bool(missing_values.any()):
                ok = False
                print(f"{label}: WARNING {int(missing_values.sum())} imputed rows have empty {col}.")

    print("Imputation check: OK" if ok else "Imputation check: REVIEW WARNINGS")
    return ok


def write_parquet_imputation_metadata(
    input_file: str | Path,
    date_column: str = "timestamp",
) -> tuple[Path, Path]:
    """Write imputation sidecar metadata files for an existing Parquet file."""
    input_path = _require_existing_parquet(input_file)
    df = pd.read_parquet(input_path)
    summary_path, rows_path = write_imputation_metadata_files(
        df,
        input_path,
        timestamp_col=date_column,
    )
    logger.info(f"Wrote imputation metadata summary: {summary_path}")
    logger.info(f"Wrote imputed row list: {rows_path}")
    return summary_path, rows_path


def _require_existing_parquet(input_file: str | Path) -> Path:
    input_path = Path(input_file)
    if not input_path.exists():
        raise FileNotFoundError(f"Parquet file not found: {input_path}")
    if input_path.suffix.lower() != ".parquet":
        raise ValueError(f"Expected a .parquet file: {input_path}")
    return input_path


def _parse_column_list(value: Optional[str]) -> Optional[list[str]]:
    if value is None:
        return None
    columns = [col.strip() for col in value.split(",") if col.strip()]
    return columns or None


def _resolve_display_columns(
    df: pd.DataFrame,
    date_column: str,
    columns: list[str],
) -> list[str]:
    display_columns = [date_column] if date_column in df.columns else []
    display_columns.extend(col for col in columns if col != date_column)
    missing = [col for col in display_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Column(s) not found: {', '.join(missing)}")
    return display_columns


def _filter_rows_by_date(
    df: pd.DataFrame,
    date_column: str,
    start_date: Optional[pd.Timestamp],
    end_date: Optional[pd.Timestamp],
) -> pd.DataFrame:
    mask = _date_range_mask(df, date_column, start_date, end_date)
    return df.loc[mask].copy()


def _date_range_mask(
    df: pd.DataFrame,
    date_column: str,
    start_date: Optional[pd.Timestamp],
    end_date: Optional[pd.Timestamp],
) -> pd.Series:
    timestamps = _timestamp_series(df, date_column)
    mask = pd.Series(True, index=df.index)

    if start_date is not None:
        mask &= timestamps.notna() & (timestamps >= pd.to_datetime(start_date).normalize())
    if end_date is not None:
        end_exclusive = pd.to_datetime(end_date).normalize() + pd.Timedelta(days=1)
        mask &= timestamps.notna() & (timestamps < end_exclusive)

    return mask


def _timestamp_series(df: pd.DataFrame, date_column: str) -> pd.Series:
    if date_column in df.columns:
        return pd.to_datetime(df[date_column], errors="coerce")
    if isinstance(df.index, pd.DatetimeIndex):
        return pd.Series(pd.to_datetime(df.index, errors="coerce"), index=df.index)
    raise ValueError(f"Date column not found: {date_column}")


def _with_display_date_column(
    filtered: pd.DataFrame,
    source_df: pd.DataFrame,
    date_column: str,
) -> pd.DataFrame:
    out = filtered.copy()
    if date_column not in out.columns and isinstance(source_df.index, pd.DatetimeIndex):
        timestamps = pd.Series(pd.to_datetime(source_df.index), index=source_df.index)
        out.insert(0, date_column, timestamps.loc[out.index].to_numpy())
    return out


def _format_range_text(
    start_date: Optional[pd.Timestamp],
    end_date: Optional[pd.Timestamp],
) -> str:
    if start_date is None and end_date is None:
        return ""
    start_text = start_date.date().isoformat() if start_date is not None else "beginning"
    end_text = end_date.date().isoformat() if end_date is not None else "end"
    return f" from {start_text} to {end_text}"


def _sync_processing_metadata_after_delete(
    parquet_path: Path,
    remaining_df: pd.DataFrame,
    date_column: str,
    create_backup: bool,
    cursor_before: Optional[pd.Timestamp],
) -> Optional[Path]:
    metadata_path, metadata_df = _build_metadata_after_delete(
        parquet_path,
        remaining_df,
        date_column=date_column,
        cursor_before=cursor_before,
    )
    if metadata_path is None:
        return None

    if create_backup and metadata_path.exists():
        stamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        metadata_backup = metadata_path.with_name(f"{metadata_path.name}.bak_{stamp}")
        shutil.copy2(metadata_path, metadata_backup)
        logger.info(f"Metadata backup written before sync: {metadata_backup}")

    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_df.to_parquet(metadata_path, index=False)

    _log_metadata_sync_result("Updated", metadata_path, metadata_df)

    return metadata_path


def _preview_processing_metadata_after_delete(
    parquet_path: Path,
    remaining_df: pd.DataFrame,
    date_column: str,
    cursor_before: Optional[pd.Timestamp],
) -> None:
    metadata_path, metadata_df = _build_metadata_after_delete(
        parquet_path,
        remaining_df,
        date_column=date_column,
        cursor_before=cursor_before,
    )
    if metadata_path is None:
        logger.info(
            "No pipeline metadata inferred for this Parquet path. "
            "Joined outputs do not control demand/diagnosis resume state."
        )
        return

    _log_metadata_sync_result("Would update", metadata_path, metadata_df)


def _build_metadata_after_delete(
    parquet_path: Path,
    remaining_df: pd.DataFrame,
    date_column: str,
    cursor_before: Optional[pd.Timestamp],
) -> tuple[Optional[Path], pd.DataFrame]:
    metadata_path = _infer_processing_metadata_path(parquet_path)
    if metadata_path is None:
        return None, pd.DataFrame(columns=["last_update", "min_timestamp", "max_timestamp", "num_rows"])

    if parquet_path.parent.name == "incremental":
        metadata_df = _build_incremental_directory_metadata(
            metadata_path.parent,
            date_column,
            cursor_before=cursor_before,
            override_file=parquet_path,
            override_df=remaining_df,
        )
    else:
        metadata_df = _build_processing_metadata(
            remaining_df,
            date_column,
            cursor_before=cursor_before,
        )

    return metadata_path, metadata_df


def _log_metadata_sync_result(
    action: str,
    metadata_path: Path,
    metadata_df: pd.DataFrame,
) -> None:

    if metadata_df.empty:
        logger.info(f"{action} processing metadata: {metadata_path} (no remaining observed rows)")
    else:
        max_timestamp = metadata_df["max_timestamp"].iloc[0]
        logger.info(
            f"{action} processing metadata: {metadata_path} "
            f"(max_timestamp={max_timestamp})"
        )


def _infer_processing_metadata_path(parquet_path: Path) -> Optional[Path]:
    parent = parquet_path.parent

    if parent.name == "incremental" and parquet_path.name != "metadata.parquet":
        return parent / "metadata.parquet"

    if parent.name == "finals" and parent.parent.name.endswith("_pipeline"):
        return parent.parent / "incremental" / "metadata.parquet"

    return None


def _build_processing_metadata(
    df: pd.DataFrame,
    date_column: str,
    cursor_before: Optional[pd.Timestamp] = None,
) -> pd.DataFrame:
    columns = ["last_update", "min_timestamp", "max_timestamp", "num_rows"]
    if df.empty:
        return pd.DataFrame(columns=columns)

    timestamps = _timestamp_series(df, date_column)
    observed_mask = pd.Series(True, index=df.index)
    if IMPUTED_COL in df.columns:
        observed_mask = ~is_imputed_series(df[IMPUTED_COL])

    observed_timestamps = timestamps[observed_mask & timestamps.notna()]
    if cursor_before is not None:
        cursor_day = pd.to_datetime(cursor_before).normalize()
        observed_timestamps = observed_timestamps[observed_timestamps < cursor_day]

    if observed_timestamps.empty:
        return pd.DataFrame(columns=columns)

    return pd.DataFrame(
        [
            {
                "last_update": pd.Timestamp.now(),
                "min_timestamp": observed_timestamps.min(),
                "max_timestamp": observed_timestamps.max(),
                "num_rows": len(observed_timestamps),
            }
        ],
        columns=columns,
    )


def _build_incremental_directory_metadata(
    incremental_dir: Path,
    date_column: str,
    cursor_before: Optional[pd.Timestamp] = None,
    override_file: Optional[Path] = None,
    override_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    frames = []
    override_resolved = override_file.resolve() if override_file is not None else None
    for parquet_file in sorted(incremental_dir.glob("*.parquet")):
        if parquet_file.name == "metadata.parquet":
            continue
        try:
            if override_resolved is not None and parquet_file.resolve() == override_resolved:
                frames.append(override_df.copy() if override_df is not None else pd.DataFrame())
            else:
                frames.append(pd.read_parquet(parquet_file))
        except Exception as exc:
            logger.warning(f"Could not read incremental file for metadata sync: {parquet_file}: {exc}")

    if not frames:
        return pd.DataFrame(columns=["last_update", "min_timestamp", "max_timestamp", "num_rows"])

    combined = pd.concat(frames, ignore_index=True, sort=False)
    return _build_processing_metadata(
        combined,
        date_column,
        cursor_before=cursor_before,
    )


def _parse_cli_date(value: Optional[str], arg_name: str) -> Optional[pd.Timestamp]:
    """Parse an optional YYYY-MM-DD CLI date as a normalized Timestamp."""
    if value is None:
        return None

    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"{arg_name} must be a valid date in YYYY-MM-DD format")

    return parsed.normalize()


def run_demand_pipeline_optimized(
    config: Optional[DemandConfig] = None,
    start_date: Optional[str | pd.Timestamp] = None,
    end_date: Optional[str | pd.Timestamp] = None,
) -> bool:
    """Run optimized demand pipeline with Parquet storage."""
    if config is None:
        config = get_config("demand")

    logger.info("=" * 80)
    logger.info("STARTING OPTIMIZED DEMAND PIPELINE")
    logger.info("=" * 80)

    try:
        from pipelines.demand.incremental_optimized import run_demand_pipeline_main_optimized

        run_demand_pipeline_main_optimized(
            config,
            start_date=start_date,
            end_date=end_date,
        )
        logger.info("Demand pipeline completed successfully")
        return True
    except Exception as e:
        logger.error(f"Demand pipeline failed: {e}", exc_info=True)
        return False


def run_diagnosis_pipeline_optimized(
    config: Optional[DiagnosisConfig] = None,
    start_date: Optional[str | pd.Timestamp] = None,
    end_date: Optional[str | pd.Timestamp] = None,
) -> bool:
    """Run optimized diagnosis pipeline with Parquet storage."""
    if config is None:
        config = get_config("diagnosis")

    logger.info("=" * 80)
    logger.info("STARTING OPTIMIZED DIAGNOSIS PIPELINE")
    logger.info("=" * 80)

    try:
        from pipelines.diagnosis.incremental_optimized import (
            run_diagnosis_pipeline_main_optimized,
        )

        run_diagnosis_pipeline_main_optimized(
            config,
            start_date=start_date,
            end_date=end_date,
        )
        logger.info("Diagnosis pipeline completed successfully")
        return True
    except Exception as e:
        logger.error(f"Diagnosis pipeline failed: {e}", exc_info=True)
        return False


def join_final_outputs(
    demand_file: Optional[Path] = None,
    diagnosis_file: Optional[Path] = None,
    output_file: Optional[Path] = None,
) -> bool:
    """Join final demand and diagnosis outputs columnwise."""
    logger.info("=" * 80)
    logger.info("JOINING DEMAND AND DIAGNOSIS DATA COLUMNWISE")
    logger.info("=" * 80)

    try:
        demand_config = get_config("demand")
        diagnosis_config = get_config("diagnosis")

        demand_path = demand_file or (
            demand_config.PIPELINE_DATA_DIR / "finals" / "demand_final.parquet"
        )
        diagnosis_path = diagnosis_file or (
            diagnosis_config.PIPELINE_DATA_DIR / "finals" / "diagnosis_final.parquet"
        )
        output_path = output_file or (
            Path(demand_config.PIPELINE_DATA_DIR.parent)
            / "finals"
            / "demand_diagnosis_joined.parquet"
        )

        output_path.parent.mkdir(parents=True, exist_ok=True)

        joiner = FinalDataJoiner(
            demand_final_file=demand_path,
            diagnosis_final_file=diagnosis_path,
            output_file=output_path,
        )
        joiner.join_and_save(
            demand_prefix="DEMAND",
            diagnosis_prefix="DIAGNOSIS",
            fill_method="ffill",
            compression="snappy",
        )

        logger.info(f"Final join completed: {output_path}")
        return True
    except Exception as e:
        logger.error(f"Final join failed: {e}", exc_info=True)
        return False


def run_demand_sample_pipeline(
    input_dir: Path,
    output_dir: Path,
    start_date: Optional[pd.Timestamp] = None,
    end_date: Optional[pd.Timestamp] = None,
) -> bool:
    """Run demand pipeline with bundled synthetic sample data."""
    logger.info("=" * 80)
    logger.info("STARTING SAMPLE DEMAND PIPELINE")
    logger.info("=" * 80)

    try:
        from pipelines.sample_runner import run_sample_demand_pipeline

        output_path = run_sample_demand_pipeline(
            input_dir,
            output_dir,
            start_date=start_date,
            end_date=end_date,
        )
        logger.info(f"Sample demand pipeline completed: {output_path}")
        return True
    except Exception as e:
        logger.error(f"Sample demand pipeline failed: {e}", exc_info=True)
        return False


def run_diagnosis_sample_pipeline(
    input_dir: Path,
    output_dir: Path,
    start_date: Optional[pd.Timestamp] = None,
    end_date: Optional[pd.Timestamp] = None,
) -> bool:
    """Run diagnosis pipeline with bundled synthetic sample data."""
    logger.info("=" * 80)
    logger.info("STARTING SAMPLE DIAGNOSIS PIPELINE")
    logger.info("=" * 80)

    try:
        from pipelines.sample_runner import run_sample_diagnosis_pipeline

        output_path = run_sample_diagnosis_pipeline(
            input_dir,
            output_dir,
            start_date=start_date,
            end_date=end_date,
        )
        logger.info(f"Sample diagnosis pipeline completed: {output_path}")
        return True
    except Exception as e:
        logger.error(f"Sample diagnosis pipeline failed: {e}", exc_info=True)
        return False


def join_sample_final_outputs(output_dir: Path) -> bool:
    """Join sample demand and diagnosis outputs."""
    logger.info("=" * 80)
    logger.info("JOINING SAMPLE DEMAND AND DIAGNOSIS DATA")
    logger.info("=" * 80)

    try:
        from pipelines.sample_runner import join_sample_outputs

        output_path = join_sample_outputs(output_dir)
        logger.info(f"Sample final join completed: {output_path}")
        return True
    except Exception as e:
        logger.error(f"Sample final join failed: {e}", exc_info=True)
        return False


def main() -> int:
    """Main entry point for optimized pipeline runner."""
    parser = argparse.ArgumentParser(
        description="Run PREDAP data processing pipelines (Optimized with Parquet)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_pipeline.py --demand                        Run demand pipeline only
  python run_pipeline.py --diagnosis                     Run diagnosis pipeline only
  python run_pipeline.py --both                          Run both pipelines
  python run_pipeline.py --all                           Run both + final join
  python run_pipeline.py --all --start-date 2024-01-01 --end-date 2024-12-31
  python run_pipeline.py --sample --all                  Run sample data + final join
  python run_pipeline.py --join-final                    Join final outputs only
  python run_pipeline.py --convert-parquet data/finals/x.parquet --to csv
  python run_pipeline.py --show-parquet data/finals/x.parquet --start-date 2026-05-20 --end-date 2026-05-28
  python run_pipeline.py --check-imputation data/demand_pipeline/finals/demand_final.parquet
  python run_pipeline.py --write-imputation-metadata data/demand_pipeline/finals/demand_final.parquet
  python run_pipeline.py --delete-parquet-rows data/finals/x.parquet --start-date 2026-05-26 --end-date 2026-05-28
  python run_pipeline.py --help                          Show this help

Features:
  - Parquet format for efficient storage (snappy compression)
  - Incremental and final Parquet outputs
  - Optional explicit date ranges with --start-date and --end-date
  - Parquet conversion to CSV or Excel with --convert-parquet
  - Parquet row inspection, imputation checks, and date-range row deletion
  - Future-dated rows are excluded from incremental and final outputs
  - Timestamp columns for tracking
  - Columnwise joining of demand and diagnosis
  - Synthetic sample mode without database access
  - Optimized data types and memory usage
        """,
    )

    parser.add_argument(
        "--demand",
        action="store_true",
        help="Run demand pipeline only",
    )
    parser.add_argument(
        "--diagnosis",
        action="store_true",
        help="Run diagnosis pipeline only",
    )
    parser.add_argument(
        "--both",
        action="store_true",
        help="Run both pipelines (default)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run both pipelines + join final outputs",
    )
    parser.add_argument(
        "--join-final",
        action="store_true",
        help="Join final demand and diagnosis outputs columnwise",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Use local synthetic CSV data instead of querying the database",
    )
    parser.add_argument(
        "--sample-input-dir",
        type=Path,
        default=PROJECT_ROOT / "data" / "sample" / "input",
        help="Directory with synthetic sample CSV inputs",
    )
    parser.add_argument(
        "--sample-output-dir",
        type=Path,
        default=PROJECT_ROOT / "data" / "sample" / "output",
        help="Directory where sample Parquet outputs will be written",
    )
    parser.add_argument(
        "--start-date",
        help=(
            "Optional inclusive start date (YYYY-MM-DD). If omitted, the "
            "pipeline resumes from the last processed day, or from 2008-01-01 "
            "on a first run."
        ),
    )
    parser.add_argument(
        "--end-date",
        help="Optional inclusive end date (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--convert-parquet",
        type=Path,
        help="Convert a final or incremental Parquet file to CSV or Excel",
    )
    parser.add_argument(
        "--show-parquet",
        type=Path,
        help="Print rows from a Parquet file, optionally filtered by --start-date/--end-date",
    )
    parser.add_argument(
        "--delete-parquet-rows",
        type=Path,
        help=(
            "Delete rows from a Parquet file for the inclusive "
            "--start-date/--end-date range"
        ),
    )
    parser.add_argument(
        "--check-imputation",
        type=Path,
        help="Validate and summarize imputation metadata in a Parquet final file",
    )
    parser.add_argument(
        "--write-imputation-metadata",
        type=Path,
        help="Write JSON/CSV imputation metadata sidecars for a Parquet file",
    )
    parser.add_argument(
        "--parquet-date-column",
        default="timestamp",
        help="Date/timestamp column used by Parquet row commands",
    )
    parser.add_argument(
        "--parquet-columns",
        help="Optional comma-separated columns to print with --show-parquet",
    )
    parser.add_argument(
        "--parquet-limit",
        type=int,
        default=100,
        help="Maximum rows printed by --show-parquet. Use 0 to print all matched rows",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview --delete-parquet-rows without writing changes",
    )
    parser.add_argument(
        "--to",
        choices=["csv", "excel", "xlsx"],
        default="csv",
        help="Output format for --convert-parquet",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional output path for --convert-parquet",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    parquet_ops = [
        args.convert_parquet,
        args.show_parquet,
        args.delete_parquet_rows,
        args.check_imputation,
        args.write_imputation_metadata,
    ]
    if sum(1 for value in parquet_ops if value) > 1:
        logger.error(
            "Use only one Parquet utility command at a time: "
            "--convert-parquet, --show-parquet, --delete-parquet-rows, "
            "--check-imputation, or --write-imputation-metadata"
        )
        return 1

    try:
        start_date = _parse_cli_date(args.start_date, "--start-date")
        end_date = _parse_cli_date(args.end_date, "--end-date")
        if start_date is not None and end_date is not None and start_date > end_date:
            raise ValueError("--start-date cannot be after --end-date")
    except ValueError as e:
        logger.error(str(e))
        return 1

    if args.convert_parquet:
        try:
            output_path = convert_parquet_file(
                input_file=args.convert_parquet,
                output_format=args.to,
                output_file=args.output,
            )
            logger.info(f"Conversion complete: {output_path}")
            return 0
        except Exception as e:
            logger.error(f"Parquet conversion failed: {e}", exc_info=True)
            return 1

    if args.show_parquet:
        try:
            print_parquet_rows(
                input_file=args.show_parquet,
                start_date=start_date,
                end_date=end_date,
                date_column=args.parquet_date_column,
                columns=_parse_column_list(args.parquet_columns),
                limit=args.parquet_limit,
            )
            return 0
        except Exception as e:
            logger.error(f"Parquet row display failed: {e}", exc_info=True)
            return 1

    if args.check_imputation:
        try:
            ok = check_parquet_imputation(
                input_file=args.check_imputation,
                date_column=args.parquet_date_column,
            )
            return 0 if ok else 1
        except Exception as e:
            logger.error(f"Imputation check failed: {e}", exc_info=True)
            return 1

    if args.write_imputation_metadata:
        try:
            write_parquet_imputation_metadata(
                input_file=args.write_imputation_metadata,
                date_column=args.parquet_date_column,
            )
            return 0
        except Exception as e:
            logger.error(f"Writing imputation metadata failed: {e}", exc_info=True)
            return 1

    if args.delete_parquet_rows:
        try:
            deleted_rows, remaining_rows, backup_path = delete_parquet_rows(
                input_file=args.delete_parquet_rows,
                start_date=start_date,
                end_date=end_date,
                date_column=args.parquet_date_column,
                dry_run=args.dry_run,
            )
            action = "Would delete" if args.dry_run else "Deleted"
            logger.info(
                f"{action} {deleted_rows} rows from {args.delete_parquet_rows}; "
                f"{remaining_rows} rows remain"
            )
            if backup_path is not None:
                logger.info(f"Backup written before deletion: {backup_path}")
            if not args.dry_run and deleted_rows > 0:
                logger.info(
                    "Processing metadata was synced when a pipeline metadata file "
                    "could be inferred from the Parquet path."
                )
            return 0
        except Exception as e:
            logger.error(f"Parquet row deletion failed: {e}", exc_info=True)
            return 1

    run_demand = False
    run_diagnosis = False
    run_join = False

    if args.demand:
        run_demand = True
    elif args.diagnosis:
        run_diagnosis = True
    elif args.join_final:
        run_join = True
    elif args.all:
        run_demand = True
        run_diagnosis = True
        run_join = True
    else:
        run_demand = True
        run_diagnosis = True

    if args.both:
        run_demand = True
        run_diagnosis = True

    logger.info("=" * 80)
    if args.sample:
        logger.info("SAMPLE PREDAP PIPELINE EXECUTION")
        logger.info(f"Sample input dir: {args.sample_input_dir}")
        logger.info(f"Sample output dir: {args.sample_output_dir}")
    else:
        logger.info("OPTIMIZED PREDAP PIPELINE EXECUTION")
    if start_date is not None or end_date is not None:
        logger.info(
            "Requested date range: "
            f"{start_date.date() if start_date is not None else 'auto'} -> "
            f"{end_date.date() if end_date is not None else 'today'}"
        )
    logger.info("=" * 80)

    results = []

    if run_demand:
        if args.sample:
            success = run_demand_sample_pipeline(
                args.sample_input_dir,
                args.sample_output_dir,
                start_date=start_date,
                end_date=end_date,
            )
            results.append(("Sample Demand Pipeline", success))
        else:
            success = run_demand_pipeline_optimized(
                start_date=start_date,
                end_date=end_date,
            )
            results.append(("Demand Pipeline", success))

    if run_diagnosis:
        if args.sample:
            success = run_diagnosis_sample_pipeline(
                args.sample_input_dir,
                args.sample_output_dir,
                start_date=start_date,
                end_date=end_date,
            )
            results.append(("Sample Diagnosis Pipeline", success))
        else:
            success = run_diagnosis_pipeline_optimized(
                start_date=start_date,
                end_date=end_date,
            )
            results.append(("Diagnosis Pipeline", success))

    if run_join:
        if args.sample:
            success = join_sample_final_outputs(args.sample_output_dir)
            results.append(("Sample Final Join", success))
        else:
            success = join_final_outputs()
            results.append(("Final Join", success))

    logger.info("=" * 80)
    logger.info("EXECUTION SUMMARY:")
    for name, success in results:
        status = "SUCCESS" if success else "FAILED"
        logger.info(f"  {name}: {status}")
    logger.info("=" * 80)

    all_success = all(success for _, success in results)
    return 0 if all_success else 1


if __name__ == "__main__":
    sys.exit(main())
