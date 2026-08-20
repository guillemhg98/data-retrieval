"""Efficient Parquet-based storage and incremental management."""
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, Tuple
import logging
from uuid import uuid4

from .imputation import (
    IMPUTED_COL,
    is_imputed_series,
    write_imputation_metadata_files,
)

logger = logging.getLogger(__name__)


def drop_future_timestamp_rows(
    df: pd.DataFrame,
    timestamp_col: str = "timestamp",
) -> pd.DataFrame:
    """Drop rows dated after today, using tomorrow as the exclusive cutoff."""
    if df.empty or timestamp_col not in df.columns:
        return df

    out = df.copy()
    out[timestamp_col] = pd.to_datetime(out[timestamp_col], errors="coerce")
    tomorrow = pd.Timestamp.today().normalize() + pd.Timedelta(days=1)
    before = len(out)
    out = out[out[timestamp_col].notna() & (out[timestamp_col] < tomorrow)]
    dropped = before - len(out)
    if dropped:
        logger.warning(
            f"Dropped {dropped} rows beyond today's date from parquet output"
        )
    return out


class ParquetIncrementalManager:
    """Efficiently manage partial incremental data in Parquet format."""

    def __init__(
        self,
        output_dir: str | Path,
        retention_days: Optional[int] = None,
        chunk_size: int = 50000,
    ):
        """
        Initialize Parquet incremental manager.

        Args:
            output_dir: Directory for parquet files
            retention_days: Days of incremental data to keep. Use None to keep all
                incremental files until the final aggregation has been built.
            chunk_size: Rows per file for optimal memory usage
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.retention_days = retention_days
        self.chunk_size = chunk_size
        self.metadata_file = self.output_dir / "metadata.parquet"

    def add_data(
        self,
        df: pd.DataFrame,
        timestamp_col: str = "timestamp",
        **kwargs
    ) -> None:
        """
        Efficiently add data to incremental storage with automatic retention.

        Args:
            df: DataFrame with timestamp column
            timestamp_col: Name of timestamp column
            **kwargs: Additional metadata to store
        """
        if df.empty:
            logger.warning("Empty DataFrame - skipping")
            return

        # Ensure timestamp column exists
        if timestamp_col not in df.columns:
            df[timestamp_col] = pd.Timestamp.now()

        df[timestamp_col] = pd.to_datetime(df[timestamp_col])
        df = drop_future_timestamp_rows(df, timestamp_col)
        if df.empty:
            logger.warning("No rows on or before today - skipping")
            return

        # Optimize data types for storage
        df = self._optimize_dtypes(df)

        # Split into chunks and save
        for i, start in enumerate(range(0, len(df), self.chunk_size)):
            chunk = df.iloc[start:start + self.chunk_size].copy()
            chunk_path = (
                self.output_dir
                / (
                    f"incremental_{pd.Timestamp.now():%Y%m%d_%H%M%S_%f}_"
                    f"{uuid4().hex[:8]}_{i:03d}.parquet"
                )
            )


            
            chunk.to_parquet(chunk_path, compression="snappy", index=False)
            logger.info(f"Saved chunk: {chunk_path.name} ({len(chunk)} rows)")

        # Clean up old files
        self._cleanup_retention(timestamp_col)

        # Update metadata
        self._update_metadata(df, timestamp_col)

    def load_all_incremental(self, timestamp_col: str = "timestamp") -> pd.DataFrame:
        """Load and concatenate all current incremental files efficiently."""
        parquet_files = self.iter_incremental_files()

        if not parquet_files:
            logger.warning("No incremental files found")
            return pd.DataFrame()

        # Read all files with optimized dtypes
        dfs = []
        for pf in parquet_files:
            try:
                df = pd.read_parquet(pf)
                dfs.append(df)
            except Exception as e:
                logger.error(f"Error reading {pf}: {e}")

        if not dfs:
            return pd.DataFrame()

        result = pd.concat(dfs, ignore_index=True)

        # Remove duplicates and sort
        if "data_id" in result.columns:
            result = result.drop_duplicates(subset=["data_id"], keep="last")
        result = result.sort_values(timestamp_col)

        return result

    def iter_incremental_files(self) -> list[Path]:
        """Return current incremental parquet files in deterministic order."""
        return sorted(self.output_dir.glob("incremental_*.parquet"))

    def clear_incremental_files(self) -> int:
        """Remove processed incremental parquet chunks, keeping metadata."""
        removed = 0
        for pf in self.iter_incremental_files():
            try:
                pf.unlink()
                removed += 1
            except Exception as e:
                logger.warning(f"Could not remove incremental file {pf}: {e}")

        if removed:
            logger.info(f"Removed {removed} processed incremental parquet files")

        return removed

    def _cleanup_retention(self, timestamp_col: str) -> None:
        """Remove incremental files older than retention period."""
        if self.retention_days is None or self.retention_days <= 0:
            return

        cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=self.retention_days)
        parquet_files = list(self.output_dir.glob("incremental_*.parquet"))

        for pf in parquet_files:
            try:
                df = pd.read_parquet(pf, columns=[timestamp_col])
                if df[timestamp_col].max() < cutoff_date:
                    pf.unlink()
                    logger.info(f"Removed old incremental file: {pf.name}")
            except Exception as e:
                logger.warning(f"Could not check {pf}: {e}")

    def _update_metadata(self, df: pd.DataFrame, timestamp_col: str) -> None:
        """Update metadata about current incremental state."""
        metadata = {
            "last_update": pd.Timestamp.now(),
            "min_timestamp": df[timestamp_col].min(),
            "max_timestamp": df[timestamp_col].max(),
            "num_rows": len(df),
        }

        metadata_df = pd.DataFrame([metadata])
        metadata_df.to_parquet(self.metadata_file, index=False)

    def _optimize_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimize data types for efficient storage."""
        for col in df.columns:
            if df[col].dtype == "object":
                # Try to convert to category for repeated strings
                if df[col].nunique() < len(df) * 0.05:  # If <5% unique
                    df[col] = df[col].astype("category")
            elif df[col].dtype in ["int64", "int32"]:
                # Downcast integers
                if df[col].min() >= 0:
                    if df[col].max() < 256:
                        df[col] = df[col].astype("uint8")
                    elif df[col].max() < 65536:
                        df[col] = df[col].astype("uint16")
                    elif df[col].max() < 4294967296:
                        df[col] = df[col].astype("uint32")
                else:
                    if df[col].min() > -128 and df[col].max() < 127:
                        df[col] = df[col].astype("int8")
                    elif df[col].min() > -32768 and df[col].max() < 32767:
                        df[col] = df[col].astype("int16")

            elif df[col].dtype == "float64":
                # Downcast floats
                if df[col].min() > np.finfo(np.float32).min and df[col].max() < np.finfo(np.float32).max:
                    df[col] = df[col].astype("float32")

        return df

    def get_last_timestamp(self, timestamp_col: str = "timestamp") -> Optional[pd.Timestamp]:
        """Get the latest processed timestamp from incremental metadata."""
        try:
            df = pd.read_parquet(self.metadata_file)
            if not df.empty:
                return pd.to_datetime(df["max_timestamp"].iloc[-1], errors="coerce")
        except Exception:
            pass
        return None


class ParquetFinalStore:
    """Efficiently manage final output as single columnwise Parquet file."""

    def __init__(self, output_file: str | Path):
        """Initialize final store."""
        self.output_file = Path(output_file)
        self.output_file.parent.mkdir(parents=True, exist_ok=True)

    def save_final(
        self,
        df: pd.DataFrame,
        index_col: str = "timestamp",
        compression: str = "snappy",
    ) -> None:
        """
        Save final aggregated data as optimized columnwise Parquet.

        Args:
            df: DataFrame with timestamp index
            index_col: Name for timestamp index column
            compression: Compression algorithm (snappy, gzip, etc.)
        """
        df = df.copy()

        # Reset index only when the timestamp is actually stored in the index.
        if index_col in df.columns:
            df = df.reset_index(drop=True)
        elif isinstance(df.index, pd.DatetimeIndex):
            df.index.name = index_col
            df = df.reset_index()
        elif df.index.name is not None:
            df = df.reset_index()

        df = drop_future_timestamp_rows(df, index_col)
        if df.empty:
            logger.warning("No final rows on or before today - skipping save")
            return

        # Optimize dtypes before saving
        df = self._optimize_dtypes(df)

        # Save with efficient column-wise storage
        df.to_parquet(
            self.output_file,
            compression=compression,
            index=False,
            # Use row_group_size for better memory efficiency
            row_group_size=100000,
        )

        logger.info(
            f"Saved final output: {self.output_file.name} "
            f"({len(df)} rows, {len(df.columns)} columns)"
        )
        summary_path, rows_path = write_imputation_metadata_files(
            df,
            self.output_file,
            timestamp_col=index_col,
        )
        logger.info(
            f"Saved imputation metadata: {summary_path.name}, {rows_path.name}"
        )

    def load_final(self) -> pd.DataFrame:
        """Load final data efficiently."""
        if not self.output_file.exists():
            logger.warning(f"Final file not found: {self.output_file}")
            return pd.DataFrame()

        try:
            df = pd.read_parquet(self.output_file)
            logger.info(f"Loaded final file: {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Error loading final file: {e}")
            return pd.DataFrame()

    def get_last_timestamp(self, timestamp_col: str = "timestamp") -> Optional[pd.Timestamp]:
        """Get the latest non-imputed timestamp stored in the final parquet file."""
        if not self.output_file.exists():
            return None

        try:
            df = pd.read_parquet(self.output_file, columns=[timestamp_col, IMPUTED_COL])
        except Exception:
            try:
                df = pd.read_parquet(self.output_file, columns=[timestamp_col])
            except Exception as e:
                logger.warning(f"Could not read final timestamp from {self.output_file}: {e}")
                return None

        if df.empty or timestamp_col not in df.columns:
            return None

        if IMPUTED_COL in df.columns:
            df = df[~is_imputed_series(df[IMPUTED_COL])]

        timestamps = pd.to_datetime(df[timestamp_col], errors="coerce").dropna()
        if timestamps.empty:
            return None

        return timestamps.max()

    def get_last_contiguous_timestamp(
        self,
        timestamp_col: str = "timestamp",
    ) -> Optional[pd.Timestamp]:
        """Get the last non-imputed day before the first missing daily row."""
        if not self.output_file.exists():
            return None

        try:
            df = pd.read_parquet(self.output_file, columns=[timestamp_col, IMPUTED_COL])
        except Exception:
            try:
                df = pd.read_parquet(self.output_file, columns=[timestamp_col])
            except Exception as e:
                logger.warning(f"Could not read final timestamp from {self.output_file}: {e}")
                return None

        if df.empty or timestamp_col not in df.columns:
            return None

        if IMPUTED_COL in df.columns:
            df = df[~is_imputed_series(df[IMPUTED_COL])]

        timestamps = (
            pd.to_datetime(df[timestamp_col], errors="coerce")
            .dropna()
            .dt.floor("D")
            .drop_duplicates()
            .sort_values()
        )
        if timestamps.empty:
            return None

        min_day = timestamps.iloc[0]
        max_day = timestamps.iloc[-1]
        expected = pd.date_range(min_day, max_day, freq="D")
        observed = pd.DatetimeIndex(timestamps)
        missing = expected.difference(observed)
        if missing.empty:
            return max_day

        first_missing = missing[0]
        return first_missing - pd.Timedelta(days=1)

    def _optimize_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimize data types for efficient storage."""
        for col in df.columns:
            if col.lower() in ["timestamp", "date", "data_visita"]:
                df[col] = pd.to_datetime(df[col])
            elif df[col].dtype == "object":
                if df[col].nunique() < len(df) * 0.05:
                    df[col] = df[col].astype("category")
                else:
                    df[col] = df[col].astype("string")
            elif df[col].dtype == "float64":
                # Check if can be float32
                if (
                    df[col].min() > np.finfo(np.float32).min
                    and df[col].max() < np.finfo(np.float32).max
                ):
                    df[col] = df[col].astype("float32")

        return df


def load_and_merge_final_outputs(
    demand_files: list[str | Path],
    diagnosis_files: list[str | Path],
    timestamp_col: str = "timestamp",
    on_keys: Optional[list[str]] = None,
) -> pd.DataFrame:
    """
    Efficiently load demand and diagnosis data and merge columnwise.

    Args:
        demand_files: Paths to demand parquet files
        diagnosis_files: Paths to diagnosis parquet files
        timestamp_col: Timestamp column name
        on_keys: Keys to merge on (default: timestamp)

    Returns:
        Merged DataFrame with demand and diagnosis columns
    """
    if on_keys is None:
        on_keys = [timestamp_col]

    # Load demand data
    demand_dfs = []
    for f in demand_files:
        try:
            df = pd.read_parquet(f)
            demand_dfs.append(df)
            logger.info(f"Loaded demand: {Path(f).name}")
        except Exception as e:
            logger.error(f"Error loading {f}: {e}")

    # Load diagnosis data
    diagnosis_dfs = []
    for f in diagnosis_files:
        try:
            df = pd.read_parquet(f)
            diagnosis_dfs.append(df)
            logger.info(f"Loaded diagnosis: {Path(f).name}")
        except Exception as e:
            logger.error(f"Error loading {f}: {e}")

    if not demand_dfs or not diagnosis_dfs:
        logger.error("Could not load demand or diagnosis data")
        return pd.DataFrame()

    # Concatenate and deduplicate
    demand_df = pd.concat(demand_dfs, ignore_index=True).drop_duplicates(
        subset=on_keys, keep="last"
    )
    diagnosis_df = pd.concat(diagnosis_dfs, ignore_index=True).drop_duplicates(
        subset=on_keys, keep="last"
    )

    # Rename diagnosis columns to avoid conflicts
    diagnosis_df.columns = [
        f"DIAG_{col}" if col not in on_keys else col for col in diagnosis_df.columns
    ]

    # Merge on timestamp columnwise
    merged = demand_df.merge(
        diagnosis_df,
        on=on_keys,
        how="outer",
        suffixes=("_demand", "_diagnosis"),
    )

    # Sort by timestamp
    if timestamp_col in merged.columns:
        merged = merged.sort_values(timestamp_col)
        merged = drop_future_timestamp_rows(merged, timestamp_col)

    logger.info(f"Merged final output: {len(merged)} rows, {len(merged.columns)} columns")

    return merged
