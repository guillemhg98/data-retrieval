"""Efficient final joiner for demand and diagnosis data with columnwise Parquet output."""
import pandas as pd
import logging
from pathlib import Path
from typing import Optional, Tuple
import warnings

from .parquet_storage import drop_future_timestamp_rows
from .imputation import write_imputation_metadata_files
from .naming import DOMAIN_DEMAND, DOMAIN_DIAGNOSIS, canonicalize_feature_name

warnings.filterwarnings("ignore")

logger = logging.getLogger(__name__)


class FinalDataJoiner:
    """Efficiently join demand and diagnosis data columnwise."""

    def __init__(
        self,
        demand_final_file: str | Path,
        diagnosis_final_file: str | Path,
        output_file: str | Path,
        timestamp_col: str = "timestamp",
    ):
        """
        Initialize final data joiner.

        Args:
            demand_final_file: Path to demand parquet file
            diagnosis_final_file: Path to diagnosis parquet file
            output_file: Output path for joined parquet file
            timestamp_col: Timestamp column name
        """
        self.demand_file = Path(demand_final_file)
        self.diagnosis_file = Path(diagnosis_final_file)
        self.output_file = Path(output_file)
        self.timestamp_col = timestamp_col
        self.output_file.parent.mkdir(parents=True, exist_ok=True)

    def join_columnwise(
        self,
        demand_prefix: str = "DEMAND",
        diagnosis_prefix: str = "DIAGNOSIS",
        fill_method: str = "ffill",
    ) -> pd.DataFrame:
        """
        Efficiently join demand and diagnosis data columnwise.

        Args:
            demand_prefix: Prefix for demand columns
            diagnosis_prefix: Prefix for diagnosis columns
            fill_method: Method to fill missing values ('ffill', 'bfill', 'interpolate', None)

        Returns:
            Joined DataFrame
        """
        logger.info("Loading demand data...")
        demand_df = self._load_parquet_efficient(self.demand_file)

        logger.info("Loading diagnosis data...")
        diagnosis_df = self._load_parquet_efficient(self.diagnosis_file)

        if demand_df.empty or diagnosis_df.empty:
            logger.error("Could not load demand or diagnosis data")
            return pd.DataFrame()

        logger.info(f"Demand: {len(demand_df)} rows, {len(demand_df.columns)} columns")
        logger.info(
            f"Diagnosis: {len(diagnosis_df)} rows, {len(diagnosis_df.columns)} columns"
        )

        # Ensure timestamp columns
        if self.timestamp_col in demand_df.columns:
            demand_df[self.timestamp_col] = pd.to_datetime(
                demand_df[self.timestamp_col]
            )
        else:
            demand_df = demand_df.reset_index()
            demand_df[self.timestamp_col] = pd.to_datetime(
                demand_df[self.timestamp_col]
            )

        if self.timestamp_col in diagnosis_df.columns:
            diagnosis_df[self.timestamp_col] = pd.to_datetime(
                diagnosis_df[self.timestamp_col]
            )
        else:
            diagnosis_df = diagnosis_df.reset_index()
            diagnosis_df[self.timestamp_col] = pd.to_datetime(
                diagnosis_df[self.timestamp_col]
            )

        # Rename legacy columns to the canonical Qualud grammar, while leaving
        # already-canonical names untouched.
        demand_cols = self._build_join_rename_map(
            demand_df,
            domain=DOMAIN_DEMAND,
            fallback_prefix=demand_prefix,
        )
        diagnosis_cols = self._build_join_rename_map(
            diagnosis_df,
            domain=DOMAIN_DIAGNOSIS,
            fallback_prefix=diagnosis_prefix,
        )

        demand_df = demand_df.rename(columns=demand_cols)
        diagnosis_df = diagnosis_df.rename(columns=diagnosis_cols)

        # Merge on timestamp
        logger.info("Joining datasets columnwise...")
        merged = demand_df.merge(
            diagnosis_df,
            on=self.timestamp_col,
            how="outer",
            suffixes=("", "_diag"),
        )

        # Sort by timestamp
        merged = merged.sort_values(self.timestamp_col)

        # Handle missing values
        if fill_method == "ffill":
            logger.info("Filling missing values with forward fill...")
            merged = merged.ffill()
        elif fill_method == "bfill":
            logger.info("Filling missing values with backward fill...")
            merged = merged.bfill()
        elif fill_method == "interpolate":
            logger.info("Interpolating missing values...")
            numeric_cols = merged.select_dtypes(include=[float, int]).columns
            merged[numeric_cols] = merged[numeric_cols].interpolate(method="linear")

        # Fill any remaining nulls with 0 for numeric columns
        numeric_cols = merged.select_dtypes(include=[float, int]).columns
        merged[numeric_cols] = merged[numeric_cols].fillna(0)
        merged = drop_future_timestamp_rows(merged, self.timestamp_col)

        logger.info(
            f"Joined output: {len(merged)} rows, {len(merged.columns)} columns"
        )

        return merged

    def save_joined(
        self,
        df: pd.DataFrame,
        compression: str = "snappy",
    ) -> None:
        """
        Save joined data efficiently to Parquet.

        Args:
            df: DataFrame to save
            compression: Compression algorithm
        """
        logger.info(f"Saving to {self.output_file.name}...")

        df.to_parquet(
            self.output_file,
            compression=compression,
            index=False,
            row_group_size=100000,
        )

        file_size = self.output_file.stat().st_size / 1024 / 1024  # MB
        logger.info(
            f"Saved successfully: {len(df)} rows, {len(df.columns)} columns, "
            f"{file_size:.2f} MB"
        )
        summary_path, rows_path = write_imputation_metadata_files(
            df,
            self.output_file,
            timestamp_col=self.timestamp_col,
        )
        logger.info(
            f"Saved imputation metadata: {summary_path.name}, {rows_path.name}"
        )

    def join_and_save(
        self,
        demand_prefix: str = "DEMAND",
        diagnosis_prefix: str = "DIAGNOSIS",
        fill_method: str = "ffill",
        compression: str = "snappy",
    ) -> Path:
        """
        Efficiently join demand and diagnosis data and save to Parquet.

        Args:
            demand_prefix: Prefix for demand columns
            diagnosis_prefix: Prefix for diagnosis columns
            fill_method: Method to fill missing values
            compression: Compression algorithm

        Returns:
            Path to output file
        """
        logger.info("Starting columnwise join...")

        merged_df = self.join_columnwise(
            demand_prefix=demand_prefix,
            diagnosis_prefix=diagnosis_prefix,
            fill_method=fill_method,
        )

        if merged_df.empty:
            logger.error("Could not create joined DataFrame")
            return None

        self.save_joined(merged_df, compression=compression)

        logger.info(f"Join complete: {self.output_file}")

        return self.output_file

    def _load_parquet_efficient(self, file_path: Path) -> pd.DataFrame:
        """Load parquet file efficiently with memory optimization."""
        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            return pd.DataFrame()

        try:
            # Get parquet file info
            import pyarrow.parquet as pq

            parquet_file = pq.ParquetFile(file_path)
            logger.info(f"Schema: {len(parquet_file.schema)} columns")

            # Load with type inference
            df = pd.read_parquet(file_path)

            logger.info(f"Loaded: {len(df)} rows")

            return df

        except Exception as e:
            logger.error(f"Error loading {file_path}: {e}")
            return pd.DataFrame()

    def _build_join_rename_map(
        self,
        df: pd.DataFrame,
        domain: str,
        fallback_prefix: str,
    ) -> dict[str, str]:
        """Return output column names for a final join."""
        rename_map = {}
        for col in df.columns:
            if col == self.timestamp_col:
                continue

            canonical = canonicalize_feature_name(col, domain)
            if canonical != col or canonical.startswith(f"{domain}__"):
                rename_map[col] = canonical
            else:
                rename_map[col] = f"{fallback_prefix}_{col}"
        return rename_map


class IncrementalFinalJoiner:
    """Join incremental parquet files from both pipelines."""

    def __init__(
        self,
        demand_incremental_dir: str | Path,
        diagnosis_incremental_dir: str | Path,
        output_file: str | Path,
        timestamp_col: str = "timestamp",
    ):
        """
        Initialize incremental joiner.

        Args:
            demand_incremental_dir: Demand incremental directory
            diagnosis_incremental_dir: Diagnosis incremental directory
            output_file: Output file for joined incremental data
            timestamp_col: Timestamp column name
        """
        self.demand_dir = Path(demand_incremental_dir)
        self.diagnosis_dir = Path(diagnosis_incremental_dir)
        self.output_file = Path(output_file)
        self.timestamp_col = timestamp_col
        self.output_file.parent.mkdir(parents=True, exist_ok=True)

    def join_incremental_columnwise(
        self,
        demand_prefix: str = "DEMAND",
        diagnosis_prefix: str = "DIAGNOSIS",
    ) -> pd.DataFrame:
        """
        Join incremental data columnwise with memory efficiency.

        Args:
            demand_prefix: Prefix for demand columns
            diagnosis_prefix: Prefix for diagnosis columns

        Returns:
            Joined DataFrame
        """
        logger.info("Loading demand incremental files...")
        demand_dfs = self._load_parquet_files(self.demand_dir)

        logger.info("Loading diagnosis incremental files...")
        diagnosis_dfs = self._load_parquet_files(self.diagnosis_dir)

        if not demand_dfs or not diagnosis_dfs:
            logger.error("Could not load incremental data")
            return pd.DataFrame()

        # Concatenate and deduplicate
        demand_df = pd.concat(demand_dfs, ignore_index=True)
        demand_df = demand_df.drop_duplicates(
            subset=[self.timestamp_col], keep="last"
        )

        diagnosis_df = pd.concat(diagnosis_dfs, ignore_index=True)
        diagnosis_df = diagnosis_df.drop_duplicates(
            subset=[self.timestamp_col], keep="last"
        )

        logger.info(
            f"Demand: {len(demand_df)} rows, Diagnosis: {len(diagnosis_df)} rows"
        )

        # Ensure timestamp columns
        for df in [demand_df, diagnosis_df]:
            if self.timestamp_col in df.columns:
                df[self.timestamp_col] = pd.to_datetime(df[self.timestamp_col])
            else:
                df = df.reset_index()

        # Rename columns
        demand_cols = self._build_join_rename_map(
            demand_df,
            domain=DOMAIN_DEMAND,
            fallback_prefix=demand_prefix,
        )
        diagnosis_cols = self._build_join_rename_map(
            diagnosis_df,
            domain=DOMAIN_DIAGNOSIS,
            fallback_prefix=diagnosis_prefix,
        )

        demand_df = demand_df.rename(columns=demand_cols)
        diagnosis_df = diagnosis_df.rename(columns=diagnosis_cols)

        # Merge
        logger.info("Joining columnwise...")
        merged = demand_df.merge(
            diagnosis_df,
            on=self.timestamp_col,
            how="outer",
        )

        merged = merged.sort_values(self.timestamp_col)

        return merged

    def save_incremental_joined(
        self,
        df: pd.DataFrame,
        compression: str = "snappy",
    ) -> None:
        """Save joined incremental data."""
        logger.info(f"Saving to {self.output_file.name}...")

        df.to_parquet(
            self.output_file,
            compression=compression,
            index=False,
        )

        logger.info(f"Saved: {len(df)} rows, {len(df.columns)} columns")

    def _load_parquet_files(self, directory: Path) -> list[pd.DataFrame]:
        """Load all parquet files from directory."""
        parquet_files = sorted(directory.glob("*.parquet"))

        if not parquet_files:
            logger.warning(f"No parquet files found in {directory}")
            return []

        dfs = []
        for pf in parquet_files:
            try:
                df = pd.read_parquet(pf)
                dfs.append(df)
                logger.info(f"Loaded: {pf.name} ({len(df)} rows)")
            except Exception as e:
                logger.error(f"Error loading {pf}: {e}")

        return dfs

    def _build_join_rename_map(
        self,
        df: pd.DataFrame,
        domain: str,
        fallback_prefix: str,
    ) -> dict[str, str]:
        """Return output column names for an incremental join."""
        rename_map = {}
        for col in df.columns:
            if col == self.timestamp_col:
                continue

            canonical = canonicalize_feature_name(col, domain)
            if canonical != col or canonical.startswith(f"{domain}__"):
                rename_map[col] = canonical
            else:
                rename_map[col] = f"{fallback_prefix}_{col}"
        return rename_map
