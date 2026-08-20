"""Compatibility entry point for the optimized Parquet pipelines.

The original runner referenced non-optimized modules that are no longer present
under ``pipelines/``. Keep this file as the stable command users can run, while
delegating the actual work to ``run_pipeline_optimized.py``.
"""
import sys

from run_pipeline_optimized import (
    check_parquet_imputation,
    convert_parquet_file,
    delete_parquet_rows,
    join_final_outputs,
    main as _optimized_main,
    print_parquet_rows,
    run_demand_pipeline_optimized,
    run_diagnosis_pipeline_optimized,
    write_parquet_imputation_metadata,
)


def run_demand_pipeline(config=None, start_date=None, end_date=None):
    """Run the demand pipeline through the optimized implementation."""
    return run_demand_pipeline_optimized(
        config,
        start_date=start_date,
        end_date=end_date,
    )


def run_diagnosis_pipeline(config=None, start_date=None, end_date=None):
    """Run the diagnosis pipeline through the optimized implementation."""
    return run_diagnosis_pipeline_optimized(
        config,
        start_date=start_date,
        end_date=end_date,
    )


def main():
    """Delegate CLI handling to the optimized runner."""
    return _optimized_main()


if __name__ == "__main__":
    sys.exit(main())
