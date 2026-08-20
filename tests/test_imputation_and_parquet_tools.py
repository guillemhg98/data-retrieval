from pathlib import Path
import json

import pandas as pd

from pipelines.shared.imputation import (
    IMPUTATION_METHOD_COL,
    IMPUTATION_SOURCE_LAST_DATE_COL,
    IMPUTED_COL,
    SAME_MONTH_DAY_METHOD,
    drop_imputed_rows,
    get_imputation_metadata_paths,
    impute_tail_to_date,
)
from run_pipeline_optimized import (
    check_parquet_imputation,
    delete_parquet_rows,
    print_parquet_rows,
    write_parquet_imputation_metadata,
)
from pipelines.shared.parquet_storage import ParquetFinalStore


def test_impute_tail_to_date_marks_estimated_rows_and_uses_same_month_day_mean():
    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2022-01-02",
                    "2023-01-02",
                    "2024-01-01",
                ]
            ),
            "value": [8, 12, 30],
        }
    )

    result = impute_tail_to_date(
        df,
        observed_until=pd.Timestamp("2024-01-01"),
        target_until=pd.Timestamp("2024-01-02"),
    )

    imputed_row = result[result["timestamp"] == pd.Timestamp("2024-01-02")].iloc[0]
    assert bool(imputed_row[IMPUTED_COL]) is True
    assert imputed_row[IMPUTATION_METHOD_COL] == SAME_MONTH_DAY_METHOD
    assert imputed_row[IMPUTATION_SOURCE_LAST_DATE_COL] == "2024-01-01"
    assert imputed_row["value"] == 10

    observed = result[result["timestamp"].isin(df["timestamp"])]
    assert observed[IMPUTED_COL].eq(False).all()


def test_impute_tail_to_date_fills_missing_calendar_days_inside_observed_range():
    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-01", "2024-01-03"]),
            "value": [10, 20],
        }
    )

    result = impute_tail_to_date(
        df,
        observed_until=pd.Timestamp("2024-01-03"),
        target_until=pd.Timestamp("2024-01-03"),
    )

    imputed_row = result[result["timestamp"] == pd.Timestamp("2024-01-02")].iloc[0]
    assert bool(imputed_row[IMPUTED_COL]) is True
    assert imputed_row["value"] == 15


def test_drop_imputed_rows_removes_estimates_and_metadata_columns():
    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-01"]),
            "value": [30],
        }
    )
    result = impute_tail_to_date(
        df,
        observed_until=pd.Timestamp("2024-01-01"),
        target_until=pd.Timestamp("2024-01-02"),
    )

    cleaned = drop_imputed_rows(result)

    assert cleaned["timestamp"].tolist() == [pd.Timestamp("2024-01-01")]
    assert IMPUTED_COL not in cleaned.columns


def test_delete_parquet_rows_removes_inclusive_date_range_and_writes_backup(tmp_path):
    parquet_path = tmp_path / "demand_pipeline" / "finals" / "demand_final.parquet"
    metadata_path = tmp_path / "demand_pipeline" / "incremental" / "metadata.parquet"
    parquet_path.parent.mkdir(parents=True)
    metadata_path.parent.mkdir(parents=True)
    pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
            "value": [1, 2, 3],
        }
    ).to_parquet(parquet_path, index=False)
    pd.DataFrame(
        [
            {
                "last_update": pd.Timestamp("2024-01-04"),
                "min_timestamp": pd.Timestamp("2024-01-01"),
                "max_timestamp": pd.Timestamp("2024-01-03"),
                "num_rows": 3,
            }
        ]
    ).to_parquet(metadata_path, index=False)

    deleted_rows, remaining_rows, backup_path = delete_parquet_rows(
        parquet_path,
        start_date=pd.Timestamp("2024-01-03"),
        end_date=pd.Timestamp("2024-01-03"),
    )

    remaining = pd.read_parquet(parquet_path)
    metadata = pd.read_parquet(metadata_path)
    assert deleted_rows == 1
    assert remaining_rows == 2
    assert backup_path is not None
    assert Path(backup_path).exists()
    assert remaining["timestamp"].tolist() == [
        pd.Timestamp("2024-01-01"),
        pd.Timestamp("2024-01-02"),
    ]
    assert metadata["max_timestamp"].iloc[0] == pd.Timestamp("2024-01-02")


def test_delete_parquet_rows_syncs_metadata_to_last_non_imputed_row(tmp_path):
    parquet_path = tmp_path / "diagnosis_pipeline" / "finals" / "diagnosis_final.parquet"
    metadata_path = tmp_path / "diagnosis_pipeline" / "incremental" / "metadata.parquet"
    parquet_path.parent.mkdir(parents=True)
    metadata_path.parent.mkdir(parents=True)

    source = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "value": [10, 20],
        }
    )
    imputed = impute_tail_to_date(
        source,
        observed_until=pd.Timestamp("2024-01-02"),
        target_until=pd.Timestamp("2024-01-03"),
    )
    imputed.to_parquet(parquet_path, index=False)
    pd.DataFrame(
        [
            {
                "last_update": pd.Timestamp("2024-01-04"),
                "min_timestamp": pd.Timestamp("2024-01-01"),
                "max_timestamp": pd.Timestamp("2024-01-03"),
                "num_rows": 3,
            }
        ]
    ).to_parquet(metadata_path, index=False)

    delete_parquet_rows(
        parquet_path,
        start_date=pd.Timestamp("2024-01-02"),
        end_date=pd.Timestamp("2024-01-02"),
    )

    metadata = pd.read_parquet(metadata_path)
    assert metadata["max_timestamp"].iloc[0] == pd.Timestamp("2024-01-01")


def test_delete_parquet_rows_creates_metadata_before_deleted_range(tmp_path):
    parquet_path = tmp_path / "demand_pipeline" / "finals" / "demand_final.parquet"
    metadata_path = tmp_path / "demand_pipeline" / "incremental" / "metadata.parquet"
    parquet_path.parent.mkdir(parents=True)

    pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2025-12-30",
                    "2025-12-31",
                    "2026-01-01",
                    "2026-05-29",
                ]
            ),
            "value": [1, 2, 3, 4],
        }
    ).to_parquet(parquet_path, index=False)

    delete_parquet_rows(
        parquet_path,
        start_date=pd.Timestamp("2026-01-01"),
        end_date=pd.Timestamp("2026-05-29"),
    )

    metadata = pd.read_parquet(metadata_path)
    assert metadata["max_timestamp"].iloc[0] == pd.Timestamp("2025-12-31")


def test_final_store_last_contiguous_timestamp_stops_before_deleted_gap(tmp_path):
    parquet_path = tmp_path / "demand_final.parquet"
    pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2025-12-30",
                    "2025-12-31",
                    "2026-05-27",
                ]
            ),
            "value": [1, 2, 3],
        }
    ).to_parquet(parquet_path, index=False)

    store = ParquetFinalStore(parquet_path)

    assert store.get_last_timestamp() == pd.Timestamp("2026-05-27")
    assert store.get_last_contiguous_timestamp() == pd.Timestamp("2025-12-31")


def test_print_parquet_rows_filters_by_date_range(tmp_path, capsys):
    parquet_path = tmp_path / "rows.parquet"
    pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
            "value": [1, 2, 3],
        }
    ).to_parquet(parquet_path, index=False)

    filtered = print_parquet_rows(
        parquet_path,
        start_date=pd.Timestamp("2024-01-02"),
        end_date=pd.Timestamp("2024-01-03"),
        columns=["value"],
        limit=0,
    )

    output = capsys.readouterr().out
    assert len(filtered) == 2
    assert "Rows matched from 2024-01-02 to 2024-01-03: 2 of 3" in output
    assert "2024-01-02" in output
    assert "2024-01-03" in output


def test_check_parquet_imputation_accepts_valid_metadata(tmp_path):
    parquet_path = tmp_path / "imputed.parquet"
    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2022-01-02",
                    "2023-01-02",
                    "2024-01-01",
                ]
            ),
            "value": [8, 12, 30],
        }
    )
    result = impute_tail_to_date(
        df,
        observed_until=pd.Timestamp("2024-01-01"),
        target_until=pd.Timestamp("2024-01-02"),
    )
    result.to_parquet(parquet_path, index=False)

    assert check_parquet_imputation(parquet_path)


def test_final_store_writes_imputation_metadata_sidecars(tmp_path):
    parquet_path = tmp_path / "demand_final.parquet"
    source = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-01", "2024-01-03"]),
            "value": [10, 20],
        }
    )
    result = impute_tail_to_date(
        source,
        observed_until=pd.Timestamp("2024-01-03"),
        target_until=pd.Timestamp("2024-01-03"),
    )

    store = ParquetFinalStore(parquet_path)
    store.save_final(result)

    summary_path, rows_path = get_imputation_metadata_paths(parquet_path)
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    rows = pd.read_csv(rows_path)

    assert summary_path.exists()
    assert rows_path.exists()
    assert summary["total_imputed_rows"] == 1
    assert summary["groups"][0]["imputed_dates"] == ["2024-01-02"]
    assert rows["timestamp"].tolist() == ["2024-01-02"]


def test_write_parquet_imputation_metadata_command_helper(tmp_path):
    parquet_path = tmp_path / "diagnosis_final.parquet"
    source = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-01", "2024-01-03"]),
            "value": [10, 20],
        }
    )
    result = impute_tail_to_date(
        source,
        observed_until=pd.Timestamp("2024-01-03"),
        target_until=pd.Timestamp("2024-01-03"),
    )
    result.to_parquet(parquet_path, index=False)

    summary_path, rows_path = write_parquet_imputation_metadata(parquet_path)
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    rows = pd.read_csv(rows_path)

    assert summary["total_imputed_rows"] == 1
    assert rows["timestamp"].tolist() == ["2024-01-02"]
