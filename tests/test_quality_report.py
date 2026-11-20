import json

import pandas as pd
import pytest

from pipelines.shared.final_joiner import FinalDataJoiner
from pipelines.shared.imputation import add_observed_imputation_metadata
from pipelines.shared.quality_report import create_quality_report


def export(root):
    paths = []
    for domain in ("demand", "diagnosis"):
        path = root / f"{domain}_pipeline/finals/{domain}_final.parquet"
        path.parent.mkdir(parents=True)
        df = add_observed_imputation_metadata(pd.DataFrame({
            "timestamp": pd.date_range("2024-01-01", periods=3),
            domain.upper() + "__TOTAL": [100, 110, 105],
        }))
        df.to_parquet(path, index=False)
        path.with_name(path.stem + "_integrity.json").write_text(json.dumps({
            "checks": {"2024-01-03": {"checked_at": "2024-01-04T10:00", "local_total": 105}},
        }))
        paths.append(path)
    FinalDataJoiner(*paths, root / "finals/demand_diagnosis_joined.parquet").join_and_save()
    return paths


def test_clean_export_generates_pngs_and_consistent_report(tmp_path):
    source = tmp_path / "input"
    export(source)
    report = create_quality_report(source, tmp_path / "report", "2024-01-01", "2024-01-03")
    assert report["status"] == "OK"
    for domain in ("demand", "diagnosis"):
        png = tmp_path / f"report/{domain}_quality.png"
        assert png.read_bytes().startswith(b"\x89PNG\r\n\x1a\n")
    assert json.loads((tmp_path / "report/summary.json").read_text())["status"] == "OK"


def test_old_csv_is_an_error_even_when_parquet_finals_match(tmp_path):
    source = tmp_path / "input"
    export(source)
    csv = source / "finals/demand_diagnosis_joined.csv"
    pd.read_csv(csv).iloc[:2].to_csv(csv, index=False)
    report = create_quality_report(source, tmp_path / "report")
    assert report["status"] == "ERROR"
    assert any(i["check"] == "stale_csv" for i in report["issues"])


def test_same_dates_but_changed_csv_values_are_detected(tmp_path):
    source = tmp_path / "input"
    export(source)
    csv = source / "finals/demand_diagnosis_joined.csv"
    df = pd.read_csv(csv)
    df.loc[0, "DEMAND__TOTAL"] = 999
    df.to_csv(csv, index=False)
    report = create_quality_report(source, tmp_path / "report")
    assert any(i["check"] == "csv_demand_values" for i in report["issues"])


def test_duplicate_dates_and_missing_file_produce_errors_not_misleading_old_pngs(tmp_path):
    source = tmp_path / "input"
    paths = export(source)
    report_dir = tmp_path / "report"
    create_quality_report(source, report_dir)
    df = pd.read_parquet(paths[0])
    pd.concat([df, df.iloc[:1]], ignore_index=True).to_parquet(paths[0])
    paths[1].unlink()
    report = create_quality_report(source, report_dir)
    assert report["status"] == "ERROR"
    assert not (report_dir / "demand_quality.png").exists()
    assert not (report_dir / "diagnosis_quality.png").exists()
    assert any(i["check"] == "duplicate_dates" for i in report["issues"])


def test_missing_provenance_is_review_and_never_claimed_observed(tmp_path):
    source = tmp_path / "input"
    paths = export(source)
    df = pd.read_parquet(paths[0]).drop(columns=["__is_imputed"])
    df.to_parquet(paths[0])
    report = create_quality_report(source, tmp_path / "report")
    assert any(i["check"] == "unknown_provenance" for i in report["issues"])
    assert report["datasets"]["demand"]["last_marked_observed"] is None


def test_invalid_date_range_is_rejected_before_writing(tmp_path):
    with pytest.raises(ValueError):
        create_quality_report(tmp_path, tmp_path / "report", "2024-02-01", "2024-01-01")
    assert not (tmp_path / "report").exists()


def test_imputed_days_remain_distinct_and_are_listed(tmp_path):
    source = tmp_path / "input"
    paths = export(source)
    df = pd.read_parquet(paths[0])
    df.loc[2, "__is_imputed"] = True
    df.to_parquet(paths[0])
    report = create_quality_report(source, tmp_path / "report")
    assert report["datasets"]["demand"]["imputed_rows_all_history"] == 1
    rows = pd.read_csv(tmp_path / "report/flagged_days.csv")
    assert "imputed" in rows.loc[rows.timestamp == "2024-01-03", "reasons"].iloc[0]
