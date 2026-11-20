import json
from unittest.mock import Mock

import pandas as pd
import pytest

from pipelines.shared.integrity import IntegrityReview, suspect_days
from pipelines.shared.imputation import impute_tail_to_date, add_observed_imputation_metadata
from pipelines.shared.parquet_storage import ParquetFinalStore, ParquetIncrementalManager
from pipelines.shared.final_joiner import FinalDataJoiner
from pipelines.demand.aggregation_optimized import aggregate_final_optimized
from pipelines.diagnosis.aggregation_optimized import aggregate_diagnosis_final_optimized


def daily(start, end, domain="DEMAND", amount=None):
    dates = pd.date_range(start, end)
    return add_observed_imputation_metadata(pd.DataFrame({
        "timestamp": dates,
        f"{domain}__TOTAL": amount if amount is not None else [10 if d.day_of_week >= 5 else 1000 for d in dates],
    }))


def test_sustained_september_collapse_is_detected_without_flagging_normal_summer():
    df = daily("2025-01-01", "2026-09-26")
    df.loc[df.timestamp.between("2026-09-01", "2026-09-25"), "DEMAND__TOTAL"] = 1
    reasons = suspect_days(df, "DEMAND__TOTAL")
    assert pd.Timestamp("2026-09-01") in reasons
    assert pd.Timestamp("2026-09-25") in reasons
    assert not any(pd.Timestamp("2026-07-01") <= d <= pd.Timestamp("2026-08-31") for d in reasons)


def test_old_july_september_imputed_and_unflagged_flat_periods_are_detected():
    df = daily("2025-01-01", "2026-09-26")
    summer = df.timestamp.between("2026-07-01", "2026-09-26")
    df.loc[summer, "DEMAND__TOTAL"] = 700
    reasons = suspect_days(df, "DEMAND__TOTAL")
    assert all(d in reasons for d in pd.date_range("2026-07-01", "2026-09-26"))
    df.loc[summer, "__is_imputed"] = True
    assert all("imputed" in suspect_days(df, "DEMAND__TOTAL")[d] for d in [pd.Timestamp("2026-07-01"), pd.Timestamp("2026-09-26")])


def test_grouped_review_only_fetches_changed_days_and_obeys_cooldown(tmp_path, monkeypatch):
    store = ParquetFinalStore(tmp_path / "final.parquet")
    df = daily("2026-07-01", "2026-09-26")
    df.loc[df.timestamp == "2026-09-01", "DEMAND__TOTAL"] = 1
    store.save_final(df)
    source = df[["timestamp", "DEMAND__TOTAL"]].rename(columns={"DEMAND__TOTAL": "source_count"})
    source.loc[source.timestamp == "2026-09-01", "source_count"] = 1000
    source["source_up_count"] = 3
    calls = []
    def query(sql, conn, params):
        calls.append((sql, params))
        assert "COUNT_BIG(*)" in sql and "GROUP BY" in sql
        assert "SELECT *" not in sql
        return source
    monkeypatch.setattr(pd, "read_sql_query", query)
    review = IntegrityReview(store, "DEMAND__TOTAL")
    ranges = review.plan(None, "dbo", "visits", "DATA", "UP", df.timestamp.min(), df.timestamp.max(),
                         review_ranges=[("2026-07-01", "2026-09-26")], now="2026-11-20 09:00")
    assert len(calls) == 1
    assert ranges == [(pd.Timestamp("2026-09-01"), pd.Timestamp("2026-09-02"), pd.Timestamp("2026-09-01"))]
    review.commit()
    second = IntegrityReview(store, "DEMAND__TOTAL")
    assert second.plan(None, "dbo", "visits", "DATA", "UP", df.timestamp.min(), df.timestamp.max(), now="2026-11-20 10:00") == []
    assert len(calls) == 1
    assert second.state["checks"]["2026-09-01"]["status"] == "suspect"


def test_review_budget_defers_days_instead_of_loading_all_history(tmp_path, monkeypatch):
    store = ParquetFinalStore(tmp_path / "final.parquet")
    df = daily("2026-01-01", "2026-09-26")
    store.save_final(df)
    captured = []
    def query(sql, conn, params):
        captured.extend(params)
        return pd.DataFrame(columns=["timestamp", "source_count", "source_up_count"])
    monkeypatch.setattr(pd, "read_sql_query", query)
    review = IntegrityReview(store, "DEMAND__TOTAL", max_days=10)
    review.plan(None, "s", "t", "d", "up", df.timestamp.min(), df.timestamp.max(), review_ranges=[("2026-07-01", "2026-09-26")])
    assert review.state["reviewed_days"] == 10
    assert review.state["deferred_days"] == 78
    assert max(captured) <= pd.Timestamp("2026-07-11")


@pytest.mark.parametrize("domain,aggregate", [("DEMAND", aggregate_final_optimized), ("DIAGNOSIS", aggregate_diagnosis_final_optimized)])
def test_retried_snapshots_replace_not_sum_and_corrupt_files_are_preserved(tmp_path, domain, aggregate):
    store = ParquetFinalStore(tmp_path / "final.parquet")
    manager = ParquetIncrementalManager(tmp_path / "incremental")
    data = daily("2024-01-01", "2024-01-01", domain, [100]).drop(columns=[c for c in daily("2024-01-01", "2024-01-01").columns if c.startswith("__")])
    manager.add_data(data.copy())
    manager.add_data(data.copy())
    result = aggregate(manager, store)
    assert result[f"{domain}__TOTAL"].iloc[0] == 100
    manager.add_data(data.copy())
    corrupt = manager.output_dir / "incremental_corrupt.parquet"
    corrupt.write_bytes(b"broken")
    before = store.output_file.read_bytes()
    with pytest.raises(RuntimeError, match="unreadable chunk"):
        aggregate(manager, store)
    assert corrupt.exists()
    assert len(manager.iter_incremental_files()) == 2
    assert store.output_file.read_bytes() == before


@pytest.mark.parametrize("domain,aggregate", [("DEMAND", aggregate_final_optimized), ("DIAGNOSIS", aggregate_diagnosis_final_optimized)])
def test_partial_selection_and_new_days_cannot_zero_old_totals(tmp_path, domain, aggregate):
    store = ParquetFinalStore(tmp_path / "final.parquet")
    manager = ParquetIncrementalManager(tmp_path / "incremental")
    store.save_final(daily("2024-01-01", "2024-01-01", domain, [100]))
    manager.add_data(pd.DataFrame({"timestamp": [pd.Timestamp("2024-01-01")], f"{domain}__TOTAL__UP__NEW": [20]}))
    manager.add_data(pd.DataFrame({"timestamp": [pd.Timestamp("2024-01-02")], f"{domain}__TOTAL": [200]}))
    result = aggregate(manager, store, partial_column_update=True)
    assert result.loc[result.timestamp == "2024-01-01", f"{domain}__TOTAL"].iloc[0] == 100


def test_imputation_preserves_weekends_and_observations_after_bounded_end():
    source = daily("2024-01-01", "2024-02-25")
    out = impute_tail_to_date(source, pd.Timestamp("2024-02-25"), pd.Timestamp("2024-03-03"))
    assert out.loc[out.timestamp == "2024-03-02", "DEMAND__TOTAL"].iloc[0] == 10
    assert out.loc[out.timestamp == "2024-03-01", "DEMAND__TOTAL"].iloc[0] == 1000
    bounded = impute_tail_to_date(out, pd.Timestamp("2024-01-15"), pd.Timestamp("2024-01-15"))
    assert pd.Timestamp("2024-02-25") in set(bounded.timestamp)


def test_join_keeps_missing_values_and_publishes_matching_csv(tmp_path):
    demand = tmp_path / "demand.parquet"
    diagnosis = tmp_path / "diagnosis.parquet"
    daily("2024-01-01", "2024-01-01").to_parquet(demand)
    daily("2024-01-01", "2024-01-03", "DIAGNOSIS").to_parquet(diagnosis)
    joiner = FinalDataJoiner(demand, diagnosis, tmp_path / "joined.parquet")
    result = joiner.join_columnwise()
    assert result["DEMAND__TOTAL"].isna().sum() == 2
    assert result["DEMAND___is_imputed"].isna().sum() == 2
    joiner.join_and_save()
    assert len(pd.read_csv(tmp_path / "joined.csv")) == 3
    manifest = json.loads((tmp_path / "joined_manifest.json").read_text())
    assert manifest["rows"] == 3
    assert len(manifest["files"]) == 2


@pytest.mark.parametrize("domain,aggregate", [("DEMAND", aggregate_final_optimized), ("DIAGNOSIS", aggregate_diagnosis_final_optimized)])
def test_source_deleted_day_invalidates_cached_observation(tmp_path, domain, aggregate):
    store = ParquetFinalStore(tmp_path / "final.parquet")
    store.save_final(daily("2024-01-01", "2024-01-03", domain, [10, 20, 30]))
    manager = ParquetIncrementalManager(tmp_path / "incremental")
    result = aggregate(manager, store, replace_dates=[pd.Timestamp("2024-01-02")], impute_until=pd.Timestamp("2024-01-03"))
    row = result.loc[result.timestamp == "2024-01-02"].iloc[0]
    assert bool(row["__is_imputed"])
    assert row[f"{domain}__TOTAL"] != 20


@pytest.mark.parametrize("domain", ["demand", "diagnosis"])
def test_runner_repairs_an_old_partial_day_without_reextracting_summer(tmp_path, monkeypatch, domain):
    import importlib
    runner = importlib.import_module(f"pipelines.{domain}.incremental_optimized")
    prefix = domain.upper()
    store = ParquetFinalStore(tmp_path / "final.parquet")
    df = daily("2025-01-01", "2026-09-26", prefix)
    df.loc[df.timestamp == "2026-09-01", prefix + "__TOTAL"] = 1
    store.save_final(df)
    source = df[["timestamp", prefix + "__TOTAL"]].rename(columns={prefix + "__TOTAL": "source_count"})
    source.loc[source.timestamp == "2026-09-01", "source_count"] = 1000
    source["source_up_count"] = 1
    monkeypatch.setattr(pd, "read_sql_query", lambda *args, **kwargs: source)
    monkeypatch.setattr(runner, "get_connection", lambda *args, **kwargs: Mock())
    monkeypatch.setattr(runner, "get_min_max_date", lambda **kwargs: (df.timestamp.min(), df.timestamp.max()))
    monkeypatch.setattr(runner, "_load_selected_rs", lambda *args: None)
    monkeypatch.setattr(runner, "_load_selected_up", lambda *args: None)
    if domain == "diagnosis":
        monkeypatch.setattr(runner, "validate_table_columns", lambda **kwargs: None)
        monkeypatch.setattr(runner, "_load_selected_codes", lambda *args: {})
    extracted = []
    def process(**kwargs):
        extracted.append((kwargs["range_start"], kwargs["range_end"]))
        kwargs["incremental_mgr"].add_data(pd.DataFrame({
            "timestamp": [pd.Timestamp("2026-09-01")], prefix + "__TOTAL": [1000],
        }))
        return pd.Timestamp("2026-09-01")
    monkeypatch.setattr(runner, f"_process_{domain}_range_with_fallback", process)
    common = dict(db_server="unused", db_database="unused", schema="s", table_name="t", date_column="d",
                  up_rs=pd.DataFrame(), incremental_dir=tmp_path / "incremental", final_file=store.output_file,
                  end_date="2026-09-26", review_ranges=[("2026-07-01", "2026-09-26")])
    if domain == "demand":
        runner.run_incremental_pipeline_optimized(**common)
    else:
        runner.run_incremental_diagnosis_pipeline_optimized(**common, up_column="UP", diag_code_column="code")
    assert extracted == [(pd.Timestamp("2026-09-01"), pd.Timestamp("2026-09-02"))]
    actual = store.load_final()
    assert actual.loc[actual.timestamp == "2026-09-01", prefix + "__TOTAL"].iloc[0] == 1000
    assert actual.timestamp.max() == pd.Timestamp("2026-09-26")
    state = json.loads(review_path(store).read_text())
    assert state["changed_days"] == 1


def review_path(store):
    return store.output_file.with_name(store.output_file.stem + "_integrity.json")


def test_changed_configuration_rebuilds_equal_counts_in_bounded_batches(tmp_path, monkeypatch):
    store = ParquetFinalStore(tmp_path / "final.parquet")
    df = daily("2024-01-01", "2024-01-03")
    store.save_final(df)
    first = IntegrityReview(store, "DEMAND__TOTAL", input_signature="old")
    first.commit()
    source = df[["timestamp", "DEMAND__TOTAL"]].rename(columns={"DEMAND__TOTAL": "source_count"})
    source["source_up_count"] = 1
    monkeypatch.setattr(pd, "read_sql_query", lambda *args, **kwargs: source)
    review = IntegrityReview(store, "DEMAND__TOTAL", input_signature="new", max_days=2)
    ranges = review.plan(None, "s", "t", "d", "UP", df.timestamp.min(), df.timestamp.max())
    assert len(review.changed_dates) == 2
    assert ranges
    review.commit()
    state = json.loads(review_path(store).read_text())
    assert len(state["pending_rebuild"]) == 1


def test_explicit_review_larger_than_budget_progresses_on_repeat(tmp_path, monkeypatch):
    store = ParquetFinalStore(tmp_path / "final.parquet")
    df = daily("2024-01-01", "2024-01-03")
    store.save_final(df)
    source = df[["timestamp", "DEMAND__TOTAL"]].rename(columns={"DEMAND__TOTAL": "source_count"})
    source["source_up_count"] = 1
    monkeypatch.setattr(pd, "read_sql_query", lambda *args, **kwargs: source)
    dates = []
    for hour in (9, 10, 11):
        review = IntegrityReview(store, "DEMAND__TOTAL", max_days=1)
        review.plan(None, "s", "t", "d", "UP", df.timestamp.min(), df.timestamp.max(),
                    review_ranges=[("2024-01-01", "2024-01-03")], now=f"2026-11-20 {hour:02d}:00")
        dates.extend(review.checked_days)
        review.commit()
    assert len(set(dates)) == 3


def test_long_imputed_gap_still_preserves_the_weekly_cycle():
    source = daily("2023-01-01", "2024-05-31")
    out = impute_tail_to_date(source, pd.Timestamp("2024-05-31"), pd.Timestamp("2024-09-26"))
    assert out.loc[out.timestamp == "2024-09-22", "DEMAND__TOTAL"].iloc[0] == 10
    assert out.loc[out.timestamp == "2024-09-23", "DEMAND__TOTAL"].iloc[0] == 1000


def test_source_max_shrinking_checks_deleted_observations_but_not_estimated_tail(tmp_path, monkeypatch):
    store = ParquetFinalStore(tmp_path / "final.parquet")
    df = daily("2024-01-01", "2024-01-04")
    df.loc[df.timestamp == "2024-01-04", "__is_imputed"] = True
    store.save_final(df)
    summary = pd.DataFrame({"timestamp": pd.to_datetime(["2024-01-01", "2024-01-02"]),
                            "source_count": [1000, 1000], "source_up_count": [1, 1]})
    monkeypatch.setattr(pd, "read_sql_query", lambda *args, **kwargs: summary)
    review = IntegrityReview(store, "DEMAND__TOTAL")
    review.plan(None, "s", "t", "d", "UP", df.timestamp.min(), df.timestamp.max(), observed_source_end="2024-01-02")
    assert pd.Timestamp("2024-01-03") in review.changed_dates
    assert pd.Timestamp("2024-01-04") not in review.checked_days


def test_review_does_not_commit_if_raw_extraction_disagrees_with_summary(tmp_path, monkeypatch):
    store = ParquetFinalStore(tmp_path / "final.parquet")
    store.save_final(daily("2024-01-01", "2024-01-01", amount=[1]))
    manager = ParquetIncrementalManager(tmp_path / "incremental")
    monkeypatch.setattr(pd, "read_sql_query", lambda *args, **kwargs: pd.DataFrame({
        "timestamp": [pd.Timestamp("2024-01-01")], "source_count": [100], "source_up_count": [1],
    }))
    review = IntegrityReview(store, "DEMAND__TOTAL")
    review.plan(None, "s", "t", "d", "UP", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01"))
    manager.add_data(pd.DataFrame({"timestamp": [pd.Timestamp("2024-01-01")], "DEMAND__TOTAL": [50]}))
    with pytest.raises(ValueError, match="extraction incomplete"):
        review.validate_chunks(manager)
    assert not review.path.exists()
    assert len(manager.iter_incremental_files()) == 1


def test_empty_replacement_does_not_leave_the_previous_final_on_disk(tmp_path):
    store = ParquetFinalStore(tmp_path / "final.parquet")
    store.save_final(daily("2024-01-01", "2024-01-01"))
    manager = ParquetIncrementalManager(tmp_path / "incremental")
    aggregate_final_optimized(manager, store, replace_dates=[pd.Timestamp("2024-01-01")])
    assert store.load_final().empty


@pytest.mark.parametrize("domain,aggregate", [("DEMAND", aggregate_final_optimized), ("DIAGNOSIS", aggregate_diagnosis_final_optimized)])
def test_empty_selection_backfill_is_recorded_to_avoid_repeating_history(tmp_path, domain, aggregate):
    import importlib
    runner = importlib.import_module(f"pipelines.{domain.lower()}.incremental_optimized")
    store = ParquetFinalStore(tmp_path / "final.parquet")
    store.save_final(daily("2024-01-01", "2024-01-03", domain))
    manager = ParquetIncrementalManager(tmp_path / "incremental")
    aggregate(manager, store, expected_geo_aliases={"UP": {"EMPTY"}}, impute_until=pd.Timestamp("2024-01-03"))
    assert store.load_final()[domain + "__TOTAL__UP__EMPTY"].eq(0).all()
    assert runner._missing_geo_sources(store, None, {"00001": "EMPTY"}) == {}


def test_editing_a_cached_total_invalidates_the_review_cooldown(tmp_path, monkeypatch):
    store = ParquetFinalStore(tmp_path / "final.parquet")
    df = daily("2024-01-01", "2024-01-01")
    store.save_final(df)
    source = pd.DataFrame({"timestamp": df.timestamp, "source_count": [1000], "source_up_count": [3]})
    query = Mock(return_value=source)
    monkeypatch.setattr(pd, "read_sql_query", query)
    first = IntegrityReview(store, "DEMAND__TOTAL")
    first.plan(None, "s", "t", "d", "UP", df.timestamp.min(), df.timestamp.max(), now="2026-11-20 09:00")
    first.commit()
    df["DEMAND__TOTAL"] = 1
    store.save_final(df)
    second = IntegrityReview(store, "DEMAND__TOTAL")
    second.plan(None, "s", "t", "d", "UP", df.timestamp.min(), df.timestamp.max(), now="2026-11-20 09:30")
    assert query.call_count == 2
    assert second.changed_dates == [pd.Timestamp("2024-01-01")]
