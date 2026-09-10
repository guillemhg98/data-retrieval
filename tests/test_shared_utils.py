import pandas as pd

from pipelines.shared.utils import (
    get_data_for_year,
    get_half_year_ranges,
    get_incremental_processing_window,
    get_min_max_date,
)


def test_get_min_max_date_applies_upper_bound_before_database_max(monkeypatch):
    captured = {}

    def fake_read_sql_query(query, conn, params):
        captured["query"] = query
        captured["params"] = params
        return pd.DataFrame(
            [
                {
                    "min_date": "2025-01-01",
                    "max_date": "2026-05-27",
                }
            ]
        )

    monkeypatch.setattr(pd, "read_sql_query", fake_read_sql_query)

    min_date, max_date = get_min_max_date(
        conn=object(),
        schema="dbo",
        table_name="diagnosis",
        date_column="DATA",
        min_valid_date="2008-01-01",
        max_valid_date="2026-06-03",
    )

    assert "AND [DATA] < ?" in captured["query"]
    assert captured["params"] == ["2008-01-01", "2026-06-03"]
    assert min_date == pd.Timestamp("2025-01-01")
    assert max_date == pd.Timestamp("2026-05-27")


def test_incremental_window_processes_only_observed_source_days_before_imputation():
    window = get_incremental_processing_window(
        min_date=pd.Timestamp("2026-01-01"),
        max_date=pd.Timestamp("2026-05-27"),
        last_processed_date=pd.Timestamp("2025-12-31"),
        requested_end_date=pd.Timestamp("2026-06-02"),
        today=pd.Timestamp("2026-06-02"),
    )

    assert window == (
        pd.Timestamp("2026-01-01"),
        pd.Timestamp("2026-05-28"),
        pd.Timestamp("2026-05-27"),
    )


def test_half_year_ranges_split_without_crossing_bounds():
    ranges = get_half_year_ranges(
        pd.Timestamp("2026-03-15"),
        pd.Timestamp("2027-02-01"),
    )

    assert ranges == [
        ("2026H1", pd.Timestamp("2026-03-15"), pd.Timestamp("2026-07-01")),
        ("2026H2", pd.Timestamp("2026-07-01"), pd.Timestamp("2027-01-01")),
        ("2027H1", pd.Timestamp("2027-01-01"), pd.Timestamp("2027-02-01")),
    ]


def test_get_data_for_year_can_filter_normalized_up_values(monkeypatch):
    captured = {}

    def fake_read_sql_query(query, conn, params):
        captured["query"] = query
        captured["params"] = params
        return pd.DataFrame()

    monkeypatch.setattr(pd, "read_sql_query", fake_read_sql_query)

    get_data_for_year(
        conn=object(),
        schema="dbo",
        table_name="visits",
        date_column="DATA_VISITA",
        year_start=pd.Timestamp("2026-01-01"),
        year_end=pd.Timestamp("2027-01-01"),
        selected_cols=["DATA_VISITA", "UP"],
        where_in={"UP": ["00348", "08368"]},
    )

    assert "RIGHT('00000' + LTRIM(RTRIM(CAST([UP] AS VARCHAR(20)))), 5) IN (?, ?)" in captured["query"]
    assert captured["params"] == [
        pd.Timestamp("2026-01-01"),
        pd.Timestamp("2027-01-01"),
        "00348",
        "08368",
    ]


def test_demand_year_failure_falls_back_to_half_year_chunks(monkeypatch):
    from pipelines.demand import incremental_optimized as demand_incremental

    calls = []

    def fake_process_range(
        conn,
        schema,
        table_name,
        date_column,
        range_label,
        range_start,
        range_end,
        end_exclusive,
        up_rs,
        incremental_mgr,
        selected_rs,
        selected_up,
        missing_geos=None,
    ):
        calls.append((range_label, range_start, range_end))
        if range_label == "2026":
            raise RuntimeError("year too large")
        return range_start

    monkeypatch.setattr(
        demand_incremental,
        "_process_demand_range",
        fake_process_range,
    )

    result = demand_incremental._process_demand_range_with_fallback(
        conn=object(),
        schema="dbo",
        table_name="visits",
        date_column="DATA_VISITA",
        range_label="2026",
        range_start=pd.Timestamp("2026-01-01"),
        range_end=pd.Timestamp("2027-01-01"),
        end_exclusive=pd.Timestamp("2027-01-01"),
        up_rs=pd.DataFrame(),
        incremental_mgr=object(),
        selected_rs=None,
        selected_up=None,
    )

    assert [label for label, _, _ in calls] == ["2026", "2026H1", "2026H2"]
    assert calls[1][1:] == (
        pd.Timestamp("2026-01-01"),
        pd.Timestamp("2026-07-01"),
    )
    assert calls[2][1:] == (
        pd.Timestamp("2026-07-01"),
        pd.Timestamp("2027-01-01"),
    )
    assert result == pd.Timestamp("2026-07-01")
