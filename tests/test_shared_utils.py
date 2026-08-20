import pandas as pd

from pipelines.shared.utils import get_incremental_processing_window, get_min_max_date


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
