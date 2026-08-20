import pandas as pd

from pipelines.demand.aggregation_optimized import (
    build_daily_features_by_group_optimized,
    build_daily_features_global_optimized,
    build_daily_total_cat_optimized,
)
from pipelines.diagnosis.aggregation_optimized import (
    build_daily_diagnosis_by_group_optimized,
    build_daily_total_by_group_optimized,
    build_daily_total_general_optimized,
    build_diagnosis_wide_format_optimized,
    _build_diagnosis_wide_final,
)
from pipelines.diagnosis.incremental_optimized import (
    _expand_diagnosis_code_spec,
    _filter_if_selected as _filter_selected_diagnosis_geo,
    _load_selected_codes,
)
from pipelines.demand.incremental_optimized import (
    _filter_if_selected as _filter_selected_demand_geo,
    _normalize_rs_values,
    _normalize_up_codes,
)
from pipelines.shared.final_joiner import FinalDataJoiner
from pipelines.sample_runner import run_sample_diagnosis_pipeline


def test_demand_columns_follow_qualud_ideal_naming():
    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01", "2026-01-01"]),
            "counts": [1, 1],
            "SERVEI_CODI": ["INF", "PED"],
            "TIPUS_CLASS": ["C9C", "9T"],
            "RS": ["girona", "barcelona"],
            "UP": ["185", "00102"],
        }
    )

    total = build_daily_total_cat_optimized(df).reset_index()
    global_daily = build_daily_features_global_optimized(df)
    rs_daily = build_daily_features_by_group_optimized(df, group_col="RS")
    up_daily = build_daily_features_by_group_optimized(df, group_col="UP")

    assert "DEMAND__TOTAL" in total.columns
    assert "DEMAND__SERVEI_CODI__INF" in global_daily.columns
    assert "DEMAND__SERVEI_CODI__INF__RS__GIRONA" in rs_daily.columns
    assert "DEMAND__TOTAL__RS__GIRONA" in rs_daily.columns
    assert "DEMAND__SERVEI_CODI__INF__UP__00185" in up_daily.columns
    assert "DEMAND__TOTAL__UP__00185" in up_daily.columns


def test_diagnosis_columns_follow_qualud_ideal_naming():
    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01", "2026-01-01"]),
            "DIAG_CODE": ["A09", "J00"],
            "RS": ["girona", "barcelona"],
            "up_c": ["185", "00102"],
            "n": [2, 3],
        }
    )

    total = build_daily_total_general_optimized(df).reset_index()
    rs_total = build_daily_total_by_group_optimized(df, "RS", "RS")
    code_wide = build_diagnosis_wide_format_optimized(df)
    rs_long = build_daily_diagnosis_by_group_optimized(df, group_col="RS")
    up_long = build_daily_diagnosis_by_group_optimized(df, group_col="up_c")
    final = _build_diagnosis_wide_final(
        pd.concat([total, rs_total, code_wide, rs_long, up_long], ignore_index=True)
    )

    assert "DIAGNOSIS__TOTAL" in final.columns
    assert "DIAGNOSIS__TOTAL__RS__GIRONA" in final.columns
    assert "DIAGNOSIS__ICD10_3__A09" in final.columns
    assert "DIAGNOSIS__ICD10_3__A09__RS__GIRONA" in final.columns
    assert "DIAGNOSIS__ICD10_3__A09__UP__00185" in final.columns


def test_final_join_keeps_canonical_columns_without_extra_prefix(tmp_path):
    demand_path = tmp_path / "demand.parquet"
    diagnosis_path = tmp_path / "diagnosis.parquet"
    joined_path = tmp_path / "joined.parquet"

    pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01"]),
            "DEMAND__TOTAL": [10],
        }
    ).to_parquet(demand_path, index=False)
    pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01"]),
            "DIAGNOSIS__TOTAL": [3],
        }
    ).to_parquet(diagnosis_path, index=False)

    joined = FinalDataJoiner(demand_path, diagnosis_path, joined_path).join_columnwise()

    assert "DEMAND__TOTAL" in joined.columns
    assert "DIAGNOSIS__TOTAL" in joined.columns
    assert "DEMAND_DEMAND__TOTAL" not in joined.columns
    assert "DIAGNOSIS_DIAGNOSIS__TOTAL" not in joined.columns


def test_diagnosis_selection_expands_ranges_and_decimal_codes(tmp_path):
    path = tmp_path / "selected_diagnosis_codes.csv"
    path.write_text(
        "\n".join(
            [
                "ICD10_3,feature_name,definition_ca",
                "J00-J02,G01,Infeccions respiratories",
                "F40-F4,G04,Salut mental",
                "S00-T88,G05,Dolor musculoesqueletic",
                "U07.1,D08,COVID-19",
            ]
        ),
        encoding="utf-8",
    )

    selected = _load_selected_codes(path)

    assert selected["J00"] == ["G01"]
    assert selected["J02"] == ["G01"]
    assert selected["F40"] == ["G04"]
    assert selected["F49"] == ["G04"]
    assert selected["S00"] == ["G05"]
    assert selected["S99"] == ["G05"]
    assert selected["T88"] == ["G05"]
    assert selected["U07"] == ["D08"]
    assert "Salut mental" not in {alias for aliases in selected.values() for alias in aliases}
    assert _expand_diagnosis_code_spec("A00-A09") == [
        "A00",
        "A01",
        "A02",
        "A03",
        "A04",
        "A05",
        "A06",
        "A07",
        "A08",
        "A09",
    ]


def test_geo_selection_maps_source_values_to_configured_subset_ids():
    df = pd.DataFrame(
        {
            "RS": ["GIRONA", "LLEIDA"],
            "UP": ["00348", "00443"],
        }
    )

    demand_rs = _filter_selected_demand_geo(
        df,
        "RS",
        {"GIRONA": "RS_64"},
        _normalize_rs_values,
    )
    demand_up = _filter_selected_demand_geo(
        df,
        "UP",
        {"00348": "MICRO_01"},
        _normalize_up_codes,
    )
    diagnosis_rs = _filter_selected_diagnosis_geo(df, "RS", {"GIRONA": "RS_64"})
    diagnosis_up = _filter_selected_diagnosis_geo(df, "UP", {"00348": "MICRO_01"})

    assert demand_rs["RS"].tolist() == ["RS_64"]
    assert demand_up["UP"].tolist() == ["MICRO_01"]
    assert diagnosis_rs["RS"].tolist() == ["RS_64"]
    assert diagnosis_up["UP"].tolist() == ["MICRO_01"]


def test_sample_diagnosis_pipeline_uses_configured_groups_and_geo_ids(tmp_path):
    output_path = run_sample_diagnosis_pipeline(
        "data/sample/input",
        tmp_path,
    )

    df = pd.read_parquet(output_path)

    for alias in [
        "G01",
        "G02",
        "G03",
        "G04",
        "G05",
        "D01",
        "D02",
        "D03",
        "D04",
        "D05",
        "D06",
        "D07",
        "D08",
        "D09",
    ]:
        assert f"DIAGNOSIS__ICD10_3__{alias}" in df.columns

    assert "DIAGNOSIS__ICD10_3__G01__RS__RS_67" in df.columns
    assert "DIAGNOSIS__ICD10_3__G01__UP__MICRO_01" in df.columns
    assert "DIAGNOSIS__TOTAL__RS__RS_67" in df.columns
    assert "DIAGNOSIS__TOTAL__UP__MICRO_01" in df.columns
