"""Run the pipelines against bundled synthetic CSV data."""
from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from pipelines.demand.aggregation_optimized import (
    build_daily_features_by_group_optimized,
    build_daily_features_global_optimized,
    build_daily_total_cat_optimized,
)
from pipelines.demand.transformations import prepare_visits_chunk
from pipelines.diagnosis.aggregation_optimized import (
    build_daily_diagnosis_by_group_optimized,
    build_daily_total_by_group_optimized,
    build_daily_total_general_optimized,
    build_diagnosis_wide_format_optimized,
)
from pipelines.diagnosis.incremental_optimized import (
    _expand_selected_code_aliases as _expand_configured_selected_code_aliases,
    _filter_if_selected as _filter_selected_diagnosis_geo,
    _load_selected_codes as _load_configured_selected_codes,
    _load_selected_rs as _load_selected_diagnosis_rs,
    _load_selected_up as _load_selected_diagnosis_up,
)
from pipelines.demand.incremental_optimized import (
    _filter_if_selected as _filter_selected_demand_geo,
    _load_selected_rs as _load_selected_demand_rs,
    _load_selected_up as _load_selected_demand_up,
    _normalize_rs_values as _normalize_demand_rs_values,
    _normalize_up_codes as _normalize_demand_up_codes,
)
from pipelines.shared.final_joiner import FinalDataJoiner
from pipelines.shared.naming import (
    DOMAIN_DIAGNOSIS,
    GEO_RS,
    GEO_UP,
    VARIABLE_ICD10_3,
    clean_geo_series,
    feature_code,
)
from pipelines.shared.parquet_storage import drop_future_timestamp_rows


SAMPLE_INPUT_FILES = {
    "up_rs": "up_rs.csv",
    "demand": "demand_visits.csv",
    "diagnosis": "diagnosis_visits.csv",
    "selected_codes": "selected_codes.csv",
}


def run_sample_demand_pipeline(
    input_dir: str | Path,
    output_dir: str | Path,
    start_date: pd.Timestamp | None = None,
    end_date: pd.Timestamp | None = None,
) -> Path:
    """Run the demand pipeline with local synthetic input data."""
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    _ensure_sample_files(input_dir, ["up_rs", "demand"])

    up_rs = _load_up_rs(input_dir)
    visits = pd.read_csv(
        input_dir / SAMPLE_INPUT_FILES["demand"],
        dtype={"UP": str},
    )

    visits = prepare_visits_chunk(visits, up_rs=up_rs)
    visits = _filter_by_date_range(visits, "DATA_VISITA", start_date, end_date)
    visits = visits[visits["DATA_VISITA"] < _tomorrow()].copy()
    visits["timestamp"] = visits["DATA_VISITA"]
    selected_rs = _load_selected_demand_rs(_selection_path("selected_rs.csv", input_dir))
    selected_up = _load_selected_demand_up(_selection_path("selected_up.csv", input_dir))

    cat_daily = build_daily_total_cat_optimized(visits)
    global_daily = build_daily_features_global_optimized(visits)
    rs_daily = build_daily_features_by_group_optimized(
        _filter_selected_demand_geo(
            visits,
            "RS",
            selected_rs,
            _normalize_demand_rs_values,
        ),
        group_col="RS",
    )
    up_daily = build_daily_features_by_group_optimized(
        _filter_selected_demand_geo(
            visits,
            "UP",
            selected_up,
            _normalize_demand_up_codes,
        ),
        group_col="UP",
    )

    incremental_dir = output_dir / "demand_pipeline" / "incremental"
    _save_parquet(_with_timestamp_column(cat_daily), incremental_dir / "demand_cat_daily.parquet")
    _save_parquet(_with_timestamp_column(global_daily), incremental_dir / "demand_global_daily.parquet")
    _save_parquet(_with_timestamp_column(rs_daily), incremental_dir / "demand_rs_daily.parquet")
    _save_parquet(_with_timestamp_column(up_daily), incremental_dir / "demand_up_daily.parquet")

    final = _combine_wide_frames([cat_daily, global_daily, rs_daily, up_daily])
    final_path = output_dir / "demand_pipeline" / "finals" / "demand_final.parquet"
    _save_parquet(final, final_path)
    return final_path


def run_sample_diagnosis_pipeline(
    input_dir: str | Path,
    output_dir: str | Path,
    start_date: pd.Timestamp | None = None,
    end_date: pd.Timestamp | None = None,
) -> Path:
    """Run the diagnosis pipeline with local synthetic input data."""
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    _ensure_sample_files(input_dir, ["up_rs", "diagnosis"])

    up_rs = _load_up_rs(input_dir)
    diagnosis = pd.read_csv(
        input_dir / SAMPLE_INPUT_FILES["diagnosis"],
        dtype={"up_c": str, "problema_salut_c": str},
    )
    selected_codes = _load_configured_selected_codes(
        _selection_path(
            "selected_diagnosis_codes.csv",
            input_dir,
            sample_fallback=SAMPLE_INPUT_FILES["selected_codes"],
        )
    ) or {}
    selected_rs = _load_selected_diagnosis_rs(_selection_path("selected_rs.csv", input_dir))
    selected_up = _load_selected_diagnosis_up(_selection_path("selected_up.csv", input_dir))

    diagnosis["timestamp"] = pd.to_datetime(
        diagnosis["data_visita"],
        errors="coerce",
    ).dt.floor("D")
    diagnosis = diagnosis.dropna(subset=["timestamp"])
    diagnosis = _filter_by_date_range(diagnosis, "timestamp", start_date, end_date)
    diagnosis = diagnosis[diagnosis["timestamp"] < _tomorrow()].copy()
    diagnosis["up_c"] = diagnosis["up_c"].astype(str).str.zfill(5)
    diagnosis["n"] = 1
    diagnosis["problema_salut_c"] = _normalize_diag_codes(diagnosis["problema_salut_c"])
    diagnosis = diagnosis.rename(columns={"problema_salut_c": "DIAG_CODE"})

    up_rs_map = up_rs[["Codi UP", "RS"]].copy()
    up_rs_map["Codi UP"] = up_rs_map["Codi UP"].astype(str).str.zfill(5)
    up_rs_map = up_rs_map.rename(columns={"Codi UP": "up_c"})
    diagnosis = diagnosis.merge(up_rs_map, on="up_c", how="left")
    diagnosis["RS"] = diagnosis["RS"].fillna("UNKNOWN")

    total_daily = build_daily_total_general_optimized(diagnosis)
    rs_total = build_daily_total_by_group_optimized(
        _filter_selected_diagnosis_geo(diagnosis, "RS", selected_rs),
        group_col="RS",
        group_label="RS",
    )
    up_total = build_daily_total_by_group_optimized(
        _filter_selected_diagnosis_geo(diagnosis, "up_c", selected_up),
        group_col="up_c",
        group_label="UP",
    )
    code_diagnosis = _expand_configured_selected_code_aliases(diagnosis, selected_codes)
    code_daily = build_diagnosis_wide_format_optimized(code_diagnosis)
    rs_long = build_daily_diagnosis_by_group_optimized(
        _filter_selected_diagnosis_geo(code_diagnosis, "RS", selected_rs),
        group_col="RS",
    )
    up_long = build_daily_diagnosis_by_group_optimized(
        _filter_selected_diagnosis_geo(code_diagnosis, "up_c", selected_up),
        group_col="up_c",
    )

    rs_wide = _pivot_diagnosis_group(rs_long, group_column="DIAG_RS", label="RS")
    up_wide = _pivot_diagnosis_group(up_long, group_column="DIAG_up_c", label="UP")

    incremental_dir = output_dir / "diagnosis_pipeline" / "incremental"
    _save_parquet(_with_timestamp_column(total_daily), incremental_dir / "diagnosis_total_daily.parquet")
    _save_parquet(_with_timestamp_column(rs_total), incremental_dir / "diagnosis_rs_total_daily.parquet")
    _save_parquet(_with_timestamp_column(up_total), incremental_dir / "diagnosis_up_total_daily.parquet")
    _save_parquet(_with_timestamp_column(code_daily), incremental_dir / "diagnosis_code_daily.parquet")
    _save_parquet(rs_long, incremental_dir / "diagnosis_rs_long.parquet")
    _save_parquet(up_long, incremental_dir / "diagnosis_up_long.parquet")

    final = _combine_wide_frames([total_daily, rs_total, up_total, code_daily, rs_wide, up_wide])
    final_path = output_dir / "diagnosis_pipeline" / "finals" / "diagnosis_final.parquet"
    _save_parquet(final, final_path)
    return final_path


def join_sample_outputs(output_dir: str | Path) -> Path:
    """Join sample demand and diagnosis final outputs."""
    output_dir = Path(output_dir)
    demand_path = output_dir / "demand_pipeline" / "finals" / "demand_final.parquet"
    diagnosis_path = output_dir / "diagnosis_pipeline" / "finals" / "diagnosis_final.parquet"
    joined_path = output_dir / "finals" / "demand_diagnosis_joined.parquet"

    joiner = FinalDataJoiner(
        demand_final_file=demand_path,
        diagnosis_final_file=diagnosis_path,
        output_file=joined_path,
    )
    return joiner.join_and_save(
        demand_prefix="DEMAND",
        diagnosis_prefix="DIAGNOSIS",
        fill_method="ffill",
        compression="snappy",
    )


def _ensure_sample_files(input_dir: Path, keys: Iterable[str]) -> None:
    missing = [
        input_dir / SAMPLE_INPUT_FILES[key]
        for key in keys
        if not (input_dir / SAMPLE_INPUT_FILES[key]).exists()
    ]
    if missing:
        missing_text = ", ".join(str(path) for path in missing)
        raise FileNotFoundError(f"Missing sample input files: {missing_text}")


def _load_up_rs(input_dir: Path) -> pd.DataFrame:
    up_rs = pd.read_csv(input_dir / SAMPLE_INPUT_FILES["up_rs"], dtype={"Codi UP": str})
    up_rs["Codi UP"] = up_rs["Codi UP"].astype(str).str.zfill(5)
    return up_rs


def _selection_path(
    filename: str,
    input_dir: Path,
    sample_fallback: str | None = None,
) -> Path:
    """Prefer shared project selections, falling back to sample-local files."""
    candidates = [
        Path.cwd() / "selections" / filename,
        input_dir / filename,
    ]
    if sample_fallback:
        candidates.append(input_dir / sample_fallback)

    for path in candidates:
        if path.exists():
            return path
    return candidates[0]


def _load_selected_codes(input_dir: Path) -> dict[str, list[str]]:
    path = input_dir / SAMPLE_INPUT_FILES["selected_codes"]
    if not path.exists():
        return {}
    selected = pd.read_csv(path, dtype=str)
    if selected.empty:
        return {}

    aliases: dict[str, list[str]] = {}
    column_lookup = {str(col).strip().lower(): col for col in selected.columns}
    alias_col = column_lookup.get("feature_name")
    for _, row in selected.iterrows():
        code = _normalize_diag_codes(pd.Series([row.iloc[0]])).iloc[0]
        if pd.isna(code) or not code:
            continue
        alias = _normalize_feature_name(row[alias_col], fallback=code) if alias_col else code
        code_aliases = aliases.setdefault(code, [])
        if alias not in code_aliases:
            code_aliases.append(alias)
    return aliases


def _expand_selected_code_aliases(
    df: pd.DataFrame,
    selected_codes: dict[str, list[str]],
    code_col: str = "DIAG_CODE",
) -> pd.DataFrame:
    if not selected_codes:
        return df.iloc[0:0].copy()

    mapping = pd.DataFrame(
        [
            (source_code, alias)
            for source_code, aliases in selected_codes.items()
            for alias in aliases
        ],
        columns=[code_col, "_DIAG_OUTPUT_ALIAS"],
    )
    out = df.merge(mapping, on=code_col, how="inner")
    if out.empty:
        return out.drop(columns=["_DIAG_OUTPUT_ALIAS"], errors="ignore")

    out[code_col] = out["_DIAG_OUTPUT_ALIAS"]
    return out.drop(columns=["_DIAG_OUTPUT_ALIAS"])


def _normalize_diag_codes(values: pd.Series) -> pd.Series:
    return values.astype("string").str.strip().str.upper().str[:3]


def _normalize_feature_name(value, fallback: str) -> str:
    if pd.isna(value):
        return fallback
    token = str(value).strip()
    if not token:
        return fallback
    token = "".join(char if char.isalnum() else "_" for char in token)
    token = "_".join(part for part in token.upper().split("_") if part)
    return token or fallback


def _tomorrow() -> pd.Timestamp:
    return pd.Timestamp.today().normalize() + pd.Timedelta(days=1)


def _filter_by_date_range(
    df: pd.DataFrame,
    timestamp_col: str,
    start_date: pd.Timestamp | None,
    end_date: pd.Timestamp | None,
) -> pd.DataFrame:
    out = df.copy()
    out[timestamp_col] = pd.to_datetime(out[timestamp_col], errors="coerce").dt.floor("D")
    out = out.dropna(subset=[timestamp_col])

    if start_date is not None:
        out = out[out[timestamp_col] >= pd.to_datetime(start_date).normalize()]
    if end_date is not None:
        end_exclusive = pd.to_datetime(end_date).normalize() + pd.Timedelta(days=1)
        out = out[out[timestamp_col] < end_exclusive]

    return out.copy()


def _with_timestamp_column(
    df: pd.DataFrame,
    timestamp_col: str = "timestamp",
) -> pd.DataFrame:
    out = df.copy()
    if timestamp_col in out.columns:
        out[timestamp_col] = pd.to_datetime(out[timestamp_col]).dt.floor("D")
        return out.reset_index(drop=True)

    out.index = pd.to_datetime(out.index).floor("D")
    out.index.name = timestamp_col
    return out.reset_index()


def _combine_wide_frames(
    frames: Iterable[pd.DataFrame],
    timestamp_col: str = "timestamp",
) -> pd.DataFrame:
    indexed = [_as_timestamp_index(frame, timestamp_col) for frame in frames]
    combined = pd.concat(indexed, axis=1).fillna(0).sort_index()
    combined.index.name = timestamp_col
    return drop_future_timestamp_rows(combined.reset_index(), timestamp_col)


def _as_timestamp_index(
    df: pd.DataFrame,
    timestamp_col: str = "timestamp",
) -> pd.DataFrame:
    out = df.copy().drop(columns=["index"], errors="ignore")

    if timestamp_col in out.columns:
        out[timestamp_col] = pd.to_datetime(out[timestamp_col]).dt.floor("D")
        out = out.set_index(timestamp_col)
    else:
        out.index = pd.to_datetime(out.index).floor("D")

    return out.groupby(level=0).sum(numeric_only=True).sort_index()


def _pivot_diagnosis_group(
    df: pd.DataFrame,
    group_column: str,
    label: str,
    timestamp_col: str = "timestamp",
) -> pd.DataFrame:
    out = df.copy()
    out[timestamp_col] = pd.to_datetime(out[timestamp_col]).dt.floor("D")
    geo_level = GEO_UP if label.upper() == GEO_UP else GEO_RS
    out[group_column] = clean_geo_series(out[group_column], geo_level)
    out["DIAG_DIAG_CODE"] = out["DIAG_DIAG_CODE"].fillna("UNKNOWN").astype(str)
    out["feature"] = [
        feature_code(DOMAIN_DIAGNOSIS, VARIABLE_ICD10_3, code, geo_level, geo)
        for code, geo in zip(out["DIAG_DIAG_CODE"], out[group_column])
    ]

    wide = out.pivot_table(
        index=timestamp_col,
        columns="feature",
        values="count",
        aggfunc="sum",
        fill_value=0,
        observed=True,
    )
    wide["timestamp"] = wide.index
    return wide


def _save_parquet(df: pd.DataFrame, path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df = drop_future_timestamp_rows(df, "timestamp")
    df.to_parquet(path, compression="snappy", index=False)
