"""Canonical feature naming for Qualud dataset variables."""
from __future__ import annotations

import re
from typing import Optional

import pandas as pd

DOMAIN_DEMAND = "DEMAND"
DOMAIN_DIAGNOSIS = "DIAGNOSIS"
VARIABLE_ICD10_3 = "ICD10_3"
VARIABLE_TOTAL = "TOTAL"
GEO_RS = "RS"
GEO_UP = "UP"

_SEPARATOR = "__"
_DEMAND_CATEGORICAL_VARS = [
    "VISI_LLOC_VISITA",
    "VISI_SITUACIO_VISITA",
    "SERVEI_CODI",
    "TIPUS_CLASS",
    "TIPUS_VISITA_AGRUPAT",
]


def feature_code(
    domain: str,
    variable: str,
    category: Optional[object] = None,
    geo_level: Optional[str] = None,
    geo: Optional[object] = None,
) -> str:
    """Build a Qualud parseable feature code.

    Format:
    {DOMAIN}__{VARIABLE}__{CATEGORY}
    {DOMAIN}__{VARIABLE}__{CATEGORY}__RS__{GEO}
    {DOMAIN}__{VARIABLE}__{CATEGORY}__UP__{GEO}
    """
    parts = [_clean_token(domain), _clean_token(variable)]
    if category is not None:
        parts.append(_clean_token(category))
    if geo_level is not None:
        parts.extend([_clean_token(geo_level), _clean_token(geo)])
    return _SEPARATOR.join(parts)


def total_code(
    domain: str,
    geo_level: Optional[str] = None,
    geo: Optional[object] = None,
) -> str:
    """Build a canonical total feature code."""
    return feature_code(domain, VARIABLE_TOTAL, geo_level=geo_level, geo=geo)


def clean_geo_series(values: pd.Series, geo_level: str) -> pd.Series:
    """Normalize geographic labels for stable RS/UP suffixes."""
    out = values.fillna("UNKNOWN").astype(str).str.strip().replace("", "UNKNOWN")
    if geo_level == GEO_RS:
        return out.str.upper()
    if geo_level == GEO_UP:
        return out.str.zfill(5)
    return out


def canonicalize_feature_name(column: str, domain: str) -> str:
    """Best-effort migration of historical column names to the ideal naming."""
    if column == "timestamp" or column.startswith(f"{domain}__"):
        return column

    if domain == DOMAIN_DEMAND:
        return _canonicalize_demand_column(column)
    if domain == DOMAIN_DIAGNOSIS:
        return _canonicalize_diagnosis_column(column)
    return column


def _canonicalize_demand_column(column: str) -> str:
    if column == "DEMANDA_TOTAL" or column == "demanda__TOTAL_CAT":
        return total_code(DOMAIN_DEMAND)

    total_match = re.match(r"^demanda__TOTAL_(RS|UP)_(.+)$", column, flags=re.I)
    if total_match:
        level = total_match.group(1).upper()
        geo = _clean_geo(total_match.group(2), level)
        return total_code(DOMAIN_DEMAND, level, geo)

    cat_match = re.match(r"^demanda__(.+?)__(.+)$", column, flags=re.I)
    if cat_match:
        return feature_code(DOMAIN_DEMAND, cat_match.group(1), cat_match.group(2))

    for variable in _DEMAND_CATEGORICAL_VARS:
        prefix = f"demanda_{variable}_"
        if not column.startswith(prefix):
            continue

        body = column[len(prefix) :]
        up_marker = "_UP_"
        if up_marker in body:
            category, geo = body.rsplit(up_marker, 1)
            return feature_code(
                DOMAIN_DEMAND,
                variable,
                category,
                GEO_UP,
                _clean_geo(geo, GEO_UP),
            )

        if "_RS_" in body:
            category, geo_suffix = body.split("_RS_", 1)
            geo = f"RS_{geo_suffix}"
            return feature_code(
                DOMAIN_DEMAND,
                variable,
                category,
                GEO_RS,
                _clean_geo(geo, GEO_RS),
            )

        if "_" in body:
            category, geo = body.rsplit("_", 1)
            return feature_code(
                DOMAIN_DEMAND,
                variable,
                category,
                GEO_RS,
                _clean_geo(geo, GEO_RS),
            )

    return column


def _canonicalize_diagnosis_column(column: str) -> str:
    if column in {"DIAG_TOTAL", "TOTAL"}:
        return total_code(DOMAIN_DIAGNOSIS)

    total_match = re.match(r"^(?:DIAG_)?TOTAL__(RS|UP)__(.+)$", column, flags=re.I)
    if total_match:
        level = total_match.group(1).upper()
        geo = _clean_geo(total_match.group(2), level)
        return total_code(DOMAIN_DIAGNOSIS, level, geo)

    total_match = re.match(r"^DIAG_TOTAL_(RS|UP)_(.+)$", column, flags=re.I)
    if total_match:
        level = total_match.group(1).upper()
        geo = _clean_geo(total_match.group(2), level)
        return total_code(DOMAIN_DIAGNOSIS, level, geo)

    code_match = re.match(r"^(?:DIAG_CODE|ICD10_3)_(.+)$", column)
    if code_match:
        return feature_code(DOMAIN_DIAGNOSIS, VARIABLE_ICD10_3, code_match.group(1))

    grouped_match = re.match(r"^ICD10_3__(.+)__(RS|UP)__(.+)$", column)
    if grouped_match:
        level = grouped_match.group(2).upper()
        geo = _clean_geo(grouped_match.group(3), level)
        return feature_code(
            DOMAIN_DIAGNOSIS,
            VARIABLE_ICD10_3,
            grouped_match.group(1),
            level,
            geo,
        )

    return column


def _clean_geo(value: object, geo_level: str) -> str:
    series = pd.Series([value])
    return str(clean_geo_series(series, geo_level).iloc[0])


def _clean_token(value: object) -> str:
    if value is None or pd.isna(value):
        return "UNKNOWN"
    token = str(value).strip()
    if not token:
        return "UNKNOWN"
    token = token.replace(_SEPARATOR, "_")
    return token
