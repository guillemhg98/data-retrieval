import pandas as pd


def prepare_visits_chunk(df: pd.DataFrame, up_rs: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["DATA_VISITA"] = pd.to_datetime(out["DATA_VISITA"], errors="coerce").dt.floor("D")
    out = out.dropna(subset=["DATA_VISITA"])

    # cada fila = una visita
    out["counts"] = 1
    out["UP"] = out["UP"].astype(str).str.zfill(5)

    # Lookup UP -> RS
    lookup = up_rs.copy()
    lookup["Codi UP"] = lookup["Codi UP"].astype(str).str.zfill(5)

    out = out.merge(
        lookup[["Codi UP", "RS"]],
        left_on="UP",
        right_on="Codi UP",
        how="left",
    ).drop(columns=["Codi UP"])

    out["RS"] = (
        out["RS"]
        .fillna("RS:SENSEESPECIFICAR_SE")
        .replace("SENSEESPECIFICAR_SE", "RS:SENSEESPECIFICAR_SE")
    )

    if "TIPUS_CLASS" in out.columns:
        out["TIPUS_CLASS"] = (
            out["TIPUS_CLASS"]
            .fillna("NA")
            .astype(str)
            .str.strip()
            .replace("", "NA")
            .str.upper()
        )
    else:
        out["TIPUS_CLASS"] = "NA"

    out["TIPUS_VISITA_AGRUPAT"] = "NA"
    out.loc[
        out["TIPUS_CLASS"].isin(["C9C", "C9R", "CALTRE"]),
        "TIPUS_VISITA_AGRUPAT"
    ] = "PRESENCIAL"

    out.loc[
        out["TIPUS_CLASS"].isin(["D9D", "DALTRE"]),
        "TIPUS_VISITA_AGRUPAT"
    ] = "DOMICILIARIA"

    out.loc[
        out["TIPUS_CLASS"].isin(["9T"]),
        "TIPUS_VISITA_AGRUPAT"
    ] = "TELEFONICA"

    for col in [
        "VISI_LLOC_VISITA",
        "VISI_SITUACIO_VISITA",
        "SERVEI_CODI",
        "TIPUS_CLASS",
        "TIPUS_VISITA_AGRUPAT",
        "UP",
        "RS",
    ]:
        if col in out.columns:
            out[col] = out[col].astype(str).fillna("NA").str.strip()

    return out
