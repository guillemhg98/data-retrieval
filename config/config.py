"""
Centralized configuration management.

This module loads configuration from environment variables and provides
defaults for both demand and diagnosis pipelines.
"""
import os
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass


class Config:
    """Base configuration class."""

    # Database Configuration
    DB_SERVER = os.getenv("DB_SERVER", "synw-aquas.sql.azuresynapse.net")
    DB_DATABASE = os.getenv("DB_DATABASE", "aquas")
    AUTH_MODE = os.getenv("AUTH_MODE", "ActiveDirectoryIntegrated")

    # Base Paths
    BASE_DIR = Path(os.getenv("BASE_DIR", Path.cwd()))
    DATA_DIR = BASE_DIR / "data"
    CONFIG_DIR = BASE_DIR / "config"
    SELECTIONS_DIR = BASE_DIR / "selections"
    SELECTED_RS_FILE = Path(
        os.getenv("SELECTED_RS_FILE", SELECTIONS_DIR / "selected_rs.csv")
    )
    SELECTED_UP_FILE = Path(
        os.getenv("SELECTED_UP_FILE", SELECTIONS_DIR / "selected_up.csv")
    )

    # Logging
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    @classmethod
    def resolve_up_rs_file(cls) -> Path:
        """Return the UP-RS Excel mapping path, with helpful fallbacks."""
        configured = Path(cls.UP_RS_FILE).expanduser()
        candidates = [configured]

        for base_dir in [Path(cls.BASE_DIR), Path.cwd()]:
            candidates.extend(
                [
                    base_dir / "UPperRS.xlsx",
                    base_dir / "UP per RS.xlsx",
                    base_dir / "UPperRS.example.xlsx",
                ]
            )

        seen = set()
        unique_candidates = []
        for path in candidates:
            try:
                key = path.resolve()
            except Exception:
                key = path.absolute()
            if key in seen:
                continue
            seen.add(key)
            unique_candidates.append(path)

        for path in unique_candidates:
            if path.exists():
                return path

        searched = "\n".join(f"  - {path}" for path in unique_candidates)
        raise FileNotFoundError(
            "UP-RS mapping Excel file not found. The pipeline needs this file "
            "to map UP codes to RS groups.\n"
            f"Searched:\n{searched}\n"
            "Fix it by restoring the tracked file with:\n"
            "  git restore UPperRS.xlsx\n"
            "or set UP_RS_FILE in .env to the real Excel path."
        )


class DemandConfig(Config):
    """Demand pipeline configuration."""

    SCHEMA = "z_inv"
    TABLE_NAME = "P1038_visites"
    DATE_COLUMN = "DATA_VISITA"

    # Data paths
    PIPELINE_DATA_DIR = Config.DATA_DIR / "demand_pipeline"
    STATE_FILE = PIPELINE_DATA_DIR / "state" / "state.json"
    SELECTED_RS_FILE = Config.SELECTED_RS_FILE
    SELECTED_UP_FILE = Config.SELECTED_UP_FILE

    OUTPUT_CAT_FILE = PIPELINE_DATA_DIR / "incremental" / "demanda_CAT_incremental.csv"
    OUTPUT_RS_FILE = PIPELINE_DATA_DIR / "incremental" / "demanda_RS_incremental.csv"
    OUTPUT_UP_FILE = PIPELINE_DATA_DIR / "incremental" / "demanda_UP_incremental.csv"

    FINAL_CAT_FILE = PIPELINE_DATA_DIR / "finals" / "demanda_CAT.csv"
    FINAL_RS_FILE = PIPELINE_DATA_DIR / "finals" / "demanda_RS.csv"
    FINAL_UP_FILE = PIPELINE_DATA_DIR / "finals" / "demanda_UP.csv"

    # Reference data
    UP_RS_FILE = Path(os.getenv("UP_RS_FILE", Config.BASE_DIR / "UPperRS.xlsx"))
    UP_RS_SHEET = "UP per RS"

    # Date settings
    MIN_VALID_DATE = "2008-01-01"
    FINAL_START_DATE = "2008-01-01"
    FINAL_END_DATE = None  # Will use today - 1 day at runtime

    @classmethod
    def get_final_end_date(cls):
        """Get final end date (today - 1 day)."""
        import pandas as pd
        return pd.Timestamp.today().normalize() - pd.Timedelta(days=1)


class DiagnosisConfig(Config):
    """Diagnosis pipeline configuration."""

    SCHEMA = "z_inv"
    TABLE_NAME = "P1038_prstb015r_filtrat"
    DATE_COLUMN = "data_visita"
    UP_COLUMN = "up_c"
    DIAG_CODE_COLUMN = "problema_salut_c"

    # Data paths
    PIPELINE_DATA_DIR = Config.DATA_DIR / "diagnosis_pipeline"
    STATE_FILE = PIPELINE_DATA_DIR / "state" / "state.json"
    SELECTED_CODES_FILE = Path(
        os.getenv(
            "SELECTED_DIAGNOSIS_CODES_FILE",
            os.getenv(
                "SELECTED_CODES_FILE",
                Config.SELECTIONS_DIR / "selected_diagnosis_codes.csv",
            ),
        )
    )
    SELECTED_RS_FILE = Config.SELECTED_RS_FILE
    SELECTED_UP_FILE = Config.SELECTED_UP_FILE

    OUTPUT_CAT_FILE = PIPELINE_DATA_DIR / "incremental" / "selected_CAT_incremental.csv"
    OUTPUT_RS_FILE = PIPELINE_DATA_DIR / "incremental" / "selected_RS_incremental.csv"
    OUTPUT_UP_FILE = PIPELINE_DATA_DIR / "incremental" / "selected_UP_incremental.csv"

    FINAL_CAT_FILE = PIPELINE_DATA_DIR / "finals" / "selected_CAT.csv"
    FINAL_RS_FILE = PIPELINE_DATA_DIR / "finals" / "selected_RS.csv"
    FINAL_UP_FILE = PIPELINE_DATA_DIR / "finals" / "selected_UP.csv"

    # Reference data
    UP_RS_FILE = Path(os.getenv("UP_RS_FILE", Config.BASE_DIR / "UPperRS.xlsx"))
    UP_RS_SHEET = "UP per RS"

    # Date settings
    MIN_VALID_DATE = "2008-01-01"
    FINAL_START_DATE = "2008-01-01"
    FINAL_END_DATE = None  # Will use today - 1 day at runtime

    @classmethod
    def get_final_end_date(cls):
        """Get final end date (today - 1 day)."""
        import pandas as pd
        return pd.Timestamp.today().normalize() - pd.Timedelta(days=1)


def get_config(pipeline: str = "demand") -> Config:
    """
    Get configuration for a specific pipeline.

    Args:
        pipeline: "demand" or "diagnosis"

    Returns:
        Config: Configuration object
    """
    if pipeline.lower() == "demand":
        return DemandConfig()
    elif pipeline.lower() == "diagnosis":
        return DiagnosisConfig()
    else:
        raise ValueError(f"Unknown pipeline: {pipeline}")
