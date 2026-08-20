"""Create a multi-year sample dataset and materialize final Parquet/CSV outputs."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.sample_runner import (
    join_sample_outputs,
    run_sample_demand_pipeline,
    run_sample_diagnosis_pipeline,
)


DEFAULT_INPUT_DIR = PROJECT_ROOT / "data" / "sample" / "multiyear_input"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "sample" / "multiyear_output"


UP_RS_ROWS = [
    {"Codi UP": "00101", "RS": "RS_BARCELONA"},
    {"Codi UP": "00102", "RS": "RS_BARCELONA"},
    {"Codi UP": "00201", "RS": "RS_GIRONA"},
    {"Codi UP": "00301", "RS": "RS_LLEIDA"},
    {"Codi UP": "00401", "RS": "RS_TARRAGONA"},
]

DEMAND_LOCATIONS = ["C", "D", "H"]
DEMAND_SITUATIONS = ["PROGRAMADA", "URGENT", "NO_PROGRAMADA"]
DEMAND_SERVICES = ["MEDFAM", "INF", "PED", "URG"]
DEMAND_CLASSES = ["C9C", "D9D", "9T", "C9R", "CALTRE"]
DEMAND_TYPES = ["PRIMERA", "SEGUIMENT", "DOMICILI", "TELEFON", "URGENT"]
DIAG_CODES = ["J00", "I10", "A09", "E11", "F41"]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate multi-year sample input and final Parquet/CSV outputs."
    )
    parser.add_argument("--start", default="2008-01-01", help="First sample date")
    parser.add_argument("--end", default="2012-12-31", help="Last sample date")
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    input_dir = args.input_dir
    output_dir = args.output_dir
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    dates = pd.date_range(args.start, args.end, freq="D")
    if dates.empty:
        raise ValueError("The requested date range is empty")

    _write_reference_files(input_dir)
    _write_demand_visits(input_dir, dates)
    _write_diagnosis_visits(input_dir, dates)

    demand_path = run_sample_demand_pipeline(input_dir, output_dir)
    diagnosis_path = run_sample_diagnosis_pipeline(input_dir, output_dir)
    joined_path = join_sample_outputs(output_dir)

    csv_paths = [
        _write_csv_next_to_parquet(demand_path),
        _write_csv_next_to_parquet(diagnosis_path),
        _write_csv_next_to_parquet(joined_path),
    ]

    print(f"Generated {len(dates)} days from {dates.min().date()} to {dates.max().date()}")
    print(f"Sample input: {input_dir}")
    print(f"Demand final parquet: {demand_path}")
    print(f"Diagnosis final parquet: {diagnosis_path}")
    print(f"Joined final parquet: {joined_path}")
    for csv_path in csv_paths:
        print(f"CSV: {csv_path}")

    joined = pd.read_parquet(joined_path)
    print(f"Joined shape: {joined.shape[0]} rows x {joined.shape[1]} columns")
    print(joined.head(10).to_string(index=False))
    return 0


def _write_reference_files(input_dir: Path) -> None:
    pd.DataFrame(UP_RS_ROWS).to_csv(input_dir / "up_rs.csv", index=False)
    pd.DataFrame({"problema_salut_c": DIAG_CODES}).to_csv(
        input_dir / "selected_codes.csv",
        index=False,
    )


def _write_demand_visits(input_dir: Path, dates: pd.DatetimeIndex) -> None:
    rows = []
    ups = [row["Codi UP"] for row in UP_RS_ROWS]

    for day_index, date in enumerate(dates):
        visits_for_day = 2 + (day_index % 4)
        for visit_index in range(visits_for_day):
            cursor = day_index + visit_index
            rows.append(
                {
                    "DATA_VISITA": date.strftime("%Y-%m-%d"),
                    "UP": ups[cursor % len(ups)],
                    "VISI_LLOC_VISITA": DEMAND_LOCATIONS[cursor % len(DEMAND_LOCATIONS)],
                    "VISI_SITUACIO_VISITA": DEMAND_SITUATIONS[
                        cursor % len(DEMAND_SITUATIONS)
                    ],
                    "SERVEI_CODI": DEMAND_SERVICES[cursor % len(DEMAND_SERVICES)],
                    "TIPUS_CLASS": DEMAND_CLASSES[cursor % len(DEMAND_CLASSES)],
                    "VISI_TIPUS_VISITA": DEMAND_TYPES[cursor % len(DEMAND_TYPES)],
                }
            )

    pd.DataFrame(rows).to_csv(input_dir / "demand_visits.csv", index=False)


def _write_diagnosis_visits(input_dir: Path, dates: pd.DatetimeIndex) -> None:
    rows = []
    ups = [row["Codi UP"] for row in UP_RS_ROWS]

    for day_index, date in enumerate(dates):
        diagnoses_for_day = 1 + (day_index % 3)
        for diagnosis_index in range(diagnoses_for_day):
            cursor = day_index * 2 + diagnosis_index
            rows.append(
                {
                    "data_visita": date.strftime("%Y-%m-%d"),
                    "up_c": ups[cursor % len(ups)],
                    "problema_salut_c": DIAG_CODES[cursor % len(DIAG_CODES)],
                }
            )

    pd.DataFrame(rows).to_csv(input_dir / "diagnosis_visits.csv", index=False)


def _write_csv_next_to_parquet(parquet_path: Path) -> Path:
    csv_path = parquet_path.with_suffix(".csv")
    pd.read_parquet(parquet_path).to_csv(csv_path, index=False)
    return csv_path


if __name__ == "__main__":
    raise SystemExit(main())
