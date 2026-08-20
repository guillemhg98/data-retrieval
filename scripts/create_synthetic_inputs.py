"""Create synthetic input CSVs with the schema expected by --sample mode."""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "synthetic" / "input"

UP_RS_ROWS = [
    {"Codi UP": "00348", "RS": "CATALUNYA CENTRAL"},
    {"Codi UP": "00443", "RS": "BARCELONA CIUTAT"},
    {"Codi UP": "00065", "RS": "CAMP DE TARRAGONA"},
    {"Codi UP": "04903", "RS": "CAMP DE TARRAGONA"},
    {"Codi UP": "00130", "RS": "GIRONA"},
    {"Codi UP": "04939", "RS": "GIRONA"},
    {"Codi UP": "00005", "RS": "ALT PIRINEU i ARAN"},
    {"Codi UP": "00089", "RS": "TERRES DE L'EBRE"},
    {"Codi UP": "08368", "RS": "TERRES DE L'EBRE"},
    {"Codi UP": "04991", "RS": "LLEIDA"},
    {"Codi UP": "06311", "RS": "BARCELONA CIUTAT"},
]

DEMAND_LOCATIONS = ["C", "D", "H"]
DEMAND_SITUATIONS = ["PROGRAMADA", "URGENT", "NO_PROGRAMADA"]
DEMAND_SERVICES = ["MEDFAM", "INF", "PED", "URG"]
DEMAND_CLASSES = ["C9C", "D9D", "9T", "C9R", "CALTRE", "DALTRE"]
DEMAND_TYPES = ["PRIMERA", "SEGUIMENT", "DOMICILI", "TELEFON", "URGENT"]
DIAG_CODES = ["J00", "J06", "J10", "J21", "A09", "M54", "F41", "E11", "U07.1", "I10", "K52", "M25", "R05", "R50", "F32", "S01"]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate synthetic CSV inputs compatible with run_pipeline.py --sample."
    )
    parser.add_argument("--start", default="2026-01-01", help="First date, YYYY-MM-DD")
    parser.add_argument("--end", default="2026-01-31", help="Last date, YYYY-MM-DD")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where synthetic input CSVs will be written",
    )
    args = parser.parse_args()

    dates = pd.date_range(args.start, args.end, freq="D")
    if dates.empty:
        raise ValueError("The requested date range is empty")

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    _write_up_rs(output_dir)
    _write_selected_codes(output_dir)
    _write_demand_visits(output_dir, dates)
    _write_diagnosis_visits(output_dir, dates)

    print(f"Synthetic input written to: {output_dir}")
    print(f"Dates: {dates.min().date()} -> {dates.max().date()}")
    print("Run with:")
    print(
        "  python run_pipeline.py --sample --all "
        f"--sample-input-dir {output_dir} --sample-output-dir data/synthetic/output"
    )
    return 0


def _write_up_rs(output_dir: Path) -> None:
    pd.DataFrame(UP_RS_ROWS).to_csv(output_dir / "up_rs.csv", index=False)


def _write_selected_codes(output_dir: Path) -> None:
    pd.DataFrame({"problema_salut_c": DIAG_CODES}).to_csv(
        output_dir / "selected_codes.csv",
        index=False,
    )


def _write_demand_visits(output_dir: Path, dates: pd.DatetimeIndex) -> None:
    rows = []
    ups = [row["Codi UP"] for row in UP_RS_ROWS]

    for day_index, day in enumerate(dates):
        visits_for_day = 2 + (day_index % 5)
        for visit_index in range(visits_for_day):
            cursor = day_index + visit_index
            rows.append(
                {
                    "DATA_VISITA": day.strftime("%Y-%m-%d"),
                    "UP": ups[cursor % len(ups)],
                    "VISI_LLOC_VISITA": DEMAND_LOCATIONS[cursor % len(DEMAND_LOCATIONS)],
                    "VISI_SITUACIO_VISITA": DEMAND_SITUATIONS[cursor % len(DEMAND_SITUATIONS)],
                    "SERVEI_CODI": DEMAND_SERVICES[cursor % len(DEMAND_SERVICES)],
                    "TIPUS_CLASS": DEMAND_CLASSES[cursor % len(DEMAND_CLASSES)],
                    "VISI_TIPUS_VISITA": DEMAND_TYPES[cursor % len(DEMAND_TYPES)],
                }
            )

    pd.DataFrame(rows).to_csv(output_dir / "demand_visits.csv", index=False)


def _write_diagnosis_visits(output_dir: Path, dates: pd.DatetimeIndex) -> None:
    rows = []
    ups = [row["Codi UP"] for row in UP_RS_ROWS]

    for day_index, day in enumerate(dates):
        diagnoses_for_day = 1 + (day_index % 4)
        for diagnosis_index in range(diagnoses_for_day):
            cursor = day_index * 2 + diagnosis_index
            rows.append(
                {
                    "data_visita": day.strftime("%Y-%m-%d"),
                    "up_c": ups[cursor % len(ups)],
                    "problema_salut_c": DIAG_CODES[cursor % len(DIAG_CODES)],
                }
            )

    pd.DataFrame(rows).to_csv(output_dir / "diagnosis_visits.csv", index=False)


if __name__ == "__main__":
    raise SystemExit(main())
