"""Check local outputs and render PNGs without SQL access or changing inputs."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipelines.shared.quality_report import create_quality_report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=ROOT / "data")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "data/quality_report")
    parser.add_argument("--start-date", help="Inclusive plot/anomaly start; default last 365 days")
    parser.add_argument("--end-date", help="Inclusive plot/anomaly end; default last stored date")
    parser.add_argument("--strict", action="store_true", help="Also return exit 1 for REVIEW warnings")
    args = parser.parse_args()
    try:
        report = create_quality_report(args.data_root, args.output_dir, args.start_date, args.end_date)
    except (ValueError, OSError, ImportError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print((args.output_dir / "summary.txt").read_text(encoding="utf-8"))
    print(f"Informe: {args.output_dir.resolve()}")
    if report["status"] == "ERROR":
        return 2
    return 1 if args.strict and report["status"] == "REVIEW" else 0


if __name__ == "__main__":
    sys.exit(main())
