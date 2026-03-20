#!/usr/bin/env python3
"""
Run the epaCC processing pipeline on a single file or a folder of CSV files.

Usage:
    python run.py <file_or_folder> [options]

Examples:
    python run.py "Endtestdaten_ohne_Fehler_ einheitliche ID/epaAC-Data-1.csv"
    python run.py "Endtestdaten_ohne_Fehler_ einheitliche ID/"
    python run.py data/ --db output/custom.db --glob "*.csv"
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def print_result(result: dict) -> None:
    src = result.get("source_file", "?")
    if "error" in result:
        print(f"  ERROR: {result['error']}")
        return

    # Support both old format (routing_results: list) and new format (routing_result: dict)
    routing_results = result.get("routing_results")
    if routing_results is None:
        rr = result.get("routing_result")
        routing_results = [rr] if rr else []

    total_inserts = sum(r.get("inserts", 0) for r in routing_results)
    total_updates = sum(r.get("updates", 0) for r in routing_results)
    total_errors  = sum(r.get("errors",  0) for r in routing_results)

    for r in routing_results:
        print(f"  table  : {r['table']} — "
              f"{r['inserts']} inserts, {r['updates']} updates, {r['errors']} errors")
    print(f"  total  : {total_inserts} inserts, {total_updates} updates, {total_errors} errors")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the epaCC import pipeline on a file or folder."
    )
    parser.add_argument(
        "input",
        help="Path to a CSV file or a directory containing CSV files.",
    )
    parser.add_argument(
        "--db",
        default="output/health_data.db",
        help="Path to the SQLite output database (default: output/health_data.db).",
    )
    parser.add_argument(
        "--logs",
        default="logs",
        help="Directory for run logs (default: logs).",
    )
    parser.add_argument(
        "--glob",
        default="*.csv",
        help="Glob pattern when input is a directory (default: *.csv).",
    )
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Stop batch processing on the first error.",
    )
    parser.add_argument(
        "--json",
        dest="output_json",
        action="store_true",
        help="Print full result as JSON instead of a human-readable summary.",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Error: '{input_path}' does not exist.", file=sys.stderr)
        sys.exit(1)

    from src.pipeline.orchestrator import Pipeline

    pipeline = Pipeline(db_path=args.db, log_dir=args.logs)

    if input_path.is_file():
        print(f"Processing file: {input_path}")
        result = pipeline.run(input_path)

        if args.output_json:
            print(json.dumps(result, indent=2, default=str))
        else:
            print_result(result)

    else:
        files = sorted(input_path.glob(args.glob))
        if not files:
            print(f"No files matching '{args.glob}' found in '{input_path}'.", file=sys.stderr)
            sys.exit(1)

        print(f"Processing {len(files)} file(s) in '{input_path}' (pattern: {args.glob})")
        results = pipeline.run_all(input_path, glob=args.glob, stop_on_error=args.stop_on_error)

        if args.output_json:
            print(json.dumps(results, indent=2, default=str))
        else:
            for result in results:
                src = result.get("source_file", "?")
                print(f"\n[{Path(src).name}]")
                print_result(result)

        succeeded = sum(1 for r in results if "error" not in r)
        failed    = len(results) - succeeded
        print(f"\nDone: {succeeded} succeeded, {failed} failed.")


if __name__ == "__main__":
    main()
