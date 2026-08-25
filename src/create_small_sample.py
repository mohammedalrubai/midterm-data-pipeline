"""
create_small_sample.py - Generate a reproducible small sample from the huge CSV.
Uses reservoir sampling (single pass, streaming) - never loads full file into memory.
Sample size is configurable from settings or command line.
Manual Excel creation is forbidden per the assignment.
"""
import argparse
import csv
import os
import sys
import random

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import DATA_DIR, DEFAULT_SAMPLE_ROWS


def create_sample(input_path, output_path, rows, seed=42):
    """
    Create a reproducible random sample using reservoir sampling.
    Single-pass streaming - O(rows) memory, never loads full file.
    """
    random.seed(seed)
    reservoir = []
    total_rows = 0

    print(f"[Sample] Reading from: {input_path}")
    print(f"[Sample] Target rows: {rows:,}")

    with open(input_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)

        for i, row in enumerate(reader):
            total_rows = i + 1
            if i < rows:
                reservoir.append(row)
            else:
                # Reservoir sampling: replace with decreasing probability
                j = random.randint(0, i)
                if j < rows:
                    reservoir[j] = row

            if total_rows % 500000 == 0:
                print(f"  ... scanned {total_rows:,} rows")

    actual_rows = len(reservoir)
    print(f"[Sample] Total rows in source: {total_rows:,}")
    print(f"[Sample] Sample rows: {actual_rows:,}")

    # Write sample
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for row in reservoir:
            writer.writerow(row)

    size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"[Sample] Created: {output_path} ({size_mb:.2f} MB)")
    return output_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create a small reproducible sample from a large CSV"
    )
    parser.add_argument("--input", required=True, help="Path to input CSV file")
    parser.add_argument(
        "--rows", type=int, default=DEFAULT_SAMPLE_ROWS,
        help=f"Number of rows to sample (default: {DEFAULT_SAMPLE_ROWS})"
    )
    parser.add_argument("--output", default=None, help="Output file path")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")

    args = parser.parse_args()

    if args.output is None:
        base = os.path.splitext(os.path.basename(args.input))[0]
        args.output = os.path.join(DATA_DIR, f"{base}_sample.csv")

    create_sample(args.input, args.output, args.rows, args.seed)
