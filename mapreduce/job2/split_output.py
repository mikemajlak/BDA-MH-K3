#!/usr/bin/env python3
"""
Splits the merged Job 2 output (tag,label,n,depression_rate) into the two
per-view CSVs the dashboard reads.

Usage: python split_output.py <merged_csv> <output_dir>
"""
import csv
import sys
from pathlib import Path

merged_path, out_dir = sys.argv[1], Path(sys.argv[2])
out_dir.mkdir(parents=True, exist_ok=True)

pressure_rows, sleep_rows = [], []

with open(merged_path, newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row["tag"] == "PRESSURE":
            pressure_rows.append(row)
        elif row["tag"] == "SLEEP":
            sleep_rows.append(row)

with open(out_dir / "pressure_tier_prevalence.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["academic_pressure_tier", "n", "depression_rate"])
    for row in sorted(pressure_rows, key=lambda r: int(r["label"])):
        writer.writerow([row["label"], row["n"], row["depression_rate"]])

with open(out_dir / "sleep_vs_depression.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["sleep_bucket", "n", "depression_rate"])
    order = ["Less than 5 hours", "5-6 hours", "7-8 hours", "More than 8 hours"]
    for label in order:
        match = next((r for r in sleep_rows if r["label"] == label), None)
        if match:
            writer.writerow([label, match["n"], match["depression_rate"]])

print(f"wrote {out_dir / 'pressure_tier_prevalence.csv'} and {out_dir / 'sleep_vs_depression.csv'}")
