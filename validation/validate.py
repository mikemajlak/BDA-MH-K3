#!/usr/bin/env python3
"""
Correctness proof: recomputes every aggregate with a pandas groupby,
independently of the MapReduce mapper/reducer code path, and diffs the two.
They should match exactly (rounding aside) -- if they don't, one of the two
implementations has a bug.

Usage:
    python validation/validate.py \
        --raw "dataset/Student Depression Dataset.csv" \
        --job1 data/processed/processed_student_health_summary.csv \
        --pressure data/processed/pressure_tier_prevalence.csv \
        --sleep data/processed/sleep_vs_depression.csv
"""
import argparse
import sys

import pandas as pd

sys.path.insert(0, "mapreduce")
from common import age_bracket, cgpa_range, sleep_hours, is_valid_student_row, HIGH_PRESSURE_THRESHOLD


def load_and_clean(raw_path):
    df = pd.read_csv(raw_path)
    valid = df.apply(
        lambda r: is_valid_student_row(str(r["Profession"]), r["Academic Pressure"], r["CGPA"]),
        axis=1,
    )
    print(f"[validate] {len(df)} raw rows, {(~valid).sum()} rejected, {valid.sum()} valid")
    df = df[valid].copy()
    df["age_bracket"] = df["Age"].apply(age_bracket)
    df["cgpa_range"] = df["CGPA"].apply(cgpa_range)
    df["sleep_hours"] = df["Sleep Duration"].apply(sleep_hours)
    df["is_high_pressure"] = (df["Academic Pressure"] >= HIGH_PRESSURE_THRESHOLD).astype(int)
    return df


def validate_job1(df, mr_path):
    expected = (
        df.groupby(["age_bracket", "Gender", "cgpa_range"])
        .agg(
            avg_sleep_hours=("sleep_hours", "mean"),
            high_academic_pressure_pct=("is_high_pressure", lambda s: round(100 * s.mean(), 2)),
            depression_rate=("Depression", lambda s: round(100 * s.mean(), 2)),
            n=("Depression", "count"),
        )
        .reset_index()
        .rename(columns={"Gender": "gender"})
    )
    expected["avg_sleep_hours"] = expected["avg_sleep_hours"].round(2)
    expected = expected.sort_values(["age_bracket", "gender", "cgpa_range"]).reset_index(drop=True)

    actual = pd.read_csv(mr_path).sort_values(["age_bracket", "gender", "cgpa_range"]).reset_index(drop=True)

    merged = expected.merge(actual, on=["age_bracket", "gender", "cgpa_range"], suffixes=("_pandas", "_mapreduce"))
    mismatches = merged[
        (merged["avg_sleep_hours_pandas"] != merged["avg_sleep_hours_mapreduce"])
        | (merged["high_academic_pressure_pct_pandas"] != merged["high_academic_pressure_pct_mapreduce"])
        | (merged["depression_rate_pandas"] != merged["depression_rate_mapreduce"])
        | (merged["n_pandas"] != merged["n_mapreduce"])
    ]
    print(f"[validate] job1: {len(expected)} pandas groups, {len(actual)} mapreduce groups, {len(mismatches)} mismatches")
    if len(mismatches):
        print(mismatches)
    return len(mismatches) == 0 and len(expected) == len(actual)


def validate_pressure(df, mr_path):
    expected = (
        df.groupby(df["Academic Pressure"].astype(int))["Depression"]
        .agg(n="count", depression_rate=lambda s: round(100 * s.mean(), 2))
        .reset_index()
        .rename(columns={"Academic Pressure": "academic_pressure_tier"})
        .sort_values("academic_pressure_tier")
        .reset_index(drop=True)
    )
    actual = pd.read_csv(mr_path).sort_values("academic_pressure_tier").reset_index(drop=True)
    merged = expected.merge(actual, on="academic_pressure_tier", suffixes=("_pandas", "_mapreduce"))
    mismatches = merged[
        (merged["n_pandas"] != merged["n_mapreduce"])
        | (merged["depression_rate_pandas"] != merged["depression_rate_mapreduce"])
    ]
    print(f"[validate] pressure tiers: {len(mismatches)} mismatches")
    if len(mismatches):
        print(mismatches)
    return len(mismatches) == 0


def validate_sleep(df, mr_path):
    sleep_df = df.dropna(subset=["sleep_hours"])
    expected = (
        sleep_df.groupby("Sleep Duration")["Depression"]
        .agg(n="count", depression_rate=lambda s: round(100 * s.mean(), 2))
        .reset_index()
        .rename(columns={"Sleep Duration": "sleep_bucket"})
        .sort_values("sleep_bucket")
        .reset_index(drop=True)
    )
    actual = pd.read_csv(mr_path).sort_values("sleep_bucket").reset_index(drop=True)
    merged = expected.merge(actual, on="sleep_bucket", suffixes=("_pandas", "_mapreduce"))
    mismatches = merged[
        (merged["n_pandas"] != merged["n_mapreduce"])
        | (merged["depression_rate_pandas"] != merged["depression_rate_mapreduce"])
    ]
    print(f"[validate] sleep buckets: {len(mismatches)} mismatches")
    if len(mismatches):
        print(mismatches)
    return len(mismatches) == 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default="dataset/Student Depression Dataset.csv")
    ap.add_argument("--job1", default="data/processed/processed_student_health_summary.csv")
    ap.add_argument("--pressure", default="data/processed/pressure_tier_prevalence.csv")
    ap.add_argument("--sleep", default="data/processed/sleep_vs_depression.csv")
    args = ap.parse_args()

    df = load_and_clean(args.raw)
    ok1 = validate_job1(df, args.job1)
    ok2 = validate_pressure(df, args.pressure)
    ok3 = validate_sleep(df, args.sleep)

    if ok1 and ok2 and ok3:
        print("[validate] ALL CHECKS PASSED -- MapReduce output matches pandas groupby exactly")
        sys.exit(0)
    else:
        print("[validate] VALIDATION FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()
