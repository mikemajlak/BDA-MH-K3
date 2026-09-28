#!/usr/bin/env python3
"""
Job 1 mapper: buckets each valid student row by (age_bracket, gender,
cgpa_range) and emits sufficient statistics -- never a pre-computed average
-- so the reducer/combiner can sum first and divide once at the end. A
mean-of-means over unequal group sizes would silently be wrong.

Output: key\tcount,sleep_sum,sleep_n,high_pressure_count,depression_count
  - sleep_n is tracked separately from count because 'Others' sleep rows
    are excluded from the sleep average but still count toward pressure/
    depression rates.
"""
import csv
import sys

sys.path.insert(0, ".")
from common import age_bracket, cgpa_range, sleep_hours, is_valid_student_row, HIGH_PRESSURE_THRESHOLD

reader = csv.reader(sys.stdin)
rejected = 0

for row in reader:
    if not row or row[0] == "id":
        continue
    try:
        (rid, gender, age, city, profession, academic_pressure, work_pressure,
         cgpa, study_satisfaction, job_satisfaction, sleep_duration, diet,
         degree, suicidal_thoughts, work_study_hours, financial_stress,
         family_history, depression, *_rest) = row
    except ValueError:
        rejected += 1
        continue

    if not is_valid_student_row(profession, academic_pressure, cgpa):
        rejected += 1
        continue

    key = f"{age_bracket(age)}|{gender}|{cgpa_range(cgpa)}"

    hours = sleep_hours(sleep_duration)
    sleep_sum = hours if hours is not None else 0.0
    sleep_n = 1 if hours is not None else 0

    is_high = 1 if float(academic_pressure) >= HIGH_PRESSURE_THRESHOLD else 0
    is_dep = 1 if depression.strip() == "1" else 0

    print(f"{key}\t1,{sleep_sum},{sleep_n},{is_high},{is_dep}")

if rejected:
    sys.stderr.write(f"[job1 mapper] rejected {rejected} invalid rows\n")
