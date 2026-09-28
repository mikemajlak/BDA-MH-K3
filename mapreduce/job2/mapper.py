#!/usr/bin/env python3
"""
Job 2 mapper: emits two tagged views from one pass over the data, so a
single MapReduce job serves both the "depression by academic pressure tier"
chart and the "depression by sleep bucket" chart.

Output: TAG|label\t1,is_depressed
  TAG is PRESSURE (label = integer pressure tier 1-5) or
       SLEEP (label = original sleep-duration bucket text).
Sleep rows with 'Others' are skipped -- no numeric/ordinal meaning.
"""
import csv
import sys

sys.path.insert(0, ".")
from common import is_valid_student_row, SLEEP_MIDPOINT

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

    is_dep = 1 if depression.strip() == "1" else 0

    pressure_tier = int(float(academic_pressure))
    print(f"PRESSURE|{pressure_tier}\t1,{is_dep}")

    sleep_bucket = sleep_duration.strip()
    if sleep_bucket in SLEEP_MIDPOINT:
        print(f"SLEEP|{sleep_bucket}\t1,{is_dep}")

if rejected:
    sys.stderr.write(f"[job2 mapper] rejected {rejected} invalid rows\n")
