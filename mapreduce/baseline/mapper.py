#!/usr/bin/env python3
"""
Map-only job (reducers=0): projects the raw HDFS CSVs down to the 8-column
baseline_student_depression.csv schema. No shuffle/reduce needed for a pure
row-wise projection+filter, which is itself a useful MapReduce pattern to
demonstrate cheaply.

Schema: id,age,gender,cgpa,sleep_duration,academic_pressure,study_satisfaction,depression_status
"""
import csv
import sys

sys.path.insert(0, ".")  # common.py is shipped alongside via -files
from common import is_valid_student_row

reader = csv.reader(sys.stdin)
writer = csv.writer(sys.stdout)

rejected = 0
for row in reader:
    if not row or row[0] == "id":
        continue  # header line from a raw part-file
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

    writer.writerow([rid, age, gender, cgpa, sleep_duration,
                      academic_pressure, study_satisfaction, depression])

if rejected:
    sys.stderr.write(f"[baseline mapper] rejected {rejected} invalid rows\n")
