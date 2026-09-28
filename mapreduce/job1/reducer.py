#!/usr/bin/env python3
"""
Job 1 reducer: finishes the aggregation started by the mapper/combiner and
divides sums into rates exactly once, here, at the end -- never earlier.
Emits processed_student_health_summary.csv (with one extra debug column,
`n`, appended: a rate computed from a handful of students, e.g. the 31+ /
CGPA<6 bucket, is not meaningful, so downstream consumers should treat rows
with a small n with caution rather than trusting the percentage at face value).
"""
import sys
from collections import defaultdict

sums = defaultdict(lambda: [0, 0.0, 0, 0, 0])

for line in sys.stdin:
    key, value = line.rstrip("\n").split("\t", 1)
    count, sleep_sum, sleep_n, high, dep = value.split(",")
    acc = sums[key]
    acc[0] += int(count)
    acc[1] += float(sleep_sum)
    acc[2] += int(sleep_n)
    acc[3] += int(high)
    acc[4] += int(dep)

print("age_bracket,gender,cgpa_range,avg_sleep_hours,high_academic_pressure_pct,depression_rate,n")
for key in sorted(sums.keys()):
    count, sleep_sum, sleep_n, high, dep = sums[key]
    age_bracket, gender, cgpa_range = key.split("|")
    avg_sleep = round(sleep_sum / sleep_n, 2) if sleep_n else ""
    high_pct = round(100.0 * high / count, 2)
    dep_rate = round(100.0 * dep / count, 2)
    print(f"{age_bracket},{gender},{cgpa_range},{avg_sleep},{high_pct},{dep_rate},{count}")
