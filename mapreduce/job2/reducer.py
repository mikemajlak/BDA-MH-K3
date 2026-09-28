#!/usr/bin/env python3
"""
Job 2 reducer: finishes the sum and computes depression_rate once. Emits a
single tagged CSV (tag,label,n,depression_rate); split_output.py splits it
into pressure_tier_prevalence.csv and sleep_vs_depression.csv.
"""
import sys
from collections import defaultdict

sums = defaultdict(lambda: [0, 0])

for line in sys.stdin:
    key, value = line.rstrip("\n").split("\t", 1)
    count, dep = value.split(",")
    acc = sums[key]
    acc[0] += int(count)
    acc[1] += int(dep)

print("tag,label,n,depression_rate")
for key in sorted(sums.keys()):
    tag, label = key.split("|", 1)
    count, dep = sums[key]
    rate = round(100.0 * dep / count, 2) if count else ""
    print(f"{tag},{label},{count},{rate}")
