#!/usr/bin/env python3
"""Job 2 combiner: partial sum(count, depression_count) per tagged key."""
import sys
from collections import defaultdict

sums = defaultdict(lambda: [0, 0])

for line in sys.stdin:
    key, value = line.rstrip("\n").split("\t", 1)
    count, dep = value.split(",")
    acc = sums[key]
    acc[0] += int(count)
    acc[1] += int(dep)

for key, (count, dep) in sums.items():
    print(f"{key}\t{count},{dep}")
