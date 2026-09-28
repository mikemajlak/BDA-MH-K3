#!/usr/bin/env python3
"""
Job 1 combiner: partial-aggregates sufficient statistics before the shuffle,
to cut network traffic. Safe because sum/count are associative and
commutative -- unlike an average, partial sums can be merged in any order
and re-summed later without changing the result. Output format matches the
mapper's, so this same accumulation logic also works as the final reducer.
"""
import sys
from collections import defaultdict

sums = defaultdict(lambda: [0, 0.0, 0, 0, 0])  # count, sleep_sum, sleep_n, high, dep

for line in sys.stdin:
    key, value = line.rstrip("\n").split("\t", 1)
    count, sleep_sum, sleep_n, high, dep = value.split(",")
    acc = sums[key]
    acc[0] += int(count)
    acc[1] += float(sleep_sum)
    acc[2] += int(sleep_n)
    acc[3] += int(high)
    acc[4] += int(dep)

for key, (count, sleep_sum, sleep_n, high, dep) in sums.items():
    print(f"{key}\t{count},{sleep_sum},{sleep_n},{high},{dep}")
