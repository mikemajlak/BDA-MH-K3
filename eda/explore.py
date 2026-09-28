"""
Exploratory pass over the raw Kaggle CSV. Run once to sanity-check assumptions
baked into mapreduce/*/mapper.py before the pipeline is trusted.

Findings (2026-09-28, 27,901 rows):
  - Sleep Duration is categorical text, not numeric: 'Less than 5 hours',
    '5-6 hours', '7-8 hours', 'More than 8 hours', 'Others' (18 rows).
    No 6-7 hour bucket exists in the survey. Mapped to bucket midpoints in
    mapreduce/common.py; 'Others' rows are dropped from sleep aggregates.
  - 43 rows (0.15%) are not valid student records: 31 have Profession !=
    'Student', 9 have Academic Pressure == 0, 9 have CGPA == 0 (a handful
    overlap). These are filtered out by mapper.py and counted as rejects.
  - Financial Stress has 3 nulls; unused by this pipeline's schema, so
    ignored.
  - No duplicate ids. Depression is already binary (0/1).
"""
import pandas as pd

df = pd.read_csv("dataset/Student Depression Dataset.csv")

not_student = df["Profession"] != "Student"
zero_ap = df["Academic Pressure"] == 0
zero_cgpa = df["CGPA"] == 0
bad = not_student | zero_ap | zero_cgpa

print(f"rows: {len(df)}")
print(f"rejected (not-student / zero pressure / zero cgpa): {bad.sum()}")
print(f"valid rows after filter: {(~bad).sum()}")
print("\nSleep Duration:\n", df["Sleep Duration"].value_counts())
print("\nAcademic Pressure:\n", df["Academic Pressure"].value_counts().sort_index())
print("\nDepression rate (raw, unfiltered):", df["Depression"].mean())
