"""
Shared cleaning/bucketing logic used by every mapper and by the pandas
validation script, so the two can never silently disagree.
"""

SLEEP_MIDPOINT = {
    "Less than 5 hours": 4.5,
    "5-6 hours": 5.5,
    "7-8 hours": 7.5,
    "More than 8 hours": 8.5,
    # 'Others' has no numeric meaning -> caller drops it
}

HIGH_PRESSURE_THRESHOLD = 4.0  # Academic Pressure >= 4 (on a 1-5 scale) counts as "high"


def age_bracket(age):
    age = float(age)
    if age <= 21:
        return "18-21"
    if age <= 25:
        return "22-25"
    if age <= 30:
        return "26-30"
    return "31+"


def cgpa_range(cgpa):
    cgpa = float(cgpa)
    if cgpa < 6:
        return "<6"
    if cgpa < 7:
        return "6-7"
    if cgpa < 8:
        return "7-8"
    if cgpa < 9:
        return "8-9"
    return "9-10"


def sleep_hours(sleep_duration_text):
    """Returns the bucket midpoint, or None for 'Others'/unrecognized text."""
    return SLEEP_MIDPOINT.get(sleep_duration_text.strip())


def is_valid_student_row(profession, academic_pressure, cgpa):
    """
    Filters out the ~0.15% of rows that are not real student records:
    non-'Student' profession, or Academic Pressure / CGPA reported as 0
    (a sentinel for "not applicable" in this survey, not a real measurement).
    """
    try:
        ap = float(academic_pressure)
        cg = float(cgpa)
    except (TypeError, ValueError):
        return False
    if profession.strip() != "Student":
        return False
    if ap == 0 or cg == 0:
        return False
    return True
