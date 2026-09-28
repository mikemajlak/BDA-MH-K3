#!/usr/bin/env python3
"""
Plotly Dash dashboard for the MapReduce outputs. Reads the three processed
CSVs from data/processed/ (produced by `hdfs dfs -getmerge`, see
run_pipeline.sh) and renders:

  1. Sleep duration vs. depression rate (the required chart)
  2. Academic pressure tier vs. depression prevalence
  3. Age bracket x CGPA range heatmap, filterable by gender

Every rate carries a 95% Wilson score confidence interval as an error bar,
computed from (n, rate) recovered from the processed CSVs -- the naive
normal approximation (rate +/- 1.96*sqrt(p(1-p)/n)) breaks down for small n
or rates near 0/100, which happens in several of the smaller cells here.
"""
import math
from pathlib import Path

import dash
import pandas as pd
import plotly.graph_objects as go
from dash import dcc, html
from dash.dependencies import Input, Output

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "processed"

# --- palette (validated categorical/sequential slots, see dataviz skill) ---
BLUE = "#2a78d6"
SURFACE = "#fcfcfb"
PAGE_PLANE = "#f9f9f7"
INK_PRIMARY = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"
GRIDLINE = "#e1e0d9"
SEQUENTIAL_BLUE = ["#cde2fb", "#9ec5f4", "#5598e7", "#2a78d6", "#184f95", "#0d366b"]

SLEEP_ORDER = ["Less than 5 hours", "5-6 hours", "7-8 hours", "More than 8 hours"]


def wilson_ci(successes, n, z=1.96):
    """95% Wilson score interval for a binomial proportion (percent scale)."""
    if n == 0:
        return 0.0, 0.0
    phat = successes / n
    denom = 1 + z ** 2 / n
    center = phat + z ** 2 / (2 * n)
    margin = z * math.sqrt((phat * (1 - phat) + z ** 2 / (4 * n)) / n)
    lower = max(0.0, (center - margin) / denom)
    upper = min(1.0, (center + margin) / denom)
    return 100 * lower, 100 * upper


def load_data():
    job1 = pd.read_csv(DATA_DIR / "processed_student_health_summary.csv")
    pressure = pd.read_csv(DATA_DIR / "pressure_tier_prevalence.csv")
    sleep = pd.read_csv(DATA_DIR / "sleep_vs_depression.csv")
    return job1, pressure, sleep


def add_wilson(df, rate_col="depression_rate", n_col="n"):
    df = df.copy()
    successes = (df[rate_col] / 100.0 * df[n_col]).round()
    ci = successes.combine(df[n_col], lambda s, n: wilson_ci(s, n))
    df["ci_lower"] = [c[0] for c in ci]
    df["ci_upper"] = [c[1] for c in ci]
    df["err_minus"] = df[rate_col] - df["ci_lower"]
    df["err_plus"] = df["ci_upper"] - df[rate_col]
    return df


def make_bar_chart(df, x_col, title, x_title):
    df = add_wilson(df)
    fig = go.Figure(
        go.Bar(
            x=df[x_col],
            y=df["depression_rate"],
            marker_color=BLUE,
            error_y=dict(type="data", symmetric=False, array=df["err_plus"], arrayminus=df["err_minus"], color=INK_SECONDARY),
            customdata=df["n"],
            hovertemplate=f"%{{x}}<br>Depression rate: %{{y:.1f}}%<br>n=%{{customdata}}<extra></extra>",
        )
    )
    fig.update_layout(
        title=title,
        xaxis_title=x_title,
        yaxis_title="Depression rate (%)",
        plot_bgcolor=SURFACE,
        paper_bgcolor=SURFACE,
        font=dict(color=INK_PRIMARY, family="system-ui, -apple-system, 'Segoe UI', sans-serif"),
        yaxis=dict(range=[0, 100], gridcolor=GRIDLINE),
        xaxis=dict(gridcolor=GRIDLINE),
        margin=dict(t=60, b=50, l=60, r=30),
    )
    return fig


def make_heatmap(job1, gender):
    df = job1[job1["gender"] == gender]
    age_order = ["18-21", "22-25", "26-30", "31+"]
    cgpa_order = ["<6", "6-7", "7-8", "8-9", "9-10"]
    pivot = df.pivot(index="age_bracket", columns="cgpa_range", values="depression_rate")
    pivot = pivot.reindex(index=age_order, columns=cgpa_order)
    n_pivot = df.pivot(index="age_bracket", columns="cgpa_range", values="n").reindex(index=age_order, columns=cgpa_order)

    fig = go.Figure(
        go.Heatmap(
            z=pivot.values,
            x=cgpa_order,
            y=age_order,
            colorscale=[[i / (len(SEQUENTIAL_BLUE) - 1), c] for i, c in enumerate(SEQUENTIAL_BLUE)],
            zmin=0,
            zmax=100,
            customdata=n_pivot.values,
            hovertemplate="Age %{y}, CGPA %{x}<br>Depression rate: %{z:.1f}%<br>n=%{customdata}<extra></extra>",
            colorbar=dict(title="Depression<br>rate (%)"),
        )
    )
    fig.update_layout(
        title=f"Depression rate by age bracket x CGPA range ({gender})",
        xaxis_title="CGPA range",
        yaxis_title="Age bracket",
        plot_bgcolor=SURFACE,
        paper_bgcolor=SURFACE,
        font=dict(color=INK_PRIMARY, family="system-ui, -apple-system, 'Segoe UI', sans-serif"),
        margin=dict(t=60, b=50, l=70, r=30),
    )
    return fig


job1_df, pressure_df, sleep_df = load_data()
sleep_df["sleep_bucket"] = pd.Categorical(sleep_df["sleep_bucket"], categories=SLEEP_ORDER, ordered=True)
sleep_df = sleep_df.sort_values("sleep_bucket")

app = dash.Dash(__name__)
app.title = "Student Depression Analytics"

app.layout = html.Div(
    style={"backgroundColor": PAGE_PLANE, "fontFamily": "system-ui, -apple-system, 'Segoe UI', sans-serif", "padding": "24px"},
    children=[
        html.H1("Student Academic Pressure & Depression Analytics", style={"color": INK_PRIMARY}),
        html.Div(
            "Caveat: this dataset is self-reported and cross-sectional (a single point in time per "
            "student). The charts below show association, not causation -- e.g. poor sleep could be a "
            "symptom of depression rather than a cause. Error bars are 95% Wilson score confidence "
            "intervals; wide bars mean the group is small and the point estimate is unreliable.",
            style={
                "backgroundColor": SURFACE,
                "border": f"1px solid {GRIDLINE}",
                "borderRadius": "8px",
                "padding": "14px 18px",
                "color": INK_SECONDARY,
                "marginBottom": "24px",
                "maxWidth": "900px",
            },
        ),
        dcc.Graph(figure=make_bar_chart(sleep_df, "sleep_bucket", "Sleep duration vs. depression rate", "Sleep duration")),
        dcc.Graph(figure=make_bar_chart(pressure_df, "academic_pressure_tier", "Academic pressure tier vs. depression prevalence", "Academic pressure tier (1-5)")),
        html.Div(
            [
                html.Label("Filter heatmap by gender:", style={"color": INK_PRIMARY, "marginRight": "8px"}),
                dcc.Dropdown(
                    id="gender-filter",
                    options=[{"label": g, "value": g} for g in sorted(job1_df["gender"].unique())],
                    value=sorted(job1_df["gender"].unique())[0],
                    clearable=False,
                    style={"width": "220px"},
                ),
            ],
            style={"display": "flex", "alignItems": "center", "marginTop": "16px"},
        ),
        dcc.Graph(id="heatmap"),
    ],
)


@app.callback(Output("heatmap", "figure"), Input("gender-filter", "value"))
def update_heatmap(gender):
    return make_heatmap(job1_df, gender)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
