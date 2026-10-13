# Student Depression Analytics — Big Data Pipeline

A Big Data Analytics pipeline that streams a student mental-health survey through **Kafka**, lands it in **HDFS**, aggregates it with three **Hadoop MapReduce** jobs (Hadoop Streaming, written in Python), validates every result against an independent **pandas** computation, and visualizes it in a **Plotly Dash** dashboard.

```
CSV ──▶ Kafka Producer ──▶ Kafka Topic ──▶ HDFS-writer Consumer ──▶ HDFS (raw)
                                                                        │
                                                                        ▼
                                              3× Hadoop MapReduce jobs (map/combine/reduce)
                                                                        │
                                                                        ▼
                                                    HDFS (processed) ──▶ local CSVs
                                                                        │
                                              ┌─────────────────────────┴───────────────┐
                                              ▼                                          ▼
                                   pandas validation (cross-check)             Plotly Dash dashboard
```

## Why this project

Academic pressure and sleep are two of the most commonly cited factors in student mental health. This project takes a public Kaggle survey of ~28,000 students and asks, at scale: **how strongly does depression correlate with academic pressure and sleep duration**, and does it differ by age, gender, or CGPA? Rather than just running a single `pandas.groupby()`, the project treats the dataset as if it were arriving continuously (via Kafka) and processes it the way a real Hadoop cluster would — with mappers, combiners, and reducers — then proves the distributed computation is correct by comparing it against the trusted single-machine answer.

## Key results

| Academic pressure tier | Depression rate |
|---|---|
| 1 (lowest) | 19.4% |
| 2 | 37.5% |
| 3 | 60.1% |
| 4 | 76.1% |
| 5 (highest) | 86.1% |

| Sleep duration | Depression rate |
|---|---|
| Less than 5 hours | 64.5% |
| 5–6 hours | 56.9% |
| 7–8 hours | 59.5% |
| More than 8 hours | 50.9% |

**Caveat:** this is self-reported, cross-sectional survey data. These numbers show *association*, not causation — poor sleep could just as easily be a symptom of depression as a cause of it. The dashboard surfaces 95% Wilson-score confidence intervals on every bar so small, unreliable groups are visually obvious.

## Architecture

| Stage | Tool | What it does |
|---|---|---|
| Ingestion | **Kafka** (Docker) | `producer.py` streams the CSV into a 3-partition topic, batch by batch, to simulate a live feed |
| Landing | **HDFS** (native) | `consumer.py` buffers Kafka messages and flushes them to HDFS as CSVs, avoiding the "small files" problem, with at-least-once + idempotent-overwrite delivery |
| Processing | **Hadoop MapReduce** (Streaming, Python) | 3 jobs: a map-only filter/projection, a groupby-style aggregation (age × gender × CGPA), and a dual-tagged aggregation (pressure tier + sleep bucket) |
| Verification | **pandas** | Independently recomputes every aggregate from the raw CSV and diffs it against the MapReduce output |
| Presentation | **Plotly Dash** | Interactive dashboard with a gender-filterable heatmap and confidence-interval error bars |

### Why Kafka is in Docker but Hadoop runs natively

Apache/bde2020 only publish `amd64` Hadoop images. On Apple Silicon (and similarly on constrained Linux VMs), running the Hadoop JVMs under emulation is extremely slow — NameNode startup alone can take 10+ minutes. Running Hadoop natively via a direct install, with Kafka in Docker (which *is* genuinely multi-arch), gets daemon startup down to seconds.

## Project structure

```
.
├── dataset/                          Kaggle "Student Depression Dataset" (~28k rows)
├── eda/explore.py                    One-off exploratory pass — findings baked into the cleaning rules
├── producer/producer.py              CSV → Kafka
├── consumer/consumer.py              Kafka → HDFS (batched, at-least-once, idempotent)
├── mapreduce/
│   ├── common.py                     Shared cleaning/bucketing logic (used by mappers AND the validator)
│   ├── baseline/mapper.py            Map-only job: filter + column projection
│   ├── job1/                         Aggregation: avg sleep, % high pressure, depression rate by age/gender/CGPA
│   └── job2/                         Aggregation: depression rate by pressure tier and by sleep bucket
├── validation/validate.py            Pandas cross-check of every MapReduce output
├── dashboard/app.py                  Plotly Dash app (reads the final CSVs only — no live Hadoop/Kafka needed)
├── data/processed/                   Committed output CSVs — browse the results without running anything
├── hadoop-conf/                      Local Hadoop configuration (HDFS, YARN, MapReduce)
├── docker-compose.yml                Kafka (KRaft mode, no ZooKeeper)
├── run_pipeline.sh                   End-to-end orchestration script
└── requirements.txt
```

## MapReduce design notes

- **Mappers emit sums, never pre-computed averages.** A mean-of-means across unequal group sizes is mathematically wrong; sums are associative/commutative and can be combined in any order, so every division happens exactly once, in the reducer.
- **Combiners pre-aggregate on each mapper node** before the shuffle, cutting network traffic — safe here because the only operations are sum and count.
- **Job 2 emits tagged keys** (`PRESSURE|3`, `SLEEP|5-6 hours`) so a single pass over the data serves two independent aggregations.
- **The baseline job uses 0 reducers** — a pure row-wise filter and projection needs no shuffle.
- **Validation is structural, not cosmetic**: `validate.py` imports the *same* `common.py` cleaning functions the mappers use, then runs an independent pandas groupby and diffs every row against the MapReduce output. A clean run prints `ALL CHECKS PASSED`.

## Quick look (no setup required)

The final output CSVs are committed under `data/processed/`, so you can explore the results immediately:

```bash
pip install pandas plotly dash
python dashboard/app.py
# open http://localhost:8050
```

## Running the full pipeline

### Prerequisites

- Java 11 or 17
- Hadoop 3.x (HDFS + YARN), configured via `hadoop-conf/`
- Docker (for Kafka)
- Python 3.9+

### Setup

```bash
git clone https://github.com/<your-username>/<repo-name>.git
cd <repo-name>
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

> **Note:** `hadoop-conf/hdfs-site.xml` and `run_pipeline.sh` reference local filesystem paths and a specific Hadoop install location. Update these to match your own machine before the first run (see your platform's notes below).

Format HDFS once:

```bash
export HADOOP_CONF_DIR=$PWD/hadoop-conf
hdfs namenode -format
```

### Run

```bash
chmod +x run_pipeline.sh
./run_pipeline.sh                   # streams the real dataset once
./run_pipeline.sh --replicate 100   # replays the dataset 100x with synthetic ids, to force multiple HDFS blocks/mappers
```

This brings up Kafka and the Hadoop daemons, streams the CSV through Kafka into HDFS, runs the three MapReduce jobs, merges the results back to `data/processed/`, and validates them against pandas.

Flags: `--skip-infra`, `--skip-streaming`, `--skip-validate` let you re-run only part of the pipeline.

Then launch the dashboard:

```bash
.venv/bin/python dashboard/app.py
# open http://localhost:8050
```

Useful UIs while it runs: HDFS NameNode at `http://localhost:9870`, YARN ResourceManager at `http://localhost:8088`.

### Known gotcha: the raw HDFS landing zone is never auto-cleared

`run_pipeline.sh` clears each job's *output* directory before every run, but not the raw ingestion directory (`/data/raw/student_depression`). Running the pipeline more than once without reformatting HDFS will accumulate duplicate copies of the dataset there, and the MapReduce counts will come out as an exact multiple of the true row count (the rates stay correct since every group is inflated equally — this is the fastest way to spot the issue). If you see `validate.py` report `n_mapreduce` as a clean multiple of `n_pandas`, clear the raw directory and re-stream:

```bash
hdfs dfs -rm -r -f /data/raw/student_depression
hdfs dfs -mkdir -p /data/raw/student_depression
./run_pipeline.sh --skip-infra
```

### Stopping everything

```bash
hdfs --daemon stop namenode; hdfs --daemon stop datanode
yarn --daemon stop resourcemanager; yarn --daemon stop nodemanager
docker compose down
```

The dashboard reads only the final CSVs in `data/processed/`, so you can view it any time afterward without Hadoop or Kafka running.

## Tech stack

`Python` · `Apache Kafka` · `Apache Hadoop` (HDFS, YARN, MapReduce Streaming) · `pandas` · `Plotly Dash` · `Docker`

## Data source

[Student Depression Dataset](https://www.kaggle.com/datasets/hopesb/student-depression-dataset) (Kaggle), ~28,000 rows, self-reported survey data.
