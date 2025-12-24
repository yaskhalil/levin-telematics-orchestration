# Levin Telematics Airflow Data Platform (Astro CLI + Airflow 3)

A production-style **Apache Airflow 3** project built with **Astronomer (Astro) CLI** that ingests **Levin vehicle telematics** data, enforces **data contracts**, performs **idempotent loading** into **Postgres**, generates **curated daily aggregates**, and supports **backfills** over historical date ranges using **dynamic task mapping**.

This repo is intentionally focused on **orchestration + reliability patterns** (backfills, validation, idempotency, artifacts), not on building an ML model.

---

## What this project does

### Data flow (high level)
1. **Extract** a partition of telematics data for a specific day (e.g., `date=YYYY-MM-DD`)
2. **Validate** the partition against a schema + constraints (data contract)
3. **Normalize** dataset-specific fields into a canonical schema
4. **Load raw** events into Postgres **idempotently** (safe re-runs)
5. **Transform** into curated **daily aggregates** per vehicle/day
6. **Publish artifacts** per run (human summary + machine metrics)

---

## Key features

- **Partitioned ingest** using `date=YYYY-MM-DD` folders
- **Data contract validation** (schema, types, ranges, null rules)
- **Idempotent raw loads** (dedupe/upsert keys so reruns don’t duplicate data)
- **Curated Postgres layer** (`agg_vehicle_day`) ready for analytics/UI
- **Backfill orchestration** with **dynamic task mapping** over a date range
- **Run artifacts** emitted on every run:
  - `include/artifacts/date=YYYY-MM-DD/summary.md`
  - `include/artifacts/date=YYYY-MM-DD/metrics.json`
- Optional (if enabled): **Streamlit data explorer** for curated tables + data quality

---

## Tech stack

- **Astronomer CLI (Astro)** for local Airflow runtime (Docker)
- **Apache Airflow 3** (TaskFlow + dynamic task mapping)
- **Postgres**
- **Python** (pandas optional)
- **Pandera** (or Great Expectations) for validation

---

## Repository structure

```
levin-telematics-airflow-platform/
  dags/
    levin_ingest_validate_publish.py
    levin_backfill_range.py

  include/
    sql/
      00_create_tables.sql
      10_daily_aggregate.sql
    validation/
      schema.py
    artifacts/
      .gitkeep

  data/
    sample/
      date=YYYY-MM-DD/
        levin.csv

  scripts/
    partition_raw.py          # optional: full dataset -> date partitions
    README_DATA.md            # optional: data notes / provenance

  tests/
    test_validation.py
    test_transforms.py
    test_dag_imports.py

  requirements.txt
  README.md
```

---

## DAGs

### 1) `levin_ingest_validate_publish`
**Purpose:** daily canonical pipeline

**Order of tasks**
1. `resolve_partition(ds)`  
2. `extract_csv(partition_path)`  
3. `validate_contract(records)`  
4. `normalize_records(valid_records)`  
5. `load_raw_idempotent(normalized_records)`  
6. `build_daily_aggregate(ds)`  
7. `publish_artifacts(ds, summaries...)`

**Outputs**
- Postgres:
  - `raw_telematics` populated (event-level)
  - `agg_vehicle_day` updated (curated daily rollups)
- Artifacts:
  - `include/artifacts/date={{ ds }}/summary.md`
  - `include/artifacts/date={{ ds }}/metrics.json`

---

### 2) `levin_backfill_range`
**Purpose:** backfill orchestration over historical partitions

**How it works**
- Accepts `startDate` and `endDate` through `dag_run.conf`
- Expands the date list
- Uses **dynamic task mapping** to run the same daily pipeline for each date

**Example conf**
```json
{
  "startDate": "2025-12-01",
  "endDate": "2025-12-14"
}
```

---

## Data format

### Demo mode (recommended)
This repo is designed to be runnable without external downloads by keeping a small sample slice in:

```
data/sample/date=YYYY-MM-DD/levin.csv
```

The pipeline reads partitions using the Airflow `ds` (execution date). For example:
- `ds=2025-12-01` → reads `data/sample/date=2025-12-01/levin.csv`

### Full dataset mode (optional)
If you want to ingest the entire Kaggle dataset, you can:
1. download it locally (outside the repo)
2. run `scripts/partition_raw.py` to produce:
   - `data/partitioned/date=YYYY-MM-DD/levin.csv`
3. update the config/env var to point Airflow at `data/partitioned/`

---

## Postgres tables

### `raw_telematics` (event-level)
Stores near-original events with a stable canonical layer.

Typical fields:
- `event_time` (timestamp)
- `vehicle_id` (text)
- `trip_id` (text, nullable)
- `speed` (numeric, nullable)
- `rpm` (numeric, nullable)
- `engine_temp` (numeric, nullable)
- `partition_date` (date)
- `payload` (jsonb) — all extra original fields stored losslessly

**Idempotency strategy**
- Unique key: `(vehicle_id, event_time, COALESCE(trip_id,''))`
- Loads use upsert / dedupe so reruns do not duplicate

---

### `agg_vehicle_day` (curated)
Daily aggregates per vehicle/day for analytics/UI.

Typical fields:
- `partition_date`, `vehicle_id`
- `num_events`
- `avg_speed`, `max_speed`
- `avg_rpm`, `max_rpm`
- `max_engine_temp`
- `trip_count` (if trip_id available)

---

## Data validation (the “contract”)

The validation step is intentionally strict enough to matter:

**Required minimum**
- timestamp column must exist + parse
- vehicle identifier must exist + non-null

**Range checks (examples)**
- `speed >= 0` and `speed <= 200` (if present)
- `rpm >= 0` and `rpm <= 10000` (if present)
- `engine_temp` within plausible bounds (if present)

**Outputs**
Validation produces counters used in artifacts:
- total rows
- valid rows
- invalid rows
- invalid breakdown per rule/constraint

---

## Run artifacts

Each partition run writes:
- `include/artifacts/date=YYYY-MM-DD/summary.md`
- `include/artifacts/date=YYYY-MM-DD/metrics.json`

**`summary.md` includes**
- partition date
- row counts + invalid rule breakdown
- load stats (inserted/updated/skipped)
- aggregate stats (# vehicles, # trips, etc.)

**`metrics.json` includes**
- validation counters
- optional durations per step
- optional aggregate metrics for dashboards

---

## Quickstart (local)

### Prerequisites
- Docker Desktop installed and running
- Astro CLI installed

### Start Airflow locally
```bash
astro dev start
```

### Open Airflow UI
Airflow UI runs locally once Astro starts (check your terminal output for the exact URL).

### Trigger the daily DAG
Trigger:
- `levin_ingest_validate_publish`

Use an execution date that matches an available sample partition in `data/sample/`.

### Trigger a backfill run
Trigger:
- `levin_backfill_range`
with `dag_run.conf`:
```json
{
  "startDate": "2025-12-01",
  "endDate": "2025-12-07"
}
```

---

## Configuration

This project supports a simple “data root” config so you can switch between sample and full partitions.

Common options:
- environment variable like `TELEMETRY_DATA_ROOT=data/sample`
- or Airflow Variable/Connection

Example roots:
- `data/sample`
- `data/partitioned`

---

## Testing (optional but recommended)

Run unit tests (validation/transforms):
```bash
pytest
```

You can also include a DAG import test to ensure the scheduler can parse DAGs:
```bash
pytest -q tests/test_dag_imports.py
```

---

## Why this repo exists (what it demonstrates)

This project is meant to demonstrate production patterns that employers care about:
- **Backfill-safe pipelines**
- **Data quality gates**
- **Idempotent re-runs**
- **Partition-aware design**
- **Artifacts + operational visibility**
- **Clean local dev experience** via Astro CLI

---

## License / data notes

- This repo includes only a small sample slice of the Levin telematics dataset for demo purposes.
- If you use the full Kaggle dataset, ensure you follow the dataset’s Kaggle licensing/terms.
