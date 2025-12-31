## Next steps and How to use(execution checklist)

This file is the operational checklist for finishing and validating the README “final product”.

### Acceptance criteria (README-aligned)
- Airflow DAG `levin_ingest_validate_publish` runs end-to-end for a chosen `ds`.
- Postgres tables:
  - `raw_telematics` contains rows for `partition_date = ds`
  - `agg_vehicle_day` contains rows for `partition_date = ds`
- Artifacts are written:
  - `include/artifacts/date=YYYY-MM-DD/summary.md`
  - `include/artifacts/date=YYYY-MM-DD/metrics.json`
- Backfill DAG `levin_backfill_range` triggers daily runs over a date range (dynamic mapping).
- `pytest` passes (validation, transformations, DAG import tests).
- **NOTE** when running manuel add {"ds" : "2018-1-1"} to allow it to run properly

---

## Run locally (Astro)

### 1) Start Airflow
```bash
astro dev start
```

### 2) Trigger a daily run
Trigger `levin_ingest_validate_publish` for a `ds` that exists in `data/v2.csv`.

Notes:
- The DAG supports a backfill override via `dag_run.conf` key `ds` (format `YYYY-MM-DD`).

### 3) Verify Postgres tables
Run in Postgres:
```sql
SELECT COUNT(*) FROM raw_telematics WHERE partition_date = DATE 'YYYY-MM-DD';
SELECT COUNT(*) FROM agg_vehicle_day WHERE partition_date = DATE 'YYYY-MM-DD';
```

### 4) Verify artifacts
Check:
- `include/artifacts/date=YYYY-MM-DD/summary.md`
- `include/artifacts/date=YYYY-MM-DD/metrics.json`

---

## Backfill run (range)

Trigger `levin_backfill_range` with:
```json
{
  "startDate": "2025-12-01",
  "endDate": "2025-12-07"
}
```

This DAG uses dynamic task mapping to trigger the daily pipeline once per day in the range.

---

## Tests
```bash
pytest
```

---

## Streamlit explorer (curated tables + data quality)

1) Ensure Postgres is running and the daily DAG has populated `raw_telematics` and `agg_vehicle_day`.

2) Run:
```bash
streamlit run streamlit_app.py
```

3) Set connection env vars if needed:
- `PGHOST` (default: `localhost`)
- `PGPORT` (default: `5432`)
- `PGDATABASE` (default: `postgres`)
- `PGUSER` (default: `postgres`)
- `PGPASSWORD` (default: `postgres`)


