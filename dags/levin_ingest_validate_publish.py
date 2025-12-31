# Daily pipeline:
# 1) resolve partition date (ds or dag_run.conf override)
# 2) extract ds rows from one CSV
# 3) validate contract (pandas-only) + counters
# 4) normalize to canonical raw schema
# 5) load raw idempotently into Postgres
# 6) build daily aggregate (SQL file) into agg_vehicle_day
# 7) publish artifacts (summary.md + metrics.json)
#
# Env vars:
#   TELEMETRY_DATA_ROOT=data
#   TELEMETRY_SOURCE_FILE=v2.csv
#   PG_CONN_ID=postgres_default

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
import sys
from typing import Any, Dict, Optional

import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Ensure project root is importable so `include.*` modules can be imported reliably.
sys.path.append(str(Path(__file__).resolve().parents[1]))

from include.transformations import normalize_telematics_df
from include.validation.schema import validate_contract

DAG_ID = "levin_ingest_validate_publish"
PG_CONN_ID = os.getenv("PG_CONN_ID", "postgres_default")
DATA_ROOT = os.getenv("TELEMETRY_DATA_ROOT", "data")
SOURCE_FILE = os.getenv("TELEMETRY_SOURCE_FILE", "v2.csv")
SOURCE_CSV = str(Path(DATA_ROOT) / SOURCE_FILE)

SQL_CREATE = Path("include/sql/00_create_tables.sql")
SQL_AGG = Path("include/sql/10_daily_aggregate.sql")

INSERT_SQL = """
INSERT INTO raw_telematics (
  event_time, partition_date, vehicle_id, device_id, trip_id,
  acc_data, gps_speed, battery, coolant_temp_c, dtc_count, engine_load, intake_air_temp,
  imap, kpl, maf, rpm, speed, t_adv, t_pos,
  source_file, payload
  )
VALUES %s
ON CONFLICT ON CONSTRAINT raw_telematics_vehicle_time_tripkey_uniq DO NOTHING;
"""


def _read_sql(p: Path) -> str:
    if not p.exists():
        raise AirflowFailException(f"Missing SQL file: {p}")
    return p.read_text(encoding="utf-8")


def _run_sql_for_ds(hook: PostgresHook, sql_path: Path, ds: str) -> None:
    hook.run(_read_sql(sql_path).replace(":ds", f"'{ds}'"))


@dag(
    dag_id=DAG_ID,
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["levin", "telematics"],
)
def levin_pipeline():
    @task
    def create_tables() -> None:
        PostgresHook(PG_CONN_ID).run(_read_sql(SQL_CREATE))

    @task
    def resolve_partition() -> str:
        """
        Use Airflow logical date `ds` by default, but allow backfills to override with dag_run.conf['ds'].
        """
        ctx = get_current_context()
        dag_run = ctx.get("dag_run")
        conf = getattr(dag_run, "conf", None) or {}
        override = conf.get("ds")
        return str(override) if override else str(ctx["ds"])

    @task
    def extract_csv(run_date: str) -> Dict[str, Any]:
        src = Path(SOURCE_CSV)
        if not src.exists():
            raise AirflowFailException(f"CSV not found: {src}")

        df = pd.read_csv(src)

        total_rows = int(len(df))

        # filter to run_date using timestamp parse (contract validation will also enforce parsing)
        df["event_time"] = pd.to_datetime(df.get("timeStamp"), errors="coerce")
        df = df[df["event_time"].notna()]

        # Helpful context for picking a valid ds when a run finds 0 rows.
        data_min_date = df["event_time"].dt.date.min()
        data_max_date = df["event_time"].dt.date.max()

        df = df[df["event_time"].dt.date == pd.to_datetime(run_date).date()]

        filtered_rows = int(len(df))
        has_data_for_run_date = filtered_rows > 0
        # Persist extracted slice for later tasks (keeps XCom small)
        out_dir = Path("/tmp/levin_telematics") / f"date={run_date}"
        out_dir.mkdir(parents=True, exist_ok=True)
        extracted_path = out_dir / "extracted.csv"
        df.to_csv(extracted_path, index=False)

        return {
            "ds": run_date,
            "source_file": str(src),
            "total_rows": total_rows,
            "filtered_rows": filtered_rows,
            "has_data_for_run_date": has_data_for_run_date,
            "data_min_date": str(data_min_date) if data_min_date is not None else None,
            "data_max_date": str(data_max_date) if data_max_date is not None else None,
            "extracted_path": str(extracted_path),
        }

    @task
    def validate_step(extract_meta: Dict[str, Any]) -> Dict[str, Any]:
        ds = str(extract_meta["ds"])
        extracted_path = Path(str(extract_meta["extracted_path"]))
        df = pd.read_csv(extracted_path)
        valid_df, result = validate_contract(df)

        out_dir = Path(extracted_path).parent
        valid_path = out_dir / "valid.csv"
        valid_df.to_csv(valid_path, index=False)

        extract_meta["valid_rows"] = int(result.valid_rows)
        extract_meta["invalid_rows"] = int(result.invalid_rows)
        extract_meta["invalid_breakdown"] = result.invalid_breakdown
        extract_meta["valid_path"] = str(valid_path)
        return extract_meta

    @task
    def normalize_step(meta: Dict[str, Any]) -> Dict[str, Any]:
        ds = str(meta["ds"])
        df = pd.read_csv(meta["valid_path"])

        norm = normalize_telematics_df(df, ds=ds, source_file=str(meta.get("source_file")))

        out_dir = Path(meta["valid_path"]).parent
        normalized_path = out_dir / "normalized.csv"
        norm.to_csv(normalized_path, index=False)

        meta["normalized_path"] = str(normalized_path)
        meta["normalized_rows"] = int(len(norm))
        return meta

    @task
    def load_raw_idempotent(meta: Dict[str, Any]) -> Dict[str, Any]:
        normalized_path = Path(str(meta["normalized_path"]))
        df = pd.read_csv(normalized_path)

        if df.empty:
            meta["attempted_inserts"] = 0
            return meta

        # Build rows in the same order as INSERT_SQL columns
        def _col(name: str) -> pd.Series:
            return df[name] if name in df.columns else pd.Series([None] * len(df))

        event_time = pd.to_datetime(df["event_time"], errors="coerce").dt.to_pydatetime()
        vehicle_id = _col("vehicle_id").fillna(_col("device_id")).astype("string").astype(object)
        device_id = _col("device_id").astype("string").astype(object)
        trip_id_val = _col("trip_id").astype("string")
        trip_id_val = trip_id_val.where(trip_id_val.notna(), None).astype(object)
        rows = list(
            zip(
                event_time,
                _col("partition_date"),
                vehicle_id,
                device_id,
                trip_id_val,
                _col("acc_data"),
                _col("gps_speed"),
                _col("battery"),
                _col("coolant_temp_c"),
                _col("dtc_count"),
                _col("engine_load"),
                _col("intake_air_temp"),
                _col("imap"),
                _col("kpl"),
                _col("maf"),
                _col("rpm"),
                _col("speed"),
                _col("t_adv"),
                _col("t_pos"),
                _col("source_file"),
                _col("payload"),
            )
        )

        pg = PostgresHook(PG_CONN_ID)
        conn = pg.get_conn()
        conn.autocommit = False
        try:
            from psycopg2.extras import execute_values  # type: ignore

            with conn.cursor() as cur:
                execute_values(cur, INSERT_SQL, rows, page_size=5000)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        meta["attempted_inserts"] = int(len(rows))
        return meta

    @task
    def build_daily_aggregate(run_date: str) -> None:
        _run_sql_for_ds(PostgresHook(PG_CONN_ID), SQL_AGG, run_date)

    @task
    def publish_artifacts(meta: Dict[str, Any]) -> None:
        ds = str(meta["ds"])
        out_dir = Path("include/artifacts") / f"date={ds}"
        out_dir.mkdir(parents=True, exist_ok=True)

        metrics = {
            "ds": ds,
            "source_file": meta.get("source_file"),
            "total_rows": meta.get("total_rows"),
            "filtered_rows": meta.get("filtered_rows"),
            "has_data_for_run_date": meta.get("has_data_for_run_date"),
            "data_min_date": meta.get("data_min_date"),
            "data_max_date": meta.get("data_max_date"),
            "valid_rows": meta.get("valid_rows"),
            "invalid_rows": meta.get("invalid_rows"),
            "invalid_breakdown": meta.get("invalid_breakdown", {}),
            "attempted_inserts": meta.get("attempted_inserts"),
        }

        (out_dir / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")

        summary_lines = [
            f"# Levin run summary ({ds})",
            "",
            f"- total_rows: {metrics.get('total_rows')}",
            f"- filtered_rows: {metrics.get('filtered_rows')}",
            f"- has_data_for_run_date: {metrics.get('has_data_for_run_date')}",
            f"- data_min_date: {metrics.get('data_min_date')}",
            f"- data_max_date: {metrics.get('data_max_date')}",
            f"- valid_rows: {metrics.get('valid_rows')}",
            f"- invalid_rows: {metrics.get('invalid_rows')}",
            f"- attempted_inserts: {metrics.get('attempted_inserts')}",
            "",
            "## Invalid breakdown",
            json.dumps(metrics.get('invalid_breakdown', {}), indent=2),
            "",
        ]
        (out_dir / "summary.md").write_text("\n".join(summary_lines), encoding="utf-8")

    create = create_tables()
    run_date = resolve_partition()
    extracted = extract_csv(run_date)
    validated = validate_step(extracted)
    normalized = normalize_step(validated)
    loaded = load_raw_idempotent(normalized)
    aggregated = build_daily_aggregate(run_date)
    create >> run_date >> extracted >> validated >> normalized >> loaded >> aggregated >> publish_artifacts(loaded)


levin_pipeline()
