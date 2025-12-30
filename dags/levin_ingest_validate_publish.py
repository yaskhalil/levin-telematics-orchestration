# 1) create tables (SQL file)
# 2) ingest ds rows from one CSV into Postgres (skip duplicates)
# 3) build daily aggregate (SQL file)
#
# Env vars:
#   TELEMETRY_SOURCE_FILE=data/v2.csv
#   PG_CONN_ID=postgres_default

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_ID = "levin_ingest_validate_publish"
PG_CONN_ID = os.getenv("PG_CONN_ID", "postgres_default")
SOURCE_CSV = os.getenv("TELEMETRY_SOURCE_FILE", "data/v2.csv")

SQL_CREATE = Path("include/sql/00_create_tables.sql")
SQL_AGG = Path("include/sql/10_daily_aggregate.sql")

INSERT_SQL = """
INSERT INTO raw_telematics (
  event_time, partition_date, device_id, trip_id,
  acc_data, gps_speed, battery, coolant_temp_c, dtc_count, engine_load, intake_air_temp,
  imap, kpl, maf, rpm, speed, t_adv, t_pos
)
VALUES %s
ON CONFLICT (device_id, trip_id, event_time) DO NOTHING;
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
    # 1) create tables (SQL file) is run
    @task
    def create_tables() -> None:
        PostgresHook(PG_CONN_ID).run(_read_sql(SQL_CREATE))

    # 2) ingest ds rows from one CSV into Postgres (skip duplicates)
    @task
    def ingest(ds: str) -> None:
        # 1) read CSV into pandas dataframe
        src = Path(SOURCE_CSV)
        if not src.exists():
            raise AirflowFailException(f"CSV not found: {src}")

        # 2) read CSV into pandas dataframe
        df = pd.read_csv(src)

        # 3) validate required columns
        # required columns
        for c in ("timeStamp", "tripID", "deviceID"):
            if c not in df.columns:
                raise AirflowFailException(f"Missing column: {c}")

        # 4) parse timestamps + filter to ds
        df["event_time"] = pd.to_datetime(df["timeStamp"], errors="coerce")
        df = df[df["event_time"].notna()]
        df = df[df["event_time"].dt.date == pd.to_datetime(ds).date()]

        # keep only needed cols (missing optional cols become None)
        def col(name):
            return df[name] if name in df.columns else [None] * len(df)

        # 5) numeric coercions (optional)
        for c in ["gps_speed", "battery", "cTemp", "dtc", "eLoad", "iat", "imap", "kpl", "maf", "rpm", "speed", "tAdv", "tPos"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        # 6) zip into list of tuples
        rows = list(
            zip(
                df["event_time"].dt.to_pydatetime(),
                [ds] * len(df),
                df["deviceID"].astype(str),
                df["tripID"].astype(str),
                col("accData"),
                col("gps_speed"),
                col("battery"),
                col("cTemp"),
                col("dtc"),
                col("eLoad"),
                col("iat"),
                col("imap"),
                col("kpl"),
                col("maf"),
                col("rpm"),
                col("speed"),
                col("tAdv"),
                col("tPos"),
            )
        )

        if not rows:
            return

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

    # 3) build daily aggregate by running the SQL (10_daily_aggregate.sql)
    @task
    def aggregate(ds: str) -> None:
        _run_sql_for_ds(PostgresHook(PG_CONN_ID), SQL_AGG, ds)

    create_tables() >> ingest() >> aggregate()


levin_pipeline()
