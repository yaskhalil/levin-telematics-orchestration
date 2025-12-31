from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Dict, List
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@dag(
    dag_id="levin_backfill_range",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["levin", "telematics", "backfill"],
)
def levin_backfill_range():
    @task
    def read_conf() -> Dict[str, Any]:
        ctx = get_current_context()
        dag_run = ctx.get("dag_run")
        conf = getattr(dag_run, "conf", None) or {}
        return conf

    @task
    def expand_dates(conf: Dict[str, Any]) -> List[str]:
        start = conf.get("startDate")
        end = conf.get("endDate")
        if not start or not end:
            raise ValueError("dag_run.conf must include startDate and endDate (YYYY-MM-DD)")

        start_d = date.fromisoformat(str(start))
        end_d = date.fromisoformat(str(end))
        if end_d < start_d:
            raise ValueError("endDate must be >= startDate")

        days = (end_d - start_d).days
        return [(start_d + timedelta(days=i)).isoformat() for i in range(days + 1)]

    @task
    def build_trigger_confs(dates: List[str]) -> List[Dict[str, str]]:
        # Build mapped TriggerDagRunOperator `conf` payloads at runtime (dates is an XComArg).
        return [{"ds": d} for d in dates]

    conf = read_conf()
    dates = expand_dates(conf)
    trigger_confs = build_trigger_confs(dates)

    # Trigger the daily DAG for each date using a ds override via conf.
    # This avoids relying on logical_date support in TriggerDagRunOperator across versions.
    TriggerDagRunOperator.partial(
        task_id="trigger_daily_pipeline",
        trigger_dag_id="levin_ingest_validate_publish",
        wait_for_completion=False,
        reset_dag_run=False,
    ).expand(
        conf=trigger_confs
    )


levin_backfill_range()


