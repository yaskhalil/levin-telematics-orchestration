from __future__ import annotations

from airflow.models import DagBag


def test_dags_import_without_errors() -> None:
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    assert dagbag.import_errors == {}, f"DAG import failures: {dagbag.import_errors}"


def test_expected_dags_present() -> None:
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    assert "levin_ingest_validate_publish" in dagbag.dags
    assert "levin_backfill_range" in dagbag.dags


