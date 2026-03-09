"""
DAG integrity tests — verify the DAG loads cleanly, has the correct
task count, and has the expected dependency graph.
These tests run WITHOUT a live Airflow/database connection.
"""
from __future__ import annotations

import importlib
import sys
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Avoid ImportError on missing airflow/DB deps in CI without Docker
# ---------------------------------------------------------------------------

airflow = pytest.importorskip("airflow", reason="airflow not installed")


# ---------------------------------------------------------------------------
# Load the DAG module under test
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def sales_dag():
    """Import the DAG module and return the DAG object."""
    # Patch DB calls so DAG file can be imported without a live DB
    with patch("etl.utils.db.get_engine", return_value=MagicMock()), \
         patch("etl.utils.db.get_session", MagicMock()):
        if "dags.sales_pipeline" in sys.modules:
            del sys.modules["dags.sales_pipeline"]
        import dags.sales_pipeline as dp
        return dp.dag


# ---------------------------------------------------------------------------
# DAG-level assertions
# ---------------------------------------------------------------------------

class TestDAGLoads:
    def test_dag_object_exists(self, sales_dag):
        assert sales_dag is not None

    def test_dag_id(self, sales_dag):
        assert sales_dag.dag_id == "sales_etl"

    def test_schedule_interval(self, sales_dag):
        assert sales_dag.schedule_interval == "@daily"

    def test_catchup_enabled(self, sales_dag):
        assert sales_dag.catchup is True

    def test_max_active_runs(self, sales_dag):
        assert sales_dag.max_active_runs == 3


# ---------------------------------------------------------------------------
# Task-level assertions
# ---------------------------------------------------------------------------

class TestDAGTasks:
    EXPECTED_TASKS = {
        "start", "extract", "validate_extract", "transform",
        "load", "validate_load", "refresh_mart", "cleanup", "end"
    }

    def test_task_count(self, sales_dag):
        assert len(sales_dag.tasks) == len(self.EXPECTED_TASKS)

    def test_all_expected_tasks_present(self, sales_dag):
        task_ids = {t.task_id for t in sales_dag.tasks}
        assert self.EXPECTED_TASKS == task_ids

    def test_no_orphan_tasks(self, sales_dag):
        """Every task (except start/end) should have at least one upstream."""
        task_map = {t.task_id: t for t in sales_dag.tasks}
        for tid, task in task_map.items():
            if tid in ("start",):
                continue
            assert len(task.upstream_task_ids) > 0, f"Task '{tid}' has no upstream"


# ---------------------------------------------------------------------------
# Dependency graph assertions
# ---------------------------------------------------------------------------

class TestDAGDependencies:
    def _upstream_ids(self, dag, task_id):
        return {t.task_id for t in dag.get_task(task_id).upstream_list}

    def _downstream_ids(self, dag, task_id):
        return {t.task_id for t in dag.get_task(task_id).downstream_list}

    def test_extract_upstream_of_validate_extract(self, sales_dag):
        assert "extract" in self._upstream_ids(sales_dag, "validate_extract")

    def test_validate_extract_upstream_of_transform(self, sales_dag):
        assert "validate_extract" in self._upstream_ids(sales_dag, "transform")

    def test_transform_upstream_of_load(self, sales_dag):
        assert "transform" in self._upstream_ids(sales_dag, "load")

    def test_load_upstream_of_validate_load(self, sales_dag):
        assert "load" in self._upstream_ids(sales_dag, "validate_load")

    def test_cleanup_has_all_done_trigger(self, sales_dag):
        cleanup = sales_dag.get_task("cleanup")
        assert cleanup.trigger_rule.value == "all_done"


# ---------------------------------------------------------------------------
# Retry / SLA configuration
# ---------------------------------------------------------------------------

class TestDAGRetryConfig:
    def test_extract_retries(self, sales_dag):
        retries = sales_dag.get_task("extract").retries
        assert retries >= 3

    def test_transform_retries(self, sales_dag):
        retries = sales_dag.get_task("transform").retries
        assert retries >= 3

    def test_load_retries(self, sales_dag):
        retries = sales_dag.get_task("load").retries
        assert retries >= 3

    def test_validate_load_no_retries(self, sales_dag):
        retries = sales_dag.get_task("validate_load").retries
        assert retries == 0
