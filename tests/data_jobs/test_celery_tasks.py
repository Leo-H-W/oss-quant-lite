from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import app.tasks.data_jobs_tasks as data_jobs_tasks
from app.tasks.data_jobs_tasks import run_data_job
from app.celery_app import celery
from app.services.data_jobs.parquet_state_store import ParquetDataJobStateStore

pytestmark = pytest.mark.module_data_jobs


def test_run_data_job_task_exists():
    assert callable(run_data_job)


def test_run_data_job_registered_in_celery():
    assert "data_jobs.run" in celery.tasks


def test_run_data_job_marks_success():
    fake_run = SimpleNamespace(
        id=10,
        job_type="stock_basic",
        status="queued",
        params_json={},
        result_json=None,
        error_message=None,
        source_name=None,
        source_mode=None,
        snapshot_tag=None,
    )
    fake_registry = MagicMock()
    fake_registry.get_job.return_value = SimpleNamespace(
        script_path="app/utils/stock_basic.py",
        source_name="tushare",
        source_mode="full",
    )
    fake_store = MagicMock()
    fake_store.get_run.return_value = fake_run
    fake_runner = MagicMock()
    fake_runner.run_script.return_value = SimpleNamespace(returncode=0, stdout="ok", stderr="")

    @contextmanager
    def _ctx():
        yield

    fake_app = SimpleNamespace(app_context=lambda: _ctx())

    with patch("app.tasks.data_jobs_tasks._build_app", return_value=fake_app), patch(
        "app.tasks.data_jobs_tasks._build_registry", return_value=fake_registry
    ), patch("app.tasks.data_jobs_tasks._build_state_store", return_value=fake_store), patch(
        "app.tasks.data_jobs_tasks._build_runner", return_value=fake_runner
    ):
        result = run_data_job(10)

    assert result["status"] == "success"
    assert fake_runner.run_script.call_args.kwargs["params"] == {
        "source_name": "tushare",
        "source_mode": "full",
    }
    assert fake_store.update_run_status.call_args_list[0].args[1] == "running"
    assert fake_store.update_run_status.call_args_list[-1].args[1] == "success"


def test_task_builds_parquet_state_store_by_default(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))

    store = data_jobs_tasks._build_state_store()

    assert isinstance(store, ParquetDataJobStateStore)


def _retry_fixture(fake_runner):
    fake_run = SimpleNamespace(
        id=11,
        job_type="daily_basic_baostock",
        status="queued",
        params_json={"start_date": "2019-01-01", "end_date": "2022-12-31"},
        result_json=None,
        error_message=None,
        source_name="baostock",
        source_mode="incremental",
        snapshot_tag=None,
    )
    fake_registry = MagicMock()
    fake_registry.get_job.return_value = SimpleNamespace(
        script_path="app/utils/daily_basic_baostock.py",
        source_name="baostock",
        source_mode="incremental",
    )
    fake_store = MagicMock()
    fake_store.get_run.return_value = fake_run

    @contextmanager
    def _ctx():
        yield

    fake_app = SimpleNamespace(app_context=lambda: _ctx())
    return fake_run, fake_registry, fake_store, fake_app


def test_run_data_job_automatically_retries_same_run(monkeypatch):
    monkeypatch.setenv("DATA_JOB_MAX_RETRIES", "1")
    monkeypatch.setenv("DATA_JOB_RETRY_BACKOFF_SECONDS", "0")
    fake_runner = MagicMock()
    fake_runner.run_script.side_effect = [
        SimpleNamespace(returncode=124, stdout="", stderr="timed out"),
        SimpleNamespace(returncode=0, stdout="resumed", stderr=""),
    ]
    fake_run, fake_registry, fake_store, fake_app = _retry_fixture(fake_runner)

    with patch("app.tasks.data_jobs_tasks._build_app", return_value=fake_app), patch(
        "app.tasks.data_jobs_tasks._build_registry", return_value=fake_registry
    ), patch("app.tasks.data_jobs_tasks._build_state_store", return_value=fake_store), patch(
        "app.tasks.data_jobs_tasks._build_runner", return_value=fake_runner
    ):
        result = run_data_job(11)

    assert result["status"] == "success"
    assert fake_runner.run_script.call_count == 2
    assert fake_run.result_json["attempt"] == 2
    assert fake_run.result_json["max_attempts"] == 2


def test_run_data_job_stops_after_configured_attempts(monkeypatch):
    monkeypatch.setenv("DATA_JOB_MAX_RETRIES", "2")
    monkeypatch.setenv("DATA_JOB_RETRY_BACKOFF_SECONDS", "0")
    fake_runner = MagicMock()
    fake_runner.run_script.return_value = SimpleNamespace(
        returncode=1, stdout="", stderr="still failing"
    )
    fake_run, fake_registry, fake_store, fake_app = _retry_fixture(fake_runner)

    with patch("app.tasks.data_jobs_tasks._build_app", return_value=fake_app), patch(
        "app.tasks.data_jobs_tasks._build_registry", return_value=fake_registry
    ), patch("app.tasks.data_jobs_tasks._build_state_store", return_value=fake_store), patch(
        "app.tasks.data_jobs_tasks._build_runner", return_value=fake_runner
    ):
        result = run_data_job(11)

    assert result["status"] == "failed"
    assert fake_runner.run_script.call_count == 3
    assert fake_run.result_json["attempt"] == 3
    assert fake_run.result_json["max_attempts"] == 3


def test_cancelled_run_is_not_retried(monkeypatch):
    monkeypatch.setenv("DATA_JOB_MAX_RETRIES", "5")
    monkeypatch.setenv("DATA_JOB_RETRY_BACKOFF_SECONDS", "0")
    fake_runner = MagicMock()
    fake_run, fake_registry, fake_store, fake_app = _retry_fixture(fake_runner)

    def cancel_during_script(*_args, **_kwargs):
        fake_run.status = "cancelled"
        return SimpleNamespace(returncode=143, stdout="", stderr="terminated")

    fake_runner.run_script.side_effect = cancel_during_script
    with patch("app.tasks.data_jobs_tasks._build_app", return_value=fake_app), patch(
        "app.tasks.data_jobs_tasks._build_registry", return_value=fake_registry
    ), patch("app.tasks.data_jobs_tasks._build_state_store", return_value=fake_store), patch(
        "app.tasks.data_jobs_tasks._build_runner", return_value=fake_runner
    ):
        result = run_data_job(11)

    assert result["status"] == "cancelled"
    assert fake_runner.run_script.call_count == 1
