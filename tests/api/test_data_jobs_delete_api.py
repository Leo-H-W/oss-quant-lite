"""任务删除 API 合约：DELETE /api/data-jobs/<run_id>。"""
from unittest.mock import patch

import pytest

from app.services.data_jobs.parquet_state_store import ParquetDataJobStateStore
from app.services.data_jobs.service import DataJobService

pytestmark = pytest.mark.module_data_jobs


def _make_service(tmp_path, is_active=lambda run_id: True):
    store = ParquetDataJobStateStore(base_dir=str(tmp_path / "state"))
    return store, DataJobService(state_store=store, is_active=is_active)


def test_delete_terminal_run_returns_200(app, tmp_path):
    store, service = _make_service(tmp_path)
    run = store.create_run("stock_basic", {})
    store.update_run_status(run, "failed", error_message="x")

    with patch("app.api.data_jobs_api.get_data_job_service", return_value=service):
        resp = app.test_client().delete(f"/api/data-jobs/{run.id}")

    assert resp.status_code == 200
    assert resp.get_json()["success"] is True
    assert store.get_run(run.id) is None


def test_delete_active_run_returns_400(app, tmp_path):
    store, service = _make_service(tmp_path)
    run = store.create_run("stock_basic", {})
    store.update_run_status(run, "running", progress=5.0)

    with patch("app.api.data_jobs_api.get_data_job_service", return_value=service):
        resp = app.test_client().delete(f"/api/data-jobs/{run.id}")

    assert resp.status_code == 400
    assert resp.get_json()["success"] is False
    assert store.get_run(run.id) is not None


def test_delete_missing_run_returns_404(app, tmp_path):
    _, service = _make_service(tmp_path)

    with patch("app.api.data_jobs_api.get_data_job_service", return_value=service):
        resp = app.test_client().delete("/api/data-jobs/424242")

    assert resp.status_code == 404
