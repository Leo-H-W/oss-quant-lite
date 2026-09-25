"""数据任务 retry API 合约：重试保留原参数；活跃任务拒绝重试。"""
from unittest.mock import patch

import pytest

from app.services.data_jobs.parquet_state_store import ParquetDataJobStateStore
from app.services.data_jobs.service import DataJobService

pytestmark = pytest.mark.module_data_jobs


def _make_service(tmp_path, is_active=lambda run_id: True):
    store = ParquetDataJobStateStore(base_dir=str(tmp_path / "state"))
    return store, DataJobService(state_store=store, is_active=is_active)


def test_retry_preserves_original_params(app, tmp_path):
    """重试必须带原 params_json 重新提交（旧前端重新 submit 会丢日期参数）。"""
    store, service = _make_service(tmp_path)
    original_params = {"start_date": "20260101", "end_date": "20260201"}
    run = store.create_run("daily_basic", original_params)
    store.update_run_status(run, "failed", error_message="worker 中断")

    with patch("app.api.data_jobs_api.get_data_job_service", return_value=service), \
         patch("app.services.data_jobs.service.run_data_job"):
        resp = app.test_client().post(f"/api/data-jobs/{run.id}/retry")

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["success"] is True
    new_run = store.get_run(data["run_id"])
    assert new_run.id != run.id
    assert new_run.job_type == "daily_basic"
    assert new_run.params_json == original_params


def test_retry_on_active_run_returns_400(app, tmp_path):
    """仍在进行的任务不允许重试，返回 400 而不是误报 404。"""
    store, service = _make_service(tmp_path)
    run = store.create_run("daily_basic", {})
    store.update_run_status(run, "running", progress=5.0)

    with patch("app.api.data_jobs_api.get_data_job_service", return_value=service):
        resp = app.test_client().post(f"/api/data-jobs/{run.id}/retry")

    assert resp.status_code == 400
    assert resp.get_json()["success"] is False


def test_retry_on_missing_run_returns_404(app, tmp_path):
    _, service = _make_service(tmp_path)

    with patch("app.api.data_jobs_api.get_data_job_service", return_value=service):
        resp = app.test_client().post("/api/data-jobs/424242/retry")

    assert resp.status_code == 404


def test_retry_on_success_run_allowed_and_keeps_params(app, tmp_path):
    """成功任务也允许重试：按原参数重跑。写入按交易日分区原子覆盖，
    重复执行只会覆盖同名分区，不会产生重复数据。"""
    store, service = _make_service(tmp_path)
    original_params = {"start_date": "20260101", "end_date": "20260201"}
    run = store.create_run("daily_basic", original_params)
    store.update_run_status(run, "success", progress=100.0)

    with patch("app.api.data_jobs_api.get_data_job_service", return_value=service), \
         patch("app.services.data_jobs.service.run_data_job"):
        resp = app.test_client().post(f"/api/data-jobs/{run.id}/retry")

    assert resp.status_code == 200
    new_run = store.get_run(resp.get_json()["run_id"])
    assert new_run.params_json == original_params
