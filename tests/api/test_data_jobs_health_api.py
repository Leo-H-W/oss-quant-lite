"""任务健康度 API 合约：真实 Flask app fixture + 健康服务边界 mock。"""
from unittest.mock import patch

import pytest

pytestmark = pytest.mark.module_data_jobs

_FAKE_REPORT = {
    "status": "warning",
    "checked_at": "2026-10-26T12:00:00",
    "checks": {
        "orphan_runs": {"status": "ok", "runs": []},
        "recent_failures": {"status": "warning", "runs": [{"id": 14}]},
        "auto_init_heartbeat": {"status": "ok"},
        "data_freshness": {"status": "ok", "stale_tables": []},
        "partition_completeness": {"status": "ok", "tables": []},
    },
}


def test_health_endpoint_returns_report(app):
    client = app.test_client()
    with patch(
        "app.api.data_jobs_api.collect_health_report", return_value=_FAKE_REPORT
    ) as collect:
        resp = client.get("/api/data-jobs/health")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["success"] is True
    assert data["health"]["status"] == "warning"
    assert "checks" in data["health"]
    assert collect.called


def test_health_endpoint_aggregates_check_details(app):
    client = app.test_client()
    with patch("app.api.data_jobs_api.collect_health_report", return_value=_FAKE_REPORT):
        resp = client.get("/api/data-jobs/health")
    checks = resp.get_json()["health"]["checks"]
    assert checks["recent_failures"]["runs"][0]["id"] == 14


def test_health_repair_endpoint_runs_repair_and_returns_actions(app):
    client = app.test_client()
    fake_actions = {
        "reaped_orphans": [17],
        "submitted": [{"job_type": "daily_basic_baostock", "run_id": 901}],
        "skipped_duplicate": [],
        "errors": [],
    }
    with patch(
        "app.api.data_jobs_api.collect_health_report", return_value=_FAKE_REPORT
    ), patch(
        "app.api.data_jobs_api.repair_health", return_value=fake_actions
    ) as repair:
        resp = client.post("/api/data-jobs/health/repair")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["success"] is True
    assert data["actions"]["reaped_orphans"] == [17]
    assert data["actions"]["submitted"][0]["job_type"] == "daily_basic_baostock"
    assert "health" in data
    assert repair.called
