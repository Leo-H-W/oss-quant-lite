"""任务进度页重试入口合约：新旧两个页面的重试都必须走 retry API。"""
from pathlib import Path

import pytest

pytestmark = pytest.mark.module_data_jobs


def test_legacy_data_jobs_page_has_retry_button_using_retry_api():
    js = Path("app/static/js/data_jobs.js").read_text(encoding="utf-8")

    assert "重试" in js
    assert "/retry" in js


def test_react_data_management_page_retry_uses_retry_api():
    tsx = Path("frontend/src/pages/DataManagementPage.tsx").read_text(encoding="utf-8")
    api = Path("frontend/src/api/dataJobs.ts").read_text(encoding="utf-8")

    assert "retryDataJob" in api
    assert "/retry" in api
    assert "retryDataJob" in tsx


def test_legacy_data_jobs_page_has_delete_button_using_delete_api():
    js = Path("app/static/js/data_jobs.js").read_text(encoding="utf-8")

    assert "删除" in js
    assert 'method: "DELETE"' in js or "method: 'DELETE'" in js


def test_react_data_management_page_delete_uses_delete_api():
    tsx = Path("frontend/src/pages/DataManagementPage.tsx").read_text(encoding="utf-8")
    api = Path("frontend/src/api/dataJobs.ts").read_text(encoding="utf-8")

    assert "deleteDataJob" in api
    assert "rawDelete" in api
    assert "deleteDataJob" in tsx
