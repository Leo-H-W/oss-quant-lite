from pathlib import Path
import pytest

pytestmark = pytest.mark.module_data_jobs


def test_data_jobs_page_merges_progress_into_history_table():
    """任务进度已并入最近任务表格：模板不再有独立的任务进度面板。"""
    html = Path("app/templates/data_management/index.html").read_text(encoding="utf-8")
    assert "dataJobHistory" in html
    assert "dataJobProgress" not in html
    assert "任务进度" not in html


def test_data_jobs_script_renders_inline_progress_and_expandable_detail():
    """JS 负责行内进度条与点行展开的详情行。"""
    script = Path("app/static/js/data_jobs.js").read_text(encoding="utf-8")

    assert "progress-bar" in script
    assert "data-job-detail" in script


def test_data_jobs_time_display_uses_shanghai_timezone():
    script = Path("app/static/js/data_jobs.js").read_text(encoding="utf-8")

    assert 'timeZone: "Asia/Shanghai"' in script


def test_data_jobs_progress_script_renders_audit_metadata():
    script = Path("app/static/js/data_jobs.js").read_text(encoding="utf-8")

    assert "progress_message" in script
    assert "source_name" in script
    assert "snapshot_tag" in script
