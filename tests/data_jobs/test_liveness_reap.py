"""活性注册表 + 孤儿收割合约：服务重启后 running 状态不再骗人。

数据任务在 web 进程内 daemon 线程执行，进程重启后线程全灭，
Parquet 里的 run 不能继续停在 pending/queued/running。
"""
from unittest.mock import patch

import pytest

from app.services.data_jobs.parquet_state_store import ParquetDataJobStateStore
from app.services.data_jobs.service import DataJobService

pytestmark = pytest.mark.module_data_jobs


def _make_run(store, status="running"):
    run = store.create_run("stock_basic", {"start_date": "20260101"})
    store.update_run_status(run, status, progress=5.0)
    return run


def test_active_registry_mark_is_clear():
    from app.tasks import data_jobs_tasks

    data_jobs_tasks.mark_run_active(999001)
    assert data_jobs_tasks.is_run_active(999001) is True
    data_jobs_tasks.clear_run_active(999001)
    assert data_jobs_tasks.is_run_active(999001) is False


def test_reap_orphan_runs_marks_dead_running_run_failed(tmp_path):
    """不在活性注册表里的 running run 只可能是上个进程的遗孤，必须强制 failed。"""
    store = ParquetDataJobStateStore(base_dir=str(tmp_path / "state"))
    run = _make_run(store, "running")

    reaped = store.reap_orphan_runs(is_active=lambda run_id: False)

    assert [r.id for r in reaped] == [run.id]
    fetched = store.get_run(run.id)
    assert fetched.status == "failed"
    assert "孤儿" in fetched.error_message or "重启" in fetched.error_message
    assert fetched.progress_message == "已中断，可重试"
    assert fetched.finished_at is not None


def test_reap_orphan_runs_keeps_active_run(tmp_path):
    """在册的 run 线程活着，跑多久都不能误清。"""
    store = ParquetDataJobStateStore(base_dir=str(tmp_path / "state"))
    run = _make_run(store, "running")

    reaped = store.reap_orphan_runs(is_active=lambda run_id: True)

    assert reaped == []
    assert store.get_run(run.id).status == "running"


def test_get_run_reconciles_orphan_at_read_time(tmp_path):
    """读状态时当场核对活性：重启后打开页面即见 failed，不等下次提交。"""
    store = ParquetDataJobStateStore(base_dir=str(tmp_path / "state"))
    run = _make_run(store, "running")
    service = DataJobService(state_store=store, is_active=lambda run_id: False)

    fetched = service.get_run(run.id)

    assert fetched.status == "failed"


def test_list_runs_reconciles_orphan_at_read_time(tmp_path):
    store = ParquetDataJobStateStore(base_dir=str(tmp_path / "state"))
    run = _make_run(store, "queued")
    service = DataJobService(state_store=store, is_active=lambda run_id: False)

    runs = service.list_runs(limit=10)

    reconciled = [r for r in runs if r.id == run.id]
    assert reconciled and reconciled[0].status == "failed"


def test_get_run_keeps_active_running_run(tmp_path):
    store = ParquetDataJobStateStore(base_dir=str(tmp_path / "state"))
    run = _make_run(store, "running")
    service = DataJobService(state_store=store, is_active=lambda run_id: True)

    fetched = service.get_run(run.id)

    assert fetched.status == "running"


def test_submit_marks_run_active_before_thread_start(tmp_path):
    """线程启动前必须先在册，否则读时判活会把刚提交的任务误杀（TOCTOU）。"""
    store = ParquetDataJobStateStore(base_dir=str(tmp_path / "state"))
    service = DataJobService(state_store=store, is_active=lambda run_id: True)
    events = []

    class FakeThread:
        def __init__(self, target=None, name=None, daemon=None):
            self._target = target

        def start(self):
            events.append("thread_start")

    def _mark(run_id):
        events.append("mark_active")

    with patch("app.services.data_jobs.service.mark_run_active", side_effect=_mark), \
         patch("app.services.data_jobs.service.threading.Thread", FakeThread):
        service.submit("stock_basic", {})

    assert events == ["mark_active", "thread_start"]
