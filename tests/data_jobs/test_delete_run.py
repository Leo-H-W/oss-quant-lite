"""任务删除合约：终态任务可删除；活跃任务拒绝删除；僵尸 run 先清偿再删。"""
import pytest

from app.services.data_jobs.parquet_state_store import ParquetDataJobStateStore
from app.services.data_jobs.service import DataJobService, JobActiveError

pytestmark = pytest.mark.module_data_jobs


def _make_service(tmp_path, is_active=lambda run_id: True):
    store = ParquetDataJobStateStore(base_dir=str(tmp_path / "state"))
    return store, DataJobService(state_store=store, is_active=is_active)


def test_store_delete_run_removes_row(tmp_path):
    store, _ = _make_service(tmp_path)
    run = store.create_run("stock_basic", {})
    store.update_run_status(run, "failed", error_message="x")

    assert store.delete_run(run.id) is True
    assert store.get_run(run.id) is None
    assert store.delete_run(run.id) is False


def test_service_delete_terminal_run(tmp_path):
    store, service = _make_service(tmp_path)
    run = store.create_run("stock_basic", {})
    store.update_run_status(run, "success", progress=100.0)

    service.delete_run(run.id)

    assert store.get_run(run.id) is None


def test_service_delete_active_run_rejected(tmp_path):
    """仍在进行的任务不允许删除。"""
    store, service = _make_service(tmp_path)
    run = store.create_run("stock_basic", {})
    store.update_run_status(run, "running", progress=5.0)

    with pytest.raises(JobActiveError):
        service.delete_run(run.id)
    assert store.get_run(run.id).status == "running"


def test_service_delete_orphan_run_reconciled_then_deleted(tmp_path):
    """重启遗留的假 running：删除时先按孤儿清偿为 failed，再允许删除。"""
    store, service = _make_service(tmp_path, is_active=lambda run_id: False)
    run = store.create_run("stock_basic", {})
    store.update_run_status(run, "running", progress=5.0)

    service.delete_run(run.id)

    assert store.get_run(run.id) is None


def test_service_delete_missing_run_raises(tmp_path):
    _, service = _make_service(tmp_path)

    with pytest.raises(ValueError):
        service.delete_run(424242)
