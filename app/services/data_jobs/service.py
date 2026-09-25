import threading
from app.utils.time_utils import now_local
from typing import Any, Callable, Dict, Optional

from app.services.data_jobs.parquet_state_store import ParquetDataJobStateStore
from app.services.data_jobs.registry import JobRegistry
from app.tasks.data_jobs_tasks import (
    clear_run_active,
    is_run_active,
    mark_run_active,
    run_data_job,
)

try:
    from flask import current_app
except Exception:  # pragma: no cover
    current_app = None


class JobActiveError(Exception):
    """重试目标仍处于活动状态（pending/queued/running），不允许重试。"""


ACTIVE_STATUSES = {"pending", "queued", "running"}

ORPHAN_ERROR_MESSAGE = "任务线程已不存在（进程可能中断或重启），判定为孤儿任务并强制失败"


def _resolve_execution_mode(explicit_mode: Optional[str] = None) -> str:
    if explicit_mode:
        return explicit_mode

    try:
        if current_app:
            mode = current_app.config.get("DATA_JOB_EXECUTION_MODE")
            if mode:
                return str(mode).lower()
    except Exception:
        pass

    # 去 Redis/Celery 后只有本地执行一种模式；app context 不可用时也按本地处理
    return "inline"


class DataJobService:
    """Facade for data job submission and querying."""

    def __init__(
        self,
        registry: Optional[JobRegistry] = None,
        state_store: Optional[Any] = None,
        execution_mode: Optional[str] = None,
        is_active: Optional[Callable[[int], bool]] = None,
    ):
        self.registry = registry or JobRegistry()
        self.state_store = state_store or ParquetDataJobStateStore()
        self.execution_mode = _resolve_execution_mode(execution_mode)
        # 判活回调：默认进程内活性注册表；测试可注入替身
        self._is_active = is_active or is_run_active
        self._reap_lock = threading.Lock()
        self._orphans_reaped = False

    def _reap_orphans_once(self) -> None:
        """每进程一次的全量孤儿收割：上个进程中断留下的假 running 在此清偿。

        扫描抛异常时标志不置位，下次调用重试（标 failed 幂等无害）。
        state_store 缺少 reap_orphan_runs（如测试替身）时直接跳过。
        """
        with self._reap_lock:
            if self._orphans_reaped:
                return
        reap = getattr(self.state_store, "reap_orphan_runs", None)
        if callable(reap):
            try:
                reap(self._is_active)
            except Exception:
                return
        with self._reap_lock:
            self._orphans_reaped = True

    def _reconcile_run(self, run):
        """读时判活：非终态且不在册的 run 当场落 failed。

        重启后第一次打开任务列表就能看到真实状态，不用等下次提交。
        """
        if run is None or getattr(run, "status", None) not in ACTIVE_STATUSES:
            return run
        if self._is_active(run.id):
            return run
        # 仅真实状态存储支持收割语义；测试替身（无 reap_orphan_runs）保持原样
        if not callable(getattr(self.state_store, "reap_orphan_runs", None)):
            return run
        return self.state_store.update_run_status(
            run,
            "failed",
            error_message=ORPHAN_ERROR_MESSAGE,
            progress_message="已中断，可重试",
        )

    def submit(self, job_type: str, params: Optional[Dict[str, Any]] = None):
        definition = self.registry.get_job(job_type)
        params = params or {}
        # 提交前先清理僵尸 run：worker 被 kill 后 run 永远停在 running，
        # 不清理的话 find_active_duplicate 会永久拒绝该作业再次提交
        reap_stale = getattr(self.state_store, "reap_stale_runs", None)
        if callable(reap_stale):
            reap_stale()
        # 注册表精确判活收割（每进程一次）：重启留下的假 running 立即清偿
        self._reap_orphans_once()
        find_active_duplicate = getattr(self.state_store, "find_active_duplicate", None)
        if callable(find_active_duplicate):
            duplicate_run = find_active_duplicate(job_type, params)
            if duplicate_run is not None:
                raise ValueError(f"duplicate running job: {duplicate_run.id}")

        snapshot_tag = now_local().strftime("%Y-%m-%d")
        try:
            run = self.state_store.create_run(
                job_type,
                params,
                source_name=definition.source_name,
                source_mode=definition.source_mode,
                snapshot_tag=snapshot_tag,
                progress_message="已创建任务，等待调度",
            )
        except TypeError:
            run = self.state_store.create_run(job_type, params)
            for field_name, field_value in {
                "source_name": definition.source_name,
                "source_mode": definition.source_mode,
                "snapshot_tag": snapshot_tag,
                "progress_message": "已创建任务，等待调度",
            }.items():
                if hasattr(run, field_name):
                    setattr(run, field_name, field_value)

        try:
            run = self.state_store.update_run_status(
                run,
                "queued",
                progress=0.0,
                progress_message="任务已入队",
            )
        except TypeError:
            run = self.state_store.update_run_status(run, "queued", progress=0.0)

        if self.execution_mode == "inline":
            # inline 任务在后台线程执行：同步跑会把提交请求挂住最长
            # DATA_JOB_TIMEOUT（默认 1 小时），浏览器超时后重试还会撞上
            # find_active_duplicate 被拒。run_data_job 自建 app context，
            # 状态推进/失败落盘全部由它负责，提交侧立即返回 queued
            thread = threading.Thread(
                target=lambda: run_data_job(run.id),
                name=f"data-job-{run.job_type}-{run.id}",
                daemon=True,
            )
            # 必须先注册再启动线程：读时判活以注册表为准，
            # 晚注册会把刚提交的任务当孤儿误杀（TOCTOU）
            mark_run_active(run.id)
            try:
                thread.start()
            except Exception:
                clear_run_active(run.id)
                self.state_store.update_run_status(
                    run,
                    "failed",
                    error_message="后台线程启动失败",
                    progress_message="任务启动失败，可重试",
                )
                raise
            return run

        run_data_job.delay(run.id)
        return run

    def delete_run(self, run_id: int):
        """删除任务记录。get_run 会先按活性把僵尸 run 清偿为 failed，
        因此重启遗留的假 running 也能删；真正活跃的 run 拒绝删除。"""
        run = self.get_run(run_id)
        if run is None:
            raise ValueError(f"job run not found: {run_id}")
        if run.status in ACTIVE_STATUSES:
            raise JobActiveError(f"job run {run_id} 仍处于 {run.status}，不允许删除")
        self.state_store.delete_run(run_id)

    def retry(self, run_id: int):
        run = self.get_run(run_id)
        if run is None:
            raise ValueError(f"job run not found: {run_id}")
        if run.status in ACTIVE_STATUSES:
            raise JobActiveError(f"job run {run_id} 仍处于 {run.status}，不允许重试")
        new_run = self.submit(run.job_type, run.params_json or {})
        # 重试成功后删除原记录：新旧记录语义等价，保留两条只会让列表越攒越乱
        delete = getattr(self.state_store, "delete_run", None)
        if callable(delete):
            delete(run_id)
        return new_run

    def list_job_definitions(self, visible_only: bool = True):
        if visible_only:
            return self.registry.list_visible_jobs()
        return self.registry.list_jobs()

    def list_runs(self, limit: int = 50, status: Optional[str] = None):
        self._reap_orphans_once()
        runs = self.state_store.list_runs(limit=limit, status=status)
        return [self._reconcile_run(run) for run in runs]

    def get_run(self, run_id: int):
        self._reap_orphans_once()
        return self._reconcile_run(self.state_store.get_run(run_id))
