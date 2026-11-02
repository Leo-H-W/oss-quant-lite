"""夜间自动数据初始化（auto-init）。

背景：数据管理页的「需要初始化」徽标由 startup_runtime.PARQUET_ASSETS 五项
资产检查驱动，其中 stock_business.parquet 只能由 scripts/build_text2sql_wide_tables.py
物化，页面上任何手动任务都补不上，导致每次打开页面都提示初始化。

本模块把补齐动作自动化：
- plan_init_jobs：纯函数，缺失/空资产 → 按依赖序的任务链；
- run_auto_init：串行编排，submit 后轮询终态再提下一个（控制取数频次，
  任何时刻只有一个任务在跑），失败即中断，他处在跑同类型任务时礼让；
  原始日频表按断档回补，股票清单/财务三表/宽表/因子值每晚例行刷新；
- AutoInitScheduler：进程内轻量调度线程，每天到点（默认 02:30）检查一次；
  状态落盘保证重启当天不重复跑。

只在 run.py 主进程启动（start_auto_init），不挂 create_app——
数据任务线程会调 create_app，挂那里会繁殖调度线程。
"""
from __future__ import annotations

import json
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from loguru import logger

from app.services.data_jobs.runner import get_retry_policy
from app.utils.job_checkpoint import clear_checkpoint, load_checkpoint, save_checkpoint
from startup_runtime import PARQUET_ASSETS

# 资产 → 补齐任务，顺序即依赖序（交易日历/基础资料 → 行情 → 宽表物化）
ASSET_JOB_CHAIN: Tuple[Tuple[str, str], ...] = (
    ("stock_trade_calendar.parquet", "trade_calendar"),
    ("stock_basic.parquet", "stock_basic"),
    ("daily_history/daily", "daily_history_by_date"),
    ("daily_basic/daily", "daily_basic"),
    ("stock_business.parquet", "text2sql_wide_table"),
)

# 日频表 → 断档回补作业。资产级检查只看"存在且非空"，发现不了
# "分区在 10-09 之后再没更新"这类日期断档；这里按交易日历比对最新分区，
# 断档则提交对应日更作业（作业自带 gap-fill，按交易日历差集回补）。
# daily_basic/stk_factor/moneyflow/cyq_perf 走 Baostock/通达信/本地复算通道，
# 绕开 tushare 高积分与 60 次/小时限频；顺序即依赖序
# （cyq_perf 复算依赖 daily_basic 换手率，必须排在最后）。
# 注意：断档感知要求分区目录已存在（缺目录属资产检查职责，而这四张表
# 不在 PARQUET_ASSETS 内），首次启用需手动跑过一次建目录。
DAILY_GAP_JOB_CHAIN: Tuple[Tuple[str, str], ...] = (
    ("daily_history/daily", "daily_history_by_date"),
    ("daily_basic/daily", "daily_basic_baostock"),
    ("stk_factor/daily", "adj_factor_baostock"),
    ("moneyflow/daily", "moneyflow_tdx"),
    ("cyq_perf/daily", "cyq_perf_compute"),
)

# 详细历史回补的候选源。旧 DAILY_GAP_JOB_CHAIN 保留给现有健康检查与调用方；
# 新链路只有在前一候选最终失败或复扫仍有缺口时才尝试下一候选。
COVERAGE_JOB_CANDIDATES: Dict[str, Tuple[str, ...]] = {
    "daily_history/daily": ("daily_history_by_date", "daily_history_fuyao", "daily_history_tencent"),
    "daily_basic/daily": ("daily_basic_baostock", "daily_basic"),
    "stk_factor/daily": ("adj_factor_baostock", "stk_factor"),
    "moneyflow/daily": ("moneyflow_tdx_backfill", "moneyflow"),
    "cyq_perf/daily": ("cyq_perf_compute", "cyq_perf"),
}

# 非详细扫描路径与例行维护任务的备用源。候选复用同一组参数。
JOB_SOURCE_FALLBACKS: Dict[str, Tuple[str, ...]] = {
    "daily_history_by_date": ("daily_history_fuyao", "daily_history_tencent"),
    "daily_basic": ("daily_basic_baostock",),
    "daily_basic_baostock": ("daily_basic",),
    "adj_factor_baostock": ("stk_factor",),
    "moneyflow_tdx": ("moneyflow",),
    "moneyflow_tdx_backfill": ("moneyflow",),
    "cyq_perf_compute": ("cyq_perf",),
    "stock_basic_fuyao": ("stock_basic",),
    "financial_fuyao": ("financial_tushare", "financial_eastmoney"),
}

PARTITION_COMPLETE_RATIO = 0.8

# 每晚例行刷新的非分区/派生数据。原始日频表只在断档时回补，而这些任务
# 没有统一的 Hive 分区可供 find_stale_daily_tables 检查，必须显式纳入链路：
# - 股票清单：发现新上市代码；财务三表：脚本内部按报告期跳过已覆盖数据；
# - 两个宽表与因子值：上游日频数据更新后同步刷新，不能只在文件缺失时构建。
NIGHTLY_MAINTENANCE_JOB_CHAIN: Tuple[str, ...] = (
    "stock_basic_fuyao",
    "financial_fuyao",
    "wide_table_builder",
    "factor_compute",
    "text2sql_wide_table",
)

# 盘后数据就绪时间（与 wide_table_status.CUTOFF_HOUR 同口径）：
# 开市日 18:00 前当日数据不可得，断档期望值回退到上一开市日
DATA_READY_HOUR = 18

TERMINAL_STATUSES = {"success", "failed", "cancelled"}

DEFAULT_RUN_TIME = "02:30"
DEFAULT_POLL_INTERVAL = 30.0
DEFAULT_RUN_TIMEOUT = 3600.0  # 单个任务最长等待，与 DATA_JOB_TIMEOUT 对齐


def plan_init_jobs(
    missing_assets: Iterable[str], empty_assets: Iterable[str]
) -> List[str]:
    """缺失/空资产 → 按依赖序排列的补齐任务链。空资产视同缺失。"""
    needed = set(missing_assets) | set(empty_assets)
    return [job_type for asset, job_type in ASSET_JOB_CHAIN if asset in needed]


def plan_gap_fill_jobs(stale_tables: Iterable[str]) -> List[str]:
    """断档日频表 → 回补作业链（顺序同 DAILY_GAP_JOB_CHAIN）。"""
    stale = set(stale_tables)
    return [job_type for table, job_type in DAILY_GAP_JOB_CHAIN if table in stale]


def _latest_partition_date(table_path: Path) -> Optional[str]:
    """扫描 year=/month=/day= 分区目录名，返回最新分区日期 YYYYMMDD；目录不存在返回 None。"""
    if not table_path.is_dir():
        return None
    latest: Optional[str] = None
    for day_dir in table_path.glob("year=*/month=*/day=*"):
        try:
            y = day_dir.parent.parent.name.split("=", 1)[1]
            m = day_dir.parent.name.split("=", 1)[1]
            d = day_dir.name.split("=", 1)[1]
            ymd = f"{y}{m}{d}"
        except (IndexError, ValueError):
            continue
        if latest is None or ymd > latest:
            latest = ymd
    return latest


def find_stale_daily_tables(
    data_dir: Optional[str], now: Optional[datetime] = None
) -> List[str]:
    """日频表最新分区落后于"应有最新交易日" → 返回断档表清单。

    应有最新交易日：交易日历中 ≤ 今天的最近开市日；若今天开市但当前
    时间早于 DATA_READY_HOUR（当日数据尚未发布），回退到上一开市日。
    目录不存在的表交给资产检查链处理，这里不重复报告。
    """
    import pandas as pd  # 延迟导入：纯规划路径不依赖 pandas

    now = now or datetime.now()
    root = Path(data_dir) if data_dir else Path("data")
    cal_path = root / "stock_trade_calendar.parquet"
    if not cal_path.is_file():
        return []

    cal = pd.read_parquet(cal_path, columns=["cal_date", "is_open"])
    today = now.strftime("%Y%m%d")
    open_days = sorted(
        str(d) for d in cal.loc[cal["is_open"] == 1, "cal_date"] if str(d) <= today
    )
    if not open_days:
        return []
    expected = open_days[-1]
    if expected == today and now.hour < DATA_READY_HOUR:
        if len(open_days) < 2:
            return []
        expected = open_days[-2]

    stale: List[str] = []
    for table, _job_type in DAILY_GAP_JOB_CHAIN:
        latest = _latest_partition_date(root / table)
        if latest is not None and latest < expected:
            stale.append(table)
    return stale


def _expected_open_dates(root: Path, now: Optional[datetime] = None) -> List[str]:
    """返回截至数据就绪时点的开市日；与旧最新分区检查共用 18:00 口径。"""
    import pandas as pd

    now = now or datetime.now()
    cal_path = root / "stock_trade_calendar.parquet"
    if not cal_path.is_file():
        return []
    cal = pd.read_parquet(cal_path, columns=["cal_date", "is_open"])
    today = now.strftime("%Y%m%d")
    open_days = sorted(
        str(d) for d in cal.loc[cal["is_open"] == 1, "cal_date"] if str(d) <= today
    )
    if open_days and open_days[-1] == today and now.hour < DATA_READY_HOUR:
        open_days = open_days[:-1]
    return open_days


def _coverage_partition_files(root: Path, rel_table: str) -> Dict[str, Path]:
    files: Dict[str, Path] = {}
    table = root / rel_table
    if not table.is_dir():
        return files
    for path in table.glob("year=*/month=*/day=*/data.parquet"):
        try:
            y = path.parent.parent.parent.name.split("=", 1)[1]
            m = path.parent.parent.name.split("=", 1)[1]
            d = path.parent.name.split("=", 1)[1]
        except (IndexError, ValueError):
            continue
        files[f"{y}{m}{d}"] = path
    return files


def _partition_row_count(path: Path) -> Optional[int]:
    """只读 footer；空/坏文件返回 None，由调用方按缺口处理。"""
    if not path.is_file() or path.stat().st_size <= 0:
        return None
    try:
        import pyarrow.parquet as pq

        return int(pq.ParquetFile(path).metadata.num_rows)
    except Exception:  # noqa: BLE001 - 坏分区必须进入修复而不是阻断其他表扫描
        return None


def find_daily_coverage_gaps(
    data_dir: Optional[str], now: Optional[datetime] = None
) -> Dict[str, List[str]]:
    """扫描已有历史基线内的缺日期、最新缺日期、坏分区和行数残缺分区。

    每张表从自己的最早分区开始，不自动向更早历史扩展。非日线表行数低于
    同日日线 80% 时按残缺处理；该阈值容忍退市、停牌和北交所源覆盖差异。
    """
    root = Path(data_dir) if data_dir else Path("data")
    open_days = _expected_open_dates(root, now)
    if not open_days:
        return {}
    expected_end = open_days[-1]
    partitions = {
        table: _coverage_partition_files(root, table) for table, _ in DAILY_GAP_JOB_CHAIN
    }
    daily_counts = {
        date: _partition_row_count(path)
        for date, path in partitions.get("daily_history/daily", {}).items()
    }

    gaps: Dict[str, List[str]] = {}
    for table, _job_type in DAILY_GAP_JOB_CHAIN:
        files = partitions.get(table) or {}
        if not files:
            # 冷启动仍由资产检查链负责；无历史基线时不臆造回补区间。
            continue
        first = min(files)
        expected = [d for d in open_days if first <= d <= expected_end]
        missing = set(expected) - set(files)
        for date, path in files.items():
            if date > expected_end:
                continue
            rows = _partition_row_count(path)
            if rows is None:
                missing.add(date)
                continue
            if table == "daily_history/daily":
                continue
            benchmark = daily_counts.get(date)
            if benchmark and rows < benchmark * PARTITION_COMPLETE_RATIO:
                missing.add(date)
        if missing:
            gaps[table] = sorted(missing)
    return gaps


def _iso_date(ymd: str) -> str:
    clean = str(ymd).replace("-", "")
    return f"{clean[:4]}-{clean[4:6]}-{clean[6:8]}"


def plan_coverage_gap_jobs(gaps: Mapping[str, Sequence[str]]) -> List[Dict[str, Any]]:
    """历史缺口 → 带显式区间的候选任务，顺序保持上游到下游。"""
    specs: List[Dict[str, Any]] = []
    for table, _old_job in DAILY_GAP_JOB_CHAIN:
        dates = sorted(set(str(d).replace("-", "") for d in gaps.get(table, [])))
        if not dates:
            continue
        params = {"start_date": _iso_date(dates[0]), "end_date": _iso_date(dates[-1])}
        job_types = COVERAGE_JOB_CANDIDATES[table]
        if table == "cyq_perf/daily":
            # 本地模型必须从完整预热窗计算；Tushare 备用只拉目标缺口区间。
            candidates = [(job_types[0], {"end_date": params["end_date"]})]
            candidates.extend((job, dict(params)) for job in job_types[1:])
        else:
            candidates = [(job, dict(params)) for job in job_types]
        specs.append({
            "table": table,
            "job_type": candidates[0][0],
            "params": candidates[0][1],
            "candidates": candidates,
        })
    return specs


def _default_job_candidates(job_type: str, params: Optional[Dict[str, Any]] = None):
    params = dict(params or {})
    return [(job_type, params)] + [
        (fallback, dict(params)) for fallback in JOB_SOURCE_FALLBACKS.get(job_type, ())
    ]


def _wait_for_terminal(
    service,
    run_id: int,
    *,
    sleep: Callable[[float], None],
    poll_interval: float,
    timeout: float,
) -> str:
    """轮询 run 到终态，超时返回 'timeout'。"""
    deadline = time.monotonic() + timeout
    while True:
        run = service.get_run(run_id)
        status = getattr(run, "status", None)
        if status in TERMINAL_STATUSES:
            return status
        if time.monotonic() >= deadline:
            return "timeout"
        sleep(poll_interval)


def run_auto_init(
    service,
    inspect_fn: Callable[[], Tuple[bool, set, set]],
    *,
    stale_tables_fn: Optional[Callable[[], List[str]]] = None,
    gap_details_fn: Optional[Callable[[], Dict[str, List[str]]]] = None,
    sleep: Callable[[float], None] = time.sleep,
    poll_interval: float = DEFAULT_POLL_INTERVAL,
    run_timeout: float = DEFAULT_RUN_TIMEOUT,
    checkpoint_path: Optional[Path] = None,
    run_date: Optional[str] = None,
) -> Dict[str, object]:
    """串行执行补齐链；详细缺口路径支持复扫验证和候选源切换。"""
    connected, existing, non_empty = inspect_fn()
    if not connected:
        logger.warning("[auto-init] 数据目录不可用，跳过本次初始化")
        return {"status": "skipped", "reason": "data_dir_unavailable", "completed": []}

    missing = [asset for asset in PARQUET_ASSETS if asset not in existing]
    empty = [asset for asset in PARQUET_ASSETS if asset in existing and asset not in non_empty]
    jobs = plan_init_jobs(missing, empty)
    # 旧资产/最新断档路径保持原有单生产者行为，避免改变既有调用方合约。
    candidates_by_job: Dict[str, List[Tuple[str, Dict[str, Any]]]] = {
        job: [(job, {})] for job in jobs
    }
    gap_table_by_job: Dict[str, str] = {}

    if gap_details_fn is not None:
        try:
            details = gap_details_fn() or {}
            gap_specs = plan_coverage_gap_jobs(details)
        except Exception as exc:  # noqa: BLE001 - 扫描失败不得误判数据完整
            logger.warning(f"[auto-init] 历史完整性扫描失败: {exc}")
            return {"status": "failed", "reason": "coverage_scan_failed", "completed": []}
        if gap_specs:
            gap_jobs = []
            for spec in gap_specs:
                job = str(spec["job_type"])
                gap_jobs.append(job)
                candidates_by_job[job] = list(spec["candidates"])
                gap_table_by_job[job] = str(spec["table"])
                if job not in jobs:
                    jobs.append(job)
            logger.info(
                f"[auto-init] 日频历史缺口: {list(details)}，追加显式回补作业: {gap_jobs}"
            )
    elif stale_tables_fn is not None:
        # 兼容旧调用方：仅最新日期断档，不启用新候选源语义。
        try:
            stale = list(stale_tables_fn() or [])
        except Exception as exc:  # noqa: BLE001 - 断档检查失败不应阻断资产链
            logger.warning(f"[auto-init] 断档检查失败，跳过: {exc}")
            stale = []
        if stale:
            gap_jobs = [j for j in plan_gap_fill_jobs(stale) if j not in jobs]
            logger.info(f"[auto-init] 日频断档: {stale}，追加回补作业: {gap_jobs}")
            jobs = jobs + gap_jobs
            for job in gap_jobs:
                candidates_by_job[job] = [(job, {})]

    maintenance_jobs = [j for j in NIGHTLY_MAINTENANCE_JOB_CHAIN if j not in jobs]
    if maintenance_jobs:
        logger.info(f"[auto-init] 追加每晚例行刷新: {maintenance_jobs}")
        jobs = jobs + maintenance_jobs
        for job in maintenance_jobs:
            candidates_by_job[job] = _default_job_candidates(job)

    if not jobs:
        if checkpoint_path:
            clear_checkpoint(checkpoint_path)
        return {"status": "ok", "completed": []}

    logger.info(f"[auto-init] 缺失资产: {missing + empty}，执行任务链: {jobs}")
    run_date = run_date or datetime.now().date().isoformat()
    completed: List[str] = []
    if checkpoint_path:
        checkpoint = load_checkpoint(checkpoint_path)
        if checkpoint.get("run_date") == run_date:
            completed = [
                str(job) for job in checkpoint.get("completed_jobs", [])
                if str(job) in jobs
                and not (
                    str(job) in gap_table_by_job
                    and details.get(gap_table_by_job[str(job)])
                )
            ]
        if completed:
            logger.info(f"[auto-init] 从链级检查点续跑，已完成: {completed}")
        save_checkpoint(checkpoint_path, {
            "version": 1,
            "run_date": run_date,
            "planned_jobs": jobs,
            "completed_jobs": completed,
        })

    wait_timeout = get_retry_policy().total_timeout(run_timeout)
    for job_type in jobs:
        if job_type in completed:
            continue
        attempted_jobs: List[str] = []
        final = "failed"
        succeeded = False
        candidates = candidates_by_job.get(job_type) or [(job_type, {})]
        for candidate_job, params in candidates:
            attempted_jobs.append(candidate_job)
            try:
                run = service.submit(candidate_job, params)
            except ValueError as exc:
                # 同类型任务正在跑时不能并发切换另一个源写同一张表。
                if "duplicate" in str(exc):
                    logger.info(f"[auto-init] {candidate_job} 已有在跑任务，本次礼让: {exc}")
                    return {"status": "busy", "completed": completed}
                raise
            logger.info(f"[auto-init] 已提交 {candidate_job} (run_id={run.id})，等待完成…")
            final = _wait_for_terminal(
                service, run.id,
                sleep=sleep, poll_interval=poll_interval, timeout=wait_timeout,
            )
            if final != "success":
                logger.warning(f"[auto-init] {candidate_job} 未成功（{final}）")
                continue

            table = gap_table_by_job.get(job_type)
            if table and gap_details_fn is not None:
                try:
                    residual = (gap_details_fn() or {}).get(table, [])
                except Exception as exc:  # noqa: BLE001 - 无法验证就不能记成功
                    logger.warning(f"[auto-init] {candidate_job} 后复扫失败: {exc}")
                    final = "verification_failed"
                    continue
                if residual:
                    logger.warning(
                        f"[auto-init] {candidate_job} 返回成功但 {table} 仍有 "
                        f"{len(residual)} 个缺口，尝试下一数据源"
                    )
                    final = "incomplete"
                    continue
            succeeded = True
            break

        if not succeeded:
            logger.warning(f"[auto-init] 逻辑节点 {job_type} 所有候选均未完成，链路中断")
            return {
                "status": "failed",
                "failed_job": job_type,
                "final_status": final,
                "attempted_jobs": attempted_jobs,
                "completed": completed,
            }
        completed.append(job_type)
        if checkpoint_path:
            # 仅在执行成功且详细缺口复扫通过后推进链级游标。
            save_checkpoint(checkpoint_path, {
                "version": 1,
                "run_date": run_date,
                "planned_jobs": jobs,
                "completed_jobs": completed,
            })

    if checkpoint_path:
        clear_checkpoint(checkpoint_path)
    logger.info(f"[auto-init] 补齐链全部完成: {completed}")
    return {"status": "success", "completed": completed}


def parse_run_time(value: str) -> Tuple[int, int]:
    """解析 'HH:MM'，非法输入抛 ValueError（配置错误应快速失败）。"""
    try:
        hour_text, minute_text = str(value).strip().split(":")
        hour, minute = int(hour_text), int(minute_text)
    except (ValueError, AttributeError):
        raise ValueError(f"无效的 AUTO_INIT_TIME: {value!r}，应为 HH:MM")
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(f"无效的 AUTO_INIT_TIME: {value!r}，应为 HH:MM")
    return hour, minute


class AutoInitScheduler:
    """进程内每日调度：到点跑一次 run_auto_init，状态落盘防同日重复。"""

    def __init__(
        self,
        app,
        *,
        run_time: str = DEFAULT_RUN_TIME,
        state_path: Optional[Path] = None,
        check_interval: float = 60.0,
    ) -> None:
        self.app = app
        self.run_time = parse_run_time(run_time)
        self.state_path = Path(state_path) if state_path else self._default_state_path(app)
        self.check_interval = check_interval
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        # 进程内串行锁：调度线程与启动补跑不得并发
        self._run_lock = threading.Lock()

    @staticmethod
    def _default_state_path(app) -> Path:
        data_dir = None
        if app is not None:
            data_dir = app.config.get("DATA_DIR")
        root = Path(data_dir) if data_dir else Path("data")
        return root / "data_job_state" / "auto_init_state.json"

    @property
    def progress_path(self) -> Path:
        return self.state_path.with_name("auto_init_progress.json")

    # -- 状态读写 -----------------------------------------------------------

    def load_state(self) -> Dict[str, object]:
        try:
            return json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}

    def mark_run(self, now: datetime, result: Dict[str, object]) -> None:
        state = {"last_run_date": now.date().isoformat(), "last_result": result}
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            self.state_path.write_text(
                json.dumps(state, ensure_ascii=False), encoding="utf-8"
            )
        except OSError as exc:
            logger.warning(f"[auto-init] 状态落盘失败（{exc}），明天可能重复检查")

    # -- 判定 ---------------------------------------------------------------

    def is_due(self, now: datetime, state: Dict[str, object]) -> bool:
        if state.get("last_run_date") == now.date().isoformat():
            return False
        return (now.hour, now.minute) >= self.run_time

    # -- 执行 ---------------------------------------------------------------

    def run_once(self, now: Optional[datetime] = None) -> Optional[Dict[str, object]]:
        """到点执行一次（立即 mark，失败也当天不重试，避免失败任务刷频）。"""
        now = now or datetime.now()
        if not self._run_lock.acquire(blocking=False):
            logger.info("[auto-init] 另一次初始化正在进行，本次跳过")
            return None
        try:
            result = self._execute(run_date=now.date().isoformat())
        finally:
            self._run_lock.release()
        self.mark_run(now, result)
        return result

    def _execute_with_health(
        self,
        run_fn: Callable[[], Dict[str, object]],
        service,
        now: Optional[datetime] = None,
        data_dir: Optional[str] = None,
    ) -> Dict[str, object]:
        """执行补齐链并附加健康快照。

        心跳检查以"本次巡检正在发生"为准（mark_run 在返回后才落盘，
        直接读状态文件会把本次误判为缺席）。健康快照失败不阻断巡检结果。
        """
        result = run_fn()
        try:
            from app.services.data_jobs.health import collect_health_report

            now = now or datetime.now()
            health = collect_health_report(
                service,
                data_dir=data_dir,
                now=now,
                auto_init_state={"last_run_date": now.date().isoformat()},
            )
            result["health"] = {"status": health["status"]}
            if health["status"] != "ok":
                logger.warning(f"[auto-init] 健康快照 {health['status']}: {health['checks']}")
        except Exception as exc:  # noqa: BLE001 - 健康快照失败不阻断巡检
            logger.warning(f"[auto-init] 健康快照失败: {exc}")
        return result

    def _execute(self, run_date: Optional[str] = None) -> Dict[str, object]:
        # 延迟导入：模块加载不触碰 Flask/service，保持纯函数可测
        from app.api.data_jobs_api import get_data_job_service
        from startup_runtime import inspect_parquet_data_assets

        data_dir = self.app.config.get("DATA_DIR") if self.app is not None else None
        service = get_data_job_service()
        try:
            return self._execute_with_health(
                lambda: run_auto_init(
                    service,
                    lambda: inspect_parquet_data_assets(data_dir),
                    gap_details_fn=lambda: find_daily_coverage_gaps(data_dir),
                    checkpoint_path=self.progress_path,
                    run_date=run_date,
                ),
                service=service,
                data_dir=data_dir,
            )
        except Exception as exc:  # noqa: BLE001 - 调度线程必须兜底
            logger.error(f"[auto-init] 初始化异常: {exc}")
            return {"status": "failed", "error": str(exc), "completed": []}

    def _loop(self) -> None:
        while not self._stop.wait(self.check_interval):
            try:
                now = datetime.now()
                if self.is_due(now, self.load_state()):
                    logger.info(f"[auto-init] 到达计划时间 {self.run_time[0]:02d}:{self.run_time[1]:02d}，开始检查")
                    self.run_once(now)
            except Exception as exc:  # noqa: BLE001 - 循环不得因单次异常退出
                logger.error(f"[auto-init] 调度循环异常: {exc}")

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._loop, name="auto-init-scheduler", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None


def start_auto_init(app) -> Optional[AutoInitScheduler]:
    """run.py 启动钩子：返回调度器实例（禁用时返回 None）。

    - AUTO_INIT_ENABLED=false：整体关闭；
    - AUTO_INIT_ON_STARTUP=true（默认）：启动时若资产缺失，后台立即补跑一次，
      新部署不必干等到半夜；资产齐全则零动作；
    - 夜间调度：每天 AUTO_INIT_TIME（默认 02:30）检查一次。
    """
    if not app.config.get("AUTO_INIT_ENABLED", True):
        logger.info("[auto-init] 已禁用（AUTO_INIT_ENABLED=false）")
        return None

    scheduler = AutoInitScheduler(
        app,
        run_time=str(app.config.get("AUTO_INIT_TIME", DEFAULT_RUN_TIME)),
    )
    scheduler.start()
    logger.info(
        f"[auto-init] 夜间调度已启动，每日 {app.config.get('AUTO_INIT_TIME', DEFAULT_RUN_TIME)} 检查数据资产"
    )

    if app.config.get("AUTO_INIT_ON_STARTUP", True):
        def _startup_catchup() -> None:
            from startup_runtime import inspect_parquet_data_assets

            data_dir = app.config.get("DATA_DIR")
            if scheduler.progress_path.is_file():
                logger.info("[auto-init] 启动检查发现未完成链级检查点，从上次节点续跑")
                if scheduler._run_lock.acquire(blocking=False):
                    try:
                        scheduler._execute()
                    finally:
                        scheduler._run_lock.release()
                return
            try:
                connected, existing, non_empty = inspect_parquet_data_assets(data_dir)
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"[auto-init] 启动检查失败: {exc}")
                return
            missing = [a for a in PARQUET_ASSETS if a not in existing]
            empty = [a for a in PARQUET_ASSETS if a in existing and a not in non_empty]
            if not connected:
                logger.warning("[auto-init] 启动检查：数据目录不可用，等待夜间调度")
                return
            stale = find_daily_coverage_gaps(data_dir)
            if not missing and not empty and not stale:
                logger.info("[auto-init] 启动检查：数据资产完整且无断档，无需补跑")
                return
            logger.info(
                f"[auto-init] 启动检查：缺失 {missing + empty}，断档 {list(stale)}，后台补跑"
            )
            # 补跑不占调度器的当日额度（不 mark_run），夜里仍按原计划巡检
            if scheduler._run_lock.acquire(blocking=False):
                try:
                    scheduler._execute()
                finally:
                    scheduler._run_lock.release()

        threading.Thread(
            target=_startup_catchup, name="auto-init-startup", daemon=True
        ).start()

    return scheduler
