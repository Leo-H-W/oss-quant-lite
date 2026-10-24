"""夜间自动数据初始化（auto-init）。

背景：数据管理页的「需要初始化」徽标由 startup_runtime.PARQUET_ASSETS 五项
资产检查驱动，其中 stock_business.parquet 只能由 scripts/build_text2sql_wide_tables.py
物化，页面上任何手动任务都补不上，导致每次打开页面都提示初始化。

本模块把补齐动作自动化：
- plan_init_jobs：纯函数，缺失/空资产 → 按依赖序的任务链；
- run_auto_init：串行编排，submit 后轮询终态再提下一个（控制取数频次，
  任何时刻只有一个任务在跑），失败即中断，他处在跑同类型任务时礼让；
- AutoInitScheduler：进程内轻量调度线程，每天到点（默认 02:30）检查一次，
  资产齐全时零 API 调用；状态落盘保证重启当天不重复跑。

只在 run.py 主进程启动（start_auto_init），不挂 create_app——
数据任务线程会调 create_app，挂那里会繁殖调度线程。
"""
from __future__ import annotations

import json
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from loguru import logger

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
DAILY_GAP_JOB_CHAIN: Tuple[Tuple[str, str], ...] = (
    ("daily_history/daily", "daily_history_by_date"),
    ("daily_basic/daily", "daily_basic"),
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
    sleep: Callable[[float], None] = time.sleep,
    poll_interval: float = DEFAULT_POLL_INTERVAL,
    run_timeout: float = DEFAULT_RUN_TIMEOUT,
) -> Dict[str, object]:
    """串行执行补齐链。返回结果 dict（status: ok/success/failed/skipped/busy）。"""
    connected, existing, non_empty = inspect_fn()
    if not connected:
        logger.warning("[auto-init] 数据目录不可用，跳过本次初始化")
        return {"status": "skipped", "reason": "data_dir_unavailable", "completed": []}

    missing = [asset for asset in PARQUET_ASSETS if asset not in existing]
    empty = [asset for asset in PARQUET_ASSETS if asset in existing and asset not in non_empty]
    jobs = plan_init_jobs(missing, empty)

    # 日期断档检查：资产齐全但分区日期落后时追加日更回补作业
    if stale_tables_fn is not None:
        try:
            stale = list(stale_tables_fn() or [])
        except Exception as exc:  # noqa: BLE001 - 断档检查失败不应阻断资产链
            logger.warning(f"[auto-init] 断档检查失败，跳过: {exc}")
            stale = []
        if stale:
            gap_jobs = [j for j in plan_gap_fill_jobs(stale) if j not in jobs]
            logger.info(f"[auto-init] 日频断档: {stale}，追加回补作业: {gap_jobs}")
            jobs = jobs + gap_jobs

    if not jobs:
        return {"status": "ok", "completed": []}

    logger.info(f"[auto-init] 缺失资产: {missing + empty}，补齐链: {jobs}")
    completed: List[str] = []
    for job_type in jobs:
        try:
            run = service.submit(job_type, {})
        except ValueError as exc:
            # 页面上有人手动跑了同类型任务：礼让，不算失败，明天夜里再补
            if "duplicate" in str(exc):
                logger.info(f"[auto-init] {job_type} 已有在跑任务，本次礼让: {exc}")
                return {"status": "busy", "completed": completed}
            raise
        logger.info(f"[auto-init] 已提交 {job_type} (run_id={run.id})，等待完成…")
        final = _wait_for_terminal(
            service, run.id,
            sleep=sleep, poll_interval=poll_interval, timeout=run_timeout,
        )
        if final != "success":
            logger.warning(
                f"[auto-init] {job_type} (run_id={run.id}) 未成功（{final}），链路中断"
            )
            return {
                "status": "failed",
                "failed_job": job_type,
                "final_status": final,
                "completed": completed,
            }
        completed.append(job_type)

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
            result = self._execute()
        finally:
            self._run_lock.release()
        self.mark_run(now, result)
        return result

    def _execute(self) -> Dict[str, object]:
        # 延迟导入：模块加载不触碰 Flask/service，保持纯函数可测
        from app.api.data_jobs_api import get_data_job_service
        from startup_runtime import inspect_parquet_data_assets

        data_dir = self.app.config.get("DATA_DIR") if self.app is not None else None
        try:
            return run_auto_init(
                get_data_job_service(),
                lambda: inspect_parquet_data_assets(data_dir),
                stale_tables_fn=lambda: find_stale_daily_tables(data_dir),
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
            stale = find_stale_daily_tables(data_dir)
            if not missing and not empty and not stale:
                logger.info("[auto-init] 启动检查：数据资产齐全且无断档，无需补跑")
                return
            logger.info(
                f"[auto-init] 启动检查：缺失 {missing + empty}，断档 {stale}，后台补跑"
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
