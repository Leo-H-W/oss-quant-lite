"""夜间自动初始化（auto-init）合约测试。

覆盖：
- text2sql_wide_table 任务注册（对 UI 不可见，保持页面 12 个可见任务不变）
- 缺失资产 → 任务链的纯函数规划（顺序、空资产视同缺失）
- run_auto_init 编排：串行提交、失败即中断、健康时零提交
- 调度器 due 判定：到点触发、同日只跑一次
"""
from datetime import datetime
from pathlib import Path

import pytest

from app.services.data_jobs.auto_init import (
    AutoInitScheduler,
    plan_init_jobs,
    run_auto_init,
)
from app.services.data_jobs.registry import JobRegistry

pytestmark = pytest.mark.module_data_jobs

PROJECT_ROOT = Path(__file__).resolve().parents[2]


# ---------------------------------------------------------------------------
# 注册表合约
# ---------------------------------------------------------------------------

def test_text2sql_wide_table_registered_as_derived_job():
    registry = JobRegistry()
    job = registry.get_job("text2sql_wide_table")

    assert job.source_mode == "derived"
    assert job.script_path == "scripts/build_text2sql_wide_tables.py"
    assert (PROJECT_ROOT / job.script_path).is_file()
    assert "daily_history_by_date" in job.dependencies
    assert "daily_basic" in job.dependencies


def test_text2sql_wide_table_hidden_from_ui_job_list():
    """新任务只供自动初始化链路提交，不得出现在页面可见任务列表，
    避免改变数据管理页现有 UI（可见任务数恒为 12）。"""
    registry = JobRegistry()
    visible_types = {job.job_type for job in registry.list_visible_jobs()}

    assert "text2sql_wide_table" not in visible_types
    assert len(visible_types) == 12


# ---------------------------------------------------------------------------
# 规划函数合约
# ---------------------------------------------------------------------------

def test_plan_init_jobs_returns_empty_when_all_ready():
    assert plan_init_jobs(missing_assets=[], empty_assets=[]) == []


def test_plan_init_jobs_orders_by_dependency():
    jobs = plan_init_jobs(
        missing_assets=[
            "stock_business.parquet",
            "daily_history/daily",
            "stock_trade_calendar.parquet",
        ],
        empty_assets=[],
    )
    assert jobs == ["trade_calendar", "daily_history_by_date", "text2sql_wide_table"]


def test_plan_init_jobs_treats_empty_as_missing():
    jobs = plan_init_jobs(missing_assets=[], empty_assets=["stock_basic.parquet"])
    assert jobs == ["stock_basic"]


def test_plan_init_jobs_wide_table_only():
    jobs = plan_init_jobs(missing_assets=["stock_business.parquet"], empty_assets=[])
    assert jobs == ["text2sql_wide_table"]


def test_plan_init_jobs_full_chain():
    jobs = plan_init_jobs(
        missing_assets=[
            "stock_basic.parquet",
            "stock_trade_calendar.parquet",
            "daily_history/daily",
            "daily_basic/daily",
            "stock_business.parquet",
        ],
        empty_assets=[],
    )
    assert jobs == [
        "trade_calendar",
        "stock_basic",
        "daily_history_by_date",
        "daily_basic",
        "text2sql_wide_table",
    ]


# ---------------------------------------------------------------------------
# 编排合约（假 service，不触碰真实状态存储）
# ---------------------------------------------------------------------------

class _FakeRun:
    def __init__(self, run_id, job_type, final_status="success"):
        self.id = run_id
        self.job_type = job_type
        self.status = "queued"
        self._final_status = final_status


class _FakeService:
    """submit 后轮询一次即达终态的假 service。"""

    def __init__(self, fail_on=None):
        self.submitted = []
        self._runs = {}
        self._fail_on = fail_on

    def submit(self, job_type, params):
        self.submitted.append(job_type)
        run = _FakeRun(
            len(self.submitted),
            job_type,
            final_status="failed" if job_type == self._fail_on else "success",
        )
        self._runs[run.id] = run
        return run

    def get_run(self, run_id):
        run = self._runs[run_id]
        run.status = run._final_status
        return run


def _healthy_inspect():
    return True, set(_ALL_ASSETS), set(_ALL_ASSETS)


_ALL_ASSETS = (
    "stock_basic.parquet",
    "stock_trade_calendar.parquet",
    "daily_history/daily",
    "daily_basic/daily",
    "stock_business.parquet",
)


def _missing_wide_table_inspect():
    existing = set(_ALL_ASSETS) - {"stock_business.parquet"}
    return True, existing, existing


def test_run_auto_init_runs_nightly_maintenance_when_healthy():
    service = _FakeService()
    result = run_auto_init(service, _healthy_inspect, sleep=lambda _: None)

    assert result["status"] == "success"
    assert service.submitted == [
        "stock_basic_fuyao",
        "financial_fuyao",
        "wide_table_builder",
        "factor_compute",
        "text2sql_wide_table",
    ]
    assert result["completed"] == service.submitted


def test_run_auto_init_submits_missing_chain_in_order():
    service = _FakeService()
    result = run_auto_init(service, _missing_wide_table_inspect, sleep=lambda _: None)

    assert result["status"] == "success"
    assert service.submitted == [
        "text2sql_wide_table",
        "stock_basic_fuyao",
        "financial_fuyao",
        "wide_table_builder",
        "factor_compute",
    ]
    assert result["completed"] == service.submitted


def test_run_auto_init_nightly_failure_interrupts_derived_chain():
    service = _FakeService(fail_on="wide_table_builder")
    result = run_auto_init(service, _healthy_inspect, sleep=lambda _: None)

    assert result["status"] == "failed"
    assert result["failed_job"] == "wide_table_builder"
    assert service.submitted == [
        "stock_basic_fuyao", "financial_fuyao", "wide_table_builder",
    ]


def test_run_auto_init_stops_chain_on_failure():
    def inspect():
        return True, set(), set()  # 全缺 → 完整链

    service = _FakeService(fail_on="stock_basic")
    result = run_auto_init(service, inspect, sleep=lambda _: None)

    assert result["status"] == "failed"
    assert result["failed_job"] == "stock_basic"
    # stock_basic 失败后不得再提交下游任务
    assert service.submitted == ["trade_calendar", "stock_basic"]


def test_run_auto_init_skips_when_data_dir_unavailable():
    service = _FakeService()
    result = run_auto_init(service, lambda: (False, set(), set()), sleep=lambda _: None)

    assert result["status"] == "skipped"
    assert service.submitted == []


def test_run_auto_init_tolerates_duplicate_submission():
    """页面手动跑同类型任务时 submit 抛 duplicate：视为他处在跑，不视作失败。"""

    class _BusyService(_FakeService):
        def submit(self, job_type, params):
            raise ValueError("duplicate running job: 7")

    result = run_auto_init(
        _BusyService(), _missing_wide_table_inspect, sleep=lambda _: None
    )
    assert result["status"] == "busy"
    assert result["completed"] == []


# ---------------------------------------------------------------------------
# 调度器 due 判定合约
# ---------------------------------------------------------------------------

def _scheduler(tmp_path, run_time="02:30"):
    return AutoInitScheduler(
        app=None,
        run_time=run_time,
        state_path=tmp_path / "auto_init_state.json",
        check_interval=60,
    )


def test_scheduler_due_after_configured_time(tmp_path):
    sched = _scheduler(tmp_path)
    now = datetime(2026, 10, 11, 2, 30)
    assert sched.is_due(now, {}) is True


def test_scheduler_not_due_before_configured_time(tmp_path):
    sched = _scheduler(tmp_path)
    now = datetime(2026, 10, 11, 2, 29)
    assert sched.is_due(now, {}) is False


def test_scheduler_not_due_twice_same_day(tmp_path):
    sched = _scheduler(tmp_path)
    now = datetime(2026, 10, 11, 3, 0)
    assert sched.is_due(now, {"last_run_date": "2026-10-11"}) is False
    # 换天后再次到点
    next_day = datetime(2026, 10, 12, 2, 31)
    assert sched.is_due(next_day, {"last_run_date": "2026-10-11"}) is True


def test_scheduler_state_roundtrip(tmp_path):
    sched = _scheduler(tmp_path)
    assert sched.load_state() == {}
    sched.mark_run(datetime(2026, 10, 11, 2, 30), {"status": "ok"})
    state = sched.load_state()
    assert state["last_run_date"] == "2026-10-11"
    assert state["last_result"]["status"] == "ok"


def test_scheduler_rejects_invalid_run_time(tmp_path):
    with pytest.raises(ValueError):
        _scheduler(tmp_path, run_time="25:99")


# ---------------------------------------------------------------------------
# 日期连续性检查（断档感知）：最新分区落后于最近开市日 → 触发回补作业
# ---------------------------------------------------------------------------

import pandas as pd

from app.services.data_jobs.auto_init import (
    find_stale_daily_tables,
    plan_gap_fill_jobs,
)


def _write_calendar(root, open_days):
    cal = pd.DataFrame({
        "exchange": ["SSE"] * len(open_days),
        "cal_date": open_days,
        "is_open": [1] * len(open_days),
        "pretrade_date": [""] * len(open_days),
    })
    cal.to_parquet(root / "stock_trade_calendar.parquet")


def _touch_partition(root, rel_table, ymd):
    part = (
        root / rel_table
        / f"year={ymd[:4]}" / f"month={ymd[4:6]}" / f"day={ymd[6:8]}"
    )
    part.mkdir(parents=True)


def test_find_stale_daily_tables_detects_lag(tmp_path):
    _write_calendar(tmp_path, ["20261021", "20261022", "20261023"])
    _touch_partition(tmp_path, "daily_history/daily", "20261009")
    _touch_partition(tmp_path, "daily_basic/daily", "20261023")

    stale = find_stale_daily_tables(tmp_path, now=datetime(2026, 10, 24, 2, 30))

    assert stale == ["daily_history/daily"]


def test_find_stale_daily_tables_fresh_is_not_stale(tmp_path):
    _write_calendar(tmp_path, ["20261022", "20261023"])
    _touch_partition(tmp_path, "daily_history/daily", "20261023")
    _touch_partition(tmp_path, "daily_basic/daily", "20261023")

    assert find_stale_daily_tables(tmp_path, now=datetime(2026, 10, 24, 2, 30)) == []


def test_find_stale_daily_tables_before_cutoff_compares_to_previous_open_day(tmp_path):
    """凌晨 02:30 检查时当日数据尚未发布，期望值须回退到上一开市日，
    否则每天夜里都会误报断档并空拉当日（数据不存在）。"""
    _write_calendar(tmp_path, ["20261023", "20261024"])
    _touch_partition(tmp_path, "daily_history/daily", "20261023")
    _touch_partition(tmp_path, "daily_basic/daily", "20261023")

    assert find_stale_daily_tables(tmp_path, now=datetime(2026, 10, 24, 2, 30)) == []


def test_find_stale_daily_tables_after_cutoff_expects_today(tmp_path):
    """18:00 后当日数据应已就绪，缺当日分区即断档。"""
    _write_calendar(tmp_path, ["20261023", "20261024"])
    _touch_partition(tmp_path, "daily_history/daily", "20261023")
    _touch_partition(tmp_path, "daily_basic/daily", "20261024")

    stale = find_stale_daily_tables(tmp_path, now=datetime(2026, 10, 24, 20, 0))

    assert stale == ["daily_history/daily"]


def test_find_stale_daily_tables_skips_missing_dir(tmp_path):
    """目录缺失由资产检查链负责，断档检查不重复提交同一作业。"""
    _write_calendar(tmp_path, ["20261023"])
    _touch_partition(tmp_path, "daily_basic/daily", "20261023")

    assert find_stale_daily_tables(tmp_path, now=datetime(2026, 10, 24, 2, 30)) == []


def test_plan_gap_fill_jobs_maps_stale_tables_in_chain_order():
    assert plan_gap_fill_jobs(["daily_basic/daily", "daily_history/daily"]) == [
        "daily_history_by_date",
        "daily_basic_baostock",
    ]
    assert plan_gap_fill_jobs([]) == []


def test_plan_gap_fill_jobs_covers_alternative_producers():
    """四张 tushare 高积分/限频表全部映射到替代生产者。"""
    assert plan_gap_fill_jobs([
        "stk_factor/daily", "moneyflow/daily", "cyq_perf/daily",
    ]) == ["adj_factor_baostock", "moneyflow_tdx", "cyq_perf_compute"]


def test_run_auto_init_gap_fill_runs_when_assets_healthy():
    service = _FakeService()
    result = run_auto_init(
        service,
        _healthy_inspect,
        stale_tables_fn=lambda: ["daily_history/daily"],
        sleep=lambda _: None,
    )

    assert result["status"] == "success"
    assert service.submitted == [
        "daily_history_by_date",
        "stock_basic_fuyao",
        "financial_fuyao",
        "wide_table_builder",
        "factor_compute",
        "text2sql_wide_table",
    ]
    assert result["completed"] == service.submitted


def test_run_auto_init_no_raw_gap_jobs_when_fresh():
    service = _FakeService()
    result = run_auto_init(
        service,
        _healthy_inspect,
        stale_tables_fn=lambda: [],
        sleep=lambda _: None,
    )

    assert result["status"] == "success"
    assert "daily_history_by_date" not in service.submitted
    assert "daily_basic_baostock" not in service.submitted
    assert service.submitted[-2:] == ["factor_compute", "text2sql_wide_table"]


def test_run_auto_init_gap_fill_failure_interrupts():
    service = _FakeService(fail_on="daily_history_by_date")
    result = run_auto_init(
        service,
        _healthy_inspect,
        stale_tables_fn=lambda: ["daily_history/daily", "daily_basic/daily"],
        sleep=lambda _: None,
    )

    assert result["status"] == "failed"
    assert service.submitted == ["daily_history_by_date"]


def test_run_auto_init_dedupes_gap_job_already_in_asset_chain():
    """资产链已含 daily_history_by_date 时，断档检查不得重复提交。"""
    existing = set(_ALL_ASSETS) - {"daily_history/daily"}
    inspect = lambda: (True, existing, existing)  # noqa: E731
    service = _FakeService()
    result = run_auto_init(
        service,
        inspect,
        stale_tables_fn=lambda: ["daily_history/daily"],
        sleep=lambda _: None,
    )

    assert result["status"] == "success"
    assert service.submitted[0] == "daily_history_by_date"
    assert service.submitted.count("daily_history_by_date") == 1
    assert service.submitted[1:] == [
        "stock_basic_fuyao",
        "financial_fuyao",
        "wide_table_builder",
        "factor_compute",
        "text2sql_wide_table",
    ]
