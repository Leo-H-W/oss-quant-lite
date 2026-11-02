"""夜间历史完整性扫描、显式回补与数据源故障切换合约。"""
from datetime import datetime
import json

import pandas as pd
import pytest

from app.services.data_jobs.auto_init import (
    find_daily_coverage_gaps,
    plan_coverage_gap_jobs,
    run_auto_init,
)

pytestmark = pytest.mark.module_data_jobs


def _calendar(root, dates):
    pd.DataFrame({
        "exchange": ["SSE"] * len(dates),
        "cal_date": dates,
        "is_open": [1] * len(dates),
        "pretrade_date": [""] * len(dates),
    }).to_parquet(root / "stock_trade_calendar.parquet")


def _partition(root, table, ymd, rows=10):
    part = root / table / f"year={ymd[:4]}" / f"month={ymd[4:6]}" / f"day={ymd[6:8]}"
    part.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"ts_code": [f"S{i}" for i in range(rows)]}).to_parquet(part / "data.parquet")


def test_coverage_scan_finds_internal_latest_and_incomplete_partitions(tmp_path):
    dates = ["20260102", "20260105", "20260106", "20260107", "20260108"]
    _calendar(tmp_path, dates)
    for date in dates:
        _partition(tmp_path, "daily_history/daily", date)
    _partition(tmp_path, "daily_basic/daily", "20260105")
    _partition(tmp_path, "daily_basic/daily", "20260107", rows=5)

    gaps = find_daily_coverage_gaps(tmp_path, now=datetime(2026, 1, 9, 2, 30))

    # 最早分区 01-05 之前的 01-02 不自动扩展；中间缺日、最新缺日和残缺都纳入。
    assert gaps == {"daily_basic/daily": ["20260106", "20260107", "20260108"]}


def test_coverage_scan_treats_corrupt_partition_as_gap(tmp_path):
    dates = ["20260105", "20260106"]
    _calendar(tmp_path, dates)
    for date in dates:
        _partition(tmp_path, "daily_history/daily", date)
        _partition(tmp_path, "moneyflow/daily", date)
    bad = tmp_path / "moneyflow/daily/year=2026/month=01/day=06/data.parquet"
    bad.write_bytes(b"not parquet")

    gaps = find_daily_coverage_gaps(tmp_path, now=datetime(2026, 1, 7, 2, 30))

    assert gaps["moneyflow/daily"] == ["20260106"]


def test_coverage_scan_preserves_data_ready_cutoff(tmp_path):
    dates = ["20260108", "20260109"]
    _calendar(tmp_path, dates)
    _partition(tmp_path, "daily_history/daily", "20260108")

    assert find_daily_coverage_gaps(tmp_path, now=datetime(2026, 1, 9, 2, 30)) == {}
    assert find_daily_coverage_gaps(tmp_path, now=datetime(2026, 1, 9, 20, 0)) == {
        "daily_history/daily": ["20260109"]
    }


def test_gap_planner_uses_explicit_ranges_and_ordered_fallbacks():
    specs = plan_coverage_gap_jobs({
        "daily_basic/daily": ["20230103", "20231229"],
        "moneyflow/daily": ["20261027", "20261030"],
        "cyq_perf/daily": ["20150323", "20261030"],
        "daily_history/daily": ["20261029"],
    })
    by_table = {spec["table"]: spec for spec in specs}

    assert [spec["job_type"] for spec in specs] == [
        "daily_history_by_date", "daily_basic_baostock",
        "moneyflow_tdx_backfill", "cyq_perf_compute",
    ]
    assert by_table["daily_basic/daily"]["candidates"] == [
        ("daily_basic_baostock", {"start_date": "2023-01-03", "end_date": "2023-12-29"}),
        ("daily_basic", {"start_date": "2023-01-03", "end_date": "2023-12-29"}),
    ]
    assert by_table["moneyflow/daily"]["candidates"][0][0] == "moneyflow_tdx_backfill"
    assert by_table["moneyflow/daily"]["candidates"][1][0] == "moneyflow"
    assert by_table["daily_history/daily"]["candidates"][-1][0] == "daily_history_tencent"
    # 本地筹码复算保留预热窗；Tushare 备用源使用显式缺口区间。
    assert by_table["cyq_perf/daily"]["candidates"] == [
        ("cyq_perf_compute", {"end_date": "2026-10-30"}),
        ("cyq_perf", {"start_date": "2015-03-23", "end_date": "2026-10-30"}),
    ]


class _Run:
    def __init__(self, run_id, status):
        self.id = run_id
        self.status = status


class _Service:
    def __init__(self, outcomes, remaining, table):
        self.outcomes = outcomes
        self.remaining = remaining
        self.table = table
        self.submitted = []
        self.runs = {}

    def submit(self, job_type, params):
        self.submitted.append((job_type, params))
        status = self.outcomes.get(job_type, "success")
        run = _Run(len(self.submitted), status)
        self.runs[run.id] = run
        return run

    def get_run(self, run_id):
        run = self.runs[run_id]
        if run.status == "success" and self.table:
            # 每个候选成功后的复扫由测试场景控制：primary_ok 表示缺口消失。
            if self.outcomes.get("primary_ok"):
                self.remaining.clear()
        return run


def _inspect():
    assets = {
        "stock_basic.parquet", "stock_trade_calendar.parquet", "daily_history/daily",
        "daily_basic/daily", "stock_business.parquet",
    }
    return True, assets, assets


def _run_gap(service, remaining, table="daily_basic/daily", checkpoint_path=None):
    def details():
        return {table: sorted(remaining[table])} if remaining.get(table) else {}

    return run_auto_init(
        service,
        _inspect,
        gap_details_fn=details,
        sleep=lambda _: None,
        checkpoint_path=checkpoint_path,
    )


def test_failed_primary_switches_to_fallback_with_same_range(tmp_path):
    table = "daily_basic/daily"
    remaining = {table: ["20230103", "20231229"]}
    service = _Service({
        "daily_basic_baostock": "failed",
        "daily_basic": "success",
        "primary_ok": True,
    }, remaining, table)

    result = _run_gap(service, remaining, table, tmp_path / "progress.json")

    assert result["status"] == "success"
    assert service.submitted[:2] == [
        ("daily_basic_baostock", {"start_date": "2023-01-03", "end_date": "2023-12-29"}),
        ("daily_basic", {"start_date": "2023-01-03", "end_date": "2023-12-29"}),
    ]
    assert result["completed"][0] == "daily_basic_baostock"
    # 整链成功后游标清除；逻辑节点已且只推进一次。
    assert result["completed"].count("daily_basic_baostock") == 1
    assert not (tmp_path / "progress.json").exists()


def test_success_with_residual_gap_also_switches_to_fallback():
    table = "daily_basic/daily"
    remaining = {table: ["20230103"]}
    # primary_ok 在第二个候选执行时才允许复扫变空。
    service = _Service({"daily_basic_baostock": "success", "daily_basic": "success"}, remaining, table)
    original_get = service.get_run

    def get_run(run_id):
        if len(service.submitted) >= 2:
            service.outcomes["primary_ok"] = True
        return original_get(run_id)

    service.get_run = get_run
    result = _run_gap(service, remaining, table)

    assert result["status"] == "success"
    assert [job for job, _ in service.submitted[:2]] == ["daily_basic_baostock", "daily_basic"]


def test_all_sources_failed_does_not_advance_checkpoint_or_run_downstream(tmp_path):
    table = "daily_basic/daily"
    remaining = {table: ["20230103"]}
    service = _Service({"daily_basic_baostock": "failed", "daily_basic": "failed"}, remaining, table)
    checkpoint = tmp_path / "progress.json"

    result = _run_gap(service, remaining, table, checkpoint)

    assert result["status"] == "failed"
    assert result["failed_job"] == "daily_basic_baostock"
    assert result["attempted_jobs"] == ["daily_basic_baostock", "daily_basic"]
    assert len(service.submitted) == 2
    assert json.loads(checkpoint.read_text())["completed_jobs"] == []


def test_maintenance_stock_and_financial_have_source_fallbacks():
    service = _Service({
        "stock_basic_fuyao": "failed",
        "stock_basic": "success",
        "financial_fuyao": "failed",
        "financial_tushare": "success",
    }, {}, "")

    result = run_auto_init(service, _inspect, sleep=lambda _: None)

    assert result["status"] == "success"
    jobs = [job for job, _ in service.submitted]
    assert jobs[:4] == ["stock_basic_fuyao", "stock_basic", "financial_fuyao", "financial_tushare"]


def test_financial_fallback_reaches_eastmoney_when_tushare_has_no_permission():
    service = _Service({
        "stock_basic_fuyao": "success",
        "financial_fuyao": "failed",
        "financial_tushare": "failed",
        "financial_eastmoney": "success",
    }, {}, "")

    result = run_auto_init(service, _inspect, sleep=lambda _: None)

    assert result["status"] == "success"
    jobs = [job for job, _ in service.submitted]
    assert jobs[:4] == [
        "stock_basic_fuyao", "financial_fuyao",
        "financial_tushare", "financial_eastmoney",
    ]


def test_legacy_checkpoint_does_not_skip_node_with_residual_gaps(tmp_path):
    table = "daily_basic/daily"
    remaining = {table: ["20230103"]}
    checkpoint = tmp_path / "progress.json"
    checkpoint.write_text(json.dumps({
        "version": 1, "run_date": "2026-11-01",
        "planned_jobs": ["daily_basic_baostock"],
        "completed_jobs": ["daily_basic_baostock"],
    }))
    service = _Service({"daily_basic_baostock": "success", "primary_ok": True}, remaining, table)

    result = run_auto_init(
        service, _inspect,
        gap_details_fn=lambda: ({table: remaining[table]} if remaining.get(table) else {}),
        sleep=lambda _: None, checkpoint_path=checkpoint, run_date="2026-11-01",
    )

    assert result["status"] == "success"
    assert service.submitted[0][0] == "daily_basic_baostock"
