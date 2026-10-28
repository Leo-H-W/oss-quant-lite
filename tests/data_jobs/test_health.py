"""任务健康度监控合约测试。

覆盖五项检查与修复动作：
- orphan_runs：活动状态但不在活性注册表的假 running（进程中断遗留）→ critical
- recent_failures：窗口内的失败任务 → warning，窗口外不计
- auto_init_heartbeat：夜间巡检心跳缺席（服务停了没人知道）→ critical
- data_freshness：日频表分区落后交易日历 → warning
- partition_completeness：最新分区行数远低于同日 daily_history 基准 → warning
- repair_health：孤儿落 failed、断档表按依赖链提交回补、重复在跑礼让
"""
from datetime import datetime, timedelta
from types import SimpleNamespace

import pandas as pd
import pytest

from app.services.data_jobs.health import (
    collect_health_report,
    repair_health,
)
from app.services.data_jobs.service import ORPHAN_ERROR_MESSAGE

pytestmark = pytest.mark.module_data_jobs


# ---------------------------------------------------------------------------
# 测试替身
# ---------------------------------------------------------------------------

def _run(id, job_type, status, **kw):
    defaults = dict(
        id=id, job_type=job_type, status=status,
        params_json={}, finished_at=None, updated_at=None,
    )
    defaults.update(kw)
    return SimpleNamespace(**defaults)


def _service(runs, active_ids=(), submit_effects=None):
    """服务边界替身：raw list_runs（不判活）+ 活性查询 + 提交记录。"""
    submitted = []

    def submit(job_type, params):
        effect = (submit_effects or {}).get(job_type)
        if isinstance(effect, Exception):
            raise effect
        submitted.append((job_type, params))
        return SimpleNamespace(id=900 + len(submitted), job_type=job_type, status="queued")

    store = SimpleNamespace(list_runs=lambda limit=50, status=None: list(runs))
    return SimpleNamespace(
        state_store=store,
        is_run_active=lambda run_id: run_id in set(active_ids),
        submit=submit,
        _submitted=submitted,
    )


# ---------------------------------------------------------------------------
# 报告结构
# ---------------------------------------------------------------------------

def test_report_shape_has_status_and_five_checks():
    svc = _service([])
    report = collect_health_report(
        svc, now=datetime(2026, 10, 26, 12, 0),
        auto_init_state={"last_run_date": "2026-10-26"},
        stale_tables_fn=lambda: [],
        completeness_fn=lambda: {"status": "ok", "tables": []},
    )
    assert report["status"] in {"ok", "warning", "critical"}
    for key in (
        "orphan_runs", "recent_failures", "auto_init_heartbeat",
        "data_freshness", "partition_completeness",
    ):
        assert key in report["checks"], f"缺少检查项 {key}"
        assert report["checks"][key]["status"] in {"ok", "warning", "critical", "skipped"}


def test_healthy_scenario_aggregates_ok():
    svc = _service([], active_ids=())
    report = collect_health_report(
        svc, now=datetime(2026, 10, 26, 12, 0),
        auto_init_state={"last_run_date": "2026-10-26"},
        stale_tables_fn=lambda: [],
        completeness_fn=lambda: {"status": "ok", "tables": []},
    )
    assert report["status"] == "ok"


# ---------------------------------------------------------------------------
# 孤儿任务（假 running）
# ---------------------------------------------------------------------------

def test_orphan_running_run_is_critical():
    orphan = _run(17, "daily_basic_baostock", "running")
    svc = _service([orphan], active_ids=())  # 不在活性注册表
    report = collect_health_report(
        svc, now=datetime(2026, 10, 26, 12, 0),
        auto_init_state={"last_run_date": "2026-10-26"},
        stale_tables_fn=lambda: [],
        completeness_fn=lambda: {"status": "ok", "tables": []},
    )
    check = report["checks"]["orphan_runs"]
    assert check["status"] == "critical"
    assert [r["id"] for r in check["runs"]] == [17]
    assert report["status"] == "critical"


def test_live_running_run_is_not_orphan():
    live = _run(18, "daily_basic_baostock", "running")
    svc = _service([live], active_ids=(18,))
    report = collect_health_report(
        svc, now=datetime(2026, 10, 26, 12, 0),
        auto_init_state={"last_run_date": "2026-10-26"},
        stale_tables_fn=lambda: [],
        completeness_fn=lambda: {"status": "ok", "tables": []},
    )
    assert report["checks"]["orphan_runs"]["status"] == "ok"


# ---------------------------------------------------------------------------
# 近期失败
# ---------------------------------------------------------------------------

def test_recent_failure_is_warning():
    failed = _run(
        14, "daily_basic", "failed",
        finished_at="2026-10-26T08:00:00", updated_at="2026-10-26T08:00:00",
    )
    svc = _service([failed])
    report = collect_health_report(
        svc, now=datetime(2026, 10, 26, 12, 0), recent_hours=24,
        auto_init_state={"last_run_date": "2026-10-26"},
        stale_tables_fn=lambda: [],
        completeness_fn=lambda: {"status": "ok", "tables": []},
    )
    check = report["checks"]["recent_failures"]
    assert check["status"] == "warning"
    assert [r["id"] for r in check["runs"]] == [14]
    assert report["status"] == "warning"


def test_old_failure_outside_window_ignored():
    failed = _run(
        12, "cyq_perf", "failed",
        finished_at="2026-09-25T22:33:42", updated_at="2026-09-25T22:33:42",
    )
    svc = _service([failed])
    report = collect_health_report(
        svc, now=datetime(2026, 10, 26, 12, 0), recent_hours=24,
        auto_init_state={"last_run_date": "2026-10-26"},
        stale_tables_fn=lambda: [],
        completeness_fn=lambda: {"status": "ok", "tables": []},
    )
    assert report["checks"]["recent_failures"]["status"] == "ok"


# ---------------------------------------------------------------------------
# auto-init 心跳
# ---------------------------------------------------------------------------

def test_heartbeat_missed_run_is_critical():
    """今天已过巡检时刻但 last_run_date 停在昨天 → 调度失语（服务挂了）。"""
    svc = _service([])
    report = collect_health_report(
        svc, now=datetime(2026, 10, 26, 12, 0),
        auto_init_state={"last_run_date": "2026-10-24"},
        stale_tables_fn=lambda: [],
        completeness_fn=lambda: {"status": "ok", "tables": []},
    )
    check = report["checks"]["auto_init_heartbeat"]
    assert check["status"] == "critical"
    assert check["last_run_date"] == "2026-10-24"
    assert check["expected_run_date"] == "2026-10-26"


def test_heartbeat_before_run_time_expects_yesterday():
    """凌晨 01:00 尚未到 02:30，期望值应为昨天，昨天跑过即健康。"""
    svc = _service([])
    report = collect_health_report(
        svc, now=datetime(2026, 10, 26, 1, 0),
        auto_init_state={"last_run_date": "2026-10-25"},
        stale_tables_fn=lambda: [],
        completeness_fn=lambda: {"status": "ok", "tables": []},
    )
    assert report["checks"]["auto_init_heartbeat"]["status"] == "ok"


def test_heartbeat_missing_state_is_critical():
    svc = _service([])
    report = collect_health_report(
        svc, now=datetime(2026, 10, 26, 12, 0),
        auto_init_state={},
        stale_tables_fn=lambda: [],
        completeness_fn=lambda: {"status": "ok", "tables": []},
    )
    assert report["checks"]["auto_init_heartbeat"]["status"] == "critical"


# ---------------------------------------------------------------------------
# 数据断档
# ---------------------------------------------------------------------------

def test_stale_daily_tables_is_warning():
    svc = _service([])
    report = collect_health_report(
        svc, now=datetime(2026, 10, 26, 20, 0),
        auto_init_state={"last_run_date": "2026-10-26"},
        stale_tables_fn=lambda: ["daily_basic/daily"],
        completeness_fn=lambda: {"status": "ok", "tables": []},
    )
    check = report["checks"]["data_freshness"]
    assert check["status"] == "warning"
    assert check["stale_tables"] == ["daily_basic/daily"]


# ---------------------------------------------------------------------------
# 分区完整度（真实 parquet 走通，捕获 631/5572 这类残缺）
# ---------------------------------------------------------------------------

def _write_daily_partition(root, table, ymd, rows):
    part = root / table / f"year={ymd[:4]}" / f"month={ymd[4:6]}" / f"day={ymd[6:8]}"
    part.mkdir(parents=True)
    df = pd.DataFrame(
        {"ts_code": [f"{i:06d}.SZ" for i in range(rows)], "close": [1.0] * rows}
    )
    df.to_parquet(part / "data.parquet", index=False)


def test_incomplete_partition_is_warning(tmp_path):
    """daily_basic 最新分区行数远低于同日 daily_history → warning 并点名表。"""
    _write_daily_partition(tmp_path, "daily_history/daily", "20261023", 100)
    _write_daily_partition(tmp_path, "daily_basic/daily", "20261023", 10)

    svc = _service([])
    report = collect_health_report(
        svc, data_dir=str(tmp_path), now=datetime(2026, 10, 26, 12, 0),
        auto_init_state={"last_run_date": "2026-10-26"},
        stale_tables_fn=lambda: [],
    )
    check = report["checks"]["partition_completeness"]
    assert check["status"] == "warning"
    flagged = {t["table"] for t in check["tables"]}
    assert "daily_basic/daily" in flagged


def test_complete_partition_is_ok(tmp_path):
    _write_daily_partition(tmp_path, "daily_history/daily", "20261023", 100)
    _write_daily_partition(tmp_path, "daily_basic/daily", "20261023", 95)

    svc = _service([])
    report = collect_health_report(
        svc, data_dir=str(tmp_path), now=datetime(2026, 10, 26, 12, 0),
        auto_init_state={"last_run_date": "2026-10-26"},
        stale_tables_fn=lambda: [],
    )
    assert report["checks"]["partition_completeness"]["status"] == "ok"


# ---------------------------------------------------------------------------
# 修复动作
# ---------------------------------------------------------------------------

def test_repair_marks_orphans_failed():
    orphan = _run(17, "daily_basic_baostock", "running")
    updates = []
    store = SimpleNamespace(
        list_runs=lambda limit=50, status=None: [orphan],
        get_run=lambda rid: orphan if rid == 17 else None,
        update_run_status=lambda run, status, **kw: updates.append((run.id, status, kw)) or run,
    )
    svc = SimpleNamespace(
        state_store=store,
        is_run_active=lambda rid: False,
        submit=lambda jt, params: SimpleNamespace(id=1),
    )
    report = collect_health_report(
        svc, now=datetime(2026, 10, 26, 12, 0),
        auto_init_state={"last_run_date": "2026-10-26"},
        stale_tables_fn=lambda: [],
        completeness_fn=lambda: {"status": "ok", "tables": []},
    )
    actions = repair_health(svc, report)

    assert actions["reaped_orphans"] == [17]
    assert updates and updates[0][0] == 17 and updates[0][1] == "failed"
    assert ORPHAN_ERROR_MESSAGE in updates[0][2]["error_message"]


def test_repair_submits_gap_fill_jobs_in_chain_order():
    svc = _service([])
    report = collect_health_report(
        svc, now=datetime(2026, 10, 26, 20, 0),
        auto_init_state={"last_run_date": "2026-10-26"},
        stale_tables_fn=lambda: ["moneyflow/daily", "daily_history/daily"],
        completeness_fn=lambda: {"status": "ok", "tables": []},
    )
    actions = repair_health(svc, report)

    # daily_history 在链中先于 moneyflow
    submitted_types = [jt for jt, _ in svc._submitted]
    assert submitted_types == ["daily_history_by_date", "moneyflow_tdx"]
    assert [s["job_type"] for s in actions["submitted"]] == submitted_types


def test_repair_tolerates_duplicate_running():
    svc = _service(
        [], submit_effects={"daily_basic_baostock": ValueError("duplicate running job: 42")}
    )
    report = collect_health_report(
        svc, now=datetime(2026, 10, 26, 20, 0),
        auto_init_state={"last_run_date": "2026-10-26"},
        stale_tables_fn=lambda: ["daily_basic/daily"],
        completeness_fn=lambda: {"status": "ok", "tables": []},
    )
    actions = repair_health(svc, report)

    assert actions["submitted"] == []
    assert "daily_basic_baostock" in actions["skipped_duplicate"]


# ---------------------------------------------------------------------------
# auto-init 集成：巡检结果携带健康快照
# ---------------------------------------------------------------------------

def test_auto_init_result_carries_health_snapshot():
    from app.services.data_jobs.auto_init import AutoInitScheduler

    app = SimpleNamespace(config={"DATA_DIR": None})
    scheduler = AutoInitScheduler(app, state_path="/nonexistent/auto_init_state.json")

    fake_service = _service([])
    result = scheduler._execute_with_health(
        lambda: {"status": "ok", "completed": []},
        service=fake_service,
        now=datetime(2026, 10, 26, 2, 30),
    )
    assert result["status"] == "ok"
    assert "health" in result
    assert result["health"]["status"] in {"ok", "warning", "critical"}
