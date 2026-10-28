"""任务健康度监控：发现"任务假跑、失败静默、巡检缺席、数据残缺"四类失语故障。

背景（2026-10-26 事故）：服务进程中断后，夜间 auto-init 连续缺席无人察觉；
daily_basic 出现 631/5572 行的残缺分区，却被资产检查视为"已有数据"。

五项检查：
- orphan_runs          活动状态但不在活性注册表的假 running → critical
- recent_failures      窗口内失败任务 → warning（不自动重试，避免刷限频）
- auto_init_heartbeat  夜间巡检心跳缺席（调度/服务停了）→ critical
- data_freshness       日频表分区落后交易日历（复用 auto_init 断档感知）→ warning
- partition_completeness 最新分区行数 vs 同日 daily_history 基准 → warning

repair_health 是唯一写路径：孤儿落 failed（复用 ORPHAN_ERROR_MESSAGE），
断档表按 DAILY_GAP_JOB_CHAIN 依赖序提交回补，在跑同类型任务礼让。

每个检查项独立 try/except：健康检查本身永远不许把调用方打挂，
单项失败降级为 {"status": "skipped", "error": ...}。
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Dict, List, Optional

from loguru import logger

from app.services.data_jobs.auto_init import (
    DAILY_GAP_JOB_CHAIN,
    DEFAULT_RUN_TIME,
    _latest_partition_date,
    find_stale_daily_tables,
    parse_run_time,
)
from app.services.data_jobs.service import ACTIVE_STATUSES, ORPHAN_ERROR_MESSAGE

SEVERITY_ORDER = {"ok": 0, "warning": 1, "critical": 2, "skipped": -1}

DEFAULT_RECENT_HOURS = 24
DEFAULT_COMPLETENESS_RATIO = 0.8
RUNS_SCAN_LIMIT = 200

_BENCHMARK_TABLE = "daily_history/daily"


def _worst(statuses) -> str:
    """聚合严重度：skipped 不参与，critical > warning > ok。"""
    effective = [s for s in statuses if s != "skipped"]
    if not effective:
        return "ok"
    return max(effective, key=lambda s: SEVERITY_ORDER[s])


def _parse_ts(value) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# 单项检查（纯函数，依赖全部注入）
# ---------------------------------------------------------------------------

def check_orphan_runs(runs, is_active: Callable[[int], bool]) -> Dict[str, object]:
    """活动状态但判活失败 → 假 running。进程中断/重启遗留，状态在撒谎。"""
    orphans = [
        {"id": r.id, "job_type": r.job_type, "status": r.status}
        for r in runs
        if getattr(r, "status", None) in ACTIVE_STATUSES and not is_active(r.id)
    ]
    return {
        "status": "critical" if orphans else "ok",
        "runs": orphans,
    }


def check_recent_failures(
    runs, now: datetime, recent_hours: int = DEFAULT_RECENT_HOURS
) -> Dict[str, object]:
    cutoff = now - timedelta(hours=recent_hours)
    failures = []
    for r in runs:
        if getattr(r, "status", None) != "failed":
            continue
        ts = _parse_ts(getattr(r, "finished_at", None)) or _parse_ts(
            getattr(r, "updated_at", None)
        )
        if ts is not None and ts >= cutoff:
            failures.append(
                {"id": r.id, "job_type": r.job_type, "finished_at": str(ts)}
            )
    return {
        "status": "warning" if failures else "ok",
        "window_hours": recent_hours,
        "runs": failures,
    }


def check_auto_init_heartbeat(
    state: Dict[str, object], now: datetime, run_time: str = DEFAULT_RUN_TIME
) -> Dict[str, object]:
    """夜间调度每天都应跑（不分交易日）。now 已过巡检时刻则期望今天，否则昨天。"""
    hour, minute = parse_run_time(run_time)
    expected = now.date() if (now.hour, now.minute) >= (hour, minute) else (
        now.date() - timedelta(days=1)
    )
    last = (state or {}).get("last_run_date")
    healthy = bool(last) and str(last) >= expected.isoformat()
    return {
        "status": "ok" if healthy else "critical",
        "last_run_date": last,
        "expected_run_date": expected.isoformat(),
    }


def check_data_freshness(stale_tables: List[str]) -> Dict[str, object]:
    stale = list(stale_tables or [])
    return {
        "status": "warning" if stale else "ok",
        "stale_tables": stale,
    }


def check_partition_completeness(
    data_dir: Optional[str],
    ratio_threshold: float = DEFAULT_COMPLETENESS_RATIO,
) -> Dict[str, object]:
    """最新分区行数 vs 同日 daily_history 基准。

    整分区覆写型任务被中途杀掉会留下"日期最新但行数残缺"的分区，
    资产检查（存在且非空）与断档检查（分区日期）都发现不了。
    只在分区日期与基准一致时比较；日期落后归 data_freshness 管。
    """
    import pandas as pd  # 延迟导入：报告其余部分不依赖 pandas

    root = Path(data_dir) if data_dir else Path("data")
    bench_latest = _latest_partition_date(root / _BENCHMARK_TABLE)
    if bench_latest is None:
        return {"status": "skipped", "reason": "benchmark_missing", "tables": []}

    bench_part = (
        root / _BENCHMARK_TABLE
        / f"year={bench_latest[:4]}"
        / f"month={bench_latest[4:6]}"
        / f"day={bench_latest[6:8]}"
        / "data.parquet"
    )
    bench_rows = len(pd.read_parquet(bench_part))
    if bench_rows <= 0:
        return {"status": "skipped", "reason": "benchmark_empty", "tables": []}

    flagged = []
    for table, _job_type in DAILY_GAP_JOB_CHAIN:
        if table == _BENCHMARK_TABLE:
            continue
        latest = _latest_partition_date(root / table)
        if latest is None or latest != bench_latest:
            continue
        part = (
            root / table
            / f"year={latest[:4]}" / f"month={latest[4:6]}" / f"day={latest[6:8]}"
            / "data.parquet"
        )
        rows = len(pd.read_parquet(part))
        ratio = rows / bench_rows
        if ratio < ratio_threshold:
            flagged.append(
                {
                    "table": table,
                    "date": latest,
                    "rows": rows,
                    "benchmark_rows": bench_rows,
                    "ratio": round(ratio, 4),
                }
            )
    return {
        "status": "warning" if flagged else "ok",
        "ratio_threshold": ratio_threshold,
        "tables": flagged,
    }


# ---------------------------------------------------------------------------
# 报告汇总
# ---------------------------------------------------------------------------

def _load_auto_init_state(data_dir: Optional[str]) -> Dict[str, object]:
    root = Path(data_dir) if data_dir else Path("data")
    try:
        return json.loads(
            (root / "data_job_state" / "auto_init_state.json").read_text(encoding="utf-8")
        )
    except (OSError, ValueError):
        return {}


def _guard(fn) -> Dict[str, object]:
    """单项检查兜底：健康检查自身永远不许抛出。"""
    try:
        return fn()
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"[health] 单项检查失败，降级 skipped: {exc}")
        return {"status": "skipped", "error": str(exc)}


def collect_health_report(
    service,
    *,
    data_dir: Optional[str] = None,
    now: Optional[datetime] = None,
    auto_init_state: Optional[Dict[str, object]] = None,
    stale_tables_fn: Optional[Callable[[], List[str]]] = None,
    completeness_fn: Optional[Callable[[], Dict[str, object]]] = None,
    recent_hours: int = DEFAULT_RECENT_HOURS,
    run_time: str = DEFAULT_RUN_TIME,
    ratio_threshold: float = DEFAULT_COMPLETENESS_RATIO,
) -> Dict[str, object]:
    """汇总五项检查。service 需暴露 state_store / is_run_active（读路径，不修复）。"""
    now = now or datetime.now()
    runs = service.state_store.list_runs(limit=RUNS_SCAN_LIMIT)

    if auto_init_state is None:
        auto_init_state = _load_auto_init_state(data_dir)
    if stale_tables_fn is None:
        stale_tables_fn = lambda: find_stale_daily_tables(data_dir, now)  # noqa: E731
    if completeness_fn is None:
        completeness_fn = lambda: check_partition_completeness(  # noqa: E731
            data_dir, ratio_threshold=ratio_threshold
        )

    checks = {
        "orphan_runs": _guard(lambda: check_orphan_runs(runs, service.is_run_active)),
        "recent_failures": _guard(
            lambda: check_recent_failures(runs, now, recent_hours=recent_hours)
        ),
        "auto_init_heartbeat": _guard(
            lambda: check_auto_init_heartbeat(auto_init_state, now, run_time=run_time)
        ),
        "data_freshness": _guard(lambda: check_data_freshness(stale_tables_fn())),
        "partition_completeness": _guard(completeness_fn),
    }
    return {
        "status": _worst([c["status"] for c in checks.values()]),
        "checked_at": now.isoformat(),
        "checks": checks,
    }


# ---------------------------------------------------------------------------
# 修复
# ---------------------------------------------------------------------------

def repair_health(service, report: Dict[str, object]) -> Dict[str, object]:
    """按报告执行修复，返回动作清单。

    - 孤儿 run → 落 failed（复用收割语义的错误文案，页面上显示"已中断，可重试"）；
    - 断档表 → 按 DAILY_GAP_JOB_CHAIN 顺序提交回补，同类型在跑礼让；
    - recent_failures 不自动重试：限频/权限类失败原地重试只会再失败一次。
    """
    actions: Dict[str, object] = {
        "reaped_orphans": [],
        "submitted": [],
        "skipped_duplicate": [],
        "errors": [],
    }

    orphan_check = report.get("checks", {}).get("orphan_runs", {})
    for orphan in orphan_check.get("runs", []):
        run = service.state_store.get_run(orphan["id"])
        if run is None or getattr(run, "status", None) not in ACTIVE_STATUSES:
            continue
        try:
            service.state_store.update_run_status(
                run,
                "failed",
                error_message=ORPHAN_ERROR_MESSAGE,
                progress_message="已中断，可重试",
            )
            actions["reaped_orphans"].append(orphan["id"])
        except Exception as exc:  # noqa: BLE001 - 单个孤儿失败不阻断其余修复
            actions["errors"].append(f"reap {orphan['id']}: {exc}")

    from app.services.data_jobs.auto_init import plan_gap_fill_jobs

    stale = report.get("checks", {}).get("data_freshness", {}).get("stale_tables", [])
    for job_type in plan_gap_fill_jobs(stale):
        try:
            run = service.submit(job_type, {})
            actions["submitted"].append({"job_type": job_type, "run_id": run.id})
        except ValueError as exc:
            if "duplicate" in str(exc):
                actions["skipped_duplicate"].append(job_type)
            else:
                actions["errors"].append(f"submit {job_type}: {exc}")
        except Exception as exc:  # noqa: BLE001
            actions["errors"].append(f"submit {job_type}: {exc}")

    return actions
