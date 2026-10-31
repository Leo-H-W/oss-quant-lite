#!/usr/bin/env python3
"""本地数据状态检查与断点增量更新。

用法：
    python data-gather.py status
    python data-gather.py update
    python data-gather.py help
"""
from datetime import datetime
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Dict, List, Optional, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from startup_runtime import inspect_parquet_data_assets


def run_auto_init(*args, **kwargs):
    """延迟加载任务链，help/status 不依赖 Flask、SQLAlchemy 等 Web 包。"""
    from app.services.data_jobs.auto_init import run_auto_init as _run

    return _run(*args, **kwargs)


def _find_stale_daily_tables(data_dir):
    from app.services.data_jobs.auto_init import find_stale_daily_tables

    return find_stale_daily_tables(data_dir)

HELP = """数据维护工具

用法：
  python data-gather.py status   显示各类本地数据的时间范围
  python data-gather.py update   断点续补缺失数据至最新可用交易日
  python data-gather.py update N 本次更新中每类任务各自最多重试 N 次（N ≥ 0）
  python data-gather.py help     显示本帮助

配置：
  DATA_DIR                         本地数据目录（默认：项目 data/）
  DATA_JOB_TIMEOUT                 单次脚本超时秒数（默认：3600）
  DATA_JOB_MAX_RETRIES             每类任务失败后的自动重试次数（默认：5）
  DATA_JOB_RETRY_BACKOFF_SECONDS   首次重试退避秒数（默认：60，指数退避）

update 复用数据任务链，按依赖顺序串行执行；已完成分区、报告期、股票批次和
任务链节点会跳过，失败后在同一 run 内有限次自动重试。全部成功后清除检查点。
"""

DAILY_TABLES: Tuple[Tuple[str, str], ...] = (
    ("日线行情", "daily_history/daily"),
    ("每日指标", "daily_basic/daily"),
    ("扩展技术因子", "stk_factor/daily"),
    ("资金流向", "moneyflow/daily"),
    ("筹码分布", "cyq_perf/daily"),
)
FINANCIAL_TABLES: Tuple[Tuple[str, str], ...] = (
    ("利润表", "income_statement"),
    ("资产负债表", "balance_sheet"),
    ("现金流量表", "cash_flow"),
)
SNAPSHOT_FILES: Tuple[Tuple[str, str], ...] = (
    ("股票基础资料", "stock_basic.parquet"),
    ("上市公司资料", "stock_company.parquet"),
    ("股票业务宽表", "stock_business.parquet"),
    ("最新日宽表", "stock_business_wide_day.parquet"),
    ("均线数据", "stock_ma_data.parquet"),
)


def _data_root() -> Path:
    return Path(os.environ.get("DATA_DIR") or PROJECT_ROOT / "data")


def _has_parquet(path: Path) -> bool:
    return path.is_file() and path.stat().st_size > 0


def _daily_range(root: Path, rel_table: str) -> Tuple[Optional[str], int]:
    dates: List[str] = []
    table = root / rel_table
    if not table.is_dir():
        return None, 0
    for day in table.glob("year=*/month=*/day=*"):
        if not _has_parquet(day / "data.parquet"):
            continue
        try:
            year = day.parent.parent.name.split("=", 1)[1]
            month = day.parent.name.split("=", 1)[1]
            value = day.name.split("=", 1)[1]
            dates.append(f"{year}{month}{value}")
        except (IndexError, ValueError):
            continue
    if not dates:
        return None, 0
    return f"{min(dates)}-{max(dates)}", len(dates)


def _factor_range(root: Path) -> Tuple[Optional[str], int]:
    table = root / "ml_factor_state" / "factor_values"
    dates = []
    if table.is_dir():
        for part in table.glob("trade_date=*"):
            if not any(_has_parquet(path) for path in part.rglob("*.parquet")):
                continue
            value = part.name.split("=", 1)[1].replace("-", "")
            if len(value) == 8 and value.isdigit():
                dates.append(value)
    if not dates:
        return None, 0
    return f"{min(dates)}-{max(dates)}", len(dates)


def _financial_range(root: Path, rel_table: str) -> Tuple[Optional[str], int]:
    periods = []
    table = root / rel_table
    if table.is_dir():
        for month in table.glob("year=*/month=*"):
            if not any(_has_parquet(path) for path in month.rglob("*.parquet")):
                continue
            try:
                year = month.parent.name.split("=", 1)[1]
                value = month.name.split("=", 1)[1]
            except (IndexError, ValueError):
                continue
            periods.append(f"{year}{value}")
    if not periods:
        return None, 0
    return f"{min(periods)}-{max(periods)}", len(periods)


def _calendar_range(root: Path) -> Optional[str]:
    path = root / "stock_trade_calendar.parquet"
    if not _has_parquet(path):
        return None
    try:
        import pandas as pd

        frame = pd.read_parquet(path, columns=["cal_date"])
        values = sorted(str(value) for value in frame["cal_date"].dropna())
        return f"{values[0]}-{values[-1]}" if values else None
    except Exception as exc:  # 状态命令不应因单个坏文件中断其他资产报告
        return f"读取失败：{exc}"


def collect_status(root: Path) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for label, rel_table in DAILY_TABLES:
        date_range, partitions = _daily_range(root, rel_table)
        rows.append({"name": label, "range": date_range, "partitions": partitions})
    date_range, partitions = _factor_range(root)
    rows.append({"name": "因子计算值", "range": date_range, "partitions": partitions})
    for label, rel_table in FINANCIAL_TABLES:
        period_range, partitions = _financial_range(root, rel_table)
        rows.append({"name": label, "range": period_range, "partitions": partitions})
    return rows


def _active_sync_processes() -> List[Dict[str, object]]:
    """无 pandas 时的只读进程兜底；找不到进程时不能据此断言 run 已结束。"""
    result = subprocess.run(
        ["ps", "-eo", "pid=,args="], capture_output=True, text=True, check=False
    )
    runs = []
    for line in result.stdout.splitlines():
        try:
            pid_text, args = line.strip().split(None, 1)
            tokens = args.split()
            if not tokens or not os.path.basename(tokens[0]).startswith("python"):
                continue
            script = next((
                token for token in tokens[1:]
                if token.endswith(".py") and ("/app/utils/" in token or "/scripts/" in token)
            ), None)
            if script:
                runs.append({
                    "id": f"pid:{pid_text}",
                    "job_type": os.path.basename(script),
                    "progress_message": "仅检测到进程，run 状态不可读",
                })
        except (ValueError, IndexError):
            continue
    return runs


def load_active_runs(root: Path, process_fallback: bool = False):
    """只读活动 run；不调用单进程 orphan reaper，避免误伤其他进程。"""
    path = root / "data_job_state" / "data_job_runs.parquet"
    if not _has_parquet(path):
        return []
    try:
        import pandas as pd

        frame = pd.read_parquet(path)
        if frame.empty:
            return []
        active = frame[frame["status"].isin(["pending", "queued", "running"])]
        runs = active.to_dict("records")
        for run in runs:
            run["id"] = int(run["id"])
        return sorted(runs, key=lambda run: run["id"])
    except Exception as exc:
        print(f"任务状态读取失败：{exc}", file=sys.stderr)
        if process_fallback:
            return _active_sync_processes() or None
        return None


def print_active_runs(runs) -> None:
    if runs is None:
        print("数据同步任务状态：无法确认，请使用 .venv/bin/python data-gather.py status。")
        return
    if not runs:
        print("当前没有数据同步任务运行。")
        return
    print("正在运行的数据同步任务：")
    for run in runs:
        message = run.get("progress_message") or run.get("status")
        print(f"  - run_id={run['id']} {run.get('job_type')}：{message}")


def _python_script_processes(script_path: str) -> List[Dict[str, object]]:
    """找命令行参数精确匹配脚本路径的 Python 进程，避免误匹配 shell 文本。"""
    result = subprocess.run(
        ["ps", "-eo", "pid=,ppid=,args="], capture_output=True, text=True, check=False
    )
    matches = []
    absolute = str(PROJECT_ROOT / script_path)
    for line in result.stdout.splitlines():
        try:
            pid_text, ppid_text, args = line.strip().split(None, 2)
            tokens = args.split()
            if not tokens or not os.path.basename(tokens[0]).startswith("python"):
                continue
            if absolute not in tokens and script_path not in tokens:
                continue
            matches.append({"pid": int(pid_text), "ppid": int(ppid_text), "args": args})
        except (ValueError, IndexError):
            continue
    return matches


def _standalone_worker_pid(pid: int) -> bool:
    """仅识别独立任务进程；run.py/Web 服务绝不能因取消单任务被终止。"""
    if pid <= 0 or pid == os.getpid():
        return False
    result = subprocess.run(
        ["ps", "-p", str(pid), "-o", "args="],
        capture_output=True, text=True, check=False,
    )
    tokens = result.stdout.strip().split()
    if not tokens or not os.path.basename(tokens[0]).startswith("python"):
        return False
    if tokens[1:] in (["-"], ["-u", "-"]):
        return True
    return any(token.endswith("data-gather.py") for token in tokens) and "update" in tokens


def cancel_active_runs(root: Path, runs: List[Dict[str, object]]) -> None:
    """用户确认后取消旧 run 并终止脚本；worker 检测 cancelled 后不再重试。"""
    import signal

    from app.services.data_jobs.parquet_state_store import ParquetDataJobStateStore
    from app.services.data_jobs.registry import JobRegistry

    store = ParquetDataJobStateStore(base_dir=str(root / "data_job_state"))
    registry = JobRegistry()
    stopped_pids = []
    for run in runs:
        run_id = int(run["id"])
        record = store.get_run(run_id)
        if record is not None and record.status in {"pending", "queued", "running"}:
            store.update_run_status(
                record, "cancelled", progress=100.0,
                progress_message="用户选择重新开始，旧任务已取消",
            )
        try:
            script_path = registry.get_job(str(run["job_type"])).script_path
        except KeyError:
            continue
        processes = _python_script_processes(script_path)
        for process in processes:
            pid = int(process["pid"])
            if pid == os.getpid():
                continue
            try:
                os.kill(pid, signal.SIGTERM)
                stopped_pids.append(pid)
            except ProcessLookupError:
                pass
            # 旧版独立 worker 没有 cancelled 检查；只终止 stdin/CLI 独立
            # worker，Web run.py 由新版 worker 检测 cancelled 后自行退出。
            parent_pid = int(process["ppid"])
            if _standalone_worker_pid(parent_pid):
                try:
                    os.kill(parent_pid, signal.SIGTERM)
                    stopped_pids.append(parent_pid)
                except ProcessLookupError:
                    pass
        if not processes and record is not None:
            worker_pid = int((record.result_json or {}).get("worker_pid") or 0)
            # 重试退避期间没有脚本子进程；独立 worker 可直接终止，Web
            # worker 则会在下一次 attempt 前读取 cancelled 并退出。
            if _standalone_worker_pid(worker_pid):
                try:
                    os.kill(worker_pid, signal.SIGTERM)
                    stopped_pids.append(worker_pid)
                except ProcessLookupError:
                    pass
        print(f"已取消 run_id={run_id}")

    # 给脚本一个短暂退出窗口，降低新旧任务并发写同一分区的可能。
    deadline = time.monotonic() + 5
    while stopped_pids and time.monotonic() < deadline:
        alive = []
        for pid in stopped_pids:
            try:
                os.kill(pid, 0)
                alive.append(pid)
            except ProcessLookupError:
                pass
        if not alive:
            break
        time.sleep(0.1)
        stopped_pids = alive


def print_status(root: Path) -> None:
    print(f"本地数据目录：{root}")
    for row in collect_status(root):
        date_range = row["range"] or "暂无本地数据"
        suffix = f"（{row['partitions']} 个分区）" if row["partitions"] else ""
        print(f"{row['name']}：{date_range}{suffix}")
    print(f"交易日历：{_calendar_range(root) or '暂无本地数据'}")
    for label, filename in SNAPSHOT_FILES:
        path = root / filename
        if _has_parquet(path):
            updated = datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
            print(f"{label}：快照，本地更新时间 {updated}")
        else:
            print(f"{label}：暂无本地数据")
    print_active_runs(load_active_runs(root, process_fallback=True))


def _run_update(root: Path, service, input_fn) -> int:
    active_runs = load_active_runs(root)
    if active_runs is None:
        print(
            "无法确认是否有任务在运行，已停止 update。请使用 .venv/bin/python data-gather.py update。",
            file=sys.stderr,
        )
        return 1
    if active_runs:
        print_active_runs(active_runs)
        ask = input_fn or input
        try:
            answer = ask("检测到已有任务，是否重新开始？[y/N]：")
        except EOFError:
            answer = ""
        if str(answer).strip().lower() not in {"y", "yes"}:
            print("保持现有任务，不重复启动更新。")
            return 0
        cancel_active_runs(root, active_runs)

    if service is None:
        try:
            from app.services.data_jobs.service import DataJobService
        except ModuleNotFoundError as exc:
            print(
                f"update 缺少项目依赖（{exc.name}）。请运行：\n"
                "  .venv/bin/python data-gather.py update\n"
                "或先安装 requirements.txt；help/status 可直接使用 python3。",
                file=sys.stderr,
            )
            return 1
        service = DataJobService()
    run_date = datetime.now().date().isoformat()
    try:
        result = run_auto_init(
            service,
            lambda: inspect_parquet_data_assets(str(root)),
            stale_tables_fn=lambda: _find_stale_daily_tables(str(root)),
            checkpoint_path=root / "data_job_state" / "data_gather_progress.json",
            run_date=run_date,
        )
    except ModuleNotFoundError as exc:
        print(
            f"update 缺少项目依赖（{exc.name}）。请使用 .venv/bin/python 或安装 requirements.txt。",
            file=sys.stderr,
        )
        return 1
    completed = result.get("completed") or []
    if completed:
        print("已完成任务：" + "、".join(str(job) for job in completed))
    status = result.get("status")
    print(f"更新结果：{status}")
    if status in {"success", "ok"}:
        return 0
    if result.get("failed_job"):
        print(f"未完成节点：{result['failed_job']}；再次运行 update 将从检查点继续。", file=sys.stderr)
    return 1


def main(argv: Optional[Sequence[str]] = None, service=None, input_fn=None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    command = args[0] if args else "help"
    if command == "help" or command in {"-h", "--help"}:
        print(HELP)
        return 0
    if len(args) != 1 and not (command == "update" and len(args) == 2):
        print("参数过多。使用 python data-gather.py help 查看帮助。", file=sys.stderr)
        return 2

    root = _data_root()
    if command == "status":
        print_status(root)
        return 0
    if command != "update":
        print(f"未知命令：{command}。使用 python data-gather.py help 查看帮助。", file=sys.stderr)
        return 2
    previous_retries = os.environ.get("DATA_JOB_MAX_RETRIES")
    if len(args) == 2:
        try:
            retries = int(args[1])
            if retries < 0:
                raise ValueError
        except ValueError:
            print("DATA_JOB_MAX_RETRIES 必须是非负整数。", file=sys.stderr)
            return 2
        os.environ["DATA_JOB_MAX_RETRIES"] = str(retries)

    try:
        return _run_update(root, service, input_fn)
    finally:
        if len(args) == 2:
            if previous_retries is None:
                os.environ.pop("DATA_JOB_MAX_RETRIES", None)
            else:
                os.environ["DATA_JOB_MAX_RETRIES"] = previous_retries


if __name__ == "__main__":
    sys.exit(main())
