"""moneyflow 通达信历史回补驱动：按交易日分片并行回补。

背景：moneyflow_tdx 生产者只跑单日（夜间增量链路用）。2026-10 实测通达信
免费服务器的分笔历史可回溯一年以上且与日线成交量逐手吻合，历史回补可行。

用法：
    # 单进程串行
    python app/utils/moneyflow_tdx_backfill.py
    # 4 路并行（每路钉一台服务器，日期轮流分片）
    MONEYFLOW_BACKFILL_SHARDS=4 MONEYFLOW_BACKFILL_SHARD_INDEX=0 python ...
    MONEYFLOW_BACKFILL_SHARDS=4 MONEYFLOW_BACKFILL_SHARD_INDEX=1 python ...

环境变量：
    MONEYFLOW_BACKFILL_START / END   回填区间 YYYYMMDD（默认当年 0101 ~ 今天）
    MONEYFLOW_BACKFILL_SHARDS        总分片数（默认 1）
    MONEYFLOW_BACKFILL_SHARD_INDEX   本分片序号（0 起）
    MONEYFLOW_TDX_CODES              标的子集，逗号分隔（调试用）

可断点续跑：分区行数 ≥ 标的数 × COMPLETE_RATIO 视为完成，跳过；
残缺分区会被 merge_save 按 ts_code 合并补全。日期按新→旧回补，
最有价值的数据先落地。
"""
import os
import sys
import time
from pathlib import Path

import pandas as pd

# 兼容直接运行（python app/utils/moneyflow_tdx_backfill.py）
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from app.utils.moneyflow_tdx import aggregate_stock, merge_save  # noqa: E402
from app.utils.job_checkpoint import clear_checkpoint, load_checkpoint, plan_checkpoint_path, save_checkpoint  # noqa: E402
from app.utils.parquet_job_helpers import normalize_ymd  # noqa: E402

REL_TABLE = "moneyflow/daily"
COMPLETE_RATIO = 0.8
DATE_INTERVAL = 1.0  # 每个交易日之间的礼节性间隔
CHUNK_STOCKS = 500


# ---------------------------------------------------------------------------
# 纯函数：计划与分片
# ---------------------------------------------------------------------------

def list_backfill_dates(open_dates, start: str, end: str) -> list:
    """区间内开市日，新→旧排序（最有价值的数据先落地）。"""
    return sorted(
        [d for d in open_dates if start <= str(d) <= end], reverse=True
    )


def existing_partition_row_count(root: Path, ymd: str) -> int:
    """分区行数；不存在/损坏返回 0（按未完成处理）。"""
    part = (
        root / REL_TABLE
        / f"year={ymd[:4]}" / f"month={ymd[4:6]}" / f"day={ymd[6:8]}"
        / "data.parquet"
    )
    if not part.is_file():
        return 0
    try:
        return len(pd.read_parquet(part))
    except Exception:  # noqa: BLE001 - 坏分区按未完成处理，重跑覆写
        return 0


def find_pending_dates(dates, root: Path, universe_size: int,
                       ratio: float = COMPLETE_RATIO) -> list:
    """缺分区或行数不足的日期。universesize<=0 时全部视为待补。"""
    threshold = universe_size * ratio
    pending = []
    for ymd in dates:
        if existing_partition_row_count(root, ymd) < threshold:
            pending.append(ymd)
    return pending


def shard_dates(dates, shards: int, shard_index: int) -> list:
    """轮流分片：shard i 拿 dates[i::n]，并集完整且互不重叠。"""
    if shards <= 0 or not 0 <= shard_index < shards:
        raise ValueError(f"非法分片参数: shards={shards}, index={shard_index}")
    return list(dates)[shard_index::shards]


# ---------------------------------------------------------------------------
# 执行
# ---------------------------------------------------------------------------

def main() -> int:
    from app.services.tongdaxin.client import (
        DEFAULT_HOSTS,
        connect_first_available,
        create_hq_api,
    )

    today = pd.Timestamp.now().strftime("%Y%m%d")
    start = normalize_ymd(os.getenv("MONEYFLOW_BACKFILL_START")) or f"{today[:4]}0101"
    end = normalize_ymd(os.getenv("MONEYFLOW_END_DATE") or os.getenv("MONEYFLOW_BACKFILL_END")) or today
    shards = int(os.getenv("MONEYFLOW_BACKFILL_SHARDS", "1"))
    shard_index = int(os.getenv("MONEYFLOW_BACKFILL_SHARD_INDEX", "0"))

    cal = pd.read_parquet(_PROJECT_ROOT / "data" / "stock_trade_calendar.parquet")
    open_dates = [str(d) for d in cal.loc[cal["is_open"] == 1, "cal_date"]]
    dates = list_backfill_dates(open_dates, start, end)

    basic = pd.read_parquet(_PROJECT_ROOT / "data" / "stock_basic.parquet")
    ts_codes = sorted(basic["ts_code"].dropna().astype(str).unique().tolist())
    ts_codes = [c for c in ts_codes if not c.endswith(".BJ")]
    only = os.getenv("MONEYFLOW_TDX_CODES")
    if only:
        wanted = {c.strip() for c in only.split(",") if c.strip()}
        ts_codes = [c for c in ts_codes if c in wanted]

    data_root = _PROJECT_ROOT / "data"
    checkpoint_root = data_root / "data_job_state" / "checkpoints"

    def date_checkpoint(ymd):
        return plan_checkpoint_path(
            checkpoint_root, "moneyflow_tdx_backfill",
            {"trade_date": ymd, "ts_codes": ts_codes},
        )

    pending_set = set(find_pending_dates(dates, data_root, len(ts_codes)))
    # 行数达到 80% 也不能掩盖“上次仍有失败股票”的检查点。
    pending = [
        ymd for ymd in dates
        if ymd in pending_set or date_checkpoint(ymd).is_file()
    ]
    my_dates = shard_dates(pending, shards, shard_index)
    print(
        f"[backfill] 分片 {shard_index}/{shards}：区间 {start}~{end}，"
        f"开市 {len(dates)} 天，待补 {len(pending)} 天，本路 {len(my_dates)} 天，"
        f"标的 {len(ts_codes)} 只",
        flush=True,
    )
    if not my_dates:
        print("[backfill] 本路无待补日期，退出", flush=True)
        return 0

    # 每路优先钉一台服务器摊薄压力，故障时回退其余服务器
    host = DEFAULT_HOSTS[shard_index % len(DEFAULT_HOSTS)]
    hosts = [host] + [h for h in DEFAULT_HOSTS if h != host]
    api = create_hq_api()
    connect_first_available(api, hosts=hosts)
    print(f"[backfill] 连接服务器 {host[0]} {host[1]}:{host[2]}", flush=True)

    total_failed = 0
    try:
        for di, ymd in enumerate(my_dates, 1):
            date_int = int(ymd)
            trade_date = f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:8]}"
            checkpoint_path = date_checkpoint(ymd)
            checkpoint = load_checkpoint(checkpoint_path)
            completed = set(checkpoint.get("completed_ts_codes", []))
            rows, failed, pending_codes = [], [], []
            total_saved_date = 0

            def flush():
                nonlocal total_saved_date
                if not pending_codes:
                    return
                total_saved_date += merge_save(rows, trade_date)
                completed.update(pending_codes)
                save_checkpoint(checkpoint_path, {
                    "version": 1,
                    "trade_date": ymd,
                    "completed_ts_codes": sorted(completed),
                })
                rows.clear()
                pending_codes.clear()

            try:
                for idx, ts_code in enumerate(ts_codes, 1):
                    if ts_code in completed:
                        continue
                    try:
                        rows.append(aggregate_stock(api, ts_code, date_int))
                        pending_codes.append(ts_code)
                    except Exception as exc:  # noqa: BLE001 - 失败股票留给下次重试
                        failed.append(ts_code)
                        try:
                            api.disconnect()
                        except Exception:  # noqa: BLE001
                            pass
                        api = create_hq_api()
                        try:
                            connect_first_available(api, hosts=hosts)
                        except Exception as exc2:  # noqa: BLE001
                            print(f"[backfill] 重连失败，本路终止: {exc2}", flush=True)
                            raise
                    if len(pending_codes) >= CHUNK_STOCKS:
                        flush()
                        print(f"[backfill] {trade_date} 进度 {idx}/{len(ts_codes)}", flush=True)
            finally:
                flush()
            total_failed += len(failed)
            print(
                f"[backfill] {trade_date} 完成：写入 {total_saved_date} 行，失败 {len(failed)} 只"
                f"（本路 {di}/{len(my_dates)} 天）",
                flush=True,
            )
            if not failed:
                clear_checkpoint(checkpoint_path)
            time.sleep(DATE_INTERVAL)
    finally:
        try:
            api.disconnect()
        except Exception:  # noqa: BLE001
            pass

    print(f"[backfill] 分片 {shard_index} 结束，累计失败 {total_failed} 只", flush=True)
    return 1 if total_failed else 0


if __name__ == "__main__":
    sys.exit(main())
