"""moneyflow 的通达信生产者：分笔成交按金额分档聚合。

tushare moneyflow 需高积分（本账号无权限），但分档资金流的本质是对
逐笔成交按单笔金额分档统计。通达信免费行情协议（pytdx，TCP 7709，
无需账号）可拉历史分笔，本脚本聚合出与 tushare moneyflow 同构的表：

- 分档阈值（tushare 口径）：小单 <5万、中单 5~20万、大单 20~100万、特大单 ≥100万
- buyorsell：0=主动买 1=主动卖，其余（中性/盘后定价等）不计入分档
- net_mf_* = (大单+特大单) 主动买 - 主动卖

硬限制：免费行情服务器的分笔历史只缓存近期（数天到数月），
历史回补不可行——默认只跑最近一个交易日，逐日积累。
北交所标的通达信协议支持不全，跳过并计数告警。
"""
import os
import sys
import time
from pathlib import Path

import pandas as pd

# 兼容直接运行（python app/utils/moneyflow_tdx.py）
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from app.utils.parquet_job_helpers import (  # noqa: E402
    available_open_trade_dates,
    normalize_ymd,
)
from app.utils.job_checkpoint import (  # noqa: E402
    clear_checkpoint,
    load_checkpoint,
    plan_checkpoint_path,
    save_checkpoint,
)
from app.utils.parquet_writer import save_to_parquet  # noqa: E402

REL_TABLE = "moneyflow/daily"
PAGE_SIZE = 800
PAGE_INTERVAL = 0.05
CHUNK_STOCKS = 200
CHECKPOINT_ROOT = _PROJECT_ROOT / "data" / "data_job_state" / "checkpoints"
# 单笔成交金额分档阈值（元），tushare 口径
BUCKETS = (("sm", 0, 50_000), ("md", 50_000, 200_000),
           ("lg", 200_000, 1_000_000), ("elg", 1_000_000, float("inf")))

OUTPUT_COLS = ["ts_code", "trade_date"]
for _tag, _, _ in BUCKETS:
    OUTPUT_COLS += [f"buy_{_tag}_vol", f"buy_{_tag}_amount",
                    f"sell_{_tag}_vol", f"sell_{_tag}_amount"]
OUTPUT_COLS += ["net_mf_vol", "net_mf_amount"]


def ts_code_to_tdx(ts_code: str):
    """600000.SH → (1, '600000')；000001.SZ → (0, '000001')；北交所返回 None。"""
    code, _, exchange = (ts_code or "").partition(".")
    if exchange == "SH":
        return 1, code
    if exchange == "SZ":
        return 0, code
    return None


def aggregate_stock(api, ts_code: str, date_int: int) -> dict:
    """分页拉单只股票当日分笔，按金额分档聚合。连接失败抛异常由上层换服务器。"""
    tdx = ts_code_to_tdx(ts_code)
    if tdx is None:
        raise ValueError(f"通达信协议不支持该市场: {ts_code}")
    market, code = tdx

    agg = {f"{side}_{tag}_{unit}": 0.0
           for tag, _, _ in BUCKETS for side in ("buy", "sell") for unit in ("vol", "amount")}
    start = 0
    while True:
        batch = api.get_history_transaction_data(market, code, start, PAGE_SIZE, date_int)
        if not batch:
            break
        for tick in batch:
            vol = float(tick.get("vol") or 0)          # 手
            amount = float(tick.get("price") or 0) * vol * 100.0  # 元
            side = tick.get("buyorsell")
            if side == 0:
                side_key = "buy"
            elif side == 1:
                side_key = "sell"
            else:
                continue  # 中性盘/盘后定价不计入主动买卖
            for tag, lo, hi in BUCKETS:
                if lo <= amount < hi:
                    agg[f"{side_key}_{tag}_vol"] += vol
                    agg[f"{side_key}_{tag}_amount"] += amount
                    break
        start += len(batch)
        if len(batch) < PAGE_SIZE:
            break
        time.sleep(PAGE_INTERVAL)

    row = {"ts_code": ts_code, "trade_date": "", **agg}
    row["net_mf_vol"] = (agg["buy_lg_vol"] + agg["buy_elg_vol"]
                         - agg["sell_lg_vol"] - agg["sell_elg_vol"])
    row["net_mf_amount"] = (agg["buy_lg_amount"] + agg["buy_elg_amount"]
                            - agg["sell_lg_amount"] - agg["sell_elg_amount"])
    return row


def merge_save(rows, trade_date: str) -> int:
    """与已有分区按 ts_code 合并后整分区覆写。"""
    if not rows:
        return 0
    from app.utils.parquet_writer import _data_root

    data_dir = _data_root(None)
    group = pd.DataFrame(rows, columns=OUTPUT_COLS)
    group["trade_date"] = trade_date
    clean = trade_date.replace("-", "")
    part_path = os.path.join(
        data_dir, REL_TABLE,
        f"year={clean[:4]}", f"month={clean[4:6]}", f"day={clean[6:8]}",
        "data.parquet",
    )
    if os.path.isfile(part_path):
        try:
            existing = pd.read_parquet(part_path)
            group = pd.concat([existing, group], ignore_index=True)
        except Exception as exc:  # noqa: BLE001
            print(f"[moneyflow_tdx] 读取已有分区失败 {part_path}: {exc}")
    group = group.drop_duplicates(subset=["ts_code"], keep="last")
    return save_to_parquet(group, trade_date, REL_TABLE)


def resolve_trade_date() -> str:
    explicit = normalize_ymd(os.getenv("DATA_JOB_TRADE_DATE"))
    if explicit:
        return explicit
    available = available_open_trade_dates()
    now = pd.Timestamp.now()
    today = now.strftime("%Y%m%d")
    eligible = [d for d in available if d <= today]
    if not eligible:
        raise RuntimeError("交易日历无可用开市日")
    # 分笔数据收盘后才就绪：18:00 前（含凌晨调度）回退到上一开市日，
    # 与 auto_init.DATA_READY_HOUR 的断档期望值同口径
    if eligible[-1] == today and now.hour < 18 and len(eligible) >= 2:
        return eligible[-2]
    return eligible[-1]


def main() -> int:
    from app.services.tongdaxin.client import (
        connect_first_available,
        create_hq_api,
    )

    trade_date = resolve_trade_date()
    date_int = int(trade_date.replace("-", ""))

    basic = pd.read_parquet(_PROJECT_ROOT / "data" / "stock_basic.parquet")
    ts_codes = sorted(basic["ts_code"].dropna().astype(str).unique().tolist())
    # 通达信免费协议对北交所支持不全，直接排除
    ts_codes = [c for c in ts_codes if not c.endswith(".BJ")]
    only = os.getenv("MONEYFLOW_TDX_CODES")
    if only:
        wanted = {c.strip() for c in only.split(",") if c.strip()}
        ts_codes = [c for c in ts_codes if c in wanted]
    print(f"[moneyflow_tdx] 交易日 {trade_date}，标的 {len(ts_codes)} 只", flush=True)

    api = create_hq_api()
    connect_first_available(api)

    checkpoint_path = plan_checkpoint_path(
        CHECKPOINT_ROOT,
        "moneyflow_tdx",
        {"trade_date": trade_date, "ts_codes": ts_codes},
    )
    checkpoint = load_checkpoint(checkpoint_path)
    completed = set(
        checkpoint.get("completed_ts_codes", [])
        if checkpoint.get("trade_date") == trade_date else []
    )
    if completed:
        print(f"[moneyflow_tdx] 从检查点续跑：已完成 {len(completed)}/{len(ts_codes)} 只")

    rows = []
    pending_codes = []
    failed = []
    total_saved = 0
    t0 = time.time()

    def flush():
        nonlocal total_saved
        if not pending_codes:
            return
        total_saved += merge_save(rows, trade_date)
        completed.update(pending_codes)
        save_checkpoint(checkpoint_path, {
            "version": 1,
            "trade_date": trade_date,
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
            except Exception as exc:  # noqa: BLE001 - 断线重连后重试一次
                try:
                    api.disconnect()
                except Exception:  # noqa: BLE001
                    pass
                api = create_hq_api()
                connect_first_available(api)
                try:
                    rows.append(aggregate_stock(api, ts_code, date_int))
                    pending_codes.append(ts_code)
                except Exception as exc2:  # noqa: BLE001
                    failed.append(ts_code)
                    print(f"[moneyflow_tdx] {ts_code} 拉取失败: {exc2}", flush=True)
            if len(pending_codes) >= CHUNK_STOCKS:
                flush()
                print(f"[moneyflow_tdx] 进度 {idx}/{len(ts_codes)}，累计写入 {total_saved}，"
                      f"耗时 {time.time() - t0:.0f}s", flush=True)
    finally:
        flush()
        try:
            api.disconnect()
        except Exception:  # noqa: BLE001
            pass

    print(f"[moneyflow_tdx] 完成：total_upsert={total_saved}，失败 {len(failed)} 只")
    if failed:
        return 1
    clear_checkpoint(checkpoint_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
