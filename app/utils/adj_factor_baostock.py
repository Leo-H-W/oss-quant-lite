"""stk_factor 复权口径的 Baostock 生产者：adj_factor + 复权收盘价。

tushare stk_factor 接口需要 5000 积分（本账号无权限），但下游
get_return_prices/get_ohlc_prices 只消费 (ts_code, trade_date, close_hfq)
三列。Baostock query_adjust_factor 免费可用，配合本地日线收盘价即可
重建复权口径：

- adj_factor：除权除息日的事件型因子按交易日向后填充（ffill）
- close_hfq = close × adj_factor（后复权，锚定上市首日，与 tushare 同概念）
- close_qfq = close × adj_factor / 最新 adj_factor（锚定最新交易日）

技术指标列（MACD/KDJ/RSI 等）不在此生成——分析页本来就走
_calculate_technical_indicators 本地兜底，此处只补齐复权链路。

默认区间：最新本地分区次日 → 今天；DATA_JOB_START_DATE/END_DATE 可显式指定。
"""
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

# 兼容直接运行（python app/utils/adj_factor_baostock.py）
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from app.services.data_reader import ParquetDataReader  # noqa: E402
from app.utils.daily_basic_baostock import (  # noqa: E402
    FETCH_TIMEOUT,
    _watchdog,
    raise_if_disconnected,
    ts_code_to_bs,
)
from app.utils.parquet_job_helpers import (  # noqa: E402
    existing_partition_dates,
    normalize_ymd,
)
from app.utils.job_checkpoint import (  # noqa: E402
    clear_checkpoint,
    load_checkpoint,
    save_checkpoint,
)
from app.utils.parquet_writer import save_to_parquet  # noqa: E402

REL_TABLE = "stk_factor/daily"
CHUNK_STOCKS = 500
REQUEST_INTERVAL = 0.05
CHECKPOINT_ROOT = _PROJECT_ROOT / "data" / "data_job_state" / "checkpoints"


def resolve_date_range() -> tuple:
    start = normalize_ymd(os.getenv("DATA_JOB_START_DATE"))
    end = normalize_ymd(os.getenv("DATA_JOB_END_DATE")) or datetime.now().strftime("%Y%m%d")
    if not start:
        have = existing_partition_dates(REL_TABLE)
        if have:
            start = (pd.Timestamp(max(have)) + pd.Timedelta(days=1)).strftime("%Y%m%d")
        else:
            start = end
    return start, end


def fetch_adjust_events(bs_module, ts_code: str) -> pd.DataFrame:
    """拉单只股票全部除权除息事件（因子为事件型，全量拉取再按日填充）。"""
    bs_code = ts_code_to_bs(ts_code)
    if not bs_code:
        return pd.DataFrame()
    rs = bs_module.query_adjust_factor(code=bs_code)
    if raise_if_disconnected(rs, ts_code):
        # 北交所等不受支持标的：无除权事件，按因子 1.0 处理
        return pd.DataFrame()
    rows = []
    while rs.error_code == "0" and rs.next():
        rows.append(rs.get_row_data())
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=rs.fields)
    df["dividOperateDate"] = pd.to_datetime(df["dividOperateDate"], errors="coerce")
    df["adjustFactor"] = pd.to_numeric(df["adjustFactor"], errors="coerce")
    return df.dropna(subset=["dividOperateDate", "adjustFactor"])


def build_daily_adj(daily: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    """事件型复权因子 → 逐日后复权/前复权收盘价。"""
    daily = daily.sort_values("trade_date").copy()
    if events.empty:
        # 从未除权：复权价即原值
        daily["adj_factor"] = 1.0
    else:
        events = events.sort_values("dividOperateDate")
        daily = pd.merge_asof(
            daily,
            events[["dividOperateDate", "adjustFactor"]],
            left_on="trade_date",
            right_on="dividOperateDate",
            direction="backward",
        )
        daily["adj_factor"] = daily["adjustFactor"].fillna(1.0)
        daily = daily.drop(columns=["dividOperateDate", "adjustFactor"])
    daily["close_hfq"] = daily["close"] * daily["adj_factor"]
    latest_factor = daily["adj_factor"].iloc[-1]
    daily["close_qfq"] = (
        daily["close"] * daily["adj_factor"] / latest_factor if latest_factor else daily["close"]
    )
    return daily[["ts_code", "trade_date", "adj_factor", "close", "close_hfq", "close_qfq"]]


def merge_save(chunk: pd.DataFrame) -> int:
    """与已有分区按 ts_code 合并后整分区覆写（同 daily_basic_baostock 语义）。"""
    if chunk.empty:
        return 0
    from app.utils.parquet_writer import _data_root

    data_dir = _data_root(None)
    chunk = chunk.copy()
    chunk["trade_date"] = pd.to_datetime(chunk["trade_date"]).dt.strftime("%Y-%m-%d")
    total = 0
    for date_value, group in chunk.groupby("trade_date", sort=True):
        clean = date_value.replace("-", "")
        part_path = os.path.join(
            data_dir, REL_TABLE,
            f"year={clean[:4]}", f"month={clean[4:6]}", f"day={clean[6:8]}",
            "data.parquet",
        )
        if os.path.isfile(part_path):
            try:
                existing = pd.read_parquet(part_path)
                # 已有分区可能带 tushare 全字段：新数据只覆盖本脚本的列
                merged = existing.set_index("ts_code")
                for col in group.columns:
                    if col in ("ts_code", "trade_date"):
                        continue
                    if col not in merged.columns:
                        merged[col] = pd.NA
                update = group.set_index("ts_code")
                merged.update(update)
                new_codes = update.index.difference(merged.index)
                group = pd.concat([merged.reset_index(), update.loc[new_codes].reset_index()], ignore_index=True)
            except Exception as exc:  # noqa: BLE001
                print(f"[adj_factor_baostock] 读取已有分区失败 {part_path}: {exc}")
        group = group.drop_duplicates(subset=["ts_code"], keep="last")
        total += save_to_parquet(group, date_value, REL_TABLE)
    return total


def main() -> int:
    import baostock as bs

    start, end = resolve_date_range()
    if start > end:
        print(f"[adj_factor_baostock] 无需拉取：start={start} > end={end}")
        return 0

    reader = ParquetDataReader()
    daily = reader.get_daily(start_date=start, end_date=end)
    if daily.empty:
        print(f"[adj_factor_baostock] 区间 {start}~{end} 无日线数据，跳过")
        return 0
    daily["trade_date"] = pd.to_datetime(daily["trade_date"])

    ts_codes = sorted(daily["ts_code"].astype(str).unique().tolist())
    print(f"[adj_factor_baostock] 区间 {start}~{end}，标的 {len(ts_codes)} 只")

    lg = bs.login()
    if lg.error_code != "0":
        print(f"[adj_factor_baostock] baostock 登录失败: {lg.error_msg}")
        return 1

    checkpoint_path = CHECKPOINT_ROOT / f"adj_factor_baostock_{start}_{end}.json"
    checkpoint = load_checkpoint(checkpoint_path)
    completed = set(
        checkpoint.get("completed_ts_codes", [])
        if checkpoint.get("start") == start and checkpoint.get("end") == end else []
    )
    if completed:
        print(f"[adj_factor_baostock] 从检查点续跑：已完成 {len(completed)}/{len(ts_codes)} 只")

    total_saved = 0
    failed = []
    buffer = []
    pending_codes = []

    def relogin():
        try:
            bs.logout()
        except Exception:  # noqa: BLE001
            pass
        lg2 = bs.login()
        if lg2.error_code != "0":
            raise RuntimeError(f"baostock 重登录失败: {lg2.error_msg}")

    def flush():
        nonlocal total_saved
        if not pending_codes:
            return
        if buffer:
            total_saved += merge_save(pd.concat(buffer, ignore_index=True))
        completed.update(pending_codes)
        save_checkpoint(checkpoint_path, {
            "version": 1,
            "start": start,
            "end": end,
            "completed_ts_codes": sorted(completed),
        })
        buffer.clear()
        pending_codes.clear()

    try:
        for idx, ts_code in enumerate(ts_codes, 1):
            if ts_code in completed:
                continue
            try:
                stock_daily = daily[daily["ts_code"] == ts_code]
                if stock_daily.empty:
                    pending_codes.append(ts_code)
                    continue
                with _watchdog(FETCH_TIMEOUT):
                    events = fetch_adjust_events(bs, ts_code)
                buffer.append(build_daily_adj(stock_daily, events))
                pending_codes.append(ts_code)
            except Exception as exc:  # noqa: BLE001
                failed.append(ts_code)
                print(f"[adj_factor_baostock] {ts_code} 处理失败: {exc}", flush=True)
                try:
                    relogin()
                except Exception as exc2:  # noqa: BLE001
                    print(f"[adj_factor_baostock] 重登录失败，中断: {exc2}", flush=True)
                    raise
            time.sleep(REQUEST_INTERVAL)

            if len(pending_codes) >= CHUNK_STOCKS:
                flush()
                print(f"[adj_factor_baostock] 进度 {idx}/{len(ts_codes)}，累计写入 {total_saved}", flush=True)
    finally:
        flush()
        bs.logout()

    print(f"[adj_factor_baostock] 完成：total_upsert={total_saved}，失败 {len(failed)} 只")
    if failed:
        print(f"[adj_factor_baostock] 失败标的: {failed[:20]}{'...' if len(failed) > 20 else ''}")
        return 1
    clear_checkpoint(checkpoint_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
