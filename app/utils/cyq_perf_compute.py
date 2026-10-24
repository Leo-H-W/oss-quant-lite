"""cyq_perf 本地复算：成本分布衰减模型（通达信筹码分布同思路）。

背景：tushare cyq_perf 需 5000 积分（本账号无权限）；通达信的筹码分布
本来就是客户端本地按换手率衰减算的，不依赖任何服务端筹码数据。
本脚本复刻该模型，产出与 tushare cyq_perf 同构的分区表。

模型：
- 价格轴分 BIN_COUNT 格，覆盖计算窗口内 [min(low), max(high)]
- 首日筹码均匀分布在当日 [low, high]
- 每日：存量筹码 × (1 - turnover) 衰减，turnover 份额按三角分布落在
  当日 [low, high]（峰值 = 当日均价 amount/vol）
- turnover = turnover_rate/100；缺失日视为 0（筹码冻结）

输出字段（tushare cyq_perf 同构）：
his_low/his_high（窗口价格极值）、cost_5pct/15/50/85/95pct（筹码分位价）、
weight_avg（加权平均成本）、winner_rate（收盘下方筹码占比，百分数）

注意：筹码从窗口首日 seed 起需要约 200+ 个交易日收敛，窗口起点过早的
日期输出仅供参考；turnover 只有 daily_basic 覆盖的区间才有效。
"""
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

# 兼容直接运行（python app/utils/cyq_perf_compute.py）
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from app.services.data_reader import ParquetDataReader  # noqa: E402
from app.utils.parquet_job_helpers import normalize_ymd  # noqa: E402
from app.utils.parquet_writer import save_to_parquet  # noqa: E402

REL_TABLE = "cyq_perf/daily"
BIN_COUNT = 100
TURNOVER_CAP = 0.5  # 单日换手封顶 50%，防异常值把筹码一天清空

OUTPUT_COLS = [
    "ts_code", "trade_date", "his_low", "his_high",
    "cost_5pct", "cost_15pct", "cost_50pct", "cost_85pct", "cost_95pct",
    "weight_avg", "winner_rate",
]


def _triangular_weights(prices: np.ndarray, low: float, high: float, peak: float) -> np.ndarray:
    """当日新增筹码在 [low, high] 上的三角分布权重（和为 1）。"""
    in_range = (prices >= low) & (prices <= high)
    if not in_range.any():
        # 一字板/越界：全部落在最接近的格子
        idx = int(np.argmin(np.abs(prices - peak)))
        w = np.zeros_like(prices)
        w[idx] = 1.0
        return w
    w = np.zeros_like(prices)
    span = max(high - low, 1e-9)
    # 三角：距 peak 越远权重越低
    w[in_range] = 1.0 - np.abs(prices[in_range] - peak) / span
    total = w.sum()
    if total <= 0:
        w[in_range] = 1.0 / in_range.sum()
    else:
        w /= total
    return w


def _percentile_price(cum: np.ndarray, prices: np.ndarray, q: float) -> float:
    idx = int(np.searchsorted(cum, q))
    idx = min(idx, len(prices) - 1)
    return float(prices[idx])


def compute_stock_cyq(stock_df: pd.DataFrame) -> pd.DataFrame:
    """单只股票逐日筹码分布指标。输入须含 trade_date/low/high/close/vol/amount/turnover_rate。"""
    df = stock_df.sort_values("trade_date").reset_index(drop=True)
    his_low = float(df["low"].min())
    his_high = float(df["high"].max())
    if not np.isfinite(his_low) or not np.isfinite(his_high) or his_high <= his_low:
        return pd.DataFrame()

    prices = np.linspace(his_low, his_high, BIN_COUNT)
    chips = np.zeros(BIN_COUNT)

    rows = []
    for r in df.itertuples():
        low, high, close = float(r.low), float(r.high), float(r.close)
        if not all(np.isfinite([low, high, close])) or close <= 0:
            continue
        avg_price = close
        if getattr(r, "vol", 0) and np.isfinite(r.vol) and r.vol > 0 and np.isfinite(r.amount):
            # amount(千元)/vol(手) → 均价(元) = amount*1000/(vol*100)
            avg_price = float(r.amount) * 10.0 / float(r.vol)
            avg_price = min(max(avg_price, low), high)
        turnover = getattr(r, "turnover_rate", None)
        turnover = float(turnover) / 100.0 if turnover is not None and np.isfinite(turnover) else 0.0
        turnover = min(max(turnover, 0.0), TURNOVER_CAP)

        add = _triangular_weights(prices, low, high, avg_price)
        if chips.sum() <= 0:
            chips = add  # 首日 seed：当日全部筹码
        else:
            chips = chips * (1.0 - turnover) + add * turnover

        cum = np.cumsum(chips)
        total = cum[-1]
        if total <= 0:
            continue
        cum = cum / total
        winner = float(np.clip(np.interp(close, prices, cum), 0, 1) * 100.0)
        rows.append({
            "ts_code": r.ts_code,
            "trade_date": r.trade_date,
            "his_low": his_low,
            "his_high": his_high,
            "cost_5pct": _percentile_price(cum, prices, 0.05),
            "cost_15pct": _percentile_price(cum, prices, 0.15),
            "cost_50pct": _percentile_price(cum, prices, 0.50),
            "cost_85pct": _percentile_price(cum, prices, 0.85),
            "cost_95pct": _percentile_price(cum, prices, 0.95),
            "weight_avg": float((prices * chips).sum() / chips.sum()),
            "winner_rate": winner,
        })
    return pd.DataFrame(rows, columns=OUTPUT_COLS)


def merge_save(chunk: pd.DataFrame) -> int:
    """与已有分区按 ts_code 合并后整分区覆写。"""
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
                group = pd.concat([existing, group], ignore_index=True)
            except Exception as exc:  # noqa: BLE001
                print(f"[cyq_perf_compute] 读取已有分区失败 {part_path}: {exc}")
        group = group.drop_duplicates(subset=["ts_code"], keep="last")
        total += save_to_parquet(group, date_value, REL_TABLE)
    return total


def main() -> int:
    start = normalize_ymd(os.getenv("DATA_JOB_START_DATE"))
    end = normalize_ymd(os.getenv("DATA_JOB_END_DATE"))
    if not start:
        # 筹码收敛需要完整预热窗：缺省从 daily_basic 有数据之日起算
        from app.utils.parquet_job_helpers import existing_partition_dates

        have = existing_partition_dates("daily_basic/daily")
        if not have:
            print("[cyq_perf_compute] daily_basic 无数据，无法获得换手率历史")
            return 1
        start = min(have)
    if not end:
        end = datetime.now().strftime("%Y%m%d")

    reader = ParquetDataReader()
    daily = reader.get_daily(start_date=start, end_date=end)
    if daily.empty:
        print(f"[cyq_perf_compute] 区间 {start}~{end} 无日线数据")
        return 1
    basic = reader.get_daily_basic(start_date=start, end_date=end)

    daily = daily.copy()
    daily["trade_date"] = pd.to_datetime(daily["trade_date"])
    if not basic.empty:
        basic = basic.copy()
        basic["trade_date"] = pd.to_datetime(basic["trade_date"])
        daily = daily.merge(
            basic[["ts_code", "trade_date", "turnover_rate"]],
            on=["ts_code", "trade_date"], how="left",
        )
    else:
        daily["turnover_rate"] = np.nan

    ts_codes = sorted(daily["ts_code"].astype(str).unique().tolist())
    print(f"[cyq_perf_compute] 区间 {start}~{end}，标的 {len(ts_codes)} 只，bins={BIN_COUNT}")

    total_saved = 0
    failed = []
    buffer = []
    t0 = time.time()
    for idx, ts_code in enumerate(ts_codes, 1):
        try:
            out = compute_stock_cyq(daily[daily["ts_code"] == ts_code])
            if not out.empty:
                buffer.append(out)
        except Exception as exc:  # noqa: BLE001
            failed.append(ts_code)
            print(f"[cyq_perf_compute] {ts_code} 计算失败: {exc}")
        if len(buffer) >= 500:
            total_saved += merge_save(pd.concat(buffer, ignore_index=True))
            buffer.clear()
            print(f"[cyq_perf_compute] 进度 {idx}/{len(ts_codes)}，累计写入 {total_saved}，"
                  f"耗时 {time.time() - t0:.0f}s", flush=True)
    if buffer:
        total_saved += merge_save(pd.concat(buffer, ignore_index=True))

    print(f"[cyq_perf_compute] 完成：total_upsert={total_saved}，失败 {len(failed)} 只")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
