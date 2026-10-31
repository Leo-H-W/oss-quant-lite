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
from typing import List

import numpy as np
import pandas as pd

# 兼容直接运行（python app/utils/cyq_perf_compute.py）
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from app.utils.parquet_job_helpers import normalize_ymd  # noqa: E402
from app.utils.job_checkpoint import clear_checkpoint, load_checkpoint, plan_checkpoint_path, save_checkpoint  # noqa: E402
from app.utils.parquet_writer import save_to_parquet  # noqa: E402

REL_TABLE = "cyq_perf/daily"
DAILY_REL = "daily_history/daily"
BASIC_REL = "daily_basic/daily"
BIN_COUNT = 100
TURNOVER_CAP = 0.5  # 单日换手封顶 50%，防异常值把筹码一天清空

# 按股票分批 + 逐分区列裁剪读取。全市场全历史一次性 get_daily 会
# concat 出千万行级 DataFrame（实测 RSS >700MB），小内存机器上被
# earlyoom SIGTERM；分批把峰值压到「批内股票 × 交易日」的规模。
# 默认 300：1.4GiB 内存机器上实测批=1000 仍会触线（输入 + 输出缓冲
# 双份大 Frame），300 时峰值 ~400MB。
DAILY_COLS = ["ts_code", "trade_date", "low", "high", "close", "vol", "amount"]
BASIC_COLS = ["ts_code", "trade_date", "turnover_rate"]
STOCK_BATCH = int(os.getenv("CYQ_PERF_STOCK_BATCH", "300"))

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


def _partition_file(data_root: str, rel_table: str, ymd: str) -> Path:
    return (
        Path(data_root) / rel_table
        / f"year={ymd[0:4]}" / f"month={ymd[4:6]}" / f"day={ymd[6:8]}"
        / "data.parquet"
    )


def _load_columns_for_codes(
    rel_table: str,
    columns: List[str],
    codes: set,
    start: str,
    end: str,
    data_root: str,
) -> pd.DataFrame:
    """逐分区读取 + 列裁剪 + 代码过滤，峰值内存只含本批次股票的行。"""
    from app.utils.parquet_job_helpers import existing_partition_dates

    frames = []
    for ymd in sorted(existing_partition_dates(rel_table, data_root)):
        if ymd < start or ymd > end:
            continue
        path = _partition_file(data_root, rel_table, ymd)
        if not path.is_file():
            continue
        try:
            df = pd.read_parquet(path, columns=columns)
        except Exception as exc:  # noqa: BLE001
            print(f"[cyq_perf_compute] 读取分区失败 {path}: {exc}")
            continue
        df = df[df["ts_code"].astype(str).isin(codes)]
        if not df.empty:
            frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _list_daily_codes(data_root: str, start: str, end: str) -> List[str]:
    """扫描日线分区收集股票代码全集（含已退市，stock_basic 不一定覆盖）。"""
    from app.utils.parquet_job_helpers import existing_partition_dates

    codes: set = set()
    for ymd in sorted(existing_partition_dates(DAILY_REL, data_root)):
        if ymd < start or ymd > end:
            continue
        path = _partition_file(data_root, DAILY_REL, ymd)
        if not path.is_file():
            continue
        try:
            codes.update(pd.read_parquet(path, columns=["ts_code"])["ts_code"].astype(str))
        except Exception as exc:  # noqa: BLE001
            print(f"[cyq_perf_compute] 读取分区失败 {path}: {exc}")
    return sorted(codes)


def main() -> int:
    import gc

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

    from app.utils.parquet_writer import _data_root

    data_root = _data_root(None)

    ts_codes = _list_daily_codes(data_root, start, end)
    if not ts_codes:
        print(f"[cyq_perf_compute] 区间 {start}~{end} 无日线数据")
        return 1
    print(f"[cyq_perf_compute] 区间 {start}~{end}，标的 {len(ts_codes)} 只，"
          f"bins={BIN_COUNT}，批大小 {STOCK_BATCH}", flush=True)

    checkpoint_path = plan_checkpoint_path(
        os.path.join(data_root, "data_job_state", "checkpoints"),
        "cyq_perf_compute",
        {"start": start, "end": end, "ts_codes": ts_codes},
    )
    checkpoint = load_checkpoint(checkpoint_path)
    completed = set(
        checkpoint.get("completed_ts_codes", [])
        if checkpoint.get("start") == start and checkpoint.get("end") == end else []
    )
    if completed:
        print(f"[cyq_perf_compute] 从检查点续跑：{len(completed)}/{len(ts_codes)}")

    total_saved = 0
    failed = []
    processed = 0
    t0 = time.time()
    for batch_start in range(0, len(ts_codes), STOCK_BATCH):
        batch = ts_codes[batch_start:batch_start + STOCK_BATCH]
        remaining = [code for code in batch if code not in completed]
        if not remaining:
            processed += len(batch)
            continue
        code_set = set(remaining)

        daily = _load_columns_for_codes(DAILY_REL, DAILY_COLS, code_set, start, end, data_root)
        if daily.empty:
            completed.update(remaining)
            save_checkpoint(checkpoint_path, {
                "version": 1, "start": start, "end": end,
                "completed_ts_codes": sorted(completed),
            })
            processed += len(batch)
            continue
        basic = _load_columns_for_codes(BASIC_REL, BASIC_COLS, code_set, start, end, data_root)

        daily["trade_date"] = pd.to_datetime(daily["trade_date"])
        if not basic.empty:
            basic["trade_date"] = pd.to_datetime(basic["trade_date"])
            daily = daily.merge(
                basic[["ts_code", "trade_date", "turnover_rate"]],
                on=["ts_code", "trade_date"], how="left",
            )
        else:
            daily["turnover_rate"] = np.nan
        del basic

        buffer = []
        batch_completed = []
        for ts_code in remaining:
            try:
                out = compute_stock_cyq(daily[daily["ts_code"] == ts_code])
                batch_completed.append(ts_code)
                if not out.empty:
                    buffer.append(out)
            except Exception as exc:  # noqa: BLE001
                failed.append(ts_code)
                print(f"[cyq_perf_compute] {ts_code} 计算失败: {exc}")
        if buffer:
            total_saved += merge_save(pd.concat(buffer, ignore_index=True))
        completed.update(batch_completed)
        save_checkpoint(checkpoint_path, {
            "version": 1, "start": start, "end": end,
            "completed_ts_codes": sorted(completed),
        })
        processed += len(batch)
        del daily, buffer
        gc.collect()
        print(f"[cyq_perf_compute] 进度 {processed}/{len(ts_codes)}，累计写入 {total_saved}，"
              f"耗时 {time.time() - t0:.0f}s", flush=True)

    print(f"[cyq_perf_compute] 完成：total_upsert={total_saved}，失败 {len(failed)} 只")
    if failed:
        return 1
    clear_checkpoint(checkpoint_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
