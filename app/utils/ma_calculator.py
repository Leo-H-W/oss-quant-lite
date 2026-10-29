"""均线衍生计算：MA5..MA120 / EMA5..EMA120，输出 stock_ma_data.parquet。

内存设计（小内存机器 earlyoom 会杀大 RSS 进程）：
单遍流式扫描 daily_history 分区，每只股票只保留最近 HISTORY_LEN 个收盘价
（deque(maxlen=HISTORY_LEN)），分区读取只取 ts_code/close 两列。
峰值内存 ≈ Python 基线 + 单分区两列 + 全市场 HISTORY_LEN 个 float，
稳定在 200MB 量级，与数据年限无关。

旧实现逐股调 get_daily 且不传日期范围：每次都把全部年份的全市场
daily_history 读进内存再过滤单股（峰值 1–2.5GB），在 1.8GB 机器上
必然触发 earlyoom SIGTERM，且 5400 次全表扫描的 IO 也不可接受。
"""
import os
import sys
from collections import deque
from pathlib import Path

import numpy as np
import pandas as pd

# 兼容直接运行（python app/utils/ma_calculator.py）
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from app.services.data_reader import ParquetDataReader  # noqa: E402
from app.utils.parquet_job_helpers import get_stock_codes  # noqa: E402
from app.utils.parquet_writer import save_single_parquet  # noqa: E402

# 计算窗口：最长均线 MA120/EMA120 需要足够预热，沿用旧实现的 250 个交易日
HISTORY_LEN = 250
STREAM_COLUMNS = ["ts_code", "close"]


def calculate_ema_manually(prices, period):
    if len(prices) < period:
        return None

    sma = np.mean(prices[:period])
    ema_values = [sma]
    multiplier = 2 / (period + 1)

    for i in range(period, len(prices)):
        ema = (prices[i] * multiplier) + (ema_values[-1] * (1 - multiplier))
        ema_values.append(ema)

    return ema_values[-1]


def stream_close_tails(reader, stock_codes=None, history_len=HISTORY_LEN):
    """单遍扫描 daily 分区，返回 {ts_code: deque(最近 history_len 个收盘价)}。

    分区目录按日期升序遍历，追加顺序即时间顺序；deque(maxlen) 自动只留尾部，
    与旧实现"全历史 sort 后 tail(250)"语义一致。
    """
    base = os.path.join(reader.data_dir, reader.TABLE_DIRS["daily"])
    if not os.path.isdir(base):
        print(f"daily_history 目录不存在: {base}", flush=True)
        return {}

    wanted = set(stock_codes) if stock_codes is not None else None
    tails = {}
    for path in reader._walk_partitions(base, None, None):
        try:
            df = pd.read_parquet(path, columns=STREAM_COLUMNS)
        except Exception as e:
            print(f"读取分区失败 {path}: {e}", flush=True)
            continue
        if df.empty:
            continue
        closes = pd.to_numeric(df["close"], errors="coerce")
        for code, close in zip(df["ts_code"].astype(str).values, closes.values):
            if wanted is not None and code not in wanted:
                continue
            if close != close:  # NaN
                continue
            buf = tails.get(code)
            if buf is None:
                buf = tails[code] = deque(maxlen=history_len)
            buf.append(float(close))
    return tails


def build_ma_rows(code_tail_pairs):
    """由 (ts_code, 收盘价序列) 计算 MA/EMA 行，口径与旧实现一致。"""
    rows = []
    for ts_code, closes in code_tail_pairs:
        if len(closes) < 5:
            continue
        series = pd.Series(list(closes), dtype="float64")
        n = len(series)

        def _ma(window):
            return series.rolling(window=window).mean().iloc[-1] if n >= window else None

        def _ewm(window):
            return series.ewm(span=window, adjust=False).mean().iloc[-1] if n >= window else None

        def _f(value):
            return float(value) if value is not None else None

        rows.append(
            {
                "ts_code": ts_code,
                "ma5": _f(_ma(5)),
                "ma10": _f(_ma(10)),
                "ma20": _f(_ma(20)),
                "ma30": _f(_ma(30)),
                "ma60": _f(_ma(60)),
                "ma120": _f(_ma(120)),
                "ema5": _f(_ewm(5)),
                "ema10": _f(_ewm(10)),
                "ema20": _f(_ewm(20)),
                "ema30": _f(_ewm(30)),
                "ema60": _f(calculate_ema_manually(series.values, 60) if n >= 60 else None),
                "ema120": _f(calculate_ema_manually(series.values, 120) if n >= 120 else None),
            }
        )
    return rows


def main():
    reader = ParquetDataReader()
    stock_codes = get_stock_codes()
    print(f"流式计算均线：{len(stock_codes)} 只股票，窗口 {HISTORY_LEN} 个交易日", flush=True)

    tails = stream_close_tails(reader, stock_codes)
    # 保持旧输出顺序：按 stock_basic 清单排序
    rows = build_ma_rows((code, tails[code]) for code in stock_codes if code in tails)

    if rows:
        save_single_parquet(pd.DataFrame(rows), "stock_ma_data.parquet")
        print(f"MA和EMA计算完成! 共 {len(rows)} 只股票", flush=True)


if __name__ == "__main__":
    main()
