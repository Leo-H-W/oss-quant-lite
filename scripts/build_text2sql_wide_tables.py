#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从 Baostock 分区 Parquet 物化 text2sql 引擎需要的宽表。

开源 text2sql 引擎（app/services/text2sql_engine.py）按 TABLE_SOURCES 约定从
data/stock_business.parquet 与 data/stock_ma_data.parquet 按需加载虚拟表；
Baostock 同步脚本只写分区目录（daily_history/daily、daily_basic/daily），
本脚本把分区数据合并成引擎期望的宽表，并计算技术指标：

  - data/stock_business.parquet  行情+估值+技术指标（macd/rsi/kdj，factor_* 列名）
  - data/stock_ma_data.parquet   5/10/20/30/60/120 日均线

幂等，可重复运行。资金流（moneyflow_*）Baostock 无对应数据源，不在此生成。

内存约束（部署环境 cgroup ~800MB 上限，全市场全历史约千万行）：
- 引擎侧实际只消费最新交易日（text2sql_engine 载入后按 trade_date 取
  max 过滤），data_reader.get_stock_business 无调用方；因此默认只保留
  最近 WIDE_TABLE_DAYS 个开市日（默认 400，约 1.6 年，MA120 充分预热），
  设 WIDE_TABLE_DAYS=0 可切回全量（需要更大内存）；
- 两遍读：第一遍只读 ts_code/trade_date 两列收集全局 category 字典，
  第二遍按全局 category 读入——避免 object 字符串列（约 700MB）；
- 数值列读入即降 float32；
- 不做全局 sort/drop_duplicates/merge（都是全量复制），排序去重和
  daily_basic/股票名称的关联全部下沉到按股票分组的小组内完成；
- 按股票流式计算，每攒 BATCH_SIZE 只用 ParquetWriter 落一次盘。
"""
import gc
import os
import sys
import glob
import logging

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "data")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("build_wide")

# 下游实际用到的列（open/pre_close/change 等不读，省内存）
HIST_COLS = ["ts_code", "trade_date", "close", "low", "high", "pct_chg", "vol", "amount"]
BASIC_COLS = ["ts_code", "trade_date", "pe", "pe_ttm", "pb", "turnover_rate", "total_mv", "circ_mv"]

# 每批处理的股票数：300 只 ≈ 60 万行 ≈ 几十 MB，远小于内存上限
BATCH_SIZE = 300

# 保留最近多少个开市日（0 = 全量，需要更大内存）
WINDOW_DAYS = int(os.getenv("WIDE_TABLE_DAYS", "400"))

BUSINESS_COLS = [
    "ts_code", "stock_name", "trade_date", "close",
    "factor_pct_change", "factor_vol", "factor_amount",
    "vol", "amount",
    "pe", "pe_ttm", "pb", "turnover_rate", "total_mv", "circ_mv",
    "factor_macd", "factor_macd_dif", "factor_macd_dea",
    "factor_rsi_6", "factor_rsi_12", "factor_rsi_24",
    "factor_kdj_k", "factor_kdj_d", "factor_kdj_j",
]
MA_COLS = ["ts_code", "trade_date"] + [f"ma{n}" for n in (5, 10, 20, 30, 60, 120)]


def _list_files(subdir: str):
    files = sorted(glob.glob(os.path.join(DATA, subdir, "**", "*.parquet"), recursive=True))
    if not files:
        raise SystemExit(f"分区目录无数据: {subdir}")
    return files


def collect_categories(files) -> dict:
    """第一遍：只读 ts_code/trade_date 两列，收集全局 category 字典。"""
    codes, dates = set(), set()
    for f in files:
        part = pd.read_parquet(f, columns=["ts_code", "trade_date"])
        codes.update(part["ts_code"].dropna().unique().tolist())
        dates.update(part["trade_date"].dropna().unique().tolist())
    return {"ts_code": sorted(codes), "trade_date": sorted(dates)}


def read_partitioned(subdir: str, columns, categories: dict, since: str = None) -> pd.DataFrame:
    """第二遍：按全局 category 读入，数值列即降 float32。

    各文件共享同一组 categories，concat 后仍是 category dtype，
    全程不出现 object 字符串大列。since（含）之前的行直接丢弃。
    """
    frames = []
    for f in _list_files(subdir):
        part = pd.read_parquet(f, columns=columns)
        if since is not None and "trade_date" in part.columns:
            # trade_date 为 YYYY-MM-DD 字符串，可直接按字典序过滤
            part = part[part["trade_date"] >= since]
            if part.empty:
                continue
        for col in ("ts_code", "trade_date"):
            if col in part.columns:
                part[col] = pd.Categorical(part[col], categories=categories[col])
        float_cols = part.select_dtypes(include=["float64"]).columns
        if len(float_cols):
            part[float_cols] = part[float_cols].astype(np.float32)
        frames.append(part)
    df = pd.concat(frames, ignore_index=True, copy=False)
    del frames
    gc.collect()
    return df


def ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False).mean()


def add_technical_factors(g: pd.DataFrame) -> pd.DataFrame:
    close = g["close"]
    # MACD(12,26,9)
    dif = ema(close, 12) - ema(close, 26)
    dea = ema(dif, 9)
    g["factor_macd_dif"] = dif.astype(np.float32)
    g["factor_macd_dea"] = dea.astype(np.float32)
    g["factor_macd"] = ((dif - dea) * 2).astype(np.float32)
    # RSI(6/12/24)
    for n in (6, 12, 24):
        delta = close.diff()
        gain = delta.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean()
        loss = (-delta.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean()
        rs = gain / loss.replace(0, np.nan)
        g[f"factor_rsi_{n}"] = (100 - 100 / (1 + rs)).astype(np.float32)
    # KDJ(9,3,3)
    low9 = g["low"].rolling(9, min_periods=1).min()
    high9 = g["high"].rolling(9, min_periods=1).max()
    rsv = (close - low9) / (high9 - low9).replace(0, np.nan) * 100
    k = rsv.ewm(com=2, adjust=False).mean()
    d = k.ewm(com=2, adjust=False).mean()
    g["factor_kdj_k"] = k.astype(np.float32)
    g["factor_kdj_d"] = d.astype(np.float32)
    g["factor_kdj_j"] = (3 * k - 2 * d).astype(np.float32)
    # 均线
    for n in (5, 10, 20, 30, 60, 120):
        g[f"ma{n}"] = close.rolling(n, min_periods=1).mean().astype(np.float32)
    return g


# 显式输出 schema：各批次 stock_name 来源不同（字典命中/回退 ts_code），
# from_pandas 自动推断会在 string/large_string 间漂移，导致 ParquetWriter
# 拒绝后续批次。固定 schema 后各批一致。
def _output_schema(cols) -> pa.Schema:
    string_cols = {"ts_code", "stock_name", "trade_date"}
    return pa.schema([
        (col, pa.string() if col in string_cols else pa.float32())
        for col in cols
    ])


def _to_table(batch: pd.DataFrame, cols) -> pa.Table:
    """批次落盘前的类型规整：category → string，套用显式 schema。"""
    out = batch.loc[:, cols].copy()
    for col in out.columns:
        if isinstance(out[col].dtype, pd.CategoricalDtype):
            out[col] = out[col].astype(str)
    return pa.Table.from_pandas(out, schema=_output_schema(cols), preserve_index=False)


def build_output_frame(g: pd.DataFrame, basic_sub: pd.DataFrame, stock_name: str) -> pd.DataFrame:
    """单只股票的输出帧：组内排序去重、关联 daily_basic、挂股票名、算因子。

    这些操作若放在全量框架上都是千万行级复制，下沉到单股组（约几千行）后
    每次只付出小组成本。
    """
    g = g.sort_values("trade_date").drop_duplicates("trade_date", keep="last")
    if not basic_sub.empty:
        g = g.merge(basic_sub, on="trade_date", how="left")
    else:
        for col in ("pe", "pe_ttm", "pb", "turnover_rate", "total_mv", "circ_mv"):
            g[col] = np.float32(np.nan)
    g["stock_name"] = stock_name
    # text2sql 引擎约定的 factor_* 列名
    g["factor_pct_change"] = g["pct_chg"]
    g["factor_vol"] = g["vol"]
    g["factor_amount"] = g["amount"]
    return add_technical_factors(g)


def main():
    hist_files = _list_files(os.path.join("daily_history", "daily"))
    log.info(f"第一遍扫描 {len(hist_files)} 个分区，收集全局字典 ...")
    categories = collect_categories(hist_files)
    log.info(f"  股票 {len(categories['ts_code'])} 只，交易日 {len(categories['trade_date'])} 天")

    since = None
    if WINDOW_DAYS > 0 and len(categories["trade_date"]) > WINDOW_DAYS:
        since = categories["trade_date"][-WINDOW_DAYS]
        log.info(f"窗口截断：仅保留 {since} 及之后（最近 {WINDOW_DAYS} 个开市日；WIDE_TABLE_DAYS=0 可全量）")

    log.info("第二遍读入日线数据 ...")
    df = read_partitioned(
        os.path.join("daily_history", "daily"), HIST_COLS, categories, since=since
    )
    log.info(f"  共 {len(df)} 行")

    basic = read_partitioned(
        os.path.join("daily_basic", "daily"), BASIC_COLS, categories
    )
    # 预切片：按股票分组的 daily_basic 查找表（basic 本身很小）
    basic_by_code = {
        code: sub.drop(columns=["ts_code"])
        for code, sub in basic.groupby("ts_code", observed=True)
    }
    del basic
    gc.collect()

    # 股票名称映射（标量字典，组内赋值，避免全量 merge 的 object 列）
    name_map = {}
    sb_path = os.path.join(DATA, "stock_basic.parquet")
    if os.path.exists(sb_path):
        sb = pd.read_parquet(sb_path, columns=["ts_code", "name"])
        name_map = dict(zip(sb["ts_code"], sb["name"]))

    log.info("按股票流式计算 MACD/RSI/KDJ/均线并分批落盘 ...")
    wide_path = os.path.join(DATA, "stock_business.parquet")
    ma_path = os.path.join(DATA, "stock_ma_data.parquet")
    wide_writer = None
    ma_writer = None
    batch = []
    total_rows = 0
    n_stocks = 0

    def flush():
        nonlocal wide_writer, ma_writer, batch, total_rows
        if not batch:
            return
        chunk = pd.concat(batch, ignore_index=True)
        wide_table = _to_table(chunk, BUSINESS_COLS)
        ma_table = _to_table(chunk, MA_COLS)
        if wide_writer is None:
            wide_writer = pq.ParquetWriter(wide_path, wide_table.schema)
            ma_writer = pq.ParquetWriter(ma_path, ma_table.schema)
        wide_writer.write_table(wide_table)
        ma_writer.write_table(ma_table)
        total_rows += chunk.shape[0]
        del chunk, wide_table, ma_table
        batch = []
        gc.collect()

    try:
        for code, g in df.groupby("ts_code", observed=True, sort=False):
            out = build_output_frame(
                g.copy(),
                basic_by_code.get(code, pd.DataFrame()),
                name_map.get(code, str(code)),
            )
            batch.append(out)
            n_stocks += 1
            if n_stocks % BATCH_SIZE == 0:
                flush()
                log.info(f"  已处理 {n_stocks} 只股票 / 累计 {total_rows} 行")
        flush()
    finally:
        if wide_writer is not None:
            wide_writer.close()
        if ma_writer is not None:
            ma_writer.close()

    del df
    gc.collect()

    wide_meta = pq.read_metadata(wide_path)
    ma_meta = pq.read_metadata(ma_path)
    log.info(f"stock_business.parquet: {wide_meta.num_rows} 行 x {wide_meta.num_columns} 列")
    log.info(f"stock_ma_data.parquet: {ma_meta.num_rows} 行 x {ma_meta.num_columns} 列")

    # 校验引擎能按 TABLE_COLUMNS 取到全部关键列（比对映射的 Parquet 列名，
    # 只读 schema，不加载数据）
    from app.services.text2sql_engine import QueryExecutor
    need = set(QueryExecutor.TABLE_COLUMNS["stock_business"].values()) \
        | set(QueryExecutor.TABLE_COLUMNS["stock_ma_data"].values())
    have = set(pq.read_schema(wide_path).names) | set(pq.read_schema(ma_path).names)
    missing = sorted(need - have)
    if missing:
        log.warning(f"引擎期望但无数据源的列（对应查询会提示字段不存在）: {missing}")
    log.info("完成")


if __name__ == "__main__":
    sys.path.insert(0, ROOT)
    main()
