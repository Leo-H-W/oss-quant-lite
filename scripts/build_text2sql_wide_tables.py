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
"""
import os
import sys
import glob
import logging

import pandas as pd
import numpy as np

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "data")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("build_wide")


def read_partitioned(subdir: str) -> pd.DataFrame:
    files = sorted(glob.glob(os.path.join(DATA, subdir, "**", "*.parquet"), recursive=True))
    if not files:
        raise SystemExit(f"分区目录无数据: {subdir}")
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    df = df.drop_duplicates(subset=["ts_code", "trade_date"])
    return df.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)


def ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False).mean()


def add_technical_factors(g: pd.DataFrame) -> pd.DataFrame:
    close = g["close"]
    # MACD(12,26,9)
    dif = ema(close, 12) - ema(close, 26)
    dea = ema(dif, 9)
    g["factor_macd_dif"] = dif
    g["factor_macd_dea"] = dea
    g["factor_macd"] = (dif - dea) * 2
    # RSI(6/12/24)
    for n in (6, 12, 24):
        delta = close.diff()
        gain = delta.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean()
        loss = (-delta.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean()
        rs = gain / loss.replace(0, np.nan)
        g[f"factor_rsi_{n}"] = 100 - 100 / (1 + rs)
    # KDJ(9,3,3)
    low9 = g["low"].rolling(9, min_periods=1).min()
    high9 = g["high"].rolling(9, min_periods=1).max()
    rsv = (close - low9) / (high9 - low9).replace(0, np.nan) * 100
    k = rsv.ewm(com=2, adjust=False).mean()
    d = k.ewm(com=2, adjust=False).mean()
    g["factor_kdj_k"] = k
    g["factor_kdj_d"] = d
    g["factor_kdj_j"] = 3 * k - 2 * d
    # 均线
    for n in (5, 10, 20, 30, 60, 120):
        g[f"ma{n}"] = close.rolling(n, min_periods=1).mean()
    return g


def main():
    log.info("读取分区数据 ...")
    hist = read_partitioned(os.path.join("daily_history", "daily"))
    basic = read_partitioned(os.path.join("daily_basic", "daily"))

    df = hist.merge(basic, on=["ts_code", "trade_date"], how="left")

    # 股票名称（全市场基础资料，只保留池内股票）
    sb_path = os.path.join(DATA, "stock_basic.parquet")
    if os.path.exists(sb_path):
        sb = pd.read_parquet(sb_path, columns=["ts_code", "name"])
        df = df.merge(sb, on="ts_code", how="left")
        df.rename(columns={"name": "stock_name"}, inplace=True)
    else:
        df["stock_name"] = df["ts_code"]

    # text2sql 引擎约定的 factor_* 列名
    df["factor_pct_change"] = df["pct_chg"]
    df["factor_vol"] = df["vol"]
    df["factor_amount"] = df["amount"]

    log.info("按股票计算 MACD/RSI/KDJ/均线 ...")
    df = pd.concat([add_technical_factors(g.copy()) for _, g in df.groupby("ts_code")],
                   ignore_index=True)

    # 对齐引擎 TABLE_COLUMNS 映射（只保留有数据源的列）
    business_cols = [
        "ts_code", "stock_name", "trade_date", "close",
        "factor_pct_change", "factor_vol", "factor_amount",
        "vol", "amount",
        "pe_ttm", "pb", "turnover_rate",
        "factor_macd", "factor_macd_dif", "factor_macd_dea",
        "factor_rsi_6", "factor_rsi_12", "factor_rsi_24",
        "factor_kdj_k", "factor_kdj_d", "factor_kdj_j",
    ]
    wide = df[business_cols].copy()
    wide.to_parquet(os.path.join(DATA, "stock_business.parquet"), index=False)
    log.info(f"stock_business.parquet: {len(wide)} 行 x {wide.shape[1]} 列, "
             f"区间 {wide['trade_date'].min()} ~ {wide['trade_date'].max()}")

    ma_cols = ["ts_code", "trade_date"] + [f"ma{n}" for n in (5, 10, 20, 30, 60, 120)]
    ma = df[ma_cols].copy()
    ma.to_parquet(os.path.join(DATA, "stock_ma_data.parquet"), index=False)
    log.info(f"stock_ma_data.parquet: {len(ma)} 行 x {ma.shape[1]} 列")

    # 校验引擎能按 TABLE_COLUMNS 取到全部关键列
    from app.services.text2sql_engine import QueryExecutor
    need = set(QueryExecutor.TABLE_COLUMNS["stock_business"]) \
        | set(QueryExecutor.TABLE_COLUMNS["stock_ma_data"])
    have = set(wide.columns) | set(ma.columns)
    missing = sorted(need - have)
    if missing:
        log.warning(f"引擎期望但无数据源的列（对应查询会提示字段不存在）: {missing}")
    log.info("完成")


if __name__ == "__main__":
    sys.path.insert(0, ROOT)
    main()
