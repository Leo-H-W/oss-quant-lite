#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Baostock 引导同步脚本（免 Tushare token）。

为开源项目 quantitative_analysis 生成其数据层要求的 Parquet 资产：
  - data/stock_basic.parquet           股票基础资料（全市场，tushare 同构字段）
  - data/stock_trade_calendar.parquet  交易日历
  - data/daily_history/daily/          股票池日线行情（按 trade_date 分区）
  - data/daily_basic/daily/            股票池日线估值指标（按 trade_date 分区）

用法：
  .venv/Scripts/python.exe scripts/bootstrap_baostock_pool.py

可通过环境变量覆盖股票池（逗号分隔 tushare 代码）：
  BS_POOL=600900.SH,600519.SH,300750.SZ
"""
import os
import sys
import time

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(BASE_DIR, "app", "utils"))

import baostock as bs
import pandas as pd

from parquet_writer import save_partitioned_parquet, save_single_parquet

DATA_DIR = os.path.join(BASE_DIR, "data")

DEFAULT_POOL = [
    "600900.SH", "600519.SH", "600036.SH",  # 防御/价值
    "300750.SZ", "300308.SZ", "002594.SZ",  # 成长
    "601899.SH", "688981.SH", "002475.SZ",  # 资源/科创/消费电子
]


def to_bs_code(ts_code: str) -> str:
    return ("sz." if ts_code.endswith(".SZ") else "sh.") + ts_code.split(".")[0]


def to_ts_code(bs_code: str) -> str:
    num = bs_code.split(".")[1]
    return f"{num}.SH" if bs_code.startswith("sh.") else f"{num}.SZ"


def fetch_stock_basic() -> pd.DataFrame:
    """全市场股票基础资料，输出与 tushare stock_basic 同构字段。"""
    rs = bs.query_stock_basic()
    rows = []
    while (rs.error_code == "0") and rs.next():
        rows.append(rs.get_row_data())
    df = pd.DataFrame(rows, columns=rs.fields)
    # Baostock: code, code_name, ipoDate, outDate, type, status
    df = df[df["type"] == "1"]  # 仅保留股票
    out = pd.DataFrame({
        "ts_code": df["code"].map(to_ts_code),
        "symbol": df["code"].map(lambda c: c.split(".")[1]),
        "name": df["code_name"],
        "area": "",
        "industry": "",
        "list_date": df["ipoDate"].replace("", None),
        "delist_date": df["outDate"].replace("", None),
        "list_status": df["status"].map({"1": "L", "0": "D", "2": "P"}),
    })
    # 行业走独立接口
    try:
        rs2 = bs.query_stock_industry()
        rows2 = []
        while (rs2.error_code == "0") and rs2.next():
            rows2.append(rs2.get_row_data())
        ind = pd.DataFrame(rows2, columns=rs2.fields)
        if not ind.empty:
            m = dict(zip(ind["code"].map(to_ts_code), ind["industry"]))
            out["industry"] = out["ts_code"].map(m).fillna("")
    except Exception as exc:  # noqa: BLE001
        print(f"[bootstrap] 行业获取失败（忽略）: {exc}")
    return out


def build_trade_calendar_from_daily(daily_frames) -> pd.DataFrame:
    """从已抓取的日线交易日并集推导日历（Baostock 的 query_trade_dates
    在部分网络环境下逐行轮询会长时间无响应，这里改用日线的真实交易日）。
    """
    days = sorted({str(d) for df in daily_frames for d in df["trade_date"]})
    pre = [""] + days[:-1]
    return pd.DataFrame({
        "exchange": "SSE",
        "cal_date": days,
        "is_open": 1,
        "pretrade_date": pre,
    })


def fetch_daily(ts_code: str, start_date: str, end_date: str):
    """返回 (daily_df, basic_df) 或 (None, None)。"""
    fields = "date,code,open,high,low,close,preclose,volume,amount,turn,tradestatus,pctChg,peTTM,pbMRQ,isST"
    rs = bs.query_history_k_data_plus(
        to_bs_code(ts_code), fields,
        start_date=start_date, end_date=end_date,
        frequency="d", adjustflag="3",
    )
    rows = []
    while (rs.error_code == "0") and rs.next():
        rows.append(rs.get_row_data())
    if not rows:
        return None, None
    df = pd.DataFrame(rows, columns=rs.fields)
    for c in ("open", "high", "low", "close", "preclose", "volume", "amount",
              "turn", "pctChg", "peTTM", "pbMRQ"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df[df["tradestatus"] == "1"]
    if df.empty:
        return None, None
    trade_date = df["date"].str.replace("-", "")
    daily = pd.DataFrame({
        "ts_code": ts_code,
        "trade_date": trade_date,
        "open": df["open"],
        "high": df["high"],
        "low": df["low"],
        "close": df["close"],
        "pre_close": df["preclose"],
        "change": df["close"] - df["preclose"],
        "pct_chg": df["pctChg"],
        "vol": df["volume"],
        "amount": df["amount"],
    })
    basic = pd.DataFrame({
        "ts_code": ts_code,
        "trade_date": trade_date,
        "turnover_rate": df["turn"],
        "pe_ttm": df["peTTM"],
        "pb": df["pbMRQ"],
    })
    return daily, basic


def main() -> None:
    pool = os.getenv("BS_POOL", ",".join(DEFAULT_POOL)).split(",")
    pool = [p.strip() for p in pool if p.strip()]
    start = os.getenv("BS_START", "2024-01-01")
    end = os.getenv("BS_END", time.strftime("%Y-%m-%d"))

    print(f"[bootstrap] 股票池 {len(pool)} 只, 区间 {start} ~ {end}")
    lg = bs.login()
    if lg.error_code != "0":
        raise SystemExit(f"Baostock 登录失败: {lg.error_msg}")
    try:
        print("[bootstrap] 1/4 股票基础资料 ...")
        basic_all = fetch_stock_basic()
        save_single_parquet(basic_all, "stock_basic.parquet", data_dir=DATA_DIR)
        print(f"  -> {len(basic_all)} 只")

        print("[bootstrap] 2/4 交易日历（从日线推导，登录后执行）...")
        daily_frames, basic_frames = [], []
        for i, ts_code in enumerate(pool, 1):
            d, b = fetch_daily(ts_code, start, end)
            n = 0 if d is None else len(d)
            print(f"[bootstrap] 3/4 日线 {i}/{len(pool)} {ts_code} -> {n} 行")
            if d is not None:
                daily_frames.append(d)
            if b is not None:
                basic_frames.append(b)

        cal = build_trade_calendar_from_daily(daily_frames)
        save_single_parquet(cal, "stock_trade_calendar.parquet", data_dir=DATA_DIR)
        print(f"  -> 交易日历 {(cal['is_open'] == 1).sum()} 天")

        if daily_frames:
            total = save_partitioned_parquet(pd.concat(daily_frames, ignore_index=True), "trade_date", "daily_history/daily", data_dir=DATA_DIR)
            print(f"  -> daily_history 写入 {total} 行")
        if basic_frames:
            total = save_partitioned_parquet(pd.concat(basic_frames, ignore_index=True), "trade_date", "daily_basic/daily", data_dir=DATA_DIR)
            print(f"  -> daily_basic 写入 {total} 行")
        print("[bootstrap] 完成")
    finally:
        bs.logout()


if __name__ == "__main__":
    main()
