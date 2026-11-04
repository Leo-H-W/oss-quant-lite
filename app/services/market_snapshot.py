"""从真实 Parquet 分区组装最新市场快照。

旧版试用功能依赖从未由当前数据链生成的 ``data/data.parquet``。这里统一按
最新交易日合并日线、每日指标、资金流和股票基础资料，供热力图与资金流统计
使用；不再要求额外的单文件宽表。
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import pandas as pd

from app.services.data_reader import ParquetDataReader


def _project_data_dir() -> str:
    return os.getenv(
        "DATA_DIR",
        str(Path(__file__).resolve().parents[2] / "data"),
    )


def _latest_trade_date(frame: pd.DataFrame) -> str:
    dates = pd.to_datetime(frame["trade_date"], errors="coerce", format="mixed").dropna()
    if dates.empty:
        raise ValueError("最新日线分区缺少有效 trade_date")
    return dates.max().strftime("%Y-%m-%d")


def _read_day(reader: ParquetDataReader, table: str, trade_date: str) -> pd.DataFrame:
    frame = reader._read_table(table, None, trade_date, trade_date)
    if frame is None or frame.empty:
        return pd.DataFrame()
    return frame.drop_duplicates(subset=["ts_code"], keep="last")


def _latest_available_market_caps(data_dir: str) -> pd.DataFrame:
    """Baostock 每日指标不含市值时，取最近一个真实 Tushare 分区的市值。"""
    import pyarrow.parquet as pq

    root = Path(data_dir) / "daily_basic" / "daily"
    for path in sorted(root.glob("year=*/month=*/day=*/data.parquet"), reverse=True):
        try:
            if "total_mv" not in pq.read_schema(path).names:
                continue
            frame = pd.read_parquet(path, columns=["ts_code", "total_mv"])
        except Exception:  # noqa: BLE001 - 单个坏分区不阻断更早的真实市值回退
            continue
        frame = frame.dropna(subset=["ts_code", "total_mv"])
        if not frame.empty:
            return frame.drop_duplicates(subset=["ts_code"], keep="last")
    return pd.DataFrame(columns=["ts_code", "total_mv"])


def load_latest_market_snapshot(
    data_dir: Optional[str] = None,
    *,
    include_market_cap: bool = False,
) -> pd.DataFrame:
    """返回最新交易日全市场快照，trade_date 统一为 YYYYMMDD。

    ``include_market_cap=True`` 时，如果最新每日指标没有 total_mv，则使用最近
    可用历史分区的真实市值。该值仅用于热力图板块规模与权重，不伪造成当日值。
    """
    root = data_dir or _project_data_dir()
    reader = ParquetDataReader(data_dir=root)
    latest_daily = reader._read_latest_partition("daily")
    if latest_daily is None or latest_daily.empty:
        raise FileNotFoundError(f"最新日线分区不存在或为空: {root}/daily_history/daily")
    trade_date = _latest_trade_date(latest_daily)

    snapshot = _read_day(reader, "daily", trade_date)
    if snapshot.empty:
        raise ValueError(f"交易日 {trade_date} 没有可用日线数据")

    basic = _read_day(reader, "daily_basic", trade_date)
    if not basic.empty:
        columns = ["ts_code"] + [
            col for col in (
                "turnover_rate", "turnover_rate_f", "volume_ratio", "pe", "pe_ttm",
                "pb", "ps", "ps_ttm", "dv_ratio", "dv_ttm", "total_share",
                "float_share", "free_share", "total_mv", "circ_mv",
            ) if col in basic.columns
        ]
        snapshot = snapshot.merge(basic[columns], on="ts_code", how="left")

    moneyflow = _read_day(reader, "moneyflow", trade_date)
    if not moneyflow.empty:
        columns = ["ts_code"] + [col for col in moneyflow.columns if col not in {"ts_code", "trade_date"}]
        snapshot = snapshot.merge(moneyflow[columns], on="ts_code", how="left")

    stock_basic = reader.get_stock_basic()
    if stock_basic is not None and not stock_basic.empty:
        columns = ["ts_code"] + [
            col for col in ("name", "industry", "area", "list_status")
            if col in stock_basic.columns
        ]
        basic = stock_basic[columns].drop_duplicates(subset=["ts_code"], keep="last")
        snapshot = snapshot.merge(basic, on="ts_code", how="left")

    # 即使某个可选源当日缺席，也保持下游合约字段存在（值保持缺失，不填零）。
    for column in (
        "name", "industry", "turnover_rate", "total_mv", "net_mf_amount",
        "buy_lg_amount", "sell_lg_amount", "buy_elg_amount", "sell_elg_amount",
    ):
        if column not in snapshot.columns:
            snapshot[column] = pd.NA

    if include_market_cap and snapshot["total_mv"].isna().all():
        caps = _latest_available_market_caps(root)
        if not caps.empty:
            snapshot = snapshot.drop(columns=["total_mv"]).merge(caps, on="ts_code", how="left")
        if "total_mv" not in snapshot.columns:
            snapshot["total_mv"] = pd.NA

    snapshot["trade_date"] = trade_date.replace("-", "")
    return snapshot
