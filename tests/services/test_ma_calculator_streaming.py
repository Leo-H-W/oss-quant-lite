"""ma_calculator 流式计算合约：单遍扫描分区、恒定内存、结果口径不变。

背景：旧实现逐股调用 get_daily 且不传日期范围，每次都把 11 年全市场
daily_history 读进内存再过滤（峰值 1–2.5GB），在 1.8GB 机器上必然被
earlyoom 杀掉。修复后必须单遍流式扫描分区、每只股票只保留最近
HISTORY_LEN 个收盘价，且 MA/EMA 计算结果与旧口径一致。
"""
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from app.services.data_reader import ParquetDataReader
from app.utils import ma_calculator

pytestmark = pytest.mark.module_data_jobs


def _write_daily_partition(root: Path, trade_date: str, rows) -> None:
    partition_dir = (
        root / "daily_history" / "daily"
        / f"year={trade_date[:4]}" / f"month={trade_date[4:6]}" / f"day={trade_date[6:]}"
    )
    partition_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(partition_dir / "data.parquet", index=False)


def _build_daily_history(root: Path, codes, num_days: int) -> None:
    """为每只股票生成 num_days 个连续"交易日"分区，收盘价逐日 +1。"""
    dates = pd.bdate_range("2024-01-01", periods=num_days).strftime("%Y%m%d")
    for i, day in enumerate(dates):
        _write_daily_partition(
            root,
            day,
            [
                {"ts_code": code, "trade_date": day, "close": float(i + 1), "vol": 100.0}
                for code in codes
            ],
        )


def test_stream_close_tails_keeps_only_recent_history(tmp_path):
    """每只股票只保留最近 history_len 个收盘价，且顺序为时间升序。"""
    data_dir = tmp_path / "data"
    _build_daily_history(data_dir, ["000001.SZ"], num_days=25)

    reader = ParquetDataReader(data_dir=str(data_dir))
    tails = ma_calculator.stream_close_tails(reader, ["000001.SZ"], history_len=10)

    assert set(tails.keys()) == {"000001.SZ"}
    assert list(tails["000001.SZ"]) == [float(v) for v in range(16, 26)]  # 最近 10 个


def test_stream_close_tails_reads_only_required_columns(tmp_path, monkeypatch):
    """分区读取必须列裁剪：只读 ts_code/close，禁止整行读入。"""
    data_dir = tmp_path / "data"
    _build_daily_history(data_dir, ["000001.SZ"], num_days=6)

    requested = []
    original = pd.read_parquet

    def spy(path, *args, **kwargs):
        requested.append(kwargs.get("columns"))
        return original(path, *args, **kwargs)

    monkeypatch.setattr(ma_calculator.pd, "read_parquet", spy)

    reader = ParquetDataReader(data_dir=str(data_dir))
    ma_calculator.stream_close_tails(reader, ["000001.SZ"], history_len=10)

    assert requested, "未发生任何分区读取"
    assert all(cols == ["ts_code", "close"] for cols in requested)


def test_stream_close_tails_never_uses_full_table_reader(tmp_path, monkeypatch):
    """流式路径不得回调 get_daily 等全表接口（旧实现的内存炸弹）。"""
    data_dir = tmp_path / "data"
    _build_daily_history(data_dir, ["000001.SZ"], num_days=6)

    def forbidden(*args, **kwargs):
        raise AssertionError("不得调用全表读取接口")

    monkeypatch.setattr(ParquetDataReader, "get_daily", forbidden)
    monkeypatch.setattr(ParquetDataReader, "_read_table", forbidden)

    reader = ParquetDataReader(data_dir=str(data_dir))
    tails = ma_calculator.stream_close_tails(reader, ["000001.SZ"], history_len=10)

    assert list(tails["000001.SZ"]) == [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]


def test_stream_close_tails_skips_unlisted_stocks(tmp_path):
    """只统计指定股票清单，parquet 里的其他股票不得进入结果。"""
    data_dir = tmp_path / "data"
    _build_daily_history(data_dir, ["000001.SZ", "999999.SZ"], num_days=6)

    reader = ParquetDataReader(data_dir=str(data_dir))
    tails = ma_calculator.stream_close_tails(reader, ["000001.SZ"], history_len=10)

    assert set(tails.keys()) == {"000001.SZ"}


def test_build_ma_rows_matches_legacy_semantics(tmp_path):
    """MA/EMA 数值与旧口径一致；不足 5 行的股票跳过。"""
    closes = [float(v) for v in range(1, 131)]  # 130 天，覆盖 ma120/ema120
    tails = {
        "000001.SZ": closes,
        "000002.SZ": closes[:4],  # 不足 5 行
    }

    rows = ma_calculator.build_ma_rows(tails.items())

    assert [r["ts_code"] for r in rows] == ["000001.SZ"]
    row = rows[0]
    series = pd.Series(closes)
    assert row["ma5"] == pytest.approx(series.rolling(5).mean().iloc[-1])
    assert row["ma120"] == pytest.approx(series.rolling(120).mean().iloc[-1])
    assert row["ema5"] == pytest.approx(series.ewm(span=5, adjust=False).mean().iloc[-1])
    assert row["ema120"] == pytest.approx(
        ma_calculator.calculate_ema_manually(series.values, 120)
    )


def test_main_writes_single_parquet(tmp_path, monkeypatch):
    """端到端：扫描分区 → 计算 → 调用 save_single_parquet 落盘。"""
    data_dir = tmp_path / "data"
    _build_daily_history(data_dir, ["000001.SZ"], num_days=30)

    monkeypatch.setattr(ma_calculator, "get_stock_codes", lambda: ["000001.SZ"])
    monkeypatch.setattr(
        ma_calculator, "ParquetDataReader", lambda: ParquetDataReader(data_dir=str(data_dir))
    )

    saved = {}
    monkeypatch.setattr(
        ma_calculator,
        "save_single_parquet",
        lambda df, name: saved.update({"df": df, "name": name}),
    )

    ma_calculator.main()

    assert saved["name"] == "stock_ma_data.parquet"
    assert saved["df"]["ts_code"].tolist() == ["000001.SZ"]
    assert saved["df"].iloc[0]["ma5"] == pytest.approx(np.mean([26.0, 27.0, 28.0, 29.0, 30.0]))
