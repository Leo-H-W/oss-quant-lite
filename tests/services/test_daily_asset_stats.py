"""数据管理页统计口径合约：日线数据资产盘点（分区元数据扫描，不加载数据）。

背景：/api/realtime-analysis/data/stats 旧实现只统计 stock_minute 分钟线
（get_minute_stats），未下载分钟数据时页面"总股票数/总记录数"恒为 0，
与日线数据资产齐全的实况不符。改为 get_daily_stats：只读分区目录 +
parquet footer 元数据（num_rows），内存恒定，结果按 TTL 缓存。
"""
from pathlib import Path

import pandas as pd
import pytest

from app.services.data_reader import ParquetDataReader
from app.services.realtime_data_manager import RealtimeDataManager

pytestmark = pytest.mark.module_data_jobs


def _write_partition(root: Path, table_dir: str, trade_date: str, rows) -> None:
    partition_dir = (
        root / table_dir
        / f"year={trade_date[:4]}" / f"month={trade_date[4:6]}" / f"day={trade_date[6:]}"
    )
    partition_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(partition_dir / "data.parquet", index=False)


@pytest.fixture
def manager_with_tmp_data(tmp_path):
    data_dir = tmp_path / "data"
    _write_partition(
        data_dir,
        "daily_history/daily",
        "20240102",
        [
            {"ts_code": "000001.SZ", "trade_date": "20240102", "close": 10.0},
            {"ts_code": "000002.SZ", "trade_date": "20240102", "close": 20.0},
        ],
    )
    _write_partition(
        data_dir,
        "daily_history/daily",
        "20240103",
        [
            {"ts_code": "000001.SZ", "trade_date": "20240103", "close": 11.0},
            {"ts_code": "600000.SH", "trade_date": "20240103", "close": 30.0},
        ],
    )
    _write_partition(
        data_dir,
        "moneyflow/daily",
        "20240103",
        [{"ts_code": "000001.SZ", "trade_date": "20240103", "buy_lg_vol": 1.0}],
    )

    manager = RealtimeDataManager()
    manager.data_reader = ParquetDataReader(data_dir=str(data_dir))
    return manager


def test_daily_stats_counts_rows_from_partition_metadata(manager_with_tmp_data):
    stats = manager_with_tmp_data.get_daily_stats()

    assert stats["period_stats"]["daily"] == 4       # 2 天 × 2 行
    assert stats["period_stats"]["moneyflow"] == 1
    # 未建分区的表计 0，不得报错
    assert stats["period_stats"]["daily_basic"] == 0
    assert stats["total_records"] == 5


def test_daily_stats_stocks_and_date_range(manager_with_tmp_data):
    stats = manager_with_tmp_data.get_daily_stats()

    # 总股票数 = 最新日线分区的 ts_code 去重
    assert stats["total_stocks"] == 2  # 000001.SZ / 600000.SH
    assert stats["latest_time"].startswith("2024-01-03")
    assert stats["earliest_time"].startswith("2024-01-02")
    # UI 需要表中文名渲染卡片
    assert stats["period_names"]["daily"] == "日线行情"
    assert stats["period_names"]["moneyflow"] == "资金流向"


def test_daily_stats_never_loads_full_frames(manager_with_tmp_data, monkeypatch):
    """内存合约：禁止 pandas 全表读，只能读 footer 元数据/单列投影。"""
    import app.services.realtime_data_manager as manager_module

    def forbidden(*args, **kwargs):
        raise AssertionError("日线资产统计不得用 pandas 全量读 parquet")

    monkeypatch.setattr(manager_module.pd, "read_parquet", forbidden)
    stats = manager_with_tmp_data.get_daily_stats()
    assert stats["total_records"] == 5


def test_daily_stats_caches_scan_within_ttl(manager_with_tmp_data, monkeypatch):
    """TTL 内第二次调用不得重复扫分区元数据。"""
    import pyarrow.parquet as pq

    calls = []
    original = pq.read_metadata

    def spy(path, *args, **kwargs):
        calls.append(str(path))
        return original(path, *args, **kwargs)

    monkeypatch.setattr(pq, "read_metadata", spy)
    manager_with_tmp_data.get_daily_stats()
    first = len(calls)
    assert first > 0

    manager_with_tmp_data.get_daily_stats()
    assert len(calls) == first, "TTL 内不得重复扫描"
