"""真实 Parquet 分区市场快照及试用功能数据源合约。"""
from pathlib import Path

import pandas as pd
import pytest

from app.services.market_snapshot import load_latest_market_snapshot
from app.services import trial_analytics


def _partition(root: Path, table: str, trade_date: str, frame: pd.DataFrame) -> None:
    part = (
        root / table / f"year={trade_date[:4]}" / f"month={trade_date[4:6]}"
        / f"day={trade_date[6:]}"
    )
    part.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(part / "data.parquet", index=False)


def _real_partition_fixture(root: Path) -> None:
    _partition(root, "daily_history/daily", "20261102", pd.DataFrame({
        "ts_code": ["000001.SZ", "000002.SZ"],
        "trade_date": ["2026-11-02"] * 2,
        "close": [11.0, 8.0],
        "pct_chg": [1.5, -2.0],
    }))
    _partition(root, "daily_basic/daily", "20261102", pd.DataFrame({
        "ts_code": ["000001.SZ", "000002.SZ"],
        "trade_date": ["2026-11-02"] * 2,
        "close": [11.0, 8.0],
        "turnover_rate": [0.5, 1.7],
    }))
    # Baostock 最新分区没有市值，允许使用最近一个真实 Tushare 分区补充。
    _partition(root, "daily_basic/daily", "20261009", pd.DataFrame({
        "ts_code": ["000001.SZ", "000002.SZ"],
        "trade_date": ["2026-10-09"] * 2,
        "close": [10.8, 8.2],
        "total_mv": [21300.0, 3900.0],
    }))
    _partition(root, "moneyflow/daily", "20261102", pd.DataFrame({
        "ts_code": ["000001.SZ", "000002.SZ"],
        "trade_date": ["2026-11-02"] * 2,
        "net_mf_amount": [14000.0, -5000.0],
        "buy_lg_amount": [20000.0, 1000.0],
        "sell_lg_amount": [10000.0, 3000.0],
        "buy_elg_amount": [30000.0, 2000.0],
        "sell_elg_amount": [20000.0, 5000.0],
    }))
    pd.DataFrame({
        "ts_code": ["000001.SZ", "000002.SZ"],
        "name": ["平安银行", "万科Ａ"],
        "industry": ["银行", "全国地产"],
    }).to_parquet(root / "stock_basic.parquet", index=False)


def test_partition_snapshot_supports_heatmap_without_legacy_data_parquet(tmp_path):
    _real_partition_fixture(tmp_path)

    frame = load_latest_market_snapshot(str(tmp_path), include_market_cap=True)

    assert set(frame["trade_date"]) == {"20261102"}
    first = frame.set_index("ts_code").loc["000001.SZ"]
    assert first["name"] == "平安银行"
    assert first["industry"] == "银行"
    assert first["turnover_rate"] == 0.5
    assert first["total_mv"] == 21300.0
    assert first["net_mf_amount"] == 14000.0


def test_moneyflow_payload_uses_partition_snapshot_when_legacy_file_missing(
    monkeypatch, tmp_path,
):
    monkeypatch.setattr(
        trial_analytics, "_wide_table_path", lambda: tmp_path / "data.parquet",
    )
    frame = pd.DataFrame({
        "ts_code": ["000001.SZ"],
        "name": ["平安银行"],
        "industry": ["银行"],
        "trade_date": ["20261102"],
        "net_mf_amount": [14000.0],
        "buy_lg_amount": [20000.0],
        "sell_lg_amount": [10000.0],
        "buy_elg_amount": [30000.0],
        "sell_elg_amount": [20000.0],
    })
    monkeypatch.setattr(
        trial_analytics, "load_latest_market_snapshot", lambda: frame.copy(),
    )

    payload = trial_analytics.moneyflow_payload()

    assert payload["summary"]["trade_date"] == "20261102"
    assert payload["summary"]["stock_count"] == 1
    assert payload["summary"]["total_net_mf_amount"] == 14000.0
    assert payload["top_inflow"][0]["ts_code"] == "000001.SZ"
