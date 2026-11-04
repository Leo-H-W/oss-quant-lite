import numpy as np
import pandas as pd
import pytest

from app.services import stock_service
from scripts import build_text2sql_wide_tables as wide_builder
from scripts.build_text2sql_wide_tables import (
    BUSINESS_COLS,
    HIST_COLS,
    add_technical_factors,
)

pytestmark = pytest.mark.module_data_jobs


def _daily_frame(closes):
    close = np.asarray(closes, dtype=float)
    pre_close = np.concatenate([[close[0]], close[:-1]])
    return pd.DataFrame({
        "ts_code": "300454.SZ",
        "trade_date": pd.date_range("2024-01-01", periods=len(close)).strftime("%Y-%m-%d"),
        "open": close * 0.995,
        "high": close * 1.01,
        "low": close * 0.99,
        "close": close,
        "pre_close": pre_close,
        "pct_chg": (close / pre_close - 1) * 100,
        "vol": 1000.0,
        "amount": close * 1000.0,
    })


def _expected_trend_up_start(frame):
    """按需求独立计算预期，包含复权链和 30 根 K 线门槛。"""
    factor = (frame["close"].shift(1) / frame["pre_close"]).fillna(1.0).cumprod()
    adjusted = {name: frame[name] * factor for name in ("open", "high", "low", "close")}
    source = (
        3 * adjusted["close"] + adjusted["high"] + adjusted["low"] + adjusted["open"]
    ) / 6
    mm = source.ewm(span=21, adjust=False).mean()
    up = mm >= mm.rolling(5, min_periods=1).max()
    enough_bars = np.arange(1, len(frame) + 1) >= 30
    return (up & ~up.shift(1, fill_value=False) & enough_bars).astype(float)


def test_trend_up_start_matches_formula_and_30_bar_gate():
    closes = [100, 99, 98, 97, 96, 95, 94, 93, 92, 91] * 3
    closes += [90, 91, 92, 91, 93, 94, 95, 94, 96, 97, 96, 98, 99, 100]
    frame = _daily_frame(closes)
    expected = _expected_trend_up_start(frame)
    assert expected.iloc[:29].sum() == 0
    assert expected.iloc[29:].sum() > 0

    result = add_technical_factors(frame)

    pd.testing.assert_series_equal(
        result["trend_up_start"].astype(float).reset_index(drop=True),
        expected.reset_index(drop=True),
        check_names=False,
    )
    assert set(result["trend_up_start"].unique()) <= {0, 1}
    assert result["trend_up_start"].notna().all()


def test_trend_up_start_is_always_zero_for_fewer_than_30_bars():
    frame = _daily_frame([100, 99, 98, 97, 96, 95, 94] * 4 + [93])

    result = add_technical_factors(frame)

    assert len(result) == 29
    assert (result["trend_up_start"] == 0).all()


def test_trend_up_start_is_in_wide_table_contract():
    assert {"open", "high", "low", "close", "pre_close"} <= set(HIST_COLS)
    assert "trend_up_start" in BUSINESS_COLS


def test_read_partitioned_fills_columns_missing_from_older_partitions(tmp_path, monkeypatch):
    partition_dir = tmp_path / "daily_basic" / "daily"
    partition_dir.mkdir(parents=True)
    pd.DataFrame({
        "ts_code": ["300454.SZ"],
        "trade_date": ["2026-11-02"],
        "pe_ttm": [100.0],
    }).to_parquet(partition_dir / "data.parquet", index=False)
    monkeypatch.setattr(wide_builder, "DATA", str(tmp_path))
    categories = {"ts_code": ["300454.SZ"], "trade_date": ["2026-11-02"]}

    result = wide_builder.read_partitioned(
        "daily_basic/daily", ["ts_code", "trade_date", "pe", "pe_ttm"], categories,
    )

    assert result.columns.tolist() == ["ts_code", "trade_date", "pe", "pe_ttm"]
    assert result["pe"].isna().all()
    assert result["pe_ttm"].iloc[0] == 100.0


def test_stock_screen_dynamic_condition_filters_trend_up_start(monkeypatch):
    business = pd.DataFrame({
        "ts_code": ["300454.SZ", "000001.SZ"],
        "trade_date": ["2026-11-02", "2026-11-02"],
        "trend_up_start": [1.0, 0.0],
    })
    basic = pd.DataFrame({
        "ts_code": ["300454.SZ", "000001.SZ"],
        "industry": ["软件服务", "银行"],
        "area": ["深圳", "深圳"],
        "symbol": ["300454", "000001"],
        "name": ["深信服", "平安银行"],
        "list_date": ["2018-05-16", "1991-04-03"],
    })
    monkeypatch.setattr(stock_service._data_reader, "get_stock_business", lambda **kwargs: business)
    monkeypatch.setattr(stock_service._data_reader, "get_stock_basic", lambda: basic)

    result = stock_service.StockService.screen_stocks({
        "trade_date": "2026-11-02",
        "dynamic_conditions": [{
            "field_a": "trend_up_start", "operator": "=", "field_b": None, "value": "1",
        }],
    })

    assert result["total"] == 1
    assert [row["ts_code"] for row in result["stocks"]] == ["300454.SZ"]
