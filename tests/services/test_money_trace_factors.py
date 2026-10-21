# -*- coding: utf-8 -*-
"""资金痕迹因子（跟踪主力资金动向）合约测试。

因子口径来源：
- CMF(20)：Chaikin Money Flow，蔡金资金流向
- MFI(14)：Money Flow Index，资金流量指标
- main_net_inflow_rate_5d/20d：主力（大单+特大单）净流入率的滚动均值，
  口径对齐华泰证券《单因子测试之资金流向因子》(2018)
- smart_money_q：开源证券"聪明钱"因子，分钟数据 S=|ret|/sqrt(vol)
  排序取前 20% 为聪明钱，Q = VWAP_smart / VWAP_all
"""
import math

import pandas as pd
import pytest

from app.services.factor_engine import FactorEngine

pytestmark = pytest.mark.module_factor_engine

MONEY_TRACE_FACTOR_IDS = [
    "cmf_20d",
    "mfi_14d",
    "main_net_inflow_rate_5d",
    "main_net_inflow_rate_20d",
    "smart_money_q",
]


def _ohlc_frame(closes, highs, lows, vols, ts_code="000001.SZ"):
    n = len(closes)
    return pd.DataFrame(
        {
            "ts_code": [ts_code] * n,
            "trade_date": pd.date_range("2024-01-01", periods=n, freq="D"),
            "open": closes,
            "high": highs,
            "low": lows,
            "close": closes,
            "vol": vols,
        }
    )


def _moneyflow_frame(daily_rates, ts_code="000001.SZ"):
    """按目标净流入率反推 buy/sell 大单字段，total buy 固定 1000。"""
    n = len(daily_rates)
    rows = {
        "ts_code": [ts_code] * n,
        "trade_date": pd.date_range("2024-01-01", periods=n, freq="D"),
        "buy_sm_amount": [100.0] * n,
        "buy_md_amount": [100.0] * n,
        "buy_lg_amount": [400.0] * n,
        "buy_elg_amount": [400.0] * n,
    }
    df = pd.DataFrame(rows)
    # rate = (big_buy - big_sell) / total_buy → big_sell = big_buy - rate*total
    df["sell_lg_amount"] = [400.0 - r * 500.0 for r in daily_rates]
    df["sell_elg_amount"] = [400.0 - r * 500.0 for r in daily_rates]
    df["sell_sm_amount"] = [100.0] * n
    df["sell_md_amount"] = [100.0] * n
    df["net_mf_amount"] = [r * 1000.0 for r in daily_rates]
    return df


# ==================== 注册契约 ====================


def test_money_trace_factors_registered_in_builtin_registry():
    engine = FactorEngine()
    for factor_id in MONEY_TRACE_FACTOR_IDS:
        assert factor_id in engine.builtin_factors, f"{factor_id} 未注册为内置因子"
        assert factor_id in engine.BUILTIN_FACTOR_SOURCES, f"{factor_id} 未声明数据源"


def test_money_trace_factors_classified_as_money_flow_type():
    engine = FactorEngine()
    listed = {item["factor_id"]: item for item in engine.get_factor_list()}
    for factor_id in MONEY_TRACE_FACTOR_IDS:
        assert listed[factor_id]["factor_type"] == "money_flow", (
            f"{factor_id} 应归类为 money_flow，实际 {listed[factor_id]['factor_type']}"
        )


def test_validation_samples_cover_money_trace_factors():
    engine = FactorEngine()
    sample_ids = {s["factor_id"] for s in engine.get_builtin_factor_validation_samples()}
    assert {"cmf_20d", "mfi_14d", "main_net_inflow_rate_5d", "smart_money_q"} <= sample_ids


# ==================== CMF(20) ====================


def test_cmf_20d_all_close_at_high_equals_one():
    """20 天全部收在最高价：资金全流入，CMF=1。"""
    engine = FactorEngine()
    n = 20
    data = {
        "ohlc_prices": _ohlc_frame(
            closes=[11.0] * n, highs=[11.0] * n, lows=[9.0] * n, vols=[100.0] * n
        )
    }
    result = engine._cmf_factor(data, "cmf_20d")
    assert len(result) == 1
    assert math.isclose(result.iloc[-1]["factor_value"], 1.0, rel_tol=1e-9)


def test_cmf_20d_balanced_flow_equals_zero():
    """10 天收最高 + 10 天收最低、等量：CMF=0。"""
    engine = FactorEngine()
    closes = [11.0] * 10 + [9.0] * 10
    data = {
        "ohlc_prices": _ohlc_frame(
            closes=closes, highs=[11.0] * 20, lows=[9.0] * 20, vols=[100.0] * 20
        )
    }
    result = engine._cmf_factor(data, "cmf_20d")
    assert math.isclose(result.iloc[-1]["factor_value"], 0.0, abs_tol=1e-12)


def test_cmf_20d_limit_day_high_eq_low_not_nan():
    """一字板（high==low）不得产生 NaN/inf，该日按中性处理。"""
    engine = FactorEngine()
    n = 20
    data = {
        "ohlc_prices": _ohlc_frame(
            closes=[10.0] * n, highs=[10.0] * n, lows=[10.0] * n, vols=[100.0] * n
        )
    }
    result = engine._cmf_factor(data, "cmf_20d")
    assert len(result) == 1
    assert math.isclose(result.iloc[-1]["factor_value"], 0.0, abs_tol=1e-12)


# ==================== MFI(14) ====================


def test_mfi_14d_all_rising_equals_100():
    """典型价连续上涨：正资金流占满，MFI=100。"""
    engine = FactorEngine()
    n = 15
    closes = [10.0 + i for i in range(n)]
    data = {
        "ohlc_prices": _ohlc_frame(
            closes=closes,
            highs=[c + 1 for c in closes],
            lows=[c - 1 for c in closes],
            vols=[100.0] * n,
        )
    }
    result = engine._mfi_factor(data, "mfi_14d")
    assert not result.empty
    assert math.isclose(result.iloc[-1]["factor_value"], 100.0, rel_tol=1e-9)


def test_mfi_14d_all_falling_equals_zero():
    """典型价连续下跌：无正资金流，MFI=0。"""
    engine = FactorEngine()
    n = 15
    closes = [100.0 - i for i in range(n)]
    data = {
        "ohlc_prices": _ohlc_frame(
            closes=closes,
            highs=[c + 1 for c in closes],
            lows=[c - 1 for c in closes],
            vols=[100.0] * n,
        )
    }
    result = engine._mfi_factor(data, "mfi_14d")
    assert not result.empty
    assert math.isclose(result.iloc[-1]["factor_value"], 0.0, abs_tol=1e-9)


def test_mfi_14d_matches_reference_formula():
    """与参照实现逐位核对：MFI = 100 - 100/(1 + PMF/NMF)，14 日窗口。"""
    engine = FactorEngine()
    n = 20
    closes = [10, 11, 10.5, 12, 11, 13, 12.5, 14, 13, 15,
              14, 16, 15, 17, 16, 18, 17, 19, 18, 20]
    highs = [c + 0.5 for c in closes]
    lows = [c - 0.5 for c in closes]
    vols = [100 + 10 * i for i in range(n)]
    data = {"ohlc_prices": _ohlc_frame(closes, highs, lows, vols)}

    result = engine._mfi_factor(data, "mfi_14d")

    tp = (pd.Series(highs) + pd.Series(lows) + pd.Series(closes)) / 3
    rmf = tp * pd.Series(vols)
    direction = tp.diff()
    pos = rmf.where(direction > 0, 0.0)
    neg = rmf.where(direction < 0, 0.0)
    expected = 100 - 100 / (1 + pos.rolling(14).sum().iloc[-1] / neg.rolling(14).sum().iloc[-1])
    assert math.isclose(result.iloc[-1]["factor_value"], expected, rel_tol=1e-9)


# ==================== 主力净流入率滚动 ====================


def test_main_net_inflow_rate_5d_is_rolling_mean_of_daily_rate():
    engine = FactorEngine()
    rates = [0.10, 0.20, 0.30, 0.40, 0.50, 0.60]
    data = {"moneyflow": _moneyflow_frame(rates)}

    result = engine._main_net_inflow_rate_factor(data, "main_net_inflow_rate_5d")

    assert len(result) == 2  # 前 4 日窗口不足
    assert math.isclose(result.iloc[-1]["factor_value"], sum(rates[1:]) / 5, rel_tol=1e-9)
    assert math.isclose(result.iloc[0]["factor_value"], sum(rates[:5]) / 5, rel_tol=1e-9)


def test_main_net_inflow_rate_20d_requires_full_window():
    engine = FactorEngine()
    rates = [0.05] * 19
    data = {"moneyflow": _moneyflow_frame(rates)}
    result = engine._main_net_inflow_rate_factor(data, "main_net_inflow_rate_20d")
    assert result.empty, "样本不足 20 日时不应输出因子值"


# ==================== 聪明钱 Q ====================


def _fake_minute_bars():
    """构造一天的 1 分钟 bar：10 根，前 2 根为'聪明钱'（大波动小量）。

    S = |ret| / sqrt(vol)：bar1、bar2 的 S 远高于其余，
    top 20%（2 根）即它们，Q = VWAP_smart / VWAP_all 可手算。
    """
    rows = []
    # (minute_idx, close, vol)
    bars = [
        (0, 10.0, 1000.0),   # 基准 bar（ret 缺失）
        (1, 10.5, 100.0),    # ret=+5%, S=0.05/10=5e-3  聪明
        (2, 10.0, 100.0),    # ret≈-4.76%, S≈4.76e-3    聪明
    ]
    # 其余 7 根：微小波动 + 大量，S 很小
    price = 10.0
    for i in range(3, 10):
        price = price + 0.001
        bars.append((i, round(price, 4), 10000.0))
    for idx, close, vol in bars:
        rows.append(
            {
                "ts_code": "000001.SZ",
                "datetime": pd.Timestamp("2024-01-02 09:31") + pd.Timedelta(minutes=idx),
                "period_type": "1min",
                "close": close,
                "vol": vol,
                "amount": close * vol,
            }
        )
    return pd.DataFrame(rows)


def test_smart_money_q_matches_formula(monkeypatch):
    engine = FactorEngine()
    minute_df = _fake_minute_bars()
    monkeypatch.setattr(
        engine, "_load_minute_bars", lambda ts_codes, s, e: minute_df
    )

    data = {"minute_1min": minute_df}
    result = engine._smart_money_q_factor(data, "smart_money_q")

    assert len(result) == 1
    # 参照计算
    df = minute_df.sort_values("datetime").reset_index(drop=True)
    ret = df["close"].pct_change()
    s = ret.abs() / (df["vol"] ** 0.5)
    n_smart = max(1, int(len(df) * 0.2))
    smart_idx = s.sort_values(ascending=False).index[:n_smart]
    vwap_smart = df.loc[smart_idx, "amount"].sum() / df.loc[smart_idx, "vol"].sum()
    vwap_all = df["amount"].sum() / df["vol"].sum()
    expected = vwap_smart / vwap_all
    assert math.isclose(result.iloc[-1]["factor_value"], expected, rel_tol=1e-9)
    assert result.iloc[-1]["trade_date"] == pd.Timestamp("2024-01-02")


def test_smart_money_q_returns_empty_without_minute_data():
    """本地无分钟数据时优雅降级为空，不抛异常。"""
    engine = FactorEngine()
    result = engine._smart_money_q_factor({"minute_1min": pd.DataFrame()}, "smart_money_q")
    assert result.empty
