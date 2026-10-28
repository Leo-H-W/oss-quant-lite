"""Baostock 生产者断流处理合约。

2026-10-26 事故：baostock 连接半开时 recv 永久阻塞，看门狗超时异常
被其 socket 层 except Exception 吞掉，错误码非 0 却不抛异常，
死连接被复用导致整轮标的静默空手而归。合约：查询错误码非 0 默认
抛 ConnectionError，由主循环重登录换连接。

2026-10-28 修正：标的维度确定性空结果（10004011 股票代码未标识sh或sz，
北交所 bj.* 不受 baostock 支持）不抛异常——351 只北交所标的每轮必报，
抛异常会触发逐股重登录风暴并把整轮任务误判失败。
"""
from types import SimpleNamespace

import pytest

from app.utils.daily_basic_baostock import fetch_stock
from app.utils.adj_factor_baostock import fetch_adjust_events

pytestmark = pytest.mark.module_data_jobs


class _DeadResult:
    error_code = "10001001"
    error_msg = "网络接收错误。"

    def next(self):  # pragma: no cover - 不应走到
        return False


class _UnsupportedResult:
    error_code = "10004011"
    error_msg = "股票代码未标识sh或sz"

    def next(self):  # pragma: no cover - 不应走到
        return False


class _EmptyOkResult:
    error_code = "0"
    error_msg = "success"
    fields = ["date", "close", "turn", "peTTM", "pbMRQ", "psTTM"]

    def next(self):
        return False


def test_daily_basic_fetch_raises_on_dead_connection():
    fake_bs = SimpleNamespace(
        query_history_k_data_plus=lambda *a, **kw: _DeadResult()
    )
    with pytest.raises(ConnectionError):
        fetch_stock(fake_bs, "600000.SH", "20261023", "20261023")


def test_daily_basic_fetch_returns_empty_on_ok_no_rows():
    """停牌等正常空数据（error_code=0 无行）不抛异常、不误判为断流。"""
    fake_bs = SimpleNamespace(
        query_history_k_data_plus=lambda *a, **kw: _EmptyOkResult()
    )
    df = fetch_stock(fake_bs, "600000.SH", "20261023", "20261023")
    assert df.empty


def test_daily_basic_fetch_returns_empty_on_unsupported_bj_code():
    """北交所标的（10004011）是确定性空结果：按空数据处理，不触发重登录。"""
    fake_bs = SimpleNamespace(
        query_history_k_data_plus=lambda *a, **kw: _UnsupportedResult()
    )
    df = fetch_stock(fake_bs, "920000.BJ", "20261023", "20261023")
    assert df.empty


def test_adj_factor_fetch_raises_on_dead_connection():
    class _DeadAdj(_DeadResult):
        fields = []

    fake_bs = SimpleNamespace(query_adjust_factor=lambda *a, **kw: _DeadAdj())
    with pytest.raises(ConnectionError):
        fetch_adjust_events(fake_bs, "600000.SH")


def test_adj_factor_fetch_returns_empty_on_unsupported_bj_code():
    """北交所标的无除权事件来源：返回空事件表，主循环按因子 1.0 落盘。"""
    class _UnsupportedAdj(_UnsupportedResult):
        fields = []

    fake_bs = SimpleNamespace(query_adjust_factor=lambda *a, **kw: _UnsupportedAdj())
    df = fetch_adjust_events(fake_bs, "920000.BJ")
    assert df.empty
