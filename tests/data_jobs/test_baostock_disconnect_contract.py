"""Baostock 生产者断流处理合约。

2026-10-26 事故：baostock 连接半开时 recv 永久阻塞，看门狗超时异常
被其 socket 层 except Exception 吞掉，错误码非 0 却不抛异常，
死连接被复用导致整轮标的静默空手而归。合约：查询错误码非 0 必须
抛 ConnectionError，由主循环重登录换连接。
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


def test_adj_factor_fetch_raises_on_dead_connection():
    class _DeadAdj(_DeadResult):
        fields = []

    fake_bs = SimpleNamespace(query_adjust_factor=lambda *a, **kw: _DeadAdj())
    with pytest.raises(ConnectionError):
        fetch_adjust_events(fake_bs, "600000.SH")
