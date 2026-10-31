"""隐藏/独立回补入口的断点合约。"""
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

_UTILS = str(__import__("pathlib").Path(__file__).resolve().parents[2] / "app" / "utils")
if _UTILS not in sys.path:
    sys.path.insert(0, _UTILS)

from app.utils import daily_history_by_code, moneyflow_tdx_backfill, stock_basic, stock_company

pytestmark = pytest.mark.module_data_jobs


def test_daily_history_by_code_resumes_completed_dates(monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(daily_history_by_code, "get_stock_codes", lambda: ["A"])
    monkeypatch.setattr(
        daily_history_by_code, "resolve_trade_dates",
        lambda: (["20261028", "20261029", "20261030"], False),
    )
    monkeypatch.setattr(daily_history_by_code, "save_partitioned_parquet", lambda *_a: 1)
    calls = []
    fail = {"value": True}

    def daily(*, ts_code, trade_date):
        calls.append(trade_date)
        if trade_date == "20261029" and fail["value"]:
            raise ConnectionError("temporary")
        return pd.DataFrame({"ts_code": [ts_code], "trade_date": [trade_date]})

    monkeypatch.setattr(
        daily_history_by_code, "DatabaseUtils",
        SimpleNamespace(init_tushare_api=lambda: SimpleNamespace(daily=daily)),
    )
    with pytest.raises(ConnectionError):
        daily_history_by_code.main()
    assert calls == ["20261028", "20261029"]

    calls.clear()
    fail["value"] = False
    daily_history_by_code.main()
    assert calls == ["20261029", "20261030"]


def test_moneyflow_backfill_resumes_partial_date_and_returns_failure(monkeypatch, tmp_path):
    project = tmp_path / "project"
    data = project / "data"
    data.mkdir(parents=True)
    pd.DataFrame({"cal_date": ["20261030"], "is_open": [1]}).to_parquet(data / "stock_trade_calendar.parquet")
    pd.DataFrame({"ts_code": ["A", "B", "C"]}).to_parquet(data / "stock_basic.parquet")
    monkeypatch.setattr(moneyflow_tdx_backfill, "_PROJECT_ROOT", project)
    monkeypatch.setenv("MONEYFLOW_BACKFILL_START", "20261030")
    monkeypatch.setenv("MONEYFLOW_BACKFILL_END", "20261030")
    monkeypatch.setattr(moneyflow_tdx_backfill, "DATE_INTERVAL", 0)
    monkeypatch.setattr(moneyflow_tdx_backfill, "merge_save", lambda rows, _date: len(rows))

    import app.services.tongdaxin.client as client
    monkeypatch.setattr(client, "create_hq_api", lambda: SimpleNamespace(disconnect=lambda: None))
    monkeypatch.setattr(client, "connect_first_available", lambda _api, **_kw: None)

    calls = []
    fail = {"value": True}

    def aggregate(_api, ts_code, _date):
        calls.append(ts_code)
        if ts_code == "B" and fail["value"]:
            raise ConnectionError("temporary")
        return {"ts_code": ts_code}

    monkeypatch.setattr(moneyflow_tdx_backfill, "aggregate_stock", aggregate)
    assert moneyflow_tdx_backfill.main() == 1
    assert calls == ["A", "B", "C"]

    calls.clear()
    fail["value"] = False
    assert moneyflow_tdx_backfill.main() == 0
    assert calls == ["B"]


def test_stock_basic_partial_snapshot_failure_is_retryable(monkeypatch):
    def fake_stock_basic(*, exchange, list_status, fields):
        if list_status == "D":
            raise ConnectionError("temporary")
        return pd.DataFrame({"ts_code": [list_status]})

    monkeypatch.setattr(stock_basic, "DatabaseUtils", SimpleNamespace(
        init_tushare_api=lambda: SimpleNamespace(stock_basic=fake_stock_basic)
    ))
    with pytest.raises(ConnectionError):
        stock_basic.main()


def test_stock_company_partial_snapshot_failure_is_retryable(monkeypatch):
    def fake_stock_company(*, exchange, fields):
        if exchange == "SZSE":
            raise ConnectionError("temporary")
        return pd.DataFrame({"ts_code": [exchange]})

    monkeypatch.setattr(stock_company, "DatabaseUtils", SimpleNamespace(
        init_tushare_api=lambda: SimpleNamespace(stock_company=fake_stock_company)
    ))
    with pytest.raises(ConnectionError):
        stock_company.main()
