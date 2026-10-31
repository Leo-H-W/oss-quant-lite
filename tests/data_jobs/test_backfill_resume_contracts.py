"""其余历史回补生产者的持久化断点合约。"""
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

from app.utils import adj_factor_baostock as adj

# financial_vip 兼容直接脚本执行，依赖 app/utils 在 import path 中。
_UTILS_DIR = str(__import__("pathlib").Path(__file__).resolve().parents[2] / "app" / "utils")
if _UTILS_DIR not in sys.path:
    sys.path.insert(0, _UTILS_DIR)
from app.utils import financial_vip, moneyflow_tdx
from app.utils.parquet_job_helpers import DailyFetchJob

pytestmark = pytest.mark.module_data_jobs


def test_daily_fetch_explicit_plan_resumes_missing_dates(monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(
        "app.utils.parquet_job_helpers.resolve_trade_dates_with_gap_fill",
        lambda _table: (["20261028", "20261029", "20261030"], False),
    )
    monkeypatch.setattr(
        "app.utils.parquet_job_helpers.save_partitioned_parquet",
        lambda df, _column, _table: len(df),
    )
    calls = []
    fail = {"value": True}

    class Job(DailyFetchJob):
        job_name = "contract_daily"
        rel_table = "daily_history/daily"

    job = Job()

    def fetch(trade_date):
        calls.append(trade_date)
        if trade_date == "20261029" and fail["value"]:
            return None
        return pd.DataFrame({"trade_date": [trade_date]})

    monkeypatch.setattr(job, "_fetch_with_retry", fetch)
    with pytest.raises(SystemExit):
        job.run()
    assert calls == ["20261028", "20261029", "20261030"]

    calls.clear()
    fail["value"] = False
    job.run()
    assert calls == ["20261029"]


def test_financial_vip_resumes_after_saved_report_period(monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(
        financial_vip, "resolve_report_periods",
        lambda _table: (["20200331", "20200630", "20200930"], False),
    )
    monkeypatch.setattr(
        financial_vip, "save_partitioned_parquet", lambda *_args: 1
    )
    calls = []
    fail = {"value": True}

    def api(*, period, fields):
        calls.append(period)
        if period == "20200630" and fail["value"]:
            raise ConnectionError("temporary")
        return pd.DataFrame({"end_date": [period]})

    monkeypatch.setitem(sys.modules, "db_utils", SimpleNamespace(
        DatabaseUtils=SimpleNamespace(init_tushare_api=lambda: SimpleNamespace(income_vip=api))
    ))
    with pytest.raises(ConnectionError):
        financial_vip.run_financial_vip_job("income_vip", "income_statement", [])
    assert calls == ["20200331", "20200630"]

    calls.clear()
    fail["value"] = False
    financial_vip.run_financial_vip_job("income_vip", "income_statement", [])
    assert calls == ["20200630", "20200930"]


def test_adj_factor_retries_only_unfinished_stocks(monkeypatch, tmp_path):
    monkeypatch.setattr(adj, "CHECKPOINT_ROOT", tmp_path / "checkpoints", raising=False)
    monkeypatch.setattr(adj, "resolve_date_range", lambda: ("20200101", "20201231"))
    daily = pd.DataFrame({
        "ts_code": ["A", "B", "C"],
        "trade_date": pd.to_datetime(["2020-01-02"] * 3),
        "close": [1.0, 2.0, 3.0],
    })
    monkeypatch.setattr(adj, "ParquetDataReader", lambda: SimpleNamespace(
        get_daily=lambda **_kwargs: daily.copy()
    ))
    monkeypatch.setitem(sys.modules, "baostock", SimpleNamespace(
        login=lambda: SimpleNamespace(error_code="0"),
        logout=lambda: SimpleNamespace(error_code="0"),
    ))
    monkeypatch.setattr(adj.time, "sleep", lambda _: None)
    monkeypatch.setattr(adj, "merge_save", lambda chunk: len(chunk))
    calls = []
    fail = {"value": True}

    def fetch(_bs, ts_code):
        calls.append(ts_code)
        if ts_code == "B" and fail["value"]:
            raise ConnectionError("temporary")
        return pd.DataFrame()

    monkeypatch.setattr(adj, "fetch_adjust_events", fetch)
    assert adj.main() == 1
    assert calls == ["A", "B", "C"]

    calls.clear()
    fail["value"] = False
    assert adj.main() == 0
    assert calls == ["B"]


def test_moneyflow_tdx_retries_only_unfinished_stocks(monkeypatch, tmp_path):
    project = tmp_path / "project"
    (project / "data").mkdir(parents=True)
    pd.DataFrame({"ts_code": ["A", "B", "C"]}).to_parquet(project / "data" / "stock_basic.parquet")
    monkeypatch.setattr(moneyflow_tdx, "_PROJECT_ROOT", project)
    monkeypatch.setattr(moneyflow_tdx, "CHECKPOINT_ROOT", tmp_path / "checkpoints", raising=False)
    monkeypatch.setattr(moneyflow_tdx, "resolve_trade_date", lambda: "20261030")
    monkeypatch.setattr(moneyflow_tdx, "merge_save", lambda rows, _date: len(rows))

    import app.services.tongdaxin.client as client
    monkeypatch.setattr(client, "create_hq_api", lambda: SimpleNamespace(disconnect=lambda: None))
    monkeypatch.setattr(client, "connect_first_available", lambda _api: None)

    calls = []
    fail = {"value": True}

    def aggregate(_api, ts_code, _date_int):
        calls.append(ts_code)
        if ts_code == "B" and fail["value"]:
            raise ConnectionError("temporary")
        return {"ts_code": ts_code}

    monkeypatch.setattr(moneyflow_tdx, "aggregate_stock", aggregate)
    assert moneyflow_tdx.main() == 1
    assert calls == ["A", "B", "B", "C"]

    calls.clear()
    fail["value"] = False
    assert moneyflow_tdx.main() == 0
    assert calls == ["B"]
