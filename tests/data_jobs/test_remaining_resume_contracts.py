"""扶摇与衍生历史计算的断点/失败状态合约。"""
from types import SimpleNamespace

import pandas as pd
import pytest

from app.utils import (
    cyq_perf_compute,
    daily_history_fuyao,
    factor_compute,
    financial_fuyao,
)

pytestmark = pytest.mark.module_data_jobs


def test_daily_history_fuyao_refetches_only_missing_dates(monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    dates = ["20261028", "20261029", "20261030"]
    monkeypatch.setattr(
        daily_history_fuyao, "resolve_trade_dates_with_gap_fill", lambda _table: (dates, False)
    )
    monkeypatch.setattr(daily_history_fuyao, "save_to_parquet", lambda *_args: 1)
    calls = []
    fail = {"value": True}

    class Fetcher:
        def fetch_dates(self, requested):
            calls.append(list(requested))
            return {
                date: pd.DataFrame({"trade_date": [date]})
                for date in requested
                if not (date == "20261029" and fail["value"])
            }

    monkeypatch.setattr(daily_history_fuyao, "FuyaoDailyFetcher", Fetcher)
    assert daily_history_fuyao.main() == 1
    calls.clear()
    fail["value"] = False
    assert daily_history_fuyao.main() == 0
    assert calls == [["20261029"]]


def test_financial_fuyao_resumes_symbols_and_fails_on_partial(monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(financial_fuyao, "TABLES", {"income": "income_statement"})
    monkeypatch.setattr(financial_fuyao, "expected_latest_period", lambda today=None: "20260630")
    monkeypatch.setattr(financial_fuyao, "latest_partition_date", lambda *_a, **_kw: None)
    monkeypatch.setattr(financial_fuyao, "all_stock_codes", lambda client=None: ["A", "B", "C"])
    monkeypatch.setattr(financial_fuyao, "_read_existing_partition", lambda *_a: None)
    monkeypatch.setattr(financial_fuyao, "save_to_parquet", lambda *_a, **_kw: 1)
    monkeypatch.setattr(
        financial_fuyao, "financial_items_to_frame",
        lambda _items, _table, symbol: pd.DataFrame({"ts_code": [symbol], "end_date": ["20260630"]}),
    )
    calls = []
    fail = {"value": True}

    class Client:
        def financial_statement(self, _table, symbol, limit):
            calls.append(symbol)
            if symbol == "B" and fail["value"]:
                raise ConnectionError("temporary")
            return [{"symbol": symbol}]

    monkeypatch.setattr(financial_fuyao, "FuyaoClient", Client)
    assert financial_fuyao.main() == 1
    assert calls == ["A", "B", "C"]

    calls.clear()
    fail["value"] = False
    assert financial_fuyao.main() == 0
    assert calls == ["B"]


def test_cyq_compute_resumes_successful_stock_batches(monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATA_JOB_START_DATE", "20200101")
    monkeypatch.setenv("DATA_JOB_END_DATE", "20201231")
    monkeypatch.setattr(cyq_perf_compute, "_list_daily_codes", lambda *_a: ["A", "B", "C"])
    monkeypatch.setattr(cyq_perf_compute, "STOCK_BATCH", 2)
    daily = pd.DataFrame({"ts_code": ["A", "B", "C"], "trade_date": ["20200102"] * 3})
    monkeypatch.setattr(
        cyq_perf_compute, "_load_columns_for_codes",
        lambda rel_table, *_a: pd.DataFrame() if rel_table == cyq_perf_compute.BASIC_REL else daily.copy(),
    )
    monkeypatch.setattr(cyq_perf_compute, "merge_save", lambda frame: len(frame))
    calls = []
    fail = {"value": True}

    def compute(frame):
        code = frame.iloc[0]["ts_code"]
        calls.append(code)
        if code == "B" and fail["value"]:
            raise ValueError("temporary")
        return pd.DataFrame({"ts_code": [code]})

    monkeypatch.setattr(cyq_perf_compute, "compute_stock_cyq", compute)
    assert cyq_perf_compute.main() == 1
    assert calls == ["A", "B", "C"]

    calls.clear()
    fail["value"] = False
    assert cyq_perf_compute.main() == 0
    assert calls == ["B"]


def test_factor_compute_resumes_factors_and_partial_failure_is_nonzero(monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATA_JOB_START_DATE", "20200101")
    monkeypatch.setenv("DATA_JOB_END_DATE", "20201231")
    monkeypatch.setenv("DATA_JOB_PARAM_FACTOR_IDS", "A,B,C")
    monkeypatch.delenv("DATA_JOB_TRADE_DATE", raising=False)
    calls = []
    fail = {"value": True}

    class Engine:
        def calculate_factor(self, factor_id, _codes, _start, _end):
            calls.append(factor_id)
            if factor_id == "B" and fail["value"]:
                raise ConnectionError("temporary")
            return pd.DataFrame({"value": [1]})

        def save_factor_values(self, _result):
            return None

    monkeypatch.setattr(factor_compute, "FactorEngine", Engine)
    with pytest.raises(SystemExit):
        factor_compute.main()
    assert calls == ["A", "B", "C"]

    calls.clear()
    fail["value"] = False
    factor_compute.main()
    assert calls == ["B"]
