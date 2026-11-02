"""factor_compute 默认历史空洞修复合约。"""
import pandas as pd
import pytest

from app.utils import factor_compute

pytestmark = pytest.mark.module_data_jobs


def test_default_range_covers_earliest_gap_through_latest():
    dates = ["20260102", "20260105", "20260106"]
    assert factor_compute.resolve_default_date_range(dates, {"20260102"}) == (
        "20260105", "20260106"
    )
    # 中间有空洞时结束日期仍取最新交易日，保证最新值也刷新。
    assert factor_compute.resolve_default_date_range(dates, {"20260102", "20260106"}) == (
        "20260105", "20260106"
    )


def test_default_range_without_gaps_or_history_only_computes_latest():
    dates = ["20260102", "20260105", "20260106"]
    assert factor_compute.resolve_default_date_range(dates, set(dates)) == (
        "20260106", "20260106"
    )
    assert factor_compute.resolve_default_date_range(dates, set()) == (
        "20260106", "20260106"
    )


def _default_engine(monkeypatch, existing_at_end):
    monkeypatch.delenv("DATA_JOB_TRADE_DATE", raising=False)
    monkeypatch.delenv("DATA_JOB_START_DATE", raising=False)
    monkeypatch.delenv("DATA_JOB_END_DATE", raising=False)
    monkeypatch.delenv("DATA_JOB_PARAM_FACTOR_IDS", raising=False)
    monkeypatch.delenv("DATA_JOB_PARAM_TS_CODES", raising=False)

    class Reader:
        def get_trade_dates(self, start_date=None, end_date=None):
            dates = ["20260102", "20260105", "20260106"]
            return [d for d in dates if (not start_date or d >= start_date) and (not end_date or d <= end_date)]

    class Engine:
        def calculate_all_factors(self, date, ts_codes):
            return pd.DataFrame([{"trade_date": date, "value": 1}])

        def save_factor_values(self, _result):
            return None

    monkeypatch.setattr(factor_compute, "ParquetDataReader", lambda: Reader())
    monkeypatch.setattr(factor_compute, "FactorEngine", lambda: Engine())
    states = iter([{"20260102"}, existing_at_end])
    monkeypatch.setattr(factor_compute, "existing_factor_dates", lambda: next(states))


def test_default_history_mode_fails_if_target_dates_remain_missing(monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    _default_engine(monkeypatch, {"20260102"})

    with pytest.raises(SystemExit) as excinfo:
        factor_compute.main()

    assert excinfo.value.code == 1


def test_default_history_mode_succeeds_after_all_dates_land(monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    _default_engine(monkeypatch, {"20260102", "20260105", "20260106"})

    factor_compute.main()
