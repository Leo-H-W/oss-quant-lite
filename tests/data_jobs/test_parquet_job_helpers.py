import pandas as pd
import pytest

from app.utils import parquet_job_helpers

pytestmark = pytest.mark.module_data_jobs


class _FakeReader:
    def get_trade_calendar(self):
        return pd.DataFrame(
            [
                {"exchange": "SSE", "cal_date": "20260603", "is_open": 1},
                {"exchange": "SSE", "cal_date": "20260604", "is_open": 1},
                {"exchange": "SSE", "cal_date": "20260605", "is_open": 0},
            ]
        )


def test_resolve_trade_dates_uses_trade_calendar_for_new_open_dates(monkeypatch):
    monkeypatch.setenv("DATA_JOB_START_DATE", "2026-06-04")
    monkeypatch.setenv("DATA_JOB_END_DATE", "2026-06-04")
    monkeypatch.delenv("DATA_JOB_TRADE_DATE", raising=False)
    monkeypatch.delenv("DATA_JOB_FULL_REFRESH", raising=False)
    monkeypatch.setattr(parquet_job_helpers, "ParquetDataReader", lambda: _FakeReader())

    trade_dates, full_refresh = parquet_job_helpers.resolve_trade_dates()

    assert trade_dates == ["20260604"]
    assert full_refresh is False


def test_resolve_trade_dates_clamps_to_today_when_calendar_has_future(monkeypatch):
    """交易日历覆盖全年（含未来日期）时，缺省解析不得把未来日期纳入结果，
    否则缺口回补会空拉未来日期浪费限频配额。"""
    from datetime import datetime

    class _FutureCalendarReader:
        def get_trade_calendar(self):
            return pd.DataFrame(
                [
                    {"exchange": "SSE", "cal_date": "20260603", "is_open": 1},
                    {"exchange": "SSE", "cal_date": "20260604", "is_open": 1},
                    {"exchange": "SSE", "cal_date": "20991231", "is_open": 1},
                ]
            )

    for key in ("DATA_JOB_START_DATE", "DATA_JOB_END_DATE", "DATA_JOB_TRADE_DATE", "DATA_JOB_FULL_REFRESH"):
        monkeypatch.delenv(key, raising=False)
    # 显式起点、不收尾：end_date 走缺省路径（应被收敛到今天而不是 20991231）
    monkeypatch.setenv("DATA_JOB_START_DATE", "2026-06-03")
    monkeypatch.setattr(parquet_job_helpers, "ParquetDataReader", lambda: _FutureCalendarReader())

    trade_dates, _ = parquet_job_helpers.resolve_trade_dates()

    today = datetime.now().strftime("%Y%m%d")
    assert trade_dates == ["20260603", "20260604"]
    assert max(trade_dates) <= today


def test_resolve_trade_dates_snaps_back_to_last_open_day_on_weekend(monkeypatch):
    """今天不是开市日（周末/节假日）时，缺省解析应回退到最近的开市日，
    保证缺口回补能触发，而不是空手返回。"""
    from datetime import datetime

    class _RecentCalendarReader:
        def get_trade_calendar(self):
            today = datetime.now()
            # 构造：昨天和前天开市，外加一个未来开市日
            from datetime import timedelta
            d1 = (today - timedelta(days=2)).strftime("%Y%m%d")
            d2 = (today - timedelta(days=1)).strftime("%Y%m%d")
            return pd.DataFrame(
                [
                    {"exchange": "SSE", "cal_date": d1, "is_open": 1},
                    {"exchange": "SSE", "cal_date": d2, "is_open": 1},
                    {"exchange": "SSE", "cal_date": "20991231", "is_open": 1},
                ]
            )

    for key in ("DATA_JOB_START_DATE", "DATA_JOB_END_DATE", "DATA_JOB_TRADE_DATE", "DATA_JOB_FULL_REFRESH"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(parquet_job_helpers, "ParquetDataReader", lambda: _RecentCalendarReader())

    trade_dates, _ = parquet_job_helpers.resolve_trade_dates()

    from datetime import timedelta
    expected = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    # latest-only 模式：恰好落在最近开市日（昨天），且不包含未来日期
    assert trade_dates == [expected]
