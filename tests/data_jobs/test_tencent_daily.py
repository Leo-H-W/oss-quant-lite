"""腾讯日线备用生产者合约。"""
import pandas as pd
import pytest

from app.services.data_jobs.registry import JobRegistry
from app.utils import daily_history_tencent

pytestmark = pytest.mark.module_data_jobs


def test_tencent_job_registered_hidden_and_ui_count_unchanged():
    registry = JobRegistry()
    job = registry.get_job("daily_history_tencent")
    assert job.script_path == "app/utils/daily_history_tencent.py"
    assert job.source_name == "tencent"
    assert "daily_history_tencent" not in {j.job_type for j in registry.list_visible_jobs()}
    assert len(registry.list_visible_jobs()) == 12


def test_parse_raw_daily_kline_to_standard_schema():
    payload = {
        "code": 0,
        "data": {"sh600519": {"day": [
            ["2023-01-03", "100.0", "102.0", "103.0", "99.0", "1234.0"],
            ["2023-01-04", "102.0", "101.0", "104.0", "100.0", "2345.0"],
        ]}},
    }

    frame = daily_history_tencent.parse_kline_rows(
        "600519.SH", payload, {"20230103", "20230104"}
    )

    assert list(frame["trade_date"]) == ["2023-01-03", "2023-01-04"]
    assert list(frame["close"]) == [102.0, 101.0]
    assert list(frame["vol"]) == [1234.0, 2345.0]
    assert frame.iloc[1]["pre_close"] == 102.0
    assert frame.iloc[1]["change"] == -1.0
    assert frame.iloc[1]["pct_change"] == pytest.approx(-0.980392, rel=1e-5)


def test_merge_preserves_existing_non_null_fields(tmp_path):
    date = "2023-01-03"
    part = tmp_path / "daily_history/daily/year=2023/month=01/day=03"
    part.mkdir(parents=True)
    pd.DataFrame([{
        "ts_code": "600519.SH", "trade_date": "2023-01-03", "close": 100.0,
        "amount": 888.0, "pre_close": 99.0,
    }]).to_parquet(part / "data.parquet")
    new = pd.DataFrame([{
        "ts_code": "600519.SH", "trade_date": "2023-01-03", "close": 102.0,
        "amount": float("nan"), "pre_close": float("nan"),
    }])

    daily_history_tencent.merge_save(new, data_dir=str(tmp_path))

    saved = pd.read_parquet(part / "data.parquet").iloc[0]
    assert saved["close"] == 102.0
    assert saved["amount"] == 888.0
    assert saved["pre_close"] == 99.0


def test_fetch_failure_is_visible_and_resume_skips_completed(monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATA_JOB_START_DATE", "20230103")
    monkeypatch.setenv("DATA_JOB_END_DATE", "20230103")
    pd.DataFrame({"cal_date": ["20230103"], "is_open": [1]}).to_parquet(
        tmp_path / "stock_trade_calendar.parquet"
    )
    pd.DataFrame({"ts_code": ["600519.SH", "000001.SZ"]}).to_parquet(tmp_path / "stock_basic.parquet")
    calls = []
    fail = {"value": True}

    def fetch(ts_code, start, end, target_dates):
        calls.append(ts_code)
        if ts_code == "000001.SZ" and fail["value"]:
            raise ConnectionError("temporary")
        return pd.DataFrame([{
            "ts_code": ts_code, "trade_date": "2023-01-03", "close": 102.0,
        }])

    monkeypatch.setattr(daily_history_tencent, "fetch_stock", fetch)
    monkeypatch.setattr(daily_history_tencent, "merge_save", lambda frame, data_dir=None: len(frame))

    assert daily_history_tencent.main() == 1
    fail["value"] = False
    assert daily_history_tencent.main() == 0
    assert calls == ["600519.SH", "000001.SZ", "000001.SZ"]
