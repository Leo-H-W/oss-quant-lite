"""daily_basic Baostock 股票级断点续跑合约。"""
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

import app.utils.daily_basic_baostock as daily_basic

pytestmark = pytest.mark.module_data_jobs


def _frame(ts_code):
    return pd.DataFrame({
        "ts_code": [ts_code],
        "trade_date": ["2020-01-02"],
        "close": [1.0],
    })


def _configure_main(monkeypatch, tmp_path, ts_codes):
    project_root = tmp_path / "project"
    data_dir = project_root / "data"
    data_dir.mkdir(parents=True)
    pd.DataFrame({"ts_code": ts_codes}).to_parquet(data_dir / "stock_basic.parquet")

    monkeypatch.setattr(daily_basic, "_PROJECT_ROOT", project_root)
    monkeypatch.setattr(
        daily_basic, "CHECKPOINT_ROOT", tmp_path / "checkpoints", raising=False
    )
    monkeypatch.setattr(daily_basic, "CHUNK_STOCKS", 2)
    monkeypatch.setattr(daily_basic.time, "sleep", lambda _: None)

    fake_bs = SimpleNamespace(
        login=lambda: SimpleNamespace(error_code="0"),
        logout=lambda: SimpleNamespace(error_code="0"),
    )
    monkeypatch.setitem(sys.modules, "baostock", fake_bs)
    monkeypatch.setattr(daily_basic, "merge_save", lambda chunk: len(chunk))


def test_interrupted_run_resumes_after_last_persisted_stock_batch(monkeypatch, tmp_path):
    """进程中断前已落盘的 A/B 不得重拉，只从 C 继续。"""
    _configure_main(monkeypatch, tmp_path, ["A", "B", "C", "D"])
    calls = []
    interrupted = {"value": True}

    def fake_fetch(_bs, ts_code, _start, _end):
        calls.append(ts_code)
        if ts_code == "C" and interrupted["value"]:
            raise KeyboardInterrupt("simulated runner kill")
        return _frame(ts_code)

    monkeypatch.setattr(daily_basic, "fetch_stock", fake_fetch)
    with pytest.raises(KeyboardInterrupt):
        daily_basic.main()
    assert calls == ["A", "B", "C"]

    calls.clear()
    interrupted["value"] = False
    assert daily_basic.main() == 0
    assert calls == ["C", "D"]
    assert not list((tmp_path / "checkpoints").glob("*.json"))


def test_failed_stocks_are_retried_without_refetching_completed_stocks(monkeypatch, tmp_path):
    """失败股票不进入完成检查点；最终成功后检查点删除，允许显式全量刷新。"""
    _configure_main(monkeypatch, tmp_path, ["A", "B", "C"])
    calls = []
    fail_b = {"value": True}

    def fake_fetch(_bs, ts_code, _start, _end):
        calls.append(ts_code)
        if ts_code == "B" and fail_b["value"]:
            raise ConnectionError("temporary disconnect")
        return _frame(ts_code)

    monkeypatch.setattr(daily_basic, "fetch_stock", fake_fetch)
    assert daily_basic.main() == 1
    assert calls == ["A", "B", "C"]
    assert list((tmp_path / "checkpoints").glob("*.json"))

    calls.clear()
    fail_b["value"] = False
    assert daily_basic.main() == 0
    assert calls == ["B"]
    assert not list((tmp_path / "checkpoints").glob("*.json"))
