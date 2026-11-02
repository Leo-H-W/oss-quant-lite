"""财务最新期完整度与备用源包装任务合约。"""
from pathlib import Path
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

_UTILS_DIR = str(Path(__file__).resolve().parents[2] / "app" / "utils")
if _UTILS_DIR not in sys.path:
    sys.path.insert(0, _UTILS_DIR)
from app.utils import financial_eastmoney, financial_fuyao, financial_tushare, financial_vip

pytestmark = pytest.mark.module_data_jobs


def _financial_mocks(monkeypatch, tmp_path, coverage):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(financial_fuyao, "TABLES", {"income": "income_statement"})
    monkeypatch.setattr(financial_fuyao, "expected_latest_period", lambda today=None: "20260930")
    monkeypatch.setattr(financial_fuyao, "latest_partition_date", lambda *_a, **_kw: "20260930")
    monkeypatch.setattr(financial_fuyao, "all_stock_codes", lambda client=None: ["A", "B"])
    monkeypatch.setattr(financial_fuyao, "latest_period_has_coverage", lambda *a, **kw: coverage.pop(0))
    missing = iter([[], []])
    monkeypatch.setattr(financial_fuyao, "find_missing_report_periods", lambda *a, **kw: next(missing))
    monkeypatch.setattr(financial_fuyao, "FuyaoClient", lambda: object())


def test_partial_latest_period_is_refetched_not_skipped(monkeypatch, tmp_path):
    _financial_mocks(monkeypatch, tmp_path, [False, True])
    calls = []
    monkeypatch.setattr(
        financial_fuyao, "_sync_table",
        lambda *args, **kwargs: calls.append((args, kwargs)) or (2, 1, 2),
    )

    assert financial_fuyao.main() == 0
    assert len(calls) == 1


def test_period_still_partial_after_sync_returns_failure(monkeypatch, tmp_path):
    _financial_mocks(monkeypatch, tmp_path, [False, False])
    monkeypatch.setattr(financial_fuyao, "_sync_table", lambda *args, **kwargs: (2, 1, 2))

    assert financial_fuyao.main() == 1


def test_tushare_financial_wrapper_runs_three_tables_in_order(monkeypatch):
    calls = []

    def run(command, **_kwargs):
        calls.append(command[-1])
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(financial_tushare.subprocess, "run", run)

    assert financial_tushare.main() == 0
    assert [path.rsplit("/", 1)[-1] for path in calls] == [
        "income_statement.py", "balance_sheet.py", "cash_flow.py",
    ]


def test_tushare_financial_wrapper_stops_on_failure(monkeypatch):
    calls = []

    def run(command, **_kwargs):
        calls.append(command[-1])
        return SimpleNamespace(returncode=1)

    monkeypatch.setattr(financial_tushare.subprocess, "run", run)

    assert financial_tushare.main() == 1
    assert len(calls) == 1


def test_financial_history_scan_finds_missing_standard_periods(tmp_path):
    for period in ("20190630", "20191231"):
        part = tmp_path / "income_statement" / f"year={period[:4]}" / f"month={period[4:6]}" / f"day={period[6:]}"
        part.mkdir(parents=True)
        pd.DataFrame({"ts_code": ["A"], "end_date": [period]}).to_parquet(part / "data.parquet")

    assert financial_fuyao.find_missing_report_periods(
        "income_statement", "20200331", str(tmp_path)
    ) == ["20190930", "20200331"]


def test_fuyao_starts_at_earliest_missing_period(monkeypatch, tmp_path):
    _financial_mocks(monkeypatch, tmp_path, [True, True])
    missing = iter([["20190930"], []])
    monkeypatch.setattr(financial_fuyao, "find_missing_report_periods", lambda *a, **kw: next(missing))
    baselines = []
    monkeypatch.setattr(
        financial_fuyao, "resolve_fetch_periods",
        lambda baseline, expected: baselines.append(baseline) or [baseline, expected],
    )
    monkeypatch.setattr(financial_fuyao, "_sync_table", lambda *args, **kwargs: (2, 1, 2))

    assert financial_fuyao.main() == 0
    assert baselines == ["20190930"]


def test_tushare_resolver_also_starts_at_earliest_missing_period(monkeypatch):
    monkeypatch.delenv("DATA_JOB_START_DATE", raising=False)
    monkeypatch.delenv("DATA_JOB_END_DATE", raising=False)
    monkeypatch.delenv("DATA_JOB_FULL_REFRESH", raising=False)
    monkeypatch.setattr(
        financial_vip, "existing_partition_dates",
        lambda *_args, **_kwargs: {"20190630", "20191231"},
    )

    periods, _ = financial_vip.resolve_report_periods("income_statement", today="20200331")

    assert periods == ["20190930", "20191231", "20200331"]


def test_eastmoney_normalizes_real_income_and_cashflow_fields():
    income = financial_eastmoney.normalize_rows([{
        "SECUCODE": "000001.SZ",
        "NOTICE_DATE": "2019-10-22 00:00:00",
        "REPORT_DATE": "2019-09-30 00:00:00",
        "TOTAL_OPERATE_INCOME": 102958000000,
        "OPERATE_INCOME": 102958000000,
        "OPERATE_COST": 12000000000,
        "SALE_EXPENSE": 2000000000,
        "MANAGE_EXPENSE": 3000000000,
        "FINANCE_EXPENSE": 100000000,
        "OPERATE_PROFIT": 30713000000,
        "TOTAL_PROFIT": 30676000000,
        "INCOME_TAX": 7055000000,
        "PARENT_NETPROFIT": 23621000000,
    }], "income_statement")
    cash = financial_eastmoney.normalize_rows([{
        "SECUCODE": "000001.SZ",
        "NOTICE_DATE": "2019-10-22 00:00:00",
        "REPORT_DATE": "2019-09-30 00:00:00",
        "NETCASH_OPERATE": 110,
        "SALES_SERVICES": 120,
        "PAY_STAFF_CASH": 30,
        "NETCASH_INVEST": -40,
        "CONSTRUCT_LONG_ASSET": 50,
        "NETCASH_FINANCE": 60,
        "CCE_ADD": 70,
        "BEGIN_CCE": 80,
        "END_CCE": 150,
    }], "cash_flow")

    assert income.iloc[0][["ts_code", "ann_date", "end_date"]].tolist() == [
        "000001.SZ", "20191022", "20190930",
    ]
    assert income.iloc[0]["total_revenue"] == 102958000000
    assert income.iloc[0]["n_income_attr_p"] == 23621000000
    assert cash.iloc[0]["n_cashflow_act"] == 110
    assert cash.iloc[0]["c_pay_acq_const_fiolta"] == 50
    assert cash.iloc[0]["n_cash_inc_cash_equi"] == 70
    assert cash.iloc[0]["c_cash_equ_end_period"] == 150


class _Response:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class _Session:
    def __init__(self):
        self.pages = []

    def get(self, _url, params, timeout):
        self.pages.append(params["pageNumber"])
        page = params["pageNumber"]
        return _Response({"result": {
            "pages": 2,
            "count": 2,
            "data": [{"REPORT_DATE": "2019-09-30", "page": page}],
        }})


def test_eastmoney_fetches_every_page():
    session = _Session()

    rows = financial_eastmoney.fetch_report_period(
        "income_statement", "20190930", session=session,
    )

    assert session.pages == [1, 2]
    assert [row["page"] for row in rows] == [1, 2]


def test_eastmoney_sync_merges_instead_of_replacing_existing_rows(monkeypatch, tmp_path):
    existing = pd.DataFrame({
        "ts_code": ["OLD.SZ"], "end_date": ["20190930"], "ann_date": ["20191001"],
    })
    partition = tmp_path / "income_statement/year=2019/month=09/day=30"
    partition.mkdir(parents=True)
    existing.to_parquet(partition / "data.parquet", index=False)
    monkeypatch.setattr(
        financial_eastmoney, "fetch_report_period",
        lambda *_args, **_kwargs: [{
            "SECUCODE": "NEW.SZ", "NOTICE_DATE": "2019-10-30",
            "REPORT_DATE": "2019-09-30", "PARENT_NETPROFIT": 123,
        }],
    )

    rows = financial_eastmoney.sync_table(
        "income_statement", "20190930", data_dir=str(tmp_path),
    )

    path = tmp_path / "income_statement/year=2019/month=09/day=30/data.parquet"
    saved = pd.read_parquet(path)
    assert rows == 2
    assert set(saved["ts_code"]) == {"OLD.SZ", "NEW.SZ"}
    assert existing["ts_code"].iloc[0] in set(saved["ts_code"])
