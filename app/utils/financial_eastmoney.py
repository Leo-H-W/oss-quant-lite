"""东方财富全市场财务三表回补（Tushare 无权限时的历史备用源）。

数据中心接口按报告期分页返回全市场真实财报摘要，写入与扶摇/Tushare 相同的
Parquet 分区。仅映射口径明确的字段；接口未提供的字段不填零、不用其他字段冒充。

用法：
    PYTHONPATH=. python app/utils/financial_eastmoney.py
    DATA_JOB_START_DATE=2019-09-30 DATA_JOB_END_DATE=2019-09-30 \
        PYTHONPATH=. python app/utils/financial_eastmoney.py
"""
from __future__ import annotations

import os
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from app.utils.financial_fuyao import (  # noqa: E402
    _merge_partition,
    _read_existing_partition,
    expected_latest_period,
    find_missing_report_periods,
    quarter_periods,
)
from app.utils.parquet_writer import save_to_parquet  # noqa: E402

API_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
PAGE_SIZE = 500
TIMEOUT_SECONDS = 30

TABLES = {
    "income_statement": "RPT_DMSK_FN_INCOME",
    "balance_sheet": "RPT_DMSK_FN_BALANCE",
    "cash_flow": "RPT_DMSK_FN_CASHFLOW",
}

_FIELD_MAPS = {
    "income_statement": {
        "TOTAL_OPERATE_INCOME": "total_revenue",
        "OPERATE_INCOME": "revenue",
        "TOTAL_OPERATE_COST": "total_cogs",
        "OPERATE_COST": "oper_cost",
        "SALE_EXPENSE": "sell_exp",
        "MANAGE_EXPENSE": "admin_exp",
        "FINANCE_EXPENSE": "fin_exp",
        "OPERATE_PROFIT": "operate_profit",
        "TOTAL_PROFIT": "total_profit",
        "INCOME_TAX": "income_tax",
        "PARENT_NETPROFIT": "n_income_attr_p",
        "DEDUCT_PARENT_NETPROFIT": "deduct_parent_net_profit",
        "INVEST_INCOME": "invest_income",
    },
    "balance_sheet": {
        "TOTAL_ASSETS": "total_assets",
        "MONETARYFUNDS": "cash_equivalents",
        "ACCOUNTS_RECE": "accounts_receiv",
        "INVENTORY": "inventory",
        "TOTAL_LIABILITIES": "total_liab",
        "ACCOUNTS_PAYABLE": "accounts_payable",
        "TOTAL_EQUITY": "total_equity",
    },
    "cash_flow": {
        "NETCASH_OPERATE": "n_cashflow_act",
        "SALES_SERVICES": "c_fr_sale_sg",
        "PAY_STAFF_CASH": "c_paid_to_for_empl",
        "NETCASH_INVEST": "n_cashflow_inv_act",
        "RECEIVE_INVEST_INCOME": "c_recp_return_invest",
        "CONSTRUCT_LONG_ASSET": "c_pay_acq_const_fiolta",
        "INVEST_PAY_CASH": "c_paid_invest",
        "NETCASH_FINANCE": "n_cash_flows_fnc_act",
        "CCE_ADD": "n_incr_cash_cash_equ",
        "BEGIN_CCE": "c_cash_equ_beg_period",
        "END_CCE": "c_cash_equ_end_period",
    },
}


def _ymd(values: pd.Series) -> pd.Series:
    return pd.to_datetime(values, errors="coerce").dt.strftime("%Y%m%d")


def normalize_rows(rows: List[Dict[str, Any]], table: str) -> pd.DataFrame:
    """东方财富行 → 项目财务字段；未披露/未提供值保持 NaN。"""
    if table not in TABLES:
        raise ValueError(f"未知财务表: {table}")
    raw = pd.DataFrame(rows or [])
    columns = [
        "ts_code", "ann_date", "end_date", "fiscal_year", "fiscal_period",
        *dict.fromkeys(_FIELD_MAPS[table].values()), "period", "currency",
    ]
    if raw.empty:
        return pd.DataFrame(columns=columns)
    if "SECUCODE" not in raw or "REPORT_DATE" not in raw:
        raise ValueError(f"{table} 响应缺少 SECUCODE/REPORT_DATE")

    frame = pd.DataFrame(index=raw.index)
    frame["ts_code"] = raw["SECUCODE"].astype(str)
    frame["ann_date"] = _ymd(raw.get("NOTICE_DATE"))
    frame["end_date"] = _ymd(raw["REPORT_DATE"])
    for source, target in _FIELD_MAPS[table].items():
        frame[target] = pd.to_numeric(raw.get(source), errors="coerce")

    # 与既有扶摇现金流量表字段兼容，同时保留 Tushare 标准字段。
    if table == "cash_flow":
        frame["construct_fix_asset"] = frame["c_pay_acq_const_fiolta"]
        frame["n_cash_inc_cash_equi"] = frame["n_incr_cash_cash_equ"]

    frame = frame.dropna(subset=["end_date"])
    frame = frame[frame["ts_code"].ne("") & frame["ts_code"].ne("None")].copy()
    frame["fiscal_year"] = frame["end_date"].str[:4]
    frame["fiscal_period"] = frame["end_date"].str[4:].map({
        "0331": "Q1", "0630": "Q2", "0930": "Q3", "1231": "Q4",
    })
    frame["period"] = "quarterly"
    frame["currency"] = "CNY"
    return frame.reset_index(drop=True)


def fetch_report_period(
    table: str,
    period: str,
    session: Optional[requests.Session] = None,
) -> List[Dict[str, Any]]:
    """分页拉取一个标准报告期的全市场数据，页数/总数不完整时失败。"""
    report_name = TABLES.get(table)
    if report_name is None:
        raise ValueError(f"未知财务表: {table}")
    clean_period = str(period).replace("-", "")
    if len(clean_period) != 8 or clean_period[4:] not in {"0331", "0630", "0930", "1231"}:
        raise ValueError(f"非法财务报告期: {period}")

    client = session or requests.Session()
    rows: List[Dict[str, Any]] = []
    page = 1
    total_pages = 1
    expected_count: Optional[int] = None
    while page <= total_pages:
        response = client.get(API_URL, params={
            "reportName": report_name,
            "columns": "ALL",
            "filter": f"(REPORT_DATE='{clean_period[:4]}-{clean_period[4:6]}-{clean_period[6:]}')",
            "pageNumber": page,
            "pageSize": PAGE_SIZE,
            "sortColumns": "SECURITY_CODE",
            "sortTypes": "1",
        }, timeout=TIMEOUT_SECONDS)
        response.raise_for_status()
        payload = response.json()
        result = payload.get("result")
        if not isinstance(result, dict):
            raise RuntimeError(
                f"东方财富 {table} {clean_period} 第 {page} 页无数据: "
                f"{payload.get('message') or payload.get('msg') or payload}"
            )
        batch = result.get("data") or []
        if not isinstance(batch, list):
            raise RuntimeError(f"东方财富 {table} 返回了非法 data 类型")
        rows.extend(batch)
        total_pages = int(result.get("pages") or 1)
        expected_count = int(result.get("count") or len(rows))
        page += 1

    if expected_count is not None and len(rows) != expected_count:
        raise RuntimeError(
            f"东方财富 {table} {clean_period} 分页不完整: "
            f"{len(rows)}/{expected_count}"
        )
    return rows


def sync_table(
    table: str,
    period: str,
    data_dir: Optional[str] = None,
    session: Optional[requests.Session] = None,
) -> int:
    """下载并原子落盘一个报告期；与已有记录按最新公告日合并。"""
    clean_period = str(period).replace("-", "")
    frame = normalize_rows(fetch_report_period(table, clean_period, session=session), table)
    frame = frame[frame["end_date"] == clean_period].copy()
    if frame.empty:
        raise RuntimeError(f"东方财富 {table} {clean_period} 没有目标报告期数据")
    existing = _read_existing_partition(table, clean_period, data_dir)
    merged = _merge_partition(existing, frame)
    rows = save_to_parquet(merged, clean_period, table, data_dir=data_dir)
    print(
        f"[financial_eastmoney] {table} period={clean_period} "
        f"source_rows={len(frame)} total_upsert={rows}",
        flush=True,
    )
    return rows


def main() -> int:
    load_dotenv()
    data_dir = os.getenv("DATA_DIR")
    start = (os.getenv("DATA_JOB_START_DATE") or "").replace("-", "")
    end = (os.getenv("DATA_JOB_END_DATE") or "").replace("-", "")
    expected = end or expected_latest_period()
    if not expected:
        print("[financial_eastmoney] 尚无过披露截止日的报告期")
        return 0

    overall_exit = 0
    for table in TABLES:
        if start:
            periods = quarter_periods(start, expected)
        else:
            # 最新期也重拉：已有但残缺的分区不能只靠“缺分区”扫描发现。
            periods = sorted(set(
                find_missing_report_periods(table, expected, data_dir) + [expected]
            ))
        if not periods:
            print(f"[financial_eastmoney] {table}: 无需回补")
            continue
        for period in periods:
            try:
                sync_table(table, period, data_dir=data_dir)
            except Exception as exc:  # noqa: BLE001 - 单期失败保留退出码并继续其他表
                print(f"[financial_eastmoney] {table} {period} 失败: {exc}", flush=True)
                overall_exit = 1
        remaining = find_missing_report_periods(table, expected, data_dir)
        if remaining:
            print(f"[financial_eastmoney] {table}: 仍缺报告期 {remaining}", flush=True)
            overall_exit = 1
    return overall_exit


if __name__ == "__main__":
    sys.exit(main())
