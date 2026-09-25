import os
from datetime import date

import pandas as pd

from db_utils import DatabaseUtils
from parquet_writer import save_single_parquet

CALENDAR_FILE = "stock_trade_calendar.parquet"
# 日历下载覆盖的起止（交易所年底才公布次年日历，结束日期写到明年即可）
START_DATE = "20050101"
END_DATE = f"{date.today().year + 1}1231"


def _data_dir() -> str:
    # 与 parquet_writer._data_root 保持一致
    return os.getenv("DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "..", "data"))


def _existing_calendar() -> pd.DataFrame:
    """读取本地已有日历，不存在或损坏时返回 None。"""
    path = os.path.join(_data_dir(), CALENDAR_FILE)
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_parquet(path)
    except Exception:
        return None
    return df if not df.empty else None


def _is_fresh_enough(df: pd.DataFrame) -> bool:
    """本地日历已覆盖到当年年底即视为足够新，无需再请求（trade_cal 低频接口，1次/小时）。"""
    max_cal_date = str(df["cal_date"].max())
    return max_cal_date >= f"{date.today().year}1231"


def main():
    # 交易日历几乎不变，本地已覆盖到当年年底就直接跳过，避免触发 Tushare 频率限制
    existing = _existing_calendar()
    if existing is not None and _is_fresh_enough(existing):
        print(f"本地交易日历已覆盖至 {existing['cal_date'].max()}，跳过下载")
        return

    pro = DatabaseUtils.init_tushare_api()
    # 从 2005 年起覆盖完整历史：财务因子公告日对齐交易日需要历史日历，
    # 只下载近年会让早期快照落在周末且无法对齐，精确匹配永远查不到
    try:
        data = pro.trade_cal(
            exchange="",
            start_date=START_DATE,
            end_date=END_DATE,
            fields="exchange,cal_date,is_open,pretrade_date",
        )
    except Exception as exc:
        # 频率超限（1次/小时）但本地有可用日历时降级为复用旧数据，避免阻塞后续任务
        if existing is not None and "频率" in str(exc):
            print(f"Tushare 频率超限，复用本地交易日历（覆盖至 {existing['cal_date'].max()}）: {exc}")
            return
        raise
    save_single_parquet(data, CALENDAR_FILE)


if __name__ == "__main__":
    main()
