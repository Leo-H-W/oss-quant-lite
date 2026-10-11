import os

from db_utils import DatabaseUtils
from parquet_job_helpers import DailyFetchJob


FIELDS = [
    "ts_code",
    "trade_date",
    "close",
    "turnover_rate",
    "turnover_rate_f",
    "volume_ratio",
    "pe",
    "pe_ttm",
    "pb",
    "ps",
    "ps_ttm",
    "dv_ratio",
    "dv_ttm",
    "total_share",
    "float_share",
    "free_share",
    "total_mv",
    "circ_mv",
]


class DailyBasicJob(DailyFetchJob):
    job_name = "daily_basic"
    rel_table = "daily_basic/daily"

    # tushare 对 daily_basic 接口按账号积分限频（实测低档账号 1 次/分钟），
    # 默认 0.12s 的间隔必然全部超限失败；可用 DAILY_BASIC_RATE_LIMIT 覆盖
    rate_limit_seconds = float(os.getenv("DAILY_BASIC_RATE_LIMIT", "62"))

    def fetch_one(self, trade_date):
        return self.api.daily_basic(trade_date=trade_date, fields=FIELDS)


def main():
    DailyBasicJob(api=DatabaseUtils.init_tushare_api()).run()


if __name__ == "__main__":
    main()
