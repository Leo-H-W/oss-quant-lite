import os

from db_utils import DatabaseUtils
from parquet_job_helpers import DailyFetchJob


class DailyHistoryByDateJob(DailyFetchJob):
    job_name = "daily_history_by_date"
    rel_table = "daily_history/daily"

    # tushare daily 接口限频 50 次/分钟，1.5s/次（40 次/分）留安全余量
    rate_limit_seconds = float(os.getenv("DAILY_HISTORY_RATE_LIMIT", "1.5"))

    def fetch_one(self, trade_date):
        return self.api.daily(trade_date=trade_date)


def main():
    DailyHistoryByDateJob(api=DatabaseUtils.init_tushare_api()).run()


if __name__ == "__main__":
    main()
