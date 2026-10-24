import os

from db_utils import DatabaseUtils
from parquet_job_helpers import DailyFetchJob


FIELDS = [
    "ts_code",
    "trade_date",
    "his_low",
    "his_high",
    "cost_5pct",
    "cost_15pct",
    "cost_50pct",
    "cost_85pct",
    "cost_95pct",
    "weight_avg",
    "winner_rate",
]


class CyqPerfJob(DailyFetchJob):
    job_name = "cyq_perf"
    rel_table = "cyq_perf/daily"

    # tushare cyq_perf 接口限频 50 次/分钟，1.5s/次留安全余量
    rate_limit_seconds = float(os.getenv("CYQ_PERF_RATE_LIMIT", "1.5"))

    def fetch_one(self, trade_date):
        return self.api.cyq_perf(trade_date=trade_date, fields=FIELDS)


def main():
    CyqPerfJob(api=DatabaseUtils.init_tushare_api()).run()


if __name__ == "__main__":
    main()
