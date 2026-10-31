import os

from db_utils import DatabaseUtils
from job_checkpoint import clear_checkpoint, load_checkpoint, plan_checkpoint_path, save_checkpoint
from parquet_job_helpers import get_stock_codes, resolve_trade_dates
from parquet_writer import save_partitioned_parquet


def main():
    pro = DatabaseUtils.init_tushare_api()
    stock_list = get_stock_codes()
    trade_dates, _ = resolve_trade_dates()

    checkpoint_path = plan_checkpoint_path(
        os.path.join(
            os.getenv("DATA_DIR", os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data")),
            "data_job_state", "checkpoints",
        ),
        "daily_history_by_code",
        {"trade_dates": trade_dates, "stock_list": stock_list},
    )
    checkpoint = load_checkpoint(checkpoint_path)
    completed = set(
        checkpoint.get("completed_trade_dates", [])
        if checkpoint.get("planned_trade_dates") == trade_dates else []
    )

    total_saved = 0
    for trade_date in trade_dates:
        if trade_date in completed:
            continue
        frames = []
        for ts_code in stock_list:
            df = pro.daily(ts_code=ts_code, trade_date=trade_date)
            if df is not None and not df.empty:
                frames.append(df)
        if frames:
            import pandas as pd

            total_saved += save_partitioned_parquet(
                pd.concat(frames, ignore_index=True), "trade_date", "daily_history/daily"
            )
        completed.add(trade_date)
        save_checkpoint(checkpoint_path, {
            "version": 1,
            "planned_trade_dates": trade_dates,
            "completed_trade_dates": sorted(completed),
        })
        print(f"[daily_history_by_code] trade_date={trade_date}")

    clear_checkpoint(checkpoint_path)
    print(
        f"[daily_history_by_code] 完成，trade_days={len(trade_dates)}, total_upsert={total_saved}"
    )


if __name__ == "__main__":
    main()
