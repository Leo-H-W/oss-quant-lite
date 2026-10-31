"""因子计算作业（衍生计算）。

把 FactorEngine 的因子计算纳入 data_jobs 流水线：数据日更完成后运行
本作业，将内置因子与自定义表达式因子的值批量计算并写入 factor_values
存储，供打分/回测读取。

Usage:
    python app/utils/factor_compute.py

环境变量（由 ScriptRunner 注入）:
    DATA_JOB_TRADE_DATE          单日模式：只算这个交易日
    DATA_JOB_START_DATE/_END_DATE 区间模式：计算整个区间（推荐回填用）
    DATA_JOB_PARAM_FACTOR_IDS    逗号分隔的因子列表；缺省算全部因子
    DATA_JOB_PARAM_TS_CODES      逗号分隔的股票列表；缺省全市场

两者都不传时默认计算最新交易日的全部因子。
"""
import os
import sys

_project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from loguru import logger

from app.services.data_reader import ParquetDataReader
from app.services.factor_engine import FactorEngine
from app.utils.job_checkpoint import clear_checkpoint, load_checkpoint, plan_checkpoint_path, save_checkpoint


def _split_env(name):
    value = os.getenv(name, "").strip()
    if not value:
        return None
    return [item.strip() for item in value.split(",") if item.strip()]


def _is_retryable(exc):
    """网络/超时失败可续跑重试；表达式等确定性错误保留部分成功语义。"""
    return isinstance(exc, (ConnectionError, TimeoutError, OSError))


def main():
    engine = FactorEngine()

    trade_date = os.getenv("DATA_JOB_TRADE_DATE") or None
    start_date = os.getenv("DATA_JOB_START_DATE") or None
    end_date = os.getenv("DATA_JOB_END_DATE") or None
    factor_ids = _split_env("DATA_JOB_PARAM_FACTOR_IDS")
    ts_codes = _split_env("DATA_JOB_PARAM_TS_CODES")

    if not start_date and not end_date and not trade_date:
        # 默认：最新交易日
        all_dates = ParquetDataReader().get_trade_dates()
        if not all_dates:
            print("没有任何交易日数据，无法计算因子")
            sys.exit(1)
        trade_date = all_dates[-1]

    if not end_date:
        end_date = trade_date or start_date
    if not start_date:
        start_date = end_date or trade_date

    checkpoint_path = plan_checkpoint_path(
        os.path.join(
            os.getenv("DATA_DIR", os.path.join(_project_root, "data")),
            "data_job_state", "checkpoints",
        ),
        "factor_compute",
        {
            "start_date": start_date, "end_date": end_date,
            "factor_ids": factor_ids, "ts_codes": ts_codes,
        },
    )
    checkpoint = load_checkpoint(checkpoint_path)
    completed_units = set(checkpoint.get("completed_units", []))
    unit_rows = checkpoint.get("unit_rows", {})
    if not isinstance(unit_rows, dict):
        unit_rows = {}
    if completed_units:
        print(f"从检查点续跑，已完成 {len(completed_units)} 个因子计算单元")

    total_saved = sum(int(unit_rows.get(str(unit), 0)) for unit in completed_units)
    per_factor_stats = {}
    failed = 0
    attempted = 0

    def mark_completed(unit, rows):
        completed_units.add(str(unit))
        unit_rows[str(unit)] = int(rows)
        save_checkpoint(checkpoint_path, {
            "version": 1,
            "completed_units": sorted(completed_units),
            "unit_rows": unit_rows,
        })

    if factor_ids:
        # 指定因子：区间一次算完（内置因子支持任意区间）
        for factor_id in factor_ids:
            if factor_id in completed_units:
                continue
            attempted += 1
            try:
                result = engine.calculate_factor(
                    factor_id, ts_codes, start_date, end_date
                )
                if result.empty:
                    per_factor_stats[factor_id] = 0
                    continue
                engine.save_factor_values(result)
                mark_completed(factor_id, len(result))
                per_factor_stats[factor_id] = len(result)
                total_saved += len(result)
            except Exception as e:
                if _is_retryable(e):
                    failed += 1
                logger.error(f"计算因子 {factor_id} 失败: {e}")
                per_factor_stats[factor_id] = f"error: {e}"
    else:
        # 全部因子：calculate_all_factors 按单日截面计算
        dates = ParquetDataReader().get_trade_dates(start_date, end_date)
        if not dates:
            print(f"区间 {start_date} ~ {end_date} 没有交易日数据")
            sys.exit(1)
        for date in dates:
            if str(date) in completed_units:
                continue
            attempted += 1
            try:
                result = engine.calculate_all_factors(date, ts_codes)
            except Exception as e:
                if _is_retryable(e):
                    failed += 1
                logger.error(f"计算 {date} 全部因子失败: {e}")
                continue
            if result.empty:
                per_factor_stats[str(date)] = 0
                continue
            engine.save_factor_values(result)
            mark_completed(date, len(result))
            per_factor_stats[str(date)] = len(result)
            total_saved += len(result)

    print(f"因子计算完成: {start_date} ~ {end_date}, 共写入 {total_saved} 条")
    print(per_factor_stats)

    # 零产出或全部失败必须以非零码退出：否则流水线把作业记成 success，
    # 缺失的因子值要等到回测覆盖率校验才暴露
    if attempted and total_saved == 0:
        print(f"{attempted} 个计算单元全部零产出，判定作业失败")
        sys.exit(1)
    if failed:
        print(f"{failed}/{attempted} 个计算单元失败，保留检查点并交由任务层自动重试")
        sys.exit(1)
    clear_checkpoint(checkpoint_path)


if __name__ == "__main__":
    main()
