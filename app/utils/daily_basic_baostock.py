"""daily_basic 的 Baostock 生产者：绕开 tushare daily_basic 60 次/小时限频。

query_history_k_data_plus 按 股票×区间 拉取，覆盖 daily_basic 核心字段：
close / turnover_rate(turn) / pe_ttm(peTTM) / pb(pbMRQ) / ps_ttm(psTTM)。
tushare 独有列（turnover_rate_f / volume_ratio / pe / ps / dv_* / 股本与市值）
保持缺失——股本市值无可靠历史来源，不拿静态股本近似充数。

默认区间：最新本地分区次日 → 今天；DATA_JOB_START_DATE/END_DATE 可显式指定。
每 CHUNK_STOCKS 只标的落一次盘：save_to_parquet 整分区覆盖，
落盘前必须与已有分区按 ts_code 合并，否则前面的标的数据被冲掉。
"""
import os
import signal
import sys
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import pandas as pd

# 兼容直接运行（python app/utils/daily_basic_baostock.py）
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from app.utils.job_checkpoint import (  # noqa: E402
    clear_checkpoint,
    load_checkpoint,
    save_checkpoint,
)
from app.utils.parquet_job_helpers import (  # noqa: E402
    existing_partition_dates,
    normalize_ymd,
)
from app.utils.parquet_writer import save_to_parquet  # noqa: E402

REL_TABLE = "daily_basic/daily"
CHUNK_STOCKS = 100
REQUEST_INTERVAL = 0.05  # baostock 限频宽松，礼节性间隔
FETCH_TIMEOUT = 30  # baostock socket 无超时，断流会永久阻塞，须看门狗
CHECKPOINT_ROOT = _PROJECT_ROOT / "data" / "data_job_state" / "checkpoints"


class FetchTimeout(Exception):
    pass


@contextmanager
def _watchdog(seconds: int):
    """SIGALRM 看门狗（脚本单线程，主线程内安全）。"""
    def handler(signum, frame):
        raise FetchTimeout(f"baostock 请求超过 {seconds}s 无响应")

    old = signal.signal(signal.SIGALRM, handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old)

# baostock 错误码处理策略：非零默认抛出（2026-10-26 断流事故教训——宁可任务
# 可见地失败，不可静默空手而归）；仅"标的维度确定性空结果"的错误码按空数据
# 处理：10004011 股票代码未标识sh或sz（北交所 bj.* 不受 baostock 支持，
# ~351 只标的每轮必报，抛异常会触发逐股重登录风暴并把整轮任务误判失败，
# 2026-10-28 夜间 daily_basic 即因此链路中断）
EMPTY_RESULT_ERROR_CODES = frozenset({"10004011"})


def raise_if_disconnected(rs, ts_code: str) -> bool:
    """连接类错误码抛 ConnectionError；标的类空结果返回 True（调用方按空处理）。"""
    if rs.error_code == "0":
        return False
    if rs.error_code in EMPTY_RESULT_ERROR_CODES:
        return True
    # 连接断流时 baostock 不抛异常，只回填错误码并返回空；
    # 必须显式抛出让上层重登录换连接，否则死连接会被复用，
    # 后续每只标的都静默空手而归（2026-10-26 事故）
    raise ConnectionError(
        f"baostock 查询失败 {ts_code}: {rs.error_code} {rs.error_msg}"
    )


KLINE_FIELDS = "date,close,turn,peTTM,pbMRQ,psTTM"
COLUMN_MAP = {
    "date": "trade_date",
    "close": "close",
    "turn": "turnover_rate",
    "peTTM": "pe_ttm",
    "pbMRQ": "pb",
    "psTTM": "ps_ttm",
}


def ts_code_to_bs(ts_code: str) -> str:
    """600000.SH → sh.600000；000001.SZ → sz.000001；北交所 bj。"""
    code, _, exchange = (ts_code or "").partition(".")
    return f"{exchange.lower()}.{code}" if code and exchange else ""


def resolve_date_range() -> tuple:
    """显式 env 优先；缺省 = 最新分区次日 → 今天。"""
    start = normalize_ymd(os.getenv("DATA_JOB_START_DATE"))
    end = normalize_ymd(os.getenv("DATA_JOB_END_DATE")) or datetime.now().strftime("%Y%m%d")
    if not start:
        have = existing_partition_dates(REL_TABLE)
        if have:
            latest = max(have)
            start = (pd.Timestamp(latest) + pd.Timedelta(days=1)).strftime("%Y%m%d")
        else:
            start = end  # 无本地数据时首跑只拉当天，历史回补请显式传区间
    return start, end


def fetch_stock(bs_module, ts_code: str, start: str, end: str) -> pd.DataFrame:
    """拉单只股票区间内的估值/换手日频数据，返回 daily_basic 口径 DataFrame。"""
    bs_code = ts_code_to_bs(ts_code)
    if not bs_code:
        return pd.DataFrame()
    rs = bs_module.query_history_k_data_plus(
        bs_code,
        KLINE_FIELDS,
        start_date=f"{start[:4]}-{start[4:6]}-{start[6:8]}",
        end_date=f"{end[:4]}-{end[4:6]}-{end[6:8]}",
        frequency="d",
        adjustflag="3",  # 不复权：与 tushare daily_basic 口径一致
    )
    if raise_if_disconnected(rs, ts_code):
        return pd.DataFrame()
    rows = []
    while rs.error_code == "0" and rs.next():
        rows.append(rs.get_row_data())
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=rs.fields).rename(columns=COLUMN_MAP)
    df.insert(0, "ts_code", ts_code)
    for col in ("close", "turnover_rate", "pe_ttm", "pb", "ps_ttm"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def merge_save(chunk: pd.DataFrame) -> int:
    """chunk 按交易日分组合并落盘（读旧分区 → concat 去重 → 整分区覆写）。"""
    if chunk.empty:
        return 0
    from app.utils.parquet_writer import _data_root

    data_dir = _data_root(None)
    chunk = chunk.copy()
    chunk["trade_date"] = pd.to_datetime(chunk["trade_date"]).dt.strftime("%Y-%m-%d")
    total = 0
    for date_value, group in chunk.groupby("trade_date", sort=True):
        clean = date_value.replace("-", "")
        part_path = os.path.join(
            data_dir, REL_TABLE,
            f"year={clean[:4]}", f"month={clean[4:6]}", f"day={clean[6:8]}",
            "data.parquet",
        )
        if os.path.isfile(part_path):
            try:
                existing = pd.read_parquet(part_path)
                group = pd.concat([existing, group], ignore_index=True)
            except Exception as exc:  # noqa: BLE001 - 坏分区不阻断，直接以新数据覆盖
                print(f"[daily_basic_baostock] 读取已有分区失败 {part_path}: {exc}")
        group = group.drop_duplicates(subset=["ts_code"], keep="last")
        total += save_to_parquet(group, date_value, REL_TABLE)
    return total


def main() -> int:
    import baostock as bs

    start, end = resolve_date_range()
    if start > end:
        print(f"[daily_basic_baostock] 无需拉取：start={start} > end={end}")
        return 0

    basic = pd.read_parquet(_PROJECT_ROOT / "data" / "stock_basic.parquet")
    ts_codes = sorted(basic["ts_code"].dropna().astype(str).unique().tolist())
    print(f"[daily_basic_baostock] 区间 {start}~{end}，标的 {len(ts_codes)} 只")

    lg = bs.login()
    if lg.error_code != "0":
        print(f"[daily_basic_baostock] baostock 登录失败: {lg.error_msg}")
        return 1

    checkpoint_path = CHECKPOINT_ROOT / f"daily_basic_baostock_{start}_{end}.json"
    checkpoint = load_checkpoint(checkpoint_path)
    if checkpoint.get("start") == start and checkpoint.get("end") == end:
        completed = {
            str(code) for code in checkpoint.get("completed_ts_codes", [])
        }
    else:
        completed = set()
    if completed:
        print(
            f"[daily_basic_baostock] 从检查点续跑：已完成 {len(completed)}/{len(ts_codes)} 只",
            flush=True,
        )

    total_saved = 0
    failed = []
    buffer = []
    pending_codes = []

    def relogin():
        try:
            bs.logout()
        except Exception:  # noqa: BLE001
            pass
        lg2 = bs.login()
        if lg2.error_code != "0":
            raise RuntimeError(f"baostock 重登录失败: {lg2.error_msg}")

    def flush():
        """先落行情分区，再原子推进检查点；失败股票不进入 pending_codes。"""
        nonlocal total_saved
        if not pending_codes:
            return
        if buffer:
            total_saved += merge_save(pd.concat(buffer, ignore_index=True))
        completed.update(pending_codes)
        save_checkpoint(checkpoint_path, {
            "version": 1,
            "start": start,
            "end": end,
            "completed_ts_codes": sorted(completed),
        })
        buffer.clear()
        pending_codes.clear()

    try:
        for idx, ts_code in enumerate(ts_codes, 1):
            if ts_code in completed:
                continue
            try:
                with _watchdog(FETCH_TIMEOUT):
                    df = fetch_stock(bs, ts_code, start, end)
                pending_codes.append(ts_code)
                if not df.empty:
                    buffer.append(df)
            except Exception as exc:  # noqa: BLE001 - 单股失败不阻断全市场
                failed.append(ts_code)
                print(f"[daily_basic_baostock] {ts_code} 拉取失败: {exc}", flush=True)
                # socket 断流后连接不可信，强制重建再续跑
                try:
                    relogin()
                except Exception as exc2:  # noqa: BLE001
                    print(f"[daily_basic_baostock] 重登录失败，中断: {exc2}", flush=True)
                    raise
            time.sleep(REQUEST_INTERVAL)

            if len(pending_codes) >= CHUNK_STOCKS:
                flush()
                print(f"[daily_basic_baostock] 进度 {idx}/{len(ts_codes)}，累计写入 {total_saved}", flush=True)
    finally:
        flush()
        bs.logout()

    print(f"[daily_basic_baostock] 完成：total_upsert={total_saved}，失败 {len(failed)} 只")
    if failed:
        print(f"[daily_basic_baostock] 失败标的: {failed[:20]}{'...' if len(failed) > 20 else ''}")
        return 1
    clear_checkpoint(checkpoint_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
