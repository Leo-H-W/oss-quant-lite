"""腾讯原始日 K 备用生产者：仅在主数据源不可用时补齐日线核心字段。

公开接口按单股票、日期区间返回不复权 OHLCV。腾讯历史 K 线没有成交额，
本任务不把缺失成交额伪造成 0；合并已有分区时也不会用 NaN 覆盖其他源的字段。
"""
import json
import os
from pathlib import Path
import sys
import time
from urllib.parse import quote
from urllib.request import Request, urlopen

import pandas as pd

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from app.utils.job_checkpoint import clear_checkpoint, load_checkpoint, plan_checkpoint_path, save_checkpoint  # noqa: E402
from app.utils.parquet_job_helpers import resolve_trade_dates_with_gap_fill  # noqa: E402
from app.utils.parquet_writer import save_to_parquet  # noqa: E402

REL_TABLE = "daily_history/daily"
API_ROOT = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param="
CHUNK_STOCKS = 100
REQUEST_INTERVAL = 0.05
REQUEST_TIMEOUT = 20
DATE_CHUNK_DAYS = 365
OUTPUT_COLS = [
    "ts_code", "trade_date", "open", "high", "low", "close", "pre_close",
    "change", "pct_change", "vol", "amount",
]


def ts_code_to_tencent(ts_code: str) -> str:
    code, _, exchange = str(ts_code).partition(".")
    if exchange == "SH":
        return f"sh{code}"
    if exchange == "SZ":
        return f"sz{code}"
    return ""


def _empty_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=OUTPUT_COLS)


def parse_kline_rows(ts_code: str, payload: dict, target_dates: set) -> pd.DataFrame:
    """腾讯原始日 K → 标准日线字段；目标日期之外的预热/分页数据丢弃。"""
    if int(payload.get("code", 0)) != 0:
        raise ConnectionError(f"腾讯日 K 返回错误: {payload.get('msg') or payload.get('code')}")
    symbol = ts_code_to_tencent(ts_code)
    data = (payload.get("data") or {}).get(symbol) or {}
    rows = data.get("day") or []
    wanted = {str(value).replace("-", "") for value in target_dates}
    records = []
    for row in rows:
        if len(row) < 6:
            continue
        trade_date = str(row[0])
        if trade_date.replace("-", "") not in wanted:
            continue
        records.append({
            "ts_code": ts_code,
            "trade_date": trade_date,
            "open": row[1], "close": row[2], "high": row[3], "low": row[4],
            "vol": row[5],
        })
    if not records:
        return _empty_frame()
    frame = pd.DataFrame(records).sort_values("trade_date")
    for column in ("open", "high", "low", "close", "vol"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["pre_close"] = frame["close"].shift(1)
    frame["change"] = frame["close"] - frame["pre_close"]
    frame["pct_change"] = frame["change"] / frame["pre_close"] * 100.0
    frame["amount"] = float("nan")
    return frame[OUTPUT_COLS].reset_index(drop=True)


def _date_chunks(start: str, end: str):
    current = pd.Timestamp(start)
    final = pd.Timestamp(end)
    while current <= final:
        chunk_end = min(current + pd.Timedelta(days=DATE_CHUNK_DAYS - 1), final)
        yield current.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")
        current = chunk_end + pd.Timedelta(days=1)


def _fetch_payload(symbol: str, start: str, end: str) -> dict:
    # 末尾逗号表示不复权；省略时腾讯可能只返回 version 而不返回 day。
    param = f"{symbol},day,{start},{end},640,"
    url = API_ROOT + quote(param, safe=",")
    request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
        return json.loads(response.read().decode("utf-8"))


def fetch_stock(ts_code: str, start: str, end: str, target_dates: set) -> pd.DataFrame:
    symbol = ts_code_to_tencent(ts_code)
    if not symbol:
        return _empty_frame()
    frames = []
    for chunk_start, chunk_end in _date_chunks(start, end):
        payload = _fetch_payload(symbol, chunk_start, chunk_end)
        frame = parse_kline_rows(ts_code, payload, target_dates)
        if not frame.empty:
            frames.append(frame)
        time.sleep(REQUEST_INTERVAL)
    if not frames:
        return _empty_frame()
    frame = pd.concat(frames, ignore_index=True)
    frame = frame.drop_duplicates(subset=["trade_date"], keep="last").sort_values("trade_date")
    # 跨分页重新计算昨收/涨跌，避免每个分页首日被人为置空。
    frame["pre_close"] = frame["close"].shift(1)
    frame["change"] = frame["close"] - frame["pre_close"]
    frame["pct_change"] = frame["change"] / frame["pre_close"] * 100.0
    return frame[OUTPUT_COLS].reset_index(drop=True)


def merge_save(chunk: pd.DataFrame, data_dir=None) -> int:
    """按 ts_code 合并；新值中的 NaN 不覆盖其他源已经落盘的非空字段。"""
    if chunk is None or chunk.empty:
        return 0
    root = Path(data_dir or os.getenv("DATA_DIR", str(_PROJECT_ROOT / "data")))
    frame = chunk.copy()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.strftime("%Y-%m-%d")
    total = 0
    for trade_date, group in frame.groupby("trade_date", sort=True):
        clean = trade_date.replace("-", "")
        path = root / REL_TABLE / f"year={clean[:4]}" / f"month={clean[4:6]}" / f"day={clean[6:8]}" / "data.parquet"
        if path.is_file():
            try:
                existing = pd.read_parquet(path)
                merged = existing.set_index("ts_code")
                update = group.set_index("ts_code")
                for column in update.columns:
                    if column not in merged.columns:
                        merged[column] = pd.NA
                merged.update(update)  # 默认不用 NaN 覆盖旧值
                new_codes = update.index.difference(merged.index)
                group = pd.concat(
                    [merged.reset_index(), update.loc[new_codes].reset_index()],
                    ignore_index=True,
                )
            except Exception as exc:  # noqa: BLE001 - 坏文件按新数据重建并明确输出
                print(f"[daily_history_tencent] 读取已有分区失败 {path}: {exc}", flush=True)
        group = group.drop_duplicates(subset=["ts_code"], keep="last")
        total += save_to_parquet(group, trade_date, REL_TABLE, data_dir=str(root))
    return total


def main() -> int:
    trade_dates, _ = resolve_trade_dates_with_gap_fill(REL_TABLE)
    targets = sorted({str(value).replace("-", "") for value in trade_dates})
    if not targets:
        print("[daily_history_tencent] 无待补交易日")
        return 0
    start, end = targets[0], targets[-1]
    root = Path(os.getenv("DATA_DIR", str(_PROJECT_ROOT / "data")))
    basic = pd.read_parquet(root / "stock_basic.parquet")
    ts_codes = list(dict.fromkeys(basic["ts_code"].dropna().astype(str)))
    ts_codes = [code for code in ts_codes if ts_code_to_tencent(code)]
    checkpoint_path = plan_checkpoint_path(
        root / "data_job_state" / "checkpoints", "daily_history_tencent",
        {"targets": targets, "ts_codes": ts_codes},
    )
    checkpoint = load_checkpoint(checkpoint_path)
    completed = set(checkpoint.get("completed_ts_codes", []))
    if completed:
        print(f"[daily_history_tencent] 从检查点续跑：{len(completed)}/{len(ts_codes)}")

    target_set = set(targets)
    completed_this_run = 0
    total_saved = 0
    failed = []
    buffer = []
    pending = []

    def flush():
        nonlocal total_saved
        if not pending:
            return
        if buffer:
            total_saved += merge_save(pd.concat(buffer, ignore_index=True), data_dir=str(root))
        completed.update(pending)
        save_checkpoint(checkpoint_path, {
            "version": 1, "targets": targets,
            "completed_ts_codes": sorted(completed),
        })
        buffer.clear()
        pending.clear()

    try:
        for index, ts_code in enumerate(ts_codes, 1):
            if ts_code in completed:
                continue
            try:
                frame = fetch_stock(ts_code, start, end, target_set)
                if not frame.empty:
                    buffer.append(frame)
                pending.append(ts_code)
                completed_this_run += 1
            except Exception as exc:  # noqa: BLE001 - 单股失败留给任务层重试
                failed.append(ts_code)
                print(f"[daily_history_tencent] {ts_code} 拉取失败: {exc}", flush=True)
            if len(pending) >= CHUNK_STOCKS:
                flush()
                print(f"[daily_history_tencent] 进度 {index}/{len(ts_codes)}", flush=True)
    finally:
        flush()

    print(
        f"[daily_history_tencent] 完成：区间 {start}~{end}，写入 {total_saved} 行，"
        f"失败 {len(failed)} 只",
        flush=True,
    )
    if failed:
        return 1
    if completed_this_run and total_saved == 0:
        # 全市场空响应通常是接口/参数异常，不能将空结果当完整数据。
        clear_checkpoint(checkpoint_path)
        print("[daily_history_tencent] 全市场零产出，判定失败", flush=True)
        return 1
    clear_checkpoint(checkpoint_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
