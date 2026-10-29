"""ParquetDataReader 内存防护合约：列裁剪 + ts_code 谓词下推。

背景：_read_table 旧实现全列读入、concat 全市场后才按股票过滤，
单股/小组合查询的内存与全市场相同（回测/组合优化/ML 训练因此被放大到
数百 MB）。修复后必须在读取每个分区时就完成列裁剪和股票过滤。
"""
from pathlib import Path

import pandas as pd
import pytest

import app.services.data_reader as data_reader_module
from app.services.data_reader import ParquetDataReader

pytestmark = pytest.mark.module_data_jobs


def _write_daily_partition(root: Path, day: str, frame: pd.DataFrame) -> None:
    partition_dir = (
        root / "daily_history" / "daily"
        / f"year={day[:4]}" / f"month={day[4:6]}" / f"day={day[6:]}"
    )
    partition_dir.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(partition_dir / "data.parquet", index=False)


@pytest.fixture
def read_parquet_spy(monkeypatch):
    """记录每次 pd.read_parquet 的 columns 参数，以及 concat 收到的各分区帧行数。

    concat 收到的帧行数是关键观测点：谓词下推生效时，每个分区帧在 concat
    之前就已按 ts_code 过滤，行数 ≤ 目标股票数。
    """
    calls = []
    original_read = pd.read_parquet
    original_concat = pd.concat

    def read_spy(path, *args, **kwargs):
        df = original_read(path, *args, **kwargs)
        calls.append({"columns": kwargs.get("columns")})
        return df

    def concat_spy(objs, *args, **kwargs):
        frames = list(objs)
        calls.append({"concat_frame_rows": [len(f) for f in frames]})
        return original_concat(frames, *args, **kwargs)

    monkeypatch.setattr(data_reader_module.pd, "read_parquet", read_spy)
    monkeypatch.setattr(data_reader_module.pd, "concat", concat_spy)
    return calls


def _concat_frame_rows(calls):
    """展开所有 concat 调用收到的分区帧行数。"""
    rows = []
    for call in calls:
        rows.extend(call.get("concat_frame_rows", []))
    return rows


def test_read_table_pushes_ts_code_filter_down_to_partition_reads(tmp_path, read_parquet_spy):
    """按股票过滤必须发生在分区读取阶段：每个分区返回的帧只含目标股票。"""
    data_dir = tmp_path / "data"
    for day in ("20240102", "20240103"):
        _write_daily_partition(
            data_dir,
            day,
            pd.DataFrame(
                [
                    {"ts_code": "000001.SZ", "trade_date": day, "close": 10.0},
                    {"ts_code": "000002.SZ", "trade_date": day, "close": 20.0},
                    {"ts_code": "600000.SH", "trade_date": day, "close": 30.0},
                ]
            ),
        )

    reader = ParquetDataReader(data_dir=str(data_dir))
    frame = reader.get_daily(ts_codes=["000001.SZ"])

    assert frame["ts_code"].unique().tolist() == ["000001.SZ"]
    assert len(frame) == 2
    # concat 收到的每个分区帧行数 ≤ 目标股票数，证明过滤在 concat 之前完成
    frame_rows = _concat_frame_rows(read_parquet_spy)
    assert frame_rows, "未发生任何分区 concat"
    assert all(rows <= 1 for rows in frame_rows)


def test_read_table_prunes_columns_at_read_time(tmp_path, read_parquet_spy):
    """读分区时只请求标准列，parquet 里混入的额外列不得被读进内存。"""
    data_dir = tmp_path / "data"
    _write_daily_partition(
        data_dir,
        "20240102",
        pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20240102",
                    "open": 9.9,
                    "high": 10.5,
                    "low": 9.8,
                    "close": 10.0,
                    "pre_close": 9.9,
                    "change": 0.1,
                    "pct_chg": 1.0,
                    "vol": 1000.0,
                    "amount": 10000.0,
                    "junk_blob": "x" * 1000,  # 非标准列：不得被读取
                }
            ]
        ),
    )

    reader = ParquetDataReader(data_dir=str(data_dir))
    frame = reader.get_daily(ts_codes=["000001.SZ"])

    assert "junk_blob" not in frame.columns
    assert frame.iloc[0]["close"] == 10.0
    read_calls = [c for c in read_parquet_spy if "columns" in c]
    assert read_calls, "未发生任何分区读取"
    for call in read_calls:
        assert call["columns"] is not None, "daily 表必须在读分区时做列裁剪"
        assert "junk_blob" not in call["columns"]


def test_read_table_pruning_tolerates_partitions_with_missing_columns(tmp_path):
    """老分区缺部分标准列时，按交集读取，不得报错。"""
    data_dir = tmp_path / "data"
    _write_daily_partition(
        data_dir,
        "20240102",
        pd.DataFrame([{"ts_code": "000001.SZ", "trade_date": "20240102", "close": 10.0}]),
    )

    reader = ParquetDataReader(data_dir=str(data_dir))
    frame = reader.get_daily(ts_codes=["000001.SZ"])

    assert len(frame) == 1
    assert frame.iloc[0]["close"] == 10.0


def test_read_table_without_standard_columns_still_pushes_down_ts_code(tmp_path, read_parquet_spy):
    """moneyflow 等无标准列定义的表：保留全列，但股票过滤仍须下推到分区级。"""
    partition_dir = (
        tmp_path / "moneyflow" / "daily" / "year=2024" / "month=01" / "day=02"
    )
    partition_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "trade_date": "20240102", "buy_lg_vol": 1.0},
            {"ts_code": "000002.SZ", "trade_date": "20240102", "buy_lg_vol": 2.0},
        ]
    ).to_parquet(partition_dir / "data.parquet", index=False)

    reader = ParquetDataReader(data_dir=str(tmp_path))
    frame = reader.get_moneyflow(ts_codes=["000001.SZ"])

    assert frame["ts_code"].unique().tolist() == ["000001.SZ"]
    assert "buy_lg_vol" in frame.columns  # 无标准列定义：全列保留
    assert all(rows <= 1 for rows in _concat_frame_rows(read_parquet_spy))
