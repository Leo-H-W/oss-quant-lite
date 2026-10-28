"""moneyflow 通达信历史回补驱动合约测试。

覆盖：
- 回补日期计划：区间过滤 + 新→旧排序
- 断点续跑：完整分区跳过、缺失/残缺分区待补
- 分片：互不重叠、并集完整、非法参数快速失败
"""
import pandas as pd
import pytest

from app.utils.moneyflow_tdx_backfill import (
    find_pending_dates,
    list_backfill_dates,
    shard_dates,
)

pytestmark = pytest.mark.module_data_jobs


def _write_partition(root, ymd, rows):
    part = (
        root / "moneyflow/daily"
        / f"year={ymd[:4]}" / f"month={ymd[4:6]}" / f"day={ymd[6:8]}"
    )
    part.mkdir(parents=True)
    pd.DataFrame(
        {"ts_code": [f"{i:06d}.SZ" for i in range(rows)], "net_mf_amount": [0.0] * rows}
    ).to_parquet(part / "data.parquet", index=False)


# ---------------------------------------------------------------------------
# 日期计划
# ---------------------------------------------------------------------------

def test_list_backfill_dates_newest_first_within_range():
    open_days = ["20260105", "20260106", "20260107", "20260108"]
    dates = list_backfill_dates(open_days, "20260106", "20260107")
    assert dates == ["20260107", "20260106"]


def test_list_backfill_dates_ignores_closed_days():
    open_days = ["20260105", "20260107"]  # 0106 非开市日
    dates = list_backfill_dates(open_days, "20260105", "20260108")
    assert dates == ["20260107", "20260105"]


# ---------------------------------------------------------------------------
# 断点续跑
# ---------------------------------------------------------------------------

def test_pending_dates_skip_complete_partitions(tmp_path):
    _write_partition(tmp_path, "20261023", 100)
    _write_partition(tmp_path, "20261022", 10)  # 残缺
    dates = ["20261023", "20261022", "20261021"]  # 01021 缺失
    pending = find_pending_dates(dates, tmp_path, universe_size=100, ratio=0.8)
    assert pending == ["20261022", "20261021"]


def test_pending_dates_treats_broken_partition_as_pending(tmp_path):
    part = tmp_path / "moneyflow/daily/year=2026/month=10/day=23"
    part.mkdir(parents=True)
    (part / "data.parquet").write_bytes(b"not a parquet")
    pending = find_pending_dates(["20261023"], tmp_path, universe_size=100)
    assert pending == ["20261023"]


# ---------------------------------------------------------------------------
# 分片
# ---------------------------------------------------------------------------

def test_shard_dates_disjoint_and_complete():
    dates = [f"202610{d:02d}" for d in range(19, 24)]
    shards = [shard_dates(dates, 4, i) for i in range(4)]
    merged = sorted(d for shard in shards for d in shard)
    assert merged == sorted(dates)
    for i in range(4):
        for j in range(i + 1, 4):
            assert not set(shards[i]) & set(shards[j])


def test_shard_dates_preserves_newest_first_order():
    dates = ["20261023", "20261022", "20261021", "20261020", "20261019"]
    assert shard_dates(dates, 2, 0) == ["20261023", "20261021", "20261019"]
    assert shard_dates(dates, 2, 1) == ["20261022", "20261020"]


def test_shard_dates_rejects_invalid_params():
    with pytest.raises(ValueError):
        shard_dates(["20261023"], 0, 0)
    with pytest.raises(ValueError):
        shard_dates(["20261023"], 2, 2)
