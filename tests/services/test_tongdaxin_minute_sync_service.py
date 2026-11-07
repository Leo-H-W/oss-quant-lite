from pathlib import Path
from unittest.mock import patch

import pytest

from app.services.minute_parquet_reader import MinuteParquetReader
from app.services.tongdaxin_minute_sync_service import TongdaxinMinuteSyncService
from app.services import tongdaxin_minute_sync_service as sync_module

pytestmark = pytest.mark.module_data_jobs


def test_sync_single_stock_writes_tongdaxin_minute_data_to_parquet(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    service = TongdaxinMinuteSyncService()

    with patch.object(service, "_fetch_minute_payload") as fetch_payload:
        fetch_payload.return_value = [
            {
                "datetime": "2026-06-05 09:35",
                "open": 10.0,
                "high": 10.2,
                "low": 9.9,
                "close": 10.1,
                "vol": 1000,
                "amount": 10100.0,
            }
        ]
        result = service.sync_single_stock_data("sh.600000", "5min", "2026-06-05", "2026-06-05")

    parquet_path = Path(tmp_path) / "stock_minute" / "5min" / "year=2026" / "month=06" / "day=05" / "data.parquet"
    synced = MinuteParquetReader(data_dir=str(tmp_path)).get_data(ts_code="sh.600000", period_type="5min")

    assert result["success"] is True
    assert result["parquet_count"] == 1
    assert parquet_path.is_file()
    assert len(synced) == 1


def test_minute_payload_paginates_until_requested_start_date(monkeypatch):
    monkeypatch.setattr(sync_module, "MINUTE_PAGE_SIZE", 2)
    service = TongdaxinMinuteSyncService()
    pages = {
        0: [
            {"datetime": "2026-06-04 14:55", "close": 10.4},
            {"datetime": "2026-06-05 09:35", "close": 10.5},
        ],
        2: [
            {"datetime": "2026-06-03 14:55", "close": 10.2},
            {"datetime": "2026-06-04 09:35", "close": 10.3},
        ],
        4: [
            {"datetime": "2026-06-02 14:55", "close": 10.0},
            {"datetime": "2026-06-03 09:35", "close": 10.1},
        ],
    }
    calls = []

    class FakeApi:
        def get_security_bars(self, category, market, code, start, count):
            calls.append((category, market, code, start, count))
            return pages.get(start, [])

    service.api = FakeApi()
    payload = service._fetch_minute_payload(
        "600519.SH", "5min", "2026-06-03", "2026-06-05"
    )

    assert [call[3:] for call in calls] == [(0, 2), (2, 2)]
    assert [row["datetime"] for row in payload] == [
        "2026-06-03 14:55", "2026-06-04 09:35",
        "2026-06-04 14:55", "2026-06-05 09:35",
    ]
