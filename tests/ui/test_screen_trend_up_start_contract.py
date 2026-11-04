from pathlib import Path

import pytest

pytestmark = pytest.mark.module_data_jobs


def test_screen_exposes_trend_up_start_dynamic_field():
    html = Path("app/templates/screen.html").read_text(encoding="utf-8")

    assert "'trend_up_start': '趋势启动信号'" in html
