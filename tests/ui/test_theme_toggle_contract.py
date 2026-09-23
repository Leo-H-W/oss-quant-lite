"""主题切换合约：base.html 含切换按钮与持久化脚本，CSS 含亮色变量覆盖。

说明：这是小型 UI 增强，不属于既有 acceptance 模块，不打模块 marker，
随 `pytest -q` 全量回归运行。
"""

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent


def test_base_template_has_theme_toggle(app):
    client = app.test_client()
    html = client.get("/").get_data(as_text=True)

    assert 'id="theme-toggle"' in html
    assert "toggleTheme" in html
    assert "localStorage" in html and "theme" in html
    # 防闪烁：样式加载前应用 data-theme
    assert "document.documentElement.setAttribute('data-theme'" in html


def test_light_theme_variables_defined():
    css = (ROOT / "app/static/css/financial-theme.css").read_text(encoding="utf-8")

    assert '[data-theme="light"]' in css
    assert "--obs-bg" in css
    # 亮色模式下的硬编码深色组件覆盖
    assert '[data-theme="light"] .navbar-financial' in css
