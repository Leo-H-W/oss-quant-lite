from pathlib import Path

import pytest

# docs/ 已移出版本控制（19cc15b5），本地无此文档时跳过
pytestmark = pytest.mark.skipif(
    not Path("docs/guides/INSTALL_GUIDE.md").exists(),
    reason="docs/guides/INSTALL_GUIDE.md 不在版本控制中",
)


def test_install_guide_avoids_full_version_positioning():
    guide = Path("docs/guides/INSTALL_GUIDE.md").read_text(encoding="utf-8")

    assert "可选增强版本" in guide
    assert "开发调试（按需启用增强能力）" in guide
    assert "部署环境（补齐能力后再使用）" in guide
    assert "完整版本" not in guide
    assert "开发者（完整功能）" not in guide
    assert "生产环境" not in guide


def test_install_guide_avoids_guaranteed_auto_install_wording():
    guide = Path("docs/guides/INSTALL_GUIDE.md").read_text(encoding="utf-8")

    assert "尝试安装核心依赖包" in guide
    assert "自动安装核心依赖包" not in guide
