from pathlib import Path

import pytest


def _requires_doc(path: str):
    # docs/ 已移出版本控制（19cc15b5），本地无此文档时跳过
    return pytest.mark.skipif(not Path(path).exists(), reason=f"{path} 不在版本控制中")


@_requires_doc("docs/guides/多因子模型系统功能列表.md")
def test_feature_list_avoids_full_report_backend_claim():
    guide = Path("docs/guides/多因子模型系统功能列表.md").read_text(encoding="utf-8")

    assert "报告列表与生成功能入口" in guide
    assert "完整报告管理后台" not in guide


@_requires_doc("docs/guides/多因子模型系统完整指南.md")
def test_full_guide_avoids_full_report_backend_claim():
    guide = Path("docs/guides/多因子模型系统完整指南.md").read_text(encoding="utf-8")

    assert "报告列表与生成功能入口" in guide
    assert "完整报告管理后台" not in guide
