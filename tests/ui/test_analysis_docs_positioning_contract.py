from pathlib import Path

import pytest

# docs/ 已移出版本控制（19cc15b5），本地无此文档时跳过
pytestmark = pytest.mark.skipif(
    not Path("docs/analysis/task.md").exists(),
    reason="docs/analysis/task.md 不在版本控制中",
)


def test_analysis_task_doc_avoids_production_ready_claims_in_summary_sections():
    doc = Path("docs/analysis/task.md").read_text(encoding="utf-8")

    assert "当前文档主要反映历史开发记录，不代表当前 master 分支已完整交付或可直接投入生产使用。" in doc
    assert "可投入生产使用" not in doc
    assert "提供完整的量化投资解决方案" not in doc
    assert "已完成功能**: 100%" not in doc
