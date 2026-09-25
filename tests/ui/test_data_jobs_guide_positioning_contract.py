from pathlib import Path
import pytest

pytestmark = [
    pytest.mark.module_data_jobs,
    # docs/ 已移出版本控制（19cc15b5），本地无此文档时跳过
    pytest.mark.skipif(
        not Path("docs/guides/data_jobs_user_guide.md").exists(),
        reason="docs/guides/data_jobs_user_guide.md 不在版本控制中",
    ),
]


def test_data_jobs_guide_uses_current_behavior_wording():
    guide = Path("docs/guides/data_jobs_user_guide.md").read_text(encoding="utf-8")

    assert "支持轮询任务状态与进度展示" in guide
    assert "增量或全量行为以后端当前任务实现为准" in guide
    assert "支持轮询运行中任务状态与进度" not in guide
    assert "默认策略为增量更新" not in guide
