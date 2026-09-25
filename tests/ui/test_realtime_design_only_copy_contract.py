from pathlib import Path


def test_readme_marks_realtime_analysis_status_honestly():
    """实时行情分析已从"仅设计"进入开发（监控/指标/信号/风险页面可用），
    README 必须在能力矩阵中如实标注其状态，而不是回退到夸大或过时的说法。"""
    readme = Path("README.md").read_text(encoding="utf-8")

    assert "实时行情分析" in readme
    # 能力矩阵中如实标注（部分实现：页面可用、依赖本地分钟数据同步）
    assert "实时行情分析 | 部分实现" in readme
    # 过时的说法不应再出现
    assert "实时行情分析当前仅做设计，不进入开发范围" not in readme
