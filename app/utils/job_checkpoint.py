"""数据任务使用的轻量 JSON 检查点。

检查点只保存编排进度，不保存行情数据。写入必须先于调用方完成数据落盘，
并通过 os.replace 原子替换，避免进程中断留下半个 JSON 文件。
"""
import json
import hashlib
import os
import tempfile
from pathlib import Path
from typing import Dict, Optional, Union

PathLike = Union[str, Path]


def plan_checkpoint_path(root: PathLike, namespace: str, plan: object) -> Path:
    """按任务名和不可变执行计划生成独立检查点路径。"""
    canonical = json.dumps(plan, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
    safe_name = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in namespace)
    return Path(root) / f"{safe_name}_{digest}.json"


def load_checkpoint(path: PathLike) -> Dict[str, object]:
    """读取检查点；不存在或损坏时返回空状态，让任务安全地幂等重跑。"""
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {}
    return value if isinstance(value, dict) else {}


def save_checkpoint(path: PathLike, payload: Dict[str, object]) -> None:
    """同目录临时文件 + 原子替换，调用方不得在数据落盘前调用。"""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_name: Optional[str] = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=str(target.parent),
            prefix=f"{target.name}.",
            suffix=".tmp",
            delete=False,
        ) as tmp:
            tmp_name = tmp.name
            json.dump(payload, tmp, ensure_ascii=False, sort_keys=True)
            tmp.flush()
            os.fsync(tmp.fileno())
        os.replace(tmp_name, target)
    finally:
        if tmp_name and os.path.exists(tmp_name):
            os.unlink(tmp_name)


def clear_checkpoint(path: PathLike) -> None:
    try:
        Path(path).unlink()
    except FileNotFoundError:
        pass
