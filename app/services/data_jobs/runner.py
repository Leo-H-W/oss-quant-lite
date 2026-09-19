from pathlib import Path
import os
import subprocess
import sys
from typing import Any, Dict, Optional, Tuple


class ScriptRunner:
    """Adapter to execute app/utils scripts in a managed way."""

    def __init__(self, project_root: Optional[Path] = None):
        self.project_root = project_root or Path(__file__).resolve().parents[3]

    @staticmethod
    def _python_executable() -> str:
        """执行任务脚本的 Python 解释器。

        必须用当前解释器（sys.executable）：后端本身运行在项目 venv 里，
        子进程只有继承同一解释器才能保证 pandas/dotenv/tushare 等依赖可用。
        写死 "python" 会按 PATH 解析到系统 Python，页面点「执行任务」直接报
        ModuleNotFoundError。
        """
        return sys.executable

    def validate_script(self, script_path: str) -> Tuple[bool, str]:
        full_path = self.project_root / script_path
        if not full_path.exists() or not full_path.is_file():
            return False, f"script not found: {script_path}"
        return True, "ok"

    def run_script(
        self,
        script_path: str,
        params: Optional[Dict[str, Any]] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
    ) -> subprocess.CompletedProcess:
        """执行数据脚本。

        timeout: 秒。默认取环境变量 DATA_JOB_TIMEOUT（缺省 3600 秒）。
        超时会杀死子进程并返回 returncode=124 的结果，而不是让 worker 永久挂起。
        """
        ok, msg = self.validate_script(script_path)
        if not ok:
            raise FileNotFoundError(msg)

        full_path = self.project_root / script_path
        merged_env = os.environ.copy()
        existing_pythonpath = merged_env.get("PYTHONPATH", "")
        project_root_text = str(self.project_root)
        if existing_pythonpath:
            merged_env["PYTHONPATH"] = f"{project_root_text}{os.pathsep}{existing_pythonpath}"
        else:
            merged_env["PYTHONPATH"] = project_root_text
        if env:
            merged_env.update(env)
        if params:
            start_date = params.get("start_date")
            end_date = params.get("end_date")
            trade_date = params.get("trade_date")
            full_refresh = params.get("full_refresh")
            source_name = params.get("source_name")
            source_mode = params.get("source_mode")
            snapshot_tag = params.get("snapshot_tag")
            if start_date:
                merged_env["DATA_JOB_START_DATE"] = str(start_date)
            if end_date:
                merged_env["DATA_JOB_END_DATE"] = str(end_date)
            if trade_date:
                merged_env["DATA_JOB_TRADE_DATE"] = str(trade_date)
            if full_refresh is not None:
                merged_env["DATA_JOB_FULL_REFRESH"] = str(full_refresh)
            if source_name:
                merged_env["DATA_JOB_SOURCE_NAME"] = str(source_name)
            if source_mode:
                merged_env["DATA_JOB_SOURCE_MODE"] = str(source_mode)
            if snapshot_tag:
                merged_env["DATA_JOB_SNAPSHOT_TAG"] = str(snapshot_tag)

            for key, value in params.items():
                if value is None:
                    continue
                env_key = f"DATA_JOB_PARAM_{str(key).upper()}"
                merged_env[env_key] = str(value)

        if timeout is None:
            try:
                timeout = int(os.environ.get("DATA_JOB_TIMEOUT", 3600))
            except (TypeError, ValueError):
                timeout = 3600

        try:
            return subprocess.run(
                [self._python_executable(), str(full_path)],
                cwd=str(self.project_root),
                env=merged_env,
                capture_output=True,
                text=True,
                # 脚本输出是 UTF-8（含中文日志与彩色转义）；text=True 默认按
                # 系统区域编码（中文 Windows 为 GBK）解码会直接 UnicodeDecodeError，
                # 读线程崩溃导致任务结果被吞。显式指定 UTF-8 并容错替换。
                encoding="utf-8",
                errors="replace",
                check=False,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired as exc:
            # 超时视为任务失败（returncode=124 约定与 shell 一致），让上层
            # 走正常的失败落盘流程，而不是把异常抛给 worker 日志后无人知晓
            timed_out_text = f"script timed out after {timeout}s and was killed"
            return subprocess.CompletedProcess(
                args=[self._python_executable(), str(full_path)],
                returncode=124,
                stdout=(exc.stdout or "") if isinstance(exc.stdout, str) else "",
                stderr=f"{(exc.stderr or '') if isinstance(exc.stderr, str) else ''}\n{timed_out_text}".strip(),
            )
