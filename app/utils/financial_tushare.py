"""财务三表 Tushare 备用任务：复用既有 VIP 生产者，顺序执行且失败可见。"""
from pathlib import Path
import subprocess
import sys


_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_SCRIPTS = ("income_statement.py", "balance_sheet.py", "cash_flow.py")


def main() -> int:
    for script in _SCRIPTS:
        path = _PROJECT_ROOT / "app" / "utils" / script
        print(f"[financial_tushare] 执行 {script}", flush=True)
        result = subprocess.run(
            [sys.executable, str(path)],
            cwd=str(_PROJECT_ROOT),
            check=False,
        )
        if result.returncode != 0:
            print(f"[financial_tushare] {script} 失败，退出码 {result.returncode}", flush=True)
            return int(result.returncode or 1)
    return 0


if __name__ == "__main__":
    sys.exit(main())
