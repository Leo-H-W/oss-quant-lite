"""data-gather.py 命令行合约：状态、增量更新与帮助。"""
import importlib.util
from pathlib import Path

import pandas as pd
import pytest

pytestmark = pytest.mark.module_data_jobs
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _load_cli():
    spec = importlib.util.spec_from_file_location("data_gather", PROJECT_ROOT / "data-gather.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _partition(root, rel_table, ymd):
    part = root / rel_table / f"year={ymd[:4]}" / f"month={ymd[4:6]}" / f"day={ymd[6:8]}"
    part.mkdir(parents=True)
    pd.DataFrame({"trade_date": [ymd]}).to_parquet(part / "data.parquet")


def test_help_lists_status_update_and_help(capsys):
    cli = _load_cli()

    assert cli.main(["help"]) == 0
    output = capsys.readouterr().out
    assert "status" in output
    assert "update" in output
    assert "help" in output


def test_status_reports_partition_date_ranges(monkeypatch, tmp_path, capsys):
    _partition(tmp_path, "daily_history/daily", "20150105")
    _partition(tmp_path, "daily_history/daily", "20261030")
    _partition(tmp_path, "daily_basic/daily", "20200102")
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    cli = _load_cli()

    assert cli.main(["status"]) == 0
    output = capsys.readouterr().out
    assert "日线行情" in output
    assert "20150105-20261030" in output
    assert "每日指标" in output
    assert "20200102-20200102" in output
    assert "资金流向" in output
    assert "暂无本地数据" in output


def test_update_uses_resumable_auto_init_pipeline(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    cli = _load_cli()
    calls = []

    def fake_run(service, inspect_fn, **kwargs):
        calls.append((service, inspect_fn(), kwargs))
        return {"status": "success", "completed": ["daily_history_by_date"]}

    monkeypatch.setattr(cli, "run_auto_init", fake_run)
    service = object()

    assert cli.main(["update"], service=service) == 0
    assert calls[0][0] is service
    assert calls[0][1][0] is True
    assert calls[0][2]["run_date"]
    assert str(calls[0][2]["checkpoint_path"]).endswith("data_gather_progress.json")
    assert "daily_history_by_date" in capsys.readouterr().out


def _write_runs(root):
    state = root / "data_job_state"
    state.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([
        {"id": 46, "job_type": "daily_basic_baostock", "status": "running",
         "progress_message": "任务开始执行", "params_json": {"start_date": "2019-01-01"}},
        {"id": 45, "job_type": "daily_basic_baostock", "status": "success",
         "progress_message": "任务执行完成", "params_json": {}},
    ]).to_parquet(state / "data_job_runs.parquet")


def test_status_lists_active_data_sync_runs(monkeypatch, tmp_path, capsys):
    _write_runs(tmp_path)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    cli = _load_cli()

    assert cli.main(["status"]) == 0
    output = capsys.readouterr().out
    assert "正在运行的数据同步任务" in output
    assert "run_id=46" in output
    assert "daily_basic_baostock" in output
    assert "run_id=45" not in output


def test_update_active_run_decline_keeps_existing_work(monkeypatch, tmp_path, capsys):
    _write_runs(tmp_path)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    cli = _load_cli()
    monkeypatch.setattr(cli, "run_auto_init", lambda *_a, **_kw: pytest.fail("不应重复提交"))

    assert cli.main(["update"], service=object(), input_fn=lambda _: "n") == 0
    assert "保持现有任务" in capsys.readouterr().out


def test_update_active_run_confirm_cancels_before_restart(monkeypatch, tmp_path):
    _write_runs(tmp_path)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    cli = _load_cli()
    cancelled = []
    monkeypatch.setattr(cli, "cancel_active_runs", lambda _root, runs: cancelled.extend(runs))
    monkeypatch.setattr(cli, "run_auto_init", lambda *_a, **_kw: {"status": "success", "completed": []})

    assert cli.main(["update"], service=object(), input_fn=lambda _: "y") == 0
    assert [run["id"] for run in cancelled] == [46]


def test_update_positional_retry_override(monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.delenv("DATA_JOB_MAX_RETRIES", raising=False)
    cli = _load_cli()
    seen = []

    def fake_run(*_args, **_kwargs):
        seen.append(cli.os.environ.get("DATA_JOB_MAX_RETRIES"))
        return {"status": "success", "completed": []}

    monkeypatch.setattr(cli, "run_auto_init", fake_run)
    assert cli.main(["update", "100"], service=object()) == 0
    assert seen == ["100"]
    assert cli.os.environ.get("DATA_JOB_MAX_RETRIES") is None

    assert cli.main(["update"], service=object()) == 0
    assert seen == ["100", None]


def test_update_rejects_invalid_retry_override(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    cli = _load_cli()
    monkeypatch.setattr(cli, "run_auto_init", lambda *_a, **_kw: pytest.fail("不应提交"))

    assert cli.main(["update", "-1"], service=object()) == 2
    assert cli.main(["update", "abc"], service=object()) == 2
    assert "非负整数" in capsys.readouterr().err
