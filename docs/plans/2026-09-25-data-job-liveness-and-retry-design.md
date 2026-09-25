# 数据任务假运行治理：活性校验 + 孤儿收割 + 重试修复

日期：2026-09-25
模块：data_jobs（兼及 ml_factor 训练任务的前端提示、两个前端页面）

## 背景与问题

数据下载任务在 web 进程内 daemon 线程执行（`app/services/data_jobs/service.py`），
状态落 `data_job_runs.parquet`。服务重启后：

- 执行线程随进程消失，但 run 永远停在 `pending/queued/running`（假运行）
- 唯一的清理是惰性的：下次提交时按 `started_at + DATA_JOB_TIMEOUT`（默认 1 小时）
  收割（`reap_stale_runs`），在此之前同参数任务被 `find_active_duplicate` 拒绝重提
- 后端 `POST /api/data-jobs/<id>/retry` 已存在，但 React 页面重试按钮未使用，
  直接重新 submit 且丢失原日期参数
- 模型训练任务纯内存存储，重启后轮询 404，前端默默停止，用户无任何提示

## 方案

照搬回测模块已验证的活性注册表模式（`app/tasks/backtest_tasks.py`），
利用"任务永远是本进程 daemon 线程"这一事实做**精确判活**：

1. **活性注册表**（`app/tasks/data_jobs_tasks.py`）：进程内 `_active_run_ids`，
   `run_data_job` 入口注册、finally 清理；`submit` 在 `thread.start()` 前注册（消除 TOCTOU）
2. **孤儿收割**（`ParquetDataJobStateStore.reap_orphan_runs(is_active)`）：
   表锁内逐候选复查回调，非终态且不在册的 run 标 failed，
   error_message 注明"进程可能中断或重启"，progress_message="已中断，可重试"
3. **触发点**：
   - `submit` 前：保留原超时收割作兜底 + 每进程一次的注册表收割（`reap_orphans_once`）
   - `get_run`/`list_runs` 读时：先做一次收割，再对非终态 run 逐个核对注册表，
     不在册当场落 failed——重启后打开页面即见真实状态
4. **retry 语义**：活跃 run 重试返回 400；not found 返回 404；
   重试成功后删除原记录（新旧记录语义等价，列表不堆积）
5. **前端**：React 与旧 Jinja 页的重试按钮都改调 `/api/data-jobs/<id>/retry`（保留原参数）；
   训练轮询 404 时提示"训练任务已随服务重启丢失，请重新训练"

### 并发安全

- 判活回调在表锁内复查，消除"快照后才提交并在册"的窗口
- 收割标志在扫描抛异常时不置位，下次调用重试；标 failed 幂等无害
- 非 inline 模式 `delay()` 是同步直调，注册/清理同样成立

## 验收标准

### 结构性合约（硬门禁，marker=module_data_jobs）

| 合约 | 测试文件 |
|---|---|
| 活性注册表 + 孤儿收割 + 读时 reconcile + submit 注册顺序 | tests/data_jobs/test_liveness_reap.py |
| retry 保留原参数 / 活跃 400 / 缺失 404 | tests/api/test_data_jobs_retry_api.py |
| 新旧页面重试入口走 retry API | tests/ui/test_data_jobs_ui_retry_contract.py |

### 数值软目标

- 无（本特性为状态正确性治理，无数值指标）

### 验收命令

```bash
pytest -m module_data_jobs -q
pytest -q
```

### 端到端手动验证

1. `./service.sh start`，提交一个下载任务
2. 运行中 `./service.sh restart`
3. 打开数据管理页：该 run 显示 failed / "已中断，可重试"，错误说明含重启原因
4. 点重试：新 run 带原参数提交并正常推进
