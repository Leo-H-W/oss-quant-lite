# 模块：data_jobs

## 职责
行情/财务数据下载入库，覆盖交易日历、股票基础信息、K线、财务报表，禁止 mock 数据进入真实路径。

## 结构性合约（自动，硬门禁）
| 合约 | 测试文件 | Marker |
|---|---|---|
| 任务运行器行为 | tests/data_jobs/test_runner.py | module_data_jobs |
| 任务 schema 校验 | tests/data_jobs/test_schemas.py | module_data_jobs |
| 任务服务逻辑 | tests/data_jobs/test_service.py | module_data_jobs |
| 去重逻辑 | tests/data_jobs/test_service_dedup.py | module_data_jobs |
| 执行模式切换 | tests/data_jobs/test_service_execution_mode.py | module_data_jobs |
| 注册表元数据 | tests/data_jobs/test_registry_metadata.py | module_data_jobs |
| 审计合约 | tests/data_jobs/test_audit_contract.py | module_data_jobs |
| 数据模型 | tests/data_jobs/test_models.py | module_data_jobs |
| 状态存储 | tests/data_jobs/test_state_store.py | module_data_jobs |
| 运行参数 | tests/data_jobs/test_runner_params.py | module_data_jobs |
| 注册表 | tests/data_jobs/test_registry.py | module_data_jobs |
| Celery 任务 | tests/data_jobs/test_celery_tasks.py | module_data_jobs |
| 数据任务 API 合约 | tests/api/test_data_jobs_api.py | module_data_jobs |
| 数据任务 API 错误码合约 | tests/api/test_data_jobs_api_error_codes.py | module_data_jobs |
| 数据任务 API 列表过滤合约 | tests/api/test_data_jobs_api_list_filters.py | module_data_jobs |
| 数据任务 UI 入口合约 | tests/ui/test_data_jobs_ui_contract.py | module_data_jobs |
| 数据任务 UI 进度合约 | tests/ui/test_data_jobs_ui_progress_contract.py | module_data_jobs |
| 活性注册表与孤儿收割合约 | tests/data_jobs/test_liveness_reap.py | module_data_jobs |
| 任务删除合约 | tests/data_jobs/test_delete_run.py | module_data_jobs |
| 数据任务重试 API 合约 | tests/api/test_data_jobs_retry_api.py | module_data_jobs |
| 数据任务删除 API 合约 | tests/api/test_data_jobs_delete_api.py | module_data_jobs |
| 任务进度页重试入口合约 | tests/ui/test_data_jobs_ui_retry_contract.py | module_data_jobs |
| 夜间自动初始化合约 | tests/data_jobs/test_auto_init.py | module_data_jobs |
| 夜间断档感知合约（日频表分区日期 vs 交易日历） | tests/data_jobs/test_auto_init.py | module_data_jobs |
| 任务健康度报告合约（孤儿/失败/心跳/断档/分区完整度五项检查） | tests/data_jobs/test_health.py | module_data_jobs |
| 健康度自动修复合约（收割孤儿 + 断档回补提交） | tests/data_jobs/test_health.py | module_data_jobs |
| 健康度 API 合约（GET /health、POST /health/repair） | tests/api/test_data_jobs_health_api.py | module_data_jobs |
| moneyflow 历史回补驱动合约（日期计划/断点续跑/分片） | tests/data_jobs/test_moneyflow_tdx_backfill.py | module_data_jobs |
| Baostock 断流处理合约（错误码非 0 必须抛出触发重登录） | tests/data_jobs/test_baostock_disconnect_contract.py | module_data_jobs |

### 健康度监控验收标准（2026-10-26 新增）

背景：2026-10-24 晚服务进程中断，夜间 auto-init 静默缺席两个夜晚无人察觉；
daily_basic 出现 631/5572 行的残缺分区却被视为"已有数据"。

- `collect_health_report` 输出五项检查：orphan_runs（假 running）、recent_failures、
  auto_init_heartbeat（巡检心跳）、data_freshness（交易日断档）、partition_completeness
  （最新分区行数 vs daily_history 同日基准，默认阈值 0.8）
- 严重度聚合：orphan/心跳缺失 → critical；失败/断档/残缺 → warning；全部通过 → ok
- `repair_health`：孤儿落 failed（复用 ORPHAN_ERROR_MESSAGE），断档表按
  DAILY_GAP_JOB_CHAIN 顺序提交回补，重复在跑任务礼让不报错
- 夜间 auto-init 结果携带健康快照落盘 auto_init_state.json

## 数值软目标（M1 里程碑 review 时人工确认）
- 无（数据下载模块以结构性合约为主，数值目标由下游模块负责）

## 上游依赖
- 无（数据入口模块）

## 验收状态
- [x] 结构性合约：CI 绿（pytest -m module_data_jobs，2026-09-25：135 passed；2026-10-26 健康度监控新增后：197 passed）
- [ ] 数值软目标：N/A
