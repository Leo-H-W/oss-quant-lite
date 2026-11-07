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
| 夜间自动初始化与分类例行刷新合约（股票清单/财务三表/宽表/因子值） | tests/data_jobs/test_auto_init.py | module_data_jobs |
| 夜间断档感知合约（日频表分区日期 vs 交易日历） | tests/data_jobs/test_auto_init.py | module_data_jobs |
| 任务健康度报告合约（孤儿/失败/心跳/断档/分区完整度五项检查） | tests/data_jobs/test_health.py | module_data_jobs |
| 健康度自动修复合约（收割孤儿 + 断档回补提交） | tests/data_jobs/test_health.py | module_data_jobs |
| 健康度 API 合约（GET /health、POST /health/repair） | tests/api/test_data_jobs_health_api.py | module_data_jobs |
| moneyflow 历史回补驱动合约（日期计划/断点续跑/分片） | tests/data_jobs/test_moneyflow_tdx_backfill.py | module_data_jobs |
| Baostock 断流处理合约（连接类错误码抛出触发重登录；10004011 北交所不支持按空数据处理） | tests/data_jobs/test_baostock_disconnect_contract.py | module_data_jobs |
| daily_basic 股票级检查点 | tests/data_jobs/test_daily_basic_checkpoint.py | module_data_jobs |
| 按日/财务/复权/资金流回补检查点 | tests/data_jobs/test_backfill_resume_contracts.py | module_data_jobs |
| 扶摇与衍生计算回补检查点 | tests/data_jobs/test_remaining_resume_contracts.py | module_data_jobs |
| 隐藏/独立回补与快照失败合约 | tests/data_jobs/test_additional_resume_contracts.py | module_data_jobs |
| data-gather CLI 状态、更新、活动任务与重试覆盖 | tests/data_jobs/test_data_gather_cli.py | module_data_jobs |
| 夜间历史缺口、复扫验证、检查点与数据源切换 | tests/data_jobs/test_nightly_completeness.py | module_data_jobs |
| 腾讯日线备用源解析、合并非空字段与断点 | tests/data_jobs/test_tencent_daily.py | module_data_jobs |
| 财务历史报告期、最新期完整度与备用源（含东方财富全市场历史回补） | tests/data_jobs/test_financial_completeness.py | module_data_jobs |
| 因子默认历史空洞修复与落盘验证 | tests/data_jobs/test_factor_history_gaps.py | module_data_jobs |
| 趋势启动信号计算、宽表 schema、选股过滤与 Web 字段 | tests/data_jobs/test_trend_up_start_contract.py、tests/ui/test_screen_trend_up_start_contract.py | module_data_jobs |
| 通达信服务器并发测速、显式主机顺序、请求异常与失败回退 | tests/services/test_tongdaxin_client.py | module_data_jobs |
| 分钟线/历史分笔分页边界与重复页保护 | tests/services/test_tongdaxin_minute_sync_service.py、tests/data_jobs/test_moneyflow_tdx_backfill.py | module_data_jobs |

### 通达信连接与分页增强验收标准（2026-11-07 新增）

设计：借鉴 [easy-tdx](https://github.com/clong365/easy_tdx) 的服务器测速、自动分页
和重复分页保护，复用项目现有 `pytdx`，不引入要求 Python 3.10+ 的新依赖。
其免费日 K 方向虽有价值，但 2026-11-07 实测现有 pytdx 标准协议在 12 台候选服务器
均无法返回日 K，因此未将未验证的日线备用源接入夜间链路。

- 默认连接先并发探测候选服务器并按延迟排序，实际连接失败后继续尝试下一台；
  显式传入的主机列表保持调用方顺序，避免破坏历史回补分片的服务器钉住策略；
  请求异常必须抛出，禁止被 pytdx 吞成 `None` 后在资金流中写成全 0
- 分钟线请求区间早于最新 800 根时继续翻页；分笔接口返回重复页或超过安全上限时
  明确失败，禁止死循环或将残缺结果标记完成
- 验收命令：`pytest -m module_data_jobs -q`
- 数值软目标：无（分页与故障处理均以结构性合约为准；真实服务器时延不作为门禁）

### 趋势启动信号验收标准（2026-11-03 新增）

设计：在 Text2SQL/选股大宽表按股票构建技术指标时，以后复权 OHLC 计算
`MM=EMA((3*C+H+L+O)/6,21)`、`UP=MM>=HHV(MM,5)`。当 `UP` 当日为 1、
前一日为 0 且累计 K 线不少于 30 根时，输出 `trend_up_start=1`，其余为 0。
字段写入 `stock_business.parquet`，选股页动态条件以 `trend_up_start = 1` 过滤。

- 信号只在状态跳变日为 1，持续 UP 不重复触发；不足 30 根 K 线始终为 0
- 宽表输入包含计算所需 OHLC/pre_close，输出 schema 包含 `trend_up_start`
- `StockService.screen_stocks` 的动态条件可实际过滤该字段
- Web 可用字段显示“趋势启动信号”并传递 `trend_up_start`
- 验收命令：`pytest -m module_data_jobs -q`
- 数值软目标：无（确定性 0/1 信号，以结构性合约为准）

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
- [x] 结构性合约：CI 绿（pytest -m module_data_jobs，2026-09-25：135 passed；2026-10-26 健康度监控新增后：197 passed；2026-10-31 断点续跑/自动重试/后台 CLI 新增后：256 passed；2026-11-01 历史完整性与数据源切换后：281 passed；2026-11-02 东方财富财务备用源新增后：285 passed；2026-11-07 通达信连接与分页增强后：299 passed，全量回归 909 passed）
- [ ] 数值软目标：N/A
