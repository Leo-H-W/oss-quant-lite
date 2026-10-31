# 数据回补断点续跑与自动重试设计

日期：2026-10-31

## 背景

`daily_basic_baostock` 等按股票循环的长任务只在内存中保存进度。脚本因
`DATA_JOB_TIMEOUT=3600` 被终止后，run 被标记为 failed；现有 `/retry` 只支持
人工按原参数重新提交，既不会自动重试，也没有股票级检查点，因此再次从第一只股票
开始。凌晨 02:30 的 auto-init 链同样在任一节点失败后中断，进程重启时没有链级
完成位置。

## 目标与范围

1. 所有数据任务在同一个 run 内有限次自动重试，默认重试 5 次（共最多 6 次尝试）；
   每个 run/每类任务独立计次，重试次数和退避时间可由环境变量配置，最终失败仍落
   `failed`，不得无限刷接口。
2. 长时间回补按持久化检查点续跑。已具备分区差集/报告期跳过能力的生产者复用
   现有机制；按股票循环但没有检查点的生产者补齐批次检查点。
3. 凌晨 02:30 auto-init 等待时间覆盖任务自动重试；链级已完成节点在同一计划日内
   持久化，进程重启后从首个未完成节点继续。
4. 检查点必须在数据落盘成功后原子更新。崩溃发生在落盘之后、检查点之前时，允许
   幂等重写一个批次，不允许把未落盘批次标记为完成。
5. 全部成功后清除检查点，使显式全量刷新仍保持原有语义；失败股票不标记完成，
   下次优先重试。

## 非目标

- 不引入外部 broker、数据库或新的常驻服务。
- 不把确定性空数据（如 Baostock 不支持的北交所标的）误判为失败。
- 不改变 Parquet 分区原子覆盖与按 `ts_code` 去重的数据口径。

## 验收标准

### 结构性合约（硬门禁）

| 合约 | 测试文件 | Marker |
|---|---|---|
| 股票级批次落盘后记录检查点，中断后跳过已完成股票 | `tests/data_jobs/test_daily_basic_checkpoint.py` | `module_data_jobs` |
| 失败股票不记录完成，下次只重试失败股票；成功后清除检查点 | `tests/data_jobs/test_daily_basic_checkpoint.py` | `module_data_jobs` |
| 同一 run 自动重试，成功/最终失败状态与尝试次数正确 | `tests/data_jobs/test_celery_tasks.py` | `module_data_jobs` |
| auto-init 等待窗口覆盖自动重试，不提前误判 timeout | `tests/data_jobs/test_auto_init.py` | `module_data_jobs` |
| auto-init 同计划日跳过已完成节点，从失败节点继续 | `tests/data_jobs/test_auto_init.py` | `module_data_jobs` |
| 活跃 run 不被 stale reaper 误清理 | `tests/data_jobs/test_stale_run_reaper.py` | `module_data_jobs` |
| 按日、财务报告期、复权因子与资金流只重试未完成单元 | `tests/data_jobs/test_backfill_resume_contracts.py` | `module_data_jobs` |
| 扶摇日线/财务、筹码与因子计算断点及部分失败状态 | `tests/data_jobs/test_remaining_resume_contracts.py` | `module_data_jobs` |
| 隐藏按股日线、独立资金流回补及快照失败状态 | `tests/data_jobs/test_additional_resume_contracts.py` | `module_data_jobs` |
| CLI status/update/help、活动 run、重新开始及每任务重试覆盖 | `tests/data_jobs/test_data_gather_cli.py` | `module_data_jobs` |

### CLI 合约

- `python data-gather.py status`：输出各数据资产范围及活动同步 run；`help/status`
  不提前导入 Flask。
- `python data-gather.py update [N]`：默认以新会话后台运行并写入
  `logs/data-gather-update.log`，SSH 断线不影响进程；复用 auto-init 断点链。
  检测到活动任务时在后台化之前询问，只有用户输入 `y` 才取消旧 run 并重新开始。
  `N` 是本次更新中**每类任务各自**的重试次数；不传时保持 `.env`/默认值，
  结束后恢复命令行覆盖。`DATA_GATHER_FOREGROUND=1` 可强制前台调试。
- `python data-gather.py help`：输出上述用法。

### 数值软目标（里程碑 checklist）

- 下载模块无数值软目标；以结构性合约为准。
- 实机恢复 2019–2022 年 `daily_basic` 回补，记录 run ID、尝试次数与最终状态到
  `acceptance/milestones.md`；不要求用重复下载凑耗时指标。

## 本地验收命令

```bash
pytest tests/data_jobs/test_daily_basic_checkpoint.py -q
pytest tests/data_jobs/test_celery_tasks.py tests/data_jobs/test_auto_init.py -q
pytest -m module_data_jobs -q
```

## 实施顺序

1. 写上述结构性合约测试并确认按预期失败。
2. 实现共享原子检查点与 `daily_basic_baostock` 股票级续跑，跑绿对应测试。
3. 实现任务层有限自动重试、活跃 run 防误收割及 auto-init 等待/链级续跑，跑绿
   `module_data_jobs`。
4. 审计其余回补生产者，复用已有分区/报告期断点；发现无断点长循环则按同一
   检查点原语补齐并增加对应 marker 合约。
5. 停止仍在使用旧逻辑的重复回补，按小区间启动新实现任务并核验持续推进。
