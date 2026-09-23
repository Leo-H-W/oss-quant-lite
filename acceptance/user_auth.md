# 模块：user_auth

## 职责
全站用户登录校验：默认 admin/admin 首次登录强制改密、邮箱链接重置密码（SMTP 未配置时降级到服务端日志）、admin 用户管理（新建用户初始密码 123456、重置他人密码为 123456，均触发首次登录强制改密）。

## 结构性合约（自动，硬门禁）
| 合约 | 测试文件 | Marker |
|---|---|---|
| 用户/密码服务 | tests/services/test_auth_service_users.py | module_user_auth |
| 重置 token 服务 | tests/services/test_auth_service_tokens.py | module_user_auth |
| 邮件降级发送 | tests/services/test_auth_service_email.py | module_user_auth |
| 认证 API 全流程 | tests/api/test_auth_api_flow.py | module_user_auth |
| 全站拦截守卫 | tests/api/test_auth_guard_contract.py | module_user_auth |
| 登录/改密/用户管理页面 | tests/ui/test_auth_pages_contract.py | module_user_auth |

## 数值软目标（里程碑 review 时人工确认）
- 无（认证模块以结构性合约为主）

## 上游依赖
- 无（基础横切模块，被全站页面与 API 依赖）

## 验收状态
- [x] 结构性合约：CI 绿（pytest -m module_user_auth，2026-09-23，35 个测试全绿）
- [x] 数值软目标：N/A
