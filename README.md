# PD 采购配送管理 API

[![wakatime](https://wakatime.com/badge/user/d46234d8-e044-4d0d-b6d9-2789ecdaca27/project/c10939fa-88e9-4557-b299-db1defe6618b.svg)](https://wakatime.com/badge/user/d46234d8-e044-4d0d-b6d9-2789ecdaca27/project/c10939fa-88e9-4557-b299-db1defe6618b)

基于 **FastAPI** 的后端：合同 OCR、客户与库房收款配置、销售报货、**报货计划**、订货计划、磅单、结余核销、收款明细与回款导入等。详见 `pyproject.toml` 中的依赖说明。

## 快速开始

### 1) 安装 uv 并创建虚拟环境

```bash
# Windows
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

# Linux/macOS
curl -LsSf https://astral.sh/uv/install.sh | sh

# 创建虚拟环境
uv venv
```

### 2) 安装依赖

```bash
uv sync
```

合同 OCR、磅单等依赖 `rapidocr-onnxruntime`、`opencv-contrib-python` 等（已在 `pyproject.toml` 中声明）。`email-validator` 等常用包也已列入依赖；若本地环境缺包，以 `uv sync` 为准补齐。

### 3) 配置环境变量

推荐使用 `.env`。**应用通过 PyMySQL 连接数据库时读取的是 `MYSQL_*` 变量**（与 `database_setup.py` 一致）。`DATABASE_URL` 在 `app/core/config.py` 中主要为占位，**请勿只配置 `DATABASE_URL` 而省略 `MYSQL_*`**。

```
APP_NAME=PD API
JWT_SECRET=请改为足够长的随机串
JWT_ALGORITHM=HS256

MYSQL_HOST=127.0.0.1
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=你的密码
MYSQL_DATABASE=PD_db
MYSQL_CHARSET=utf8mb4

# 监听端口（main.py 默认 8007）
PORT=8007

# 可选：OpenAI（若使用相关能力）
# OPENAI_API_KEY=

# 可选：逗号分隔，默认 *
# CORS_ALLOW_ORIGINS=http://127.0.0.1:3000,http://localhost:3000

# 可选：日志目录与级别
# LOG_DIR=logs
# LOG_LEVEL=INFO
```

### 4) 初始化 / 同步数据库表结构

```bash
python database_setup.py
```

`database_setup.py` 会创建（若不存在）核心业务表，例如：`pd_users`、`pd_user_permissions`、`pd_contracts`、`pd_deliveries`、`pd_delivery_plans`（含 **创建人 / 最后修改人** 字段：`created_by`、`created_by_name`、`updated_by`、`updated_by_name`）、`pd_weighbills`、`pd_balance_details`、`pd_payment_details`、`pd_warehouse_payees`、`pd_payment_upload_logs`、固定 50 槽位 `pd_product_categories` 等，并初始化权限定义数据。

- **全新库**：直接执行上述命令或依赖应用启动时的 `create_tables()` 即可。
- **已有库**：若曾早于某次迭代建库，报货计划相关能力会在首次调用服务时尝试自动 `ALTER TABLE pd_delivery_plans` 补全操作人字段；若无 DDL 权限，需由 DBA 按 `database_setup.py` 中的定义手工补列。

### 5) 运行应用

```bash
# 开发环境（热重载）
uv run main.py

# 或
uvicorn main:app --reload --host 0.0.0.0 --port 8007
```

实际端口以环境变量 `PORT` 为准（未设置时默认为 **8007**）。

### 6) 访问地址

- Swagger 文档: `http://127.0.0.1:<PORT>/docs`（将 `<PORT>` 换成你的 `PORT`）

## 应用行为摘要

- **启动时**：执行 `create_tables()` 检查/创建表；并启动定时任务（默认每天 00:10，上海时区）将到期合同批量标记为「已失效」（宽限逻辑见 `expire_contracts_after_grace`）。
- **鉴权**：见下文「接口鉴权说明」。
- **用户角色**：库表约束与业务侧一致的角色包括：**管理员**、**大区经理**、**自营库管理**、**财务**、**会计**、**审核主管**（枚举见 `database_setup.py` 中 `pd_users` 的 `CHECK`，前端可选 `GET /api/v1/user/roles`）。

## API 说明概要

以下路径均挂在 **`/api/v1`** 下；完整参数与响应以 **`/docs`** 为准。

### 通用

- `GET /healthz`：健康检查。
- `GET /init-db`：手动触发数据库初始化（**仅建议开发/调试；生产勿对公网开放**）。

### 用户与权限（前缀 `/api/v1/user`）

登录成功后携带：`Authorization: Bearer <token>`。

- `POST /api/v1/user/auth/login`、`POST /api/v1/user/auth/logout`、`POST /api/v1/user/auth/refresh`：登录与令牌。
- `GET /api/v1/user/me`、`PUT /api/v1/user/me`、`PUT /api/v1/user/me/password`：当前用户资料与改密。
- `GET /api/v1/user/roles`：系统预置角色列表（含 **审核主管** 等）。
- `POST /api/v1/user/users`、`GET /api/v1/user/users`、`GET/PUT/DELETE /api/v1/user/users/{id}`、`POST /api/v1/user/users/{id}/reset-password`：用户管理（权限约束见路由实现与 `/docs`）。
- 权限矩阵、角色模板等见 `/docs` 中 **用户认证与权限** 分组。

### 合同管理（`/api/v1/contracts`）

- `POST /api/v1/contracts/ocr`：上传合同图片，OCR 识别；可选自动保存与图片落盘。**自动写入数据库时**须带查询参数 `plan_no`（报货计划编号，即 `pd_delivery_plans.plan_no`）。
- `POST /api/v1/contracts/manual`：手动录入合同（含品种与单价明细）；请求 JSON **必须**包含 `plan_no`（同上）；服务端会解析为 `delivery_plan_id` 落库。
- `GET /api/v1/contracts`：分页列表，支持精确条件与模糊关键词。
- `GET /api/v1/contracts/{contract_id}`：详情（含品种明细）。
- `GET /api/v1/contracts/{contract_id}/image`：预览合同图片。
- `PUT /api/v1/contracts/{contract_id}`：更新合同与品种明细。
- `DELETE /api/v1/contracts/{contract_id}`：删除合同。
- `POST /api/v1/contracts/export`：导出 CSV（合同 ID 列表为空可表示导出全部，以实际接口行为为准）。

### 客户管理（`/api/v1/customers`）

- 冶炼厂客户的增删改查。
- `GET/POST /api/v1/customers/warehouse-payees`、`PUT /api/v1/customers/warehouse-payees/{id}`：库房收款员配置（与业务里 `pd_warehouse_payees` / `pd_payees` 等逻辑配合使用，细节以代码与 `/docs` 为准）。

### 销售台账 / 报货订单（`/api/v1/deliveries`）

- 报货单的增删改查、联单上传、审核相关能力（部分接口需登录，见 `/docs`）。

### 报货计划（`/api/v1/delivery-plans`）

- `POST /api/v1/delivery-plans/`：**录入**（**需登录**）；写入 `created_by` / `created_by_name`，并将 `updated_by` / `updated_by_name` 设为同一操作人。
- `GET /api/v1/delivery-plans/`：分页列表（支持计划编号、状态、冶炼厂、计划开始日期区间等筛选）。
- `GET /api/v1/delivery-plans/{plan_id}`：详情（含品类单价明细；响应中含操作人字段）。
- `PUT /api/v1/delivery-plans/{plan_id}`：**修改**（**需登录**）；更新主表或替换 `items` 时写入 **最后修改人**。
- `DELETE /api/v1/delivery-plans/{plan_id}`：删除（当前实现未强制 JWT，生产建议收紧）。
- `POST /api/v1/delivery-plans/increment-confirmed-trucks`：按 `plan_no` 累加已定车数并重算未定车数（**需登录**）；同时更新最后修改人。

### 订货计划（`/api/v1/order-plans`）

- 录入、列表、详情、改车数、审核；审核通过与报货计划已定车数联动（与 `increment-confirmed-trucks` 规则一致，详见 `/docs` 与该模块说明）。

### 磅单管理（`/api/v1/weighbills`）

- OCR、创建、列表、详情、图片、修改、删除、确认、与报货匹配、按合同查价等。

### 磅单结余 / 收款（`/api/v1/balances`、`/api/v1/payment` 等）

- 结余生成、支付回单、收款明细、回款导入与上传日志等；完整列表见 `/docs` 中 **磅单结余管理**、**收款明细管理** 等分组。

### 品类管理（`/api/v1/product-categories`）

- 固定 50 槽位品类的查询、写入与按名删除。

## 接口鉴权说明（当前实现）

- **挂载方式**：`main.py` 中为 `/api/v1` 挂载了 `HTTPBearer(auto_error=False)`；`register_pd_auth_routes` 注册 `/api/v1/user` 下认证与用户管理路由。
- **是否必须登录**：由各路由是否声明 `Depends(get_current_user)` 决定（**以代码与 `/docs` 为准**）。
- **已强制登录的典型能力**（会持续迭代，以下非穷尽）：
  - **报货计划**：`POST /api/v1/delivery-plans/`、`PUT /api/v1/delivery-plans/{plan_id}`、`POST /api/v1/delivery-plans/increment-confirmed-trucks`。
  - **报货、收款、磅单**等模块中的部分写操作与敏感读操作。
- **仍可能匿名访问的接口**：如部分合同/客户/结余/品类、报货计划列表与详情/删除等；**生产环境**建议在网关统一鉴权，或逐步为这些路由补上登录与权限校验。

## 安全性说明

- **务必修改 `JWT_SECRET`**，勿使用示例或仓库中的默认值。
- **`GET /init-db` 无鉴权**，勿对公网开放。
- **业务接口鉴权未全覆盖**，见上一节。
- 密钥与生产配置勿提交到版本库。

## 开发与架构备注

- 数据库连接：`core/database.py` 的 `get_conn()` 使用 `DictCursor`；部分模块仍从 `contract_service` 等引入另一套 `get_conn()`（默认游标），行为略有差异，后续可考虑统一。
- 报货计划：`app/services/delivery_plan_service.py` 在首次访问时会对旧库尝试补全 `pd_delivery_plans` 上的操作人相关列（见 `_ensure_plan_audit_columns`）。
- 请求日志中间件会注入当前用户上下文（见 `main.py` 中 `request_logger` 与 `get_user_identity_from_authorization`）。
