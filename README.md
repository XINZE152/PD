# PD FastAPI Starter

[![wakatime](https://wakatime.com/badge/user/d46234d8-e044-4d0d-b6d9-2789ecdaca27/project/c10939fa-88e9-4557-b299-db1defe6618b.svg)](https://wakatime.com/badge/user/d46234d8-e044-4d0d-b6d9-2789ecdaca27/project/c10939fa-88e9-4557-b299-db1defe6618b)

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
# 如使用 EmailStr 字段，请确保安装 email-validator
uv pip install email-validator
```

### 3) 配置环境变量

推荐使用 .env 文件，以下为最小示例：

```
APP_NAME=PD API
JWT_SECRET=change-me
JWT_ALGORITHM=HS256

MYSQL_HOST=127.0.0.1
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=123456
MYSQL_DATABASE=PD_db
MYSQL_CHARSET=utf8mb4

# 供应用数据库连接使用
DATABASE_URL=mysql+pymysql://root:123456@127.0.0.1:3306/PD_db?charset=utf8mb4
```

### 4) 初始化/同步数据库表结构

```bash
# 一次性创建基础表（或补齐缺失索引/列）
python database_setup.py
```

### 5) 运行应用

```bash
# 快速运行（开发环境）
uv run main.py

# 热重载
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# 生产模式
uvicorn main:app --host 0.0.0.0 --port 8000
```

### 6) 访问地址

以下示例基于 .env 中 PORT=8007：

- 内网文档地址: http://127.0.0.1:8007/docs

## API 详细说明

通用：
- GET /healthz: 健康检查，返回服务状态。
- GET /init-db: 手动触发数据库初始化（调试用，生产环境不建议开放）。

认证：
- POST /api/v1/auth/login: 登录并签发 JWT（当前未校验账号密码，见安全说明）。

用户与权限（PD 用户体系）：
- POST /api/v1/user/auth/login: 用户登录，校验账号密码并返回 JWT。
- POST /api/v1/user/auth/logout: 用户登出（前端清除 token）。
- POST /api/v1/user/auth/refresh: 刷新访问令牌。
- GET /api/v1/user/me: 获取当前登录用户信息。
- PUT /api/v1/user/me: 更新当前用户资料（不含角色）。
- PUT /api/v1/user/me/password: 修改当前用户密码。
- POST /api/v1/user/users: 创建用户（管理员/大区经理权限）。
- GET /api/v1/user/users: 用户列表（分页/筛选，管理员/大区经理权限）。
- GET /api/v1/user/users/{user_id}: 用户详情。
- PUT /api/v1/user/users/{user_id}: 更新用户信息。
- DELETE /api/v1/user/users/{user_id}: 删除用户（软删除）。
- POST /api/v1/user/users/{user_id}/reset-password: 管理员重置用户密码。

合同管理：
- POST /api/v1/contracts/ocr: 上传合同图片，OCR 识别合同信息，可选择自动保存与图片落盘。
- POST /api/v1/contracts/manual: 手动录入合同（含品种与单价明细）。
- GET /api/v1/contracts: 分页查询合同列表，支持精确条件与模糊关键词。
- GET /api/v1/contracts/{contract_id}: 查询合同详情（含品种明细）。
- GET /api/v1/contracts/{contract_id}/image: 预览合同图片。
- PUT /api/v1/contracts/{contract_id}: 更新合同与品种明细。
- DELETE /api/v1/contracts/{contract_id}: 删除合同。
- POST /api/v1/contracts/export: 导出合同数据（CSV 文件下载，传合同ID列表为空则导出全部）。

客户管理：
- POST /api/v1/customers: 创建客户。
- GET /api/v1/customers: 查询客户列表。
- GET /api/v1/customers/{customer_id}: 查询客户详情。
- PUT /api/v1/customers/{customer_id}: 更新客户信息。
- DELETE /api/v1/customers/{customer_id}: 删除客户。

销售台账/报货订单：
- POST /api/v1/deliveries: 新增报货订单/销售台账记录。
- GET /api/v1/deliveries: 查询报货订单列表。
- GET /api/v1/deliveries/{delivery_id}: 查询单条报货订单。
- GET /api/v1/deliveries/{delivery_id}/image: 预览联单图片。
- PUT /api/v1/deliveries/{delivery_id}: 更新报货订单。
- DELETE /api/v1/deliveries/{delivery_id}: 删除报货订单。
- POST /api/v1/deliveries/{delivery_id}/upload-order: 上传报货单附件。

磅单管理：
- POST /api/v1/weighbills/ocr: 上传磅单图片并 OCR 识别。
- POST /api/v1/weighbills/create: 新增磅单（按品种上传）。
- GET /api/v1/weighbills: 查询磅单列表。
- GET /api/v1/weighbills/{bill_id}: 查询磅单详情。
- GET /api/v1/weighbills/{bill_id}/image: 预览磅单图片。
- PUT /api/v1/weighbills/modify: 更新磅单（支持修改信息和图片）。
- DELETE /api/v1/weighbills/{bill_id}: 删除磅单。
- POST /api/v1/weighbills/{bill_id}/confirm: 磅单确认/锁定。
- GET /api/v1/weighbills/match/delivery: 匹配磅单与报货订单。
- GET /api/v1/weighbills/contract/price: 根据合同查询价格信息。

磅单结余/支付回单：
- 磅单上传/修改成功后会自动生成结余明细。
- 也可手动触发：POST /api/v1/balances/generate。
- 结余、支付回单等相关路由已注册到主路由，详见 docs 页面。

## 安全性说明（当前状态）

- /api/v1/auth/login 目前仅签发 JWT，没有校验账号密码。
- 业务接口未统一接入鉴权与权限控制，存在未授权访问风险。
- JWT_SECRET 有默认值，若未配置会导致弱密钥问题。
- /init-db 公开可访问，生产环境不建议开放。
- 代码中存在另一套用户认证路由，但当前未挂载到主应用。
