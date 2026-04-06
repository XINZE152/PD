"""
分配规划路由
支持生成调度计划、查看优化结果、测试数据管理
"""
import logging
import uuid
from datetime import datetime, timedelta
from typing import Any, Optional
import random
from decimal import Decimal

from fastapi import APIRouter, HTTPException, Query, Body
from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.services.allocation_service import (
    compute_manager_daily_allocation,
    get_active_contracts,
    get_warehouses,
    get_warehouse_daily_capacity,
    solve_dispatch_plan,
    save_predictions_to_db,
    get_filter_options,
    query_ai_purchase_quantity,
)
from app.services.contract_service import get_conn


router = APIRouter(prefix="/allocation", tags=["分配规划"])
# 不挂载全局 HTTPBearer：供外部/脚本直接调用，OpenAPI 中不显示锁与 Authorization
public_router = APIRouter(prefix="/allocation", tags=["分配规划"])
logger = logging.getLogger(__name__)


# ============ 响应模型 ============

class AllocationPlanResponse(BaseModel):
    """分配计划响应：含排产方案与求解元数据。"""

    model_config = ConfigDict(title="分配计划响应")

    success: bool = Field(True, description="是否成功")
    message: str = Field("调度计划生成成功", description="提示信息")
    plan: dict = Field(
        ...,
        description="排产方案：仓库 → 合同编号 → 冶炼厂 → 日期 → 车数",
    )
    meta: dict = Field(..., description="元数据（求解状态、时间窗口、车数汇总等）")


class ContractStatusResponse(BaseModel):
    """单份生效合同的进度概况。"""

    model_config = ConfigDict(title="合同状态项")

    contract_no: str = Field(..., description="合同编号")
    smelter_company: str = Field(..., description="冶炼厂名称")
    total_quantity: float = Field(..., description="合同总吨位")
    total_trucks: int = Field(..., description="需求总车数")
    delivered_trucks: int = Field(..., description="已发车数（报单条数）")
    remaining_trucks: int = Field(..., description="剩余车数")


class ContractsStatusResponse(BaseModel):
    """生效合同状态列表。"""

    model_config = ConfigDict(title="合同状态列表响应")

    success: bool = Field(True, description="是否成功")
    contracts: list[ContractStatusResponse] = Field(..., description="合同状态列表")


class ManagerDailyDemandResponse(BaseModel):
    """大区经理每日分配需求（当日及未来）"""
    success: bool = True
    tonnage_per_truck: int = 35
    days: list
    meta: dict


class SetupTestDataRequest(BaseModel):
    """写入分配规划联调用的测试合同/报单/磅单。"""

    model_config = ConfigDict(title="设置测试数据请求")

    num_contracts: int = Field(5, ge=1, le=20, description="要生成的测试合同数量")
    num_deliveries_per_contract: int = Field(
        2, ge=0, le=5, description="每个合同最多生成的报货（销售台账）条数"
    )
    num_weighbills_per_contract: int = Field(
        1, ge=0, le=3, description="每个合同最多生成的磅单条数"
    )
    contract_prefix: str = Field("TEST", description="合同编号前缀，用于区分测试数据")


class SetupTestDataResponse(BaseModel):
    """设置测试数据后的统计结果。"""

    model_config = ConfigDict(title="设置测试数据响应")

    success: bool = Field(True, description="是否成功")
    message: str = Field(..., description="结果说明")
    inserted_contracts: int = Field(..., description="新插入的合同数")
    inserted_deliveries: int = Field(..., description="新插入的报单数")
    inserted_weighbills: int = Field(..., description="新插入的磅单数")


class CleanupTestDataResponse(BaseModel):
    """按前缀清理测试数据后的统计结果。"""

    model_config = ConfigDict(title="清理测试数据响应")

    success: bool = Field(True, description="是否成功")
    message: str = Field(..., description="结果说明")
    deleted_contracts: int = Field(..., description="删除的合同数")
    deleted_deliveries: int = Field(..., description="删除的报单数")
    deleted_weighbills: int = Field(..., description="删除的磅单数")


class WarehousesListResponse(BaseModel):
    """仓库名称列表。"""

    model_config = ConfigDict(title="仓库列表响应")

    success: bool = Field(True, description="是否成功")
    warehouses: list[str] = Field(..., description="仓库名称列表")
    count: int = Field(..., description="仓库数量")


class WarehouseCapacityResponse(BaseModel):
    """各仓库每日最大可发车数：默认不封顶（值为 null）；可设 ALLOCATION_DAILY_CAP_PER_WAREHOUSE 封顶。"""

    model_config = ConfigDict(title="仓库日产能响应")

    success: bool = Field(True, description="是否成功")
    daily_capacity: dict[str, Optional[int]] = Field(
        ...,
        description="仓库名称 → 每日最大车数；未配置封顶时为 null（排产模型中无日上限约束）",
    )


class ActiveContractItemResponse(BaseModel):
    """参与排产的单条合同（已按截至日扣减已发车）。"""

    model_config = ConfigDict(title="生效合同项")

    contract_no: str = Field(..., description="合同编号")
    smelter: str = Field(..., description="冶炼厂")
    total_tons: float = Field(..., description="剩余需求吨位")
    total_trucks: int = Field(..., description="剩余需求车数")
    start_date: str = Field(..., description="合同开始日期")
    end_date: str = Field(..., description="合同结束日期")


class ActiveContractsListResponse(BaseModel):
    """生效合同列表（供排产读取）。"""

    model_config = ConfigDict(title="生效合同列表响应")

    success: bool = Field(True, description="是否成功")
    contracts: list[ActiveContractItemResponse] = Field(..., description="合同列表")
    count: int = Field(..., description="合同条数")


class PurchaseQuantityQueryRequest(BaseModel):
    """AI 预测报货数量：统一查询请求体。"""

    model_config = ConfigDict(
        title="AI预测报货查询请求",
        json_schema_extra={
            "example": {
                "start_date": "2026-04-01",
                "end_date": "2026-04-07",
                "warehouse": None,
                "contract_no": None,
                "smelter": None,
            }
        },
    )

    start_date: str = Field(..., description="规划/展示起始日 YYYY-MM-DD")
    end_date: str = Field(..., description="规划/展示结束日 YYYY-MM-DD，须 ≥ start_date")
    warehouse: Optional[str] = Field(None, description="仓库名称；空表示全部")
    contract_no: Optional[str] = Field(None, description="合同编号，后端按子串模糊匹配；空表示不筛")
    smelter: Optional[str] = Field(None, description="冶炼厂关键字（如 金利、豫光）；空表示全部")

    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def strip_dates(cls, v: object) -> object:
        if isinstance(v, str):
            return v.strip()
        return v

    @field_validator("warehouse", "contract_no", "smelter", mode="before")
    @classmethod
    def empty_filter_to_none(cls, v: object) -> Optional[str]:
        if v is None:
            return None
        if isinstance(v, str) and not v.strip():
            return None
        return v.strip() if isinstance(v, str) else str(v)


class PurchaseQuantityDataPayload(BaseModel):
    """统一查询返回的 data 字段：warehouse_options + 四层嵌套 plan。"""

    model_config = ConfigDict(
        title="AI预测报货查询数据",
        json_schema_extra={
            "example": {
                "warehouse_options": ["山东仓库", "山西仓库", "666"],
                "plan": {
                    "山东仓库": {
                        "HT-2026-001": {
                            "金利": {
                                "2026-04-05": 2,
                                "2026-04-06": 1,
                                "2026-04-07": 0,
                            }
                        },
                        "HT-2026-002": {
                            "豫光": {"2026-04-05": 3, "2026-04-06": 2}
                        },
                    },
                    "山西仓库": {
                        "HT-2026-010": {"株冶": {"2026-04-05": 1}}
                    },
                },
            }
        },
    )

    warehouse_options: list[str] = Field(
        default_factory=list,
        description="仓库名称列表，供「大区经理（仓库）」下拉",
    )
    plan: dict[str, dict[str, dict[str, dict[str, int]]]] = Field(
        default_factory=dict,
        description="仓库 → 合同编号 → 冶炼厂 → 日期(YYYY-MM-DD) → 预测车数；区间内无数据日为 0",
    )


class PurchaseQuantityQueryEnvelope(BaseModel):
    """统一查询 HTTP 响应：{ success, message, data }；失败时 data 为 null。"""

    model_config = ConfigDict(
        title="AI预测报货查询响应",
        json_schema_extra={
            "example": {
                "success": True,
                "message": "",
                "data": {
                    "warehouse_options": ["山东仓库", "山西仓库", "666"],
                    "plan": {
                        "山东仓库": {
                            "HT-2026-001": {
                                "金利": {
                                    "2026-04-05": 2,
                                    "2026-04-06": 1,
                                    "2026-04-07": 0,
                                }
                            },
                            "HT-2026-002": {
                                "豫光": {"2026-04-05": 3, "2026-04-06": 2}
                            },
                        },
                        "山西仓库": {
                            "HT-2026-010": {"株冶": {"2026-04-05": 1}}
                        },
                    },
                },
            }
        },
    )

    success: bool = Field(..., description="是否成功")
    message: str = Field("", description="失败时的说明；成功时可为空字符串")
    data: Optional[PurchaseQuantityDataPayload] = Field(
        None,
        description="成功且可查时为 warehouse_options 与 plan；失败或无数据时为 null",
    )


# ============ 辅助函数 ============

def _get_db_conn():
    """获取数据库连接（兼容旧代码）"""
    return get_conn()


def _setup_warehouses():
    """设置仓库"""
    warehouses = [
        ('河南金铅仓库', '张经理'),
        ('河北仓库', '李经理'),
        ('山东仓库', '王经理'),
        ('山西仓库', '赵经理')
    ]

    with _get_db_conn() as conn:
        with conn.cursor() as cur:
            for name, manager in warehouses:
                try:
                    cur.execute(
                        'INSERT INTO pd_warehouses (warehouse_name, regional_manager, is_active, created_at, updated_at) '
                        'VALUES (%s, %s, 1, NOW(), NOW())',
                        (name, manager)
                    )
                except Exception as e:
                    if 'Duplicate entry' not in str(e):
                        raise


def _insert_test_contracts(num_contracts: int, prefix: str) -> list:
    """插入测试合同"""
    smelters = [
        "河南金利金铅集团有限公司",
        "河北金铅冶炼有限公司",
        "山东再生铅有限公司",
        "山西铅业集团"
    ]
    products = ["电动车", "黑皮", "新能源", "通信", "摩托车"]

    inserted = []
    day_stamp = datetime.now().strftime("%Y%m%d")
    run_token = uuid.uuid4().hex[:8]

    with _get_db_conn() as conn:
        with conn.cursor() as cur:
            for i in range(num_contracts):
                contract_no = f"{prefix}_{day_stamp}_{run_token}_{i+1:03d}"
                smelter = random.choice(smelters)
                contract_date = (datetime.now() - timedelta(days=random.randint(0, 1))).date()
                end_date = contract_date + timedelta(days=random.randint(5, 10))
                total_quantity = random.randint(100, 500)
                truck_count = total_quantity // 35

                cur.execute("""
                    INSERT INTO pd_contracts
                    (contract_no, contract_date, end_date, smelter_company,
                     total_quantity, truck_count, arrival_payment_ratio, final_payment_ratio,
                     status, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                """, (
                    contract_no, contract_date, end_date, smelter,
                    total_quantity, truck_count, Decimal("0.9"), Decimal("0.1"), "生效中"
                ))

                contract_id = cur.lastrowid

                num_products = random.randint(1, 3)
                selected_products = random.sample(products, num_products)
                for j, product_name in enumerate(selected_products):
                    unit_price = Decimal(str(random.randint(15000, 18000))) + Decimal("0.00")
                    cur.execute("""
                        INSERT INTO pd_contract_products
                        (contract_id, product_name, unit_price, sort_order)
                        VALUES (%s, %s, %s, %s)
                    """, (contract_id, product_name, unit_price, j))

                inserted.append({
                    "contract_no": contract_no,
                    "smelter": smelter,
                    "total_quantity": total_quantity,
                    "truck_count": truck_count,
                    "contract_date": contract_date,
                    "end_date": end_date
                })

    return inserted


def _insert_test_deliveries(contracts: list, max_per_contract: int) -> int:
    """插入测试报单"""
    statuses = ['已发货', '已装车', '在途', '已签收']
    inserted = 0

    with _get_db_conn() as conn:
        with conn.cursor() as cur:
            for contract in contracts:
                contract_no = contract["contract_no"]
                smelter = contract["smelter"]
                truck_count = contract["truck_count"]

                if not truck_count:
                    continue

                num_delivered = random.randint(0, min(max_per_contract, truck_count))

                for i in range(num_delivered):
                    cur.execute('''
                        INSERT INTO pd_deliveries
                        (contract_no, status, warehouse, target_factory_name, product_name,
                         quantity, vehicle_no, driver_name, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ''', (
                        contract_no, random.choice(statuses),
                        random.choice(['河南金铅仓库', '河北仓库']),
                        smelter, '电动车', 35.0,
                        f'豫A{random.randint(10000,99999)}', '测试司机'
                    ))
                    inserted += 1

    return inserted


def _insert_test_weighbills(contracts: list, max_per_contract: int) -> int:
    """插入测试磅单"""
    inserted = 0

    with _get_db_conn() as conn:
        with conn.cursor() as cur:
            for contract in contracts:
                contract_no = contract["contract_no"]

                num_weighbills = random.randint(0, max_per_contract)

                for i in range(num_weighbills):
                    cur.execute('''
                        INSERT INTO pd_weighbills
                        (weigh_date, delivery_time, contract_no, vehicle_no,
                         product_name, gross_weight, tare_weight, net_weight,
                         unit_price, total_amount, upload_status, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ''', (
                        datetime.now().date(), datetime.now(), contract_no,
                        f'豫B{random.randint(10000,99999)}', '电动车',
                        random.randint(40, 50), random.randint(10, 15),
                        random.randint(30, 35), 16000.0,
                        random.randint(30, 35) * 16000.0, '已上传'
                    ))
                    inserted += 1

    return inserted


def _cleanup_test_data(prefix: str = "TEST") -> dict:
    """清理测试数据"""
    deleted = {"contracts": 0, "deliveries": 0, "weighbills": 0}

    with _get_db_conn() as conn:
        with conn.cursor() as cur:
            # 删除测试磅单
            cur.execute("DELETE FROM pd_weighbills WHERE contract_no LIKE %s", (f'{prefix}%',))
            deleted["weighbills"] = cur.rowcount

            # 删除测试报单
            cur.execute("DELETE FROM pd_deliveries WHERE contract_no LIKE %s", (f'{prefix}%',))
            deleted["deliveries"] = cur.rowcount

            # 删除测试合同品种
            cur.execute("""
                DELETE FROM pd_contract_products
                WHERE contract_id IN (
                    SELECT id FROM pd_contracts WHERE contract_no LIKE %s
                )
            """, (f'{prefix}%',))

            # 删除测试合同
            cur.execute("DELETE FROM pd_contracts WHERE contract_no LIKE %s", (f'{prefix}%',))
            deleted["contracts"] = cur.rowcount

    return deleted


def _save_predictions_to_db(plan: dict, prediction_date: str, is_test: bool = False):
    """保存预测结果到数据库"""
    save_predictions_to_db(plan, prediction_date, is_test)


def _run_dispatch_and_save(
    window_start: str,
    H: int,
    *,
    as_of_date: str,
    is_test: bool,
) -> tuple[bool, str]:
    """求解排产并写入 pd_allocation_predictions；返回 (是否成功, 说明)。"""
    contracts = get_active_contracts(as_of_date=as_of_date)
    if not contracts:
        return False, "no active contracts"
    warehouses = get_warehouses()
    if not warehouses:
        return False, "no warehouses"
    daily_cap = get_warehouse_daily_capacity()
    window_end = (
        datetime.strptime(window_start, "%Y-%m-%d") + timedelta(days=H - 1)
    ).strftime("%Y-%m-%d")
    plan, status = solve_dispatch_plan(
        contracts=contracts,
        warehouses=warehouses,
        daily_cap=daily_cap,
        window_start=window_start,
        window_end=window_end,
        solver_msg=False,
    )
    if status not in ("Optimal", "Feasible"):
        return False, f"solver status={status}"
    _save_predictions_to_db(plan, window_start, is_test=is_test)
    return True, "ok"


def run_daily_prediction(H: int = 10) -> None:
    """供 APScheduler 调用：按当日窗口生成正式预测并入库（与 GET /plan 核心逻辑一致）。"""
    window_start = datetime.now().strftime("%Y-%m-%d")
    ok, msg = _run_dispatch_and_save(
        window_start, H, as_of_date=window_start, is_test=False
    )
    if not ok:
        logger.warning("run_daily_prediction skipped: %s", msg)


def run_test_prediction(num_contracts: int = 5, H: int = 10) -> None:
    """供 lifespan / 定时任务调用：写入测试合同与关联数据后生成测试预测（is_test）。"""
    try:
        _setup_warehouses()
        contracts = _insert_test_contracts(num_contracts=num_contracts, prefix="TEST")
        _insert_test_deliveries(contracts=contracts, max_per_contract=2)
        _insert_test_weighbills(contracts=contracts, max_per_contract=1)
    except Exception:
        logger.exception("run_test_prediction: test data setup failed")
        return
    window_start = datetime.now().strftime("%Y-%m-%d")
    ok, msg = _run_dispatch_and_save(
        window_start, H, as_of_date=window_start, is_test=True
    )
    if not ok:
        logger.warning("run_test_prediction skipped: %s", msg)


@router.post(
    "/test-data/setup",
    summary="写入分配规划测试数据",
    response_description="返回本次插入的合同、报单、磅单数量",
    response_model=SetupTestDataResponse,
)
async def setup_test_data(request: SetupTestDataRequest = Body(...)):
    """
    设置测试数据

    功能:
    - 创建指定数量的测试合同
    - 为每个合同创建报单
    - 为每个合同创建磅单
    - 自动设置仓库

    用于在没有真实数据时测试分配规划功能
    """
    try:
        _setup_warehouses()

        contracts = _insert_test_contracts(
            num_contracts=request.num_contracts,
            prefix=request.contract_prefix,
        )

        deliveries_count = _insert_test_deliveries(
            contracts=contracts,
            max_per_contract=request.num_deliveries_per_contract,
        )

        weighbills_count = _insert_test_weighbills(
            contracts=contracts,
            max_per_contract=request.num_weighbills_per_contract,
        )

        return SetupTestDataResponse(
            success=True,
            message=(
                f"测试数据设置成功: {len(contracts)}个合同, "
                f"{deliveries_count}个报单, {weighbills_count}个磅单"
            ),
            inserted_contracts=len(contracts),
            inserted_deliveries=deliveries_count,
            inserted_weighbills=weighbills_count,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"设置测试数据失败: {str(e)}")


@router.post(
    "/test-data/cleanup",
    summary="清理分配规划测试数据",
    response_description="按合同编号前缀删除测试合同及关联报单、磅单",
    response_model=CleanupTestDataResponse,
)
async def cleanup_test_data(
    prefix: str = Query(
        "TEST",
        title="合同编号前缀",
        description="测试合同编号前缀，仅删除合同编号以此前缀开头的数据",
    ),
):
    """
    清理测试数据

    删除所有以指定前缀开头的测试数据:
    - 测试合同
    - 测试报单
    - 测试磅单
    """
    try:
        deleted = _cleanup_test_data(prefix=prefix)

        return CleanupTestDataResponse(
            success=True,
            message=f"测试数据清理成功: 删除{deleted['contracts']}个合同, {deleted['deliveries']}个报单, {deleted['weighbills']}个磅单",
            deleted_contracts=deleted["contracts"],
            deleted_deliveries=deleted["deliveries"],
            deleted_weighbills=deleted["weighbills"]
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"清理测试数据失败: {str(e)}")


@router.get(
    "/status",
    summary="查询生效合同进度",
    response_description="各合同总车数、已发车、剩余车数",
    response_model=ContractsStatusResponse,
)
async def get_contracts_status():
    """
    获取所有生效中合同的状态(含已发车数)

    返回每个合同的:
    - 原始需求车数
    - 已发车数
    - 剩余车数
    """
    try:
        contracts_status = []
        with _get_db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT contract_no, smelter_company, total_quantity, truck_count
                    FROM pd_contracts
                    WHERE status = '生效中'
                    ORDER BY contract_no
                """)
                rows = cur.fetchall()

                for row in rows:
                    contract_no = row[0]
                    smelter_company = row[1]
                    total_quantity = row[2]
                    truck_count = row[3] or 0

                    cur.execute("""
                        SELECT COUNT(*) as count
                        FROM pd_deliveries
                        WHERE contract_no = %s
                    """, (contract_no,))
                    delivery_row = cur.fetchone()
                    delivered_trucks = delivery_row[0] if delivery_row else 0

                    contracts_status.append(ContractStatusResponse(
                        contract_no=contract_no,
                        smelter_company=smelter_company,
                        total_quantity=total_quantity,
                        total_trucks=truck_count,
                        delivered_trucks=delivered_trucks,
                        remaining_trucks=max(0, truck_count - delivered_trucks)
                    ))

        return ContractsStatusResponse(
            success=True,
            contracts=contracts_status
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取合同状态失败: {str(e)}")


@public_router.get(
    "/plan",
    summary="生成调度分配计划",
    description=(
        "**无需登录**：不要求 `Authorization`，可在内网或脚本中直接调用以写入 `pd_allocation_predictions`。\n\n"
        "**注意**：会执行线性规划并改写数据库中对应 `prediction_date` 的快照，请勿对公网暴露或应配合网关鉴权。"
    ),
    response_description="线性规划排产结果及 meta 元数据",
    response_model=AllocationPlanResponse,
)
async def generate_allocation_plan(
    window_start: Optional[str] = Query(
        None,
        title="规划窗口起始日",
        description="规划窗口起始日期，格式 YYYY-MM-DD；不传则默认为当天",
    ),
    H: int = Query(
        10,
        ge=1,
        le=30,
        title="规划窗口天数",
        description="从起始日起连续规划的天数，取值 1～30",
    ),
    as_of_date: Optional[str] = Query(
        None,
        title="已发车统计截至日",
        description="计算已发车数时截至该日；不传则与规划窗口起始日相同",
    ),
    include_solver_log: bool = Query(
        False,
        title="返回求解器日志",
        description="为 true 时在求解过程中附带求解器输出（便于排查不可行等问题）",
    ),
):
    """
    生成调度分配计划

    功能:
    - 从数据库读取生效中的合同
    - 统计每个合同的已发车数(从报单和磅单)
    - 动态调整剩余需求
    - 使用线性规划优化调度
    - 最小化各冶炼厂每日到货车数的方差

    返回:
    - plan: {仓库: {合同编号: {冶炼厂: {日期: 车数}}}}
    - meta: 元数据(求解状态、窗口、总车数等)
    """
    try:
        if window_start is None:
            window_start = datetime.now().strftime("%Y-%m-%d")

        if as_of_date is None:
            as_of_date = window_start

        contracts = get_active_contracts(as_of_date=as_of_date)

        if not contracts:
            raise HTTPException(status_code=400, detail="无生效中的合同")

        warehouses = get_warehouses()

        if not warehouses:
            raise HTTPException(status_code=400, detail="无可用仓库")

        daily_cap = get_warehouse_daily_capacity()

        window_end = (
            datetime.strptime(window_start, "%Y-%m-%d") + timedelta(days=H - 1)
        ).strftime("%Y-%m-%d")

        plan, status = solve_dispatch_plan(
            contracts=contracts,
            warehouses=warehouses,
            daily_cap=daily_cap,
            window_start=window_start,
            window_end=window_end,
            solver_msg=include_solver_log,
        )

        meta = {
            "solver_status": status,
            "window_start": window_start,
            "window_end": window_end,
            "H": H,
            "tons_per_truck": 35,
            "contracts_count": len(contracts),
            "smelters": sorted({c.smelter for c in contracts}),
            "total_trucks_needed": sum(c.total_trucks for c in contracts),
            "total_trucks_planned": sum(
                cnt
                for contract_map in plan.values()
                for smelter_map in contract_map.values()
                for date_map in smelter_map.values()
                for cnt in date_map.values()
            ),
        }

        if status not in ("Optimal", "Feasible"):
            logger.warning(
                "generate_allocation_plan: solver not ok status=%s window_start=%s window_end=%s H=%s contracts=%s",
                status,
                window_start,
                window_end,
                H,
                len(contracts),
            )
            raise HTTPException(
                status_code=422,
                detail=(
                    f"规划求解未成功，状态为 {status}（常见为 Infeasible："
                    "合同在窗口内的有效发货日与剩余车数、均衡约束等冲突）。"
                    "可尝试增大 H、调整 window_start/as_of_date。"
                    "若配置了 ALLOCATION_DAILY_CAP_PER_WAREHOUSE，可清空或删除该变量以取消每库日上限。"
                ),
            )

        _save_predictions_to_db(plan, window_start, is_test=False)

        return AllocationPlanResponse(
            success=True,
            message="调度计划生成成功",
            plan=plan,
            meta=meta,
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"生成调度计划失败: {str(e)}")


@router.get(
    "/warehouses",
    summary="获取仓库列表",
    response_description="当前启用仓库名称及数量",
    response_model=WarehousesListResponse,
)
async def get_warehouses_list():
    """获取所有仓库列表"""
    try:
        warehouses = get_warehouses()
        return WarehousesListResponse(
            success=True,
            warehouses=warehouses,
            count=len(warehouses),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取仓库列表失败: {str(e)}")


@router.get(
    "/capacity",
    summary="获取各仓库日产能",
    response_description="各仓库每日最大可发车数",
    response_model=WarehouseCapacityResponse,
)
async def get_warehouse_capacity():
    """获取各仓库每日发货能力"""
    try:
        capacity = get_warehouse_daily_capacity()
        return WarehouseCapacityResponse(success=True, daily_capacity=capacity)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取仓库产能失败: {str(e)}")


@public_router.post(
    "/purchase-quantity/query",
    summary="AI 预测报货数量：统一查询",
    description=(
        "**无需登录**：不要求也不校验 `Authorization` Bearer Token，可被内网脚本或对接方直接调用。\n\n"
        "**请求体须为合法 JSON**（`Content-Type: application/json`）："
        "键名必须用 **英文双引号** 包裹；不要用 Python/JavaScript 单引号；不要尾逗号；不要注释。"
        "若出现 **422** 且 `json_invalid` / `Expecting property name enclosed in double quotes`，"
        "说明正文不是标准 JSON，请对照下方示例修正。\n\n"
        "**前提**：库表 `pd_allocation_predictions` 中须已有预测数据（由 **GET /api/v1/allocation/plan** 生成并写入，该接口同样无需登录；"
        "若从未调用或表为空，则 `plan` 恒为 `{}`，`message` 会提示先生成计划）。\n\n"
        "一次请求返回 `warehouse_options` 与 `plan`（仓库→合同→冶炼厂→日期→车数），"
        "数据来自最近一次写入的快照，按 `delivery_date` 落在请求区间内筛选。"
    ),
    response_description="校验参数、服务端筛选，返回下拉仓库列表与四层嵌套 plan",
    response_model=PurchaseQuantityQueryEnvelope,
)
async def post_purchase_quantity_query(
    body: PurchaseQuantityQueryRequest = Body(
        openapi_examples={
            "仅日期区间": {
                "summary": "最简：只传起止日，其余可省略或为 null",
                "value": {
                    "start_date": "2026-04-01",
                    "end_date": "2026-04-07",
                    "warehouse": None,
                    "contract_no": None,
                    "smelter": None,
                },
            },
            "带筛选条件": {
                "summary": "指定仓库 / 合同子串 / 冶炼厂关键字",
                "value": {
                    "start_date": "2026-04-01",
                    "end_date": "2026-04-30",
                    "warehouse": "河南金铅仓库",
                    "contract_no": "HT-2024",
                    "smelter": "金利",
                },
            },
        }
    ),
):
    """
    统一 JSON：`{ "success", "message", "data" }`；`data` 含 `warehouse_options` 与四层嵌套 `plan`
    （仓库→合同→冶炼厂→日期→车数）。数据来自最近一次 `pd_allocation_predictions` 快照，
    按 `delivery_date` 落在 [start_date, end_date] 筛选；区间内每个日期均有键，无预测为 0。
    失败时 `success` 为 false、`data` 为 null，HTTP 仍为 200（与 `/t1/get_purchase_suggestion` 一致）。
    本接口为公开调用，不传登录用户：`warehouse_options` 为**全部启用仓库**（不按大区经理裁剪）。
    若需「仅本人负责仓库」，请使用带登录态的同逻辑接口（如兼容路由）或后续单独封装。
    """
    raw = query_ai_purchase_quantity(
        body.start_date,
        body.end_date,
        warehouse=body.warehouse,
        contract_no=body.contract_no,
        smelter=body.smelter,
        current_user=None,
    )
    if raw.get("success") and raw.get("data") is not None:
        payload = PurchaseQuantityDataPayload(**raw["data"])
        return PurchaseQuantityQueryEnvelope(
            success=True,
            message=raw.get("message") or "",
            data=payload,
        )
    return PurchaseQuantityQueryEnvelope(
        success=False,
        message=raw.get("message") or "查询失败",
        data=None,
    )


@router.get(
    "/manager-daily-demand",
    response_model=ManagerDailyDemandResponse,
    summary="分配需求：大区经理每日运货量",
)
async def get_manager_daily_demand():
    """
    依据生效中合同在该报货计划上的生效期限（多份合同取最早起始、最晚截止；
    无截止日期时按签订日起 4 天），叠加报货计划 `plan_start_date`，
    将订货计划（待审核/审核通过）剩余车数在有效日内均分，按大区经理汇总每日吨数与车数。

    仅返回**当日及未来**的日期；已无剩余车数的订货计划不参与。
    """
    try:
        result = compute_manager_daily_allocation()
        if not result.get("success"):
            raise HTTPException(
                status_code=500,
                detail=result.get("error") or "计算分配需求失败",
            )
        return ManagerDailyDemandResponse(
            success=True,
            tonnage_per_truck=int(result.get("tonnage_per_truck") or 35),
            days=result.get("days") or [],
            meta=result.get("meta") or {},
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"分配需求计算失败: {str(e)}")


@router.get(
    "/contracts",
    summary="获取生效合同列表（排产输入）",
    response_description="已按默认规则扣减已发车的合同需求，供排产使用",
    response_model=ActiveContractsListResponse,
)
async def get_active_contracts_list():
    """获取所有生效中的合同(含已发车调整)"""
    try:
        contracts = get_active_contracts()
        contracts_data = [
            ActiveContractItemResponse(
                contract_no=c.contract_no,
                smelter=c.smelter,
                total_tons=c.total_tons,
                total_trucks=c.total_trucks,
                start_date=c.start_date,
                end_date=c.end_date,
            )
            for c in contracts
        ]
        return ActiveContractsListResponse(
            success=True,
            contracts=contracts_data,
            count=len(contracts),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取合同列表失败: {str(e)}")


@router.post(
    "/test_plan",
    summary="一键联调：造数并生成计划",
    response_description="含测试数据统计、排产 plan 与 meta",
)
async def test_plan(
    num_contracts: int = Query(
        3,
        ge=1,
        le=10,
        title="测试合同数量",
        description="本次插入的测试合同条数",
    ),
    H: int = Query(
        7,
        ge=1,
        le=30,
        title="规划窗口天数",
        description="排产覆盖的天数",
    ),
):
    """
    测试完整流程: 插入测试数据 + 生成调度计划

    自动执行:
    1. 清理旧的TESTPLAN测试数据
    2. 插入新的测试合同
    3. 生成调度计划
    """
    try:
        managers, smelters, contracts = get_filter_options()
        return {
            "success": True,
            "regional_managers": managers,
            "smelters": smelters,
            "contracts": contracts
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取筛选选项失败: {str(e)}")

