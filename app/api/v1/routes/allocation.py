"""
分配规划路由
支持生成调度计划、查看优化结果、测试数据管理
"""
from datetime import datetime, timedelta
from typing import Optional
import random
from decimal import Decimal

from fastapi import APIRouter, HTTPException, Query, Body
from pydantic import BaseModel, Field

from app.services.allocation_service import (
    get_active_contracts,
    get_warehouses,
    get_warehouse_daily_capacity,
    solve_dispatch_plan
)
from app.services.contract_service import get_conn


router = APIRouter(prefix="/allocation", tags=["分配规划"])


# ============ 响应模型 ============

class AllocationPlanResponse(BaseModel):
    """分配计划响应"""
    success: bool = True
    message: str = "调度计划生成成功"
    plan: dict  # {仓库: {合同编号: {冶炼厂: {日期: 车数}}}}
    meta: dict  # 元数据


class ContractStatusResponse(BaseModel):
    """合同状态响应"""
    contract_no: str
    smelter_company: str
    total_quantity: float
    total_trucks: int
    delivered_trucks: int
    remaining_trucks: int


class ContractsStatusResponse(BaseModel):
    """合同状态列表响应"""
    success: bool = True
    contracts: list[ContractStatusResponse]


class SetupTestDataRequest(BaseModel):
    """设置测试数据请求"""
    num_contracts: int = Field(5, ge=1, le=20, description="合同数量")
    num_deliveries_per_contract: int = Field(2, ge=0, le=5, description="每个合同的报单数量")
    num_weighbills_per_contract: int = Field(1, ge=0, le=3, description="每个合同的磅单数量")
    contract_prefix: str = Field("TEST", description="合同编号前缀")


class SetupTestDataResponse(BaseModel):
    """设置测试数据响应"""
    success: bool = True
    message: str
    inserted_contracts: int
    inserted_deliveries: int
    inserted_weighbills: int


class CleanupTestDataResponse(BaseModel):
    """清理测试数据响应"""
    success: bool = True
    message: str
    deleted_contracts: int
    deleted_deliveries: int
    deleted_weighbills: int


# ============ 辅助函数 ============

def _get_db_conn():
    """获取数据库连接（兼容旧代码）"""
    return get_conn()


def _setup_warehouses():
    """设置仓库"""
    warehouses = ['河南金铅仓库', '河北仓库', '山东仓库', '山西仓库']

    with _get_db_conn() as conn:
        with conn.cursor() as cur:
            for name in warehouses:
                try:
                    cur.execute(
                        'INSERT INTO pd_warehouses (warehouse_name, is_active, created_at, updated_at) '
                        'VALUES (%s, 1, NOW(), NOW())',
                        (name,)
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

    with _get_db_conn() as conn:
        with conn.cursor() as cur:
            for i in range(num_contracts):
                contract_no = f"{prefix}_{datetime.now().strftime('%Y%m%d')}_{i+1:03d}"
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


# ============ 路由 ============

@router.post("/test-data/setup", response_model=SetupTestDataResponse)
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
        # 1. 设置仓库
        _setup_warehouses()

        # 2. 插入测试合同
        contracts = _insert_test_contracts(
            num_contracts=request.num_contracts,
            prefix=request.contract_prefix
        )

        # 3. 插入测试报单
        deliveries_count = _insert_test_deliveries(
            contracts=contracts,
            max_per_contract=request.num_deliveries_per_contract
        )

        # 4. 插入测试磅单
        weighbills_count = _insert_test_weighbills(
            contracts=contracts,
            max_per_contract=request.num_weighbills_per_contract
        )

        return SetupTestDataResponse(
            success=True,
            message=f"测试数据设置成功: {len(contracts)}个合同, {deliveries_count}个报单, {weighbills_count}个磅单",
            inserted_contracts=len(contracts),
            inserted_deliveries=deliveries_count,
            inserted_weighbills=weighbills_count
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"设置测试数据失败: {str(e)}")


@router.post("/test-data/cleanup", response_model=CleanupTestDataResponse)
async def cleanup_test_data(prefix: str = "TEST"):
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


@router.get("/status", response_model=ContractsStatusResponse)
async def get_contracts_status():
    """
    获取所有生效中合同的状态(含已发车数)

    返回每个合同的:
    - 原始需求车数
    - 已发车数
    - 剩余车数
    """
    contracts_status = []

    try:
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


@router.get("/plan", response_model=AllocationPlanResponse)
async def generate_allocation_plan(
    window_start: Optional[str] = Query(
        None,
        description="规划窗口起始日期(YYYY-MM-DD),默认今天"
    ),
    H: int = Query(
        10,
        ge=1,
        le=30,
        description="规划窗口天数(1-30天)"
    ),
    as_of_date: Optional[str] = Query(
        None,
        description="截至日期(用于计算已发车数),默认与window_start相同"
    ),
    include_solver_log: bool = Query(
        False,
        description="是否包含求解器日志"
    )
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
            solver_msg=include_solver_log
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
            raise HTTPException(status_code=500, detail=f"规划求解失败: {status}")

        return AllocationPlanResponse(
            success=True,
            message="调度计划生成成功",
            plan=plan,
            meta=meta
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"生成调度计划失败: {str(e)}")


@router.get("/warehouses")
async def get_warehouses_list():
    """获取所有仓库列表"""
    try:
        warehouses = get_warehouses()
        return {
            "success": True,
            "warehouses": warehouses,
            "count": len(warehouses)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取仓库列表失败: {str(e)}")


@router.get("/capacity")
async def get_warehouse_capacity():
    """获取各仓库每日发货能力"""
    try:
        capacity = get_warehouse_daily_capacity()
        return {
            "success": True,
            "daily_capacity": capacity
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取仓库产能失败: {str(e)}")


@router.get("/contracts")
async def get_active_contracts_list():
    """获取所有生效中的合同(含已发车调整)"""
    try:
        contracts = get_active_contracts()
        contracts_data = [
            {
                "contract_no": c.contract_no,
                "smelter": c.smelter,
                "total_tons": c.total_tons,
                "total_trucks": c.total_trucks,
                "start_date": c.start_date,
                "end_date": c.end_date
            }
            for c in contracts
        ]
        return {
            "success": True,
            "contracts": contracts_data,
            "count": len(contracts)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取合同列表失败: {str(e)}")


@router.post("/test_plan")
async def test_plan(
    num_contracts: int = Query(3, ge=1, le=10, description="测试合同数量"),
    H: int = Query(7, ge=1, le=30, description="规划窗口天数")
):
    """
    测试完整流程: 插入测试数据 + 生成调度计划

    自动执行:
    1. 清理旧的TESTPLAN测试数据
    2. 插入新的测试合同
    3. 生成调度计划
    """
    try:
        prefix = "TESTPLAN"

        # 1. 清理旧数据
        _cleanup_test_data(prefix=prefix)

        # 2. 设置仓库
        _setup_warehouses()

        # 3. 插入测试合同
        contracts = _insert_test_contracts(num_contracts=num_contracts, prefix=prefix)

        # 4. 生成调度计划
        window_start = datetime.now().strftime("%Y-%m-%d")
        contracts_data = get_active_contracts(as_of_date=window_start)
        warehouses = get_warehouses()
        daily_cap = get_warehouse_daily_capacity()
        window_end = (datetime.now() + timedelta(days=H - 1)).strftime("%Y-%m-%d")

        plan, status = solve_dispatch_plan(
            contracts=contracts_data,
            warehouses=warehouses,
            daily_cap=daily_cap,
            window_start=window_start,
            window_end=window_end,
            solver_msg=False
        )

        return {
            "success": True,
            "message": f"测试完成: 创建{len(contracts)}个合同并生成{H}天调度计划",
            "test_data": {
                "inserted_contracts": len(contracts),
                "contract_prefix": prefix
            },
            "plan": plan,
            "meta": {
                "solver_status": status,
                "window_start": window_start,
                "window_end": window_end,
                "H": H
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"测试流程失败: {str(e)}")
