import pandas as pd
import io
import re
import os
import shutil
import uuid
import json
from datetime import datetime
from pathlib import Path
from fastapi import HTTPException, APIRouter, Depends, Query, UploadFile, File, Form
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List
from datetime import date, datetime
from decimal import Decimal
from enum import IntEnum

from app.core.paths import UPLOADS_DIR
from app.services.payment_services import PaymentExcelProcessor
from core.database import get_conn
from core.logging import get_logger
from core.auth import get_current_user
from app.services.payment_services import (
    PaymentService,
    PaymentStage,
    calculate_payment_amount,
    WEIGHBILL_NO_PATTERNS,  # 从 services 导入
    AMOUNT_PATTERNS,       # 从 services 导入
)

logger = get_logger(__name__)

# 上传目录配置
PAYMENT_UPLOAD_DIR = UPLOADS_DIR / "payments"
PAYMENT_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# 允许的文件扩展名
ALLOWED_EXTENSIONS = {'.xlsx', '.xls'}
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB
# ========== Pydantic 模型定义 ==========

class PaymentStageEnum(IntEnum):
    """回款阶段"""
    DEPOSIT = 0      # 定金
    DELIVERY = 1     # 到货款（90%）
    FINAL = 2        # 尾款（10%）


class CreatePaymentReq(BaseModel):
    """创建收款明细请求"""
    model_config = ConfigDict(json_schema_extra={
        "example": {
            "sales_order_id": 1001,
            "smelter_name": "某某冶炼厂",
            "contract_no": "HT-2025-001",
            "unit_price": 15000.00,
            "net_weight": 100.50,
            "material_name": "铜精矿",
            "remark": "第一季度供货"
        }
    })

    sales_order_id: int = Field(..., description="销售订单ID")
    smelter_name: str = Field(..., description="冶炼厂名称")
    contract_no: str = Field(..., description="合同编号")
    unit_price: float = Field(..., gt=0, description="合同单价（元/吨）")
    net_weight: float = Field(..., gt=0, description="净重（吨）")
    material_name: Optional[str] = Field(None, description="物料名称")
    remark: Optional[str] = Field(None, description="备注")


class RecordPaymentReq(BaseModel):
    """录入回款请求"""
    model_config = ConfigDict(json_schema_extra={
        "example": {
            "payment_detail_id": 1,
            "contract_no": "HT-2025-001",
            "vehicle_no": "冀A12345",
            "product_name": "电动车",
            "payment_amount": 1356750.00,
            "payment_stage": 1,
            "payment_date": "2025-02-24",
            "payment_method": "银行转账",
            "transaction_no": "TRX20250224001",
            "remark": "到货款90%"
        }
    })

    payment_detail_id: Optional[int] = Field(None, gt=0, description="收款明细ID，已知时优先使用")
    weighbill_id: Optional[int] = Field(None, gt=0, description="磅单ID，可选")
    delivery_id: Optional[int] = Field(None, gt=0, description="报单ID，可选")
    contract_no: Optional[str] = Field(None, description="合同编号，用于自动匹配")
    vehicle_no: Optional[str] = Field(None, description="车号，用于自动匹配")
    product_name: Optional[str] = Field(None, description="品种，用于自动匹配")
    payment_amount: float = Field(..., gt=0, description="回款金额")
    payment_stage: PaymentStageEnum = Field(PaymentStageEnum.DELIVERY, description="回款阶段：0-定金, 1-到货款(90%), 2-尾款(10%)")
    payment_date: Optional[date] = Field(None, description="回款日期，默认今天")
    payment_method: Optional[str] = Field(None, description="支付方式")
    transaction_no: Optional[str] = Field(None, description="交易流水号")
    remark: Optional[str] = Field(None, description="备注")


class UpdatePaymentReq(BaseModel):
    """更新收款明细请求"""
    smelter_name: Optional[str] = Field(None, description="冶炼厂名称")
    contract_no: Optional[str] = Field(None, description="合同编号")
    material_name: Optional[str] = Field(None, description="物料名称")
    remark: Optional[str] = Field(None, description="备注")


class UpdatePaymentStatusReq(BaseModel):
    """手动更新付款状态请求"""
    is_paid: Optional[int] = Field(None, description="是否回款：0-否, 1-是")
    is_paid_out: Optional[int] = Field(None, description="是否支付：0-待打款, 1-已打款")


class PaymentListQuery(BaseModel):
    """收款明细列表查询参数"""
    page: int = Field(1, ge=1, description="页码")
    size: int = Field(20, ge=1, le=100, description="每页数量")
    status: Optional[int] = Field(None, ge=0, le=3, description="状态筛选：0-未回款, 1-部分回款, 2-已结清, 3-超额回款")
    smelter_name: Optional[str] = Field(None, description="冶炼厂名称筛选")
    contract_no: Optional[str] = Field(None, description="合同编号筛选")
    start_date: Optional[date] = Field(None, description="开始日期")
    end_date: Optional[date] = Field(None, description="结束日期")
    keyword: Optional[str] = Field(None, description="关键词搜索（冶炼厂/合同号/物料）")


class PaymentResp(BaseModel):
    """收款明细响应 - 包含打款信息列表所需全部字段"""
    model_config = ConfigDict(from_attributes=True)

    # ========== 基础信息 ==========
    id: int
    sales_order_id: int
    smelter_name: str
    contract_no: str
    material_name: Optional[str]
    unit_price: float
    net_weight: float
    total_amount: float
    paid_amount: float                    # 已打款金额
    unpaid_amount: float
    status: int
    status_name: str
    remark: Optional[str]
    created_at: datetime
    updated_at: datetime

    # ========== 回款阶段金额（用于收款明细） ==========
    arrival_payment_amount: Optional[float] = None
    final_payment_amount: Optional[float] = None
    arrival_paid_amount: Optional[float] = None
    final_paid_amount: Optional[float] = None
    collection_status: Optional[int] = None
    collection_status_name: Optional[str] = None
    last_payment_date: Optional[str] = None

    # ========== 支付状态字段（打款信息列表核心字段） ==========
    is_paid: Optional[int] = Field(None, description="是否回款：0-否, 1-是")
    is_paid_out: Optional[int] = Field(None, description="是否支付：0-待打款, 1-已打款")

    # ========== 排期信息（第一行） ==========
    payment_schedule_date: Optional[str] = Field(None, description="排期日期")

    # ========== 磅单字段（第二行信息） ==========
    weighbill_id: Optional[int] = None
    weigh_date: Optional[str] = Field(None, description="磅单日期")
    delivery_time: Optional[str] = None
    weigh_ticket_no: Optional[str] = Field(None, description="过磅单号")
    weighbill_vehicle_no: Optional[str] = Field(None, description="车号")
    weighbill_product_name: Optional[str] = Field(None, description="品种")
    gross_weight: Optional[float] = None
    tare_weight: Optional[float] = None
    weighbill_net_weight: Optional[float] = Field(None, description="净重")
    weighbill_unit_price: Optional[float] = Field(None, description="采购单价")
    weighbill_total_amount: Optional[float] = Field(None, description="应打款金额")
    weighbill_image: Optional[str] = None
    ocr_status: Optional[str] = None
    is_manual_corrected: Optional[int] = None
    weighbill_uploader_id: Optional[int] = None
    weighbill_uploader_name: Optional[str] = None
    weighbill_uploaded_at: Optional[str] = None

    # ========== 销售台账/报货订单字段（第一行信息） ==========
    delivery_id: Optional[int] = None
    report_date: Optional[str] = Field(None, description="报单日期")
    warehouse: Optional[str] = None
    target_factory_id: Optional[int] = None
    target_factory_name: Optional[str] = Field(None, description="报送冶炼厂")
    delivery_quantity: Optional[float] = None
    delivery_vehicle_no: Optional[str] = None
    driver_name: Optional[str] = Field(None, description="司机姓名")
    driver_phone: Optional[str] = Field(None, description="司机电话")
    driver_id_card: Optional[str] = Field(None, description="身份证号")
    has_delivery_order: Optional[str] = Field(None, description="是否自带联单：是/否")
    delivery_order_image: Optional[str] = None
    delivery_upload_status: Optional[str] = Field(None, description="是否上传联单：已上传/未上传")
    source_type: Optional[str] = None
    shipper: Optional[str] = Field(None, description="报单人/发货人（大区经理、仓库）")
    service_fee: Optional[float] = None
    delivery_fee: Optional[float] = Field(None, description="联单费：无联单时为150，有联单时为service_fee")
    delivery_contract_no: Optional[str] = None
    delivery_contract_unit_price: Optional[float] = None
    delivery_total_amount: Optional[float] = None
    delivery_status: Optional[str] = None
    delivery_uploader_id: Optional[int] = None
    delivery_uploader_name: Optional[str] = None
    delivery_uploaded_at: Optional[str] = None

    # ========== 收款人信息 ==========
    payee: Optional[str] = Field(None, description="收款人")
    payee_account: Optional[str] = Field(None, description="收款人账号")


class PaymentListResp(BaseModel):
    """收款明细列表响应 - 用于打款信息列表"""
    total: int
    page: int
    size: int
    items: List[PaymentResp]


class PaymentRecordResp(BaseModel):
    """回款记录响应"""
    id: int
    payment_amount: float
    payment_stage: int
    payment_stage_name: str
    payment_date: date
    payment_method: Optional[str]
    transaction_no: Optional[str]
    remark: Optional[str]
    created_at: datetime


class PaymentDetailResp(PaymentResp):
    """收款明细详情响应（含回款记录）"""
    payment_records: List[PaymentRecordResp]
    payment_count: int


class PaymentResultResp(BaseModel):
    """录入回款结果响应"""
    payment_detail_id: int
    total_amount: float
    paid_amount: float
    unpaid_amount: float
    status: int
    status_name: str
    current_payment: float
    payment_stage: int
    payment_stage_name: str


class PaymentStatsResp(BaseModel):
    """回款统计响应"""
    total_count: int
    total_amount: float
    total_paid: float
    total_unpaid: float
    collection_rate: float
    status_breakdown: List[dict]

class ContractShippingProgressResp(BaseModel):
    """合同发运进度响应"""
    contract_no: str
    smelter_name: str
    total_vehicles: int              # 总车数
    planned_total_weight: float      # 计划总吨数
    shipped_vehicles: int            # 已运车数
    remaining_vehicles: int          # 剩余车数
    shipped_weight: float            # 已运吨数
    remaining_weight: float          # 剩余吨数
    last_ship_date: Optional[str]
    progress_rate: float             # 发运进度百分比


class ContractPaymentSummaryResp(BaseModel):
    """合同回款汇总响应"""
    contract_no: str
    smelter_name: str
    order_count: int                 # 订单数量
    total_receivable: float          # 应收总额
    total_received: float            # 已收总额
    total_unreceived: float          # 未收总额
    collection_rate: float           # 回款率
    contract_status: int             # 合同整体状态
    contract_status_name: str
    status_breakdown: dict           # 状态分布
    last_payment_date: Optional[str]


class ContractOrderDetail(BaseModel):
    """合同下订单明细"""
    id: int
    sales_order_id: int
    material_name: Optional[str]
    unit_price: float
    net_weight: float
    total_amount: float
    paid_amount: float
    unpaid_amount: float
    arrival_payment_amount: Optional[float] = None
    final_payment_amount: Optional[float] = None
    arrival_paid_amount: Optional[float] = None
    final_paid_amount: Optional[float] = None
    collection_status: Optional[int] = None
    status: int
    status_name: Optional[str]
    remark: Optional[str]
    created_at: Optional[str]
    weigh_ticket_no: Optional[str]
    weigh_date: Optional[str]
    shipped_weight: Optional[float]
    payment_record_count: int


class ContractPaymentDetailResp(BaseModel):
    """合同回款明细响应"""
    contract_info: dict
    total_orders: int
    page: int
    size: int
    orders: List[ContractOrderDetail]
    payment_records: List[PaymentRecordResp]
    payment_record_count: int

class UpdateCollectionReq(BaseModel):
    """编辑回款请求（金利分阶段日期，豫光单一日期）"""
    arrival_paid_amount: Optional[float] = Field(None, ge=0, description="已回款首笔金额（覆盖模式，一次付清）")
    final_paid_amount: Optional[float] = Field(None, ge=0, description="本次回尾款金额（累加模式，可多次回款，系统会自动累加到已有尾款上）")
    arrival_payment_date: Optional[str] = Field(None, description="首笔回款日期，格式：YYYY-MM-DD（金利必填，豫光可用）")
    final_payment_date: Optional[str] = Field(None, description="本次尾款回款日期，格式：YYYY-MM-DD（仅金利使用）")
    payment_date: Optional[str] = Field(None, description="回款日期，格式：YYYY-MM-DD（兼容旧接口，豫光可用）")
    remark: Optional[str] = Field(None, description="备注")

class UploadResponse(BaseModel):
    """上传响应模型"""
    success: bool
    message: str
    data: Optional[dict] = None

class PaymentExcelImportReq(BaseModel):
    """Excel导入请求"""
    file_id: str = Field(..., description="上传文件ID（通过/upload-excel接口获取）")
    company_type: Optional[str] = Field(None, description="公司类型：yuguang-豫光, jinli-金利，不传则自动检测")


class PaymentExcelImportResp(BaseModel):
    """Excel导入响应"""
    success: bool
    message: str
    total_rows: int
    success_count: int
    fail_count: int
    details: List[dict]
# ========== 路由定义 ==========

router = APIRouter(tags=["收款明细管理"])


def register_pd_payment_routes(app):
    """注册收款明细路由到主应用"""
    app.include_router(router, prefix="/api/v1/payment")


def check_finance_permission(current_user: dict):
    """检查是否为财务人员（财务/会计/管理员）"""
    allowed_roles = ["管理员", "财务", "会计"]
    if current_user.get("role") not in allowed_roles:
        raise HTTPException(status_code=403, detail="仅财务人员可操作")


def check_admin_or_finance_permission(current_user: dict):
    """检查是否为管理员或财务"""
    allowed_roles = ["管理员", "财务"]
    if current_user.get("role") not in allowed_roles:
        raise HTTPException(status_code=403, detail="权限不足，需要管理员或财务权限")


# ========== 收款明细管理接口 ==========

@router.post("/details", summary="创建收款明细", response_model=dict)
def create_payment_detail(
    body: CreatePaymentReq,
    current_user: dict = Depends(get_current_user)
):
    """
    创建收款明细台账（根据销售业务数据生成）

    - 根据销售订单ID、冶炼厂、合同等信息生成
    - 自动计算回款总额 = 合同单价 × 净重
    - 初始状态为"未回款"
    """
    check_finance_permission(current_user)

    try:
        payment_id = PaymentService.create_payment_detail(
            sales_order_id=body.sales_order_id,
            smelter_name=body.smelter_name,
            contract_no=body.contract_no,
            unit_price=Decimal(str(body.unit_price)),
            net_weight=Decimal(str(body.net_weight)),
            material_name=body.material_name,
            remark=body.remark,
            created_by=current_user.get("id")
        )

        # 计算总额用于返回
        total_amount = calculate_payment_amount(
            Decimal(str(body.unit_price)),
            Decimal(str(body.net_weight))
        )

        return {
            "msg": "创建收款明细成功",
            "payment_id": payment_id,
            "total_amount": float(total_amount),
            "status": 0,
            "status_name": "未回款"
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("创建收款明细异常")
        raise HTTPException(status_code=500, detail="创建收款明细失败")


@router.get("/details", summary="回款信息列表", response_model=dict)
def list_payment_details(
    page: int = Query(1, ge=1, description="页码"),
    size: int = Query(20, ge=1, le=100, description="每页数量"),
    status: Optional[int] = Query(None, ge=0, le=3, description="回款明细状态筛选"),
    smelter_name: Optional[str] = Query(None, description="冶炼厂名称"),
    contract_no: Optional[str] = Query(None, description="合同编号"),
    start_date: Optional[date] = Query(None, description="开始日期"),
    end_date: Optional[date] = Query(None, description="结束日期"),
    keyword: Optional[str] = Query(None, description="关键词搜索"),
    # 回款列表筛选参数
    collection_status: Optional[int] = Query(None, ge=0, le=2, description="回款状态筛选：0-待回款, 1-已回首笔待回尾款, 2-已回尾款"),
    current_user: dict = Depends(get_current_user)
):
    """
    获取回款信息列表
    
    只返回已上传磅单的数据，包含销售相关的回款字段：
    - 第一行：合同编号、报单日期、报送冶炼厂、司机电话、司机姓名、车号、品种、是否自带联单、是否上传联单、报单人/发货人
    - 第二行：磅单日期、过磅单号、净重
    - 第三行：销售单价、应回款首笔金额、应回款尾款金额、已回款首笔金额、已回款尾款金额、回款日期
    - 第四行：回款状态、操作
    
    回款状态说明：
    - 0: 待回款（已上传磅单后默认）
    - 1: 已回首笔待回尾款
    - 2: 已回尾款
    """
    check_finance_permission(current_user)

    try:
        result = PaymentService.list_payment_details(
            page=page,
            size=size,
            status=status,
            smelter_name=smelter_name,
            contract_no=contract_no,
            start_date=start_date,
            end_date=end_date,
            keyword=keyword,
            collection_status=collection_status
        )
        return result

    except Exception:
        logger.exception("查询回款信息列表异常")
        raise HTTPException(status_code=500, detail="查询失败")
    
@router.get("/payment-out", summary="打款信息列表（打款排期列表）", response_model=dict)
def list_payment_out_details(
    page: int = Query(1, ge=1, description="页码"),
    size: int = Query(20, ge=1, le=100, description="每页数量"),
    status: Optional[int] = Query(None, ge=0, le=3, description="状态筛选"),
    smelter_name: Optional[str] = Query(None, description="冶炼厂名称"),
    contract_no: Optional[str] = Query(None, description="合同编号"),
    start_date: Optional[date] = Query(None, description="开始日期"),
    end_date: Optional[date] = Query(None, description="结束日期"),
    keyword: Optional[str] = Query(None, description="关键词搜索"),
    # 打款列表筛选参数
    is_paid_out: Optional[int] = Query(None, ge=0, le=1, description="打款状态筛选：0-待打款, 1-已打款"),
    payment_schedule_date: Optional[str] = Query(None, description="排期日期筛选"),
    has_schedule: Optional[int] = Query(None, ge=0, le=1, description="排期状态筛选：0-待排期, 1-已排期"),
    current_user: dict = Depends(get_current_user)
):
    """
    获取打款信息列表（打款排期列表）
    
    只返回已排期的数据，包含采购相关的打款字段：
    - 第一行：排款日期
    - 第二行：合同编号、报单日期、报送冶炼厂、司机电话、司机姓名、车号、品种、是否自带联单、是否上传联单、报单人/发货人
    - 第三行：磅单日期、过磅单号、净重
    - 第四行：采购单价、应打款金额、已打款金额、收款人、收款人账号
    - 第五行：应回款首笔金额、应回款尾款金额、已回款首笔金额、已回款尾款金额、回款日期、回款状态
    - 第六行：打款状态、排期状态、操作
    
    打款状态说明：
    - 0: 待打款
    - 1: 已打款
    
    排期状态说明：
    - 已排期：已设置排款日期
    - 待排期：未设置排款日期
    """
    check_finance_permission(current_user)

    try:
        result = PaymentService.list_payment_out_details(
            page=page,
            size=size,
            status=status,
            smelter_name=smelter_name,
            contract_no=contract_no,
            start_date=start_date,
            end_date=end_date,
            keyword=keyword,
            is_paid_out=is_paid_out,
            payment_schedule_date=payment_schedule_date,
            has_schedule=has_schedule
        )
        return result

    except Exception:
        logger.exception("查询打款信息列表异常")
        raise HTTPException(status_code=500, detail="查询失败")


@router.put("/details/{payment_id}/collection", summary="编辑回款信息", response_model=dict)
def update_collection_payment(
        payment_id: int,
        body: UpdateCollectionReq,
        current_user: dict = Depends(get_current_user)
):
    """
    编辑回款信息（金利分阶段日期，豫光单一日期）
    - payment_id填写实际为payment_detail_id
    
    填写已回款首笔金额、已回款尾款金额，自动：
    - 计算 paid_amount = 首笔 + 尾款
    - 计算 unpaid_amount = 总额 - 已付
    - 判断回款状态
    - 同步更新回款记录

    金利冶炼厂：
    - 分阶段回款：首笔（约90%）+ 尾款（约10%）
    - 需要分别录入首笔回款日期和尾款回款日期
    - 字段：arrival_paid_amount + arrival_payment_date, final_paid_amount + final_payment_date
    
    豫光冶炼厂：
    - 一次性回款：只录入首笔金额，尾款自动为0
    - 只需要一个回款日期
    - 字段：arrival_paid_amount + arrival_payment_date（或 payment_date）
    
    请求示例-金利：
    {
        "arrival_paid_amount": 90000.00,
        "final_paid_amount": 10000.00,
        "arrival_payment_date": "2026-03-01",
        "final_payment_date": "2026-03-15",
        "remark": "分阶段回款"
    }
    
    请求示例-豫光：
    {
        "arrival_paid_amount": 100000.00,
        "arrival_payment_date": "2026-03-01",
        "remark": "一次性回款"
    }
    """
    check_finance_permission(current_user)

    try:
        result = PaymentService.update_collection_payment(
            payment_id=payment_id,
            arrival_paid_amount=body.arrival_paid_amount,
            final_paid_amount=body.final_paid_amount,
            arrival_payment_date=body.arrival_payment_date,
            final_payment_date=body.final_payment_date,
            payment_date=body.payment_date,
            remark=body.remark,
            updated_by=current_user.get("id")
        )

        return {
            "success": True,
            "msg": "回款更新成功",
            "data": result
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("编辑回款异常")
        raise HTTPException(status_code=500, detail="更新失败")


@router.get("/details/{payment_id}", summary="收款明细详情", response_model=PaymentDetailResp)
def get_payment_detail(
    payment_id: int,
    current_user: dict = Depends(get_current_user)
):
    """
    获取收款明细详情（包含所有回款记录）
    """
    check_finance_permission(current_user)

    detail = PaymentService.get_payment_detail(payment_id)
    if not detail:
        raise HTTPException(status_code=404, detail="收款明细不存在")

    return detail


@router.put("/details/{payment_id}", summary="更新收款明细")
def update_payment_detail(
    payment_id: int,
    body: UpdatePaymentReq,
    current_user: dict = Depends(get_current_user)
):
    """
    更新收款明细基础信息

    注意：不允许修改金额相关字段（单价、重量、总额等）
    如需修改金额，请删除后重新创建或联系管理员
    """
    check_finance_permission(current_user)

    try:
        PaymentService.update_payment_detail(
            payment_id=payment_id,
            smelter_name=body.smelter_name,
            contract_no=body.contract_no,
            material_name=body.material_name,
            remark=body.remark,
            updated_by=current_user.get("id")
        )
        return {"msg": "更新成功"}

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("更新收款明细异常")
        raise HTTPException(status_code=500, detail="更新失败")


@router.put("/details/{payment_id}/status", summary="手动更新付款状态")
def update_payment_status(
    payment_id: int,
    body: UpdatePaymentStatusReq,
    current_user: dict = Depends(get_current_user)
):
    """
    手动更新付款状态（支持人工干预）
    
    - is_paid: 是否回款（0-否, 1-是）
    - is_paid_out: 是否支付（0-待打款, 1-已打款）
    
    注意：此接口用于人工修正状态，正常情况下状态由系统自动更新
    """
    check_finance_permission(current_user)

    try:
        result = PaymentService.update_payment_status(
            payment_id=payment_id,
            is_paid=body.is_paid,
            is_paid_out=body.is_paid_out,
            updated_by=current_user.get("id")
        )
        return {"msg": "状态更新成功", "data": result}

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("更新付款状态异常")
        raise HTTPException(status_code=500, detail="更新失败")


@router.delete("/details/{payment_id}", summary="删除收款明细")
def delete_payment_detail(
    payment_id: int,
    current_user: dict = Depends(get_current_user)
):
    """
    删除收款明细

    注意：已有回款记录的明细无法删除
    """
    check_admin_or_finance_permission(current_user)

    try:
        PaymentService.delete_payment_detail(payment_id)
        return {"msg": "删除成功"}

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("删除收款明细异常")
        raise HTTPException(status_code=500, detail="删除失败")


# ========== 合同发运进度接口（静态路由放在动态路由之前） ==========

@router.get("/contracts/shipping-progress", summary="合同发运进度列表", response_model=dict)
def list_contract_shipping_progress(
    contract_no: Optional[str] = Query(None, description="合同编号筛选"),
    smelter_name: Optional[str] = Query(None, description="冶炼厂名称筛选"),
    page: int = Query(1, ge=1, description="页码"),
    size: int = Query(20, ge=1, le=100, description="每页数量"),
    current_user: dict = Depends(get_current_user)
):
    """
    获取合同发运进度列表
    
    统计每个合同的发运情况：
    - 总车数、总吨数（计划）
    - 已运车数、已运吨数（根据磅单）
    - 剩余车数、剩余吨数
    - 发运进度百分比
    
    关联逻辑：合同 -> 销售订单 -> 磅单
    """
    check_finance_permission(current_user)
    
    try:
        result = PaymentService.get_contract_shipping_progress(
            contract_no=contract_no,
            smelter_name=smelter_name,
            page=page,
            size=size
        )
        return {
            "msg": "查询成功",
            "data": result
        }
    except Exception:
        logger.exception("查询合同发运进度异常")
        raise HTTPException(status_code=500, detail="查询失败")


# ========== 合同回款汇总接口（静态路由放在动态路由之前） ==========

@router.get("/contracts/payment-summary", summary="合同回款汇总列表", response_model=dict)
def list_contract_payment_summary(
    contract_no: Optional[str] = Query(None, description="合同编号筛选"),
    smelter_name: Optional[str] = Query(None, description="冶炼厂名称筛选"),
    status: Optional[int] = Query(None, ge=0, le=3, description="状态筛选"),
    page: int = Query(1, ge=1, description="页码"),
    size: int = Query(20, ge=1, le=100, description="每页数量"),
    current_user: dict = Depends(get_current_user)
):
    """
    获取合同回款汇总列表（按合同编号分组统计）
    
    统计每个合同：
    - 应收总额：合同应回款总金额
    - 已收总额：已录入的回款金额
    - 未收总额：剩余未回款金额
    - 回款率：已收/应收
    - 回款状态分布
    
    用于财务快速查看各合同的整体回款情况
    """
    check_finance_permission(current_user)
    
    try:
        result = PaymentService.get_contract_payment_summary(
            contract_no=contract_no,
            smelter_name=smelter_name,
            status=status,
            page=page,
            size=size
        )
        return {
            "msg": "查询成功",
            "data": result
        }
    except Exception:
        logger.exception("查询合同回款汇总异常")
        raise HTTPException(status_code=500, detail="查询失败")


# ========== 合同回款明细接口（动态路由放在静态路由之后） ==========

@router.get("/contracts/{contract_no}/payment-details", summary="合同回款明细", response_model=dict)
def get_contract_payment_details(
    contract_no: str,
    page: int = Query(1, ge=1, description="页码"),
    size: int = Query(20, ge=1, le=100, description="每页数量"),
    current_user: dict = Depends(get_current_user)
):
    """
    获取单个合同的回款明细
    
    展示内容：
    - 合同基本信息（应收、已收、未收、回款率）
    - 该合同下所有销售订单的收款明细
    - 该合同下的所有回款记录
    
    用于查看单个合同的详细回款情况
    """
    check_finance_permission(current_user)
    
    try:
        result = PaymentService.get_contract_payment_details(
            contract_no=contract_no,
            page=page,
            size=size
        )
        return {
            "msg": "查询成功",
            "data": result
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception:
        logger.exception("查询合同回款明细异常")
        raise HTTPException(status_code=500, detail="查询失败")


# ========== 回款录入接口（核心功能） ==========

@router.post("/records", summary="录入回款记录", response_model=dict)
def record_payment(
    body: RecordPaymentReq,
    current_user: dict = Depends(get_current_user)
):
    """
    录入回款记录（支持分段收款）

    录入后会自动：
    1. 更新收款明细的已回款金额
    2. 更新付款状态 is_paid = 1（已回首笔款）
    3. 补全之前缺失的回款相关字段
    
    Args:
        body: 回款记录请求体
        current_user: 当前用户信息

    Returns:
        录入结果信息（包含完整的收款明细信息）
    """
    check_finance_permission(current_user)

    try:
        resolved_payment_detail_id = PaymentService.resolve_payment_detail_id(
            payment_detail_id=body.payment_detail_id,
            weighbill_id=body.weighbill_id,
            delivery_id=body.delivery_id,
            contract_no=body.contract_no,
            vehicle_no=body.vehicle_no,
            product_name=body.product_name,
        )

        result = PaymentService.record_payment(
            payment_detail_id=resolved_payment_detail_id,
            payment_amount=Decimal(str(body.payment_amount)),
            payment_stage=PaymentStage(body.payment_stage),
            payment_date=body.payment_date,
            payment_method=body.payment_method,
            transaction_no=body.transaction_no,
            remark=body.remark,
            recorded_by=current_user.get("id")
        )
        
        # 返回完整的收款明细信息
        full_detail = PaymentService.get_payment_detail(resolved_payment_detail_id)
        
        return {
            "msg": "回款记录录入成功",
            "data": {
                "record_result": result,
                "payment_detail": full_detail
            }
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("录入回款记录异常")
        raise HTTPException(status_code=500, detail="录入失败")


# 在 payment.py 中添加

class CreatePaymentByWeighbillReq(BaseModel):
    """根据磅单创建回款信息"""
    weighbill_id: Optional[int] = Field(None, description="磅单ID，已知时优先使用")
    delivery_id: Optional[int] = Field(None, gt=0, description="报单ID，可选")
    contract_no: str = Field(..., description="合同编号")
    smelter_name: Optional[str] = Field(None, description="冶炼厂名称")
    vehicle_no: Optional[str] = Field(None, description="车号，用于自动匹配")
    product_name: Optional[str] = Field(None, description="品种，用于自动匹配")


@router.post("/details/create-by-weighbill", summary="根据磅单手动创建回款信息", response_model=dict)
def create_payment_by_weighbill(
        body: CreatePaymentByWeighbillReq,
        current_user: dict = Depends(get_current_user)
):
    """
    手动为已上传的磅单创建回款信息
    （用于自动创建失败时的补救）
    """
    check_finance_permission(current_user)

    try:
        resolved_weighbill_id = PaymentService.resolve_weighbill_id_for_payment(
            weighbill_id=body.weighbill_id,
            delivery_id=body.delivery_id,
            contract_no=body.contract_no,
            smelter_name=body.smelter_name,
            vehicle_no=body.vehicle_no,
            product_name=body.product_name,
        )

        with get_conn() as conn:
            with conn.cursor() as cur:
                # 获取磅单信息
                cur.execute("""
                    SELECT w.*, d.target_factory_name, d.payee, d.id as delivery_id
                    FROM pd_weighbills w
                    JOIN pd_deliveries d ON w.delivery_id = d.id
                    WHERE w.id = %s
                """, (resolved_weighbill_id,))
                weighbill = cur.fetchone()

                if not weighbill:
                    raise HTTPException(status_code=404, detail="磅单不存在")

                # 检查是否已存在回款信息
                cur.execute("""
                    SELECT id FROM pd_payment_details WHERE weighbill_id = %s
                """, (resolved_weighbill_id,))
                if cur.fetchone():
                    return {"msg": "该磅单已存在回款信息，无需重复创建"}

                # 创建回款信息
                from decimal import Decimal
                from app.services.payment_services import PaymentService, calculate_payment_amount

                unit_price = Decimal(str(weighbill['unit_price'])) if weighbill.get('unit_price') else None
                net_weight = Decimal(str(weighbill['net_weight'])) if weighbill.get('net_weight') else None

                result = PaymentService.create_or_update_by_weighbill(
                    weighbill_id=resolved_weighbill_id,
                    delivery_id=weighbill['delivery_id'],
                    contract_no=body.contract_no,
                    smelter_name=body.smelter_name or weighbill.get('target_factory_name', ''),
                    material_name=weighbill.get('product_name'),
                    unit_price=unit_price,
                    net_weight=net_weight,
                    total_amount=calculate_payment_amount(unit_price,
                                                          net_weight) if unit_price and net_weight else None,
                    created_by=current_user.get("id")
                )

                return {
                    "msg": "回款信息创建成功",
                    "data": result
                }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("手动创建回款信息异常")
        raise HTTPException(status_code=500, detail=f"创建失败: {str(e)}")
    
@router.post("/upload-excel", summary="上传回款 Excel 文件", response_model=UploadResponse)
async def upload_payment_excel(
    file: UploadFile = File(..., description="回款明细Excel文件"),
    remark: Optional[str] = Form(None, description="备注说明"),
    current_user: dict = Depends(get_current_user)
):
    """
    上传回款明细Excel文件
    
    功能：
    1. 接收Excel文件（.xlsx/.xls）
    2. 保存到 uploads/payments/ 目录
    3. 生成唯一文件名避免覆盖
    4. 可选：记录上传日志
    
    **请求示例：**
    ```bash
    curl -X POST "http://api/payments/upload-excel" \\
      -H "Authorization: Bearer {token}" \\
      -F "file=@金利首付款明细.xlsx" \\
      -F "remark=3月份回款数据"
    ```
    """
    
    # ========== 1. 验证文件类型 ==========
    file_ext = Path(file.filename).suffix.lower()
    if file_ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"不支持的文件格式: {file_ext}，请上传Excel文件(.xlsx/.xls)"
        )
    
    # ========== 2. 读取并验证文件大小 ==========
    contents = await file.read()
    file_size = len(contents)
    
    if file_size > MAX_FILE_SIZE:
        raise HTTPException(
            status_code=400,
            detail=f"文件大小不能超过10MB，当前: {file_size / 1024 / 1024:.2f}MB"
        )
    
    if file_size == 0:
        raise HTTPException(status_code=400, detail="文件不能为空")
    
    # ========== 3. 生成唯一文件名 ==========
    # 格式: 原文件名(清理)_时间戳_随机4位.xlsx
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    random_suffix = uuid.uuid4().hex[:4]
    
    # 清理原文件名（移除非法字符）
    safe_name = "".join(c for c in file.filename if c.isalnum() or c in (' ', '-', '_', '.')).rstrip()
    safe_name = Path(safe_name).stem  # 去掉扩展名
    
    saved_filename = f"{safe_name}_{timestamp}_{random_suffix}{file_ext}"
    file_path = PAYMENT_UPLOAD_DIR / saved_filename
    
    # ========== 4. 保存文件 ==========
    try:
        with open(file_path, "wb") as f:
            f.write(contents)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"文件保存失败: {str(e)}")
    
    # ========== 5. 记录上传日志（可选）==========
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 检查是否存在上传记录表
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS pd_payment_upload_logs (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        original_filename VARCHAR(255) NOT NULL COMMENT '原始文件名',
                        saved_filename VARCHAR(255) NOT NULL COMMENT '保存文件名',
                        file_path VARCHAR(500) NOT NULL COMMENT '文件路径',
                        file_size BIGINT COMMENT '文件大小(字节)',
                        remark VARCHAR(500) COMMENT '备注',
                        uploaded_by BIGINT COMMENT '上传人ID',
                        uploaded_by_name VARCHAR(64) COMMENT '上传人姓名',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_created_at (created_at)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='回款文件上传日志';
                """)
                
                # 插入上传记录
                cur.execute("""
                    INSERT INTO pd_payment_upload_logs 
                    (original_filename, saved_filename, file_path, file_size, 
                     remark, uploaded_by, uploaded_by_name)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    file.filename,
                    saved_filename,
                    str(file_path),
                    file_size,
                    remark,
                    current_user.get("id"),
                    current_user.get("name") or current_user.get("account")
                ))
                conn.commit()
    except Exception as e:
        # 记录日志失败不影响主流程
        print(f"记录上传日志失败: {e}")
    
    # ========== 6. 构造返回数据 ==========
    # 构建访问URL（假设有静态文件服务）
    file_url = f"/uploads/payments/{saved_filename}"
    
    return UploadResponse(
        success=True,
        message="文件上传成功",
        data={
            "original_filename": file.filename,
            "saved_filename": saved_filename,
            "file_path": str(file_path),
            "file_url": file_url,
            "file_size": file_size,
            "file_size_human": f"{file_size / 1024:.2f} KB",
            "upload_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "remark": remark,
            "uploader": current_user.get("name") or current_user.get("account")
        }
    )


@router.get("/uploads", summary="查询已上传文件", response_model=dict)
async def list_uploaded_files(
    page: int = 1,
    page_size: int = 20,
    current_user: dict = Depends(get_current_user)
):
    """
    查询已上传的回款文件列表
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 查询总数
                cur.execute("SELECT COUNT(*) FROM pd_payment_upload_logs")
                total = cur.fetchone()[0]
                
                # 分页查询
                offset = (page - 1) * page_size
                cur.execute("""
                    SELECT * FROM pd_payment_upload_logs
                    ORDER BY created_at DESC
                    LIMIT %s OFFSET %s
                """, (page_size, offset))
                
                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                
                data = []
                for row in rows:
                    item = dict(zip(columns, row))
                    if item.get('created_at'):
                        item['created_at'] = str(item['created_at'])
                    data.append(item)
                
                return {
                    "success": True,
                    "data": data,
                    "total": total,
                    "page": page,
                    "page_size": page_size
                }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询失败: {str(e)}")


@router.get("/uploads/{filename}", summary="查看已上传文件")
async def download_uploaded_file(
    filename: str,
    current_user: dict = Depends(get_current_user)
):
    """
    下载已上传的回款文件
    """
    file_path = PAYMENT_UPLOAD_DIR / filename
    
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="文件不存在")
    
    return FileResponse(
        path=str(file_path),
        filename=filename,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


@router.delete("/uploads/{filename}", summary="删除已上传文件")
async def delete_uploaded_file(
    filename: str,
    current_user: dict = Depends(get_current_user)
):
    """
    删除已上传的回款文件
    """
    file_path = PAYMENT_UPLOAD_DIR / filename
    
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="文件不存在")
    
    try:
        # 删除物理文件
        os.remove(file_path)
        
        # 删除数据库记录
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM pd_payment_upload_logs WHERE saved_filename = %s",
                    (filename,)
                )
                conn.commit()
        
        return {
            "success": True,
            "message": "文件删除成功"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"删除失败: {str(e)}")

@router.post("/import-excel", summary="Excel批量导入回款数据", response_model=dict)
async def import_payment_excel(
    body: PaymentExcelImportReq,
    current_user: dict = Depends(get_current_user)
):
    """
    批量导入回款明细Excel文件
    
    核心逻辑：
    1. 读取已上传的Excel文件
    2. 自动检测表头，识别磅单编号列和金额列
    3. 区分豫光和金利公司：
       - 豫光：含税金额 × 90% → arrival_paid_amount
       - 金利：结算金额 × 100% → arrival_paid_amount
    4. 根据磅单号匹配磅单/报单，获取合同信息
    5. 更新 pd_payment_details.arrival_paid_amount
    6. 保存原始数据到 pd_payment_excel_imports 表
    
    请求示例：
    {
        "file_id": "金利回款明细_20250311_143022_a7f3.xlsx",
        "company_type": "jinli"  // 可选，不传则自动检测
    }
    """
    check_finance_permission(current_user)
    
    try:
        # ========== 1. 查找并读取Excel文件 ==========
        file_path = PAYMENT_UPLOAD_DIR / body.file_id
        if not file_path.exists():
            # 从数据库查找文件路径
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT file_path FROM pd_payment_upload_logs 
                        WHERE saved_filename = %s OR original_filename = %s
                        ORDER BY created_at DESC LIMIT 1
                    """, (body.file_id, body.file_id))
                    row = cur.fetchone()
                    if row:
                        file_path = Path(row['file_path'])
                    else:
                        raise HTTPException(status_code=404, detail="文件不存在，请先调用 /upload-excel 上传")
        
        # ========== 2. 解析Excel，检测表头 ==========
        try:
            # 先读取原始数据检测表头行
            df_raw = pd.read_excel(file_path, header=None)
            processor = PaymentExcelProcessor()
            header_info = processor.detect_headers(df_raw)
            
            # 使用检测到的表头行重新读取
            df = pd.read_excel(file_path, header=header_info['header_row'])
            # 清理列名
            df.columns = [str(col).strip() if pd.notna(col) else f"Col_{i}" 
                         for i, col in enumerate(df.columns)]
            
            logger.info(f"检测到表头行: {header_info['header_row']}, "
                       f"磅单列: {header_info['weighbill_col']}, "
                       f"金额列: {header_info['amount_col']}, "
                       f"公司类型: {header_info['company_type']}")
            
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Excel解析失败: {str(e)}")
        
        # ========== 3. 确定公司类型 ==========
        company_type = body.company_type or header_info.get('company_type', 'yuguang')
        
        # 文件名辅助判断
        filename_lower = str(file_path.name).lower()
        if not body.company_type:
            if '金利' in filename_lower or 'jinli' in filename_lower:
                company_type = 'jinli'
            elif '豫光' in filename_lower or 'yuguang' in filename_lower:
                company_type = 'yuguang'
        
        # ========== 4. 解析数据行 ==========
        try:
            records = processor.parse_data(df)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        
        if not records:
            raise HTTPException(status_code=400, detail="未从Excel中解析到有效数据（磅单号+金额）")
        
        # ========== 5. 逐行处理并入库 ==========
        results = []
        success_count = 0
        fail_count = 0
        
        with get_conn() as conn:
            with conn.cursor() as cur:
                for record in records:
                    result_item = {
                        'row_index': record['row_index'],
                        'weighbill_no': record['weighbill_no'],
                        'original_amount': record['amount'],
                        'status': 'pending'
                    }
                    
                    try:
                        # 5.1 查找磅单和合同信息
                        match_info = PaymentService.find_weighbill_and_contract(
                            record['weighbill_no']
                        )
                        
                        if not match_info.get('found'):
                            result_item.update({
                                'status': 'failed',
                                'reason': '未找到匹配的磅单或报单'
                            })
                            fail_count += 1
                            results.append(result_item)
                            continue
                        
                        # 5.2 根据公司类型计算金额
                        original_amount = Decimal(str(record['amount']))
                        
                        if company_type == 'jinli':
                            # 金利：结算金额直接作为已回款首笔金额
                            processed_amount = original_amount
                        else:
                            # 豫光：含税金额的90%作为已回款首笔金额
                            processed_amount = (original_amount * Decimal('0.9')).quantize(Decimal('0.01'))
                        
                        # 5.3 更新或创建回款记录
                        update_result = PaymentService.update_arrival_paid_amount(
                            weighbill_no=record['weighbill_no'],
                            amount=float(processed_amount),
                            match_info=match_info,
                            company_type=company_type  # 传递公司类型
                        )
                        
                        # 5.4 保存原始导入数据到 pd_payment_excel_imports
                        # 确保表存在
                        cur.execute("""
                            CREATE TABLE IF NOT EXISTS pd_payment_excel_imports (
                                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                                payment_detail_id BIGINT COMMENT '关联的收款明细ID',
                                weighbill_no VARCHAR(64) COMMENT '磅单号',
                                original_amount DECIMAL(15, 2) COMMENT 'Excel中的原始金额',
                                processed_amount DECIMAL(15, 2) COMMENT '处理后金额',
                                company_type VARCHAR(20) COMMENT '公司类型：yuguang/jinli',
                                raw_data JSON COMMENT '原始行数据',
                                imported_by BIGINT COMMENT '导入人ID',
                                imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                status VARCHAR(20) DEFAULT 'success' COMMENT '处理状态',
                                fail_reason VARCHAR(500) COMMENT '失败原因',
                                INDEX idx_weighbill_no (weighbill_no),
                                INDEX idx_payment_detail_id (payment_detail_id),
                                INDEX idx_imported_at (imported_at),
                                INDEX idx_company_type (company_type)
                            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='回款Excel导入明细记录';
                        """)
                        
                        # 插入导入记录
                        cur.execute("""
                            INSERT INTO pd_payment_excel_imports 
                            (payment_detail_id, weighbill_no, original_amount, 
                             processed_amount, company_type, raw_data, 
                             imported_by, status, fail_reason)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            update_result.get('payment_id'),
                            record['weighbill_no'],
                            float(original_amount),
                            float(processed_amount),
                            company_type,
                            json.dumps(record['raw_data'], ensure_ascii=False, default=str),
                            current_user.get('id'),
                            'success',
                            None
                        ))
                        
                        result_item.update({
                            'status': 'success',
                            'payment_id': update_result.get('payment_id'),
                            'action': update_result.get('action'),
                            'processed_amount': float(processed_amount),
                            'contract_no': match_info.get('contract_no'),
                            'smelter_name': match_info.get('smelter_name')
                        })
                        success_count += 1
                        
                    except Exception as e:
                        logger.exception(f"处理行 {record['row_index']} 失败")
                        result_item.update({
                            'status': 'failed',
                            'reason': str(e)
                        })
                        fail_count += 1
                        
                        # 记录失败数据
                        try:
                            cur.execute("""
                                INSERT INTO pd_payment_excel_imports 
                                (weighbill_no, original_amount, company_type, 
                                 raw_data, imported_by, status, fail_reason)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """, (
                                record['weighbill_no'],
                                float(record['amount']),
                                company_type,
                                json.dumps(record['raw_data'], ensure_ascii=False, default=str),
                                current_user.get('id'),
                                'failed',
                                str(e)[:500]
                            ))
                        except Exception as log_err:
                            logger.error(f"记录失败数据时出错: {log_err}")
                
                conn.commit()
        
        # ========== 6. 更新上传日志的处理状态 ==========
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE pd_payment_upload_logs 
                        SET processed = 1, processed_at = NOW(),
                            success_count = %s, fail_count = %s,
                            company_type = %s
                        WHERE saved_filename = %s
                    """, (success_count, fail_count, company_type, body.file_id))
                    conn.commit()
        except Exception as e:
            logger.warning(f"更新上传日志状态失败: {e}")
        
        # ========== 7. 返回结果 ==========
        return {
            "success": True,
            "message": f"导入完成：成功 {success_count} 条，失败 {fail_count} 条",
            "company_type": company_type,
            "total_rows": len(records),
            "success_count": success_count,
            "fail_count": fail_count,
            "details": results
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Excel导入回款数据异常")
        raise HTTPException(status_code=500, detail=f"导入失败: {str(e)}")
    
@router.get("/import-records", summary="查询Excel导入记录")
async def list_import_records(
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
    company_type: Optional[str] = Query(None, description="公司类型筛选：yuguang/jinli"),
    status: Optional[str] = Query(None, description="处理状态：success/failed"),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    current_user: dict = Depends(get_current_user)
):
    """查询Excel导入的历史记录（包含原始数据）"""
    check_finance_permission(current_user)
    
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                where_clauses = ["1=1"]
                params = []
                
                if company_type:
                    where_clauses.append("company_type = %s")
                    params.append(company_type)
                
                if status:
                    where_clauses.append("status = %s")
                    params.append(status)
                
                if start_date:
                    where_clauses.append("DATE(imported_at) >= %s")
                    params.append(start_date)
                
                if end_date:
                    where_clauses.append("DATE(imported_at) <= %s")
                    params.append(end_date)
                
                where_sql = " AND ".join(where_clauses)
                
                # 查询总数
                cur.execute(f"""
                    SELECT COUNT(*) as total FROM pd_payment_excel_imports
                    WHERE {where_sql}
                """, tuple(params))
                total = cur.fetchone()['total']
                
                # 分页查询
                offset = (page - 1) * size
                cur.execute(f"""
                    SELECT 
                        id,
                        payment_detail_id,
                        weighbill_no,
                        original_amount,
                        processed_amount,
                        company_type,
                        raw_data,
                        imported_by,
                        imported_at,
                        status,
                        fail_reason
                    FROM pd_payment_excel_imports
                    WHERE {where_sql}
                    ORDER BY imported_at DESC
                    LIMIT %s OFFSET %s
                """, tuple(params + [size, offset]))
                
                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                
                items = []
                for row in rows:
                    item = dict(zip(columns, row))
                    # 解析JSON
                    if item.get('raw_data') and isinstance(item['raw_data'], str):
                        try:
                            item['raw_data'] = json.loads(item['raw_data'])
                        except:
                            pass
                    # 时间格式化
                    if item.get('imported_at'):
                        item['imported_at'] = str(item['imported_at'])
                    items.append(item)
                
                return {
                    "success": True,
                    "total": total,
                    "page": page,
                    "size": size,
                    "items": items
                }
                
    except Exception as e:
        logger.exception("查询导入记录异常")
        raise HTTPException(status_code=500, detail=f"查询失败: {str(e)}")


@router.get("/import-records/export", summary="导出Excel导入记录")
async def export_import_records(
    company_type: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    current_user: dict = Depends(get_current_user)
):
    """导出Excel导入记录为Excel文件"""
    check_finance_permission(current_user)
    
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                where_clauses = ["1=1"]
                params = []
                
                if company_type:
                    where_clauses.append("company_type = %s")
                    params.append(company_type)
                
                if status:
                    where_clauses.append("status = %s")
                    params.append(status)
                
                if start_date:
                    where_clauses.append("DATE(imported_at) >= %s")
                    params.append(start_date)
                
                if end_date:
                    where_clauses.append("DATE(imported_at) <= %s")
                    params.append(end_date)
                
                where_sql = " AND ".join(where_clauses)
                
                cur.execute(f"""
                    SELECT 
                        weighbill_no as '磅单号',
                        original_amount as '原始金额',
                        processed_amount as '处理后金额',
                        company_type as '公司类型',
                        status as '处理状态',
                        fail_reason as '失败原因',
                        imported_at as '导入时间'
                    FROM pd_payment_excel_imports
                    WHERE {where_sql}
                    ORDER BY imported_at DESC
                """, tuple(params))
                
                rows = cur.fetchall()
                
                if not rows:
                    raise HTTPException(status_code=404, detail="无数据可导出")
                
                # 创建DataFrame并导出
                df = pd.DataFrame(rows)
                
                # 转换时间格式
                if '导入时间' in df.columns:
                    df['导入时间'] = pd.to_datetime(df['导入时间']).dt.strftime('%Y-%m-%d %H:%M:%S')
                
                # 生成导出文件
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                export_filename = f"导入记录导出_{timestamp}.xlsx"
                export_path = PAYMENT_UPLOAD_DIR / export_filename
                
                df.to_excel(export_path, index=False, engine='openpyxl')
                
                return FileResponse(
                    path=str(export_path),
                    filename=export_filename,
                    media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("导出导入记录异常")
        raise HTTPException(status_code=500, detail=f"导出失败: {str(e)}")
    
