"""
磅单管理路由 - 支持一报单多品种（最多4个）
"""
import logging
import os
import shutil
from typing import Dict, List, Optional

from fastapi import APIRouter, UploadFile, File, HTTPException, Depends, Query, Form
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from app.core.paths import TEMP_UPLOADS_DIR
from app.services.weighbill_service import WeighbillService, get_weighbill_service
from app.services.contract_service import get_conn
from core.auth import get_current_user

router = APIRouter(prefix="/weighbills", tags=["磅单管理"])
logger = logging.getLogger(__name__)

# ============ 请求/响应模型 ============

class WeighbillOCRResponse(BaseModel):
    weigh_date: Optional[str] = None
    weigh_ticket_no: Optional[str] = None
    contract_no: Optional[str] = None
    vehicle_no: Optional[str] = None
    product_name: Optional[str] = None
    gross_weight: Optional[float] = None
    tare_weight: Optional[float] = None
    net_weight: Optional[float] = None
    delivery_unit: Optional[str] = None
    receive_unit: Optional[str] = None
    ocr_message: str = ""
    ocr_success: bool = True
    raw_text: Optional[str] = None
    ocr_time: float = 0
    # 自动填充
    matched_delivery_id: Optional[int] = None
    warehouse: Optional[str] = None
    target_factory_name: Optional[str] = None
    driver_name: Optional[str] = None
    driver_phone: Optional[str] = None
    driver_id_card: Optional[str] = None
    unit_price: Optional[float] = None
    total_amount: Optional[float] = None
    match_message: Optional[str] = None
    price_message: Optional[str] = None


class WeighbillUploadRequest(BaseModel):
    delivery_id: int
    product_name: str
    weigh_date: str
    weigh_ticket_no: Optional[str] = None
    contract_no: Optional[str] = None
    vehicle_no: Optional[str] = None
    gross_weight: Optional[float] = None
    tare_weight: Optional[float] = None
    net_weight: float
    delivery_time: Optional[str] = None
    unit_price: Optional[float] = None
    warehouse_name: Optional[str] = None


class WeighbillOut(BaseModel):
    id: Optional[int] = None
    delivery_id: int
    weigh_date: Optional[str] = None
    delivery_time: Optional[str] = None
    weigh_ticket_no: Optional[str] = None
    contract_no: Optional[str] = None
    vehicle_no: Optional[str] = None
    product_name: str
    gross_weight: Optional[float] = None
    tare_weight: Optional[float] = None
    net_weight: Optional[float] = None
    unit_price: Optional[float] = None
    total_amount: Optional[float] = None
    weighbill_image: Optional[str] = None
    upload_status: str = "待上传"
    ocr_status: str = "待上传磅单"
    ocr_status_display: str = "待上传磅单"
    is_manual_corrected: int = 0
    is_manual_corrected_display: str = "否"
    payment_schedule_date: Optional[str] = None
    payment_schedule_status: Optional[str] = None
    is_paid_out: Optional[int] = None
    is_paid_out_display: Optional[str] = None
    collection_status: Optional[int] = None
    collection_status_display: Optional[str] = None
    uploader_id: Optional[int] = None
    uploader_name: Optional[str] = None
    uploaded_at: Optional[str] = None
    # 报单信息
    report_date: Optional[str] = None
    warehouse_name: Optional[str] = None
    warehouse: Optional[str] = None
    target_factory_name: Optional[str] = None
    driver_phone: Optional[str] = None
    driver_name: Optional[str] = None
    driver_id_card: Optional[str] = None
    has_delivery_order: Optional[str] = None
    has_delivery_order_display: str = "否"
    shipper: Optional[str] = None
    reporter_name: Optional[str] = None
    payee: Optional[str] = None
    service_fee: Optional[float] = None
    operations: Optional[dict] = None
    # 新增计算字段
    payable_unit_price: Optional[float] = Field(None, description="应付单价 = 合同单价/1.048")
    payable_amount_calculated: Optional[float] = Field(None, description="应付金额 = 应付单价*净重-联单费")
    receivable_amount_calculated: Optional[float] = Field(None, description="回款金额 = 合同单价*净重-联单费")


class WeighbillGroupOut(BaseModel):
    delivery_id: int
    contract_no: Optional[str] = None
    report_date: Optional[str] = None
    target_factory_name: Optional[str] = None
    driver_phone: Optional[str] = None
    driver_name: Optional[str] = None
    driver_id_card: Optional[str] = None
    vehicle_no: Optional[str] = None
    has_delivery_order: Optional[str] = None
    has_delivery_order_display: str = "否"
    upload_status: Optional[str] = None
    upload_status_display: str = "否"
    shipper: Optional[str] = None
    reporter_name: Optional[str] = None
    payee: Optional[str] = None
    warehouse: Optional[str] = None
    service_fee: Optional[float] = None
    total_weighbills: int = 0
    uploaded_weighbills: int = 0
    weighbills: List[WeighbillOut] = []


class PaymentScheduleRequest(BaseModel):
    payment_schedule_date: str = Field(..., description="排款日期，格式：YYYY-MM-DD")


class PayeeOption(BaseModel):
    """收款人选项"""
    id: int
    payee_name: str
    payee_account: Optional[str] = ""
    payee_bank_name: Optional[str] = ""
    is_active: int = 1


class WeighbillBatchUploadResponse(BaseModel):
    """批量上传磅单响应"""
    success: bool
    need_select_payee: bool = False  # 是否需要选择收款人
    message: str
    warehouse_name: Optional[str] = None
    payees: Optional[List[PayeeOption]] = None  # 需要选择时返回
    payee_id: Optional[int] = None  # 已选择时返回
    payee_name: Optional[str] = None  # 已选择时返回
    total: int = 0
    success_count: int = 0
    failed_count: int = 0
    success_list: List[Dict] = []
    failed_list: List[Dict] = []

class BatchPriceUpdateItem(BaseModel):
    product_name: str = Field(..., description="品种名称")
    unit_price: float = Field(..., description="新单价（元/吨）")

class BatchPriceUpdateRequest(BaseModel):
    delivery_id: int = Field(..., description="报单ID")
    prices: List[BatchPriceUpdateItem] = Field(..., description="单价更新列表")
# ============ 路由 ============

@router.post("/ocr", summary="OCR 识别磅单", response_model=WeighbillOCRResponse)
async def ocr_weighbill(
        file: UploadFile = File(..., description="磅单图片"),
        auto_match: bool = Query(True, description="是否自动关联匹配"),
        service: WeighbillService = Depends(get_weighbill_service)
):
    """OCR识别磅单"""
    allowed_types = ["image/jpeg", "image/jpg", "image/png", "image/bmp"]
    if file.content_type not in allowed_types:
        raise HTTPException(status_code=400, detail="仅支持jpg/png/bmp格式")

    temp_path = TEMP_UPLOADS_DIR / f"weighbill_{os.urandom(4).hex()}.jpg"
    temp_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(temp_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        processed_path = service.preprocess_image(str(temp_path))
        result = service.recognize_weighbill(processed_path)

        if processed_path != str(temp_path) and os.path.exists(processed_path):
            os.remove(processed_path)
        os.remove(temp_path)

        if not result["success"]:
            raise HTTPException(status_code=400, detail=result.get("error", "识别失败"))

        ocr_data = result["data"]

        if auto_match:
            ocr_data = service.auto_fill_data(ocr_data)

        return WeighbillOCRResponse(**ocr_data)

    except HTTPException:
        raise
    except Exception as e:
        if temp_path.exists():
            os.remove(temp_path)
        raise HTTPException(status_code=500, detail=f"处理失败: {str(e)}")


@router.post("/create", summary="上传磅单", response_model=dict)
async def upload_weighbill(
        delivery_id: int = Form(..., description="报单ID"),
        product_name: str = Form(..., description="品种名称"),
        weigh_date: str = Form(..., description="磅单日期"),
        weigh_ticket_no: Optional[str] = Form(None, description="过磅单号"),
        contract_no: Optional[str] = Form(None, description="合同编号"),
        vehicle_no: Optional[str] = Form(None, description="车牌号"),
        gross_weight: Optional[float] = Form(None, description="毛重"),
        tare_weight: Optional[float] = Form(None, description="皮重"),
        net_weight: float = Form(..., description="净重"),
        delivery_time: Optional[str] = Form(None, description="送货时间"),
        unit_price: Optional[float] = Form(None, description="单价（不传则自动获取）"),
        warehouse_name: Optional[str] = Form(None, description="磅单仓库名称"),
        warehouse: Optional[str] = Form(None, description="送货库房"),
        payee: Optional[str] = Form(None, description="收款人"),
        payee_id: Optional[int] = Form(None, description="收款人ID"),
        is_manual: bool = Form(False, description="是否人工修正"),
        weighbill_image: UploadFile = File(..., description="磅单图片"),
        service: WeighbillService = Depends(get_weighbill_service),
        current_user: dict = Depends(get_current_user)
):
    # ========== 新增校验：必须指定库房或收款人 ==========
    final_warehouse = warehouse_name or warehouse
    final_payee = payee
    
    if not final_warehouse and not final_payee and not payee_id:
        raise HTTPException(
            status_code=400, 
            detail="必须指定库房(warehouse_name/warehouse)或收款人(payee/payee_id)至少一项"
        )
    
    # 如果指定了库房，验证库房是否存在
    if final_warehouse:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id FROM pd_warehouses 
                    WHERE warehouse_name = %s AND is_active = 1
                """, (final_warehouse,))
                if not cur.fetchone():
                    raise HTTPException(
                        status_code=400,
                        detail=f"库房 '{final_warehouse}' 不存在或已停用"
                    )
    
    # 如果指定了payee_id，验证收款人是否存在
    if payee_id:
        payee_info = service._get_payee_by_id(payee_id)
        if not payee_info:
            raise HTTPException(
                status_code=400,
                detail=f"收款人ID {payee_id} 不存在"
            )
        # 如果同时指定了库房，验证收款人是否属于该库房
        if final_warehouse and payee_info.get('warehouse_name') != final_warehouse:
            raise HTTPException(
                status_code=400,
                detail=f"收款人ID {payee_id} 不属于库房 '{final_warehouse}'"
            )
    # ========== 校验结束 ==========
    """上传磅单（按品种上传）"""
    try:
        # 自动获取单价
        final_unit_price = unit_price
        if not final_unit_price and contract_no and product_name:
            final_unit_price = service.get_contract_price_by_product(contract_no, product_name)

        data = {
            "weigh_date": weigh_date,
            "weigh_ticket_no": weigh_ticket_no,
            "contract_no": contract_no,
            "vehicle_no": vehicle_no,
            "gross_weight": gross_weight,
            "tare_weight": tare_weight,
            "net_weight": net_weight,
            "delivery_time": delivery_time,
            "unit_price": final_unit_price,
            "warehouse_name": warehouse_name,
            "warehouse": warehouse,
            "payee": payee,
        }

        image_bytes = await weighbill_image.read()

        result = service.upload_weighbill(
            delivery_id=delivery_id,
            product_name=product_name,
            data=data,
            image_file=image_bytes,
            current_user=current_user,
            is_manual=is_manual
        )

        if result["success"]:
            # ========== 新增：自动创建/更新收款明细 ==========
            try:
                from app.services.payment_services import PaymentService, calculate_payment_amount
                from decimal import Decimal

                # 获取报单信息（用于获取冶炼厂、收款人等）
                delivery_info = service.get_delivery_info(delivery_id)

                weighbill_id = result["data"].get("weighbill_id")

                # 确保有合同号
                final_contract_no = data.get('contract_no') or delivery_info.get('contract_no', '')

                # 计算金额
                calculated_amount = None
                if final_unit_price and net_weight:
                    calculated_amount = calculate_payment_amount(
                        Decimal(str(final_unit_price)),
                        Decimal(str(net_weight))
                    )

                # 获取收款人（从报单）
                payee_name = delivery_info.get("payee", "") if delivery_info else ""

                # 创建或更新收款明细
                payment_result = PaymentService.create_or_update_by_weighbill(
                    weighbill_id=weighbill_id,
                    delivery_id=delivery_id,
                    contract_no=final_contract_no,
                    smelter_name=delivery_info.get("target_factory_name", "") if delivery_info else "",
                    material_name=product_name,
                    unit_price=Decimal(str(final_unit_price)) if final_unit_price else None,
                    net_weight=Decimal(str(net_weight)) if net_weight else None,
                    total_amount=calculated_amount,
                    payee=payee_name,
                    created_by=current_user.get("id") if current_user else None
                )

                result["data"]["payment_detail_created"] = True
                result["data"]["payment_detail_id"] = payment_result.get("id") if isinstance(payment_result,
                                                                                             dict) else None

                # 自动生成结余明细（不阻断主流程）
                try:
                    from app.services.balance_service import get_balance_service

                    balance_service = get_balance_service()
                    balance_result = balance_service.generate_balance_details(weighbill_id=weighbill_id)
                    result["data"]["balance_generated"] = balance_result.get("success", False)
                    result["data"]["balance_generated_count"] = len(balance_result.get("data", []))
                    payee_sync_result = balance_service.sync_balance_payee_info(weighbill_id=weighbill_id)
                    result["data"]["balance_payee_matched"] = payee_sync_result.get("matched", False)
                    result["data"]["balance_payee_account"] = payee_sync_result.get("payee_account")
                    result["data"]["balance_payee_bank_name"] = payee_sync_result.get("payee_bank_name")
                except Exception as e:
                    logger.warning(f"自动生成结余明细失败: {e}")
                    result["data"]["balance_generated"] = False
                    result["data"]["balance_generated_error"] = str(e)

            except Exception as e:
                logger.error(f"自动创建收款明细失败: {e}", exc_info=True)
                result["data"]["payment_detail_created"] = False
                result["data"]["payment_detail_error"] = str(e)
            # ========== 新增结束 ==========
            
            return result
        else:
            logger.warning(
                "upload_weighbill request rejected delivery_id=%s product_name=%s weigh_date=%s contract_no=%s vehicle_no=%s error=%s",
                delivery_id,
                product_name,
                weigh_date,
                contract_no,
                vehicle_no,
                result.get("error"),
            )
            raise HTTPException(status_code=400, detail=result.get("error"))

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/modify", summary="修改磅单", response_model=dict)
async def modify_weighbill(
        weighbill_id: int = Form(..., description="磅单ID"),
    matched_delivery_id: Optional[int] = Form(None, description="匹配的报单ID"),
        weigh_date: Optional[str] = Form(None, description="磅单日期"),
        weigh_ticket_no: Optional[str] = Form(None, description="过磅单号"),
        contract_no: Optional[str] = Form(None, description="合同编号"),
        vehicle_no: Optional[str] = Form(None, description="车牌号"),
        gross_weight: Optional[float] = Form(None, description="毛重"),
        tare_weight: Optional[float] = Form(None, description="皮重"),
        net_weight: Optional[float] = Form(None, description="净重"),
        delivery_time: Optional[str] = Form(None, description="送货时间"),
        unit_price: Optional[float] = Form(None, description="单价"),
    warehouse_name: Optional[str] = Form(None, description="磅单仓库名称"),
    warehouse: Optional[str] = Form(None, description="送货库房"),
    payee: Optional[str] = Form(None, description="收款人"),
        is_manual: bool = Form(True, description="是否人工修正"),
        weighbill_image: Optional[UploadFile] = File(None, description="新的磅单图片（可选）"),
        service: WeighbillService = Depends(get_weighbill_service),
        current_user: dict = Depends(get_current_user)
):
    """修改磅单（支持修改信息和图片）"""
    try:
        existing = service.get_weighbill(weighbill_id)
        if not existing:
            raise HTTPException(status_code=404, detail="磅单不存在")

        # 构建更新数据
        data = {}
        fields = ['weigh_date', 'weigh_ticket_no', 'contract_no', 'vehicle_no',
              'gross_weight', 'tare_weight', 'net_weight', 'delivery_time', 'unit_price',
              'warehouse_name', 'warehouse', 'payee']

        for f in fields:
            value = locals().get(f)
            if value is not None:
                data[f] = value

        if not data and not weighbill_image:
            raise HTTPException(status_code=400, detail="没有要修改的字段")

        # 自动获取单价
        final_product = existing.get('product_name')
        final_contract = data.get('contract_no') or existing.get('contract_no')

        if 'unit_price' not in data and final_contract and final_product:
            data['unit_price'] = service.get_contract_price_by_product(final_contract, final_product)

        image_bytes = None
        if weighbill_image:
            image_bytes = await weighbill_image.read()

        target_delivery_id = matched_delivery_id or existing.get('delivery_id')

        result = service.upload_weighbill(
            delivery_id=target_delivery_id,
            product_name=final_product,
            data=data,
            image_file=image_bytes,
            current_user=current_user,
            is_manual=True
        )

        if result["success"]:
            # ========== 新增：更新收款明细 ==========
            try:
                from app.services.payment_services import PaymentService, calculate_payment_amount
                from decimal import Decimal
                
                delivery_id = target_delivery_id
                delivery_info = service.get_delivery_info(delivery_id)
                
                final_unit_price = data.get('unit_price') or existing.get('unit_price')
                final_net_weight = data.get('net_weight') or existing.get('net_weight')
                final_contract_no = data.get('contract_no') or existing.get('contract_no')
                
                calculated_amount = None
                if final_unit_price and final_net_weight:
                    calculated_amount = calculate_payment_amount(
                        Decimal(str(final_unit_price)), 
                        Decimal(str(final_net_weight))
                    )
                
                # 更新收款明细
                PaymentService.create_or_update_by_weighbill(
                    weighbill_id=weighbill_id,
                    delivery_id=delivery_id,
                    contract_no=final_contract_no,
                    smelter_name=delivery_info.get("target_factory_name", "") if delivery_info else "",
                    material_name=final_product,
                    unit_price=Decimal(str(final_unit_price)) if final_unit_price else None,
                    net_weight=Decimal(str(final_net_weight)) if final_net_weight else None,
                    total_amount=calculated_amount,
                    payee=delivery_info.get("payee", "") if delivery_info else "",
                    payee_account="",
                    created_by=current_user.get("id")
                )
                
                result["data"]["payment_detail_updated"] = True

                # 自动生成结余明细（不阻断主流程）
                try:
                    from app.services.balance_service import get_balance_service

                    balance_service = get_balance_service()
                    balance_result = balance_service.generate_balance_details(weighbill_id=weighbill_id)
                    result["data"]["balance_generated"] = balance_result.get("success", False)
                    result["data"]["balance_generated_count"] = len(balance_result.get("data", []))
                    payee_sync_result = balance_service.sync_balance_payee_info(weighbill_id=weighbill_id)
                    result["data"]["balance_payee_matched"] = payee_sync_result.get("matched", False)
                    result["data"]["balance_payee_account"] = payee_sync_result.get("payee_account")
                    result["data"]["balance_payee_bank_name"] = payee_sync_result.get("payee_bank_name")
                except Exception as e:
                    logger.warning(f"自动生成结余明细失败: {e}")
                    result["data"]["balance_generated"] = False
                    result["data"]["balance_generated_error"] = str(e)
                
            except Exception as e:
                logger.warning(f"更新收款明细失败: {e}")
                result["data"]["payment_detail_updated"] = False
            # ========== 新增结束 ==========
            
            return {"success": True, "message": "磅单修改成功", "data": result.get("data")}
        else:
            raise HTTPException(status_code=400, detail=result.get("error"))

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/", summary="查询磅单列表", response_model=dict)
async def list_weighbills(
        exact_delivery_id: Optional[int] = Query(None, description="精确报单ID"),
        exact_weighbill_id: Optional[int] = Query(None, description="精确磅单ID"),
        exact_shipper: Optional[str] = Query(None, description="精确发货人/报单人"),
        exact_contract_no: Optional[str] = Query(None, description="精确合同编号"),
        exact_report_date: Optional[str] = Query(None, description="精确报单日期"),
        exact_driver_name: Optional[str] = Query(None, description="精确司机姓名"),
        exact_vehicle_no: Optional[str] = Query(None, description="精确车号"),
        exact_weigh_date: Optional[str] = Query(None, description="精确磅单日期"),
        exact_ocr_status: Optional[str] = Query(None, description="精确磅单状态：待上传磅单/已确认"),
        # === 新增 ===
        exact_schedule_status: Optional[int] = Query(None, description="排款状态：0=待排期, 1=已排期"),
        exact_payout_status: Optional[int] = Query(None, description="打款状态：0=待打款, 1=已打款"),
        exact_collection_status: Optional[int] = Query(None, description="回款状态：0=待回款, 1=已回首笔, 2=已回款"),
        page: int = Query(1, ge=1),
        page_size: int = Query(20, ge=1, le=100),
        service: WeighbillService = Depends(get_weighbill_service)
):
    """
    查询磅单列表（按报单ID分组）

    新增筛选参数：
    - exact_schedule_status : 排款状态（0待排期/1已排期）
    - exact_payout_status    : 打款状态（0待打款/1已打款）
    - exact_collection_status: 回款状态（0待回款/1已回首笔/2已回款）
    """
    try:
        return service.list_weighbills_grouped(
            exact_delivery_id=exact_delivery_id,
            exact_weighbill_id=exact_weighbill_id,
            exact_shipper=exact_shipper,
            exact_contract_no=exact_contract_no,
            exact_report_date=exact_report_date,
            exact_driver_name=exact_driver_name,
            exact_vehicle_no=exact_vehicle_no,
            exact_weigh_date=exact_weigh_date,
            exact_ocr_status=exact_ocr_status,
            exact_schedule_status=exact_schedule_status,
            exact_payout_status=exact_payout_status,
            exact_collection_status=exact_collection_status,
            page=page,
            page_size=page_size,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{weighbill_id}", summary="查看磅单详情", response_model=WeighbillOut)
async def get_weighbill(
        weighbill_id: int,
        service: WeighbillService = Depends(get_weighbill_service)
):
    """查看磅单详情"""
    bill = service.get_weighbill(weighbill_id)
    if not bill:
        raise HTTPException(status_code=404, detail="磅单不存在")
    return bill


@router.get("/delivery/{delivery_id}", summary="查看报单下的磅单列表", response_model=WeighbillGroupOut)
async def get_weighbills_by_delivery(
        delivery_id: int,
        service: WeighbillService = Depends(get_weighbill_service)
):
    """获取指定报单的所有磅单"""
    try:
        result = service.list_weighbills_grouped(
            exact_delivery_id=delivery_id,
            page=1,
            page_size=100
        )
        if result.get("success") and result.get("data"):
            return result["data"][0]
        raise HTTPException(status_code=404, detail="报单不存在或无磅单记录")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{weighbill_id}", summary="删除磅单")
async def delete_weighbill(
        weighbill_id: int,
        service: WeighbillService = Depends(get_weighbill_service)
):
    """删除磅单"""
    try:
        bill = service.get_weighbill(weighbill_id)
        if not bill:
            raise HTTPException(status_code=404, detail="磅单不存在")

        image_path = bill.get("weighbill_image")
        if image_path and os.path.exists(image_path):
            try:
                os.remove(image_path)
            except Exception as e:
                logger.warning(f"删除磅单图片失败: {e}")

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM pd_weighbills WHERE id = %s", (weighbill_id,))

        return {"success": True, "message": "磅单删除成功"}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{weighbill_id}/image", summary="查看磅单图片")
async def get_weighbill_image(
        weighbill_id: int,
        service: WeighbillService = Depends(get_weighbill_service)
):
    """查看磅单图片"""
    try:
        bill = service.get_weighbill(weighbill_id)
        if not bill:
            raise HTTPException(status_code=404, detail="磅单不存在")

        image_path = bill.get("weighbill_image")
        if not image_path:
            raise HTTPException(status_code=404, detail="该磅单没有上传图片")

        if not os.path.exists(image_path):
            raise HTTPException(status_code=404, detail="图片文件不存在")

        return FileResponse(
            path=image_path,
            media_type="image/jpeg",
            filename=f"weighbill_{weighbill_id}_{bill.get('product_name', '')}.jpg"
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取图片失败: {str(e)}")


@router.put("/{weighbill_id}/payment-schedule", summary="设置排款日期", response_model=dict)
async def set_payment_schedule(
        weighbill_id: int,
        request: PaymentScheduleRequest,
        service: WeighbillService = Depends(get_weighbill_service)
):
    """设置磅单排款日期"""
    try:
        result = service.set_payment_schedule_date(weighbill_id, request.payment_schedule_date)

        if result["success"]:
            return result
        else:
            raise HTTPException(status_code=400, detail=result.get("error"))

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/batch-upload", response_model=WeighbillBatchUploadResponse, summary="批量上传磅单")
async def batch_upload_weighbills(
    warehouse_name: str = Form(..., description="库房名称（将自动关联该库房的收款人）"),
    payee_id: Optional[int] = Form(None, description="收款人ID（首次不传，当返回need_select_payee=true时，选择后传入）"),
    weighbill_images: List[UploadFile] = File(..., description="磅单图片列表（支持多张）"),
    service: WeighbillService = Depends(get_weighbill_service),
    current_user: dict = Depends(get_current_user)
):
    """
    批量上传磅单接口（支持多收款人选择）
    
    与单条上传接口 /create 并存，本接口用于批量处理
    
    调用流程：
    1. 首次调用：只传 warehouse_name + 图片
       - 如果该库房只有1个收款人：直接处理
       - 如果该库房有多个收款人：返回 need_select_payee=true + payees列表
    
    2. 二次调用：传 warehouse_name + payee_id（用户选择的）+ 图片
       - 使用指定的收款人批量处理所有磅单
    
    每张磅单自动：
    - OCR识别：日期、车号、合同号、品种、重量等
    - 匹配报单：根据【日期 + 车牌号】自动关联报货订单
    - 获取单价：根据合同号和品种自动匹配单价
    - 创建收款明细和结余明细（复用单条上传的逻辑）
    """
    try:
        if not weighbill_images:
            raise HTTPException(status_code=400, detail="请至少上传一张磅单图片")

        # 读取所有图片字节
        image_bytes_list = []
        for image_file in weighbill_images:
            allowed_types = ["image/jpeg", "image/jpg", "image/png", "image/bmp"]
            if image_file.content_type not in allowed_types:
                raise HTTPException(
                    status_code=400, 
                    detail=f"不支持的文件格式: {image_file.filename}，仅支持jpg/png/bmp"
                )
            
            content = await image_file.read()
            image_bytes_list.append(content)

        # 调用批量上传服务
        result = service.batch_upload_weighbills(
            warehouse_name=warehouse_name,
            payee_id=payee_id,
            image_files=image_bytes_list,
            current_user=current_user
        )

        if not result.get("success"):
            raise HTTPException(status_code=400, detail=result.get("error"))

        # 需要选择收款人
        if result.get("need_select_payee"):
            return WeighbillBatchUploadResponse(
                success=True,
                need_select_payee=True,
                message=result.get("message", "请选择收款人"),
                warehouse_name=result.get("warehouse_name"),
                payees=result.get("payees", [])
            )

        # 处理完成
        return WeighbillBatchUploadResponse(
            success=True,
            need_select_payee=False,
            message=f"批量上传完成：成功 {result['success_count']}/{result['total']} 条",
            warehouse_name=result.get("warehouse_name"),
            payee_id=result.get("payee_id"),
            payee_name=result.get("payee_name"),
            total=result["total"],
            success_count=result["success_count"],
            failed_count=result["failed_count"],
            success_list=result["success_list"],
            failed_list=result["failed_list"]
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"批量上传失败: {str(e)}")


@router.post("/batch-update-prices", summary="批量修改报单下磅单单价")
async def batch_update_weighbill_prices(
    request: BatchPriceUpdateRequest,
    service: WeighbillService = Depends(get_weighbill_service),
    current_user: dict = Depends(get_current_user)
):
    """
    批量修改指定报单下多个品种的磅单单价。
    - 每个品种对应的磅单必须已存在（通常报单创建时已生成占位磅单）
    - 修改单价后自动重新计算总金额，并同步更新收款明细和结余明细
    - 如果某个品种修改失败（如磅单不存在），会单独记录错误，不影响其他品种
    """
    try:
        # 转换为服务层期望的格式
        updates = [{"product_name": item.product_name, "unit_price": item.unit_price} for item in request.prices]
        result = service.batch_update_unit_prices(
            delivery_id=request.delivery_id,
            price_updates=updates,
            current_user=current_user
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"批量修改失败: {str(e)}")