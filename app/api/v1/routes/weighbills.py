"""
磅单管理路由 - 支持一报单多品种（最多4个）
"""
import logging
import json
import os
import re
import shutil
from pathlib import Path
from typing import Optional, List

from fastapi import APIRouter, UploadFile, File, HTTPException, Depends, Query, Body, Form
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field, ValidationError

from app.core.paths import TEMP_UPLOADS_DIR, UPLOADS_DIR
from app.services.weighbill_service import WeighbillService, get_weighbill_service
from app.services.contract_service import get_conn
from app.services.payment_services import PaymentService
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
    uploader_id: Optional[int] = None
    uploader_name: Optional[str] = None
    uploaded_at: Optional[str] = None
    # 报单信息
    report_date: Optional[str] = None
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


# ============ 路由 ============

@router.post("/ocr", response_model=WeighbillOCRResponse)
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


@router.post("/create", response_model=dict)
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
        is_manual: bool = Form(False, description="是否人工修正"),
        weighbill_image: UploadFile = File(..., description="磅单图片"),
        service: WeighbillService = Depends(get_weighbill_service),
        current_user: dict = Depends(get_current_user)
):
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

            except Exception as e:
                logger.error(f"自动创建收款明细失败: {e}", exc_info=True)
                result["data"]["payment_detail_created"] = False
                result["data"]["payment_detail_error"] = str(e)
            # ========== 新增结束 ==========
            
            return result
        else:
            raise HTTPException(status_code=400, detail=result.get("error"))

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/modify", response_model=dict)
async def modify_weighbill(
        weighbill_id: int = Form(..., description="磅单ID"),
        weigh_date: Optional[str] = Form(None, description="磅单日期"),
        weigh_ticket_no: Optional[str] = Form(None, description="过磅单号"),
        contract_no: Optional[str] = Form(None, description="合同编号"),
        vehicle_no: Optional[str] = Form(None, description="车牌号"),
        gross_weight: Optional[float] = Form(None, description="毛重"),
        tare_weight: Optional[float] = Form(None, description="皮重"),
        net_weight: Optional[float] = Form(None, description="净重"),
        delivery_time: Optional[str] = Form(None, description="送货时间"),
        unit_price: Optional[float] = Form(None, description="单价"),
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
                  'gross_weight', 'tare_weight', 'net_weight', 'delivery_time', 'unit_price']

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

        result = service.upload_weighbill(
            delivery_id=existing.get('delivery_id'),
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
                
                delivery_id = existing.get('delivery_id')
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


@router.get("/", response_model=dict)
async def list_weighbills(
    exact_shipper: Optional[str] = Query(None, description="精确发货人/报单人"),
    exact_contract_no: Optional[str] = Query(None, description="精确合同编号"),
    exact_report_date: Optional[str] = Query(None, description="精确报单日期"),
    exact_driver_name: Optional[str] = Query(None, description="精确司机姓名"),
    exact_vehicle_no: Optional[str] = Query(None, description="精确车号"),
    exact_weigh_date: Optional[str] = Query(None, description="精确磅单日期"),
    exact_ocr_status: Optional[str] = Query(None, description="精确磅单状态：待上传磅单/已上传磅单"),
        page: int = Query(1, ge=1),
        page_size: int = Query(20, ge=1, le=100),
        service: WeighbillService = Depends(get_weighbill_service)
):
    """
    查询磅单列表（按报单ID分组）

    表头：合同编号、报单日期、报送冶炼厂、司机电话、司机姓名、司机身份证号、
          车牌号、品种、是否自带联单、是否上传联单、报单人/发货人、收款人、送货库房、
          磅单日期、过磅单号、毛重、皮重、净重、单价、金额、磅单状态、磅单图片、
          人工修正、送货时间、操作
    """
    try:
        return service.list_weighbills_grouped(
            exact_shipper=exact_shipper,
            exact_contract_no=exact_contract_no,
            exact_report_date=exact_report_date,
            exact_driver_name=exact_driver_name,
            exact_vehicle_no=exact_vehicle_no,
            exact_weigh_date=exact_weigh_date,
            exact_ocr_status=exact_ocr_status,
            page=page,
            page_size=page_size,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{weighbill_id}", response_model=WeighbillOut)
async def get_weighbill(
        weighbill_id: int,
        service: WeighbillService = Depends(get_weighbill_service)
):
    """查看磅单详情"""
    bill = service.get_weighbill(weighbill_id)
    if not bill:
        raise HTTPException(status_code=404, detail="磅单不存在")
    return bill


@router.get("/delivery/{delivery_id}", response_model=WeighbillGroupOut)
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


@router.delete("/{weighbill_id}")
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


@router.get("/{weighbill_id}/image")
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


@router.put("/{weighbill_id}/payment-schedule", response_model=dict)
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