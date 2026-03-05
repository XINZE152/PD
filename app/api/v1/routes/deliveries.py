"""
销售台账/报货订单路由
"""
import os
import shutil
from decimal import Decimal
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Depends, Query, Body
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from app.services.delivery_service import DeliveryService, get_delivery_service
from core.auth import get_current_user

router = APIRouter(prefix="/deliveries", tags=["销售台账/报货订单"])


# ============ 请求/响应模型 ============

class DeliveryCreateRequest(BaseModel):
    report_date: str = Field(..., description="报货日期")
    warehouse: Optional[str] = Field(None, description="送货库房")
    target_factory_id: Optional[int] = Field(None, description="目标工厂ID")
    target_factory_name: str = Field(..., description="目标工厂名称")
    product_name: str = Field(..., description="货物品种")
    quantity: float = Field(..., description="数量（吨）")
    vehicle_no: str = Field(..., description="车牌号")
    driver_name: str = Field(..., description="司机姓名")
    driver_phone: str = Field(..., description="司机电话")
    driver_id_card: Optional[str] = Field(None, description="身份证号")
    has_delivery_order: str = Field("无", description="是否有联单：有/无")
    payee: Optional[str] = Field(None, description="收款人")
    status: str = Field("待确认", description="状态")
    uploaded_by: Optional[str] = Field(None, description="上传者身份：司机/公司（用于判断来源）")
    reporter_id: Optional[int] = Field(None, description="报单人ID（关联pd_users.id）")  # 新增
    reporter_name: Optional[str] = Field(None, description="报单人姓名")  # 新增


class DeliveryUpdateRequest(BaseModel):
    report_date: Optional[str] = None
    warehouse: Optional[str] = None
    target_factory_id: Optional[int] = None
    target_factory_name: Optional[str] = None
    product_name: Optional[str] = None
    quantity: Optional[float] = None
    vehicle_no: Optional[str] = None
    driver_name: Optional[str] = None
    driver_phone: Optional[str] = None
    driver_id_card: Optional[str] = None
    has_delivery_order: Optional[str] = None
    payee: Optional[str] = None
    status: Optional[str] = None
    uploaded_by: Optional[str] = None
    reporter_id: Optional[int] = None  # 新增
    reporter_name: Optional[str] = None  # 新增


class DeliveryOut(BaseModel):
    id: int
    report_date: Optional[str] = None
    warehouse: Optional[str] = None
    target_factory_name: Optional[str] = None
    product_name: Optional[str] = None
    products: Optional[List[str]] = None
    quantity: Optional[float] = None
    vehicle_no: Optional[str] = None
    driver_name: Optional[str] = None
    driver_phone: Optional[str] = None
    driver_id_card: Optional[str] = None
    has_delivery_order: Optional[str] = None
    has_delivery_order_display: Optional[str] = None
    delivery_order_image: Optional[str] = None
    upload_status: Optional[str] = None
    upload_status_display: Optional[str] = None
    source_type: Optional[str] = None
    shipper: Optional[str] = None  # 报单人/发货人（冗余，实际用reporter_name）
    reporter_id: Optional[int] = None  # 新增：报单人ID
    reporter_name: Optional[str] = None  # 新增：报单人姓名
    payee: Optional[str] = None
    service_fee: Optional[float] = None  # 联单费
    contract_no: Optional[str] = None
    contract_unit_price: Optional[float] = None
    total_amount: Optional[float] = None
    status: Optional[str] = None
    uploader_id: Optional[int] = None
    uploader_name: Optional[str] = None
    uploaded_at: Optional[str] = None
    created_at: Optional[str] = None
    operations: Optional[dict] = None


# ============ 路由 ============

@router.post("/", response_model=dict)
async def create_delivery(
        report_date: str = Form(...),
        warehouse: Optional[str] = Form(None),
        target_factory_id: Optional[int] = Form(None),
        target_factory_name: str = Form(...),
        product_name: str = Form(..., description="主品种，随便填"),
        products: Optional[str] = Form(None, description="品种列表，逗号分隔，最多4个，用于计算品种数量"),
        quantity: float = Form(...),
        vehicle_no: str = Form(...),
        driver_name: str = Form(...),
        driver_phone: str = Form(...),
        driver_id_card: Optional[str] = Form(None),
        has_delivery_order: str = Form("无"),
        payee: Optional[str] = Form(None),
        status: str = Form("待确认"),
        uploaded_by: Optional[str] = Form(None),
        reporter_id: Optional[int] = Form(None, description="报单人ID"),  # 新增
        reporter_name: Optional[str] = Form(None, description="报单人姓名"),  # 新增
        confirm_flag: bool = Form(False, description="二次确认标志"),
        delivery_order_image: Optional[UploadFile] = File(None),
        service: DeliveryService = Depends(get_delivery_service),
        current_user: dict = Depends(get_current_user)
):
    """创建报货订单（支持上传联单图片）"""
    try:
        data = {
            "report_date": report_date,
            "warehouse": warehouse,
            "target_factory_id": target_factory_id,
            "target_factory_name": target_factory_name,
            "product_name": product_name,
            "products": products,  # ← 添加这行！
            "quantity": quantity,
            "vehicle_no": vehicle_no,
            "driver_name": driver_name,
            "driver_phone": driver_phone,
            "driver_id_card": driver_id_card,
            "has_delivery_order": has_delivery_order,
            "payee": payee,
            "status": status,
            "uploaded_by": uploaded_by,
            "reporter_id": reporter_id,
            "reporter_name": reporter_name,
        }

        image_bytes = None
        if delivery_order_image:
            image_bytes = await delivery_order_image.read()

        result = service.create_delivery(data, image_bytes, current_user, confirm_flag)

        if result.get("need_confirm"):
            raise HTTPException(
                status_code=409,
                detail={
                    "message": result.get("error"),
                    "existing_orders": result.get("existing_orders"),
                    "need_confirm": True
                }
            )

        if result["success"]:
            return result
        else:
            raise HTTPException(status_code=400, detail=result.get("error"))

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============ JSON 专用接口 ============

class DeliveryCreateJsonRequest(BaseModel):
    """JSON 格式创建报货订单请求体"""
    report_date: str = Field(..., description="报货日期")
    warehouse: Optional[str] = Field(None, description="送货库房")
    target_factory_id: Optional[int] = Field(None, description="目标工厂ID")
    target_factory_name: str = Field(..., description="目标工厂名称")
    product_name: str = Field(..., description="主品种")
    products: Optional[str] = Field(None, description="品种列表，逗号分隔")
    quantity: float = Field(..., description="数量（吨）")
    vehicle_no: str = Field(..., description="车牌号")
    driver_name: str = Field(..., description="司机姓名")
    driver_phone: str = Field(..., description="司机电话")
    driver_id_card: Optional[str] = Field(None, description="身份证号")
    has_delivery_order: str = Field("无", description="是否有联单：有/无")
    payee: Optional[str] = Field(None, description="收款人")
    status: str = Field("待确认", description="状态")
    uploaded_by: Optional[str] = Field(None, description="上传者身份：司机/公司")
    reporter_id: Optional[int] = Field(None, description="报单人ID")
    reporter_name: Optional[str] = Field(None, description="报单人姓名")
    confirm_flag: bool = Field(False, description="二次确认标志")


@router.post("/json", response_model=dict)
async def create_delivery_json(
        body: DeliveryCreateJsonRequest,
        service: DeliveryService = Depends(get_delivery_service),
        current_user: dict = Depends(get_current_user)
):
    """JSON 格式创建报货订单（不支持文件上传）"""
    try:
        # 转换为字典，兼容原有逻辑
        data = body.model_dump(exclude_none=False)

        # 调用原有服务方法
        result = service.create_delivery(data, None, current_user, data.get("confirm_flag", False))

        if result.get("need_confirm"):
            raise HTTPException(
                status_code=409,
                detail={
                    "message": result.get("error"),
                    "existing_orders": result.get("existing_orders"),
                    "need_confirm": True
                }
            )

        if result["success"]:
            return result
        else:
            raise HTTPException(status_code=400, detail=result.get("error"))

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
@router.get("/", response_model=dict)
async def list_deliveries(
    exact_shipper: Optional[str] = Query(None, description="精确发货人/报单人"),
    exact_contract_no: Optional[str] = Query(None, description="精确合同编号"),
    exact_report_date: Optional[str] = Query(None, description="精确报单日期"),
    exact_driver_name: Optional[str] = Query(None, description="精确司机姓名"),
    exact_vehicle_no: Optional[str] = Query(None, description="精确车号"),
    exact_has_delivery_order: Optional[str] = Query(None, description="是否自带联单：有/无"),
    exact_upload_status: Optional[str] = Query(None, description="是否上传联单：已上传/待上传"),
    exact_reporter_name: Optional[str] = Query(None, description="精确报单人姓名"),  # 新增
    exact_reporter_id: Optional[int] = Query(None, description="精确报单人ID"),  # 新增
    exact_factory_name: Optional[str] = Query(None, description="精确目标工厂"),
    exact_status: Optional[str] = Query(None, description="精确状态"),
    exact_driver_phone: Optional[str] = Query(None, description="精确司机电话"),
    fuzzy_keywords: Optional[str] = Query(None, description="模糊关键词"),
        date_from: Optional[str] = Query(None, description="开始日期"),
        date_to: Optional[str] = Query(None, description="结束日期"),
        page: int = Query(1, ge=1),
        page_size: int = Query(20, ge=1, le=100),
        service: DeliveryService = Depends(get_delivery_service)
):
    """查询报货订单列表"""
    return service.list_deliveries(
        exact_shipper=exact_shipper,
        exact_contract_no=exact_contract_no,
        exact_report_date=exact_report_date,
        exact_driver_name=exact_driver_name,
        exact_vehicle_no=exact_vehicle_no,
        exact_has_delivery_order=exact_has_delivery_order,
        exact_upload_status=exact_upload_status,
        exact_reporter_name=exact_reporter_name,  # 新增
        exact_reporter_id=exact_reporter_id,      # 新增
        exact_factory_name=exact_factory_name,
        exact_status=exact_status,
        exact_driver_phone=exact_driver_phone,
        fuzzy_keywords=fuzzy_keywords,
        date_from=date_from,
        date_to=date_to,
        page=page,
        page_size=page_size
    )


@router.get("/{delivery_id}", response_model=DeliveryOut)
async def get_delivery(
        delivery_id: int,
        service: DeliveryService = Depends(get_delivery_service)
):
    """查看订单详情"""
    delivery = service.get_delivery(delivery_id)
    if not delivery:
        raise HTTPException(status_code=404, detail="订单不存在")
    return delivery


@router.put("/{delivery_id}", response_model=dict)
async def update_delivery(
        delivery_id: int,
        request: DeliveryUpdateRequest,
        service: DeliveryService = Depends(get_delivery_service),
        current_user: str = "admin"
):
    """编辑报货订单（纯JSON，不涉及文件上传）"""
    try:
        data = {k: v for k, v in request.dict().items() if v is not None}

        if not data:
            raise HTTPException(status_code=400, detail="没有要更新的字段")

        result = service.update_delivery(delivery_id, data, None, False)

        if result["success"]:
            return result
        else:
            raise HTTPException(status_code=400, detail=result.get("error"))

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{delivery_id}")
async def delete_delivery(
        delivery_id: int,
        service: DeliveryService = Depends(get_delivery_service)
):
    """删除订单"""
    result = service.delete_delivery(delivery_id)
    if result["success"]:
        return result
    else:
        raise HTTPException(status_code=400, detail=result.get("error"))


@router.post("/{delivery_id}/upload-order")
async def upload_delivery_order(
        delivery_id: int,
        image: UploadFile = File(..., description="联单图片"),
        has_delivery_order: Optional[str] = Form(None, description="同步修改联单状态：有/无"),
        uploaded_by: str = Form("公司"),
        service: DeliveryService = Depends(get_delivery_service)
):
    """上传联单（仅未上传时可调用）"""
    try:
        delivery = service.get_delivery(delivery_id)
        if not delivery:
            raise HTTPException(status_code=404, detail="订单不存在")

        if delivery.get('upload_status') == '已上传':
            raise HTTPException(
                status_code=400,
                detail="该订单已上传联单，如需修改请使用 modify-order 接口"
            )

        image_bytes = await image.read()

        data = {}
        if has_delivery_order:
            data['has_delivery_order'] = has_delivery_order
            data['uploaded_by'] = uploaded_by

        result = service.update_delivery(delivery_id, data, image_bytes, uploaded_by=uploaded_by)

        if result["success"]:
            return {"success": True, "message": "联单上传成功", "data": result["data"]}
        else:
            raise HTTPException(status_code=400, detail=result.get("error"))

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{delivery_id}/modify-order")
async def modify_delivery_order(
        delivery_id: int,
        image: UploadFile = File(..., description="新的联单图片"),
        has_delivery_order: Optional[str] = Form(None, description="同步修改联单状态：有/无"),
        uploaded_by: str = Form("公司"),
        service: DeliveryService = Depends(get_delivery_service)
):
    """修改联单（已上传过的支持覆盖替换）"""
    try:
        delivery = service.get_delivery(delivery_id)
        if not delivery:
            raise HTTPException(status_code=404, detail="订单不存在")

        if delivery.get('upload_status') != '已上传':
            raise HTTPException(
                status_code=400,
                detail="该订单未上传联单，请使用 upload-order 接口"
            )

        image_bytes = await image.read()

        data = {}
        if has_delivery_order:
            data['has_delivery_order'] = has_delivery_order
            data['uploaded_by'] = uploaded_by

        result = service.update_delivery(delivery_id, data, image_bytes, uploaded_by=uploaded_by)

        if result["success"]:
            return {"success": True, "message": "联单修改成功", "data": result["data"]}
        else:
            raise HTTPException(status_code=400, detail=result.get("error"))

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{delivery_id}/image")
async def delete_delivery_image(
    delivery_id: int,
    service: DeliveryService = Depends(get_delivery_service)
):
    """删除联单图片"""
    result = service.update_delivery(delivery_id, {}, None, delete_image=True)
    if result["success"]:
        return {"success": True, "message": "联单图片已删除，联单费已更新为150元"}
    else:
        raise HTTPException(status_code=400, detail=result.get("error"))


@router.get("/{delivery_id}/view-order")
async def view_delivery_order(
    delivery_id: int,
    service: DeliveryService = Depends(get_delivery_service)
):
    """查看联单图片"""
    try:
        delivery = service.get_delivery(delivery_id)
        if not delivery:
            raise HTTPException(status_code=404, detail="订单不存在")

        image_path = delivery.get("delivery_order_image")
        if not image_path:
            raise HTTPException(status_code=404, detail="该订单没有上传联单图片")

        if not os.path.exists(image_path):
            raise HTTPException(status_code=404, detail="联单图片文件不存在")

        return FileResponse(
            path=image_path,
            media_type="image/jpeg",
            filename=f"delivery_order_{delivery_id}.jpg"
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取联单图片失败: {str(e)}")


@router.get("/{delivery_id}/image")
async def get_delivery_image(
    delivery_id: int,
    service: DeliveryService = Depends(get_delivery_service)
):
    """查看联单图片（兼容旧接口）"""
    return await view_delivery_order(delivery_id, service)