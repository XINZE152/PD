"""
销售台账/报货订单路由
"""
import json
import os
import re
from typing import List, Dict,Optional, Any
import logging
from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Depends, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from datetime import datetime
import mimetypes
from fastapi import Request, Response
from fastapi import Body
from urllib.parse import quote
from fastapi import Query
from app.services.delivery_service import DeliveryService, get_delivery_service
from core.auth import get_current_user
from core.database import get_conn

router = APIRouter(prefix="/deliveries", tags=["销售台账/报货订单"])
logger = logging.getLogger(__name__)

# ============ 请求/响应模型 ============

class DeliveryCreateRequest(BaseModel):
    report_date: str = Field(..., description="报货日期")
    target_factory_id: Optional[int] = Field(None, description="目标工厂ID")
    target_factory_name: str = Field(..., description="目标工厂名称")
    product_name: str = Field(..., description="货物品种")
    quantity: float = Field(..., description="数量（吨）")
    vehicle_no: str = Field(..., description="车牌号")
    driver_name: str = Field(..., description="司机姓名")
    driver_phone: str = Field(..., description="司机电话")
    driver_id_card: Optional[str] = Field(None, description="身份证号")
    has_delivery_order: str = Field("无", description="是否有联单：有/无")
    status: str = Field("待确认", description="状态")
    uploaded_by: Optional[str] = Field(None, description="上传者身份：司机/公司（用于判断来源）")
    reporter_id: Optional[int] = Field(None, description="报单人ID（关联pd_users.id）")  # 新增
    reporter_name: Optional[str] = Field(None, description="报单人姓名")  # 新增


class DeliveryUpdateRequest(BaseModel):
    report_date: Optional[str] = None
    target_factory_id: Optional[int] = None
    target_factory_name: Optional[str] = None
    product_name: Optional[str] = None
    quantity: Optional[float] = None
    vehicle_no: Optional[str] = None
    driver_name: Optional[str] = None
    driver_phone: Optional[str] = None
    driver_id_card: Optional[str] = None
    has_delivery_order: Optional[str] = None
    status: Optional[str] = None
    uploaded_by: Optional[str] = None
    reporter_id: Optional[int] = None  # 新增
    reporter_name: Optional[str] = None  # 新增
    contract_no: Optional[str] = None

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
    voucher_images: Optional[List[str]] = None
    position: Optional[str] = None  # 岗位
    submitter_name: Optional[str] = None  # 提交人姓名

class BatchDeliveryOrderItem(BaseModel):
    """单个联单上传项（内部使用）"""
    delivery_id: int = Field(..., description="报单ID")
    has_delivery_order: Optional[str] = Field(None, description="是否有联单：有/无")
    uploaded_by: Optional[str] = Field("公司", description="上传者身份：司机/公司")


class BatchUploadResult(BaseModel):
    """单个上传结果"""
    index: int = Field(..., description="批次中的索引")
    delivery_id: int = Field(..., description="报单ID")
    success: bool = Field(..., description="是否成功")
    message: str = Field(..., description="结果消息")
    image_path: Optional[str] = Field(None, description="图片保存路径")
    upload_status: Optional[str] = Field(None, description="上传状态")
    service_fee: Optional[float] = Field(None, description="联单费")
    source_type: Optional[str] = Field(None, description="来源类型")


class BatchDeliveryOrderResponse(BaseModel):
    """批量上传响应"""
    success: bool = Field(..., description="整体是否成功")
    message: str = Field(..., description="整体消息")
    total_count: int = Field(..., description="总数量")
    success_count: int = Field(..., description="成功数量")
    failed_count: int = Field(..., description="失败数量")
    results: List[BatchUploadResult] = Field(..., description="详细结果列表")

class TextExtractRequest(BaseModel):
    """文本提取请求"""
    text: str = Field(..., description="待提取的非结构化文本")
    report_date: Optional[str] = Field(None, description="报单日期 (ISO格式 YYYY-MM-DD)")
    
    class Config:
        # 允许任意字符串，包括包含换行符的
        json_schema_extra = {
            "example": {
                "text": "金利 自带联单 车号：冀A013TJ",
                "report_date": "2024-01-01"
            }
        }

class ContractMatchInfo(BaseModel):
    """合同匹配信息"""
    matched: bool = Field(..., description="是否匹配到合同")
    match_type: str = Field(..., description="匹配类型: exact/fuzzy/none/error")
    reason: Optional[str] = Field(None, description="未匹配原因或匹配说明")
    contract_no: Optional[str] = Field(None, description="合同编号")
    contract_id: Optional[int] = Field(None, description="合同ID")
    unit_price: Optional[float] = Field(None, description="合同单价")
    smelter_company: Optional[str] = Field(None, description="冶炼厂/工厂名称")
    matched_product: Optional[str] = Field(None, description="匹配到的品种")


class ValidationResult(BaseModel):
    """字段验证结果"""
    is_valid: bool = Field(..., description="验证是否通过")
    missing_fields: List[str] = Field(default_factory=list, description="缺失的必填字段列表")
    data: Dict[str, Any] = Field(default_factory=dict, description="验证后的数据")


class TextExtractResponse(BaseModel):
    """文本提取响应"""
    success: bool = Field(..., description="操作是否成功")
    message: str = Field(..., description="提示信息")
    extracted: Dict[str, Any] = Field(default_factory=dict, description="提取的字段数据")
    validation: ValidationResult = Field(..., description="验证结果")
    contract_match: ContractMatchInfo = Field(..., description="合同匹配信息")
    contract_no: Optional[str] = Field(None, description="匹配到的合同编号（若有）")
    contract_id: Optional[int] = Field(None, description="匹配到的合同ID（若有）")
    ready_to_create: bool = Field(..., description="是否可直接创建报单")
    suggested_data: Optional[Dict[str, Any]] = Field(None, description="建议的报单数据")
# ============ 路由 ============

@router.post("/parse", summary="解析报单文本", response_model=TextExtractResponse)
async def parse_delivery_text(
    request: Request,  # 使用原始 Request 绕过 Pydantic 验证
    service: DeliveryService = Depends(get_delivery_service)
):
    """解析上传的非结构化报单文本（支持包含换行符的 JSON）"""
    try:
        # 读取原始字节
        body_bytes = await request.body()
        body_str = body_bytes.decode('utf-8', errors='ignore')
        
        # 自定义 JSON 解析：先清理控制字符，再解析
        try:
            # 方法1：尝试标准解析（处理已转义的 \n）
            data = json.loads(body_str)
        except json.JSONDecodeError as e:
            # 方法2：如果失败，尝试清理原始控制字符后再解析
            # 将原始换行符、回车符替换为 \n 转义序列
            cleaned_json = body_str.replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
            try:
                data = json.loads(cleaned_json)
            except json.JSONDecodeError:
                raise HTTPException(status_code=400, detail=f"JSON解析错误: {str(e)}")
        
        text = data.get('text', '')
        report_date = data.get('report_date')
        
        if not text:
            raise HTTPException(status_code=400, detail="text 字段不能为空")
        
        # 清理文本内容（将转义的换行符等替换为空格）
        # 注意：此时 text 中的 \n 已经是字符串，不是控制字符
        clean_text = text.replace('\\n', ' ').replace('\\r', ' ').replace('\\t', ' ')
        # 移除其他不可打印控制字符（以防万一）
        clean_text = ''.join(char for char in clean_text if ord(char) >= 32 or char in '\n\r\t')
        # 合并多余空格
        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
        
        result = service.extract_with_contract(clean_text, report_date=report_date)
        
        return TextExtractResponse(
            success=result.get('success', True),
            message=result.get('reason', '解析完成'),
            extracted=result.get('extracted', {}),
            validation=result.get('validation', {
                'is_valid': False,
                'missing_fields': [],
                'data': {}
            }),
            contract_match=result.get('contract_match', {
                'matched': False,
                'match_type': 'none'
            }),
            contract_no=(result.get('contract_match') or {}).get('contract_no') or result.get('extracted', {}).get('contract_no'),
            contract_id=(result.get('contract_match') or {}).get('contract_id') or result.get('extracted', {}).get('contract_id'),
            ready_to_create=result.get('ready_to_create', False),
            suggested_data=result.get('suggested_data')
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"解析报单文本失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/", summary="新增报货订单", response_model=dict)
async def create_delivery(
    report_date: str = Form(...),
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
    status: str = Form("待确认"),
    uploaded_by: Optional[str] = Form(None),
    reporter_id: Optional[int] = Form(None, description="报单人ID"),
    reporter_name: Optional[str] = Form(None, description="报单人姓名"),
    position: Optional[str] = Form(None, description="岗位"),
    submitter_name: Optional[str] = Form(None, description="提交人姓名"),
    confirm_flag: bool = Form(False, description="二次确认标志"),
    delivery_order_image: Optional[UploadFile] = File(None, description="有联单时上传的联单图片"),
    voucher_images: List[UploadFile] = File(None, description="无联单时上传的凭证图片（最多6张）"),
    service: DeliveryService = Depends(get_delivery_service),
    current_user: dict = Depends(get_current_user)
):
    """创建报货订单（支持联单图片或凭证图片）"""
    try:
        data = {
            "report_date": report_date,
            "target_factory_id": target_factory_id,
            "target_factory_name": target_factory_name,
            "product_name": product_name,
            "products": products,
            "quantity": quantity,
            "vehicle_no": vehicle_no,
            "driver_name": driver_name,
            "driver_phone": driver_phone,
            "driver_id_card": driver_id_card,
            "has_delivery_order": has_delivery_order,
            "status": status,
            "uploaded_by": uploaded_by,
            "reporter_id": reporter_id,
            "reporter_name": reporter_name,
            "position": position,
            "submitter_name": submitter_name,
        }

        # 读取联单图片
        delivery_img_bytes = None
        if delivery_order_image:
            delivery_img_bytes = await delivery_order_image.read()

        # 读取多张凭证图片
        voucher_bytes_list = []
        if voucher_images:
            for f in voucher_images:
                voucher_bytes_list.append(await f.read())

        result = service.create_delivery(
            data,
            delivery_order_image=delivery_img_bytes,
            voucher_images=voucher_bytes_list,
            current_user=current_user,
            confirm_flag=confirm_flag
        )

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
    status: str = Field("待确认", description="状态")
    uploaded_by: Optional[str] = Field(None, description="上传者身份：司机/公司")
    reporter_id: Optional[int] = Field(None, description="报单人ID")
    reporter_name: Optional[str] = Field(None, description="报单人姓名")
    position: Optional[str] = Field(None, description="岗位")
    submitter_name: Optional[str] = Field(None, description="提交人姓名")
    confirm_flag: bool = Field(False, description="二次确认标志")

@router.post("/{delivery_id}/vouchers/append", summary="追加凭证图片")
async def append_voucher_images(
    delivery_id: int,
    images: List[UploadFile] = File(..., description="要追加的凭证图片，最多不超过总张数6"),
    service: DeliveryService = Depends(get_delivery_service)
):
    """追加凭证图片（不会删除原有图片）"""
    try:
        img_bytes_list = [await f.read() for f in images]
        result = service.add_voucher_images(delivery_id, img_bytes_list)
        if result["success"]:
            return result
        else:
            raise HTTPException(status_code=400, detail=result.get("error"))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/{delivery_id}/vouchers/{index}", summary="删除指定凭证图片")
async def delete_voucher_image(
    delivery_id: int,
    index: int,
    service: DeliveryService = Depends(get_delivery_service)
):
    """按索引删除凭证图片（0-based）"""
    result = service.remove_voucher_image(delivery_id, index)
    if result["success"]:
        return result
    else:
        raise HTTPException(status_code=400, detail=result.get("error"))

@router.get("/{delivery_id}/vouchers", summary="获取凭证图片列表")
async def list_voucher_images(
    delivery_id: int,
    service: DeliveryService = Depends(get_delivery_service)
):
    """返回凭证图片路径列表"""
    paths = service.get_voucher_images(delivery_id)
    return {"voucher_images": paths}


@router.get(
    "/{delivery_id}/vouchers/image",
    summary="预览凭证图片",
    responses={
        200: {
            "content": {
                "image/jpeg": {},
                "image/png": {},
                "image/bmp": {},
                "image/webp": {},
                "image/gif": {},
            },
            "description": "凭证图片",
        }
    },
)
async def view_voucher_image(
    delivery_id: int,
    index: int = Query(0, ge=0, description="图片索引，从0开始（默认第一张）"),
    service: DeliveryService = Depends(get_delivery_service)
):
    """
    按索引预览指定订单的凭证图片（支持多张图片）
    """
    try:
        # 获取订单详情（包含 voucher_images 列表）
        delivery = service.get_delivery(delivery_id)
        if not delivery:
            raise HTTPException(status_code=404, detail="订单不存在")

        # 获取凭证图片列表
        voucher_paths = delivery.get("voucher_images", [])
        if not voucher_paths:
            raise HTTPException(status_code=404, detail="该订单没有凭证图片")

        # 校验索引范围
        if index < 0 or index >= len(voucher_paths):
            raise HTTPException(
                status_code=404,
                detail=f"图片索引 {index} 超出范围，共有 {len(voucher_paths)} 张"
            )

        image_path = voucher_paths[index]
        if not image_path or not os.path.exists(image_path):
            raise HTTPException(status_code=404, detail="图片文件不存在")

        # 自动识别 MIME 类型
        mime_type, _ = mimetypes.guess_type(image_path)
        if not mime_type:
            mime_type = "image/jpeg"  # 默认

        # 对文件名进行 RFC 5987 编码（支持中文）
        filename = os.path.basename(image_path)
        try:
            # 尝试 ASCII 编码
            filename.encode('ascii')
            disposition = f'inline; filename="{filename}"'
        except UnicodeEncodeError:
            # 非 ASCII 文件名，使用 UTF-8 编码
            encoded_filename = quote(filename, safe='')
            disposition = f"inline; filename*=UTF-8''{encoded_filename}"

        return FileResponse(
            path=image_path,
            media_type=mime_type,
            headers={"Content-Disposition": disposition}
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"预览凭证图片失败: {e}")
        raise HTTPException(status_code=500, detail=f"预览失败: {str(e)}")
@router.put("/{delivery_id}/vouchers", summary="整体替换凭证图片")
async def replace_voucher_images(
    delivery_id: int,
    voucher_images: List[UploadFile] = File(..., description="新的凭证图片列表，最多6张"),
    service: DeliveryService = Depends(get_delivery_service),
    current_user: dict = Depends(get_current_user)
):
    """整体替换凭证图片（会删除原有所有凭证图片）"""
    try:
        voucher_bytes_list = [await f.read() for f in voucher_images]
        result = service.update_delivery(
            delivery_id,
            data={},
            delivery_order_image=None,
            voucher_images=voucher_bytes_list,
            uploaded_by=current_user.get('name') if current_user else None
        )
        if result["success"]:
            return result
        else:
            raise HTTPException(status_code=400, detail=result.get("error"))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/json", summary="JSON 新增报货订单", response_model=dict)
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
@router.get("/", summary="查询报货订单列表", response_model=dict)
async def list_deliveries(
    exact_delivery_id: Optional[int] = Query(None, description="精确报单ID"),
    exact_shipper: Optional[str] = Query(None, description="精确发货人/报单人"),
    exact_contract_no: Optional[str] = Query(None, description="精确合同编号"),
    exact_report_date: Optional[str] = Query(None, description="精确报单日期"),
    exact_driver_name: Optional[str] = Query(None, description="精确司机姓名"),
    exact_vehicle_no: Optional[str] = Query(None, description="精确车号"),
    exact_has_delivery_order: Optional[str] = Query(None, description="是否自带联单：有/无"),
    exact_upload_status: Optional[str] = Query(None, description="是否上传联单：已上传/未上传"),
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
        exact_delivery_id=exact_delivery_id,
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


@router.get("/{delivery_id}", summary="查看报货订单详情", response_model=DeliveryOut)
async def get_delivery(
        delivery_id: int,
        service: DeliveryService = Depends(get_delivery_service)
):
    """查看订单详情"""
    delivery = service.get_delivery(delivery_id)
    if not delivery:
        raise HTTPException(status_code=404, detail="订单不存在")
    return delivery


@router.put("/{delivery_id}", summary="编辑报货订单", response_model=dict)
async def update_delivery(
        delivery_id: int,
        request: DeliveryUpdateRequest,
        service: DeliveryService = Depends(get_delivery_service),
        current_user: dict = Depends(get_current_user)   # 注入当前用户
):
    try:
        data = {k: v for k, v in request.dict().items() if v is not None}
        if not data:
            raise HTTPException(status_code=400, detail="没有要更新的字段")

        # 调用服务层，传入 current_user
        result = service.update_delivery(
            delivery_id,
            data,
            None,
            None,
            delete_image=False,
            uploaded_by=None,
            current_user=current_user          # 新增参数
        )

        if result["success"]:
            return result
        else:
            raise HTTPException(status_code=400, detail=result.get("error"))

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{delivery_id}", summary="删除报货订单")
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


@router.post("/{delivery_id}/upload-order", summary="上传联单图片")
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

# deliveries.py

class AuditRequest(BaseModel):
    status: str = Field(..., description="新审核状态，如'已确认'")

@router.post("/{delivery_id}/audit", summary="审核报单")
async def audit_delivery(
    delivery_id: int,
    body: AuditRequest,
    service: DeliveryService = Depends(get_delivery_service),
    current_user: dict = Depends(get_current_user)
):
    """
    审核报单，修改审核状态。
    仅审核主管或管理员可操作。
    """
    result = service.audit_delivery(delivery_id, body.status, current_user)
    if result["success"]:
        return result
    raise HTTPException(status_code=400, detail=result.get("error"))
@router.put("/{delivery_id}/modify-order", summary="修改联单图片")
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


@router.delete("/{delivery_id}/image", summary="删除联单图片")
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


@router.get("/{delivery_id}/view-order", summary="查看联单图片")
async def view_delivery_order(
    delivery_id: int,
    service: DeliveryService = Depends(get_delivery_service)
):
    """查看联单图片（仅支持图片格式，PDF 请使用 /view-pdf 接口）"""
    try:
        delivery = service.get_delivery(delivery_id)
        if not delivery:
            raise HTTPException(status_code=404, detail="订单不存在")

        image_path = delivery.get("delivery_order_image")
        if not image_path:
            raise HTTPException(status_code=404, detail="该订单没有上传联单文件")

        if not os.path.exists(image_path):
            raise HTTPException(status_code=404, detail="联单文件不存在")

        # 获取文件扩展名（小写）
        ext = os.path.splitext(image_path)[1].lower()

        # 定义图片扩展名集合
        image_exts = {'.jpg', '.jpeg', '.png', '.bmp', '.webp', '.gif'}

        if ext == '.pdf':
            # 如果是 PDF，返回错误并提示使用 PDF 接口
            raise HTTPException(
                status_code=400,
                detail="该联单为 PDF 格式，请使用 /view-pdf 接口预览"
            )
        elif ext in image_exts:
            # 图片格式，正常返回
            return FileResponse(
                path=image_path,
                media_type=f"image/{ext[1:]}",  # 如 image/jpeg
                filename=f"delivery_order_{delivery_id}{ext}"
            )
        else:
            # 其他未知格式，作为通用二进制文件返回
            return FileResponse(
                path=image_path,
                media_type="application/octet-stream",
                filename=f"delivery_order_{delivery_id}{ext}"
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"查看联单文件失败: {e}")
        raise HTTPException(status_code=500, detail=f"查看失败: {str(e)}")

@router.get("/{delivery_id}/image", summary="查看联单图片（兼容旧接口）")
async def get_delivery_image(
    delivery_id: int,
    service: DeliveryService = Depends(get_delivery_service)
):
    """查看联单图片（兼容旧接口）"""
    return await view_delivery_order(delivery_id, service)


@router.post("/batch-upload-orders", summary="批量上传联单图片", response_model=BatchDeliveryOrderResponse)
async def batch_upload_delivery_orders(
        files: List[UploadFile] = File(..., description="联单图片列表（与delivery_ids一一对应）"),
        delivery_ids: str = Form(..., description="报单ID列表，字符串，如：[1,2,3]"),
        has_delivery_orders: Optional[str] = Form(None,
                                                  description="联单状态列表，字符串，如：[\"有\",\"有\",\"无\"]"),
        uploaded_by: str = Form("公司", description="上传者身份：司机/公司"),
        use_batch_mode: bool = Form(True, description="是否使用批量模式（复用数据库连接，推荐）"),
        service: DeliveryService = Depends(get_delivery_service),
        current_user: dict = Depends(get_current_user)
):
    """
    批量上传联单图片到对应报单

    ## 依旧是form格式

    ## 使用说明

    ### 参数对应关系
    - `files`: 联单图片文件列表，与 `delivery_ids` **按索引一一对应**
    - `delivery_ids`: JSON 数组格式的报单 ID 列表
    - `has_delivery_orders`: JSON 数组格式的联单状态列表（可选，默认全部为"有"）

    ### 调用示例
    ```bash
    curl -X POST "http://api/deliveries/batch-upload-orders" \

      -H "Authorization: Bearer {token}" \

      -F "files=@order1.jpg" \

      -F "files=@order2.jpg" \

      -F "delivery_ids=[101, 102]" \

      -F "has_delivery_orders=[\"有\",\"有\"]" \

      -F "uploaded_by=公司" \

      -F "use_batch_mode=true"

    ```

    ### 注意事项
    1. 图片数量必须与 delivery_ids 长度一致
    2. 已上传联单的报单会被跳过（返回错误，需单独调用 modify-order 接口修改）
    3. 单张图片失败不影响其他图片处理
    4. 建议单次上传不超过 50 张图片
    5. 使用 batch_mode=true 时复用数据库连接，性能更好

    ### 响应说明
    - `success`: 整体处理是否完成（只要接口正常返回就是 true）
    - `success_count`: 实际上传成功的数量
    - `failed_count`: 失败的数量
    - `results`: 每个文件的详细处理结果
    """
    import json

    # 限制单次上传数量
    MAX_BATCH_SIZE = 50
    if len(files) > MAX_BATCH_SIZE:
        raise HTTPException(
            status_code=400,
            detail=f"单次上传数量不能超过 {MAX_BATCH_SIZE} 张，当前 {len(files)} 张"
        )

    try:
        # ==================== 智能解析 delivery_ids ====================
        delivery_id_list = None

        # 尝试1：标准 JSON 格式 [1,2,3]
        try:
            parsed = json.loads(delivery_ids)
            if isinstance(parsed, list):
                delivery_id_list = parsed
            elif isinstance(parsed, int):
                delivery_id_list = [parsed]
        except (json.JSONDecodeError, ValueError):
            pass

        # 尝试2：逗号分隔格式 "1,2,3" 或 "1"
        if delivery_id_list is None:
            try:
                id_strs = [s.strip() for s in delivery_ids.replace('，', ',').split(',') if s.strip()]
                delivery_id_list = [int(s) for s in id_strs]
            except ValueError:
                pass

        # 尝试3：单个数字字符串 "1"
        if delivery_id_list is None:
            try:
                delivery_id_list = [int(delivery_ids.strip())]
            except ValueError:
                pass

        if delivery_id_list is None or len(delivery_id_list) == 0:
            raise HTTPException(status_code=400, detail="delivery_ids 格式错误，请使用 [1,2,3] 或 1,2,3 或 1 格式")

        # ==================== 智能解析 has_delivery_orders ====================
        has_order_list = None

        if has_delivery_orders:
            # 尝试1：标准 JSON 格式 ["有","无"]
            try:
                parsed = json.loads(has_delivery_orders)
                if isinstance(parsed, list):
                    has_order_list = parsed
                elif isinstance(parsed, str):
                    has_order_list = [parsed]
            except (json.JSONDecodeError, ValueError):
                pass

            # 尝试2：逗号分隔格式 "有,有,无" 或 "有"
            if has_order_list is None:
                clean_str = has_delivery_orders.strip().strip('"').strip("'")
                has_order_list = [s.strip().strip('"').strip("'") for s in clean_str.replace('，', ',').split(',') if
                                  s.strip()]

            # 验证长度
            if len(has_order_list) != len(delivery_id_list):
                raise HTTPException(
                    status_code=400,
                    detail=f"has_delivery_orders 数量({len(has_order_list)})与 delivery_ids({len(delivery_id_list)}) 不一致"
                )

        # 如果没有提供 has_delivery_orders，默认全部为"有"
        if has_order_list is None:
            has_order_list = ['有'] * len(delivery_id_list)

        # 验证图片数量
        if len(files) != len(delivery_id_list):
            raise HTTPException(
                status_code=400,
                detail=f"图片数量({len(files)})与报单ID数量({len(delivery_id_list)})不一致"
            )

        # ==================== 批量模式（推荐）====================
        if use_batch_mode and len(files) > 1:
            # 预读取所有图片并验证
            items = []
            pre_check_results = []

            for idx, (file, delivery_id) in enumerate(zip(files, delivery_id_list)):
                # 验证文件类型
                allowed_types = ["image/jpeg", "image/jpg", "image/png", "image/bmp", "image/webp"]
                if file.content_type not in allowed_types:
                    pre_check_results.append({
                        "index": idx,
                        "delivery_id": delivery_id,
                        "success": False,
                        "message": f"不支持的文件格式: {file.content_type}，仅支持 jpg/png/bmp/webp",
                        "pre_check_failed": True
                    })
                    continue

                try:
                    # 读取图片字节（限制 10MB）
                    MAX_FILE_SIZE = 10 * 1024 * 1024
                    image_bytes = await file.read()

                    if len(image_bytes) > MAX_FILE_SIZE:
                        pre_check_results.append({
                            "index": idx,
                            "delivery_id": delivery_id,
                            "success": False,
                            "message": f"文件大小超过 10MB 限制",
                            "pre_check_failed": True
                        })
                        continue

                    # 预检查报单状态（避免在事务中查询）
                    delivery = service.get_delivery(delivery_id)
                    if not delivery:
                        pre_check_results.append({
                            "index": idx,
                            "delivery_id": delivery_id,
                            "success": False,
                            "message": "报单不存在",
                            "pre_check_failed": True
                        })
                        continue

                    if delivery.get('upload_status') == '已上传':
                        pre_check_results.append({
                            "index": idx,
                            "delivery_id": delivery_id,
                            "success": False,
                            "message": "已上传联单，请使用 modify-order 接口修改",
                            "image_path": delivery.get('delivery_order_image'),
                            "upload_status": "已上传",
                            "service_fee": float(delivery.get('service_fee', 0)),
                            "pre_check_failed": True
                        })
                        continue

                    # 通过预检查，加入批量处理列表
                    items.append({
                        'index': idx,
                        'delivery_id': delivery_id,
                        'image_bytes': image_bytes,
                        'has_delivery_order': has_order_list[idx] if has_order_list else '有'
                    })

                except Exception as e:
                    pre_check_results.append({
                        "index": idx,
                        "delivery_id": delivery_id,
                        "success": False,
                        "message": f"文件读取失败: {str(e)}",
                        "pre_check_failed": True
                    })

            # 调用批量更新服务（复用数据库连接）
            batch_results = []
            if items:
                batch_results = service.batch_update_delivery_images(items, uploaded_by)

            # 合并预检查失败结果和批量处理结果
            all_results = pre_check_results + batch_results

            # 按索引排序
            all_results.sort(key=lambda x: x.get('index', 0))

            # 统计结果
            success_count = sum(1 for r in all_results if r.get('success'))
            failed_count = len(all_results) - success_count

            # 转换为响应模型
            results = []
            for r in all_results:
                results.append(BatchUploadResult(
                    index=r['index'],
                    delivery_id=r.get('delivery_id', delivery_id_list[r['index']]),
                    success=r.get('success', False),
                    message=r.get('message') or r.get('error', '处理失败'),
                    image_path=r.get('image_path'),
                    upload_status=r.get('upload_status'),
                    service_fee=r.get('service_fee'),
                    source_type=r.get('source_type')
                ))

            return BatchDeliveryOrderResponse(
                success=True,
                message=f"批量上传完成（批量模式）：成功 {success_count}/{len(files)} 条",
                total_count=len(files),
                success_count=success_count,
                failed_count=failed_count,
                results=results
            )

        # ==================== 单条模式（兼容旧逻辑）====================
        else:
            results = []
            success_count = 0
            failed_count = 0

            for idx, (file, delivery_id) in enumerate(zip(files, delivery_id_list)):
                try:
                    # 验证文件类型
                    allowed_types = ["image/jpeg", "image/jpg", "image/png", "image/bmp", "image/webp"]
                    if file.content_type not in allowed_types:
                        results.append(BatchUploadResult(
                            index=idx,
                            delivery_id=delivery_id,
                            success=False,
                            message=f"不支持的文件格式: {file.content_type}",
                            image_path=None,
                            upload_status=None,
                            service_fee=None,
                            source_type=None
                        ))
                        failed_count += 1
                        continue

                    # 读取图片（限制 10MB）
                    MAX_FILE_SIZE = 10 * 1024 * 1024
                    image_bytes = await file.read()

                    if len(image_bytes) > MAX_FILE_SIZE:
                        results.append(BatchUploadResult(
                            index=idx,
                            delivery_id=delivery_id,
                            success=False,
                            message="文件大小超过 10MB 限制",
                            image_path=None,
                            upload_status=None,
                            service_fee=None,
                            source_type=None
                        ))
                        failed_count += 1
                        continue

                    # 检查报单
                    delivery = service.get_delivery(delivery_id)
                    if not delivery:
                        results.append(BatchUploadResult(
                            index=idx,
                            delivery_id=delivery_id,
                            success=False,
                            message="报单不存在",
                            image_path=None,
                            upload_status=None,
                            service_fee=None,
                            source_type=None
                        ))
                        failed_count += 1
                        continue

                    if delivery.get('upload_status') == '已上传':
                        results.append(BatchUploadResult(
                            index=idx,
                            delivery_id=delivery_id,
                            success=False,
                            message="已上传联单，请使用 modify-order 接口修改",
                            image_path=delivery.get('delivery_order_image'),
                            upload_status='已上传',
                            service_fee=float(delivery.get('service_fee', 0)),
                            source_type=delivery.get('source_type')
                        ))
                        failed_count += 1
                        continue

                    # 准备数据
                    data = {
                        'has_delivery_order': has_order_list[idx] if has_order_list else '有',
                        'uploaded_by': uploaded_by
                    }

                    # 调用服务层更新
                    result = service.update_delivery(delivery_id, data, image_bytes, uploaded_by=uploaded_by)

                    if result.get("success"):
                        results.append(BatchUploadResult(
                            index=idx,
                            delivery_id=delivery_id,
                            success=True,
                            message="联单上传成功",
                            image_path=result["data"].get("delivery_order_image"),
                            upload_status=result["data"].get("upload_status"),
                            service_fee=result["data"].get("service_fee"),
                            source_type=result["data"].get("source_type")
                        ))
                        success_count += 1
                    else:
                        results.append(BatchUploadResult(
                            index=idx,
                            delivery_id=delivery_id,
                            success=False,
                            message=result.get("error", "上传失败"),
                            image_path=None,
                            upload_status=None,
                            service_fee=None,
                            source_type=None
                        ))
                        failed_count += 1

                except Exception as e:
                    logger.error(f"单条模式处理第{idx}项失败: {e}")
                    results.append(BatchUploadResult(
                        index=idx,
                        delivery_id=delivery_id,
                        success=False,
                        message=f"处理异常: {str(e)}",
                        image_path=None,
                        upload_status=None,
                        service_fee=None,
                        source_type=None
                    ))
                    failed_count += 1

            return BatchDeliveryOrderResponse(
                success=True,
                message=f"批量上传完成（单条模式）：成功 {success_count}/{len(files)} 条",
                total_count=len(files),
                success_count=success_count,
                failed_count=failed_count,
                results=results
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("批量上传联单异常")
        raise HTTPException(status_code=500, detail=f"批量上传失败: {str(e)}")


@router.post("/{delivery_id}/upload-pdf", summary="上传联单PDF")
async def upload_delivery_pdf(
    delivery_id: int,
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user),
    service: DeliveryService = Depends(get_delivery_service)
):
    """上传联单 PDF 文件（仅支持 PDF 格式）"""
    if file.content_type != "application/pdf":
        raise HTTPException(status_code=400, detail="只支持 PDF 文件")
    contents = await file.read()
    result = service.upload_delivery_pdf(delivery_id, contents, uploaded_by=current_user.get("name"))
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["error"])
    return result

@router.put("/{delivery_id}/modify-pdf", summary="修改联单PDF")
async def modify_delivery_pdf(
    delivery_id: int,
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user),
    service: DeliveryService = Depends(get_delivery_service)
):
    """替换联单 PDF 文件（覆盖原有）"""
    if file.content_type != "application/pdf":
        raise HTTPException(status_code=400, detail="只支持 PDF 文件")
    contents = await file.read()
    result = service.update_delivery_pdf(delivery_id, contents, uploaded_by=current_user.get("name"))
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["error"])
    return result

@router.get("/{delivery_id}/view-pdf", summary="预览联单PDF")
async def view_delivery_pdf(
    delivery_id: int,
    service: DeliveryService = Depends(get_delivery_service),
    current_user: dict = Depends(get_current_user)
):
    """预览已上传的联单 PDF 文件"""
    delivery = service.get_delivery(delivery_id)
    if not delivery:
        raise HTTPException(status_code=404, detail="订单不存在")
    pdf_path = delivery.get("delivery_order_pdf")  # 如果您已分离字段
    # 或暂时仍从 delivery_order_image 读取，但要校验扩展名
    if not pdf_path or not os.path.exists(pdf_path):
        raise HTTPException(status_code=404, detail="PDF 文件不存在")
    ext = os.path.splitext(pdf_path)[1].lower()
    if ext != '.pdf':
        raise HTTPException(status_code=400, detail="文件不是 PDF 格式")
    return FileResponse(pdf_path, media_type="application/pdf", filename=f"delivery_{delivery_id}.pdf")

@router.delete("/{delivery_id}/pdf", summary="删除联单PDF")
async def delete_delivery_pdf(
    delivery_id: int,
    service: DeliveryService = Depends(get_delivery_service),
    current_user: dict = Depends(get_current_user)
):
    """删除联单 PDF 文件"""
    result = service.delete_delivery_pdf(delivery_id)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["error"])
    return result