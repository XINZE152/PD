"""
磅单结余管理 + 支付回单路由（优化版）
"""
import json
import mimetypes
import os
import re
import shutil
from decimal import Decimal
from pathlib import Path
from typing import List, Optional, Dict
from fastapi.responses import FileResponse
from fastapi import APIRouter, UploadFile, File, HTTPException, Depends, Query, Body, Form
from pydantic import BaseModel, Field, ValidationError

from app.core.paths import UPLOADS_DIR
from app.services.balance_service import BalanceService, get_balance_service, UPLOAD_DIR
from app.services.contract_service import get_conn

router = APIRouter(prefix="/balances", tags=["磅单结余管理"])


def _resolve_payment_receipt_image_path(image_path: str) -> Optional[Path]:
    raw_path = Path(str(image_path))
    candidates: List[Path] = []

    if raw_path.is_absolute():
        candidates.append(raw_path)
    else:
        candidates.extend([
            raw_path,
            UPLOAD_DIR / raw_path,
            UPLOADS_DIR / raw_path,
            UPLOAD_DIR / raw_path.name,
        ])
        if raw_path.parts and raw_path.parts[0] == "uploads":
            candidates.append(UPLOADS_DIR.parent / raw_path)

    seen = set()
    for candidate in candidates:
        candidate_key = str(candidate)
        if candidate_key in seen:
            continue
        seen.add(candidate_key)
        if candidate.exists() and candidate.is_file():
            return candidate

    return None


# ========== 请求/响应模型 ==========

class PaymentReceiptOCRResponse(BaseModel):
    """OCR识别响应模型"""
    receipt_no: Optional[str] = None
    payment_date: Optional[str] = None
    payment_time: Optional[str] = None
    payer_name: Optional[str] = None
    payer_account: Optional[str] = None
    payee_name: Optional[str] = None
    payee_account: Optional[str] = None
    amount: Optional[float] = None           # 转账金额（小写）
    fee: Optional[float] = 0.0               # 手续费
    total_amount: Optional[float] = None     # 合计（小写）- 新增
    bank_name: Optional[str] = None          # 付款行
    payee_bank_name: Optional[str] = None    # 收款行
    remark: Optional[str] = None
    ocr_message: str = ""
    raw_text: Optional[str] = None
    ocr_time: float = 0
    ocr_success: bool = True


class PaymentReceiptCreateRequest(BaseModel):
    """创建支付回单请求模型"""
    receipt_no: Optional[str] = Field(None, description="回单编号")
    payment_date: str = Field(..., description="支付日期")
    payment_time: Optional[str] = Field(None, description="支付时间")
    payer_name: Optional[str] = Field(None, description="付款人")
    payer_account: Optional[str] = Field(None, description="付款账号")
    payee_name: str = Field(..., description="收款人（司机）")
    payee_account: Optional[str] = Field(None, description="收款账号")
    amount: float = Field(..., description="转账金额（小写）")
    fee: Optional[float] = Field(0.0, description="手续费")
    total_amount: Optional[float] = Field(None, description="合计金额（小写），不传则自动计算")  # 新增
    bank_name: Optional[str] = Field(None, description="付款银行")
    payee_bank_name: Optional[str] = Field(None, description="收款银行")
    remark: Optional[str] = Field(None, description="备注")


class SettlementItem(BaseModel):
    balance_id: int = Field(..., description="结余明细ID")
    amount: float = Field(..., description="本次核销金额")


class BalanceOut(BaseModel):
    id: int
    schedule_date: Optional[str] = None
    contract_no: Optional[str] = None
    report_date: Optional[str] = None
    target_factory_name: Optional[str] = None
    driver_name: Optional[str] = None
    driver_phone: Optional[str] = None
    driver_id_card: Optional[str] = None
    payee_name: Optional[str] = None
    payee_account: Optional[str] = None
    payee_bank_name: Optional[str] = None
    vehicle_no: Optional[str] = None
    product_name: Optional[str] = None
    has_delivery_order: Optional[str] = None
    upload_status: Optional[str] = None
    shipper: Optional[str] = None
    weigh_date: Optional[str] = None
    weigh_ticket_no: Optional[str] = None
    net_weight: Optional[float] = None
    purchase_unit_price: Optional[float] = None
    payable_amount: Optional[float] = None
    paid_amount: Optional[float] = None
    balance_amount: Optional[float] = None
    payment_status: int = 0
    payment_status_name: Optional[str] = None  # 新增
    payout_status: Optional[int] = None
    payout_status_name: Optional[str] = None
    schedule_date: Optional[str] = None
    schedule_status: Optional[int] = None
    schedule_status_name: Optional[str] = None
    created_at: Optional[str] = None
    # 关联的磅单字段
    weighbill_image: Optional[str] = None  # 新增
    # 关联的支付回单摘要（可选）
    receipt_count: Optional[int] = 0  # 新增：关联的回单数量

class PaymentReceiptListOut(BaseModel):
    """支付回单列表响应"""
    id: int
    receipt_no: Optional[str] = None
    receipt_image: Optional[str] = None
    payment_date: Optional[str] = None
    payment_time: Optional[str] = None
    payer_name: Optional[str] = None
    payer_account: Optional[str] = None
    payee_name: Optional[str] = None
    payee_account: Optional[str] = None
    amount: Optional[float] = None           # 转账金额
    fee: Optional[float] = None              # 手续费
    total_amount: Optional[float] = None     # 合计 - 新增
    bank_name: Optional[str] = None
    payee_bank_name: Optional[str] = None
    remark: Optional[str] = None
    ocr_status: int
    ocr_status_name: str
    is_manual_corrected: int
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
class PaymentReceiptDetailOut(PaymentReceiptListOut):
    """支付回单详情（包含核销记录）"""
    ocr_raw_data: Optional[str] = None
    settlements: Optional[List[Dict]] = None


class PaymentReceiptListResp(BaseModel):
    """支付回单列表响应"""
    success: bool
    data: List[PaymentReceiptListOut]
    total: int
    page: int
    page_size: int

class PayeeSummaryOut(BaseModel):
    """收款人汇总响应模型"""
    payee_name: str
    driver_phone: Optional[str] = None
    payment_schedule_date: Optional[str] = None
    bill_count: int
    total_payable: float
    total_paid: float
    total_balance: float
    related_contracts: Optional[str] = None
    related_vehicles: Optional[str] = None
    first_bill_date: Optional[str] = None
    last_bill_date: Optional[str] = None
    pending_count: int
    partial_count: int
    status_summary: str


class ReporterSummaryOut(BaseModel):
    """报单人/发货人汇总响应模型"""
    reporter_name: str
    payment_schedule_date: Optional[str] = None
    bill_count: int
    total_payable: float
    total_paid: float
    total_balance: float
    related_contracts: Optional[str] = None
    related_vehicles: Optional[str] = None
    first_bill_date: Optional[str] = None
    last_bill_date: Optional[str] = None
    pending_count: int
    partial_count: int
    status_summary: str


class PayeeDetailSummary(BaseModel):
    """收款人明细汇总"""
    driver_name: str
    driver_phone: Optional[str] = None
    total_bills: int
    total_payable: float
    total_paid: float
    total_balance: float


class PayeeBalanceDetailOut(BalanceOut):
    """收款人下的结余明细"""
    weighbill_image: Optional[str] = None
    weigh_date: Optional[str] = None
    weigh_vehicle_no: Optional[str] = None
    weigh_product_name: Optional[str] = None
    weigh_net_weight: Optional[float] = None
# ========== 路由 ==========

@router.post("/generate", summary="生成结余明细")
async def generate_balance(
        contract_no: Optional[str] = Query(None, description="指定合同编号"),
        delivery_id: Optional[int] = Query(None, description="指定报货订单"),
        weighbill_id: Optional[int] = Query(None, description="指定磅单ID"),
        service: BalanceService = Depends(get_balance_service)
):
    """
    生成磅单结余明细
    根据已确认的磅单数据，自动生成应付明细
    """
    result = service.generate_balance_details(contract_no, delivery_id, weighbill_id)
    if result["success"]:
        return result
    else:
        raise HTTPException(status_code=400, detail=result.get("error"))


@router.get("/", summary="查询结余明细列表", response_model=dict)
async def list_balances(
        exact_contract_no: Optional[str] = Query(None, description="精确合同编号"),
        exact_driver_name: Optional[str] = Query(None, description="精确司机姓名"),
        fuzzy_keywords: Optional[str] = Query(None, description="模糊关键词（空格分隔）"),
        payment_status: Optional[int] = Query(None, description="0=待支付, 1=部分支付, 2=已结清"),
        page: int = Query(1, ge=1),
        page_size: int = Query(20, ge=1, le=100),
        service: BalanceService = Depends(get_balance_service)
):
    """查询结余明细列表"""
    return service.list_balance_details(
        exact_contract_no,
        exact_driver_name,
        fuzzy_keywords,
        payment_status,
        page,
        page_size,
    )


@router.get("/grouped", summary="查询打款分组列表", response_model=dict)
async def list_balances_grouped(
        exact_contract_no: Optional[str] = Query(None, description="精确合同编号"),
        exact_driver_name: Optional[str] = Query(None, description="精确司机姓名"),
        fuzzy_keywords: Optional[str] = Query(None, description="模糊关键词（收款人/合同/司机/电话/车牌）"),
        payment_status: Optional[int] = Query(None, description="支付状态：0=待支付, 1=部分支付, 2=已结清"),
        payout_status: Optional[int] = Query(None, description="打款状态：0=待打款, 1=已打款"),
        schedule_status: Optional[int] = Query(None, description="排期状态：0=待排期, 1=已排期"),
        date_from: Optional[str] = Query(None, description="排款日期开始"),
        date_to: Optional[str] = Query(None, description="排款日期结束"),
        page: int = Query(1, ge=1),
        page_size: int = Query(20, ge=1, le=100),
        service: BalanceService = Depends(get_balance_service)
):
    """
    打款信息列表（按报单分组）

    表头字段：
    - 排款日期、合同编号、报单日期、报送冶炼厂
    - 司机电话、司机姓名、车号、身份证号
    - 品种、是否自带联单、是否上传联单
    - 报单人/发货人（大区经理、仓库）
    - 磅单日期、过磅单号、净重、采购单价
    - 联单费、应打款金额、已打款金额
    - 收款人、收款人账号
    - 打款状态（已打款、待打款）
    - 回款状态（待回款/已回首笔待回尾款/已回尾款）
    - 操作

    查询条件支持收款人、合同编号、报单日期、司机姓名、车号、磅单日期、支款日期、打款状态
    """
    result = service.list_balance_details_grouped(
        exact_contract_no=exact_contract_no,
        exact_driver_name=exact_driver_name,
        fuzzy_keywords=fuzzy_keywords,
        payment_status=payment_status,
        payout_status=payout_status,
        schedule_status=schedule_status,
        date_from=date_from,
        date_to=date_to,
        page=page,
        page_size=page_size,
    )
    if result["success"]:
        return result
    else:
        raise HTTPException(status_code=400, detail=result.get("error"))


@router.put("/{balance_id}/payment", summary="录入打款信息", response_model=dict)
async def update_balance_payment(
        balance_id: int,
        paid_amount: float = Form(..., description="已打款金额"),
        payout_date: str = Form(..., description="打款日期，格式：YYYY-MM-DD"),
        receipt_image: UploadFile = File(..., description="支付回单图片"),
        service: BalanceService = Depends(get_balance_service)
):
    """
    编辑打款信息。
    仅需上传已打款金额、打款日期和支付回单，收款人相关字段自动从结余明细匹配。
    """
    try:
        if receipt_image.content_type not in ["image/jpeg", "image/jpg", "image/png", "image/bmp"]:
            raise HTTPException(status_code=400, detail="仅支持jpg/png/bmp格式的支付回单")

        balance = service.get_balance_detail(balance_id)
        if not balance:
            raise HTTPException(status_code=404, detail="结余明细不存在")

        payable_amount = Decimal(str(balance.get("payable_amount") or 0))
        previous_paid_amount = Decimal(str(balance.get("paid_amount") or 0))
        requested_paid_amount = Decimal(str(paid_amount))

        if requested_paid_amount < 0:
            raise HTTPException(status_code=400, detail="已打款金额不能小于0")
        if requested_paid_amount < previous_paid_amount:
            raise HTTPException(status_code=400, detail="已打款金额不能小于当前已打款金额")
        if requested_paid_amount > payable_amount:
            raise HTTPException(status_code=400, detail="已打款金额不能大于应打款金额")

        settle_amount = requested_paid_amount - previous_paid_amount
        if settle_amount <= 0:
            raise HTTPException(status_code=400, detail="当前没有新增打款金额可用于上传支付回单")

        payee_name = balance.get("payee_name")
        payee_account = balance.get("payee_account")
        payee_bank_name = balance.get("payee_bank_name")
        if not payee_name:
            raise HTTPException(status_code=400, detail="该结余明细未匹配到收款人，无法自动创建支付回单")

        file_ext = Path(receipt_image.filename or "").suffix.lower() or ".jpg"
        safe_payee = re.sub(r'[^\w\-]', '_', payee_name)
        filename = f"receipt_{safe_payee}_{payout_date}_{os.urandom(4).hex()[:8]}{file_ext}"
        file_path = UPLOAD_DIR / filename
        file_path.parent.mkdir(parents=True, exist_ok=True)

        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(receipt_image.file, buffer)

        receipt_result = service.recognize_payment_receipt(str(file_path))
        receipt_data = receipt_result.get("data", {}) if isinstance(receipt_result, dict) else {}
        payment_receipt_data = {
            "receipt_no": receipt_data.get("receipt_no"),
            "payment_date": payout_date,
            "payment_time": receipt_data.get("payment_time"),
            "payer_name": receipt_data.get("payer_name"),
            "payer_account": receipt_data.get("payer_account"),
            "payee_name": payee_name,
            "payee_account": payee_account,
            "amount": float(settle_amount),
            "fee": receipt_data.get("fee", 0) or 0,
            "total_amount": receipt_data.get("total_amount") or float(settle_amount),
            "bank_name": receipt_data.get("bank_name"),
            "payee_bank_name": payee_bank_name,
            "remark": receipt_data.get("remark"),
            "raw_text": receipt_data.get("raw_text"),
        }

        created_receipt = service.create_payment_receipt(payment_receipt_data, str(file_path), is_manual=True)
        if not created_receipt.get("success"):
            if file_path.exists():
                os.remove(file_path)
            raise HTTPException(status_code=400, detail=created_receipt.get("error") or "支付回单保存失败")

        receipt_id = created_receipt.get("data", {}).get("id")
        verify_result = service.verify_payment(
            receipt_id=receipt_id,
            balance_items=[{"balance_id": balance_id, "amount": float(settle_amount)}]
        )
        if not verify_result.get("success"):
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM pd_payment_receipts WHERE id = %s", (receipt_id,))
            if file_path.exists():
                os.remove(file_path)
            raise HTTPException(status_code=400, detail=verify_result.get("error") or "支付回单核销失败")

        payout_status = 1 if requested_paid_amount > 0 else 0
        payment_status = 0
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE pd_balance_details
                    SET payout_status = %s,
                        payout_date = %s,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (payout_status, payout_date, balance_id)
                )

                cur.execute(
                    """
                    SELECT payable_amount, paid_amount, payment_status
                    FROM pd_balance_details
                    WHERE id = %s
                    """,
                    (balance_id,)
                )
                row = cur.fetchone()
                if row:
                    payment_status = row[2]

        return {
            "success": True,
            "message": "打款信息更新成功",
            "data": {
                "id": balance_id,
                "paid_amount": paid_amount,
                "payee_name": payee_name,
                "payee_account": payee_account,
                "payee_bank_name": payee_bank_name,
                "payout_date": payout_date,
                "payout_status": payout_status,
                "payout_status_name": "已打款" if payout_status == 1 else "待打款",
                "payment_status": payment_status,
                "payment_status_name": {0: "待支付", 1: "部分支付", 2: "已结清"}.get(payment_status, "未知"),
                "receipt_id": receipt_id,
                "receipt_image": str(file_path),
                "settled_amount": float(settle_amount),
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"更新失败: {str(e)}")


@router.post("/payment-receipts/ocr", summary="OCR 识别支付回单", response_model=PaymentReceiptOCRResponse)
async def ocr_payment_receipt(
        file: UploadFile = File(..., description="支付回单图片"),
        service: BalanceService = Depends(get_balance_service)
):
    """
    OCR识别支付回单
    """
    allowed_types = ["image/jpeg", "image/jpg", "image/png", "image/bmp"]
    if file.content_type not in allowed_types:
        raise HTTPException(status_code=400, detail="仅支持jpg/png/bmp格式")

    temp_path = Path("uploads/temp") / f"receipt_{os.urandom(4).hex()}.jpg"
    temp_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(temp_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        processed_path = service.preprocess_image(str(temp_path))
        result = service.recognize_payment_receipt(processed_path)

        if processed_path != str(temp_path) and os.path.exists(processed_path):
            os.remove(processed_path)
        os.remove(temp_path)

        if not result["success"]:
            raise HTTPException(status_code=400, detail=result.get("error"))

        return PaymentReceiptOCRResponse(**result["data"])

    except HTTPException:
        raise
    except Exception as e:
        if temp_path.exists():
            os.remove(temp_path)
        raise HTTPException(status_code=500, detail=f"处理失败: {str(e)}")


@router.post("/payment-receipts", summary="保存支付回单", response_model=dict)
async def create_payment_receipt(
        request: Optional[str] = Form(None, description="回单数据JSON字符串（与request_json二选一）"),
        request_json: Optional[str] = Form(None, description="回单数据JSON字符串（与request二选一）"),
        files: List[UploadFile] = File(..., description="回单图片，最多6张（必填）"),
        is_manual: bool = Form(True),
        service: BalanceService = Depends(get_balance_service)
):
    """
    保存支付回单（OCR后确认或纯手动录入）
    支持上传最多6张回单图片。
    """
    # 提前初始化 saved_paths，确保异常处理中可用
    saved_paths = []
    try:
        # 合并请求数据：优先使用 request_json，若为空则使用 request
        json_str = request_json if request_json else request
        if not json_str:
            raise HTTPException(status_code=422, detail="缺少回单数据，请提供 request 或 request_json")

        try:
            data_dict = json.loads(json_str)
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail=f"JSON解析失败: {exc.msg}")

        # 验证数据模型
        try:
            create_request = PaymentReceiptCreateRequest(**data_dict)
        except ValidationError as exc:
            raise HTTPException(status_code=422, detail=exc.errors())

        # 检查文件数量
        if len(files) == 0:
            raise HTTPException(status_code=400, detail="至少上传一张回单图片")
        if len(files) > 6:
            raise HTTPException(status_code=400, detail="最多上传6张回单图片")

        # 验证文件类型
        allowed_types = ["image/jpeg", "image/jpg", "image/png", "image/bmp"]
        for file in files:
            if file.content_type not in allowed_types:
                raise HTTPException(status_code=400, detail=f"文件 {file.filename} 格式不支持，仅支持jpg/png/bmp")

        data = create_request.dict()

        # 保存所有图片
        for idx, file in enumerate(files):
            file_ext = Path(file.filename).suffix.lower() or ".jpg"
            safe_payee = re.sub(r'[^\w\-]', '_', create_request.payee_name)
            filename = f"receipt_{safe_payee}_{create_request.payment_date}_{idx}_{os.urandom(4).hex()[:8]}{file_ext}"
            file_path = UPLOAD_DIR / filename
            file_path.parent.mkdir(parents=True, exist_ok=True)

            with open(file_path, "wb") as buffer:
                shutil.copyfileobj(file.file, buffer)
            saved_paths.append(str(file_path))

        # 调用服务创建记录
        result = service.create_payment_receipt(data, saved_paths, is_manual)

        if result["success"]:
            return result
        else:
            # 失败时删除已保存的图片
            for path in saved_paths:
                if os.path.exists(path):
                    os.remove(path)
            raise HTTPException(status_code=400, detail=result.get("error"))

    except HTTPException:
        raise
    except Exception as e:
        # 异常时删除已保存的图片
        for path in saved_paths:
            if os.path.exists(path):
                os.remove(path)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/match/pending", summary="匹配待支付结余")
async def match_pending(
        payee_name: str = Query(..., description="收款人姓名（司机）"),
        amount: float = Query(..., description="支付金额"),
        date_range: int = Query(7, description="查询天数范围"),
        service: BalanceService = Depends(get_balance_service)
):
    """
    根据收款人+金额匹配待支付结余
    用于支付回单与结余明细的匹配
    """
    matches = service.match_pending_payments(payee_name, amount, date_range)
    return {
        "success": True,
        "matched_count": len(matches),
        "data": matches
    }


@router.post("/verify-payment", summary="核销支付回单", response_model=dict)
async def verify_payment(
        receipt_id: int = Form(..., description="支付回单ID"),
        items: List[SettlementItem] = Body(..., description="核销明细列表"),
        service: BalanceService = Depends(get_balance_service)
):
    """
    核销支付（支持分批核销）

    示例:
    {
        "receipt_id": 1,
        "items": [
            {"balance_id": 1, "amount": 5000},
            {"balance_id": 2, "amount": 3000}
        ]
    }
    """
    balance_items = [{"balance_id": item.balance_id, "amount": item.amount} for item in items]

    result = service.verify_payment(receipt_id, balance_items)
    if result["success"]:
        return result
    else:
        raise HTTPException(status_code=400, detail=result.get("error"))


@router.get(
    "/payment-receipts/{receipt_id}/image",
    summary="查看支付回单图片",
    responses={
        200: {
            "content": {
                "image/jpeg": {},
                "image/png": {},
                "image/bmp": {},
                "image/webp": {},
            },
            "description": "支付回单图片",
        }
    },
)
async def get_payment_receipt_image(
        receipt_id: int,
        index: int = Query(0, ge=0, description="图片索引，从0开始"),
        service: BalanceService = Depends(get_balance_service)
):
    """
    查看支付回单图片
    - 支持多张图片，通过 index 参数选择具体哪一张（默认第一张）
    """
    receipt = service.get_payment_receipt(receipt_id)
    if not receipt:
        raise HTTPException(status_code=404, detail="支付回单不存在")

    # 优先使用多图片列表
    image_paths = receipt.get('receipt_images', [])
    if not image_paths:
        # 兼容旧数据（只有单张图片）
        single_path = receipt.get('receipt_image')
        if single_path:
            image_paths = [single_path]

    if not image_paths:
        raise HTTPException(status_code=404, detail="该回单没有图片")

    if index >= len(image_paths):
        raise HTTPException(
            status_code=404,
            detail=f"图片索引 {index} 超出范围，共有 {len(image_paths)} 张"
        )

    image_path = image_paths[index]
    full_path = _resolve_payment_receipt_image_path(image_path)
    if not full_path or not full_path.exists():
        raise HTTPException(status_code=404, detail="图片文件不存在")

    # 自动识别 MIME 类型
    mime_type, _ = mimetypes.guess_type(str(full_path))
    if not mime_type:
        mime_type = "image/jpeg"

    # 对中文文件名进行 RFC 5987/RFC 6266 编码
    from urllib.parse import quote
    filename = full_path.name
    try:
        filename.encode('ascii')
        disposition = f'inline; filename="{filename}"'
    except UnicodeEncodeError:
        encoded_filename = quote(filename, safe='')
        disposition = f"inline; filename*=UTF-8''{encoded_filename}"

    return FileResponse(
        path=str(full_path),
        media_type=mime_type,
        headers={"Content-Disposition": disposition}
    )

@router.get("/payment-receipts/{receipt_id}", summary="查看支付回单详情")
async def get_payment_receipt(
        receipt_id: int,
        service: BalanceService = Depends(get_balance_service)
):
    """查看支付回单详情（包含核销记录）"""
    receipt = service.get_payment_receipt(receipt_id)
    if not receipt:
        raise HTTPException(status_code=404, detail="支付回单不存在")

    # 转换状态
    status_map = {0: "待确认", 1: "已确认", 2: "已核销"}
    receipt['ocr_status_label'] = status_map.get(receipt.get('ocr_status'), "未知")

    return receipt


@router.get("/payment-receipts", summary="查询支付回单列表", response_model=PaymentReceiptListResp)
async def list_payment_receipts(
        exact_payee_name: Optional[str] = Query(None, description="精确收款人姓名"),
        exact_ocr_status: Optional[int] = Query(None, ge=0, le=2, description="状态：0待确认/1已确认/2已核销"),
        date_from: Optional[str] = Query(None, description="开始日期"),
        date_to: Optional[str] = Query(None, description="结束日期"),
        fuzzy_keywords: Optional[str] = Query(None, description="模糊关键词（空格分隔）"),
        page: int = Query(1, ge=1),
        page_size: int = Query(20, ge=1, le=100),
        service: BalanceService = Depends(get_balance_service)
):
    """
    查询支付回单列表

    支持筛选：
    - 精确收款人姓名
    - 状态筛选（0待确认/1已确认/2已核销）
    - 日期范围
    - 模糊搜索（回单号/收款人/付款人/银行/备注）
    """
    result = service.list_payment_receipts(
        exact_payee_name=exact_payee_name,
        exact_ocr_status=exact_ocr_status,
        date_from=date_from,
        date_to=date_to,
        fuzzy_keywords=fuzzy_keywords,
        page=page,
        page_size=page_size
    )

    if result["success"]:
        return result
    else:
        raise HTTPException(status_code=500, detail=result.get("error"))


@router.get("/summary/by-payee", summary="按收款人汇总结余", response_model=dict)
async def list_balance_by_payee(
    payee_name: Optional[str] = Query(None, description="精确收款人姓名"),
    driver_phone: Optional[str] = Query(None, description="精确司机电话"),
    fuzzy_keywords: Optional[str] = Query(None, description="模糊关键词（姓名/电话/车牌/合同号）"),
    payment_schedule_date: Optional[str] = Query(None, description="排款日期（YYYY-MM-DD）"),
    min_balance: Optional[float] = Query(0.01, description="最小结余金额，默认0.01"),
    payment_status: Optional[int] = Query(None, description="0=待支付, 1=部分支付, 不传则显示有结余的"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    service: BalanceService = Depends(get_balance_service)
):
    """
    按收款人汇总统计结余

    用于：快速查看每个司机还有多少钱没付，涉及多少车货

    示例返回：
    {
        "data": [
            {
                "payee_name": "张三",
                "driver_phone": "13800138000",
                "bill_count": 5,              // 5车货
                "total_payable": 50000.00,     // 应付5万
                "total_paid": 20000.00,        // 已付2万
                "total_balance": 30000.00,     // 还剩3万没付
                "related_contracts": "HT-001, HT-002",
                "related_vehicles": "京A12345, 京B67890",
                "status_summary": "3笔待支付,2笔部分支付"
            }
        ],
        "summary": {
            "total_payees": 10,    // 共10个收款人有结余
            "total_balance": 150000.00  // 总待付金额15万
        }
    }
    """
    result = service.list_balance_summary_by_payee(
        payee_name=payee_name,
        driver_phone=driver_phone,
        fuzzy_keywords=fuzzy_keywords,
        payment_schedule_date=payment_schedule_date,
        min_balance=min_balance,
        payment_status=payment_status,
        page=page,
        page_size=page_size
    )

    if result["success"]:
        return result
    else:
        raise HTTPException(status_code=400, detail=result.get("error"))


@router.get("/summary/by-shipper", summary="按报单人汇总结余", response_model=dict)
async def list_balance_by_reporter(
    reporter_name: Optional[str] = Query(None, description="精确报单人/发货人"),
    fuzzy_keywords: Optional[str] = Query(None, description="模糊关键词（姓名/电话/车牌/合同号）"),
    payment_schedule_date: Optional[str] = Query(None, description="排款日期（YYYY-MM-DD）"),
    min_balance: Optional[float] = Query(0.01, description="最小结余金额，默认0.01"),
    payment_status: Optional[int] = Query(None, description="0=待支付, 1=部分支付, 不传则显示有结余的"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    service: BalanceService = Depends(get_balance_service)
):
    """
    按报单人/发货人汇总统计结余

    返回每个报单人的：
    - 磅单数
    - 总应付、总已付、总结余
    - 关联合同、车牌
    """
    result = service.list_balance_summary_by_reporter(
        reporter_name=reporter_name,
        fuzzy_keywords=fuzzy_keywords,
        payment_schedule_date=payment_schedule_date,
        min_balance=min_balance,
        payment_status=payment_status,
        page=page,
        page_size=page_size
    )

    if result["success"]:
        return result
    else:
        raise HTTPException(status_code=400, detail=result.get("error"))


@router.get("/summary/by-payee/{payee_name}/details", summary="查看收款人结余明细", response_model=dict)
async def get_payee_balance_details(
        payee_name: str,
        driver_phone: Optional[str] = Query(None, description="司机电话（精确匹配）"),
        payment_status: Optional[int] = Query(None, description="0=待支付, 1=部分支付"),
        page: int = Query(1, ge=1),
        page_size: int = Query(20, ge=1, le=100),
        service: BalanceService = Depends(get_balance_service)
):
    """
    查看指定收款人的具体结余明细列表

    点击汇总行的"查看明细"后调用，显示该司机的所有具体账单
    """
    result = service.get_payee_balance_details(
        payee_name=payee_name,
        driver_phone=driver_phone,
        payment_status=payment_status,
        page=page,
        page_size=page_size
    )

    if result["success"]:
        return result
    else:
        raise HTTPException(status_code=404, detail=result.get("error"))


@router.post("/summary/by-payee/{payee_name}/batch-verify", summary="按收款人批量核销", response_model=dict)
async def batch_verify_by_payee(
        payee_name: str,
        receipt_id: int = Form(..., description="支付回单ID"),
        driver_phone: Optional[str] = Form(None, description="司机电话"),
        service: BalanceService = Depends(get_balance_service)
):
    """
    按收款人批量核销支付

    将一个支付回单的金额，自动分配到该收款人的多笔结余明细上
    分配顺序：按创建时间从早到晚

    适用场景：司机一次打款覆盖多车货的结余
    """
    result = service.batch_verify_by_payee(
        payee_name=payee_name,
        receipt_id=receipt_id,
        driver_phone=driver_phone
    )

    if result["success"]:
        return result
    else:
        raise HTTPException(status_code=400, detail=result.get("error"))


@router.get("/summary/by-shipper/{reporter_name}/details", summary="查看报单人结余明细", response_model=dict)
async def get_reporter_balance_details(
        reporter_name: str,
        payment_status: Optional[int] = Query(None, description="0=待支付, 1=部分支付"),
        page: int = Query(1, ge=1),
        page_size: int = Query(20, ge=1, le=100),
        service: BalanceService = Depends(get_balance_service)
):
    """
    查看指定报单人的具体结余明细列表

    点击汇总行的"查看明细"后调用，显示该报单人的所有具体账单
    """
    result = service.get_reporter_balance_details(
        reporter_name=reporter_name,
        payment_status=payment_status,
        page=page,
        page_size=page_size
    )

    if result["success"]:
        return result
    else:
        raise HTTPException(status_code=404, detail=result.get("error"))
@router.get("/{balance_id}", summary="查看结余明细详情", response_model=BalanceOut)
async def get_balance(
        balance_id: int,
        service: BalanceService = Depends(get_balance_service)
):
    """查看结余明细详情（包含支付记录）"""
    balance = service.get_balance_detail(balance_id)
    if not balance:
        raise HTTPException(status_code=404, detail="结余明细不存在")

    # 转换状态为可读字符串
    status_map = {0: "待支付", 1: "部分支付", 2: "已结清"}
    balance['payment_status_label'] = status_map.get(balance.get('payment_status'), "未知")

    return balance