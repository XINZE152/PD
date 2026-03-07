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

from app.services.balance_service import BalanceService, get_balance_service, UPLOAD_DIR
from app.services.contract_service import get_conn

router = APIRouter(prefix="/balances", tags=["磅单结余管理"])


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

@router.post("/generate")
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


@router.get("/", response_model=dict)
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


@router.get("/grouped", response_model=dict)
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


@router.put("/{balance_id}/payment", response_model=dict)
async def update_balance_payment(
        balance_id: int,
        paid_amount: float = Form(..., description="已打款金额"),
        payee_name: Optional[str] = Form(None, description="收款人姓名"),
        payee_account: Optional[str] = Form(None, description="收款人账号"),
        payout_date: Optional[str] = Form(None, description="打款日期，格式：YYYY-MM-DD"),
        service: BalanceService = Depends(get_balance_service)
):
    """
    编辑打款信息
    输入Form格式

    支持修改：
    - 已打款金额（必填）
    - 收款人姓名（可选）
    - 收款人账号（可选）
    - 打款日期（可选）

    打款状态自动判断：
    - 已打款金额 > 0 → 已打款 (payout_status=1)
    - 已打款金额 = 0 → 待打款 (payout_status=0)

    支付状态自动重新计算：
    - 已打款金额 <= 0 → 待支付
    - 已打款金额 >= 应付金额 → 已结清
    - 0 < 已打款金额 < 应付金额 → 部分支付
    """
    try:
        # 先检查结余明细是否存在
        result = service.recalculate_balance(balance_id)
        if not result["success"]:
            raise HTTPException(status_code=404, detail="结余明细不存在")

        # 根据已打款金额自动判断打款状态
        payout_status = 1 if paid_amount > 0 else 0

        # 更新打款信息
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 构建动态更新字段
                update_fields = ["paid_amount = %s", "payout_status = %s"]
                params = [paid_amount, payout_status]

                # 收款人姓名（如果提供）
                if payee_name is not None:
                    update_fields.append("payee_name = %s")
                    params.append(payee_name)

                # 收款人账号（如果提供）
                if payee_account is not None:
                    update_fields.append("payee_account = %s")
                    params.append(payee_account)

                # 打款日期（如果提供）
                if payout_date is not None:
                    update_fields.append("payout_date = %s")
                    params.append(payout_date)

                update_fields.append("updated_at = NOW()")
                params.append(balance_id)

                # 执行更新
                sql = f"""
                    UPDATE pd_balance_details 
                    SET {', '.join(update_fields)}
                    WHERE id = %s
                """
                cur.execute(sql, tuple(params))

                # 重新计算结余金额和支付状态
                cur.execute("""
                    SELECT payable_amount, paid_amount 
                    FROM pd_balance_details 
                    WHERE id = %s
                """, (balance_id,))
                row = cur.fetchone()
                if row:
                    payable, paid = Decimal(str(row[0])), Decimal(str(row[1]))
                    balance = payable - paid

                    # 确定支付状态
                    if paid <= 0:
                        payment_status = 0  # 待支付
                    elif paid >= payable:
                        payment_status = 2  # 已结清
                    else:
                        payment_status = 1  # 部分支付

                    cur.execute("""
                        UPDATE pd_balance_details 
                        SET balance_amount = %s, payment_status = %s 
                        WHERE id = %s
                    """, (balance, payment_status, balance_id))

        return {
            "success": True,
            "message": "打款信息更新成功",
            "data": {
                "id": balance_id,
                "paid_amount": paid_amount,
                "payee_name": payee_name,
                "payee_account": payee_account,
                "payout_date": payout_date,
                "payout_status": payout_status,
                "payout_status_name": "已打款" if payout_status == 1 else "待打款",
                "payment_status": payment_status,
                "payment_status_name": {0: "待支付", 1: "部分支付", 2: "已结清"}.get(payment_status, "未知")
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"更新失败: {str(e)}")


@router.post("/payment-receipts/ocr", response_model=PaymentReceiptOCRResponse)
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


@router.post("/payment-receipts", response_model=dict)
async def create_payment_receipt(
    request: Optional[PaymentReceiptCreateRequest] = Body(None),
    request_json: Optional[str] = Form(None, description="回单数据JSON字符串"),
        receipt_image: UploadFile = File(..., description="回单图片（必填）"),
        is_manual: bool = Form(True),
        service: BalanceService = Depends(get_balance_service)
):
    """
    保存支付回单（OCR后确认或纯手动录入）
    """
    try:
        if request is None:
            if not request_json:
                raise HTTPException(status_code=422, detail="缺少回单数据")
            try:
                request = PaymentReceiptCreateRequest(**json.loads(request_json))
            except json.JSONDecodeError as exc:
                raise HTTPException(status_code=400, detail=f"request_json不是合法JSON: {exc.msg}")
            except ValidationError as exc:
                raise HTTPException(status_code=422, detail=exc.errors())

        data = request.dict()

        # 保存图片
        file_ext = Path(receipt_image.filename).suffix.lower() or ".jpg"
        safe_payee = re.sub(r'[^\w\-]', '_', request.payee_name)
        filename = f"receipt_{safe_payee}_{request.payment_date}_{os.urandom(4).hex()[:8]}{file_ext}"
        file_path = UPLOAD_DIR / filename

        file_path.parent.mkdir(parents=True, exist_ok=True)

        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(receipt_image.file, buffer)

        result = service.create_payment_receipt(data, str(file_path), is_manual)

        if result["success"]:
            return result
        else:
            # 失败时删除图片
            if file_path.exists():
                os.remove(file_path)
            raise HTTPException(status_code=400, detail=result.get("error"))

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/match/pending")
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


@router.post("/verify-payment", response_model=dict)
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


@router.get("/payment-receipts/{receipt_id}/image")
async def get_payment_receipt_image(
        receipt_id: int,
        service: BalanceService = Depends(get_balance_service)
):
    """
    查看支付回单图片
    """
    receipt = service.get_payment_receipt(receipt_id)
    if not receipt:
        raise HTTPException(status_code=404, detail="支付回单不存在")

    image_path = receipt.get('receipt_image')
    if not image_path:
        raise HTTPException(status_code=404, detail="该回单没有图片")

    # 构建完整路径
    full_path = Path(image_path)
    if not full_path.is_absolute():
        # 如果是相对路径，拼接上传目录
        full_path = UPLOAD_DIR / image_path

    if not full_path.exists():
        raise HTTPException(status_code=404, detail="图片文件不存在")

    # 自动识别 MIME 类型
    mime_type, _ = mimetypes.guess_type(str(full_path))
    if not mime_type:
        mime_type = "image/jpeg"

    return FileResponse(
        path=str(full_path),
        media_type=mime_type,
        filename=full_path.name
    )

@router.get("/payment-receipts/{receipt_id}")
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


@router.get("/payment-receipts", response_model=PaymentReceiptListResp)
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


@router.get("/summary/by-payee", response_model=dict)
async def list_balance_by_payee(
        payee_name: Optional[str] = Query(None, description="精确收款人姓名"),
        driver_phone: Optional[str] = Query(None, description="精确司机电话"),
        fuzzy_keywords: Optional[str] = Query(None, description="模糊关键词（姓名/电话/车牌/合同号）"),
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
        min_balance=min_balance,
        payment_status=payment_status,
        page=page,
        page_size=page_size
    )

    if result["success"]:
        return result
    else:
        raise HTTPException(status_code=400, detail=result.get("error"))


@router.get("/summary/by-payee/{payee_name}/details", response_model=dict)
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


@router.post("/summary/by-payee/{payee_name}/batch-verify", response_model=dict)
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
@router.get("/{balance_id}", response_model=BalanceOut)
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