"""
合同管理路由 - 完整版
支持OCR识别、手动录入、查看、编辑、导出
"""
import csv
import os
import re
import shutil
import json
from io import StringIO
from decimal import Decimal
from typing import List, Optional

from fastapi import APIRouter, UploadFile, File, HTTPException, Depends, Query, Body, Form
from fastapi.responses import StreamingResponse, FileResponse
from pydantic import BaseModel
from datetime import date

from app.core.paths import UPLOADS_DIR
from app.services.contract_service import ContractService, get_contract_service

router = APIRouter(prefix="/contracts", tags=["合同管理"])

UPLOAD_DIR = UPLOADS_DIR / "contracts"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


# ============ 请求/响应模型 ============

class ProductItem(BaseModel):
    product_name: str
    unit_price: Optional[float] = None

class ContractProductOut(BaseModel):
    id: int
    product_name: str
    unit_price: Optional[float] = None
    sort_order: int

class ContractOCRResponse(BaseModel):
    contract_no: Optional[str] = None
    contract_date: Optional[str] = None
    end_date: Optional[str] = None
    smelter_company: Optional[str] = None
    total_quantity: Optional[float] = None
    truck_count: Optional[float] = None
    arrival_payment_ratio: float = 0.9
    final_payment_ratio: float = 0.1
    products: List[ProductItem] = []
    contract_unit_price: Optional[float] = None
    remittance_unit_price: Optional[float] = None
    unit_price: Optional[float] = None
    ocr_success: bool = True
    ocr_message: str = ""
    saved_to_db: bool = False
    contract_id: Optional[int] = None
    db_message: Optional[str] = None
    image_saved: bool = False
    image_path: Optional[str] = None
    image_filename: Optional[str] = None
    raw_text: Optional[str] = None

class ContractCreateRequest(BaseModel):
    contract_no: str
    contract_date: Optional[str] = None
    end_date: Optional[str] = None
    smelter_company: Optional[str] = None
    total_quantity: Optional[float] = None
    arrival_payment_ratio: float = 0.9
    final_payment_ratio: float = 0.1
    products: List[ProductItem] = []
    status: str = "生效中"
    remarks: Optional[str] = None

class ContractUpdateRequest(BaseModel):
    contract_no: Optional[str] = None
    contract_date: Optional[str] = None
    end_date: Optional[str] = None
    smelter_company: Optional[str] = None
    total_quantity: Optional[float] = None
    arrival_payment_ratio: Optional[float] = None
    final_payment_ratio: Optional[float] = None
    products: Optional[List[ProductItem]] = None
    status: Optional[str] = None
    remarks: Optional[str] = None

class ContractOut(BaseModel):
    id: int
    seq_no: Optional[int] = None
    contract_no: str
    contract_date: Optional[date] = None
    end_date: Optional[date] = None
    smelter_company: Optional[str] = None
    total_quantity: Optional[float] = None
    truck_count: Optional[float] = None
    arrival_payment_ratio: float
    final_payment_ratio: float
    status: str
    products: List[ContractProductOut] = []
    contract_image_path: Optional[str] = None  # 新增：图片路径
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


# ============ 路由 ============

@router.post("/ocr", response_model=ContractOCRResponse)
async def ocr_recognize(
    file: UploadFile = File(..., description="合同图片"),
    auto_save: bool = Query(True, description="是否自动保存（默认true，OCR可能不完整）"),
    save_image: bool = Query(False, description="是否保存图片"),
    service: ContractService = Depends(get_contract_service)
):
    """OCR识别合同 - 支持不完整识别，用户后续补充"""
    allowed_types = ["image/jpeg", "image/jpg", "image/png", "image/bmp"]
    if file.content_type not in allowed_types:
        raise HTTPException(status_code=400, detail="仅支持jpg/png/bmp格式")

    temp_path = UPLOAD_DIR / f"temp_{os.urandom(4).hex()}.jpg"

    try:
        with open(temp_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        processed_path = service.preprocess_image(str(temp_path))
        result = service.recognize_contract(processed_path)

        if processed_path != str(temp_path) and os.path.exists(processed_path):
            os.remove(processed_path)

        data = result["data"]
        contract_no = data.get("contract_no")

        # 图片处理
        image_saved = False
        image_path = None
        image_filename = None

        if save_image and contract_no:
            safe_name = re.sub(r'[^\w\-]', '_', contract_no)
            image_filename = f"{safe_name}.jpg"
            final_path = UPLOAD_DIR / image_filename

            if final_path.exists():
                os.remove(final_path)

            os.rename(temp_path, final_path)
            image_saved = True
            image_path = str(final_path)
        else:
            os.remove(temp_path)

        # 自动保存逻辑
        if auto_save and contract_no:
            existing = service.get_contract_detail_by_no(contract_no)
            if existing:
                data["saved_to_db"] = False
                data["db_message"] = f"合同 {contract_no} 已存在"
                data["contract_id"] = existing["id"]
            else:
                save_data = {
                    "contract_no": contract_no,
                    "contract_date": data.get("contract_date"),
                    "end_date": data.get("end_date"),
                    "smelter_company": data.get("smelter_company"),
                    "total_quantity": Decimal(str(data["total_quantity"])) if data.get("total_quantity") else None,
                    "arrival_payment_ratio": Decimal(str(data["arrival_payment_ratio"])),
                    "final_payment_ratio": Decimal(str(data["final_payment_ratio"])),
                    "contract_image_path": image_path,
                }

                products_data = []
                for p in data.get("products", []):
                    products_data.append({
                        "product_name": p["product_name"],
                        "unit_price": Decimal(str(p["unit_price"])) if p.get("unit_price") else None,
                    })

                result_db = service.create_contract(save_data, products_data)

                if result_db["success"]:
                    data["saved_to_db"] = True
                    data["contract_id"] = result_db["data"]["id"]
                    if data.get("products"):
                        data["db_message"] = "合同已自动保存"
                    else:
                        data["db_message"] = "合同已保存，但品种信息为空"
                else:
                    data["saved_to_db"] = False
                    data["db_message"] = f"保存失败: {result_db.get('error')}"
                    if result_db.get("existing_id"):
                        data["contract_id"] = result_db["existing_id"]
        else:
            data["saved_to_db"] = False
            if not contract_no:
                data["db_message"] = "未识别到合同编号，请手动填写后保存"
            else:
                data["db_message"] = "OCR结果不完整，请检查并补充后手动保存"

        data["image_saved"] = image_saved
        data["image_path"] = image_path
        data["image_filename"] = image_filename

        return ContractOCRResponse(**data)

    except HTTPException:
        if temp_path.exists():
            os.remove(temp_path)
        raise
    except Exception as e:
        if temp_path.exists():
            os.remove(temp_path)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/manual", response_model=ContractOut)
async def create_manual(
    contract_data: str = Form(..., description="合同数据JSON字符串"),
    file: Optional[UploadFile] = File(None, description="合同图片（可选）"),
    service: ContractService = Depends(get_contract_service)
):
    """
    手动录入合同（支持图片上传）

    contract_data格式示例：
    {
        "contract_no": "HT-2024-001",
        "contract_date": "2024-01-15",
        "end_date": "2024-01-20",
        "smelter_company": "河南金利金铅集团有限公司",
        "total_quantity": 100.5,
        "arrival_payment_ratio": 0.9,
        "final_payment_ratio": 0.1,
        "products": [{"product_name": "电动车", "unit_price": 8500.00}],
        "status": "生效中",
        "remarks": "备注信息"
    }
    """
    try:
        # 解析JSON数据
        request_data = json.loads(contract_data)
        request = ContractCreateRequest(**request_data)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"JSON解析失败: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"参数格式错误: {str(e)}")

    # 检查合同编号是否已存在
    existing = service.get_contract_detail_by_no(request.contract_no)
    if existing:
        raise HTTPException(status_code=400, detail=f"合同编号 {request.contract_no} 已存在")

    # 处理图片上传
    image_path = None
    if file:
        allowed_types = ["image/jpeg", "image/jpg", "image/png", "image/bmp"]
        if file.content_type not in allowed_types:
            raise HTTPException(status_code=400, detail="仅支持jpg/png/bmp格式")

        # 生成安全文件名
        safe_name = re.sub(r'[^\w\-]', '_', request.contract_no)
        image_filename = f"{safe_name}.jpg"
        image_path = UPLOAD_DIR / image_filename

        # 如果文件已存在，先删除
        if image_path.exists():
            os.remove(image_path)

        # 保存图片
        with open(image_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        image_path = str(image_path)

    try:
        data = {
            "contract_no": request.contract_no,
            "contract_date": request.contract_date,
            "end_date": request.end_date,
            "smelter_company": request.smelter_company,
            "total_quantity": Decimal(str(request.total_quantity)) if request.total_quantity else None,
            "arrival_payment_ratio": Decimal(str(request.arrival_payment_ratio)),
            "final_payment_ratio": Decimal(str(request.final_payment_ratio)),
            "status": request.status,
            "remarks": request.remarks,
            "contract_image_path": image_path,
        }

        products = []
        for p in request.products:
            products.append({
                "product_name": p.product_name,
                "unit_price": Decimal(str(p.unit_price)) if p.unit_price else None,
            })

        result = service.create_contract(data, products)

        if result["success"]:
            detail = service.get_contract_detail(result["data"]["id"])
            return detail
        else:
            # 如果创建失败，删除已上传的图片
            if image_path and os.path.exists(image_path):
                os.remove(image_path)
            raise HTTPException(status_code=400, detail=result.get("error"))

    except HTTPException:
        # 如果创建失败，删除已上传的图片
        if image_path and os.path.exists(image_path):
            os.remove(image_path)
        raise
    except Exception as e:
        # 如果创建失败，删除已上传的图片
        if image_path and os.path.exists(image_path):
            os.remove(image_path)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/", response_model=dict)
async def list_contracts(
    exact_contract_no: Optional[str] = Query(None, description="精确合同编号"),
    exact_smelter_company: Optional[str] = Query(None, description="精确冶炼厂"),
    exact_status: Optional[str] = Query(None, description="精确状态"),
    date_from: Optional[str] = Query(None, description="签订开始日期"),
    date_to: Optional[str] = Query(None, description="签订结束日期"),
    fuzzy_keywords: Optional[str] = Query(None, description="模糊关键词（空格分隔）"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    service: ContractService = Depends(get_contract_service)
):
    """获取合同列表（分页）"""
    return service.list_contracts(
        page,
        page_size,
        exact_contract_no,
        exact_smelter_company,
        exact_status,
        date_from,
        date_to,
        fuzzy_keywords,
    )


@router.get("/id/{contract_id:int}", response_model=ContractOut)
async def get_contract(
    contract_id: int,
    service: ContractService = Depends(get_contract_service)
):
    """查看合同详情"""
    detail = service.get_contract_detail(contract_id)
    if not detail:
        raise HTTPException(status_code=404, detail="合同不存在")
    return detail


@router.put("/id/{contract_id:int}", response_model=dict)
async def update_contract(
    contract_id: int,
    contract_data: Optional[str] = Form(None, description="合同数据JSON字符串"),
    file: Optional[UploadFile] = File(None, description="新的合同图片（可选）"),
    service: ContractService = Depends(get_contract_service)
):
    """
    编辑合同（支持更新图片）

    contract_data格式示例（可选，如果不传则不更新字段）：
    {
        "contract_no": "HT-2024-001",
        "contract_date": "2024-01-15",
        "end_date": "2024-01-20",
        "smelter_company": "河南金利金铅集团有限公司",
        "total_quantity": 100.5,
        "arrival_payment_ratio": 0.9,
        "final_payment_ratio": 0.1,
        "products": [{"product_name": "电动车", "unit_price": 8500.00}],
        "status": "生效中",
        "remarks": "备注信息"
    }
    """
    # 解析JSON数据（如果提供）
    request = None
    if contract_data:
        try:
            request_data = json.loads(contract_data)
            request = ContractUpdateRequest(**request_data)
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=400, detail=f"JSON解析失败: {str(e)}")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"参数格式错误: {str(e)}")

    # 获取原合同信息
    old_contract = service.get_contract_detail(contract_id)
    if not old_contract:
        raise HTTPException(status_code=404, detail="合同不存在")

    # 处理图片上传
    new_image_path = None
    old_image_path = old_contract.get("contract_image_path")

    if file:
        allowed_types = ["image/jpeg", "image/jpg", "image/png", "image/bmp"]
        if file.content_type not in allowed_types:
            raise HTTPException(status_code=400, detail="仅支持jpg/png/bmp格式")

        # 确定合同编号（可能变更）
        contract_no = request.contract_no if request and request.contract_no else old_contract["contract_no"]

        # 生成安全文件名
        safe_name = re.sub(r'[^\w\-]', '_', contract_no)
        image_filename = f"{safe_name}.jpg"
        new_image_path = UPLOAD_DIR / image_filename

        # 如果新路径与旧路径不同且旧文件存在，先删除旧文件
        if old_image_path and str(new_image_path) != old_image_path and os.path.exists(old_image_path):
            os.remove(old_image_path)

        # 如果新文件已存在（可能是其他合同的文件），先删除
        if new_image_path.exists():
            os.remove(new_image_path)

        # 保存新图片
        with open(new_image_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        new_image_path = str(new_image_path)

    try:
        # 构建更新数据
        data = {}
        if request:
            if request.contract_no is not None:
                data["contract_no"] = request.contract_no
            if request.contract_date is not None:
                data["contract_date"] = request.contract_date
            if request.end_date is not None:
                data["end_date"] = request.end_date
            if request.smelter_company is not None:
                data["smelter_company"] = request.smelter_company
            if request.total_quantity is not None:
                data["total_quantity"] = Decimal(str(request.total_quantity))
            if request.arrival_payment_ratio is not None:
                data["arrival_payment_ratio"] = Decimal(str(request.arrival_payment_ratio))
            if request.final_payment_ratio is not None:
                data["final_payment_ratio"] = Decimal(str(request.final_payment_ratio))
            if request.status is not None:
                data["status"] = request.status
            if request.remarks is not None:
                data["remarks"] = request.remarks

        # 如果有新图片，添加到更新数据
        if new_image_path:
            data["contract_image_path"] = new_image_path

        # 处理品种明细
        products = None
        if request and request.products is not None:
            products = []
            for p in request.products:
                products.append({
                    "product_name": p.product_name,
                    "unit_price": Decimal(str(p.unit_price)) if p.unit_price else None,
                })

        result = service.update_contract(contract_id, data, products)

        if result["success"]:
            return {"success": True, "message": "更新成功", "data": result.get("data")}
        else:
            # 如果更新失败且上传了新图片，删除新图片
            if new_image_path and os.path.exists(new_image_path):
                os.remove(new_image_path)
            raise HTTPException(status_code=400, detail=result.get("error"))

    except HTTPException:
        # 如果更新失败且上传了新图片，删除新图片
        if new_image_path and os.path.exists(new_image_path):
            os.remove(new_image_path)
        raise
    except Exception as e:
        # 如果更新失败且上传了新图片，删除新图片
        if new_image_path and os.path.exists(new_image_path):
            os.remove(new_image_path)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/id/{contract_id:int}/image")
async def get_contract_image(
        contract_id: int,
        service: ContractService = Depends(get_contract_service)
):
    """
    查看合同图片
    直接返回图片文件
    """
    try:
        contract = service.get_contract_detail(contract_id)
        if not contract:
            raise HTTPException(status_code=404, detail="合同不存在")

        image_path = contract.get("contract_image_path")
        if not image_path:
            raise HTTPException(status_code=404, detail="该合同没有上传图片")

        if not os.path.exists(image_path):
            raise HTTPException(status_code=404, detail="图片文件不存在")

        return FileResponse(
            path=image_path,
            media_type="image/jpeg",
            filename=f"contract_{contract.get('contract_no')}.jpg"
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取图片失败: {str(e)}")


@router.delete("/id/{contract_id:int}")
async def delete_contract(
    contract_id: int,
    service: ContractService = Depends(get_contract_service)
):
    """删除合同"""
    # 获取合同信息（用于删除图片）
    contract = service.get_contract_detail(contract_id)
    if contract:
        image_path = contract.get("contract_image_path")
        if image_path and os.path.exists(image_path):
            os.remove(image_path)

    result = service.delete_contract(contract_id)
    if result["success"]:
        return {"success": True, "message": "删除成功"}
    else:
        raise HTTPException(status_code=400, detail=result.get("error"))


@router.post("/export")
async def export_contracts(
    contract_ids: List[int] = Body(None, description="要导出的合同ID列表，空则导出全部"),
    service: ContractService = Depends(get_contract_service)
):
    """导出合同"""
    data = service.export_contracts(contract_ids)
    columns: List[str] = []
    for row in data:
        for key in row.keys():
            if key not in columns:
                columns.append(key)

    buffer = StringIO()
    writer = csv.writer(buffer)
    if columns:
        writer.writerow(columns)
        for row in data:
            writer.writerow([row.get(col) for col in columns])

    filename = "contracts_export.csv"
    if contract_ids and len(contract_ids) == 1 and data:
        contract_no = str(data[0].get("contract_no") or "").strip()
        if contract_no:
            safe_name = re.sub(r"[^A-Za-z0-9_-]", "_", contract_no)
            filename = f"{safe_name}.csv"

    csv_bytes = buffer.getvalue().encode("utf-8-sig")
    headers = {"Content-Disposition": f"attachment; filename=\"{filename}\""}
    return StreamingResponse(
        iter([csv_bytes]),
        media_type="text/csv; charset=utf-8",
        headers=headers,
    )