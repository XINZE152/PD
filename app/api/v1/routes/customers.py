"""
客户管理路由 - 冶炼厂客户档案
支持手动录入、编辑、查询
"""
from typing import Optional

from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field

from app.services.customer_service import CustomerService, get_customer_service

router = APIRouter(prefix="/customers", tags=["客户管理"])


# ============ 请求/响应模型 ============

class CustomerCreateRequest(BaseModel):
    smelter_name: str = Field(..., description="冶炼厂名称", max_length=128)
    address: Optional[str] = Field(None, description="冶炼厂地址", max_length=255)
    contact_person: Optional[str] = Field(None, description="联系人", max_length=64)
    contact_phone: Optional[str] = Field(None, description="联系电话", max_length=32)
    contact_address: Optional[str] = Field(None, description="联系人地址", max_length=255)
    credit_code: Optional[str] = Field(None, description="统一社会信用代码", max_length=32)  # 新增


class CustomerUpdateRequest(BaseModel):
    smelter_name: Optional[str] = Field(None, description="冶炼厂名称", max_length=128)
    address: Optional[str] = Field(None, description="冶炼厂地址", max_length=255)
    contact_person: Optional[str] = Field(None, description="联系人", max_length=64)
    contact_phone: Optional[str] = Field(None, description="联系电话", max_length=32)
    contact_address: Optional[str] = Field(None, description="联系人地址", max_length=255)
    credit_code: Optional[str] = Field(None, description="统一社会信用代码", max_length=32)  # 新增


class WarehousePayeeCreateRequest(BaseModel):
    """收款员新增请求"""
    
    warehouse_id: Optional[int] = Field(None, description="库房ID，可选")
    warehouse_name: Optional[str] = Field(None, description="库房名称，可选", max_length=100)
    payee_name: str = Field(..., min_length=1, max_length=100, description="收款人姓名")
    payee_account: Optional[str] = Field(None, max_length=100, description="收款账号")
    payee_bank_name: Optional[str] = Field(None, max_length=100, description="开户行")
    is_active: int = Field(1, ge=0, le=1, description="是否启用: 1启用, 0禁用")


class WarehousePayeeUpdateRequest(BaseModel):
    """收款员编辑请求"""

    warehouse_id: Optional[int] = Field(None, description="库房ID，可选")
    warehouse_name: Optional[str] = Field(None, description="库房名称，可选", max_length=100)
    payee_name: Optional[str] = Field(None, min_length=1, max_length=100, description="收款人姓名")
    payee_account: Optional[str] = Field(None, max_length=100, description="收款账号")
    payee_bank_name: Optional[str] = Field(None, max_length=100, description="开户行")
    is_active: Optional[int] = Field(None, ge=0, le=1, description="是否启用: 1启用, 0禁用")


class CustomerOut(BaseModel):
    id: int
    smelter_name: str
    address: Optional[str] = None
    contact_person: Optional[str] = None
    contact_phone: Optional[str] = None
    contact_address: Optional[str] = None
    credit_code: Optional[str] = None  # 新增
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


# ============ 路由 ============

@router.post("/", summary="新增客户", response_model=dict)
async def create_customer(
        request: CustomerCreateRequest,
        service: CustomerService = Depends(get_customer_service)
):
    """手动录入客户"""
    try:
        data = {
            "smelter_name": request.smelter_name,
            "address": request.address,
            "contact_person": request.contact_person,
            "contact_phone": request.contact_phone,
            "contact_address": request.contact_address,
            "credit_code": request.credit_code,
        }

        result = service.create_customer(data)

        if result["success"]:
            return result
        else:
            raise HTTPException(status_code=400, detail=result.get("error"))

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/", summary="查询客户列表", response_model=dict)
async def list_customers(
        exact_smelter_name: Optional[str] = Query(None, description="精确冶炼厂名称"),
        exact_contact_person: Optional[str] = Query(None, description="精确联系人"),
        exact_contact_phone: Optional[str] = Query(None, description="精确联系电话"),
        exact_credit_code: Optional[str] = Query(None, description="精确统一社会信用代码"),  # 新增
        fuzzy_keywords: Optional[str] = Query(None, description="模糊关键词（空格分隔）"),
        page: int = Query(1, ge=1),
        page_size: int = Query(20, ge=1, le=100),
        service: CustomerService = Depends(get_customer_service)
):
    """查询客户列表（支持搜索）"""
    return service.list_customers(
        exact_smelter_name=exact_smelter_name,
        exact_contact_person=exact_contact_person,
        exact_contact_phone=exact_contact_phone,
        exact_credit_code=exact_credit_code,  # 新增
        fuzzy_keywords=fuzzy_keywords,
        page=page,
        page_size=page_size,
    )


@router.get("/warehouse-payees", summary="查询库房收款员信息列表")
async def list_warehouse_payees(
    warehouse_name: Optional[str] = Query(None, description="库房名称(模糊搜索)"),
    payee_name: Optional[str] = Query(None, description="收款员名称(模糊搜索)"),
    is_active: Optional[int] = Query(None, ge=0, le=1, description="是否启用: 1启用, 0禁用"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页条数"),
    service: CustomerService = Depends(get_customer_service),
):
    """分页查询 pd_warehouse_payees 列表"""
    result = service.list_warehouse_payees(
        warehouse_name=warehouse_name,
        payee_name=payee_name,
        is_active=is_active,
        page=page,
        page_size=page_size,
    )
    if result.get("success"):
        return result
    raise HTTPException(status_code=500, detail=result.get("error", "查询库房收款员信息失败"))


@router.get("/{customer_id}", summary="查看客户详情", response_model=CustomerOut)
async def get_customer(
        customer_id: int,
        service: CustomerService = Depends(get_customer_service)
):
    """查看客户详情"""
    customer = service.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="客户不存在")

    # 转换时间格式
    for key in ['created_at', 'updated_at']:
        if customer.get(key):
            customer[key] = str(customer[key])

    return customer


@router.put("/{customer_id}", summary="编辑客户信息", response_model=dict)
async def update_customer(
        customer_id: int,
        request: CustomerUpdateRequest,
        service: CustomerService = Depends(get_customer_service)
):
    """编辑客户信息"""
    try:
        data = {}
        if request.smelter_name is not None:
            data["smelter_name"] = request.smelter_name
        if request.address is not None:
            data["address"] = request.address
        if request.contact_person is not None:
            data["contact_person"] = request.contact_person
        if request.contact_phone is not None:
            data["contact_phone"] = request.contact_phone
        if request.contact_address is not None:
            data["contact_address"] = request.contact_address
        if request.credit_code is not None:
            data["credit_code"] = request.credit_code

        result = service.update_customer(customer_id, data)

        if result["success"]:
            return result
        else:
            raise HTTPException(status_code=400, detail=result.get("error"))

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{customer_id}", summary="删除客户")
async def delete_customer(
        customer_id: int,
        service: CustomerService = Depends(get_customer_service)
):
    """删除客户"""
    result = service.delete_customer(customer_id)
    if result["success"]:
        return result
    else:
        raise HTTPException(status_code=400, detail=result.get("error"))

@router.post("/warehouse-payees", summary="新增库房收款员信息")
async def create_warehouse_payee(
    request: WarehousePayeeCreateRequest,
    service: CustomerService = Depends(get_customer_service),
):
    """新增 pd_warehouse_payees 记录"""
    result = service.create_warehouse_payee(request.model_dump())
    if result.get("success"):
        return result
    raise HTTPException(status_code=500, detail=result.get("error", "新增库房收款员信息失败"))


@router.put("/warehouse-payees/{payee_id}", summary="编辑库房收款员信息")
async def update_warehouse_payee(
    payee_id: int,
    request: WarehousePayeeUpdateRequest,
    service: CustomerService = Depends(get_customer_service),
):
    """编辑 pd_warehouse_payees 指定字段"""
    update_data = request.model_dump(exclude_unset=True)
    result = service.update_warehouse_payee(payee_id, update_data)
    if result.get("success"):
        return result
    error = result.get("error", "编辑库房收款员信息失败")
    if "不存在" in str(error):
        raise HTTPException(status_code=404, detail=error)
    if "没有要更新的字段" in str(error):
        raise HTTPException(status_code=400, detail=error)
    raise HTTPException(status_code=500, detail=error)


