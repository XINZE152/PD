"""
报货计划：录入、查询、修改、删除
"""
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import AliasChoices, BaseModel, ConfigDict, Field

from app.services.delivery_plan_service import DeliveryPlanService, get_delivery_plan_service
from core.auth import get_current_user

router = APIRouter(prefix="/delivery-plans", tags=["报货计划"])


def _operator_from_user(current_user: dict) -> tuple[Optional[int], Optional[str]]:
    uid = current_user.get("id")
    try:
        op_id = int(uid) if uid is not None else None
    except (TypeError, ValueError):
        op_id = None
    op_name = current_user.get("name") or current_user.get("account")
    return op_id, op_name if op_name else None


class DeliveryPlanProductItem(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    category: str = Field(
        ...,
        max_length=64,
        description="品类",
        validation_alias=AliasChoices("category", "category_name"),
    )
    unit_price: float = Field(..., ge=0, description="单价（元）")


class DeliveryPlanCreateRequest(BaseModel):
    plan_no: str = Field(..., description="计划编号", max_length=64)
    smelter_name: Optional[str] = Field(None, description="冶炼厂", max_length=128)
    plan_name: Optional[str] = Field(None, description="计划名", max_length=128)
    plan_start_date: str = Field(..., description="计划开始日期 YYYY-MM-DD")
    planned_trucks: int = Field(0, ge=0, description="计划车数")
    planned_tonnage: float = Field(0, ge=0, description="计划吨数")
    plan_status: str = Field("生效中", description="计划状态", max_length=32)
    confirmed_trucks: int = Field(0, ge=0, description="已定车数")
    unconfirmed_trucks: int = Field(0, ge=0, description="未定车数")
    items: List[DeliveryPlanProductItem] = Field(
        default_factory=list,
        description="品类与单价明细（同一计划内品类不可重复）",
    )


class IncrementConfirmedTrucksRequest(BaseModel):
    plan_no: str = Field(..., min_length=1, description="报货计划编号", max_length=64)
    truck_count: int = Field(..., ge=1, description="本次累加车数（累加到已定车数）")


class DeliveryPlanUpdateRequest(BaseModel):
    plan_no: Optional[str] = Field(None, description="计划编号", max_length=64)
    smelter_name: Optional[str] = Field(None, description="冶炼厂", max_length=128)
    plan_name: Optional[str] = Field(None, description="计划名", max_length=128)
    plan_start_date: Optional[str] = Field(None, description="计划开始日期 YYYY-MM-DD")
    planned_trucks: Optional[int] = Field(None, ge=0, description="计划车数")
    planned_tonnage: Optional[float] = Field(None, ge=0, description="计划吨数")
    plan_status: Optional[str] = Field(None, description="计划状态", max_length=32)
    confirmed_trucks: Optional[int] = Field(None, ge=0, description="已定车数")
    unconfirmed_trucks: Optional[int] = Field(None, ge=0, description="未定车数")
    items: Optional[List[DeliveryPlanProductItem]] = Field(
        None,
        description="传入则整表替换品类单价；不传则不改动明细",
    )


def _items_to_service_payload(items: List[DeliveryPlanProductItem]) -> list[dict]:
    return [{"category": it.category.strip(), "unit_price": it.unit_price} for it in items]


@router.post("/", summary="录入报货计划", response_model=dict)
async def create_delivery_plan(
    request: DeliveryPlanCreateRequest,
    current_user: dict = Depends(get_current_user),
    service: DeliveryPlanService = Depends(get_delivery_plan_service),
):
    payload = request.model_dump()
    payload["items"] = _items_to_service_payload(request.items)
    op_id, op_name = _operator_from_user(current_user)
    result = service.create_plan(payload, operator_id=op_id, operator_name=op_name)
    if result.get("success"):
        return result
    err = result.get("error", "录入失败")
    if "计划编号已存在" in str(err):
        raise HTTPException(status_code=400, detail=err)
    raise HTTPException(status_code=400, detail=err)


@router.post(
    "/increment-confirmed-trucks",
    summary="累加已定车数并重算未定车数",
    response_model=dict,
)
async def increment_confirmed_trucks(
    request: IncrementConfirmedTrucksRequest,
    current_user: dict = Depends(get_current_user),
    service: DeliveryPlanService = Depends(get_delivery_plan_service),
):
    op_id, op_name = _operator_from_user(current_user)
    result = service.increment_confirmed_trucks_by_plan_no(
        request.plan_no.strip(),
        request.truck_count,
        operator_id=op_id,
        operator_name=op_name,
    )
    if result.get("success"):
        return result
    err = result.get("error", "更新失败")
    if "不存在" in str(err):
        raise HTTPException(status_code=404, detail=err)
    raise HTTPException(status_code=400, detail=err)


@router.get("/", summary="查询报货计划列表", response_model=dict)
async def list_delivery_plans(
    plan_no: Optional[str] = Query(None, description="计划编号（模糊）"),
    plan_status: Optional[str] = Query(None, description="计划状态（精确）"),
    smelter_name: Optional[str] = Query(None, description="冶炼厂（模糊）"),
    plan_start_from: Optional[str] = Query(None, description="计划开始日期起 YYYY-MM-DD"),
    plan_start_to: Optional[str] = Query(None, description="计划开始日期止 YYYY-MM-DD"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    service: DeliveryPlanService = Depends(get_delivery_plan_service),
):
    result = service.list_plans(
        plan_no=plan_no,
        plan_status=plan_status,
        smelter_name=smelter_name,
        plan_start_from=plan_start_from,
        plan_start_to=plan_start_to,
        page=page,
        page_size=page_size,
    )
    if result.get("success"):
        return result
    raise HTTPException(status_code=500, detail=result.get("error", "查询失败"))


@router.get("/{plan_id}", summary="报货计划详情（含品类单价）", response_model=dict)
async def get_delivery_plan(
    plan_id: int,
    service: DeliveryPlanService = Depends(get_delivery_plan_service),
):
    result = service.get_plan(plan_id)
    if result.get("success"):
        return result
    err = result.get("error", "查询失败")
    if "不存在" in str(err):
        raise HTTPException(status_code=404, detail=err)
    raise HTTPException(status_code=500, detail=err)


@router.put("/{plan_id}", summary="修改报货计划", response_model=dict)
async def update_delivery_plan(
    plan_id: int,
    request: DeliveryPlanUpdateRequest,
    current_user: dict = Depends(get_current_user),
    service: DeliveryPlanService = Depends(get_delivery_plan_service),
):
    data = request.model_dump(exclude_unset=True)
    if "items" in data:
        data["items"] = (
            _items_to_service_payload(request.items)
            if request.items is not None
            else []
        )
    op_id, op_name = _operator_from_user(current_user)
    result = service.update_plan(
        plan_id, data, operator_id=op_id, operator_name=op_name
    )
    if result.get("success"):
        return result
    err = result.get("error", "更新失败")
    if "不存在" in str(err):
        raise HTTPException(status_code=404, detail=err)
    if "计划编号已存在" in str(err):
        raise HTTPException(status_code=400, detail=err)
    raise HTTPException(status_code=400, detail=err)


@router.delete("/{plan_id}", summary="删除报货计划", response_model=dict)
async def delete_delivery_plan(
    plan_id: int,
    service: DeliveryPlanService = Depends(get_delivery_plan_service),
):
    result = service.delete_plan(plan_id)
    if result.get("success"):
        return result
    err = result.get("error", "删除失败")
    if "不存在" in str(err):
        raise HTTPException(status_code=404, detail=err)
    raise HTTPException(status_code=400, detail=err)
