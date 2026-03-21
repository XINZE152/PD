"""
报货计划：录入、查询、修改、删除
"""
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.services.delivery_plan_service import DeliveryPlanService, get_delivery_plan_service

router = APIRouter(prefix="/delivery-plans", tags=["报货计划"])


class DeliveryPlanCreateRequest(BaseModel):
    plan_no: str = Field(..., description="计划编号", max_length=64)
    plan_start_date: str = Field(..., description="计划开始日期 YYYY-MM-DD")
    planned_trucks: int = Field(0, ge=0, description="计划车数")
    planned_tonnage: float = Field(0, ge=0, description="计划吨数")
    plan_status: str = Field("生效中", description="计划状态", max_length=32)
    confirmed_trucks: int = Field(0, ge=0, description="已定车数")
    unconfirmed_trucks: int = Field(0, ge=0, description="未定车数")


class IncrementConfirmedTrucksRequest(BaseModel):
    plan_no: str = Field(..., min_length=1, description="报货计划编号", max_length=64)
    truck_count: int = Field(..., ge=1, description="本次累加车数（累加到已定车数）")


class DeliveryPlanUpdateRequest(BaseModel):
    plan_no: Optional[str] = Field(None, description="计划编号", max_length=64)
    plan_start_date: Optional[str] = Field(None, description="计划开始日期 YYYY-MM-DD")
    planned_trucks: Optional[int] = Field(None, ge=0, description="计划车数")
    planned_tonnage: Optional[float] = Field(None, ge=0, description="计划吨数")
    plan_status: Optional[str] = Field(None, description="计划状态", max_length=32)
    confirmed_trucks: Optional[int] = Field(None, ge=0, description="已定车数")
    unconfirmed_trucks: Optional[int] = Field(None, ge=0, description="未定车数")


@router.post("/", summary="录入报货计划", response_model=dict)
async def create_delivery_plan(
    request: DeliveryPlanCreateRequest,
    service: DeliveryPlanService = Depends(get_delivery_plan_service),
):
    result = service.create_plan(request.model_dump())
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
    service: DeliveryPlanService = Depends(get_delivery_plan_service),
):
    """
    按计划编号将 `truck_count` 累加到已定车数；
    未定车数 = max(0, 计划车数 - 累加后的已定车数)（已定可大于计划，此时未定车数为 0）。
    """
    result = service.increment_confirmed_trucks_by_plan_no(
        request.plan_no.strip(),
        request.truck_count,
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
    plan_start_from: Optional[str] = Query(None, description="计划开始日期起 YYYY-MM-DD"),
    plan_start_to: Optional[str] = Query(None, description="计划开始日期止 YYYY-MM-DD"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    service: DeliveryPlanService = Depends(get_delivery_plan_service),
):
    result = service.list_plans(
        plan_no=plan_no,
        plan_status=plan_status,
        plan_start_from=plan_start_from,
        plan_start_to=plan_start_to,
        page=page,
        page_size=page_size,
    )
    if result.get("success"):
        return result
    raise HTTPException(status_code=500, detail=result.get("error", "查询失败"))


@router.put("/{plan_id}", summary="修改报货计划", response_model=dict)
async def update_delivery_plan(
    plan_id: int,
    request: DeliveryPlanUpdateRequest,
    service: DeliveryPlanService = Depends(get_delivery_plan_service),
):
    data = request.model_dump(exclude_unset=True)
    result = service.update_plan(plan_id, data)
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
