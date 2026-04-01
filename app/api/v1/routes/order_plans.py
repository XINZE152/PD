"""
订货计划：录入、列表筛选、修改车数（仅审核通过/审核未通过可改；改车数不改变审核状态）、审核
"""
from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field, model_validator

from app.services.order_plan_service import get_order_plan_service, OrderPlanService
from core.auth import get_current_user

router = APIRouter(prefix="/order-plans", tags=["订货计划"])

# 录入订货计划：大区经理每人每个报货计划一条；管理员可代录/运维
_ORDER_PLAN_CREATE_ROLES = frozenset({"大区经理", "管理员"})


def _operator_from_user(current_user: dict) -> tuple[Optional[int], Optional[str]]:
    uid = current_user.get("id")
    try:
        op_id = int(uid) if uid is not None else None
    except (TypeError, ValueError):
        op_id = None
    op_name = current_user.get("name") or current_user.get("account")
    return op_id, op_name if op_name else None


def require_regional_manager_for_order_plan_create(
    current_user: dict = Depends(get_current_user),
) -> dict:
    role = (current_user.get("role") or "").strip()
    if role not in _ORDER_PLAN_CREATE_ROLES:
        raise HTTPException(
            status_code=403,
            detail="仅大区经理或管理员可录入订货计划",
        )
    return current_user


class OrderPlanCreateRequest(BaseModel):
    plan_no: str = Field(
        ...,
        min_length=1,
        max_length=64,
        description="报货计划编号（关联 pd_delivery_plans.plan_no）",
    )
    truck_count: int = Field(..., ge=0, description="订货车数")


class OrderPlanTruckCountPatch(BaseModel):
    truck_count: int = Field(..., ge=1, description="订货车数（须 ≥1，不可改为 0）")


class OrderPlanAuditRequest(BaseModel):
    audit_result: Literal["审核通过", "审核未通过"] = Field(
        ..., description="审核结论：审核通过 / 审核未通过"
    )
    remark: Optional[str] = Field(
        None,
        max_length=4000,
        description="审核备注/原因；**审核未通过时必填**，审核通过时可选",
    )

    @model_validator(mode="after")
    def remark_required_when_rejected(self):
        if self.audit_result == "审核未通过":
            if self.remark is None or not str(self.remark).strip():
                raise ValueError("审核未通过时必须填写审核备注，请写明原因")
        return self


@router.post("/", summary="录入订货计划", response_model=dict)
async def create_order_plan(
    request: OrderPlanCreateRequest,
    current_user: dict = Depends(require_regional_manager_for_order_plan_create),
    service: OrderPlanService = Depends(get_order_plan_service),
):
    op_id, op_name = _operator_from_user(current_user)
    result = service.create(
        request.plan_no,
        request.truck_count,
        operator_id=op_id,
        operator_name=op_name,
    )
    if result.get("success"):
        return result
    err = result.get("error", "录入失败")
    if "不存在" in str(err):
        raise HTTPException(status_code=404, detail=err)
    raise HTTPException(status_code=400, detail=err)


@router.post(
    "/{order_plan_id}/audit",
    summary="审核订货计划（通过时按车数累加报货计划已定车数，与 increment-confirmed-trucks 同逻辑）",
    response_model=dict,
)
async def audit_order_plan(
    order_plan_id: int,
    body: OrderPlanAuditRequest,
    current_user: dict = Depends(get_current_user),
    service: OrderPlanService = Depends(get_order_plan_service),
):
    op_id, op_name = _operator_from_user(current_user)
    result = service.audit(
        order_plan_id,
        body.audit_result,
        body.remark,
        operator_id=op_id,
        operator_name=op_name,
    )
    if result.get("success"):
        return result
    err = result.get("error", "审核失败")
    if "不存在" in str(err):
        raise HTTPException(status_code=404, detail=err)
    raise HTTPException(status_code=400, detail=err)


@router.get(
    "/",
    summary="订货计划列表（支持多条件筛选）",
    response_model=dict,
    description="列表每条记录包含 `audit_remark`（审核备注，未填写时为 null）。",
)
async def list_order_plans(
    audit_status: Optional[str] = Query(
        None, description="审核状态：待审核/审核通过/审核未通过"
    ),
    plan_no: Optional[str] = Query(None, description="报货计划编号（模糊）"),
    smelter_name: Optional[str] = Query(None, description="冶炼厂（模糊）"),
    operator_name: Optional[str] = Query(
        None, description="操作人姓名（匹配创建人或最后操作人，模糊）"
    ),
    updated_from: Optional[str] = Query(
        None, description="最后操作时间起（含），格式 YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS"
    ),
    updated_to: Optional[str] = Query(
        None, description="最后操作时间止（含），格式 YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS"
    ),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    service: OrderPlanService = Depends(get_order_plan_service),
):
    result = service.list_plans(
        audit_status=audit_status,
        plan_no=plan_no,
        smelter_name=smelter_name,
        operator_name=operator_name,
        updated_from=updated_from,
        updated_to=updated_to,
        page=page,
        page_size=page_size,
    )
    if result.get("success"):
        return result
    raise HTTPException(status_code=400, detail=result.get("error", "查询失败"))


@router.get("/{order_plan_id}", summary="订货计划详情", response_model=dict)
async def get_order_plan(
    order_plan_id: int,
    service: OrderPlanService = Depends(get_order_plan_service),
):
    result = service.get(order_plan_id)
    if result.get("success"):
        return result
    err = result.get("error", "查询失败")
    if "不存在" in str(err):
        raise HTTPException(status_code=404, detail=err)
    raise HTTPException(status_code=500, detail=err)


@router.patch(
    "/{order_plan_id}/truck-count",
    summary="仅修改车数（仅审核通过/审核未通过可改；不改变审核状态；车数须 ≥1）",
    response_model=dict,
)
async def patch_order_plan_truck_count(
    order_plan_id: int,
    body: OrderPlanTruckCountPatch,
    current_user: dict = Depends(get_current_user),
    service: OrderPlanService = Depends(get_order_plan_service),
):
    op_id, op_name = _operator_from_user(current_user)
    result = service.update_truck_count_only(
        order_plan_id,
        body.truck_count,
        operator_id=op_id,
        operator_name=op_name,
    )
    if result.get("success"):
        return result
    err = result.get("error", "更新失败")
    if "不存在" in str(err):
        raise HTTPException(status_code=404, detail=err)
    raise HTTPException(status_code=400, detail=err)
