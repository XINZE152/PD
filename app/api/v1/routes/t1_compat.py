"""
兼容旧版「采购建议」路径 POST /t1/get_purchase_suggestion。

原先多对接外部大模型网关，易返回 403；现改为走本系统分配预测数据（与
POST /allocation/purchase-quantity/query 同源逻辑）。
"""
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from app.services.allocation_service import (
    get_warehouse_names_by_ids,
    query_ai_purchase_quantity,
)
from core.auth import get_current_user

from app.api.v1.routes.allocation import (
    PurchaseQuantityDataPayload,
    PurchaseQuantityQueryEnvelope,
)

router = APIRouter(prefix="/t1", tags=["兼容"])


class _PurchaseSuggestionDemandItem(BaseModel):
    category_id: int = 0
    demand: int = 0


class GetPurchaseSuggestionRequest(BaseModel):
    """与旧 Swagger 一致；`demands` / `price_type` 不参与本地预测筛选，可忽略。"""

    model_config = ConfigDict(title="采购建议（兼容）")

    warehouse_ids: list[int] = Field(default_factory=list)
    demands: list[_PurchaseSuggestionDemandItem] = Field(default_factory=list)
    price_type: str | None = Field(None, description="旧字段，忽略")
    start_date: str | None = Field(
        None,
        description="可选；不传则默认当天起连续 7 天（含首尾）",
    )
    end_date: str | None = Field(None, description="可选；须与 start_date 同传")


@router.post(
    "/get_purchase_suggestion",
    summary="采购建议（兼容）：本地分配预测",
    response_description="与统一查询接口相同的 success/message/data 结构",
    response_model=PurchaseQuantityQueryEnvelope,
)
async def get_purchase_suggestion(
    body: GetPurchaseSuggestionRequest,
    current_user: dict = Depends(get_current_user),
):
    start = (body.start_date or "").strip() or None
    end = (body.end_date or "").strip() or None
    if (start is None) ^ (end is None):
        return PurchaseQuantityQueryEnvelope(
            success=False,
            message="start_date 与 end_date 须同时填写或同时省略",
            data=None,
        )
    if not start and not end:
        today = datetime.now().date()
        start = today.strftime("%Y-%m-%d")
        end = (today + timedelta(days=6)).strftime("%Y-%m-%d")

    raw_ids = [i for i in body.warehouse_ids if isinstance(i, int) and i > 0]
    extra: dict = {}
    if raw_ids:
        names = get_warehouse_names_by_ids(raw_ids)
        if not names:
            return PurchaseQuantityQueryEnvelope(
                success=False,
                message="warehouse_ids 在系统中无匹配的仓库",
                data=None,
            )
        extra["warehouse_names"] = names

    raw = query_ai_purchase_quantity(start, end, **extra, current_user=current_user)

    if raw.get("success") and raw.get("data") is not None:
        payload = PurchaseQuantityDataPayload(**raw["data"])
        return PurchaseQuantityQueryEnvelope(
            success=True,
            message=raw.get("message") or "",
            data=payload,
        )
    status_code = int(raw.get("status_code") or 500)
    raise HTTPException(
        status_code=status_code,
        detail=raw.get("message") or "查询失败",
    )
