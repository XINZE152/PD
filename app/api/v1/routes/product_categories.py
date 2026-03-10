"""
品类管理路由 - 固定50个槽位
"""
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.services.product_category_service import ProductCategoryService, get_product_category_service

router = APIRouter(prefix="/product-categories", tags=["品类管理"])


class ProductCategoryCreateRequest(BaseModel):
    category_name: str = Field(..., description="品类名称", min_length=1, max_length=64)


class ProductCategoryDeleteRequest(BaseModel):
    category_name: str = Field(..., description="要删除的品类名称", min_length=1, max_length=64)


@router.get("/", response_model=dict)
async def list_product_categories(
    service: ProductCategoryService = Depends(get_product_category_service),
):
    """查询固定50槽位品类列表"""
    result = service.list_categories()
    if result.get("success"):
        return result
    raise HTTPException(status_code=500, detail=result.get("error", "查询品类列表失败"))


@router.post("/", response_model=dict)
async def add_product_category(
    request: ProductCategoryCreateRequest,
    service: ProductCategoryService = Depends(get_product_category_service),
):
    """新增品类，自动写入第一个空槽位"""
    result = service.add_category(request.category_name)
    if result.get("success"):
        return result
    raise HTTPException(status_code=400, detail=result.get("error", "新增品类失败"))


@router.delete("/", response_model=dict)
async def delete_product_category(
    request: ProductCategoryDeleteRequest,
    service: ProductCategoryService = Depends(get_product_category_service),
):
    """按品类名称删除品类，将对应槽位置空"""
    result = service.delete_category(request.category_name)
    if result.get("success"):
        return result

    error = result.get("error", "删除品类失败")
    if "不存在" in str(error):
        raise HTTPException(status_code=404, detail=error)
    raise HTTPException(status_code=400, detail=error)