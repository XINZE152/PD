"""
品类管理服务 - 固定50个槽位
"""
import logging
from typing import Any, Dict, List, Optional, Tuple

from core.database import get_conn

logger = logging.getLogger(__name__)


class ProductCategoryService:
    """品类管理服务"""

    MAX_CATEGORIES = 50
    CATEGORY_FIELDS = [f"category_{index}" for index in range(1, MAX_CATEGORIES + 1)]

    def _ensure_row(self, cur) -> Dict[str, Any]:
        cur.execute("SELECT * FROM pd_product_categories ORDER BY id ASC LIMIT 1")
        row = cur.fetchone()
        if row:
            return row

        cur.execute("INSERT INTO pd_product_categories () VALUES ()")
        cur.execute("SELECT * FROM pd_product_categories WHERE id = %s", (cur.lastrowid,))
        return cur.fetchone()

    def _extract_categories(self, row: Dict[str, Any]) -> List[Dict[str, Any]]:
        categories: List[Dict[str, Any]] = []
        for index, field_name in enumerate(self.CATEGORY_FIELDS, start=1):
            category_name = row.get(field_name)
            if category_name:
                categories.append(
                    {
                        "slot": index,
                        "field": field_name,
                        "name": category_name,
                    }
                )
        return categories

    def _find_existing_slot(self, row: Dict[str, Any], category_name: str) -> Optional[Tuple[int, str]]:
        normalized = category_name.strip()
        for index, field_name in enumerate(self.CATEGORY_FIELDS, start=1):
            current_value = row.get(field_name)
            if current_value and str(current_value).strip() == normalized:
                return index, field_name
        return None

    def list_categories(self) -> Dict[str, Any]:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    row = self._ensure_row(cur)
                    categories = self._extract_categories(row)
                    slots = {field_name: row.get(field_name) for field_name in self.CATEGORY_FIELDS}
                    return {
                        "success": True,
                        "data": {
                            "id": row.get("id"),
                            "categories": categories,
                            "slots": slots,
                            "used_count": len(categories),
                            "remaining_count": self.MAX_CATEGORIES - len(categories),
                        },
                    }
        except Exception as e:
            logger.error(f"查询品类列表失败: {e}")
            return {"success": False, "error": str(e)}

    def add_category(self, category_name: str) -> Dict[str, Any]:
        normalized = (category_name or "").strip()
        if not normalized:
            return {"success": False, "error": "品类名称不能为空"}
        if len(normalized) > 64:
            return {"success": False, "error": "品类名称不能超过64个字符"}

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    row = self._ensure_row(cur)

                    existing = self._find_existing_slot(row, normalized)
                    if existing:
                        return {
                            "success": False,
                            "error": f"品类 '{normalized}' 已存在于槽位 {existing[0]}",
                        }

                    empty_field = None
                    empty_slot = None
                    for index, field_name in enumerate(self.CATEGORY_FIELDS, start=1):
                        current_value = row.get(field_name)
                        if current_value is None or str(current_value).strip() == "":
                            empty_field = field_name
                            empty_slot = index
                            break

                    if empty_field is None:
                        return {"success": False, "error": "品类槽位已满，最多支持50个品类"}

                    cur.execute(
                        f"UPDATE pd_product_categories SET {empty_field} = %s WHERE id = %s",
                        (normalized, row["id"]),
                    )

                    return {
                        "success": True,
                        "message": "品类添加成功",
                        "data": {
                            "id": row["id"],
                            "slot": empty_slot,
                            "field": empty_field,
                            "name": normalized,
                        },
                    }
        except Exception as e:
            logger.error(f"添加品类失败: {e}")
            return {"success": False, "error": str(e)}

    def delete_category(self, category_name: str) -> Dict[str, Any]:
        normalized = (category_name or "").strip()
        if not normalized:
            return {"success": False, "error": "品类名称不能为空"}

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    row = self._ensure_row(cur)
                    existing = self._find_existing_slot(row, normalized)
                    if not existing:
                        return {"success": False, "error": f"品类 '{normalized}' 不存在"}

                    slot, field_name = existing
                    cur.execute(
                        f"UPDATE pd_product_categories SET {field_name} = NULL WHERE id = %s",
                        (row["id"],),
                    )

                    return {
                        "success": True,
                        "message": "品类删除成功",
                        "data": {
                            "id": row["id"],
                            "slot": slot,
                            "field": field_name,
                            "name": normalized,
                        },
                    }
        except Exception as e:
            logger.error(f"删除品类失败: {e}")
            return {"success": False, "error": str(e)}


_product_category_service = None


def get_product_category_service() -> ProductCategoryService:
    global _product_category_service
    if _product_category_service is None:
        _product_category_service = ProductCategoryService()
    return _product_category_service