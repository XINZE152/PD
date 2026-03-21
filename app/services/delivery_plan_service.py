"""
报货计划：录入、查询、更新与删除（含品类单价明细）
"""
import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from pymysql.cursors import DictCursor

from app.services.contract_service import get_conn

logger = logging.getLogger(__name__)

_PLAN_AUDIT_COLS_ENSURED = False


def _ensure_plan_audit_columns() -> None:
    """旧库补全报货计划操作人相关字段（仅执行一次）。"""
    global _PLAN_AUDIT_COLS_ENSURED
    if _PLAN_AUDIT_COLS_ENSURED:
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'pd_delivery_plans'
                    """
                )
                existing = {row[0] for row in (cur.fetchall() or [])}
                parts: list[str] = []
                if "created_by" not in existing:
                    parts.append(
                        "ADD COLUMN created_by BIGINT DEFAULT NULL COMMENT '创建人用户ID'"
                    )
                if "created_by_name" not in existing:
                    parts.append(
                        "ADD COLUMN created_by_name VARCHAR(64) DEFAULT NULL COMMENT '创建人姓名'"
                    )
                if "updated_by" not in existing:
                    parts.append(
                        "ADD COLUMN updated_by BIGINT DEFAULT NULL COMMENT '最后修改人用户ID'"
                    )
                if "updated_by_name" not in existing:
                    parts.append(
                        "ADD COLUMN updated_by_name VARCHAR(64) DEFAULT NULL COMMENT '最后修改人姓名'"
                    )
                if parts:
                    cur.execute("ALTER TABLE pd_delivery_plans " + ", ".join(parts))
            conn.commit()
        _PLAN_AUDIT_COLS_ENSURED = True
    except Exception as e:
        logger.warning("ensure_plan_audit_columns skipped/failed: %s", e)


def _serialize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row)
    for key, val in out.items():
        if isinstance(val, datetime):
            out[key] = val.isoformat(sep=" ", timespec="seconds")
        elif isinstance(val, date):
            out[key] = val.isoformat()
        elif isinstance(val, Decimal):
            out[key] = float(val)
    return out


def _normalize_items(items: Optional[List[Dict[str, Any]]]) -> List[tuple]:
    """返回 (category_name, unit_price, sort_order) 列表；校验重复品类。"""
    if not items:
        return []
    seen: set[str] = set()
    normalized: List[tuple] = []
    for idx, it in enumerate(items):
        name = (it.get("category") or it.get("category_name") or "").strip()
        if not name:
            raise ValueError("品类不能为空")
        if name in seen:
            raise ValueError(f"品类重复: {name}")
        seen.add(name)
        price = float(it.get("unit_price", 0))
        if price < 0:
            raise ValueError("单价不能为负")
        normalized.append((name, price, idx))
    return normalized


def _fetch_products_for_plan_ids(cur, plan_ids: List[int]) -> Dict[int, List[Dict[str, Any]]]:
    if not plan_ids:
        return {}
    placeholders = ",".join(["%s"] * len(plan_ids))
    cur.execute(
        f"""
        SELECT id, plan_id, category_name, unit_price, sort_order, created_at, updated_at
        FROM pd_delivery_plan_products
        WHERE plan_id IN ({placeholders})
        ORDER BY plan_id, sort_order, id
        """,
        tuple(plan_ids),
    )
    rows = cur.fetchall() or []
    by_plan: Dict[int, List[Dict[str, Any]]] = {}
    for r in rows:
        pid = int(r["plan_id"])
        sr = _serialize_row(dict(r))
        by_plan.setdefault(pid, []).append(
            {
                "id": sr["id"],
                "category_name": sr["category_name"],
                "unit_price": sr["unit_price"],
                "sort_order": sr["sort_order"],
                "created_at": sr.get("created_at"),
                "updated_at": sr.get("updated_at"),
            }
        )
    return by_plan


class DeliveryPlanService:
    _PLAN_SELECT = """
        id, plan_no, smelter_name, plan_name, plan_start_date, planned_trucks, planned_tonnage,
        plan_status, confirmed_trucks, unconfirmed_trucks,
        created_by, created_by_name, updated_by, updated_by_name,
        created_at, updated_at
    """

    def create_plan(
        self,
        data: Dict[str, Any],
        *,
        operator_id: Optional[int] = None,
        operator_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        items_raw = data.get("items") or []
        try:
            rows_to_insert = _normalize_items(items_raw)
        except ValueError as e:
            return {"success": False, "error": str(e)}

        _ensure_plan_audit_columns()
        try:
            with get_conn() as conn:
                prev_ac = conn.get_autocommit()
                conn.autocommit(False)
                plan_id: Optional[int] = None
                try:
                    with conn.cursor(DictCursor) as cur:
                        cur.execute(
                            """
                            INSERT INTO pd_delivery_plans (
                                plan_no, smelter_name, plan_name, plan_start_date, planned_trucks, planned_tonnage,
                                plan_status, confirmed_trucks, unconfirmed_trucks,
                                created_by, created_by_name, updated_by, updated_by_name
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                data["plan_no"],
                                data.get("smelter_name"),
                                data.get("plan_name"),
                                data["plan_start_date"],
                                int(data.get("planned_trucks", 0)),
                                float(data.get("planned_tonnage", 0)),
                                data.get("plan_status") or "生效中",
                                int(data.get("confirmed_trucks", 0)),
                                int(data.get("unconfirmed_trucks", 0)),
                                operator_id,
                                operator_name,
                                operator_id,
                                operator_name,
                            ),
                        )
                        plan_id = cur.lastrowid
                        for cat, price, sort_order in rows_to_insert:
                            cur.execute(
                                """
                                INSERT INTO pd_delivery_plan_products
                                (plan_id, category_name, unit_price, sort_order)
                                VALUES (%s, %s, %s, %s)
                                """,
                                (plan_id, cat, price, sort_order),
                            )
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise
                finally:
                    conn.autocommit(prev_ac)

            return {
                "success": True,
                "message": "报货计划录入成功",
                "data": {"id": plan_id},
            }
        except Exception as e:
            logger.error("create delivery plan failed: %s", e)
            err = str(e)
            if "Duplicate entry" in err and "uk_plan_no" in err:
                return {"success": False, "error": "计划编号已存在"}
            if "Duplicate entry" in err and "uk_plan_category" in err:
                return {"success": False, "error": "同一计划下品类不能重复"}
            return {"success": False, "error": err}

    def increment_confirmed_trucks_by_plan_no(
        self,
        plan_no: str,
        truck_count: int,
        *,
        operator_id: Optional[int] = None,
        operator_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        if truck_count < 1:
            return {"success": False, "error": "车数须为正整数"}
        _ensure_plan_audit_columns()
        try:
            with get_conn() as conn:
                with conn.cursor(DictCursor) as cur:
                    cur.execute(
                        """
                        UPDATE pd_delivery_plans
                        SET confirmed_trucks = confirmed_trucks + %s,
                            unconfirmed_trucks = GREATEST(0, planned_trucks - confirmed_trucks - %s),
                            updated_by = %s,
                            updated_by_name = %s
                        WHERE plan_no = %s
                        """,
                        (truck_count, truck_count, operator_id, operator_name, plan_no),
                    )
                    if cur.rowcount == 0:
                        return {"success": False, "error": f"报货计划编号不存在: {plan_no}"}
                    conn.commit()
                    cur.execute(
                        f"""
                        SELECT {self._PLAN_SELECT.strip()}
                        FROM pd_delivery_plans
                        WHERE plan_no = %s
                        """,
                        (plan_no,),
                    )
                    row = cur.fetchone()
                    out = _serialize_row(row) if row else {}
                    return {
                        "success": True,
                        "message": "已定/未定车数已更新",
                        "data": out,
                    }
        except Exception as e:
            logger.error("increment confirmed trucks failed: %s", e)
            return {"success": False, "error": str(e)}

    def get_plan(self, plan_id: int) -> Dict[str, Any]:
        _ensure_plan_audit_columns()
        try:
            with get_conn() as conn:
                with conn.cursor(DictCursor) as cur:
                    cur.execute(
                        f"SELECT {self._PLAN_SELECT.strip()} FROM pd_delivery_plans WHERE id = %s",
                        (plan_id,),
                    )
                    row = cur.fetchone()
                    if not row:
                        return {"success": False, "error": f"报货计划 ID {plan_id} 不存在"}
                    data = _serialize_row(row)
                    prods = _fetch_products_for_plan_ids(cur, [plan_id])
                    data["items"] = prods.get(plan_id, [])
                    return {"success": True, "data": data}
        except Exception as e:
            logger.error("get delivery plan failed: %s", e)
            return {"success": False, "error": str(e)}

    def list_plans(
        self,
        plan_no: Optional[str] = None,
        plan_status: Optional[str] = None,
        smelter_name: Optional[str] = None,
        plan_start_from: Optional[str] = None,
        plan_start_to: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Dict[str, Any]:
        _ensure_plan_audit_columns()
        try:
            with get_conn() as conn:
                with conn.cursor(DictCursor) as cur:
                    where_clauses: list[str] = []
                    params: list[Any] = []

                    if plan_no:
                        where_clauses.append("plan_no LIKE %s")
                        params.append(f"%{plan_no}%")
                    if plan_status:
                        where_clauses.append("plan_status = %s")
                        params.append(plan_status)
                    if smelter_name:
                        where_clauses.append("smelter_name LIKE %s")
                        params.append(f"%{smelter_name}%")
                    if plan_start_from:
                        where_clauses.append("plan_start_date >= %s")
                        params.append(plan_start_from)
                    if plan_start_to:
                        where_clauses.append("plan_start_date <= %s")
                        params.append(plan_start_to)

                    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

                    cur.execute(
                        f"SELECT COUNT(*) AS total FROM pd_delivery_plans {where_sql}",
                        tuple(params),
                    )
                    total_row = cur.fetchone()
                    total = int(total_row["total"]) if total_row else 0

                    offset = (page - 1) * page_size
                    cur.execute(
                        f"""
                        SELECT {self._PLAN_SELECT.strip()}
                        FROM pd_delivery_plans
                        {where_sql}
                        ORDER BY plan_start_date DESC, id DESC
                        LIMIT %s OFFSET %s
                        """,
                        tuple(params + [page_size, offset]),
                    )
                    rows = cur.fetchall() or []
                    plan_ids = [int(r["id"]) for r in rows]
                    products_by_plan = _fetch_products_for_plan_ids(cur, plan_ids)
                    out_rows = []
                    for r in rows:
                        ser = _serialize_row(r)
                        ser["items"] = products_by_plan.get(int(r["id"]), [])
                        out_rows.append(ser)

                    return {
                        "success": True,
                        "data": out_rows,
                        "total": total,
                        "page": page,
                        "page_size": page_size,
                    }
        except Exception as e:
            logger.error("list delivery plans failed: %s", e)
            return {"success": False, "error": str(e)}

    def update_plan(
        self,
        plan_id: int,
        data: Dict[str, Any],
        *,
        operator_id: Optional[int] = None,
        operator_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        allowed = {
            "plan_no",
            "smelter_name",
            "plan_name",
            "plan_start_date",
            "planned_trucks",
            "planned_tonnage",
            "plan_status",
            "confirmed_trucks",
            "unconfirmed_trucks",
        }
        raw = dict(data)
        replace_items: Optional[List[Dict[str, Any]]] = None
        if "items" in raw:
            replace_items = raw.pop("items")

        try:
            if replace_items is not None:
                rows_to_insert = _normalize_items(replace_items)
            else:
                rows_to_insert = None
        except ValueError as e:
            return {"success": False, "error": str(e)}

        _ensure_plan_audit_columns()
        try:
            with get_conn() as conn:
                prev_ac = conn.get_autocommit()
                conn.autocommit(False)
                try:
                    with conn.cursor(DictCursor) as cur:
                        cur.execute("SELECT id FROM pd_delivery_plans WHERE id = %s", (plan_id,))
                        if not cur.fetchone():
                            conn.rollback()
                            return {"success": False, "error": f"报货计划 ID {plan_id} 不存在"}

                        update_fields: list[str] = []
                        params: list[Any] = []
                        for field in allowed:
                            if field in raw and raw[field] is not None:
                                update_fields.append(f"{field} = %s")
                                params.append(raw[field])

                        will_touch = bool(update_fields) or rows_to_insert is not None
                        if will_touch and (operator_id is not None or operator_name is not None):
                            update_fields.extend(["updated_by = %s", "updated_by_name = %s"])
                            params.extend([operator_id, operator_name])

                        if update_fields:
                            params.append(plan_id)
                            cur.execute(
                                f"UPDATE pd_delivery_plans SET {', '.join(update_fields)} WHERE id = %s",
                                tuple(params),
                            )

                        if rows_to_insert is not None:
                            cur.execute(
                                "DELETE FROM pd_delivery_plan_products WHERE plan_id = %s",
                                (plan_id,),
                            )
                            for cat, price, sort_order in rows_to_insert:
                                cur.execute(
                                    """
                                    INSERT INTO pd_delivery_plan_products
                                    (plan_id, category_name, unit_price, sort_order)
                                    VALUES (%s, %s, %s, %s)
                                    """,
                                    (plan_id, cat, price, sort_order),
                                )

                        if not update_fields and rows_to_insert is None:
                            conn.rollback()
                            return {"success": False, "error": "没有要更新的字段"}

                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise
                finally:
                    conn.autocommit(prev_ac)

                return {"success": True, "message": "报货计划更新成功", "data": {"id": plan_id}}
        except Exception as e:
            logger.error("update delivery plan failed: %s", e)
            err = str(e)
            if "Duplicate entry" in err and "uk_plan_no" in err:
                return {"success": False, "error": "计划编号已存在"}
            if "Duplicate entry" in err and "uk_plan_category" in err:
                return {"success": False, "error": "同一计划下品类不能重复"}
            return {"success": False, "error": err}

    def delete_plan(self, plan_id: int) -> Dict[str, Any]:
        try:
            with get_conn() as conn:
                with conn.cursor(DictCursor) as cur:
                    cur.execute("DELETE FROM pd_delivery_plans WHERE id = %s", (plan_id,))
                    if cur.rowcount == 0:
                        return {"success": False, "error": f"报货计划 ID {plan_id} 不存在"}
                    conn.commit()
                    return {"success": True, "message": "报货计划已删除", "data": {"id": plan_id}}
        except Exception as e:
            logger.error("delete delivery plan failed: %s", e)
            return {"success": False, "error": str(e)}


_delivery_plan_service: Optional[DeliveryPlanService] = None


def get_delivery_plan_service() -> DeliveryPlanService:
    global _delivery_plan_service
    if _delivery_plan_service is None:
        _delivery_plan_service = DeliveryPlanService()
    return _delivery_plan_service
