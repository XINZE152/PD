"""
报货计划：查询与更新
"""
import logging
from datetime import date, datetime
from typing import Any, Dict, Optional

from pymysql.cursors import DictCursor

from app.services.contract_service import get_conn

logger = logging.getLogger(__name__)


def _serialize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row)
    for key, val in out.items():
        if isinstance(val, datetime):
            out[key] = val.isoformat(sep=" ", timespec="seconds")
        elif isinstance(val, date):
            out[key] = val.isoformat()
    return out


class DeliveryPlanService:
    def list_plans(
        self,
        plan_no: Optional[str] = None,
        plan_status: Optional[str] = None,
        plan_start_from: Optional[str] = None,
        plan_start_to: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Dict[str, Any]:
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
                        SELECT id, plan_no, plan_start_date, planned_trucks, planned_tonnage,
                               plan_status, confirmed_trucks, unconfirmed_trucks,
                               created_at, updated_at
                        FROM pd_delivery_plans
                        {where_sql}
                        ORDER BY plan_start_date DESC, id DESC
                        LIMIT %s OFFSET %s
                        """,
                        tuple(params + [page_size, offset]),
                    )
                    rows = [_serialize_row(r) for r in (cur.fetchall() or [])]

                    return {
                        "success": True,
                        "data": rows,
                        "total": total,
                        "page": page,
                        "page_size": page_size,
                    }
        except Exception as e:
            logger.error("list delivery plans failed: %s", e)
            return {"success": False, "error": str(e)}

    def update_plan(self, plan_id: int, data: Dict[str, Any]) -> Dict[str, Any]:
        allowed = {
            "plan_no",
            "plan_start_date",
            "planned_trucks",
            "planned_tonnage",
            "plan_status",
            "confirmed_trucks",
            "unconfirmed_trucks",
        }
        try:
            with get_conn() as conn:
                with conn.cursor(DictCursor) as cur:
                    cur.execute("SELECT id FROM pd_delivery_plans WHERE id = %s", (plan_id,))
                    if not cur.fetchone():
                        return {"success": False, "error": f"报货计划 ID {plan_id} 不存在"}

                    update_fields: list[str] = []
                    params: list[Any] = []
                    for field in allowed:
                        if field in data and data[field] is not None:
                            update_fields.append(f"{field} = %s")
                            params.append(data[field])

                    if not update_fields:
                        return {"success": False, "error": "没有要更新的字段"}

                    params.append(plan_id)
                    cur.execute(
                        f"UPDATE pd_delivery_plans SET {', '.join(update_fields)} WHERE id = %s",
                        tuple(params),
                    )
                    conn.commit()

                    return {"success": True, "message": "报货计划更新成功", "data": {"id": plan_id}}
        except Exception as e:
            logger.error("update delivery plan failed: %s", e)
            err = str(e)
            if "Duplicate entry" in err and "uk_plan_no" in err:
                return {"success": False, "error": "计划编号已存在"}
            return {"success": False, "error": err}


_delivery_plan_service: Optional[DeliveryPlanService] = None


def get_delivery_plan_service() -> DeliveryPlanService:
    global _delivery_plan_service
    if _delivery_plan_service is None:
        _delivery_plan_service = DeliveryPlanService()
    return _delivery_plan_service
