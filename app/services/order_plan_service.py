"""
订货计划：录入、列表筛选、仅修改车数（与报货计划关联并带入冶炼厂）
"""
import logging
from datetime import date, datetime
from typing import Any, Dict, Optional

from pymysql.cursors import DictCursor

from app.services.contract_service import get_conn
from app.services.delivery_plan_service import (
    apply_adjust_confirmed_trucks,
    apply_increment_confirmed_trucks,
    get_delivery_plan_service,
)

logger = logging.getLogger(__name__)

_ORDER_PLAN_REMARK_ENSURED = False

AUDIT_STATUS_PENDING = "待审核"
AUDIT_STATUS_APPROVED = "审核通过"
AUDIT_STATUS_REJECTED = "审核未通过"
VALID_AUDIT_STATUSES = frozenset(
    {AUDIT_STATUS_PENDING, AUDIT_STATUS_APPROVED, AUDIT_STATUS_REJECTED}
)


def _ensure_order_plan_remark_column() -> None:
    """旧库补全订货计划审核备注字段（仅执行一次）。"""
    global _ORDER_PLAN_REMARK_ENSURED
    if _ORDER_PLAN_REMARK_ENSURED:
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'pd_order_plans'
                    """
                )
                existing = {row[0] for row in (cur.fetchall() or [])}
                if "audit_remark" not in existing:
                    cur.execute(
                        """
                        ALTER TABLE pd_order_plans
                        ADD COLUMN audit_remark TEXT DEFAULT NULL COMMENT '审核备注/原因'
                        """
                    )
            conn.commit()
        _ORDER_PLAN_REMARK_ENSURED = True
    except Exception as e:
        logger.warning("ensure_order_plan_remark_column skipped/failed: %s", e)


def _serialize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row)
    for key, val in out.items():
        if isinstance(val, datetime):
            out[key] = val.isoformat(sep=" ", timespec="seconds")
        elif isinstance(val, date):
            out[key] = val.isoformat()
    return out


class OrderPlanService:
    _SELECT = """
        id, delivery_plan_id, plan_no, smelter_name, truck_count, audit_status, audit_remark,
        created_by, created_by_name, updated_by, updated_by_name,
        created_at, updated_at
    """

    def _lookup_delivery_plan(
        self, cur, plan_no: str
    ) -> Optional[Dict[str, Any]]:
        cur.execute(
            """
            SELECT id, plan_no, smelter_name, plan_status
            FROM pd_delivery_plans
            WHERE plan_no = %s
            LIMIT 1
            """,
            (plan_no,),
        )
        return cur.fetchone()

    def _validate_truck_limit(
        self,
        cur,
        delivery_plan_id: int,
        candidate_truck_count: int,
        *,
        include_candidate: bool,
        exclude_order_plan_id: Optional[int] = None,
    ) -> Optional[str]:
        """
        校验同一报货计划下「待审核 + 审核通过」订货计划总车数不超过报货计划 planned_trucks。
        """
        cur.execute(
            "SELECT planned_trucks FROM pd_delivery_plans WHERE id = %s LIMIT 1",
            (delivery_plan_id,),
        )
        dp = cur.fetchone()
        if not dp:
            return f"报货计划 ID {delivery_plan_id} 不存在"

        planned_limit = int(dp.get("planned_trucks") or 0)
        if exclude_order_plan_id is None:
            cur.execute(
                """
                SELECT COALESCE(SUM(truck_count), 0) AS used_trucks
                FROM pd_order_plans
                WHERE delivery_plan_id = %s
                  AND audit_status IN (%s, %s)
                """,
                (delivery_plan_id, AUDIT_STATUS_PENDING, AUDIT_STATUS_APPROVED),
            )
        else:
            cur.execute(
                """
                SELECT COALESCE(SUM(truck_count), 0) AS used_trucks
                FROM pd_order_plans
                WHERE delivery_plan_id = %s
                  AND id <> %s
                  AND audit_status IN (%s, %s)
                """,
                (
                    delivery_plan_id,
                    exclude_order_plan_id,
                    AUDIT_STATUS_PENDING,
                    AUDIT_STATUS_APPROVED,
                ),
            )
        used_row = cur.fetchone() or {}
        used_trucks = int(used_row.get("used_trucks") or 0)
        projected = used_trucks + (candidate_truck_count if include_candidate else 0)
        if projected > planned_limit:
            return (
                f"报货计划总车数超出报货计划上限：计划车数{planned_limit}车，"
                f"当前待审核/审核通过合计{used_trucks}车，"
                f"本次提交{candidate_truck_count}车，提交后将达{projected}车"
            )
        return None

    def create(
        self,
        plan_no: str,
        truck_count: int,
        *,
        operator_id: Optional[int] = None,
        operator_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        _ensure_order_plan_remark_column()
        plan_no = (plan_no or "").strip()
        if not plan_no:
            return {"success": False, "error": "报货计划编号不能为空"}
        if truck_count < 0:
            return {"success": False, "error": "车数不能为负"}

        try:
            with get_conn() as conn:
                with conn.cursor(DictCursor) as cur:
                    dp = self._lookup_delivery_plan(cur, plan_no)
                    if not dp:
                        return {
                            "success": False,
                            "error": f"报货计划编号不存在: {plan_no}",
                        }
                    dp_status = (dp.get("plan_status") or "生效中").strip()
                    if dp_status != "生效中":
                        return {
                            "success": False,
                            "error": "该报货计划已失效或未处于生效状态，无法录入订货计划",
                        }
                    delivery_plan_id = int(dp["id"])
                    plan_no_db = (dp.get("plan_no") or plan_no).strip()
                    smelter = dp.get("smelter_name")

                    if operator_id is not None:
                        cur.execute(
                            """
                            SELECT id FROM pd_order_plans
                            WHERE delivery_plan_id = %s AND created_by = %s
                            LIMIT 1
                            """,
                            (delivery_plan_id, operator_id),
                        )
                    elif operator_name:
                        cur.execute(
                            """
                            SELECT id FROM pd_order_plans
                            WHERE delivery_plan_id = %s
                              AND created_by IS NULL
                              AND created_by_name = %s
                            LIMIT 1
                            """,
                            (delivery_plan_id, operator_name),
                        )
                    else:
                        cur.execute("SELECT 0 AS id WHERE 1=0")
                    if cur.fetchone():
                        return {
                            "success": False,
                            "error": "您在该报货计划下已有订货计划，每位大区经理同一报货计划仅限一条",
                        }

                    limit_err = self._validate_truck_limit(
                        cur,
                        delivery_plan_id,
                        truck_count,
                        include_candidate=True,
                    )
                    if limit_err:
                        return {"success": False, "error": limit_err}

                    cur.execute(
                        """
                        INSERT INTO pd_order_plans (
                            delivery_plan_id, plan_no, smelter_name, truck_count, audit_status,
                            created_by, created_by_name, updated_by, updated_by_name
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            delivery_plan_id,
                            plan_no_db,
                            smelter,
                            truck_count,
                            AUDIT_STATUS_PENDING,
                            operator_id,
                            operator_name,
                            operator_id,
                            operator_name,
                        ),
                    )
                    new_id = cur.lastrowid
                    conn.commit()

                    cur.execute(
                        f"SELECT {self._SELECT.strip()} FROM pd_order_plans WHERE id = %s",
                        (new_id,),
                    )
                    row = cur.fetchone()
                    return {
                        "success": True,
                        "message": "订货计划录入成功",
                        "data": _serialize_row(row) if row else {"id": new_id},
                    }
        except Exception as e:
            logger.error("create order plan failed: %s", e)
            return {"success": False, "error": str(e)}

    def get(self, order_plan_id: int) -> Dict[str, Any]:
        _ensure_order_plan_remark_column()
        try:
            with get_conn() as conn:
                with conn.cursor(DictCursor) as cur:
                    cur.execute(
                        f"SELECT {self._SELECT.strip()} FROM pd_order_plans WHERE id = %s",
                        (order_plan_id,),
                    )
                    row = cur.fetchone()
                    if not row:
                        return {
                            "success": False,
                            "error": f"订货计划 ID {order_plan_id} 不存在",
                        }
                    return {"success": True, "data": _serialize_row(row)}
        except Exception as e:
            logger.error("get order plan failed: %s", e)
            return {"success": False, "error": str(e)}

    def list_plans(
        self,
        *,
        audit_status: Optional[str] = None,
        plan_no: Optional[str] = None,
        smelter_name: Optional[str] = None,
        operator_name: Optional[str] = None,
        updated_from: Optional[str] = None,
        updated_to: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Dict[str, Any]:
        _ensure_order_plan_remark_column()
        try:
            with get_conn() as conn:
                with conn.cursor(DictCursor) as cur:
                    where_clauses: list[str] = []
                    params: list[Any] = []

                    if audit_status:
                        if audit_status not in VALID_AUDIT_STATUSES:
                            return {
                                "success": False,
                                "error": f"无效的状态，允许值：{', '.join(sorted(VALID_AUDIT_STATUSES))}",
                            }
                        where_clauses.append("audit_status = %s")
                        params.append(audit_status)
                    if plan_no:
                        where_clauses.append("plan_no LIKE %s")
                        params.append(f"%{plan_no.strip()}%")
                    if smelter_name:
                        where_clauses.append("smelter_name LIKE %s")
                        params.append(f"%{smelter_name.strip()}%")
                    if operator_name:
                        q = f"%{operator_name.strip()}%"
                        where_clauses.append(
                            "(created_by_name LIKE %s OR updated_by_name LIKE %s)"
                        )
                        params.extend([q, q])
                    if updated_from:
                        uf = updated_from.strip()
                        if len(uf) == 10 and uf[4] == "-" and uf[7] == "-":
                            uf = uf + " 00:00:00"
                        where_clauses.append("updated_at >= %s")
                        params.append(uf)
                    if updated_to:
                        ut = updated_to.strip()
                        if len(ut) == 10 and ut[4] == "-" and ut[7] == "-":
                            ut = ut + " 23:59:59"
                        where_clauses.append("updated_at <= %s")
                        params.append(ut)

                    where_sql = (
                        "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
                    )

                    cur.execute(
                        f"SELECT COUNT(*) AS total FROM pd_order_plans {where_sql}",
                        tuple(params),
                    )
                    tr = cur.fetchone()
                    total = int(tr["total"]) if tr else 0

                    offset = (page - 1) * page_size
                    cur.execute(
                        f"""
                        SELECT {self._SELECT.strip()}
                        FROM pd_order_plans
                        {where_sql}
                        ORDER BY updated_at DESC, id DESC
                        LIMIT %s OFFSET %s
                        """,
                        tuple(params + [page_size, offset]),
                    )
                    rows = [_serialize_row(dict(r)) for r in (cur.fetchall() or [])]
                    return {
                        "success": True,
                        "data": rows,
                        "total": total,
                        "page": page,
                        "page_size": page_size,
                    }
        except Exception as e:
            logger.error("list order plans failed: %s", e)
            return {"success": False, "error": str(e)}

    def update_truck_count_only(
        self,
        order_plan_id: int,
        truck_count: int,
        *,
        operator_id: Optional[int] = None,
        operator_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        仅当订货计划当前为「审核通过」或「审核未通过」时可改车数（待审核不可改）。
        只更新车数与操作人，审核状态不变；车数须 ≥1。
        """
        if truck_count < 1:
            return {"success": False, "error": "车数须大于 0"}

        _ensure_order_plan_remark_column()
        try:
            with get_conn() as conn:
                prev_ac = conn.get_autocommit()
                conn.autocommit(False)
                try:
                    with conn.cursor(DictCursor) as cur:
                        cur.execute(
                            """
                            SELECT id, audit_status, delivery_plan_id, truck_count, plan_no
                            FROM pd_order_plans
                            WHERE id = %s
                            FOR UPDATE
                            """,
                            (order_plan_id,),
                        )
                        row = cur.fetchone()
                        if not row:
                            conn.rollback()
                            return {
                                "success": False,
                                "error": f"订货计划 ID {order_plan_id} 不存在",
                            }

                        current_status = row.get("audit_status")
                        if current_status not in (
                            AUDIT_STATUS_APPROVED,
                            AUDIT_STATUS_REJECTED,
                        ):
                            conn.rollback()
                            return {
                                "success": False,
                                "error": "仅「审核通过」或「审核未通过」状态的订货计划可修改车数",
                            }

                        old_truck_count = int(row.get("truck_count") or 0)
                        plan_no_row = (row.get("plan_no") or "").strip()

                        # 仅「待审核/审核通过」计入报货计划车数上限，「审核未通过」不计入
                        include_candidate = current_status in (
                            AUDIT_STATUS_PENDING,
                            AUDIT_STATUS_APPROVED,
                        )
                        delivery_plan_id = int(row.get("delivery_plan_id"))
                        limit_err = self._validate_truck_limit(
                            cur,
                            delivery_plan_id,
                            truck_count,
                            include_candidate=include_candidate,
                            exclude_order_plan_id=order_plan_id,
                        )
                        if limit_err:
                            conn.rollback()
                            return {"success": False, "error": limit_err}

                        # 审核通过时已累加报货计划已定车数；改车数须同步，避免再次审核时重复累加
                        delta_confirmed = 0
                        if current_status == AUDIT_STATUS_APPROVED:
                            delta_confirmed = truck_count - old_truck_count
                        if delta_confirmed != 0:
                            if not plan_no_row:
                                conn.rollback()
                                return {
                                    "success": False,
                                    "error": "订货计划缺少报货计划编号，无法同步已定车数",
                                }
                            try:
                                apply_adjust_confirmed_trucks(
                                    cur,
                                    plan_no_row,
                                    delta_confirmed,
                                    operator_id=operator_id,
                                    operator_name=operator_name,
                                )
                            except ValueError as e:
                                conn.rollback()
                                return {"success": False, "error": str(e)}

                        cur.execute(
                            """
                            UPDATE pd_order_plans
                            SET truck_count = %s,
                                updated_by = %s,
                                updated_by_name = %s
                            WHERE id = %s
                              AND audit_status = %s
                            """,
                            (
                                truck_count,
                                operator_id,
                                operator_name,
                                order_plan_id,
                                current_status,
                            ),
                        )
                        if cur.rowcount == 0:
                            conn.rollback()
                            return {
                                "success": False,
                                "error": "仅「审核通过」或「审核未通过」可修改车数，或数据已被他人更新，请刷新后重试",
                            }

                        cur.execute(
                            f"SELECT {self._SELECT.strip()} FROM pd_order_plans WHERE id = %s",
                            (order_plan_id,),
                        )
                        out = cur.fetchone()
                    conn.commit()
                    return {
                        "success": True,
                        "message": "车数已更新",
                        "data": _serialize_row(out) if out else {},
                    }
                except Exception:
                    conn.rollback()
                    raise
                finally:
                    conn.autocommit(prev_ac)
        except Exception as e:
            logger.error("update order plan truck_count failed: %s", e)
            return {"success": False, "error": str(e)}

    def audit(
        self,
        order_plan_id: int,
        audit_result: str,
        remark: Optional[str],
        *,
        operator_id: Optional[int] = None,
        operator_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        if audit_result not in (AUDIT_STATUS_APPROVED, AUDIT_STATUS_REJECTED):
            return {
                "success": False,
                "error": f"audit_result 须为「{AUDIT_STATUS_APPROVED}」或「{AUDIT_STATUS_REJECTED}」",
            }

        _ensure_order_plan_remark_column()
        rmk = (remark or "").strip()
        if audit_result == AUDIT_STATUS_REJECTED and not rmk:
            return {
                "success": False,
                "error": "审核未通过时必须填写审核备注，请写明原因",
            }
        rmk_val: Optional[str] = rmk if rmk else None

        try:
            with get_conn() as conn:
                prev_ac = conn.get_autocommit()
                conn.autocommit(False)
                delivery_plan_id: Optional[int] = None
                try:
                    with conn.cursor(DictCursor) as cur:
                        cur.execute(
                            f"""
                            SELECT {self._SELECT.strip()}
                            FROM pd_order_plans
                            WHERE id = %s
                            FOR UPDATE
                            """,
                            (order_plan_id,),
                        )
                        row = cur.fetchone()
                        if not row:
                            conn.rollback()
                            return {
                                "success": False,
                                "error": f"订货计划 ID {order_plan_id} 不存在",
                            }
                        if row.get("audit_status") != AUDIT_STATUS_PENDING:
                            conn.rollback()
                            return {"success": False, "error": "仅「待审核」状态可审核"}

                        plan_no = (row.get("plan_no") or "").strip()
                        truck_count = int(row.get("truck_count") or 0)
                        delivery_plan_id = int(row["delivery_plan_id"])

                        if audit_result == AUDIT_STATUS_APPROVED:
                            try:
                                apply_increment_confirmed_trucks(
                                    cur,
                                    plan_no,
                                    truck_count,
                                    operator_id=operator_id,
                                    operator_name=operator_name,
                                )
                            except ValueError as e:
                                conn.rollback()
                                return {"success": False, "error": str(e)}

                        cur.execute(
                            """
                            UPDATE pd_order_plans
                            SET audit_status = %s,
                                audit_remark = %s,
                                updated_by = %s,
                                updated_by_name = %s
                            WHERE id = %s
                            """,
                            (
                                audit_result,
                                rmk_val,
                                operator_id,
                                operator_name,
                                order_plan_id,
                            ),
                        )
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise
                finally:
                    conn.autocommit(prev_ac)
        except Exception as e:
            logger.error("audit order plan failed: %s", e)
            return {"success": False, "error": str(e)}

        op_msg = (
            "审核已通过"
            if audit_result == AUDIT_STATUS_APPROVED
            else "审核已驳回"
        )
        payload: Dict[str, Any] = {
            "order_plan": self.get(order_plan_id).get("data") or {},
        }
        if audit_result == AUDIT_STATUS_APPROVED and delivery_plan_id is not None:
            dp = get_delivery_plan_service().get_plan(delivery_plan_id)
            if dp.get("success"):
                payload["delivery_plan"] = dp.get("data")
            else:
                payload["delivery_plan"] = None
        else:
            payload["delivery_plan"] = None

        return {
            "success": True,
            "message": op_msg,
            "data": payload,
        }


_order_plan_service: Optional[OrderPlanService] = None


def get_order_plan_service() -> OrderPlanService:
    global _order_plan_service
    if _order_plan_service is None:
        _order_plan_service = OrderPlanService()
    return _order_plan_service
