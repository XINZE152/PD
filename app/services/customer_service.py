"""
客户服务 - 冶炼厂客户档案管理
"""
import logging
from typing import Dict, Optional, Any
from app.services.contract_service import get_conn
from pymysql.cursors import DictCursor

logger = logging.getLogger(__name__)


class CustomerService:
    """客户服务"""

    def create_warehouse_payee(self, data: Dict) -> Dict[str, Any]:
        """新增收款员信息（不强制关联库房）"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # 收款员不强制关联库房，直接使用传入的值
                    warehouse_id = data.get("warehouse_id")
                    warehouse_name = data.get("warehouse_name")
                    
                    cur.execute(
                        """
                        INSERT INTO pd_warehouse_payees
                        (warehouse_id, warehouse_name, payee_name, payee_account, payee_bank_name, is_active)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (
                            warehouse_id,
                            warehouse_name,
                            data.get("payee_name"),
                            data.get("payee_account"),
                            data.get("payee_bank_name"),
                            data.get("is_active", 1),
                        ),
                    )
                    conn.commit()  # 确保提交事务
                    
                    return {
                        "success": True,
                        "message": "收款员信息创建成功",
                        "data": {"id": cur.lastrowid},
                    }
        except Exception as e:
            logger.error(f"创建收款员信息失败: {e}")
            return {"success": False, "error": str(e)}

    def update_warehouse_payee(self, payee_id: int, data: Dict) -> Dict[str, Any]:
        """编辑收款员信息"""
        try:
            with get_conn() as conn:
                with conn.cursor(DictCursor) as cur:
                    cur.execute("SELECT id FROM pd_warehouse_payees WHERE id = %s", (payee_id,))
                    if not cur.fetchone():
                        return {"success": False, "error": f"记录ID {payee_id} 不存在"}

                    allowed_fields = [
                        "warehouse_id",      # 添加库房ID字段
                        "warehouse_name",
                        "payee_name",
                        "payee_account",
                        "payee_bank_name",
                        "is_active",
                    ]
                    update_fields = []
                    params = []
                    for field in allowed_fields:
                        if field in data:
                            update_fields.append(f"{field} = %s")
                            params.append(data[field])

                    if not update_fields:
                        return {"success": False, "error": "没有要更新的字段"}

                    params.append(payee_id)
                    cur.execute(
                        f"UPDATE pd_warehouse_payees SET {', '.join(update_fields)} WHERE id = %s",
                        tuple(params),
                    )
                    conn.commit()  # 确保提交事务

                    return {
                        "success": True,
                        "message": "收款员信息更新成功",
                        "data": {"id": payee_id},
                    }
        except Exception as e:
            logger.error(f"更新收款员信息失败: {e}")
            return {"success": False, "error": str(e)}

    def list_warehouse_payees(
        self,
        warehouse_name: Optional[str] = None,
        payee_name: Optional[str] = None,
        is_active: Optional[int] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Dict[str, Any]:
        """查询收款员信息列表"""
        try:
            with get_conn() as conn:
                # 使用 DictCursor 直接返回字典格式
                with conn.cursor(DictCursor) as cur:
                    where_clauses = []
                    params = []

                    if warehouse_name:
                        where_clauses.append("warehouse_name LIKE %s")
                        params.append(f"%{warehouse_name}%")
                    if payee_name:
                        where_clauses.append("payee_name LIKE %s")
                        params.append(f"%{payee_name}%")
                    if is_active is not None:
                        where_clauses.append("is_active = %s")
                        params.append(is_active)

                    where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

                    # 查询总数
                    cur.execute(
                        f"SELECT COUNT(*) as total FROM pd_warehouse_payees {where_clause}",
                        tuple(params),
                    )
                    result = cur.fetchone()
                    total = result['total'] if result else 0

                    # 查询分页数据
                    offset = (page - 1) * page_size
                    cur.execute(
                        f"""
                        SELECT id, warehouse_id, warehouse_name, payee_name, payee_account, 
                               payee_bank_name, is_active, created_at, updated_at
                        FROM pd_warehouse_payees
                        {where_clause}
                        ORDER BY id DESC
                        LIMIT %s OFFSET %s
                        """,
                        tuple(params + [page_size, offset]),
                    )

                    # DictCursor 直接返回字典列表
                    data = cur.fetchall()

                    return {
                        "success": True,
                        "data": data,
                        "total": total,
                        "page": page,
                        "page_size": page_size,
                    }
        except Exception as e:
            logger.error(f"查询收款员信息失败: {e}")
            return {"success": False, "error": str(e), "data": [], "total": 0}

    def create_customer(self, data: Dict) -> Dict[str, Any]:
        """创建客户"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # 检查客户名称是否已存在
                    cur.execute(
                        "SELECT id FROM pd_customers WHERE smelter_name = %s",
                        (data.get("smelter_name"),)
                    )
                    if cur.fetchone():
                        return {"success": False, "error": f"客户 '{data['smelter_name']}' 已存在"}

                    # 检查信用代码是否已存在（如果提供了）
                    if data.get("credit_code"):
                        cur.execute(
                            "SELECT id FROM pd_customers WHERE credit_code = %s",
                            (data["credit_code"],)
                        )
                        if cur.fetchone():
                            return {"success": False, "error": f"统一社会信用代码 '{data['credit_code']}' 已被使用"}

                    cur.execute("""
                        INSERT INTO pd_customers 
                        (smelter_name, address, contact_person, contact_phone, contact_address, credit_code)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        data.get("smelter_name"),
                        data.get("address"),
                        data.get("contact_person"),
                        data.get("contact_phone"),
                        data.get("contact_address"),
                        data.get("credit_code"),  # 新增
                    ))

                    customer_id = cur.lastrowid

                    return {
                        "success": True,
                        "message": "客户创建成功",
                        "data": {
                            "id": customer_id,
                            "smelter_name": data["smelter_name"],
                        }
                    }

        except Exception as e:
            logger.error(f"创建客户失败: {e}")
            return {"success": False, "error": str(e)}

    def update_customer(self, customer_id: int, data: Dict) -> Dict[str, Any]:
        """更新客户"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # 检查是否存在
                    cur.execute("SELECT id FROM pd_customers WHERE id = %s", (customer_id,))
                    if not cur.fetchone():
                        return {"success": False, "error": f"客户ID {customer_id} 不存在"}

                    # 如果要修改名称，检查新名称是否已被其他客户使用
                    if data.get("smelter_name"):
                        cur.execute(
                            "SELECT id FROM pd_customers WHERE smelter_name = %s AND id != %s",
                            (data["smelter_name"], customer_id)
                        )
                        if cur.fetchone():
                            return {"success": False, "error": f"客户名称 '{data['smelter_name']}' 已被其他客户使用"}

                    # 如果要修改信用代码，检查是否已被其他客户使用
                    if data.get("credit_code"):
                        cur.execute(
                            "SELECT id FROM pd_customers WHERE credit_code = %s AND id != %s",
                            (data["credit_code"], customer_id)
                        )
                        if cur.fetchone():
                            return {"success": False,
                                    "error": f"统一社会信用代码 '{data['credit_code']}' 已被其他客户使用"}

                    # 构建更新SQL
                    update_fields = []
                    params = []
                    fields = ["smelter_name", "address", "contact_person", "contact_phone", "contact_address",
                              "credit_code"]  # 新增 credit_code

                    for field in fields:
                        if field in data:
                            update_fields.append(f"{field} = %s")
                            params.append(data[field])

                    if not update_fields:
                        return {"success": False, "error": "没有要更新的字段"}

                    params.append(customer_id)
                    sql = f"UPDATE pd_customers SET {', '.join(update_fields)} WHERE id = %s"
                    cur.execute(sql, tuple(params))

                    return {
                        "success": True,
                        "message": "客户更新成功",
                        "data": {"id": customer_id}
                    }

        except Exception as e:
            logger.error(f"更新客户失败: {e}")
            return {"success": False, "error": str(e)}

    def get_customer(self, customer_id: int) -> Optional[Dict]:
        """获取客户详情"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM pd_customers WHERE id = %s", (customer_id,))
                    row = cur.fetchone()
                    if not row:
                        return None

                    columns = [desc[0] for desc in cur.description]
                    return dict(zip(columns, row))

        except Exception as e:
            logger.error(f"查询客户失败: {e}")
            return None

    def get_customer_by_name(self, smelter_name: str) -> Optional[Dict]:
        """根据名称获取客户"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT * FROM pd_customers WHERE smelter_name = %s",
                        (smelter_name,)
                    )
                    row = cur.fetchone()
                    if not row:
                        return None

                    columns = [desc[0] for desc in cur.description]
                    return dict(zip(columns, row))

        except Exception as e:
            logger.error(f"查询客户失败: {e}")
            return None

    def list_customers(
            self,
            exact_smelter_name: Optional[str] = None,
            exact_contact_person: Optional[str] = None,
            exact_contact_phone: Optional[str] = None,
            exact_credit_code: Optional[str] = None,  # 新增参数
            fuzzy_keywords: Optional[str] = None,
            page: int = 1,
            page_size: int = 20
    ) -> Dict[str, Any]:
        """获取客户列表（支持搜索）"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    where_clauses = []
                    params = []

                    if exact_smelter_name:
                        where_clauses.append("smelter_name = %s")
                        params.append(exact_smelter_name)
                    if exact_contact_person:
                        where_clauses.append("contact_person = %s")
                        params.append(exact_contact_person)
                    if exact_contact_phone:
                        where_clauses.append("contact_phone = %s")
                        params.append(exact_contact_phone)
                    if exact_credit_code:  # 新增
                        where_clauses.append("credit_code = %s")
                        params.append(exact_credit_code)

                    if fuzzy_keywords:
                        tokens = [t for t in fuzzy_keywords.split() if t]
                        or_clauses = []
                        for token in tokens:
                            like = f"%{token}%"
                            or_clauses.append(
                                "(smelter_name LIKE %s OR contact_person LIKE %s OR contact_phone LIKE %s "
                                "OR address LIKE %s OR contact_address LIKE %s OR credit_code LIKE %s)"
                                # 新增 credit_code
                            )
                            params.extend([like, like, like, like, like, like])
                        if or_clauses:
                            where_clauses.append("(" + " OR ".join(or_clauses) + ")")

                    where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

                    # 总数
                    count_sql = f"SELECT COUNT(*) FROM pd_customers {where_clause}"
                    cur.execute(count_sql, tuple(params))
                    total = cur.fetchone()[0]

                    # 分页数据
                    offset = (page - 1) * page_size
                    data_sql = f"""
                        SELECT * FROM pd_customers 
                        {where_clause}
                        ORDER BY created_at DESC
                        LIMIT %s OFFSET %s
                    """
                    cur.execute(data_sql, tuple(params + [page_size, offset]))

                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    data = [dict(zip(columns, row)) for row in rows]

                    return {
                        "success": True,
                        "data": data,
                        "total": total,
                        "page": page,
                        "page_size": page_size
                    }

        except Exception as e:
            logger.error(f"查询客户列表失败: {e}")
            return {"success": False, "error": str(e), "data": [], "total": 0}

    def delete_customer(self, customer_id: int) -> Dict[str, Any]:
        """删除客户"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    try:
                        # 开启事务
                        conn.begin()

                        # 先确认客户存在并锁定记录
                        cur.execute(
                            "SELECT smelter_name FROM pd_customers WHERE id = %s FOR UPDATE",
                            (customer_id,)
                        )
                        result = cur.fetchone()
                        if not result:
                            conn.rollback()
                            return {"success": False, "error": f"客户ID {customer_id} 不存在"}

                        smelter_name = result[0]

                        # 检查是否有关联合同
                        cur.execute(
                            "SELECT COUNT(*) FROM pd_contracts WHERE smelter_company = %s",
                            (smelter_name,)
                        )
                        count = cur.fetchone()[0]
                        if count > 0:
                            conn.rollback()
                            return {"success": False, "error": "该客户已有关联合同，无法删除"}

                        # 执行删除
                        cur.execute("DELETE FROM pd_customers WHERE id = %s", (customer_id,))

                        if cur.rowcount == 0:
                            conn.rollback()
                            return {"success": False, "error": "删除失败"}

                        conn.commit()
                        return {"success": True, "message": "删除成功"}

                    except Exception as e:
                        conn.rollback()
                        raise e

        except Exception as e:
            logger.error(f"删除客户失败: {e}")
            return {"success": False, "error": str(e)}


_customer_service = None


def get_customer_service():
    global _customer_service
    if _customer_service is None:
        _customer_service = CustomerService()
    return _customer_service