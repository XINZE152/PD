# payment_services.py
import re
from typing import Optional, Dict, Any
from enum import IntEnum
from datetime import datetime, date
from decimal import Decimal, ROUND_HALF_UP

from core.database import get_conn
from core.table_access import build_dynamic_select, _quote_identifier
from core.logging import get_logger

logger = get_logger(__name__)


# ========== 枚举定义 ==========

class PaymentStatus(IntEnum):
    """回款状态枚举"""
    UNPAID = 0       # 未回款
    PARTIAL = 1      # 部分回款
    PAID = 2         # 已结清
    OVERPAID = 3     # 超额回款（异常）


class PaymentStage(IntEnum):
    """回款阶段枚举"""
    DEPOSIT = 0      # 定金
    DELIVERY = 1     # 到货款（90%）
    FINAL = 2        # 尾款（10%）


# ========== 工具函数 ==========

def validate_amount(amount: float) -> bool:
    """验证金额格式（必须为正数，最多2位小数）"""
    if amount is None or amount < 0:
        return False
    return bool(re.match(r'^\d+\.?\d{0,2}$', str(amount)))


def calculate_payment_amount(unit_price: Decimal, net_weight: Decimal) -> Decimal:
    """
    计算回款金额
    回款金额 = 回款单价（合同单价）* 净重

    Args:
        unit_price: 合同单价
        net_weight: 净重

    Returns:
        计算后的回款金额（保留2位小数）
    """
    amount = unit_price * net_weight
    return amount.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)


def determine_payment_status(total_amount: Decimal, paid_amount: Decimal) -> PaymentStatus:
    """
    根据已付金额确定回款状态

    Args:
        total_amount: 应回款总额
        paid_amount: 已回款金额

    Returns:
        回款状态
    """
    if paid_amount <= 0:
        return PaymentStatus.UNPAID
    elif paid_amount >= total_amount:
        if paid_amount > total_amount:
            return PaymentStatus.OVERPAID
        return PaymentStatus.PAID
    else:
        return PaymentStatus.PARTIAL


# ========== 收款明细服务 ==========

class PaymentService:
    """
    冶炼厂回款明细服务

    功能：
    1. 根据销售业务数据生成收款明细台账
    2. 支持财务人员录入收款信息
    3. 支持分段收款模式（定金/到货款90%/尾款10%）
    4. 自动计算累计已付金额与未付金额
    5. 上传磅单时自动创建/更新收款明细
    6. 支持付款状态自动和手动更新
    """

    TABLE_NAME = "pd_payment_details"
    RECORD_TABLE = "pd_payment_records"

    @staticmethod
    def resolve_payment_detail_id(
        payment_detail_id: Optional[int] = None,
        weighbill_id: Optional[int] = None,
        delivery_id: Optional[int] = None,
        contract_no: Optional[str] = None,
        vehicle_no: Optional[str] = None,
        product_name: Optional[str] = None,
    ) -> int:
        """根据显式ID或业务字段自动匹配收款明细。"""
        normalized_contract_no = contract_no.strip() if isinstance(contract_no, str) and contract_no.strip() else None
        normalized_vehicle_no = vehicle_no.strip() if isinstance(vehicle_no, str) and vehicle_no.strip() else None
        normalized_product_name = product_name.strip() if isinstance(product_name, str) and product_name.strip() else None

        with get_conn() as conn:
            with conn.cursor() as cur:
                if payment_detail_id:
                    cur.execute(
                        f"SELECT id FROM {PaymentService.TABLE_NAME} WHERE id = %s",
                        (payment_detail_id,),
                    )
                    row = cur.fetchone()
                    if row:
                        return int(row["id"])

                where_clauses = []
                params = []

                if weighbill_id:
                    where_clauses.append("pd.weighbill_id = %s")
                    params.append(weighbill_id)
                if delivery_id:
                    where_clauses.append("COALESCE(pd.delivery_id, pd.sales_order_id, wb.delivery_id) = %s")
                    params.append(delivery_id)
                if normalized_contract_no:
                    where_clauses.append("COALESCE(pd.contract_no, wb.contract_no, d.contract_no) = %s")
                    params.append(normalized_contract_no)
                if normalized_vehicle_no:
                    where_clauses.append("COALESCE(wb.vehicle_no, d.vehicle_no) = %s")
                    params.append(normalized_vehicle_no)
                if normalized_product_name:
                    where_clauses.append("COALESCE(wb.product_name, d.product_name, pd.material_name) = %s")
                    params.append(normalized_product_name)

                if not where_clauses:
                    raise ValueError("缺少收款匹配条件，请至少提供收款明细ID、报单ID、合同编号、车号或品种中的一个")

                query_sql = f"""
                    SELECT
                        pd.id,
                        pd.status,
                        pd.updated_at
                    FROM {PaymentService.TABLE_NAME} pd
                    LEFT JOIN pd_weighbills wb ON wb.id = pd.weighbill_id
                    LEFT JOIN pd_deliveries d ON d.id = COALESCE(pd.delivery_id, pd.sales_order_id, wb.delivery_id)
                    WHERE {' AND '.join(where_clauses)}
                    ORDER BY
                        CASE WHEN pd.status = %s THEN 1 ELSE 0 END,
                        pd.updated_at DESC,
                        pd.id DESC
                    LIMIT 2
                """
                cur.execute(query_sql, tuple(params + [int(PaymentStatus.PAID)]))
                rows = cur.fetchall()

                if not rows:
                    raise ValueError("未找到匹配的收款明细")
                if len(rows) > 1:
                    raise ValueError("匹配到多条收款明细，请补充报单ID、车号或品种后重试")

                return int(rows[0]["id"])

    @staticmethod
    def resolve_weighbill_id_for_payment(
        weighbill_id: Optional[int] = None,
        delivery_id: Optional[int] = None,
        contract_no: Optional[str] = None,
        smelter_name: Optional[str] = None,
        vehicle_no: Optional[str] = None,
        product_name: Optional[str] = None,
    ) -> int:
        """手动补建回款信息时自动匹配唯一磅单。"""
        normalized_contract_no = contract_no.strip() if isinstance(contract_no, str) and contract_no.strip() else None
        normalized_smelter_name = smelter_name.strip() if isinstance(smelter_name, str) and smelter_name.strip() else None
        normalized_vehicle_no = vehicle_no.strip() if isinstance(vehicle_no, str) and vehicle_no.strip() else None
        normalized_product_name = product_name.strip() if isinstance(product_name, str) and product_name.strip() else None

        with get_conn() as conn:
            with conn.cursor() as cur:
                if weighbill_id:
                    cur.execute("SELECT id FROM pd_weighbills WHERE id = %s", (weighbill_id,))
                    row = cur.fetchone()
                    if row:
                        return int(row["id"])

                where_clauses = ["pd.id IS NULL", "d.id IS NOT NULL"]
                params = []

                if delivery_id:
                    where_clauses.append("wb.delivery_id = %s")
                    params.append(delivery_id)
                if normalized_contract_no:
                    where_clauses.append("wb.contract_no = %s")
                    params.append(normalized_contract_no)
                if normalized_smelter_name:
                    where_clauses.append("d.target_factory_name = %s")
                    params.append(normalized_smelter_name)
                if normalized_vehicle_no:
                    where_clauses.append("wb.vehicle_no = %s")
                    params.append(normalized_vehicle_no)
                if normalized_product_name:
                    where_clauses.append("wb.product_name = %s")
                    params.append(normalized_product_name)

                if not (delivery_id or normalized_contract_no or normalized_vehicle_no or normalized_product_name):
                    raise ValueError("缺少磅单匹配条件，请至少提供磅单ID、报单ID、合同编号、车号或品种中的一个")

                query_sql = """
                    SELECT wb.id
                    FROM pd_weighbills wb
                    LEFT JOIN pd_deliveries d ON d.id = wb.delivery_id
                    LEFT JOIN pd_payment_details pd ON pd.weighbill_id = wb.id
                    WHERE {where_sql}
                    ORDER BY
                        CASE WHEN wb.upload_status = '已上传' THEN 0 ELSE 1 END,
                        wb.updated_at DESC,
                        wb.id DESC
                    LIMIT 2
                """.format(where_sql=" AND ".join(where_clauses))
                cur.execute(query_sql, tuple(params))
                rows = cur.fetchall()

                if not rows:
                    raise ValueError("未找到可用于创建回款信息的磅单")
                if len(rows) > 1:
                    raise ValueError("匹配到多条磅单，请补充报单ID、车号或品种后重试")

                return int(rows[0]["id"])

    @staticmethod
    def _get_collection_status_name(
        smelter_name: Optional[str],
        arrival_paid_amount: Optional[float],
        final_paid_amount: Optional[float],
        paid_amount: Optional[float],
        collection_status: Optional[int]
    ) -> str:
        name = smelter_name or ""
        arrival_paid = float(arrival_paid_amount or 0)
        final_paid = float(final_paid_amount or 0)
        paid = float(paid_amount or 0)

        if "金利" in name:
            if final_paid > 0:
                return "已回款"
            if arrival_paid > 0:
                return "已回首笔待回尾款"
            return "待回款"

        if "豫光" in name:
            return "已回款" if paid > 0 else "待回款"

        collection_map = {
            0: "待回款",
            1: "已回首笔待回尾款",
            2: "已回款"
        }
        return collection_map.get(collection_status, "未知")

    @staticmethod
    def ensure_tables_exist():
        """
        确保收款明细表和回款记录表存在
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 检查主表
                cur.execute(f"SHOW TABLES LIKE '{PaymentService.TABLE_NAME}'")
                if not cur.fetchone():
                    raise RuntimeError(f"{PaymentService.TABLE_NAME} 表不存在，请先执行数据库初始化")

                # 检查记录表
                cur.execute(f"SHOW TABLES LIKE '{PaymentService.RECORD_TABLE}'")
                if not cur.fetchone():
                    raise RuntimeError(f"{PaymentService.RECORD_TABLE} 表不存在，请先执行数据库初始化")

    @staticmethod
    def create_or_update_by_weighbill(
        weighbill_id: int,
        delivery_id: int,
        contract_no: str,
        smelter_name: str,
        material_name: Optional[str] = None,
        unit_price: Optional[Decimal] = None,
        net_weight: Optional[Decimal] = None,
        total_amount: Optional[Decimal] = None,
        payee: Optional[str] = None,
        payee_account: Optional[str] = None,
        created_by: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        根据磅单信息创建或更新收款明细

        上传磅单时调用，自动：
        1. 创建/更新收款明细
        2. 根据合同比例计算首笔和尾款金额
        3. 预生成两条回款记录（首笔+尾款），金额为0待后续编辑
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 检查是否已存在该磅单对应的收款明细
                cur.execute(
                    f"SELECT id FROM {PaymentService.TABLE_NAME} WHERE weighbill_id = %s",
                    (weighbill_id,)
                )
                existing = cur.fetchone()

                # 计算总额
                if total_amount is None and unit_price is not None and net_weight is not None:
                    total_amount = calculate_payment_amount(unit_price, net_weight)

                # 获取合同回款比例
                arrival_ratio = Decimal('0.9')  # 默认90%
                final_ratio = Decimal('0.1')  # 默认10%

                try:
                    cur.execute("""
                        SELECT arrival_payment_ratio, final_payment_ratio 
                        FROM pd_contracts 
                        WHERE contract_no = %s
                    """, (contract_no,))
                    contract = cur.fetchone()
                    if contract:
                        arrival_ratio = Decimal(str(contract['arrival_payment_ratio'] or 0.9))
                        final_ratio = Decimal(str(contract['final_payment_ratio'] or 0.1))
                except Exception as e:
                    logger.warning(f"获取合同比例失败，使用默认值: {e}")

                # 计算首笔和尾款金额
                if total_amount:
                    arrival_amount = (total_amount * arrival_ratio).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                    final_amount = (total_amount * final_ratio).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                    # 修正舍入误差
                    if arrival_amount + final_amount != total_amount:
                        final_amount = total_amount - arrival_amount
                else:
                    arrival_amount = final_amount = Decimal('0')

                if existing:
                    # 更新现有记录
                    payment_id = existing['id']
                    update_fields = []
                    params = []

                    if unit_price is not None:
                        update_fields.append("unit_price = %s")
                        params.append(float(unit_price))
                    if net_weight is not None:
                        update_fields.append("net_weight = %s")
                        params.append(float(net_weight))
                    if total_amount is not None:
                        update_fields.append("total_amount = %s")
                        update_fields.append("unpaid_amount = %s")
                        update_fields.append("arrival_payment_amount = %s")
                        update_fields.append("final_payment_amount = %s")
                        params.extend([
                            float(total_amount),
                            float(total_amount),
                            float(arrival_amount),
                            float(final_amount)
                        ])
                    if material_name:
                        update_fields.append("material_name = %s")
                        params.append(material_name)
                    # payee/payee_account 属于打款域，统一由 pd_balance_details 维护，
                    # 这里不再写入 pd_payment_details，避免双写不同步。

                    update_fields.append("updated_at = %s")
                    params.append(datetime.now())
                    params.append(payment_id)

                    if update_fields:
                        update_sql = f"""
                            UPDATE {_quote_identifier(PaymentService.TABLE_NAME)}
                            SET {', '.join(update_fields)}
                            WHERE id = %s
                        """
                        cur.execute(update_sql, tuple(params))

                        # 更新回款记录的首笔/尾款计划金额
                        cur.execute(f"""
                            UPDATE {PaymentService.RECORD_TABLE}
                            SET payment_amount = CASE 
                                WHEN payment_stage = 0 THEN %s  -- 首笔
                                WHEN payment_stage = 2 THEN %s  -- 尾款
                                ELSE payment_amount
                            END
                            WHERE payment_detail_id = %s
                        """, (float(arrival_amount), float(final_amount), payment_id))

                        # 检查并补充缺失的回款记录
                        cur.execute(f"""
                            SELECT payment_stage FROM {PaymentService.RECORD_TABLE}
                            WHERE payment_detail_id = %s
                        """, (payment_id,))
                        existing_stages = {r['payment_stage'] for r in cur.fetchall()}

                        # 补充首笔记录（如缺失）
                        if 0 not in existing_stages and arrival_amount > 0:
                            cur.execute(f"""
                                INSERT INTO {PaymentService.RECORD_TABLE}
                                (payment_detail_id, payment_amount, payment_stage, payment_date, remark, created_at)
                                VALUES (%s, %s, %s, %s, %s, %s)
                            """, (payment_id, float(arrival_amount), 0, date.today(), "预生成-到货款", datetime.now()))

                        # 补充尾款记录（如缺失）
                        if 2 not in existing_stages and final_amount > 0:
                            cur.execute(f"""
                                INSERT INTO {PaymentService.RECORD_TABLE}
                                (payment_detail_id, payment_amount, payment_stage, payment_date, remark, created_at)
                                VALUES (%s, %s, %s, %s, %s, %s)
                            """, (payment_id, 0, 2, date.today(), "预生成-尾款待回款", datetime.now()))

                        conn.commit()
                        logger.info(f"根据磅单更新收款明细: ID={payment_id}, 磅单ID={weighbill_id}")
                else:
                    # 创建新记录
                    data = {
                        "sales_order_id": delivery_id,
                        "delivery_id": delivery_id,
                        "smelter_name": smelter_name,
                        "contract_no": contract_no,
                        "material_name": material_name or "",
                        "unit_price": float(unit_price) if unit_price else 0,
                        "net_weight": float(net_weight) if net_weight else 0,
                        "total_amount": float(total_amount) if total_amount else 0,
                        "arrival_payment_amount": float(arrival_amount),
                        "final_payment_amount": float(final_amount),
                        "paid_amount": 0.00,
                        "arrival_paid_amount": 0.00,
                        "final_paid_amount": 0.00,
                        "unpaid_amount": float(total_amount) if total_amount else 0,
                        "status": int(PaymentStatus.UNPAID),
                        "collection_status": 0,
                        "is_paid": 0,
                        "weighbill_id": weighbill_id,
                        "created_by": created_by,
                        "created_at": datetime.now(),
                        "updated_at": datetime.now()
                    }

                    # 动态获取表结构
                    cur.execute(f"SHOW COLUMNS FROM {PaymentService.TABLE_NAME}")
                    columns = [r["Field"] for r in cur.fetchall()]
                    data = {k: v for k, v in data.items() if k in columns}

                    cols = list(data.keys())
                    vals = list(data.values())
                    cols_sql = ",".join([_quote_identifier(c) for c in cols])
                    placeholders = ",".join(["%s"] * len(vals))

                    sql = f"INSERT INTO {_quote_identifier(PaymentService.TABLE_NAME)} ({cols_sql}) VALUES ({placeholders})"
                    cur.execute(sql, tuple(vals))
                    payment_id = cur.lastrowid

                    # 预生成两条回款记录（金额为0，待后续编辑）
                    # 首笔记录（到货款）
                    if arrival_amount > 0:
                        cur.execute(f"""
                            INSERT INTO {PaymentService.RECORD_TABLE}
                            (payment_detail_id, payment_amount, payment_stage, payment_date, payment_method, remark, created_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (payment_id, 0, 0, date.today(), "", "预生成-到货款待回款", datetime.now()))

                    # 尾款记录
                    if final_amount > 0:
                        cur.execute(f"""
                            INSERT INTO {PaymentService.RECORD_TABLE}
                            (payment_detail_id, payment_amount, payment_stage, payment_date, payment_method, remark, created_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (payment_id, 0, 2, date.today(), "", "预生成-尾款待回款", datetime.now()))

                    conn.commit()
                    logger.info(
                        f"根据磅单创建收款明细: ID={payment_id}, 磅单ID={weighbill_id}, 首笔={arrival_amount}, 尾款={final_amount}")

                # 返回完整的收款明细信息
                return PaymentService.get_payment_detail(payment_id)

    @staticmethod
    def create_payment_detail(
        sales_order_id: int,
        smelter_name: str,
        contract_no: str,
        unit_price: Decimal,
        net_weight: Decimal,
        material_name: Optional[str] = None,
        remark: Optional[str] = None,
        created_by: Optional[int] = None
    ) -> int:
        """
        创建收款明细台账（根据销售业务数据生成）

        Args:
            sales_order_id: 销售订单ID
            smelter_name: 冶炼厂名称
            contract_no: 合同编号
            unit_price: 合同单价
            net_weight: 净重
            material_name: 物料名称（可选）
            remark: 备注（可选）
            created_by: 创建人ID（可选）

        Returns:
            收款明细ID

        Raises:
            ValueError: 参数校验失败
        """
        # 参数校验
        if not sales_order_id or sales_order_id <= 0:
            raise ValueError("销售订单ID无效")

        if not smelter_name:
            raise ValueError("冶炼厂名称不能为空")

        if not contract_no:
            raise ValueError("合同编号不能为空")

        if unit_price is None or unit_price < 0:
            raise ValueError("合同单价无效")

        if net_weight is None or net_weight < 0:
            raise ValueError("净重无效")

        # 计算应回款总额
        total_amount = calculate_payment_amount(unit_price, net_weight)

        with get_conn() as conn:
            with conn.cursor() as cur:
                # 检查是否已存在该销售订单的收款明细
                cur.execute(
                    f"SELECT id FROM {PaymentService.TABLE_NAME} WHERE sales_order_id=%s AND status!=%s",
                    (sales_order_id, int(PaymentStatus.OVERPAID))
                )
                if cur.fetchone():
                    raise ValueError("该销售订单已存在收款明细")

                # 动态获取表结构
                cur.execute(f"SHOW COLUMNS FROM {PaymentService.TABLE_NAME}")
                columns = [r["Field"] for r in cur.fetchall()]

                # 准备插入数据
                data = {
                    "sales_order_id": sales_order_id,
                    "smelter_name": smelter_name,
                    "contract_no": contract_no,
                    "material_name": material_name or "",
                    "unit_price": float(unit_price),
                    "net_weight": float(net_weight),
                    "total_amount": float(total_amount),
                    "paid_amount": 0.00,
                    "unpaid_amount": float(total_amount),
                    "status": int(PaymentStatus.UNPAID),
                    "is_paid": 0,           # 未回款
                    "created_by": created_by,
                    "created_at": datetime.now(),
                    "updated_at": datetime.now()
                }

                if remark and "remark" in columns:
                    data["remark"] = remark

                # 构建插入SQL
                data = {k: v for k, v in data.items() if k in columns}
                cols = list(data.keys())
                vals = list(data.values())

                cols_sql = ",".join([_quote_identifier(c) for c in cols])
                placeholders = ",".join(["%s"] * len(vals))

                sql = f"INSERT INTO {_quote_identifier(PaymentService.TABLE_NAME)} ({cols_sql}) VALUES ({placeholders})"
                cur.execute(sql, tuple(vals))

                payment_id = cur.lastrowid
                conn.commit()

                logger.info(f"创建收款明细成功: ID={payment_id}, 订单={sales_order_id}, 总额={total_amount}")
                return payment_id

    @staticmethod
    def record_payment(
        payment_detail_id: int,
        payment_amount: Decimal,
        payment_stage: PaymentStage = PaymentStage.DELIVERY,
        payment_date: Optional[date] = None,
        payment_method: Optional[str] = None,
        transaction_no: Optional[str] = None,
        remark: Optional[str] = None,
        recorded_by: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        录入回款记录（支持分段收款）
        
        录入后会自动更新 is_paid = 1（已回首笔款）

        Args:
            payment_detail_id: 收款明细ID
            payment_amount: 回款金额
            payment_stage: 回款阶段（定金/到货款/尾款）
            payment_date: 回款日期（默认今天）
            payment_method: 支付方式
            transaction_no: 交易流水号
            remark: 备注
            recorded_by: 录入人ID

        Returns:
            更新后的收款明细信息

        Raises:
            ValueError: 参数校验失败或明细不存在
        """
        # 参数校验
        if not payment_detail_id or payment_detail_id <= 0:
            raise ValueError("收款明细ID无效")

        if payment_amount is None or payment_amount <= 0:
            raise ValueError("回款金额必须大于0")

        payment_date = payment_date or date.today()

        with get_conn() as conn:
            with conn.cursor() as cur:
                # 获取收款明细
                select_sql = build_dynamic_select(
                    cur,
                    PaymentService.TABLE_NAME,
                    where_clause="id=%s",
                    select_fields=["id", "total_amount", "paid_amount", "unpaid_amount", "status", "weighbill_id"]
                )
                cur.execute(select_sql, (payment_detail_id,))
                detail = cur.fetchone()

                if not detail:
                    raise ValueError("收款明细不存在")

                if detail["status"] == PaymentStatus.PAID:
                    raise ValueError("该订单已结清，无法继续录入回款")

                total_amount = Decimal(str(detail["total_amount"]))
                current_paid = Decimal(str(detail["paid_amount"]))
                new_paid = current_paid + payment_amount
                unpaid_amount = total_amount - new_paid

                # 确定新的状态
                new_status = determine_payment_status(total_amount, new_paid)

                # 插入回款记录
                record_data = {
                    "payment_detail_id": payment_detail_id,
                    "payment_amount": float(payment_amount),
                    "payment_stage": int(payment_stage),
                    "payment_date": payment_date,
                    "payment_method": payment_method or "",
                    "transaction_no": transaction_no or "",
                    "remark": remark or "",
                    "recorded_by": recorded_by,
                    "created_at": datetime.now()
                }

                # 动态获取记录表结构
                cur.execute(f"SHOW COLUMNS FROM {PaymentService.RECORD_TABLE}")
                record_columns = [r["Field"] for r in cur.fetchall()]

                # 过滤存在的字段
                record_data = {k: v for k, v in record_data.items() if k in record_columns}

                cols = list(record_data.keys())
                vals = list(record_data.values())
                cols_sql = ",".join([_quote_identifier(c) for c in cols])
                placeholders = ",".join(["%s"] * len(vals))

                record_sql = f"INSERT INTO {_quote_identifier(PaymentService.RECORD_TABLE)} ({cols_sql}) VALUES ({placeholders})"
                cur.execute(record_sql, tuple(vals))

                # 更新收款明细 - 自动更新 is_paid = 1（已回首笔款）
                update_sql = f"""
                    UPDATE {_quote_identifier(PaymentService.TABLE_NAME)}
                    SET paid_amount = %s,
                        unpaid_amount = %s,
                        status = %s,
                        is_paid = 1,
                        updated_at = %s
                    WHERE id = %s
                """
                cur.execute(update_sql, (
                    float(new_paid),
                    float(unpaid_amount),
                    int(new_status),
                    datetime.now(),
                    payment_detail_id
                ))

                conn.commit()

                # 返回结果
                return {
                    "payment_detail_id": payment_detail_id,
                    "total_amount": float(total_amount),
                    "paid_amount": float(new_paid),
                    "unpaid_amount": float(unpaid_amount),
                    "status": int(new_status),
                    "status_name": new_status.name,
                    "current_payment": float(payment_amount),
                    "payment_stage": int(payment_stage),
                    "payment_stage_name": payment_stage.name,
                    "is_paid": 1,  # 已回首笔款
                    "is_paid_out": detail.get("is_paid_out", 0)  # 保持原支付状态
                }

    @staticmethod
    def update_payment_status(
        payment_id: int,
        is_paid: Optional[int] = None,
        is_paid_out: Optional[int] = None,
        updated_by: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        手动更新付款状态（支持人工干预）
        
        Args:
            payment_id: 收款明细ID
            is_paid: 是否回款（0-否, 1-是）
            is_paid_out: 是否支付（0-待打款, 1-已打款）
            updated_by: 更新人ID
            
        Returns:
            更新后的状态信息
            
        Raises:
            ValueError: 收款明细不存在
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 检查收款明细是否存在
                cur.execute(
                    f"SELECT id, weighbill_id, is_paid FROM {PaymentService.TABLE_NAME} WHERE id=%s",
                    (payment_id,)
                )
                existing = cur.fetchone()
                
                if not existing:
                    raise ValueError("收款明细不存在")
                
                # 动态构建更新字段
                update_fields = []
                params = []
                
                if is_paid is not None:
                    update_fields.append("is_paid = %s")
                    params.append(is_paid)
                
                # is_paid_out 属于打款域，不再写 pd_payment_details，改写到 pd_balance_details.payout_status
                if not update_fields and is_paid_out is None:
                    return {
                        "payment_id": payment_id,
                        "is_paid": existing.get("is_paid"),
                        "is_paid_out": None,
                        "message": "无更新内容"
                    }
                
                if update_fields:
                    update_fields.append("updated_at = %s")
                    params.append(datetime.now())
                    params.append(payment_id)

                    update_sql = f"""
                        UPDATE {_quote_identifier(PaymentService.TABLE_NAME)}
                        SET {', '.join(update_fields)}
                        WHERE id = %s
                    """
                    cur.execute(update_sql, tuple(params))

                resolved_is_paid_out = None
                if is_paid_out is not None:
                    weighbill_id = existing.get("weighbill_id")
                    if weighbill_id:
                        cur.execute(
                            "UPDATE pd_balance_details SET payout_status = %s, updated_at = %s WHERE weighbill_id = %s",
                            (is_paid_out, datetime.now(), weighbill_id)
                        )
                        cur.execute(
                            "SELECT payout_status FROM pd_balance_details WHERE weighbill_id = %s LIMIT 1",
                            (weighbill_id,)
                        )
                        b_row = cur.fetchone()
                        if b_row:
                            resolved_is_paid_out = b_row.get("payout_status")
                conn.commit()
                
                logger.info(f"手动更新付款状态: ID={payment_id}, is_paid={is_paid}, is_paid_out={is_paid_out}")
                
                return {
                    "payment_id": payment_id,
                    "is_paid": is_paid if is_paid is not None else existing.get("is_paid"),
                    "is_paid_out": resolved_is_paid_out,
                    "message": "状态更新成功"
                }

    @staticmethod
    def list_payment_details(
            page: int = 1,
            size: int = 20,
            status: Optional[int] = None,
            smelter_name: Optional[str] = None,
            contract_no: Optional[str] = None,
            start_date: Optional[date] = None,
            end_date: Optional[date] = None,
            keyword: Optional[str] = None,
            # 回款列表筛选参数
            collection_status: Optional[int] = None,  # 回款状态筛选：0-待回款, 1-已回首笔待回尾款, 2-已回款
            arrival_paid: Optional[int] = None,        # 是否已回首笔：0-否, 1-是
            final_paid: Optional[int] = None           # 是否已回尾款：0-否, 1-是
    ) -> Dict[str, Any]:
        """
        查询回款信息列表
        
        只返回已上传磅单的数据（有磅单信息才能回款）
        表头包含销售相关的回款字段
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 构建WHERE条件
                where_clauses = ["1=1"]
                params = []

                if status is not None:
                    where_clauses.append("pd.status = %s")
                    params.append(status)

                if smelter_name:
                    where_clauses.append("COALESCE(pd.smelter_name, d.target_factory_name) LIKE %s")
                    params.append(f"%{smelter_name}%")

                if contract_no:
                    where_clauses.append("COALESCE(pd.contract_no, d.contract_no) LIKE %s")
                    params.append(f"%{contract_no}%")

                if start_date:
                    where_clauses.append("DATE(pd.created_at) >= %s")
                    params.append(start_date)

                if end_date:
                    where_clauses.append("DATE(pd.created_at) <= %s")
                    params.append(end_date)

                # 回款状态筛选
                if collection_status is not None:
                    where_clauses.append("pd.collection_status = %s")
                    params.append(collection_status)

                if keyword:
                    where_clauses.append(
                        "(pd.contract_no LIKE %s OR pd.smelter_name LIKE %s OR wb.weigh_ticket_no LIKE %s OR d.driver_name LIKE %s)")
                    keyword_pattern = f"%{keyword}%"
                    params.extend([keyword_pattern, keyword_pattern, keyword_pattern, keyword_pattern])

                where_sql = " AND ".join(where_clauses)

                # 查询总数
                count_sql = f"""
                    SELECT COUNT(*) as total 
                    FROM {PaymentService.TABLE_NAME} pd
                    LEFT JOIN pd_weighbills wb ON wb.id = pd.weighbill_id
                    LEFT JOIN pd_deliveries d ON d.id = COALESCE(pd.delivery_id, wb.delivery_id)
                    WHERE {where_sql}
                """
                cur.execute(count_sql, tuple(params))
                total = cur.fetchone()["total"]

                # 分页查询 - 回款信息列表字段
                offset = (page - 1) * size
                query_sql = f"""
                    SELECT 
                        -- ========== 第一行：基础信息 ==========
                        pd.contract_no as 合同编号,
                        d.report_date as 报单日期,
                        pd.smelter_name as 报送冶炼厂,
                        d.driver_phone as 司机电话,
                        d.driver_name as 司机姓名,
                        COALESCE(wb.vehicle_no, d.vehicle_no) as 车号,
                        COALESCE(wb.product_name, d.product_name, pd.material_name) as 品种,
                        d.has_delivery_order as 是否自带联单,
                        d.upload_status as 是否上传联单,
                        d.shipper as 报单人发货人,
                        
                        -- ========== 第二行：磅单信息 ==========
                        wb.weigh_date as 磅单日期,
                        wb.weigh_ticket_no as 过磅单号,
                        wb.net_weight as 净重,
                        
                        -- ========== 第三行：回款信息（核心） ==========
                        COALESCE(pd.unit_price, wb.unit_price) as 销售单价,
                        pd.arrival_payment_amount as 应回款首笔金额,
                        pd.final_payment_amount as 应回款尾款金额,
                        pd.arrival_paid_amount as 已回款首笔金额,
                        pd.final_paid_amount as 已回款尾款金额,
                        (SELECT MAX(pr.payment_date) FROM pd_payment_records pr WHERE pr.payment_detail_id = pd.id) as 回款日期,
                        
                        -- ========== 第四行：状态 ==========
                        pd.collection_status as 回款状态,
                        CASE 
                            WHEN pd.collection_status = 0 THEN '待回款'
                            WHEN pd.collection_status = 1 THEN '已回首笔待回尾款'
                            WHEN pd.collection_status = 2 THEN '已回尾款'
                            WHEN pd.collection_status IS NULL THEN '未生成回款'
                            ELSE '未知'
                        END as 回款状态显示,
                        
                        -- ========== 其他必要字段 ==========
                        pd.id as payment_detail_id,
                        wb.id as weighbill_id,
                        COALESCE(pd.delivery_id, d.id) as delivery_id,
                        pd.total_amount as 应收总额,
                        pd.paid_amount as 已回款总额,
                        pd.unpaid_amount as 未回款金额,
                        pd.created_at,
                        pd.updated_at
                        
                    FROM {PaymentService.TABLE_NAME} pd
                    LEFT JOIN pd_weighbills wb ON wb.id = pd.weighbill_id
                    LEFT JOIN pd_deliveries d ON d.id = COALESCE(pd.delivery_id, wb.delivery_id)
                    WHERE {where_sql}
                    ORDER BY pd.created_at DESC
                    LIMIT %s OFFSET %s
                """

                cur.execute(query_sql, tuple(params + [size, offset]))
                rows = cur.fetchall()

                # 处理数据
                items = []
                for row in rows:
                    item = dict(row)
                    
                    # 转换时间字段为字符串
                    time_fields = ['报单日期', '磅单日期', '回款日期', 'created_at', 'updated_at']
                    for field in time_fields:
                        if item.get(field):
                            item[field] = str(item[field])
                    
                    # 格式化金额（保留2位小数）
                    amount_fields = ['净重', '销售单价', '应回款首笔金额', '应回款尾款金额', 
                                   '已回款首笔金额', '已回款尾款金额', '应收总额', '已回款总额', '未回款金额']
                    for field in amount_fields:
                        if item.get(field) is not None:
                            item[field] = round(float(item[field]), 2)
                    
                    items.append(item)

                return {
                    "total": total,
                    "page": page,
                    "size": size,
                    "items": items,
                    "summary": {
                        "待回款笔数": sum(1 for i in items if i.get('回款状态') == 0),
                        "已回首笔待回尾款笔数": sum(1 for i in items if i.get('回款状态') == 1),
                        "已回尾款笔数": sum(1 for i in items if i.get('回款状态') == 2),
                        "未生成回款笔数": sum(1 for i in items if i.get('回款状态') is None),
                    }
                }
            
    @staticmethod
    def list_payment_out_details(
            page: int = 1,
            size: int = 20,
            status: Optional[int] = None,
            smelter_name: Optional[str] = None,
            contract_no: Optional[str] = None,
            start_date: Optional[date] = None,
            end_date: Optional[date] = None,
            keyword: Optional[str] = None,
            # 打款列表筛选参数
            is_paid_out: Optional[int] = None,           # 打款状态：0-待打款, 1-已打款
            payment_schedule_date: Optional[str] = None,  # 排期日期
            has_schedule: Optional[int] = None           # 是否已排期：0-待排期, 1-已排期
    ) -> Dict[str, Any]:
        """
        查询打款信息列表（打款排期列表）
        
        只返回已排期的数据（有排期日期才能打款）
        表头包含采购相关的打款字段
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SHOW COLUMNS FROM {PaymentService.TABLE_NAME}")
                columns = [r["Field"] for r in cur.fetchall()]
                has_payee = "payee" in columns
                has_payee_account = "payee_account" in columns

                # 构建WHERE条件 - 必须已排期
                where_clauses = ["wb.payment_schedule_date IS NOT NULL"]  # 必须已排期
                params = []

                if status is not None:
                    where_clauses.append("pd.status = %s")
                    params.append(status)

                if smelter_name:
                    where_clauses.append("pd.smelter_name LIKE %s")
                    params.append(f"%{smelter_name}%")

                if contract_no:
                    where_clauses.append("pd.contract_no LIKE %s")
                    params.append(f"%{contract_no}%")

                if start_date:
                    where_clauses.append("DATE(pd.created_at) >= %s")
                    params.append(start_date)

                if end_date:
                    where_clauses.append("DATE(pd.created_at) <= %s")
                    params.append(end_date)

                # 打款状态筛选
                if is_paid_out is not None:
                    where_clauses.append("COALESCE(b.payout_status, 0) = %s")
                    params.append(is_paid_out)

                # 排期日期筛选
                if payment_schedule_date:
                    where_clauses.append("wb.payment_schedule_date = %s")
                    params.append(payment_schedule_date)

                # 是否已排期筛选
                if has_schedule is not None:
                    if has_schedule == 1:
                        where_clauses.append("wb.payment_schedule_date IS NOT NULL")
                    else:
                        where_clauses.append("wb.payment_schedule_date IS NULL")

                if keyword:
                    payee_filter = "COALESCE(b.payee_name, pd.payee, d.payee)" if has_payee else "COALESCE(b.payee_name, d.payee)"
                    where_clauses.append(
                        f"(pd.contract_no LIKE %s OR pd.smelter_name LIKE %s OR wb.weigh_ticket_no LIKE %s OR d.driver_name LIKE %s OR {payee_filter} LIKE %s)")
                    keyword_pattern = f"%{keyword}%"
                    params.extend([keyword_pattern, keyword_pattern, keyword_pattern, keyword_pattern, keyword_pattern])

                where_sql = " AND ".join(where_clauses)

                # 查询总数
                count_sql = f"""
                    SELECT COUNT(*) as total 
                    FROM {PaymentService.TABLE_NAME} pd
                    LEFT JOIN pd_deliveries d ON d.id = pd.delivery_id
                    LEFT JOIN pd_weighbills wb ON wb.id = pd.weighbill_id
                    LEFT JOIN pd_balance_details b ON b.weighbill_id = wb.id
                    WHERE {where_sql}
                """
                cur.execute(count_sql, tuple(params))
                total = cur.fetchone()["total"]

                # 分页查询 - 打款信息列表字段
                offset = (page - 1) * size
                payee_select = "COALESCE(b.payee_name, pd.payee, d.payee)" if has_payee else "COALESCE(b.payee_name, d.payee)"
                payee_account_select = "COALESCE(b.payee_account, pd.payee_account)" if has_payee_account else "b.payee_account"
                is_paid_out_select = "COALESCE(b.payout_status, 0)"
                query_sql = f"""
                    SELECT 
                        -- ========== 第一行：排期信息 ==========
                        wb.payment_schedule_date as 排款日期,
                        
                        -- ========== 第二行：基础信息 ==========
                        pd.contract_no as 合同编号,
                        d.report_date as 报单日期,
                        d.target_factory_name as 报送冶炼厂,
                        d.driver_phone as 司机电话,
                        d.driver_name as 司机姓名,
                        wb.vehicle_no as 车号,
                        wb.product_name as 品种,
                        d.has_delivery_order as 是否自带联单,
                        d.upload_status as 是否上传联单,
                        d.shipper as 报单人发货人,
                        
                        -- ========== 第三行：磅单信息 ==========
                        wb.weigh_date as 磅单日期,
                        wb.weigh_ticket_no as 过磅单号,
                        wb.net_weight as 净重,
                        
                        -- ========== 第四行：打款信息（核心） ==========
                        wb.unit_price as 采购单价,
                        GREATEST(
                            COALESCE(b.payable_amount, wb.total_amount, pd.total_amount, 0) -
                            CASE
                                WHEN d.has_delivery_order = '无' THEN COALESCE(d.service_fee, 150)
                                ELSE COALESCE(d.service_fee, 0)
                            END,
                            0
                        ) as 应打款金额,
                        COALESCE(b.paid_amount, pd.paid_amount, 0) as 已打款金额,
                        {payee_select} as 收款人,
                        {payee_account_select} as 收款人账号,
                        CASE
                            WHEN d.has_delivery_order = '无' THEN COALESCE(d.service_fee, 150)
                            ELSE COALESCE(d.service_fee, 0)
                        END as 联单费,
                        
                        -- ========== 第五行：回款信息（辅助） ==========
                        pd.arrival_payment_amount as 应回款首笔金额,
                        pd.final_payment_amount as 应回款尾款金额,
                        pd.arrival_paid_amount as 已回款首笔金额,
                        pd.final_paid_amount as 已回款尾款金额,
                        (SELECT MAX(pr.payment_date) FROM pd_payment_records pr WHERE pr.payment_detail_id = pd.id) as 回款日期,
                        pd.collection_status as 回款状态,
                        
                        -- ========== 第六行：打款状态 ==========
                        b.payout_date as 打款日期,
                        {is_paid_out_select} as 打款状态,
                        CASE 
                            WHEN {is_paid_out_select} = 1 THEN '已打款'
                            ELSE '待打款'
                        END as 打款状态显示,
                        
                        -- ========== 排期状态 ==========
                        CASE 
                            WHEN wb.payment_schedule_date IS NOT NULL THEN '已排期'
                            ELSE '待排期'
                        END as 排期状态,
                        
                        -- ========== 其他必要字段 ==========
                        pd.id as payment_detail_id,
                        b.id as balance_id,
                        wb.id as weighbill_id,
                        d.id as delivery_id,
                        COALESCE(b.balance_amount, pd.unpaid_amount, 0) as 未打款金额,
                        pd.created_at,
                        pd.updated_at,

                        wb.gross_weight,
                        wb.tare_weight,
                        wb.weighbill_image,
                        wb.ocr_status,
                        wb.is_manual_corrected,
                        wb.uploader_id as weighbill_uploader_id,
                        wb.uploader_name as weighbill_uploader_name

                    FROM {PaymentService.TABLE_NAME} pd
                    LEFT JOIN pd_deliveries d ON d.id = pd.delivery_id
                    LEFT JOIN pd_weighbills wb ON wb.id = pd.weighbill_id
                    LEFT JOIN pd_balance_details b ON b.weighbill_id = wb.id
                    WHERE {where_sql}
                    ORDER BY pd.created_at DESC
                    LIMIT %s OFFSET %s
                """

                cur.execute(query_sql, tuple(params + [size, offset]))
                rows = cur.fetchall()

                # 处理数据
                items = []
                for row in rows:
                    item = dict(row)
                    
                    # 转换时间字段为字符串
                    time_fields = ['排款日期', '打款日期', '报单日期', '磅单日期', '回款日期', 'created_at', 'updated_at']
                    for field in time_fields:
                        if item.get(field):
                            item[field] = str(item[field])
                    
                    # 格式化金额（保留2位小数）
                    amount_fields = ['净重', '采购单价', '应打款金额', '已打款金额', '未打款金额', '联单费',
                                   '应回款首笔金额', '应回款尾款金额', '已回款首笔金额', '已回款尾款金额']
                    for field in amount_fields:
                        if item.get(field) is not None:
                            item[field] = round(float(item[field]), 2)
                    
                    items.append(item)

                return {
                    "success": True,
                    "data": items,
                    "total": total,
                    "page": page,
                    "size": size,
                    "items": items,
                    "summary": {
                        "待打款笔数": sum(1 for i in items if i.get('打款状态') == 0),
                        "已打款笔数": sum(1 for i in items if i.get('打款状态') == 1),
                        "已排期笔数": sum(1 for i in items if i.get('排期状态') == '已排期'),
                        "待排期笔数": sum(1 for i in items if i.get('排期状态') == '待排期'),
                    }
                }

    @staticmethod
    def update_collection_payment(
            payment_id: int,
            arrival_paid_amount: Optional[float] = None,
            final_paid_amount: Optional[float] = None,
            arrival_payment_date: Optional[str] = None,
            final_payment_date: Optional[str] = None,
            payment_date: Optional[str] = None,
            remark: Optional[str] = None,
            updated_by: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        编辑回款金额

        逻辑：
        1. 更新 arrival_paid_amount、final_paid_amount
        2. 自动计算 paid_amount = arrival_paid_amount + final_paid_amount
        3. 自动计算 unpaid_amount = total_amount - paid_amount
        4. 自动判断 collection_status
        5. 同步更新 pd_payment_records 中的对应记录金额
        6. 更新 last_payment_date
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 获取当前收款明细
                cur.execute(f"""
                    SELECT id,
                        total_amount,
                        arrival_payment_amount,
                        final_payment_amount,
                        arrival_paid_amount,
                        final_paid_amount,
                        smelter_name,
                        contract_no,
                        delivery_id,
                        weighbill_id
                    FROM pd_payment_details
                    WHERE id = %s
                """, (payment_id,))

                detail = cur.fetchone()
                if not detail:
                    raise ValueError("收款明细不存在")

                total_amount = Decimal(str(detail["total_amount"]))
                smelter_name = detail["smelter_name"] or ""
                is_jinli = "金利" in smelter_name

                # 新值（如果没传则保持原值）
                cur_arrival = Decimal(str(detail.get("arrival_paid_amount") or 0))
                cur_final = Decimal(str(detail.get("final_paid_amount") or 0))

                new_arrival = Decimal(str(arrival_paid_amount)) if arrival_paid_amount is not None else cur_arrival
                new_final = Decimal(str(final_paid_amount)) if final_paid_amount is not None else cur_final

                # 豫光：尾款强制为0
                if not is_jinli:
                    new_final = Decimal('0')

                # 自动计算
                new_paid = new_arrival + new_final
                new_unpaid = total_amount - new_paid

                # 确定回款状态
                if is_jinli:
                    if new_final > 0:
                        collection_status = 2
                        status_name = "已回款"
                    elif new_arrival > 0:
                        collection_status = 1
                        status_name = "已回首笔待回款尾款"
                    else:
                        collection_status = 0
                        status_name = "待回款"
                else:  # 豫光或其他
                    collection_status = 2 if new_arrival > 0 else 0
                    status_name = "已回款" if new_arrival > 0 else "待回款"

                if is_jinli:

                    arrival_date = arrival_payment_date or payment_date

                    # 首笔日期必须填写
                    if new_arrival > 0 and not arrival_date:
                        raise ValueError("金利首笔回款必须填写回款日期")

                    # 尾款日期必须填写
                    if new_final > 0 and not final_payment_date:
                        raise ValueError("金利尾款回款必须填写回款日期")

                    final_date = final_payment_date
                    # 金利：尾款日期
                    if final_payment_date:
                        final_date = final_payment_date
                    elif payment_date and new_final > cur_final:
                        final_date = payment_date
                    else:
                        final_date = datetime.now().strftime('%Y-%m-%d') if new_final > 0 else None
                else:
                    # 豫光：只有一个日期
                    single_date = arrival_payment_date or payment_date

                    if new_arrival > 0 and not single_date:
                        raise ValueError("回款必须填写日期")
                    if not single_date and new_arrival > cur_arrival:
                        single_date = datetime.now().strftime('%Y-%m-%d')
                    arrival_date = single_date
                    final_date = None  # 豫光没有尾款日期

                # 确定payment_detail总状态
                if new_paid >= total_amount:
                    payment_status = PaymentStatus.PAID
                elif new_paid > 0:
                    payment_status = PaymentStatus.PARTIAL
                else:
                    payment_status = PaymentStatus.UNPAID

                # 更新收款明细
                update_fields = [
                    "arrival_paid_amount = %s",
                    "final_paid_amount = %s", 
                    "paid_amount = %s",
                    "unpaid_amount = %s",
                    "collection_status = %s",
                    "status = %s",
                    "is_paid = CASE WHEN %s > 0 THEN 1 ELSE 0 END",
                    "updated_at = %s"
                ]
                params = [
                    float(new_arrival),
                    float(new_final),
                    float(new_paid),
                    float(new_unpaid),
                    collection_status,
                    int(payment_status),
                    float(new_paid),
                    datetime.now()
                ]
                
                # 检查并更新日期字段
                cur.execute(f"SHOW COLUMNS FROM {PaymentService.TABLE_NAME} LIKE 'arrival_payment_date'")
                has_arrival_date_col = cur.fetchone() is not None
                cur.execute(f"SHOW COLUMNS FROM {PaymentService.TABLE_NAME} LIKE 'final_payment_date'")
                has_final_date_col = cur.fetchone() is not None
                
                if has_arrival_date_col and arrival_date:
                    update_fields.append("arrival_payment_date = %s")
                    params.append(arrival_date)
                if has_final_date_col and final_date:
                    update_fields.append("final_payment_date = %s")
                    params.append(final_date)
                    
                params.append(payment_id)
                
                # ✅ 构建并执行 UPDATE SQL
                update_sql = f"""
                    UPDATE {_quote_identifier(PaymentService.TABLE_NAME)}
                    SET {', '.join(update_fields)}
                    WHERE id = %s
                """
                cur.execute(update_sql, tuple(params))
                
                # 同步更新回款记录
                # 更新首笔记录（阶段0）
                if arrival_paid_amount is not None or (is_jinli and arrival_payment_date) or (not is_jinli and payment_date):
                    cur.execute(f"""
                        SELECT id, payment_date FROM {PaymentService.RECORD_TABLE}
                        WHERE payment_detail_id = %s AND payment_stage = 0
                    """, (payment_id,))
                    arrival_record = cur.fetchone()
                    
                    record_date = arrival_date or datetime.now().strftime('%Y-%m-%d')
                    
                    if arrival_record:
                        # 更新现有记录
                        cur.execute(f"""
                            UPDATE {_quote_identifier(PaymentService.RECORD_TABLE)}
                            SET payment_amount = %s,
                                payment_date = %s,
                                remark = COALESCE(%s, remark),
                                recorded_by = COALESCE(%s, recorded_by),
                                updated_at = %s
                            WHERE id = %s
                        """, (
                            float(new_arrival),
                            record_date,
                            remark or ("到货款回款" if is_jinli else "回款录入"),
                            updated_by,
                            datetime.now(),
                            arrival_record['id']
                        ))
                    elif new_arrival > 0:
                        # 创建新记录
                        cur.execute(f"""
                            INSERT INTO {_quote_identifier(PaymentService.RECORD_TABLE)}
                            (payment_detail_id, payment_amount, payment_stage, payment_date, 
                             payment_method, remark, recorded_by, created_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            payment_id, 
                            float(new_arrival), 
                            0,
                            record_date,
                            "银行转账",
                            remark or ("到货款回款" if is_jinli else "回款录入"),
                            updated_by,
                            datetime.now()
                        ))

                # 更新尾款记录（阶段2）- 仅金利有
                if is_jinli and (final_paid_amount is not None or final_payment_date):
                    cur.execute(f"""
                        SELECT id FROM {PaymentService.RECORD_TABLE}
                        WHERE payment_detail_id = %s AND payment_stage = 2
                    """, (payment_id,))
                    final_record = cur.fetchone()
                    
                    record_date = final_date or datetime.now().strftime('%Y-%m-%d')
                    
                    if final_record:
                        cur.execute(f"""
                            UPDATE {_quote_identifier(PaymentService.RECORD_TABLE)}
                            SET payment_amount = %s,
                                payment_date = %s,
                                remark = COALESCE(%s, remark),
                                recorded_by = COALESCE(%s, recorded_by),
                                updated_at = %s
                            WHERE id = %s
                        """, (
                            float(new_final),
                            record_date,
                            remark or "尾款回款",
                            updated_by,
                            datetime.now(),
                            final_record['id']
                        ))
                    elif new_final > 0:
                        # 创建新记录
                        cur.execute(f"""
                            INSERT INTO {_quote_identifier(PaymentService.RECORD_TABLE)}
                            (payment_detail_id, payment_amount, payment_stage, payment_date,
                             payment_method, remark, recorded_by, created_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            payment_id,
                            float(new_final),
                            2,
                            record_date,
                            "银行转账",
                            remark or "尾款回款",
                            updated_by,
                            datetime.now()
                        ))

                conn.commit()

                return {
                    "payment_id": payment_id,
                    "is_jinli": is_jinli,
                    "arrival_paid_amount": float(new_arrival),
                    "final_paid_amount": float(new_final),
                    "paid_amount": float(new_paid),
                    "unpaid_amount": float(new_unpaid),
                    "collection_status": collection_status,
                    "collection_status_name": status_name,
                    "payment_status": int(payment_status),
                    "arrival_payment_date": arrival_date,
                    "final_payment_date": final_date if is_jinli else None,
                    "last_payment_date": final_date or arrival_date,
                    "message": "回款更新成功"
                }
    @staticmethod
    def get_payment_detail(payment_id: int) -> Optional[Dict[str, Any]]:
        """
        获取收款明细详情（包含回款记录）
        
        Args:
            payment_id: 收款明细ID
            
        Returns:
            收款明细详情，包含回款记录列表
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 查询收款明细主表及关联信息
                query_sql = f"""
                    SELECT 
                        pd.*,
                        wb.id as weighbill_id,
                        wb.weigh_date,
                        wb.delivery_time,
                        wb.weigh_ticket_no,
                        wb.vehicle_no as weighbill_vehicle_no,
                        wb.product_name as weighbill_product_name,
                        wb.gross_weight,
                        wb.tare_weight,
                        wb.net_weight as weighbill_net_weight,
                        wb.unit_price as weighbill_unit_price,
                        wb.total_amount as weighbill_total_amount,
                        wb.weighbill_image,
                        wb.ocr_status,
                        wb.is_manual_corrected,
                        wb.payment_schedule_date,
                        wb.uploader_id as weighbill_uploader_id,
                        wb.uploader_name as weighbill_uploader_name,
                        wb.uploaded_at as weighbill_uploaded_at,
                        d.id as delivery_id,
                        d.report_date,
                        d.warehouse,
                        d.target_factory_id,
                        d.target_factory_name,
                        d.quantity as delivery_quantity,
                        d.vehicle_no as delivery_vehicle_no,
                        d.driver_name,
                        d.driver_phone,
                        d.driver_id_card,
                        d.has_delivery_order,
                        d.delivery_order_image,
                        d.upload_status as delivery_upload_status,
                        d.source_type,
                        d.shipper,
                        d.service_fee,
                        d.contract_no as delivery_contract_no,
                        d.contract_unit_price as delivery_contract_unit_price,
                        d.total_amount as delivery_total_amount,
                        d.status as delivery_status,
                        d.uploader_id as delivery_uploader_id,
                        d.uploader_name as delivery_uploader_name,
                        d.uploaded_at as delivery_uploaded_at
                    FROM {PaymentService.TABLE_NAME} pd
                    LEFT JOIN pd_deliveries d ON d.id = COALESCE(pd.delivery_id, pd.sales_order_id)
                    LEFT JOIN pd_weighbills wb ON wb.delivery_id = d.id OR wb.id = pd.weighbill_id
                    WHERE pd.id = %s
                    LIMIT 1
                """
                cur.execute(query_sql, (payment_id,))
                detail = cur.fetchone()
                
                if not detail:
                    return None
                
                detail = dict(detail)
                
                # 添加状态名称
                detail['status_name'] = PaymentStatus(detail['status']).name if detail.get('status') is not None else None
                
                # 转换时间字段
                time_fields = [
                    'created_at', 'updated_at', 'weigh_date', 'delivery_time',
                    'weighbill_uploaded_at', 'report_date', 'delivery_uploaded_at',
                    'payment_schedule_date'
                ]
                for field in time_fields:
                    if detail.get(field):
                        detail[field] = str(detail[field])
                
                # 计算联单费
                has_delivery_order = detail.get('has_delivery_order')
                if has_delivery_order == '无' or has_delivery_order == '否':
                    detail['delivery_fee'] = 150.0
                else:
                    detail['delivery_fee'] = float(detail.get('service_fee') or 0)

                detail['collection_status_name'] = PaymentService._get_collection_status_name(
                    detail.get('smelter_name'),
                    detail.get('arrival_paid_amount'),
                    detail.get('final_paid_amount'),
                    detail.get('paid_amount'),
                    detail.get('collection_status')
                )

                # 确保布尔状态字段有默认值
                if detail.get('is_paid') is None:
                    detail['is_paid'] = 1 if (detail.get('paid_amount') or 0) > 0 else 0
                if detail.get('is_paid_out') is None:
                    detail['is_paid_out'] = 0

                # 查询回款记录
                records_sql = f"""
                    SELECT 
                        id,
                        payment_amount,
                        payment_stage,
                        payment_date,
                        payment_method,
                        transaction_no,
                        remark,
                        created_at
                    FROM {PaymentService.RECORD_TABLE}
                    WHERE payment_detail_id = %s
                    ORDER BY payment_date DESC, created_at DESC
                """
                cur.execute(records_sql, (payment_id,))
                records = cur.fetchall()
                
                payment_records = []
                for record in records:
                    rec = dict(record)
                    rec['payment_stage_name'] = PaymentStage(rec['payment_stage']).name if rec.get('payment_stage') is not None else None
                    rec['payment_date'] = str(rec['payment_date']) if rec.get('payment_date') else None
                    rec['created_at'] = str(rec['created_at']) if rec.get('created_at') else None
                    payment_records.append(rec)
                
                detail['payment_records'] = payment_records
                detail['payment_count'] = len(payment_records)
                
                return detail

    @staticmethod
    def update_payment_detail(
        payment_id: int,
        smelter_name: Optional[str] = None,
        contract_no: Optional[str] = None,
        material_name: Optional[str] = None,
        remark: Optional[str] = None,
        updated_by: Optional[int] = None
    ) -> bool:
        """
        更新收款明细基础信息
        
        Args:
            payment_id: 收款明细ID
            smelter_name: 冶炼厂名称
            contract_no: 合同编号
            material_name: 物料名称
            remark: 备注
            updated_by: 更新人ID
            
        Returns:
            是否更新成功
            
        Raises:
            ValueError: 收款明细不存在
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 检查收款明细是否存在
                cur.execute(
                    f"SELECT id, status FROM {PaymentService.TABLE_NAME} WHERE id=%s",
                    (payment_id,)
                )
                existing = cur.fetchone()
                
                if not existing:
                    raise ValueError("收款明细不存在")
                
                # 如果已结清，限制修改
                if existing['status'] == PaymentStatus.PAID:
                    # 只允许修改备注
                    if smelter_name or contract_no or material_name:
                        raise ValueError("已结清的收款明细只允许修改备注")
                
                # 动态构建更新字段
                update_fields = []
                params = []
                
                if smelter_name is not None:
                    update_fields.append("smelter_name = %s")
                    params.append(smelter_name)
                
                if contract_no is not None:
                    update_fields.append("contract_no = %s")
                    params.append(contract_no)
                
                if material_name is not None:
                    update_fields.append("material_name = %s")
                    params.append(material_name)
                
                if remark is not None:
                    update_fields.append("remark = %s")
                    params.append(remark)
                
                if not update_fields:
                    return True  # 没有需要更新的字段
                
                update_fields.append("updated_at = %s")
                params.append(datetime.now())
                
                params.append(payment_id)
                
                update_sql = f"""
                    UPDATE {_quote_identifier(PaymentService.TABLE_NAME)}
                    SET {', '.join(update_fields)}
                    WHERE id = %s
                """
                
                cur.execute(update_sql, tuple(params))
                conn.commit()
                
                logger.info(f"更新收款明细成功: ID={payment_id}")
                return True

    @staticmethod
    def delete_payment_detail(payment_id: int) -> bool:
        """
        删除收款明细
        
        Args:
            payment_id: 收款明细ID
            
        Returns:
            是否删除成功
            
        Raises:
            ValueError: 收款明细不存在或已有回款记录无法删除
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 检查收款明细是否存在
                cur.execute(
                    f"SELECT id, paid_amount, status FROM {PaymentService.TABLE_NAME} WHERE id=%s",
                    (payment_id,)
                )
                existing = cur.fetchone()
                
                if not existing:
                    raise ValueError("收款明细不存在")
                
                # 检查是否有回款记录
                if existing['paid_amount'] > 0 or existing['status'] != PaymentStatus.UNPAID:
                    raise ValueError("已有回款记录的明细无法删除，请先删除回款记录")
                
                # 检查是否存在关联的回款记录表记录
                cur.execute(
                    f"SELECT COUNT(*) as count FROM {PaymentService.RECORD_TABLE} WHERE payment_detail_id=%s",
                    (payment_id,)
                )
                record_count = cur.fetchone()['count']
                
                if record_count > 0:
                    raise ValueError(f"存在{record_count}条回款记录，无法删除收款明细")
                
                # 执行删除
                delete_sql = f"DELETE FROM {_quote_identifier(PaymentService.TABLE_NAME)} WHERE id = %s"
                cur.execute(delete_sql, (payment_id,))
                conn.commit()
                
                logger.info(f"删除收款明细成功: ID={payment_id}")
                return True

    @staticmethod
    def get_contract_shipping_progress(
        contract_no: Optional[str] = None,
        smelter_name: Optional[str] = None,
        page: int = 1,
        size: int = 20
    ) -> Dict[str, Any]:
        """
        获取合同发运进度列表
        统计每个合同的车数、吨数、已运/剩余情况
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 构建WHERE条件
                where_clauses = ["1=1"]
                params = []
                
                if contract_no:
                    where_clauses.append("c.contract_no LIKE %s")
                    params.append(f"%{contract_no}%")
                
                if smelter_name:
                    where_clauses.append("c.smelter_company LIKE %s")
                    params.append(f"%{smelter_name}%")
                
                where_sql = " AND ".join(where_clauses)
                
                # 查询总数
                count_sql = f"""
                    SELECT COUNT(*) as total 
                    FROM pd_contracts c
                    WHERE {where_sql}
                """
                cur.execute(count_sql, tuple(params))
                total = cur.fetchone()["total"]
                
                # 查询合同发运进度
                offset = (page - 1) * size
                query_sql = f"""
                    SELECT 
                        c.contract_no,
                        c.smelter_company as smelter_name,
                        (
                            SELECT COALESCE(SUM(d.quantity), 0)
                            FROM pd_deliveries d
                            WHERE d.contract_no = c.contract_no
                        ) as planned_total_weight,
                        (
                            SELECT COUNT(*)
                            FROM pd_weighbills wb
                            WHERE wb.contract_no = c.contract_no
                              AND wb.ocr_status IN ('已上传磅单', '已确认')
                        ) as shipped_vehicles,
                        (
                            SELECT COALESCE(SUM(wb.net_weight), 0)
                            FROM pd_weighbills wb
                            WHERE wb.contract_no = c.contract_no
                              AND wb.ocr_status IN ('已上传磅单', '已确认')
                        ) as shipped_weight,
                        (
                            SELECT MAX(wb.weigh_date)
                            FROM pd_weighbills wb
                            WHERE wb.contract_no = c.contract_no
                              AND wb.ocr_status IN ('已上传磅单', '已确认')
                        ) as last_ship_date
                    FROM pd_contracts c
                    WHERE {where_sql}
                    ORDER BY c.created_at DESC
                    LIMIT %s OFFSET %s
                """
                
                cur.execute(query_sql, tuple(params + [size, offset]))
                rows = cur.fetchall()
                
                items = []
                for row in rows:
                    item = dict(row)
                    planned_weight = float(item.get('planned_total_weight') or 0)
                    shipped_weight = float(item.get('shipped_weight') or 0)
                    shipped_vehicles = int(item.get('shipped_vehicles') or 0)
                    remaining_weight = planned_weight - shipped_weight
                    
                    # 估算剩余车数
                    if shipped_vehicles > 0 and shipped_weight > 0:
                        avg_weight_per_vehicle = shipped_weight / shipped_vehicles
                        remaining_vehicles = int(remaining_weight / avg_weight_per_vehicle) if avg_weight_per_vehicle > 0 else 0
                    else:
                        remaining_vehicles = int(remaining_weight / 30) if remaining_weight > 0 else 0
                    
                    total_vehicles = shipped_vehicles + remaining_vehicles
                    
                    items.append({
                        "contract_no": item["contract_no"],
                        "smelter_name": item["smelter_name"],
                        "total_vehicles": total_vehicles,
                        "planned_total_weight": round(planned_weight, 2),
                        "shipped_vehicles": shipped_vehicles,
                        "remaining_vehicles": remaining_vehicles,
                        "shipped_weight": round(shipped_weight, 2),
                        "remaining_weight": round(remaining_weight, 2),
                        "last_ship_date": str(item["last_ship_date"]) if item.get("last_ship_date") else None,
                        "progress_rate": round(shipped_weight / planned_weight * 100, 2) if planned_weight > 0 else 0
                    })
                
                return {
                    "total": total,
                    "page": page,
                    "size": size,
                    "items": items
                }

    @staticmethod
    def get_contract_payment_summary(
        contract_no: Optional[str] = None,
        smelter_name: Optional[str] = None,
        status: Optional[int] = None,
        page: int = 1,
        size: int = 20
    ) -> Dict[str, Any]:
        """
        获取合同回款汇总列表（按合同编号分组）
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                where_clauses = ["1=1"]
                params = []
                
                if contract_no:
                    where_clauses.append("pd.contract_no LIKE %s")
                    params.append(f"%{contract_no}%")
                
                if smelter_name:
                    where_clauses.append("pd.smelter_name LIKE %s")
                    params.append(f"%{smelter_name}%")
                
                if status is not None:
                    where_clauses.append("pd.status = %s")
                    params.append(status)
                
                where_sql = " AND ".join(where_clauses)
                
                count_sql = f"""
                    SELECT COUNT(DISTINCT pd.contract_no) as total 
                    FROM {PaymentService.TABLE_NAME} pd
                    WHERE {where_sql}
                """
                cur.execute(count_sql, tuple(params))
                total = cur.fetchone()["total"]
                
                offset = (page - 1) * size
                query_sql = f"""
                    SELECT 
                        pd.contract_no,
                        pd.smelter_name,
                        SUM(pd.total_amount) as total_receivable,
                        SUM(pd.paid_amount) as total_received,
                        SUM(pd.unpaid_amount) as total_unreceived,
                        COUNT(DISTINCT pd.id) as order_count,
                        SUM(CASE WHEN pd.status = 0 THEN 1 ELSE 0 END) as unpaid_count,
                        SUM(CASE WHEN pd.status = 1 THEN 1 ELSE 0 END) as partial_count,
                        SUM(CASE WHEN pd.status = 2 THEN 1 ELSE 0 END) as paid_count,
                        SUM(CASE WHEN pd.status = 3 THEN 1 ELSE 0 END) as overpaid_count,
                        MAX(pr.payment_date) as last_payment_date
                    FROM {PaymentService.TABLE_NAME} pd
                    LEFT JOIN {PaymentService.RECORD_TABLE} pr ON pd.id = pr.payment_detail_id
                    WHERE {where_sql}
                    GROUP BY pd.contract_no, pd.smelter_name
                    ORDER BY SUM(pd.total_amount) DESC
                    LIMIT %s OFFSET %s
                """
                
                cur.execute(query_sql, tuple(params + [size, offset]))
                rows = cur.fetchall()
                
                items = []
                for row in rows:
                    item = dict(row)
                    total_receivable = float(item.get('total_receivable') or 0)
                    total_received = float(item.get('total_received') or 0)
                    total_unreceived = float(item.get('total_unreceived') or 0)
                    
                    # 确定合同整体回款状态
                    order_count = int(item.get('order_count') or 0)
                    unpaid_count = int(item.get('unpaid_count') or 0)
                    paid_count = int(item.get('paid_count') or 0)
                    overpaid_count = int(item.get('overpaid_count') or 0)
                    
                    if unpaid_count == order_count:
                        contract_status = 0
                        contract_status_name = "未回款"
                    elif paid_count == order_count:
                        contract_status = 2
                        contract_status_name = "已结清"
                    elif overpaid_count > 0:
                        contract_status = 3
                        contract_status_name = "超额回款"
                    else:
                        contract_status = 1
                        contract_status_name = "部分回款"
                    
                    items.append({
                        "contract_no": item["contract_no"],
                        "smelter_name": item["smelter_name"],
                        "order_count": order_count,
                        "total_receivable": round(total_receivable, 2),
                        "total_received": round(total_received, 2),
                        "total_unreceived": round(total_unreceived, 2),
                        "collection_rate": round(total_received / total_receivable * 100, 2) if total_receivable > 0 else 0,
                        "contract_status": contract_status,
                        "contract_status_name": contract_status_name,
                        "status_breakdown": {
                            "unpaid": unpaid_count,
                            "partial": int(item.get("partial_count") or 0),
                            "paid": paid_count,
                            "overpaid": overpaid_count
                        },
                        "last_payment_date": str(item["last_payment_date"]) if item.get("last_payment_date") else None
                    })
                
                return {
                    "total": total,
                    "page": page,
                    "size": size,
                    "items": items
                }

    @staticmethod
    def get_contract_payment_details(
        contract_no: str,
        page: int = 1,
        size: int = 20
    ) -> Dict[str, Any]:
        """
        获取单个合同的回款明细列表
        """
        if not contract_no:
            raise ValueError("合同编号不能为空")
        
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 查询合同基本信息
                contract_sql = f"""
                    SELECT DISTINCT
                        pd.contract_no,
                        pd.smelter_name,
                        SUM(pd.total_amount) as contract_total,
                        SUM(pd.paid_amount) as contract_paid,
                        SUM(pd.unpaid_amount) as contract_unpaid
                    FROM {PaymentService.TABLE_NAME} pd
                    WHERE pd.contract_no = %s
                    GROUP BY pd.contract_no, pd.smelter_name
                """
                cur.execute(contract_sql, (contract_no,))
                contract_info = cur.fetchone()
                
                if not contract_info:
                    raise ValueError("合同不存在")
                
                # 查询该合同下的所有收款明细
                where_sql = "pd.contract_no = %s"
                params = [contract_no]
                
                count_sql = f"""
                    SELECT COUNT(*) as total 
                    FROM {PaymentService.TABLE_NAME} pd
                    WHERE {where_sql}
                """
                cur.execute(count_sql, tuple(params))
                total = cur.fetchone()["total"]
                
                offset = (page - 1) * size
                query_sql = f"""
                    SELECT 
                        pd.id,
                        pd.sales_order_id,
                        pd.material_name,
                        pd.unit_price,
                        pd.net_weight,
                        pd.total_amount,
                        pd.paid_amount,
                        pd.unpaid_amount,
                        pd.arrival_payment_amount,
                        pd.final_payment_amount,
                        pd.arrival_paid_amount,
                        pd.final_paid_amount,
                        pd.collection_status,
                        pd.status,
                        pd.is_paid,
                        pd.is_paid_out,
                        pd.remark,
                        pd.created_at,
                        pd.payee,
                        pd.payee_account,
                        wb.weigh_ticket_no,
                        wb.weigh_date,
                        wb.net_weight as shipped_weight,
                        (SELECT COUNT(*) FROM {PaymentService.RECORD_TABLE} pr WHERE pr.payment_detail_id = pd.id) as payment_record_count
                    FROM {PaymentService.TABLE_NAME} pd
                    LEFT JOIN pd_deliveries d ON d.id = COALESCE(pd.delivery_id, pd.sales_order_id)
                    LEFT JOIN pd_weighbills wb ON wb.delivery_id = d.id OR wb.id = pd.weighbill_id
                    WHERE {where_sql}
                    ORDER BY pd.created_at DESC
                    LIMIT %s OFFSET %s
                """
                
                cur.execute(query_sql, tuple(params + [size,offset]))
                rows = cur.fetchall()
            items = []
            for row in rows:
                item = dict(row)
                item['status_name'] = PaymentStatus(item['status']).name if item.get('status') is not None else None
                item['created_at'] = str(item['created_at']) if item.get('created_at') else None
                item['weigh_date'] = str(item['weigh_date']) if item.get('weigh_date') else None
                
                # 确保布尔状态字段有默认值
                if item.get('is_paid') is None:
                    item['is_paid'] = 1 if (item.get('paid_amount') or 0) > 0 else 0
                if item.get('is_paid_out') is None:
                    item['is_paid_out'] = 0
                
                item['collection_status_name'] = PaymentService._get_collection_status_name(
                    contract_info.get('smelter_name'),
                    item.get('arrival_paid_amount'),
                    item.get('final_paid_amount'),
                    item.get('paid_amount'),
                    item.get('collection_status')
                )
                items.append(item)
            
            # 查询该合同下的所有回款记录
            records_sql = f"""
                SELECT 
                    pr.id,
                    pr.payment_detail_id,
                    pr.payment_amount,
                    pr.payment_stage,
                    pr.payment_date,
                    pr.payment_method,
                    pr.transaction_no,
                    pr.remark,
                    pr.created_at
                FROM {PaymentService.RECORD_TABLE} pr
                INNER JOIN {PaymentService.TABLE_NAME} pd ON pr.payment_detail_id = pd.id
                WHERE pd.contract_no = %s
                ORDER BY pr.payment_date DESC, pr.created_at DESC
            """
            cur.execute(records_sql, (contract_no,))
            records = cur.fetchall()
            
            payment_records = []
            for record in records:
                rec = dict(record)
                rec['payment_stage_name'] = PaymentStage(rec['payment_stage']).name if rec.get('payment_stage') is not None else None
                rec['payment_date'] = str(rec['payment_date']) if rec.get('payment_date') else None
                rec['created_at'] = str(rec['created_at']) if rec.get('created_at') else None
                payment_records.append(rec)
            
            return {
                "contract_info": {
                    "contract_no": contract_info["contract_no"],
                    "smelter_name": contract_info["smelter_name"],
                    "total_receivable": float(contract_info["contract_total"]),
                    "total_received": float(contract_info["contract_paid"]),
                    "total_unreceived": float(contract_info["contract_unpaid"]),
                    "collection_rate": round(float(contract_info["contract_paid"]) / float(contract_info["contract_total"]) * 100, 2) if float(contract_info["contract_total"]) > 0 else 0
                },
                "total_orders": total,
                "page": page,
                "size": size,
                "orders": items,
                "payment_records": payment_records,
                "payment_record_count": len(payment_records)
            }