"""
销售台账/报货订单服务 - 完整版
支持合同品种匹配 + 车数校验（向后匹配）
"""
import json
import logging
import os
import re
import uuid
from openai import OpenAI
from decimal import Decimal, ROUND_FLOOR
from typing import Dict, List, Optional, Any
from datetime import datetime
from app.core.paths import UPLOADS_DIR
from app.services.delivery_contract_price_service import get_delivery_contract_price_service
from app.utils.product_mapping import convert_to_mill_product
from core.database import get_conn

logger = logging.getLogger(__name__)

_DELIVERY_ORDER_PLAN_COLUMNS_ENSURED = False


def _ensure_delivery_order_plan_columns() -> None:
    """旧库补全报单订货计划关联字段。"""
    global _DELIVERY_ORDER_PLAN_COLUMNS_ENSURED
    if _DELIVERY_ORDER_PLAN_COLUMNS_ENSURED:
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'pd_deliveries'
                    """
                )
                existing = {row["COLUMN_NAME"] for row in (cur.fetchall() or [])}
                if "order_plan_id" not in existing:
                    cur.execute(
                        """
                        ALTER TABLE pd_deliveries
                        ADD COLUMN order_plan_id BIGINT DEFAULT NULL
                            COMMENT '关联订货计划ID（与报单人、合同报货计划对应）'
                        """
                    )
                if "is_last_truck_for_order_plan" not in existing:
                    cur.execute(
                        """
                        ALTER TABLE pd_deliveries
                        ADD COLUMN is_last_truck_for_order_plan TINYINT DEFAULT 0
                            COMMENT '是否订货计划最后一车'
                        """
                    )
                cur.execute(
                    "SHOW INDEX FROM pd_deliveries WHERE Key_name = %s",
                    ("idx_order_plan_id",),
                )
                if not cur.fetchone():
                    try:
                        cur.execute(
                            "CREATE INDEX idx_order_plan_id ON pd_deliveries(order_plan_id)"
                        )
                    except Exception as ie:
                        logger.warning("create idx_order_plan_id skipped: %s", ie)
            conn.commit()
        _DELIVERY_ORDER_PLAN_COLUMNS_ENSURED = True
    except Exception as e:
        logger.warning("ensure_delivery_order_plan_columns skipped/failed: %s", e)


# 使用绝对路径，避免工作目录变化导致的问题
UPLOAD_DIR = UPLOADS_DIR / "delivery_orders"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# 标准车容量（吨）
STANDARD_TRUCK_CAPACITY = Decimal('35')


def _attach_contract_product_prices_to_delivery_rows(data: List[dict]) -> None:
    """为列表行附加 contract_product_prices（来自 pd_delivery_contract_product_prices）。"""
    if not data:
        return
    try:
        ids = [int(x["id"]) for x in data]
        pmap = get_delivery_contract_price_service().fetch_prices_by_delivery_ids(ids)
        for item in data:
            item["contract_product_prices"] = pmap.get(int(item["id"]), [])
    except Exception as e:
        logger.warning("attach contract_product_prices: %s", e)
        for item in data:
            item["contract_product_prices"] = []


class DeliveryService:
    """报货订单服务"""
    # ============ 文本提取和合同匹配方法 ============
    def __init__(self):
        """初始化 OpenAI 客户端"""
        self.client = OpenAI(
            api_key=os.getenv("DASHSCOPE_API_KEY"),
            base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
        )
        self._convert_to_mill_product = convert_to_mill_product

    def _normalize_driver_id_card(self, value: Optional[Any]) -> Optional[str]:
        """清洗司机身份证号，避免将无效超长值写入数据库。"""
        if value is None:
            return None

        normalized = str(value).strip()
        if normalized == "":
            return None

        normalized = re.sub(r"\s+", "", normalized).upper()
        return normalized

    def _normalize_driver_id_card_with_warnings(self, value: Optional[Any]) -> tuple:
        """
        規範化身份證號，支持不全補0、位數過多截斷並輸出提示。
        返回: (normalized_value, warnings_list)
        """
        warnings = []
        if value is None:
            return None, warnings

        raw = str(value).strip()
        if raw == "":
            return None, warnings

        normalized = re.sub(r"\s+", "", raw).upper()
        digits = re.sub(r"[^\dX]", "", normalized)
        if not digits:
            return None, warnings

        orig_len = len(digits)
        if orig_len < 18:
            normalized = digits + '0' * (18 - orig_len)
            warnings.append(f"身份證號位數不足，已自動末尾補0補足18位（原{orig_len}位）")
        elif orig_len > 18:
            normalized = digits[:18]
            warnings.append(f"身份證號位數過多，已截斷至18位（原{orig_len}位）")
        else:
            normalized = digits

        return normalized, warnings

    def _normalize_has_delivery_order(self, value: Optional[str]) -> Optional[str]:
        """将联单状态统一为数据库可接受值：有/无。"""
        if value is None:
            return None

        raw = str(value).strip()
        if raw == "":
            return None

        positive = {"有", "是", "true", "1", "yes", "y", "已上传", "uploaded"}
        negative = {"无", "否", "false", "0", "no", "n", "未上传", "pending"}

        low = raw.lower()
        if raw in positive or low in positive:
            return "有"
        if raw in negative or low in negative:
            return "无"

        return raw

    def _normalize_upload_status(self, value: Optional[str]) -> Optional[str]:
        """将上传状态统一为数据库可接受值：已上传/待上传。"""
        if value is None:
            return None

        raw = str(value).strip()
        if raw == "":
            return None

        positive = {"已上传", "上传", "是", "true", "1", "uploaded"}
        negative = {"待上传", "未上传", "未上传联单", "否", "false", "0", "pending"}

        low = raw.lower()
        if raw in positive or low in positive:
            return "已上传"
        if raw in negative or low in negative:
            return "待上传"

        return raw

    def _delivery_has_products_column(self) -> bool:
        """兼容旧库：动态检测 pd_deliveries 是否存在 products 列。"""
        cached = getattr(self, "_products_column_exists", None)
        if cached is not None:
            return cached

        exists = False
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT 1
                        FROM information_schema.COLUMNS
                        WHERE TABLE_SCHEMA = DATABASE()
                          AND TABLE_NAME = 'pd_deliveries'
                          AND COLUMN_NAME = 'products'
                        LIMIT 1
                        """
                    )
                    exists = cur.fetchone() is not None
        except Exception as e:
            logger.warning(f"检测 pd_deliveries.products 字段失败，将按不存在处理: {e}")

        self._products_column_exists = exists
        return exists

    def _weighbill_has_warehouse_name_column(self) -> bool:
        """兼容旧库：动态检测 pd_weighbills 是否存在 warehouse_name 列。"""
        cached = getattr(self, "_weighbill_warehouse_name_exists", None)
        if cached is not None:
            return cached

        exists = False
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SHOW COLUMNS FROM pd_weighbills LIKE 'warehouse_name'")
                    exists = cur.fetchone() is not None
        except Exception as e:
            logger.warning(f"检测 pd_weighbills.warehouse_name 字段失败，将按不存在处理: {e}")

        self._weighbill_warehouse_name_exists = exists
        return exists

    def _weighbill_has_audit_columns(self) -> bool:
        """兼容旧库：检测 pd_weighbills 是否有 audit_status 列"""
        cached = getattr(self, "_weighbill_audit_columns_exists", None)
        if cached is not None:
            return cached
        exists = False
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SHOW COLUMNS FROM pd_weighbills LIKE 'audit_status'")
                    exists = cur.fetchone() is not None
        except Exception as e:
            logger.warning("检测 pd_weighbills.audit_status 字段失败: %s", e)
        self._weighbill_audit_columns_exists = exists
        return exists

    def _weighbill_has_order_plan_last_column(self) -> bool:
        """兼容旧库：pd_weighbills.is_last_truck_for_order_plan"""
        cached = getattr(self, "_weighbill_order_plan_last_exists", None)
        if cached is not None:
            return cached
        exists = False
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SHOW COLUMNS FROM pd_weighbills LIKE 'is_last_truck_for_order_plan'"
                    )
                    exists = cur.fetchone() is not None
        except Exception as e:
            logger.warning("检测 pd_weighbills.is_last_truck_for_order_plan 失败: %s", e)
        self._weighbill_order_plan_last_exists = exists
        return exists

    def _ensure_weighbill_order_plan_last_column(self) -> None:
        if self._weighbill_has_order_plan_last_column():
            return
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        ALTER TABLE pd_weighbills
                        ADD COLUMN is_last_truck_for_order_plan TINYINT DEFAULT 0
                            COMMENT '是否为订货计划最后一车'
                        """
                    )
                conn.commit()
            self._weighbill_order_plan_last_exists = True
        except Exception as e:
            logger.warning("ensure_weighbill_order_plan_last_column skipped/failed: %s", e)

    def _get_upload_status(self, image_path: Optional[str]) -> str:
        if image_path and os.path.exists(image_path):
            return "联单已上传"
        return "联单未上传"

    def _determine_source_type(self, has_order: str, uploaded_by: str = None) -> str:
        """
        确定来源类型
        - 有联单 -> 司机
        - 无联单 -> 公司
        - 公司人员上传有联单 -> 可指定为公司
        """
        if has_order == '有':
            if uploaded_by == '公司':
                return '公司'
            return '司机'
        else:
            return '公司'

    def _calculate_service_fee(self, has_delivery_order: str) -> Decimal:
        """
        计算联单费
        - 无联单：150元
        - 有联单：0元
        """
        if has_delivery_order == '无':
            return Decimal('150')
        return Decimal('0')

    def _calculate_trucks(self, quantity: Decimal) -> int:
        """
        计算车数（向下取整）
        车数 = floor(quantity / 35)
        """
        if quantity <= 0:
            return 1
        return int((quantity / STANDARD_TRUCK_CAPACITY).to_integral_value(rounding=ROUND_FLOOR))

    def _match_contract_with_truck_check(
        self, 
        factory_name: str,
        product_name: str,
        planned_trucks: int,
        report_date: str,
        exact_contract_no: Optional[str] = None  # 新增：精确匹配合同编号
    ) -> Dict:
        """
        匹配合同（向后匹配策略）：
        1. 先按品种匹配（只匹配unit_price>0的有效品种）
        2. 按时间倒序排列（最新的优先）
        3. 依次检查每个合同的车数：
           - 如果够，匹配成功
           - 如果不够，记录并继续下一个
        4. 如果所有合同车数都不够，返回详细错误

        返回: {
            'matched': bool,
            'contract_no': str or None,
            'unit_price': float or None,
            'total_amount': float or None,
            'is_last_delivery': bool,
            'contract_total_trucks': int,
            'contract_used_trucks': int,
            'contract_remaining_trucks': int,
            'this_delivery_trucks': int,
            'skipped_contracts': list,  # 跳过的合同列表
            'reason': str or None
        }
        """
        from datetime import datetime
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    effective_date = report_date or datetime.today().date().isoformat()
                # ========== 新增：优先精确匹配合同编号 ==========
                    if exact_contract_no:
                        cur.execute("""
                            SELECT c.id, c.contract_no, p.unit_price, c.total_quantity,
                                CEIL(c.total_quantity / 35) as contract_trucks
                            FROM pd_contracts c
                            JOIN pd_contract_products p ON p.contract_id = c.id
                            WHERE c.contract_no = %s
                            AND c.status = '生效中'
                            AND p.product_name = %s
                            AND p.unit_price > 0
                            LIMIT 1
                        """, (exact_contract_no, product_name))
                        
                        exact_match = cur.fetchone()
                        if exact_match:
                            # 转换为字典
                            if isinstance(exact_match, dict):
                                match = exact_match
                            else:
                                columns = [desc[0] for desc in cur.description]
                                match = dict(zip(columns, exact_match))
                            
                            contract_no = match["contract_no"]
                            unit_price = match["unit_price"]
                            contract_trucks = int(match.get("contract_trucks") or 0)
                            
                            # 统计已用车数
                            cur.execute("""
                                SELECT COALESCE(SUM(planned_trucks), 0) as used_trucks
                                FROM pd_deliveries
                                WHERE contract_no = %s AND contract_id = %s
                                AND status = '审核通过'
                            """, (contract_no, match["id"]))
                            
                            used_trucks = int(cur.fetchone()['used_trucks'] or 0)
                            remaining = contract_trucks - used_trucks
                            
                            if planned_trucks <= remaining:
                                return {
                                    'matched': True,
                                    'contract_no': contract_no,
                                    'contract_id': match["id"],  # 返回合同ID
                                    'unit_price': float(unit_price) if unit_price else None,
                                    'is_last_delivery': (used_trucks + planned_trucks) >= contract_trucks,
                                    'contract_total_trucks': contract_trucks,
                                    'contract_used_trucks': used_trucks,
                                    'contract_remaining_trucks': remaining - planned_trucks,
                                    'this_delivery_trucks': planned_trucks,
                                    'match_type': 'exact_no',  # 精确匹配
                                    'reason': None
                                }
                            else:
                                return {
                                    'matched': False,
                                    'reason': f'精确匹配到合同 {contract_no}，但车数不足（需{planned_trucks}车，仅余{remaining}车）',
                                    'contract_no': None,
                                    'unit_price': None,
                                    'skipped_contracts': []
                                }
                    # 步骤1：品种匹配（仅 unit_price>0 的有效品种）
                    # ===== 需求3：优先匹配最先到期的合同 =====
                    cur.execute("""
                        SELECT c.id AS contract_id, c.contract_no, p.unit_price, c.total_quantity,
                               FLOOR(c.total_quantity / 35) as contract_trucks,
                               c.contract_date,
                               c.end_date
                        FROM pd_contracts c
                        JOIN pd_contract_products p ON p.contract_id = c.id
                        WHERE c.smelter_company = %s
                        AND p.product_name = %s
                        AND p.unit_price > 0              -- 只匹配有效价格（价格>0）
                        AND c.status = '生效中'
                        AND c.contract_date <= %s
                        AND (c.end_date IS NULL OR c.end_date >= %s)
                        ORDER BY c.end_date ASC, c.contract_date ASC, p.sort_order ASC
                    """, (factory_name, product_name, effective_date, effective_date))
                    matching_contracts = cur.fetchall()

                    if not matching_contracts:
                        return {
                            'matched': False,
                            'reason': f'未找到匹配品种[{product_name}]的有效合同（价格必须>0）',
                            'contract_no': None,
                            'unit_price': None,
                            'skipped_contracts': []
                        }

                    # 步骤2：遍历匹配合同，选取车数余量足够的
                    report_date_obj = None
                    if report_date:
                        try:
                            report_date_obj = datetime.strptime(report_date, "%Y-%m-%d").date()
                        except ValueError:
                            # 如果日期格式不正确，使用当前日期
                            report_date_obj = datetime.today().date()
                    else:
                        report_date_obj = datetime.today().date()
                    skipped_contracts = []  # 记录车数不足被跳过的合同

                    for idx, contract in enumerate(matching_contracts):
                        contract_no = contract.get("contract_no")
                        unit_price = contract.get("unit_price")
                        contract_trucks = int(contract.get("contract_trucks") or 0)

                        # ===== 需求1：检查报单日期是否在合同有效期内 =====
                        from datetime import datetime
                        contract_date = contract.get("contract_date")
                        end_date = contract.get("end_date")
                                   # 检查是否在合同开始日期之后
                        if contract_date:
                            try:
                                contract_date_obj = datetime.strptime(str(contract_date), "%Y-%m-%d").date()
                                if report_date_obj < contract_date_obj:
                                    skipped_contracts.append({
                                        'index': idx + 1,
                                        'contract_no': contract_no,
                                        'unit_price': float(unit_price) if unit_price else None,
                                        'total_trucks': contract_trucks,
                                        'used_trucks': 0,
                                        'remaining_trucks': contract_trucks,
                                        'need_trucks': planned_trucks,
                                        'skip_reason': f'报单日期{report_date}早于合同开始日期{contract_date}'
                                    })
                                    continue  # 跳过该合同，继续下一个
                            except (ValueError, TypeError):
                                pass
                        
                        # 检查是否在合同截止日期之前
                        if end_date:
                            try:
                                end_date_obj = datetime.strptime(str(end_date), "%Y-%m-%d").date()
                                if report_date_obj > end_date_obj:
                                    skipped_contracts.append({
                                        'index': idx + 1,
                                        'contract_no': contract_no,
                                        'unit_price': float(unit_price) if unit_price else None,
                                        'total_trucks': contract_trucks,
                                        'used_trucks': 0,
                                        'remaining_trucks': contract_trucks,
                                        'need_trucks': planned_trucks,
                                        'skip_reason': f'报单日期{report_date}晚于合同截止日期{end_date}'
                                    })
                                    continue  # 跳过该合同，继续下一个
                            except (ValueError, TypeError):
                                pass

                        # 统计该合同已匹配的报单车数总和
                        cur.execute("""
                            SELECT COALESCE(SUM(planned_trucks), 0) as used_trucks
                            FROM pd_deliveries
                            WHERE contract_no = %s
                            AND status = '审核通过'
                        """, (contract_no,))
                        
                        used_trucks = int(cur.fetchone()['used_trucks'] or 0)
                        remaining = contract_trucks - used_trucks

                        # 检查车数是否足够
                        # 新逻辑：匹配后必须至少剩余1车，即 planned_trucks < remaining
                        if planned_trucks < remaining:  # 严格小于，确保匹配后至少剩1车
                            # 找到够车数的合同！判断是否最后一单
                            is_last = (used_trucks + planned_trucks) >= contract_trucks

                            logger.debug(f"选择合同用于报单: contract_no={contract_no}, contract_trucks={contract_trucks}, used_trucks={used_trucks}, remaining_before={remaining}, this_delivery_trucks={planned_trucks}")

                            return {
                                'matched': True,
                                'contract_no': contract_no,
                                'contract_id': contract.get('contract_id'),
                                'unit_price': float(unit_price) if unit_price else None,
                                'is_last_delivery': is_last,
                                'contract_total_trucks': contract_trucks,
                                'contract_used_trucks': used_trucks,
                                'contract_remaining_trucks': remaining - planned_trucks,
                                'this_delivery_trucks': planned_trucks,
                                'matched_index': idx + 1,  # 第几个匹配的合同
                                'total_matched': len(matching_contracts),
                                'skipped_contracts': skipped_contracts,
                                'reason': None
                            }
                        else:
                            # 车数不够，或刚好用完无法保证剩余1车，记录并继续下一个
                            skip_reason = f'车数不足（需{planned_trucks}车，仅余{remaining}车）'
                            if planned_trucks == remaining:
                                skip_reason = f'车数刚好用完无法保证剩余1车（需{planned_trucks}车，仅余{remaining}车，匹配后剩余0车）'
                            
                            skipped_contracts.append({
                                'index': idx + 1,
                                'contract_no': contract_no,
                                'unit_price': float(unit_price) if unit_price else None,
                                'total_trucks': contract_trucks,
                                'used_trucks': used_trucks,
                                'remaining_trucks': remaining,
                                'need_trucks': planned_trucks,
                                'skip_reason': skip_reason
                            })
                            # 车数不足，继续检查下一个合同
                            continue

                    # 步骤3：匹配合同车数均不足
                    error_details = []
                    for info in skipped_contracts:
                        error_details.append(
                            f"[{info['contract_no']}]总{info['total_trucks']}车/"
                            f"已用{info['used_trucks']}车/"
                            f"剩{info['remaining_trucks']}车"
                        )

                    return {
                        'matched': False,
                        'reason': f'找到{len(matching_contracts)}个匹配品种[{product_name}]的合同，'
                                  f'但车数均不足本单需要{planned_trucks}车：'
                                  f'{"; ".join(error_details)}',
                        'contract_no': None,
                        'unit_price': None,
                        'skipped_contracts': skipped_contracts,
                        'suggest': '请拆分报单数量，或创建新车数充足的新合同'
                    }
                        

                    # 步骤3：匹配合同车数均不足
                    # 构建详细的错误信息
                    error_details = []
                    for info in skipped_contracts:
                        error_details.append(
                            f"[{info['contract_no']}]总{info['total_trucks']}车/"
                            f"已用{info['used_trucks']}车/"
                            f"剩{info['remaining_trucks']}车"
                        )

                    return {
                        'matched': False,
                        'reason': f'找到{len(matching_contracts)}个匹配品种[{product_name}]的合同，'
                                  f'但车数均不足本单需要{planned_trucks}车：'
                                  f'{"; ".join(error_details)}',
                        'contract_no': None,
                        'unit_price': None,
                        'skipped_contracts': skipped_contracts,
                        'suggest': '请拆分报单数量，或创建新车数充足的新合同'
                    }

        except Exception as e:
            logger.error(f"合同匹配失败: {e}")
            return {
                'matched': False,
                'reason': f'匹配异常: {str(e)}',
                'contract_no': None,
                'unit_price': None,
                'skipped_contracts': []
            }

    def _match_order_plan_for_delivery(
        self,
        contract_id: Optional[int],
        reporter_id: Optional[int],
        planned_trucks: int,
    ) -> Dict[str, Any]:
        """
        合同关联报货计划 + 报单人(与订货计划 created_by 一致) 定位订货计划，按审核通过报单累计车数校验余量。
        最后一车判定与合同侧一致：(已用 + 本单) >= 计划总车数。
        """
        if not contract_id or planned_trucks < 1:
            return {
                "matched": True,
                "skipped": True,
                "order_plan_id": None,
                "is_last_truck_for_order_plan": False,
                "reason": None,
            }
        if reporter_id is None:
            return {
                "matched": True,
                "skipped": True,
                "order_plan_id": None,
                "is_last_truck_for_order_plan": False,
                "reason": None,
            }
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT delivery_plan_id FROM pd_contracts WHERE id = %s LIMIT 1
                        """,
                        (contract_id,),
                    )
                    crow = cur.fetchone()
                    if not crow:
                        return {
                            "matched": False,
                            "skipped": False,
                            "order_plan_id": None,
                            "is_last_truck_for_order_plan": False,
                            "reason": "合同不存在，无法校验订货计划",
                        }
                    dpid = crow.get("delivery_plan_id")
                    if dpid is None:
                        return {
                            "matched": True,
                            "skipped": True,
                            "order_plan_id": None,
                            "is_last_truck_for_order_plan": False,
                            "reason": None,
                        }

                    cur.execute(
                        """
                        SELECT id, truck_count, plan_no
                        FROM pd_order_plans
                        WHERE delivery_plan_id = %s
                          AND created_by = %s
                          AND audit_status = %s
                        ORDER BY id DESC
                        LIMIT 1
                        """,
                        (dpid, reporter_id, "审核通过"),
                    )
                    orow = cur.fetchone()
                    if not orow:
                        return {
                            "matched": False,
                            "skipped": False,
                            "order_plan_id": None,
                            "is_last_truck_for_order_plan": False,
                            "reason": (
                                "未找到与报单人对应的、已审核通过且与合同同一报货计划的订货计划，"
                                "请先录入并审核订货计划"
                            ),
                        }

                    order_plan_id = int(orow["id"])
                    plan_total = int(orow.get("truck_count") or 0)
                    plan_no = (orow.get("plan_no") or "").strip()

                    cur.execute(
                        """
                        SELECT COALESCE(SUM(planned_trucks), 0) AS used_trucks
                        FROM pd_deliveries
                        WHERE order_plan_id = %s AND status = '审核通过'
                        """,
                        (order_plan_id,),
                    )
                    urow = cur.fetchone()
                    used_trucks = int((urow or {}).get("used_trucks") or 0)
                    remaining = plan_total - used_trucks

                    if planned_trucks > remaining:
                        return {
                            "matched": False,
                            "skipped": False,
                            "order_plan_id": None,
                            "is_last_truck_for_order_plan": False,
                            "reason": (
                                f"订货计划[{plan_no}]车数不足：本单需{planned_trucks}车，"
                                f"计划共{plan_total}车、已用{used_trucks}车，仅剩{remaining}车"
                            ),
                        }

                    is_last = (used_trucks + planned_trucks) >= plan_total

                    return {
                        "matched": True,
                        "skipped": False,
                        "order_plan_id": order_plan_id,
                        "is_last_truck_for_order_plan": is_last,
                        "order_plan_no": plan_no,
                        "order_plan_total_trucks": plan_total,
                        "order_plan_used_trucks": used_trucks,
                        "order_plan_remaining_trucks": remaining - planned_trucks,
                        "this_delivery_trucks": planned_trucks,
                        "reason": None,
                    }
        except Exception as e:
            logger.error("订货计划匹配失败: %s", e)
            return {
                "matched": False,
                "skipped": False,
                "order_plan_id": None,
                "is_last_truck_for_order_plan": False,
                "reason": f"订货计划校验异常: {e}",
            }

    def _get_contract_price_by_product(self, contract_no: str, product_name: str) -> Optional[float]:
        """
        根据合同编号和品种获取单价
        """
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT p.unit_price 
                        FROM pd_contract_products p
                        JOIN pd_contracts c ON p.contract_id = c.id
                        WHERE c.contract_no = %s 
                        AND p.product_name = %s
                        AND p.unit_price IS NOT NULL
                        LIMIT 1
                    """, (contract_no, product_name))

                    row = cur.fetchone()
                    if not row:
                        return None

                    unit_price = row.get('unit_price') if isinstance(row, dict) else row[0]
                    if unit_price is not None:
                        return float(unit_price)
                    return None
        except Exception as e:
            logger.error(f"获取品种单价失败: {e}")
            return None

    def _create_weighbills(self, delivery_id: int, contract_no: str,
                           vehicle_no: str, products: List[str],
                           is_last_for_contract: bool,
                           unit_price: float,
                           warehouse_name: Optional[str],
                           uploader_id: int,
                           uploader_name: str,
                           is_last_for_order_plan: bool = False) -> bool:
        """
        为每个品种创建磅单占位记录
        """
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    for product_name in products:
                        # 检查是否已存在
                        cur.execute("""
                            SELECT id FROM pd_weighbills 
                            WHERE delivery_id = %s AND product_name = %s
                        """, (delivery_id, product_name))

                        if cur.fetchone():
                            logger.warning(f"品种 {product_name} 的磅单已存在，跳过")
                            continue

                        # 创建磅单，标记最后一车，默认审核状态为待审核
                        is_last_mark = 1 if is_last_for_contract else 0
                        is_last_op_mark = 1 if is_last_for_order_plan else 0
                        insert_fields = [
                            "delivery_id", "contract_no", "vehicle_no", "product_name",
                            "is_last_truck_for_contract", "unit_price", "upload_status",
                            "ocr_status", "uploader_id", "uploader_name", "uploaded_at"
                        ]
                        insert_values = [
                            delivery_id, contract_no, vehicle_no, product_name,
                            is_last_mark, unit_price, '待上传', '待上传磅单',
                            uploader_id, uploader_name
                        ]
                        if self._weighbill_has_order_plan_last_column():
                            _ip = insert_fields.index("is_last_truck_for_contract") + 1
                            insert_fields.insert(_ip, "is_last_truck_for_order_plan")
                            insert_values.insert(_ip, is_last_op_mark)

                        if self._weighbill_has_warehouse_name_column():
                            insert_fields.insert(4, "warehouse_name")
                            insert_values.insert(4, warehouse_name)
                        if self._weighbill_has_audit_columns():
                            insert_fields.insert(insert_fields.index("upload_status"), "audit_status")
                            insert_values.insert(insert_values.index('待上传'), "待审核")

                        placeholders = ", ".join(["%s"] * len(insert_values))
                        cur.execute(f"""
                            INSERT INTO pd_weighbills 
                            ({', '.join(insert_fields)})
                            VALUES ({placeholders}, NOW())
                        """, tuple(insert_values))

                    logger.info(f"报单{delivery_id}:创建{len(products)}个品种磅单,"
                               f"合同最后一单={is_last_for_contract}")
                    return True

        except Exception as e:
            logger.error(f"创建磅单记录失败: {e}")
            return False

    def check_duplicate_in_24h(self, driver_phone: str, driver_id_card: str, exclude_id: int = None) -> Dict[str, Any]:
        """
        检查同一司机24小时内是否已报单
        """
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    conditions = ["created_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)"]
                    params = []

                    if driver_phone:
                        conditions.append("(driver_phone = %s OR driver_id_card = %s)")
                        params.extend([driver_phone, driver_phone])
                    if driver_id_card:
                        conditions.append("(driver_phone = %s OR driver_id_card = %s)")
                        params.extend([driver_id_card, driver_id_card])

                    if exclude_id:
                        conditions.append("id != %s")
                        params.append(exclude_id)

                    where_sql = "(" + " OR ".join(conditions[1:]) + ")" if len(conditions) > 1 else "1=1"
                    where_sql = f"{conditions[0]} AND ({where_sql})"

                    cur.execute(f"""
                        SELECT id, contract_no, report_date, vehicle_no, driver_name, 
                               driver_phone, driver_id_card, created_at
                        FROM pd_deliveries
                        WHERE {where_sql}
                        ORDER BY created_at DESC
                    """, tuple(params))

                    rows = cur.fetchall()

                    existing_orders = []
                    for row in rows:
                        order = dict(row) if isinstance(row, dict) else {
                            'id': row[0],
                            'contract_no': row[1],
                            'report_date': row[2],
                            'vehicle_no': row[3],
                            'driver_name': row[4],
                            'driver_phone': row[5],
                            'driver_id_card': row[6],
                            'created_at': row[7],
                        }
                        for key in ['report_date', 'created_at']:
                            if order.get(key):
                                order[key] = str(order[key])
                        existing_orders.append(order)

                    return {
                        "is_duplicate": len(existing_orders) > 0,
                        "existing_orders": existing_orders,
                        "duplicate_count": len(existing_orders)
                    }

        except Exception as e:
            logger.error(f"检查重复报单失败: {e}")
            return {"is_duplicate": False, "existing_orders": [], "duplicate_count": 0, "error": str(e)}

    def _build_operations(self, has_delivery_order: str, upload_status: str, image_path: Optional[str]) -> Dict[str, bool]:
        """
        构建操作权限标记
        """
        has_image = image_path and os.path.exists(image_path)
        is_uploaded = upload_status == '已上传' or has_image

        return {
            "can_upload": not is_uploaded,
            "can_modify": is_uploaded,
            "can_view": is_uploaded
        }

    def _parse_products(self, products_raw: Any, product_name: str = None) -> List[str]:
        """
        解析品种列表
        """
        products = []

        if products_raw is None:
            products = []
        elif isinstance(products_raw, str):
            # 兼容中英文逗号、顿号、斜杠、竖线等常见分隔符
            normalized = re.sub(r"[，、/|+；;]+", ",", products_raw)
            products = [p.strip() for p in normalized.split(',') if p.strip()]
        elif isinstance(products_raw, (list, tuple)):
            products = list(products_raw)

        # 去重，最多4个
        products = list(dict.fromkeys(products))[:4]

        # 如果没有，使用主品种
        if not products and product_name:
            products = [product_name]

        return products

    def _save_delivery_image(self, image_bytes: bytes, vehicle_no: str) -> str:
        """保存单张联单图片，返回路径"""
        safe_name = re.sub(r'[^\w\-]', '_', str(vehicle_no or 'unknown'))
        filename = f"delivery_{safe_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}.jpg"
        file_path = UPLOAD_DIR / filename
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "wb") as f:
            f.write(image_bytes)
        return str(file_path)

    def _save_voucher_image(self, image_bytes: bytes, vehicle_no: str, index: int) -> str:
        """保存单张凭证图片，返回路径"""
        safe_name = re.sub(r'[^\w\-]', '_', str(vehicle_no or 'unknown'))
        filename = f"voucher_{safe_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{index}_{uuid.uuid4().hex[:4]}.jpg"
        # 存放在 UPLOAD_DIR/vouchers/ 子目录下
        file_path = UPLOAD_DIR / 'vouchers' / filename
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "wb") as f:
            f.write(image_bytes)
        return str(file_path)

    def create_delivery(
            self,
            data: Dict,
            delivery_order_image: bytes = None,
            voucher_images: List[bytes] = None,
            current_user: dict = None,
            confirm_flag: bool = False
    ) -> Dict[str, Any]:
        """创建报货订单（支持有联单图片或无联单多张凭证图片）"""
        temp_files = []  # 记录所有临时文件，用于异常时清理
        try:
            # ---------- 参数校验 ----------
            driver_phone = data.get('driver_phone')
            id_card_warnings = []
            driver_id_card, id_card_warnings = self._normalize_driver_id_card_with_warnings(data.get('driver_id_card'))
            data['driver_id_card'] = driver_id_card
            warnings = list(id_card_warnings)

            logger.info(f"【DEBUG】create_delivery 开始，data={data}, current_user={current_user}")

            # 处理来源类型
            has_order = self._normalize_has_delivery_order(data.get('has_delivery_order', '无'))
            if has_order not in ('有', '无'):
                return {"success": False, "error": "has_delivery_order 仅支持：有/无（或 是/否）"}
            data['has_delivery_order'] = has_order

            # ---------- 新增：图片互斥校验 ----------
            if has_order == '有':
                if voucher_images:
                    return {"success": False, "error": "有联单时不能上传凭证图片"}
                # 联单图片处理
                if delivery_order_image:
                    image_path = self._save_delivery_image(delivery_order_image, data.get('vehicle_no'))
                    temp_files.append(image_path)
                    data['delivery_order_image'] = image_path
                    data['upload_status'] = '已上传'
                else:
                    data['delivery_order_image'] = None
                    data['upload_status'] = '待上传'
                # 清空凭证图片字段
                data['voucher_images'] = None
            else:  # 无联单
                if delivery_order_image:
                    return {"success": False, "error": "无联单时不能上传联单图片，请使用凭证图片"}
                if voucher_images and len(voucher_images) > 6:
                    return {"success": False, "error": "凭证图片最多6张"}
                # 保存多张凭证图片
                voucher_paths = []
                if voucher_images:
                    for idx, img_bytes in enumerate(voucher_images):
                        path = self._save_voucher_image(img_bytes, data.get('vehicle_no'), idx)
                        temp_files.append(path)
                        voucher_paths.append(path)
                data['voucher_images'] = voucher_paths if voucher_paths else None
                # 联单图片字段置空
                data['delivery_order_image'] = None
                data['upload_status'] = '待上传'

            # ---------- 原有逻辑：处理操作人信息、计算联单费等 ----------
            uploaded_by = data.get('uploaded_by')
            source_type = self._determine_source_type(has_order, uploaded_by)
            data['source_type'] = source_type

            uploader_id = None
            uploader_name = "system"
            user_role = None  # 用户角色，用于填充岗位
            if current_user:
                uploader_id = current_user.get("id")
                uploader_name = current_user.get("name") or current_user.get("account") or "system"
                user_role = current_user.get("role")  # 从 current_user 提取 role

            reporter_id = data.get('reporter_id') or uploader_id
            reporter_name = data.get('reporter_name') or data.get('shipper') or uploader_name
            if not data.get('shipper'):
                data['shipper'] = reporter_name

            # 自动填充岗位：优先使用传入的 position，否则使用用户的 role
            if not data.get('position') and user_role:
                data['position'] = user_role
                logger.info(f"【DEBUG】自动填充岗位: {user_role}")

            service_fee = self._calculate_service_fee(has_order)
            data['service_fee'] = service_fee

            # 24小时重复校验
            if not confirm_flag:
                if driver_phone or driver_id_card:
                    duplicate_check = self.check_duplicate_in_24h(driver_phone, driver_id_card)
                    if duplicate_check.get("is_duplicate"):
                        return {
                            "success": False,
                            "need_confirm": True,
                            "error": f"该司机24小时内已有 {duplicate_check.get('duplicate_count', 0)} 笔报单，是否继续提交？",
                            "existing_orders": duplicate_check.get("existing_orders", [])
                        }

            # 处理品种列表
            products = self._parse_products(data.get('products'), data.get('product_name'))
            if not products:
                return {"success": False, "error": "货物品种不能为空"}
    
           # ===== 对所有品种进行映射转换 =====
            mapped_products = []
            for p in products:
                mapped_product = self._convert_to_mill_product(p)
                mapped_products.append(mapped_product)
            products = mapped_products

            # 更新主品种为映射后的值
            if products:
                data['product_name'] = products[0]
            # =====================================

            # 合同匹配使用映射后的品种
            mill_main_product = products[0] if products else data.get('product_name')
            # ----------------------------------------------

            # 计算本单总车数
            quantity = Decimal(str(data.get('quantity', 0)))
            planned_trucks = self._calculate_trucks(quantity)
            data['planned_trucks'] = planned_trucks
            # 新建报单为「待审核」；仅人工审核结论为「审核通过」或「审核未通过」。
            data['status'] = '待审核'
            # 合同匹配
            target_factory = data.get('target_factory_name')
            exact_contract_no = data.get('contract_no')  # 获取用户指定的合同编号
            
            match_result = self._match_contract_with_truck_check(
                factory_name=target_factory,
                product_name=mill_main_product,
                planned_trucks=planned_trucks,
                report_date=data.get('report_date'),
                exact_contract_no=exact_contract_no  # 传入精确匹配
            )
            
            if not match_result['matched']:
                return {
                    "success": False,
                    "error": match_result['reason'],
                    "suggest": match_result.get('suggest', '请检查合同编号是否正确，或拆分报单数量')
                }

            contract_no = match_result['contract_no']
            contract_id = match_result.get('contract_id')
            unit_price = match_result['unit_price']
            is_last_delivery = match_result['is_last_delivery']
            total_amount = float(Decimal(str(unit_price)) * quantity) if (unit_price and quantity) else None
            data['contract_no'] = contract_no
            data['contract_id'] = contract_id
            data['contract_unit_price'] = unit_price
            data['total_amount'] = total_amount

            _ensure_delivery_order_plan_columns()
            op_match = self._match_order_plan_for_delivery(
                contract_id, reporter_id, planned_trucks
            )
            if not op_match["matched"]:
                return {
                    "success": False,
                    "error": op_match["reason"],
                    "suggest": "请确认订货计划已审核、报单人与订货计划录入人一致，或调整车数/订货计划",
                }
            order_plan_id = op_match.get("order_plan_id")
            is_last_truck_for_order_plan = bool(op_match.get("is_last_truck_for_order_plan"))
            order_plan_flag_int = 1 if is_last_truck_for_order_plan else 0
            order_plan_truck_info = None
            if not op_match.get("skipped"):
                order_plan_truck_info = {
                    "order_plan_id": order_plan_id,
                    "order_plan_no": op_match.get("order_plan_no"),
                    "order_plan_total_trucks": op_match.get("order_plan_total_trucks"),
                    "order_plan_used_trucks": op_match.get("order_plan_used_trucks"),
                    "order_plan_remaining_trucks": op_match.get("order_plan_remaining_trucks"),
                    "this_delivery_trucks": op_match.get("this_delivery_trucks"),
                }

            # ---------- 插入数据库 ----------
            with get_conn() as conn:
                with conn.cursor() as cur:
                    has_products_column = self._delivery_has_products_column()

                    insert_fields = [
                        'report_date', 'warehouse', 'target_factory_id', 'target_factory_name',
                        'product_name', 'quantity', 'planned_trucks', 'vehicle_no',
                        'driver_name', 'driver_phone', 'driver_id_card', 'has_delivery_order',
                        'delivery_order_image', 'upload_status', 'source_type', 'shipper',
                        'payee', 'service_fee', 'contract_no', 'contract_id',
                        'order_plan_id', 'is_last_truck_for_order_plan',
                        'contract_unit_price',
                        'total_amount', 'status', 'uploader_id', 'uploader_name',
                        'reporter_id', 'reporter_name', 'voucher_images',
                        # ===== 需求4：新增字段 =====
                        'position',
                        # ===== 需求4结束 =====
                    ]
                    main_product = products[0] if products else data.get('product_name')
                    # 确保主品种也经过映射
                    values = [
                        data.get('report_date'),
                        data.get('warehouse'),
                        data.get('target_factory_id'),
                        target_factory,
                        main_product,
                        quantity,
                        planned_trucks,
                        data.get('vehicle_no'),
                        data.get('driver_name'),
                        driver_phone,
                        driver_id_card,
                        has_order,
                        data.get('delivery_order_image'),
                        data.get('upload_status'),
                        source_type,
                        data.get('shipper'),
                        data.get('payee'),
                        service_fee,
                        contract_no,
                        contract_id,
                        order_plan_id,
                        order_plan_flag_int,
                        unit_price,
                        total_amount,
                        data.get('status', '待审核'),
                        uploader_id,
                        uploader_name,
                        reporter_id,
                        reporter_name,
                        json.dumps(data.get('voucher_images')) if data.get('voucher_images') else None,
                        data.get('position'),
                    ]

                    if has_products_column:
                        insert_fields.insert(5, 'products')
                        values.insert(5, ','.join(products) if products else None)

                    placeholders = ','.join(['%s'] * len(values))
                    fields_str = ','.join(insert_fields)
                    sql = f"""
                        INSERT INTO pd_deliveries 
                        ({fields_str}, uploaded_at)
                        VALUES ({placeholders}, NOW())
                    """
                    cur.execute(sql, tuple(values))
                    delivery_id = cur.lastrowid

                    # 创建磅单记录（原有逻辑）
                    if products and contract_no:
                        self._ensure_weighbill_order_plan_last_column()
                        self._create_weighbills(
                            delivery_id=delivery_id,
                            contract_no=contract_no,
                            vehicle_no=data.get('vehicle_no'),
                            products=products,
                            is_last_for_contract=is_last_delivery,
                            unit_price=unit_price,
                            warehouse_name=data.get('warehouse'),
                            uploader_id=uploader_id,
                            uploader_name=uploader_name,
                            is_last_for_order_plan=is_last_truck_for_order_plan,
                        )

            # 从合同品种表同步到 pd_delivery_contract_product_prices，供列表 contract_product_prices 使用
            try:
                sync_res = get_delivery_contract_price_service().sync_from_contract(delivery_id)
                if not sync_res.get("success"):
                    logger.info(
                        "create_delivery: 合同品类单价未同步 delivery_id=%s reason=%s",
                        delivery_id,
                        sync_res.get("error"),
                    )
            except Exception as ex:
                logger.warning(
                    "create_delivery: 同步合同品类单价异常 delivery_id=%s %s",
                    delivery_id,
                    ex,
                )

            # 构建返回数据
            operations = self._build_operations(has_order, data.get('upload_status'), data.get('delivery_order_image'))

            msg_suffix = ""
            if is_last_delivery:
                msg_suffix += "（合同最后一单）"
            if is_last_truck_for_order_plan:
                msg_suffix += "（订货计划最后一车）"
            response_data = {
                "success": True,
                "message": "报货订单创建成功" + msg_suffix,
                "warnings": warnings if warnings else [],
                "data": {
                    "id": delivery_id,
                    "contract_no": contract_no,
                    "products": products,
                    "quantity": float(quantity),
                    "planned_trucks": planned_trucks,
                    "contract_unit_price": unit_price,
                    "total_amount": total_amount,
                    "source_type": source_type,
                    "upload_status": data.get('upload_status'),
                    "service_fee": float(service_fee) if service_fee else 0,
                    "uploader_id": uploader_id,
                    "uploader_name": uploader_name,
                    "reporter_id": reporter_id,
                    "reporter_name": reporter_name,
                    "is_last_delivery": is_last_delivery,
                    "order_plan_id": order_plan_id,
                    "is_last_truck_for_order_plan": is_last_truck_for_order_plan,
                    "voucher_images": data.get('voucher_images'),  # 新增字段
                    "contract_truck_info": {
                        "contract_total_trucks": match_result['contract_total_trucks'],
                        "contract_used_trucks": match_result['contract_used_trucks'],
                        "contract_remaining_trucks": match_result['contract_remaining_trucks'],
                        "this_delivery_trucks": match_result['this_delivery_trucks']
                    },
                    "order_plan_truck_info": order_plan_truck_info,
                    "operations": operations
                }
            }

            # 如果有跳过的合同，添加匹配过程信息
            if match_result.get('skipped_contracts'):
                response_data["data"]["match_process"] = {
                    "total_matched_contracts": match_result.get('total_matched', 0),
                    "matched_index": match_result.get('matched_index', 1),
                    "skipped_count": len(match_result['skipped_contracts']),
                    "skipped_contracts": match_result['skipped_contracts']
                }
                response_data["message"] += f"（已跳过{len(match_result['skipped_contracts'])}个车数不足的合同）"

            return response_data

        except Exception as e:
            # 异常时清理已保存的临时文件
            for f in temp_files:
                try:
                    if os.path.exists(f):
                        os.remove(f)
                except:
                    pass
            logger.exception(f"【DEBUG】创建报货订单异常: {e}")
            return {"success": False, "error": str(e)}

    def update_delivery(
            self,
            delivery_id: int,
            data: Dict,
            delivery_order_image: bytes = None,
            voucher_images: List[bytes] = None,
            delete_image: bool = False,
            uploaded_by: str = None,
            current_user: dict = None  # 新增参数
    ) -> Dict[str, Any]:
        """更新报货订单（支持替换凭证图片列表）"""
        temp_new_files = []
        old_images_to_delete = []

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # 查询原记录
                    cur.execute(
                        """SELECT has_delivery_order, delivery_order_image, upload_status,
                                  driver_phone, driver_id_card, planned_trucks, contract_no,
                                  voucher_images, vehicle_no
                           FROM pd_deliveries WHERE id = %s""",
                        (delivery_id,)
                    )
                    old = cur.fetchone()
                    if not old:
                        return {"success": False, "error": f"订单ID {delivery_id} 不存在"}

                    old = dict(old) if isinstance(old, dict) else {
                        'has_delivery_order': old[0],
                        'delivery_order_image': old[1],
                        'upload_status': old[2],
                        'driver_phone': old[3],
                        'driver_id_card': old[4],
                        'planned_trucks': old[5],
                        'contract_no': old[6],
                        'voucher_images': old[7],
                        'vehicle_no': old[8],
                    }

                    # 解析原凭证图片列表（兼容历史数据中可能存储的布尔或非列表值）
                    old_vouchers = []
                    raw_vouchers = old.get('voucher_images')
                    if raw_vouchers:
                        try:
                            parsed = json.loads(raw_vouchers) if isinstance(raw_vouchers, str) else raw_vouchers
                            old_vouchers = parsed if isinstance(parsed, list) else []
                        except Exception:
                            old_vouchers = []

                    has_order = self._normalize_has_delivery_order(
                        data.get('has_delivery_order', old['has_delivery_order']))
                    if has_order not in ('有', '无'):
                        return {"success": False, "error": "has_delivery_order 仅支持：有/无（或 是/否）"}

                    # ---------- 互斥校验 ----------
                    if has_order == '有':
                        if voucher_images:
                            return {"success": False, "error": "有联单时不能上传凭证图片"}
                    else:  # 无联单
                        if delivery_order_image:
                            return {"success": False, "error": "无联单时不能上传联单图片，请使用凭证图片"}

                    # ---------- 处理图片 ----------
                    new_delivery_image = old['delivery_order_image']
                    new_upload_status = old['upload_status']
                    new_vouchers = old_vouchers.copy() if isinstance(old_vouchers, list) else []

                    # 处理联单图片（有联单时）
                    if has_order == '有':
                        if delete_image and old['delivery_order_image']:
                            old_images_to_delete.append(old['delivery_order_image'])
                            new_delivery_image = None
                            new_upload_status = '待上传'
                        if delivery_order_image:
                            path = self._save_delivery_image(delivery_order_image,
                                                             data.get('vehicle_no') or old.get('vehicle_no'))
                            temp_new_files.append(path)
                            if old['delivery_order_image']:
                                old_images_to_delete.append(old['delivery_order_image'])
                            new_delivery_image = path
                            new_upload_status = '已上传'
                        # 凭证图片置空
                        new_vouchers = []
                    else:  # 无联单
                        # 联单图片相关字段置空
                        if old['delivery_order_image']:
                            old_images_to_delete.append(old['delivery_order_image'])
                        new_delivery_image = None

                        # 处理凭证图片：如果提供了新列表，则整体替换
                        if voucher_images is not None:
                            # 删除旧凭证图片文件
                            for p in list(old_vouchers):
                                if p and os.path.exists(p):
                                    old_images_to_delete.append(p)
                            # 保存新凭证图片
                            new_paths = []
                            for idx, img_bytes in enumerate(voucher_images):
                                if len(new_paths) >= 6:
                                    break  # 最多6张
                                path = self._save_voucher_image(img_bytes,
                                                                data.get('vehicle_no') or old.get('vehicle_no'), idx)
                                temp_new_files.append(path)
                                new_paths.append(path)
                            new_vouchers = new_paths
                            new_upload_status = '待上传'
                        # 如果没有提供 voucher_images，则保持原有凭证列表（不修改）

                    # ========== 新增：处理品种字段映射 ==========
                    if 'product_name' in data:
                        data['product_name'] = self._convert_to_mill_product(data['product_name'])
                    
                    if 'products' in data:
                        # 解析品种列表
                        raw_products = data['products']
                        if isinstance(raw_products, str):
                            product_list = [p.strip() for p in raw_products.split(',') if p.strip()]
                        elif isinstance(raw_products, list):
                            product_list = raw_products
                        else:
                            product_list = []
                        
                        # 映射每个品种
                        mapped_list = [self._convert_to_mill_product(p) for p in product_list]
                        data['products'] = ','.join(mapped_list) if mapped_list else None
                    # =========================================

                    if 'status' in data:
                        user_role = current_user.get("role") if current_user else None
                        # 允许修改状态的角色：审核主管、管理员
                        if user_role not in ["审核主管", "管理员"]:
                            return {
                                "success": False,
                                "error": "无权修改报单状态，仅审核主管或管理员可操作"
                            }
                    # 准备更新数据
                    update_data = {
                        'has_delivery_order': has_order,
                        'delivery_order_image': new_delivery_image,
                        'upload_status': new_upload_status,
                        'voucher_images': json.dumps(new_vouchers) if new_vouchers else None,
                    }

                    # 合并用户传入的其他字段
                    for key in ['report_date', 'warehouse', 'target_factory_id', 'target_factory_name',
                                'product_name', 'quantity', 'vehicle_no', 'driver_name', 'driver_phone',
                                'driver_id_card', 'shipper', 'payee', 'service_fee', 'contract_no',
                                'contract_unit_price', 'total_amount', 'status', 'reporter_id', 'reporter_name','position']:
                        if key in data:
                            update_data[key] = data[key]

                    # 如果修改了数量，重新计算车数
                    if 'quantity' in data:
                        new_quantity = Decimal(str(data['quantity']))
                        update_data['planned_trucks'] = self._calculate_trucks(new_quantity)

                    # 构建更新SQL
                    fields = list(update_data.keys())
                    set_clause = ', '.join([f"{f}=%s" for f in fields])
                    params = [update_data[f] for f in fields]
                    params.append(delivery_id)
                    cur.execute(f"UPDATE pd_deliveries SET {set_clause} WHERE id = %s", tuple(params))

                    # 删除旧图片文件
                    for p in old_images_to_delete:
                        try:
                            if os.path.exists(p):
                                os.remove(p)
                        except Exception as e:
                            logger.warning(f"删除旧图片失败: {e}")

                    # 合同编号变更时重新同步品类单价表
                    if "contract_no" in data:
                        try:
                            sr = get_delivery_contract_price_service().sync_from_contract(delivery_id)
                            if not sr.get("success"):
                                logger.info(
                                    "update_delivery: 合同品类单价未同步 id=%s reason=%s",
                                    delivery_id,
                                    sr.get("error"),
                                )
                        except Exception as ex:
                            logger.warning(
                                "update_delivery: 同步合同品类单价异常 id=%s %s",
                                delivery_id,
                                ex,
                            )

                    # 返回结果
                    operations = self._build_operations(has_order, new_upload_status, new_delivery_image)
                    return {
                        "success": True,
                        "message": "更新成功",
                        "data": {
                            "id": delivery_id,
                            "has_delivery_order": has_order,
                            "upload_status": new_upload_status,
                            "delivery_order_image": new_delivery_image,
                            "voucher_images": new_vouchers,
                            "operations": operations
                        }
                    }

        except Exception as e:
            # 清理临时新文件
            for f in temp_new_files:
                try:
                    if os.path.exists(f):
                        os.remove(f)
                except:
                    pass
            logger.error(f"更新报货订单失败: {e}")
            return {"success": False, "error": str(e)}

    # delivery_service.py - class DeliveryService

    def _delete_unuploaded_weighbills_for_delivery(self, delivery_id: int) -> None:
        """报单审核驳回时删除尚未实际上传的磅单占位记录（仅待上传）。"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        DELETE FROM pd_weighbills
                        WHERE delivery_id = %s AND upload_status = '待上传'
                        """,
                        (delivery_id,),
                    )
        except Exception as e:
            logger.warning("删除待上传磅单占位失败 delivery_id=%s: %s", delivery_id, e)

    def audit_delivery(self, delivery_id: int, new_status: str, current_user: dict) -> Dict[str, Any]:
        """
        审核报单，修改审核状态（仅限审核主管/管理员）
        """
        user_role = current_user.get("role") if current_user else None
        if user_role not in ["审核主管", "管理员"]:
            return {"success": False, "error": "无权审核报单"}

        # 可选：限制允许修改的状态值
        valid_status = ['审核通过', '审核未通过']
        if new_status not in valid_status:
            return {
                "success": False,
                "error": f"无效状态，可选：{valid_status}"
            }

        # 调用通用更新方法（只传递 status 字段）
        result = self.update_delivery(
            delivery_id,
            data={'status': new_status},
            current_user=current_user
        )
        if result.get("success") and new_status == "审核未通过":
            self._delete_unuploaded_weighbills_for_delivery(delivery_id)
        return result
    def add_voucher_images(self, delivery_id: int, image_bytes_list: List[bytes], vehicle_no: str = None) -> Dict[
        str, Any]:
        """向指定订单追加凭证图片（最多6张）"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # 查询现有凭证
                    cur.execute(
                        "SELECT has_delivery_order, voucher_images, vehicle_no FROM pd_deliveries WHERE id = %s",
                        (delivery_id,))
                    row = cur.fetchone()
                    if not row:
                        return {"success": False, "error": "订单不存在"}
                    if row['has_delivery_order'] == '有':
                        return {"success": False, "error": "有联单的订单不能上传凭证图片"}
                    if isinstance(row, dict):
                        existing = row.get('voucher_images')
                        vehicle_no = vehicle_no or row.get('vehicle_no')
                    else:
                        existing = row[0]
                        vehicle_no = vehicle_no or row[1]

                    current_vouchers = []
                    if existing:
                        try:
                            parsed = json.loads(existing) if isinstance(existing, str) else existing
                            current_vouchers = parsed if isinstance(parsed, list) else []
                        except Exception:
                            current_vouchers = []

                    # 检查数量
                    if len(current_vouchers) + len(image_bytes_list) > 6:
                        return {"success": False, "error": f"凭证图片总数不能超过6张，当前已有{len(current_vouchers)}张"}

                    # 保存新图片
                    new_paths = []
                    base_idx = len(current_vouchers)
                    for idx, img_bytes in enumerate(image_bytes_list):
                        path = self._save_voucher_image(img_bytes, vehicle_no, base_idx + idx)
                        new_paths.append(path)
                        current_vouchers.append(path)

                    # 更新数据库
                    cur.execute(
                        "UPDATE pd_deliveries SET voucher_images = %s WHERE id = %s",
                        (json.dumps(current_vouchers), delivery_id)
                    )
                    conn.commit()
                    return {"success": True, "message": "追加成功", "voucher_images": current_vouchers}
        except Exception as e:
            logger.error(f"追加凭证图片失败: {e}")
            return {"success": False, "error": str(e)}

    def remove_voucher_image(self, delivery_id: int, index: int) -> Dict[str, Any]:
        """删除指定索引的凭证图片（0-based）"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT voucher_images FROM pd_deliveries WHERE id = %s", (delivery_id,))
                    row = cur.fetchone()
                    if not row:
                        return {"success": False, "error": "订单不存在"}
                    if isinstance(row, dict):
                        existing = row.get('voucher_images')
                    else:
                        existing = row[0]

                    if not existing:
                        return {"success": False, "error": "没有凭证图片可删除"}

                    try:
                        parsed = json.loads(existing) if isinstance(existing, str) else existing
                        vouchers = parsed if isinstance(parsed, list) else []
                    except Exception:
                        vouchers = []

                    if not vouchers:
                        return {"success": False, "error": "没有凭证图片可删除"}

                    if index < 0 or index >= len(vouchers):
                        return {"success": False, "error": f"索引 {index} 超出范围，当前共 {len(vouchers)} 张"}

                    # 删除文件
                    path_to_delete = vouchers.pop(index)
                    if path_to_delete and os.path.exists(path_to_delete):
                        os.remove(path_to_delete)

                    # 更新数据库
                    new_value = json.dumps(vouchers) if vouchers else None
                    cur.execute(
                        "UPDATE pd_deliveries SET voucher_images = %s WHERE id = %s",
                        (new_value, delivery_id)
                    )
                    conn.commit()
                    return {"success": True, "message": "删除成功", "voucher_images": vouchers}
        except Exception as e:
            logger.error(f"删除凭证图片失败: {e}")
            return {"success": False, "error": str(e)}

    def get_voucher_images(self, delivery_id: int) -> List[str]:
        """获取凭证图片路径列表"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT voucher_images FROM pd_deliveries WHERE id = %s", (delivery_id,))
                    row = cur.fetchone()
                    if not row:
                        return []
                    val = row[0] if not isinstance(row, dict) else row.get('voucher_images')
                    if not val:
                        return []
                    try:
                        parsed = json.loads(val) if isinstance(val, str) else val
                        return parsed if isinstance(parsed, list) else []
                    except Exception:
                        return []
        except Exception as e:
            logger.error(f"获取凭证图片失败: {e}")
            return []

    def batch_update_delivery_images(self, items: List[Dict], uploaded_by: str) -> List[Dict]:
        """
        批量更新报单联单图片（复用数据库连接，提高性能）

        参数:
            items: 上传项列表，每项包含：
                - delivery_id: 报单ID
                - image_bytes: 图片字节数据
                - has_delivery_order: 联单状态（有/无）
            uploaded_by: 上传者身份（司机/公司）

        返回:
            结果列表，每项包含 success、delivery_id、image_path 等
        """
        results = []

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    for idx, item in enumerate(items):
                        delivery_id = item.get('delivery_id')
                        image_bytes = item.get('image_bytes')
                        has_order = item.get('has_delivery_order', '有')

                        try:
                            # 查询原报单
                            cur.execute(
                                """SELECT has_delivery_order, delivery_order_image, upload_status, vehicle_no 
                                   FROM pd_deliveries WHERE id = %s""",
                                (delivery_id,)
                            )
                            old = cur.fetchone()

                            if not old:
                                results.append({
                                    "index": idx,
                                    "delivery_id": delivery_id,
                                    "success": False,
                                    "error": f"报单ID {delivery_id} 不存在"
                                })
                                continue

                            # 统一转换为字典（兼容不同 cursor 类型）
                            if not isinstance(old, dict):
                                old = {
                                    'has_delivery_order': old[0],
                                    'delivery_order_image': old[1],
                                    'upload_status': old[2],
                                    'vehicle_no': old[3]
                                }

                            # 检查是否已上传
                            if old.get('upload_status') == '已上传':
                                results.append({
                                    "index": idx,
                                    "delivery_id": delivery_id,
                                    "success": False,
                                    "error": "已上传联单，请使用修改接口",
                                    "image_path": old.get('delivery_order_image'),
                                    "upload_status": "已上传"
                                })
                                continue

                            # 保存图片文件
                            safe_name = re.sub(r'[^\w\-]', '_', str(old.get('vehicle_no', delivery_id)))
                            filename = f"delivery_{safe_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}.jpg"
                            file_path = UPLOAD_DIR / filename

                            with open(file_path, "wb") as f:
                                f.write(image_bytes)

                            # 计算联单费
                            service_fee = self._calculate_service_fee(has_order)

                            # 确定来源类型
                            source_type = self._determine_source_type(has_order, uploaded_by)

                            # 更新数据库
                            cur.execute("""
                                UPDATE pd_deliveries 
                                SET has_delivery_order = %s,
                                    delivery_order_image = %s,
                                    upload_status = '已上传',
                                    source_type = %s,
                                    service_fee = %s,
                                    uploaded_at = NOW(),
                                    updated_at = NOW()
                                WHERE id = %s
                            """, (
                                has_order,
                                str(file_path),
                                source_type,
                                service_fee,
                                delivery_id
                            ))

                            results.append({
                                "index": idx,
                                "delivery_id": delivery_id,
                                "success": True,
                                "message": "联单上传成功",
                                "image_path": str(file_path),
                                "upload_status": "已上传",
                                "service_fee": float(service_fee),
                                "source_type": source_type
                            })

                        except Exception as e:
                            logger.error(f"批量上传第{idx}项失败: {e}")
                            results.append({
                                "index": idx,
                                "delivery_id": delivery_id,
                                "success": False,
                                "error": str(e)
                            })

                    # 统一提交事务
                    conn.commit()

        except Exception as e:
            logger.error(f"批量更新数据库连接失败: {e}")
            # 标记所有未完成的为失败
            for i, r in enumerate(results):
                if 'success' not in r:
                    results[i] = {
                        "index": r.get('index', i),
                        "delivery_id": r.get('delivery_id'),
                        "success": False,
                        "error": f"数据库连接失败: {str(e)}"
                    }
            # 为未处理的项添加失败结果
            processed_indices = {r['index'] for r in results if 'index' in r}
            for idx in range(len(items)):
                if idx not in processed_indices:
                    results.append({
                        "index": idx,
                        "delivery_id": items[idx].get('delivery_id'),
                        "success": False,
                        "error": f"数据库连接失败: {str(e)}"
                    })

        return results

    def get_delivery(self, delivery_id: int) -> Optional[Dict]:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM pd_deliveries WHERE id = %s", (delivery_id,))
                    row = cur.fetchone()
                    if not row:
                        return None

                    data = dict(row) if isinstance(row, dict) else {desc[0]: row[idx] for idx, desc in
                                                                    enumerate(cur.description)}

                    # 解析 voucher_images（兼容历史非列表或布尔值）
                    raw_vouchers = data.get('voucher_images')
                    if raw_vouchers:
                        try:
                            parsed = json.loads(raw_vouchers) if isinstance(raw_vouchers, str) else raw_vouchers
                            data['voucher_images'] = parsed if isinstance(parsed, list) else []
                        except Exception:
                            data['voucher_images'] = []
                    else:
                        data['voucher_images'] = []

                    for key in ['report_date', 'created_at', 'updated_at', 'uploaded_at']:
                        if data.get(key):
                            data[key] = str(data[key])

                    # 解析品种列表
                    if data.get('products'):
                        data['products'] = [p.strip() for p in data['products'].split(',') if p.strip()]
                    else:
                        data['products'] = [data.get('product_name')] if data.get('product_name') else []

                    data["has_delivery_order_display"] = '是' if data.get('has_delivery_order') == '有' else '否'
                    data["upload_status_display"] = '是' if data.get('upload_status') == '已上传' else '否'

                    if data.get('service_fee'):
                        data['service_fee'] = float(data['service_fee'])

                    # 查询关联的磅单
                    cur.execute("""
                        SELECT id, product_name, is_last_truck_for_contract,
                               net_weight, upload_status, ocr_status, weigh_date
                        FROM pd_weighbills
                        WHERE delivery_id = %s
                        ORDER BY product_name
                    """, (delivery_id,))

                    weighbills = []
                    for w_row in cur.fetchall():
                        wb = dict(w_row) if isinstance(w_row, dict) else {
                            'id': w_row[0],
                            'product_name': w_row[1],
                            'is_last_truck_for_contract': w_row[2],
                            'net_weight': w_row[3],
                            'upload_status': w_row[4],
                            'ocr_status': w_row[5],
                            'weigh_date': w_row[6],
                        }
                        weighbill = {
                            'id': wb.get('id'),
                            'product_name': wb.get('product_name'),
                            'is_last_for_contract': bool(wb.get('is_last_truck_for_contract')),
                            'net_weight': float(wb.get('net_weight')) if wb.get('net_weight') else None,
                            'status': wb.get('ocr_status') or wb.get('upload_status')
                        }
                        weighbills.append(weighbill)

                    data['weighbills'] = weighbills
                    data['is_last_for_contract'] = any(w['is_last_for_contract'] for w in weighbills)

                    data['operations'] = self._build_operations(
                        data.get('has_delivery_order'),
                        data.get('upload_status'),
                        data.get('delivery_order_image')
                    )
                    if 'position' not in data:
                        data['position'] = None
                    # ========== 新增部分：处理 PDF 字段 ==========
                    # 添加一个布尔标志，方便前端判断
                    data['has_pdf'] = bool(data.get('delivery_order_pdf'))
                    # 如果希望前端直接获取 PDF 文件名，也可以添加：
                    if data.get('delivery_order_pdf'):
                        data['pdf_filename'] = os.path.basename(data['delivery_order_pdf'])
                    # ========== 新增结束 ==========

                    return data
        except Exception as e:
            logger.error(f"查询订单失败: {e}")
            return None

    def list_deliveries(
            self,
            exact_delivery_id: int = None,
            exact_shipper: str = None,
            exact_contract_no: str = None,
            exact_report_date: str = None,
            exact_driver_name: str = None,
            exact_vehicle_no: str = None,
            exact_has_delivery_order: str = None,
            exact_upload_status: str = None,
            exact_reporter_name: str = None,
            exact_reporter_id: int = None,
            exact_factory_name: str = None,
            exact_status: str = None,
            exact_driver_phone: str = None,
            fuzzy_keywords: str = None,
            date_from: str = None,
            date_to: str = None,
            page: int = 1,
            page_size: int = 20
    ) -> Dict[str, Any]:
        """查询订单列表"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    where_clauses = []
                    params = []

                    if exact_delivery_id is not None:
                        where_clauses.append("id = %s")
                        params.append(exact_delivery_id)

                    if exact_shipper:
                        where_clauses.append("shipper = %s")
                        params.append(exact_shipper)

                    if exact_contract_no:
                        where_clauses.append("contract_no = %s")
                        params.append(exact_contract_no)

                    if exact_report_date:
                        where_clauses.append("report_date = %s")
                        params.append(exact_report_date)

                    if exact_driver_name:
                        where_clauses.append("driver_name = %s")
                        params.append(exact_driver_name)

                    if exact_vehicle_no:
                        where_clauses.append("vehicle_no = %s")
                        params.append(exact_vehicle_no)

                    if exact_has_delivery_order:
                        where_clauses.append("has_delivery_order = %s")
                        params.append(exact_has_delivery_order)

                    normalized_upload_status = self._normalize_upload_status(exact_upload_status)
                    if normalized_upload_status:
                        where_clauses.append("upload_status = %s")
                        params.append(normalized_upload_status)

                    if exact_reporter_name:
                        where_clauses.append("reporter_name = %s")
                        params.append(exact_reporter_name)

                    if exact_reporter_id is not None:
                        where_clauses.append("reporter_id = %s")
                        params.append(exact_reporter_id)

                    if exact_factory_name:
                        where_clauses.append("target_factory_name = %s")
                        params.append(exact_factory_name)

                    if exact_status:
                        where_clauses.append("status = %s")
                        params.append(exact_status)

                    if exact_driver_phone:
                        where_clauses.append("driver_phone = %s")
                        params.append(exact_driver_phone)

                    if fuzzy_keywords:
                        tokens = [t for t in fuzzy_keywords.split() if t]
                        or_clauses = []
                        for token in tokens:
                            like = f"%{token}%"
                            or_clauses.append(
                                "(vehicle_no LIKE %s OR driver_name LIKE %s OR driver_phone LIKE %s "
                                "OR target_factory_name LIKE %s OR product_name LIKE %s OR contract_no LIKE %s "
                                "OR reporter_name LIKE %s OR shipper LIKE %s)")
                            params.extend([like, like, like, like, like, like, like, like])
                        if or_clauses:
                            where_clauses.append("(" + " OR ".join(or_clauses) + ")")

                    if date_from:
                        where_clauses.append("report_date >= %s")
                        params.append(date_from)

                    if date_to:
                        where_clauses.append("report_date <= %s")
                        params.append(date_to)

                    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

                    cur.execute(f"SELECT COUNT(*) as total FROM pd_deliveries {where_sql}", tuple(params))
                    total = cur.fetchone()["total"]

                    offset = (page - 1) * page_size
                    cur.execute(f"""
                        SELECT * FROM pd_deliveries 
                        {where_sql}
                        ORDER BY created_at DESC
                        LIMIT %s OFFSET %s
                    """, tuple(params + [page_size, offset]))

                    rows = cur.fetchall()
                    data = []
                    for row in rows:
                        item = dict(row)
                        # 解析 voucher_images（兼容历史非列表或布尔值）
                        raw_vouchers = item.get('voucher_images')
                        if raw_vouchers:
                            try:
                                parsed = json.loads(raw_vouchers) if isinstance(raw_vouchers, str) else raw_vouchers
                                item['voucher_images'] = parsed if isinstance(parsed, list) else []
                            except Exception:
                                item['voucher_images'] = []
                        else:
                            item['voucher_images'] = []
                        for key in ['report_date', 'created_at', 'updated_at', 'uploaded_at']:
                            if item.get(key):
                                item[key] = str(item[key])

                        # 解析品种列表
                        if item.get('products'):
                            item['products'] = [p.strip() for p in item['products'].split(',') if p.strip()]
                            item['product_count'] = len(item['products'])
                        else:
                            item['products'] = [item.get('product_name')] if item.get('product_name') else []
                            item['product_count'] = 1

                        item["has_delivery_order_display"] = '是' if item.get('has_delivery_order') == '有' else '否'
                        item["upload_status_display"] = '是' if item.get('upload_status') == '已上传' else '否'

                        if item.get('service_fee'):
                            item['service_fee'] = float(item['service_fee'])

                        item['operations'] = self._build_operations(
                            item.get('has_delivery_order'),
                            item.get('upload_status'),
                            item.get('delivery_order_image')
                        )

                        item['contract_no'] = item.get('contract_no')
                        item['planned_trucks'] = item.get('planned_trucks', 1)

                        data.append(item)

                    _attach_contract_product_prices_to_delivery_rows(data)

                    return {
                        "success": True,
                        "data": data,
                        "total": total,
                        "page": page,
                        "page_size": page_size
                    }

        except Exception as e:
            logger.error(f"查询列表失败: {e}")
            return {"success": False, "error": str(e), "data": [], "total": 0}

    def delete_delivery(self, delivery_id: int) -> Dict[str, Any]:
        """删除订单（级联删除关联磅单）"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # 先删除关联磅单图片
                    cur.execute("SELECT weighbill_image FROM pd_weighbills WHERE delivery_id = %s", (delivery_id,))
                    for row in cur.fetchall():
                        image_path = row.get('weighbill_image') if isinstance(row, dict) else row[0]
                        if image_path and os.path.exists(image_path):
                            try:
                                os.remove(image_path)
                            except:
                                pass

                    # 获取联单图片路径
                    cur.execute("SELECT delivery_order_image FROM pd_deliveries WHERE id = %s", (delivery_id,))
                    row = cur.fetchone()

                    image_path = row.get('delivery_order_image') if isinstance(row, dict) else (row[0] if row else None)
                    if image_path:
                        cur.execute("DELETE FROM pd_deliveries WHERE id = %s", (delivery_id,))

                        if os.path.exists(image_path):
                            try:
                                os.remove(image_path)
                            except Exception as e:
                                logger.warning(f"删除图片文件失败: {e}")
                    else:
                        cur.execute("DELETE FROM pd_deliveries WHERE id = %s", (delivery_id,))

                    return {"success": True, "message": "删除成功"}

        except Exception as e:
            logger.error(f"删除订单失败: {e}")
            return {"success": False, "error": str(e)}

    # ============ 新增：文本提取和合同匹配方法 ============
        
    def extract_from_text(self, text: str) -> Dict[str, Any]:
        
        """
        使用通义千问API从非结构化文本中提取报货订单字段
        """
        if not text or not isinstance(text, str):
            return {}

        # 将换行符、制表符等替换为空格，避免破坏 JSON
        cleaned_text = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
        # 合并多个空格为单个空格
        cleaned_text = ' '.join(cleaned_text.split())

        # 调用通义千问API提取信息
        try:
            completion = self.client.chat.completions.create(
                model="qwen-vl-plus",
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": f"""请从以下报单文本中提取关键信息，以JSON格式返回：
                                
            提取字段：
            - vehicle_no: 车牌号（标准7位，格式如豫U12345、京A88888，省简称+字母+5位）
            - driver_name: 司机姓名（可为2字、3字、4字或更多，请完整识别勿截断）
            - driver_phone: 司机手机号（11位）
            - driver_id_card: 身份证号（18位，末位可为数字或X）
            - products: 货物品类列表（最多4个；如["电动","通信"]）
            - product_name: 主品种（若识别到多个，取第一个；与 products[0] 保持一致）
            - has_delivery_order: 是否**随车自带纸质联单**（有/无/需办）。注意：
              ·「无联单」「不带联单」「没有联单」「走公司凭证」「仅凭证」「需要做联单」「待办联单」均为**无**（司机未随车带联单或走公司侧流程）。
              · 仅当明确写「有联单」「自带联单」「随车联单」等才填「有」。
              ·「需办」仅用于明确写后续要去办理、且与「有联单」易混的短句；不要因「已上传凭证」填「有」。
            - target_factory_name: 目标工厂（金利、豫光、万洋、大华、金凤、南方、中原、华铂等）

            报单文本：
            {text}

            请仅返回JSON格式数据，不要包含其他说明。示例：
            {{
                "vehicle_no": "豫U12345",
                "driver_name": "张三",
                "driver_phone": "13800138000",
                "driver_id_card": "410881199001011234",
                "products": ["电动", "通信"],
                "product_name": "电动",
                "has_delivery_order": "无",
                "target_factory_name": "金利"
            }}"""
                                        },
                                    ],
                                },
                            ],
                        )
            
            # 解析API返回结果
            content = completion.choices[0].message.content
            
            # 尝试从返回内容中提取JSON
            import json
            import re
            
            # 查找JSON块
            json_match = re.search(r'\{.*\}', content, re.DOTALL)
            if json_match:
                result = json.loads(json_match.group())
            else:
                result = json.loads(content)
            
            # 清理和验证提取的数据
            result = self._clean_extracted_data(result)
            
            # 设置默认值
            if 'target_factory_name' not in result or not result['target_factory_name']:
                result['target_factory_name'] = '金利'
            products = self._parse_products(result.get('products'), result.get('product_name'))
            if not products:
                products = ['普通']
            result['products'] = products
            result['product_name'] = products[0]
            if 'has_delivery_order' not in result or not result['has_delivery_order']:
                result['has_delivery_order'] = '无'
                
            return result
            
        except Exception as e:
            logger.error(f"API提取信息失败: {e}")
            # 降级到空结果
            return {
                'target_factory_name': '金利',
                'products': ['普通'],
                'product_name': '普通',
                'has_delivery_order': '无'
            }
    def _clean_extracted_data(self, data: Dict) -> Dict:
        """清理和验证提取的数据"""
        result = {}
        result['warnings'] = []
        
        # 车牌号：标准7位（省简称+字母+5位），也支持新能源8位
        if data.get('vehicle_no'):
            plate = str(data['vehicle_no']).strip().upper()
            if re.match(r'[京津沪渝冀豫云辽黑湘皖鲁新苏浙赣鄂桂甘晋蒙陕吉闽贵粤青藏川宁琼][A-Z][A-Z0-9]{5,6}', plate):
                result['vehicle_no'] = plate
        
        # 司机姓名：支持2字、3字、4字或更多
        if data.get('driver_name'):
            name = str(data['driver_name']).strip()
            if len(name) >= 1 and re.match(r'^[\u4e00-\u9fa5a-zA-Z·]+$', name):
                result['driver_name'] = name
        
        # 手机号
        if data.get('driver_phone'):
            phone = re.sub(r'\D', '', str(data['driver_phone']))
            if len(phone) == 11 and phone.startswith(('13', '14', '15', '16', '17', '18', '19')):
                result['driver_phone'] = phone
        
        # 身份证号：不全補0，位數過多截斷並提示
        if data.get('driver_id_card'):
            normalized, card_warnings = self._normalize_driver_id_card_with_warnings(data['driver_id_card'])
            if normalized:
                result['driver_id_card'] = normalized
                result['warnings'].extend(card_warnings)
        
        # 品种：支持识别多个品类，返回 products 列表 + product_name（首个）
        products = self._parse_products(data.get('products'), data.get('product_name'))
        if products:
            result['products'] = products
            result['product_name'] = products[0]
        
        # 联单状态标准化：优先识别「无联单」类长句，禁止用子串「有」误判（如「需要做联单」）；
        # 「已上传」单独出现多为凭证上传，不得等同「有联单」。
        if data.get('has_delivery_order'):
            order_status = str(data['has_delivery_order']).strip()
            osn = order_status.replace(" ", "").replace("　", "")
            low = order_status.lower()

            no_slip_phrases = (
                "无联单",
                "没有联单",
                "不带联单",
                "未带联单",
                "无纸质联单",
                "不需联单",
                "无需联单",
                "走凭证",
                "公司凭证",
                "仅凭证",
                "只要凭证",
                "凭证报单",
                "需要做联单",
                "须做联单",
                "待做联单",
                "待办联单",
                "办理联单手续",
            )
            yes_slip_phrases = ("有联单", "自带联单", "随车联单", "随车带联单", "纸质联单")

            if any(p in osn for p in no_slip_phrases):
                result["has_delivery_order"] = "无"
            elif any(p in osn for p in yes_slip_phrases):
                result["has_delivery_order"] = "有"
            elif "已上传" in osn and "联单" in osn and not any(
                p in osn for p in ("无联单", "没有", "不带", "未")
            ):
                result["has_delivery_order"] = "有"
            elif order_status in {"需办", "待办", "办理"} or osn in ("需办联单",):
                result["has_delivery_order"] = "需办"
            else:
                positive_exact = {"有", "是", "自带", "true", "1", "yes"}
                negative_exact = {"无", "否", "没有", "不带", "false", "0", "no"}
                if order_status in positive_exact or low in positive_exact:
                    result["has_delivery_order"] = "有"
                elif order_status in negative_exact or low in negative_exact:
                    result["has_delivery_order"] = "无"
                else:
                    # 模型输出含糊时默认无联单，避免多收联单费
                    result["has_delivery_order"] = "无"
        
        # 目标工厂
        if data.get('target_factory_name'):
            factory = str(data['target_factory_name']).strip()
            valid_factories = ['金利', '豫光', '万洋', '大华', '金凤', '南方', '中原', '华铂']
            for f in valid_factories:
                if f in factory:
                    result['target_factory_name'] = f
                    break
        
        return result
    def validate_extracted(self, data: Dict[str, Optional[str]]) -> Dict[str, any]:
        """验证提取的数据完整性"""
        required_fields = ['vehicle_no', 'driver_name', 'driver_phone']
        missing = [f for f in required_fields if not data.get(f)]
        
        if data.get('driver_id_card'):
            id_card = data['driver_id_card']
            if len(id_card) != 18 or not re.match(r'^\d{17}[\dXx]$', id_card):
                data['driver_id_card_error'] = '身份證號格式不正確（應為18位）'
        
        if data.get('vehicle_no'):
            plate = data['vehicle_no']
            if not re.match(r'[京津沪渝冀豫云辽黑湘皖鲁新苏浙赣鄂桂甘晋蒙陕吉闽贵粤青藏川宁琼][A-Z][A-Z0-9]{5,6}', plate):
                data['vehicle_no_error'] = '車牌號格式不正確（標準7位：省+字母+5位）'
        
        return {
            'is_valid': len(missing) == 0,
            'missing_fields': missing,
            'data': data
        }

    def match_contract_by_factory_and_product(
        self, 
        factory_name: Optional[str], 
        product_name: Optional[str],
        report_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """根据工厂名称和品种匹配合同"""
        if not factory_name or not product_name:
            return {
                'matched': False,
                'contract_no': None,
                'contract_id': None,
                'unit_price': None,
                'smelter_company': None,
                'match_type': 'none',
                'reason': '工厂名称或品种为空'
            }
        
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    effective_date = report_date or datetime.today().date().isoformat()
                    
                    # 工厂别名映射
                    factory_keywords = [factory_name]
                    factory_aliases = {
                        '金利': ['金利', '河南金利', '金利金铅'],
                        '豫光': ['豫光', '河南豫光', '豫光金铅'],
                        '万洋': ['万洋', '河南万洋'],
                        '大华': ['大华', '河北大华'],
                        '金凤': ['金凤', '河北金凤'],
                        '南方': ['南方', '广东南方'],
                        '中原': ['中原', '河南中原'],
                        '华铂': ['华铂', '安徽华铂'],
                    }
                    
                    for key, aliases in factory_aliases.items():
                        if key in factory_name:
                            factory_keywords.extend(aliases)
                    
                    factory_keywords = list(set(factory_keywords))
                    
                    # 构建工厂查询条件
                    factory_conditions = []
                    for keyword in factory_keywords:
                        factory_conditions.append("c.smelter_company LIKE %s")
                    
                    factory_sql = " OR ".join(factory_conditions)
                    factory_params = [f"%{k}%" for k in factory_keywords]
                    
                    # 先精确匹配品种
                    sql = f"""
                        SELECT 
                            c.id,
                            c.contract_no,
                            c.smelter_company,
                            p.unit_price,
                            p.product_name as matched_product,
                            c.contract_date,
                            c.end_date
                        FROM pd_contracts c
                        JOIN pd_contract_products p ON p.contract_id = c.id
                        WHERE ({factory_sql})
                        AND p.product_name = %s
                        AND p.unit_price > 0
                        AND c.status = '生效中'
                        AND c.contract_date <= %s
                        AND (c.end_date IS NULL OR c.end_date >= %s)
                        ORDER BY c.contract_date ASC, c.created_at ASC, p.sort_order ASC
                        LIMIT 1
                    """
                    
                    params = factory_params + [product_name, effective_date, effective_date]
                    cur.execute(sql, tuple(params))
                    
                    row = cur.fetchone()
                    
                    if row:
                        contract_id = row['id'] if isinstance(row, dict) else row[0]
                        contract_no = row['contract_no'] if isinstance(row, dict) else row[1]
                        unit_price_val = (row['unit_price'] if isinstance(row, dict) else row[3])
                        contract_date = row.get('contract_date') if isinstance(row, dict) else row[5]
                        logger.debug(f"匹配到合同(exact): id={contract_id}, no={contract_no}, date={contract_date}, unit_price={unit_price_val}")
                        return {
                            'matched': True,
                            'contract_no': contract_no,
                            'contract_id': contract_id,
                            'unit_price': float(unit_price_val) if unit_price_val is not None else None,
                            'smelter_company': row['smelter_company'] if isinstance(row, dict) else row[2],
                            'match_type': 'exact',
                            'matched_product': row['matched_product'] if isinstance(row, dict) else row[4],
                        }
                    
                    # 模糊匹配品种
                    fuzzy_sql = f"""
                        SELECT 
                            c.id,
                            c.contract_no,
                            c.smelter_company,
                            p.unit_price,
                            p.product_name as matched_product,
                            c.contract_date,
                            c.end_date
                        FROM pd_contracts c
                        JOIN pd_contract_products p ON p.contract_id = c.id
                        WHERE ({factory_sql})
                        AND p.product_name LIKE %s
                        AND p.unit_price > 0
                        AND c.status = '生效中'
                        AND c.contract_date <= %s
                        AND (c.end_date IS NULL OR c.end_date >= %s)
                        ORDER BY c.contract_date ASC, c.created_at ASC, p.sort_order ASC
                        LIMIT 1
                    """
                    
                    fuzzy_params = factory_params + [f"%{product_name}%", effective_date, effective_date]
                    cur.execute(fuzzy_sql, tuple(fuzzy_params))
                    
                    row = cur.fetchone()
                    
                    if row:
                        contract_id = row['id'] if isinstance(row, dict) else row[0]
                        contract_no = row['contract_no'] if isinstance(row, dict) else row[1]
                        unit_price_val = (row['unit_price'] if isinstance(row, dict) else row[3])
                        contract_date = row.get('contract_date') if isinstance(row, dict) else row[5]
                        logger.debug(f"匹配到合同(fuzzy): id={contract_id}, no={contract_no}, date={contract_date}, unit_price={unit_price_val}")
                        return {
                            'matched': True,
                            'contract_no': contract_no,
                            'contract_id': contract_id,
                            'unit_price': float(unit_price_val) if unit_price_val is not None else None,
                            'smelter_company': row['smelter_company'] if isinstance(row, dict) else row[2],
                            'match_type': 'fuzzy',
                            'matched_product': row['matched_product'] if isinstance(row, dict) else row[4],
                        }
                    
                    return {
                        'matched': False,
                        'contract_no': None,
                        'contract_id': None,
                        'unit_price': None,
                        'smelter_company': None,
                        'match_type': 'none',
                        'reason': f'未找到工厂[{factory_name}]品种[{product_name}]的生效合同'
                    }
                    
        except Exception as e:
            logger.error(f"合同匹配失败: {e}")
            return {
                'matched': False,
                'contract_no': None,
                'contract_id': None,
                'unit_price': None,
                'smelter_company': None,
                'match_type': 'error',
                'reason': f'匹配异常: {str(e)}'
            }

    def extract_with_contract(
        self, 
        text: str, 
        report_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """提取信息并自动匹配合同"""
        # 1. 提取基础信息
        extracted = self.extract_from_text(text)
        
        # 2. 验证数据
        validation = self.validate_extracted(extracted.copy())

        # ---- 转换品种为冶炼厂品种（支持多个品类）----
        original_products = self._parse_products(extracted.get('products'), extracted.get('product_name'))
        mapped_products: List[str] = []
        for p in original_products:
            mapped = self._convert_to_mill_product(p) if p else None
            if mapped and mapped not in mapped_products:
                mapped_products.append(mapped)
        mapped_products = mapped_products[:4]
        if mapped_products:
            extracted['products'] = mapped_products
            extracted['product_name'] = mapped_products[0]
        mill_product = extracted.get('product_name')
        # ----------------------------------------------
        
        # 3. 匹配合同
        factory_for_match = extracted.get('target_factory_name')
        contract_match = self.match_contract_by_factory_and_product(
            factory_name=factory_for_match,
            product_name=mill_product,
            report_date=report_date
        )

        logger.debug(f"合同匹配结果: factory={factory_for_match}, product={extracted.get('product_name')}, result={contract_match}")
        
        # 4. 组装结果
        result = {
            'success': True,
            'extracted': extracted,
            'validation': validation,
            'contract_match': contract_match,
            'ready_to_create': validation['is_valid'] and contract_match['matched']
        }
        
        # 5. 如果匹配成功，添加合同信息到提取数据
        if contract_match['matched']:
            extracted['contract_no'] = contract_match['contract_no']
            extracted['contract_id'] = contract_match['contract_id']
            extracted['contract_unit_price'] = contract_match['unit_price']
            extracted['smelter_company'] = contract_match['smelter_company']
            result['suggested_data'] = {
                'contract_no': contract_match['contract_no'],
                'target_factory_name': contract_match['smelter_company'],
                'product_name': extracted.get('product_name'),
                'products': ",".join(extracted.get('products') or []),
                'unit_price': contract_match['unit_price'],
            }
        
        return result

    def upload_delivery_pdf(self, delivery_id: int, pdf_bytes: bytes, uploaded_by: str = None) -> Dict[str, Any]:
        """上传联单 PDF 文件，保存路径到 delivery_order_pdf"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # 查询报单（只需 vehicle_no 用于生成文件名）
                    cur.execute("SELECT vehicle_no FROM pd_deliveries WHERE id = %s", (delivery_id,))
                    row = cur.fetchone()
                    if not row:
                        return {"success": False, "error": f"报单ID {delivery_id} 不存在"}
                    vehicle_no = row['vehicle_no'] if isinstance(row, dict) else row[0]

                    # 检查 PDF 是否已存在（可选）
                    cur.execute("SELECT delivery_order_pdf FROM pd_deliveries WHERE id = %s", (delivery_id,))
                    existing = cur.fetchone()
                    existing_pdf = existing['delivery_order_pdf'] if isinstance(existing, dict) else existing[0]
                    if existing_pdf:
                        return {"success": False, "error": "PDF 已存在，如需替换请使用修改接口"}

                    # 保存 PDF 文件
                    safe_name = re.sub(r'[^\w\-]', '_', str(vehicle_no or delivery_id))
                    filename = f"delivery_{safe_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}.pdf"
                    file_path = UPLOAD_DIR / filename
                    with open(file_path, "wb") as f:
                        f.write(pdf_bytes)

                    # 上传 PDF 也属于联单上传，需同步更新上传状态
                    cur.execute("""
                        UPDATE pd_deliveries 
                        SET delivery_order_pdf = %s,
                            upload_status = '已上传',
                            uploaded_at = NOW(),
                            updated_at = NOW()
                        WHERE id = %s
                    """, (str(file_path), delivery_id))
                    conn.commit()

                    return {
                        "success": True,
                        "message": "PDF 上传成功",
                        "data": {
                            "delivery_id": delivery_id,
                            "pdf_path": str(file_path),
                            "upload_status": "已上传"
                        }
                    }
        except Exception as e:
            logger.error(f"上传 PDF 失败: {e}")
            return {"success": False, "error": str(e)}

    def update_delivery_pdf(self, delivery_id: int, pdf_bytes: bytes, uploaded_by: str = None) -> Dict[str, Any]:
        """替换 PDF 文件（覆盖原有）"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # 查询原 PDF 路径
                    cur.execute("SELECT delivery_order_pdf, vehicle_no FROM pd_deliveries WHERE id = %s",
                                (delivery_id,))
                    row = cur.fetchone()
                    if not row:
                        return {"success": False, "error": "订单不存在"}
                    old_pdf = row['delivery_order_pdf'] if isinstance(row, dict) else row[0]
                    vehicle_no = row['vehicle_no'] if isinstance(row, dict) else row[1]

                    # 保存新 PDF
                    safe_name = re.sub(r'[^\w\-]', '_', str(vehicle_no or delivery_id))
                    filename = f"delivery_{safe_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}.pdf"
                    file_path = UPLOAD_DIR / filename
                    with open(file_path, "wb") as f:
                        f.write(pdf_bytes)

                    # 替换 PDF 时也保持上传状态为已上传
                    cur.execute("UPDATE pd_deliveries SET delivery_order_pdf = %s, upload_status = '已上传', uploaded_at = NOW(), updated_at = NOW() WHERE id = %s",
                                (str(file_path), delivery_id))
                    conn.commit()

                    # 删除旧文件
                    if old_pdf and os.path.exists(old_pdf):
                        os.remove(old_pdf)

                    return {"success": True, "message": "PDF 替换成功", "pdf_path": str(file_path), "upload_status": "已上传"}
        except Exception as e:
            logger.error(f"替换 PDF 失败: {e}")
            return {"success": False, "error": str(e)}
        
    def list_deliveries_by_manager(
        self,
        manager_name: str,
        audit_status: str = None,
        date_from: str = None,
        date_to: str = None,
        page: int = 1,
        page_size: int = 20
        ) -> Dict[str, Any]:
        """
        按大区经理查询报单列表
            
        逻辑：通过 position 字段匹配大区经理，或通过 reporter_name 匹配
        """
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    where_clauses = []
                    params = []

                    # 匹配大区经理（通过 position 字段或 reporter_name）
                    where_clauses.append("(position = %s OR reporter_name = %s OR uploader_name = %s)")
                    params.extend([manager_name, manager_name, manager_name])

                    # 审核状态筛选
                    if audit_status:
                        if audit_status == '待审核':
                            where_clauses.append("status = '待审核'")
                        elif audit_status == '已审核':
                            where_clauses.append("status = '审核通过'")

                    if date_from:
                        where_clauses.append("report_date >= %s")
                        params.append(date_from)

                    if date_to:
                        where_clauses.append("report_date <= %s")
                        params.append(date_to)

                    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

                    # 查询总数
                    cur.execute(f"SELECT COUNT(*) as total FROM pd_deliveries {where_sql}", tuple(params))
                    total = cur.fetchone()["total"]

                    # 查询列表
                    offset = (page - 1) * page_size
                    cur.execute(f"""
                        SELECT * FROM pd_deliveries 
                        {where_sql}
                        ORDER BY created_at DESC
                        LIMIT %s OFFSET %s
                    """, tuple(params + [page_size, offset]))

                    rows = cur.fetchall()
                    data = []
                    for row in rows:
                        item = dict(row)
                        
                        # 解析 voucher_images
                        raw_vouchers = item.get('voucher_images')
                        if raw_vouchers:
                            try:
                                parsed = json.loads(raw_vouchers) if isinstance(raw_vouchers, str) else raw_vouchers
                                item['voucher_images'] = parsed if isinstance(parsed, list) else []
                            except Exception:
                                item['voucher_images'] = []
                        else:
                            item['voucher_images'] = []
                        
                        # 格式化日期
                        for key in ['report_date', 'created_at', 'updated_at', 'uploaded_at']:
                            if item.get(key):
                                item[key] = str(item[key])

                        # 解析品种列表
                        if item.get('products'):
                            item['products'] = [p.strip() for p in item['products'].split(',') if p.strip()]
                            item['product_count'] = len(item['products'])
                        else:
                            item['products'] = [item.get('product_name')] if item.get('product_name') else []
                            item['product_count'] = 1

                        # 显示转换
                        item["has_delivery_order_display"] = '是' if item.get('has_delivery_order') == '有' else '否'
                        item["upload_status_display"] = '是' if item.get('upload_status') == '已上传' else '否'

                        if item.get('service_fee'):
                            item['service_fee'] = float(item['service_fee'])

                        # 操作权限
                        item['operations'] = self._build_operations(
                            item.get('has_delivery_order'),
                            item.get('upload_status'),
                            item.get('delivery_order_image')
                        )

                        data.append(item)

                    _attach_contract_product_prices_to_delivery_rows(data)

                    return {
                        "success": True,
                        "data": data,
                        "total": total,
                        "page": page,
                        "page_size": page_size,
                        "manager_name": manager_name,
                        "audit_status": audit_status
                    }

        except Exception as e:
            logger.error(f"按大区经理查询报单失败: {e}")
            return {"success": False, "error": str(e), "data": [], "total": 0}

    def delete_delivery_pdf(self, delivery_id: int) -> Dict[str, Any]:
        """删除 PDF 文件"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT delivery_order_pdf FROM pd_deliveries WHERE id = %s", (delivery_id,))
                    row = cur.fetchone()
                    if not row:
                        return {"success": False, "error": "订单不存在"}
                    pdf_path = row['delivery_order_pdf'] if isinstance(row, dict) else row[0]
                    if not pdf_path:
                        return {"success": False, "error": "该订单没有 PDF 文件"}

                    cur.execute("UPDATE pd_deliveries SET delivery_order_pdf = NULL WHERE id = %s", (delivery_id,))
                    conn.commit()

                    if os.path.exists(pdf_path):
                        os.remove(pdf_path)
                    return {"success": True, "message": "PDF 删除成功"}
        except Exception as e:
            logger.error(f"删除 PDF 失败: {e}")
            return {"success": False, "error": str(e)}

_delivery_service = None

def get_delivery_service():
    global _delivery_service
    if _delivery_service is None:
        _delivery_service = DeliveryService()
    return _delivery_service



