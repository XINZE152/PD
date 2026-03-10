"""
合同录入服务 - 完整版
支持OCR识别、手动录入、查看、编辑、导出
"""
import os
import re
import logging
import tempfile
from decimal import Decimal, ROUND_FLOOR
from typing import List, Dict, Optional, Any, Tuple
from contextlib import contextmanager
from datetime import datetime, timedelta, date

import pymysql
from PIL import Image, ImageEnhance, ImageFilter
from pathlib import Path

try:
    from rapidocr_onnxruntime import RapidOCR
    RAPIDOCR_AVAILABLE = True
except ImportError:
    RAPIDOCR_AVAILABLE = False
    raise ImportError("请安装 RapidOCR：pip install rapidocr-onnxruntime")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PRODUCT_TYPES = ["电动车", "黑皮", "新能源", "通信", "摩托车", "大白", "牵引"]


# ============ 数据库 ============

def get_db_config() -> dict:
    def require_env(name: str) -> str:
        value = os.getenv(name)
        if not value:
            raise ValueError(f"Missing required env var: {name}")
        return value

    return {
        "host": require_env("MYSQL_HOST"),
        "port": int(require_env("MYSQL_PORT")),
        "user": require_env("MYSQL_USER"),
        "password": require_env("MYSQL_PASSWORD"),
        "database": require_env("MYSQL_DATABASE"),
        "charset": os.getenv("MYSQL_CHARSET", "utf8mb4"),
        "autocommit": True,
    }


@contextmanager
def get_conn():
    config = get_db_config()
    connection = pymysql.connect(**config)
    try:
        yield connection
    finally:
        connection.close()


# ============ 核心服务 ============

class ContractService:
    def __init__(self):
        self.ocr = None
        self._init_ocr()

    def _init_ocr(self):
        try:
            self.ocr = RapidOCR()
            logger.info("RapidOCR 初始化成功")
        except Exception as e:
            logger.error(f"RapidOCR 初始化失败: {e}")
            raise

    def recognize_contract(self, image_path: str) -> Dict[str, Any]:
        """OCR识别合同 - 即使不完整也返回结果"""
        try:
            result, elapse = self.ocr(image_path)
            total_elapse = sum(elapse) if isinstance(elapse, list) else float(elapse or 0)

            if not result:
                return {
                    "success": True,
                    "data": {
                        "contract_no": None,
                        "contract_date": None,
                        "end_date": None,
                        "smelter_company": None,
                        "total_quantity": None,
                        "arrival_payment_ratio": 0.9,
                        "final_payment_ratio": 0.1,
                        "products": [],
                        "contract_unit_price": None,
                        "remittance_unit_price": None,
                        "unit_price": None,
                        "ocr_success": False,
                        "ocr_message": "未能识别到任何文本",
                        "raw_text": "",
                        "ocr_time": 0,
                    }
                }

            text_lines = []
            for item in result:
                bbox, text, confidence = item
                text_lines.append({"text": text.strip(), "confidence": float(confidence), "bbox": bbox})

            full_text = "\n".join([line["text"] for line in text_lines])
            full_text = self._fix_common_ocr_errors(full_text)

            logger.info("=== OCR 识别文本 ===")
            for i, line in enumerate(text_lines):
                logger.info(f"{i}: {line['text']}")
            logger.info("===================")

            data = self._parse_contract(text_lines, full_text)
            data["ocr_time"] = round(total_elapse, 3)

            return {"success": True, "data": data}

        except Exception as e:
            logger.error(f"识别异常: {e}")
            return {
                "success": True,
                "data": {
                    "contract_no": None,
                    "contract_date": None,
                    "end_date": None,
                    "smelter_company": None,
                    "total_quantity": None,
                    "arrival_payment_ratio": 0.9,
                    "final_payment_ratio": 0.1,
                    "products": [],
                    "contract_unit_price": None,
                    "remittance_unit_price": None,
                    "unit_price": None,
                    "ocr_success": False,
                    "ocr_message": f"识别异常: {str(e)}",
                    "raw_text": "",
                    "ocr_time": 0,
                }
            }

    def _fix_common_ocr_errors(self, text: str) -> str:
        """修正常见OCR识别错误"""
        corrections = {
            "方：": "甲方：",
            "方:": "甲方:",
            "乙万": "乙方",
            "合司": "合同",
            "编亏": "编号",
            "金辆": "金铅",
        }
        for wrong, right in corrections.items():
            text = text.replace(wrong, right)
        return text

    def _parse_contract(self, text_lines: List[Dict], full_text: str) -> Dict:
        """解析合同信息 - 缺失字段留空"""

        contract_no = self._extract_contract_no(full_text)
        contract_date = self._extract_contract_date(full_text)
        end_date = self._extract_end_date(full_text) or self._infer_end_date(contract_date)
        smelter = self._extract_smelter(full_text)
        arrival_ratio = self._extract_payment_ratio(full_text)

        try:
            products, total_quantity = self._extract_products_multiline(text_lines)
        except Exception as e:
            logger.warning(f"提取产品失败: {e}")
            products = []
            total_quantity = None

        main_price = None
        for p in products:
            if p.get("unit_price") and p["unit_price"] > 0:
                main_price = p["unit_price"]
                break

        truck_count = self._calculate_truck_count(total_quantity)

        return {
            "contract_no": contract_no,
            "contract_date": contract_date,
            "end_date": end_date,
            "smelter_company": smelter,
            "total_quantity": float(total_quantity) if total_quantity else None,
            "truck_count": float(truck_count) if truck_count else None,
            "arrival_payment_ratio": float(arrival_ratio) if arrival_ratio else 0.9,
            "final_payment_ratio": float(Decimal("1") - arrival_ratio) if arrival_ratio else 0.1,
            "products": products if products else [],
            "contract_unit_price": float(main_price) if main_price else None,
            "remittance_unit_price": float(main_price) if main_price else None,
            "unit_price": float(main_price / Decimal("1.3")) if main_price else None,
            "raw_text": full_text,
            "ocr_success": True,
            "ocr_message": self._generate_ocr_message(contract_no, products),
        }

    def _calculate_truck_count(self, total_quantity: Optional[Decimal]) -> Optional[Decimal]:
        if total_quantity is None:
            return None
        try:
            return (Decimal(str(total_quantity)) / Decimal("35")).to_integral_value(rounding=ROUND_FLOOR)
        except Exception:
            return None

    def _generate_ocr_message(self, contract_no, products) -> str:
        """生成OCR结果说明"""
        missing = []
        if not contract_no:
            missing.append("合同编号")
        if not products:
            missing.append("品种表格")

        if missing:
            return f"已识别，以下字段缺失需手动填写: {', '.join(missing)}"
        return "识别完成"

    def _extract_contract_no(self, text: str) -> Optional[str]:
        """提取合同编号"""
        patterns = [
            r"合同编号[：:]\s*([A-Za-z0-9\-]+)",
            r"编号[：:]\s*([A-Za-z0-9\-]+)",
            r"([A-Z]{2,6}-\d{6,12})",
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1).strip()
        return None

    def _extract_contract_date(self, text: str) -> Optional[str]:
        """提取签订日期"""
        patterns = [
            r"签订时间[：:]\s*(\d{4}[-年]\d{1,2}[-月]\d{1,2})",
            r"签订日期[：:]\s*(\d{4}[-年]\d{1,2}[-月]\d{1,2})",
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                date_str = match.group(1).replace("年", "-").replace("月", "-").replace("日", "")
                return date_str
        return None

    def _extract_end_date(self, text: str) -> Optional[str]:
        """提取截止日期"""
        patterns = [
            r"合同期限.*?(\d{4}[-年]\d{1,2}[-月]\d{1,2})",
            r"有效期至[：:]\s*(\d{4}[-年]\d{1,2}[-月]\d{1,2})",
            r"截止日期[：:]\s*(\d{4}[-年]\d{1,2}[-月]\d{1,2})",
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                date_str = match.group(1).replace("年", "-").replace("月", "-").replace("日", "")
                return date_str
        return None

    def _infer_end_date(self, start_date: Optional[str]) -> Optional[str]:
        """根据签订日期推断截止日期（默认5天）"""
        if not start_date:
            return None
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = start + timedelta(days=5)
            return end.strftime("%Y-%m-%d")
        except:
            return None

    def _compute_end_date(self, contract_date: Optional[str]) -> Optional[str]:
        if not contract_date:
            return None

        if isinstance(contract_date, date):
            base = contract_date
        else:
            base = datetime.strptime(str(contract_date), "%Y-%m-%d").date()

        return (base + timedelta(days=5)).strftime("%Y-%m-%d")

    def _normalize_products(self, products: List[Dict]) -> List[tuple]:
        normalized = []
        for product in products or []:
            name = (product.get("product_name") or "").strip()
            if not name:
                continue
            price = product.get("unit_price")
            if price is None or price == "":
                price_val = None
            else:
                price_val = str(Decimal(str(price)).quantize(Decimal("0.01")))
            normalized.append((name, price_val))
        normalized.sort()
        return normalized

    def _find_duplicate_contract(self, data: Dict, products: List[Dict]) -> Optional[int]:
        fields = [
            "contract_date",
            "end_date",
            "smelter_company",
            "total_quantity",
            "arrival_payment_ratio",
            "final_payment_ratio",
            "status",
            "remarks",
        ]
        conditions = []
        params = []
        for field in fields:
            value = data.get(field)
            conditions.append(f"(({field} = %s) OR ({field} IS NULL AND %s IS NULL))")
            params.extend([value, value])

        where_sql = " AND ".join(conditions)
        target_products = self._normalize_products(products)

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT id FROM pd_contracts WHERE {where_sql}",
                        tuple(params),
                    )
                    for row in cur.fetchall():
                        contract_id = row[0]
                        cur.execute(
                            """
                            SELECT product_name, unit_price
                            FROM pd_contract_products
                            WHERE contract_id = %s
                            ORDER BY product_name, unit_price
                            """,
                            (contract_id,),
                        )
                        existing_products = [
                            {"product_name": r[0], "unit_price": r[1]} for r in cur.fetchall()
                        ]
                        if self._normalize_products(existing_products) == target_products:
                            return contract_id
        except Exception as e:
            logger.error(f"合同查重失败: {e}")

        return None

    def _extract_smelter(self, text: str) -> Optional[str]:
        """提取冶炼公司"""
        match = re.search(r"甲方[：:]\s*(.+?)(?:\n|$)", text)
        if match:
            return match.group(1).strip()

        match = re.search(r"交货地点[：:]\s*(.+?)(?:\n|$)", text)
        if match:
            location = match.group(1).strip()
            if "再生铅" in location or "分厂" in location:
                return "河南金利金铅集团有限公司"

        return None

    def _extract_payment_ratio(self, text: str) -> Optional[Decimal]:
        """提取到货款比例"""
        patterns = [
            r"到货款.*?(\d+)%",
            r"付到货款.*?(\d+)%",
            r"(\d+)%.*到货款",
            r"结算付到货款的(\d+)%",
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return Decimal(str(int(match.group(1)) / 100))
        return None

    def _extract_products_multiline(self, text_lines: List[Dict]) -> Tuple[List[Dict], Optional[Decimal]]:
        """提取产品列表和总数量"""
        products = []
        total_quantity = None

        name_start = name_end = price_start = price_end = qty_start = None

        for i, line in enumerate(text_lines):
            text = line["text"]
            if text == "品名":
                name_start = i
            elif "单价" in text and "元" in text and name_start is not None and name_end is None:
                name_end = i
                price_start = i
            elif "数量" in text and "吨" in text and price_start is not None and price_end is None:
                price_end = i
                qty_start = i

        if name_start is not None and name_end is not None:
            names = []
            for i in range(name_start + 1, name_end):
                text = text_lines[i]["text"]
                if text in PRODUCT_TYPES:
                    names.append(text)

            prices = []
            if price_start is not None and price_end is not None:
                for i in range(price_start + 1, price_end):
                    text = text_lines[i]["text"]
                    match = re.match(r'^(\d+\.?\d*)$', text)
                    if match:
                        prices.append(match.group(1))

            if qty_start is not None:
                for i in range(qty_start + 1, len(text_lines)):
                    text = text_lines[i]["text"]
                    match = re.match(r'^(\d+\.?\d*)$', text)
                    if match:
                        val = Decimal(match.group(1))
                        if val >= 50:
                            total_quantity = val
                            break

            for i, name in enumerate(names):
                products.append({
                    "product_name": name,
                    "unit_price": Decimal(prices[i]) if i < len(prices) else Decimal("0"),
                })

        return products, total_quantity

    def preprocess_image(self, image_path: str) -> str:
        """图片预处理"""
        try:
            img = Image.open(image_path)
            if img.mode != "RGB":
                img = img.convert("RGB")

            enhancer = ImageEnhance.Contrast(img)
            img = enhancer.enhance(1.5)
            img = img.filter(ImageFilter.SHARPEN)

            max_size = 2000
            if max(img.size) > max_size:
                ratio = max_size / max(img.size)
                new_size = (int(img.size[0] * ratio), int(img.size[1] * ratio))
                img = img.resize(new_size, Image.Resampling.LANCZOS)

            temp_path = tempfile.mktemp(suffix=".jpg")
            img.save(temp_path, "JPEG", quality=95)
            return temp_path

        except Exception as e:
            logger.error(f"预处理失败: {e}")
            return image_path

    # ============ 数据库操作 ============

    def create_contract(self, data: Dict, products: List[Dict]) -> Dict[str, Any]:
        """创建合同（包含品种明细）"""
        try:
            if "total_quantity" in data and "truck_count" not in data:
                data["truck_count"] = self._calculate_truck_count(data.get("total_quantity"))
            if data.get("contract_date"):
                data["end_date"] = self._compute_end_date(data.get("contract_date"))
            duplicate_id = self._find_duplicate_contract(data, products)
            if duplicate_id:
                return {
                    "success": False,
                    "error": "合同信息已存在",
                    "existing_id": duplicate_id,
                }
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT id FROM pd_contracts WHERE contract_no = %s", (data.get("contract_no"),))
                    if cur.fetchone():
                        return {"success": False, "error": f"合同编号 {data['contract_no']} 已存在"}

                    cur.execute("""
                        INSERT INTO pd_contracts 
                        (contract_no, contract_date, end_date, smelter_company, 
                         total_quantity, truck_count, arrival_payment_ratio, final_payment_ratio,
                         contract_image_path, status, remarks)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        data.get("contract_no"),
                        data.get("contract_date"),
                        data.get("end_date"),
                        data.get("smelter_company"),
                        data.get("total_quantity"),
                        data.get("truck_count"),
                        data.get("arrival_payment_ratio", Decimal("0.9")),
                        data.get("final_payment_ratio", Decimal("0.1")),
                        data.get("contract_image_path"),
                        data.get("status", "生效中"),
                        data.get("remarks"),
                    ))

                    contract_id = cur.lastrowid

                    for idx, product in enumerate(products):
                        cur.execute("""
                            INSERT INTO pd_contract_products 
                            (contract_id, product_name, unit_price, sort_order)
                            VALUES (%s, %s, %s, %s)
                        """, (
                            contract_id,
                            product["product_name"],
                            product.get("unit_price"),
                            idx,
                        ))

                    return {
                        "success": True,
                        "message": "合同创建成功",
                        "data": {"id": contract_id, "contract_no": data["contract_no"]}
                    }

        except Exception as e:
            logger.error(f"创建合同失败: {e}")
            return {"success": False, "error": str(e)}

    def update_contract(self, contract_id: int, data: Dict, products: List[Dict] = None) -> Dict[str, Any]:
        """更新合同（含图片重命名）"""
        try:
            if "total_quantity" in data:
                data["truck_count"] = self._calculate_truck_count(data.get("total_quantity"))
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # 获取原合同信息（包括图片路径和合同号）
                    cur.execute("SELECT contract_no, contract_image_path FROM pd_contracts WHERE id = %s",
                                (contract_id,))
                    old = cur.fetchone()
                    if not old:
                        return {"success": False, "error": f"合同ID {contract_id} 不存在"}

                    old_contract_no, old_image_path = old

                    # 如果要修改合同编号，检查新编号是否被占用
                    new_contract_no = data.get("contract_no")
                    if new_contract_no and new_contract_no != old_contract_no:
                        cur.execute("SELECT id FROM pd_contracts WHERE contract_no = %s AND id != %s",
                                    (new_contract_no, contract_id))
                        if cur.fetchone():
                            return {"success": False, "error": f"合同编号 {new_contract_no} 已被使用"}

                    # 处理图片重命名（如果合同编号变更且存在旧图片）
                    new_image_path = data.get("contract_image_path")  # 可能传入新图片路径
                    if new_contract_no and new_contract_no != old_contract_no and old_image_path and not new_image_path:
                        # 只有合同号变更但没有传入新图片时，重命名旧图片
                        old_path = Path(old_image_path)
                        if old_path.exists():
                            # 生成新文件名
                            safe_name = re.sub(r'[^\w\-]', '_', new_contract_no)
                            new_filename = f"{safe_name}.jpg"
                            new_path = old_path.parent / new_filename

                            # 如果新文件名已存在，先删除
                            if new_path.exists() and new_path != old_path:
                                os.remove(new_path)

                            # 重命名文件
                            os.rename(old_path, new_path)
                            new_image_path = str(new_path)
                            data["contract_image_path"] = new_image_path
                    elif new_image_path:
                        # 传入了新图片路径，直接使用（路由层已处理文件保存和旧文件删除）
                        pass

                    # 构建更新SQL
                    update_fields = []
                    params = []
                    fields = ["contract_no", "contract_date", "end_date", "smelter_company",
                              "total_quantity", "truck_count", "arrival_payment_ratio", "final_payment_ratio",
                              "status", "remarks", "contract_image_path"]

                    for field in fields:
                        if field in data:
                            update_fields.append(f"{field} = %s")
                            params.append(data[field])

                    if update_fields:
                        params.append(contract_id)
                        sql = f"UPDATE pd_contracts SET {', '.join(update_fields)} WHERE id = %s"
                        cur.execute(sql, tuple(params))

                    # 更新品种明细
                    if products is not None:
                        cur.execute("DELETE FROM pd_contract_products WHERE contract_id = %s", (contract_id,))
                        for idx, product in enumerate(products):
                            cur.execute("""
                                INSERT INTO pd_contract_products 
                                (contract_id, product_name, unit_price, sort_order)
                                VALUES (%s, %s, %s, %s)
                            """, (contract_id, product["product_name"], product.get("unit_price"), idx))

                    return {
                        "success": True,
                        "message": "合同更新成功",
                        "data": {
                            "id": contract_id,
                            "contract_no": new_contract_no or old_contract_no,
                            "image_path": new_image_path
                        }
                    }

        except Exception as e:
            logger.error(f"更新合同失败: {e}")
            return {"success": False, "error": str(e)}

    def get_contract_detail(self, contract_id: int) -> Optional[Dict]:
        """获取合同详情（含品种明细）"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM pd_contracts WHERE id = %s", (contract_id,))
                    row = cur.fetchone()
                    if not row:
                        return None

                    columns = [desc[0] for desc in cur.description]
                    contract = dict(zip(columns, row))

                    # 转换时间字段为字符串
                    for key in ['contract_date', 'end_date', 'created_at', 'updated_at']:
                        if contract.get(key) and isinstance(contract[key], datetime):
                            contract[key] = contract[key].strftime('%Y-%m-%d %H:%M:%S')
                        elif contract.get(key) and isinstance(contract[key], date):
                            contract[key] = contract[key].strftime('%Y-%m-%d')

                    # 如果 seq_no 为 None，用 id 代替
                    if contract.get('seq_no') is None:
                        contract['seq_no'] = contract['id']

                    cur.execute("""
                        SELECT * FROM pd_contract_products 
                        WHERE contract_id = %s ORDER BY sort_order
                    """, (contract_id,))

                    products = []
                    for row in cur.fetchall():
                        cols = [desc[0] for desc in cur.description]
                        product = dict(zip(cols, row))
                        # 转换product的时间字段
                        for key in ['created_at', 'updated_at']:
                            if product.get(key) and isinstance(product[key], datetime):
                                product[key] = product[key].strftime('%Y-%m-%d %H:%M:%S')
                        products.append(product)

                    contract["products"] = products
                    return contract

        except Exception as e:
            logger.error(f"查询失败: {e}")
            return None

    def get_contract_detail_by_no(self, contract_no: str) -> Optional[Dict]:
        """根据合同编号获取详情"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT id FROM pd_contracts WHERE contract_no = %s", (contract_no,))
                    row = cur.fetchone()
                    if row:
                        return self.get_contract_detail(row[0])
                    return None
        except:
            return None

    def list_contracts(
        self,
        page: int = 1,
        page_size: int = 20,
        exact_contract_no: Optional[str] = None,
        exact_smelter_company: Optional[str] = None,
        exact_status: Optional[str] = None,
        fuzzy_keywords: Optional[str] = None,
    ) -> Dict[str, Any]:
        """获取合同列表（分页）"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    where_clauses = []
                    params = []

                    if exact_contract_no:
                        where_clauses.append("c.contract_no = %s")
                        params.append(exact_contract_no)
                    if exact_smelter_company:
                        where_clauses.append("c.smelter_company = %s")
                        params.append(exact_smelter_company)
                    if exact_status:
                        where_clauses.append("c.status = %s")
                        params.append(exact_status)

                    if fuzzy_keywords:
                        tokens = [t for t in fuzzy_keywords.split() if t]
                        or_clauses = []
                        for token in tokens:
                            like = f"%{token}%"
                            or_clauses.append(
                                "(c.contract_no LIKE %s OR c.smelter_company LIKE %s OR c.remarks LIKE %s "
                                "OR EXISTS (SELECT 1 FROM pd_contract_products p WHERE p.contract_id = c.id "
                                "AND p.product_name LIKE %s))"
                            )
                            params.extend([like, like, like, like])
                        if or_clauses:
                            where_clauses.append("(" + " OR ".join(or_clauses) + ")")

                    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

                    cur.execute(f"SELECT COUNT(*) FROM pd_contracts c {where_sql}", tuple(params))
                    total = cur.fetchone()[0]

                    offset = (page - 1) * page_size
                    cur.execute(f"""
                        SELECT c.*, 
                               (SELECT COUNT(*) FROM pd_contract_products WHERE contract_id = c.id) as product_count,
                               (SELECT COUNT(*) FROM pd_deliveries d WHERE d.contract_no = c.contract_no) as truck_count
                        FROM pd_contracts c
                        {where_sql}
                        ORDER BY c.seq_no DESC, c.created_at DESC
                        LIMIT %s OFFSET %s
                    """, tuple(params + [page_size, offset]))

                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    data = [dict(zip(columns, row)) for row in rows]

                    for item in data:
                        for key in ['contract_date', 'end_date', 'created_at', 'updated_at']:
                            if item.get(key) and isinstance(item[key], datetime):
                                item[key] = item[key].strftime('%Y-%m-%d %H:%M:%S')
                            elif item.get(key) and isinstance(item[key], date):
                                item[key] = item[key].strftime('%Y-%m-%d')

                        if item.get('seq_no') is None:
                            item['seq_no'] = item['id']

                    return {
                        "success": True,
                        "data": data,
                        "total": total,
                        "page": page,
                        "page_size": page_size
                    }

        except Exception as e:
            logger.error(f"查询失败: {e}")
            return {"success": False, "error": str(e), "data": [], "total": 0}

    def delete_contract(self, contract_id: int) -> Dict[str, Any]:
        """删除合同"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM pd_contracts WHERE id = %s", (contract_id,))
                    return {"success": True, "message": "删除成功"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def export_contracts(self, contract_ids: List[int] = None) -> List[Dict]:
        """导出合同"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    if contract_ids:
                        format_ids = ','.join(['%s'] * len(contract_ids))
                        cur.execute(f"""
                            SELECT c.*, p.product_name, p.unit_price
                            FROM pd_contracts c
                            LEFT JOIN pd_contract_products p ON c.id = p.contract_id
                            WHERE c.id IN ({format_ids})
                            ORDER BY c.seq_no
                        """, tuple(contract_ids))
                    else:
                        cur.execute("""
                            SELECT c.*, p.product_name, p.unit_price
                            FROM pd_contracts c
                            LEFT JOIN pd_contract_products p ON c.id = p.contract_id
                            ORDER BY c.seq_no
                        """)

                    columns = [desc[0] for desc in cur.description]
                    return [dict(zip(columns, row)) for row in cur.fetchall()]

        except Exception as e:
            logger.error(f"导出失败: {e}")
            return []


_contract_service = None


def expire_contracts_after_grace(grace_days: int = 5) -> int:
    """合同生效后超过指定天数自动失效"""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE pd_contracts
                    SET status = '已失效'
                    WHERE status = '生效中'
                      AND contract_date IS NOT NULL
                      AND DATE_ADD(contract_date, INTERVAL %s DAY) <= CURDATE()
                    """,
                    (grace_days,),
                )
                return cur.rowcount
    except Exception as e:
        logger.error(f"合同自动失效失败: {e}")
        return 0

def get_contract_service():
    global _contract_service
    if _contract_service is None:
        _contract_service = ContractService()
    return _contract_service