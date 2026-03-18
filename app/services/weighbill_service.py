"""
磅单服务 - 支持一报单多品种（最多4个）
"""
import logging
import os
import re
import tempfile
import uuid
from decimal import Decimal, ROUND_HALF_UP
import cv2  # 新增导入
import numpy as np
from cv2 import dnn_superres
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime

from PIL import Image, ImageEnhance, ImageFilter

try:
    from rapidocr_onnxruntime import RapidOCR
    RAPIDOCR_AVAILABLE = True
except ImportError:
    RAPIDOCR_AVAILABLE = False

from app.core.paths import UPLOADS_DIR
from app.services.contract_service import get_conn

logger = logging.getLogger(__name__)

UPLOAD_DIR = UPLOADS_DIR / "weighbills"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


class WeighbillService:
    """磅单服务"""

    def __init__(self):
        self.ocr = None
        self._weighbill_has_warehouse_name = None
        if RAPIDOCR_AVAILABLE:
            try:
                self.ocr = RapidOCR()
                logger.info("磅单OCR初始化成功")
            except Exception as e:
                logger.error(f"磅单OCR初始化失败: {e}")

    def _has_weighbill_warehouse_name_column(self) -> bool:
        """兼容旧库：动态检查 pd_weighbills 是否已有 warehouse_name 字段。"""
        if self._weighbill_has_warehouse_name is not None:
            return self._weighbill_has_warehouse_name

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SHOW COLUMNS FROM pd_weighbills LIKE 'warehouse_name'")
                    self._weighbill_has_warehouse_name = cur.fetchone() is not None
        except Exception as e:
            logger.warning(f"检查 pd_weighbills.warehouse_name 字段失败: {e}")
            self._weighbill_has_warehouse_name = False

        return self._weighbill_has_warehouse_name

    # ========== 图片预处理 ==========

    def _apply_super_resolution(self, image: Image.Image) -> Image.Image:
        """同 contract_service 中的实现"""
        if image.width < 800 or image.height < 600:
            try:
                # 模型路径可根据项目结构调整，此处放在 app/models/ 下
                model_path = Path(__file__).parent / "models" / "ESPCN_x2.pb"
                if not model_path.exists():
                    logger.warning("超分辨率模型文件不存在，跳过")
                    return image

                img_cv = cv2.cvtColor(np.array(image), cv2.COLOR_RGB2BGR)
                sr = dnn_superres.DnnSuperResImpl.create()
                sr.readModel(str(model_path))
                sr.setModel("fsrcnn", 2)
                result = sr.upsample(img_cv)

                result_rgb = cv2.cvtColor(result, cv2.COLOR_BGR2RGB)
                return Image.fromarray(result_rgb)
            except Exception as e:
                logger.error(f"超分辨率处理失败: {e}")
                return image
        return image

    def preprocess_image(self, image_path: str) -> str:
        try:
            img = Image.open(image_path)
            if img.mode != "RGB":
                img = img.convert("RGB")

            # 新增超分辨率处理
            img = self._apply_super_resolution(img)

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

    # ========== OCR识别 ==========

    def recognize_weighbill(self, image_path: str) -> Dict[str, Any]:
        """OCR识别磅单"""
        if not self.ocr:
            return {
                "success": True,
                "data": self._empty_result("OCR未初始化"),
                "ocr_success": False
            }

        try:
            result, elapse = self.ocr(image_path)
            total_elapse = sum(elapse) if isinstance(elapse, list) else float(elapse or 0)

            if not result:
                return {
                    "success": True,
                    "data": self._empty_result("未能识别到文本"),
                    "ocr_success": False
                }

            text_lines = []
            for item in result:
                bbox, text, confidence = item
                text_lines.append({"text": text.strip(), "confidence": float(confidence)})

            full_text = "\n".join([line["text"] for line in text_lines])

            logger.info("=== 磅单OCR识别文本 ===")
            for i, line in enumerate(text_lines):
                logger.info(f"{i}: {line['text']}")

            data = self._parse_weighbill(text_lines, full_text)
            data["ocr_time"] = round(total_elapse, 3)
            data["raw_text"] = full_text

            return {
                "success": True,
                "data": data,
                "ocr_success": True
            }

        except Exception as e:
            logger.error(f"磅单识别异常: {e}")
            return {
                "success": True,
                "data": self._empty_result(f"识别异常: {str(e)}"),
                "ocr_success": False
            }

    def _empty_result(self, message: str) -> Dict:
        """返回空结果结构"""
        return {
            "weigh_date": None,
            "weigh_ticket_no": None,
            "contract_no": None,
            "vehicle_no": None,
            "product_name": None,
            "gross_weight": None,
            "tare_weight": None,
            "net_weight": None,
            "delivery_unit": None,
            "receive_unit": None,
            "ocr_message": message,
        }

    def _parse_weighbill(self, text_lines: List[Dict], full_text: str) -> Dict:
        """解析磅单信息"""
        weigh_date = self._extract_date(full_text)
        ticket_no = self._extract_ticket_no(full_text)
        contract_no = self._extract_contract_no(full_text)
        vehicle_no = self._extract_vehicle_no(full_text)
        product_name = self._extract_product_name(full_text)
        gross, tare, net = self._extract_weights(full_text)
        delivery, receive = self._extract_units(full_text)

        missing = []
        if not weigh_date:
            missing.append("日期")
        if not vehicle_no:
            missing.append("车牌号")
        if not net:
            missing.append("净重")
        if not contract_no:
            missing.append("合同编号")

        message = "识别完成"
        if missing:
            message = f"已识别，以下字段缺失需手动填写: {', '.join(missing)}"

        return {
            "weigh_date": weigh_date,
            "weigh_ticket_no": ticket_no,
            "contract_no": contract_no,
            "vehicle_no": vehicle_no,
            "product_name": product_name,
            "gross_weight": gross,
            "tare_weight": tare,
            "net_weight": net,
            "delivery_unit": delivery,
            "receive_unit": receive,
            "ocr_message": message,
        }

    def _extract_date(self, text: str) -> Optional[str]:
        patterns = [
            r"日期[：:]\s*(\d{4}年\d{1,2}月\d{1,2}日)",
            r"(\d{4}年\d{1,2}月\d{1,2}日)",
            r"(\d{4}-\d{2}-\d{2})",
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1).replace("年", "-").replace("月", "-").replace("日", "")
        return None

    def _extract_ticket_no(self, text: str) -> Optional[str]:
        patterns = [r"单据号[：:]\s*(\d+)", r"磅单号[：:]\s*(\d+)", r"单号[：:]\s*(\d+)"]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1)
        return None

    def _extract_contract_no(self, text: str) -> Optional[str]:
        patterns = [
            r"合同编号[：:]\s*([A-Za-z0-9\-]+)",
            r"合同号[：:]\s*([A-Za-z0-9\-]+)",
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1).strip()
        return None

    def _extract_vehicle_no(self, text: str) -> Optional[str]:
        patterns = [
            r"车号[：:]\s*([京津沪渝冀豫云辽黑湘皖鲁新苏浙赣鄂桂甘晋蒙陕吉闽贵粤青藏川宁琼][A-Z][A-Z0-9]{4,6})",
            r"车牌[：:]\s*([京津沪渝冀豫云辽黑湘皖鲁新苏浙赣鄂桂甘晋蒙陕吉闽贵粤青藏川宁琼][A-Z][A-Z0-9]{4,6})",
            r"([京津沪渝冀豫云辽黑湘皖鲁新苏浙赣鄂桂甘晋蒙陕吉闽贵粤青藏川宁琼][A-Z][A-Z0-9]{4,6})",
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1)
        return None

    def _extract_product_name(self, text: str) -> Optional[str]:
        patterns = [
            r"货物名称[：:]\s*(.+?)(?:\n|$)",
            r"品名[：:]\s*(.+?)(?:\n|$)",
            r"货名[：:]\s*(.+?)(?:\n|$)",
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1).strip()
        return None

    def _extract_weights(self, text: str) -> tuple:
        gross = tare = net = None
        match = re.search(r"毛重[：:]\s*(\d+\.?\d*)", text)
        if match:
            gross = float(match.group(1))
        match = re.search(r"皮重[：:]\s*(\d+\.?\d*)", text)
        if match:
            tare = float(match.group(1))
        match = re.search(r"净重[：:]\s*(\d+\.?\d*)", text)
        if match:
            net = float(match.group(1))
        return gross, tare, net

    def _extract_units(self, text: str) -> tuple:
        delivery = receive = None
        match = re.search(r"送货单位[：:]\s*(.+?)(?:\n|$)", text)
        if match:
            delivery = match.group(1).strip()
        match = re.search(r"收货单位[：:]\s*(.+?)(?:\n|$)", text)
        if match:
            receive = match.group(1).strip()
        return delivery, receive

    # ========== 合同价格查询 ==========

    def get_contract_price_by_product(self, contract_no: str, product_name: str) -> Optional[float]:
        """根据合同编号和品种获取单价"""
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
                    if row and row[0]:
                        return float(row[0])

                    # 未找到，返回该合同第一个有价格的品种
                    cur.execute("""
                        SELECT p.unit_price
                        FROM pd_contract_products p
                        JOIN pd_contracts c ON p.contract_id = c.id
                        WHERE c.contract_no = %s 
                        AND p.unit_price IS NOT NULL
                        LIMIT 1
                    """, (contract_no,))
                    row = cur.fetchone()
                    if row and row[0]:
                        return float(row[0])
                    return None
        except Exception as e:
            logger.error(f"获取品种单价失败: {e}")
            return None

    # ========== 新增：获取报单信息方法 ==========
    def get_delivery_info(self, delivery_id: int) -> Optional[Dict[str, Any]]:
        """获取报单信息（用于创建收款明细）"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT
                            d.*
                        FROM pd_deliveries d
                        WHERE d.id = %s
                    """, (delivery_id,))
                    row = cur.fetchone()
                    if not row:
                        return None
                    columns = [desc[0] for desc in cur.description]
                    return dict(zip(columns, row))
        except Exception as e:
            logger.error(f"获取报单信息失败: {e}")
            return None
    # ========== 新增结束 ==========

    # ========== 报单匹配 ==========

    def match_delivery_info(self, weigh_date: str, vehicle_no: str,
                            driver_name: Optional[str] = None,
                            contract_no: Optional[str] = None) -> Optional[Dict]:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    params = [vehicle_no, weigh_date, weigh_date, weigh_date, weigh_date]
                    extra_conditions = ""
                    if driver_name:
                        extra_conditions += " AND driver_name = %s"
                        params.append(driver_name)
                    if contract_no:
                        extra_conditions += " AND contract_no = %s"
                        params.append(contract_no)

                    cur.execute(f"""
                        SELECT * FROM pd_deliveries 
                        WHERE vehicle_no = %s 
                        AND (
                            report_date = %s 
                            OR report_date = DATE_ADD(%s, INTERVAL 1 DAY)
                            OR report_date = DATE_SUB(%s, INTERVAL 1 DAY)
                        )
                        AND status IN ('已确认', '已完成')         -- 新增：只匹配已确认报单
                        {extra_conditions}
                        ORDER BY ABS(DATEDIFF(report_date, %s)), created_at ASC
                        LIMIT 1
                    """, tuple(params))
                    row = cur.fetchone()
                    if not row:
                        return None
                    columns = [desc[0] for desc in cur.description]
                    return dict(zip(columns, row))
        except Exception as e:
            logger.error(f"匹配报货订单失败: {e}")
            return None

    def auto_fill_data(self, ocr_data: Dict) -> Dict:
        result = ocr_data.copy()
        weigh_date = ocr_data.get("weigh_date")
        vehicle_no = ocr_data.get("vehicle_no")
        contract_no = ocr_data.get("contract_no")
        product_name = ocr_data.get("product_name")
        net_weight = ocr_data.get("net_weight")

        # 匹配报货订单（传入可选合同号）
        if weigh_date and vehicle_no:
            delivery = self.match_delivery_info(
                weigh_date, vehicle_no,
                contract_no=contract_no  # 司机姓名暂未解析，可后续扩展
            )
            if delivery:
                result["matched_delivery_id"] = delivery["id"]
                result["warehouse"] = delivery.get("warehouse")
                result["target_factory_name"] = delivery.get("target_factory_name")
                result["driver_name"] = delivery.get("driver_name")
                result["driver_phone"] = delivery.get("driver_phone")
                result["driver_id_card"] = delivery.get("driver_id_card")
                result["match_message"] = "已匹配报货订单"
            else:
                result["match_message"] = "未找到匹配的报货订单，请手动填写"

        # 获取合同单价（与原逻辑相同）
        if contract_no and product_name:
            price = self.get_contract_price_by_product(contract_no, product_name)
            if price:
                result["unit_price"] = price
                if net_weight:
                    result["total_amount"] = round(price * net_weight, 2)
                result["price_message"] = f"已获取合同单价（品种：{product_name}）"
            else:
                result["price_message"] = "未找到合同单价，请手动填写"
        elif contract_no:
            price = self.get_contract_price_by_product(contract_no, "废电瓶")
            if price:
                result["unit_price"] = price
                if net_weight:
                    result["total_amount"] = round(price * net_weight, 2)
                result["price_message"] = "已获取合同默认单价"
            else:
                result["price_message"] = "未找到合同单价，请手动填写"

        return result

    # ========== 核心：上传/修改磅单 ==========

    def get_warehouse_payees(self, warehouse_name: str) -> Dict[str, Any]:
        """查询库房的收款人列表（新表结构）"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # 先查询库房ID
                    cur.execute("""
                        SELECT id FROM pd_warehouses 
                        WHERE warehouse_name = %s AND is_active = 1
                    """, (warehouse_name,))
                    warehouse_row = cur.fetchone()
                    
                    if not warehouse_row:
                        return {
                            "success": False,
                            "error": f"库房 '{warehouse_name}' 不存在或已停用"
                        }
                    
                    warehouse_id = warehouse_row['id']
                    
                    # 查询该库房下的收款人
                    cur.execute("""
                        SELECT id, payee_name, payee_account, payee_bank_name, is_active
                        FROM pd_payees
                        WHERE warehouse_id = %s AND is_active = 1
                        ORDER BY id ASC
                    """, (warehouse_id,))
                    
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    payees = [dict(zip(columns, row)) for row in rows]
                    
                    return {
                        "success": True,
                        "count": len(payees),
                        "warehouse_id": warehouse_id,
                        "payees": payees
                    }
                    
        except Exception as e:
            logger.error(f"查询库房收款人失败: {e}")
            return {"success": False, "error": str(e)}

    def _get_payee_by_id(self, payee_id: int) -> Optional[Dict]:
        """根据ID获取收款人详情（新表结构）"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT p.*, w.warehouse_name
                        FROM pd_payees p
                        JOIN pd_warehouses w ON p.warehouse_id = w.id
                        WHERE p.id = %s
                    """, (payee_id,))
                    row = cur.fetchone()
                    if row:
                        columns = [desc[0] for desc in cur.description]
                        return dict(zip(columns, row))
                    return None
        except Exception as e:
            logger.error(f"查询收款人失败: {e}")
            return None

    def _recognize_from_bytes(self, image_bytes: bytes) -> Dict[str, Any]:
        """从字节流识别磅单"""
        temp_path = None
        try:
            # 保存临时文件
            temp_path = tempfile.mktemp(suffix=".jpg")
            with open(temp_path, "wb") as f:
                f.write(image_bytes)

            # 预处理并识别
            processed_path = self.preprocess_image(temp_path)
            result = self.recognize_weighbill(processed_path)

            # 清理临时文件
            if processed_path != temp_path and os.path.exists(processed_path):
                os.remove(processed_path)
            if os.path.exists(temp_path):
                os.remove(temp_path)

            return result

        except Exception as e:
            if temp_path and os.path.exists(temp_path):
                os.remove(temp_path)
            return {"success": False, "error": str(e)}

    def _match_delivery_by_ocr(self, ocr_data: Dict) -> Optional[Dict]:
        """根据OCR数据匹配报单"""
        weigh_date = ocr_data.get("weigh_date")
        vehicle_no = ocr_data.get("vehicle_no")

        if not weigh_date or not vehicle_no:
            return None

        return self.match_delivery_info(weigh_date, vehicle_no)

    def _normalize_delivery_payee(self, payee_value: Optional[Any]) -> Optional[str]:
        """兼容前端传收款人ID或名称，统一转成收款人名称。"""
        if payee_value is None:
            return None

        raw = str(payee_value).strip()
        if raw == "":
            return None

        if raw.isdigit():
            payee_info = self._get_payee_by_id(int(raw))
            if payee_info:
                return payee_info.get("payee_name") or raw

        return raw

    def _sync_delivery_fields(self, delivery_id: int, data: Dict[str, Any]) -> None:
        """将磅单上传时附带的库房/收款人同步回报单。"""
        warehouse = data.get("warehouse")
        payee = self._normalize_delivery_payee(data.get("payee")) if "payee" in data else None

        update_fields = []
        params = []

        if warehouse is not None:
            update_fields.append("warehouse = %s")
            params.append(warehouse)
        if "payee" in data:
            update_fields.append("payee = %s")
            params.append(payee)

        if not update_fields:
            return

        with get_conn() as conn:
            with conn.cursor() as cur:
                params.append(delivery_id)
                cur.execute(
                    f"UPDATE pd_deliveries SET {', '.join(update_fields)}, updated_at = NOW() WHERE id = %s",
                    tuple(params)
                )

    @staticmethod
    def _normalize_warehouse_name(data: Dict[str, Any], existing: Optional[Dict[str, Any]] = None) -> Optional[str]:
        warehouse_name = data.get("warehouse_name")
        if warehouse_name is None:
            warehouse_name = data.get("warehouse")
        if warehouse_name is None and existing:
            warehouse_name = existing.get("warehouse_name")
        if warehouse_name is None:
            return None

        warehouse_name = str(warehouse_name).strip()
        return warehouse_name or None

    def _upload_failure(
            self,
            error: str,
            delivery_id: Optional[int],
            product_name: Optional[str],
            payload: Optional[Dict[str, Any]] = None,
            current_user: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """记录上传失败原因，便于排查前端请求参数问题。"""
        logger.warning(
            "weighbill upload rejected delivery_id=%s product_name=%s user_id=%s payload_keys=%s error=%s",
            delivery_id,
            product_name,
            current_user.get("id") if current_user else None,
            sorted((payload or {}).keys()),
            error,
        )
        return {"success": False, "error": error}

    def upload_weighbill(
            self,
            delivery_id: int,
            product_name: str,
            data: Dict[str, Any],
            image_file: bytes = None,
            current_user: dict = None,
            is_manual: bool = False
    ) -> Dict[str, Any]:
        """上传或修改磅单，按 delivery_id + product_name 幂等写入。"""
        temp_file_path = None
        old_image_path = None

        try:
            if not delivery_id:
                return self._upload_failure("报单ID不能为空", delivery_id, product_name, data, current_user)

            normalized_product = str(product_name).strip() if product_name is not None else ""
            if not normalized_product:
                return self._upload_failure("品种名称不能为空", delivery_id, product_name, data, current_user)

            payload = dict(data or {})
            uploader_id = current_user.get("id") if current_user else None
            uploader_name = None
            if current_user:
                uploader_name = current_user.get("name") or current_user.get("account")
            uploader_name = uploader_name or "system"

            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT id FROM pd_deliveries WHERE id = %s", (delivery_id,))
                    if not cur.fetchone():
                        return self._upload_failure(
                            f"报单ID {delivery_id} 不存在",
                            delivery_id,
                            normalized_product,
                            payload,
                            current_user,
                        )

                    cur.execute(
                        "SELECT * FROM pd_weighbills WHERE delivery_id = %s AND product_name = %s LIMIT 1",
                        (delivery_id, normalized_product)
                    )
                    existing_row = cur.fetchone()
                    existing = None
                    if existing_row:
                        if isinstance(existing_row, dict):
                            existing = dict(existing_row)
                        else:
                            columns = [desc[0] for desc in cur.description]
                            existing = dict(zip(columns, existing_row))

                    if image_file:
                        safe_product = re.sub(r"[^\w\-]", "_", normalized_product) or "product"
                        filename = (
                            f"weighbill_{delivery_id}_{safe_product}_"
                            f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}.jpg"
                        )
                        file_path = UPLOAD_DIR / filename
                        with open(file_path, "wb") as f:
                            f.write(image_file)
                        temp_file_path = str(file_path)
                        old_image_path = existing.get("weighbill_image") if existing else None

                    final_weigh_date = payload.get("weigh_date") if payload.get("weigh_date") is not None else (existing.get("weigh_date") if existing else None)
                    final_delivery_time = payload.get("delivery_time") if payload.get("delivery_time") is not None else (existing.get("delivery_time") if existing else None)
                    final_weigh_ticket_no = payload.get("weigh_ticket_no") if payload.get("weigh_ticket_no") is not None else (existing.get("weigh_ticket_no") if existing else None)
                    final_contract_no = payload.get("contract_no") or existing.get("contract_no") if existing else None
                    final_contract_id = None
                    final_vehicle_no = payload.get("vehicle_no") if payload.get("vehicle_no") is not None else (existing.get("vehicle_no") if existing else None)
                    final_gross_weight = payload.get("gross_weight") if payload.get("gross_weight") is not None else (existing.get("gross_weight") if existing else None)
                    final_tare_weight = payload.get("tare_weight") if payload.get("tare_weight") is not None else (existing.get("tare_weight") if existing else None)
                    final_net_weight = payload.get("net_weight") if payload.get("net_weight") is not None else (existing.get("net_weight") if existing else None)
                    final_unit_price = payload.get("unit_price") if payload.get("unit_price") is not None else (existing.get("unit_price") if existing else None)
                    final_warehouse_name = self._normalize_warehouse_name(payload, existing)
                    final_image_path = temp_file_path if temp_file_path else (existing.get("weighbill_image") if existing else None)

                    if final_net_weight in (None, ""):
                        return self._upload_failure(
                            "净重不能为空",
                            delivery_id,
                            normalized_product,
                            payload,
                            current_user,
                        )
                    
                    if final_contract_no:
                        with get_conn() as conn:
                            with conn.cursor() as cur:
                                cur.execute("SELECT id FROM pd_contracts WHERE contract_no = %s", (final_contract_no,))
                                row = cur.fetchone()
                                if row:
                                    final_contract_id = row[0] if not isinstance(row, dict) else row["id"]

                    net_weight_decimal = Decimal(str(final_net_weight)).quantize(Decimal('0.001'), rounding=ROUND_HALF_UP)
                    gross_weight_decimal = None if final_gross_weight in (None, "") else Decimal(str(final_gross_weight)).quantize(Decimal('0.001'), rounding=ROUND_HALF_UP)
                    tare_weight_decimal = None if final_tare_weight in (None, "") else Decimal(str(final_tare_weight)).quantize(Decimal('0.001'), rounding=ROUND_HALF_UP)
                    unit_price_decimal = None if final_unit_price in (None, "") else Decimal(str(final_unit_price)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                    total_amount_decimal = None
                    if unit_price_decimal is not None:
                        total_amount_decimal = (unit_price_decimal * net_weight_decimal).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

                    final_upload_status = '已上传' if final_image_path else '待上传'
                    if final_upload_status == '已上传':
                        final_ocr_status = '已修正' if is_manual else '已确认'
                    else:
                        final_ocr_status = '待上传磅单'

                    if existing:
                        params = [
                            final_weigh_date,
                            final_delivery_time,
                            final_weigh_ticket_no,
                            final_contract_no,
                            final_contract_id,
                            final_vehicle_no,
                            gross_weight_decimal,
                            tare_weight_decimal,
                            net_weight_decimal,
                            unit_price_decimal,
                            total_amount_decimal,
                            final_warehouse_name,
                            final_image_path,
                            final_upload_status,
                            final_ocr_status,
                            1 if is_manual else existing.get('is_manual_corrected', 0),
                            uploader_id,
                            uploader_name,
                        ]

                        sql = """
                            UPDATE pd_weighbills
                            SET weigh_date = %s,
                                delivery_time = %s,
                                weigh_ticket_no = %s,
                                contract_no = %s,
                                contract_id = %s,
                                vehicle_no = %s,
                                gross_weight = %s,
                                tare_weight = %s,
                                net_weight = %s,
                                unit_price = %s,
                                total_amount = %s,
                                warehouse_name = %s,
                                weighbill_image = %s,
                                upload_status = %s,
                                ocr_status = %s,
                                is_manual_corrected = %s,
                                uploader_id = %s,
                                uploader_name = %s,
                                uploaded_at = CASE WHEN %s = '已上传' THEN NOW() ELSE uploaded_at END,
                                updated_at = NOW()
                            WHERE id = %s
                        """
                        params.extend([final_upload_status, existing['id']])
                        if not self._has_weighbill_warehouse_name_column():
                            sql = sql.replace(",\n                                warehouse_name = %s", "")
                            del params[10]
                        cur.execute(sql, tuple(params))
                        weighbill_id = existing['id']
                        action = 'updated'
                    else:
                        insert_fields = [
                            "weigh_date", "delivery_time", "weigh_ticket_no", "contract_no","contract_id",
                            "delivery_id", "vehicle_no", "product_name", "gross_weight",
                            "tare_weight", "net_weight", "unit_price", "total_amount",
                            "weighbill_image", "upload_status", "ocr_status",
                            "is_manual_corrected", "uploader_id", "uploader_name", "uploaded_at"
                        ]
                        insert_values = [
                            final_weigh_date,
                            final_delivery_time,
                            final_weigh_ticket_no,
                            final_contract_no,
                            final_contract_id,
                            delivery_id,
                            final_vehicle_no,
                            normalized_product,
                            gross_weight_decimal,
                            tare_weight_decimal,
                            net_weight_decimal,
                            unit_price_decimal,
                            total_amount_decimal,
                            final_image_path,
                            final_upload_status,
                            final_ocr_status,
                            1 if is_manual else 0,
                            uploader_id,
                            uploader_name,
                        ]
                        if self._has_weighbill_warehouse_name_column():
                            insert_fields.insert(4, "warehouse_name")
                            insert_values.insert(4, final_warehouse_name)

                        placeholders = ", ".join(["%s"] * (len(insert_values)))
                        sql = f"""
                            INSERT INTO pd_weighbills (
                                {', '.join(insert_fields)}
                            ) VALUES ({placeholders}, NOW())
                        """
                        cur.execute(sql, tuple(insert_values))
                        weighbill_id = cur.lastrowid
                        action = 'created'

            if any(key in payload for key in ("warehouse", "payee")):
                self._sync_delivery_fields(delivery_id, payload)

            delivery_info = self.get_delivery_info(delivery_id) or {}
            final_payee = delivery_info.get("payee")
            final_warehouse = delivery_info.get("warehouse")
            final_warehouse_name = payload.get("warehouse_name")
            if final_warehouse_name is None:
                final_warehouse_name = payload.get("warehouse")
            if final_warehouse_name is None and existing:
                final_warehouse_name = existing.get("warehouse_name")

            if temp_file_path and old_image_path and old_image_path != temp_file_path and os.path.exists(old_image_path):
                try:
                    os.remove(old_image_path)
                except Exception as e:
                    logger.warning(f"删除旧磅单图片失败: {e}")

            return {
                "success": True,
                "message": "磅单上传成功" if action == 'created' else "磅单修改成功",
                "data": {
                    "weighbill_id": weighbill_id,
                    "delivery_id": delivery_id,
                    "product_name": normalized_product,
                    "upload_status": final_upload_status,
                    "ocr_status": final_ocr_status,
                    "is_manual_corrected": 1 if is_manual else 0,
                    "unit_price": float(unit_price_decimal) if unit_price_decimal is not None else None,
                    "net_weight": float(net_weight_decimal),
                    "total_amount": float(total_amount_decimal) if total_amount_decimal is not None else None,
                    "weighbill_image": final_image_path,
                    "warehouse_name": final_warehouse_name,
                    "warehouse": final_warehouse,
                    "payee": final_payee,
                }
            }

        except Exception as e:
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                except Exception:
                    pass
            logger.error(
                "上传/修改磅单失败 delivery_id=%s product_name=%s user_id=%s: %s",
                delivery_id,
                product_name,
                current_user.get("id") if current_user else None,
                e,
            )
            return {"success": False, "error": str(e)}

    def batch_upload_weighbills(
            self,
            warehouse_name: str,
            payee_id: Optional[int],
            image_files: List[bytes],
            current_user: dict = None
    ) -> Dict[str, Any]:
        """
        批量上传磅单（支持自动选择或指定收款人）
        
        如果 payee_id 为 None：
            - 查询该库房收款人，1个则直接用，多个则返回选择列表
        如果 payee_id 不为 None：
            - 使用指定的收款人进行批量上传
        """
        
        # 阶段1：检查收款人（当 payee_id 为空时）
        if payee_id is None:
            payees_result = self.get_warehouse_payees(warehouse_name)
            
            if not payees_result.get("success"):
                return payees_result
            
            payees = payees_result.get("payees", [])
            count = len(payees)
            
            if count == 0:
                return {
                    "success": False,
                    "error": f"库房 '{warehouse_name}' 未找到收款人信息，请先配置"
                }
            
            elif count == 1:
                # 只有一个，直接使用
                payee_info = payees[0]
                payee_id = payee_info["id"]
                
            else:
                # 多个收款人，返回让用户选择
                return {
                    "success": True,
                    "need_select_payee": True,  # 标记需要选择
                    "message": f"库房 '{warehouse_name}' 有 {count} 个收款人，请选择",
                    "warehouse_name": warehouse_name,
                    "payees": [
                        {
                            "id": p["id"],
                            "payee_name": p["payee_name"],
                            "payee_account": p.get("payee_account", ""),
                            "payee_bank_name": p.get("payee_bank_name", ""),
                            "is_active": p.get("is_active", 1)
                        }
                        for p in payees
                    ]
                }
        
        # 阶段2：使用指定的 payee_id 进行批量上传
        # 验证收款人是否存在且属于该库房
        payee_info = self._get_payee_by_id(payee_id)
        if not payee_info:
            return {
                "success": False,
                "error": f"收款人ID {payee_id} 不存在"
            }
        
        if payee_info.get("warehouse_name") != warehouse_name:
            return {
                "success": False,
                "error": f"收款人ID {payee_id} 不属于库房 '{warehouse_name}'"
            }
        
        # 开始批量处理
        fixed_payee = payee_info.get("payee_name", "")
        
        results = {
            "success": True,
            "need_select_payee": False,
            "warehouse_name": warehouse_name,
            "payee_id": payee_id,
            "payee_name": fixed_payee,
            "total": len(image_files),
            "success_count": 0,
            "failed_count": 0,
            "success_list": [],
            "failed_list": []
        }

        # 逐张处理图片
        for idx, image_bytes in enumerate(image_files):
            try:
                # OCR识别
                ocr_result = self._recognize_from_bytes(image_bytes)
                if not ocr_result.get("success"):
                    results["failed_list"].append({
                        "index": idx,
                        "filename": f"image_{idx}",
                        "error": ocr_result.get("error", "OCR识别失败")
                    })
                    results["failed_count"] += 1
                    continue

                ocr_data = ocr_result.get("data", {})

                # 自动匹配报单
                delivery_info = self._match_delivery_by_ocr(ocr_data)
                if not delivery_info:
                    results["failed_list"].append({
                        "index": idx,
                        "filename": f"image_{idx}",
                        "weigh_ticket_no": ocr_data.get("weigh_ticket_no"),
                        "vehicle_no": ocr_data.get("vehicle_no"),
                        "weigh_date": ocr_data.get("weigh_date"),
                        "error": "未找到匹配的报单（请检查日期和车牌号）"
                    })
                    results["failed_count"] += 1
                    continue

                delivery_id = delivery_info["id"]
                product_name = ocr_data.get("product_name") or delivery_info.get("product_name", "废电瓶")

                # 获取合同单价
                contract_no = ocr_data.get("contract_no") or delivery_info.get("contract_no")
                unit_price = self.get_contract_price_by_product(contract_no, product_name)

                # 构建磅单数据
                weighbill_data = {
                    "weigh_date": ocr_data.get("weigh_date"),
                    "weigh_ticket_no": ocr_data.get("weigh_ticket_no"),
                    "contract_no": contract_no,
                    "vehicle_no": ocr_data.get("vehicle_no"),
                    "gross_weight": ocr_data.get("gross_weight"),
                    "tare_weight": ocr_data.get("tare_weight"),
                    "net_weight": ocr_data.get("net_weight"),
                    "delivery_time": ocr_data.get("delivery_time"),
                    "unit_price": unit_price,
                    "warehouse": warehouse_name,
                    "payee": fixed_payee,
                }

                # 上传磅单（复用现有的 upload_weighbill 方法）
                upload_result = self.upload_weighbill(
                    delivery_id=delivery_id,
                    product_name=product_name,
                    data=weighbill_data,
                    image_file=image_bytes,
                    current_user=current_user,
                    is_manual=False
                )

                if upload_result.get("success"):
                    result_data = upload_result.get("data", {})
                    results["success_list"].append({
                        "index": idx,
                        "weighbill_id": result_data.get("weighbill_id"),
                        "delivery_id": delivery_id,
                        "product_name": product_name,
                        "vehicle_no": ocr_data.get("vehicle_no"),
                        "weigh_ticket_no": ocr_data.get("weigh_ticket_no"),
                        "net_weight": ocr_data.get("net_weight"),
                        "unit_price": unit_price,
                        "total_amount": result_data.get("total_amount"),
                        "payee": fixed_payee,
                        "warehouse": warehouse_name,
                    })
                    results["success_count"] += 1
                else:
                    results["failed_list"].append({
                        "index": idx,
                        "filename": f"image_{idx}",
                        "error": upload_result.get("error", "上传失败")
                    })
                    results["failed_count"] += 1

            except Exception as e:
                logger.error(f"处理第{idx}张图片失败: {e}")
                results["failed_list"].append({
                    "index": idx,
                    "filename": f"image_{idx}",
                    "error": str(e)
                })
                results["failed_count"] += 1

        return results

    # ========== 查询 ==========

    def get_weighbill(self, weighbill_id: int) -> Optional[Dict]:
        """获取磅单详情（包含报单信息）"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT w.*, d.report_date, d.warehouse, d.target_factory_name,
                               d.driver_name, d.driver_phone, d.driver_id_card,
                               d.has_delivery_order, d.shipper, d.payee, d.reporter_name,
                               d.service_fee, d.contract_no as d_contract_no,
                               pd.collection_status, pd.is_paid_out,
                               b.payout_status
                        FROM pd_weighbills w
                        LEFT JOIN pd_deliveries d ON w.delivery_id = d.id
                        LEFT JOIN pd_payment_details pd ON pd.weighbill_id = w.id
                        LEFT JOIN pd_balance_details b ON b.weighbill_id = w.id
                        WHERE w.id = %s
                    """, (weighbill_id,))
                    row = cur.fetchone()
                    if not row:
                        return None

                    columns = [desc[0] for desc in cur.description]
                    data = dict(zip(columns, row))

                    # 转换时间
                    for key in ["weigh_date", "delivery_time", "created_at", "updated_at", "uploaded_at", "payment_schedule_date", "report_date"]:
                        if data.get(key):
                            data[key] = str(data[key])

                    # 转换金额
                    for key in ["gross_weight", "tare_weight", "net_weight", "unit_price", "total_amount", "service_fee"]:
                        if data.get(key):
                            data[key] = float(data[key])

                    if data.get("warehouse_name") is None:
                        data["warehouse_name"] = data.get("warehouse")

                    # 显示字段
                    data["is_manual_corrected_display"] = "是" if data.get("is_manual_corrected") == 1 else "否"
                    data["ocr_status_display"] = data.get("ocr_status", "待上传磅单")
                    data["has_delivery_order_display"] = "是" if data.get("has_delivery_order") == "有" else "否"
                    payout_status = data.get("payout_status")
                    if payout_status is None:
                        payout_status = data.get("is_paid_out")
                    if payout_status is not None:
                        data["is_paid_out_display"] = "已打款" if payout_status == 1 else "待打款"
                    if data.get("collection_status") is not None:
                        collection_map = {
                            0: "待回款",
                            1: "已回首笔待回尾款",
                            2: "已回款",
                        }
                        data["collection_status_display"] = collection_map.get(data.get("collection_status"), "")

                    # 操作权限
                    is_uploaded = data.get("upload_status") == "已上传" and data.get("weighbill_image")
                    data["operations"] = {
                        "can_upload": not is_uploaded,
                        "can_modify": is_uploaded,
                        "can_view": is_uploaded
                    }

                    return data

        except Exception as e:
            logger.error(f"查询磅单失败: {e}")
            return None

    def list_weighbills_grouped(
            self,
            exact_shipper: str = None,
            exact_contract_no: str = None,
            exact_report_date: str = None,
            exact_driver_name: str = None,
            exact_vehicle_no: str = None,
            exact_weigh_date: str = None,
            exact_ocr_status: str = None,
            exact_delivery_id: int = None,
            exact_weighbill_id: int = None,
            exact_schedule_status: int = None,  # 新增：排款状态 0=待排期,1=已排期
            exact_payout_status: int = None,  # 新增：打款状态 0=待打款,1=已打款
            exact_collection_status: int = None,  # 新增：回款状态 0=待回款,1=已回首笔,2=已回款
            page: int = 1,
            page_size: int = 20
    ) -> Dict[str, Any]:
        """
        查询磅单列表（按报单ID分组）
        返回嵌套结构：报单信息 + 该报单下的所有磅单列表
        """
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # 构建报单查询条件
                    delivery_where = ["1=1"]
                    delivery_params = []

                    if exact_shipper:
                        delivery_where.append("d.shipper = %s")
                        delivery_params.append(exact_shipper)
                    if exact_contract_no:
                        delivery_where.append("d.contract_no = %s")
                        delivery_params.append(exact_contract_no)
                    if exact_report_date:
                        delivery_where.append("d.report_date = %s")
                        delivery_params.append(exact_report_date)
                    if exact_driver_name:
                        delivery_where.append("d.driver_name = %s")
                        delivery_params.append(exact_driver_name)
                    if exact_vehicle_no:
                        delivery_where.append("d.vehicle_no = %s")
                        delivery_params.append(exact_vehicle_no)
                    if exact_delivery_id is not None:
                        delivery_where.append("d.id = %s")
                        delivery_params.append(exact_delivery_id)

                    delivery_sql = " AND ".join(delivery_where)

                    # 查询报单总数
                    cur.execute(f"""
                        SELECT COUNT(DISTINCT d.id) 
                        FROM pd_deliveries d
                        WHERE {delivery_sql}
                    """, tuple(delivery_params))
                    total = cur.fetchone()[0]

                    # 分页查询报单ID
                    offset = (page - 1) * page_size
                    cur.execute(f"""
                        SELECT DISTINCT d.id,d.created_at
                        FROM pd_deliveries d
                        WHERE {delivery_sql}
                        ORDER BY d.created_at DESC
                        LIMIT %s OFFSET %s
                    """, tuple(delivery_params + [page_size, offset]))
                    delivery_ids = [row[0] for row in cur.fetchall()]

                    if not delivery_ids:
                        return {"success": True, "data": [], "total": 0, "page": page, "page_size": page_size}

                    # 查询报单详细信息
                    format_ids = ','.join(['%s'] * len(delivery_ids))
                    cur.execute(f"""
                        SELECT d.*,
                               (SELECT COUNT(*) FROM pd_weighbills WHERE delivery_id = d.id) as total_weighbills,
                               (SELECT COUNT(*) FROM pd_weighbills WHERE delivery_id = d.id AND upload_status = '已上传') as uploaded_weighbills
                        FROM pd_deliveries d
                        WHERE d.id IN ({format_ids})
                        ORDER BY d.created_at DESC
                    """, tuple(delivery_ids))

                    delivery_columns = [desc[0] for desc in cur.description]
                    delivery_rows = cur.fetchall()

                    # 查询这些报单的所有磅单（带筛选条件）
                    weighbill_where = [f"w.delivery_id IN ({format_ids})"]
                    weighbill_params = list(delivery_ids)

                    if exact_weigh_date:
                        weighbill_where.append("w.weigh_date = %s")
                        weighbill_params.append(exact_weigh_date)
                    if exact_ocr_status:
                        weighbill_where.append("w.ocr_status = %s")
                        weighbill_params.append(exact_ocr_status)
                    if exact_weighbill_id is not None:
                        weighbill_where.append("w.id = %s")
                        weighbill_params.append(exact_weighbill_id)

                    # === 新增状态筛选（修复 NULL 问题） ===
                    if exact_schedule_status is not None:
                        if exact_schedule_status == 0:
                            weighbill_where.append("(b.schedule_status = 0 OR b.schedule_status IS NULL)")
                        else:
                            weighbill_where.append("b.schedule_status = %s")
                            weighbill_params.append(exact_schedule_status)

                    if exact_payout_status is not None:
                        if exact_payout_status == 0:
                            weighbill_where.append("(b.payout_status = 0 OR b.payout_status IS NULL)")
                        else:
                            weighbill_where.append("b.payout_status = %s")
                            weighbill_params.append(exact_payout_status)

                    if exact_collection_status is not None:
                        if exact_collection_status == 0:
                            weighbill_where.append("(pd.collection_status = 0 OR pd.collection_status IS NULL)")
                        else:
                            weighbill_where.append("pd.collection_status = %s")
                            weighbill_params.append(exact_collection_status)

                    weighbill_sql = " AND ".join(weighbill_where)

                    cur.execute(f"""
                        SELECT w.*, 
                               d.report_date, d.warehouse, d.target_factory_name,
                               d.driver_name, d.driver_phone, d.driver_id_card,
                               d.has_delivery_order, d.shipper, d.payee, d.reporter_name,
                               d.service_fee,
                               b.schedule_status,
                               b.payout_status,
                               b.payable_amount as balance_payable_amount,
                               pd.collection_status, pd.is_paid_out
                        FROM pd_weighbills w
                        JOIN pd_deliveries d ON w.delivery_id = d.id
                        LEFT JOIN pd_balance_details b ON w.id = b.weighbill_id
                        LEFT JOIN pd_payment_details pd ON pd.weighbill_id = w.id
                        WHERE {weighbill_sql}
                        ORDER BY w.delivery_id, w.product_name
                    """, tuple(weighbill_params))

                    weighbill_columns = [desc[0] for desc in cur.description]
                    weighbill_rows = cur.fetchall()

                    # 组织磅单数据
                    weighbill_map = {}
                    for row in weighbill_rows:
                        wb = dict(zip(weighbill_columns, row))
                        delivery_id = wb['delivery_id']

                        # 转换字段
                        for key in ["weigh_date", "delivery_time", "created_at", "updated_at", "uploaded_at"]:
                            if wb.get(key):
                                wb[key] = str(wb[key])
                        for key in ["gross_weight", "tare_weight", "net_weight", "unit_price", "total_amount",
                                    "service_fee"]:
                            if wb.get(key):
                                wb[key] = float(wb[key])

                        if wb.get("warehouse_name") is None:
                            wb["warehouse_name"] = wb.get("warehouse")

                        # ========== 新增：计算应付单价、应付金额和回款金额 ==========
                        unit_price_val = wb.get("unit_price")
                        net_weight_val = wb.get("net_weight")
                        service_fee_val = wb.get("service_fee") or 0

                        if unit_price_val:
                            unit_price_d = Decimal(str(unit_price_val))

                            # 应付单价 = 合同单价 / 1.048
                            payable_unit_price = (unit_price_d / Decimal('1.048')).quantize(Decimal('0.01'),
                                                                                            rounding=ROUND_HALF_UP)
                            wb["payable_unit_price"] = float(payable_unit_price)

                            if net_weight_val:
                                net_weight_d = Decimal(str(net_weight_val))
                                service_fee_d = Decimal(str(service_fee_val))

                                # 应付金额 = 应付单价 * 磅单净重 - 联单费
                                payable_calc = (payable_unit_price * net_weight_d - service_fee_d).quantize(
                                    Decimal('0.01'), rounding=ROUND_HALF_UP)
                                wb["payable_amount_calculated"] = float(payable_calc)

                                # 回款金额 = 合同单价 * 磅单净重 - 联单费
                                receivable_calc = (unit_price_d * net_weight_d - service_fee_d).quantize(
                                    Decimal('0.01'), rounding=ROUND_HALF_UP)
                                wb["receivable_amount_calculated"] = float(receivable_calc)
                            else:
                                wb["payable_amount_calculated"] = None
                                wb["receivable_amount_calculated"] = None
                        else:
                            wb["payable_unit_price"] = None
                            wb["payable_amount_calculated"] = None
                            wb["receivable_amount_calculated"] = None
                        # ========== 新增结束 ==========

                        # 应打款金额：优先使用结余表应付金额；无结余时按公式估算
                        if wb.get("balance_payable_amount") is not None:
                            wb["payable_amount"] = float(wb.get("balance_payable_amount") or 0)
                        else:
                            net_weight = Decimal(str(wb.get("net_weight") or 0))
                            unit_price = Decimal(str(wb.get("unit_price") or 0))
                            wb["payable_amount"] = float(
                                (net_weight * unit_price / Decimal('1.048')).quantize(Decimal('0.01'),
                                                                                     rounding=ROUND_HALF_UP)
                            )

                        wb["is_manual_corrected_display"] = "是" if wb.get("is_manual_corrected") == 1 else "否"
                        wb["ocr_status_display"] = wb.get("ocr_status", "待上传磅单")
                        wb["has_delivery_order_display"] = "是" if wb.get("has_delivery_order") == "有" else "否"
                        payout_status = wb.get("payout_status")
                        if payout_status is None:
                            payout_status = wb.get("is_paid_out")
                        if payout_status is not None:
                            wb["is_paid_out_display"] = "已打款" if payout_status == 1 else "待打款"
                        if wb.get("collection_status") is not None:
                            collection_map = {
                                0: "待回款",
                                1: "已回首笔待回尾款",
                                2: "已回款",
                            }
                            wb["collection_status_display"] = collection_map.get(wb.get("collection_status"), "")

                        is_uploaded = wb.get("upload_status") == "已上传" and wb.get("weighbill_image")
                        wb["operations"] = {
                            "can_upload": not is_uploaded,
                            "can_modify": is_uploaded,
                            "can_view": is_uploaded
                        }

                        if delivery_id not in weighbill_map:
                            weighbill_map[delivery_id] = []
                        weighbill_map[delivery_id].append(wb)

                    # 组装结果
                    result_data = []
                    for row in delivery_rows:
                        delivery = dict(zip(delivery_columns, row))

                        for key in ['report_date', 'created_at', 'updated_at', 'uploaded_at']:
                            if delivery.get(key):
                                delivery[key] = str(delivery[key])

                        if delivery.get('products'):
                            delivery['products'] = [p.strip() for p in delivery['products'].split(',') if p.strip()]
                        else:
                            delivery['products'] = [delivery.get('product_name')] if delivery.get(
                                'product_name') else []

                        delivery["has_delivery_order_display"] = "是" if delivery.get(
                            "has_delivery_order") == "有" else "否"
                        delivery["upload_status_display"] = "是" if delivery.get("upload_status") == "已上传" else "否"
                        if delivery.get('service_fee'):
                            delivery['service_fee'] = float(delivery['service_fee'])

                        delivery_id = delivery['id']
                        weighbills = weighbill_map.get(delivery_id, [])

                        # 如果没有磅单记录，创建待上传占位
                        if not weighbills:
                            # 占位磅单只应在以下情况下出现：
                            # 1. OCR 状态筛选为 None 或 "待上传磅单"
                            if exact_ocr_status and exact_ocr_status != "待上传磅单":
                                continue
                            # 2. 排款状态筛选为 None 或 0（待排期）
                            if exact_schedule_status is not None and exact_schedule_status != 0:
                                continue
                            # 3. 打款状态筛选为 None 或 0（待打款）
                            if exact_payout_status is not None and exact_payout_status != 0:
                                continue
                            # 4. 回款状态筛选为 None 或 0（待回款）
                            if exact_collection_status is not None and exact_collection_status != 0:
                                continue

                            for product in delivery.get('products', []):
                                weighbills.append({
                                    "id": None,
                                    "delivery_id": delivery_id,
                                    "product_name": product,
                                    "warehouse_name": delivery.get("warehouse"),
                                    "ocr_status": "待上传磅单",
                                    "ocr_status_display": "待上传磅单",
                                    "upload_status": "待上传",
                                    "payable_unit_price": None,
                                    "payable_amount_calculated": None,
                                    "receivable_amount_calculated": None,
                                    "operations": {"can_upload": True, "can_modify": False, "can_view": False}
                                })
                        result_data.append({
                            "delivery_id": delivery_id,
                            "contract_no": delivery.get("contract_no"),
                            "report_date": delivery.get("report_date"),
                            "target_factory_name": delivery.get("target_factory_name"),
                            "driver_phone": delivery.get("driver_phone"),
                            "driver_name": delivery.get("driver_name"),
                            "driver_id_card": delivery.get("driver_id_card"),
                            "vehicle_no": delivery.get("vehicle_no"),
                            "has_delivery_order": delivery.get("has_delivery_order"),
                            "has_delivery_order_display": delivery.get("has_delivery_order_display"),
                            "upload_status": delivery.get("upload_status"),
                            "upload_status_display": delivery.get("upload_status_display"),
                            "shipper": delivery.get("shipper"),
                            "reporter_name": delivery.get("reporter_name"),
                            "payee": delivery.get("payee"),
                            "warehouse": delivery.get("warehouse"),
                            "service_fee": delivery.get("service_fee"),
                            "payable_amount": round(sum((wb.get("payable_amount") or 0) for wb in weighbills), 2),
                            "total_weighbills": delivery.get("total_weighbills", 0),
                            "uploaded_weighbills": delivery.get("uploaded_weighbills", 0),
                            "weighbills": weighbills
                        })

                    return {
                        "success": True,
                        "data": result_data,
                        "total": total,
                        "page": page,
                        "page_size": page_size
                    }

        except Exception as e:
            logger.error(f"查询磅单列表失败: {e}")
            return {"success": False, "error": str(e), "data": [], "total": 0}

    # ========== 排款日期 ==========

    def set_payment_schedule_date(self, weighbill_id: int, payment_schedule_date: str) -> Dict[str, Any]:
        """设置磅单排款日期，同时更新结余明细的排期状态"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT id FROM pd_weighbills WHERE id = %s", (weighbill_id,))
                    if not cur.fetchone():
                        return {"success": False, "error": "磅单不存在"}

                    # 更新磅单排款日期
                    cur.execute("""
                        UPDATE pd_weighbills 
                        SET payment_schedule_date = %s, updated_at = NOW()
                        WHERE id = %s
                    """, (payment_schedule_date, weighbill_id))

                    # 同时更新结余明细的排期状态和排款日期
                    cur.execute("""
                        UPDATE pd_balance_details 
                        SET schedule_date = %s, schedule_status = 1, updated_at = NOW()
                        WHERE weighbill_id = %s
                    """, (payment_schedule_date, weighbill_id))

                    return {
                        "success": True,
                        "message": "排款日期设置成功",
                        "data": {"id": weighbill_id, "payment_schedule_date": payment_schedule_date, "schedule_status": 1}
                    }

        except Exception as e:
            logger.error(f"设置排款日期失败: {e}")
            return {"success": False, "error": str(e)}


_weighbill_service = None


def get_weighbill_service():
    global _weighbill_service
    if _weighbill_service is None:
        _weighbill_service = WeighbillService()
    return _weighbill_service