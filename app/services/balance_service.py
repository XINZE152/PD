"""
磅单结余管理 + 支付回单处理服务（优化版）
"""
import logging
import os
import re
import shutil
import tempfile
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta

from PIL import Image, ImageEnhance, ImageFilter

try:
    from rapidocr_onnxruntime import RapidOCR

    RAPIDOCR_AVAILABLE = True
except ImportError:
    RAPIDOCR_AVAILABLE = False

from app.core.paths import UPLOADS_DIR
from app.services.contract_service import get_conn

logger = logging.getLogger(__name__)

UPLOAD_DIR = UPLOADS_DIR / "payment_receipts"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


class BalanceService:
    """磅单结余服务"""

    def __init__(self):
        self.ocr = None
        if RAPIDOCR_AVAILABLE:
            try:
                self.ocr = RapidOCR()
                logger.info("支付回单OCR初始化成功")
            except Exception as e:
                logger.error(f"支付回单OCR初始化失败: {e}")

    # ========== 状态常量 ==========
    OCR_STATUS_PENDING = 0  # 待确认
    OCR_STATUS_CONFIRMED = 1  # 已确认
    OCR_STATUS_VERIFIED = 2  # 已核销

    PAY_STATUS_PENDING = 0  # 待支付
    PAY_STATUS_PARTIAL = 1  # 部分支付
    PAY_STATUS_SETTLED = 2  # 已结清

    # ========== 磅单结余生成 ==========

    def generate_balance_details(self, contract_no: str = None,
                                 delivery_id: int = None,
                                 weighbill_id: int = None) -> Dict[str, Any]:
        """
        根据磅单数据自动生成结余明细
        应付金额 = 净重 × 合同单价
        """
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # 构建查询条件
                    conditions = ["w.ocr_status IN ('已确认','已上传磅单')"]
                    params = []

                    if contract_no:
                        conditions.append("w.contract_no = %s")
                        params.append(contract_no)
                    if delivery_id:
                        conditions.append("w.delivery_id = %s")
                        params.append(delivery_id)
                    if weighbill_id:
                        conditions.append("w.id = %s")
                        params.append(weighbill_id)

                    # 排除已生成结余的磅单
                    conditions.append("NOT EXISTS (SELECT 1 FROM pd_balance_details b WHERE b.weighbill_id = w.id)")

                    where_sql = " AND ".join(conditions)

                    # 查询符合条件的磅单（增加获取payee和uploader_id）
                    cur.execute(f"""
                        SELECT 
                            w.id as weighbill_id,
                            w.contract_no,
                            w.delivery_id,
                            w.vehicle_no,
                            w.product_name,
                            w.net_weight,
                            w.unit_price,
                            d.driver_name,
                            d.driver_phone,
                            d.payee,
                            d.uploader_id
                        FROM pd_weighbills w
                        LEFT JOIN pd_deliveries d ON w.delivery_id = d.id
                        WHERE {where_sql}
                    """, tuple(params))

                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()

                    generated = []
                    for row in rows:
                        data = dict(zip(columns, row))

                        # 计算应付金额
                        net_weight = data.get('net_weight') or 0
                        unit_price = data.get('unit_price') or 0
                        payable = Decimal(str(net_weight)) * Decimal(str(unit_price))

                        # 确定收款人姓名：优先payee，否则driver_name
                        receiver_name = data.get('payee') if data.get('payee') else data.get('driver_name')

                        # 插入结余明细（增加payee_id）
                        cur.execute("""
                            INSERT INTO pd_balance_details 
                            (contract_no, delivery_id, weighbill_id, driver_name, driver_phone,
                             vehicle_no, payee_id, payable_amount, paid_amount, balance_amount, payment_status)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            data.get('contract_no'),
                            data.get('delivery_id'),
                            data.get('weighbill_id'),
                            receiver_name,  # 收款人姓名
                            data.get('driver_phone'),
                            data.get('vehicle_no'),
                            data.get('uploader_id'),  # 收款人ID
                            payable,  # 应付金额
                            0,  # 已付金额
                            payable,  # 结余金额
                            self.PAY_STATUS_PENDING
                        ))

                        generated.append({
                            'balance_id': cur.lastrowid,
                            'weighbill_id': data.get('weighbill_id'),
                            'driver_name': receiver_name,
                            'payable_amount': float(payable)
                        })

                    return {
                        "success": True,
                        "message": f"成功生成 {len(generated)} 条结余明细",
                        "data": generated
                    }

        except Exception as e:
            logger.error(f"生成结余明细失败: {e}")
            return {"success": False, "error": str(e)}

    def recalculate_balance(self, balance_id: int) -> Dict[str, Any]:
        """重新计算结余金额和状态"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # 获取当前数据
                    cur.execute("""
                        SELECT payable_amount, paid_amount 
                        FROM pd_balance_details 
                        WHERE id = %s
                    """, (balance_id,))

                    row = cur.fetchone()
                    if not row:
                        return {"success": False, "error": "结余明细不存在"}

                    payable, paid = Decimal(str(row[0])), Decimal(str(row[1]))
                    balance = payable - paid

                    # 确定状态
                    if paid <= 0:
                        status = self.PAY_STATUS_PENDING
                    elif paid >= payable:
                        status = self.PAY_STATUS_SETTLED
                    else:
                        status = self.PAY_STATUS_PARTIAL

                    # 更新
                    cur.execute("""
                        UPDATE pd_balance_details 
                        SET balance_amount = %s, payment_status = %s 
                        WHERE id = %s
                    """, (balance, status, balance_id))

                    return {
                        "success": True,
                        "data": {
                            'payable': float(payable),
                            'paid': float(paid),
                            'balance': float(balance),
                            'status': status
                        }
                    }

        except Exception as e:
            logger.error(f"重新计算结余失败: {e}")
            return {"success": False, "error": str(e)}

    # ========== 支付回单OCR（待完善） ==========

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

    def recognize_payment_receipt(self, image_path: str) -> Dict[str, Any]:
        """
        OCR识别支付回单
        支持格式：农业银行等标准转账回单格式
        """
        if not self.ocr:
            return {
                "success": True,
                "data": self._empty_receipt_result("OCR未初始化"),
                "ocr_success": False
            }

        try:
            result, elapse = self.ocr(image_path)
            total_elapse = sum(elapse) if isinstance(elapse, list) else float(elapse or 0)

            if not result:
                return {
                    "success": True,
                    "data": self._empty_receipt_result("未能识别到文本"),
                    "ocr_success": False
                }

            text_lines = []
            for item in result:
                bbox, text, confidence = item
                text_lines.append({"text": text.strip(), "confidence": float(confidence)})

            full_text = "\n".join([line["text"] for line in text_lines])

            logger.info("=== 支付回单OCR识别文本 ===")
            for i, line in enumerate(text_lines):
                logger.info(f"{i}: {line['text']}")

            # 解析回单字段
            parsed_data = self._parse_receipt_text(full_text, text_lines)

            # 构造返回数据
            data = {
                # 基础信息
                "receipt_no": parsed_data.get("receipt_no"),
                "payment_date": parsed_data.get("payment_date"),
                "payment_time": parsed_data.get("payment_time"),

                # 金额信息（三个字段）
                "amount": parsed_data.get("amount"),  # 转账金额（小写）
                "fee": parsed_data.get("fee", 0.0),  # 手续费
                "total_amount": parsed_data.get("total_amount"),  # 合计（小写）

                # 付款方信息
                "payer_name": parsed_data.get("payer_name"),
                "payer_account": parsed_data.get("payer_account"),
                "bank_name": parsed_data.get("bank_name"),  # 付款行

                # 收款方信息
                "payee_name": parsed_data.get("payee_name"),
                "payee_account": parsed_data.get("payee_account"),
                "payee_bank_name": parsed_data.get("payee_bank_name"),  # 收款行

                # 其他
                "remark": parsed_data.get("remark"),

                # 元信息
                "ocr_message": "识别成功" if parsed_data.get("receipt_no") else "识别完成，部分字段可能需要人工核对",
                "raw_text": full_text,
                "ocr_time": round(total_elapse, 3)
            }

            return {
                "success": True,
                "data": data,
                "ocr_success": True
            }

        except Exception as e:
            logger.error(f"支付回单识别异常: {e}")
            return {
                "success": True,
                "data": self._empty_receipt_result(f"识别异常: {str(e)}"),
                "ocr_success": False
            }


    def _parse_receipt_text(self, full_text: str, text_lines: List[Dict]) -> Dict[str, Any]:
        """
        解析回单文本，提取关键字段
        适配标准银行转账回单格式（农行等）
        """
        import re

        result = {}
        lines = [line["text"] for line in text_lines]

        # 构建位置映射（用于处理跨行字段和表格结构）
        line_map = {i: line["text"].strip() for i, line in enumerate(text_lines)}

        # ========== 1. 基础信息 ==========

        # 网银流水号
        receipt_patterns = [
            r'网银流水号[：:]?\s*(\d{16,20})',
            r'回单编号[：:]?\s*(\d{16,20})',
            r'交易单号[：:]?\s*(\d{16,20})',
            r'流水号[：:]?\s*(\d{16,20})',
        ]
        for pattern in receipt_patterns:
            match = re.search(pattern, full_text)
            if match:
                result["receipt_no"] = match.group(1)
                break

        # 备用：找16-20位数字（排除日期时间）
        if not result.get("receipt_no"):
            for line in lines:
                nums = re.findall(r'\b(\d{16,20})\b', line)
                for num in nums:
                    if not re.match(r'^(20\d{12})$', num):  # 排除时间戳
                        result["receipt_no"] = num
                        break
                if result.get("receipt_no"):
                    break

        # 交易日期和时间
        datetime_patterns = [
            r'交易时间[：:]?\s*(\d{4}[-/年]\d{1,2}[-/月]\d{1,2})[日\s]*(\d{1,2}:\d{2}:\d{2})?',
            r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})',
        ]
        for pattern in datetime_patterns:
            match = re.search(pattern, full_text)
            if match:
                date_str = match.group(1).replace('年', '-').replace('月', '-').replace('/', '-').rstrip('-')
                parts = date_str.split('-')
                if len(parts) == 3:
                    date_str = f"{parts[0]}-{int(parts[1]):02d}-{int(parts[2]):02d}"
                result["payment_date"] = date_str
                if len(match.groups()) > 1 and match.group(2):
                    result["payment_time"] = match.group(2)
                break

        # ========== 2. 金额相关（区分转账金额、手续费、合计） ==========

        # 2.1 转账金额（小写）- 优先匹配"转账金额"关键词
        transfer_amount = None
        transfer_patterns = [
            r'转账金额[（(]小写[）)]?[：:]?\s*[¥￥]?\s*([\d,]+\.?\d{0,2})',
            r'转账金额[：:]?\s*[¥￥]?\s*([\d,]+\.?\d{0,2})',
            r'交易金额[：:]?\s*[¥￥]?\s*([\d,]+\.?\d{0,2})',
            r'汇款金额[：:]?\s*[¥￥]?\s*([\d,]+\.?\d{0,2})',
        ]
        for pattern in transfer_patterns:
            match = re.search(pattern, full_text)
            if match:
                amount_str = match.group(1).replace(',', '')
                try:
                    transfer_amount = float(amount_str)
                    result["amount"] = transfer_amount
                except ValueError:
                    pass
                break

        # 2.2 手续费
        fee_amount = 0.0
        fee_patterns = [
            r'手续费[：:]?\s*[¥￥]?\s*([\d,]+\.?\d{0,2})',
            r'费用[：:]?\s*[¥￥]?\s*([\d,]+\.?\d{0,2})',
        ]
        for pattern in fee_patterns:
            match = re.search(pattern, full_text)
            if match:
                fee_str = match.group(1).replace(',', '')
                try:
                    fee_amount = float(fee_str)
                    result["fee"] = fee_amount
                except ValueError:
                    pass
                break

        # 如果没识别到手续费，默认为0
        if "fee" not in result:
            result["fee"] = 0.0

        # 2.3 合计（小写）- 新增字段
        total_amount = None
        total_patterns = [
            r'合计[（(]小写[）)]?[：:]?\s*[¥￥]?\s*([\d,]+\.?\d{0,2})',
            r'合计金额[：:]?\s*[¥￥]?\s*([\d,]+\.?\d{0,2})',
            r'总计[：:]?\s*[¥￥]?\s*([\d,]+\.?\d{0,2})',
        ]
        for pattern in total_patterns:
            match = re.search(pattern, full_text)
            if match:
                total_str = match.group(1).replace(',', '')
                try:
                    total_amount = float(total_str)
                    result["total_amount"] = total_amount
                except ValueError:
                    pass
                break

        # 如果合计没识别到，但识别到了转账金额和手续费，自动计算
        if total_amount is None and transfer_amount is not None:
            result["total_amount"] = transfer_amount + fee_amount

        # ========== 3. 付款方信息 ==========

        # 付款户名（处理跨行）
        payer_name = None
        for i, line in enumerate(lines):
            if '账户户名' in line or (line == '付款方' and i < len(lines) - 1):
                for j in range(i + 1, min(i + 3, len(lines))):
                    next_line = lines[j].strip()
                    # 脱敏名：*开源
                    if re.match(r'^[*＊][\u4e00-\u9fa5a-zA-Z]+$', next_line):
                        payer_name = next_line.replace('＊', '*')
                        break
                    # 纯中文名
                    elif re.match(r'^[\u4e00-\u9fa5]{2,4}$', next_line) and next_line not in ['收款方', '付款方',
                                                                                              '开户行']:
                        payer_name = next_line
                        break
            if payer_name:
                break

        if not payer_name:
            match = re.search(r'账户户名[：:]?\s*([*＊][\u4e00-\u9fa5]+)', full_text)
            if match:
                payer_name = match.group(1).replace('＊', '*')

        if payer_name:
            result["payer_name"] = payer_name

        # 付款账户（脱敏卡号）
        payer_account_patterns = [
            r'付款账户[：:]?\s*(\d{6,8}[*＊]+\d{3,4})',
            r'付款账号[：:]?\s*(\d{6,8}[*＊]+\d{3,4})',
        ]
        for pattern in payer_account_patterns:
            match = re.search(pattern, full_text)
            if match:
                result["payer_account"] = match.group(1).replace('＊', '*')
                break

        if not result.get("payer_account"):
            match = re.search(r'(\d{4,6}[*＊]{2,6}\d{3,4})', full_text)
            if match:
                result["payer_account"] = match.group(1).replace('＊', '*')

        # ========== 4. 收款方信息 ==========

        # 收款户名
        payee_name = None
        for i, line in enumerate(lines):
            if line == '收款方' and i < len(lines) - 1:
                for j in range(i + 1, min(i + 3, len(lines))):
                    next_line = lines[j].strip()
                    if re.match(r'^[\u4e00-\u9fa5]{2,4}$', next_line) and next_line not in ['付款方', '收款方',
                                                                                            '开户行', '账户户名']:
                        payee_name = next_line
                        break
                    elif re.match(r'^[*＊][\u4e00-\u9fa5]+$', next_line):
                        payee_name = next_line.replace('＊', '*')
                        break
            if payee_name:
                break

        if not payee_name:
            match = re.search(r'收款方[：:]?\s*([\u4e00-\u9fa5]{2,4})', full_text)
            if match:
                payee_name = match.group(1)

        if payee_name:
            result["payee_name"] = payee_name

        # 收款账户（完整卡号）
        payee_account_patterns = [
            r'收款账户[：:]?\s*(\d{16,19})',
            r'收款账号[：:]?\s*(\d{16,19})',
        ]
        for pattern in payee_account_patterns:
            match = re.search(pattern, full_text)
            if match:
                result["payee_account"] = match.group(1)
                break

        if not result.get("payee_account"):
            all_cards = re.findall(r'\b(\d{16,19})\b', full_text)
            valid_cards = [c for c in all_cards if 16 <= len(c) <= 19]
            if valid_cards:
                result["payee_account"] = valid_cards[0]

        # ========== 5. 银行信息（付款行 + 收款行） ==========

        # 收集所有开户行信息（带位置索引）
        bank_list = []
        for i, line in enumerate(lines):
            if '开户行' in line and i < len(lines) - 1:
                next_line = lines[i + 1].strip()
                # 匹配标准银行名称
                if re.match(r'^[中国工农建邮储交通招商民生中信光大浦发平安华夏兴业广发]+银行', next_line):
                    bank_list.append({"index": i, "name": next_line})

        # 第一个开户行 = 付款行
        if len(bank_list) >= 1:
            result["bank_name"] = bank_list[0]["name"]

        # 第二个开户行 = 收款行（新增字段）
        if len(bank_list) >= 2:
            result["payee_bank_name"] = bank_list[1]["name"]
        else:
            # 备用：尝试通过"收款方"附近找银行名
            for i, line in enumerate(lines):
                if '收款方' in line:
                    # 向后查找银行名
                    for j in range(i, min(i + 5, len(lines))):
                        if re.match(r'^[中国工农建邮储交通招商民生中信光大浦发平安华夏兴业广发]+银行', lines[j]):
                            result["payee_bank_name"] = lines[j]
                            break
                    break

        # ========== 6. 附言/备注 ==========

        remark_patterns = [
            r'附言[：:]?\s*([^\n]+)',
            r'备注[：:]?\s*([^\n]+)',
            r'用途[：:]?\s*([^\n]+)',
        ]
        for pattern in remark_patterns:
            match = re.search(pattern, full_text)
            if match:
                remark = match.group(1).strip()
                if not any(x in remark for x in ['致电商', '客服热线', '不作为', '重要提示', '上列款项']):
                    result["remark"] = remark
                    break

        return result


    def _empty_receipt_result(self, message: str) -> Dict:
        """返回空结果结构"""
        return {
            "receipt_no": None,
            "payment_date": None,
            "payment_time": None,
            "amount": None,
            "fee": 0.0,
            "total_amount": None,  # 新增
            "payer_name": None,
            "payer_account": None,
            "bank_name": None,
            "payee_name": None,
            "payee_account": None,
            "payee_bank_name": None,
            "remark": None,
            "ocr_message": message,
            "raw_text": "",
            "ocr_time": 0
        }

    # ========== 匹配核销逻辑 ==========

    def match_pending_payments(self, payee_name: str, amount: float,
                               date_range: int = 7) -> List[Dict]:
        """
        根据收款人+金额匹配待支付结余
        使用组合索引 (payee_name, amount) 提高查询效率
        """
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # 查询待支付数据，允许金额误差0.01
                    cur.execute("""
                        SELECT * FROM pd_balance_details 
                        WHERE payment_status IN (0, 1)
                        AND driver_name LIKE %s
                        AND ABS(payable_amount - %s) <= 0.01
                        AND created_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
                        ORDER BY balance_amount DESC, created_at ASC
                        LIMIT 10
                    """, (f"%{payee_name}%", amount, date_range))

                    columns = [desc[0] for desc in cur.description]
                    results = []
                    for row in cur.fetchall():
                        data = dict(zip(columns, row))
                        for key in ['created_at', 'updated_at']:
                            if data.get(key):
                                data[key] = str(data[key])
                        results.append(data)

                    return results

        except Exception as e:
            logger.error(f"匹配待支付数据失败: {e}")
            return []

    def verify_payment(self, receipt_id: int, balance_items: List[Dict]) -> Dict[str, Any]:
        """
        核销支付（支持分批核销）
        balance_items: [{"balance_id": 1, "amount": 1000}, ...]
        """
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # 获取回单信息
                    cur.execute("""
                        SELECT amount, ocr_status 
                        FROM pd_payment_receipts 
                        WHERE id = %s
                    """, (receipt_id,))

                    row = cur.fetchone()
                    if not row:
                        return {"success": False, "error": "支付回单不存在"}

                    receipt_amount, ocr_status = Decimal(str(row[0])), row[1]

                    if ocr_status == self.OCR_STATUS_VERIFIED:
                        return {"success": False, "error": "该回单已核销"}

                    total_settled = Decimal('0')
                    settled_items = []

                    for item in balance_items:
                        balance_id = item.get('balance_id')
                        settle_amount = Decimal(str(item.get('amount', 0)))

                        # 获取结余当前状态
                        cur.execute("""
                            SELECT payable_amount, paid_amount, payment_status
                            FROM pd_balance_details 
                            WHERE id = %s
                        """, (balance_id,))

                        row = cur.fetchone()
                        if not row:
                            continue

                        payable, paid, status = Decimal(str(row[0])), Decimal(str(row[1])), row[2]

                        # 验证核销金额
                        remaining = payable - paid
                        if settle_amount > remaining:
                            settle_amount = remaining  # 不能超过剩余应付

                        new_paid = paid + settle_amount

                        # 确定新状态
                        if new_paid >= payable:
                            new_status = self.PAY_STATUS_SETTLED
                        elif new_paid > 0:
                            new_status = self.PAY_STATUS_PARTIAL
                        else:
                            new_status = self.PAY_STATUS_PENDING

                        # 更新结余明细
                        new_balance = payable - new_paid
                        cur.execute("""
                            UPDATE pd_balance_details 
                            SET paid_amount = %s, balance_amount = %s, payment_status = %s 
                            WHERE id = %s
                        """, (new_paid, new_balance, new_status, balance_id))

                        # 插入关联表
                        cur.execute("""
                            INSERT INTO pd_receipt_settlements 
                            (receipt_id, balance_id, settled_amount)
                            VALUES (%s, %s, %s)
                            ON DUPLICATE KEY UPDATE settled_amount = %s
                        """, (receipt_id, balance_id, settle_amount, settle_amount))

                        total_settled += settle_amount
                        settled_items.append({
                            'balance_id': balance_id,
                            'settled_amount': float(settle_amount),
                            'status': new_status
                        })

                    # 更新回单状态
                    new_receipt_status = self.OCR_STATUS_VERIFIED if total_settled >= receipt_amount else self.OCR_STATUS_CONFIRMED
                    cur.execute("""
                        UPDATE pd_payment_receipts 
                        SET ocr_status = %s 
                        WHERE id = %s
                    """, (new_receipt_status, receipt_id))

                    return {
                        "success": True,
                        "message": f"成功核销 {len(settled_items)} 条明细",
                        "data": {
                            'receipt_id': receipt_id,
                            'total_settled': float(total_settled),
                            'receipt_status': new_receipt_status,
                            'items': settled_items
                        }
                    }

        except Exception as e:
            logger.error(f"核销支付失败: {e}")
            return {"success": False, "error": str(e)}

    # ========== CRUD操作 ==========

    def create_payment_receipt(self, data: Dict, image_path: str,
                               is_manual: bool = False) -> Dict[str, Any]:
        """创建支付回单记录"""
        try:
            # 自动计算合计金额（如果未提供）
            amount = Decimal(str(data.get('amount', 0)))
            fee = Decimal(str(data.get('fee', 0)))
            total_amount = data.get('total_amount')

            if total_amount is None:
                total_amount = amount + fee
            else:
                total_amount = Decimal(str(total_amount))

            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO pd_payment_receipts 
                        (receipt_no, receipt_image, payment_date, payment_time,
                         payer_name, payer_account, payee_name, payee_account,
                         amount, fee, total_amount, bank_name, payee_bank_name, remark, 
                         ocr_status, ocr_raw_data, is_manual_corrected)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        data.get('receipt_no'),
                        image_path,
                        data.get('payment_date'),
                        data.get('payment_time'),
                        data.get('payer_name'),
                        data.get('payer_account'),
                        data.get('payee_name'),
                        data.get('payee_account'),
                        amount,
                        fee,
                        total_amount,  # 新增
                        data.get('bank_name'),
                        data.get('payee_bank_name'),
                        data.get('remark'),
                        self.OCR_STATUS_CONFIRMED if is_manual else self.OCR_STATUS_PENDING,
                        data.get('raw_text'),
                        1 if is_manual else 0
                    ))

                    return {
                        "success": True,
                        "message": "支付回单保存成功",
                        "data": {"id": cur.lastrowid}
                    }

        except Exception as e:
            logger.error(f"保存支付回单失败: {e}")
            return {"success": False, "error": str(e)}

    def get_balance_detail(self, balance_id: int) -> Optional[Dict]:
        """获取结余明细详情（包含核销记录）"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # 主表
                    cur.execute("""
                        SELECT b.*, w.weighbill_image, d.payee as payee_name
                        FROM pd_balance_details b
                        LEFT JOIN pd_weighbills w ON b.weighbill_id = w.id
                        LEFT JOIN pd_deliveries d ON b.delivery_id = d.id
                        WHERE b.id = %s
                    """, (balance_id,))

                    row = cur.fetchone()
                    if not row:
                        return None

                    columns = [desc[0] for desc in cur.description]
                    data = dict(zip(columns, row))

                    # 转换时间
                    for key in ['created_at', 'updated_at']:
                        if data.get(key):
                            data[key] = str(data[key])

                    # 查询关联的支付回单
                    cur.execute("""
                        SELECT r.id, r.payee_name, r.amount, r.payment_date, 
                               s.settled_amount, r.receipt_image
                        FROM pd_receipt_settlements s
                        JOIN pd_payment_receipts r ON s.receipt_id = r.id
                        WHERE s.balance_id = %s
                        ORDER BY s.created_at DESC
                    """, (balance_id,))

                    receipts = []
                    for r in cur.fetchall():
                        receipts.append({
                            'receipt_id': r[0],
                            'payee_name': r[1],
                            'amount': float(r[2]) if r[2] else None,
                            'payment_date': str(r[3]) if r[3] else None,
                            'settled_amount': float(r[4]) if r[4] else None,
                            'receipt_image': r[5]
                        })

                    data['payment_receipts'] = receipts
                    return data

        except Exception as e:
            logger.error(f"查询结余明细失败: {e}")
            return None

    def list_balance_details(self,
                             exact_contract_no: str = None,
                             exact_driver_name: str = None,
                             fuzzy_keywords: str = None,
                             payment_status: int = None,
                             page: int = 1,
                             page_size: int = 20) -> Dict[str, Any]:
        """查询结余明细列表（扩展版，包含关联信息）"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    conditions = ["1=1"]
                    params = []

                    if exact_contract_no:
                        conditions.append("b.contract_no = %s")
                        params.append(exact_contract_no)
                    if exact_driver_name:
                        conditions.append("b.driver_name = %s")
                        params.append(exact_driver_name)
                    if fuzzy_keywords:
                        tokens = [t for t in fuzzy_keywords.split() if t]
                        or_clauses = []
                        for token in tokens:
                            like = f"%{token}%"
                            or_clauses.append(
                                "(b.contract_no LIKE %s OR b.driver_name LIKE %s OR b.driver_phone LIKE %s OR b.vehicle_no LIKE %s)"
                            )
                            params.extend([like, like, like, like])
                        if or_clauses:
                            conditions.append("(" + " OR ".join(or_clauses) + ")")
                    if payment_status is not None:
                        conditions.append("b.payment_status = %s")
                        params.append(payment_status)

                    where_sql = " AND ".join(conditions)

                    # 查询总数
                    cur.execute(f"""
                        SELECT COUNT(*) FROM pd_balance_details b
                        WHERE {where_sql}
                    """, tuple(params))
                    total = cur.fetchone()[0]

                    # 分页数据 - 关联磅单获取图片
                    offset = (page - 1) * page_size
                    cur.execute(f"""
                        SELECT 
                            b.*,
                            w.weighbill_image,
                            w.weigh_date,
                            w.weigh_ticket_no,
                            w.net_weight,
                            w.product_name,
                            w.unit_price as weighbill_unit_price,
                            d.report_date,
                            d.target_factory_name,
                            d.driver_id_card,
                            d.has_delivery_order,
                            d.upload_status,
                            d.shipper,
                            d.service_fee,
                            (SELECT COUNT(*) FROM pd_receipt_settlements rs 
                             JOIN pd_payment_receipts pr ON rs.receipt_id = pr.id 
                             WHERE rs.balance_id = b.id) as receipt_count
                        FROM pd_balance_details b
                        LEFT JOIN pd_weighbills w ON b.weighbill_id = w.id
                        LEFT JOIN pd_deliveries d ON b.delivery_id = d.id
                        WHERE {where_sql}
                        ORDER BY b.created_at DESC
                        LIMIT %s OFFSET %s
                    """, tuple(params + [page_size, offset]))

                    columns = [desc[0] for desc in cur.description]
                    data = []
                    status_map = {0: "待支付", 1: "部分支付", 2: "已结清"}
                    payout_map = {0: "待打款", 1: "已打款"}
                    schedule_map = {0: "待排期", 1: "已排期"}

                    for row in cur.fetchall():
                        item = dict(zip(columns, row))
                        for key in ['created_at', 'updated_at', 'schedule_date', 'report_date', 'weigh_date']:
                            if item.get(key):
                                item[key] = str(item[key])
                        # 添加状态名称
                        item['payment_status_name'] = status_map.get(item.get('payment_status'), "未知")
                        item['payout_status_name'] = payout_map.get(item.get('payout_status'), "未知")
                        item['schedule_status_name'] = schedule_map.get(item.get('schedule_status'), "未知")

                        if not item.get('purchase_unit_price'):
                            item['purchase_unit_price'] = item.get('weighbill_unit_price')

                        data.append(item)

                    return {
                        "success": True,
                        "data": data,
                        "total": total,
                        "page": page,
                        "page_size": page_size
                    }

        except Exception as e:
            logger.error(f"查询结余列表失败: {e}")
            return {"success": False, "error": str(e), "data": [], "total": 0}

    def get_payment_receipt(self, receipt_id: int) -> Optional[Dict]:
        """获取支付回单详情（增加新字段）"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT id, receipt_no, receipt_image, payment_date, payment_time,
                               payer_name, payer_account, payee_name, payee_account,
                               amount, fee, total_amount, bank_name, payee_bank_name, remark,
                               ocr_status, is_manual_corrected, ocr_raw_data,
                               created_at, updated_at
                        FROM pd_payment_receipts 
                        WHERE id = %s
                    """, (receipt_id,))

                    row = cur.fetchone()
                    if not row:
                        return None

                    # 映射字段
                    columns = ['id', 'receipt_no', 'receipt_image', 'payment_date', 'payment_time',
                               'payer_name', 'payer_account', 'payee_name', 'payee_account',
                               'amount', 'fee', 'total_amount', 'bank_name', 'payee_bank_name', 'remark',
                               'ocr_status', 'is_manual_corrected', 'ocr_raw_data',
                               'created_at', 'updated_at']

                    data = dict(zip(columns, row))

                    # 转换时间
                    for key in ['payment_date', 'payment_time', 'created_at', 'updated_at']:
                        if data.get(key):
                            data[key] = str(data[key])

                    # 转换金额
                    for key in ['amount', 'fee']:
                        if data.get(key) is not None:
                            data[key] = float(data[key])

                    # 转换状态
                    status_map = {0: "待确认", 1: "已确认", 2: "已核销"}
                    data['ocr_status_label'] = status_map.get(data.get('ocr_status'), "未知")

                    # 查询核销的结余明细
                    cur.execute("""
                        SELECT b.id, b.driver_name, b.vehicle_no, b.payable_amount,
                               s.settled_amount
                        FROM pd_receipt_settlements s
                        JOIN pd_balance_details b ON s.balance_id = b.id
                        WHERE s.receipt_id = %s
                    """, (receipt_id,))

                    settlements = []
                    for r in cur.fetchall():
                        settlements.append({
                            'balance_id': r[0],
                            'driver_name': r[1],
                            'vehicle_no': r[2],
                            'payable_amount': float(r[3]) if r[3] else None,
                            'settled_amount': float(r[4]) if r[4] else None
                        })

                    data['settlements'] = settlements
                    return data

        except Exception as e:
            logger.error(f"查询支付回单失败: {e}")
            return None

    def list_payment_receipts(
        self,
        exact_payee_name: str = None,
        exact_ocr_status: int = None,
        date_from: str = None,
        date_to: str = None,
        fuzzy_keywords: str = None,
        page: int = 1,
        page_size: int = 20
    ) -> Dict[str, Any]:
        """查询支付回单列表（增加新字段）"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    conditions = ["1=1"]
                    params = []

                    if exact_payee_name:
                        conditions.append("payee_name = %s")
                        params.append(exact_payee_name)

                    if exact_ocr_status is not None:
                        conditions.append("ocr_status = %s")
                        params.append(exact_ocr_status)

                    if date_from:
                        conditions.append("payment_date >= %s")
                        params.append(date_from)

                    if date_to:
                        conditions.append("payment_date <= %s")
                        params.append(date_to)

                    if fuzzy_keywords:
                        tokens = [t for t in fuzzy_keywords.split() if t]
                        or_clauses = []
                        for token in tokens:
                            like = f"%{token}%"
                            or_clauses.append(
                                "(receipt_no LIKE %s OR payee_name LIKE %s OR payer_name LIKE %s "
                                "OR bank_name LIKE %s OR payee_bank_name LIKE %s OR remark LIKE %s)"
                            )
                            params.extend([like, like, like, like, like, like])
                        if or_clauses:
                            conditions.append("(" + " OR ".join(or_clauses) + ")")

                    where_sql = " AND ".join(conditions)

                    # 查询总数
                    cur.execute(f"SELECT COUNT(*) FROM pd_payment_receipts WHERE {where_sql}", tuple(params))
                    total = cur.fetchone()[0]

                    # 分页数据（增加新字段）
                    offset = (page - 1) * page_size
                    cur.execute(f"""
                        SELECT id, receipt_no, receipt_image, payment_date, payment_time,
                               payer_name, payer_account, payee_name, payee_account,
                               amount, fee, total_amount, bank_name, payee_bank_name, remark,
                               ocr_status, is_manual_corrected, created_at, updated_at
                        FROM pd_payment_receipts 
                        WHERE {where_sql}
                        ORDER BY created_at DESC
                        LIMIT %s OFFSET %s
                    """, tuple(params + [page_size, offset]))

                    columns = ['id', 'receipt_no', 'receipt_image', 'payment_date', 'payment_time',
                               'payer_name', 'payer_account', 'payee_name', 'payee_account',
                               'amount', 'fee', 'total_amount', 'bank_name', 'payee_bank_name', 'remark',
                               'ocr_status', 'is_manual_corrected', 'created_at', 'updated_at']

                    data = []
                    status_map = {0: "待确认", 1: "已确认", 2: "已核销"}

                    for row in cur.fetchall():
                        item = dict(zip(columns, row))

                        # 转换时间字段
                        for key in ['payment_date', 'payment_time', 'created_at', 'updated_at']:
                            if item.get(key):
                                item[key] = str(item[key])

                        # 转换金额
                        for key in ['amount', 'fee']:
                            if item.get(key) is not None:
                                item[key] = float(item[key])

                        # 添加状态名称
                        item['ocr_status_name'] = status_map.get(item.get('ocr_status'), "未知")
                        data.append(item)

                    return {
                        "success": True,
                        "data": data,
                        "total": total,
                        "page": page,
                        "page_size": page_size
                    }

        except Exception as e:
            logger.error(f"查询支付回单列表失败: {e}")
            return {"success": False, "error": str(e), "data": [], "total": 0}

    # ========== 按收款人汇总统计 ==========

    def list_balance_summary_by_payee(
            self,
            payee_name: str = None,
            driver_phone: str = None,
            fuzzy_keywords: str = None,
            min_balance: float = 0.01,
            payment_status: int = None,
            page: int = 1,
            page_size: int = 20
    ) -> Dict[str, Any]:
        """
        按收款人汇总统计结余

        返回每个收款人的：
        - 司机姓名、电话
        - 涉及磅单数
        - 总应付、总已付、总结余
        - 关联的合同列表
        """
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # 构建WHERE条件（在分组前过滤）
                    where_clauses = ["1=1"]
                    params = []

                    # 精确收款人姓名
                    if payee_name:
                        where_clauses.append("driver_name = %s")
                        params.append(payee_name)

                    # 精确司机电话
                    if driver_phone:
                        where_clauses.append("driver_phone = %s")
                        params.append(driver_phone)

                    # 支付状态筛选
                    if payment_status is not None:
                        where_clauses.append("payment_status = %s")
                        params.append(payment_status)
                    else:
                        # 默认只显示待支付和部分支付的（有结余的）
                        where_clauses.append("payment_status IN (0, 1)")

                    # 最小结余金额
                    if min_balance is not None:
                        where_clauses.append("balance_amount >= %s")
                        params.append(min_balance)

                    # 模糊搜索（收款人姓名、电话、车牌号）
                    if fuzzy_keywords:
                        tokens = [t for t in fuzzy_keywords.split() if t]
                        or_clauses = []
                        for token in tokens:
                            like = f"%{token}%"
                            or_clauses.append(
                                "(driver_name LIKE %s OR driver_phone LIKE %s OR vehicle_no LIKE %s OR contract_no LIKE %s)"
                            )
                            params.extend([like, like, like, like])
                        if or_clauses:
                            where_clauses.append("(" + " OR ".join(or_clauses) + ")")

                    where_sql = " AND ".join(where_clauses)

                    # 查询总数（分组后的记录数）
                    count_sql = f"""
                        SELECT COUNT(*) FROM (
                            SELECT driver_name, driver_phone
                            FROM pd_balance_details
                            WHERE {where_sql}
                            GROUP BY driver_name, driver_phone
                        ) t
                    """
                    cur.execute(count_sql, tuple(params))
                    total = cur.fetchone()[0]

                    # 分页查询汇总数据
                    offset = (page - 1) * page_size
                    query_sql = f"""
                        SELECT 
                            driver_name as payee_name,
                            driver_phone,
                            COUNT(*) as bill_count,
                            SUM(payable_amount) as total_payable,
                            SUM(paid_amount) as total_paid,
                            SUM(balance_amount) as total_balance,
                            GROUP_CONCAT(DISTINCT contract_no ORDER BY contract_no SEPARATOR ', ') as related_contracts,
                            GROUP_CONCAT(DISTINCT vehicle_no ORDER BY vehicle_no SEPARATOR ', ') as related_vehicles,
                            MIN(created_at) as first_bill_date,
                            MAX(created_at) as last_bill_date,
                            SUM(CASE WHEN payment_status = 0 THEN 1 ELSE 0 END) as pending_count,
                            SUM(CASE WHEN payment_status = 1 THEN 1 ELSE 0 END) as partial_count
                        FROM pd_balance_details
                        WHERE {where_sql}
                        GROUP BY driver_name, driver_phone
                        ORDER BY total_balance DESC, last_bill_date DESC
                        LIMIT %s OFFSET %s
                    """

                    cur.execute(query_sql, tuple(params + [page_size, offset]))

                    columns = [desc[0] for desc in cur.description]
                    data = []

                    for row in cur.fetchall():
                        item = dict(zip(columns, row))

                        # 转换金额为float
                        for key in ['total_payable', 'total_paid', 'total_balance']:
                            if item.get(key) is not None:
                                item[key] = float(item[key])

                        # 转换时间
                        for key in ['first_bill_date', 'last_bill_date']:
                            if item.get(key):
                                item[key] = str(item[key])

                        # 添加状态标签
                        pending = item.get('pending_count', 0)
                        partial = item.get('partial_count', 0)
                        if pending > 0 and partial > 0:
                            item['status_summary'] = f"{pending}笔待支付,{partial}笔部分支付"
                        elif pending > 0:
                            item['status_summary'] = f"{pending}笔待支付"
                        elif partial > 0:
                            item['status_summary'] = f"{partial}笔部分支付"
                        else:
                            item['status_summary'] = "全部结清"

                        data.append(item)

                    return {
                        "success": True,
                        "data": data,
                        "total": total,
                        "page": page,
                        "page_size": page_size,
                        "summary": {
                            "total_payees": total,
                            "total_balance": sum(d.get('total_balance', 0) for d in data)
                        }
                    }

        except Exception as e:
            logger.error(f"按收款人汇总查询失败: {e}")
            return {"success": False, "error": str(e), "data": [], "total": 0}

    def list_balance_details_grouped(self,
                                     exact_contract_no: str = None,
                                     exact_driver_name: str = None,
                                     fuzzy_keywords: str = None,
                                     payment_status: int = None,
                                     payout_status: int = None,
                                     schedule_status: int = None,
                                     date_from: str = None,
                                     date_to: str = None,
                                     page: int = 1,
                                     page_size: int = 20) -> Dict[str, Any]:
        """
        查询结余明细列表（按报单分组，包含完整关联信息）
        支持已排期和未排期的所有信息
        """
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # 构建WHERE条件
                    conditions = ["1=1"]
                    params = []

                    if exact_contract_no:
                        conditions.append("b.contract_no = %s")
                        params.append(exact_contract_no)
                    if exact_driver_name:
                        conditions.append("b.driver_name = %s")
                        params.append(exact_driver_name)
                    if payment_status is not None:
                        conditions.append("b.payment_status = %s")
                        params.append(payment_status)
                    if payout_status is not None:
                        conditions.append("b.payout_status = %s")
                        params.append(payout_status)
                    if schedule_status is not None:
                        conditions.append("b.schedule_status = %s")
                        params.append(schedule_status)
                    if date_from:
                        conditions.append("b.schedule_date >= %s")
                        params.append(date_from)
                    if date_to:
                        conditions.append("b.schedule_date <= %s")
                        params.append(date_to)
                    if fuzzy_keywords:
                        tokens = [t for t in fuzzy_keywords.split() if t]
                        or_clauses = []
                        for token in tokens:
                            like = f"%{token}%"
                            or_clauses.append(
                                "(b.contract_no LIKE %s OR b.driver_name LIKE %s OR b.driver_phone LIKE %s OR b.vehicle_no LIKE %s OR b.payee_name LIKE %s)"
                            )
                            params.extend([like, like, like, like, like])
                        if or_clauses:
                            conditions.append("(" + " OR ".join(or_clauses) + ")")

                    where_sql = " AND ".join(conditions)

                    # 查询报单分组总数
                    cur.execute(f"""
                        SELECT COUNT(DISTINCT b.delivery_id) 
                        FROM pd_balance_details b
                        WHERE {where_sql}
                    """, tuple(params))
                    total = cur.fetchone()[0]

                    # 分页查询报单ID列表
                    offset = (page - 1) * page_size
                    cur.execute(f"""
                        SELECT DISTINCT b.delivery_id, MAX(b.created_at) as max_created_at
                        FROM pd_balance_details b
                        WHERE {where_sql}
                        GROUP BY b.delivery_id
                        ORDER BY max_created_at DESC
                        LIMIT %s OFFSET %s
                    """, tuple(params + [page_size, offset]))
                    delivery_rows = cur.fetchall()
                    delivery_ids = [row[0] for row in delivery_rows] if delivery_rows else []

                    if not delivery_ids:
                        return {
                            "success": True,
                            "data": [],
                            "total": 0,
                            "page": page,
                            "page_size": page_size
                        }

                    # 查询报单详细信息
                    format_ids = ','.join(['%s'] * len(delivery_ids))
                    cur.execute(f"""
                        SELECT d.*,
                               (SELECT COUNT(*) FROM pd_balance_details WHERE delivery_id = d.id) as total_items,
                               (SELECT COUNT(*) FROM pd_balance_details WHERE delivery_id = d.id AND payment_status = 0) as pending_items,
                               (SELECT COUNT(*) FROM pd_balance_details WHERE delivery_id = d.id AND payment_status = 1) as partial_items,
                               (SELECT COUNT(*) FROM pd_balance_details WHERE delivery_id = d.id AND payment_status = 2) as settled_items,
                               (SELECT COALESCE(SUM(payable_amount), 0) FROM pd_balance_details WHERE delivery_id = d.id) as total_payable,
                               (SELECT COALESCE(SUM(paid_amount), 0) FROM pd_balance_details WHERE delivery_id = d.id) as total_paid,
                               (SELECT COALESCE(SUM(balance_amount), 0) FROM pd_balance_details WHERE delivery_id = d.id) as total_balance
                        FROM pd_deliveries d
                        WHERE d.id IN ({format_ids})
                        ORDER BY d.created_at DESC
                    """, tuple(delivery_ids))

                    delivery_columns = [desc[0] for desc in cur.description]
                    delivery_rows = cur.fetchall()

                    # 查询这些报单的所有结余明细（含关联信息）
                    cur.execute(f"""
                        SELECT 
                            b.*,
                            w.id as wb_id, w.weigh_date, w.delivery_time, w.weigh_ticket_no,
                            w.contract_no as wb_contract_no, w.vehicle_no as wb_vehicle_no,
                            w.product_name as wb_product_name, w.gross_weight, w.tare_weight,
                            w.net_weight, w.unit_price as wb_unit_price, w.total_amount as wb_total_amount,
                            w.weighbill_image, w.upload_status as wb_upload_status,
                            w.ocr_status, w.is_manual_corrected, w.payment_schedule_date as wb_payment_schedule_date,
                            w.uploader_id as wb_uploader_id, w.uploader_name as wb_uploader_name,
                            w.uploaded_at as wb_uploaded_at, w.created_at as wb_created_at, w.updated_at as wb_updated_at
                        FROM pd_balance_details b
                        LEFT JOIN pd_weighbills w ON b.weighbill_id = w.id
                        WHERE b.delivery_id IN ({format_ids})
                        ORDER BY b.delivery_id, b.created_at
                    """, tuple(delivery_ids))

                    balance_columns = [desc[0] for desc in cur.description]
                    balance_rows = cur.fetchall()

                    # 查询结余明细关联的回单信息
                    balance_ids = [row[0] for row in balance_rows]
                    receipts_map = {}
                    if balance_ids:
                        format_balance_ids = ','.join(['%s'] * len(balance_ids))
                        cur.execute(f"""
                            SELECT 
                                rs.balance_id,
                                pr.id as receipt_id, pr.receipt_no, pr.payment_date, pr.payment_time,
                                pr.payer_name, pr.payer_account, pr.payee_name, pr.payee_account,
                                pr.amount, pr.fee, pr.total_amount, pr.bank_name, pr.payee_bank_name,
                                pr.remark, pr.ocr_status, pr.is_manual_corrected, pr.receipt_image,
                                pr.created_at as receipt_created_at,
                                rs.settled_amount
                            FROM pd_receipt_settlements rs
                            JOIN pd_payment_receipts pr ON rs.receipt_id = pr.id
                            WHERE rs.balance_id IN ({format_balance_ids})
                            ORDER BY rs.balance_id, pr.payment_date DESC
                        """, tuple(balance_ids))

                        for row in cur.fetchall():
                            balance_id = row[0]
                            if balance_id not in receipts_map:
                                receipts_map[balance_id] = []
                            receipts_map[balance_id].append({
                                'receipt_id': row[1],
                                'receipt_no': row[2],
                                'payment_date': str(row[3]) if row[3] else None,
                                'payment_time': str(row[4]) if row[4] else None,
                                'payer_name': row[5],
                                'payer_account': row[6],
                                'payee_name': row[7],
                                'payee_account': row[8],
                                'amount': float(row[9]) if row[9] else 0,
                                'fee': float(row[10]) if row[10] else 0,
                                'total_amount': float(row[11]) if row[11] else 0,
                                'bank_name': row[12],
                                'payee_bank_name': row[13],
                                'remark': row[14],
                                'ocr_status': row[15],
                                'ocr_status_label': {0: "待确认", 1: "已确认", 2: "已核销"}.get(row[15], "未知"),
                                'is_manual_corrected': row[16],
                                'receipt_image': row[17],
                                'created_at': str(row[18]) if row[18] else None,
                                'settled_amount': float(row[19]) if row[19] else 0
                            })

                    # 组装结余明细数据
                    balance_map = {}
                    status_map = {0: "待支付", 1: "部分支付", 2: "已结清"}
                    payout_map = {0: "待打款", 1: "已打款"}
                    schedule_map = {0: "待排期", 1: "已排期"}
                    # 回款状态映射
                    collection_map = {
                        0: "待回款",  # 已上传磅单，默认待回款
                        1: "已回首笔待回尾款",  # 已回款首笔
                        2: "已回尾款"  # 回款完成
                    }

                    for row in balance_rows:
                        item = dict(zip(balance_columns, row))

                        # 转换时间字段
                        for key in ['created_at', 'updated_at', 'schedule_date']:
                            if item.get(key):
                                item[key] = str(item[key])

                        # 转换金额字段
                        for key in ['payable_amount', 'paid_amount', 'balance_amount', 'purchase_unit_price']:
                            if item.get(key) is not None:
                                item[key] = float(item[key])

                        # 状态名称
                        item['payment_status_name'] = status_map.get(item.get('payment_status'), "未知")
                        item['payout_status_name'] = payout_map.get(item.get('payout_status'), "未知")
                        item['schedule_status_name'] = schedule_map.get(item.get('schedule_status'), "未知")

                        # 计算回款状态（基于支付状态简化处理，实际应根据收款明细表判断）
                        paid = item.get('paid_amount', 0) or 0
                        payable = item.get('payable_amount', 0) or 0
                        if paid >= payable:
                            collection_status = 2  # 已回尾款
                        elif paid > 0:
                            collection_status = 1  # 已回首笔待回尾款
                        else:
                            collection_status = 0  # 待回款

                        item['collection_status'] = collection_status
                        item['collection_status_name'] = collection_map.get(collection_status, "未知")

                        # 构建磅单对象
                        weighbill = None
                        if item.get('wb_id'):
                            weighbill = {
                                'id': item['wb_id'],
                                'weigh_date': str(item['weigh_date']) if item.get('weigh_date') else None,
                                'delivery_time': str(item['delivery_time']) if item.get('delivery_time') else None,
                                'weigh_ticket_no': item['weigh_ticket_no'],
                                'contract_no': item['wb_contract_no'],
                                'vehicle_no': item['wb_vehicle_no'],
                                'product_name': item['wb_product_name'],
                                'gross_weight': float(item['gross_weight']) if item.get('gross_weight') else None,
                                'tare_weight': float(item['tare_weight']) if item.get('tare_weight') else None,
                                'net_weight': float(item['net_weight']) if item.get('net_weight') else None,
                                'unit_price': float(item['wb_unit_price']) if item.get('wb_unit_price') else None,
                                'total_amount': float(item['wb_total_amount']) if item.get('wb_total_amount') else None,
                                'weighbill_image': item['weighbill_image'],
                                'upload_status': item['wb_upload_status'],
                                'ocr_status': item['ocr_status'],
                                'is_manual_corrected': item['is_manual_corrected'],
                                'payment_schedule_date': str(item['wb_payment_schedule_date']) if item.get(
                                    'wb_payment_schedule_date') else None,
                                'uploader_id': item['wb_uploader_id'],
                                'uploader_name': item['wb_uploader_name'],
                                'uploaded_at': str(item['wb_uploaded_at']) if item.get('wb_uploaded_at') else None,
                                'created_at': str(item['wb_created_at']) if item.get('wb_created_at') else None,
                                'updated_at': str(item['wb_updated_at']) if item.get('wb_updated_at') else None
                            }

                        # 获取关联回单
                        balance_id = item['id']
                        payment_receipts = receipts_map.get(balance_id, [])

                        # 计算联单费（从报单获取）
                        service_fee = 0

                        # 操作权限
                        balance_amount = item.get('balance_amount', 0)
                        schedule_status_val = item.get('schedule_status', 0)
                        receipt_count = len(payment_receipts)

                        operations = {
                            'can_edit': True,  # 编辑打款信息权限
                            'can_verify': balance_amount > 0,
                            'can_schedule': schedule_status_val == 0,
                            'can_modify_schedule': schedule_status_val == 1,
                            'can_view_receipts': receipt_count > 0,
                            'can_view_weighbill': item.get('wb_id') is not None
                        }

                        # 构建最终结余对象（包含所有表头字段）
                        balance_item = {
                            # === 排款日期 ===
                            'schedule_date': item['schedule_date'],
                            'schedule_status': item['schedule_status'],
                            'schedule_status_name': item['schedule_status_name'],

                            # === 合同编号 ===
                            'contract_no': item['contract_no'],

                            # === 报单日期 ===
                            'report_date': None,  # 从报单填充

                            # === 报送冶炼厂 ===
                            'target_factory_name': None,  # 从报单填充

                            # === 司机电话 ===
                            'driver_phone': item['driver_phone'],

                            # === 司机姓名 ===
                            'driver_name': item['driver_name'],

                            # === 车号 ===
                            'vehicle_no': item['vehicle_no'],

                            # === 身份证号 ===
                            'driver_id_card': None,  # 从报单填充

                            # === 品种 ===
                            'product_name': None,  # 从磅单填充

                            # === 是否自带联单 ===
                            'has_delivery_order': None,  # 从报单填充
                            'has_delivery_order_display': None,

                            # === 是否上传联单 ===
                            'upload_status': None,
                            'upload_status_display': None,

                            # === 报单人/发货人 ===
                            'shipper': None,
                            'reporter_name': None,

                            # === 磅单日期 ===
                            'weigh_date': weighbill['weigh_date'] if weighbill else None,

                            # === 过磅单号 ===
                            'weigh_ticket_no': weighbill['weigh_ticket_no'] if weighbill else None,

                            # === 净重 ===
                            'net_weight': weighbill['net_weight'] if weighbill else None,

                            # === 采购单价 ===
                            'purchase_unit_price': item.get('purchase_unit_price'),

                            # === 联单费 ===
                            'service_fee': service_fee,

                            # === 应打款金额 ===
                            'payable_amount': item['payable_amount'],

                            # === 已打款金额 ===
                            'paid_amount': item['paid_amount'],

                            # === 收款人 ===
                            'payee_name': item['payee_name'],

                            # === 收款人账号 ===
                            'payee_account': item['payee_account'],

                            # === 打款状态 ===
                            'payout_status': item['payout_status'],
                            'payout_status_name': item['payout_status_name'],

                            # === 回款状态 ===
                            'collection_status': item['collection_status'],
                            'collection_status_name': item['collection_status_name'],

                            # === 结余基础信息 ===
                            'id': item['id'],
                            'delivery_id': item['delivery_id'],
                            'weighbill_id': item['weighbill_id'],
                            'payee_id': item['payee_id'],
                            'payment_status': item['payment_status'],
                            'payment_status_name': item['payment_status_name'],
                            'balance_amount': item['balance_amount'],
                            'created_at': item['created_at'],
                            'updated_at': item['updated_at'],

                            # === 关联对象 ===
                            'weighbill': weighbill,
                            'payment_receipts': payment_receipts,
                            'receipt_count': receipt_count,
                            'operations': operations
                        }

                        delivery_id = item['delivery_id']
                        if delivery_id not in balance_map:
                            balance_map[delivery_id] = []
                        balance_map[delivery_id].append(balance_item)

                    # 组装最终结果
                    result_data = []
                    for row in delivery_rows:
                        delivery = dict(zip(delivery_columns, row))

                        # 转换时间字段
                        for key in ['report_date', 'created_at', 'updated_at', 'uploaded_at']:
                            if delivery.get(key):
                                delivery[key] = str(delivery[key])

                        # 转换金额字段
                        for key in ['quantity', 'service_fee', 'contract_unit_price', 'total_amount',
                                    'total_payable', 'total_paid', 'total_balance']:
                            if delivery.get(key) is not None:
                                delivery[key] = float(delivery[key])

                        # 转换整数字段
                        for key in ['total_items', 'pending_items', 'partial_items', 'settled_items']:
                            if delivery.get(key) is not None:
                                delivery[key] = int(delivery[key])

                        # 解析品种列表
                        if delivery.get('products'):
                            delivery['products'] = [p.strip() for p in delivery['products'].split(',') if p.strip()]
                        else:
                            delivery['products'] = [delivery.get('product_name')] if delivery.get(
                                'product_name') else []

                        # 显示字段转换
                        has_order = delivery.get('has_delivery_order')
                        upload_status = delivery.get('upload_status')

                        delivery_id = delivery['id']
                        balance_items = balance_map.get(delivery_id, [])

                        # 填充报单信息到每条结余明细
                        for item in balance_items:
                            item['report_date'] = delivery.get('report_date')
                            item['target_factory_name'] = delivery.get('target_factory_name')
                            item['driver_id_card'] = delivery.get('driver_id_card')
                            item['shipper'] = delivery.get('shipper')
                            item['reporter_name'] = delivery.get('reporter_name')
                            item['service_fee'] = delivery.get('service_fee', 0)
                            item['has_delivery_order'] = has_order
                            item['has_delivery_order_display'] = '是' if has_order == '有' else '否'
                            item['upload_status'] = upload_status
                            item['upload_status_display'] = '是' if upload_status == '已上传' else '否'

                            # 品种从磅单获取
                            if item['weighbill']:
                                item['product_name'] = item['weighbill'].get('product_name')

                        result_data.append({
                            # 报单基础信息
                            'delivery_id': delivery_id,
                            'contract_no': delivery.get('contract_no'),
                            'report_date': delivery.get('report_date'),
                            'target_factory_name': delivery.get('target_factory_name'),
                            'driver_name': delivery.get('driver_name'),
                            'driver_phone': delivery.get('driver_phone'),
                            'driver_id_card': delivery.get('driver_id_card'),
                            'vehicle_no': delivery.get('vehicle_no'),
                            'shipper': delivery.get('shipper'),
                            'reporter_name': delivery.get('reporter_name'),
                            'warehouse': delivery.get('warehouse'),
                            'has_delivery_order': has_order,
                            'has_delivery_order_display': '是' if has_order == '有' else '否',
                            'service_fee': delivery.get('service_fee', 0),

                            # 结余统计
                            'balance_summary': {
                                'total_items': delivery.get('total_items', 0),
                                'pending_items': delivery.get('pending_items', 0),
                                'partial_items': delivery.get('partial_items', 0),
                                'settled_items': delivery.get('settled_items', 0),
                                'total_payable': delivery.get('total_payable', 0),
                                'total_paid': delivery.get('total_paid', 0),
                                'total_balance': delivery.get('total_balance', 0)
                            },

                            # 结余明细列表（已填充完整表头字段）
                            'balance_items': balance_items
                        })

                    return {
                        "success": True,
                        "data": result_data,
                        "total": total,
                        "page": page,
                        "page_size": page_size
                    }

        except Exception as e:
            logger.error(f"查询分组结余列表失败: {e}")
            return {"success": False, "error": str(e), "data": [], "total": 0}
    def get_payee_balance_details(
            self,
            payee_name: str,
            driver_phone: str = None,
            payment_status: int = None,
            page: int = 1,
            page_size: int = 20
    ) -> Dict[str, Any]:
        """
        获取指定收款人的具体结余明细列表
        """
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # 先查询收款人汇总信息
                    where_sql = "driver_name = %s"
                    params = [payee_name]

                    if driver_phone:
                        where_sql += " AND driver_phone = %s"
                        params.append(driver_phone)

                    if payment_status is not None:
                        where_sql += " AND payment_status = %s"
                        params.append(payment_status)

                    # 汇总信息
                    cur.execute(f"""
                        SELECT 
                            driver_name,
                            driver_phone,
                            COUNT(*) as total_bills,
                            SUM(payable_amount) as total_payable,
                            SUM(paid_amount) as total_paid,
                            SUM(balance_amount) as total_balance
                        FROM pd_balance_details
                        WHERE {where_sql}
                        GROUP BY driver_name, driver_phone
                    """, tuple(params))

                    summary_row = cur.fetchone()
                    if not summary_row:
                        return {"success": False, "error": "收款人不存在或无结余记录"}

                    summary_columns = [desc[0] for desc in cur.description]
                    summary = dict(zip(summary_columns, summary_row))

                    # 转换金额
                    for key in ['total_payable', 'total_paid', 'total_balance']:
                        if summary.get(key) is not None:
                            summary[key] = float(summary[key])

                    # 查询明细列表
                    count_sql = f"SELECT COUNT(*) FROM pd_balance_details WHERE {where_sql}"
                    cur.execute(count_sql, tuple(params))
                    total = cur.fetchone()[0]

                    offset = (page - 1) * page_size
                    detail_sql = f"""
                        SELECT 
                            b.*,
                            w.weighbill_image,
                            w.weigh_date,
                            w.vehicle_no as weigh_vehicle_no,
                            w.product_name as weigh_product_name,
                            w.net_weight as weigh_net_weight
                        FROM pd_balance_details b
                        LEFT JOIN pd_weighbills w ON b.weighbill_id = w.id
                        WHERE {where_sql}
                        ORDER BY b.created_at DESC
                        LIMIT %s OFFSET %s
                    """

                    cur.execute(detail_sql, tuple(params + [page_size, offset]))

                    columns = [desc[0] for desc in cur.description]
                    details = []
                    status_map = {0: "待支付", 1: "部分支付", 2: "已结清"}

                    for row in cur.fetchall():
                        item = dict(zip(columns, row))

                        # 转换时间
                        for key in ['created_at', 'updated_at', 'weigh_date']:
                            if item.get(key):
                                item[key] = str(item[key])

                        # 转换金额
                        for key in ['payable_amount', 'paid_amount', 'balance_amount']:
                            if item.get(key) is not None:
                                item[key] = float(item[key])

                        # 状态名称
                        item['payment_status_name'] = status_map.get(item.get('payment_status'), "未知")

                        details.append(item)

                    return {
                        "success": True,
                        "summary": summary,
                        "details": details,
                        "total": total,
                        "page": page,
                        "page_size": page_size
                    }

        except Exception as e:
            logger.error(f"查询收款人明细失败: {e}")
            return {"success": False, "error": str(e)}

    def batch_verify_by_payee(
            self,
            payee_name: str,
            receipt_id: int,
            driver_phone: str = None,
            max_amount: float = None
    ) -> Dict[str, Any]:
        """
        按收款人批量核销

        将一个支付回单的金额，自动分配到该收款人的多笔结余明细上
        """
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # 获取支付回单信息
                    cur.execute("""
                        SELECT amount, ocr_status 
                        FROM pd_payment_receipts 
                        WHERE id = %s
                    """, (receipt_id,))

                    row = cur.fetchone()
                    if not row:
                        return {"success": False, "error": "支付回单不存在"}

                    receipt_amount, ocr_status = Decimal(str(row[0])), row[1]

                    if ocr_status == self.OCR_STATUS_VERIFIED:
                        return {"success": False, "error": "该回单已核销"}

                    # 查询该收款人所有待支付的结余明细
                    where_sql = "driver_name = %s AND payment_status IN (0, 1)"
                    params = [payee_name]

                    if driver_phone:
                        where_sql += " AND driver_phone = %s"
                        params.append(driver_phone)

                    cur.execute(f"""
                        SELECT id, payable_amount, paid_amount, balance_amount
                        FROM pd_balance_details
                        WHERE {where_sql}
                        ORDER BY created_at ASC
                    """, tuple(params))

                    balance_items = cur.fetchall()
                    if not balance_items:
                        return {"success": False, "error": "该收款人没有待支付的结余明细"}

                    # 自动分配核销金额
                    remaining_amount = receipt_amount
                    settled_items = []

                    for balance_id, payable, paid, balance in balance_items:
                        if remaining_amount <= 0:
                            break

                        payable_d = Decimal(str(payable))
                        paid_d = Decimal(str(paid))
                        balance_d = Decimal(str(balance))

                        # 本次可核销金额
                        settle_amount = min(balance_d, remaining_amount)
                        new_paid = paid_d + settle_amount
                        new_balance = payable_d - new_paid

                        # 确定新状态
                        if new_paid >= payable_d:
                            new_status = self.PAY_STATUS_SETTLED
                        else:
                            new_status = self.PAY_STATUS_PARTIAL

                        # 更新结余明细
                        cur.execute("""
                            UPDATE pd_balance_details 
                            SET paid_amount = %s, balance_amount = %s, payment_status = %s 
                            WHERE id = %s
                        """, (float(new_paid), float(new_balance), new_status, balance_id))

                        # 插入关联表
                        cur.execute("""
                            INSERT INTO pd_receipt_settlements 
                            (receipt_id, balance_id, settled_amount)
                            VALUES (%s, %s, %s)
                            ON DUPLICATE KEY UPDATE settled_amount = %s
                        """, (receipt_id, balance_id, float(settle_amount), float(settle_amount)))

                        settled_items.append({
                            'balance_id': balance_id,
                            'settled_amount': float(settle_amount),
                            'status': new_status
                        })

                        remaining_amount -= settle_amount

                    # 更新回单状态
                    new_receipt_status = self.OCR_STATUS_VERIFIED if remaining_amount <= 0 else self.OCR_STATUS_CONFIRMED
                    cur.execute("""
                        UPDATE pd_payment_receipts 
                        SET ocr_status = %s 
                        WHERE id = %s
                    """, (new_receipt_status, receipt_id))

                    return {
                        "success": True,
                        "message": f"成功核销 {len(settled_items)} 条明细",
                        "data": {
                            'receipt_id': receipt_id,
                            'payee_name': payee_name,
                            'total_settled': float(receipt_amount - remaining_amount),
                            'remaining_unused': float(remaining_amount) if remaining_amount > 0 else 0,
                            'receipt_status': new_receipt_status,
                            'items': settled_items
                        }
                    }

        except Exception as e:
            logger.error(f"批量核销失败: {e}")
            return {"success": False, "error": str(e)}

_balance_service = None


def get_balance_service():
    global _balance_service
    if _balance_service is None:
        _balance_service = BalanceService()
    return _balance_service