import os

import pymysql
from dotenv import load_dotenv


def get_mysql_config() -> dict:
	load_dotenv()

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
		"charset": require_env("MYSQL_CHARSET") if os.getenv("MYSQL_CHARSET") else "utf8mb4",
		"autocommit": True,
	}


def get_mysql_config_without_db() -> dict:
	"""获取不指定数据库的配置（用于创建数据库）"""
	load_dotenv()

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
		"charset": require_env("MYSQL_CHARSET") if os.getenv("MYSQL_CHARSET") else "utf8mb4",
		"autocommit": True,
	}


def create_database_if_not_exists():
	"""自动创建数据库（如果不存在）"""
	config = get_mysql_config_without_db()
	database_name = os.getenv("MYSQL_DATABASE")

	connection = pymysql.connect(**config)
	try:
		with connection.cursor() as cursor:
			cursor.execute(
				f"CREATE DATABASE IF NOT EXISTS {database_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
			print(f"数据库 '{database_name}' 检查/创建完成")
	finally:
		connection.close()


TABLE_STATEMENTS = [
	# ========== 原有表 ==========
	"""
	CREATE TABLE IF NOT EXISTS pd_summary (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		contract_no VARCHAR(64) NOT NULL,
		report_date DATE,
		driver_phone VARCHAR(32),
		driver_name VARCHAR(64),
		vehicle_no VARCHAR(32),
		product_name VARCHAR(64),
		weigh_date DATE,
		weigh_ticket_no VARCHAR(64),
		net_weight DECIMAL(12, 3),
		unit_price DECIMAL(12, 2),
		amount DECIMAL(14, 2),
		planned_truck_count INT,
		shipper VARCHAR(64),
		payee VARCHAR(64),
		other_fees DECIMAL(14, 2),
		amount_payable DECIMAL(14, 2),
		payment_schedule_date DATE,
		remarks TEXT,
		remittance_unit_price DECIMAL(12, 2),
		remittance_amount DECIMAL(14, 2),
		received_payment_date DATE,
		arrival_payment_90 DECIMAL(14, 2),
		final_payment_date DATE,
		final_payment_10 DECIMAL(14, 2),
		payout_date DATE,
		payout_details TEXT,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	""",
	"""
	CREATE TABLE IF NOT EXISTS pd_users (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(64) NOT NULL,
		account VARCHAR(64) NOT NULL UNIQUE,
		password_hash VARCHAR(255) NOT NULL,
		role VARCHAR(32) NOT NULL,
		phone VARCHAR(32),
		email VARCHAR(128),
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		CHECK (role IN (
			'管理员',
			'大区经理',
			'自营库管理',
			'财务',
			'会计'
		))
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	""",
	"""
	CREATE TABLE IF NOT EXISTS pd_customers (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		smelter_name VARCHAR(128) NOT NULL,
		address VARCHAR(255),
		contact_person VARCHAR(64),
		contact_phone VARCHAR(32),
		contact_address VARCHAR(255) COMMENT '联系人地址',
		credit_code VARCHAR(32) COMMENT '统一社会信用代码',
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		UNIQUE KEY uk_smelter_name (smelter_name),
		UNIQUE KEY uk_credit_code (credit_code)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	""",
	"""
	CREATE TABLE IF NOT EXISTS pd_deliveries (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		report_date DATE COMMENT '报货日期',
		warehouse VARCHAR(64) COMMENT '送货库房',
		target_factory_id BIGINT COMMENT '目标工厂ID（关联pd_customers）',
		target_factory_name VARCHAR(128) COMMENT '目标工厂名称',
		product_name VARCHAR(64) COMMENT '货物品种',
		products VARCHAR(255) COMMENT '品种列表，逗号分隔，最多4个',
		quantity DECIMAL(12, 3) COMMENT '数量（吨）',
		vehicle_no VARCHAR(32) COMMENT '车牌号',
		driver_name VARCHAR(64) COMMENT '司机姓名',
		driver_phone VARCHAR(32) COMMENT '司机电话',
		driver_id_card VARCHAR(18) COMMENT '司机身份证号',
		has_delivery_order ENUM('有', '无') DEFAULT '无' COMMENT '是否有联单',
		delivery_order_image VARCHAR(255) COMMENT '联单图片路径',
		upload_status ENUM('已上传', '待上传') DEFAULT '待上传' COMMENT '联单上传状态',
		source_type ENUM('司机', '公司') DEFAULT '公司' COMMENT '来源：司机/公司',
		shipper VARCHAR(64) COMMENT '发货人（默认操作人）',
		reporter_id BIGINT COMMENT '报单人ID（关联pd_users.id）',
		reporter_name VARCHAR(64) COMMENT '报单人姓名',
		payee VARCHAR(64) COMMENT '收款人',
		service_fee DECIMAL(14, 2) DEFAULT 0 COMMENT '服务费',
		contract_no VARCHAR(64) COMMENT '关联合同编号',
		contract_unit_price DECIMAL(12, 2) COMMENT '合同单价',
		total_amount DECIMAL(14, 2) COMMENT '总价（单价×数量）',
		status VARCHAR(32) DEFAULT '待确认' COMMENT '状态：待确认/已确认/已完成/已取消',
		uploader_id BIGINT COMMENT '上传人ID（关联pd_users.id）',
		uploader_name VARCHAR(64) COMMENT '上传人姓名（冗余存储）',
		uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '上传时间',
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		INDEX idx_report_date (report_date),
		INDEX idx_contract_no (contract_no),
		INDEX idx_target_factory (target_factory_id),
		INDEX idx_vehicle_no (vehicle_no),
		INDEX idx_status (status),
		INDEX idx_shipper (shipper),
		INDEX idx_has_delivery_order (has_delivery_order),
		INDEX idx_upload_status (upload_status),
		INDEX idx_driver_phone_created_at (driver_phone, created_at)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='销售台账/报货订单';
	""",
	"""
	CREATE TABLE IF NOT EXISTS pd_weighbills (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		weigh_date DATE COMMENT '磅单日期',
		delivery_time DATETIME COMMENT '送货时间',
		weigh_ticket_no VARCHAR(64) COMMENT '过磅单号',
		contract_no VARCHAR(64) COMMENT '合同编号（OCR识别）',
		delivery_id BIGINT COMMENT '关联的报货订单ID（通过日期+车牌匹配）',
		vehicle_no VARCHAR(32) COMMENT '车牌号',
		product_name VARCHAR(64) COMMENT '货物名称',
		gross_weight DECIMAL(12, 3) COMMENT '毛重（吨）',
		tare_weight DECIMAL(12, 3) COMMENT '皮重（吨）',
		net_weight DECIMAL(12, 3) COMMENT '净重（吨）',
		unit_price DECIMAL(12, 2) COMMENT '合同单价',
		total_amount DECIMAL(14, 2) COMMENT '总价（净重×单价）',
		weighbill_image VARCHAR(255) COMMENT '磅单图片路径',
		upload_status ENUM('已上传', '待上传') DEFAULT '待上传' COMMENT '磅单上传状态',
		ocr_status VARCHAR(32) DEFAULT '待确认' COMMENT 'OCR状态：待确认/已确认/已修正',
		ocr_raw_data TEXT COMMENT 'OCR原始识别文本',
		is_manual_corrected TINYINT DEFAULT 0 COMMENT '是否人工修正',
		payment_schedule_date DATE COMMENT '排款日期',
		uploader_id BIGINT COMMENT '上传人ID（关联pd_users.id）',
		uploader_name VARCHAR(64) COMMENT '上传人姓名（冗余存储）',
		uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '上传时间',
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		INDEX idx_weigh_date (weigh_date),
		INDEX idx_vehicle_no (vehicle_no),
		INDEX idx_contract_no (contract_no),
		INDEX idx_delivery_id (delivery_id),
		INDEX idx_status (ocr_status),
		INDEX idx_upload_status (upload_status),
		UNIQUE KEY uk_delivery_product (delivery_id, product_name)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='磅单表';
	""",
	"""
	CREATE TABLE IF NOT EXISTS pd_weighbill_settlements (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		payable_amount DECIMAL(14, 2),
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	""",
	"""
	CREATE TABLE IF NOT EXISTS pd_receipts (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		remittance_amount DECIMAL(14, 2),
		received_payment_date DATE,
		arrival_payment_90 DECIMAL(14, 2),
		final_payment_date DATE,
		final_payment_10 DECIMAL(14, 2),
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	""",
	"""
	CREATE TABLE IF NOT EXISTS pd_payout_details (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		payout_amount DECIMAL(14, 2),
		payout_details TEXT,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	""",

	# ========== 新增合同管理表 ==========
	"""
	CREATE TABLE IF NOT EXISTS pd_contracts (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		seq_no INT UNIQUE COMMENT '序号（自动生成，使用触发器或应用层生成）',
		contract_no VARCHAR(64) NOT NULL UNIQUE COMMENT '合同编号',
		contract_date DATE COMMENT '合同签订日期',
		end_date DATE COMMENT '合同截止日期',
		smelter_company VARCHAR(128) COMMENT '冶炼公司',
		total_quantity DECIMAL(12, 3) COMMENT '合同总数量（吨）',
		truck_count DECIMAL(12, 2) COMMENT '车数（总数量/35）',
		arrival_payment_ratio DECIMAL(5, 4) DEFAULT 0.9 COMMENT '到货款比例',
		final_payment_ratio DECIMAL(5, 4) DEFAULT 0.1 COMMENT '尾款比例',
		contract_image_path VARCHAR(255) COMMENT '合同图片路径',
		status VARCHAR(32) DEFAULT '生效中' COMMENT '状态：生效中/已到期/已终止',
		remarks TEXT COMMENT '备注',
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		INDEX idx_seq_no (seq_no)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	""",
	"""
	CREATE TABLE IF NOT EXISTS pd_contract_products (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		contract_id BIGINT NOT NULL COMMENT '合同ID',
		product_name VARCHAR(64) NOT NULL COMMENT '品种名称',
		unit_price DECIMAL(12, 2) COMMENT '单价（元）',
		sort_order INT DEFAULT 0 COMMENT '排序',
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY (contract_id) REFERENCES pd_contracts(id) ON DELETE CASCADE,
		INDEX idx_contract_id (contract_id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	""",
	# 磅单结余管理
	"""
	CREATE TABLE IF NOT EXISTS pd_payment_receipts (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		receipt_no VARCHAR(64) COMMENT '银行回单流水号',
		receipt_image VARCHAR(255) NOT NULL COMMENT '回单图片存储路径',
		payment_date DATE NOT NULL COMMENT '支付日期',
		payment_time TIME COMMENT '支付时间',
		payer_name VARCHAR(64) COMMENT '付款人姓名',
		payer_account VARCHAR(32) COMMENT '付款账号',
		payee_name VARCHAR(64) NOT NULL COMMENT '收款人姓名（司机）',
		payee_account VARCHAR(32) COMMENT '收款账号',
		amount DECIMAL(14, 2) NOT NULL COMMENT '转账金额（小写）',
		fee DECIMAL(14, 2) DEFAULT 0.00 COMMENT '手续费',
		total_amount DECIMAL(14, 2) NOT NULL COMMENT '合计金额（小写）= 转账金额 + 手续费',
		bank_name VARCHAR(64) COMMENT '付款银行名称',
		payee_bank_name VARCHAR(64) COMMENT '收款银行名称',
		remark VARCHAR(255) COMMENT '备注/用途',
		ocr_status TINYINT DEFAULT 0 COMMENT '0=待确认, 1=已确认, 2=已核销',
		is_manual_corrected TINYINT DEFAULT 0 COMMENT '0=自动, 1=人工修正',
		ocr_raw_data TEXT COMMENT 'OCR原始识别文本',
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		INDEX idx_payee_amount (payee_name, amount),
		INDEX idx_payment_date (payment_date),
		INDEX idx_ocr_status (ocr_status),
		INDEX idx_receipt_no (receipt_no)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='支付回单表';
	""",
	"""
	CREATE TABLE IF NOT EXISTS pd_balance_details (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		contract_no VARCHAR(64) COMMENT '合同编号',
		delivery_id BIGINT COMMENT '报货订单ID',
		weighbill_id BIGINT NOT NULL COMMENT '磅单ID',
		driver_name VARCHAR(64) COMMENT '司机姓名',
		driver_phone VARCHAR(32) COMMENT '司机电话',
		vehicle_no VARCHAR(32) COMMENT '车牌号',
		payee_id BIGINT COMMENT '收款人ID',
		payee_name VARCHAR(64) COMMENT '收款人姓名',
		payee_account VARCHAR(32) COMMENT '收款账号',
		purchase_unit_price DECIMAL(14, 2) DEFAULT 0 COMMENT '采购单价',
		payable_amount DECIMAL(14, 2) NOT NULL COMMENT '应付金额',
		paid_amount DECIMAL(14, 2) DEFAULT 0 COMMENT '已支付金额',
		balance_amount DECIMAL(14, 2) COMMENT '结余金额',
		payment_status TINYINT DEFAULT 0 COMMENT '0=待支付, 1=部分支付, 2=已结清',
		payout_status TINYINT DEFAULT 0 COMMENT '打款状态：0=待打款, 1=已打款',
		payout_date DATE COMMENT '打款日期',
		schedule_date DATE COMMENT '排款日期',
		schedule_status TINYINT DEFAULT 0 COMMENT '排期状态：0=待排期, 1=已排期',
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		UNIQUE KEY uk_weighbill (weighbill_id),
		INDEX idx_contract_no (contract_no),
		INDEX idx_driver_name (driver_name),
		INDEX idx_payee_id (payee_id),
		INDEX idx_payment_status (payment_status),
		INDEX idx_created_at (created_at),
		INDEX idx_payee_name (payee_name),
		INDEX idx_schedule_date (schedule_date),
		INDEX idx_schedule_status (schedule_status),
		INDEX idx_payout_status (payout_status)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='磅单结余明细表';
	""",
	"""
	CREATE TABLE IF NOT EXISTS pd_receipt_settlements (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		receipt_id BIGINT NOT NULL COMMENT '支付回单ID',
		balance_id BIGINT NOT NULL COMMENT '结余明细ID',
		settled_amount DECIMAL(14, 2) COMMENT '本次核销金额',
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		UNIQUE KEY uk_receipt_balance (receipt_id, balance_id),
		INDEX idx_receipt_id (receipt_id),
		INDEX idx_balance_id (balance_id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='支付回单与结余核销关联表';
	""",
	"""
	CREATE TABLE IF NOT EXISTS pd_payment_details (
		id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '收款明细ID',
		sales_order_id BIGINT NOT NULL COMMENT '销售订单ID',
		delivery_id BIGINT COMMENT '报货订单ID',
		weighbill_id BIGINT COMMENT '磅单ID',
		smelter_name VARCHAR(100) NOT NULL COMMENT '冶炼厂名称',
		contract_no VARCHAR(50) NOT NULL COMMENT '合同编号',
		material_name VARCHAR(100) DEFAULT '' COMMENT '物料名称',
		unit_price DECIMAL(15, 2) NOT NULL COMMENT '合同单价（元/吨）',
		net_weight DECIMAL(15, 4) NOT NULL COMMENT '净重（吨）',
		total_amount DECIMAL(15, 2) NOT NULL COMMENT '应回款总额',
		arrival_payment_amount DECIMAL(15, 2) DEFAULT 0.00 COMMENT '应回款首笔金额',
		final_payment_amount DECIMAL(15, 2) DEFAULT 0.00 COMMENT '应回款尾款金额',
		paid_amount DECIMAL(15, 2) DEFAULT 0.00 COMMENT '累计已付金额',
		arrival_paid_amount DECIMAL(15, 2) DEFAULT 0.00 COMMENT '已回款首笔金额',
		final_paid_amount DECIMAL(15, 2) DEFAULT 0.00 COMMENT '已回款尾款金额',
		unpaid_amount DECIMAL(15, 2) NOT NULL COMMENT '未付金额',
		status TINYINT DEFAULT 0 COMMENT '回款状态：0-未回款, 1-部分回款, 2-已结清, 3-超额回款',
		collection_status TINYINT DEFAULT 0 COMMENT '回款状态：0-待回款, 1-已回首笔待回尾款, 2-已回款',
		is_paid TINYINT DEFAULT 0 COMMENT '是否回款：0-否, 1-是',
		is_paid_out TINYINT DEFAULT 0 COMMENT '是否支付：0-待打款, 1-已打款',
		payment_schedule_date DATE COMMENT '排款日期',
		remark TEXT COMMENT '备注',
		created_by BIGINT COMMENT '创建人ID',
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

		INDEX idx_sales_order_id (sales_order_id),
		INDEX idx_delivery_id (delivery_id),
		INDEX idx_weighbill_id (weighbill_id),
		INDEX idx_smelter_name (smelter_name),
		INDEX idx_contract_no (contract_no),
		INDEX idx_status (status),
		INDEX idx_collection_status (collection_status),
		INDEX idx_created_at (created_at)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='收款明细台账表';
	""",
	"""
	CREATE TABLE IF NOT EXISTS pd_payment_records (
		id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '回款记录ID',
		payment_detail_id BIGINT NOT NULL COMMENT '关联的收款明细ID',
		payment_amount DECIMAL(15, 2) NOT NULL COMMENT '本次回款金额',
		payment_stage TINYINT DEFAULT 1 COMMENT '回款阶段：0-定金, 1-到货款(90%), 2-尾款(10%)',
		payment_date DATE NOT NULL COMMENT '回款日期',
		payment_method VARCHAR(50) DEFAULT '' COMMENT '支付方式（银行转账/现金/承兑等）',
		transaction_no VARCHAR(100) DEFAULT '' COMMENT '交易流水号',
		remark TEXT COMMENT '备注',
		recorded_by BIGINT COMMENT '录入人ID',
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '录入时间',

		INDEX idx_payment_detail_id (payment_detail_id),
		INDEX idx_payment_date (payment_date),
		INDEX idx_payment_stage (payment_stage),

		FOREIGN KEY (payment_detail_id) REFERENCES pd_payment_details(id) ON DELETE CASCADE
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='回款记录明细表';
"""
]


def create_tables() -> None:
	# 第1步：先创建数据库（如果不存在）
	create_database_if_not_exists()

	# 第2步：创建表
	config = get_mysql_config()
	connection = pymysql.connect(**config)
	try:
		with connection.cursor() as cursor:
			for statement in TABLE_STATEMENTS:
				cursor.execute(statement)
		print("所有数据表创建完成")
	finally:
		connection.close()


if __name__ == "__main__":
	create_tables()