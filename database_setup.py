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


def build_product_categories_table_statement() -> str:
	"""构建固定 50 个品类槽位的品类表。"""
	category_columns = "\n".join(
		f"\t\tcategory_{index} VARCHAR(64) DEFAULT NULL COMMENT '品类槽位{index}',"
		for index in range(1, 51)
	)
	return f"""
	CREATE TABLE IF NOT EXISTS pd_product_categories (
		id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
	{category_columns}
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='固定50槽位品类表';
	"""


TABLE_STATEMENTS = [
	# ========== 原有表 ==========
	"""
	CREATE TABLE IF NOT EXISTS pd_users (
		id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
		name VARCHAR(64) NOT NULL COMMENT '姓名',
		account VARCHAR(64) NOT NULL UNIQUE COMMENT '账号',
		password_hash VARCHAR(255) NOT NULL COMMENT '密码哈希',
		role VARCHAR(32) NOT NULL COMMENT '角色',
		phone VARCHAR(32) COMMENT '手机号',
		email VARCHAR(128) COMMENT '邮箱',
		status TINYINT DEFAULT 0 COMMENT '状态：0=正常, 1=冻结, 2=已注销',
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
		CHECK (role IN (
			'管理员',
			'大区经理',
			'自营库管理',
			'财务',
			'会计'
		))
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户表';
	""",
	"""
	CREATE TABLE IF NOT EXISTS pd_user_permissions (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		user_id BIGINT NOT NULL COMMENT '用户ID（关联pd_users.id）',
	
		-- 角色字段（单选）
		role VARCHAR(32) NOT NULL DEFAULT '会计' COMMENT '角色：管理员/大区经理/自营库管理/财务/会计',
	
		-- 权限字段（布尔值，1=有权限，0=无权限）
		perm_permission_manage TINYINT DEFAULT 0 COMMENT '权限管理权限',
		perm_jinli_payment TINYINT DEFAULT 0 COMMENT '金利回款管理权限',
		perm_yuguang_payment TINYINT DEFAULT 0 COMMENT '豫光回款管理权限',
		perm_schedule TINYINT DEFAULT 0 COMMENT '排期管理权限',
		perm_payout TINYINT DEFAULT 0 COMMENT '打款管理权限',
		perm_payout_stats TINYINT DEFAULT 0 COMMENT '打款统计权限',
		perm_report_stats TINYINT DEFAULT 0 COMMENT '统计与报表权限',
		perm_contract_progress TINYINT DEFAULT 0 COMMENT '合同发运进度权限',
		perm_contract_manage TINYINT DEFAULT 0 COMMENT '销售合同管理权限',
		perm_customer_manage TINYINT DEFAULT 0 COMMENT '客户管理权限',
		perm_delivery_manage TINYINT DEFAULT 0 COMMENT '报货管理权限',
		perm_weighbill_manage TINYINT DEFAULT 0 COMMENT '磅单管理权限',
		perm_warehouse_manage TINYINT DEFAULT 0 COMMENT '库房管理权限',
		perm_payee_manage TINYINT DEFAULT 0 COMMENT '收款人管理权限',
		perm_account_manage TINYINT DEFAULT 0 COMMENT '账号管理权限',
		perm_role_manage TINYINT DEFAULT 0 COMMENT '角色管理权限',
		perm_ai_detect TINYINT DEFAULT 0 COMMENT 'AI检测权限',
		perm_ai_predict TINYINT DEFAULT 0 COMMENT 'AI预测权限',
	
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	
		UNIQUE KEY uk_user_id (user_id),
		INDEX idx_role (role),
	
		FOREIGN KEY (user_id) REFERENCES pd_users(id) ON DELETE CASCADE
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户权限配置表';
	""",
	"""
	CREATE TABLE IF NOT EXISTS pd_customers (
		id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
		smelter_name VARCHAR(128) NOT NULL COMMENT '冶炼厂名称',
		address VARCHAR(255) COMMENT '公司地址',
		contact_person VARCHAR(64) COMMENT '联系人',
		contact_phone VARCHAR(32) COMMENT '联系人电话',
		contact_address VARCHAR(255) COMMENT '联系人地址',
		credit_code VARCHAR(32) COMMENT '统一社会信用代码',
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
		UNIQUE KEY uk_smelter_name (smelter_name),
		UNIQUE KEY uk_credit_code (credit_code)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='客户表';
	""",
	"""
	CREATE TABLE IF NOT EXISTS pd_deliveries (
		id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
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
		delivery_order_pdf VARCHAR(255) DEFAULT NULL COMMENT '联单PDF文件路径',
		voucher_images JSON DEFAULT NULL COMMENT '凭证图片路径数组，最多6张，仅在 has_delivery_order="无" 时使用',
		upload_status ENUM('已上传', '待上传') DEFAULT '待上传' COMMENT '联单上传状态',
		source_type ENUM('司机', '公司') DEFAULT '公司' COMMENT '来源：司机/公司',
		shipper VARCHAR(64) COMMENT '发货人（默认操作人）',
		reporter_id BIGINT COMMENT '报单人ID（关联pd_users.id）',
		reporter_name VARCHAR(64) COMMENT '报单人姓名',
		-- ===== 需求4：新增岗位和提交人姓名字段 =====
		position VARCHAR(50) COMMENT '岗位',
		-- ===== 需求4结束 =====
		payee VARCHAR(64) COMMENT '收款人',
		service_fee DECIMAL(14, 2) DEFAULT 0 COMMENT '服务费',
		contract_no VARCHAR(64) COMMENT '关联合同编号',
		contract_id BIGINT COMMENT '关联合同ID（外键，用于数据完整性）',
		contract_unit_price DECIMAL(12, 2) COMMENT '合同单价',
		total_amount DECIMAL(14, 2) COMMENT '总价（单价×数量）',
		status VARCHAR(32) DEFAULT '待确认' COMMENT '状态：待确认/已确认/已完成/已取消',
		uploader_id BIGINT COMMENT '上传人ID（关联pd_users.id）',
		uploader_name VARCHAR(64) COMMENT '上传人姓名（冗余存储）',
		planned_trucks INT DEFAULT 1 COMMENT '预计车数（quantity/35向上取整）',
		uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '上传时间',
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
		INDEX idx_report_date (report_date),
		INDEX idx_contract_no (contract_no),
		INDEX idx_contract_id (contract_id),
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
	CREATE TABLE IF NOT EXISTS pd_delivery_plans (
		id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
		plan_no VARCHAR(64) NOT NULL COMMENT '计划编号',
		plan_start_date DATE NOT NULL COMMENT '计划开始日期',
		planned_trucks INT NOT NULL DEFAULT 0 COMMENT '计划车数',
		planned_tonnage DECIMAL(12, 3) NOT NULL DEFAULT 0.000 COMMENT '计划吨数',
		plan_status VARCHAR(32) DEFAULT '草稿' COMMENT '计划状态',
		confirmed_trucks INT NOT NULL DEFAULT 0 COMMENT '已定车数',
		unconfirmed_trucks INT NOT NULL DEFAULT 0 COMMENT '未定车数',
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
		UNIQUE KEY uk_plan_no (plan_no),
		INDEX idx_plan_start_date (plan_start_date),
		INDEX idx_plan_status (plan_status)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='报货计划表';
	""",
	"""
	CREATE TABLE IF NOT EXISTS pd_weighbills (
		id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
		weigh_date DATE COMMENT '磅单日期',
		delivery_time DATETIME COMMENT '送货时间',
		warehouse_name VARCHAR(64) COMMENT '磅单仓库名称',
		weigh_ticket_no VARCHAR(64) COMMENT '过磅单号',
		contract_no VARCHAR(64) COMMENT '合同编号（OCR识别）',
		contract_id BIGINT COMMENT '关联合同ID（外键）',
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
		is_last_truck_for_contract TINYINT DEFAULT 0 COMMENT '是否为合同最后一车',
		uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '上传时间',
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
		INDEX idx_weigh_date (weigh_date),
		INDEX idx_vehicle_no (vehicle_no),
		INDEX idx_contract_no (contract_no),
		INDEX idx_contract_id (contract_id),  -- 新增索引
		INDEX idx_delivery_id (delivery_id),
		INDEX idx_status (ocr_status),
		INDEX idx_upload_status (upload_status),
		UNIQUE KEY uk_delivery_product (delivery_id, product_name)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='磅单表';
	""",
	"""
	CREATE TABLE IF NOT EXISTS pd_role_templates (
		id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
		role VARCHAR(32) NOT NULL UNIQUE COMMENT '角色名称',
		template_json TEXT NOT NULL COMMENT '权限模板JSON',
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='角色权限模板表';
	""",
	"""
	CREATE TABLE IF NOT EXISTS pd_warehouses (
		id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
		warehouse_name VARCHAR(64) NOT NULL UNIQUE COMMENT '库房名称',
		public_account VARCHAR(32) COMMENT '对公账号',
		is_active TINYINT DEFAULT 1 COMMENT '是否启用：1=启用，0=停用',
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
		INDEX idx_warehouse_name (warehouse_name),
		INDEX idx_is_active (is_active)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='库房表';
	""",
	"""
	CREATE TABLE IF NOT EXISTS pd_payees (
		id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
		warehouse_id BIGINT DEFAULT NULL COMMENT '所属库房ID，可选',
		warehouse_name VARCHAR(100) DEFAULT NULL COMMENT '库房名称，可选',
		payee_name VARCHAR(64) NOT NULL COMMENT '收款人姓名',
		payee_account VARCHAR(32) DEFAULT NULL COMMENT '收款账号',
		payee_bank_name VARCHAR(64) DEFAULT NULL COMMENT '收款银行名称',
		is_active TINYINT DEFAULT 1 COMMENT '是否启用：1=启用，0=停用',
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
		INDEX idx_warehouse_id (warehouse_id),
		INDEX idx_payee_name (payee_name),
		INDEX idx_is_active (is_active)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='收款人表';
	""",
	# ========== 新增合同管理表 ==========
	"""
	CREATE TABLE IF NOT EXISTS pd_contracts (
		id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
		seq_no INT UNIQUE COMMENT '序号（自动生成，使用触发器或应用层生成）',
		contract_no VARCHAR(64) NOT NULL UNIQUE COMMENT '合同编号',
		contract_date DATE COMMENT '合同签订日期',
		end_date DATE COMMENT '合同截止日期',
		smelter_company VARCHAR(128) COMMENT '冶炼公司',
		total_quantity DECIMAL(12, 3) COMMENT '合同总数量（吨）',
		truck_count DECIMAL(12, 2) COMMENT '车数（总数量/35）',
		prepayment_ratio DECIMAL(5,4) DEFAULT 0.0000 COMMENT '预付款比例',
		arrival_payment_ratio DECIMAL(5, 4) DEFAULT 0.9 COMMENT '到货款比例',
		final_payment_ratio DECIMAL(5, 4) DEFAULT 0.1 COMMENT '尾款比例',
		contract_image_path VARCHAR(255) COMMENT '合同图片路径',
		status VARCHAR(32) DEFAULT '生效中' COMMENT '状态：生效中/已到期/已终止',
		remarks TEXT COMMENT '备注',
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
		INDEX idx_seq_no (seq_no)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='合同表';
	""",
	"""
	CREATE TABLE IF NOT EXISTS pd_contract_products (
		id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
		contract_id BIGINT NOT NULL COMMENT '合同ID',
		product_name VARCHAR(64) NOT NULL COMMENT '品种名称',
		unit_price DECIMAL(12, 2) COMMENT '单价（元）',
		sort_order INT DEFAULT 0 COMMENT '排序',
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
		FOREIGN KEY (contract_id) REFERENCES pd_contracts(id) ON DELETE CASCADE,
		INDEX idx_contract_id (contract_id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='合同品种表';
	""",
	build_product_categories_table_statement(),
	# 磅单结余管理
	"""
	CREATE TABLE IF NOT EXISTS pd_payment_receipts (
		id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
		receipt_no VARCHAR(64) COMMENT '银行回单流水号',
		receipt_image VARCHAR(255) NOT NULL COMMENT '回单图片存储路径',
		receipt_images TEXT COMMENT '所有回单图片路径JSON数组',
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
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
		INDEX idx_payee_amount (payee_name, amount),
		INDEX idx_payment_date (payment_date),
		INDEX idx_ocr_status (ocr_status),
		INDEX idx_receipt_no (receipt_no)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='支付回单表';
	""",
	"""
	CREATE TABLE IF NOT EXISTS pd_balance_details (
		id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
		contract_no VARCHAR(64) COMMENT '合同编号',
		delivery_id BIGINT COMMENT '报货订单ID',
		weighbill_id BIGINT NOT NULL COMMENT '磅单ID',
		driver_name VARCHAR(64) COMMENT '司机姓名',
		driver_phone VARCHAR(32) COMMENT '司机电话',
		vehicle_no VARCHAR(32) COMMENT '车牌号',
		payee_id BIGINT COMMENT '收款人ID',
		payee_name VARCHAR(64) COMMENT '收款人姓名',
		payee_account VARCHAR(32) COMMENT '收款账号',
		payee_bank_name VARCHAR(64) COMMENT '收款银行名称',
		purchase_unit_price DECIMAL(14, 2) DEFAULT 0 COMMENT '采购单价',
		payable_amount DECIMAL(14, 2) NOT NULL COMMENT '应付金额',
		paid_amount DECIMAL(14, 2) DEFAULT 0 COMMENT '已支付金额',
		balance_amount DECIMAL(14, 2) COMMENT '结余金额',
		payment_status TINYINT DEFAULT 0 COMMENT '0=待支付, 1=部分支付, 2=已结清',
		payout_status TINYINT DEFAULT 0 COMMENT '打款状态：0=待打款, 1=已打款',
		payout_date DATE COMMENT '打款日期',
		schedule_date DATE COMMENT '排款日期',
		schedule_status TINYINT DEFAULT 0 COMMENT '排期状态：0=待排期, 1=已排期',
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
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
		id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
		receipt_id BIGINT NOT NULL COMMENT '支付回单ID',
		balance_id BIGINT NOT NULL COMMENT '结余明细ID',
		settled_amount DECIMAL(14, 2) COMMENT '本次核销金额',
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
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
	""",
	"""
	CREATE TABLE IF NOT EXISTS pd_permission_definitions (
		field_name VARCHAR(64) PRIMARY KEY COMMENT '权限字段名（如 perm_schedule）',
		label VARCHAR(64) NOT NULL COMMENT '权限显示名称',
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='权限字段定义表';
	""",
	"""
	CREATE TABLE IF NOT EXISTS pd_payment_excel_imports (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		payment_detail_id BIGINT COMMENT '关联的收款明细ID',
		weighbill_no VARCHAR(64) COMMENT '磅单号',
		original_amount DECIMAL(15, 2) COMMENT 'Excel中的原始金额',
		processed_amount DECIMAL(15, 2) COMMENT '处理后金额（豫光90%，金利100%）',
		company_type VARCHAR(20) COMMENT '公司类型：yuguang/jinli',
		raw_data JSON COMMENT '原始行数据',
		imported_by BIGINT COMMENT '导入人ID',
		imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		status VARCHAR(20) DEFAULT 'success' COMMENT '处理状态：success/failed',
		fail_reason VARCHAR(500) DEFAULT NULL COMMENT '失败原因',
		INDEX idx_weighbill_no (weighbill_no),
		INDEX idx_payment_detail_id (payment_detail_id),
		INDEX idx_imported_at (imported_at),
		INDEX idx_company_type (company_type)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='回款Excel导入明细记录';
"""
]

def init_permission_definitions():
	"""初始化默认权限字段定义（与原有的 PERMISSION_FIELDS/PERMISSION_LABELS 保持一致）"""
	config = get_mysql_config()
	connection = pymysql.connect(**config)
	try:
		with connection.cursor() as cursor:
			default_perms = [
				('perm_permission_manage', '权限管理'),
				('perm_jinli_payment', '金利回款管理'),
				('perm_yuguang_payment', '豫光回款管理'),
				('perm_schedule', '排期管理'),
				('perm_payout', '打款管理'),
				('perm_payout_stats', '打款统计'),
				('perm_report_stats', '统计与报表'),
				('perm_contract_progress', '合同发运进度'),
				('perm_contract_manage', '销售合同管理'),
				('perm_customer_manage', '客户管理'),
				('perm_delivery_manage', '报货管理'),
				('perm_weighbill_manage', '磅单管理'),
				('perm_warehouse_manage', '库房管理'),      # 新增
				('perm_payee_manage', '收款人管理'),        # 新增
				('perm_account_manage', '账号管理'),
				('perm_role_manage', '角色管理'),
				('perm_ai_detect', 'AI检测'),
				('perm_ai_predict', 'AI预测'),
			]
			for field, label in default_perms:
				cursor.execute(
					"INSERT IGNORE INTO pd_permission_definitions (field_name, label) VALUES (%s, %s)",
					(field, label)
				)
		connection.commit()
		print("默认权限字段定义初始化完成")
	finally:
		connection.close()


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
		init_permission_definitions()
	finally:
		connection.close()


if __name__ == "__main__":
	create_tables()