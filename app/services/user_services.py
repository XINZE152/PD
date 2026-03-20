import bcrypt
import re
from typing import Optional, Dict, Any
from enum import IntEnum
import json
from core.database import get_conn
from core.table_access import build_dynamic_select, _quote_identifier
from core.logging import get_logger

logger = get_logger(__name__)


# ========== 枚举定义 ==========

class UserStatus(IntEnum):
    """用户状态枚举"""
    NORMAL = 0   # 正常
    FROZEN = 1   # 冻结
    DELETED = 2  # 已注销（软删除）


# user_services.py

class UserRole:
    ADMIN = "管理员"
    MANAGER = "大区经理"
    WAREHOUSE = "自营库管理"
    FINANCE = "财务"
    ACCOUNTANT = "会计"
    AUDITOR = "审核主管"      # 新增

    VALID_ROLES = [ADMIN, MANAGER, WAREHOUSE, FINANCE, ACCOUNTANT, AUDITOR]

    # 角色层级（数字越大权限越高，用于权限比较）
    HIERARCHY = {
        ADMIN: 100,
        MANAGER: 80,
        AUDITOR: 70,          # 介于管理员和大区经理之间
        WAREHOUSE: 60,
        FINANCE: 60,
        ACCOUNTANT: 40,
    }


# ========== 工具函数 ==========

def hash_pwd(password: str) -> str:
    """密码加密"""
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt(rounds=12)).decode()


def verify_pwd(password: str, hashed: str) -> bool:
    """密码校验"""
    return bcrypt.checkpw(password.encode(), hashed.encode())


def validate_account(account: str) -> bool:
    """验证账号格式（字母数字下划线，3-20位）"""
    return bool(re.match(r'^[a-zA-Z0-9_]{3,20}$', account))


def validate_phone(phone: str) -> bool:
    """验证手机号格式"""
    return bool(re.match(r'^1[3-9]\d{9}$', phone))


def validate_email(email: str) -> bool:
    """验证邮箱格式"""
    return bool(re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email))


# ========== 用户认证服务 ==========

class AuthService:
    
    @staticmethod
    def ensure_table_exists():
        """
        确保 pd_users 表存在（兼容老库，自动建表）
        实际应在 database_setup.py 中执行，这里仅做检查
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SHOW TABLES LIKE 'pd_users'")
                if not cur.fetchone():
                    raise RuntimeError("pd_users 表不存在，请先执行数据库初始化")
                
                # 检查必要字段
                cur.execute("SHOW COLUMNS FROM pd_users")
                columns = [r["Field"] for r in cur.fetchall()]
                
                required = ["id", "name", "account", "password_hash", "role"]
                missing = [f for f in required if f not in columns]
                if missing:
                    raise RuntimeError(f"pd_users 表缺少必要字段: {missing}")
    
    @staticmethod
    def authenticate(account: str, password: str) -> Dict[str, Any]:
        """
        用户认证（登录）
        
        Args:
            account: 登录账号
            password: 密码
            
        Returns:
            用户信息字典
            
        Raises:
            ValueError: 账号或密码错误
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 动态查询，兼容字段变化
                select_sql = build_dynamic_select(
                    cur,
                    "pd_users",
                    where_clause="account=%s AND status!=%s",
                    select_fields=["id", "name", "account", "password_hash", "role", "status", "phone", "email"]
                )
                cur.execute(select_sql, (account, int(UserStatus.DELETED)))
                user = cur.fetchone()
                
                if not user:
                    raise ValueError("账号或密码错误")
                
                # 验证密码
                stored_hash = user.pop("password_hash")
                if not verify_pwd(password, stored_hash):
                    raise ValueError("账号或密码错误")
                
                return user
    
    @staticmethod
    def create_user(
        name: str,
        account: str,
        password: str,
        role: str,
        phone: Optional[str] = None,
        email: Optional[str] = None,
        created_by: Optional[int] = None
    ) -> int:
        """
        创建新用户
        
        Args:
            name: 用户姓名
            account: 登录账号
            password: 密码
            role: 角色
            phone: 手机号（可选）
            email: 邮箱（可选）
            created_by: 创建人ID（可选）
            
        Returns:
            新用户ID
            
        Raises:
            ValueError: 参数校验失败或账号已存在
        """
        # 参数校验
        if not validate_account(account):
            raise ValueError("账号格式错误（3-20位字母数字下划线）")
        
        if phone and not validate_phone(phone):
            raise ValueError("手机号格式错误")
        
        if email and not validate_email(email):
            raise ValueError("邮箱格式错误")
        
        if role not in UserRole.VALID_ROLES:
            raise ValueError(f"无效的角色: {role}")
        
        pwd_hash = hash_pwd(password)
        
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 检查账号是否已存在
                cur.execute("SELECT 1 FROM pd_users WHERE account=%s LIMIT 1", (account,))
                if cur.fetchone():
                    raise ValueError("账号已存在")
                
                # 检查手机号是否已被使用
                if phone:
                    cur.execute("SELECT 1 FROM pd_users WHERE phone=%s AND status!=%s LIMIT 1", 
                               (phone, int(UserStatus.DELETED)))
                    if cur.fetchone():
                        raise ValueError("手机号已被注册")
                
                # 动态获取表结构，兼容字段变化
                cur.execute("SHOW COLUMNS FROM pd_users")
                columns = [r["Field"] for r in cur.fetchall()]
                
                # 准备插入数据
                data = {
                    "name": name,
                    "account": account,
                    "password_hash": pwd_hash,
                    "role": role,
                    "status": int(UserStatus.NORMAL)
                }
                
                if phone and "phone" in columns:
                    data["phone"] = phone
                if email and "email" in columns:
                    data["email"] = email
                
                # 构建插入SQL
                cols = list(data.keys())
                vals = list(data.values())
                
                cols_sql = ",".join([_quote_identifier(c) for c in cols])
                placeholders = ",".join(["%s"] * len(vals))
                
                sql = f"INSERT INTO {_quote_identifier('pd_users')} ({cols_sql}) VALUES ({placeholders})"
                cur.execute(sql, tuple(vals))
                
                user_id = cur.lastrowid
                conn.commit()
                
                logger.info(f"创建用户成功: {account} (ID: {user_id}, 角色: {role})")
                return user_id
    
    @staticmethod
    def get_user_by_id(user_id: int) -> Optional[Dict[str, Any]]:
        """
        根据ID获取用户信息（不含密码）
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                select_sql = build_dynamic_select(
                    cur,
                    "pd_users",
                    where_clause="id=%s",
                    select_fields=["id", "name", "account", "role", "phone", "email", "status", "created_at", "updated_at"]
                )
                cur.execute(select_sql, (user_id,))
                return cur.fetchone()
    
    @staticmethod
    def get_user_by_account(account: str) -> Optional[Dict[str, Any]]:
        """
        根据账号获取用户信息
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                select_sql = build_dynamic_select(
                    cur,
                    "pd_users",
                    where_clause="account=%s AND status!=%s",
                    select_fields=["id", "name", "account", "role", "phone", "email", "status"]
                )
                cur.execute(select_sql, (account, int(UserStatus.DELETED)))
                return cur.fetchone()
    
    @staticmethod
    def update_user(user_id: int, **kwargs) -> bool:
        """
        更新用户信息
        
        Args:
            user_id: 用户ID
            **kwargs: 要更新的字段
            
        Returns:
            是否更新成功
        """
        allowed_fields = ["name", "phone", "email", "role"]
        updates = {k: v for k, v in kwargs.items() if k in allowed_fields and v is not None}
        
        if not updates:
            raise ValueError("无有效更新字段")
        
        # 验证数据
        if "phone" in updates and updates["phone"] and not validate_phone(updates["phone"]):
            raise ValueError("手机号格式错误")
        if "email" in updates and updates["email"] and not validate_email(updates["email"]):
            raise ValueError("邮箱格式错误")
        if "role" in updates and updates["role"] not in UserRole.VALID_ROLES:
            raise ValueError("无效的角色")
        
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 检查用户是否存在
                cur.execute("SELECT 1 FROM pd_users WHERE id=%s", (user_id,))
                if not cur.fetchone():
                    raise ValueError("用户不存在")
                
                # 检查手机号唯一性
                if "phone" in updates and updates["phone"]:
                    cur.execute(
                        "SELECT 1 FROM pd_users WHERE phone=%s AND id!=%s AND status!=%s LIMIT 1",
                        (updates["phone"], user_id, int(UserStatus.DELETED))
                    )
                    if cur.fetchone():
                        raise ValueError("手机号已被其他用户使用")
                
                # 构建更新SQL
                set_parts = []
                vals = []
                for k, v in updates.items():
                    set_parts.append(f"{_quote_identifier(k)}=%s")
                    vals.append(v)
                
                set_clause = ", ".join(set_parts)
                sql = f"UPDATE {_quote_identifier('pd_users')} SET {set_clause} WHERE id=%s"
                vals.append(user_id)
                
                cur.execute(sql, tuple(vals))
                conn.commit()
                
                logger.info(f"更新用户成功: ID={user_id}, 字段={list(updates.keys())}")
                return True
    
    @staticmethod
    def change_password(user_id: int, old_password: str, new_password: str) -> bool:
        """
        用户修改密码
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 获取当前密码哈希
                cur.execute(
                    "SELECT password_hash FROM pd_users WHERE id=%s AND status!=%s",
                    (user_id, int(UserStatus.DELETED))
                )
                row = cur.fetchone()
                if not row:
                    raise ValueError("用户不存在")
                
                # 验证旧密码
                if not verify_pwd(old_password, row["password_hash"]):
                    raise ValueError("旧密码错误")
                
                # 更新密码
                new_hash = hash_pwd(new_password)
                cur.execute(
                    "UPDATE pd_users SET password_hash=%s WHERE id=%s",
                    (new_hash, user_id)
                )
                conn.commit()
                
                logger.info(f"用户修改密码成功: ID={user_id}")
                return True
    
    @staticmethod
    def admin_reset_password(user_id: int, new_password: str) -> bool:
        """
        管理员重置密码
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 检查用户是否存在
                cur.execute("SELECT 1 FROM pd_users WHERE id=%s", (user_id,))
                if not cur.fetchone():
                    raise ValueError("用户不存在")
                
                new_hash = hash_pwd(new_password)
                cur.execute(
                    "UPDATE pd_users SET password_hash=%s WHERE id=%s",
                    (new_hash, user_id)
                )
                conn.commit()
                
                logger.info(f"管理员重置密码: ID={user_id}")
                return True
    
    @staticmethod
    def set_user_status(user_id: int, status: UserStatus) -> bool:
        """
        设置用户状态（冻结/解冻/注销）
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT status FROM pd_users WHERE id=%s", (user_id,))
                row = cur.fetchone()
                if not row:
                    raise ValueError("用户不存在")
                
                old_status = row["status"]
                if old_status == status:
                    raise ValueError("状态未变化")
                
                cur.execute(
                    "UPDATE pd_users SET status=%s WHERE id=%s",
                    (int(status), user_id)
                )
                conn.commit()
                
                status_names = {0: "正常", 1: "冻结", 2: "注销"}
                logger.info(f"用户状态变更: ID={user_id}, {status_names.get(old_status)} -> {status_names.get(status)}")
                return True
    
    @staticmethod
    def delete_user(user_id: int) -> bool:
        """
        删除用户（软删除，设置状态为已注销）
        """
        return AuthService.set_user_status(user_id, UserStatus.DELETED)
    
    @staticmethod
    def list_users(
        page: int = 1,
        size: int = 20,
        role: Optional[str] = None,
        keyword: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        获取用户列表（分页）
        
        Args:
            page: 页码
            size: 每页数量
            role: 角色筛选
            keyword: 关键词搜索（姓名/账号）
            
        Returns:
            包含列表和分页信息的字典
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 构建WHERE条件
                where_conditions = ["status != %s"]
                params = [int(UserStatus.DELETED)]
                
                if role:
                    where_conditions.append("role = %s")
                    params.append(role)
                
                if keyword:
                    where_conditions.append("(name LIKE %s OR account LIKE %s)")
                    params.extend([f"%{keyword}%", f"%{keyword}%"])
                
                where_clause = " AND ".join(where_conditions)
                
                # 查询总数
                count_sql = f"SELECT COUNT(*) as total FROM pd_users WHERE {where_clause}"
                cur.execute(count_sql, tuple(params))
                total = cur.fetchone()["total"]
                
                # 查询列表
                offset = (page - 1) * size
                select_sql = f"""
                    SELECT id, name, account, role, phone, email, status, created_at, updated_at
                    FROM pd_users
                    WHERE {where_clause}
                    ORDER BY created_at DESC
                    LIMIT %s OFFSET %s
                """
                params.extend([size, offset])
                
                cur.execute(select_sql, tuple(params))
                rows = cur.fetchall()
                
                return {
                    "total": total,
                    "page": page,
                    "size": size,
                    "pages": (total + size - 1) // size,
                    "list": rows
                }

    @staticmethod
    def list_managers() -> list:
        """
        獲取所有大區經理（正常狀態）
        返回: [{"id": 1, "name": "張三", "account": "zhangsan"}, ...]
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, name, account, role, phone
                    FROM pd_users
                    WHERE role = %s AND status = %s
                    ORDER BY name
                """, (UserRole.MANAGER, int(UserStatus.NORMAL)))
                rows = cur.fetchall()
                return [dict(r) for r in rows]
    
    @staticmethod
    def check_permission(user_role: str, required_role: str) -> bool:
        """
        检查角色权限是否满足要求
        """
        user_level = UserRole.HIERARCHY.get(user_role, 0)
        required_level = UserRole.HIERARCHY.get(required_role, 0)
        return user_level >= required_level


# ========== 权限管理服务 ==========

class PermissionService:
    """用户权限管理服务（动态权限字段版）"""

    # 缓存（类级别）
    _fields_cache = None          # List[str]
    _labels_cache = None          # Dict[str, str]

    VALID_ROLES = ['管理员', '大区经理', '自营库管理', '财务', '会计', '审核主管']

    @classmethod
    def _load_definitions(cls):
        """从数据库加载权限字段定义，更新缓存"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT field_name, label FROM pd_permission_definitions ORDER BY field_name")
                rows = cur.fetchall()
                cls._fields_cache = [row['field_name'] for row in rows]
                cls._labels_cache = {row['field_name']: row['label'] for row in rows}

    @classmethod
    def get_all_fields(cls):
        """获取所有权限字段名列表（动态）"""
        if cls._fields_cache is None:
            cls._load_definitions()
        return cls._fields_cache

    @classmethod
    def get_label(cls, field_name):
        """获取指定权限字段的显示名称"""
        if cls._labels_cache is None:
            cls._load_definitions()
        return cls._labels_cache.get(field_name, field_name)

    @classmethod
    def refresh_cache(cls):
        """刷新缓存（在增删权限定义后调用）"""
        cls._fields_cache = None
        cls._labels_cache = None
        cls._load_definitions()
    @staticmethod
    def ensure_table_exists():
        """确保权限表、角色模板表、权限定义表存在"""
        # 原有代码不变，但需保证 pd_permission_definitions 已创建
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 原有 pd_user_permissions 表创建代码...
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS pd_role_templates (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        role VARCHAR(32) NOT NULL UNIQUE,
                        template_json TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
                
                # 检查是否已有数据，有则跳过初始化
                cur.execute("SELECT COUNT(*) as count FROM pd_role_templates")
                row = cur.fetchone()
                if row and row['count'] > 0:
                    return  # 已有数据，不覆盖
                
                # 只在表为空时才插入默认模板
                default_role_templates = {
                    '管理员': {f: 1 for f in PermissionService.get_all_fields()},
                    '大区经理': {
                        'perm_schedule': 1,
                        'perm_payout': 1,
                        'perm_payout_stats': 1,
                        'perm_report_stats': 1,
                        'perm_contract_progress': 1,
                        'perm_contract_manage': 1,
                        'perm_customer_manage': 1,
                        'perm_delivery_manage': 1,
                        'perm_weighbill_manage': 1,
                        'perm_warehouse_manage': 1,
                        'perm_payee_manage': 1,
                        'perm_account_manage': 1,
                        'perm_role_manage': 1,
                    },
                    '自营库管理': {
                        'perm_delivery_manage': 1,
                        'perm_weighbill_manage': 1,
                        'perm_warehouse_manage': 1,
                    },
                    '财务': {
                        'perm_jinli_payment': 1,
                        'perm_yuguang_payment': 1,
                        'perm_schedule': 1,
                        'perm_payout': 1,
                        'perm_payout_stats': 1,
                        'perm_report_stats': 1,
                        'perm_payee_manage': 1,
                    },
                    '会计': {
                        'perm_jinli_payment': 1,
                        'perm_yuguang_payment': 1,
                        'perm_report_stats': 1,
                    },
                    '审核主管': {  # 新增
                        'perm_delivery_manage': 1,  # 报货管理（查看、审核）
                        'perm_weighbill_manage': 1,  # 磅单管理
                        'perm_contract_progress': 1,  # 合同发运进度
                        'perm_report_stats': 1,  # 统计报表（可选）
                        'perm_customer_manage': 1,  # 客户管理（可选）
                    },
                }
                for role, perms in default_role_templates.items():
                    # 补齐所有字段（未在模板中定义的置0）
                    full_perms = {f: perms.get(f, 0) for f in PermissionService.get_all_fields()}
                    cur.execute("""
                        INSERT INTO pd_role_templates (role, template_json) 
                        VALUES (%s, %s)
                    """, (role, json.dumps(full_perms)))
                conn.commit()
                for role, perms in default_role_templates.items():
                    # 补齐所有字段（未在模板中定义的置0）
                    full_perms = {f: perms.get(f, 0) for f in PermissionService.get_all_fields()}
                    cur.execute("""
                        INSERT INTO pd_role_templates (role, template_json) 
                        VALUES (%s, %s) 
                        ON DUPLICATE KEY UPDATE template_json = VALUES(template_json)
                    """, (role, json.dumps(full_perms)))
                conn.commit()

    # ---------- 角色模板相关 ----------
    @staticmethod
    def get_role_template(role: str) -> Dict[str, int]:
        """从数据库获取角色模板，并确保包含所有现有字段"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT template_json FROM pd_role_templates WHERE role=%s", (role,))
                row = cur.fetchone()
                if row:
                    template = json.loads(row['template_json'])
                else:
                    # 如果数据库中没有，返回空模板（但通常不应该发生）
                    template = {}
        # 补齐所有字段
        all_fields = PermissionService.get_all_fields()
        full_template = {field: template.get(field, 0) for field in all_fields}
        return full_template

    # 在 PermissionService 类中添加
    from typing import List, Optional

    @staticmethod
    def apply_role_template_to_users(role: str, user_ids: Optional[List[int]] = None) -> int:
        """
        将指定角色的权限模板应用到用户（覆盖用户现有权限）
        Args:
            role: 角色名称
            user_ids: 可选的用户ID列表，如果为None则应用到所有该角色的用户
        Returns:
            更新的用户数量
        """
        # 获取模板（已包含所有字段）
        template = PermissionService.get_role_template(role)
        all_fields = PermissionService.get_all_fields()

        with get_conn() as conn:
            with conn.cursor() as cur:
                # 确定要更新的用户ID列表
                if user_ids is None:
                    cur.execute("SELECT user_id FROM pd_user_permissions WHERE role=%s", (role,))
                    rows = cur.fetchall()
                    user_ids = [row['user_id'] for row in rows]
                    if not user_ids:
                        return 0

                # 对每个用户重建权限
                for uid in user_ids:
                    # 删除现有权限
                    cur.execute("DELETE FROM pd_user_permissions WHERE user_id=%s", (uid,))
                    # 插入新权限（基于模板）
                    fields = ['user_id', 'role'] + all_fields
                    values = [uid, role] + [template.get(f, 0) for f in all_fields]
                    placeholders = ','.join(['%s'] * len(values))
                    fields_sql = ','.join(fields)
                    sql = f"INSERT INTO pd_user_permissions ({fields_sql}) VALUES ({placeholders})"
                    cur.execute(sql, tuple(values))

                conn.commit()
                logger.info(f"已将角色 {role} 的模板应用到 {len(user_ids)} 个用户")
                return len(user_ids)

    @staticmethod
    def update_role_template(role: str, permissions: Dict[str, bool], apply_to_existing: bool = False) -> bool:
        """
        更新角色模板
        Args:
            role: 角色名称
            permissions: 权限字典
            apply_to_existing: 是否将更新后的模板应用到现有用户
        """
        # 构建完整权限字典
        all_fields = PermissionService.get_all_fields()
        full_permissions = {
            field: 1 if permissions.get(field, False) else 0
            for field in all_fields
        }

        with get_conn() as conn:
            with conn.cursor() as cur:
                # 更新模板表
                cur.execute("""
                    INSERT INTO pd_role_templates (role, template_json) 
                    VALUES (%s, %s) 
                    ON DUPLICATE KEY UPDATE template_json = VALUES(template_json)
                """, (role, json.dumps(full_permissions)))
                conn.commit()

        # 如果需要应用到现有用户
        if apply_to_existing:
            PermissionService.apply_role_template_to_users(role)

        return True

    @staticmethod
    def get_all_role_templates() -> Dict[str, Dict]:
        """获取所有角色模板（从数据库读取）"""
        templates = {}
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT role, template_json FROM pd_role_templates")
                rows = cur.fetchall()
                for row in rows:
                    role = row['role']
                    templates[role] = json.loads(row['template_json'])
        return templates

    # ---------- 用户权限操作 ----------
    @staticmethod
    def create_default_permissions(user_id: int, role: str) -> bool:
        """为新用户创建默认权限（基于角色模板）"""
        if role not in PermissionService.VALID_ROLES:
            role = '会计'

        # 获取角色模板（已包含所有字段）
        template = PermissionService.get_role_template(role)
        all_fields = PermissionService.get_all_fields()

        with get_conn() as conn:
            with conn.cursor() as cur:
                # 检查是否已存在
                cur.execute("SELECT id FROM pd_user_permissions WHERE user_id=%s", (user_id,))
                if cur.fetchone():
                    return False

                # 构建插入数据
                fields = ['user_id', 'role'] + all_fields
                values = [user_id, role]
                for field in all_fields:
                    values.append(template.get(field, 0))

                placeholders = ','.join(['%s'] * len(values))
                fields_sql = ','.join(fields)

                sql = f"INSERT INTO pd_user_permissions ({fields_sql}) VALUES ({placeholders})"
                cur.execute(sql, tuple(values))
                conn.commit()

                logger.info(f"创建默认权限: user_id={user_id}, role={role}")
                return True

    @staticmethod
    def get_user_permissions(user_id: int) -> Optional[Dict[str, Any]]:
        """获取用户权限详情（动态字段）"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 获取用户基本信息
                cur.execute("""
                    SELECT id, name, account, role as base_role 
                    FROM pd_users 
                    WHERE id=%s AND status!=%s
                """, (user_id, int(UserStatus.DELETED)))
                user = cur.fetchone()
                if not user:
                    return None

                # 获取权限配置
                cur.execute("SELECT * FROM pd_user_permissions WHERE user_id=%s", (user_id,))
                perm_row = cur.fetchone()
                if not perm_row:
                    PermissionService.create_default_permissions(user_id, user['base_role'])
                    cur.execute("SELECT * FROM pd_user_permissions WHERE user_id=%s", (user_id,))
                    perm_row = cur.fetchone()

                # 构建权限字典（只保留权限字段）
                all_fields = PermissionService.get_all_fields()
                permissions = {}
                for field in all_fields:
                    permissions[field] = bool(perm_row.get(field, 0)) if perm_row else False

                # 添加显示名称
                permissions_with_labels = {}
                for field, value in permissions.items():
                    permissions_with_labels[field] = {
                        'value': value,
                        'label': PermissionService.get_label(field)
                    }

                return {
                    'user_id': user_id,
                    'name': user['name'],
                    'account': user['account'],
                    'base_role': user['base_role'],
                    'current_role': perm_row['role'] if perm_row else user['base_role'],
                    'role': perm_row['role'] if perm_row else user['base_role'],
                    'permissions': permissions,
                    'permissions_with_labels': permissions_with_labels,
                    'updated_at': str(perm_row['updated_at']) if perm_row else None
                }

    @staticmethod
    def update_permissions(user_id: int, role: str = None, permissions: Dict[str, bool] = None) -> bool:
        """更新用户权限和角色"""
        if role and role not in PermissionService.VALID_ROLES:
            raise ValueError(f"无效的角色，可选: {PermissionService.VALID_ROLES}")

        with get_conn() as conn:
            with conn.cursor() as cur:
                # 检查权限记录是否存在
                cur.execute("SELECT id FROM pd_user_permissions WHERE user_id=%s", (user_id,))
                perm_row = cur.fetchone()
                if not perm_row:
                    # 获取用户角色创建默认
                    cur.execute("SELECT role FROM pd_users WHERE id=%s", (user_id,))
                    user = cur.fetchone()
                    if not user:
                        raise ValueError("用户不存在")
                    PermissionService.create_default_permissions(user_id, role or user['role'])
                    cur.execute("SELECT id FROM pd_user_permissions WHERE user_id=%s", (user_id,))
                    perm_row = cur.fetchone()

                # 构建更新
                updates = []
                params = []
                if role:
                    updates.append("role=%s")
                    params.append(role)
                if permissions:
                    # 只更新传入的权限字段
                    for perm_field, value in permissions.items():
                        # 验证字段是否存在
                        if perm_field not in PermissionService.get_all_fields():
                            raise ValueError(f"无效的权限字段: {perm_field}")
                        updates.append(f"{perm_field}=%s")
                        params.append(1 if value else 0)
                if not updates:
                    return True

                params.append(user_id)
                set_clause = ", ".join(updates)
                sql = f"UPDATE pd_user_permissions SET {set_clause} WHERE user_id=%s"
                cur.execute(sql, tuple(params))
                conn.commit()

                logger.info(f"更新权限: user_id={user_id}, updates={updates}")
                return True

    @staticmethod
    def check_permission(user_id: int, permission_field: str) -> bool:
        """检查用户是否有指定权限"""
        if permission_field not in PermissionService.get_all_fields():
            return False
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT {permission_field} FROM pd_user_permissions WHERE user_id=%s", (user_id,))
                row = cur.fetchone()
                if not row:
                    return False
                return bool(row.get(permission_field, 0))

    @staticmethod
    def list_all_permissions(page: int = 1, size: int = 20, role: str = None, keyword: str = None) -> Dict[str, Any]:
        """获取所有用户权限列表（动态字段）"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                where_conditions = ["u.status != %s"]
                params = [int(UserStatus.DELETED)]
                if role:
                    where_conditions.append("(p.role=%s OR (p.role IS NULL AND u.role=%s))")
                    params.extend([role, role])
                if keyword:
                    where_conditions.append("(u.name LIKE %s OR u.account LIKE %s)")
                    params.extend([f"%{keyword}%", f"%{keyword}%"])
                where_clause = " AND ".join(where_conditions)

                # 总数
                count_sql = f"""
                    SELECT COUNT(*) as total 
                    FROM pd_users u
                    LEFT JOIN pd_user_permissions p ON u.id=p.user_id
                    WHERE {where_clause}
                """
                cur.execute(count_sql, tuple(params))
                total = cur.fetchone()['total']

                # 动态构建查询字段
                all_fields = PermissionService.get_all_fields()
                select_fields = ','.join([f'p.{f}' for f in all_fields])

                offset = (page - 1) * size
                select_sql = f"""
                    SELECT 
                        u.id as user_id,
                        u.name,
                        u.account,
                        COALESCE(p.role, u.role) as role,
                        {select_fields}
                    FROM pd_users u
                    LEFT JOIN pd_user_permissions p ON u.id=p.user_id
                    WHERE {where_clause}
                    ORDER BY u.created_at DESC
                    LIMIT %s OFFSET %s
                """
                params.extend([size, offset])
                cur.execute(select_sql, tuple(params))
                rows = cur.fetchall()

                result_list = []
                for row in rows:
                    user_data = {
                        'user_id': row['user_id'],
                        'name': row['name'],
                        'account': row['account'],
                        'role': row['role'],
                    }
                    # 添加权限字段
                    for field in all_fields:
                        user_data[field] = bool(row.get(field, 0))
                    # 添加权限标签列表
                    user_data['permissions_list'] = [
                        {
                            'field': field,
                            'label': PermissionService.get_label(field),
                            'value': bool(row.get(field, 0))
                        }
                        for field in all_fields
                    ]
                    result_list.append(user_data)

                return {
                    "total": total,
                    "page": page,
                    "size": size,
                    "pages": (total + size - 1) // size,
                    "list": result_list
                }

    @staticmethod
    def delete_permissions(user_id: int) -> bool:
        """删除用户权限（用户删除时调用）"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM pd_user_permissions WHERE user_id=%s", (user_id,))
                conn.commit()
                return True

    # ---------- 动态权限字段管理 ----------
    @staticmethod
    def add_permission_definition(field_name: str, label: str) -> bool:
        """
        动态添加一个新的权限字段
        - 校验字段名格式（必须以 perm_ 开头，只含小写字母、数字、下划线）
        - 检查是否已存在
        - 执行 ALTER TABLE pd_user_permissions ADD COLUMN
        - 插入 pd_permission_definitions
        - 更新 pd_role_templates 中的每个角色模板 JSON，添加该字段（默认0）
        - 刷新缓存
        """
        # 1. 校验格式
        if not re.match(r'^perm_[a-z][a-z0-9_]*$', field_name):
            raise ValueError("字段名必须以 'perm_' 开头，且只能包含小写字母、数字、下划线")

        with get_conn() as conn:
            with conn.cursor() as cur:
                # 2. 检查是否已存在（通过定义表）
                cur.execute("SELECT 1 FROM pd_permission_definitions WHERE field_name=%s", (field_name,))
                if cur.fetchone():
                    raise ValueError(f"权限字段 {field_name} 已存在")

                # 3. 执行 ALTER TABLE 添加列
                alter_sql = f"ALTER TABLE pd_user_permissions ADD COLUMN `{field_name}` TINYINT DEFAULT 0 COMMENT %s"
                cur.execute(alter_sql, (label,))

                # 4. 插入定义表
                cur.execute(
                    "INSERT INTO pd_permission_definitions (field_name, label) VALUES (%s, %s)",
                    (field_name, label)
                )

                # 5. 更新角色模板表：为每个角色的 JSON 增加该字段，值为0
                cur.execute("SELECT role, template_json FROM pd_role_templates")
                templates = cur.fetchall()
                for row in templates:
                    role = row['role']
                    template = json.loads(row['template_json'])
                    if field_name not in template:
                        template[field_name] = 0
                        cur.execute(
                            "UPDATE pd_role_templates SET template_json=%s WHERE role=%s",
                            (json.dumps(template), role)
                        )

                conn.commit()

        # 6. 刷新缓存
        PermissionService.refresh_cache()
        logger.info(f"新增权限字段成功: {field_name} ({label})")
        return True

    @staticmethod
    def remove_permission_definition(field_name: str) -> bool:
        """
        动态删除一个权限字段
        - 检查字段是否存在且不是系统保留字段
        - 执行 ALTER TABLE pd_user_permissions DROP COLUMN
        - 从 pd_permission_definitions 删除
        - 从所有角色模板 JSON 中移除该字段
        - 刷新缓存
        """
        # 可选保护：禁止删除某些核心权限
        protected = ['perm_permission_manage']  # 根据需要调整
        if field_name in protected:
            raise ValueError(f"字段 {field_name} 为系统保留，不可删除")

        with get_conn() as conn:
            with conn.cursor() as cur:
                # 检查是否存在
                cur.execute("SELECT 1 FROM pd_permission_definitions WHERE field_name=%s", (field_name,))
                if not cur.fetchone():
                    raise ValueError(f"权限字段 {field_name} 不存在")

                # 执行 ALTER TABLE 删除列
                alter_sql = f"ALTER TABLE pd_user_permissions DROP COLUMN `{field_name}`"
                cur.execute(alter_sql)

                # 从定义表删除
                cur.execute("DELETE FROM pd_permission_definitions WHERE field_name=%s", (field_name,))

                # 从角色模板 JSON 中移除该字段
                cur.execute("SELECT role, template_json FROM pd_role_templates")
                templates = cur.fetchall()
                for row in templates:
                    role = row['role']
                    template = json.loads(row['template_json'])
                    if field_name in template:
                        del template[field_name]
                        cur.execute(
                            "UPDATE pd_role_templates SET template_json=%s WHERE role=%s",
                            (json.dumps(template), role)
                        )

                conn.commit()

        # 刷新缓存
        PermissionService.refresh_cache()
        logger.info(f"删除权限字段成功: {field_name}")
        return True