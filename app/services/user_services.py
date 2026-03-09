import bcrypt
import re
from typing import Optional, Dict, Any, List
from enum import IntEnum
from datetime import datetime

from core.database import get_conn
from core.table_access import build_dynamic_select, _quote_identifier, build_select_list
from core.logging import get_logger

logger = get_logger(__name__)


# ========== 枚举定义 ==========

class UserStatus(IntEnum):
    """用户状态枚举"""
    NORMAL = 0   # 正常
    FROZEN = 1   # 冻结
    DELETED = 2  # 已注销（软删除）


class UserRole:
    """用户角色"""
    ADMIN = "管理员"
    MANAGER = "大区经理"
    WAREHOUSE = "自营库管理"
    FINANCE = "财务"
    ACCOUNTANT = "会计"
    
    VALID_ROLES = [ADMIN, MANAGER, WAREHOUSE, FINANCE, ACCOUNTANT]
    
    # 角色层级（数字越大权限越高）
    HIERARCHY = {
        ADMIN: 100,
        MANAGER: 80,
        WAREHOUSE: 60,
        FINANCE: 60,
        ACCOUNTANT: 40
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
    def check_permission(user_role: str, required_role: str) -> bool:
        """
        检查角色权限是否满足要求
        """
        user_level = UserRole.HIERARCHY.get(user_role, 0)
        required_level = UserRole.HIERARCHY.get(required_role, 0)
        return user_level >= required_level


# ========== 权限管理服务 ==========

class PermissionService:
    """用户权限管理服务"""

    # 所有权限字段定义（用于动态操作）
    PERMISSION_FIELDS = [
        'perm_permission_manage',  # 权限管理权限
        'perm_jinli_payment',  # 金利回款管理权限
        'perm_yuguang_payment',  # 豫光回款管理权限
        'perm_schedule',  # 排期管理权限
        'perm_payout',  # 打款管理权限
        'perm_payout_stats',  # 打款统计权限
        'perm_report_stats',  # 统计与报表权限
        'perm_contract_progress',  # 合同发运进度权限
        'perm_contract_manage',  # 销售合同管理权限
        'perm_customer_manage',  # 客户管理权限
        'perm_delivery_manage',  # 报货管理权限
        'perm_weighbill_manage',  # 磅单管理权限
        'perm_warehouse_manage',  # 库房和收款人信息管理权限
        'perm_account_manage',  # 账号管理权限
        'perm_role_manage',  # 角色管理权限
        'perm_ai_detect',  # AI检测权限
        'perm_ai_predict',  # AI预测权限
    ]

    # 权限显示名称映射
    PERMISSION_LABELS = {
        'perm_permission_manage': '权限管理',
        'perm_jinli_payment': '金利回款管理',
        'perm_yuguang_payment': '豫光回款管理',
        'perm_schedule': '排期管理',
        'perm_payout': '打款管理',
        'perm_payout_stats': '打款统计',
        'perm_report_stats': '统计与报表',
        'perm_contract_progress': '合同发运进度',
        'perm_contract_manage': '销售合同管理',
        'perm_customer_manage': '客户管理',
        'perm_delivery_manage': '报货管理',
        'perm_weighbill_manage': '磅单管理',
        'perm_warehouse_manage': '库房和收款人信息管理',
        'perm_account_manage': '账号管理',
        'perm_role_manage': '角色管理',
        'perm_ai_detect': 'AI检测',
        'perm_ai_predict': 'AI预测',
    }

    # 角色权限模板（创建时默认权限）
    ROLE_TEMPLATES = {
        '管理员': {f: 1 for f in PERMISSION_FIELDS},  # 全部权限
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
            'perm_account_manage': 1,
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
        },
        '会计': {
            'perm_jinli_payment': 1,
            'perm_yuguang_payment': 1,
            'perm_report_stats': 1,
        },
    }

    VALID_ROLES = ['管理员', '大区经理', '自营库管理', '财务', '会计']

    @staticmethod
    def ensure_table_exists():
        """确保权限表存在"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS pd_user_permissions (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        role VARCHAR(32) NOT NULL DEFAULT '会计',
                        perm_permission_manage TINYINT DEFAULT 0,
                        perm_jinli_payment TINYINT DEFAULT 0,
                        perm_yuguang_payment TINYINT DEFAULT 0,
                        perm_schedule TINYINT DEFAULT 0,
                        perm_payout TINYINT DEFAULT 0,
                        perm_payout_stats TINYINT DEFAULT 0,
                        perm_report_stats TINYINT DEFAULT 0,
                        perm_contract_progress TINYINT DEFAULT 0,
                        perm_contract_manage TINYINT DEFAULT 0,
                        perm_customer_manage TINYINT DEFAULT 0,
                        perm_delivery_manage TINYINT DEFAULT 0,
                        perm_weighbill_manage TINYINT DEFAULT 0,
                        perm_warehouse_manage TINYINT DEFAULT 0,
                        perm_account_manage TINYINT DEFAULT 0,
                        perm_role_manage TINYINT DEFAULT 0,
                        perm_ai_detect TINYINT DEFAULT 0,
                        perm_ai_predict TINYINT DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        UNIQUE KEY uk_user_id (user_id),
                        INDEX idx_role (role)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)

    @staticmethod
    def create_default_permissions(user_id: int, role: str) -> bool:
        """
        为新用户创建默认权限
        """
        if role not in PermissionService.VALID_ROLES:
            role = '会计'

        # 获取角色模板
        template = PermissionService.ROLE_TEMPLATES.get(role, {})

        with get_conn() as conn:
            with conn.cursor() as cur:
                # 检查是否已存在
                cur.execute("SELECT id FROM pd_user_permissions WHERE user_id=%s", (user_id,))
                if cur.fetchone():
                    return False

                # 构建插入数据
                fields = ['user_id', 'role'] + PermissionService.PERMISSION_FIELDS
                values = [user_id, role]

                for perm_field in PermissionService.PERMISSION_FIELDS:
                    values.append(1 if perm_field in template else 0)

                placeholders = ','.join(['%s'] * len(values))
                fields_sql = ','.join(fields)

                sql = f"INSERT INTO pd_user_permissions ({fields_sql}) VALUES ({placeholders})"
                cur.execute(sql, tuple(values))
                conn.commit()

                logger.info(f"创建默认权限: user_id={user_id}, role={role}")
                return True

    @staticmethod
    def get_user_permissions(user_id: int) -> Optional[Dict[str, Any]]:
        """
        获取用户权限详情
        """
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
                cur.execute("""
                    SELECT * FROM pd_user_permissions 
                    WHERE user_id=%s
                """, (user_id,))
                perm_row = cur.fetchone()

                # 如果没有权限记录，创建默认
                if not perm_row:
                    PermissionService.create_default_permissions(user_id, user['base_role'])
                    cur.execute("SELECT * FROM pd_user_permissions WHERE user_id=%s", (user_id,))
                    perm_row = cur.fetchone()

                # 构建权限字典
                permissions = {}
                for field in PermissionService.PERMISSION_FIELDS:
                    permissions[field] = bool(perm_row.get(field, 0)) if perm_row else False

                # 添加显示名称
                permissions_with_labels = {}
                for field, value in permissions.items():
                    permissions_with_labels[field] = {
                        'value': value,
                        'label': PermissionService.PERMISSION_LABELS.get(field, field)
                    }

                return {
                    'user_id': user_id,
                    'name': user['name'],
                    'account': user['account'],
                    'base_role': user['base_role'],  # pd_users表中的角色
                    'current_role': perm_row['role'] if perm_row else user['base_role'],  # 权限表中的角色
                    'role': perm_row['role'] if perm_row else user['base_role'],  # 当前生效角色
                    'permissions': permissions,
                    'permissions_with_labels': permissions_with_labels,
                    'updated_at': str(perm_row['updated_at']) if perm_row else None
                }

    @staticmethod
    def update_permissions(user_id: int, role: str = None, permissions: Dict[str, bool] = None) -> bool:
        """
        更新用户权限和角色

        Args:
            user_id: 用户ID
            role: 新角色（可选）
            permissions: 权限字典（可选，如 {'perm_schedule': True, 'perm_payout': False}）
        """
        # 验证角色
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

                # 更新角色
                if role:
                    updates.append("role=%s")
                    params.append(role)

                    # 如果更换角色，可以选择是否重置为角色模板
                    # 这里不自动重置，保持原有权限逻辑

                # 更新权限
                if permissions:
                    for perm_field, value in permissions.items():
                        if perm_field in PermissionService.PERMISSION_FIELDS:
                            updates.append(f"{perm_field}=%s")
                            params.append(1 if value else 0)

                if not updates:
                    return True  # 无更新

                params.append(user_id)
                set_clause = ", ".join(updates)
                sql = f"UPDATE pd_user_permissions SET {set_clause} WHERE user_id=%s"

                cur.execute(sql, tuple(params))
                conn.commit()

                logger.info(f"更新权限: user_id={user_id}, updates={updates}")
                return True

    @staticmethod
    def check_permission(user_id: int, permission_field: str) -> bool:
        """
        检查用户是否有指定权限
        """
        if permission_field not in PermissionService.PERMISSION_FIELDS:
            return False

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT {permission_field} FROM pd_user_permissions 
                    WHERE user_id=%s
                """, (user_id,))
                row = cur.fetchone()

                if not row:
                    return False

                return bool(row.get(permission_field, 0))

    @staticmethod
    def list_all_permissions(page: int = 1, size: int = 20, role: str = None, keyword: str = None) -> Dict[str, Any]:
        """
        获取所有用户权限列表
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 构建查询条件
                where_conditions = ["u.status != %s"]
                params = [int(UserStatus.DELETED)]

                if role:
                    where_conditions.append("(p.role=%s OR (p.role IS NULL AND u.role=%s))")
                    params.extend([role, role])

                if keyword:
                    where_conditions.append("(u.name LIKE %s OR u.account LIKE %s)")
                    params.extend([f"%{keyword}%", f"%{keyword}%"])

                where_clause = " AND ".join(where_conditions)

                # 查询总数
                count_sql = f"""
                    SELECT COUNT(*) as total 
                    FROM pd_users u
                    LEFT JOIN pd_user_permissions p ON u.id=p.user_id
                    WHERE {where_clause}
                """
                cur.execute(count_sql, tuple(params))
                total = cur.fetchone()['total']

                # 查询列表
                offset = (page - 1) * size
                select_sql = f"""
                    SELECT 
                        u.id as user_id,
                        u.name,
                        u.account,
                        COALESCE(p.role, u.role) as role,
                        {','.join([f'p.{f}' for f in PermissionService.PERMISSION_FIELDS])}
                    FROM pd_users u
                    LEFT JOIN pd_user_permissions p ON u.id=p.user_id
                    WHERE {where_clause}
                    ORDER BY u.created_at DESC
                    LIMIT %s OFFSET %s
                """
                params.extend([size, offset])

                cur.execute(select_sql, tuple(params))
                rows = cur.fetchall()

                # 处理结果
                result_list = []
                for row in rows:
                    user_data = {
                        'user_id': row['user_id'],
                        'name': row['name'],
                        'account': row['account'],
                        'role': row['role'],
                    }

                    # 添加权限
                    for field in PermissionService.PERMISSION_FIELDS:
                        user_data[field] = bool(row.get(field, 0))

                    # 添加权限标签
                    user_data['permissions_list'] = [
                        {
                            'field': field,
                            'label': PermissionService.PERMISSION_LABELS.get(field, field),
                            'value': bool(row.get(field, 0))
                        }
                        for field in PermissionService.PERMISSION_FIELDS
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