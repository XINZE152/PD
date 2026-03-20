from fastapi import HTTPException, APIRouter, Depends
from pydantic import BaseModel, Field
from typing import Optional, Dict
from datetime import datetime
from enum import IntEnum
from fastapi.security import HTTPBearer
from core.database import get_conn
from core.logging import get_logger
from core.table_access import build_dynamic_select
from core.auth import create_access_token, get_current_user
from pymysql.cursors import DictCursor  
from services.pd_auth_service import (
    AuthService, 
    UserStatus, 
    UserRole,
    verify_pwd,
    hash_pwd,
    PermissionService,
)

logger = get_logger(__name__)
security = HTTPBearer(auto_error=False)

# ========== Pydantic 模型定义 ==========

class LoginReq(BaseModel):
    account: str = Field(..., description="登录账号")
    password: str = Field(..., description="密码")


class LoginResp(BaseModel):
    uid: int
    token: str
    expires_in: int
    user: dict


class CreateUserReq(BaseModel):
    name: str = Field(..., description="用户姓名")
    account: str = Field(..., description="登录账号")
    password: str = Field(..., min_length=6, description="初始密码")
    role: str = Field(..., description="角色：管理员/大区经理/自营库管理/财务/会计/审核主管")
    phone: Optional[str] = Field(None, description="手机号")
    email: Optional[str] = Field(None, description="邮箱")


class UpdateUserReq(BaseModel):
    name: Optional[str] = Field(None, description="用户姓名")
    phone: Optional[str] = Field(None, description="手机号")
    email: Optional[str] = Field(None, description="邮箱")
    role: Optional[str] = Field(None, description="角色")


class UpdatePwdReq(BaseModel):
    old_password: str = Field(..., description="旧密码")
    new_password: str = Field(..., min_length=6, description="新密码")


class ResetPwdReq(BaseModel):
    admin_key: str = Field(..., description="后台管理密钥")
    new_password: str = Field(..., min_length=6, description="新密码")


class UserListQuery(BaseModel):
    page: int = Field(1, ge=1, description="页码")
    size: int = Field(20, ge=1, le=100, description="每页数量")
    role: Optional[str] = Field(None, description="按角色筛选")
    keyword: Optional[str] = Field(None, description="关键词搜索（姓名/账号）")


class UserResp(BaseModel):
    id: int
    name: str
    account: str
    role: str
    phone: Optional[str]
    email: Optional[str]
    status: int
    created_at: datetime
    updated_at: datetime


# ========== 新增：权限管理模型 ==========

class PermissionUpdateReq(BaseModel):
    role: Optional[str] = Field(None, description="角色：管理员/大区经理/自营库管理/财务/会计/审核主管")
    permissions: Optional[Dict[str, bool]] = Field(None,
                                                   description="权限字典，如 {'perm_schedule': true, 'perm_payout': false}")


class PermissionListQuery(BaseModel):
    page: int = Field(1, ge=1, description="页码")
    size: int = Field(20, ge=1, le=100, description="每页数量")
    role: Optional[str] = Field(None, description="按角色筛选")
    keyword: Optional[str] = Field(None, description="关键词搜索（姓名/账号）")


# ========== 路由定义 ==========

router = APIRouter(tags=["用户认证与权限"])


def register_pd_auth_routes(app):
    """注册用户认证路由到主应用"""
    app.include_router(
        router,
        prefix="/api/v1/user",
        dependencies=[Depends(security)]   # 添加这行
    )

    # 新增：确保权限表存在
    try:
        PermissionService.ensure_table_exists()
        logger.info("权限表初始化检查完成")
    except Exception as e:
        logger.warning(f"权限表初始化检查: {e}")


def _err(msg: str, code: int = 400):
    raise HTTPException(status_code=code, detail=msg)


def check_admin_permission(current_user: dict):
    """检查是否为管理员"""
    if current_user.get("role") != "管理员":
        raise HTTPException(status_code=403, detail="仅管理员可操作")


def check_manager_permission(current_user: dict):
    """检查是否为大区经理及以上权限"""
    if current_user.get("role") not in ["管理员", "大区经理"]:
        raise HTTPException(status_code=403, detail="权限不足")


# ========== 认证接口 ==========

@router.post("/auth/login", summary="用户登录", response_model=LoginResp)
def login(body: LoginReq):
    """
    用户登录接口
    - 验证账号密码
    - 检查用户状态（冻结/注销）
    - 返回 JWT Token
    """
    try:
        user = AuthService.authenticate(body.account, body.password)
        
        # 检查用户状态
        status = user.get("status", 0)
        if status == UserStatus.FROZEN:
            raise HTTPException(status_code=403, detail="账号已冻结，请联系管理员")
        if status == UserStatus.DELETED:
            raise HTTPException(status_code=403, detail="账号已注销")
        
        # 创建 Token
        token = create_access_token(
            user_id=user["id"],
            role=user["role"],
            token_type="pd_auth"
        )
        
        logger.info(f"用户登录成功: {user['account']} (ID: {user['id']})")
        
        return LoginResp(
            uid=user["id"],
            token=token,
            expires_in=3600 * 24,  # 24小时
            user={
                "id": user["id"],
                "name": user["name"],
                "account": user["account"],
                "role": user["role"]
            }
        )
        
    except ValueError as e:
        raise HTTPException(status_code=401, detail=str(e))
    except Exception as e:
        logger.exception("登录异常")
        raise HTTPException(status_code=500, detail="登录失败")


@router.post("/auth/logout", summary="用户登出")
def logout(current_user: dict = Depends(get_current_user)):
    """
    用户登出（前端清除token即可，后端可加入黑名单）
    """
    logger.info(f"用户登出: {current_user.get('account')}")
    return {"msg": "登出成功"}


@router.post("/auth/refresh", summary="刷新Token")
def refresh_token(current_user: dict = Depends(get_current_user)):
    """
    刷新访问令牌
    """
    new_token = create_access_token(
        user_id=current_user["id"],
        role=current_user.get("role"),
        token_type="pd_auth"
    )
    return {
        "token": new_token,
        "expires_in": 3600 * 24
    }


# ========== 当前用户接口 ==========

@router.get("/me", summary="获取当前用户信息", response_model=UserResp)
def get_me(current_user: dict = Depends(get_current_user)):
    """
    获取当前登录用户的详细信息
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            select_sql = build_dynamic_select(
                cur,
                "pd_users",
                where_clause="id=%s",
                select_fields=["id", "name", "account", "role", "phone", "email", "status", "created_at", "updated_at"]
            )
            cur.execute(select_sql, (current_user["id"],))
            user = cur.fetchone()
            
            if not user:
                raise HTTPException(status_code=404, detail="用户不存在")
            
            return UserResp(**user)


@router.put("/me", summary="更新当前用户信息")
def update_me(body: UpdateUserReq, current_user: dict = Depends(get_current_user)):
    """
    用户自主更新个人信息（不能修改角色）
    """
    # 不允许自主修改角色
    update_data = body.model_dump(exclude_none=True)
    update_data.pop("role", None)
    
    if not update_data:
        return {"msg": "无更新内容"}
    
    try:
        AuthService.update_user(current_user["id"], **update_data)
        return {"msg": "更新成功"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/me/password", summary="修改密码")
def change_password(body: UpdatePwdReq, current_user: dict = Depends(get_current_user)):
    """
    用户自主修改密码
    """
    try:
        AuthService.change_password(
            current_user["id"],
            body.old_password,
            body.new_password
        )
        return {"msg": "密码修改成功"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ========== 用户管理接口（管理员/大区经理） ==========

@router.post("/users", summary="创建用户")
def create_user(
    body: CreateUserReq,
    current_user: dict = Depends(get_current_user)
):
    """
    创建新用户
    - 管理员：可创建任何角色
    - 大区经理：只能创建自营库管理、财务、会计
    """
    # 权限检查
    current_role = current_user.get("role")
    
    if current_role == "大区经理":
        if body.role in ["管理员", "大区经理"]:
            raise HTTPException(status_code=403, detail="大区经理不能创建管理员或其他大区经理")
    elif current_role != "管理员":
        raise HTTPException(status_code=403, detail="无权创建用户")
    
    # 验证角色合法性
    if body.role not in UserRole.VALID_ROLES:
        raise HTTPException(status_code=400, detail=f"无效的角色，可选: {UserRole.VALID_ROLES}")
    
    try:
        user_id = AuthService.create_user(
            name=body.name,
            account=body.account,
            password=body.password,
            role=body.role,
            phone=body.phone,
            email=body.email,
            created_by=current_user["id"]
        )

        # 新增：自动创建默认权限
        try:
            PermissionService.create_default_permissions(user_id, body.role)
        except Exception as e:
            logger.warning(f"创建默认权限失败: {e}")

        return {"msg": "创建成功", "user_id": user_id}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/managers", summary="查看所有大区经理")
def list_managers(
    current_user: dict = Depends(get_current_user)
):
    """
    獲取所有大區經理列表（正常狀態），用於下拉選項等場景
    """
    if current_user.get("role") not in ["管理员", "大区经理"]:
        raise HTTPException(status_code=403, detail="无权查看大区经理列表")
    managers = AuthService.list_managers()
    return {"list": managers}


@router.get("/users", summary="用户列表")
def list_users(
    page: int = 1,
    size: int = 20,
    role: Optional[str] = None,
    keyword: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    """
    获取用户列表（支持分页、筛选）
    """
    # 权限检查
    if current_user.get("role") not in ["管理员", "大区经理"]:
        raise HTTPException(status_code=403, detail="无权查看用户列表")
    
    result = AuthService.list_users(
        page=page,
        size=size,
        role=role,
        keyword=keyword
    )
    return result


@router.get("/users/{user_id}", summary="用户详情")
def get_user(
    user_id: int,
    current_user: dict = Depends(get_current_user)
):
    """
    获取指定用户详情
    """
    user = AuthService.get_user_by_id(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="用户不存在")
    return user


@router.put("/users/{user_id}", summary="更新用户信息")
def update_user(
    user_id: int,
    body: UpdateUserReq,
    current_user: dict = Depends(get_current_user)
):
    """
    更新指定用户信息
    """
    # 权限检查：只能管理下级角色
    target_user = AuthService.get_user_by_id(user_id)
    if not target_user:
        raise HTTPException(status_code=404, detail="用户不存在")
    
    current_role = current_user.get("role")
    target_role = target_user.get("role")
    
    # 管理员可以修改任何人
    if current_role != "管理员":
        # 大区经理只能修改比自己低级的
        if current_role == "大区经理":
            if target_role in ["管理员", "大区经理"]:
                raise HTTPException(status_code=403, detail="无权修改该用户")
        else:
            raise HTTPException(status_code=403, detail="无权修改用户")
    
    # 不能修改自己的角色
    if user_id == current_user["id"] and body.role:
        raise HTTPException(status_code=400, detail="不能修改自己的角色")
    
    try:
        AuthService.update_user(user_id, **body.model_dump(exclude_none=True))

        # 新增：如果修改了角色，同步更新权限表
        if body.role:
            try:
                PermissionService.update_permissions(user_id, role=body.role)
                # 重置为角色默认模板
                PermissionService.delete_permissions(user_id)
                PermissionService.create_default_permissions(user_id, body.role)
            except Exception as e:
                logger.warning(f"同步更新权限失败: {e}")

        return {"msg": "更新成功"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/users/{user_id}", summary="删除用户")
def delete_user(
    user_id: int,
    current_user: dict = Depends(get_current_user)
):
    """
    删除用户（软删除，设置状态为已注销）
    """
    # 不能删除自己
    if user_id == current_user["id"]:
        raise HTTPException(status_code=400, detail="不能删除自己")
    
    check_admin_permission(current_user)
    
    try:
        # 新增：先删除权限记录
        try:
            PermissionService.delete_permissions(user_id)
        except Exception as e:
            logger.warning(f"删除权限记录失败: {e}")

        AuthService.delete_user(user_id)
        return {"msg": "用户已删除"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/users/{user_id}/reset-password", summary="重置密码")
def admin_reset_password(
    user_id: int,
    body: ResetPwdReq,
    current_user: dict = Depends(get_current_user)
):
    """
    管理员重置用户密码
    """
    # 权限检查
    if current_user.get("role") not in ["管理员", "大区经理"]:
        raise HTTPException(status_code=403, detail="无权重置密码")
    
    # 密钥验证（双重验证）
    if body.admin_key != "pd_admin_2025":
        raise HTTPException(status_code=403, detail="管理密钥错误")
    
    try:
        AuthService.admin_reset_password(user_id, body.new_password)
        return {"msg": "密码已重置"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/users/{user_id}/freeze", summary="冻结用户")
def freeze_user(
    user_id: int,
    current_user: dict = Depends(get_current_user)
):
    """
    冻结用户账号
    """
    # 不能冻结自己
    if user_id == current_user["id"]:
        raise HTTPException(status_code=400, detail="不能冻结自己")
    
    check_manager_permission(current_user)
    
    try:
        AuthService.set_user_status(user_id, UserStatus.FROZEN)
        return {"msg": "用户已冻结"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/users/{user_id}/unfreeze", summary="解冻用户")
def unfreeze_user(
    user_id: int,
    current_user: dict = Depends(get_current_user)
):
    """
    解冻用户账号
    """
    check_manager_permission(current_user)
    
    try:
        AuthService.set_user_status(user_id, UserStatus.NORMAL)
        return {"msg": "用户已解冻"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ========== 角色相关接口 ==========

@router.get("/roles", summary="获取角色列表")
def get_roles():
    """
    获取系统预定义的角色列表
    """
    return {
        "roles": [
            {"code": "管理员", "name": "管理员", "description": "系统管理员，拥有所有权限"},
            {"code": "大区经理", "name": "大区经理", "description": "可管理下级用户和数据"},
            {"code": "自营库管理", "name": "自营库管理", "description": "管理库存和物流"},
            {"code": "财务", "name": "财务", "description": "处理财务相关操作"},
            {"code": "会计", "name": "会计", "description": "查看财务数据"},
            {"code": "审核主管", "name": "审核主管", "description": "负责报单审核与发运相关核查"}
        ]
    }


# ========== 新增：权限管理接口 ==========

@router.get("/permissions", summary="获取所有用户权限列表")
def list_permissions(
        page: int = 1,
        size: int = 20,
        role: Optional[str] = None,
        keyword: Optional[str] = None,
        current_user: dict = Depends(get_current_user)
):
    """
    获取所有用户的权限列表（需要权限管理权限或管理员）
    """
    # 权限检查
    if current_user.get("role") != "管理员":
        if not PermissionService.check_permission(current_user["id"], "perm_permission_manage"):
            raise HTTPException(status_code=403, detail="无权限查看权限列表")

    result = PermissionService.list_all_permissions(
        page=page,
        size=size,
        role=role,
        keyword=keyword
    )
    return result


@router.get("/permissions/me", summary="获取当前用户权限")
def get_my_permissions(current_user: dict = Depends(get_current_user)):
    """
    获取当前登录用户的权限详情
    """
    result = PermissionService.get_user_permissions(current_user["id"])
    if not result:
        raise HTTPException(status_code=404, detail="用户不存在")

    return {
        "success": True,
        "data": result
    }


@router.get("/permissions/{user_id}", summary="获取指定用户权限")
def get_user_permission(
        user_id: int,
        current_user: dict = Depends(get_current_user)
):
    """
    获取指定用户的权限详情
    - 管理员可查看任何人
    - 其他人只能查看自己
    """
    # 权限检查
    if current_user.get("role") != "管理员" and current_user["id"] != user_id:
        raise HTTPException(status_code=403, detail="只能查看自己的权限")

    result = PermissionService.get_user_permissions(user_id)
    if not result:
        raise HTTPException(status_code=404, detail="用户不存在")

    return {
        "success": True,
        "data": result
    }


@router.put("/permissions/{user_id}", summary="修改用户权限和角色")
def update_user_permission(
        user_id: int,
        body: PermissionUpdateReq,
        current_user: dict = Depends(get_current_user)
):
    """
    修改指定用户的权限和角色

    **权限要求：**
    - 需要 `perm_permission_manage` 权限或管理员角色
    - 不能修改自己的角色（需由其他管理员修改）
    """
    # 权限检查
    if current_user.get("role") != "管理员":
        if not PermissionService.check_permission(current_user["id"], "perm_permission_manage"):
            raise HTTPException(status_code=403, detail="无权限修改用户权限")

    # 不能修改自己的角色
    if user_id == current_user["id"] and body.role:
        raise HTTPException(status_code=400, detail="不能修改自己的角色，请联系其他管理员")

    try:
        PermissionService.update_permissions(
            user_id=user_id,
            role=body.role,
            permissions=body.permissions
        )

        # 如果修改了角色，同步更新pd_users表
        if body.role:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE pd_users SET role=%s WHERE id=%s",
                        (body.role, user_id)
                    )
                    conn.commit()

        return {
            "success": True,
            "message": "权限更新成功"
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("更新权限失败")
        raise HTTPException(status_code=500, detail=f"更新失败: {str(e)}")


@router.get("/permissions/roles/templates", summary="获取角色权限模板")
def get_role_templates(current_user: dict = Depends(get_current_user)):
    """
    获取各角色的默认权限模板（从数据库读取）
    """
    # 权限检查
    if current_user.get("role") != "管理员":
        if not PermissionService.check_permission(current_user["id"], "perm_permission_manage"):
            raise HTTPException(status_code=403, detail="无权限查看")

    # 从数据库获取模板
    templates_data = PermissionService.get_all_role_templates()
    permission_fields = PermissionService.get_all_fields()

    templates = {}
    for role, perms in templates_data.items():
        templates[role] = {
            'role': role,
            'permissions': [
                {
                    'field': field,
                    'label': PermissionService.get_label(field),
                    'value': bool(perms.get(field, 0))
                }
                for field in PermissionService.get_all_fields()
            ]
        }

    return {
        "success": True,
        "data": templates,
        "valid_roles": PermissionService.VALID_ROLES,
        "permission_fields": [
            {
                'field': field,
                'label': PermissionService.get_label(field)
            }
            for field in PermissionService.get_all_fields()
        ]
    }


class UpdateRoleTemplateReq(BaseModel):
    permissions: Dict[str, bool]
    apply_to_existing: bool = False  # 新增参数，默认为 False

@router.put("/permissions/roles/{role}/template", summary="修改角色权限模板")
def update_role_template(
    role: str,
    body: UpdateRoleTemplateReq,
    current_user: dict = Depends(get_current_user)
):
    """
    修改角色权限模板（持久化到数据库）
    - apply_to_existing: 是否将更新后的模板应用到现有用户（覆盖用户权限）
    """
    # 权限检查
    if current_user.get("role") != "管理员":
        raise HTTPException(status_code=403, detail="仅管理员可修改角色模板")

    if role not in PermissionService.VALID_ROLES:
        raise HTTPException(status_code=400, detail=f"无效的角色，可选: {PermissionService.VALID_ROLES}")

    # 管理员角色必须拥有所有权限，不可修改
    if role == "管理员":
        raise HTTPException(status_code=400, detail="管理员角色必须拥有所有权限，不可修改")

    # 验证权限字段
    invalid_perms = [p for p in body.permissions.keys() if p not in PermissionService.get_all_fields()]
    if invalid_perms:
        raise HTTPException(status_code=400, detail=f"无效的权限字段: {invalid_perms}")

    try:
        PermissionService.update_role_template(role, body.permissions, apply_to_existing=body.apply_to_existing)
        return {
            "success": True,
            "message": f"【{role}】角色权限模板更新成功" +
                      ("，并已应用到现有用户" if body.apply_to_existing else "")
        }
    except Exception as e:
        logger.exception("更新角色模板失败")
        raise HTTPException(status_code=500, detail=f"更新失败: {str(e)}")


@router.post("/permissions/{user_id}/reset", summary="重置用户权限为角色模板")
def reset_user_permissions(
        user_id: int,
        current_user: dict = Depends(get_current_user)
):
    """
    重置用户权限为角色默认模板
    """
    # 权限检查
    if current_user.get("role") != "管理员":
        if not PermissionService.check_permission(current_user["id"], "perm_permission_manage"):
            raise HTTPException(status_code=403, detail="无权限重置权限")

    # 不能重置自己
    if user_id == current_user["id"]:
        raise HTTPException(status_code=400, detail="不能重置自己的权限")

    with get_conn() as conn:
        with conn.cursor() as cur:
            # 获取当前角色
            cur.execute("SELECT role FROM pd_user_permissions WHERE user_id=%s", (user_id,))
            row = cur.fetchone()

            if not row:
                raise HTTPException(status_code=404, detail="用户权限记录不存在")

            role = row['role']

            # 删除旧权限
            cur.execute("DELETE FROM pd_user_permissions WHERE user_id=%s", (user_id,))

            # 重新创建默认权限
            PermissionService.create_default_permissions(user_id, role)

            conn.commit()

    return {
        "success": True,
        "message": f"权限已重置为【{role}】角色默认模板"
    }
# ========== 权限定义管理接口 ==========

class AddPermissionDefReq(BaseModel):
    field_name: str = Field(..., description="权限字段名，如 perm_new_feature", pattern="^perm_[a-z][a-z0-9_]*$")
    label: str = Field(..., description="权限显示名称", min_length=1, max_length=64)


@router.post("/permission/definitions", summary="新增权限字段定义")
def add_permission_definition(
    body: AddPermissionDefReq,
    current_user: dict = Depends(get_current_user)
):
    """
    动态添加一个新的权限字段
    - 需要权限管理权限或管理员
    - 会执行 ALTER TABLE 修改数据库结构
    - 自动更新所有角色模板
    """
    # 权限检查
    if current_user.get("role") != "管理员":
        if not PermissionService.check_permission(current_user["id"], "perm_permission_manage"):
            raise HTTPException(status_code=403, detail="无权限管理权限定义")

    try:
        PermissionService.add_permission_definition(body.field_name, body.label)
        return {"success": True, "message": f"权限字段 {body.field_name} 添加成功"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("添加权限字段失败")
        raise HTTPException(status_code=500, detail=f"添加失败: {str(e)}")


@router.delete("/permission/definitions/{field_name}", summary="删除权限字段定义")
def delete_permission_definition(
    field_name: str,
    current_user: dict = Depends(get_current_user)
):
    """
    删除一个权限字段
    - 需要权限管理权限或管理员
    - 会执行 ALTER TABLE 删除列，并从所有角色模板中移除
    """
    # 权限检查
    if current_user.get("role") != "管理员":
        if not PermissionService.check_permission(current_user["id"], "perm_permission_manage"):
            raise HTTPException(status_code=403, detail="无权限管理权限定义")

    try:
        PermissionService.remove_permission_definition(field_name)
        return {"success": True, "message": f"权限字段 {field_name} 删除成功"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("删除权限字段失败")
        raise HTTPException(status_code=500, detail=f"删除失败: {str(e)}")


@router.get("/permission/definitions", summary="获取所有权限字段定义")
def list_permission_definitions(current_user: dict = Depends(get_current_user)):
    """
    获取当前系统中所有可用的权限字段定义（需要权限管理权限或管理员）
    """
    # 权限检查（可选，也可开放给所有登录用户）
    if current_user.get("role") != "管理员":
        if not PermissionService.check_permission(current_user["id"], "perm_permission_manage"):
            raise HTTPException(status_code=403, detail="无权限查看权限定义")

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT field_name, label, created_at FROM pd_permission_definitions ORDER BY field_name")
            rows = cur.fetchall()
            return {
                "success": True,
                "data": rows,
                "total": len(rows)
            }
    
# ========== 库房管理接口 ==========

class WarehouseCreateReq(BaseModel):
    warehouse_name: str = Field(..., description="库房名称")
    public_account: Optional[str] = Field(None, description="对公账号")

class WarehouseUpdateReq(BaseModel):
    warehouse_name: Optional[str] = Field(None, description="库房名称")
    public_account: Optional[str] = Field(None, description="对公账号")
    is_active: Optional[int] = Field(None, description="是否启用：1=启用，0=停用")

@router.post("/warehouses", summary="创建库房")
def create_warehouse(
    body: WarehouseCreateReq,
    current_user: dict = Depends(get_current_user)
):
    """创建新库房"""
    check_manager_permission(current_user)
    
    with get_conn() as conn:
        with conn.cursor() as cur:
            # 检查名称是否已存在
            cur.execute("SELECT 1 FROM pd_warehouses WHERE warehouse_name=%s", (body.warehouse_name,))
            if cur.fetchone():
                raise HTTPException(status_code=400, detail="库房名称已存在")
            
            cur.execute("""
                INSERT INTO pd_warehouses (warehouse_name, public_account) 
                VALUES (%s, %s)
            """, (body.warehouse_name, body.public_account))
            conn.commit()
            warehouse_id = cur.lastrowid
    
    return {"success": True, "message": "库房创建成功", "warehouse_id": warehouse_id}

@router.get("/warehouses", summary="库房列表")
def list_warehouses(
    keyword: Optional[str] = None,
    is_active: Optional[int] = None,
    current_user: dict = Depends(get_current_user)
):
    """获取库房列表"""
    with get_conn() as conn:
        with conn.cursor(DictCursor) as cur:  # 使用 DictCursor
            where = ["1=1"]
            params = []
            if keyword:
                where.append("w.warehouse_name LIKE %s")
                params.append(f"%{keyword}%")
            if is_active is not None:
                where.append("w.is_active = %s")
                params.append(is_active)
            
            cur.execute(f"""
                SELECT w.*, COUNT(p.id) as payee_count
                FROM pd_warehouses w
                LEFT JOIN pd_payees p ON w.id = p.warehouse_id AND p.is_active = 1
                WHERE {' AND '.join(where)}
                GROUP BY w.id
                ORDER BY w.created_at DESC
            """, tuple(params))
            
            # DictCursor 直接返回字典列表
            data = cur.fetchall()
            return {"success": True, "data": data}

@router.get("/warehouses/{warehouse_id}", summary="库房详情")
def get_warehouse(
    warehouse_id: int,
    current_user: dict = Depends(get_current_user)
):
    """获取库房详情（包含收款人列表）"""
    with get_conn() as conn:
        with conn.cursor(DictCursor) as cur:  # 使用 DictCursor
            cur.execute("SELECT * FROM pd_warehouses WHERE id=%s", (warehouse_id,))
            warehouse = cur.fetchone()
            if not warehouse:
                raise HTTPException(status_code=404, detail="库房不存在")
            
            # 获取该库房的收款人
            cur.execute("""
                SELECT id, payee_name, payee_account, payee_bank_name, is_active
                FROM pd_payees WHERE warehouse_id=%s
            """, (warehouse_id,))
            warehouse['payees'] = cur.fetchall()
            
            return {"success": True, "data": warehouse}

@router.put("/warehouses/{warehouse_id}", summary="更新库房")
def update_warehouse(
    warehouse_id: int,
    body: WarehouseUpdateReq,
    current_user: dict = Depends(get_current_user)
):
    """更新库房信息"""
    check_manager_permission(current_user)
    
    updates = []
    params = []
    if body.warehouse_name is not None:
        updates.append("warehouse_name=%s")
        params.append(body.warehouse_name)
    if body.public_account is not None:
        updates.append("public_account=%s")
        params.append(body.public_account)
    if body.is_active is not None:
        updates.append("is_active=%s")
        params.append(body.is_active)
    
    if not updates:
        return {"success": True, "message": "无更新内容"}
    
    params.append(warehouse_id)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE pd_warehouses SET {', '.join(updates)} WHERE id=%s
            """, tuple(params))
            conn.commit()
    
    return {"success": True, "message": "库房更新成功"}

@router.delete("/warehouses/{warehouse_id}", summary="删除库房")
def delete_warehouse(
    warehouse_id: int,
    current_user: dict = Depends(get_current_user)
):
    """删除库房（会级联删除收款人）"""
    check_admin_permission(current_user)
    
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM pd_warehouses WHERE id=%s", (warehouse_id,))
            conn.commit()
    
    return {"success": True, "message": "库房删除成功"}


# ========== 收款人管理接口 ==========

class PayeeCreateReq(BaseModel):
    warehouse_id: Optional[int] = Field(None, description="所属库房ID（可选）")
    payee_name: str = Field(..., description="收款人姓名")
    payee_account: str = Field(..., description="收款账号")
    payee_bank_name: Optional[str] = Field(None, description="收款银行名称")

class PayeeUpdateReq(BaseModel):
    warehouse_id: Optional[int] = Field(None, description="所属库房ID")
    payee_name: Optional[str] = Field(None, description="收款人姓名")
    payee_account: Optional[str] = Field(None, description="收款账号")
    payee_bank_name: Optional[str] = Field(None, description="收款银行名称")
    is_active: Optional[int] = Field(None, description="是否启用：1=启用，0=停用")

@router.post("/payees", summary="创建收款人")
def create_payee(
    body: PayeeCreateReq,
    current_user: dict = Depends(get_current_user)
):
    """创建新收款人"""
    check_manager_permission(current_user)
    
    with get_conn() as conn:
        with conn.cursor(DictCursor) as cur:
            # 验证库房是否存在
            if body.warehouse_id is not None:
                cur.execute("SELECT 1 FROM pd_warehouses WHERE id=%s", (body.warehouse_id,))
                if not cur.fetchone():
                    raise HTTPException(status_code=400, detail="所属库房不存在")
            
            cur.execute("""
                INSERT INTO pd_payees (warehouse_id, payee_name, payee_account, payee_bank_name) 
                VALUES (%s, %s, %s, %s)
            """, (body.warehouse_id, body.payee_name, body.payee_account, body.payee_bank_name))
            conn.commit()
            payee_id = cur.lastrowid
    
    return {"success": True, "message": "收款人创建成功", "payee_id": payee_id}

@router.get("/payees", summary="收款人列表")
def list_payees(
    warehouse_id: Optional[int] = None,
    keyword: Optional[str] = None,
    is_active: Optional[int] = None,
    current_user: dict = Depends(get_current_user)
):
    with get_conn() as conn:
        with conn.cursor(DictCursor) as cur:
            where = ["1=1"]
            params = []
            if warehouse_id:
                where.append("p.warehouse_id = %s")
                params.append(warehouse_id)
            if keyword:
                where.append("(p.payee_name LIKE %s OR p.payee_account LIKE %s)")
                params.extend([f"%{keyword}%", f"%{keyword}%"])
            if is_active is not None:
                where.append("p.is_active = %s")
                params.append(is_active)

            cur.execute(f"""
                SELECT p.*, w.warehouse_name
                FROM pd_payees p
                LEFT JOIN pd_warehouses w ON p.warehouse_id = w.id
                WHERE {' AND '.join(where)}
                ORDER BY w.warehouse_name, p.payee_name
            """, tuple(params))

            rows = cur.fetchall()
            return {"success": True, "data": rows}

@router.get("/payees/{payee_id}", summary="收款人详情")
def get_payee(
    payee_id: int,
    current_user: dict = Depends(get_current_user)
):
    with get_conn() as conn:
        with conn.cursor(DictCursor) as cur:
            cur.execute("""
                SELECT p.*, w.warehouse_name 
                FROM pd_payees p
                LEFT JOIN pd_warehouses w ON p.warehouse_id = w.id
                WHERE p.id=%s
            """, (payee_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="收款人不存在")
            return {"success": True, "data": row}

@router.put("/payees/{payee_id}", summary="更新收款人")
def update_payee(
    payee_id: int,
    body: PayeeUpdateReq,
    current_user: dict = Depends(get_current_user)
):
    """更新收款人信息"""
    check_manager_permission(current_user)
    
    updates = []
    params = []
    if body.warehouse_id is not None:
        # 验证新库房是否存在
        with get_conn() as conn:
            with conn.cursor() as cur:
                if body.warehouse_id is not None:
                    cur.execute("SELECT 1 FROM pd_warehouses WHERE id=%s", (body.warehouse_id,))
                    if not cur.fetchone():
                        raise HTTPException(status_code=400, detail="所属库房不存在")
        updates.append("warehouse_id=%s")
        params.append(body.warehouse_id)
    if body.payee_name is not None:
        updates.append("payee_name=%s")
        params.append(body.payee_name)
    if body.payee_account is not None:
        updates.append("payee_account=%s")
        params.append(body.payee_account)
    if body.payee_bank_name is not None:
        updates.append("payee_bank_name=%s")
        params.append(body.payee_bank_name)
    if body.is_active is not None:
        updates.append("is_active=%s")
        params.append(body.is_active)
    
    if not updates:
        return {"success": True, "message": "无更新内容"}
    
    params.append(payee_id)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE pd_payees SET {', '.join(updates)} WHERE id=%s
            """, tuple(params))
            conn.commit()
    
    return {"success": True, "message": "收款人更新成功"}

@router.delete("/payees/{payee_id}", summary="删除收款人")
def delete_payee(
    payee_id: int,
    current_user: dict = Depends(get_current_user)
):
    """删除收款人"""
    check_manager_permission(current_user)
    
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM pd_payees WHERE id=%s", (payee_id,))
            conn.commit()
    
    return {"success": True, "message": "收款人删除成功"} 