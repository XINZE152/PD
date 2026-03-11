from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import jwt
from fastapi import Header, HTTPException, status

from app.core.config import settings
from core.database import get_conn


def create_access_token(
    user_id: int,
    role: str,
    token_type: str = "pd_auth",
    expires_in_seconds: int = 3600 * 24,
) -> str:
    now = datetime.now(timezone.utc)
    payload: Dict[str, Any] = {
        "uid": user_id,
        "role": role,
        "typ": token_type,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(seconds=expires_in_seconds)).timestamp()),
    }
    return jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)


def _decode_token(token: str) -> Dict[str, Any]:
    try:
        return jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_algorithm])
    except jwt.ExpiredSignatureError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired") from exc
    except jwt.InvalidTokenError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token") from exc


def get_current_user(authorization: Optional[str] = Header(None)) -> Dict[str, Any]:
    import logging
    logger = logging.getLogger("core.auth")
    try:
        if not authorization or not authorization.startswith("Bearer "):
            logger.warning("auth failed: Missing token, Authorization header=%s", authorization)
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing token")

        token = authorization.split(" ", 1)[1].strip()
        payload = _decode_token(token)

        user_id = payload.get("uid") or payload.get("sub")
        if not user_id:
            logger.warning("auth failed: Invalid token payload, Authorization header=%s", authorization)
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token payload")

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, name, account, role, status FROM pd_users WHERE id = %s",
                    (user_id,),
                )
                user = cur.fetchone()
                if not user or user.get("status") == 2:
                    logger.warning("auth failed: User not found, user_id=%s, Authorization header=%s", user_id, authorization)
                    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")

                return {
                    "id": user["id"],
                    "name": user.get("name"),
                    "account": user.get("account"),
                    "role": user.get("role"),
                }
    except HTTPException as e:
        logger.warning("auth failed: %s, Authorization header=%s", e.detail, authorization)
        raise


def get_user_identity_from_authorization(authorization: Optional[str]) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        return "-"

    token = authorization.split(" ", 1)[1].strip()
    try:
        payload = _decode_token(token)
    except HTTPException:
        return "-"

    user_id = payload.get("uid") or payload.get("sub")
    role = payload.get("role")
    if user_id and role:
        return f"uid={user_id} role={role}"
    if user_id:
        return f"uid={user_id}"
    return "-"
