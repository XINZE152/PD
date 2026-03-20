from dotenv import load_dotenv
import os

import time
from fastapi import FastAPI, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer
from contextlib import asynccontextmanager
from pathlib import Path
import sys
import uvicorn

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
# 确保能导入 database_setup
sys.path.append(str(Path(__file__).parent))
from database_setup import create_tables
from app.api.v1.api import api_router
from app.core.config import settings
from app.api.v1.user.routes import register_pd_auth_routes
from app.core.logging import get_logger, reset_log_user, set_log_user, setup_logging
from core.auth import get_user_identity_from_authorization
from app.services.contract_service import expire_contracts_after_grace

load_dotenv()
@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理 - 启动时初始化数据库"""
    setup_logging()
    logger = get_logger("app.lifespan")
    print("正在检查数据库初始化...")
    try:
        create_tables()
        print("数据库初始化完成")
    except Exception as e:
        print(f"数据库初始化失败: {e}")
        logger.exception("database init failed")
    expired_count = expire_contracts_after_grace()
    logger.info("contract expire sync finished updated=%s", expired_count)
    scheduler = BackgroundScheduler(timezone="Asia/Shanghai")
    scheduler.add_job(
        func=expire_contracts_after_grace,
        trigger=CronTrigger(hour=0, minute=10),
        kwargs={"grace_days": 4},
        id="expire_contracts",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("contract expire scheduler started")
    yield
    scheduler.shutdown(wait=False)
    print("应用关闭")


# ========== 只添加这行 ==========
security = HTTPBearer(auto_error=False)


app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    lifespan=lifespan
)

cors_origins = [origin.strip() for origin in os.getenv("CORS_ALLOW_ORIGINS", "*").split(",") if origin.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins or ["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ========== 只添加 dependencies ==========
app.include_router(api_router, prefix="/api/v1", dependencies=[Depends(security)])
register_pd_auth_routes(app)
logger = get_logger("app")


@app.middleware("http")
async def request_logger(request: Request, call_next):
    start_time = time.perf_counter()
    identity = get_user_identity_from_authorization(request.headers.get("Authorization"))
    token = set_log_user(identity)
    try:
        response = await call_next(request)
    except Exception:
        logger.exception("request failed method=%s path=%s", request.method, request.url.path)
        return JSONResponse(status_code=500, content={"detail": "Internal Server Error"})
    finally:
        reset_log_user(token)

    duration_ms = (time.perf_counter() - start_time) * 1000
    logger.info(
        "request method=%s path=%s status=%s duration_ms=%.2f",
        request.method,
        request.url.path,
        response.status_code,
        duration_ms,
    )

    if request.method in {"POST", "PUT", "DELETE"}:
        logger.info(
            "audit method=%s path=%s status=%s duration_ms=%.2f",
            request.method,
            request.url.path,
            response.status_code,
            duration_ms,
        )

    return response


@app.get("/healthz")
def health_check() -> dict:
    return {"status": "ok"}


@app.get("/init-db")
def manual_init_db():
    """手动触发数据库初始化（调试用）"""
    try:
        create_tables()
        return {"success": True, "message": "数据库初始化完成"}
    except Exception as e:
        return {"success": False, "error": str(e)}


if __name__ == "__main__":
    load_dotenv()
    port = int(os.getenv("PORT", "8007"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)