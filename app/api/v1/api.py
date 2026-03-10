from fastapi import APIRouter

from app.api.v1.routes import balances, contracts, customers, deliveries, weighbills, payment, product_categories

api_router = APIRouter()
api_router.include_router(contracts.router, tags=["合同管理"])
api_router.include_router(customers.router, tags=["客户管理"])
api_router.include_router(deliveries.router, tags=["销售台账/报货订单"])
api_router.include_router(weighbills.router, tags=["磅单管理"])
api_router.include_router(balances.router, tags=["磅单结余管理"])
api_router.include_router(payment.router, tags=["PD收款明细管理"])
api_router.include_router(product_categories.router, tags=["品类管理"])