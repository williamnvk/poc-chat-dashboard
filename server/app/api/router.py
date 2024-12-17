from fastapi import APIRouter
from app.api.endpoints import upload, query, tables, orders, products   
api_router = APIRouter()

api_router.include_router(upload.router, tags=["upload"])
api_router.include_router(query.router, tags=["query"])
api_router.include_router(tables.router, tags=["tables"])
api_router.include_router(orders.router, tags=["orders"]) 
api_router.include_router(products.router, tags=["products"])