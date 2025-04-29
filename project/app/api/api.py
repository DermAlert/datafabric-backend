# from api.routes import generate_ocr_multi_pages
from fastapi import APIRouter

from app.api.routes import token_routes

api_router = APIRouter()

api_router.include_router(
    token_routes.router, prefix="/token", tags=["Token Routes"]
)