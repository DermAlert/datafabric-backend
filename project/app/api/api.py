# from api.routes import generate_ocr_multi_pages
from fastapi import APIRouter

from app.api.routes import (
    token_routes, connection_routes, connections, metadata_viewer, data_visualization, equivalence
)

api_router = APIRouter()

api_router.include_router(
    token_routes.router, prefix="/token", tags=["Token Routes"]
)

api_router.include_router(
    connection_routes.router, prefix="/connection", tags=["Connection Routes"]
)

api_router.include_router(
    connections.router, prefix="/connections", tags=["Connections"]
)

api_router.include_router(
    metadata_viewer.router, prefix="/metadata", tags=["Metadata Viewer"]
)

api_router.include_router(
    data_visualization.router, prefix="/visualize-data", tags=["Data Visualization"]
)

api_router.include_router(
    equivalence.router, prefix="/equivalence", tags=["Equivalence"]
)

