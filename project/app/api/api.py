# from api.routes import generate_ocr_multi_pages
from fastapi import APIRouter

from app.api.routes import (
    delta, token_routes, connection_routes, data_connection, metadata_viewer, data_visualization, equivalence, image_path_routes, dataset_routes
)

api_router = APIRouter()

# teste delta spark
api_router.include_router(
    delta.router, prefix="/delta", tags=["Delta"]
)

api_router.include_router(
    token_routes.router, prefix="/token", tags=["Token Routes"]
)

api_router.include_router(
    connection_routes.router, prefix="/connection", tags=["Connection Routes"]
)

api_router.include_router(
    data_connection.router, prefix="/data-connections", tags=["Data Connection"]
)

api_router.include_router(
    metadata_viewer.router, prefix="/metadata", tags=["Metadata Viewer"]
)

api_router.include_router(
    image_path_routes.router, prefix="/image-paths", tags=["Image Path Management"]
)

api_router.include_router(
    equivalence.router, prefix="/equivalence", tags=["Equivalence"]
)

api_router.include_router(
    dataset_routes.router, prefix="/datasets", tags=["Datasets"]
)

