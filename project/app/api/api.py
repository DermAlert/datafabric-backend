# from api.routes import generate_ocr_multi_pages
from fastapi import APIRouter

from app.api.routes import (
    delta, token_routes, connection_routes, data_connection, metadata_viewer, data_visualization, equivalence, image_path_routes, dataset_routes, delta_sharing_management, delta_sharing_protocol, bronze_routes, relationship_routes, federation_routes, silver_routes, internal
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

# api_router.include_router(
#     dataset_routes.router, prefix="/datasets", tags=["Datasets"]
# )

# Bronze Layer (Smart Bronze Architecture)
api_router.include_router(
    bronze_routes.router, prefix="/bronze", tags=["Bronze Layer"]
)

# Silver Layer (Data Transformation)
api_router.include_router(
    silver_routes.router, tags=["Silver Layer"]
)

# Table Relationships (Metadata)
api_router.include_router(
    relationship_routes.router, prefix="/relationships", tags=["Table Relationships"]
)

# Federations (Cross-Database Relationship Groups)
api_router.include_router(
    federation_routes.router, prefix="/federations", tags=["Federations"]
)

# Delta Sharing Management APIs
api_router.include_router(
    delta_sharing_management.router, prefix="/delta-sharing", tags=["Delta Sharing Management"]
)

# Delta Sharing Protocol APIs (compatible with standard clients)
api_router.include_router(
    delta_sharing_protocol.router, prefix="/delta-sharing", tags=["Delta Sharing Protocol"]
)

# Internal APIs (for Airflow and other internal services)
api_router.include_router(
    internal.router, prefix="/internal", tags=["Internal"]
)

