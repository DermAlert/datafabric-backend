"""
Dataset-related database models.
"""

from .bronze import (
    RelationshipType,
    JoinStrategy,
    IngestionStatus,
    SourceRelationship,
    DatasetBronzeConfig,
    DatasetIngestionGroup,
    IngestionGroupTable,
    InterSourceLink,
    BronzeColumnMapping,
    # Bronze Config Architecture
    BronzeExecutionStatus,
    BronzeVirtualizedConfig,
    BronzePersistentConfig,
    BronzeExecution,
)

from .silver import (
    NormalizationType,
    TransformStatus,
    NormalizationRule,
    VirtualizedConfig,
    TransformConfig,
    TransformExecution,
)

__all__ = [
    # Bronze
    "RelationshipType",
    "JoinStrategy",
    "IngestionStatus",
    "SourceRelationship",
    "DatasetBronzeConfig",
    "DatasetIngestionGroup",
    "IngestionGroupTable",
    "InterSourceLink",
    "BronzeColumnMapping",
    # Bronze Config Architecture
    "BronzeExecutionStatus",
    "BronzeVirtualizedConfig",
    "BronzePersistentConfig",
    "BronzeExecution",
    # Silver
    "NormalizationType",
    "TransformStatus",
    "NormalizationRule",
    "VirtualizedConfig",
    "TransformConfig",
    "TransformExecution",
]
