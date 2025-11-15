"""
Schema registry for data validation
"""

from ingestion_framework.schema_registry.schemas import get_schema, validate_schema

__all__ = ["get_schema", "validate_schema"]