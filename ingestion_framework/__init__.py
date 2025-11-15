"""
Ingestion framework for ETL pipeline
Handles data extraction, transformation, and loading
"""

__version__ = "1.0.0"

def __getattr__(name):
    if name == "BaseIngestion":
        from ingestion_framework.base.ingestion import BaseIngestion
        return BaseIngestion
    elif name == "settings":
        from ingestion_framework.core.config import settings
        return settings
    elif name == "get_logger":
        from ingestion_framework.utils.logger import get_logger
        return get_logger
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")

__all__ = ["BaseIngestion", "settings", "get_logger"]