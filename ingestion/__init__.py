"""
Ingestion module for ETL pipeline
"""
__version__ = "1.0.0"

def __getattr__(name):
    if name == "BaseIngestion":
        from ingestion.base import BaseIngestion
        return BaseIngestion
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")

__all__ = ["BaseIngestion"]