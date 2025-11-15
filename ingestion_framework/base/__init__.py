"""
Base classes for ingestion framework
"""

try:
    from ingestion_framework.base.ingestion import BaseIngestion
    __all__ = ["BaseIngestion"]
except ImportError as e:
    import warnings
    warnings.warn(f"Failed to import BaseIngestion: {e}")
    __all__ = []