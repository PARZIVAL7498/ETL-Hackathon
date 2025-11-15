"""
Ingestion Service Package - Handles data classification and enrichment
"""

__version__ = "1.0.0"
__author__ = "ETL Team"

try:
    from ingestion_service.app.services.classification_service import ClassificationService
    __all__ = ["ClassificationService"]
except ImportError as e:
    import warnings
    warnings.warn(f"Failed to import ClassificationService: {e}")
    __all__ = []