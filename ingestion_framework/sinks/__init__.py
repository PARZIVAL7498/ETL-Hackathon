"""
Data sink modules for storing ingested data
"""

def __getattr__(name):
    if name == "RawLayer":
        from ingestion_framework.sinks.raw_layer import RawLayer
        return RawLayer
    elif name == "CloudStorage":
        from ingestion_framework.sinks.cloud_storage import CloudStorage
        return CloudStorage
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")

__all__ = ["RawLayer", "CloudStorage"]