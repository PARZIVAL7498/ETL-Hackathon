"""
Core configuration module
"""

__version__ = "1.0.0"

def __getattr__(name):
    if name == "settings":
        from ingestion_framework.core.config import settings
        return settings
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")

__all__ = ["settings"]