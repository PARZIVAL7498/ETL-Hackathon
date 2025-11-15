import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

"""
Data source ingestion modules
"""

__version__ = "1.0.0"

def __getattr__(name):
    if name == "APIIngestion":
        from ingestion.sources.api import APIIngestion
        return APIIngestion
    elif name == "DatabaseIngestion":
        from ingestion.sources.database import DatabaseIngestion
        return DatabaseIngestion
    elif name == "FileSystemIngestion":
        from ingestion.sources.filesystem import FileSystemIngestion
        return FileSystemIngestion
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")

__all__ = ["APIIngestion", "DatabaseIngestion", "FileSystemIngestion"]