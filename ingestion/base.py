"""
Base classes for ingestion
"""

from abc import ABC, abstractmethod
import os
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Union
from ingestion_framework.utils.logger import get_logger

class BaseIngestion(ABC):
    """Base class for all ingestion sources"""
    
    def __init__(self, source_system: str, source_entity: str):
        """Initialize ingestion base
        
        Args:
            source_system: Name of source system (e.g., 'example-scrape')
            source_entity: Name of entity being ingested (e.g., 'products')
        """
        self.source_system = source_system
        self.source_entity = source_entity
        self.logger = get_logger(self.__class__.__name__)

    @abstractmethod
    def extract(self) -> List[Dict[str, Any]]:
        """Extract data from source
        
        Returns:
            List[Dict]: List of extracted records
            
        Raises:
            NotImplementedError: Must be implemented by subclass
        """
        pass

    def ingest(self) -> Union[List[Dict[str, Any]], None]:
        """Main ingestion method
        
        Returns:
            Extracted data or None on failure
        """
        try:
            self.logger.info(f"Starting ingestion from {self.source_system}/{self.source_entity}")
            data = self.extract()
            
            if not isinstance(data, list):
                raise TypeError(f"extract() must return list, got {type(data)}")
            
            record_count = len(data)
            self.logger.info(f"Ingestion completed: {record_count} records")
            return data
            
        except TypeError as e:
            self.logger.error(f"Type error in ingestion: {e}", exc_info=True)
            raise
        except Exception as e:
            self.logger.error(f"Ingestion failed: {e}", exc_info=True)
            raise


def cleanup_old_zone_files(zone_path: str, days_to_keep: int = 7) -> int:
    """Remove zone files older than specified days
    
    Args:
        zone_path: Path to zone directory
        days_to_keep: Number of days to retain files
        
    Returns:
        Number of files removed
        
    Raises:
        ValueError: If zone_path doesn't exist
        OSError: If file deletion fails
    """
    zone_path = Path(zone_path)
    
    if not zone_path.exists():
        raise ValueError(f"Zone path does not exist: {zone_path}")
    
    if not zone_path.is_dir():
        raise ValueError(f"Zone path is not a directory: {zone_path}")
    
    cutoff = datetime.now() - timedelta(days=days_to_keep)
    removed_count = 0
    
    try:
        for file in zone_path.glob("*.json"):
            if file.is_file() and os.path.getmtime(file) < cutoff.timestamp():
                os.remove(file)
                print(f"Removed: {file}")
                removed_count += 1
        
        print(f"Cleanup completed: {removed_count} files removed")
        return removed_count
        
    except OSError as e:
        print(f"Error removing file: {e}")
        raise
