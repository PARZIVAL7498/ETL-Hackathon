"""
Base classes for ingestion
"""

from abc import ABC, abstractmethod

class BaseIngestion(ABC):
    """Base class for all ingestion sources"""
    
    def __init__(self, source_system, source_entity):
        self.source_system = source_system
        self.source_entity = source_entity

    @abstractmethod
    def extract(self):
        """Extract data from source"""
        pass

    def ingest(self):
        """Main ingestion method"""
        try:
            from ingestion_framework.utils.logger import get_logger
            logger = get_logger(self.__class__.__name__)
            
            logger.info(f"Starting ingestion from {self.source_system}/{self.source_entity}")
            data = self.extract()
            logger.info(f"Ingestion completed: {len(data) if isinstance(data, list) else 'unknown'} records")
            return data
            
        except Exception as e:
            from ingestion_framework.utils.logger import get_logger
            logger = get_logger(self.__class__.__name__)
            logger.error(f"Ingestion failed: {e}")
            raise
