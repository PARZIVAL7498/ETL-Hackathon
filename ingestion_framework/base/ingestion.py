from abc import ABC, abstractmethod
from tenacity import retry, stop_after_attempt, wait_fixed
from ingestion_framework.utils.logger import get_logger
from ingestion_framework.sinks.raw_layer import RawLayer
from datetime import datetime
import hashlib

class BaseIngestion(ABC):
    def __init__(self, source_system, source_entity):
        self.source_system = source_system
        self.source_entity = source_entity
        self.logger = get_logger(self.__class__.__name__)
        self.raw_layer = RawLayer(source_system, source_entity)

    @abstractmethod
    def extract(self):
        pass

    def get_data_hash(self, data):
        return hashlib.md5(str(data).encode()).hexdigest()

    def is_duplicate(self, data_hash):
        # This is a placeholder for a real idempotency check.
        # In a production system, you would check a database or a key-value store.
        return False

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def ingest(self):
        try:
            self.logger.info(f"Starting ingestion for {self.source_system}/{self.source_entity}")
            data = self.extract()
            data_hash = self.get_data_hash(data)

            if self.is_duplicate(data_hash):
                self.logger.info("Data is a duplicate, skipping ingestion.")
                return

            ingestion_timestamp = datetime.utcnow()
            self.raw_layer.write(str(data), ingestion_timestamp)
            self.logger.info(f"Ingestion completed for {self.source_system}/{self.source_entity}")

        except Exception as e:
            self.logger.error(f"Ingestion failed for {self.source_system}/{self.source_entity} with error: {e}")
            raise