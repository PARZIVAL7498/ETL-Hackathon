from abc import ABC, abstractmethod
from ingestion_framework.utils.logger import get_logger

class BaseIngestion(ABC):
    def __init__(self, source_system, source_entity):
        self.source_system = source_system
        self.source_entity = source_entity
        self.logger = get_logger(self.__class__.__name__)

    @abstractmethod
    def extract(self):
        pass
