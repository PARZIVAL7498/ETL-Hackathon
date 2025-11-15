import requests
from ingestion.base import BaseIngestion
from ingestion_framework.utils.logger import get_logger

logger = get_logger(__name__)

class APIIngestion(BaseIngestion):
    def __init__(self, source_system, source_entity, config):
        super().__init__(source_system, source_entity)
        self.config = config

    def extract(self):
        try:
            url = self.config.get("url")
            if not url:
                raise ValueError("URL not provided in config")
            
            headers = self.config.get("headers", {})
            timeout = self.config.get("timeout", 30)
            
            logger.info(f"Fetching data from {url}")
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Successfully fetched {len(data)} records")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise
        except ValueError as e:
            logger.error(f"Configuration error: {e}")
            raise
