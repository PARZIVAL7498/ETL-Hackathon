from ingestion_framework.utils.logger import get_logger
from ingestion_framework.schema_registry.schemas import get_schema, validate_schema
import os
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_fixed

logger = get_logger(__name__)

class IngestionService:
    def __init__(self, source, raw_zone_path="raw_zone"):
        self.source = source
        self.raw_zone_path = raw_zone_path

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def extract(self, data):
        """
        Extracts data from a source.
        In a real-world scenario, this method would connect to a data source
        and extract the data. For this example, we will just return the data.
        """
        logger.info(f"Extracting data from source: {self.source}")
        return data

    def validate(self, data):
        """
        Validates the data against the schema.
        """
        logger.info(f"Validating data for source: {self.source}")
        schema = get_schema(self.source)
        if not schema:
            logger.warning(f"No schema found for source: {self.source}")
            return False
        is_valid = validate_schema(data, schema)
        if not is_valid:
            logger.error(f"Data validation failed for source: {self.source}")
        return is_valid

    def store_raw(self, data):
        """
        Stores the raw data in the raw zone.
        """
        logger.info(f"Storing raw data for source: {self.source}")
        date = datetime.utcnow()
        storage_path = os.path.join(
            self.raw_zone_path, self.source, str(date.year), str(date.month), str(date.day)
        )
        os.makedirs(storage_path, exist_ok=True)
        file_path = os.path.join(storage_path, f"{date.timestamp()}.json")
        with open(file_path, "w") as f:
            f.write(str(data))
        logger.info(f"Raw data stored at: {file_path}")
        return file_path

    def ingest(self, data):
        """
        Orchestrates the ingestion process.
        """
        logger.info(f"Starting ingestion for source: {self.source}")
        extracted_data = self.extract(data)
        if self.validate(extracted_data):
            file_path = self.store_raw(extracted_data)
            logger.info(f"Ingestion completed for source: {self.source}")
            return file_path
        else:
            logger.error(f"Ingestion failed for source: {self.source}")
            return None
