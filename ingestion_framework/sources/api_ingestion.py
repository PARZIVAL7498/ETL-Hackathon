from ingestion_framework.base.ingestion import BaseIngestion
from ingestion_framework.utils.validator import SchemaValidator
from ingestion_framework.sinks.cloud_storage import CloudStorage
import requests
from datetime import datetime

class APIIngestion(BaseIngestion):
    def __init__(self, source, url, schema_path, storage_type="s3"):
        super().__init__(source)
        self.url = url
        self.validator = SchemaValidator(schema_path)
        self.storage = CloudStorage(storage_type)

    def extract(self):
        response = requests.get(self.url)
        response.raise_for_status()
        return response.json()

    def validate(self, data):
        return self.validator.validate(data)

    def store_raw(self, data):
        date = datetime.utcnow()
        key = f"raw/{self.source}/{date.year}/{date.month}/{date.day}/{date.timestamp()}.json"
        self.storage.write(str(data), key)
