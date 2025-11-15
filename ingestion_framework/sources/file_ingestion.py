from ingestion_framework.base.ingestion import BaseIngestion
from ingestion_framework.utils.validator import SchemaValidator
from ingestion_framework.sinks.cloud_storage import CloudStorage
import json
from datetime import datetime

class FileIngestion(BaseIngestion):
    def __init__(self, source, file_path, schema_path, storage_type="s3"):
        super().__init__(source)
        self.file_path = file_path
        self.validator = SchemaValidator(schema_path)
        self.storage = CloudStorage(storage_type)

    def extract(self):
        with open(self.file_path, "r") as f:
            return json.load(f)

    def validate(self, data):
        return self.validator.validate(data)

    def store_raw(self, data):
        date = datetime.utcnow()
        key = f"raw/{self.source}/{date.year}/{date.month}/{date.day}/{date.timestamp()}.json"
        self.storage.write(str(data), key)
