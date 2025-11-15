from ingestion_framework.base.ingestion import BaseIngestion
import json

class FilesystemIngestion(BaseIngestion):
    def __init__(self, source_system, source_entity, file_path):
        super().__init__(source_system, source_entity)
        self.file_path = file_path

    def extract(self):
        with open(self.file_path, "r") as f:
            return json.load(f)
