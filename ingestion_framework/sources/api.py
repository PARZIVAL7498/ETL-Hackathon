from ingestion_framework.base.ingestion import BaseIngestion
from ingestion_framework.core.config import settings
import requests

class APIIngestion(BaseIngestion):
    def __init__(self, source_system, source_entity, url):
        super().__init__(source_system, source_entity)
        self.url = url

    def extract(self):
        headers = {"Authorization": f"Bearer {settings.API_KEY}"}
        response = requests.get(self.url, headers=headers)
        response.raise_for_status()
        return response.json()
