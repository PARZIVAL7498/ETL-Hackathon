from ingestion_framework.base.ingestion import BaseIngestion
from ingestion_framework.core.config import settings
import psycopg2

class DatabaseIngestion(BaseIngestion):
    def __init__(self, source_system, source_entity, query):
        super().__init__(source_system, source_entity)
        self.query = query

    def extract(self):
        conn = psycopg2.connect(
            host=settings.DB_HOST,
            port=settings.DB_PORT,
            user=settings.DB_USER,
            password=settings.DB_PASSWORD,
            dbname=settings.DB_NAME,
        )
        cursor = conn.cursor()
        cursor.execute(self.query)
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        return data
