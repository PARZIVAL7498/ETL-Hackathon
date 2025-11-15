import psycopg2
from ingestion.base import BaseIngestion
from ingestion_framework.utils.logger import get_logger

logger = get_logger(__name__)

class DatabaseIngestion(BaseIngestion):
    def __init__(self, source_system, source_entity, config):
        super().__init__(source_system, source_entity)
        self.config = config

    def extract(self):
        try:
            db_config = self.config.get("database", {})
            
            if not all(k in db_config for k in ["host", "port", "user", "password", "dbname"]):
                raise ValueError("Missing required database configuration keys")
            
            conn = psycopg2.connect(
                host=db_config["host"],
                port=db_config["port"],
                user=db_config["user"],
                password=db_config["password"],
                dbname=db_config["dbname"],
            )
            
            cursor = conn.cursor()
            query = self.config.get("query", "SELECT * FROM opportunities")
            cursor.execute(query)
            data = cursor.fetchall()
            cursor.close()
            conn.close()
            
            logger.info(f"Successfully extracted {len(data)} records")
            return data
            
        except psycopg2.Error as e:
            logger.error(f"Database error: {e}")
            raise
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            raise
