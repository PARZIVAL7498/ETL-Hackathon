"""
Base ingestion class for framework
"""

from abc import ABC, abstractmethod
from tenacity import retry, stop_after_attempt, wait_fixed

class BaseIngestion(ABC):
    """Abstract base class for ingestion"""
    
    def __init__(self, source_system, source_entity):
        self.source_system = source_system
        self.source_entity = source_entity

    @abstractmethod
    def extract(self):
        """Extract data from source"""
        pass

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        reraise=True
    )
    def ingest(self):
        """Main ingestion method with retry logic"""
        try:
            # Import here to avoid circular imports
            from ingestion_framework.utils.logger import get_logger
            from ingestion_framework.sinks.raw_layer import RawLayer
            from ingestion_framework.schema_registry import versioning, migration, generator
            from ingestion_framework.core.config import settings
            from datetime import datetime

            logger = get_logger(self.__class__.__name__)
            
            logger.info(f"🔄 Starting ingestion: {self.source_system}/{self.source_entity}")
            
            # Extract data
            data = self.extract()
            
            # Save to raw layer
            raw_layer = RawLayer(self.source_system, self.source_entity)
            file_path = raw_layer.write(data, datetime.utcnow())
            
            # Generate inferred schema from sample (first N records)
            sample = data if isinstance(data, list) else [data]
            sample = sample[:200]

            # Use generator to build json schema + db metadata
            bundle = generator.generate_schema_with_metadata(sample, source=self.source_system, entity=self.source_entity)
            old_schema = versioning.load_latest_schema(self.source_system, self.source_entity)
            cmp = versioning.compare_schemas(old_schema, bundle["json_schema"]) if old_schema else {"compatible": True, "diff":[]}
            
            logger.info(f"Schema comparison: compatible={cmp['compatible']}, diff={cmp['diff']}")
            
            if not old_schema or not cmp["compatible"]:
                meta_path = generator.save_schema_bundle(self.source_system, self.source_entity, bundle)
                logger.info(f"Saved new schema bundle metadata: {meta_path}")
                # plan migrations if old exists and incompatible
                if old_schema:
                    if settings.MIGRATION_TARGET == "postgres":
                        table = settings.POSTGRES_TABLE_NAME or f"{self.source_system}_{self.source_entity}"
                        stmts = migration.plan_migration_postgres(old_schema, bundle["json_schema"], table)
                        logger.info("Planned Postgres migration statements:\n" + "\n".join(stmts))
                        if settings.AUTO_APPLY_MIGRATIONS:
                            conn_info = {
                                "host": settings.DB_HOST,
                                "port": settings.DB_PORT,
                                "dbname": settings.DB_NAME,
                                "user": settings.DB_USER,
                                "password": settings.DB_PASSWORD,
                            }
                            migration.apply_postgres_migration(conn_info, stmts)
                    elif settings.MIGRATION_TARGET == "mongo":
                        pipelines = migration.plan_migration_mongo(old_schema, bundle["json_schema"], settings.POSTGRES_TABLE_NAME or f"{self.source_system}_{self.source_entity}")
                        logger.info("Planned MongoDB pipelines: %s", pipelines)
            logger.info(f"✅ Ingestion completed: {file_path}")
            return file_path
            
        except Exception as e:
            from ingestion_framework.utils.logger import get_logger
            logger = get_logger(self.__class__.__name__)
            logger.error(f"❌ Ingestion failed: {e}")
            raise