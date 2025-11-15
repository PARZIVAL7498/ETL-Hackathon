import os
from datetime import datetime
import json
from ingestion_framework.core.config import settings
from ingestion_framework.utils.logger import get_logger
from ingestion_framework.utils.metadata import create_metadata, write_metadata

logger = get_logger(__name__)

class RawLayer:
    def __init__(self, source_system, source_entity):
        self.source_system = source_system
        self.source_entity = source_entity
        self.raw_zone_path = settings.RAW_ZONE_PATH
        self._ensure_directory_exists()

    def _ensure_directory_exists(self):
        if not os.path.exists(self.raw_zone_path):
            os.makedirs(self.raw_zone_path)
            logger.info(f"Created raw zone directory: {self.raw_zone_path}")

    def write(self, data, ingestion_timestamp):
        try:
            filename = f"{self.source_system}_{self.source_entity}_{ingestion_timestamp.strftime('%Y%m%d_%H%M%S')}.json"
            file_path = os.path.join(self.raw_zone_path, filename)
            
            with open(file_path, "w") as f:
                if isinstance(data, str):
                    f.write(data)
                else:
                    json.dump(data, f, indent=2)
            
            # Write metadata
            metadata = create_metadata(
                self.source_system,
                self.source_entity,
                ingestion_timestamp,
                file_path
            )
            metadata_path = file_path.replace(".json", "_metadata.json")
            write_metadata(metadata, metadata_path)
            
            logger.info(f"Data written to raw layer: {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"Failed to write to raw layer: {e}")
            raise
