import os
import json
from datetime import datetime
from ingestion_framework.core.config import settings
from ingestion_framework.utils.logger import get_logger

logger = get_logger(__name__)

class RawLayer:
    def __init__(self, source_system: str, source_entity: str):
        self.source_system = source_system
        self.source_entity = source_entity
        self.raw_zone = settings.RAW_ZONE_PATH or "raw_zone"
        os.makedirs(self.raw_zone, exist_ok=True)

    def write(self, data, ingestion_timestamp: datetime):
        filename = f"{self.source_system}_{self.source_entity}_{ingestion_timestamp.strftime('%Y%m%d_%H%M%S')}.json"
        file_path = os.path.join(self.raw_zone, filename)
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                if isinstance(data, (str, bytes)):
                    if isinstance(data, bytes):
                        f.write(data.decode("utf-8"))
                    else:
                        f.write(data)
                else:
                    json.dump(data, f, indent=2, default=str)
            # metadata
            metadata = {
                "source_system": self.source_system,
                "source_entity": self.source_entity,
                "ingestion_timestamp": ingestion_timestamp.isoformat(),
                "file_path": file_path
            }
            meta_path = file_path.replace(".json", "_metadata.json")
            with open(meta_path, "w", encoding="utf-8") as mf:
                json.dump(metadata, mf, indent=2)
            logger.info(f"Wrote raw data to {file_path}")
            return file_path
        except Exception as e:
            logger.error(f"Failed to write raw file: {e}")
            raise
