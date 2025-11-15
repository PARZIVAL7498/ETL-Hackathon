import sys
import json
from ingestion.sources.api import APIIngestion
from ingestion_framework.utils.logger import get_logger

logger = get_logger(__name__)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("Usage: python main.py <config_file>")
        sys.exit(1)

    config_file = sys.argv[1]
    with open(config_file, "r") as f:
        config = json.load(f)

    ingestion_type = config.get("ingestion_type", "api")
    
    if ingestion_type == "api":
        ingestion = APIIngestion(
            config["source_system"],
            config["source_entity"],
            config
        )
    else:
        logger.error(f"Unsupported ingestion type: {ingestion_type}")
        sys.exit(1)

    ingestion.ingest()
