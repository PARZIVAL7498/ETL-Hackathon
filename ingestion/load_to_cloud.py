import os
import sys
import json
from ingestion_framework.sinks.cloud_storage import CloudStorage
from ingestion_framework.utils.logger import get_logger
from ingestion_framework.core.config import settings

logger = get_logger(__name__)

def load_to_cloud(input_file, storage_type="s3"):
    """Load processed data to cloud storage"""
    try:
        if not os.path.exists(input_file):
            raise FileNotFoundError(f"Input file not found: {input_file}")
        
        with open(input_file, "r") as f:
            data = f.read()
        
        cloud_storage = CloudStorage(storage_type=storage_type)
        key = f"processed_zone/{os.path.basename(input_file)}"
        
        cloud_storage.write(data, key)
        logger.info(f"Successfully loaded {input_file} to {storage_type}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to load to cloud: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("Usage: python load_to_cloud.py <input_file> [storage_type]")
        sys.exit(1)

    input_file = sys.argv[1]
    storage_type = sys.argv[2] if len(sys.argv) > 2 else "s3"
    
    load_to_cloud(input_file, storage_type)
