from datetime import datetime
import json

def create_metadata(source_system, source_entity, ingestion_timestamp, file_path):
    """
    Creates metadata for ingested data.
    """
    return {
        "source_system": source_system,
        "source_entity": source_entity,
        "ingestion_timestamp": ingestion_timestamp.isoformat(),
        "file_path": file_path,
        "created_at": datetime.utcnow().isoformat(),
    }

def write_metadata(metadata, metadata_path):
    """
    Writes metadata to a file.
    """
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)
