from jsonschema import validate, ValidationError
from ingestion_framework.utils.logger import get_logger
import json
import os

logger = get_logger(__name__)

SCHEMAS_DIR = "schemas"

def get_schema(source_system, source_entity):
    """Load schema from file"""
    try:
        schema_path = os.path.join(SCHEMAS_DIR, source_system, f"{source_entity}.json")
        with open(schema_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.warning(f"Schema not found: {schema_path}")
        return None
    except Exception as e:
        logger.error(f"Error loading schema: {e}")
        raise

def validate_schema(data, source_system, source_entity):
    """Validate data against schema"""
    try:
        schema = get_schema(source_system, source_entity)
        if schema:
            validate(instance=data, schema=schema)
            logger.info(f"Schema validation passed for {source_system}/{source_entity}")
            return True
        else:
            logger.warning("No schema found, skipping validation")
            return True
    except ValidationError as e:
        logger.error(f"Schema validation failed: {e.message}")
        return False
    except Exception as e:
        logger.error(f"Validation error: {e}")
        raise
