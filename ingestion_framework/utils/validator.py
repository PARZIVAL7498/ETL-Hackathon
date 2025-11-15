import json
from jsonschema import validate, ValidationError
from ingestion_framework.utils.logger import get_logger

logger = get_logger(__name__)

def validate_data(data, schema):
    """
    Validates data against a JSON schema.
    """
    try:
        validate(instance=data, schema=schema)
        logger.info("Data validation passed")
        return True
    except ValidationError as e:
        logger.error(f"Data validation failed: {e.message}")
        return False
