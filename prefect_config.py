"""
Prefect configuration for ETL pipeline
"""

from prefect import Flow
import os
from pathlib import Path

PROJECT_ROOT = Path('/Users/pakshaljain/Desktop/ETL-Hackathon')
RAW_ZONE = PROJECT_ROOT / 'raw_zone'
PROCESSED_ZONE = PROJECT_ROOT / 'processed_zone'

# Ensure directories exist
RAW_ZONE.mkdir(exist_ok=True)
PROCESSED_ZONE.mkdir(exist_ok=True)

PREFECT_CONFIG = {
    'project_root': str(PROJECT_ROOT),
    'raw_zone': str(RAW_ZONE),
    'processed_zone': str(PROCESSED_ZONE),
    'log_level': 'INFO',
    'retries': 3,
    'retry_delay': 300,  # 5 minutes
}