"""
Main ETL Flow using Prefect
"""

from prefect import flow, task, get_run_logger
from prefect.task_runs import task_run
from datetime import datetime
import subprocess
import os
from pathlib import Path

PROJECT_ROOT = '/Users/pakshaljain/Desktop/ETL-Hackathon'

# ============== TASKS ==============

@task(name="Ingest Data", retries=3, retry_delay_seconds=300)
def ingest_data():
    """
    Task: Ingest data from raw sources
    """
    logger = get_run_logger()
    logger.info("🔵 Starting data ingestion...")
    
    try:
        result = subprocess.run(
            ['python', f'{PROJECT_ROOT}/ingestion/main.py', 
             f'{PROJECT_ROOT}/ingestion/configs/mock.txt'],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=300
        )
        
        if result.returncode == 0:
            logger.info("✅ Data ingestion completed successfully")
            return {
                'status': 'success',
                'timestamp': datetime.now().isoformat(),
                'output': result.stdout
            }
        else:
            logger.error(f"❌ Ingestion failed: {result.stderr}")
            raise Exception(f"Ingestion failed: {result.stderr}")
            
    except subprocess.TimeoutExpired:
        logger.error("❌ Ingestion timed out after 5 minutes")
        raise Exception("Ingestion timeout")
    except Exception as e:
        logger.error(f"❌ Ingestion error: {e}")
        raise

@task(name="Validate Data", retries=2, retry_delay_seconds=300)
def validate_data(ingest_result):
    """
    Task: Validate ingested data
    """
    logger = get_run_logger()
    logger.info("🔵 Validating ingested data...")
    
    try:
        # Check if raw_zone has files
        raw_zone = Path(f'{PROJECT_ROOT}/raw_zone')
        json_files = list(raw_zone.glob('*.json'))
        
        if not json_files:
            raise Exception("No JSON files found in raw_zone")
        
        logger.info(f"✅ Validation passed: Found {len(json_files)} JSON files")
        return {
            'status': 'valid',
            'file_count': len(json_files),
            'files': [str(f) for f in json_files]
        }
        
    except Exception as e:
        logger.error(f"❌ Validation error: {e}")
        raise

@task(name="Process Data", retries=3, retry_delay_seconds=300)
def process_data(validation_result):
    """
    Task: Process and transform data
    """
    logger = get_run_logger()
    logger.info("🔵 Starting data processing...")
    
    try:
        result = subprocess.run(
            ['python', f'{PROJECT_ROOT}/ingestion_service/app/services/classification_service.py'],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=600
        )
        
        if result.returncode == 0:
            logger.info("✅ Data processing completed successfully")
            return {
                'status': 'success',
                'timestamp': datetime.now().isoformat(),
                'output': result.stdout
            }
        else:
            logger.error(f"❌ Processing failed: {result.stderr}")
            raise Exception(f"Processing failed: {result.stderr}")
            
    except subprocess.TimeoutExpired:
        logger.error("❌ Processing timed out after 10 minutes")
        raise Exception("Processing timeout")
    except Exception as e:
        logger.error(f"❌ Processing error: {e}")
        raise

@task(name="Load Data", retries=2, retry_delay_seconds=300)
def load_data(process_result):
    """
    Task: Load processed data to cloud
    """
    logger = get_run_logger()
    logger.info("🔵 Starting data load...")
    
    try:
        result = subprocess.run(
            ['python', f'{PROJECT_ROOT}/ingestion/load_to_cloud.py'],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=600
        )
        
        if result.returncode == 0:
            logger.info("✅ Data load completed successfully")
            return {
                'status': 'success',
                'timestamp': datetime.now().isoformat(),
                'output': result.stdout
            }
        else:
            logger.error(f"❌ Load failed: {result.stderr}")
            raise Exception(f"Load failed: {result.stderr}")
            
    except subprocess.TimeoutExpired:
        logger.error("❌ Load timed out after 10 minutes")
        raise Exception("Load timeout")
    except Exception as e:
        logger.error(f"❌ Load error: {e}")
        raise

# ============== FLOW ==============

@flow(name="ETL Pipeline", description="Data Ingestion, Validation, Processing, and Loading")
def etl_pipeline():
    """
    Main ETL Flow:
    1. Ingest data from raw sources
    2. Validate ingested data
    3. Process and transform data
    4. Load processed data to cloud
    """
    
    logger = get_run_logger()
    logger.info("=" * 60)
    logger.info("🚀 Starting ETL Pipeline")
    logger.info("=" * 60)
    
    # Task 1: Ingest
    ingest_result = ingest_data()
    
    # Task 2: Validate
    validation_result = validate_data(ingest_result)
    
    # Task 3: Process
    process_result = process_data(validation_result)
    
    # Task 4: Load
    load_result = load_data(process_result)
    
    logger.info("=" * 60)
    logger.info("✅ ETL Pipeline Completed Successfully")
    logger.info("=" * 60)
    
    return {
        'ingest': ingest_result,
        'validation': validation_result,
        'process': process_result,
        'load': load_result,
        'completed_at': datetime.now().isoformat()
    }

if __name__ == '__main__':
    # Run flow locally
    etl_pipeline()