"""Simple, self-contained Prefect ETL for local testing.

- Ingest: create a mock JSON file in raw_zone if none exist
- Validate: list JSON files in raw_zone
- Process: read first JSON, add a "processed_at" field, write to processed_zone
- Load: simulate upload by copying processed file to processed_zone/loaded/
This avoids fragile subprocess calls and should run reliably.
"""

from prefect import flow, task, get_run_logger
from pathlib import Path
from datetime import datetime
import json
import os
import shutil

PROJECT_ROOT = Path("/Users/pakshaljain/Desktop/ETL-Hackathon").resolve()
RAW_ZONE = PROJECT_ROOT / "raw_zone"
PROCESSED_ZONE = PROJECT_ROOT / "processed_zone"
LOADED_ZONE = PROCESSED_ZONE / "loaded"

RAW_ZONE.mkdir(parents=True, exist_ok=True)
PROCESSED_ZONE.mkdir(parents=True, exist_ok=True)
LOADED_ZONE.mkdir(parents=True, exist_ok=True)


@task(name="Ingest (mock)", retries=1)
def ingest_task():
    logger = get_run_logger()
    logger.info("🔵 Ingest: ensuring at least one input file exists in raw_zone")
    sample = RAW_ZONE / "mock.json"
    if not sample.exists():
        logger.info("Creating mock input: %s", str(sample))
        sample.write_text(json.dumps([{"id": 1, "value": "example"}], indent=2))
    else:
        logger.info("Found existing input: %s", str(sample))
    return {"status": "ingested", "path": str(sample)}


@task(name="Validate", retries=1)
def validate_task(ingest_result):
    logger = get_run_logger()
    logger.info("🔵 Validate: listing JSON files in raw_zone")
    files = sorted([str(p.resolve()) for p in RAW_ZONE.glob("*.json")])
    logger.info("Found %d files", len(files))
    return {"status": "valid", "files": files}


@task(name="Process", retries=2)
def process_task(validation_result):
    logger = get_run_logger()
    logger.info("🔵 Process: transform first validated file")
    files = validation_result.get("files") or []
    if not files:
        raise RuntimeError("No input files to process")
    input_path = Path(files[0])
    output_path = PROCESSED_ZONE / (input_path.stem + "_processed.json")

    logger.info("Reading input: %s", str(input_path))
    data = json.loads(input_path.read_text())

    # Example transform: add processed metadata
    processed = {
        "processed_at": datetime.utcnow().isoformat() + "Z",
        "records": data,
        "record_count": len(data) if isinstance(data, list) else 1,
    }

    output_path.write_text(json.dumps(processed, indent=2))
    logger.info("Wrote processed file: %s", str(output_path))
    return {"status": "processed", "output_file": str(output_path)}


@task(name="Load (simulate)", retries=1)
def load_task(process_result):
    logger = get_run_logger()
    logger.info("🔵 Load: simulate uploading processed file")
    output = process_result.get("output_file")
    if not output:
        raise RuntimeError("No processed output to load")
    src = Path(output)
    if not src.exists():
        raise FileNotFoundError(f"Processed file missing: {src}")
    dest = LOADED_ZONE / src.name
    shutil.copy2(src, dest)
    logger.info("Simulated load: copied %s -> %s", str(src), str(dest))
    return {"status": "loaded", "loaded_path": str(dest)}


@flow(name="ETL Pipeline - simple")
def etl_dag():
    logger = get_run_logger()
    logger.info("=== Starting simple ETL pipeline ===")
    ing = ingest_task()
    val = validate_task(ing)
    proc = process_task(val)
    ld = load_task(proc)
    logger.info("=== ETL finished ===")
    return {"ingest": ing, "validate": val, "process": proc, "load": ld}


# Backwards compatibility for tools that import `dag`
dag = etl_dag

if __name__ == "__main__":
    etl_dag()