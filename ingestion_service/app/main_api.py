from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional
import os
import shutil
import io
import json

from ingestion_framework.utils.logger import get_logger
from ingestion_framework.schema_registry import versioning
from ingestion_framework.core.config import settings
from ingestion_framework.llm.query_adapter import (
    LLMQueryAdapter,
    QuerySanitizer,
    QueryExecutor,
)

logger = get_logger(__name__)
app = FastAPI(title="ETL Ingestion API")

UPLOAD_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

@app.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    source: str = Form(...),
    entity: str = Form(...),
):
    """
    Upload a file (.txt, .md, .pdf supported). Saves file to uploads/ and returns saved path.
    Actual extraction/ingestion should be triggered separately (or extend this endpoint).
    """
    fname = os.path.basename(file.filename or "uploaded")
    if not fname.lower().endswith((".txt", ".md", ".pdf", ".json", ".csv")):
        raise HTTPException(status_code=400, detail="Unsupported file type")
    dest = os.path.join(UPLOAD_DIR, f"{source}__{entity}__{fname}")
    try:
        with open(dest, "wb") as f:
            content = await file.read()
            f.write(content)
        logger.info("Saved upload: %s", dest)
        return {"status": "accepted", "path": dest}
    except Exception as e:
        logger.error("Upload failed: %s", e)
        raise HTTPException(status_code=500, detail="Upload failed")

@app.get("/schema/{source}/{entity}")
def get_schema(source: str, entity: str):
    """
    Retrieve latest JSON schema for a source/entity. Returns 404 if none exists.
    """
    schema = versioning.load_latest_schema(source, entity)
    if not schema:
        return JSONResponse(status_code=404, content={"detail": "Schema not found"})
    # attempt to load companion metadata bundle if present
    meta_path = None
    try:
        base_files = [f for f in os.listdir(os.path.join(os.path.dirname(versioning.__file__), "schemas")) if f.startswith(f"{source}__{entity}__v")]
        base_files.sort()
        if base_files:
            ver_tag = base_files[-1].replace(".json", "")
            meta_name = f"{ver_tag}__meta.json"
            meta_path = os.path.join(os.path.dirname(versioning.__file__), "schemas", meta_name)
            if os.path.exists(meta_path):
                with open(meta_path, "r", encoding="utf-8") as mf:
                    meta = json.load(mf)
            else:
                meta = None
        else:
            meta = None
    except Exception:
        meta = None
    return {"schema": schema, "metadata": meta}

@app.post("/query")
def execute_query(payload: dict):
    """
    Execute a query or accept a natural-language request (nl). For NL queries this endpoint
    will return a placeholder unless you wire an LLM adapter.
    Security: only allows SELECT statements when executing against Postgres here.
    Body examples:
      {"engine":"postgres","query":"SELECT id, title FROM my_table LIMIT 10"}
      {"nl":"top products last month"}
    """
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Invalid payload")
    if "nl" in payload:
        # Placeholder for LLM-driven flow
        return JSONResponse(status_code=501, content={"detail": "NL -> SQL flow not implemented on server. Provide LLM adapter."})
    engine = payload.get("engine")
    sql = payload.get("query")
    if not engine or not sql:
        raise HTTPException(status_code=400, detail="engine and query required")
    if engine.lower() == "postgres":
        # basic safety: only allow SELECT
        q = sql.strip().lower()
        if not q.startswith("select"):
            raise HTTPException(status_code=403, detail="Only SELECT statements allowed via API")
        # check DB config
        if not settings.DB_HOST or not settings.DB_USER:
            raise HTTPException(status_code=503, detail="Database not configured")
        try:
            import psycopg2
        except Exception:
            raise HTTPException(status_code=503, detail="psycopg2 not available on server")
        try:
            conn = psycopg2.connect(
                host=settings.DB_HOST,
                port=settings.DB_PORT,
                dbname=settings.DB_NAME,
                user=settings.DB_USER,
                password=settings.DB_PASSWORD,
                connect_timeout=5,
            )
            cur = conn.cursor()
            cur.execute(sql)
            cols = [c.name for c in cur.description] if cur.description else []
            rows = cur.fetchall()
            result = [dict(zip(cols, r)) for r in rows]
            cur.close()
            conn.close()
            return {"rows": result, "count": len(result)}
        except Exception as e:
            logger.error("Query execution error: %s", e)
            raise HTTPException(status_code=500, detail="Query execution failed")
    else:
        raise HTTPException(status_code=400, detail="Unsupported engine")

@app.post("/query/nl")
def execute_nl_query(payload: dict):
    """
    Execute a natural-language query using LLM -> SQL -> Execute flow.
    
    Body:
      {
        "nl": "show me top 10 products",
        "source": "salesforce",
        "entity": "opportunities",
        "engine": "postgres"
      }
    
    Returns: {"results": [...], "generated_sql": "...", "row_count": N}
    """
    nl = payload.get("nl")
    source = payload.get("source")
    entity = payload.get("entity")
    engine = payload.get("engine", "postgres")
    
    if not nl or not source or not entity:
        raise HTTPException(
            status_code=400,
            detail="nl, source, entity required"
        )
    
    try:
        # Load schema
        schema = versioning.load_latest_schema(source, entity)
        if not schema:
            raise HTTPException(
                status_code=404,
                detail=f"Schema not found for {source}/{entity}"
            )
        
        # Generate SQL via LLM
        try:
            adapter = LLMQueryAdapter()
            sql = adapter.generate_sql(nl, schema, db_type=engine)
        except Exception as e:
            logger.error(f"LLM generation failed: {e}")
            raise HTTPException(
                status_code=503,
                detail="LLM service unavailable or API key missing"
            )
        
        # Sanitize SQL
        if not QuerySanitizer.sanitize(sql):
            raise HTTPException(
                status_code=403,
                detail="Generated query failed safety checks"
            )
        
        # Execute query
        executor = QueryExecutor(
            engine=engine,
            conn_config={
                "host": settings.DB_HOST,
                "port": settings.DB_PORT,
                "dbname": settings.DB_NAME,
                "user": settings.DB_USER,
                "password": settings.DB_PASSWORD,
            }
        )
        results = executor.execute(sql)
        
        return {
            "nl_query": nl,
            "generated_sql": sql,
            "results": results,
            "row_count": len(results)
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))