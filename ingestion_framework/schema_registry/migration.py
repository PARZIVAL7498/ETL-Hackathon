import logging
from typing import Dict, List
from ingestion_framework.schema_registry import versioning

logger = logging.getLogger(__name__)

PG_TYPE_MAP = {
    "string": "text",
    "integer": "bigint",
    "number": "numeric",
    "boolean": "boolean",
    "object": "jsonb",
    "array": "jsonb",
    "null": "text",
}

def _json_type_to_pg(t):
    if isinstance(t, list):
        # choose best-effort mapping (prefer non-null)
        for it in t:
            if it != "null":
                return PG_TYPE_MAP.get(it, "text")
        return "text"
    return PG_TYPE_MAP.get(t, "text")

def plan_migration_postgres(old_schema: Dict, new_schema: Dict, table_name: str) -> List[str]:
    stmts = []
    old_props = (old_schema or {}).get("properties", {})
    new_props = new_schema.get("properties", {})
    # additions
    for k, v in new_props.items():
        if k not in old_props:
            pg_type = _json_type_to_pg(v.get("type"))
            stmts.append(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS \"{k}\" {pg_type};")
    # type changes (best-effort)
    for k, v in old_props.items():
        if k in new_props:
            old_t = old_props[k].get("type")
            new_t = new_props[k].get("type")
            if old_t != new_t:
                # simple cast statement
                target_type = _json_type_to_pg(new_t)
                stmts.append(f"ALTER TABLE {table_name} ALTER COLUMN \"{k}\" TYPE {target_type} USING \"{k}\"::text::{target_type};")
    # removals - not automatically dropped; provide advisory comment
    for k in old_props:
        if k not in new_props:
            stmts.append(f"-- REMOVAL: column \"{k}\" no longer present in new schema. Manual action may be required.")
    return stmts

def plan_migration_mongo(old_schema: Dict, new_schema: Dict, collection_name: str) -> List[Dict]:
    """
    Return a list of update pipelines to migrate documents in MongoDB.
    Example pipeline: {'filter': {...}, 'update': {'$set': {...}}}
    """
    pipelines = []
    old_props = (old_schema or {}).get("properties", {})
    new_props = new_schema.get("properties", {})
    # add fields with null if absent
    set_ops = {}
    for k in new_props:
        if k not in old_props:
            set_ops[k] = None
    if set_ops:
        pipelines.append({"filter": {}, "update": {"$set": set_ops}})
    # type migrations would be more complex — advise manual
    return pipelines

# executor helpers
def apply_postgres_migration(conn_info: Dict, stmts: List[str]):
    """
    conn_info: {host,port,dbname,user,password}
    This executes migration statements inside a transaction.
    """
    try:
        import psycopg2
    except Exception as e:
        logger.error("psycopg2 not installed; cannot apply postgres migrations")
        raise
    conn = None
    try:
        conn = psycopg2.connect(
            host=conn_info.get("host"),
            port=conn_info.get("port", 5432),
            dbname=conn_info.get("dbname"),
            user=conn_info.get("user"),
            password=conn_info.get("password"),
        )
        cur = conn.cursor()
        for s in stmts:
            if s.strip().startswith("--"):
                logger.info(s)
                continue
            logger.info("Executing: %s", s)
            cur.execute(s)
        conn.commit()
        cur.close()
        logger.info("Postgres migration applied")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error("Migration failed: %s", e)
        raise
    finally:
        if conn:
            conn.close()