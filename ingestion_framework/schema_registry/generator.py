import os
import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from ingestion_framework.schema_registry import versioning

SCHEMAS_DIR = os.path.join(os.path.dirname(__file__), "schemas")
os.makedirs(SCHEMAS_DIR, exist_ok=True)

PG_TYPE_MAP = {
    "string": "text",
    "integer": "bigint",
    "number": "numeric",
    "boolean": "boolean",
    "object": "jsonb",
    "array": "jsonb",
    "null": "text",
}

MONGO_TYPE_MAP = {
    "string": "string",
    "integer": "int64",
    "number": "double",
    "boolean": "bool",
    "object": "document",
    "array": "array",
    "null": "null",
}

NEO4J_TYPE_MAP = {
    "string": "STRING",
    "integer": "INTEGER",
    "number": "FLOAT",
    "boolean": "BOOLEAN",
    "object": "MAP",
    "array": "LIST",
    "null": "STRING",
}

def _choose_type(t):
    if isinstance(t, list):
        # prefer non-null type
        for it in t:
            if it != "null":
                return it
        return t[0]
    return t

def _build_postgres_ddl(table_name: str, properties: Dict[str, Any]) -> str:
    cols = []
    for k, v in properties.items():
        jtype = _choose_type(v.get("type"))
        pgtype = PG_TYPE_MAP.get(jtype, "text")
        cols.append(f'    "{k}" {pgtype}')
    cols_sql = ",\n".join(cols) or "    -- no columns inferred"
    ddl = f"CREATE TABLE IF NOT EXISTS {table_name} (\n{cols_sql}\n);"
    return ddl

def generate_schema_with_metadata(records: List[Dict], source: Optional[str]=None, entity: Optional[str]=None) -> Dict:
    """
    Generate full schema bundle:
      - json_schema: produced by versioning.generate_json_schema
      - db_mappings: {postgres, mongo, neo4j}
      - metadata: created_at, sample_size, primary_key_guess, examples
    """
    sample = records if isinstance(records, list) else [records]
    sample = sample[:200]
    json_schema = versioning.generate_json_schema(sample)
    properties = json_schema.get("properties", {})

    # build DB mappings
    postgres = {}
    mongo = {}
    neo4j = {}
    examples = {}
    for k, v in properties.items():
        jtype = v.get("type")
        chosen = _choose_type(jtype)
        postgres[k] = PG_TYPE_MAP.get(chosen, "text")
        mongo[k] = MONGO_TYPE_MAP.get(chosen, "string")
        neo4j[k] = NEO4J_TYPE_MAP.get(chosen, "STRING")
        examples[k] = v.get("examples", [])[:3]

    primary_key = "id" if "id" in properties else None

    table_name = f"{source}_{entity}" if source and entity else "inferred_table"
    ddl = _build_postgres_ddl(table_name, properties)

    bundle = {
        "json_schema": json_schema,
        "db_mappings": {
            "postgres": postgres,
            "mongo": mongo,
            "neo4j": neo4j,
            "postgres_ddl": ddl,
        },
        "metadata": {
            "created_at": datetime.utcnow().isoformat(),
            "sample_size": len(sample),
            "primary_key_guess": primary_key,
            "source": source,
            "entity": entity,
            "examples": examples,
        },
    }
    return bundle

def save_schema_bundle(source: str, entity: str, bundle: Dict) -> str:
    """
    Save json_schema via versioning.save_new_schema to bump version,
    then write metadata bundle to a companion file with same version id.
    Returns path to the saved metadata bundle.
    """
    # save main json schema and get path (to determine version)
    schema_path = versioning.save_new_schema(source, entity, bundle["json_schema"])
    # determine version string from schema_path filename
    base = os.path.basename(schema_path)
    ver_tag = base.replace(".json", "")
    meta_name = f"{ver_tag}__meta.json"
    meta_path = os.path.join(SCHEMAS_DIR, meta_name)
    with open(meta_path, "w", encoding="utf-8") as mf:
        json.dump(bundle, mf, indent=2, default=str)
    return meta_path