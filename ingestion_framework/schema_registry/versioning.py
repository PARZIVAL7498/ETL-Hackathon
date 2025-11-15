import os
import json
from typing import Any, Dict, List, Optional

SCHEMAS_DIR = os.path.join(os.path.dirname(__file__), "schemas")
os.makedirs(SCHEMAS_DIR, exist_ok=True)

TYPE_MAP = {
    "int": "integer",
    "float": "number",
    "str": "string",
    "bool": "boolean",
    "list": "array",
    "dict": "object",
    "none": "null",
}

def _infer_type(value: Any) -> str:
    if value is None:
        return "null"
    t = type(value).__name__
    return TYPE_MAP.get(t, "string")

def generate_json_schema(records: List[Dict]) -> Dict:
    """
    Simple schema inference: collects types per property and required-ness.
    """
    props = {}
    required = set()
    total = len(records) or 1
    for rec in records:
        for k, v in rec.items():
            t = _infer_type(v)
            entry = props.setdefault(k, {"types": set(), "examples": []})
            entry["types"].add(t)
            if len(entry["examples"]) < 3:
                entry["examples"].append(v)
        # required detection
        for k in rec.keys():
            # track presence count via an ad-hoc count field
            pass
    # determine required as fields present in all records
    for k in props:
        present_count = sum(1 for r in records if k in r)
        if present_count == total:
            required.add(k)
    properties = {}
    for k, v in props.items():
        types = list(v["types"])
        if len(types) == 1:
            prop_type = types[0]
        else:
            prop_type = types
        properties[k] = {"type": prop_type, "examples": v["examples"]}
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": properties,
        "required": sorted(list(required)),
    }
    return schema

def _schema_version_path(source: str, entity: str, version: int) -> str:
    fn = f"{source}__{entity}__v{version}.json"
    return os.path.join(SCHEMAS_DIR, fn)

def load_latest_schema(source: str, entity: str) -> Optional[Dict]:
    files = [f for f in os.listdir(SCHEMAS_DIR) if f.startswith(f"{source}__{entity}__v")]
    if not files:
        return None
    files.sort()
    latest = files[-1]
    with open(os.path.join(SCHEMAS_DIR, latest), "r", encoding="utf-8") as fh:
        return json.load(fh)

def save_new_schema(source: str, entity: str, schema: Dict) -> str:
    # determine next version
    files = [f for f in os.listdir(SCHEMAS_DIR) if f.startswith(f"{source}__{entity}__v")]
    if not files:
        ver = 1
    else:
        files.sort()
        last = files[-1]
        try:
            ver = int(last.split("__v")[-1].split(".")[0]) + 1
        except Exception:
            ver = len(files) + 1
    path = _schema_version_path(source, entity, ver)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(schema, fh, indent=2)
    return path

def compare_schemas(old: Dict, new: Dict) -> Dict:
    """
    Very small comparator. Returns {'compatible': bool, 'diff': [..]}
    Rules:
      - Adding fields => compatible (backwards compatible)
      - Removing fields => incompatible
      - Narrowing types (e.g., number -> integer) => incompatible
    """
    diff = []
    old_props = old.get("properties", {}) if old else {}
    new_props = new.get("properties", {})
    # removed fields
    for k in old_props:
        if k not in new_props:
            diff.append({"type": "removed_field", "field": k})
    # type changes
    for k, v in old_props.items():
        if k in new_props:
            old_type = v.get("type")
            new_type = new_props[k].get("type")
            if old_type != new_type:
                # if new_type is list and contains old_type => OK
                if isinstance(new_type, list) and old_type in new_type:
                    continue
                diff.append({"type": "type_changed", "field": k, "from": old_type, "to": new_type})
    compatible = all(d["type"] != "removed_field" for d in diff) and not any(d["type"] == "type_changed" for d in diff)
    return {"compatible": compatible, "diff": diff}