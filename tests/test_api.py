import os
import io
import json
import tempfile
from fastapi.testclient import TestClient

from ingestion_service.app import main_api as api_module
from ingestion_framework.schema_registry import versioning

client = TestClient(api_module.app)

def test_upload_txt_file():
    content = b"sample text"
    files = {"file": ("sample.txt", io.BytesIO(content), "text/plain")}
    data = {"source": "tests", "entity": "upload"}
    resp = client.post("/upload", files=files, data=data)
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "accepted"
    assert os.path.exists(body["path"])
    # cleanup
    os.remove(body["path"])

def test_schema_retrieval_roundtrip(tmp_path):
    # create a sample schema via versioning (simulates earlier ingestion)
    sample_schema = {"type": "object", "properties": {"id": {"type": "string"}}}
    path = versioning.save_new_schema("tests", "products", sample_schema)
    resp = client.get("/schema/tests/products")
    assert resp.status_code == 200
    j = resp.json()
    assert "schema" in j
    assert j["schema"]["properties"]["id"]["type"] == "string"
    # cleanup created schema file
    os.remove(path)

def test_query_requires_engine_and_query():
    resp = client.post("/query", json={})
    assert resp.status_code == 400

def test_nl_query_not_implemented():
    resp = client.post("/query", json={"nl": "top products"})
    assert resp.status_code == 501