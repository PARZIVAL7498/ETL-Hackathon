import pytest
from ingestion_framework.llm.query_adapter import (
    QuerySanitizer,
    QueryExecutor,
)
from ingestion_framework.schema_registry import versioning

def test_query_sanitizer_allows_select():
    query = "SELECT id, title FROM products LIMIT 10"
    assert QuerySanitizer.sanitize(query) == True

def test_query_sanitizer_rejects_drop():
    query = "DROP TABLE products"
    assert QuerySanitizer.sanitize(query) == False

def test_query_sanitizer_rejects_delete():
    query = "DELETE FROM products WHERE id = 1"
    assert QuerySanitizer.sanitize(query) == False

def test_query_sanitizer_rejects_injection():
    query = "SELECT * FROM products WHERE id = '1' OR '1'='1'"
    assert QuerySanitizer.sanitize(query) == False

def test_query_sanitizer_rejects_comments():
    query = "SELECT * FROM products -- malicious comment"
    assert QuerySanitizer.sanitize(query) == False

def test_query_sanitizer_requires_select():
    query = "INSERT INTO products VALUES (1, 'test')"
    assert QuerySanitizer.sanitize(query) == False

def test_schema_generation_for_llm():
    sample = [
        {"id": "p1", "title": "Product A", "price": 99.99},
        {"id": "p2", "title": "Product B", "price": 149.99},
    ]
    schema = versioning.generate_json_schema(sample)
    assert "properties" in schema
    assert "id" in schema["properties"]
    assert schema["properties"]["price"]["type"] == "number"