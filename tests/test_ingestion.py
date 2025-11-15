import unittest
from unittest.mock import patch, MagicMock
from ingestion_framework.sources.api_ingestion import APIIngestion
from ingestion_framework.sources.file_ingestion import FileIngestion

class TestIngestion(unittest.TestCase):
    @patch("ingestion_framework.sinks.cloud_storage.CloudStorage")
    @patch("requests.get")
    def test_api_ingestion(self, mock_get, mock_storage):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"product_id": 1, "quantity": 10, "price": 100}
        mock_get.return_value = mock_response

        ingestion = APIIngestion(
            source="sales",
            url="http://test.com/api/sales",
            schema_path="schemas/sales.json",
        )
        ingestion.ingest()

        mock_storage.return_value.write.assert_called_once()

    @patch("ingestion_framework.sinks.cloud_storage.CloudStorage")
    def test_file_ingestion(self, mock_storage):
        ingestion = FileIngestion(
            source="sales",
            file_path="schemas/sales.json", # Using schema as data for testing
            schema_path="schemas/sales.json",
        )
        ingestion.ingest()

        mock_storage.return_value.write.assert_called_once()

if __name__ == "__main__":
    unittest.main()
