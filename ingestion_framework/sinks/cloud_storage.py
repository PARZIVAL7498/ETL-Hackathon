import logging

try:
    import boto3
except Exception:
    boto3 = None

try:
    from google.cloud import storage
except Exception:
    storage = None

from ingestion_framework.core.config import settings
from ingestion_framework.utils.logger import get_logger

logger = get_logger(__name__)

class CloudStorage:
    def __init__(self, storage_type: str = "s3"):
        self.storage_type = storage_type.lower()
        if self.storage_type == "s3":
            if not boto3:
                raise ImportError("boto3 not installed. pip install boto3")
            if not settings.AWS_ACCESS_KEY_ID or not settings.AWS_SECRET_ACCESS_KEY or not settings.S3_BUCKET:
                raise ValueError("AWS credentials or S3_BUCKET missing in .env")
            self.client = boto3.client(
                "s3",
                aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
                region_name=settings.AWS_REGION,
            )
            logger.info(f"Initialized S3 client for bucket {settings.S3_BUCKET}")
        elif self.storage_type == "gcs":
            if not storage:
                raise ImportError("google-cloud-storage not installed. pip install google-cloud-storage")
            if not settings.GCP_PROJECT or not settings.GCS_BUCKET:
                raise ValueError("GCP_PROJECT or GCS_BUCKET missing in .env")
            self.client = storage.Client(project=settings.GCP_PROJECT)
            logger.info(f"Initialized GCS client for bucket {settings.GCS_BUCKET}")
        else:
            raise ValueError("Unsupported storage_type. Use 's3' or 'gcs'")

    def write(self, data: bytes | str, key: str):
        try:
            if isinstance(data, str):
                body = data.encode("utf-8")
            else:
                body = data
            if self.storage_type == "s3":
                self.client.put_object(Bucket=settings.S3_BUCKET, Key=key, Body=body)
                logger.info(f"Uploaded to s3://{settings.S3_BUCKET}/{key}")
            else:
                bucket = self.client.bucket(settings.GCS_BUCKET)
                blob = bucket.blob(key)
                blob.upload_from_string(body)
                logger.info(f"Uploaded to gs://{settings.GCS_BUCKET}/{key}")
        except Exception as e:
            logger.error(f"Upload failed: {e}")
            raise

    def read(self, key: str) -> bytes:
        try:
            if self.storage_type == "s3":
                resp = self.client.get_object(Bucket=settings.S3_BUCKET, Key=key)
                return resp["Body"].read()
            else:
                bucket = self.client.bucket(settings.GCS_BUCKET)
                blob = bucket.blob(key)
                return blob.download_as_bytes()
        except Exception as e:
            logger.error(f"Download failed: {e}")
            raise
