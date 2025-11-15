import logging

try:
    import boto3
except ImportError:
    boto3 = None

try:
    from google.cloud import storage
except ImportError:
    storage = None

from ingestion_framework.core.config import settings
from ingestion_framework.utils.logger import get_logger

logger = get_logger(__name__)

class CloudStorage:
    def __init__(self, storage_type="s3"):
        self.storage_type = storage_type
        
        if storage_type == "s3":
            if not boto3:
                raise ImportError("boto3 is not installed. Install with: pip install boto3")
            
            if not settings.AWS_ACCESS_KEY_ID or not settings.AWS_SECRET_ACCESS_KEY:
                raise ValueError("AWS credentials not configured in .env file")
            
            self.client = boto3.client(
                "s3",
                aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
                region_name=settings.AWS_REGION,
            )
            logger.info(f"Initialized S3 client for bucket: {settings.S3_BUCKET}")
            
        elif storage_type == "gcs":
            if not storage:
                raise ImportError("google-cloud-storage is not installed. Install with: pip install google-cloud-storage")
            
            if not settings.GCP_PROJECT:
                raise ValueError("GCP_PROJECT not configured in .env file")
            
            self.client = storage.Client(project=settings.GCP_PROJECT)
            logger.info(f"Initialized GCS client for bucket: {settings.GCS_BUCKET}")
            
        else:
            raise ValueError(f"Unsupported storage type: {storage_type}. Use 's3' or 'gcs'")

    def write(self, data, key):
        """Write data to cloud storage"""
        try:
            if self.storage_type == "s3":
                if not settings.S3_BUCKET:
                    raise ValueError("S3_BUCKET not configured in .env file")
                
                self.client.put_object(
                    Bucket=settings.S3_BUCKET,
                    Key=key,
                    Body=data if isinstance(data, bytes) else data.encode()
                )
                logger.info(f"✅ Uploaded to S3: s3://{settings.S3_BUCKET}/{key}")
                
            elif self.storage_type == "gcs":
                if not settings.GCS_BUCKET:
                    raise ValueError("GCS_BUCKET not configured in .env file")
                
                bucket = self.client.bucket(settings.GCS_BUCKET)
                blob = bucket.blob(key)
                blob.upload_from_string(data)
                logger.info(f"✅ Uploaded to GCS: gs://{settings.GCS_BUCKET}/{key}")
                
        except Exception as e:
            logger.error(f"❌ Upload failed: {e}")
            raise

    def read(self, key):
        """Read data from cloud storage"""
        try:
            if self.storage_type == "s3":
                response = self.client.get_object(Bucket=settings.S3_BUCKET, Key=key)
                data = response['Body'].read()
                logger.info(f"✅ Downloaded from S3: {key}")
                return data
                
            elif self.storage_type == "gcs":
                bucket = self.client.bucket(settings.GCS_BUCKET)
                blob = bucket.blob(key)
                data = blob.download_as_bytes()
                logger.info(f"✅ Downloaded from GCS: {key}")
                return data
                
        except Exception as e:
            logger.error(f"❌ Download failed: {e}")
            raise
