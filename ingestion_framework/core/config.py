from pydantic import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    # Database settings
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_USER: str
    DB_PASSWORD: str
    DB_NAME: str

    # API settings
    API_KEY: str

    # Raw layer settings
    RAW_ZONE_PATH: str = "raw_zone"

    # AWS settings
    AWS_ACCESS_KEY_ID: Optional[str] = None
    AWS_SECRET_ACCESS_KEY: Optional[str] = None
    AWS_REGION: str = "us-east-1"
    S3_BUCKET: Optional[str] = None

    # GCP settings
    GCP_PROJECT: Optional[str] = None
    GCS_BUCKET: Optional[str] = None

    class Config:
        env_file = ".env"

settings = Settings()