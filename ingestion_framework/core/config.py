from pydantic import BaseSettings, Field
from typing import Optional

class Settings(BaseSettings):
    # Database
    DB_HOST: str = Field("localhost", env="DB_HOST")
    DB_PORT: int = Field(5432, env="DB_PORT")
    DB_USER: Optional[str] = Field(None, env="DB_USER")
    DB_PASSWORD: Optional[str] = Field(None, env="DB_PASSWORD")
    DB_NAME: Optional[str] = Field(None, env="DB_NAME")

    # Salesforce / API
    SALESFORCE_INSTANCE: Optional[str] = Field(None, env="SALESFORCE_INSTANCE")
    SALESFORCE_CLIENT_ID: Optional[str] = Field(None, env="SALESFORCE_CLIENT_ID")
    SALESFORCE_CLIENT_SECRET: Optional[str] = Field(None, env="SALESFORCE_CLIENT_SECRET")
    SALESFORCE_USERNAME: Optional[str] = Field(None, env="SALESFORCE_USERNAME")
    SALESFORCE_PASSWORD: Optional[str] = Field(None, env="SALESFORCE_PASSWORD")
    RAW_ZONE_PATH: str = Field("raw_zone", env="RAW_ZONE_PATH")
    PROCESSED_ZONE_PATH: str = Field("processed_zone", env="PROCESSED_ZONE_PATH")

    # AWS
    AWS_ACCESS_KEY_ID: Optional[str] = Field(None, env="AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY: Optional[str] = Field(None, env="AWS_SECRET_ACCESS_KEY")
    AWS_REGION: str = Field("us-east-1", env="AWS_REGION")
    S3_BUCKET: Optional[str] = Field(None, env="S3_BUCKET")

    # GCP
    GCP_PROJECT: Optional[str] = Field(None, env="GCP_PROJECT")
    GCS_BUCKET: Optional[str] = Field(None, env="GCS_BUCKET")

    # LLM / OpenAI
    OPENAI_API_KEY: Optional[str] = Field(None, env="OPENAI_API_KEY")
    LLM_MODEL: str = Field("gpt-3.5-turbo", env="LLM_MODEL")

    LOG_LEVEL: str = Field("INFO", env="LOG_LEVEL")
    AUTO_APPLY_MIGRATIONS: bool = Field(False, env="AUTO_APPLY_MIGRATIONS")
    MIGRATION_TARGET: str = Field("postgres", env="MIGRATION_TARGET")  # or "mongo"
    POSTGRES_TABLE_NAME: Optional[str] = Field(None, env="POSTGRES_TABLE_NAME")

    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings()