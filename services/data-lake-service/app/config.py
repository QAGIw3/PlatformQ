from pydantic import BaseSettings

class Settings(BaseSettings):
    MINIO_ENDPOINT: str = "localhost:9000"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin"
    DATA_LAKE_BUCKET: str = "platformq-data-lake"
    SPARK_MASTER: str = "local[*]"
    SPARK_EXECUTOR_MEMORY: str = "4g"
    SPARK_DRIVER_MEMORY: str = "2g"
    PULSAR_URL: str = "pulsar://localhost:6650"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings() 