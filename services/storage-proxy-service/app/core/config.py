from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    STORAGE_BACKEND: str = "minio"
    AUTH_SERVICE_URL: str = "http://auth-service:8000"
    
    # MinIO Settings
    MINIO_API_HOST: str = "minio:9000"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin"
    
    # IPFS Settings
    IPFS_API_URL: str = "/dns/platformq-ipfs-kubo/tcp/5001/http"

    # Arweave Settings
    ARWEAVE_WALLET_FILE: str = "arweave-wallet.json"

    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'

settings = Settings() 