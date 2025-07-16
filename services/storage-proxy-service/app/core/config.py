from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    STORAGE_BACKEND: str = "minio"
    AUTH_SERVICE_URL: str = "http://auth-service:8000"
    
    # Arweave Settings
    ARWEAVE_WALLET_FILE: str = "arweave-wallet.json"

    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'

settings = Settings() 