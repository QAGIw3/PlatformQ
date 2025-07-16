import json
from fastapi import Depends
from .base import BaseStorage
from .minio import MinIOStorage
from .ipfs import IPFSStorage
from .arweave import ArweaveStorage
from ..core.config import settings
from ..api.deps import get_user_storage_config

def get_storage_backend(
    user_storage: dict = Depends(get_user_storage_config)
) -> BaseStorage:
    """
    Returns a storage backend instance based on the user's configuration.
    If the user has no configuration, it falls back to the system default.
    """
    backend_name = user_storage.get("backend") or settings.STORAGE_BACKEND
    config_str = user_storage.get("config")

    if backend_name == "minio":
        if config_str:
            # If user provides their own MinIO config
            config = json.loads(config_str)
            return MinIOStorage(
                host=config.get("host"),
                access_key=config.get("access_key"),
                secret_key=config.get("secret_key"),
            )
        # Fallback to system default MinIO
        return MinIOStorage(
            host=settings.MINIO_API_HOST,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
        )
    elif backend_name == "ipfs":
        if config_str:
            # If user provides their own IPFS node URL
            config = json.loads(config_str)
            return IPFSStorage(api_url=config.get("api_url"))
        # Fallback to system default IPFS
        return IPFSStorage(api_url=settings.IPFS_API_URL)
    elif backend_name == "arweave":
        # Arweave configuration is simpler for now, just the wallet file
        return ArweaveStorage(wallet_file=settings.ARWEAVE_WALLET_FILE)
    else:
        raise ValueError(f"Unsupported storage backend: {backend_name}")

# You might also want a dependency for getting the user-selected storage
# For now, we'll stick to a global one. 