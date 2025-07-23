from abc import ABC, abstractmethod
from fastapi import UploadFile
from typing import AsyncGenerator

class BaseStorage(ABC):
    """
    Abstract base class for a storage backend.
    """

    @abstractmethod
    async def upload(self, file: UploadFile, tenant_id: str) -> str:
        """
        Uploads a file and returns its identifier (e.g., CID for IPFS, object name for MinIO).
        """
        pass

    @abstractmethod
    async def download(self, identifier: str, tenant_id: str) -> AsyncGenerator[bytes, None]:
        """
        Downloads a file and yields its content as a byte stream.
        """
        pass 