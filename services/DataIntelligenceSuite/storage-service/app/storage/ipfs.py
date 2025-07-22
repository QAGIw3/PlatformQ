import ipfshttpclient
from fastapi import UploadFile, HTTPException
from typing import AsyncGenerator
import asyncio

from .base import BaseStorage

# In a real app, this would come from config/vault
IPFS_API_URL = "/dns/platformq-ipfs-kubo/tcp/5001/http"

class IPFSStorage(BaseStorage):
    """
    IPFS storage backend.
    """
    def __init__(self, api_url: str = IPFS_API_URL):
        self._api_url = api_url

    async def upload(self, file: UploadFile, tenant_id: str) -> str:
        """
        Uploads a file to the IPFS node and returns its CID.
        The tenant_id is not used for IPFS but is part of the interface.
        """
        try:
            # ipfshttpclient is synchronous, run in a thread
            def _upload():
                with ipfshttpclient.connect(self._api_url) as client:
                    result = client.add(file.file, pin=True)
                    return result['Hash']
            
            loop = asyncio.get_event_loop()
            cid = await loop.run_in_executor(None, _upload)
            return cid
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to upload to IPFS: {e}")

    async def download(self, identifier: str, tenant_id: str) -> AsyncGenerator[bytes, None]:
        """
        Downloads a file from IPFS by its CID.
        The tenant_id is not used for IPFS but is part of the interface.
        """
        try:
            # ipfshttpclient is synchronous, run in a thread
            def _download():
                with ipfshttpclient.connect(self._api_url) as client:
                    return client.cat(identifier)

            loop = asyncio.get_event_loop()
            content = await loop.run_in_executor(None, _download)
            yield content
        except Exception as e:
            raise HTTPException(status_code=404, detail=f"Failed to download from IPFS: {e}") 