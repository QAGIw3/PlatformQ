import arweave
from arweave.wallet import Wallet
from fastapi import UploadFile, HTTPException
from typing import AsyncGenerator
import asyncio

from .base import BaseStorage

# In a real app, this wallet should be managed securely via Vault or another secrets manager.
# For this example, we assume a wallet file is present in the service's root directory.
ARWEAVE_WALLET_FILE = "arweave-wallet.json"

class ArweaveStorage(BaseStorage):
    """
    Arweave storage backend.
    """
    def __init__(self, wallet_file: str = ARWEAVE_WALLET_FILE):
        try:
            self._wallet = Wallet(wallet_file)
        except FileNotFoundError:
            raise RuntimeError(f"Arweave wallet file not found at: {wallet_file}. Please ensure it is mounted in the service.")
        
    async def upload(self, file: UploadFile, tenant_id: str) -> str:
        """
        Uploads file data to Arweave and returns the transaction ID.
        """
        try:
            # arweave-python-client is synchronous, so we run it in a thread
            def _upload():
                file_data = file.file.read()
                tx = arweave.Transaction(self._wallet, data=file_data)
                tx.add_tag('Content-Type', file.content_type)
                tx.add_tag('App-Name', 'platformQ')
                tx.add_tag('Tenant-ID', tenant_id)
                
                tx.sign()
                tx.send()
                return tx.id

            loop = asyncio.get_event_loop()
            tx_id = await loop.run_in_executor(None, _upload)
            return tx_id
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to upload to Arweave: {e}")

    async def download(self, identifier: str, tenant_id: str) -> AsyncGenerator[bytes, None]:
        """
        Downloads a file from Arweave by its transaction ID.
        """
        try:
            # arweave-python-client is synchronous
            def _download():
                tx = arweave.Transaction(self._wallet, id=identifier)
                return tx.get_data()

            loop = asyncio.get_event_loop()
            data = await loop.run_in_executor(None, _download)
            yield data
        except Exception as e:
            raise HTTPException(status_code=404, detail=f"Failed to download from Arweave: {e}") 