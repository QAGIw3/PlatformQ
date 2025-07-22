import hashlib
import json
from ..schemas import digital_asset as schemas

def compute_cid(asset: schemas.DigitalAssetCreate) -> str:
    """
    Computes a deterministic CID for a digital asset.
    This is a simplified example. A real implementation would use a
    proper CID library like multihash.
    """
    # Create a dictionary with the asset's core data
    content = {
        "asset_name": asset.asset_name,
        "asset_type": asset.asset_type,
        "owner_id": str(asset.owner_id),
        "source_tool": asset.source_tool,
        "raw_data_uri": asset.raw_data_uri,
        "tags": asset.tags,
        "metadata": asset.metadata,
    }
    
    # Sort keys for a deterministic hash
    json_str = json.dumps(content, sort_keys=True)
    
    # Use SHA-256 as the hashing algorithm
    return f"bafybeig{hashlib.sha256(json_str.encode()).hexdigest()}" 