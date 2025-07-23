"""
Lakehouse management for data platform
"""

from typing import Optional, Dict, Any
from data_intelligence_common.core.lakehouse import LakehouseManager, TableFormat

from ..config import settings


class DataPlatformLakehouse:
    """Enhanced lakehouse management for data platform"""
    
    def __init__(self, lakehouse_manager: LakehouseManager):
        self.lakehouse = lakehouse_manager
        self.default_format = TableFormat.ICEBERG
        
    async def create_managed_table(
        self,
        table_name: str,
        schema: Dict[str, Any],
        partition_by: Optional[List[str]] = None
    ):
        """Create a managed table with platform defaults"""
        return await self.lakehouse.create_table(
            table_name,
            schema,
            format=self.default_format,
            properties={
                "managed_by": "data-platform-service",
                "created_at": datetime.utcnow().isoformat()
            }
        )
