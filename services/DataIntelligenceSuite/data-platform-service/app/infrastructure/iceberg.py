"""
Iceberg Infrastructure Client

Manages Apache Iceberg table operations
"""

class IcebergCatalog:
    """Iceberg catalog for table management"""
    
    def __init__(self, catalog_name: str, warehouse_location: str, minio_client):
        self.catalog_name = catalog_name
        self.warehouse_location = warehouse_location
        self.minio_client = minio_client 