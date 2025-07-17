"""Cassandra adapter for unified data access"""

import logging
from typing import Any, Dict, List, Optional, Type
from datetime import datetime
from cassandra.cluster import Session
from cassandra.query import SimpleStatement, ConsistencyLevel
from cassandra import DriverException

from .base import StoreAdapter
from ..models import BaseModel, FieldType
from ..query import Query, QueryBuilder
from ..exceptions import DataAccessError, NotFoundError

logger = logging.getLogger(__name__)


class CassandraAdapter(StoreAdapter):
    """Cassandra-specific adapter implementation"""
    
    def __init__(self, session: Session, consistency_level: ConsistencyLevel = ConsistencyLevel.QUORUM):
        self.session = session
        self.consistency_level = consistency_level
        self.query_builder = QueryBuilder()
    
    async def create(self, instance: BaseModel) -> BaseModel:
        """Create a new instance in Cassandra"""
        try:
            # Ensure keyspace and table exist
            await self._ensure_table_exists(instance.__class__)
            
            # Build INSERT query
            table = f"{instance.__keyspace__}.{instance.__tablename__}"
            data = instance.to_dict()
            
            columns = list(data.keys())
            values = list(data.values())
            placeholders = ['?' for _ in columns]
            
            cql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
            
            statement = SimpleStatement(cql, consistency_level=self.consistency_level)
            self.session.execute(statement, values)
            
            return instance
            
        except DriverException as e:
            logger.error(f"Failed to create in Cassandra: {e}")
            raise DataAccessError(f"Failed to create {instance.__class__.__name__}: {e}")
    
    async def get_by_id(self, model_class: Type[BaseModel], id: Any, tenant_id: Optional[str] = None) -> Optional[BaseModel]:
        """Get instance by ID from Cassandra"""
        try:
            table = f"{model_class.__keyspace__}.{model_class.__tablename__}"
            pk_field = model_class.get_primary_key_field()
            
            if not pk_field:
                raise DataAccessError(f"No primary key defined for {model_class.__name__}")
            
            cql = f"SELECT * FROM {table} WHERE {pk_field.name} = ?"
            params = [id]
            
            # Add tenant filter if multi-tenant
            if tenant_id and hasattr(model_class, 'tenant_id'):
                cql += " AND tenant_id = ?"
                params.append(tenant_id)
            
            statement = SimpleStatement(cql, consistency_level=self.consistency_level)
            result = self.session.execute(statement, params)
            
            row = result.one()
            if row:
                return self._row_to_model(row, model_class)
            
            return None
            
        except DriverException as e:
            logger.error(f"Failed to get by ID from Cassandra: {e}")
            raise DataAccessError(f"Failed to get {model_class.__name__} by ID: {e}")
    
    async def update(self, instance: BaseModel, tenant_id: Optional[str] = None) -> BaseModel:
        """Update an instance in Cassandra"""
        try:
            table = f"{instance.__keyspace__}.{instance.__tablename__}"
            pk_field = instance.get_primary_key_field()
            
            if not pk_field:
                raise DataAccessError(f"No primary key defined for {instance.__class__.__name__}")
            
            data = instance.to_dict()
            pk_value = data.pop(pk_field.name)
            
            # Build UPDATE query
            set_clauses = [f"{col} = ?" for col in data.keys()]
            values = list(data.values())
            
            cql = f"UPDATE {table} SET {', '.join(set_clauses)} WHERE {pk_field.name} = ?"
            values.append(pk_value)
            
            # Add tenant filter if multi-tenant
            if tenant_id and hasattr(instance, 'tenant_id'):
                cql += " AND tenant_id = ?"
                values.append(tenant_id)
            
            statement = SimpleStatement(cql, consistency_level=self.consistency_level)
            self.session.execute(statement, values)
            
            return instance
            
        except DriverException as e:
            logger.error(f"Failed to update in Cassandra: {e}")
            raise DataAccessError(f"Failed to update {instance.__class__.__name__}: {e}")
    
    async def delete(self, model_class: Type[BaseModel], id: Any, tenant_id: Optional[str] = None) -> bool:
        """Delete an instance from Cassandra"""
        try:
            table = f"{model_class.__keyspace__}.{model_class.__tablename__}"
            pk_field = model_class.get_primary_key_field()
            
            if not pk_field:
                raise DataAccessError(f"No primary key defined for {model_class.__name__}")
            
            cql = f"DELETE FROM {table} WHERE {pk_field.name} = ?"
            params = [id]
            
            # Add tenant filter if multi-tenant
            if tenant_id and hasattr(model_class, 'tenant_id'):
                cql += " AND tenant_id = ?"
                params.append(tenant_id)
            
            statement = SimpleStatement(cql, consistency_level=self.consistency_level)
            self.session.execute(statement, params)
            
            return True
            
        except DriverException as e:
            logger.error(f"Failed to delete from Cassandra: {e}")
            raise DataAccessError(f"Failed to delete {model_class.__name__}: {e}")
    
    async def find(self, query: Query) -> List[BaseModel]:
        """Find instances matching query in Cassandra"""
        try:
            # Build CQL query
            cql, params = self.query_builder.build_cassandra_query(query)
            
            statement = SimpleStatement(cql, consistency_level=self.consistency_level)
            result = self.session.execute(statement, params)
            
            instances = []
            for row in result:
                instance = self._row_to_model(row, query.model_class)
                instances.append(instance)
            
            return instances
            
        except DriverException as e:
            logger.error(f"Failed to find in Cassandra: {e}")
            raise DataAccessError(f"Failed to find {query.model_class.__name__}: {e}")
    
    async def count(self, query: Query) -> int:
        """Count instances matching query in Cassandra"""
        try:
            # Build count query
            cql, params = self.query_builder.build_cassandra_query(query)
            
            # Replace SELECT clause with COUNT(*)
            cql = cql.replace("SELECT *", "SELECT COUNT(*)", 1)
            cql = cql.replace(f"SELECT {', '.join(query.select_fields or [])}", "SELECT COUNT(*)", 1)
            
            statement = SimpleStatement(cql, consistency_level=self.consistency_level)
            result = self.session.execute(statement, params)
            
            row = result.one()
            return row[0] if row else 0
            
        except DriverException as e:
            logger.error(f"Failed to count in Cassandra: {e}")
            raise DataAccessError(f"Failed to count {query.model_class.__name__}: {e}")
    
    async def execute_raw(self, query: str, params: Optional[List[Any]] = None) -> Any:
        """Execute raw CQL query"""
        try:
            statement = SimpleStatement(query, consistency_level=self.consistency_level)
            result = self.session.execute(statement, params or [])
            return result
            
        except DriverException as e:
            logger.error(f"Failed to execute raw query in Cassandra: {e}")
            raise DataAccessError(f"Failed to execute raw query: {e}")
    
    async def bulk_create(self, instances: List[BaseModel]) -> List[BaseModel]:
        """Bulk create instances using batch"""
        if not instances:
            return []
        
        try:
            # Group by table
            from collections import defaultdict
            by_table = defaultdict(list)
            
            for instance in instances:
                table = f"{instance.__keyspace__}.{instance.__tablename__}"
                by_table[table].append(instance)
            
            # Execute batches per table
            created = []
            for table, table_instances in by_table.items():
                # Build batch CQL
                batch_cql = "BEGIN BATCH\n"
                all_params = []
                
                for instance in table_instances:
                    data = instance.to_dict()
                    columns = list(data.keys())
                    values = list(data.values())
                    placeholders = ['?' for _ in columns]
                    
                    batch_cql += f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(placeholders)});\n"
                    all_params.extend(values)
                
                batch_cql += "APPLY BATCH;"
                
                statement = SimpleStatement(batch_cql, consistency_level=self.consistency_level)
                self.session.execute(statement, all_params)
                
                created.extend(table_instances)
            
            return created
            
        except DriverException as e:
            logger.error(f"Failed to bulk create in Cassandra: {e}")
            raise DataAccessError(f"Failed to bulk create: {e}")
    
    def _row_to_model(self, row: Any, model_class: Type[BaseModel]) -> BaseModel:
        """Convert Cassandra row to model instance"""
        data = {}
        
        # Convert row to dict
        for field_name, field_def in model_class._fields.items():
            if hasattr(row, field_name):
                value = getattr(row, field_name)
                
                # Type conversions
                if value is not None:
                    if field_def.field_type == FieldType.DATETIME and isinstance(value, str):
                        value = datetime.fromisoformat(value)
                    elif field_def.field_type == FieldType.JSON and isinstance(value, str):
                        import json
                        value = json.loads(value)
                
                data[field_name] = value
        
        return model_class(**data)
    
    async def _ensure_table_exists(self, model_class: Type[BaseModel]):
        """Ensure table exists for model (simplified - real implementation would be more complex)"""
        # This is a simplified version - in production, you'd want more sophisticated
        # schema management, possibly using migrations
        try:
            # Create keyspace if not exists
            keyspace = model_class.__keyspace__
            self.session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {keyspace}
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 3}}
            """)
            
            # For now, we assume tables are created externally
            # In production, you'd have a proper migration system
            
        except Exception as e:
            logger.warning(f"Could not ensure table exists: {e}") 