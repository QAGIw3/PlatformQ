"""Query builder for unified data access"""

from typing import Any, Dict, List, Optional, Union, Type, Callable
from datetime import datetime
from enum import Enum
import json

from .models import BaseModel
from .exceptions import QueryError


class QueryOperator(Enum):
    """Supported query operators"""
    EQ = "="
    NE = "!="
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="
    IN = "IN"
    NOT_IN = "NOT IN"
    LIKE = "LIKE"
    ILIKE = "ILIKE"
    BETWEEN = "BETWEEN"
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"
    EXISTS = "EXISTS"
    NOT_EXISTS = "NOT EXISTS"


class OrderDirection(Enum):
    """Sort order directions"""
    ASC = "ASC"
    DESC = "DESC"


class JoinType(Enum):
    """Join types for cross-store queries"""
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"


class QueryCondition:
    """Represents a single query condition"""
    
    def __init__(self, field: str, operator: QueryOperator, value: Any = None):
        self.field = field
        self.operator = operator
        self.value = value
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert condition to dictionary"""
        return {
            'field': self.field,
            'operator': self.operator.value,
            'value': self.value
        }
    
    def __repr__(self):
        if self.value is None:
            return f"{self.field} {self.operator.value}"
        return f"{self.field} {self.operator.value} {self.value}"


class Query:
    """Fluent query builder for data access"""
    
    def __init__(self, model_class: Type[BaseModel]):
        self.model_class = model_class
        self.conditions: List[QueryCondition] = []
        self.order_by_fields: List[tuple] = []
        self.limit_value: Optional[int] = None
        self.offset_value: Optional[int] = None
        self.select_fields: Optional[List[str]] = None
        self.joins: List[Dict[str, Any]] = []
        self.group_by_fields: List[str] = []
        self.having_conditions: List[QueryCondition] = []
        self._distinct = False
        self._for_update = False
    
    def filter(self, **kwargs) -> 'Query':
        """Add filter conditions using kwargs syntax"""
        for field, value in kwargs.items():
            # Parse field operators (e.g., created_at__gte)
            if '__' in field:
                field_name, op_suffix = field.rsplit('__', 1)
                operator = self._parse_operator_suffix(op_suffix)
            else:
                field_name = field
                operator = QueryOperator.EQ
            
            # Handle None values
            if value is None:
                if operator == QueryOperator.EQ:
                    operator = QueryOperator.IS_NULL
                elif operator == QueryOperator.NE:
                    operator = QueryOperator.IS_NOT_NULL
            
            self.conditions.append(QueryCondition(field_name, operator, value))
        
        return self
    
    def where(self, field: str, operator: Union[str, QueryOperator], value: Any = None) -> 'Query':
        """Add a where condition with explicit operator"""
        if isinstance(operator, str):
            operator = QueryOperator(operator)
        
        self.conditions.append(QueryCondition(field, operator, value))
        return self
    
    def or_where(self, *conditions: QueryCondition) -> 'Query':
        """Add OR conditions (requires special handling)"""
        # This would need to be implemented with proper OR logic
        # For now, we'll store them separately
        if not hasattr(self, 'or_conditions'):
            self.or_conditions = []
        self.or_conditions.append(conditions)
        return self
    
    def order_by(self, field: str, direction: Union[str, OrderDirection] = OrderDirection.ASC) -> 'Query':
        """Add order by clause"""
        if isinstance(direction, str):
            if direction.startswith('-'):
                field = direction[1:]
                direction = OrderDirection.DESC
            else:
                direction = OrderDirection.ASC if direction.upper() == 'ASC' else OrderDirection.DESC
        
        self.order_by_fields.append((field, direction))
        return self
    
    def limit(self, count: int) -> 'Query':
        """Set result limit"""
        self.limit_value = count
        return self
    
    def offset(self, count: int) -> 'Query':
        """Set result offset"""
        self.offset_value = count
        return self
    
    def select(self, *fields: str) -> 'Query':
        """Select specific fields"""
        self.select_fields = list(fields)
        return self
    
    def distinct(self) -> 'Query':
        """Make query return distinct results"""
        self._distinct = True
        return self
    
    def join(self, other_model: Type[BaseModel], on: str, join_type: JoinType = JoinType.INNER) -> 'Query':
        """Add a join clause for cross-store queries"""
        self.joins.append({
            'model': other_model,
            'on': on,
            'type': join_type
        })
        return self
    
    def group_by(self, *fields: str) -> 'Query':
        """Add group by clause"""
        self.group_by_fields.extend(fields)
        return self
    
    def having(self, field: str, operator: Union[str, QueryOperator], value: Any = None) -> 'Query':
        """Add having clause"""
        if isinstance(operator, str):
            operator = QueryOperator(operator)
        
        self.having_conditions.append(QueryCondition(field, operator, value))
        return self
    
    def for_update(self) -> 'Query':
        """Add FOR UPDATE clause for locking"""
        self._for_update = True
        return self
    
    def _parse_operator_suffix(self, suffix: str) -> QueryOperator:
        """Parse Django-style operator suffixes"""
        suffix_map = {
            'eq': QueryOperator.EQ,
            'ne': QueryOperator.NE,
            'gt': QueryOperator.GT,
            'gte': QueryOperator.GTE,
            'lt': QueryOperator.LT,
            'lte': QueryOperator.LTE,
            'in': QueryOperator.IN,
            'nin': QueryOperator.NOT_IN,
            'like': QueryOperator.LIKE,
            'ilike': QueryOperator.ILIKE,
            'between': QueryOperator.BETWEEN,
            'isnull': QueryOperator.IS_NULL,
            'isnotnull': QueryOperator.IS_NOT_NULL,
            'exists': QueryOperator.EXISTS,
            'notexists': QueryOperator.NOT_EXISTS
        }
        
        return suffix_map.get(suffix, QueryOperator.EQ)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert query to dictionary representation"""
        query_dict = {
            'model': self.model_class.__name__,
            'conditions': [c.to_dict() for c in self.conditions],
        }
        
        if self.order_by_fields:
            query_dict['order_by'] = [(f, d.value) for f, d in self.order_by_fields]
        
        if self.limit_value is not None:
            query_dict['limit'] = self.limit_value
        
        if self.offset_value is not None:
            query_dict['offset'] = self.offset_value
        
        if self.select_fields:
            query_dict['select'] = self.select_fields
        
        if self.joins:
            query_dict['joins'] = self.joins
        
        if self.group_by_fields:
            query_dict['group_by'] = self.group_by_fields
        
        if self.having_conditions:
            query_dict['having'] = [c.to_dict() for c in self.having_conditions]
        
        if self._distinct:
            query_dict['distinct'] = True
        
        if self._for_update:
            query_dict['for_update'] = True
        
        return query_dict
    
    def __repr__(self):
        parts = [f"Query({self.model_class.__name__})"]
        
        if self.conditions:
            parts.append(f"where({', '.join(str(c) for c in self.conditions)})")
        
        if self.order_by_fields:
            order_parts = [f"{f} {d.value}" for f, d in self.order_by_fields]
            parts.append(f"order_by({', '.join(order_parts)})")
        
        if self.limit_value:
            parts.append(f"limit({self.limit_value})")
        
        if self.offset_value:
            parts.append(f"offset({self.offset_value})")
        
        return '.'.join(parts)


class QueryBuilder:
    """Advanced query builder with store-specific optimizations"""
    
    def __init__(self):
        self.store_handlers = {}
    
    def register_store_handler(self, store_type: str, handler: Callable):
        """Register a handler for translating queries to store-specific format"""
        self.store_handlers[store_type] = handler
    
    def build_cassandra_query(self, query: Query) -> tuple:
        """Build CQL query from Query object"""
        model = query.model_class
        table = f"{model.__keyspace__}.{model.__tablename__}"
        
        # Build SELECT clause
        if query.select_fields:
            fields = ', '.join(query.select_fields)
        else:
            fields = '*'
        
        cql = f"SELECT {fields} FROM {table}"
        params = []
        
        # Build WHERE clause
        if query.conditions:
            where_parts = []
            for condition in query.conditions:
                if condition.operator == QueryOperator.EQ:
                    where_parts.append(f"{condition.field} = ?")
                    params.append(condition.value)
                elif condition.operator == QueryOperator.IN:
                    placeholders = ', '.join(['?' for _ in condition.value])
                    where_parts.append(f"{condition.field} IN ({placeholders})")
                    params.extend(condition.value)
                elif condition.operator in [QueryOperator.GT, QueryOperator.GTE, QueryOperator.LT, QueryOperator.LTE]:
                    where_parts.append(f"{condition.field} {condition.operator.value} ?")
                    params.append(condition.value)
                # Add more operator support as needed
            
            if where_parts:
                cql += " WHERE " + " AND ".join(where_parts)
        
        # ORDER BY (limited in Cassandra)
        if query.order_by_fields:
            order_parts = [f"{f} {d.value}" for f, d in query.order_by_fields]
            cql += " ORDER BY " + ", ".join(order_parts)
        
        # LIMIT
        if query.limit_value:
            cql += f" LIMIT {query.limit_value}"
        
        # ALLOW FILTERING (use cautiously)
        if query.conditions and not self._has_partition_key_condition(query, model):
            cql += " ALLOW FILTERING"
        
        return cql, params
    
    def build_elasticsearch_query(self, query: Query) -> Dict[str, Any]:
        """Build Elasticsearch query from Query object"""
        es_query = {
            'query': {'bool': {'must': [], 'must_not': [], 'should': []}},
        }
        
        # Build query conditions
        for condition in query.conditions:
            if condition.operator == QueryOperator.EQ:
                es_query['query']['bool']['must'].append({
                    'term': {condition.field: condition.value}
                })
            elif condition.operator == QueryOperator.NE:
                es_query['query']['bool']['must_not'].append({
                    'term': {condition.field: condition.value}
                })
            elif condition.operator == QueryOperator.LIKE:
                es_query['query']['bool']['must'].append({
                    'wildcard': {condition.field: f"*{condition.value}*"}
                })
            elif condition.operator == QueryOperator.IN:
                es_query['query']['bool']['must'].append({
                    'terms': {condition.field: condition.value}
                })
            elif condition.operator in [QueryOperator.GT, QueryOperator.GTE, QueryOperator.LT, QueryOperator.LTE]:
                range_query = {'range': {condition.field: {}}}
                if condition.operator == QueryOperator.GT:
                    range_query['range'][condition.field]['gt'] = condition.value
                elif condition.operator == QueryOperator.GTE:
                    range_query['range'][condition.field]['gte'] = condition.value
                elif condition.operator == QueryOperator.LT:
                    range_query['range'][condition.field]['lt'] = condition.value
                elif condition.operator == QueryOperator.LTE:
                    range_query['range'][condition.field]['lte'] = condition.value
                es_query['query']['bool']['must'].append(range_query)
        
        # Clean up empty arrays
        if not es_query['query']['bool']['must']:
            del es_query['query']['bool']['must']
        if not es_query['query']['bool']['must_not']:
            del es_query['query']['bool']['must_not']
        if not es_query['query']['bool']['should']:
            del es_query['query']['bool']['should']
        
        # If no conditions, match all
        if not es_query['query']['bool']:
            es_query['query'] = {'match_all': {}}
        
        # Sorting
        if query.order_by_fields:
            es_query['sort'] = []
            for field, direction in query.order_by_fields:
                es_query['sort'].append({field: {'order': direction.value.lower()}})
        
        # Pagination
        if query.offset_value:
            es_query['from'] = query.offset_value
        if query.limit_value:
            es_query['size'] = query.limit_value
        
        # Field selection
        if query.select_fields:
            es_query['_source'] = query.select_fields
        
        return es_query
    
    def build_sql_query(self, query: Query, dialect: str = 'postgresql') -> tuple:
        """Build SQL query from Query object (for Trino/Presto)"""
        model = query.model_class
        table = f"{model.__keyspace__}.{model.__tablename__}"
        
        # Build SELECT clause
        if query._distinct:
            select_clause = "SELECT DISTINCT"
        else:
            select_clause = "SELECT"
        
        if query.select_fields:
            fields = ', '.join(query.select_fields)
        else:
            fields = '*'
        
        sql = f"{select_clause} {fields} FROM {table}"
        params = {}
        param_counter = 1
        
        # Build JOINs
        for join in query.joins:
            join_model = join['model']
            join_table = f"{join_model.__keyspace__}.{join_model.__tablename__}"
            sql += f" {join['type'].value} JOIN {join_table} ON {join['on']}"
        
        # Build WHERE clause
        if query.conditions:
            where_parts = []
            for condition in query.conditions:
                param_name = f"p{param_counter}"
                
                if condition.operator == QueryOperator.EQ:
                    where_parts.append(f"{condition.field} = :{param_name}")
                    params[param_name] = condition.value
                    param_counter += 1
                elif condition.operator == QueryOperator.IN:
                    in_params = []
                    for val in condition.value:
                        param_name = f"p{param_counter}"
                        in_params.append(f":{param_name}")
                        params[param_name] = val
                        param_counter += 1
                    where_parts.append(f"{condition.field} IN ({', '.join(in_params)})")
                elif condition.operator == QueryOperator.BETWEEN:
                    param1 = f"p{param_counter}"
                    param2 = f"p{param_counter + 1}"
                    where_parts.append(f"{condition.field} BETWEEN :{param1} AND :{param2}")
                    params[param1] = condition.value[0]
                    params[param2] = condition.value[1]
                    param_counter += 2
                elif condition.operator == QueryOperator.LIKE:
                    where_parts.append(f"{condition.field} LIKE :{param_name}")
                    params[param_name] = condition.value
                    param_counter += 1
                elif condition.operator in [QueryOperator.IS_NULL, QueryOperator.IS_NOT_NULL]:
                    where_parts.append(f"{condition.field} {condition.operator.value}")
                else:
                    where_parts.append(f"{condition.field} {condition.operator.value} :{param_name}")
                    params[param_name] = condition.value
                    param_counter += 1
            
            if where_parts:
                sql += " WHERE " + " AND ".join(where_parts)
        
        # GROUP BY
        if query.group_by_fields:
            sql += " GROUP BY " + ", ".join(query.group_by_fields)
        
        # HAVING
        if query.having_conditions:
            having_parts = []
            for condition in query.having_conditions:
                param_name = f"p{param_counter}"
                having_parts.append(f"{condition.field} {condition.operator.value} :{param_name}")
                params[param_name] = condition.value
                param_counter += 1
            sql += " HAVING " + " AND ".join(having_parts)
        
        # ORDER BY
        if query.order_by_fields:
            order_parts = [f"{f} {d.value}" for f, d in query.order_by_fields]
            sql += " ORDER BY " + ", ".join(order_parts)
        
        # LIMIT/OFFSET
        if query.limit_value:
            sql += f" LIMIT {query.limit_value}"
        if query.offset_value:
            sql += f" OFFSET {query.offset_value}"
        
        # FOR UPDATE
        if query._for_update:
            sql += " FOR UPDATE"
        
        return sql, params
    
    def _has_partition_key_condition(self, query: Query, model: Type[BaseModel]) -> bool:
        """Check if query has partition key condition (for Cassandra)"""
        # This would need to check the model's partition key definition
        # For now, assume 'id' is the partition key
        pk_field = model.get_primary_key_field()
        if pk_field:
            return any(c.field == pk_field.name for c in query.conditions)
        return False 