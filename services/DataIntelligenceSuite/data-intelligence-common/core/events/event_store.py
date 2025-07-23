"""
Event Store for DataIntelligenceSuite

Provides event persistence and querying capabilities.
"""

import logging
from typing import Any, Dict, Optional, List, Tuple, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import json
import asyncio

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, BatchStatement
from cassandra import ConsistencyLevel

from .event_bus import Event

logger = logging.getLogger(__name__)


class QueryOperator(Enum):
    """Query operators"""
    EQUALS = "="
    NOT_EQUALS = "!="
    GREATER_THAN = ">"
    LESS_THAN = "<"
    GREATER_EQUAL = ">="
    LESS_EQUAL = "<="
    IN = "IN"
    CONTAINS = "CONTAINS"


@dataclass
class EventQuery:
    """Event query specification"""
    # Time range
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    
    # Filters
    event_types: Optional[List[str]] = None
    sources: Optional[List[str]] = None
    correlation_ids: Optional[List[str]] = None
    
    # Custom filters
    filters: List[Tuple[str, QueryOperator, Any]] = field(default_factory=list)
    
    # Pagination
    limit: int = 100
    offset: int = 0
    
    # Ordering
    order_by: str = "timestamp"
    order_desc: bool = True


class EventStore:
    """
    Event store for persistent event storage and retrieval.
    
    Features:
    - Event persistence to Cassandra
    - Time-based partitioning
    - Efficient querying
    - Event replay capabilities
    - Snapshot support
    """
    
    def __init__(
        self,
        cassandra_hosts: List[str] = ["localhost"],
        keyspace: str = "event_store",
        replication_factor: int = 3
    ):
        self.hosts = cassandra_hosts
        self.keyspace = keyspace
        self.replication_factor = replication_factor
        
        self._cluster: Optional[Cluster] = None
        self._session = None
        
    async def initialize(self):
        """Initialize event store"""
        # Connect to Cassandra
        self._cluster = Cluster(self.hosts)
        self._session = self._cluster.connect()
        
        # Create keyspace
        await self._create_keyspace()
        
        # Create tables
        await self._create_tables()
        
        logger.info("Initialized event store")
        
    async def shutdown(self):
        """Shutdown event store"""
        if self._cluster:
            self._cluster.shutdown()
            
        logger.info("Shutdown event store")
        
    async def _create_keyspace(self):
        """Create keyspace if not exists"""
        query = f"""
        CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
        WITH REPLICATION = {{
            'class': 'SimpleStrategy',
            'replication_factor': {self.replication_factor}
        }}
        """
        
        self._session.execute(query)
        self._session.set_keyspace(self.keyspace)
        
    async def _create_tables(self):
        """Create event tables"""
        # Events table with time-based partitioning
        events_table = """
        CREATE TABLE IF NOT EXISTS events (
            partition_key text,
            event_id text,
            timestamp timestamp,
            event_type text,
            source text,
            correlation_id text,
            causation_id text,
            priority int,
            headers map<text, text>,
            payload text,
            PRIMARY KEY (partition_key, timestamp, event_id)
        ) WITH CLUSTERING ORDER BY (timestamp DESC, event_id ASC)
        """
        
        # Event index by type
        type_index = """
        CREATE INDEX IF NOT EXISTS events_by_type
        ON events (event_type)
        """
        
        # Event index by correlation
        correlation_index = """
        CREATE INDEX IF NOT EXISTS events_by_correlation
        ON events (correlation_id)
        """
        
        # Snapshots table
        snapshots_table = """
        CREATE TABLE IF NOT EXISTS snapshots (
            aggregate_id text,
            version bigint,
            timestamp timestamp,
            event_id text,
            state text,
            metadata map<text, text>,
            PRIMARY KEY (aggregate_id, version)
        ) WITH CLUSTERING ORDER BY (version DESC)
        """
        
        # Execute table creation
        self._session.execute(events_table)
        self._session.execute(type_index)
        self._session.execute(correlation_index)
        self._session.execute(snapshots_table)
        
    async def append_event(self, event: Event):
        """Append event to store"""
        # Generate partition key (daily partitions)
        partition_key = self._get_partition_key(event.timestamp)
        
        # Prepare insert statement
        insert_stmt = """
        INSERT INTO events (
            partition_key, event_id, timestamp, event_type,
            source, correlation_id, causation_id, priority,
            headers, payload
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # Execute insert
        self._session.execute(
            insert_stmt,
            (
                partition_key,
                event.event_id,
                event.timestamp,
                event.event_type,
                event.source,
                event.correlation_id,
                event.causation_id,
                event.priority.value,
                event.headers,
                json.dumps(event.payload)
            )
        )
        
    async def append_events(self, events: List[Event]):
        """Append multiple events in batch"""
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        
        insert_stmt = self._session.prepare("""
        INSERT INTO events (
            partition_key, event_id, timestamp, event_type,
            source, correlation_id, causation_id, priority,
            headers, payload
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        for event in events:
            partition_key = self._get_partition_key(event.timestamp)
            
            batch.add(
                insert_stmt,
                (
                    partition_key,
                    event.event_id,
                    event.timestamp,
                    event.event_type,
                    event.source,
                    event.correlation_id,
                    event.causation_id,
                    event.priority.value,
                    event.headers,
                    json.dumps(event.payload)
                )
            )
            
        self._session.execute(batch)
        
    async def query_events(self, query: EventQuery) -> List[Event]:
        """Query events from store"""
        # Build CQL query
        cql_parts = ["SELECT * FROM events"]
        where_clauses = []
        params = []
        
        # Time range filter
        if query.start_time and query.end_time:
            # Get all partitions in range
            partitions = self._get_partitions_in_range(query.start_time, query.end_time)
            where_clauses.append(f"partition_key IN ({','.join(['?' for _ in partitions])})")
            params.extend(partitions)
            
            where_clauses.append("timestamp >= ?")
            params.append(query.start_time)
            
            where_clauses.append("timestamp <= ?")
            params.append(query.end_time)
            
        # Event type filter
        if query.event_types:
            where_clauses.append(f"event_type IN ({','.join(['?' for _ in query.event_types])})")
            params.extend(query.event_types)
            
        # Source filter
        if query.sources:
            where_clauses.append(f"source IN ({','.join(['?' for _ in query.sources])})")
            params.extend(query.sources)
            
        # Correlation ID filter
        if query.correlation_ids:
            where_clauses.append(f"correlation_id IN ({','.join(['?' for _ in query.correlation_ids])})")
            params.extend(query.correlation_ids)
            
        # Build final query
        if where_clauses:
            cql_parts.append("WHERE " + " AND ".join(where_clauses))
            
        # Add ordering
        if query.order_desc:
            cql_parts.append(f"ORDER BY timestamp DESC")
        else:
            cql_parts.append(f"ORDER BY timestamp ASC")
            
        # Add limit
        cql_parts.append(f"LIMIT {query.limit}")
        
        # Execute query
        cql = " ".join(cql_parts)
        rows = self._session.execute(cql, params)
        
        # Convert to events
        events = []
        for row in rows:
            event = Event(
                event_id=row.event_id,
                event_type=row.event_type,
                timestamp=row.timestamp,
                source=row.source,
                correlation_id=row.correlation_id,
                causation_id=row.causation_id,
                priority=row.priority,
                headers=row.headers,
                payload=json.loads(row.payload) if row.payload else {}
            )
            events.append(event)
            
        return events
        
    async def get_event(self, event_id: str) -> Optional[Event]:
        """Get single event by ID"""
        # Need to scan all partitions (inefficient - consider secondary index)
        query = """
        SELECT * FROM events
        WHERE event_id = ?
        ALLOW FILTERING
        """
        
        rows = self._session.execute(query, [event_id])
        
        for row in rows:
            return Event(
                event_id=row.event_id,
                event_type=row.event_type,
                timestamp=row.timestamp,
                source=row.source,
                correlation_id=row.correlation_id,
                causation_id=row.causation_id,
                priority=row.priority,
                headers=row.headers,
                payload=json.loads(row.payload) if row.payload else {}
            )
            
        return None
        
    async def get_events_by_correlation(self, correlation_id: str) -> List[Event]:
        """Get all events with correlation ID"""
        query = EventQuery(
            correlation_ids=[correlation_id],
            limit=1000
        )
        
        return await self.query_events(query)
        
    async def replay_events(
        self,
        query: EventQuery,
        handler: Callable[[Event], Any],
        batch_size: int = 100
    ):
        """Replay events matching query"""
        offset = 0
        
        while True:
            # Get batch of events
            query.offset = offset
            query.limit = batch_size
            
            events = await self.query_events(query)
            
            if not events:
                break
                
            # Process events
            for event in events:
                if asyncio.iscoroutinefunction(handler):
                    await handler(event)
                else:
                    handler(event)
                    
            offset += len(events)
            
            # Break if less than batch size
            if len(events) < batch_size:
                break
                
    async def save_snapshot(
        self,
        aggregate_id: str,
        version: int,
        state: Any,
        event_id: str,
        metadata: Optional[Dict[str, str]] = None
    ):
        """Save aggregate snapshot"""
        insert_stmt = """
        INSERT INTO snapshots (
            aggregate_id, version, timestamp, event_id, state, metadata
        ) VALUES (?, ?, ?, ?, ?, ?)
        """
        
        self._session.execute(
            insert_stmt,
            (
                aggregate_id,
                version,
                datetime.utcnow(),
                event_id,
                json.dumps(state),
                metadata or {}
            )
        )
        
    async def get_snapshot(
        self,
        aggregate_id: str,
        version: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """Get aggregate snapshot"""
        if version is None:
            # Get latest snapshot
            query = """
            SELECT * FROM snapshots
            WHERE aggregate_id = ?
            LIMIT 1
            """
            params = [aggregate_id]
        else:
            # Get specific version
            query = """
            SELECT * FROM snapshots
            WHERE aggregate_id = ? AND version = ?
            """
            params = [aggregate_id, version]
            
        rows = self._session.execute(query, params)
        
        for row in rows:
            return {
                "aggregate_id": row.aggregate_id,
                "version": row.version,
                "timestamp": row.timestamp,
                "event_id": row.event_id,
                "state": json.loads(row.state) if row.state else None,
                "metadata": row.metadata
            }
            
        return None
        
    def _get_partition_key(self, timestamp: datetime) -> str:
        """Get partition key for timestamp (daily partitions)"""
        return timestamp.strftime("%Y%m%d")
        
    def _get_partitions_in_range(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> List[str]:
        """Get all partition keys in time range"""
        partitions = []
        current = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
        
        while current <= end_time:
            partitions.append(self._get_partition_key(current))
            current += timedelta(days=1)
            
        return partitions
        
    async def prune_events(self, retention_days: int):
        """Prune old events beyond retention period"""
        cutoff_date = datetime.utcnow() - timedelta(days=retention_days)
        
        # Get old partitions
        old_partitions = []
        current = cutoff_date.replace(hour=0, minute=0, second=0, microsecond=0)
        
        while current > datetime(2020, 1, 1):  # Reasonable start date
            partition_key = self._get_partition_key(current)
            old_partitions.append(partition_key)
            current -= timedelta(days=1)
            
            # Batch delete to avoid timeout
            if len(old_partitions) >= 30:
                await self._delete_partitions(old_partitions)
                old_partitions = []
                
        # Delete remaining partitions
        if old_partitions:
            await self._delete_partitions(old_partitions)
            
    async def _delete_partitions(self, partition_keys: List[str]):
        """Delete events in partitions"""
        for partition_key in partition_keys:
            delete_stmt = "DELETE FROM events WHERE partition_key = ?"
            self._session.execute(delete_stmt, [partition_key])
            
        logger.info(f"Deleted {len(partition_keys)} event partitions") 