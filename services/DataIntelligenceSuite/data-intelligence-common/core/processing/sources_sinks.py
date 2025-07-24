"""
Common data sources and sinks for unified processing.

Provides implementations for various data sources and sinks.
"""

import asyncio
import json
from typing import Any, Dict, List, Optional, AsyncIterator, Union, Callable
from pathlib import Path
from datetime import datetime
import pandas as pd

from .unified_processor import DataSource, DataSink
from ..events import EventBus, Event
from ...clients.base import BaseClient

# Try to import optional dependencies
try:
    import pulsar
    PULSAR_AVAILABLE = True
except ImportError:
    PULSAR_AVAILABLE = False

try:
    from kafka import KafkaProducer, KafkaConsumer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False


class FileSource(DataSource):
    """Read data from files"""
    
    def __init__(
        self,
        path: Union[str, Path],
        format: str = "json",
        batch_size: int = 1000,
        **kwargs
    ):
        self.path = Path(path)
        self.format = format
        self.batch_size = batch_size
        self.kwargs = kwargs
        
    async def read(self) -> AsyncIterator[Dict[str, Any]]:
        """Read data from file"""
        if self.format == "json":
            async for item in self._read_json():
                yield item
        elif self.format == "jsonl":
            async for item in self._read_jsonl():
                yield item
        elif self.format == "csv":
            async for item in self._read_csv():
                yield item
        elif self.format == "parquet":
            async for item in self._read_parquet():
                yield item
        else:
            raise ValueError(f"Unsupported format: {self.format}")
            
    async def _read_json(self) -> AsyncIterator[Dict[str, Any]]:
        """Read JSON file"""
        with open(self.path, 'r') as f:
            data = json.load(f)
            
        if isinstance(data, list):
            for item in data:
                yield item
        else:
            yield data
            
    async def _read_jsonl(self) -> AsyncIterator[Dict[str, Any]]:
        """Read JSON Lines file"""
        with open(self.path, 'r') as f:
            for line in f:
                if line.strip():
                    yield json.loads(line)
                    
    async def _read_csv(self) -> AsyncIterator[Dict[str, Any]]:
        """Read CSV file"""
        # Read in chunks for large files
        for chunk in pd.read_csv(self.path, chunksize=self.batch_size, **self.kwargs):
            for _, row in chunk.iterrows():
                yield row.to_dict()
                
    async def _read_parquet(self) -> AsyncIterator[Dict[str, Any]]:
        """Read Parquet file"""
        df = pd.read_parquet(self.path, **self.kwargs)
        for _, row in df.iterrows():
            yield row.to_dict()
            
    async def get_schema(self) -> Dict[str, Any]:
        """Get data schema"""
        if self.format in ["csv", "parquet"]:
            # Read first row to infer schema
            df = pd.read_csv(self.path, nrows=1) if self.format == "csv" else pd.read_parquet(self.path)
            return {
                "columns": list(df.columns),
                "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()}
            }
        else:
            # For JSON, sample first record
            async for item in self.read():
                return {"fields": list(item.keys()) if isinstance(item, dict) else []}
                
    async def estimate_size(self) -> int:
        """Estimate data size"""
        return self.path.stat().st_size


class FileSink(DataSink):
    """Write data to files"""
    
    def __init__(
        self,
        path: Union[str, Path],
        format: str = "json",
        mode: str = "w",
        **kwargs
    ):
        self.path = Path(path)
        self.format = format
        self.mode = mode
        self.kwargs = kwargs
        self._buffer = []
        self._file = None
        
    async def write(self, data: Union[Any, List[Any]]) -> None:
        """Write data to file"""
        if isinstance(data, list):
            self._buffer.extend(data)
        else:
            self._buffer.append(data)
            
    async def commit(self) -> None:
        """Commit buffered data"""
        if not self._buffer:
            return
            
        if self.format == "json":
            await self._write_json()
        elif self.format == "jsonl":
            await self._write_jsonl()
        elif self.format == "csv":
            await self._write_csv()
        elif self.format == "parquet":
            await self._write_parquet()
            
        self._buffer.clear()
        
    async def _write_json(self) -> None:
        """Write JSON file"""
        with open(self.path, self.mode) as f:
            json.dump(self._buffer, f, **self.kwargs)
            
    async def _write_jsonl(self) -> None:
        """Write JSON Lines file"""
        mode = 'a' if self.mode == 'a' else 'w'
        with open(self.path, mode) as f:
            for item in self._buffer:
                f.write(json.dumps(item) + '\n')
                
    async def _write_csv(self) -> None:
        """Write CSV file"""
        df = pd.DataFrame(self._buffer)
        mode = 'a' if self.mode == 'a' else 'w'
        header = mode == 'w'
        df.to_csv(self.path, mode=mode, header=header, index=False, **self.kwargs)
        
    async def _write_parquet(self) -> None:
        """Write Parquet file"""
        df = pd.DataFrame(self._buffer)
        df.to_parquet(self.path, **self.kwargs)
        
    async def rollback(self) -> None:
        """Clear buffer without writing"""
        self._buffer.clear()


class EventBusSource(DataSource):
    """Read data from event bus"""
    
    def __init__(
        self,
        event_bus: EventBus,
        topic: str,
        subscription: str,
        event_types: Optional[List[str]] = None,
        timeout: Optional[float] = None
    ):
        self.event_bus = event_bus
        self.topic = topic
        self.subscription = subscription
        self.event_types = event_types
        self.timeout = timeout
        self._consumer = None
        
    async def read(self) -> AsyncIterator[Event]:
        """Read events from bus"""
        # Subscribe to events
        subscription = await self.event_bus.subscribe(
            topic_pattern=self.topic,
            handler=lambda e: e,  # Just return the event
            subscription_name=self.subscription,
            event_types=self.event_types
        )
        
        # This would need proper implementation with the event bus
        # For now, yield a placeholder
        yield Event(
            event_type="placeholder",
            source="event_bus_source",
            payload={}
        )
        
    async def get_schema(self) -> Dict[str, Any]:
        """Get event schema"""
        return {
            "type": "event",
            "fields": ["event_id", "event_type", "timestamp", "source", "payload"]
        }
        
    async def estimate_size(self) -> int:
        """Estimate data size - unknown for streams"""
        return -1  # Unknown


class EventBusSink(DataSink):
    """Write data to event bus"""
    
    def __init__(
        self,
        event_bus: EventBus,
        topic: str,
        event_type: str,
        source: str
    ):
        self.event_bus = event_bus
        self.topic = topic
        self.event_type = event_type
        self.source = source
        self._buffer = []
        
    async def write(self, data: Union[Any, List[Any]]) -> None:
        """Buffer data for writing"""
        if isinstance(data, list):
            self._buffer.extend(data)
        else:
            self._buffer.append(data)
            
    async def commit(self) -> None:
        """Publish buffered events"""
        for item in self._buffer:
            event = Event(
                event_type=self.event_type,
                source=self.source,
                payload=item if isinstance(item, dict) else {"data": item}
            )
            await self.event_bus.publish(self.topic, event)
            
        self._buffer.clear()
        
    async def rollback(self) -> None:
        """Clear buffer without publishing"""
        self._buffer.clear()


class DatabaseSource(DataSource):
    """Read data from database"""
    
    def __init__(
        self,
        client: BaseClient,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        batch_size: int = 1000
    ):
        self.client = client
        self.query = query
        self.params = params or {}
        self.batch_size = batch_size
        
    async def read(self) -> AsyncIterator[Dict[str, Any]]:
        """Read data from database"""
        # Execute query
        result = await self.client.execute_query(
            self.query,
            params=self.params
        )
        
        # Yield results
        for row in result:
            yield row
            
    async def get_schema(self) -> Dict[str, Any]:
        """Get data schema"""
        # Get schema from query
        schema = await self.client.get_query_schema(self.query)
        return schema
        
    async def estimate_size(self) -> int:
        """Estimate result size"""
        # Run count query
        count_query = f"SELECT COUNT(*) as count FROM ({self.query}) t"
        result = await self.client.execute_query(count_query, params=self.params)
        
        if result and len(result) > 0:
            count = result[0].get('count', 0)
            # Estimate 1KB per row
            return count * 1024
        
        return 0


class DatabaseSink(DataSink):
    """Write data to database"""
    
    def __init__(
        self,
        client: BaseClient,
        table: str,
        mode: str = "append",  # append, overwrite, upsert
        batch_size: int = 1000
    ):
        self.client = client
        self.table = table
        self.mode = mode
        self.batch_size = batch_size
        self._buffer = []
        
    async def write(self, data: Union[Dict[str, Any], List[Dict[str, Any]]]) -> None:
        """Buffer data for writing"""
        if isinstance(data, list):
            self._buffer.extend(data)
        else:
            self._buffer.append(data)
            
        # Write if buffer is full
        if len(self._buffer) >= self.batch_size:
            await self._flush()
            
    async def commit(self) -> None:
        """Commit buffered data"""
        await self._flush()
        
    async def _flush(self) -> None:
        """Flush buffer to database"""
        if not self._buffer:
            return
            
        if self.mode == "append":
            await self.client.insert_batch(self.table, self._buffer)
        elif self.mode == "overwrite":
            # Truncate and insert
            await self.client.truncate_table(self.table)
            await self.client.insert_batch(self.table, self._buffer)
        elif self.mode == "upsert":
            await self.client.upsert_batch(self.table, self._buffer)
            
        self._buffer.clear()
        
    async def rollback(self) -> None:
        """Clear buffer without writing"""
        self._buffer.clear()


class LambdaSource(DataSource):
    """Generate data using a lambda function"""
    
    def __init__(
        self,
        generator: Callable[[], AsyncIterator[Any]],
        schema: Optional[Dict[str, Any]] = None,
        estimated_size: int = -1
    ):
        self.generator = generator
        self._schema = schema
        self._estimated_size = estimated_size
        
    async def read(self) -> AsyncIterator[Any]:
        """Generate data"""
        async for item in self.generator():
            yield item
            
    async def get_schema(self) -> Dict[str, Any]:
        """Get data schema"""
        return self._schema or {}
        
    async def estimate_size(self) -> int:
        """Estimate data size"""
        return self._estimated_size


class LambdaSink(DataSink):
    """Process data using a lambda function"""
    
    def __init__(
        self,
        processor: Callable[[Union[Any, List[Any]]], None],
        batch: bool = False
    ):
        self.processor = processor
        self.batch = batch
        self._buffer = []
        
    async def write(self, data: Union[Any, List[Any]]) -> None:
        """Process data"""
        if self.batch:
            if isinstance(data, list):
                self._buffer.extend(data)
            else:
                self._buffer.append(data)
        else:
            await self.processor(data)
            
    async def commit(self) -> None:
        """Process buffered data"""
        if self.batch and self._buffer:
            await self.processor(self._buffer)
            self._buffer.clear()
            
    async def rollback(self) -> None:
        """Clear buffer"""
        self._buffer.clear()


# Pulsar source/sink if available
if PULSAR_AVAILABLE:
    class PulsarSource(DataSource):
        """Read data from Pulsar"""
        
        def __init__(
            self,
            service_url: str,
            topic: str,
            subscription: str,
            **kwargs
        ):
            self.service_url = service_url
            self.topic = topic
            self.subscription = subscription
            self.kwargs = kwargs
            self._client = None
            self._consumer = None
            
        async def read(self) -> AsyncIterator[Dict[str, Any]]:
            """Read messages from Pulsar"""
            # Initialize client and consumer
            if not self._client:
                self._client = pulsar.Client(self.service_url)
                self._consumer = self._client.subscribe(
                    self.topic,
                    self.subscription,
                    **self.kwargs
                )
                
            # Read messages
            while True:
                msg = self._consumer.receive()
                try:
                    data = json.loads(msg.data().decode('utf-8'))
                    yield data
                    self._consumer.acknowledge(msg)
                except Exception as e:
                    self._consumer.negative_acknowledge(msg)
                    raise e
                    
        async def get_schema(self) -> Dict[str, Any]:
            """Get message schema"""
            return {"type": "pulsar_message"}
            
        async def estimate_size(self) -> int:
            """Unknown for streams"""
            return -1


# Export all sources and sinks
__all__ = [
    'FileSource', 'FileSink',
    'EventBusSource', 'EventBusSink',
    'DatabaseSource', 'DatabaseSink',
    'LambdaSource', 'LambdaSink'
]

if PULSAR_AVAILABLE:
    __all__.append('PulsarSource')

if KAFKA_AVAILABLE:
    # Add Kafka source/sink when implemented
    pass 