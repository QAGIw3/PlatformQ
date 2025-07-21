"""
Parallel Settlement Engine

High-performance settlement processing using concurrent execution,
batch operations, and optimized database writes.
"""

import asyncio
import logging
from typing import Dict, List, Optional, Tuple, Any, Set
from datetime import datetime, timedelta
from decimal import Decimal
from dataclasses import dataclass, field
from enum import Enum
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing as mp
from collections import defaultdict
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
import psycopg2
from psycopg2 import pool
from contextlib import contextmanager

import httpx
from platformq_shared.cache import CacheManager
from platformq_shared.event_publisher import EventPublisher

logger = logging.getLogger(__name__)


class SettlementStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    SETTLED = "settled"
    FAILED = "failed"
    DISPUTED = "disputed"


class SettlementType(Enum):
    SPOT = "spot"
    FUTURES = "futures"
    OPTIONS = "options"
    SWAP = "swap"
    COLLATERAL = "collateral"


@dataclass
class Settlement:
    """Settlement record"""
    settlement_id: str
    settlement_type: SettlementType
    contract_id: str
    buyer_id: str
    seller_id: str
    amount: Decimal
    price: Decimal
    resource_type: str
    settlement_date: datetime
    status: SettlementStatus
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database"""
        return {
            'settlement_id': self.settlement_id,
            'settlement_type': self.settlement_type.value,
            'contract_id': self.contract_id,
            'buyer_id': self.buyer_id,
            'seller_id': self.seller_id,
            'amount': float(self.amount),
            'price': float(self.price),
            'resource_type': self.resource_type,
            'settlement_date': self.settlement_date,
            'status': self.status.value,
            'metadata': self.metadata
        }


@dataclass
class SettlementBatch:
    """Batch of settlements for processing"""
    batch_id: str
    settlements: List[Settlement]
    created_at: datetime
    priority: int = 0
    
    def size(self) -> int:
        return len(self.settlements)
        
    def total_value(self) -> Decimal:
        return sum(s.amount * s.price for s in self.settlements)


class ConnectionPool:
    """Database connection pool for parallel operations"""
    
    def __init__(self, dsn: str, min_conn: int = 10, max_conn: int = 50):
        self.pool = psycopg2.pool.ThreadedConnectionPool(
            min_conn,
            max_conn,
            dsn
        )
        
    @contextmanager
    def get_connection(self):
        """Get connection from pool"""
        conn = self.pool.getconn()
        try:
            yield conn
        finally:
            self.pool.putconn(conn)
            
    def close(self):
        """Close all connections"""
        self.pool.closeall()


class ParallelSettlementEngine:
    """Processes settlements in parallel batches"""
    
    def __init__(
        self,
        db_dsn: str,
        max_workers: int = None,
        batch_size: int = 1000,
        cache_manager: Optional[CacheManager] = None,
        event_publisher: Optional[EventPublisher] = None
    ):
        self.db_dsn = db_dsn
        self.max_workers = max_workers or mp.cpu_count()
        self.batch_size = batch_size
        self.cache = cache_manager or CacheManager()
        self.event_publisher = event_publisher
        
        # Connection pool for parallel DB operations
        self.conn_pool = ConnectionPool(db_dsn, min_conn=self.max_workers, max_conn=self.max_workers * 2)
        
        # Thread pool for I/O bound operations
        self.thread_pool = ThreadPoolExecutor(max_workers=self.max_workers)
        
        # Process pool for CPU bound operations
        self.process_pool = ProcessPoolExecutor(max_workers=self.max_workers // 2)
        
        # Settlement queues by priority
        self.settlement_queues = {
            0: asyncio.Queue(),  # Low priority
            1: asyncio.Queue(),  # Normal priority
            2: asyncio.Queue(),  # High priority
            3: asyncio.Queue()   # Critical priority
        }
        
        # Batch accumulator
        self.pending_settlements = defaultdict(list)
        self.batch_lock = asyncio.Lock()
        
        # Performance metrics
        self.metrics = {
            'settlements_processed': 0,
            'batches_processed': 0,
            'average_batch_time': 0,
            'failed_settlements': 0,
            'total_value_settled': Decimal('0')
        }
        
        # Background tasks
        self.background_tasks = []
        
    async def start(self):
        """Start settlement processing"""
        # Start batch processors for each priority
        for priority in range(4):
            task = asyncio.create_task(self._process_priority_queue(priority))
            self.background_tasks.append(task)
            
        # Start batch accumulator
        task = asyncio.create_task(self._batch_accumulator())
        self.background_tasks.append(task)
        
        logger.info("Parallel settlement engine started")
        
    async def stop(self):
        """Stop settlement processing"""
        # Cancel background tasks
        for task in self.background_tasks:
            task.cancel()
            
        # Wait for tasks to complete
        await asyncio.gather(*self.background_tasks, return_exceptions=True)
        
        # Shutdown pools
        self.thread_pool.shutdown(wait=True)
        self.process_pool.shutdown(wait=True)
        self.conn_pool.close()
        
        logger.info("Parallel settlement engine stopped")
        
    async def add_settlement(
        self,
        settlement: Settlement,
        priority: int = 1
    ) -> bool:
        """Add settlement for processing"""
        try:
            # Validate priority
            priority = max(0, min(3, priority))
            
            # Add to pending for batching
            async with self.batch_lock:
                self.pending_settlements[priority].append(settlement)
                
                # Check if batch is ready
                if len(self.pending_settlements[priority]) >= self.batch_size:
                    await self._create_and_queue_batch(priority)
                    
            return True
            
        except Exception as e:
            logger.error(f"Error adding settlement: {e}")
            return False
            
    async def _batch_accumulator(self):
        """Background task to create batches periodically"""
        while True:
            try:
                await asyncio.sleep(1)  # Check every second
                
                async with self.batch_lock:
                    for priority, settlements in self.pending_settlements.items():
                        if settlements:
                            # Create batch if enough time has passed or batch is large enough
                            if len(settlements) >= self.batch_size // 2:
                                await self._create_and_queue_batch(priority)
                                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in batch accumulator: {e}")
                
    async def _create_and_queue_batch(self, priority: int):
        """Create batch and add to queue"""
        settlements = self.pending_settlements[priority]
        if not settlements:
            return
            
        # Create batch
        batch = SettlementBatch(
            batch_id=f"batch_{datetime.utcnow().timestamp()}_{priority}",
            settlements=settlements.copy(),
            created_at=datetime.utcnow(),
            priority=priority
        )
        
        # Clear pending
        self.pending_settlements[priority].clear()
        
        # Add to queue
        await self.settlement_queues[priority].put(batch)
        
        logger.info(f"Created batch {batch.batch_id} with {batch.size()} settlements")
        
    async def _process_priority_queue(self, priority: int):
        """Process settlements from a priority queue"""
        queue = self.settlement_queues[priority]
        
        while True:
            try:
                # Get batch from queue
                batch = await queue.get()
                
                # Process batch
                await self._process_batch(batch)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error processing priority {priority} queue: {e}")
                
    async def _process_batch(self, batch: SettlementBatch):
        """Process a batch of settlements"""
        start_time = datetime.utcnow()
        
        try:
            logger.info(f"Processing batch {batch.batch_id} with {batch.size()} settlements")
            
            # Step 1: Validate settlements in parallel
            validated = await self._validate_settlements_parallel(batch.settlements)
            
            # Step 2: Calculate net positions
            net_positions = await self._calculate_net_positions(validated)
            
            # Step 3: Update balances in parallel
            balance_updates = await self._update_balances_parallel(net_positions)
            
            # Step 4: Record settlements in database
            await self._record_settlements_batch(validated)
            
            # Step 5: Trigger resource allocation (for physical delivery)
            await self._trigger_resource_allocation(validated)
            
            # Step 6: Publish settlement events
            await self._publish_settlement_events(validated)
            
            # Update metrics
            elapsed = (datetime.utcnow() - start_time).total_seconds()
            self.metrics['batches_processed'] += 1
            self.metrics['settlements_processed'] += len(validated)
            self.metrics['average_batch_time'] = (
                (self.metrics['average_batch_time'] * (self.metrics['batches_processed'] - 1) + elapsed) /
                self.metrics['batches_processed']
            )
            self.metrics['total_value_settled'] += batch.total_value()
            
            logger.info(f"Processed batch {batch.batch_id} in {elapsed:.2f} seconds")
            
        except Exception as e:
            logger.error(f"Error processing batch {batch.batch_id}: {e}")
            await self._handle_batch_failure(batch)
            
    async def _validate_settlements_parallel(
        self,
        settlements: List[Settlement]
    ) -> List[Settlement]:
        """Validate settlements in parallel"""
        loop = asyncio.get_event_loop()
        
        # Run validation in thread pool
        validation_tasks = []
        for settlement in settlements:
            task = loop.run_in_executor(
                self.thread_pool,
                self._validate_single_settlement,
                settlement
            )
            validation_tasks.append(task)
            
        results = await asyncio.gather(*validation_tasks, return_exceptions=True)
        
        # Filter valid settlements
        validated = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Validation error for settlement {settlements[i].settlement_id}: {result}")
                self.metrics['failed_settlements'] += 1
            elif result:
                validated.append(settlements[i])
            else:
                logger.warning(f"Settlement {settlements[i].settlement_id} failed validation")
                self.metrics['failed_settlements'] += 1
                
        return validated
        
    def _validate_single_settlement(self, settlement: Settlement) -> bool:
        """Validate a single settlement"""
        try:
            # Check basic requirements
            if settlement.amount <= 0 or settlement.price <= 0:
                return False
                
            # Check parties exist
            if not settlement.buyer_id or not settlement.seller_id:
                return False
                
            # Check settlement date
            if settlement.settlement_date > datetime.utcnow() + timedelta(days=30):
                return False
                
            # Additional validations based on type
            if settlement.settlement_type == SettlementType.OPTIONS:
                # Validate option-specific fields
                if 'strike_price' not in settlement.metadata:
                    return False
                    
            return True
            
        except Exception as e:
            logger.error(f"Validation error: {e}")
            return False
            
    async def _calculate_net_positions(
        self,
        settlements: List[Settlement]
    ) -> Dict[str, Dict[str, Decimal]]:
        """Calculate net positions for each participant"""
        loop = asyncio.get_event_loop()
        
        # Use process pool for CPU-intensive calculation
        result = await loop.run_in_executor(
            self.process_pool,
            self._calculate_net_positions_cpu,
            settlements
        )
        
        return result
        
    @staticmethod
    def _calculate_net_positions_cpu(
        settlements: List[Settlement]
    ) -> Dict[str, Dict[str, Decimal]]:
        """CPU-intensive net position calculation"""
        positions = defaultdict(lambda: defaultdict(Decimal))
        
        for settlement in settlements:
            # Buyer pays cash, receives resource
            positions[settlement.buyer_id]['cash'] -= settlement.amount * settlement.price
            positions[settlement.buyer_id][settlement.resource_type] += settlement.amount
            
            # Seller receives cash, delivers resource
            positions[settlement.seller_id]['cash'] += settlement.amount * settlement.price
            positions[settlement.seller_id][settlement.resource_type] -= settlement.amount
            
        return dict(positions)
        
    async def _update_balances_parallel(
        self,
        net_positions: Dict[str, Dict[str, Decimal]]
    ) -> Dict[str, bool]:
        """Update participant balances in parallel"""
        update_tasks = []
        
        for participant_id, positions in net_positions.items():
            task = self._update_participant_balance(participant_id, positions)
            update_tasks.append(task)
            
        results = await asyncio.gather(*update_tasks, return_exceptions=True)
        
        # Create result map
        success_map = {}
        for i, (participant_id, _) in enumerate(net_positions.items()):
            if isinstance(results[i], Exception):
                logger.error(f"Balance update failed for {participant_id}: {results[i]}")
                success_map[participant_id] = False
            else:
                success_map[participant_id] = results[i]
                
        return success_map
        
    async def _update_participant_balance(
        self,
        participant_id: str,
        positions: Dict[str, Decimal]
    ) -> bool:
        """Update single participant balance"""
        loop = asyncio.get_event_loop()
        
        # Run in thread pool for database operation
        result = await loop.run_in_executor(
            self.thread_pool,
            self._update_balance_db,
            participant_id,
            positions
        )
        
        return result
        
    def _update_balance_db(
        self,
        participant_id: str,
        positions: Dict[str, Decimal]
    ) -> bool:
        """Update balance in database"""
        with self.conn_pool.get_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    # Start transaction
                    cursor.execute("BEGIN")
                    
                    # Update each position
                    for asset, amount in positions.items():
                        cursor.execute(
                            """
                            INSERT INTO participant_balances (participant_id, asset, balance)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (participant_id, asset)
                            DO UPDATE SET balance = participant_balances.balance + EXCLUDED.balance
                            """,
                            (participant_id, asset, float(amount))
                        )
                        
                    # Commit transaction
                    cursor.execute("COMMIT")
                    
                return True
                
            except Exception as e:
                logger.error(f"Database error updating balance: {e}")
                conn.rollback()
                return False
                
    async def _record_settlements_batch(self, settlements: List[Settlement]):
        """Record settlements in database using batch insert"""
        loop = asyncio.get_event_loop()
        
        # Prepare batch data
        batch_data = [s.to_dict() for s in settlements]
        
        # Execute batch insert in thread pool
        await loop.run_in_executor(
            self.thread_pool,
            self._batch_insert_settlements,
            batch_data
        )
        
    def _batch_insert_settlements(self, settlements_data: List[Dict[str, Any]]):
        """Batch insert settlements into database"""
        with self.conn_pool.get_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    # Use COPY for maximum performance
                    columns = settlements_data[0].keys()
                    
                    # Create temporary table
                    cursor.execute(
                        f"""
                        CREATE TEMP TABLE temp_settlements (
                            settlement_id VARCHAR PRIMARY KEY,
                            settlement_type VARCHAR,
                            contract_id VARCHAR,
                            buyer_id VARCHAR,
                            seller_id VARCHAR,
                            amount DECIMAL,
                            price DECIMAL,
                            resource_type VARCHAR,
                            settlement_date TIMESTAMP,
                            status VARCHAR,
                            metadata JSONB
                        )
                        """
                    )
                    
                    # Use execute_values for batch insert
                    from psycopg2.extras import execute_values
                    
                    execute_values(
                        cursor,
                        """
                        INSERT INTO temp_settlements 
                        (settlement_id, settlement_type, contract_id, buyer_id, seller_id,
                         amount, price, resource_type, settlement_date, status, metadata)
                        VALUES %s
                        """,
                        [
                            (
                                s['settlement_id'], s['settlement_type'], s['contract_id'],
                                s['buyer_id'], s['seller_id'], s['amount'], s['price'],
                                s['resource_type'], s['settlement_date'], s['status'],
                                json.dumps(s['metadata'])
                            )
                            for s in settlements_data
                        ]
                    )
                    
                    # Insert into main table
                    cursor.execute(
                        """
                        INSERT INTO settlements
                        SELECT * FROM temp_settlements
                        ON CONFLICT (settlement_id) DO NOTHING
                        """
                    )
                    
                    conn.commit()
                    
            except Exception as e:
                logger.error(f"Batch insert error: {e}")
                conn.rollback()
                raise
                
    async def _trigger_resource_allocation(self, settlements: List[Settlement]):
        """Trigger physical resource allocation for settlements"""
        # Group by resource type
        allocations_by_type = defaultdict(list)
        
        for settlement in settlements:
            if settlement.settlement_type in [SettlementType.FUTURES, SettlementType.SPOT]:
                allocations_by_type[settlement.resource_type].append({
                    'buyer_id': settlement.buyer_id,
                    'amount': settlement.amount,
                    'settlement_id': settlement.settlement_id
                })
                
        # Trigger allocations in parallel
        tasks = []
        for resource_type, allocations in allocations_by_type.items():
            task = self._allocate_resources(resource_type, allocations)
            tasks.append(task)
            
        await asyncio.gather(*tasks, return_exceptions=True)
        
    async def _allocate_resources(
        self,
        resource_type: str,
        allocations: List[Dict[str, Any]]
    ):
        """Allocate specific resource type"""
        try:
            # This would call provisioning service
            # For now, just log
            total_amount = sum(a['amount'] for a in allocations)
            logger.info(f"Allocating {total_amount} {resource_type} for {len(allocations)} settlements")
            
        except Exception as e:
            logger.error(f"Resource allocation error: {e}")
            
    async def _publish_settlement_events(self, settlements: List[Settlement]):
        """Publish settlement completion events"""
        if not self.event_publisher:
            return
            
        # Create events
        events = []
        for settlement in settlements:
            event = {
                'event_type': 'settlement_completed',
                'settlement_id': settlement.settlement_id,
                'settlement_type': settlement.settlement_type.value,
                'buyer_id': settlement.buyer_id,
                'seller_id': settlement.seller_id,
                'amount': float(settlement.amount),
                'price': float(settlement.price),
                'resource_type': settlement.resource_type,
                'timestamp': datetime.utcnow().isoformat()
            }
            events.append(event)
            
        # Publish in batches
        batch_size = 100
        for i in range(0, len(events), batch_size):
            batch = events[i:i + batch_size]
            try:
                await self.event_publisher.publish_batch(
                    topic='settlement-completed-events',
                    events=batch
                )
            except Exception as e:
                logger.error(f"Error publishing events: {e}")
                
    async def _handle_batch_failure(self, batch: SettlementBatch):
        """Handle failed batch processing"""
        # Re-queue individual settlements with higher priority
        for settlement in batch.settlements:
            settlement.status = SettlementStatus.FAILED
            await self.add_settlement(settlement, priority=min(batch.priority + 1, 3))
            
    def get_metrics(self) -> Dict[str, Any]:
        """Get performance metrics"""
        return {
            'settlements_processed': self.metrics['settlements_processed'],
            'batches_processed': self.metrics['batches_processed'],
            'average_batch_time_seconds': self.metrics['average_batch_time'],
            'failed_settlements': self.metrics['failed_settlements'],
            'total_value_settled': float(self.metrics['total_value_settled']),
            'throughput_per_second': (
                self.metrics['settlements_processed'] / max(self.metrics['average_batch_time'] * self.metrics['batches_processed'], 1)
            ),
            'queue_sizes': {
                priority: queue.qsize()
                for priority, queue in self.settlement_queues.items()
            }
        }
        
    async def force_settle_batch(self, settlement_ids: List[str]) -> Dict[str, bool]:
        """Force immediate settlement of specific settlements"""
        # Fetch settlements from database
        settlements = await self._fetch_settlements(settlement_ids)
        
        # Create high-priority batch
        batch = SettlementBatch(
            batch_id=f"forced_{datetime.utcnow().timestamp()}",
            settlements=settlements,
            created_at=datetime.utcnow(),
            priority=3  # Critical priority
        )
        
        # Process immediately
        await self._process_batch(batch)
        
        # Return success map
        return {s.settlement_id: s.status == SettlementStatus.SETTLED for s in settlements}
        
    async def _fetch_settlements(self, settlement_ids: List[str]) -> List[Settlement]:
        """Fetch settlements from database"""
        loop = asyncio.get_event_loop()
        
        result = await loop.run_in_executor(
            self.thread_pool,
            self._fetch_settlements_db,
            settlement_ids
        )
        
        return result
        
    def _fetch_settlements_db(self, settlement_ids: List[str]) -> List[Settlement]:
        """Fetch settlements from database"""
        settlements = []
        
        with self.conn_pool.get_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    # Use ANY for efficient IN query
                    cursor.execute(
                        """
                        SELECT settlement_id, settlement_type, contract_id,
                               buyer_id, seller_id, amount, price,
                               resource_type, settlement_date, status, metadata
                        FROM settlements
                        WHERE settlement_id = ANY(%s)
                        """,
                        (settlement_ids,)
                    )
                    
                    for row in cursor.fetchall():
                        settlement = Settlement(
                            settlement_id=row[0],
                            settlement_type=SettlementType(row[1]),
                            contract_id=row[2],
                            buyer_id=row[3],
                            seller_id=row[4],
                            amount=Decimal(str(row[5])),
                            price=Decimal(str(row[6])),
                            resource_type=row[7],
                            settlement_date=row[8],
                            status=SettlementStatus(row[9]),
                            metadata=row[10] or {}
                        )
                        settlements.append(settlement)
                        
            except Exception as e:
                logger.error(f"Error fetching settlements: {e}")
                
        return settlements 