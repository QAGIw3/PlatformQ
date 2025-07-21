"""
Availability Monitor Oracle

Monitors and tracks uptime/downtime for compute resources.
Essential for insurance claims and SLA verification.
"""

from typing import Dict, Any, List, Optional, Tuple, Set
from decimal import Decimal
from datetime import datetime, timedelta
import logging
import asyncio
from enum import Enum
from collections import defaultdict
import statistics

from web3 import Web3
from fastapi import HTTPException
from prometheus_client import Counter, Gauge, Histogram
import aiohttp
import ping3

from ..core.blockchain import BlockchainClient
from ..models.availability import AvailabilityStatus, DowntimeEvent, SLAMetrics
from ..utils.signing import sign_oracle_data

logger = logging.getLogger(__name__)

# Metrics
AVAILABILITY_CHECKS = Counter(
    'oracle_availability_checks_total',
    'Total availability checks performed',
    ['resource_type', 'resource_id', 'status']
)
CURRENT_AVAILABILITY = Gauge(
    'oracle_resource_availability',
    'Current resource availability (1=up, 0=down)',
    ['resource_type', 'resource_id']
)
DOWNTIME_DURATION = Histogram(
    'oracle_downtime_duration_seconds',
    'Duration of downtime events',
    ['resource_type']
)
SLA_COMPLIANCE = Gauge(
    'oracle_sla_compliance_percent',
    'SLA compliance percentage',
    ['resource_type', 'resource_id']
)


class ResourceStatus(str, Enum):
    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"
    DEGRADED = "degraded"
    MAINTENANCE = "maintenance"


class AvailabilityMonitor:
    """Monitors compute resource availability for insurance and SLA tracking"""
    
    def __init__(
        self,
        blockchain_client: BlockchainClient,
        monitor_contract_address: str,
        signing_key: str,
        check_interval: int = 60  # seconds
    ):
        self.blockchain = blockchain_client
        self.monitor_contract_address = monitor_contract_address
        self.signing_key = signing_key
        self.check_interval = check_interval
        
        # Resource monitoring configuration
        self._monitored_resources = {}  # resource_id -> monitoring_config
        self._availability_history = defaultdict(list)  # resource_id -> [status_records]
        self._downtime_events = defaultdict(list)  # resource_id -> [downtime_events]
        self._current_status = {}  # resource_id -> current_status
        
        # Active monitoring tasks
        self._monitoring_tasks = {}  # resource_id -> asyncio.Task
        
        # SLA thresholds
        self.sla_thresholds = {
            'quantum': 0.99,    # 99% uptime
            'ai': 0.995,        # 99.5% uptime
            'network': 0.999    # 99.9% uptime
        }
        
        # Check methods by resource type
        self.check_methods = {
            'quantum': self._check_quantum_availability,
            'ai': self._check_ai_availability,
            'network': self._check_network_availability
        }
        
        # HTTP session for health checks
        self._session = None
        
    async def initialize(self):
        """Initialize the availability monitor"""
        self._session = aiohttp.ClientSession()
        
    async def shutdown(self):
        """Shutdown the monitor"""
        # Cancel all monitoring tasks
        for task in self._monitoring_tasks.values():
            task.cancel()
        
        # Close HTTP session
        if self._session:
            await self._session.close()
    
    async def start_monitoring(
        self,
        resource_id: int,
        resource_type: str,
        endpoint: str,
        check_config: Dict[str, Any]
    ):
        """
        Start monitoring a resource
        
        Args:
            resource_id: Resource identifier
            resource_type: Type of resource (quantum/ai/network)
            endpoint: Resource endpoint for health checks
            check_config: Configuration for health checks
        """
        try:
            # Store monitoring configuration
            self._monitored_resources[resource_id] = {
                'resource_type': resource_type,
                'endpoint': endpoint,
                'check_config': check_config,
                'started_at': datetime.utcnow()
            }
            
            # Start monitoring task
            if resource_id not in self._monitoring_tasks:
                task = asyncio.create_task(
                    self._monitor_resource(resource_id)
                )
                self._monitoring_tasks[resource_id] = task
                
            logger.info(f"Started monitoring resource {resource_id}")
            
        except Exception as e:
            logger.error(f"Failed to start monitoring: {e}")
            raise
    
    async def stop_monitoring(self, resource_id: int):
        """Stop monitoring a resource"""
        if resource_id in self._monitoring_tasks:
            self._monitoring_tasks[resource_id].cancel()
            del self._monitoring_tasks[resource_id]
            
        if resource_id in self._monitored_resources:
            del self._monitored_resources[resource_id]
    
    async def check_availability(
        self,
        resource_id: int
    ) -> Dict[str, Any]:
        """
        Check current availability of a resource
        
        Args:
            resource_id: Resource identifier
            
        Returns:
            Availability status
        """
        try:
            if resource_id not in self._monitored_resources:
                raise ValueError(f"Resource {resource_id} not monitored")
            
            config = self._monitored_resources[resource_id]
            resource_type = config['resource_type']
            
            # Perform availability check
            check_method = self.check_methods.get(resource_type)
            if not check_method:
                raise ValueError(f"Unknown resource type: {resource_type}")
            
            status = await check_method(resource_id, config)
            
            # Update metrics
            AVAILABILITY_CHECKS.labels(
                resource_type=resource_type,
                resource_id=resource_id,
                status=status['status']
            ).inc()
            
            CURRENT_AVAILABILITY.labels(
                resource_type=resource_type,
                resource_id=resource_id
            ).set(1 if status['available'] else 0)
            
            # Update current status
            self._current_status[resource_id] = status
            
            # Record in history
            self._record_status(resource_id, status)
            
            return status
            
        except Exception as e:
            logger.error(f"Failed to check availability: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    async def get_downtime_records(
        self,
        resource_id: int,
        start_time: datetime,
        end_time: datetime
    ) -> List[Dict[str, Any]]:
        """
        Get downtime records for a time period
        
        Args:
            resource_id: Resource identifier
            start_time: Start of period
            end_time: End of period
            
        Returns:
            List of downtime events
        """
        try:
            events = self._downtime_events.get(resource_id, [])
            
            # Filter by time range
            filtered_events = [
                event for event in events
                if (event['start_time'] >= start_time and 
                    event['start_time'] <= end_time)
            ]
            
            return filtered_events
            
        except Exception as e:
            logger.error(f"Failed to get downtime records: {e}")
            raise
    
    async def get_availability_metrics(
        self,
        resource_id: int,
        period_hours: int = 24
    ) -> Dict[str, Any]:
        """
        Get availability metrics for a resource
        
        Args:
            resource_id: Resource identifier
            period_hours: Period to calculate metrics for
            
        Returns:
            Availability metrics including SLA compliance
        """
        try:
            # Get historical data
            history = self._availability_history.get(resource_id, [])
            
            # Filter by time period
            cutoff_time = datetime.utcnow() - timedelta(hours=period_hours)
            period_history = [
                record for record in history
                if record['timestamp'] >= cutoff_time
            ]
            
            if not period_history:
                return {
                    'resource_id': resource_id,
                    'period_hours': period_hours,
                    'availability_percent': 100.0,
                    'downtime_minutes': 0,
                    'incident_count': 0,
                    'sla_compliant': True
                }
            
            # Calculate metrics
            total_checks = len(period_history)
            available_checks = sum(
                1 for record in period_history
                if record['status'] == ResourceStatus.AVAILABLE
            )
            
            availability_percent = (available_checks / total_checks) * 100
            
            # Get downtime events
            downtime_events = await self.get_downtime_records(
                resource_id,
                cutoff_time,
                datetime.utcnow()
            )
            
            total_downtime = sum(
                event['duration_seconds'] 
                for event in downtime_events
            )
            
            # Check SLA compliance
            resource_type = self._monitored_resources[resource_id]['resource_type']
            sla_threshold = self.sla_thresholds.get(resource_type, 0.99) * 100
            sla_compliant = availability_percent >= sla_threshold
            
            # Update SLA metric
            SLA_COMPLIANCE.labels(
                resource_type=resource_type,
                resource_id=resource_id
            ).set(availability_percent)
            
            return {
                'resource_id': resource_id,
                'resource_type': resource_type,
                'period_hours': period_hours,
                'availability_percent': availability_percent,
                'downtime_minutes': total_downtime / 60,
                'incident_count': len(downtime_events),
                'sla_threshold': sla_threshold,
                'sla_compliant': sla_compliant,
                'current_status': self._current_status.get(resource_id, {}).get('status', 'unknown')
            }
            
        except Exception as e:
            logger.error(f"Failed to get availability metrics: {e}")
            raise
    
    async def sign_downtime_event(
        self,
        resource_id: int,
        downtime_event: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Sign downtime event for on-chain verification
        
        Args:
            resource_id: Resource identifier
            downtime_event: Downtime event data
            
        Returns:
            Signed downtime data
        """
        try:
            # Prepare data for signing
            oracle_data = {
                'resource_id': resource_id,
                'start_time': int(downtime_event['start_time'].timestamp()),
                'end_time': int(downtime_event['end_time'].timestamp()),
                'duration': downtime_event['duration_seconds'],
                'reason': downtime_event.get('reason', 'unavailable')
            }
            
            # Sign the data
            signed_data = sign_oracle_data(
                oracle_data,
                self.signing_key,
                self.monitor_contract_address
            )
            
            return {
                'oracle_data': oracle_data,
                'signature': signed_data['signature'],
                'message_hash': signed_data['message_hash'],
                'signer': signed_data['signer']
            }
            
        except Exception as e:
            logger.error(f"Failed to sign downtime event: {e}")
            raise
    
    async def submit_downtime_record(
        self,
        resource_id: int,
        signed_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Submit downtime record to blockchain
        
        Args:
            resource_id: Resource identifier
            signed_data: Signed downtime data
            
        Returns:
            Transaction result
        """
        try:
            monitor_contract = await self.blockchain.get_contract(
                self.monitor_contract_address,
                "AvailabilityMonitor"
            )
            
            tx = await monitor_contract.functions.recordDowntime(
                resource_id,
                signed_data['oracle_data']['start_time'],
                signed_data['oracle_data']['end_time'],
                signed_data['oracle_data']['duration'],
                signed_data['oracle_data']['reason'],
                signed_data['signature']
            ).transact()
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            return {
                'tx_hash': tx,
                'block_number': receipt['blockNumber'],
                'gas_used': receipt['gasUsed']
            }
            
        except Exception as e:
            logger.error(f"Failed to submit downtime record: {e}")
            raise
    
    # Private monitoring methods
    
    async def _monitor_resource(self, resource_id: int):
        """Monitor a resource continuously"""
        while resource_id in self._monitored_resources:
            try:
                # Check availability
                status = await self.check_availability(resource_id)
                
                # Handle status changes
                await self._handle_status_change(resource_id, status)
                
                # Wait for next check
                await asyncio.sleep(self.check_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error monitoring resource {resource_id}: {e}")
                await asyncio.sleep(self.check_interval)
    
    async def _check_quantum_availability(
        self,
        resource_id: int,
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check quantum resource availability"""
        endpoint = config['endpoint']
        
        try:
            # Check quantum system API
            async with self._session.get(
                f"{endpoint}/health",
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # Check specific quantum metrics
                    qpu_online = data.get('qpu_online', False)
                    control_system = data.get('control_system', False)
                    calibration_valid = data.get('calibration_valid', False)
                    
                    available = qpu_online and control_system and calibration_valid
                    
                    return {
                        'resource_id': resource_id,
                        'status': ResourceStatus.AVAILABLE if available else ResourceStatus.UNAVAILABLE,
                        'available': available,
                        'timestamp': datetime.utcnow(),
                        'details': {
                            'qpu_online': qpu_online,
                            'control_system': control_system,
                            'calibration_valid': calibration_valid
                        }
                    }
                else:
                    return self._unavailable_status(resource_id, f"HTTP {response.status}")
                    
        except Exception as e:
            return self._unavailable_status(resource_id, str(e))
    
    async def _check_ai_availability(
        self,
        resource_id: int,
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check AI resource availability"""
        endpoint = config['endpoint']
        
        try:
            # Check AI accelerator API
            async with self._session.get(
                f"{endpoint}/status",
                timeout=aiohttp.ClientTimeout(total=20)
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # Check specific AI metrics
                    gpu_available = data.get('gpu_available', False)
                    memory_available = data.get('memory_available', 0) > 1000  # MB
                    temperature_ok = data.get('temperature', 100) < 85  # Celsius
                    
                    available = gpu_available and memory_available and temperature_ok
                    
                    return {
                        'resource_id': resource_id,
                        'status': ResourceStatus.AVAILABLE if available else ResourceStatus.DEGRADED,
                        'available': available,
                        'timestamp': datetime.utcnow(),
                        'details': {
                            'gpu_available': gpu_available,
                            'memory_mb': data.get('memory_available', 0),
                            'temperature_c': data.get('temperature', 0),
                            'utilization': data.get('utilization', 0)
                        }
                    }
                else:
                    return self._unavailable_status(resource_id, f"HTTP {response.status}")
                    
        except Exception as e:
            return self._unavailable_status(resource_id, str(e))
    
    async def _check_network_availability(
        self,
        resource_id: int,
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check network resource availability"""
        endpoint = config['endpoint']
        check_config = config['check_config']
        
        try:
            # Parse endpoint for ping check
            host = endpoint.replace('http://', '').replace('https://', '').split('/')[0]
            
            # Ping check
            ping_result = ping3.ping(host, timeout=2)
            
            if ping_result is not None:
                # Additional HTTP check
                try:
                    async with self._session.get(
                        endpoint,
                        timeout=aiohttp.ClientTimeout(total=10)
                    ) as response:
                        http_ok = response.status < 500
                except:
                    http_ok = False
                
                available = ping_result < 100 and http_ok  # < 100ms and HTTP OK
                
                return {
                    'resource_id': resource_id,
                    'status': ResourceStatus.AVAILABLE if available else ResourceStatus.DEGRADED,
                    'available': available,
                    'timestamp': datetime.utcnow(),
                    'details': {
                        'ping_ms': round(ping_result * 1000, 2) if ping_result else None,
                        'http_ok': http_ok,
                        'packet_loss': 0 if ping_result else 100
                    }
                }
            else:
                return self._unavailable_status(resource_id, "Ping failed")
                
        except Exception as e:
            return self._unavailable_status(resource_id, str(e))
    
    def _unavailable_status(self, resource_id: int, reason: str) -> Dict[str, Any]:
        """Create unavailable status response"""
        return {
            'resource_id': resource_id,
            'status': ResourceStatus.UNAVAILABLE,
            'available': False,
            'timestamp': datetime.utcnow(),
            'reason': reason,
            'details': {}
        }
    
    def _record_status(self, resource_id: int, status: Dict[str, Any]):
        """Record status in history"""
        history = self._availability_history[resource_id]
        
        # Add to history
        history.append({
            'status': status['status'],
            'timestamp': status['timestamp'],
            'available': status['available']
        })
        
        # Keep only recent history (24 hours)
        cutoff_time = datetime.utcnow() - timedelta(hours=24)
        self._availability_history[resource_id] = [
            record for record in history
            if record['timestamp'] > cutoff_time
        ]
    
    async def _handle_status_change(self, resource_id: int, new_status: Dict[str, Any]):
        """Handle resource status changes"""
        # Get previous status
        previous = self._current_status.get(resource_id, {})
        
        # Check if status changed
        if previous.get('status') != new_status['status']:
            # Handle transition to unavailable
            if new_status['status'] == ResourceStatus.UNAVAILABLE:
                # Create downtime event
                downtime_event = {
                    'resource_id': resource_id,
                    'start_time': new_status['timestamp'],
                    'end_time': None,
                    'duration_seconds': 0,
                    'reason': new_status.get('reason', 'unknown'),
                    'status': 'ongoing'
                }
                
                self._downtime_events[resource_id].append(downtime_event)
                
                logger.warning(f"Resource {resource_id} went down at {new_status['timestamp']}")
                
            # Handle transition from unavailable
            elif previous.get('status') == ResourceStatus.UNAVAILABLE:
                # Close downtime event
                events = self._downtime_events[resource_id]
                if events and events[-1]['status'] == 'ongoing':
                    event = events[-1]
                    event['end_time'] = new_status['timestamp']
                    event['duration_seconds'] = int(
                        (event['end_time'] - event['start_time']).total_seconds()
                    )
                    event['status'] = 'resolved'
                    
                    # Record downtime metric
                    DOWNTIME_DURATION.labels(
                        resource_type=self._monitored_resources[resource_id]['resource_type']
                    ).observe(event['duration_seconds'])
                    
                    # Sign and submit significant downtime events (> 5 minutes)
                    if event['duration_seconds'] > 300:
                        try:
                            signed_data = await self.sign_downtime_event(resource_id, event)
                            await self.submit_downtime_record(resource_id, signed_data)
                        except Exception as e:
                            logger.error(f"Failed to submit downtime record: {e}")
                    
                    logger.info(
                        f"Resource {resource_id} recovered after "
                        f"{event['duration_seconds']} seconds downtime"
                    ) 