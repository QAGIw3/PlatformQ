"""Compute market adapter for unified compute resource trading."""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
import uuid

from ..core import MatchingEngine, MarketConfig
from ..models.order import Order, OrderType, OrderSide
from ..state import IgniteStateManager
from ..events import FlinkEventProcessor


logger = logging.getLogger(__name__)


class ComputeResourceType(str, Enum):
    """Types of compute resources."""
    GPU = "gpu"
    CPU = "cpu"
    MEMORY = "memory"
    STORAGE = "storage"
    BANDWIDTH = "bandwidth"
    FPGA = "fpga"
    TPU = "tpu"
    QUANTUM = "quantum"


class ComputeMarketType(str, Enum):
    """Types of compute markets."""
    SPOT = "spot"
    FUTURES = "futures"
    OPTIONS = "options"
    RESERVED = "reserved"
    BURST = "burst"


class ComputeMarketAdapter:
    """
    Unified adapter for compute resource trading across different market types.
    
    This consolidates compute functionality from:
    1. compute-market-service (spot markets)
    2. derivatives-engine-service (futures/options)
    3. provisioning services (physical allocation)
    """
    
    def __init__(
        self,
        matching_engine: MatchingEngine,
        state_manager: IgniteStateManager,
        event_processor: FlinkEventProcessor
    ):
        self.matching_engine = matching_engine
        self.state_manager = state_manager
        self.event_processor = event_processor
        
        # Resource inventory tracking
        self.resource_inventory: Dict[str, Dict[str, Any]] = {}
        
        # Provider registry
        self.providers: Dict[str, Dict[str, Any]] = {}
        
        # Allocation tracking
        self.active_allocations: Dict[str, Dict[str, Any]] = {}
        
        # Market configurations by resource and type
        self.compute_markets: Dict[Tuple[ComputeResourceType, ComputeMarketType], str] = {}
    
    async def create_compute_market(
        self,
        resource_type: ComputeResourceType,
        market_type: ComputeMarketType,
        specifications: Dict[str, Any]
    ) -> str:
        """Create a new compute resource market."""
        # Generate market ID
        market_id = f"COMPUTE-{resource_type.value.upper()}-{market_type.value.upper()}"
        
        if market_type == ComputeMarketType.FUTURES:
            # Add expiry to market ID for futures
            expiry = specifications.get('expiry', datetime.utcnow() + timedelta(days=30))
            market_id += f"-{expiry.strftime('%Y%m%d')}"
        elif market_type == ComputeMarketType.OPTIONS:
            # Add strike and type for options
            strike = specifications.get('strike', 100)
            option_type = specifications.get('option_type', 'CALL')
            market_id += f"-{strike}-{option_type}"
        
        # Create market configuration
        config = MarketConfig(
            market_id=market_id,
            product_type=f"compute_{market_type.value}",
            tick_size=Decimal(specifications.get('tick_size', '0.01')),
            lot_size=Decimal(specifications.get('lot_size', '1')),
            max_order_size=Decimal(specifications.get('max_order_size', '1000')),
            price_bands=(Decimal('0.5'), Decimal('2.0')),  # 50% to 200% of reference
            circuit_breaker_threshold=Decimal('0.1'),  # 10% move
            halt_duration_seconds=300
        )
        
        # Store configuration
        self.matching_engine.market_configs[market_id] = config
        
        # Create order book
        if market_id not in self.matching_engine.order_books:
            from ..models.orderbook import OrderBook
            order_book = OrderBook(
                market_id=market_id,
                tick_size=config.tick_size
            )
            self.matching_engine.order_books[market_id] = order_book
            self.matching_engine.metrics[market_id] = self.matching_engine.MatchingMetrics()
        
        # Store market mapping
        self.compute_markets[(resource_type, market_type)] = market_id
        
        # Store market details
        market_data = {
            'market_id': market_id,
            'resource_type': resource_type.value,
            'market_type': market_type.value,
            'specifications': specifications,
            'status': 'open',
            'created_at': datetime.utcnow().isoformat(),
            **config.__dict__
        }
        
        await self.state_manager.put_market(market_id, market_data)
        
        logger.info(f"Created compute market: {market_id}")
        return market_id
    
    async def register_provider(
        self,
        provider_id: str,
        resources: Dict[ComputeResourceType, Dict[str, Any]]
    ) -> bool:
        """Register a compute resource provider."""
        try:
            provider_data = {
                'provider_id': provider_id,
                'resources': {
                    resource_type.value: specs
                    for resource_type, specs in resources.items()
                },
                'status': 'active',
                'reputation_score': 100,  # Start with perfect score
                'total_capacity': self._calculate_total_capacity(resources),
                'available_capacity': self._calculate_total_capacity(resources),
                'registered_at': datetime.utcnow().isoformat()
            }
            
            self.providers[provider_id] = provider_data
            
            # Update resource inventory
            for resource_type, specs in resources.items():
                if resource_type not in self.resource_inventory:
                    self.resource_inventory[resource_type] = {
                        'total_capacity': 0,
                        'available_capacity': 0,
                        'providers': []
                    }
                
                capacity = specs.get('capacity', 0)
                self.resource_inventory[resource_type]['total_capacity'] += capacity
                self.resource_inventory[resource_type]['available_capacity'] += capacity
                self.resource_inventory[resource_type]['providers'].append(provider_id)
            
            # Store in state manager
            await self.state_manager.put(
                f"provider:{provider_id}",
                provider_data
            )
            
            # Emit provider registration event
            await self.event_processor.publish_compute_event({
                'event_type': 'provider_registered',
                'provider_id': provider_id,
                'resources': provider_data['resources'],
                'timestamp': datetime.utcnow().isoformat()
            })
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to register provider {provider_id}: {e}")
            return False
    
    async def submit_compute_order(
        self,
        user_id: str,
        resource_type: ComputeResourceType,
        market_type: ComputeMarketType,
        quantity: Decimal,
        duration_hours: Optional[int] = None,
        specifications: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Submit an order for compute resources."""
        # Get market ID
        market_id = self.compute_markets.get((resource_type, market_type))
        
        if not market_id:
            return {
                'success': False,
                'reason': f"No market found for {resource_type.value} {market_type.value}"
            }
        
        # Calculate order parameters based on market type
        order_params = await self._calculate_order_params(
            resource_type,
            market_type,
            quantity,
            duration_hours,
            specifications
        )
        
        # Create order
        order = Order(
            user_id=user_id,
            market_id=market_id,
            product_type=f"compute_{market_type.value}",
            side=OrderSide.BUY,  # Users typically buy compute
            type=order_params['order_type'],
            quantity=quantity,
            price=order_params.get('price'),
            time_in_force='GTC',
            client_order_id=f"COMPUTE-{uuid.uuid4().hex[:8]}"
        )
        
        # Add compute-specific metadata
        order._metadata = {
            'resource_type': resource_type.value,
            'duration_hours': duration_hours,
            'specifications': specifications or {},
            'qos_tier': specifications.get('qos_tier', 'standard') if specifications else 'standard'
        }
        
        # Submit to matching engine
        result = await self.matching_engine.process_order(order)
        
        # Handle successful matches
        if result['success'] and result.get('trades'):
            await self._process_compute_trades(
                result['trades'],
                resource_type,
                market_type,
                duration_hours
            )
        
        return result
    
    async def allocate_resources(
        self,
        allocation_id: str,
        user_id: str,
        resource_type: ComputeResourceType,
        quantity: Decimal,
        duration_hours: int,
        specifications: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Allocate physical compute resources."""
        try:
            # Find available providers
            providers = await self._find_suitable_providers(
                resource_type,
                quantity,
                specifications
            )
            
            if not providers:
                return {
                    'success': False,
                    'reason': 'No available providers found'
                }
            
            # Select best provider based on score
            selected_provider = providers[0]
            
            # Create allocation
            allocation = {
                'allocation_id': allocation_id,
                'user_id': user_id,
                'provider_id': selected_provider['provider_id'],
                'resource_type': resource_type.value,
                'quantity': str(quantity),
                'duration_hours': duration_hours,
                'specifications': specifications,
                'status': 'active',
                'start_time': datetime.utcnow().isoformat(),
                'end_time': (datetime.utcnow() + timedelta(hours=duration_hours)).isoformat(),
                'price_per_hour': str(selected_provider['price_per_hour'])
            }
            
            # Update provider capacity
            provider = self.providers[selected_provider['provider_id']]
            provider['available_capacity'] -= float(quantity)
            
            # Update resource inventory
            self.resource_inventory[resource_type]['available_capacity'] -= float(quantity)
            
            # Store allocation
            self.active_allocations[allocation_id] = allocation
            await self.state_manager.put(
                f"allocation:{allocation_id}",
                allocation
            )
            
            # Emit allocation event
            await self.event_processor.publish_compute_event({
                'event_type': 'resource_allocated',
                'allocation': allocation,
                'timestamp': datetime.utcnow().isoformat()
            })
            
            return {
                'success': True,
                'allocation': allocation
            }
            
        except Exception as e:
            logger.error(f"Failed to allocate resources: {e}")
            return {
                'success': False,
                'reason': str(e)
            }
    
    async def release_resources(self, allocation_id: str) -> bool:
        """Release allocated compute resources."""
        if allocation_id not in self.active_allocations:
            return False
        
        allocation = self.active_allocations[allocation_id]
        
        # Update provider capacity
        provider = self.providers.get(allocation['provider_id'])
        if provider:
            provider['available_capacity'] += float(allocation['quantity'])
        
        # Update resource inventory
        resource_type = ComputeResourceType(allocation['resource_type'])
        self.resource_inventory[resource_type]['available_capacity'] += float(allocation['quantity'])
        
        # Update allocation status
        allocation['status'] = 'completed'
        allocation['actual_end_time'] = datetime.utcnow().isoformat()
        
        # Remove from active allocations
        del self.active_allocations[allocation_id]
        
        # Store final state
        await self.state_manager.put(
            f"allocation:{allocation_id}",
            allocation
        )
        
        # Emit release event
        await self.event_processor.publish_compute_event({
            'event_type': 'resource_released',
            'allocation_id': allocation_id,
            'timestamp': datetime.utcnow().isoformat()
        })
        
        return True
    
    async def get_compute_metrics(self) -> Dict[str, Any]:
        """Get comprehensive compute market metrics."""
        metrics = {
            'providers': {
                'total': len(self.providers),
                'active': len([p for p in self.providers.values() if p['status'] == 'active'])
            },
            'allocations': {
                'active': len(self.active_allocations),
                'total_value': sum(
                    float(a['quantity']) * float(a['price_per_hour']) * a['duration_hours']
                    for a in self.active_allocations.values()
                )
            },
            'resources': {}
        }
        
        # Resource-specific metrics
        for resource_type, inventory in self.resource_inventory.items():
            utilization = 0
            if inventory['total_capacity'] > 0:
                utilization = (
                    (inventory['total_capacity'] - inventory['available_capacity']) /
                    inventory['total_capacity'] * 100
                )
            
            metrics['resources'][resource_type.value] = {
                'total_capacity': inventory['total_capacity'],
                'available_capacity': inventory['available_capacity'],
                'utilization_percent': utilization,
                'provider_count': len(inventory['providers'])
            }
        
        # Market metrics
        metrics['markets'] = {}
        for (resource_type, market_type), market_id in self.compute_markets.items():
            if market_id in self.matching_engine.order_books:
                orderbook = self.matching_engine.order_books[market_id]
                best_bid, best_ask = orderbook.get_best_bid_ask()
                
                metrics['markets'][market_id] = {
                    'resource_type': resource_type.value,
                    'market_type': market_type.value,
                    'best_bid': str(best_bid) if best_bid else None,
                    'best_ask': str(best_ask) if best_ask else None,
                    'spread': str(best_ask - best_bid) if best_bid and best_ask else None
                }
        
        return metrics
    
    async def _calculate_order_params(
        self,
        resource_type: ComputeResourceType,
        market_type: ComputeMarketType,
        quantity: Decimal,
        duration_hours: Optional[int],
        specifications: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Calculate order parameters based on market type."""
        if market_type == ComputeMarketType.SPOT:
            # Get current spot price
            spot_price = await self._get_spot_price(resource_type, specifications)
            return {
                'order_type': OrderType.LIMIT,
                'price': spot_price * Decimal('1.05')  # 5% above spot
            }
        
        elif market_type == ComputeMarketType.FUTURES:
            # Calculate futures price based on duration
            base_price = await self._get_spot_price(resource_type, specifications)
            time_premium = Decimal('0.01') * Decimal(duration_hours or 24)
            return {
                'order_type': OrderType.LIMIT,
                'price': base_price * (Decimal('1') + time_premium)
            }
        
        elif market_type == ComputeMarketType.RESERVED:
            # Reserved instances get discount
            base_price = await self._get_spot_price(resource_type, specifications)
            discount = Decimal('0.7')  # 30% discount
            return {
                'order_type': OrderType.LIMIT,
                'price': base_price * discount
            }
        
        else:
            # Default to market order
            return {
                'order_type': OrderType.MARKET
            }
    
    async def _process_compute_trades(
        self,
        trades: List[Dict[str, Any]],
        resource_type: ComputeResourceType,
        market_type: ComputeMarketType,
        duration_hours: Optional[int]
    ):
        """Process compute-specific trade logic."""
        for trade in trades:
            # Add compute metadata
            trade['compute_details'] = {
                'resource_type': resource_type.value,
                'market_type': market_type.value,
                'duration_hours': duration_hours,
                'allocation_required': market_type in [
                    ComputeMarketType.SPOT,
                    ComputeMarketType.RESERVED
                ]
            }
            
            # Trigger allocation for spot/reserved trades
            if trade['compute_details']['allocation_required']:
                allocation_result = await self.allocate_resources(
                    allocation_id=f"ALLOC-{trade['trade_id']}",
                    user_id=trade['taker_user_id'],
                    resource_type=resource_type,
                    quantity=Decimal(trade['quantity']),
                    duration_hours=duration_hours or 1,
                    specifications={}
                )
                
                trade['allocation_id'] = allocation_result.get('allocation', {}).get('allocation_id')
    
    async def _find_suitable_providers(
        self,
        resource_type: ComputeResourceType,
        quantity: Decimal,
        specifications: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Find providers that can fulfill the resource request."""
        suitable_providers = []
        
        for provider_id, provider in self.providers.items():
            if provider['status'] != 'active':
                continue
            
            resources = provider.get('resources', {}).get(resource_type.value, {})
            if not resources:
                continue
            
            available = resources.get('capacity', 0) - resources.get('allocated', 0)
            if available < float(quantity):
                continue
            
            # Check specifications match
            if specifications:
                provider_specs = resources.get('specifications', {})
                if not self._specs_match(specifications, provider_specs):
                    continue
            
            # Calculate score
            score = provider['reputation_score']
            price_per_hour = resources.get('price_per_hour', 1.0)
            
            suitable_providers.append({
                'provider_id': provider_id,
                'score': score,
                'price_per_hour': Decimal(str(price_per_hour)),
                'available_capacity': available
            })
        
        # Sort by score (descending) then price (ascending)
        suitable_providers.sort(
            key=lambda p: (-p['score'], p['price_per_hour'])
        )
        
        return suitable_providers
    
    def _specs_match(self, required: Dict[str, Any], available: Dict[str, Any]) -> bool:
        """Check if provider specifications match requirements."""
        for key, value in required.items():
            if key not in available:
                return False
            
            if isinstance(value, (int, float)):
                if available[key] < value:
                    return False
            elif available[key] != value:
                return False
        
        return True
    
    def _calculate_total_capacity(self, resources: Dict[ComputeResourceType, Dict[str, Any]]) -> float:
        """Calculate total capacity across all resource types."""
        total = 0
        for resource_type, specs in resources.items():
            capacity = specs.get('capacity', 0)
            # Normalize different resource types
            if resource_type == ComputeResourceType.GPU:
                total += capacity * 100  # GPUs are more valuable
            elif resource_type == ComputeResourceType.CPU:
                total += capacity
            elif resource_type == ComputeResourceType.MEMORY:
                total += capacity / 1024  # Convert GB to TB
            elif resource_type == ComputeResourceType.STORAGE:
                total += capacity / 1024  # Convert GB to TB
            else:
                total += capacity
        
        return total
    
    async def _get_spot_price(
        self,
        resource_type: ComputeResourceType,
        specifications: Optional[Dict[str, Any]]
    ) -> Decimal:
        """Get current spot price for resource type."""
        # Base prices per resource type
        base_prices = {
            ComputeResourceType.GPU: Decimal('2.50'),  # per GPU-hour
            ComputeResourceType.CPU: Decimal('0.10'),  # per vCPU-hour
            ComputeResourceType.MEMORY: Decimal('0.01'),  # per GB-hour
            ComputeResourceType.STORAGE: Decimal('0.0001'),  # per GB-hour
            ComputeResourceType.BANDWIDTH: Decimal('0.05'),  # per GB
            ComputeResourceType.FPGA: Decimal('1.50'),  # per FPGA-hour
            ComputeResourceType.TPU: Decimal('5.00'),  # per TPU-hour
            ComputeResourceType.QUANTUM: Decimal('100.00')  # per QPU-hour
        }
        
        price = base_prices.get(resource_type, Decimal('1.00'))
        
        # Adjust for specifications
        if specifications:
            # Premium for high-performance variants
            if specifications.get('performance_tier') == 'high':
                price *= Decimal('1.5')
            elif specifications.get('performance_tier') == 'ultra':
                price *= Decimal('2.0')
            
            # Premium for specific locations
            if specifications.get('region'):
                # Add region-based pricing logic
                pass
        
        return price 