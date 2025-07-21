"""Derivatives service adapter for trading core integration."""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime
from decimal import Decimal
import uuid

from ..core import MatchingEngine, MarketConfig
from ..models.order import Order, OrderType, OrderSide
from ..models.trade import Trade
from ..state import IgniteStateManager
from ..events import FlinkEventProcessor


logger = logging.getLogger(__name__)


class DerivativesAdapter:
    """
    Adapter to allow derivatives-engine-service to use the unified matching engine.
    
    This provides a compatibility layer that:
    1. Translates derivatives-specific order types and structures
    2. Handles derivatives-specific validations and risk checks
    3. Manages futures/options specific features (funding, settlement, etc.)
    4. Integrates with neuromorphic and other advanced features
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
        
        # Derivatives-specific handlers
        self.pre_match_handlers: Dict[str, List[Callable]] = {}
        self.post_match_handlers: Dict[str, List[Callable]] = {}
        
        # Product-specific configurations
        self.product_configs: Dict[str, Dict[str, Any]] = {}
    
    async def register_derivatives_market(
        self,
        market_id: str,
        product_type: str,
        contract_specs: Dict[str, Any]
    ) -> bool:
        """Register a derivatives market with the unified engine."""
        try:
            # Create market configuration
            config = MarketConfig(
                market_id=market_id,
                product_type=product_type,
                tick_size=Decimal(contract_specs.get('tick_size', '0.01')),
                lot_size=Decimal(contract_specs.get('lot_size', '1')),
                max_order_size=Decimal(contract_specs.get('max_order_size', '10000')),
                price_bands=(
                    Decimal(contract_specs.get('price_band_low', '0.95')),
                    Decimal(contract_specs.get('price_band_high', '1.05'))
                ),
                circuit_breaker_threshold=Decimal(
                    contract_specs.get('circuit_breaker_threshold', '0.05')
                ),
                halt_duration_seconds=contract_specs.get('halt_duration', 300)
            )
            
            # Store configuration
            self.matching_engine.market_configs[market_id] = config
            
            # Create order book if not exists
            if market_id not in self.matching_engine.order_books:
                from ..models.orderbook import OrderBook
                order_book = OrderBook(
                    market_id=market_id,
                    tick_size=config.tick_size
                )
                self.matching_engine.order_books[market_id] = order_book
                self.matching_engine.metrics[market_id] = self.matching_engine.MatchingMetrics()
            
            # Store product-specific configuration
            self.product_configs[market_id] = {
                'product_type': product_type,
                'contract_specs': contract_specs,
                'expiry': contract_specs.get('expiry'),
                'settlement_type': contract_specs.get('settlement_type', 'cash'),
                'multiplier': Decimal(contract_specs.get('multiplier', '1')),
                'funding_interval': contract_specs.get('funding_interval'),
                'option_type': contract_specs.get('option_type'),  # call/put
                'strike': contract_specs.get('strike'),
                'is_perpetual': contract_specs.get('is_perpetual', False)
            }
            
            # Store in state manager
            await self.state_manager.put_market(market_id, {
                'market_id': market_id,
                'status': 'open',
                **config.__dict__,
                **self.product_configs[market_id]
            })
            
            logger.info(f"Registered derivatives market: {market_id} ({product_type})")
            return True
            
        except Exception as e:
            logger.error(f"Failed to register derivatives market {market_id}: {e}")
            return False
    
    async def submit_derivatives_order(
        self,
        order_data: Dict[str, Any],
        neuromorphic_hint: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Submit a derivatives order with enhanced processing."""
        try:
            market_id = order_data['market_id']
            product_config = self.product_configs.get(market_id, {})
            
            # Create order object
            order = Order(
                order_id=order_data.get('id', str(uuid.uuid4())),
                user_id=order_data['trader_id'],
                market_id=market_id,
                product_type=product_config.get('product_type', 'derivatives'),
                side=OrderSide(order_data['side'].lower()),
                type=self._map_order_type(order_data['order_type']),
                quantity=Decimal(str(order_data['quantity'])),
                price=Decimal(str(order_data['price'])) if order_data.get('price') else None,
                stop_price=Decimal(str(order_data.get('stop_price', 0))) if order_data.get('stop_price') else None,
                time_in_force=order_data.get('time_in_force', 'GTC'),
                client_order_id=order_data.get('client_order_id')
            )
            
            # Apply neuromorphic hint if provided
            if neuromorphic_hint:
                order._neuromorphic_hint = neuromorphic_hint
            
            # Run pre-match handlers
            if market_id in self.pre_match_handlers:
                for handler in self.pre_match_handlers[market_id]:
                    order = await handler(order, product_config)
            
            # Submit to unified matching engine
            result = await self.matching_engine.process_order(order)
            
            # Run post-match handlers
            if result['success'] and market_id in self.post_match_handlers:
                for handler in self.post_match_handlers[market_id]:
                    await handler(result, product_config)
            
            # Handle derivatives-specific post-processing
            if result['success'] and result.get('trades'):
                await self._process_derivatives_trades(
                    result['trades'],
                    product_config
                )
            
            return result
            
        except Exception as e:
            logger.error(f"Error submitting derivatives order: {e}")
            return {
                'success': False,
                'order_id': order_data.get('id', ''),
                'reason': str(e),
                'timestamp': datetime.utcnow().timestamp()
            }
    
    async def add_pre_match_handler(
        self,
        market_id: str,
        handler: Callable
    ):
        """Add a pre-match handler for derivatives-specific logic."""
        if market_id not in self.pre_match_handlers:
            self.pre_match_handlers[market_id] = []
        self.pre_match_handlers[market_id].append(handler)
    
    async def add_post_match_handler(
        self,
        market_id: str,
        handler: Callable
    ):
        """Add a post-match handler for derivatives-specific logic."""
        if market_id not in self.post_match_handlers:
            self.post_match_handlers[market_id] = []
        self.post_match_handlers[market_id].append(handler)
    
    async def _process_derivatives_trades(
        self,
        trades: List[Dict[str, Any]],
        product_config: Dict[str, Any]
    ):
        """Process derivatives-specific trade logic."""
        for trade in trades:
            # Calculate notional value with multiplier
            multiplier = product_config.get('multiplier', Decimal('1'))
            trade['notional_value'] = str(
                Decimal(trade['price']) * 
                Decimal(trade['quantity']) * 
                multiplier
            )
            
            # Add derivatives metadata
            trade['product_type'] = product_config['product_type']
            trade['contract_specs'] = {
                'multiplier': str(multiplier),
                'expiry': product_config.get('expiry'),
                'settlement_type': product_config.get('settlement_type')
            }
            
            # Store enhanced trade data
            await self.state_manager.put_trade(
                trade['trade_id'],
                trade
            )
    
    def _map_order_type(self, derivatives_order_type: str) -> OrderType:
        """Map derivatives order types to unified types."""
        mapping = {
            'LIMIT': OrderType.LIMIT,
            'MARKET': OrderType.MARKET,
            'STOP': OrderType.STOP,
            'STOP_LIMIT': OrderType.STOP_LIMIT,
            'TAKE_PROFIT': OrderType.STOP,  # Map to stop with inverted logic
            'TRAILING_STOP': OrderType.STOP,  # Would need custom handling
            'ICEBERG': OrderType.LIMIT,  # With hidden quantity
            'POST_ONLY': OrderType.POST_ONLY,
            'REDUCE_ONLY': OrderType.LIMIT,  # With reduce-only flag
        }
        
        return mapping.get(
            derivatives_order_type.upper(),
            OrderType.LIMIT
        )
    
    async def get_derivatives_orderbook(
        self,
        market_id: str,
        depth: int = 20,
        aggregate: bool = False
    ) -> Dict[str, Any]:
        """Get orderbook with derivatives-specific enhancements."""
        orderbook = self.matching_engine.get_order_book(market_id, depth)
        
        if not orderbook:
            return None
        
        product_config = self.product_configs.get(market_id, {})
        
        # Add derivatives-specific data
        orderbook['product_type'] = product_config.get('product_type')
        orderbook['multiplier'] = str(product_config.get('multiplier', 1))
        
        # Calculate implied metrics for options
        if product_config.get('product_type') == 'option':
            orderbook['implied_volatility'] = await self._calculate_implied_vol(
                market_id,
                orderbook
            )
        
        # Add funding rate for perpetuals
        if product_config.get('is_perpetual'):
            orderbook['funding_rate'] = await self._get_funding_rate(market_id)
            orderbook['next_funding_time'] = await self._get_next_funding_time(market_id)
        
        return orderbook
    
    async def trigger_settlement(
        self,
        market_id: str,
        settlement_price: Decimal
    ) -> Dict[str, Any]:
        """Trigger settlement for expired derivatives contracts."""
        product_config = self.product_configs.get(market_id, {})
        
        if not product_config:
            return {
                'success': False,
                'reason': 'Market not found'
            }
        
        # Cancel all open orders
        order_book = self.matching_engine.order_books.get(market_id)
        if order_book:
            # Clear the order book
            order_book.clear()
        
        # Mark market as settled
        await self.state_manager.put_market(market_id, {
            'market_id': market_id,
            'status': 'settled',
            'settlement_price': str(settlement_price),
            'settlement_time': datetime.utcnow().isoformat()
        })
        
        # Emit settlement event
        await self.event_processor.publish_settlement_event({
            'market_id': market_id,
            'settlement_price': str(settlement_price),
            'product_type': product_config['product_type'],
            'settlement_type': product_config.get('settlement_type', 'cash')
        })
        
        return {
            'success': True,
            'market_id': market_id,
            'settlement_price': str(settlement_price),
            'timestamp': datetime.utcnow().timestamp()
        }
    
    async def _calculate_implied_vol(
        self,
        market_id: str,
        orderbook: Dict[str, Any]
    ) -> Optional[float]:
        """Calculate implied volatility from orderbook."""
        # Placeholder - would implement actual IV calculation
        return None
    
    async def _get_funding_rate(self, market_id: str) -> Optional[str]:
        """Get current funding rate for perpetual contracts."""
        # Placeholder - would fetch from funding engine
        return None
    
    async def _get_next_funding_time(self, market_id: str) -> Optional[str]:
        """Get next funding time for perpetual contracts."""
        # Placeholder - would calculate based on funding interval
        return None
    
    def get_adapter_metrics(self) -> Dict[str, Any]:
        """Get adapter-specific metrics."""
        return {
            'registered_markets': len(self.product_configs),
            'markets_by_type': self._count_by_product_type(),
            'pre_handlers': {
                market: len(handlers) 
                for market, handlers in self.pre_match_handlers.items()
            },
            'post_handlers': {
                market: len(handlers)
                for market, handlers in self.post_match_handlers.items()
            }
        }
    
    def _count_by_product_type(self) -> Dict[str, int]:
        """Count markets by product type."""
        counts = {}
        for config in self.product_configs.values():
            product_type = config.get('product_type', 'unknown')
            counts[product_type] = counts.get(product_type, 0) + 1
        return counts 