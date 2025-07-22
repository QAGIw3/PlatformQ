"""
Real-time Risk Analytics Flink Job

Processes trading events, position updates, and market data to compute
real-time risk metrics using Apache Flink's Python API (PyFlink).
"""

import os
import json
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Any

from pyflink.common import WatermarkStrategy, Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import (
    ProcessFunction, KeyedProcessFunction, ProcessWindowFunction,
    AggregateFunction, RuntimeContext
)
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor
from pyflink.datastream.window import SlidingEventTimeWindows, Time

import pulsar
from pyignite import Client as IgniteClient
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TradingEvent:
    """Represents a trading event"""
    def __init__(self, data: Dict[str, Any]):
        self.event_type = data.get("event_type")
        self.user_id = data.get("user_id")
        self.symbol = data.get("symbol")
        self.side = data.get("side")
        self.quantity = Decimal(str(data.get("quantity", 0)))
        self.price = Decimal(str(data.get("price", 0)))
        self.timestamp = data.get("timestamp", datetime.utcnow().timestamp())
        self.order_id = data.get("order_id")
        self.position_id = data.get("position_id")


class ExposureCalculator(KeyedProcessFunction):
    """Calculates real-time exposure from trading events"""
    
    def __init__(self):
        self.exposure_state = None
        self.ignite_client = None
        
    def open(self, runtime_context: RuntimeContext):
        """Initialize state and connections"""
        # Initialize state
        state_descriptor = ValueStateDescriptor(
            "user-exposure",
            Types.PICKLED_BYTE_ARRAY()
        )
        self.exposure_state = runtime_context.get_state(state_descriptor)
        
        # Initialize Ignite connection
        self.ignite_client = IgniteClient()
        self.ignite_client.connect([
            (os.environ.get("IGNITE_HOST", "localhost"), 10800)
        ])
        
    def process_element(self, value: str, ctx: KeyedProcessFunction.Context):
        """Process trading event and update exposure"""
        try:
            # Parse event
            event_data = json.loads(value)
            event = TradingEvent(event_data)
            
            # Get current exposure
            current_exposure = self.exposure_state.value()
            if current_exposure is None:
                current_exposure = {
                    "user_id": event.user_id,
                    "positions": {},
                    "total_exposure": Decimal("0"),
                    "net_exposure": Decimal("0"),
                    "position_count": 0
                }
            
            # Update based on event type
            if event.event_type == "ORDER_FILLED":
                position_key = f"{event.symbol}_{event.position_id}"
                
                if position_key not in current_exposure["positions"]:
                    current_exposure["positions"][position_key] = {
                        "symbol": event.symbol,
                        "quantity": Decimal("0"),
                        "avg_price": Decimal("0"),
                        "market_value": Decimal("0")
                    }
                
                position = current_exposure["positions"][position_key]
                
                # Update position
                if event.side == "buy":
                    new_quantity = position["quantity"] + event.quantity
                    if new_quantity > 0:
                        position["avg_price"] = (
                            (position["quantity"] * position["avg_price"] + 
                             event.quantity * event.price) / new_quantity
                        )
                    position["quantity"] = new_quantity
                else:  # sell
                    position["quantity"] -= event.quantity
                    
                # Calculate market value (simplified)
                position["market_value"] = position["quantity"] * event.price
                
            elif event.event_type == "POSITION_CLOSED":
                # Remove position
                position_key = f"{event.symbol}_{event.position_id}"
                if position_key in current_exposure["positions"]:
                    del current_exposure["positions"][position_key]
            
            # Recalculate aggregate metrics
            total_exposure = Decimal("0")
            net_exposure = Decimal("0")
            
            for pos in current_exposure["positions"].values():
                exposure = abs(pos["market_value"])
                total_exposure += exposure
                net_exposure += pos["market_value"]
                
            current_exposure["total_exposure"] = total_exposure
            current_exposure["net_exposure"] = net_exposure
            current_exposure["position_count"] = len(current_exposure["positions"])
            
            # Update state
            self.exposure_state.update(current_exposure)
            
            # Write to Ignite for real-time queries
            cache = self.ignite_client.get_or_create_cache("user_exposure")
            cache.put(event.user_id, current_exposure)
            
            # Emit exposure update
            exposure_update = {
                "user_id": event.user_id,
                "timestamp": event.timestamp,
                "total_exposure": float(total_exposure),
                "net_exposure": float(net_exposure),
                "position_count": current_exposure["position_count"],
                "update_type": event.event_type
            }
            
            yield json.dumps(exposure_update)
            
        except Exception as e:
            logger.error(f"Error processing trading event: {e}")


class RiskLimitMonitor(KeyedProcessFunction):
    """Monitors risk limits and generates alerts"""
    
    def __init__(self):
        self.limits_state = None
        self.ignite_client = None
        self.pulsar_client = None
        self.alert_producer = None
        
    def open(self, runtime_context: RuntimeContext):
        """Initialize state and connections"""
        # Initialize state
        limits_descriptor = ValueStateDescriptor(
            "risk-limits",
            Types.PICKLED_BYTE_ARRAY()
        )
        self.limits_state = runtime_context.get_state(limits_descriptor)
        
        # Initialize Ignite
        self.ignite_client = IgniteClient()
        self.ignite_client.connect([
            (os.environ.get("IGNITE_HOST", "localhost"), 10800)
        ])
        
        # Initialize Pulsar for alerts
        self.pulsar_client = pulsar.Client(
            os.environ.get("PULSAR_URL", "pulsar://localhost:6650")
        )
        self.alert_producer = self.pulsar_client.create_producer(
            "persistent://public/default/risk-alerts"
        )
        
    def process_element(self, value: str, ctx: KeyedProcessFunction.Context):
        """Check exposure against limits"""
        try:
            # Parse exposure update
            exposure = json.loads(value)
            user_id = exposure["user_id"]
            
            # Get or load risk limits
            limits = self.limits_state.value()
            if limits is None:
                # Load from Ignite
                limits_cache = self.ignite_client.get_or_create_cache("risk_limits")
                limits = limits_cache.get(user_id)
                
                if limits is None:
                    # Use defaults
                    limits = {
                        "max_exposure": 1000000.0,
                        "max_net_exposure": 500000.0,
                        "max_positions": 100,
                        "max_single_position": 100000.0
                    }
                    
                self.limits_state.update(limits)
            
            # Check limits and generate alerts
            alerts = []
            
            if exposure["total_exposure"] > limits["max_exposure"]:
                alerts.append({
                    "user_id": user_id,
                    "alert_type": "MAX_EXPOSURE_BREACH",
                    "severity": "HIGH",
                    "message": f"Total exposure {exposure['total_exposure']:.2f} exceeds limit {limits['max_exposure']:.2f}",
                    "timestamp": exposure["timestamp"],
                    "current_value": exposure["total_exposure"],
                    "limit_value": limits["max_exposure"]
                })
                
            if abs(exposure["net_exposure"]) > limits["max_net_exposure"]:
                alerts.append({
                    "user_id": user_id,
                    "alert_type": "NET_EXPOSURE_BREACH", 
                    "severity": "MEDIUM",
                    "message": f"Net exposure {exposure['net_exposure']:.2f} exceeds limit {limits['max_net_exposure']:.2f}",
                    "timestamp": exposure["timestamp"],
                    "current_value": abs(exposure["net_exposure"]),
                    "limit_value": limits["max_net_exposure"]
                })
                
            if exposure["position_count"] > limits["max_positions"]:
                alerts.append({
                    "user_id": user_id,
                    "alert_type": "POSITION_COUNT_BREACH",
                    "severity": "LOW",
                    "message": f"Position count {exposure['position_count']} exceeds limit {limits['max_positions']}",
                    "timestamp": exposure["timestamp"],
                    "current_value": exposure["position_count"],
                    "limit_value": limits["max_positions"]
                })
            
            # Send alerts
            for alert in alerts:
                self.alert_producer.send(
                    json.dumps(alert).encode('utf-8')
                )
                yield json.dumps(alert)
                
        except Exception as e:
            logger.error(f"Error monitoring risk limits: {e}")


class PortfolioVaRCalculator(ProcessWindowFunction):
    """Calculates portfolio VaR in sliding windows"""
    
    def __init__(self):
        self.market_data_cache = None
        
    def open(self, runtime_context: RuntimeContext):
        """Initialize connections"""
        # Connect to Ignite for market data
        ignite_client = IgniteClient()
        ignite_client.connect([
            (os.environ.get("IGNITE_HOST", "localhost"), 10800)
        ])
        self.market_data_cache = ignite_client.get_or_create_cache("market_data")
        
    def process(self, key: str, context: ProcessWindowFunction.Context, 
                elements: List[str]) -> List[str]:
        """Calculate VaR for portfolio"""
        try:
            # Aggregate positions from window
            positions = {}
            
            for element in elements:
                data = json.loads(element)
                if "positions" in data:
                    positions.update(data["positions"])
            
            if not positions:
                return
                
            # Get market data for VaR calculation
            returns_data = []
            portfolio_value = Decimal("0")
            
            for position in positions.values():
                symbol = position["symbol"]
                quantity = position["quantity"]
                
                # Get historical prices from cache
                hist_prices = self.market_data_cache.get(f"{symbol}_hist_prices")
                if hist_prices and len(hist_prices) > 1:
                    # Calculate returns
                    prices = np.array([float(p) for p in hist_prices])
                    returns = np.diff(prices) / prices[:-1]
                    
                    # Weight by position
                    position_value = quantity * Decimal(str(prices[-1]))
                    portfolio_value += position_value
                    
                    weighted_returns = returns * float(position_value)
                    returns_data.extend(weighted_returns)
            
            if returns_data and portfolio_value > 0:
                # Calculate VaR (95% confidence)
                returns_array = np.array(returns_data)
                var_95 = np.percentile(returns_array, 5) * -1
                
                # Calculate CVaR (Expected Shortfall)
                worst_returns = returns_array[returns_array <= -var_95]
                cvar_95 = -np.mean(worst_returns) if len(worst_returns) > 0 else var_95
                
                var_result = {
                    "user_id": key,
                    "timestamp": datetime.utcnow().timestamp(),
                    "portfolio_value": float(portfolio_value),
                    "var_95": float(var_95),
                    "cvar_95": float(cvar_95),
                    "confidence_level": 0.95,
                    "time_horizon": "1d",
                    "method": "historical_simulation"
                }
                
                yield json.dumps(var_result)
                
        except Exception as e:
            logger.error(f"Error calculating VaR: {e}")


def create_risk_analytics_job():
    """Create and configure the Flink job"""
    
    # Set up execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    env.enable_checkpointing(60000)  # Checkpoint every minute
    
    # Configure Pulsar sources
    trading_events_source = FlinkKafkaConsumer(
        topics="trading-events",
        deserialization_schema=SimpleStringSchema(),
        properties={
            "bootstrap.servers": os.environ.get("KAFKA_BROKER", "localhost:9092"),
            "group.id": "risk-analytics-job"
        }
    )
    
    position_updates_source = FlinkKafkaConsumer(
        topics="position-updates", 
        deserialization_schema=SimpleStringSchema(),
        properties={
            "bootstrap.servers": os.environ.get("KAFKA_BROKER", "localhost:9092"),
            "group.id": "risk-analytics-job"
        }
    )
    
    # Create streams
    trading_events = env.add_source(trading_events_source) \
        .name("Trading Events Source")
        
    position_updates = env.add_source(position_updates_source) \
        .name("Position Updates Source")
    
    # Calculate exposure from trading events
    exposure_updates = trading_events \
        .key_by(lambda x: json.loads(x).get("user_id")) \
        .process(ExposureCalculator()) \
        .name("Calculate Exposure")
    
    # Monitor risk limits
    risk_alerts = exposure_updates \
        .key_by(lambda x: json.loads(x).get("user_id")) \
        .process(RiskLimitMonitor()) \
        .name("Monitor Risk Limits")
    
    # Calculate portfolio VaR
    portfolio_var = position_updates \
        .key_by(lambda x: json.loads(x).get("user_id")) \
        .window(SlidingEventTimeWindows.of(
            Time.minutes(5),
            Time.minutes(1)
        )) \
        .process(PortfolioVaRCalculator()) \
        .name("Calculate Portfolio VaR")
    
    # Configure sinks
    exposure_sink = FlinkKafkaProducer(
        topic="exposure-updates",
        serialization_schema=SimpleStringSchema(),
        producer_config={
            "bootstrap.servers": os.environ.get("KAFKA_BROKER", "localhost:9092")
        }
    )
    
    var_sink = FlinkKafkaProducer(
        topic="var-calculations",
        serialization_schema=SimpleStringSchema(),
        producer_config={
            "bootstrap.servers": os.environ.get("KAFKA_BROKER", "localhost:9092")
        }
    )
    
    # Add sinks
    exposure_updates.add_sink(exposure_sink).name("Exposure Sink")
    portfolio_var.add_sink(var_sink).name("VaR Sink")
    
    return env


def main():
    """Main entry point"""
    try:
        logger.info("Starting Risk Analytics Flink Job")
        
        # Create and execute job
        env = create_risk_analytics_job()
        env.execute("Risk Analytics Real-time Processing")
        
    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise


if __name__ == "__main__":
    main() 