from prometheus_client import Counter, Histogram, Gauge, generate_latest
import time

class PrometheusMetrics:
    """
    Prometheus metrics collector for derivatives engine service
    """
    def __init__(self):
        # Trading metrics
        self.trades_total = Counter('derivatives_trades_total', 'Total number of trades', ['market_id', 'trade_type'])
        self.trade_volume = Counter('derivatives_trade_volume_usd', 'Total trade volume in USD', ['market_id'])
        self.trade_latency = Histogram('derivatives_trade_latency_seconds', 'Trade execution latency')
        
        # Position metrics
        self.open_positions = Gauge('derivatives_open_positions', 'Number of open positions', ['market_id'])
        self.total_collateral = Gauge('derivatives_total_collateral_usd', 'Total collateral locked in USD')
        
        # Liquidation metrics
        self.liquidations_total = Counter('derivatives_liquidations_total', 'Total number of liquidations', ['market_id'])
        self.liquidation_volume = Counter('derivatives_liquidation_volume_usd', 'Total liquidation volume in USD')
        
        # System metrics
        self.active_markets = Gauge('derivatives_active_markets', 'Number of active derivative markets')
        self.websocket_connections = Gauge('derivatives_websocket_connections', 'Number of active WebSocket connections')
        
    def generate_metrics(self):
        """
        Generate Prometheus metrics in text format
        """
        return generate_latest() 