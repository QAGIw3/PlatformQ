from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import httpx

from .base_analyzer import BaseAnalyzer
from ..models.analytics_models import (
    TimeSeries, TimeInterval, ChainMetrics
)
from ..config import config


class TransactionAnalyzer(BaseAnalyzer):
    """Analyzer for blockchain transaction data"""
    
    def __init__(self):
        super().__init__("TransactionAnalyzer")
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
    async def collect_data(
        self,
        chain: str,
        start_time: datetime,
        end_time: datetime,
        **kwargs
    ) -> pd.DataFrame:
        """Collect transaction data from blockchain"""
        self.logger.info(f"Collecting transaction data for {chain} from {start_time} to {end_time}")
        
        # In production, would query from blockchain indexer or database
        # For now, generate sample data
        timestamps = pd.date_range(start=start_time, end=end_time, freq='1H')
        
        data = []
        for ts in timestamps:
            # Simulate transaction data
            base_tx_count = 1000 + np.random.randint(-200, 300)
            data.append({
                'timestamp': ts,
                'transaction_count': base_tx_count,
                'transaction_volume': str(base_tx_count * np.random.uniform(1000, 5000)),
                'gas_used': str(base_tx_count * np.random.uniform(21000, 50000)),
                'average_gas_price': str(np.random.uniform(20, 100) * 10**9),  # Gwei
                'failed_transactions': int(base_tx_count * np.random.uniform(0.01, 0.05)),
                'unique_addresses': int(base_tx_count * np.random.uniform(0.3, 0.5)),
                'contract_calls': int(base_tx_count * np.random.uniform(0.6, 0.8)),
                'block_count': len(timestamps)
            })
        
        df = pd.DataFrame(data)
        
        # Validate data
        is_valid, issues = await self.validate_data(df)
        if not is_valid:
            self.logger.warning(f"Data validation issues: {issues}")
        
        return df
    
    async def calculate_metrics(
        self,
        data: pd.DataFrame,
        metrics: List[str],
        interval: TimeInterval
    ) -> Dict[str, TimeSeries]:
        """Calculate transaction metrics"""
        # Aggregate data by interval
        aggregated = self.aggregate_by_interval(data, interval)
        
        results = {}
        
        if 'transaction_count' in metrics:
            results['transaction_count'] = self.create_time_series(
                aggregated,
                'transaction_count',
                'transaction_count',
                data.attrs.get('chain', 'unknown'),
                interval
            )
        
        if 'transaction_volume' in metrics:
            # Convert string volumes to float
            aggregated['transaction_volume_float'] = aggregated['transaction_volume'].astype(float)
            results['transaction_volume'] = self.create_time_series(
                aggregated,
                'transaction_volume',
                'transaction_volume_float',
                data.attrs.get('chain', 'unknown'),
                interval
            )
        
        if 'gas_metrics' in metrics:
            # Calculate gas metrics
            aggregated['gas_used_float'] = aggregated['gas_used'].astype(float)
            aggregated['avg_gas_price_float'] = aggregated['average_gas_price'].astype(float)
            
            results['gas_used'] = self.create_time_series(
                aggregated,
                'gas_used',
                'gas_used_float',
                data.attrs.get('chain', 'unknown'),
                interval
            )
            
            results['average_gas_price'] = self.create_time_series(
                aggregated,
                'average_gas_price',
                'avg_gas_price_float',
                data.attrs.get('chain', 'unknown'),
                interval
            )
        
        if 'success_rate' in metrics:
            # Calculate success rate
            aggregated['success_rate'] = (
                (aggregated['transaction_count'] - aggregated['failed_transactions']) /
                aggregated['transaction_count'] * 100
            )
            
            results['success_rate'] = self.create_time_series(
                aggregated,
                'success_rate',
                'success_rate',
                data.attrs.get('chain', 'unknown'),
                interval
            )
        
        if 'tps' in metrics:
            # Calculate transactions per second
            seconds_per_interval = {
                TimeInterval.ONE_MINUTE: 60,
                TimeInterval.FIVE_MINUTES: 300,
                TimeInterval.FIFTEEN_MINUTES: 900,
                TimeInterval.ONE_HOUR: 3600,
                TimeInterval.FOUR_HOURS: 14400,
                TimeInterval.ONE_DAY: 86400,
                TimeInterval.ONE_WEEK: 604800
            }
            
            seconds = seconds_per_interval.get(interval, 3600)
            aggregated['tps'] = aggregated['transaction_count'] / seconds
            
            results['tps'] = self.create_time_series(
                aggregated,
                'tps',
                'tps',
                data.attrs.get('chain', 'unknown'),
                interval
            )
        
        return results
    
    async def generate_insights(
        self,
        metrics: Dict[str, TimeSeries]
    ) -> List[Dict[str, Any]]:
        """Generate insights from transaction metrics"""
        insights = []
        
        # Analyze transaction count trends
        if 'transaction_count' in metrics:
            tx_series = metrics['transaction_count']
            if tx_series.data_points:
                values = [dp.value for dp in tx_series.data_points]
                df = pd.DataFrame({
                    'timestamp': [dp.timestamp for dp in tx_series.data_points],
                    'value': values
                }).set_index('timestamp')
                
                # Find trend
                trend_info = self.find_trends(df['value'])
                
                insights.append({
                    'type': 'trend',
                    'metric': 'transaction_count',
                    'insight': f"Transaction count is showing {trend_info['trend']} trend",
                    'details': trend_info,
                    'severity': 'info'
                })
                
                # Detect anomalies
                anomalies = self.detect_anomalies(df['value'])
                if anomalies.any():
                    anomaly_timestamps = df.index[anomalies].tolist()
                    insights.append({
                        'type': 'anomaly',
                        'metric': 'transaction_count',
                        'insight': f"Detected {len(anomaly_timestamps)} anomalies in transaction count",
                        'details': {
                            'timestamps': anomaly_timestamps,
                            'values': df.loc[anomalies, 'value'].tolist()
                        },
                        'severity': 'warning'
                    })
        
        # Analyze gas price trends
        if 'average_gas_price' in metrics:
            gas_series = metrics['average_gas_price']
            if gas_series.data_points:
                values = [dp.value for dp in gas_series.data_points]
                current_gas = values[-1] if values else 0
                avg_gas = sum(values) / len(values) if values else 0
                
                if current_gas > avg_gas * 1.5:
                    insights.append({
                        'type': 'alert',
                        'metric': 'gas_price',
                        'insight': 'Gas prices are significantly above average',
                        'details': {
                            'current': current_gas,
                            'average': avg_gas,
                            'increase_percent': ((current_gas - avg_gas) / avg_gas * 100)
                        },
                        'severity': 'high'
                    })
        
        # Analyze success rate
        if 'success_rate' in metrics:
            success_series = metrics['success_rate']
            if success_series.data_points:
                recent_values = [dp.value for dp in success_series.data_points[-10:]]
                avg_success_rate = sum(recent_values) / len(recent_values) if recent_values else 0
                
                if avg_success_rate < 95:
                    insights.append({
                        'type': 'performance',
                        'metric': 'success_rate',
                        'insight': 'Transaction success rate is below optimal levels',
                        'details': {
                            'current_rate': avg_success_rate,
                            'optimal_rate': 95,
                            'gap': 95 - avg_success_rate
                        },
                        'severity': 'medium'
                    })
        
        # Analyze TPS
        if 'tps' in metrics:
            tps_series = metrics['tps']
            if tps_series.data_points:
                values = [dp.value for dp in tps_series.data_points]
                max_tps = max(values) if values else 0
                avg_tps = sum(values) / len(values) if values else 0
                
                insights.append({
                    'type': 'capacity',
                    'metric': 'tps',
                    'insight': f"Network processing average {avg_tps:.2f} TPS",
                    'details': {
                        'average_tps': avg_tps,
                        'max_tps': max_tps,
                        'current_tps': values[-1] if values else 0
                    },
                    'severity': 'info'
                })
        
        return insights
    
    async def analyze_transaction_patterns(
        self,
        chain: str,
        time_window: timedelta
    ) -> Dict[str, Any]:
        """Analyze transaction patterns over time"""
        end_time = datetime.utcnow()
        start_time = end_time - time_window
        
        # Collect data
        data = await self.collect_data(chain, start_time, end_time)
        
        patterns = {
            'peak_hours': [],
            'low_activity_periods': [],
            'weekly_pattern': {},
            'anomalies': []
        }
        
        # Analyze hourly patterns
        data['hour'] = pd.to_datetime(data['timestamp']).dt.hour
        hourly_avg = data.groupby('hour')['transaction_count'].mean()
        
        # Find peak hours
        threshold = hourly_avg.mean() + hourly_avg.std()
        peak_hours = hourly_avg[hourly_avg > threshold].index.tolist()
        patterns['peak_hours'] = peak_hours
        
        # Find low activity periods
        low_threshold = hourly_avg.mean() - hourly_avg.std()
        low_hours = hourly_avg[hourly_avg < low_threshold].index.tolist()
        patterns['low_activity_periods'] = low_hours
        
        # Analyze weekly patterns
        data['day_of_week'] = pd.to_datetime(data['timestamp']).dt.day_name()
        weekly_avg = data.groupby('day_of_week')['transaction_count'].mean()
        patterns['weekly_pattern'] = weekly_avg.to_dict()
        
        return patterns
    
    async def calculate_network_health_score(
        self,
        chain: str,
        metrics: Dict[str, TimeSeries]
    ) -> float:
        """Calculate overall network health score (0-100)"""
        score = 100.0
        
        # Check success rate
        if 'success_rate' in metrics:
            success_series = metrics['success_rate']
            if success_series.data_points:
                recent_values = [dp.value for dp in success_series.data_points[-10:]]
                avg_success = sum(recent_values) / len(recent_values) if recent_values else 0
                
                # Deduct points for low success rate
                if avg_success < 99:
                    score -= (99 - avg_success) * 2
        
        # Check for anomalies
        if 'transaction_count' in metrics:
            tx_series = metrics['transaction_count']
            if tx_series.data_points:
                values = [dp.value for dp in tx_series.data_points]
                df = pd.Series(values)
                anomalies = self.detect_anomalies(df)
                
                # Deduct points for anomalies
                anomaly_ratio = anomalies.sum() / len(anomalies)
                score -= anomaly_ratio * 20
        
        # Check gas price stability
        if 'average_gas_price' in metrics:
            gas_series = metrics['average_gas_price']
            if gas_series.data_points:
                values = [dp.value for dp in gas_series.data_points]
                df = pd.Series(values)
                volatility = df.std() / df.mean() if df.mean() > 0 else 0
                
                # Deduct points for high volatility
                if volatility > 0.5:
                    score -= min(volatility * 10, 20)
        
        return max(0, min(100, score))
    
    async def close(self):
        """Clean up resources"""
        await self.http_client.aclose() 