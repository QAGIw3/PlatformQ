from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

from ..models.analytics_models import (
    TimeSeries, DataPoint, TimeInterval,
    MetricType, AnalyticsQuery
)


class BaseAnalyzer(ABC):
    """Base class for blockchain data analyzers"""
    
    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(f"{__name__}.{name}")
        
    @abstractmethod
    async def collect_data(
        self,
        chain: str,
        start_time: datetime,
        end_time: datetime,
        **kwargs
    ) -> pd.DataFrame:
        """Collect raw data for analysis"""
        pass
    
    @abstractmethod
    async def calculate_metrics(
        self,
        data: pd.DataFrame,
        metrics: List[str],
        interval: TimeInterval
    ) -> Dict[str, TimeSeries]:
        """Calculate specified metrics from raw data"""
        pass
    
    @abstractmethod
    async def generate_insights(
        self,
        metrics: Dict[str, TimeSeries]
    ) -> List[Dict[str, Any]]:
        """Generate insights from calculated metrics"""
        pass
    
    def aggregate_by_interval(
        self,
        df: pd.DataFrame,
        interval: TimeInterval,
        timestamp_col: str = 'timestamp'
    ) -> pd.DataFrame:
        """Aggregate data by time interval"""
        # Convert interval to pandas frequency
        interval_map = {
            TimeInterval.ONE_MINUTE: '1T',
            TimeInterval.FIVE_MINUTES: '5T',
            TimeInterval.FIFTEEN_MINUTES: '15T',
            TimeInterval.ONE_HOUR: '1H',
            TimeInterval.FOUR_HOURS: '4H',
            TimeInterval.ONE_DAY: '1D',
            TimeInterval.ONE_WEEK: '1W',
            TimeInterval.ONE_MONTH: '1M'
        }
        
        freq = interval_map.get(interval, '1H')
        
        # Set timestamp as index
        df[timestamp_col] = pd.to_datetime(df[timestamp_col])
        df.set_index(timestamp_col, inplace=True)
        
        # Resample and aggregate
        return df.resample(freq).agg({
            col: self._get_aggregation_func(col)
            for col in df.columns
        })
    
    def _get_aggregation_func(self, column_name: str) -> str:
        """Determine aggregation function based on column name"""
        if 'count' in column_name.lower():
            return 'sum'
        elif 'price' in column_name.lower() or 'rate' in column_name.lower():
            return 'mean'
        elif 'volume' in column_name.lower() or 'value' in column_name.lower():
            return 'sum'
        elif 'address' in column_name.lower() and 'unique' in column_name.lower():
            return 'nunique'
        else:
            return 'mean'
    
    def calculate_moving_average(
        self,
        series: pd.Series,
        window: int
    ) -> pd.Series:
        """Calculate moving average"""
        return series.rolling(window=window, min_periods=1).mean()
    
    def calculate_percentage_change(
        self,
        series: pd.Series,
        periods: int = 1
    ) -> pd.Series:
        """Calculate percentage change"""
        return series.pct_change(periods=periods) * 100
    
    def detect_anomalies(
        self,
        series: pd.Series,
        method: str = 'zscore',
        threshold: float = 3.0
    ) -> pd.Series:
        """Detect anomalies in time series"""
        if method == 'zscore':
            z_scores = np.abs((series - series.mean()) / series.std())
            return z_scores > threshold
        elif method == 'iqr':
            Q1 = series.quantile(0.25)
            Q3 = series.quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - threshold * IQR
            upper_bound = Q3 + threshold * IQR
            return (series < lower_bound) | (series > upper_bound)
        else:
            raise ValueError(f"Unknown anomaly detection method: {method}")
    
    def calculate_correlation(
        self,
        df: pd.DataFrame,
        columns: List[str]
    ) -> pd.DataFrame:
        """Calculate correlation matrix"""
        return df[columns].corr()
    
    def find_trends(
        self,
        series: pd.Series,
        window: int = 7
    ) -> Dict[str, Any]:
        """Identify trends in time series"""
        # Calculate moving averages
        ma_short = self.calculate_moving_average(series, window)
        ma_long = self.calculate_moving_average(series, window * 3)
        
        # Determine trend
        current_trend = 'neutral'
        if len(ma_short) > 0 and len(ma_long) > 0:
            if ma_short.iloc[-1] > ma_long.iloc[-1]:
                current_trend = 'upward'
            elif ma_short.iloc[-1] < ma_long.iloc[-1]:
                current_trend = 'downward'
        
        # Calculate trend strength
        if len(series) > 1:
            trend_strength = abs(series.iloc[-1] - series.iloc[0]) / series.iloc[0]
        else:
            trend_strength = 0
        
        return {
            'trend': current_trend,
            'strength': trend_strength,
            'ma_short': ma_short.iloc[-1] if len(ma_short) > 0 else None,
            'ma_long': ma_long.iloc[-1] if len(ma_long) > 0 else None
        }
    
    def create_time_series(
        self,
        df: pd.DataFrame,
        metric_name: str,
        value_column: str,
        chain: str,
        interval: TimeInterval
    ) -> TimeSeries:
        """Create TimeSeries object from DataFrame"""
        data_points = []
        
        for idx, row in df.iterrows():
            data_points.append(DataPoint(
                timestamp=idx if isinstance(idx, datetime) else datetime.fromtimestamp(idx),
                value=float(row[value_column])
            ))
        
        return TimeSeries(
            metric=metric_name,
            chain=chain,
            interval=interval,
            start_time=df.index.min(),
            end_time=df.index.max(),
            data_points=data_points
        )
    
    def calculate_statistics(
        self,
        series: pd.Series
    ) -> Dict[str, float]:
        """Calculate basic statistics for a series"""
        return {
            'mean': float(series.mean()),
            'median': float(series.median()),
            'std': float(series.std()),
            'min': float(series.min()),
            'max': float(series.max()),
            'q25': float(series.quantile(0.25)),
            'q75': float(series.quantile(0.75))
        }
    
    def forecast_simple(
        self,
        series: pd.Series,
        periods: int,
        method: str = 'linear'
    ) -> pd.Series:
        """Simple forecasting methods"""
        if method == 'linear':
            # Linear regression
            x = np.arange(len(series))
            y = series.values
            coeffs = np.polyfit(x, y, 1)
            
            # Forecast
            future_x = np.arange(len(series), len(series) + periods)
            forecast = np.polyval(coeffs, future_x)
            
            # Create forecast series
            last_date = series.index[-1]
            freq = pd.infer_freq(series.index)
            future_dates = pd.date_range(
                start=last_date,
                periods=periods + 1,
                freq=freq
            )[1:]
            
            return pd.Series(forecast, index=future_dates)
        
        elif method == 'moving_average':
            # Simple moving average forecast
            ma = series.rolling(window=min(7, len(series))).mean().iloc[-1]
            return pd.Series([ma] * periods)
        
        else:
            raise ValueError(f"Unknown forecast method: {method}")
    
    def identify_patterns(
        self,
        series: pd.Series,
        pattern_type: str = 'peaks'
    ) -> List[Dict[str, Any]]:
        """Identify patterns in time series"""
        patterns = []
        
        if pattern_type == 'peaks':
            # Find local maxima
            for i in range(1, len(series) - 1):
                if series.iloc[i] > series.iloc[i-1] and series.iloc[i] > series.iloc[i+1]:
                    patterns.append({
                        'type': 'peak',
                        'timestamp': series.index[i],
                        'value': series.iloc[i],
                        'index': i
                    })
        
        elif pattern_type == 'valleys':
            # Find local minima
            for i in range(1, len(series) - 1):
                if series.iloc[i] < series.iloc[i-1] and series.iloc[i] < series.iloc[i+1]:
                    patterns.append({
                        'type': 'valley',
                        'timestamp': series.index[i],
                        'value': series.iloc[i],
                        'index': i
                    })
        
        return patterns
    
    def calculate_volatility(
        self,
        series: pd.Series,
        window: int = 30
    ) -> pd.Series:
        """Calculate rolling volatility"""
        returns = series.pct_change().dropna()
        return returns.rolling(window=window).std() * np.sqrt(window)
    
    async def validate_data(
        self,
        df: pd.DataFrame
    ) -> Tuple[bool, List[str]]:
        """Validate collected data"""
        issues = []
        
        # Check for empty data
        if df.empty:
            issues.append("DataFrame is empty")
            return False, issues
        
        # Check for required columns
        required_columns = ['timestamp']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            issues.append(f"Missing required columns: {missing_columns}")
        
        # Check for null values
        null_counts = df.isnull().sum()
        if null_counts.any():
            issues.append(f"Null values found: {null_counts[null_counts > 0].to_dict()}")
        
        # Check data types
        if 'timestamp' in df.columns:
            try:
                pd.to_datetime(df['timestamp'])
            except:
                issues.append("Invalid timestamp format")
        
        return len(issues) == 0, issues 