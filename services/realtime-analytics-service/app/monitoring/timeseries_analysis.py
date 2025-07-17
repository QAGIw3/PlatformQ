"""
Time-Series Analysis for Simulation Convergence

Provides advanced time-series analysis capabilities for monitoring
and predicting simulation convergence patterns.
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from scipy import signal
from scipy.stats import norm
from sklearn.preprocessing import StandardScaler
from pyignite import Client as IgniteClient
from elasticsearch import AsyncElasticsearch
import asyncio
from statsmodels.tsa.stattools import adfuller, acf, pacf
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.holtwinters import ExponentialSmoothing
import ruptures as rpt

logger = logging.getLogger(__name__)


class TimeSeriesAnalyzer:
    """Advanced time-series analysis for simulation metrics"""
    
    def __init__(self, ignite_config: Dict[str, Any], es_config: Dict[str, Any]):
        self.ignite_config = ignite_config
        self.es_config = es_config
        self.ignite_client = None
        self.es_client = None
        self._initialize_clients()
        
    def _initialize_clients(self):
        """Initialize storage clients"""
        # Initialize Ignite
        self.ignite_client = IgniteClient()
        self.ignite_client.connect([
            (self.ignite_config.get('host', 'ignite'), 
             self.ignite_config.get('port', 10800))
        ])
        
        # Create caches
        self.analysis_cache = self.ignite_client.get_or_create_cache('timeseries_analysis')
        self.pattern_cache = self.ignite_client.get_or_create_cache('convergence_patterns')
        
        # Initialize Elasticsearch
        self.es_client = AsyncElasticsearch(
            hosts=[self.es_config.get('host', 'http://elasticsearch:9200')]
        )
        
    async def analyze_convergence_pattern(self, 
                                        simulation_id: str,
                                        window_hours: int = 24) -> Dict[str, Any]:
        """Analyze convergence pattern for a simulation"""
        # Get time-series data
        data = await self._get_simulation_timeseries(simulation_id, window_hours)
        
        if len(data) < 10:
            return {
                'status': 'insufficient_data',
                'message': 'Not enough data points for analysis'
            }
            
        # Convert to pandas DataFrame
        df = pd.DataFrame(data)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.set_index('timestamp').sort_index()
        
        # Extract residual series
        if 'convergence' not in df.columns or 'residual' not in df.iloc[0]['convergence']:
            return {
                'status': 'no_convergence_data',
                'message': 'No convergence data available'
            }
            
        residuals = pd.Series([
            d['residual'] for d in df['convergence'] 
            if isinstance(d, dict) and 'residual' in d
        ], index=df.index[:len([d for d in df['convergence'] if isinstance(d, dict)])])
        
        analysis_results = {
            'simulation_id': simulation_id,
            'timestamp': datetime.utcnow().isoformat(),
            'data_points': len(residuals),
            'time_range': {
                'start': residuals.index[0].isoformat(),
                'end': residuals.index[-1].isoformat()
            }
        }
        
        # Stationarity analysis
        stationarity = self._analyze_stationarity(residuals)
        analysis_results['stationarity'] = stationarity
        
        # Trend analysis
        trend = self._analyze_trend(residuals)
        analysis_results['trend'] = trend
        
        # Seasonality analysis
        seasonality = self._analyze_seasonality(residuals)
        analysis_results['seasonality'] = seasonality
        
        # Change point detection
        change_points = self._detect_change_points(residuals)
        analysis_results['change_points'] = change_points
        
        # Convergence rate analysis
        conv_rate = self._analyze_convergence_rate(residuals)
        analysis_results['convergence_rate'] = conv_rate
        
        # Pattern classification
        pattern = self._classify_convergence_pattern(residuals, conv_rate)
        analysis_results['pattern_type'] = pattern
        
        # Forecast convergence
        forecast = self._forecast_convergence(residuals)
        analysis_results['forecast'] = forecast
        
        # Store analysis results
        self.analysis_cache.put(f"analysis_{simulation_id}", analysis_results)
        
        return analysis_results
        
    async def _get_simulation_timeseries(self, 
                                       simulation_id: str,
                                       window_hours: int) -> List[Dict]:
        """Get time-series data for simulation"""
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"simulation_id": simulation_id}},
                        {"range": {
                            "timestamp": {
                                "gte": f"now-{window_hours}h"
                            }
                        }}
                    ]
                }
            },
            "size": 10000,
            "sort": [{"timestamp": "asc"}]
        }
        
        result = await self.es_client.search(
            index=f"simulation-metrics-{simulation_id}",
            body=query
        )
        
        return [hit["_source"] for hit in result["hits"]["hits"]]
        
    def _analyze_stationarity(self, series: pd.Series) -> Dict[str, Any]:
        """Analyze stationarity of time series"""
        # Augmented Dickey-Fuller test
        try:
            adf_result = adfuller(series.dropna())
            
            return {
                'is_stationary': adf_result[1] < 0.05,  # p-value < 0.05
                'adf_statistic': float(adf_result[0]),
                'p_value': float(adf_result[1]),
                'critical_values': {
                    '1%': float(adf_result[4]['1%']),
                    '5%': float(adf_result[4]['5%']),
                    '10%': float(adf_result[4]['10%'])
                },
                'interpretation': 'stationary' if adf_result[1] < 0.05 else 'non-stationary'
            }
        except Exception as e:
            logger.error(f"Stationarity analysis failed: {e}")
            return {'error': str(e)}
            
    def _analyze_trend(self, series: pd.Series) -> Dict[str, Any]:
        """Analyze trend in time series"""
        try:
            # Simple linear regression for trend
            x = np.arange(len(series))
            y = series.values
            
            # Remove NaN values
            mask = ~np.isnan(y)
            x = x[mask]
            y = y[mask]
            
            if len(x) < 2:
                return {'error': 'Insufficient data'}
                
            # Fit linear trend
            slope, intercept = np.polyfit(x, y, 1)
            
            # Calculate R-squared
            y_pred = slope * x + intercept
            ss_res = np.sum((y - y_pred) ** 2)
            ss_tot = np.sum((y - np.mean(y)) ** 2)
            r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
            
            # Exponential fit for convergence
            if np.all(y > 0):
                log_y = np.log(y)
                exp_slope, exp_intercept = np.polyfit(x, log_y, 1)
                exp_rate = exp_slope
            else:
                exp_rate = 0
                
            return {
                'linear_slope': float(slope),
                'linear_intercept': float(intercept),
                'r_squared': float(r_squared),
                'exponential_rate': float(exp_rate),
                'trend_direction': 'decreasing' if slope < 0 else 'increasing' if slope > 0 else 'flat',
                'trend_strength': 'strong' if abs(r_squared) > 0.7 else 'moderate' if abs(r_squared) > 0.3 else 'weak'
            }
        except Exception as e:
            logger.error(f"Trend analysis failed: {e}")
            return {'error': str(e)}
            
    def _analyze_seasonality(self, series: pd.Series) -> Dict[str, Any]:
        """Analyze seasonality patterns"""
        try:
            if len(series) < 24:  # Need at least 24 points
                return {'has_seasonality': False, 'reason': 'insufficient_data'}
                
            # Resample to regular intervals
            series_regular = series.resample('1H').mean().interpolate()
            
            if len(series_regular) < 24:
                return {'has_seasonality': False, 'reason': 'insufficient_regular_data'}
                
            # Seasonal decomposition
            decomposition = seasonal_decompose(
                series_regular, 
                model='additive',
                period=min(24, len(series_regular) // 2)
            )
            
            seasonal_component = decomposition.seasonal.dropna()
            
            # Calculate seasonality strength
            if decomposition.resid.std() > 0:
                seasonality_strength = seasonal_component.std() / decomposition.resid.std()
            else:
                seasonality_strength = 0
                
            # Find dominant period using FFT
            fft = np.fft.fft(series_regular.dropna())
            frequencies = np.fft.fftfreq(len(fft))
            
            # Get dominant frequency (excluding DC component)
            power = np.abs(fft[1:len(fft)//2])
            freqs = frequencies[1:len(frequencies)//2]
            
            if len(power) > 0:
                dominant_freq_idx = np.argmax(power)
                dominant_period = 1 / freqs[dominant_freq_idx] if freqs[dominant_freq_idx] > 0 else 0
            else:
                dominant_period = 0
                
            return {
                'has_seasonality': seasonality_strength > 0.5,
                'seasonality_strength': float(seasonality_strength),
                'dominant_period_hours': float(dominant_period),
                'seasonal_amplitude': float(seasonal_component.std()),
                'interpretation': 'strong' if seasonality_strength > 1 else 'moderate' if seasonality_strength > 0.5 else 'weak'
            }
        except Exception as e:
            logger.error(f"Seasonality analysis failed: {e}")
            return {'error': str(e)}
            
    def _detect_change_points(self, series: pd.Series) -> List[Dict[str, Any]]:
        """Detect change points in convergence pattern"""
        try:
            values = series.dropna().values
            
            if len(values) < 10:
                return []
                
            # Use Pelt algorithm for change point detection
            algo = rpt.Pelt(model="rbf").fit(values)
            change_points = algo.predict(pen=10)
            
            # Remove last point (it's always the end)
            if change_points and change_points[-1] == len(values):
                change_points = change_points[:-1]
                
            # Analyze each change point
            results = []
            for cp_idx in change_points:
                if cp_idx < len(values) - 1:
                    # Get values before and after change point
                    before_values = values[max(0, cp_idx-10):cp_idx]
                    after_values = values[cp_idx:min(len(values), cp_idx+10)]
                    
                    # Calculate change magnitude
                    before_mean = np.mean(before_values)
                    after_mean = np.mean(after_values)
                    change_magnitude = abs(after_mean - before_mean)
                    
                    # Determine change type
                    if after_mean < before_mean * 0.5:
                        change_type = 'improvement'
                    elif after_mean > before_mean * 1.5:
                        change_type = 'degradation'
                    else:
                        change_type = 'shift'
                        
                    results.append({
                        'index': int(cp_idx),
                        'timestamp': series.index[cp_idx].isoformat(),
                        'change_type': change_type,
                        'magnitude': float(change_magnitude),
                        'before_mean': float(before_mean),
                        'after_mean': float(after_mean)
                    })
                    
            return results
        except Exception as e:
            logger.error(f"Change point detection failed: {e}")
            return []
            
    def _analyze_convergence_rate(self, residuals: pd.Series) -> Dict[str, Any]:
        """Analyze convergence rate characteristics"""
        try:
            values = residuals.dropna().values
            
            if len(values) < 3:
                return {'error': 'insufficient_data'}
                
            # Calculate instantaneous rates
            rates = []
            for i in range(1, len(values)):
                if values[i-1] > 0:
                    rate = (values[i-1] - values[i]) / values[i-1]
                    rates.append(rate)
                    
            if not rates:
                return {'error': 'no_valid_rates'}
                
            # Calculate rate statistics
            rates_array = np.array(rates)
            
            # Fit exponential decay
            if np.all(values > 0):
                log_values = np.log(values)
                x = np.arange(len(log_values))
                decay_rate, _ = np.polyfit(x, log_values, 1)
            else:
                decay_rate = 0
                
            return {
                'average_rate': float(np.mean(rates_array)),
                'rate_std': float(np.std(rates_array)),
                'min_rate': float(np.min(rates_array)),
                'max_rate': float(np.max(rates_array)),
                'exponential_decay_rate': float(decay_rate),
                'is_accelerating': np.mean(rates_array[-5:]) > np.mean(rates_array[:5]) if len(rates_array) > 10 else False,
                'rate_trend': 'improving' if np.mean(rates_array[-5:]) > np.mean(rates_array) else 'worsening'
            }
        except Exception as e:
            logger.error(f"Convergence rate analysis failed: {e}")
            return {'error': str(e)}
            
    def _classify_convergence_pattern(self, 
                                    residuals: pd.Series,
                                    conv_rate: Dict[str, Any]) -> str:
        """Classify the convergence pattern"""
        try:
            # Check for various patterns
            values = residuals.dropna().values
            
            if len(values) < 5:
                return 'unknown'
                
            # Exponential convergence
            if conv_rate.get('exponential_decay_rate', 0) < -0.1:
                return 'exponential'
                
            # Linear convergence
            x = np.arange(len(values))
            slope, _ = np.polyfit(x, values, 1)
            if slope < 0 and conv_rate.get('rate_std', 1) < 0.1:
                return 'linear'
                
            # Oscillatory convergence
            # Check for sign changes in differences
            diffs = np.diff(values)
            sign_changes = np.sum(np.diff(np.sign(diffs)) != 0)
            if sign_changes > len(diffs) * 0.3:
                return 'oscillatory'
                
            # Stagnant
            if conv_rate.get('average_rate', 0) < 0.01:
                return 'stagnant'
                
            # Diverging
            if slope > 0:
                return 'diverging'
                
            return 'irregular'
            
        except Exception as e:
            logger.error(f"Pattern classification failed: {e}")
            return 'unknown'
            
    def _forecast_convergence(self, residuals: pd.Series) -> Dict[str, Any]:
        """Forecast future convergence behavior"""
        try:
            values = residuals.dropna()
            
            if len(values) < 10:
                return {'error': 'insufficient_data'}
                
            # Use Exponential Smoothing for forecast
            model = ExponentialSmoothing(
                values,
                trend='add',
                seasonal=None,
                damped_trend=True
            )
            
            fit = model.fit()
            
            # Forecast next 10 periods
            forecast_periods = min(10, len(values) // 2)
            forecast = fit.forecast(steps=forecast_periods)
            
            # Calculate confidence intervals
            residual_std = np.std(fit.resid)
            lower_bound = forecast - 1.96 * residual_std
            upper_bound = forecast + 1.96 * residual_std
            
            # Estimate convergence time
            target_residual = 1e-6
            if forecast.iloc[-1] < target_residual:
                convergence_time = 0
            else:
                # Simple linear extrapolation
                if len(forecast) > 1 and forecast.iloc[-1] < forecast.iloc[0]:
                    rate = (forecast.iloc[-1] - forecast.iloc[0]) / len(forecast)
                    if rate < 0:
                        convergence_time = int((target_residual - forecast.iloc[-1]) / rate)
                    else:
                        convergence_time = None  # Not converging
                else:
                    convergence_time = None
                    
            return {
                'forecast_values': forecast.tolist(),
                'lower_bound': lower_bound.tolist(),
                'upper_bound': upper_bound.tolist(),
                'forecast_periods': forecast_periods,
                'estimated_convergence_time': convergence_time,
                'confidence_level': 0.95,
                'model_params': {
                    'alpha': fit.params['smoothing_level'],
                    'beta': fit.params.get('smoothing_trend', 0),
                    'phi': fit.params.get('damping_trend', 0)
                }
            }
        except Exception as e:
            logger.error(f"Convergence forecast failed: {e}")
            return {'error': str(e)}
            
    async def detect_anomalous_patterns(self, 
                                      simulation_ids: List[str]) -> List[Dict[str, Any]]:
        """Detect anomalous convergence patterns across simulations"""
        patterns = []
        
        for sim_id in simulation_ids:
            analysis = await self.analyze_convergence_pattern(sim_id, window_hours=6)
            
            if 'error' not in analysis:
                # Check for anomalies
                anomalies = []
                
                # Diverging pattern
                if analysis.get('pattern_type') == 'diverging':
                    anomalies.append({
                        'type': 'divergence',
                        'severity': 'critical',
                        'description': 'Simulation is diverging'
                    })
                    
                # Stagnant convergence
                if analysis.get('pattern_type') == 'stagnant':
                    anomalies.append({
                        'type': 'stagnation',
                        'severity': 'warning',
                        'description': 'Convergence has stagnated'
                    })
                    
                # Sudden changes
                change_points = analysis.get('change_points', [])
                for cp in change_points:
                    if cp['change_type'] == 'degradation':
                        anomalies.append({
                            'type': 'sudden_degradation',
                            'severity': 'warning',
                            'description': f"Sudden degradation at {cp['timestamp']}",
                            'details': cp
                        })
                        
                if anomalies:
                    patterns.append({
                        'simulation_id': sim_id,
                        'anomalies': anomalies,
                        'pattern_type': analysis.get('pattern_type'),
                        'convergence_rate': analysis.get('convergence_rate', {})
                    })
                    
        return patterns
        
    async def compare_simulation_patterns(self, 
                                        sim_ids: List[str]) -> Dict[str, Any]:
        """Compare convergence patterns across multiple simulations"""
        analyses = {}
        
        # Analyze each simulation
        for sim_id in sim_ids:
            analysis = await self.analyze_convergence_pattern(sim_id)
            if 'error' not in analysis:
                analyses[sim_id] = analysis
                
        if len(analyses) < 2:
            return {'error': 'insufficient_simulations'}
            
        # Compare patterns
        comparison = {
            'simulations': list(analyses.keys()),
            'pattern_distribution': {},
            'convergence_rates': {},
            'best_performing': None,
            'worst_performing': None
        }
        
        # Pattern distribution
        for sim_id, analysis in analyses.items():
            pattern = analysis.get('pattern_type', 'unknown')
            comparison['pattern_distribution'][pattern] = comparison['pattern_distribution'].get(pattern, 0) + 1
            
            # Convergence rates
            rate = analysis.get('convergence_rate', {}).get('average_rate', 0)
            comparison['convergence_rates'][sim_id] = rate
            
        # Find best and worst
        if comparison['convergence_rates']:
            comparison['best_performing'] = max(comparison['convergence_rates'], 
                                              key=comparison['convergence_rates'].get)
            comparison['worst_performing'] = min(comparison['convergence_rates'], 
                                               key=comparison['convergence_rates'].get)
            
        return comparison
        
    def close(self):
        """Close connections"""
        if self.ignite_client:
            self.ignite_client.close()
        if self.es_client:
            asyncio.create_task(self.es_client.close()) 