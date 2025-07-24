"""
Time Series Forecasting Algorithm

Implements various forecasting methods for the Analytics Engine.
"""

from typing import Dict, Any, List, Optional, Union, Tuple
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.statespace.sarimax import SARIMAX
import warnings
warnings.filterwarnings('ignore')

from data_intelligence_common.core.algorithms import BaseAlgorithm, AlgorithmConfig
from data_intelligence_common.monitoring import StructuredLogger

# Try to import optional dependencies
try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False

logger = StructuredLogger.get_logger(__name__)


class ForecastMethod(str, Enum):
    """Forecasting methods"""
    MOVING_AVERAGE = "moving_average"
    EXPONENTIAL_SMOOTHING = "exponential_smoothing"
    ARIMA = "arima"
    SARIMA = "sarima"
    PROPHET = "prophet"
    ENSEMBLE = "ensemble"


@dataclass
class ForecastingConfig(AlgorithmConfig):
    """Configuration for forecasting algorithm"""
    method: ForecastMethod = ForecastMethod.EXPONENTIAL_SMOOTHING
    forecast_horizon: int = 30  # Number of periods to forecast
    confidence_level: float = 0.95
    
    # Time series parameters
    frequency: str = "D"  # Daily by default (D, H, M, W, MS, Q, Y)
    seasonality: Optional[str] = None  # auto, daily, weekly, monthly, yearly
    
    # Method-specific parameters
    # Moving average
    window_size: int = 7
    
    # Exponential smoothing
    trend: Optional[str] = "add"  # add, mul, None
    seasonal: Optional[str] = "add"  # add, mul, None
    seasonal_periods: Optional[int] = None
    
    # ARIMA
    arima_order: Tuple[int, int, int] = (1, 1, 1)  # (p, d, q)
    auto_arima: bool = True
    
    # SARIMA
    seasonal_order: Tuple[int, int, int, int] = (1, 1, 1, 12)  # (P, D, Q, s)
    
    # Prophet
    changepoint_prior_scale: float = 0.05
    seasonality_prior_scale: float = 10.0
    holidays_prior_scale: float = 10.0
    
    # Feature engineering
    use_external_features: bool = False
    external_features: List[str] = field(default_factory=list)
    
    # Validation
    validation_split: float = 0.2
    use_cross_validation: bool = False
    cv_folds: int = 3


class ForecastingAlgorithm(BaseAlgorithm):
    """
    Time series forecasting algorithm supporting multiple methods.
    
    Features:
    - Multiple forecasting methods (MA, ES, ARIMA, Prophet)
    - Automatic seasonality detection
    - Confidence intervals
    - External feature support
    - Model validation and selection
    """
    
    # Set config class for factory
    __config_class__ = ForecastingConfig
    
    def __init__(self, config: ForecastingConfig):
        super().__init__(config)
        self.config = config
        self._models = {}
        self._forecast_results = {}
        self._feature_importance = {}
        
    async def initialize(self):
        """Initialize algorithm components"""
        await super().initialize()
        
        # Check Prophet availability
        if self.config.method == ForecastMethod.PROPHET and not PROPHET_AVAILABLE:
            raise ValueError("Prophet is not installed. Install with: pip install prophet")
            
        logger.info(f"Forecasting algorithm initialized with method: {self.config.method}")
        
    async def train(self, data: pd.DataFrame, **kwargs) -> Dict[str, Any]:
        """Train forecasting model"""
        start_time = datetime.utcnow()
        
        try:
            # Prepare time series data
            ts_data = self._prepare_time_series(data)
            
            # Detect seasonality if not specified
            if self.config.seasonality is None:
                self.config.seasonal_periods = self._detect_seasonality(ts_data)
                
            # Split data for validation
            if self.config.validation_split > 0:
                split_idx = int(len(ts_data) * (1 - self.config.validation_split))
                train_data = ts_data[:split_idx]
                val_data = ts_data[split_idx:]
            else:
                train_data = ts_data
                val_data = None
                
            # Train model based on method
            if self.config.method == ForecastMethod.MOVING_AVERAGE:
                model = self._train_moving_average(train_data)
            elif self.config.method == ForecastMethod.EXPONENTIAL_SMOOTHING:
                model = self._train_exponential_smoothing(train_data)
            elif self.config.method == ForecastMethod.ARIMA:
                model = self._train_arima(train_data)
            elif self.config.method == ForecastMethod.SARIMA:
                model = self._train_sarima(train_data)
            elif self.config.method == ForecastMethod.PROPHET:
                model = self._train_prophet(train_data)
            elif self.config.method == ForecastMethod.ENSEMBLE:
                model = self._train_ensemble(train_data)
            else:
                raise ValueError(f"Unknown method: {self.config.method}")
                
            self._models['primary'] = model
            
            # Validate if data available
            validation_metrics = {}
            if val_data is not None:
                validation_metrics = await self._validate_model(model, train_data, val_data)
                
            # Store metadata
            self._metadata.update({
                'trained_at': datetime.utcnow().isoformat(),
                'training_samples': len(train_data),
                'forecast_method': self.config.method.value,
                'seasonality': self.config.seasonal_periods,
                'validation_metrics': validation_metrics
            })
            
            result = {
                'status': 'success',
                'training_time': (datetime.utcnow() - start_time).total_seconds(),
                'samples_trained': len(train_data),
                'method': self.config.method.value,
                'validation_metrics': validation_metrics
            }
            
            logger.info("Forecasting model trained", **result)
            return result
            
        except Exception as e:
            logger.error(f"Training failed: {e}")
            raise
            
    async def predict(
        self,
        data: Union[pd.DataFrame, Dict[str, Any], None] = None,
        horizon: Optional[int] = None,
        **kwargs
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Generate forecast"""
        if 'primary' not in self._models:
            raise ValueError("Model not trained")
            
        horizon = horizon or self.config.forecast_horizon
        
        try:
            # Generate forecast based on method
            if self.config.method == ForecastMethod.MOVING_AVERAGE:
                forecast = self._forecast_moving_average(horizon, data)
            elif self.config.method == ForecastMethod.EXPONENTIAL_SMOOTHING:
                forecast = self._forecast_exponential_smoothing(horizon)
            elif self.config.method == ForecastMethod.ARIMA:
                forecast = self._forecast_arima(horizon)
            elif self.config.method == ForecastMethod.SARIMA:
                forecast = self._forecast_sarima(horizon)
            elif self.config.method == ForecastMethod.PROPHET:
                forecast = self._forecast_prophet(horizon)
            elif self.config.method == ForecastMethod.ENSEMBLE:
                forecast = self._forecast_ensemble(horizon)
                
            # Format results
            results = self._format_forecast_results(forecast)
            
            # Record metrics
            self.record_metric('forecasts_generated', len(results))
            
            return results
            
        except Exception as e:
            logger.error(f"Prediction failed: {e}")
            raise
            
    def _prepare_time_series(self, data: pd.DataFrame) -> pd.Series:
        """Prepare time series data"""
        # Assume first column is timestamp, second is value
        if len(data.columns) < 2:
            raise ValueError("Data must have at least timestamp and value columns")
            
        # Set timestamp as index
        ts_data = data.set_index(data.columns[0])
        
        # Get target column (assume second column or 'value')
        if 'value' in ts_data.columns:
            ts_data = ts_data['value']
        else:
            ts_data = ts_data[ts_data.columns[0]]
            
        # Ensure datetime index
        if not isinstance(ts_data.index, pd.DatetimeIndex):
            ts_data.index = pd.to_datetime(ts_data.index)
            
        # Sort by time
        ts_data = ts_data.sort_index()
        
        # Handle missing values
        ts_data = ts_data.fillna(method='ffill').fillna(method='bfill')
        
        # Set frequency
        if ts_data.index.freq is None:
            ts_data = ts_data.asfreq(self.config.frequency)
            
        return ts_data
        
    def _detect_seasonality(self, data: pd.Series) -> Optional[int]:
        """Detect seasonality in time series"""
        # Simple seasonality detection based on frequency
        freq_map = {
            'H': 24,      # Hourly -> Daily seasonality
            'D': 7,       # Daily -> Weekly seasonality
            'W': 52,      # Weekly -> Yearly seasonality
            'MS': 12,     # Monthly -> Yearly seasonality
            'Q': 4,       # Quarterly -> Yearly seasonality
        }
        
        freq = data.index.freq
        if freq:
            freq_str = freq.name if hasattr(freq, 'name') else str(freq)
            return freq_map.get(freq_str, None)
            
        return None
        
    def _train_moving_average(self, data: pd.Series) -> Dict[str, Any]:
        """Train moving average model"""
        return {
            'method': 'moving_average',
            'window': self.config.window_size,
            'last_values': data.tail(self.config.window_size).values
        }
        
    def _train_exponential_smoothing(self, data: pd.Series) -> Any:
        """Train exponential smoothing model"""
        model = ExponentialSmoothing(
            data,
            trend=self.config.trend,
            seasonal=self.config.seasonal,
            seasonal_periods=self.config.seasonal_periods
        )
        
        fitted = model.fit()
        return fitted
        
    def _train_arima(self, data: pd.Series) -> Any:
        """Train ARIMA model"""
        if self.config.auto_arima:
            # Simple auto ARIMA selection
            best_aic = np.inf
            best_order = None
            best_model = None
            
            for p in range(3):
                for d in range(2):
                    for q in range(3):
                        try:
                            model = ARIMA(data, order=(p, d, q))
                            fitted = model.fit()
                            if fitted.aic < best_aic:
                                best_aic = fitted.aic
                                best_order = (p, d, q)
                                best_model = fitted
                        except:
                            continue
                            
            self.config.arima_order = best_order
            return best_model
        else:
            model = ARIMA(data, order=self.config.arima_order)
            return model.fit()
            
    def _train_sarima(self, data: pd.Series) -> Any:
        """Train SARIMA model"""
        model = SARIMAX(
            data,
            order=self.config.arima_order,
            seasonal_order=self.config.seasonal_order,
            enforce_stationarity=False,
            enforce_invertibility=False
        )
        
        return model.fit(disp=False)
        
    def _train_prophet(self, data: pd.Series) -> Any:
        """Train Prophet model"""
        # Prepare data for Prophet
        prophet_data = pd.DataFrame({
            'ds': data.index,
            'y': data.values
        })
        
        model = Prophet(
            changepoint_prior_scale=self.config.changepoint_prior_scale,
            seasonality_prior_scale=self.config.seasonality_prior_scale,
            holidays_prior_scale=self.config.holidays_prior_scale,
            interval_width=self.config.confidence_level
        )
        
        # Add seasonality if detected
        if self.config.seasonal_periods:
            if self.config.seasonal_periods == 7:
                model.add_seasonality(name='weekly', period=7, fourier_order=3)
            elif self.config.seasonal_periods == 12:
                model.add_seasonality(name='monthly', period=30.5, fourier_order=5)
                
        model.fit(prophet_data)
        return model
        
    def _train_ensemble(self, data: pd.Series) -> Dict[str, Any]:
        """Train ensemble of models"""
        models = {}
        
        # Train multiple models
        try:
            models['es'] = self._train_exponential_smoothing(data)
        except:
            pass
            
        try:
            models['arima'] = self._train_arima(data)
        except:
            pass
            
        if PROPHET_AVAILABLE:
            try:
                models['prophet'] = self._train_prophet(data)
            except:
                pass
                
        return {
            'method': 'ensemble',
            'models': models,
            'weights': {name: 1.0/len(models) for name in models}  # Equal weights
        }
        
    def _forecast_moving_average(self, horizon: int, new_data: Optional[pd.DataFrame]) -> pd.DataFrame:
        """Generate moving average forecast"""
        model = self._models['primary']
        last_values = model['last_values']
        
        forecasts = []
        lower = []
        upper = []
        
        for i in range(horizon):
            # Simple MA forecast
            forecast = np.mean(last_values)
            forecasts.append(forecast)
            
            # Simple confidence intervals
            std = np.std(last_values)
            margin = 1.96 * std  # 95% CI
            lower.append(forecast - margin)
            upper.append(forecast + margin)
            
            # Roll forward
            last_values = np.roll(last_values, -1)
            last_values[-1] = forecast
            
        # Create forecast index
        last_date = pd.Timestamp.now()
        forecast_index = pd.date_range(
            start=last_date,
            periods=horizon,
            freq=self.config.frequency
        )
        
        return pd.DataFrame({
            'forecast': forecasts,
            'lower': lower,
            'upper': upper
        }, index=forecast_index)
        
    def _forecast_exponential_smoothing(self, horizon: int) -> pd.DataFrame:
        """Generate exponential smoothing forecast"""
        model = self._models['primary']
        
        forecast = model.forecast(horizon)
        
        # Get prediction intervals
        forecast_df = pd.DataFrame({
            'forecast': forecast,
            'lower': forecast - 1.96 * model.sse ** 0.5,
            'upper': forecast + 1.96 * model.sse ** 0.5
        })
        
        return forecast_df
        
    def _forecast_arima(self, horizon: int) -> pd.DataFrame:
        """Generate ARIMA forecast"""
        model = self._models['primary']
        
        forecast_result = model.forecast(steps=horizon)
        
        # Get prediction intervals
        forecast_df = pd.DataFrame({
            'forecast': forecast_result,
            'lower': forecast_result - 1.96 * model.resid.std(),
            'upper': forecast_result + 1.96 * model.resid.std()
        })
        
        return forecast_df
        
    def _forecast_sarima(self, horizon: int) -> pd.DataFrame:
        """Generate SARIMA forecast"""
        model = self._models['primary']
        
        forecast_result = model.forecast(steps=horizon)
        
        # Get prediction intervals
        forecast_df = pd.DataFrame({
            'forecast': forecast_result,
            'lower': forecast_result - 1.96 * model.resid.std(),
            'upper': forecast_result + 1.96 * model.resid.std()
        })
        
        return forecast_df
        
    def _forecast_prophet(self, horizon: int) -> pd.DataFrame:
        """Generate Prophet forecast"""
        model = self._models['primary']
        
        # Create future dataframe
        future = model.make_future_dataframe(periods=horizon, freq=self.config.frequency)
        
        # Generate forecast
        forecast = model.predict(future)
        
        # Extract relevant columns
        forecast_df = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(horizon)
        forecast_df = forecast_df.rename(columns={
            'yhat': 'forecast',
            'yhat_lower': 'lower',
            'yhat_upper': 'upper'
        })
        forecast_df.set_index('ds', inplace=True)
        
        return forecast_df
        
    def _forecast_ensemble(self, horizon: int) -> pd.DataFrame:
        """Generate ensemble forecast"""
        model_info = self._models['primary']
        models = model_info['models']
        weights = model_info['weights']
        
        forecasts = {}
        
        # Get forecasts from each model
        for name, model in models.items():
            self._models['primary'] = model
            
            if name == 'es':
                forecast = self._forecast_exponential_smoothing(horizon)
            elif name == 'arima':
                forecast = self._forecast_arima(horizon)
            elif name == 'prophet':
                forecast = self._forecast_prophet(horizon)
                
            forecasts[name] = forecast
            
        # Restore ensemble model
        self._models['primary'] = model_info
        
        # Combine forecasts
        combined = pd.DataFrame()
        
        for col in ['forecast', 'lower', 'upper']:
            weighted_sum = None
            
            for name, forecast in forecasts.items():
                weight = weights[name]
                if weighted_sum is None:
                    weighted_sum = forecast[col] * weight
                else:
                    weighted_sum += forecast[col] * weight
                    
            combined[col] = weighted_sum
            
        return combined
        
    def _format_forecast_results(self, forecast_df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Format forecast results"""
        results = []
        
        for idx, row in forecast_df.iterrows():
            result = {
                'timestamp': idx.isoformat() if hasattr(idx, 'isoformat') else str(idx),
                'forecast': float(row['forecast']),
                'lower_bound': float(row['lower']),
                'upper_bound': float(row['upper']),
                'confidence_level': self.config.confidence_level
            }
            results.append(result)
            
        return results
        
    async def _validate_model(
        self,
        model: Any,
        train_data: pd.Series,
        val_data: pd.Series
    ) -> Dict[str, float]:
        """Validate model performance"""
        # Generate forecast for validation period
        self._models['primary'] = model
        forecast_results = await self.predict(horizon=len(val_data))
        
        # Extract forecasts
        forecasts = [r['forecast'] for r in forecast_results]
        actuals = val_data.values
        
        # Calculate metrics
        mae = np.mean(np.abs(forecasts - actuals))
        mse = np.mean((forecasts - actuals) ** 2)
        rmse = np.sqrt(mse)
        mape = np.mean(np.abs((forecasts - actuals) / actuals)) * 100
        
        return {
            'mae': float(mae),
            'mse': float(mse),
            'rmse': float(rmse),
            'mape': float(mape)
        }
        
    async def update(self, new_data: pd.DataFrame, **kwargs) -> Dict[str, Any]:
        """Update model with new data"""
        # For now, retrain the model
        return await self.train(new_data, **kwargs)
        
    def get_params(self) -> Dict[str, Any]:
        """Get algorithm parameters"""
        params = {
            'method': self.config.method.value,
            'forecast_horizon': self.config.forecast_horizon,
            'confidence_level': self.config.confidence_level,
            'frequency': self.config.frequency
        }
        
        if self.config.method == ForecastMethod.ARIMA:
            params['arima_order'] = self.config.arima_order
        elif self.config.method == ForecastMethod.SARIMA:
            params['seasonal_order'] = self.config.seasonal_order
            
        return params
        
    def set_params(self, **params) -> None:
        """Set algorithm parameters"""
        for key, value in params.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)


# Algorithm registration
__algorithm_class__ = ForecastingAlgorithm
__algorithm_name__ = "forecasting" 