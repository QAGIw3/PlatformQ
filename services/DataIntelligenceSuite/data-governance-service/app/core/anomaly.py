"""Anomaly Detector with ML-powered detection methods"""

import logging
from typing import Dict, Any, List, Optional, Tuple, Union
from datetime import datetime
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor
from sklearn.svm import OneClassSVM
from sklearn.preprocessing import StandardScaler
import joblib
import os

from app.core.config import Settings
from app.core.ml_optimizer import MLQualityOptimizer


logger = logging.getLogger(__name__)


class AnomalyMethod:
    """Anomaly detection methods"""
    STATISTICAL = "statistical"
    ISOLATION_FOREST = "isolation_forest"
    LOF = "lof"  # Local Outlier Factor
    ONE_CLASS_SVM = "one_class_svm"
    PROPHET = "prophet"
    LSTM = "lstm"
    ENSEMBLE = "ensemble"


class AnomalyDetector:
    """ML-powered anomaly detection engine"""
    
    def __init__(self, settings: Settings, ml_optimizer: Optional['MLQualityOptimizer'] = None):
        self.settings = settings
        self.ml_optimizer = ml_optimizer
        self.models: Dict[str, Any] = {}
        self.scalers: Dict[str, StandardScaler] = {}
        self.detection_history: List[Dict[str, Any]] = []
        
    async def initialize(self):
        """Initialize anomaly detector"""
        logger.info("Initializing anomaly detector")
        
        # Load pre-trained models if available
        await self._load_models()
        
        logger.info("Anomaly detector initialized")
        
    async def detect_anomalies(self, dataset_id: str, data: pd.DataFrame,
                             methods: Optional[List[str]] = None,
                             sensitivity: float = 0.95,
                             auto_remediate: bool = False) -> Dict[str, Any]:
        """Detect anomalies using specified methods"""
        logger.info(f"Detecting anomalies in dataset {dataset_id}")
        
        # Default methods
        if not methods:
            methods = self.settings.anomaly_detection_methods
            
        # Initialize results
        results = {
            "dataset_id": dataset_id,
            "timestamp": datetime.utcnow().isoformat(),
            "row_count": len(data),
            "methods_used": methods,
            "sensitivity": sensitivity,
            "anomalies": [],
            "summary": {},
            "remediation_applied": False
        }
        
        # Detect anomalies for each numeric column
        numeric_columns = data.select_dtypes(include=[np.number]).columns
        
        for column in numeric_columns:
            col_anomalies = await self._detect_column_anomalies(
                data[column], column, methods, sensitivity
            )
            results["anomalies"].extend(col_anomalies)
            
        # Ensemble detection if multiple methods used
        if len(methods) > 1 and AnomalyMethod.ENSEMBLE in methods:
            ensemble_anomalies = await self._ensemble_detection(
                data, numeric_columns, methods, sensitivity
            )
            results["anomalies"].extend(ensemble_anomalies)
            
        # Summarize results
        results["summary"] = self._summarize_anomalies(results["anomalies"])
        
        # Apply auto-remediation if requested
        if auto_remediate and results["anomalies"]:
            remediation_result = await self._auto_remediate(data, results["anomalies"])
            results["remediation_applied"] = remediation_result["success"]
            results["remediation_details"] = remediation_result
            
        # Track detection history
        self.detection_history.append({
            "dataset_id": dataset_id,
            "timestamp": results["timestamp"],
            "anomaly_count": len(results["anomalies"]),
            "methods": methods
        })
        
        return results
        
    async def _detect_column_anomalies(self, column_data: pd.Series, column_name: str,
                                     methods: List[str], sensitivity: float) -> List[Dict[str, Any]]:
        """Detect anomalies in a single column"""
        anomalies = []
        
        # Remove NaN values
        clean_data = column_data.dropna()
        if len(clean_data) < 10:
            return anomalies
            
        # Statistical method
        if AnomalyMethod.STATISTICAL in methods:
            stat_anomalies = await self._statistical_detection(clean_data, column_name, sensitivity)
            anomalies.extend(stat_anomalies)
            
        # Isolation Forest
        if AnomalyMethod.ISOLATION_FOREST in methods:
            if_anomalies = await self._isolation_forest_detection(clean_data, column_name, sensitivity)
            anomalies.extend(if_anomalies)
            
        # Local Outlier Factor
        if AnomalyMethod.LOF in methods:
            lof_anomalies = await self._lof_detection(clean_data, column_name, sensitivity)
            anomalies.extend(lof_anomalies)
            
        # One-Class SVM
        if AnomalyMethod.ONE_CLASS_SVM in methods:
            svm_anomalies = await self._one_class_svm_detection(clean_data, column_name, sensitivity)
            anomalies.extend(svm_anomalies)
            
        return anomalies
        
    async def _statistical_detection(self, data: pd.Series, column_name: str,
                                   sensitivity: float) -> List[Dict[str, Any]]:
        """Statistical anomaly detection using Z-score and IQR"""
        anomalies = []
        
        # Z-score method
        mean = data.mean()
        std = data.std()
        z_threshold = 3.0 * (2 - sensitivity)  # Adjust threshold based on sensitivity
        
        z_scores = np.abs((data - mean) / std)
        z_anomalies = data[z_scores > z_threshold]
        
        for idx, value in z_anomalies.items():
            anomalies.append({
                "method": "z_score",
                "column": column_name,
                "index": int(idx),
                "value": float(value),
                "z_score": float(z_scores[idx]),
                "severity": "high" if z_scores[idx] > 4 else "medium"
            })
            
        # IQR method
        q1 = data.quantile(0.25)
        q3 = data.quantile(0.75)
        iqr = q3 - q1
        iqr_multiplier = 1.5 * (2 - sensitivity)
        
        lower_bound = q1 - iqr_multiplier * iqr
        upper_bound = q3 + iqr_multiplier * iqr
        
        iqr_anomalies = data[(data < lower_bound) | (data > upper_bound)]
        
        for idx, value in iqr_anomalies.items():
            # Check if not already detected by z-score
            if idx not in z_anomalies.index:
                anomalies.append({
                    "method": "iqr",
                    "column": column_name,
                    "index": int(idx),
                    "value": float(value),
                    "bounds": [float(lower_bound), float(upper_bound)],
                    "severity": "medium"
                })
                
        return anomalies
        
    async def _isolation_forest_detection(self, data: pd.Series, column_name: str,
                                        sensitivity: float) -> List[Dict[str, Any]]:
        """Isolation Forest anomaly detection"""
        anomalies = []
        
        # Prepare data
        X = data.values.reshape(-1, 1)
        
        # Scale data
        scaler_key = f"if_{column_name}"
        if scaler_key not in self.scalers:
            self.scalers[scaler_key] = StandardScaler()
            
        X_scaled = self.scalers[scaler_key].fit_transform(X)
        
        # Train or load model
        model_key = f"if_{column_name}"
        if model_key not in self.models:
            contamination = 1 - sensitivity
            self.models[model_key] = IsolationForest(
                contamination=contamination,
                random_state=42,
                n_estimators=100
            )
            
        # Detect anomalies
        model = self.models[model_key]
        predictions = model.fit_predict(X_scaled)
        scores = model.score_samples(X_scaled)
        
        # Get anomaly indices
        anomaly_mask = predictions == -1
        anomaly_indices = data.index[anomaly_mask]
        
        for idx in anomaly_indices:
            anomalies.append({
                "method": "isolation_forest",
                "column": column_name,
                "index": int(idx),
                "value": float(data[idx]),
                "anomaly_score": float(-scores[data.index.get_loc(idx)]),
                "severity": "high" if -scores[data.index.get_loc(idx)] > 0.5 else "medium"
            })
            
        return anomalies
        
    async def _lof_detection(self, data: pd.Series, column_name: str,
                           sensitivity: float) -> List[Dict[str, Any]]:
        """Local Outlier Factor detection"""
        anomalies = []
        
        # Prepare data
        X = data.values.reshape(-1, 1)
        
        # Scale data
        scaler_key = f"lof_{column_name}"
        if scaler_key not in self.scalers:
            self.scalers[scaler_key] = StandardScaler()
            
        X_scaled = self.scalers[scaler_key].fit_transform(X)
        
        # LOF detection
        contamination = 1 - sensitivity
        lof = LocalOutlierFactor(
            n_neighbors=20,
            contamination=contamination,
            novelty=False
        )
        
        predictions = lof.fit_predict(X_scaled)
        scores = lof.negative_outlier_factor_
        
        # Get anomaly indices
        anomaly_mask = predictions == -1
        anomaly_indices = data.index[anomaly_mask]
        
        for idx in anomaly_indices:
            anomalies.append({
                "method": "lof",
                "column": column_name,
                "index": int(idx),
                "value": float(data[idx]),
                "lof_score": float(-scores[data.index.get_loc(idx)]),
                "severity": "medium"
            })
            
        return anomalies
        
    async def _one_class_svm_detection(self, data: pd.Series, column_name: str,
                                     sensitivity: float) -> List[Dict[str, Any]]:
        """One-Class SVM anomaly detection"""
        anomalies = []
        
        # Prepare data
        X = data.values.reshape(-1, 1)
        
        # Scale data
        scaler_key = f"svm_{column_name}"
        if scaler_key not in self.scalers:
            self.scalers[scaler_key] = StandardScaler()
            
        X_scaled = self.scalers[scaler_key].fit_transform(X)
        
        # One-Class SVM
        nu = 1 - sensitivity  # nu is an upper bound on the fraction of outliers
        svm = OneClassSVM(
            kernel="rbf",
            gamma="auto",
            nu=nu
        )
        
        svm.fit(X_scaled)
        predictions = svm.predict(X_scaled)
        scores = svm.decision_function(X_scaled)
        
        # Get anomaly indices
        anomaly_mask = predictions == -1
        anomaly_indices = data.index[anomaly_mask]
        
        for idx in anomaly_indices:
            anomalies.append({
                "method": "one_class_svm",
                "column": column_name,
                "index": int(idx),
                "value": float(data[idx]),
                "decision_score": float(scores[data.index.get_loc(idx)]),
                "severity": "medium"
            })
            
        return anomalies
        
    async def _ensemble_detection(self, data: pd.DataFrame, columns: List[str],
                                methods: List[str], sensitivity: float) -> List[Dict[str, Any]]:
        """Ensemble anomaly detection combining multiple methods"""
        anomalies = []
        
        # For multivariate anomaly detection
        if len(columns) > 1:
            # Prepare data
            X = data[columns].dropna()
            
            if len(X) < 10:
                return anomalies
                
            # Scale data
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X)
            
            # Collect predictions from multiple methods
            all_predictions = []
            
            # Isolation Forest
            if AnomalyMethod.ISOLATION_FOREST in methods:
                contamination = 1 - sensitivity
                if_model = IsolationForest(contamination=contamination, random_state=42)
                if_pred = if_model.fit_predict(X_scaled)
                all_predictions.append(if_pred)
                
            # One-Class SVM
            if AnomalyMethod.ONE_CLASS_SVM in methods:
                nu = 1 - sensitivity
                svm_model = OneClassSVM(kernel="rbf", gamma="auto", nu=nu)
                svm_pred = svm_model.fit_predict(X_scaled)
                all_predictions.append(svm_pred)
                
            # Ensemble voting
            if all_predictions:
                predictions_array = np.array(all_predictions)
                # Majority voting: -1 if majority says anomaly
                ensemble_pred = np.sign(np.sum(predictions_array, axis=0))
                
                # Get anomaly indices
                anomaly_mask = ensemble_pred == -1
                anomaly_indices = X.index[anomaly_mask]
                
                for idx in anomaly_indices:
                    anomalies.append({
                        "method": "ensemble",
                        "type": "multivariate",
                        "index": int(idx),
                        "columns": columns.tolist(),
                        "severity": "high",
                        "votes": int(np.sum(predictions_array[:, X.index.get_loc(idx)] == -1))
                    })
                    
        return anomalies
        
    async def train_custom_model(self, dataset_id: str, data: pd.DataFrame,
                               method: str, target_column: Optional[str] = None) -> Dict[str, Any]:
        """Train a custom anomaly detection model"""
        logger.info(f"Training custom {method} model for dataset {dataset_id}")
        
        result = {
            "dataset_id": dataset_id,
            "method": method,
            "trained_at": datetime.utcnow().isoformat(),
            "success": False
        }
        
        try:
            if method == AnomalyMethod.PROPHET:
                # Prophet requires time series data
                # This is a placeholder - implement actual Prophet training
                result["message"] = "Prophet model training not yet implemented"
                
            elif method == AnomalyMethod.LSTM:
                # LSTM for sequence anomaly detection
                # This is a placeholder - implement actual LSTM training
                result["message"] = "LSTM model training not yet implemented"
                
            else:
                # Train standard ML model
                numeric_data = data.select_dtypes(include=[np.number])
                
                if numeric_data.empty:
                    result["error"] = "No numeric data found for training"
                    return result
                    
                # Prepare and scale data
                X = numeric_data.dropna()
                scaler = StandardScaler()
                X_scaled = scaler.fit_transform(X)
                
                # Train model based on method
                if method == AnomalyMethod.ISOLATION_FOREST:
                    model = IsolationForest(contamination=0.1, random_state=42)
                elif method == AnomalyMethod.ONE_CLASS_SVM:
                    model = OneClassSVM(kernel="rbf", gamma="auto", nu=0.1)
                else:
                    result["error"] = f"Unknown method: {method}"
                    return result
                    
                model.fit(X_scaled)
                
                # Save model
                model_path = os.path.join(self.settings.ml_model_path, f"{dataset_id}_{method}.pkl")
                os.makedirs(os.path.dirname(model_path), exist_ok=True)
                
                joblib.dump({
                    "model": model,
                    "scaler": scaler,
                    "columns": numeric_data.columns.tolist(),
                    "method": method
                }, model_path)
                
                result["success"] = True
                result["model_path"] = model_path
                result["features"] = numeric_data.columns.tolist()
                
        except Exception as e:
            logger.error(f"Error training model: {e}")
            result["error"] = str(e)
            
        return result
        
    def _summarize_anomalies(self, anomalies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Summarize detected anomalies"""
        if not anomalies:
            return {
                "total_anomalies": 0,
                "by_method": {},
                "by_column": {},
                "by_severity": {}
            }
            
        summary = {
            "total_anomalies": len(anomalies),
            "by_method": {},
            "by_column": {},
            "by_severity": {"high": 0, "medium": 0, "low": 0}
        }
        
        for anomaly in anomalies:
            # By method
            method = anomaly["method"]
            if method not in summary["by_method"]:
                summary["by_method"][method] = 0
            summary["by_method"][method] += 1
            
            # By column
            if "column" in anomaly:
                column = anomaly["column"]
                if column not in summary["by_column"]:
                    summary["by_column"][column] = 0
                summary["by_column"][column] += 1
                
            # By severity
            severity = anomaly.get("severity", "medium")
            if severity in summary["by_severity"]:
                summary["by_severity"][severity] += 1
                
        return summary
        
    async def _auto_remediate(self, data: pd.DataFrame, 
                            anomalies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Automatically remediate detected anomalies"""
        # This is a placeholder for auto-remediation logic
        # In production, this would work with the remediation orchestrator
        
        return {
            "success": False,
            "message": "Auto-remediation not yet implemented",
            "anomalies_processed": 0
        }
        
    async def _load_models(self):
        """Load pre-trained models from storage"""
        model_dir = self.settings.ml_model_path
        
        if not os.path.exists(model_dir):
            return
            
        for filename in os.listdir(model_dir):
            if filename.endswith('.pkl'):
                try:
                    model_path = os.path.join(model_dir, filename)
                    model_data = joblib.load(model_path)
                    
                    # Extract model info from filename
                    parts = filename.replace('.pkl', '').split('_')
                    if len(parts) >= 2:
                        dataset_id = '_'.join(parts[:-1])
                        method = parts[-1]
                        
                        model_key = f"{method}_{dataset_id}"
                        self.models[model_key] = model_data["model"]
                        
                        if "scaler" in model_data:
                            scaler_key = f"{method}_{dataset_id}"
                            self.scalers[scaler_key] = model_data["scaler"]
                            
                        logger.info(f"Loaded model: {model_key}")
                        
                except Exception as e:
                    logger.error(f"Error loading model {filename}: {e}")
                    
    async def get_detection_history(self, dataset_id: Optional[str] = None,
                                  limit: int = 100) -> List[Dict[str, Any]]:
        """Get anomaly detection history"""
        history = self.detection_history
        
        if dataset_id:
            history = [h for h in history if h["dataset_id"] == dataset_id]
            
        # Sort by timestamp descending
        history.sort(key=lambda x: x["timestamp"], reverse=True)
        
        return history[:limit]
