"""
Model explainability utilities.

Provides model interpretation using SHAP, LIME, and other techniques.
"""

from typing import Any, Dict, List, Optional, Union, Tuple, Callable
from enum import Enum
from dataclasses import dataclass, field
import numpy as np
import pandas as pd
from abc import ABC, abstractmethod
import matplotlib.pyplot as plt
import seaborn as sns

try:
    import shap
except ImportError:
    shap = None
    
try:
    import lime
    import lime.lime_tabular
    import lime.lime_text
except ImportError:
    lime = None

from ..caching import CacheManager
from ..events import EventBus, Event
from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class ExplanationType(str, Enum):
    """Types of model explanations"""
    SHAP = "shap"
    LIME = "lime"
    PERMUTATION = "permutation"
    PARTIAL_DEPENDENCE = "partial_dependence"
    FEATURE_IMPORTANCE = "feature_importance"
    COUNTERFACTUAL = "counterfactual"


class ExplanationScope(str, Enum):
    """Scope of explanation"""
    GLOBAL = "global"
    LOCAL = "local"


@dataclass
class FeatureImportance:
    """Feature importance information"""
    feature_name: str
    importance: float
    
    # Additional metrics
    std_deviation: Optional[float] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "feature_name": self.feature_name,
            "importance": self.importance,
            "std_deviation": self.std_deviation,
            "min_value": self.min_value,
            "max_value": self.max_value
        }


@dataclass
class LocalExplanation:
    """Local explanation for single prediction"""
    instance_id: Optional[str] = None
    prediction: Optional[float] = None
    actual: Optional[float] = None
    
    # Feature contributions
    feature_contributions: Dict[str, float] = field(default_factory=dict)
    
    # Base value (for additive explanations)
    base_value: Optional[float] = None
    
    # Metadata
    explanation_type: ExplanationType = ExplanationType.SHAP
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "instance_id": self.instance_id,
            "prediction": self.prediction,
            "actual": self.actual,
            "feature_contributions": self.feature_contributions,
            "base_value": self.base_value,
            "explanation_type": self.explanation_type.value
        }


@dataclass
class GlobalExplanation:
    """Global model explanation"""
    # Feature importances
    feature_importances: List[FeatureImportance] = field(default_factory=list)
    
    # Interaction effects
    interaction_effects: Optional[Dict[Tuple[str, str], float]] = None
    
    # Summary statistics
    mean_abs_shap_values: Optional[Dict[str, float]] = None
    
    # Metadata
    explanation_type: ExplanationType = ExplanationType.SHAP
    num_samples: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "feature_importances": [fi.to_dict() for fi in self.feature_importances],
            "interaction_effects": {
                f"{k[0]}_{k[1]}": v for k, v in self.interaction_effects.items()
            } if self.interaction_effects else None,
            "mean_abs_shap_values": self.mean_abs_shap_values,
            "explanation_type": self.explanation_type.value,
            "num_samples": self.num_samples
        }


class BaseExplainer(ABC):
    """Base class for model explainers"""
    
    @abstractmethod
    def explain_local(
        self,
        model: Any,
        X: Union[np.ndarray, pd.DataFrame],
        instance_idx: int,
        **kwargs
    ) -> LocalExplanation:
        """Explain single prediction"""
        pass
        
    @abstractmethod
    def explain_global(
        self,
        model: Any,
        X: Union[np.ndarray, pd.DataFrame],
        **kwargs
    ) -> GlobalExplanation:
        """Explain model globally"""
        pass


class ShapExplainer(BaseExplainer):
    """SHAP-based model explainer"""
    
    def __init__(self):
        if shap is None:
            raise ImportError("SHAP is not installed. Install with: pip install shap")
            
        self.explainer = None
        self.shap_values = None
        
    def explain_local(
        self,
        model: Any,
        X: Union[np.ndarray, pd.DataFrame],
        instance_idx: int,
        feature_names: Optional[List[str]] = None,
        **kwargs
    ) -> LocalExplanation:
        """Explain single prediction using SHAP"""
        # Create explainer if needed
        if self.explainer is None:
            self.explainer = self._create_explainer(model, X)
            
        # Get instance
        if isinstance(X, pd.DataFrame):
            instance = X.iloc[[instance_idx]]
            feature_names = feature_names or X.columns.tolist()
        else:
            instance = X[instance_idx:instance_idx+1]
            
        # Calculate SHAP values
        shap_values = self.explainer.shap_values(instance)
        
        # Handle multi-class
        if isinstance(shap_values, list):
            # Use values for predicted class
            prediction = model.predict(instance)[0]
            if hasattr(model, 'predict_proba'):
                class_idx = np.argmax(model.predict_proba(instance)[0])
                shap_values = shap_values[class_idx]
            else:
                shap_values = shap_values[0]
                
        # Create explanation
        explanation = LocalExplanation(
            instance_id=str(instance_idx),
            prediction=float(model.predict(instance)[0]),
            explanation_type=ExplanationType.SHAP
        )
        
        # Add feature contributions
        shap_values = shap_values.flatten()
        if feature_names:
            explanation.feature_contributions = dict(zip(feature_names, shap_values))
        else:
            explanation.feature_contributions = {
                f"feature_{i}": val for i, val in enumerate(shap_values)
            }
            
        # Add base value
        if hasattr(self.explainer, 'expected_value'):
            if isinstance(self.explainer.expected_value, np.ndarray):
                explanation.base_value = float(self.explainer.expected_value[0])
            else:
                explanation.base_value = float(self.explainer.expected_value)
                
        return explanation
        
    def explain_global(
        self,
        model: Any,
        X: Union[np.ndarray, pd.DataFrame],
        feature_names: Optional[List[str]] = None,
        max_samples: int = 1000,
        **kwargs
    ) -> GlobalExplanation:
        """Explain model globally using SHAP"""
        # Sample if dataset is large
        if len(X) > max_samples:
            if isinstance(X, pd.DataFrame):
                X_sample = X.sample(n=max_samples, random_state=42)
            else:
                idx = np.random.choice(len(X), max_samples, replace=False)
                X_sample = X[idx]
        else:
            X_sample = X
            
        # Create explainer
        if self.explainer is None:
            self.explainer = self._create_explainer(model, X_sample)
            
        # Calculate SHAP values
        shap_values = self.explainer.shap_values(X_sample)
        
        # Handle multi-class
        if isinstance(shap_values, list):
            # Average across classes
            shap_values = np.mean(np.abs(shap_values), axis=0)
        else:
            shap_values = np.abs(shap_values)
            
        # Get feature names
        if isinstance(X, pd.DataFrame) and feature_names is None:
            feature_names = X.columns.tolist()
        elif feature_names is None:
            feature_names = [f"feature_{i}" for i in range(X.shape[1])]
            
        # Calculate mean absolute SHAP values
        mean_abs_shap = np.mean(shap_values, axis=0)
        
        # Create feature importances
        feature_importances = []
        for i, fname in enumerate(feature_names):
            importance = FeatureImportance(
                feature_name=fname,
                importance=float(mean_abs_shap[i]),
                std_deviation=float(np.std(shap_values[:, i])),
                min_value=float(np.min(shap_values[:, i])),
                max_value=float(np.max(shap_values[:, i]))
            )
            feature_importances.append(importance)
            
        # Sort by importance
        feature_importances.sort(key=lambda x: x.importance, reverse=True)
        
        # Create global explanation
        explanation = GlobalExplanation(
            feature_importances=feature_importances,
            mean_abs_shap_values=dict(zip(feature_names, mean_abs_shap)),
            explanation_type=ExplanationType.SHAP,
            num_samples=len(X_sample)
        )
        
        # Store for visualization
        self.shap_values = shap_values
        
        return explanation
        
    def _create_explainer(self, model: Any, X: Union[np.ndarray, pd.DataFrame]) -> Any:
        """Create SHAP explainer based on model type"""
        model_type = type(model).__name__
        
        # Tree-based models
        if 'Tree' in model_type or 'Forest' in model_type or 'XGB' in model_type or 'LGB' in model_type:
            return shap.TreeExplainer(model)
            
        # Linear models
        elif 'Linear' in model_type or 'Logistic' in model_type:
            return shap.LinearExplainer(model, X)
            
        # Deep learning models
        elif 'keras' in str(type(model)) or 'torch' in str(type(model)):
            return shap.DeepExplainer(model, X)
            
        # Default to kernel explainer
        else:
            return shap.KernelExplainer(model.predict, shap.sample(X, 100))
            
    def plot_waterfall(
        self,
        explanation: LocalExplanation,
        max_features: int = 10
    ):
        """Plot waterfall chart for local explanation"""
        if shap is None:
            logger.warning("SHAP not available for plotting")
            return
            
        # Sort features by absolute contribution
        sorted_features = sorted(
            explanation.feature_contributions.items(),
            key=lambda x: abs(x[1]),
            reverse=True
        )[:max_features]
        
        # Create waterfall plot
        feature_names = [f[0] for f in sorted_features]
        values = [f[1] for f in sorted_features]
        
        plt.figure(figsize=(10, 6))
        plt.barh(range(len(values)), values)
        plt.yticks(range(len(values)), feature_names)
        plt.xlabel("SHAP Value")
        plt.title("Feature Contributions")
        plt.tight_layout()
        
        return plt.gcf()
        
    def plot_summary(
        self,
        X: Union[np.ndarray, pd.DataFrame],
        feature_names: Optional[List[str]] = None,
        plot_type: str = "bar"
    ):
        """Plot SHAP summary"""
        if shap is None or self.shap_values is None:
            logger.warning("SHAP not available for plotting")
            return
            
        plt.figure(figsize=(10, 8))
        shap.summary_plot(
            self.shap_values,
            X,
            feature_names=feature_names,
            plot_type=plot_type,
            show=False
        )
        
        return plt.gcf()


class LimeExplainer(BaseExplainer):
    """LIME-based model explainer"""
    
    def __init__(self, mode: str = "tabular"):
        if lime is None:
            raise ImportError("LIME is not installed. Install with: pip install lime")
            
        self.mode = mode
        self.explainer = None
        
    def explain_local(
        self,
        model: Any,
        X: Union[np.ndarray, pd.DataFrame],
        instance_idx: int,
        feature_names: Optional[List[str]] = None,
        num_features: int = 10,
        **kwargs
    ) -> LocalExplanation:
        """Explain single prediction using LIME"""
        # Create explainer if needed
        if self.explainer is None:
            self.explainer = self._create_explainer(X, feature_names, model)
            
        # Get instance
        if isinstance(X, pd.DataFrame):
            instance = X.iloc[instance_idx].values
            feature_names = feature_names or X.columns.tolist()
        else:
            instance = X[instance_idx]
            
        # Get prediction function
        if hasattr(model, 'predict_proba'):
            predict_fn = model.predict_proba
        else:
            predict_fn = model.predict
            
        # Explain instance
        exp = self.explainer.explain_instance(
            instance,
            predict_fn,
            num_features=num_features
        )
        
        # Create explanation
        explanation = LocalExplanation(
            instance_id=str(instance_idx),
            prediction=float(model.predict(instance.reshape(1, -1))[0]),
            explanation_type=ExplanationType.LIME
        )
        
        # Add feature contributions
        for feature_idx, contribution in exp.as_list():
            if feature_names and feature_idx < len(feature_names):
                feature_name = feature_names[feature_idx]
            else:
                feature_name = f"feature_{feature_idx}"
            explanation.feature_contributions[feature_name] = contribution
            
        return explanation
        
    def explain_global(
        self,
        model: Any,
        X: Union[np.ndarray, pd.DataFrame],
        feature_names: Optional[List[str]] = None,
        num_samples: int = 100,
        **kwargs
    ) -> GlobalExplanation:
        """Explain model globally using LIME"""
        # LIME is primarily for local explanations
        # We aggregate local explanations for global view
        
        # Sample instances
        if len(X) > num_samples:
            if isinstance(X, pd.DataFrame):
                sample_idx = X.sample(n=num_samples, random_state=42).index
            else:
                sample_idx = np.random.choice(len(X), num_samples, replace=False)
        else:
            sample_idx = range(len(X))
            
        # Get feature names
        if isinstance(X, pd.DataFrame) and feature_names is None:
            feature_names = X.columns.tolist()
        elif feature_names is None:
            feature_names = [f"feature_{i}" for i in range(X.shape[1])]
            
        # Collect feature contributions
        feature_contributions = defaultdict(list)
        
        for idx in sample_idx:
            local_exp = self.explain_local(
                model, X, idx, feature_names
            )
            
            for feature, contribution in local_exp.feature_contributions.items():
                feature_contributions[feature].append(abs(contribution))
                
        # Calculate aggregated importances
        feature_importances = []
        for feature, contributions in feature_contributions.items():
            importance = FeatureImportance(
                feature_name=feature,
                importance=float(np.mean(contributions)),
                std_deviation=float(np.std(contributions)),
                min_value=float(np.min(contributions)),
                max_value=float(np.max(contributions))
            )
            feature_importances.append(importance)
            
        # Sort by importance
        feature_importances.sort(key=lambda x: x.importance, reverse=True)
        
        return GlobalExplanation(
            feature_importances=feature_importances,
            explanation_type=ExplanationType.LIME,
            num_samples=len(sample_idx)
        )
        
    def _create_explainer(
        self,
        X: Union[np.ndarray, pd.DataFrame],
        feature_names: Optional[List[str]],
        model: Any
    ) -> Any:
        """Create LIME explainer"""
        if self.mode == "tabular":
            if isinstance(X, pd.DataFrame):
                training_data = X.values
            else:
                training_data = X
                
            # Determine mode
            if hasattr(model, 'predict_proba'):
                mode = "classification"
            else:
                mode = "regression"
                
            return lime.lime_tabular.LimeTabularExplainer(
                training_data,
                feature_names=feature_names,
                mode=mode
            )
        else:
            raise ValueError(f"Unsupported LIME mode: {self.mode}")


class ModelExplainer:
    """
    Unified model explainer interface.
    
    Features:
    - Multiple explanation methods
    - Local and global explanations
    - Visualization support
    - Caching for performance
    """
    
    def __init__(
        self,
        cache_manager: Optional[CacheManager] = None,
        event_bus: Optional[EventBus] = None
    ):
        self.cache = cache_manager
        self.event_bus = event_bus
        
        # Available explainers
        self._explainers = {
            ExplanationType.SHAP: ShapExplainer() if shap else None,
            ExplanationType.LIME: LimeExplainer() if lime else None
        }
        
        # Cache for explanations
        self._explanation_cache: Dict[str, Any] = {}
        
    def explain(
        self,
        model: Any,
        X: Union[np.ndarray, pd.DataFrame],
        y: Optional[Union[np.ndarray, pd.Series]] = None,
        explanation_type: ExplanationType = ExplanationType.SHAP,
        scope: ExplanationScope = ExplanationScope.GLOBAL,
        instance_idx: Optional[int] = None,
        feature_names: Optional[List[str]] = None,
        **kwargs
    ) -> Union[LocalExplanation, GlobalExplanation]:
        """Generate model explanation"""
        # Check if explainer is available
        explainer = self._explainers.get(explanation_type)
        if explainer is None:
            raise ValueError(f"Explainer {explanation_type} not available")
            
        # Generate cache key
        cache_key = f"{id(model)}_{explanation_type}_{scope}_{instance_idx}"
        
        # Check cache
        if self.cache and cache_key in self._explanation_cache:
            logger.info("Using cached explanation")
            return self._explanation_cache[cache_key]
            
        # Generate explanation
        if scope == ExplanationScope.LOCAL:
            if instance_idx is None:
                raise ValueError("instance_idx required for local explanation")
            explanation = explainer.explain_local(
                model, X, instance_idx, feature_names, **kwargs
            )
        else:
            explanation = explainer.explain_global(
                model, X, feature_names, **kwargs
            )
            
        # Add actual values if provided
        if y is not None and scope == ExplanationScope.LOCAL:
            if isinstance(y, pd.Series):
                explanation.actual = float(y.iloc[instance_idx])
            else:
                explanation.actual = float(y[instance_idx])
                
        # Cache explanation
        if self.cache:
            self._explanation_cache[cache_key] = explanation
            
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type=f"explanation.{scope.value}.generated",
                source="model_explainer",
                data={
                    "explanation_type": explanation_type.value,
                    "model_type": type(model).__name__
                }
            ))
            
        return explanation
        
    def compare_explanations(
        self,
        model: Any,
        X: Union[np.ndarray, pd.DataFrame],
        methods: List[ExplanationType],
        instance_idx: Optional[int] = None,
        feature_names: Optional[List[str]] = None
    ) -> Dict[str, Union[LocalExplanation, GlobalExplanation]]:
        """Compare explanations from different methods"""
        results = {}
        
        scope = ExplanationScope.LOCAL if instance_idx is not None else ExplanationScope.GLOBAL
        
        for method in methods:
            try:
                explanation = self.explain(
                    model, X,
                    explanation_type=method,
                    scope=scope,
                    instance_idx=instance_idx,
                    feature_names=feature_names
                )
                results[method.value] = explanation
            except Exception as e:
                logger.error(f"Failed to generate {method} explanation: {e}")
                
        return results
        
    def get_top_features(
        self,
        explanation: Union[LocalExplanation, GlobalExplanation],
        n: int = 10
    ) -> List[Tuple[str, float]]:
        """Get top contributing features"""
        if isinstance(explanation, LocalExplanation):
            # Sort by absolute contribution
            sorted_features = sorted(
                explanation.feature_contributions.items(),
                key=lambda x: abs(x[1]),
                reverse=True
            )
            return sorted_features[:n]
            
        else:  # GlobalExplanation
            # Return top feature importances
            return [
                (fi.feature_name, fi.importance)
                for fi in explanation.feature_importances[:n]
            ]
            
    def plot_explanation(
        self,
        explanation: Union[LocalExplanation, GlobalExplanation],
        plot_type: str = "bar",
        max_features: int = 20,
        figsize: Tuple[int, int] = (10, 8)
    ):
        """Plot explanation visualization"""
        plt.figure(figsize=figsize)
        
        if isinstance(explanation, LocalExplanation):
            # Local explanation plot
            features = self.get_top_features(explanation, max_features)
            feature_names = [f[0] for f in features]
            values = [f[1] for f in features]
            
            plt.barh(range(len(values)), values)
            plt.yticks(range(len(values)), feature_names)
            plt.xlabel("Feature Contribution")
            plt.title("Local Feature Contributions")
            
        else:  # GlobalExplanation
            # Global explanation plot
            features = self.get_top_features(explanation, max_features)
            feature_names = [f[0] for f in features]
            values = [f[1] for f in features]
            
            if plot_type == "bar":
                plt.bar(range(len(values)), values)
                plt.xticks(range(len(values)), feature_names, rotation=45, ha='right')
                plt.ylabel("Importance")
                plt.title("Global Feature Importance")
            else:
                # Alternative plot types can be added
                pass
                
        plt.tight_layout()
        return plt.gcf()
        
    def generate_report(
        self,
        model: Any,
        X: Union[np.ndarray, pd.DataFrame],
        y: Optional[Union[np.ndarray, pd.Series]] = None,
        feature_names: Optional[List[str]] = None,
        sample_size: int = 100
    ) -> Dict[str, Any]:
        """Generate comprehensive explainability report"""
        report = {
            "model_type": type(model).__name__,
            "num_features": X.shape[1],
            "num_samples": len(X)
        }
        
        # Global explanation
        try:
            global_exp = self.explain(
                model, X,
                explanation_type=ExplanationType.SHAP,
                scope=ExplanationScope.GLOBAL,
                feature_names=feature_names,
                max_samples=sample_size
            )
            
            report["global_explanation"] = {
                "top_features": self.get_top_features(global_exp, 10),
                "feature_importances": global_exp.to_dict()
            }
        except Exception as e:
            logger.error(f"Failed to generate global explanation: {e}")
            
        # Sample local explanations
        sample_idx = np.random.choice(len(X), min(5, len(X)), replace=False)
        local_explanations = []
        
        for idx in sample_idx:
            try:
                local_exp = self.explain(
                    model, X, y,
                    explanation_type=ExplanationType.SHAP,
                    scope=ExplanationScope.LOCAL,
                    instance_idx=int(idx),
                    feature_names=feature_names
                )
                local_explanations.append(local_exp.to_dict())
            except Exception as e:
                logger.error(f"Failed to generate local explanation for instance {idx}: {e}")
                
        report["sample_local_explanations"] = local_explanations
        
        # Model performance summary if labels provided
        if y is not None:
            predictions = model.predict(X)
            
            if hasattr(model, 'predict_proba'):
                # Classification metrics
                from sklearn.metrics import accuracy_score, classification_report
                report["performance"] = {
                    "accuracy": accuracy_score(y, predictions),
                    "classification_report": classification_report(y, predictions, output_dict=True)
                }
            else:
                # Regression metrics
                from sklearn.metrics import mean_squared_error, r2_score
                report["performance"] = {
                    "mse": mean_squared_error(y, predictions),
                    "r2": r2_score(y, predictions)
                }
                
        return report 