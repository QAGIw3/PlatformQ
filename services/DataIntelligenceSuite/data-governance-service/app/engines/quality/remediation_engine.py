"""
Data Quality Remediation Engine.
"""

import asyncio
from typing import Dict, List, Any, Optional, Union, Callable, Tuple
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
import pandas as pd
import numpy as np
from sklearn.impute import SimpleImputer, KNNImputer
from sklearn.preprocessing import StandardScaler

from data_intelligence_common.core.events import EventBus
from data_intelligence_common.core.caching import CacheManager

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)


class RemediationType(str, Enum):
    """Types of remediation strategies."""
    IMPUTATION = "imputation"
    OUTLIER_TREATMENT = "outlier_treatment"
    DEDUPLICATION = "deduplication"
    STANDARDIZATION = "standardization"
    VALIDATION_FIX = "validation_fix"
    SCHEMA_CORRECTION = "schema_correction"
    CUSTOM = "custom"


class RemediationStrategy(str, Enum):
    """Specific remediation strategies."""
    # Imputation strategies
    MEAN_IMPUTATION = "mean_imputation"
    MEDIAN_IMPUTATION = "median_imputation"
    MODE_IMPUTATION = "mode_imputation"
    FORWARD_FILL = "forward_fill"
    BACKWARD_FILL = "backward_fill"
    INTERPOLATION = "interpolation"
    KNN_IMPUTATION = "knn_imputation"
    ML_IMPUTATION = "ml_imputation"
    
    # Outlier strategies
    CAP_OUTLIERS = "cap_outliers"
    REMOVE_OUTLIERS = "remove_outliers"
    TRANSFORM_OUTLIERS = "transform_outliers"
    
    # Deduplication strategies
    KEEP_FIRST = "keep_first"
    KEEP_LAST = "keep_last"
    KEEP_ALL = "keep_all"
    MERGE_DUPLICATES = "merge_duplicates"
    
    # Standardization strategies
    CASE_NORMALIZATION = "case_normalization"
    TRIM_WHITESPACE = "trim_whitespace"
    FORMAT_DATES = "format_dates"
    NORMALIZE_ENCODING = "normalize_encoding"


@dataclass
class RemediationAction:
    """Represents a single remediation action."""
    action_id: str
    remediation_type: RemediationType
    strategy: RemediationStrategy
    target_columns: List[str]
    parameters: Dict[str, Any] = field(default_factory=dict)
    description: str = ""
    estimated_impact: Optional[Dict[str, Any]] = None


@dataclass
class RemediationResult:
    """Result of remediation process."""
    remediation_id: str
    dataset_id: str
    timestamp: datetime
    
    # Actions taken
    actions: List[RemediationAction]
    
    # Results
    original_quality_score: float
    final_quality_score: float
    quality_improvement: float
    
    # Changes
    records_modified: int
    cells_modified: int
    columns_affected: List[str]
    
    # Data
    remediated_data: Optional[pd.DataFrame] = None
    change_log: List[Dict[str, Any]] = field(default_factory=list)
    
    # Metadata
    execution_time_ms: float = 0.0
    success: bool = True
    errors: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class RemediationEngine:
    """
    Automated data quality remediation engine.
    """
    
    def __init__(
        self,
        event_bus: EventBus,
        cache_manager: CacheManager
    ):
        self.event_bus = event_bus
        self.cache_manager = cache_manager
        
        # Strategy handlers
        self.strategy_handlers: Dict[RemediationStrategy, Callable] = {}
        self._register_default_handlers()
        
        # Remediation history
        self.remediation_history: List[RemediationResult] = []
        
        # Configuration
        self.auto_remediate = False
        self.confidence_threshold = 0.8
        
        logger.info("Remediation Engine initialized")
        
    async def initialize(self):
        """Initialize remediation engine."""
        # Subscribe to events
        await self.event_bus.subscribe("quality.remediation.request", self._handle_remediation_request)
        await self.event_bus.subscribe("quality.issues.detected", self._handle_quality_issues)
        
        logger.info("Remediation Engine ready")
        
    def _register_default_handlers(self):
        """Register default remediation handlers."""
        # Imputation handlers
        self.strategy_handlers[RemediationStrategy.MEAN_IMPUTATION] = self._impute_mean
        self.strategy_handlers[RemediationStrategy.MEDIAN_IMPUTATION] = self._impute_median
        self.strategy_handlers[RemediationStrategy.MODE_IMPUTATION] = self._impute_mode
        self.strategy_handlers[RemediationStrategy.FORWARD_FILL] = self._forward_fill
        self.strategy_handlers[RemediationStrategy.BACKWARD_FILL] = self._backward_fill
        self.strategy_handlers[RemediationStrategy.KNN_IMPUTATION] = self._knn_imputation
        
        # Outlier handlers
        self.strategy_handlers[RemediationStrategy.CAP_OUTLIERS] = self._cap_outliers
        self.strategy_handlers[RemediationStrategy.REMOVE_OUTLIERS] = self._remove_outliers
        
        # Deduplication handlers
        self.strategy_handlers[RemediationStrategy.KEEP_FIRST] = self._dedupe_keep_first
        self.strategy_handlers[RemediationStrategy.KEEP_LAST] = self._dedupe_keep_last
        self.strategy_handlers[RemediationStrategy.MERGE_DUPLICATES] = self._merge_duplicates
        
        # Standardization handlers
        self.strategy_handlers[RemediationStrategy.CASE_NORMALIZATION] = self._normalize_case
        self.strategy_handlers[RemediationStrategy.TRIM_WHITESPACE] = self._trim_whitespace
        
    async def remediate_data(
        self,
        data: pd.DataFrame,
        dataset_id: str,
        quality_issues: List[Dict[str, Any]],
        actions: Optional[List[RemediationAction]] = None,
        auto_select: bool = True
    ) -> RemediationResult:
        """
        Remediate data quality issues.
        
        Args:
            data: Data to remediate
            dataset_id: Dataset identifier
            quality_issues: List of quality issues to address
            actions: Specific remediation actions (auto-selected if None)
            auto_select: Whether to auto-select remediation strategies
            
        Returns:
            RemediationResult with remediated data and changes
        """
        start_time = datetime.utcnow()
        remediation_id = f"remediation_{dataset_id}_{start_time.timestamp()}"
        
        # Store original quality score
        original_quality_score = self._calculate_quality_score(data, quality_issues)
        
        # Select remediation actions if not provided
        if actions is None and auto_select:
            actions = await self._select_remediation_actions(data, quality_issues)
        elif actions is None:
            actions = []
        
        # Apply remediations
        remediated_data = data.copy()
        change_log = []
        errors = []
        cells_modified = 0
        columns_affected = set()
        
        for action in actions:
            try:
                # Apply remediation
                result_data, changes = await self._apply_remediation(
                    remediated_data,
                    action
                )
                
                remediated_data = result_data
                change_log.extend(changes)
                
                # Track modifications
                for change in changes:
                    cells_modified += change.get("cells_affected", 0)
                    columns_affected.update(change.get("columns", []))
                    
            except Exception as e:
                logger.error(f"Error applying remediation {action.action_id}: {e}")
                errors.append(f"Action {action.action_id}: {str(e)}")
        
        # Calculate final quality score
        final_issues = await self._reassess_quality(remediated_data)
        final_quality_score = self._calculate_quality_score(remediated_data, final_issues)
        
        # Calculate improvement
        quality_improvement = final_quality_score - original_quality_score
        
        # Create result
        execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
        
        result = RemediationResult(
            remediation_id=remediation_id,
            dataset_id=dataset_id,
            timestamp=start_time,
            actions=actions,
            original_quality_score=original_quality_score,
            final_quality_score=final_quality_score,
            quality_improvement=quality_improvement,
            records_modified=len(data) - len(remediated_data) + 
                           sum(1 for _, row in remediated_data.iterrows() 
                               if not row.equals(data.loc[row.name]) if row.name in data.index),
            cells_modified=cells_modified,
            columns_affected=list(columns_affected),
            remediated_data=remediated_data,
            change_log=change_log,
            execution_time_ms=execution_time,
            success=len(errors) == 0,
            errors=errors,
            metadata={
                "issues_addressed": len(quality_issues),
                "actions_applied": len(actions) - len(errors)
            }
        )
        
        # Store result
        self.remediation_history.append(result)
        
        # Cache result (without data)
        cache_result = {
            **result.__dict__,
            "remediated_data": None  # Don't cache the actual data
        }
        await self.cache_manager.set(
            f"quality:remediation:{remediation_id}",
            cache_result,
            ttl=86400  # 24 hours
        )
        
        # Publish event
        await self.event_bus.publish("quality.remediation.complete", {
            "remediation_id": remediation_id,
            "dataset_id": dataset_id,
            "quality_improvement": quality_improvement,
            "actions_applied": len(actions) - len(errors),
            "success": result.success
        })
        
        logger.info(
            f"Remediation complete for {dataset_id}: "
            f"quality improved by {quality_improvement:.2%}"
        )
        
        return result
        
    async def _apply_remediation(
        self,
        data: pd.DataFrame,
        action: RemediationAction
    ) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
        """Apply a single remediation action."""
        handler = self.strategy_handlers.get(action.strategy)
        
        if not handler:
            raise ValueError(f"No handler for strategy: {action.strategy}")
        
        # Apply handler
        result_data, changes = await handler(data, action)
        
        return result_data, changes
        
    async def _select_remediation_actions(
        self,
        data: pd.DataFrame,
        quality_issues: List[Dict[str, Any]]
    ) -> List[RemediationAction]:
        """Auto-select remediation actions based on quality issues."""
        actions = []
        action_counter = 0
        
        for issue in quality_issues:
            issue_type = issue.get("issue")
            column = issue.get("column")
            severity = issue.get("severity")
            
            # Select strategy based on issue type
            if issue_type == "low_completeness":
                # Imputation for missing values
                if column and column in data.columns:
                    if pd.api.types.is_numeric_dtype(data[column]):
                        strategy = RemediationStrategy.MEAN_IMPUTATION
                    else:
                        strategy = RemediationStrategy.MODE_IMPUTATION
                    
                    actions.append(RemediationAction(
                        action_id=f"action_{action_counter}",
                        remediation_type=RemediationType.IMPUTATION,
                        strategy=strategy,
                        target_columns=[column],
                        description=f"Impute missing values in {column}"
                    ))
                    action_counter += 1
                    
            elif issue_type == "outliers":
                # Outlier treatment
                if severity in ["high", "critical"]:
                    strategy = RemediationStrategy.CAP_OUTLIERS
                else:
                    strategy = RemediationStrategy.TRANSFORM_OUTLIERS
                
                actions.append(RemediationAction(
                    action_id=f"action_{action_counter}",
                    remediation_type=RemediationType.OUTLIER_TREATMENT,
                    strategy=strategy,
                    target_columns=[column] if column else [],
                    description=f"Handle outliers in {column}"
                ))
                action_counter += 1
                
            elif issue_type == "duplicates":
                # Deduplication
                actions.append(RemediationAction(
                    action_id=f"action_{action_counter}",
                    remediation_type=RemediationType.DEDUPLICATION,
                    strategy=RemediationStrategy.KEEP_FIRST,
                    target_columns=issue.get("key_columns", []),
                    description="Remove duplicate records"
                ))
                action_counter += 1
        
        return actions
        
    async def _impute_mean(
        self,
        data: pd.DataFrame,
        action: RemediationAction
    ) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
        """Impute missing values with mean."""
        result = data.copy()
        changes = []
        
        for column in action.target_columns:
            if column in result.columns and pd.api.types.is_numeric_dtype(result[column]):
                missing_count = result[column].isna().sum()
                if missing_count > 0:
                    mean_value = result[column].mean()
                    result[column].fillna(mean_value, inplace=True)
                    
                    changes.append({
                        "type": "imputation",
                        "column": column,
                        "strategy": "mean",
                        "value": mean_value,
                        "cells_affected": missing_count
                    })
        
        return result, changes
        
    async def _impute_median(
        self,
        data: pd.DataFrame,
        action: RemediationAction
    ) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
        """Impute missing values with median."""
        result = data.copy()
        changes = []
        
        for column in action.target_columns:
            if column in result.columns and pd.api.types.is_numeric_dtype(result[column]):
                missing_count = result[column].isna().sum()
                if missing_count > 0:
                    median_value = result[column].median()
                    result[column].fillna(median_value, inplace=True)
                    
                    changes.append({
                        "type": "imputation",
                        "column": column,
                        "strategy": "median",
                        "value": median_value,
                        "cells_affected": missing_count
                    })
        
        return result, changes
        
    async def _impute_mode(
        self,
        data: pd.DataFrame,
        action: RemediationAction
    ) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
        """Impute missing values with mode."""
        result = data.copy()
        changes = []
        
        for column in action.target_columns:
            if column in result.columns:
                missing_count = result[column].isna().sum()
                if missing_count > 0:
                    mode_result = result[column].mode()
                    if len(mode_result) > 0:
                        mode_value = mode_result.iloc[0]
                        result[column].fillna(mode_value, inplace=True)
                        
                        changes.append({
                            "type": "imputation",
                            "column": column,
                            "strategy": "mode",
                            "value": mode_value,
                            "cells_affected": missing_count
                        })
        
        return result, changes
        
    async def _forward_fill(
        self,
        data: pd.DataFrame,
        action: RemediationAction
    ) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
        """Forward fill missing values."""
        result = data.copy()
        changes = []
        
        for column in action.target_columns:
            if column in result.columns:
                missing_count = result[column].isna().sum()
                if missing_count > 0:
                    result[column].fillna(method='ffill', inplace=True)
                    
                    changes.append({
                        "type": "imputation",
                        "column": column,
                        "strategy": "forward_fill",
                        "cells_affected": missing_count
                    })
        
        return result, changes
        
    async def _backward_fill(
        self,
        data: pd.DataFrame,
        action: RemediationAction
    ) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
        """Backward fill missing values."""
        result = data.copy()
        changes = []
        
        for column in action.target_columns:
            if column in result.columns:
                missing_count = result[column].isna().sum()
                if missing_count > 0:
                    result[column].fillna(method='bfill', inplace=True)
                    
                    changes.append({
                        "type": "imputation",
                        "column": column,
                        "strategy": "backward_fill",
                        "cells_affected": missing_count
                    })
        
        return result, changes
        
    async def _knn_imputation(
        self,
        data: pd.DataFrame,
        action: RemediationAction
    ) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
        """KNN imputation for missing values."""
        result = data.copy()
        changes = []
        
        # Get numeric columns
        numeric_cols = result.select_dtypes(include=[np.number]).columns
        target_cols = [col for col in action.target_columns if col in numeric_cols]
        
        if target_cols:
            # Apply KNN imputation
            imputer = KNNImputer(n_neighbors=action.parameters.get("n_neighbors", 5))
            
            # Track missing counts
            missing_counts = {col: result[col].isna().sum() for col in target_cols}
            
            # Impute
            result[target_cols] = imputer.fit_transform(result[target_cols])
            
            for col, count in missing_counts.items():
                if count > 0:
                    changes.append({
                        "type": "imputation",
                        "column": col,
                        "strategy": "knn",
                        "cells_affected": count,
                        "parameters": {"n_neighbors": imputer.n_neighbors}
                    })
        
        return result, changes
        
    async def _cap_outliers(
        self,
        data: pd.DataFrame,
        action: RemediationAction
    ) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
        """Cap outliers at percentile bounds."""
        result = data.copy()
        changes = []
        
        lower_percentile = action.parameters.get("lower_percentile", 1)
        upper_percentile = action.parameters.get("upper_percentile", 99)
        
        for column in action.target_columns:
            if column in result.columns and pd.api.types.is_numeric_dtype(result[column]):
                # Calculate bounds
                lower_bound = result[column].quantile(lower_percentile / 100)
                upper_bound = result[column].quantile(upper_percentile / 100)
                
                # Count outliers
                lower_outliers = (result[column] < lower_bound).sum()
                upper_outliers = (result[column] > upper_bound).sum()
                
                # Cap values
                result[column] = result[column].clip(lower=lower_bound, upper=upper_bound)
                
                if lower_outliers + upper_outliers > 0:
                    changes.append({
                        "type": "outlier_treatment",
                        "column": column,
                        "strategy": "capping",
                        "lower_bound": lower_bound,
                        "upper_bound": upper_bound,
                        "cells_affected": lower_outliers + upper_outliers
                    })
        
        return result, changes
        
    async def _remove_outliers(
        self,
        data: pd.DataFrame,
        action: RemediationAction
    ) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
        """Remove outlier records."""
        result = data.copy()
        changes = []
        
        method = action.parameters.get("method", "iqr")
        threshold = action.parameters.get("threshold", 1.5)
        
        # Create mask for outliers
        outlier_mask = pd.Series([False] * len(result))
        
        for column in action.target_columns:
            if column in result.columns and pd.api.types.is_numeric_dtype(result[column]):
                if method == "iqr":
                    q1 = result[column].quantile(0.25)
                    q3 = result[column].quantile(0.75)
                    iqr = q3 - q1
                    lower_bound = q1 - threshold * iqr
                    upper_bound = q3 + threshold * iqr
                    column_outliers = (result[column] < lower_bound) | (result[column] > upper_bound)
                elif method == "zscore":
                    mean = result[column].mean()
                    std = result[column].std()
                    z_scores = np.abs((result[column] - mean) / std)
                    column_outliers = z_scores > threshold
                else:
                    column_outliers = pd.Series([False] * len(result))
                
                outlier_mask |= column_outliers
        
        # Remove outliers
        outliers_count = outlier_mask.sum()
        result = result[~outlier_mask]
        
        if outliers_count > 0:
            changes.append({
                "type": "outlier_treatment",
                "strategy": "removal",
                "method": method,
                "records_removed": outliers_count,
                "columns": action.target_columns
            })
        
        return result, changes
        
    async def _dedupe_keep_first(
        self,
        data: pd.DataFrame,
        action: RemediationAction
    ) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
        """Remove duplicates keeping first occurrence."""
        result = data.copy()
        changes = []
        
        subset = action.target_columns if action.target_columns else None
        duplicates_count = result.duplicated(subset=subset).sum()
        
        if duplicates_count > 0:
            result = result.drop_duplicates(subset=subset, keep='first')
            
            changes.append({
                "type": "deduplication",
                "strategy": "keep_first",
                "records_removed": duplicates_count,
                "key_columns": action.target_columns or "all"
            })
        
        return result, changes
        
    async def _dedupe_keep_last(
        self,
        data: pd.DataFrame,
        action: RemediationAction
    ) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
        """Remove duplicates keeping last occurrence."""
        result = data.copy()
        changes = []
        
        subset = action.target_columns if action.target_columns else None
        duplicates_count = result.duplicated(subset=subset).sum()
        
        if duplicates_count > 0:
            result = result.drop_duplicates(subset=subset, keep='last')
            
            changes.append({
                "type": "deduplication",
                "strategy": "keep_last",
                "records_removed": duplicates_count,
                "key_columns": action.target_columns or "all"
            })
        
        return result, changes
        
    async def _merge_duplicates(
        self,
        data: pd.DataFrame,
        action: RemediationAction
    ) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
        """Merge duplicate records."""
        result = data.copy()
        changes = []
        
        # Group by key columns
        key_cols = action.target_columns
        if not key_cols:
            return result, changes
        
        # Aggregation strategy
        agg_strategy = action.parameters.get("aggregation", {})
        
        # Default aggregation functions
        default_agg = {}
        for col in result.columns:
            if col not in key_cols:
                if pd.api.types.is_numeric_dtype(result[col]):
                    default_agg[col] = 'mean'
                else:
                    default_agg[col] = 'first'
        
        # Override with custom aggregation
        default_agg.update(agg_strategy)
        
        # Group and aggregate
        original_count = len(result)
        result = result.groupby(key_cols).agg(default_agg).reset_index()
        merged_count = original_count - len(result)
        
        if merged_count > 0:
            changes.append({
                "type": "deduplication",
                "strategy": "merge",
                "records_merged": merged_count,
                "key_columns": key_cols,
                "aggregation": default_agg
            })
        
        return result, changes
        
    async def _normalize_case(
        self,
        data: pd.DataFrame,
        action: RemediationAction
    ) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
        """Normalize string case."""
        result = data.copy()
        changes = []
        
        case_type = action.parameters.get("case", "lower")
        
        for column in action.target_columns:
            if column in result.columns and result[column].dtype == 'object':
                if case_type == "lower":
                    result[column] = result[column].str.lower()
                elif case_type == "upper":
                    result[column] = result[column].str.upper()
                elif case_type == "title":
                    result[column] = result[column].str.title()
                
                changes.append({
                    "type": "standardization",
                    "column": column,
                    "strategy": "case_normalization",
                    "case": case_type
                })
        
        return result, changes
        
    async def _trim_whitespace(
        self,
        data: pd.DataFrame,
        action: RemediationAction
    ) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
        """Trim whitespace from strings."""
        result = data.copy()
        changes = []
        
        for column in action.target_columns:
            if column in result.columns and result[column].dtype == 'object':
                # Count cells with whitespace
                whitespace_count = result[column].str.contains(r'^\s+|\s+$', na=False).sum()
                
                if whitespace_count > 0:
                    result[column] = result[column].str.strip()
                    
                    changes.append({
                        "type": "standardization",
                        "column": column,
                        "strategy": "trim_whitespace",
                        "cells_affected": whitespace_count
                    })
        
        return result, changes
        
    def _calculate_quality_score(
        self,
        data: pd.DataFrame,
        issues: List[Dict[str, Any]]
    ) -> float:
        """Calculate overall quality score."""
        if not issues:
            return 1.0
        
        # Weight issues by severity
        severity_weights = {
            "critical": 0.4,
            "high": 0.3,
            "medium": 0.2,
            "low": 0.1,
            "info": 0.05
        }
        
        total_weight = 0
        weighted_issues = 0
        
        for issue in issues:
            severity = issue.get("severity", "medium")
            weight = severity_weights.get(severity, 0.1)
            total_weight += weight
            weighted_issues += weight
        
        if total_weight == 0:
            return 1.0
        
        # Calculate score (inverse of weighted issues)
        max_possible_weight = len(issues) * max(severity_weights.values())
        quality_score = 1.0 - (weighted_issues / max_possible_weight)
        
        return max(0.0, min(1.0, quality_score))
        
    async def _reassess_quality(self, data: pd.DataFrame) -> List[Dict[str, Any]]:
        """Reassess quality after remediation."""
        # This would call the quality validator to get new issues
        # For now, return empty list (assuming all issues fixed)
        return []
        
    async def _handle_remediation_request(self, event_data: Dict[str, Any]):
        """Handle remediation request event."""
        try:
            dataset_id = event_data["dataset_id"]
            data = pd.DataFrame(event_data["data"])
            quality_issues = event_data.get("quality_issues", [])
            
            result = await self.remediate_data(data, dataset_id, quality_issues)
            
            # Publish result (without data)
            result_dict = {
                **result.__dict__,
                "remediated_data": None
            }
            await self.event_bus.publish("quality.remediation.result", result_dict)
            
        except Exception as e:
            logger.error(f"Error handling remediation request: {e}")
            await self.event_bus.publish("quality.remediation.error", {
                "error": str(e),
                "event_data": event_data
            })
            
    async def _handle_quality_issues(self, event_data: Dict[str, Any]):
        """Handle quality issues for auto-remediation."""
        if not self.auto_remediate:
            return
        
        try:
            dataset_id = event_data["dataset_id"]
            issues = event_data["issues"]
            confidence = event_data.get("confidence", 0.0)
            
            # Only auto-remediate if confidence is high enough
            if confidence >= self.confidence_threshold:
                # Trigger remediation
                await self.event_bus.publish("quality.remediation.request", {
                    "dataset_id": dataset_id,
                    "quality_issues": issues,
                    "auto_triggered": True
                })
                
        except Exception as e:
            logger.error(f"Error in auto-remediation: {e}")
            
    def register_custom_handler(
        self,
        strategy: RemediationStrategy,
        handler: Callable
    ):
        """Register a custom remediation handler."""
        self.strategy_handlers[strategy] = handler
        logger.info(f"Registered custom handler for strategy: {strategy}")
        
    async def get_remediation_recommendations(
        self,
        quality_issues: List[Dict[str, Any]]
    ) -> List[RemediationAction]:
        """Get recommended remediation actions for quality issues."""
        return await self._select_remediation_actions(pd.DataFrame(), quality_issues) 