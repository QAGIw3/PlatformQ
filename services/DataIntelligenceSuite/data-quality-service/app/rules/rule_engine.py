"""
Data Quality Rule Engine

Manages and executes data quality rules with support for various rule types,
conditions, and actions.
"""

from datetime import datetime
from typing import Dict, List, Optional, Any, Union
from enum import Enum
from dataclasses import dataclass, field
import re
import json
import asyncio

from data_intelligence_common import StructuredLogger, MetricsCollector
from data_intelligence_common.vault_consul import VaultConsulIntegration

logger = StructuredLogger.get_logger(__name__)


class RuleType(Enum):
    """Types of data quality rules"""
    VALIDATION = "validation"          # Validate data against criteria
    TRANSFORMATION = "transformation"  # Transform data to fix issues
    PROFILING = "profiling"           # Profile data characteristics
    ANOMALY = "anomaly"               # Detect anomalies
    CONSISTENCY = "consistency"       # Check cross-dataset consistency
    COMPLIANCE = "compliance"         # Check regulatory compliance
    CUSTOM = "custom"                 # Custom rule logic


class ConditionOperator(Enum):
    """Condition operators"""
    EQUALS = "equals"
    NOT_EQUALS = "not_equals"
    GREATER_THAN = "greater_than"
    LESS_THAN = "less_than"
    GREATER_EQUAL = "greater_equal"
    LESS_EQUAL = "less_equal"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    MATCHES = "matches"  # Regex match
    IN = "in"
    NOT_IN = "not_in"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"
    BETWEEN = "between"


class ActionType(Enum):
    """Action types for rule execution"""
    LOG = "log"
    ALERT = "alert"
    REJECT = "reject"
    TRANSFORM = "transform"
    FLAG = "flag"
    QUARANTINE = "quarantine"
    REMEDIATE = "remediate"
    CUSTOM = "custom"


@dataclass
class RuleCondition:
    """Rule condition definition"""
    field: str
    operator: ConditionOperator
    value: Any
    case_sensitive: bool = True
    
    def evaluate(self, data: Dict[str, Any]) -> bool:
        """Evaluate condition against data"""
        field_value = self._get_field_value(data, self.field)
        
        try:
            if self.operator == ConditionOperator.EQUALS:
                return self._compare_values(field_value, self.value, self.case_sensitive)
            elif self.operator == ConditionOperator.NOT_EQUALS:
                return not self._compare_values(field_value, self.value, self.case_sensitive)
            elif self.operator == ConditionOperator.GREATER_THAN:
                return float(field_value) > float(self.value)
            elif self.operator == ConditionOperator.LESS_THAN:
                return float(field_value) < float(self.value)
            elif self.operator == ConditionOperator.GREATER_EQUAL:
                return float(field_value) >= float(self.value)
            elif self.operator == ConditionOperator.LESS_EQUAL:
                return float(field_value) <= float(self.value)
            elif self.operator == ConditionOperator.CONTAINS:
                return self._contains(field_value, self.value, self.case_sensitive)
            elif self.operator == ConditionOperator.NOT_CONTAINS:
                return not self._contains(field_value, self.value, self.case_sensitive)
            elif self.operator == ConditionOperator.MATCHES:
                flags = 0 if self.case_sensitive else re.IGNORECASE
                return bool(re.match(self.value, str(field_value), flags))
            elif self.operator == ConditionOperator.IN:
                return field_value in self.value
            elif self.operator == ConditionOperator.NOT_IN:
                return field_value not in self.value
            elif self.operator == ConditionOperator.IS_NULL:
                return field_value is None
            elif self.operator == ConditionOperator.IS_NOT_NULL:
                return field_value is not None
            elif self.operator == ConditionOperator.BETWEEN:
                return self.value[0] <= field_value <= self.value[1]
            else:
                logger.warning("unknown_operator", operator=self.operator)
                return False
        except Exception as e:
            logger.error("condition_evaluation_error", error=str(e))
            return False
    
    def _get_field_value(self, data: Dict[str, Any], field_path: str) -> Any:
        """Get nested field value"""
        parts = field_path.split('.')
        value = data
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                return None
        return value
    
    def _compare_values(self, val1: Any, val2: Any, case_sensitive: bool) -> bool:
        """Compare two values"""
        if not case_sensitive and isinstance(val1, str) and isinstance(val2, str):
            return val1.lower() == val2.lower()
        return val1 == val2
    
    def _contains(self, haystack: Any, needle: Any, case_sensitive: bool) -> bool:
        """Check if haystack contains needle"""
        if isinstance(haystack, str) and isinstance(needle, str):
            if not case_sensitive:
                return needle.lower() in haystack.lower()
            return needle in haystack
        elif isinstance(haystack, (list, tuple)):
            return needle in haystack
        return False


@dataclass
class RuleAction:
    """Rule action definition"""
    type: ActionType
    params: Dict[str, Any] = field(default_factory=dict)
    
    async def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the action"""
        result = {
            "action": self.type.value,
            "status": "success",
            "timestamp": datetime.utcnow().isoformat()
        }
        
        try:
            if self.type == ActionType.LOG:
                logger.info("rule_action_log", **self.params, **context)
            elif self.type == ActionType.ALERT:
                # Would integrate with alerting system
                result["alert_sent"] = True
            elif self.type == ActionType.REJECT:
                result["rejected"] = True
                result["reason"] = self.params.get("reason", "Rule violation")
            elif self.type == ActionType.TRANSFORM:
                result["transformation"] = self.params.get("transformation")
            elif self.type == ActionType.FLAG:
                result["flag"] = self.params.get("flag_name", "quality_issue")
            elif self.type == ActionType.QUARANTINE:
                result["quarantined"] = True
                result["location"] = self.params.get("quarantine_location")
            elif self.type == ActionType.REMEDIATE:
                result["remediation"] = self.params.get("remediation_type")
            elif self.type == ActionType.CUSTOM:
                # Execute custom action
                result["custom_result"] = await self._execute_custom_action(context)
        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)
            logger.error("action_execution_error", action=self.type.value, error=str(e))
        
        return result
    
    async def _execute_custom_action(self, context: Dict[str, Any]) -> Any:
        """Execute custom action logic"""
        # This would be extended with custom action implementations
        return {"custom": "executed"}


@dataclass
class QualityRule:
    """Data quality rule definition"""
    id: str
    name: str
    description: str
    type: RuleType
    conditions: List[RuleCondition]
    actions: List[RuleAction]
    enabled: bool = True
    priority: int = 0  # Higher priority rules execute first
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    
    # Rule logic
    condition_logic: str = "AND"  # AND or OR
    
    def evaluate_conditions(self, data: Dict[str, Any]) -> bool:
        """Evaluate all conditions"""
        if not self.conditions:
            return True
        
        results = [cond.evaluate(data) for cond in self.conditions]
        
        if self.condition_logic == "AND":
            return all(results)
        elif self.condition_logic == "OR":
            return any(results)
        else:
            logger.warning("unknown_condition_logic", logic=self.condition_logic)
            return False


@dataclass
class RuleExecutionResult:
    """Result of rule execution"""
    rule_id: str
    rule_name: str
    passed: bool
    conditions_met: bool
    actions_executed: List[Dict[str, Any]]
    execution_time_ms: float
    timestamp: datetime
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class RuleEngine:
    """
    Executes data quality rules
    """
    
    def __init__(
        self,
        vault_consul: VaultConsulIntegration,
        metrics_collector: Optional[MetricsCollector] = None
    ):
        self.vault_consul = vault_consul
        self.metrics = metrics_collector
        
        # Rule storage (in production, this would be in a database)
        self.rules: Dict[str, QualityRule] = {}
        
        # Rule execution history
        self.execution_history: List[RuleExecutionResult] = []
        
        # Define metrics
        if self.metrics:
            self._define_metrics()
    
    def _define_metrics(self):
        """Define Prometheus metrics"""
        self.metrics.define_metric(
            "dq_rules_total",
            "gauge",
            "Total number of data quality rules",
            ["type", "enabled"]
        )
        
        self.metrics.define_metric(
            "dq_rule_executions_total",
            "counter",
            "Total rule executions",
            ["rule_id", "status"]
        )
        
        self.metrics.define_metric(
            "dq_rule_execution_duration_seconds",
            "histogram",
            "Rule execution duration",
            ["rule_id"]
        )
        
        self.metrics.define_metric(
            "dq_rule_violations_total",
            "counter",
            "Total rule violations",
            ["rule_id", "rule_type"]
        )
    
    async def initialize(self):
        """Initialize rule engine"""
        logger.info("initializing_rule_engine")
        
        # Load rules from configuration
        await self._load_rules_from_config()
        
        # Update metrics
        self._update_rule_metrics()
        
        logger.info("rule_engine_initialized", rule_count=len(self.rules))
    
    async def _load_rules_from_config(self):
        """Load rules from Consul configuration"""
        try:
            rules_config = await self.vault_consul.get_config("data-quality/rules", {})
            
            for rule_id, rule_data in rules_config.items():
                rule = self._parse_rule(rule_id, rule_data)
                if rule:
                    self.rules[rule_id] = rule
        except Exception as e:
            logger.error("load_rules_error", error=str(e))
    
    def _parse_rule(self, rule_id: str, rule_data: Dict[str, Any]) -> Optional[QualityRule]:
        """Parse rule from configuration"""
        try:
            # Parse conditions
            conditions = []
            for cond_data in rule_data.get("conditions", []):
                condition = RuleCondition(
                    field=cond_data["field"],
                    operator=ConditionOperator(cond_data["operator"]),
                    value=cond_data["value"],
                    case_sensitive=cond_data.get("case_sensitive", True)
                )
                conditions.append(condition)
            
            # Parse actions
            actions = []
            for action_data in rule_data.get("actions", []):
                action = RuleAction(
                    type=ActionType(action_data["type"]),
                    params=action_data.get("params", {})
                )
                actions.append(action)
            
            # Create rule
            rule = QualityRule(
                id=rule_id,
                name=rule_data["name"],
                description=rule_data.get("description", ""),
                type=RuleType(rule_data["type"]),
                conditions=conditions,
                actions=actions,
                enabled=rule_data.get("enabled", True),
                priority=rule_data.get("priority", 0),
                tags=rule_data.get("tags", []),
                metadata=rule_data.get("metadata", {}),
                condition_logic=rule_data.get("condition_logic", "AND")
            )
            
            return rule
        except Exception as e:
            logger.error("parse_rule_error", rule_id=rule_id, error=str(e))
            return None
    
    def _update_rule_metrics(self):
        """Update rule count metrics"""
        if not self.metrics:
            return
        
        # Count rules by type and status
        for rule in self.rules.values():
            self.metrics.update_metric(
                "dq_rules_total",
                1,
                {
                    "type": rule.type.value,
                    "enabled": str(rule.enabled).lower()
                }
            )
    
    async def execute_rules(
        self,
        data: Union[Dict[str, Any], List[Dict[str, Any]]],
        rule_ids: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
        rule_types: Optional[List[RuleType]] = None
    ) -> List[RuleExecutionResult]:
        """Execute rules against data"""
        # Convert single record to list
        records = data if isinstance(data, list) else [data]
        
        # Get applicable rules
        rules_to_execute = self._get_applicable_rules(rule_ids, tags, rule_types)
        
        # Sort by priority
        rules_to_execute.sort(key=lambda r: r.priority, reverse=True)
        
        results = []
        
        for record in records:
            for rule in rules_to_execute:
                result = await self._execute_single_rule(rule, record)
                results.append(result)
                self.execution_history.append(result)
        
        return results
    
    def _get_applicable_rules(
        self,
        rule_ids: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
        rule_types: Optional[List[RuleType]] = None
    ) -> List[QualityRule]:
        """Get rules that should be executed"""
        rules = []
        
        for rule in self.rules.values():
            if not rule.enabled:
                continue
            
            # Filter by rule IDs
            if rule_ids and rule.id not in rule_ids:
                continue
            
            # Filter by tags
            if tags and not any(tag in rule.tags for tag in tags):
                continue
            
            # Filter by type
            if rule_types and rule.type not in rule_types:
                continue
            
            rules.append(rule)
        
        return rules
    
    async def _execute_single_rule(
        self,
        rule: QualityRule,
        data: Dict[str, Any]
    ) -> RuleExecutionResult:
        """Execute a single rule"""
        start_time = datetime.utcnow()
        
        try:
            # Evaluate conditions
            conditions_met = rule.evaluate_conditions(data)
            
            # Execute actions if conditions are met
            actions_executed = []
            if conditions_met:
                for action in rule.actions:
                    action_result = await action.execute({
                        "rule_id": rule.id,
                        "rule_name": rule.name,
                        "data": data
                    })
                    actions_executed.append(action_result)
            
            # Calculate execution time
            execution_time_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
            
            # Create result
            result = RuleExecutionResult(
                rule_id=rule.id,
                rule_name=rule.name,
                passed=not conditions_met,  # Rule passes if conditions NOT met (no violation)
                conditions_met=conditions_met,
                actions_executed=actions_executed,
                execution_time_ms=execution_time_ms,
                timestamp=datetime.utcnow()
            )
            
            # Update metrics
            if self.metrics:
                self.metrics.update_metric(
                    "dq_rule_executions_total",
                    1,
                    {"rule_id": rule.id, "status": "success"}
                )
                
                self.metrics.update_metric(
                    "dq_rule_execution_duration_seconds",
                    execution_time_ms / 1000,
                    {"rule_id": rule.id}
                )
                
                if conditions_met:
                    self.metrics.update_metric(
                        "dq_rule_violations_total",
                        1,
                        {"rule_id": rule.id, "rule_type": rule.type.value}
                    )
            
            logger.debug(
                "rule_executed",
                rule_id=rule.id,
                passed=result.passed,
                execution_time_ms=execution_time_ms
            )
            
            return result
            
        except Exception as e:
            logger.error("rule_execution_error", rule_id=rule.id, error=str(e))
            
            if self.metrics:
                self.metrics.update_metric(
                    "dq_rule_executions_total",
                    1,
                    {"rule_id": rule.id, "status": "error"}
                )
            
            return RuleExecutionResult(
                rule_id=rule.id,
                rule_name=rule.name,
                passed=False,
                conditions_met=False,
                actions_executed=[],
                execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000,
                timestamp=datetime.utcnow(),
                error=str(e)
            )
    
    def add_rule(self, rule: QualityRule):
        """Add a rule to the engine"""
        self.rules[rule.id] = rule
        self._update_rule_metrics()
        logger.info("rule_added", rule_id=rule.id, rule_name=rule.name)
    
    def remove_rule(self, rule_id: str):
        """Remove a rule from the engine"""
        if rule_id in self.rules:
            del self.rules[rule_id]
            self._update_rule_metrics()
            logger.info("rule_removed", rule_id=rule_id)
    
    def get_rule(self, rule_id: str) -> Optional[QualityRule]:
        """Get a specific rule"""
        return self.rules.get(rule_id)
    
    def get_all_rules(self) -> List[QualityRule]:
        """Get all rules"""
        return list(self.rules.values())
    
    def get_execution_history(
        self,
        rule_id: Optional[str] = None,
        limit: int = 100
    ) -> List[RuleExecutionResult]:
        """Get execution history"""
        history = self.execution_history
        
        if rule_id:
            history = [r for r in history if r.rule_id == rule_id]
        
        return history[-limit:] 