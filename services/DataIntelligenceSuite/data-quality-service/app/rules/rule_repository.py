"""
Rule Repository

Manages persistence and retrieval of data quality rules.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
import json

from data_intelligence_common import StructuredLogger
from data_intelligence_common.vault_consul import VaultConsulIntegration
from .rule_engine import QualityRule, RuleType, RuleCondition, RuleAction, ConditionOperator, ActionType

logger = StructuredLogger.get_logger(__name__)


class RuleRepository:
    """
    Repository for managing data quality rules
    """
    
    def __init__(self, vault_consul: VaultConsulIntegration):
        self.vault_consul = vault_consul
        self.rules_cache: Dict[str, QualityRule] = {}
        self.last_sync: Optional[datetime] = None
    
    async def initialize(self):
        """Initialize repository"""
        logger.info("initializing_rule_repository")
        await self.sync_rules()
        logger.info("rule_repository_initialized", rule_count=len(self.rules_cache))
    
    async def sync_rules(self):
        """Sync rules from Consul"""
        try:
            # Get all rules from Consul
            rules_data = await self.vault_consul.get_config("data-quality/rules", {})
            
            # Parse and cache rules
            self.rules_cache.clear()
            for rule_id, rule_data in rules_data.items():
                rule = self._parse_rule_data(rule_id, rule_data)
                if rule:
                    self.rules_cache[rule_id] = rule
            
            self.last_sync = datetime.utcnow()
            logger.info("rules_synced", count=len(self.rules_cache))
            
        except Exception as e:
            logger.error("sync_rules_error", error=str(e))
    
    def _parse_rule_data(self, rule_id: str, data: Dict[str, Any]) -> Optional[QualityRule]:
        """Parse rule data into QualityRule object"""
        try:
            # Parse conditions
            conditions = []
            for cond in data.get("conditions", []):
                condition = RuleCondition(
                    field=cond["field"],
                    operator=ConditionOperator(cond["operator"]),
                    value=cond["value"],
                    case_sensitive=cond.get("case_sensitive", True)
                )
                conditions.append(condition)
            
            # Parse actions
            actions = []
            for act in data.get("actions", []):
                action = RuleAction(
                    type=ActionType(act["type"]),
                    params=act.get("params", {})
                )
                actions.append(action)
            
            # Create rule
            rule = QualityRule(
                id=rule_id,
                name=data["name"],
                description=data.get("description", ""),
                type=RuleType(data["type"]),
                conditions=conditions,
                actions=actions,
                enabled=data.get("enabled", True),
                priority=data.get("priority", 0),
                tags=data.get("tags", []),
                metadata=data.get("metadata", {}),
                condition_logic=data.get("condition_logic", "AND"),
                created_at=datetime.fromisoformat(data["created_at"]) if "created_at" in data else datetime.utcnow(),
                updated_at=datetime.fromisoformat(data["updated_at"]) if "updated_at" in data else datetime.utcnow()
            )
            
            return rule
            
        except Exception as e:
            logger.error("parse_rule_error", rule_id=rule_id, error=str(e))
            return None
    
    def _serialize_rule(self, rule: QualityRule) -> Dict[str, Any]:
        """Serialize rule to dictionary"""
        return {
            "name": rule.name,
            "description": rule.description,
            "type": rule.type.value,
            "conditions": [
                {
                    "field": cond.field,
                    "operator": cond.operator.value,
                    "value": cond.value,
                    "case_sensitive": cond.case_sensitive
                }
                for cond in rule.conditions
            ],
            "actions": [
                {
                    "type": action.type.value,
                    "params": action.params
                }
                for action in rule.actions
            ],
            "enabled": rule.enabled,
            "priority": rule.priority,
            "tags": rule.tags,
            "metadata": rule.metadata,
            "condition_logic": rule.condition_logic,
            "created_at": rule.created_at.isoformat(),
            "updated_at": rule.updated_at.isoformat()
        }
    
    async def get_rule(self, rule_id: str) -> Optional[QualityRule]:
        """Get a specific rule"""
        return self.rules_cache.get(rule_id)
    
    async def get_all_rules(self) -> List[QualityRule]:
        """Get all rules"""
        return list(self.rules_cache.values())
    
    async def get_rules_by_type(self, rule_type: RuleType) -> List[QualityRule]:
        """Get rules by type"""
        return [r for r in self.rules_cache.values() if r.type == rule_type]
    
    async def get_rules_by_tags(self, tags: List[str]) -> List[QualityRule]:
        """Get rules by tags"""
        return [
            r for r in self.rules_cache.values()
            if any(tag in r.tags for tag in tags)
        ]
    
    async def save_rule(self, rule: QualityRule) -> bool:
        """Save a rule"""
        try:
            # Update timestamps
            if rule.id not in self.rules_cache:
                rule.created_at = datetime.utcnow()
            rule.updated_at = datetime.utcnow()
            
            # Serialize rule
            rule_data = self._serialize_rule(rule)
            
            # Save to Consul
            key = f"data-quality/rules/{rule.id}"
            await self.vault_consul.consul.kv.put(key, json.dumps(rule_data))
            
            # Update cache
            self.rules_cache[rule.id] = rule
            
            logger.info("rule_saved", rule_id=rule.id)
            return True
            
        except Exception as e:
            logger.error("save_rule_error", rule_id=rule.id, error=str(e))
            return False
    
    async def delete_rule(self, rule_id: str) -> bool:
        """Delete a rule"""
        try:
            # Delete from Consul
            key = f"data-quality/rules/{rule_id}"
            await self.vault_consul.consul.kv.delete(key)
            
            # Remove from cache
            if rule_id in self.rules_cache:
                del self.rules_cache[rule_id]
            
            logger.info("rule_deleted", rule_id=rule_id)
            return True
            
        except Exception as e:
            logger.error("delete_rule_error", rule_id=rule_id, error=str(e))
            return False
    
    async def bulk_save_rules(self, rules: List[QualityRule]) -> Dict[str, bool]:
        """Save multiple rules"""
        results = {}
        
        for rule in rules:
            success = await self.save_rule(rule)
            results[rule.id] = success
        
        return results
    
    async def search_rules(
        self,
        query: str,
        rule_type: Optional[RuleType] = None,
        tags: Optional[List[str]] = None,
        enabled_only: bool = True
    ) -> List[QualityRule]:
        """Search rules by query"""
        results = []
        query_lower = query.lower()
        
        for rule in self.rules_cache.values():
            # Filter by enabled status
            if enabled_only and not rule.enabled:
                continue
            
            # Filter by type
            if rule_type and rule.type != rule_type:
                continue
            
            # Filter by tags
            if tags and not any(tag in rule.tags for tag in tags):
                continue
            
            # Search in name and description
            if (query_lower in rule.name.lower() or 
                query_lower in rule.description.lower() or
                any(query_lower in tag.lower() for tag in rule.tags)):
                results.append(rule)
        
        return results
    
    async def get_rule_statistics(self) -> Dict[str, Any]:
        """Get rule statistics"""
        stats = {
            "total_rules": len(self.rules_cache),
            "enabled_rules": sum(1 for r in self.rules_cache.values() if r.enabled),
            "disabled_rules": sum(1 for r in self.rules_cache.values() if not r.enabled),
            "rules_by_type": {},
            "rules_by_priority": {},
            "last_sync": self.last_sync.isoformat() if self.last_sync else None
        }
        
        # Count by type
        for rule_type in RuleType:
            count = sum(1 for r in self.rules_cache.values() if r.type == rule_type)
            if count > 0:
                stats["rules_by_type"][rule_type.value] = count
        
        # Count by priority
        for rule in self.rules_cache.values():
            priority = str(rule.priority)
            stats["rules_by_priority"][priority] = stats["rules_by_priority"].get(priority, 0) + 1
        
        return stats 