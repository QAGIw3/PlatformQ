"""
Event Enricher Pulsar Function

Lightweight stateless event enrichment that runs directly on Pulsar brokers.
Reduces latency and service complexity by eliminating the need for separate microservices.
"""

from pulsar import Function
import json
import time
from typing import Dict, Any, Optional
import logging


class EventEnricherFunction(Function):
    """
    Pulsar Function for enriching events with additional metadata
    """
    
    def __init__(self):
        self.enrichment_cache = {}
        self.logger = logging.getLogger(__name__)
        
    def process(self, input_data: bytes, context: Any) -> bytes:
        """
        Process incoming event and enrich with metadata
        
        Args:
            input_data: Raw event bytes
            context: Pulsar Function context
            
        Returns:
            Enriched event as bytes
        """
        try:
            # Parse input event
            event = json.loads(input_data.decode('utf-8'))
            
            # Get event type and ID
            event_type = event.get('event_type', 'unknown')
            event_id = event.get('event_id') or context.get_message_id()
            
            # Add standard enrichments
            enriched_event = {
                **event,
                'enrichments': {
                    'processed_at': int(time.time() * 1000),
                    'processor_function': 'event-enricher',
                    'function_instance_id': context.get_instance_id(),
                    'source_topic': context.get_current_message_topic_name(),
                    'partition_key': context.get_partition_key(),
                }
            }
            
            # Type-specific enrichments
            if event_type == 'compute.order.created':
                enriched_event['enrichments'].update(self._enrich_compute_order(event))
            elif event_type == 'settlement.initiated':
                enriched_event['enrichments'].update(self._enrich_settlement(event))
            elif event_type == 'sla.violation':
                enriched_event['enrichments'].update(self._enrich_sla_violation(event))
            elif event_type == 'user.action':
                enriched_event['enrichments'].update(self._enrich_user_action(event))
                
            # Add derived fields
            enriched_event['enrichments']['processing_latency_ms'] = (
                enriched_event['enrichments']['processed_at'] - event.get('timestamp', 0)
            )
            
            # Log metrics
            context.record_metric('events_enriched', 1)
            context.record_metric('enrichment_latency_ms', 
                                 enriched_event['enrichments']['processing_latency_ms'])
            
            return json.dumps(enriched_event).encode('utf-8')
            
        except Exception as e:
            self.logger.error(f"Error enriching event: {e}")
            context.record_metric('enrichment_errors', 1)
            
            # Return original event with error flag
            error_event = json.loads(input_data.decode('utf-8'))
            error_event['enrichment_error'] = str(e)
            return json.dumps(error_event).encode('utf-8')
            
    def _enrich_compute_order(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich compute order events"""
        resource_type = event.get('resource_type', 'unknown')
        quantity = float(event.get('quantity', 0))
        
        enrichments = {
            'resource_category': self._categorize_resource(resource_type),
            'order_size': self._categorize_order_size(resource_type, quantity),
            'estimated_cost_range': self._estimate_cost_range(resource_type, quantity),
        }
        
        # Add GPU-specific enrichments
        if resource_type.lower() == 'gpu':
            enrichments['gpu_generation'] = self._get_gpu_generation(
                event.get('specifications', {}).get('gpu_type')
            )
            enrichments['requires_specialized_cooling'] = quantity > 8
            
        return enrichments
        
    def _enrich_settlement(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich settlement events"""
        return {
            'settlement_day': self._get_day_of_week(event.get('settlement_date')),
            'is_end_of_month': self._is_end_of_month(event.get('settlement_date')),
            'settlement_priority': self._calculate_priority(event),
        }
        
    def _enrich_sla_violation(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich SLA violation events"""
        violation_type = event.get('violation_type', 'unknown')
        severity = event.get('severity', 'low')
        
        return {
            'requires_immediate_action': severity in ['critical', 'high'],
            'violation_category': self._categorize_violation(violation_type),
            'estimated_penalty': self._estimate_penalty(violation_type, severity),
            'escalation_required': severity == 'critical',
        }
        
    def _enrich_user_action(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich user action events"""
        action = event.get('action', 'unknown')
        user_id = event.get('user_id')
        
        return {
            'action_category': self._categorize_action(action),
            'is_sensitive_action': action in ['delete', 'transfer', 'withdraw'],
            'requires_2fa': self._requires_2fa(action),
            'user_tier': self._get_user_tier_from_cache(user_id),
        }
        
    # Helper methods
    def _categorize_resource(self, resource_type: str) -> str:
        """Categorize compute resources"""
        categories = {
            'gpu': 'accelerated_compute',
            'cpu': 'general_compute',
            'tpu': 'ai_specialized',
            'memory': 'memory_optimized',
            'storage': 'storage_optimized',
        }
        return categories.get(resource_type.lower(), 'other')
        
    def _categorize_order_size(self, resource_type: str, quantity: float) -> str:
        """Categorize order size"""
        thresholds = {
            'gpu': {'small': 1, 'medium': 4, 'large': 8},
            'cpu': {'small': 16, 'medium': 64, 'large': 256},
            'memory': {'small': 64, 'medium': 256, 'large': 1024},
            'storage': {'small': 1000, 'medium': 5000, 'large': 10000},
        }
        
        resource_thresholds = thresholds.get(resource_type.lower(), 
                                            {'small': 10, 'medium': 50, 'large': 100})
        
        if quantity <= resource_thresholds['small']:
            return 'small'
        elif quantity <= resource_thresholds['medium']:
            return 'medium'
        elif quantity <= resource_thresholds['large']:
            return 'large'
        else:
            return 'enterprise'
            
    def _estimate_cost_range(self, resource_type: str, quantity: float) -> Dict[str, float]:
        """Estimate cost range for orders"""
        # Simplified cost estimation
        base_costs = {
            'gpu': 2.5,  # per GPU per hour
            'cpu': 0.1,  # per vCPU per hour
            'memory': 0.01,  # per GB per hour
            'storage': 0.0001,  # per GB per hour
        }
        
        base_cost = base_costs.get(resource_type.lower(), 0.05)
        min_cost = base_cost * quantity * 0.8
        max_cost = base_cost * quantity * 1.2
        
        return {
            'min_hourly_cost': round(min_cost, 2),
            'max_hourly_cost': round(max_cost, 2),
            'estimated_daily_cost': round((min_cost + max_cost) / 2 * 24, 2),
        }
        
    def _get_gpu_generation(self, gpu_type: Optional[str]) -> str:
        """Determine GPU generation"""
        if not gpu_type:
            return 'unknown'
            
        gpu_type_lower = gpu_type.lower()
        if 'h100' in gpu_type_lower:
            return 'hopper'
        elif 'a100' in gpu_type_lower:
            return 'ampere'
        elif 'v100' in gpu_type_lower:
            return 'volta'
        elif 't4' in gpu_type_lower:
            return 'turing'
        else:
            return 'other'
            
    def _get_day_of_week(self, date_str: Optional[str]) -> Optional[str]:
        """Get day of week from date string"""
        # Simplified - in production would parse date properly
        return 'monday'  # Placeholder
        
    def _is_end_of_month(self, date_str: Optional[str]) -> bool:
        """Check if date is end of month"""
        # Simplified - in production would parse date properly
        return False  # Placeholder
        
    def _calculate_priority(self, event: Dict[str, Any]) -> str:
        """Calculate settlement priority"""
        amount = float(event.get('amount', 0))
        if amount > 10000:
            return 'high'
        elif amount > 1000:
            return 'medium'
        else:
            return 'low'
            
    def _categorize_violation(self, violation_type: str) -> str:
        """Categorize SLA violations"""
        categories = {
            'uptime': 'availability',
            'latency': 'performance',
            'throughput': 'performance',
            'error_rate': 'reliability',
            'response_time': 'performance',
        }
        return categories.get(violation_type.lower(), 'other')
        
    def _estimate_penalty(self, violation_type: str, severity: str) -> float:
        """Estimate penalty amount"""
        base_penalties = {
            'uptime': 100,
            'latency': 50,
            'throughput': 75,
            'error_rate': 60,
        }
        
        severity_multipliers = {
            'low': 1,
            'medium': 2,
            'high': 5,
            'critical': 10,
        }
        
        base = base_penalties.get(violation_type.lower(), 25)
        multiplier = severity_multipliers.get(severity.lower(), 1)
        
        return base * multiplier
        
    def _categorize_action(self, action: str) -> str:
        """Categorize user actions"""
        categories = {
            'create': 'write',
            'update': 'write',
            'delete': 'write',
            'read': 'read',
            'list': 'read',
            'login': 'auth',
            'logout': 'auth',
            'transfer': 'financial',
            'withdraw': 'financial',
        }
        return categories.get(action.lower(), 'other')
        
    def _requires_2fa(self, action: str) -> bool:
        """Check if action requires 2FA"""
        sensitive_actions = ['delete', 'transfer', 'withdraw', 'update_password', 
                           'add_api_key', 'change_permissions']
        return action.lower() in sensitive_actions
        
    def _get_user_tier_from_cache(self, user_id: Optional[str]) -> str:
        """Get user tier from cache"""
        if not user_id:
            return 'unknown'
            
        # In production, this would check distributed cache
        # For now, return based on simple logic
        if user_id.startswith('vip-'):
            return 'platinum'
        elif user_id.startswith('pro-'):
            return 'gold'
        else:
            return 'standard'


# Configuration for Pulsar Function deployment
function_config = {
    "name": "event-enricher",
    "py": "event_enricher.py",
    "classname": "event_enricher.EventEnricherFunction",
    "inputs": [
        "persistent://platformq/raw/events"
    ],
    "output": "persistent://platformq/enriched/events",
    "log-topic": "persistent://platformq/functions/logs",
    "processing-guarantees": "ATLEAST_ONCE",
    "parallelism": 4,
    "cpu": 0.5,
    "ram": 536870912,  # 512MB
    "disk": 1073741824,  # 1GB
    "user-config": {
        "cache_ttl_seconds": 300,
        "max_cache_size": 1000
    }
} 