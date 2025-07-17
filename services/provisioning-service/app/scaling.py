from pyignite import Client as IgniteClient
from typing import Dict
import logging
import asyncio

from platformq.shared.scaling_utils import ScalingUtils

logger = logging.getLogger(__name__)

class AdaptiveScaler:
    def __init__(self, ignite_client: IgniteClient, resource_cache):
        self.ignite_client = ignite_client
        self.resource_cache = resource_cache

    def initialize_tenant_resources(self, tenant_id: str):
        ScalingUtils.update_scale_factor(self.ignite_client, 'resource_states', tenant_id, 1.0)

    def handle_anomaly(self, anomaly):
        tenant_id = anomaly.details.get('tenant_id', 'default')
        current_factor = ScalingUtils.get_resource_state(self.ignite_client, 'resource_states', tenant_id).get('scale_factor', 1.0)
        if anomaly.severity > 0.5:
            new_factor = current_factor * 1.2
            ScalingUtils.update_scale_factor(self.ignite_client, 'resource_states', tenant_id, new_factor)
        # Cache for high volume
        if self.resource_cache.size() > 1000:
            logger.warning("High cache size, applying backpressure")
            asyncio.sleep(1) 