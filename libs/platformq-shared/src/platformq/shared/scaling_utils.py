import logging
from pyignite import Client as IgniteClient
from typing import Dict

logger = logging.getLogger(__name__)

class ScalingUtils:
    @staticmethod
    def get_resource_state(ignite_client: IgniteClient, cache_name: str, key: str) -> Dict:
        cache = ignite_client.get_cache(cache_name)
        return cache.get(key) or {}

    @staticmethod
    def update_scale_factor(ignite_client: IgniteClient, cache_name: str, key: str, factor: float):
        cache = ignite_client.get_cache(cache_name)
        state = ScalingUtils.get_resource_state(ignite_client, cache_name, key)
        state['scale_factor'] = factor
        cache.put(key, state)
        logger.info(f"Updated scale factor for {key} to {factor}") 