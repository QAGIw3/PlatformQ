"""Service Discovery Integration with Consul

Handles service registration, health checks, and discovery.
"""

import logging
import asyncio
from typing import Dict, Any, List, Optional
import aiohttp
import json

from app.core.config import Settings


logger = logging.getLogger(__name__)


async def register_service(settings: Settings):
    """Register service with Consul"""
    service_config = {
        "ID": f"{settings.consul_service_name}-{settings.api_port}",
        "Name": settings.consul_service_name,
        "Tags": [
            "batch-processing",
            "spark",
            "data-intelligence",
            settings.environment
        ],
        "Address": settings.api_host,
        "Port": settings.api_port,
        "Meta": {
            "version": settings.service_version,
            "environment": settings.environment,
            "service_type": "batch-processing"
        },
        "Check": {
            "HTTP": f"http://{settings.api_host}:{settings.api_port}/api/v1/health",
            "Interval": settings.consul_health_check_interval,
            "Timeout": "5s"
        }
    }
    
    try:
        url = f"http://{settings.consul_host}:{settings.consul_port}/v1/agent/service/register"
        
        async with aiohttp.ClientSession() as session:
            async with session.put(url, json=service_config) as resp:
                if resp.status == 200:
                    logger.info(f"Successfully registered service with Consul: {settings.consul_service_name}")
                else:
                    logger.error(f"Failed to register service: {resp.status} - {await resp.text()}")
                    
    except Exception as e:
        logger.error(f"Error registering service with Consul: {e}")
        raise


async def deregister_service(settings: Settings):
    """Deregister service from Consul"""
    service_id = f"{settings.consul_service_name}-{settings.api_port}"
    
    try:
        url = f"http://{settings.consul_host}:{settings.consul_port}/v1/agent/service/deregister/{service_id}"
        
        async with aiohttp.ClientSession() as session:
            async with session.put(url) as resp:
                if resp.status == 200:
                    logger.info(f"Successfully deregistered service from Consul: {service_id}")
                else:
                    logger.error(f"Failed to deregister service: {resp.status}")
                    
    except Exception as e:
        logger.error(f"Error deregistering service from Consul: {e}") 