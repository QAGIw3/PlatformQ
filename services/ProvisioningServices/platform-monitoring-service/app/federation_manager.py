"""Prometheus Federation Manager"""

import asyncio
import logging
import yaml
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

import aiohttp
import aiofiles
from prometheus_client import Counter, Gauge, Histogram

from config import settings
from models import (
    RegionConfig,
    FederationStatus,
    RegionStatus,
    AlertRule,
    RuleGroup,
    PrometheusTarget
)

logger = logging.getLogger(__name__)

# Metrics
federation_sync_counter = Counter(
    'federation_sync_total',
    'Total number of federation syncs',
    ['status']
)
federation_regions_gauge = Gauge(
    'federation_regions_active',
    'Number of active regions in federation'
)
federation_targets_gauge = Gauge(
    'federation_targets_total',
    'Total number of scrape targets',
    ['region']
)
federation_sync_duration = Histogram(
    'federation_sync_duration_seconds',
    'Duration of federation sync operations'
)


class FederationManager:
    """Manages Prometheus federation across regions"""
    
    def __init__(self):
        self.regions: Dict[str, RegionConfig] = {}
        self.alert_rules: Dict[str, AlertRule] = {}
        self.running = False
        self.sync_task = None
        self.session = None
        self.config_path = Path(settings.FEDERATION_CONFIG_PATH)
        
    async def start(self):
        """Start the federation manager"""
        logger.info("Starting Federation Manager")
        self.running = True
        self.session = aiohttp.ClientSession()
        
        # Create config directory if it doesn't exist
        self.config_path.mkdir(parents=True, exist_ok=True)
        
        # Load existing configuration
        await self._load_configuration()
        
        # Start sync task
        self.sync_task = asyncio.create_task(self._sync_loop())
        
    async def stop(self):
        """Stop the federation manager"""
        logger.info("Stopping Federation Manager")
        self.running = False
        
        if self.sync_task:
            self.sync_task.cancel()
            try:
                await self.sync_task
            except asyncio.CancelledError:
                pass
                
        if self.session:
            await self.session.close()
            
    async def is_ready(self) -> bool:
        """Check if federation manager is ready"""
        return self.running and len(self.regions) > 0
        
    async def register_region(self, region_id: str, config: RegionConfig):
        """Register a new region for federation"""
        logger.info(f"Registering region: {region_id}")
        
        # Validate region connectivity
        if not await self._validate_region(config):
            raise ValueError(f"Region {region_id} validation failed")
            
        self.regions[region_id] = config
        
        # Update federation configuration
        await self._update_federation_config()
        
        # Update metrics
        federation_regions_gauge.set(len(self.regions))
        
        logger.info(f"Region {region_id} registered successfully")
        
    async def unregister_region(self, region_id: str):
        """Unregister a region from federation"""
        logger.info(f"Unregistering region: {region_id}")
        
        if region_id not in self.regions:
            raise ValueError(f"Region {region_id} not found")
            
        del self.regions[region_id]
        
        # Update federation configuration
        await self._update_federation_config()
        
        # Update metrics
        federation_regions_gauge.set(len(self.regions))
        
        logger.info(f"Region {region_id} unregistered successfully")
        
    async def list_regions(self) -> List[RegionConfig]:
        """List all registered regions"""
        return list(self.regions.values())
        
    async def get_status(self) -> FederationStatus:
        """Get current federation status"""
        region_status = {}
        total_targets = 0
        active_alerts = 0
        total_series = 0
        
        for region_id, config in self.regions.items():
            try:
                status = await self._get_region_status(region_id, config)
                region_status[region_id] = status
                total_targets += status.get('targets', 0)
                active_alerts += status.get('alerts', 0)
                total_series += status.get('series', 0)
            except Exception as e:
                logger.error(f"Failed to get status for region {region_id}: {e}")
                region_status[region_id] = {
                    'status': RegionStatus.UNKNOWN,
                    'error': str(e)
                }
                
        # Determine global status
        statuses = [s.get('status', RegionStatus.UNKNOWN) for s in region_status.values()]
        if all(s == RegionStatus.HEALTHY for s in statuses):
            global_status = RegionStatus.HEALTHY
        elif any(s == RegionStatus.UNHEALTHY for s in statuses):
            global_status = RegionStatus.UNHEALTHY
        elif any(s == RegionStatus.DEGRADED for s in statuses):
            global_status = RegionStatus.DEGRADED
        else:
            global_status = RegionStatus.UNKNOWN
            
        return FederationStatus(
            global_status=global_status,
            regions=region_status,
            last_sync=datetime.utcnow(),
            total_targets=total_targets,
            active_alerts=active_alerts,
            total_series=total_series
        )
        
    async def create_alert_rule(self, rule: AlertRule):
        """Create a new alert rule across all regions"""
        logger.info(f"Creating alert rule: {rule.name}")
        
        self.alert_rules[rule.name] = rule
        
        # Update alert rules configuration
        await self._update_alert_rules()
        
        logger.info(f"Alert rule {rule.name} created successfully")
        
    async def list_alert_rules(self) -> List[AlertRule]:
        """List all configured alert rules"""
        return list(self.alert_rules.values())
        
    async def sync_configuration(self):
        """Force sync federation configuration"""
        with federation_sync_duration.time():
            try:
                await self._update_federation_config()
                await self._update_alert_rules()
                await self._reload_prometheus_config()
                federation_sync_counter.labels(status='success').inc()
            except Exception as e:
                logger.error(f"Federation sync failed: {e}")
                federation_sync_counter.labels(status='failure').inc()
                raise
                
    async def _sync_loop(self):
        """Background sync loop"""
        while self.running:
            try:
                await self.sync_configuration()
            except Exception as e:
                logger.error(f"Sync loop error: {e}")
                
            await asyncio.sleep(settings.FEDERATION_SYNC_INTERVAL)
            
    async def _validate_region(self, config: RegionConfig) -> bool:
        """Validate region connectivity"""
        try:
            # Check Prometheus endpoint
            async with self.session.get(
                f"{config.prometheus_url}/api/v1/query",
                params={'query': 'up'},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    logger.error(f"Prometheus health check failed: {resp.status}")
                    return False
                    
            # Check Thanos sidecar if configured
            if config.thanos_sidecar_url:
                async with self.session.get(
                    f"{config.thanos_sidecar_url}/-/healthy",
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status != 200:
                        logger.error(f"Thanos sidecar health check failed: {resp.status}")
                        return False
                        
            return True
            
        except Exception as e:
            logger.error(f"Region validation failed: {e}")
            return False
            
    async def _get_region_status(self, region_id: str, config: RegionConfig) -> Dict[str, Any]:
        """Get detailed status for a region"""
        status = {
            'status': RegionStatus.HEALTHY,
            'targets': 0,
            'alerts': 0,
            'series': 0
        }
        
        try:
            # Get targets
            async with self.session.get(
                f"{config.prometheus_url}/api/v1/targets",
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    active_targets = [t for t in data['data']['activeTargets'] if t['health'] == 'up']
                    status['targets'] = len(active_targets)
                    
                    # Update metrics
                    federation_targets_gauge.labels(region=region_id).set(status['targets'])
                    
                    # Check target health
                    unhealthy_targets = len(data['data']['activeTargets']) - len(active_targets)
                    if unhealthy_targets > 0:
                        status['status'] = RegionStatus.DEGRADED
                        
            # Get alerts
            async with self.session.get(
                f"{config.prometheus_url}/api/v1/alerts",
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    status['alerts'] = len(data['data']['alerts'])
                    
            # Get series count
            async with self.session.get(
                f"{config.prometheus_url}/api/v1/query",
                params={'query': 'prometheus_tsdb_symbol_table_size_bytes'},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data['data']['result']:
                        # Estimate series count from symbol table size
                        symbol_size = float(data['data']['result'][0]['value'][1])
                        status['series'] = int(symbol_size / 100)  # Rough estimate
                        
        except Exception as e:
            logger.error(f"Failed to get region status: {e}")
            status['status'] = RegionStatus.UNHEALTHY
            status['error'] = str(e)
            
        return status
        
    async def _update_federation_config(self):
        """Update Prometheus federation configuration"""
        # Generate global Prometheus configuration
        config = {
            'global': {
                'scrape_interval': settings.PROMETHEUS_SCRAPE_INTERVAL,
                'evaluation_interval': settings.PROMETHEUS_EVALUATION_INTERVAL,
                'external_labels': {
                    'federation': 'global',
                    'cluster': 'platform-q'
                }
            },
            'scrape_configs': self._generate_federation_scrape_configs(),
            'remote_write': [
                {
                    'url': f"{settings.THANOS_QUERY_URL}/api/v1/receive",
                    'queue_config': {
                        'capacity': 10000,
                        'max_shards': 200,
                        'min_shards': 1,
                        'max_samples_per_send': 5000,
                        'batch_send_deadline': '5s',
                        'min_backoff': '30ms',
                        'max_backoff': '100ms'
                    }
                }
            ]
        }
        
        # Write configuration
        config_file = self.config_path / "prometheus.yml"
        async with aiofiles.open(config_file, 'w') as f:
            await f.write(yaml.dump(config, default_flow_style=False))
            
        logger.info("Updated federation configuration")
        
    async def _update_alert_rules(self):
        """Update alert rules configuration"""
        # Group rules by tenant
        rule_groups = {}
        
        for rule in self.alert_rules.values():
            group_name = f"tenant_{rule.tenant_id}" if rule.tenant_id else "global"
            if group_name not in rule_groups:
                rule_groups[group_name] = RuleGroup(
                    name=group_name,
                    interval="30s",
                    rules=[]
                )
            
            # Convert to Prometheus rule format
            prom_rule = {
                'alert': rule.name,
                'expr': rule.expr,
                'for': rule.for_duration,
                'labels': rule.labels,
                'annotations': rule.annotations
            }
            rule_groups[group_name].rules.append(prom_rule)
            
        # Write rules file
        rules_file = self.config_path / "rules.yml"
        rules_config = {'groups': [group.model_dump() for group in rule_groups.values()]}
        
        async with aiofiles.open(rules_file, 'w') as f:
            await f.write(yaml.dump(rules_config, default_flow_style=False))
            
        logger.info("Updated alert rules configuration")
        
    async def _reload_prometheus_config(self):
        """Reload Prometheus configuration"""
        try:
            async with self.session.post(
                f"{settings.PROMETHEUS_GLOBAL_URL}/-/reload",
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    logger.info("Prometheus configuration reloaded successfully")
                else:
                    logger.error(f"Failed to reload Prometheus config: {resp.status}")
        except Exception as e:
            logger.error(f"Failed to reload Prometheus config: {e}")
            
    def _generate_federation_scrape_configs(self) -> List[Dict[str, Any]]:
        """Generate scrape configurations for federation"""
        configs = []
        
        # Add regional Prometheus federation
        for region_id, config in self.regions.items():
            configs.append({
                'job_name': f'federate_{region_id}',
                'scrape_interval': '15s',
                'honor_labels': True,
                'metrics_path': '/federate',
                'params': {
                    'match[]': [
                        '{job=~"platform-.*"}',
                        '{__name__=~"platform_.*"}',
                        'up{job=~"platform-.*"}'
                    ]
                },
                'static_configs': [{
                    'targets': [config.prometheus_url.replace('http://', '').replace('https://', '')],
                    'labels': {
                        'region': region_id,
                        'datacenter': config.consul_datacenter
                    }
                }]
            })
            
        # Add Thanos components
        configs.extend([
            {
                'job_name': 'thanos-query',
                'static_configs': [{
                    'targets': [settings.THANOS_QUERY_URL.replace('http://', '')]
                }]
            },
            {
                'job_name': 'thanos-store',
                'static_configs': [{
                    'targets': [settings.THANOS_STORE_URL.replace('http://', '')]
                }]
            },
            {
                'job_name': 'thanos-compact',
                'static_configs': [{
                    'targets': [settings.THANOS_COMPACT_URL.replace('http://', '')]
                }]
            }
        ])
        
        return configs
        
    async def _load_configuration(self):
        """Load existing configuration from disk"""
        try:
            # Load regions config
            regions_file = self.config_path / "regions.yml"
            if regions_file.exists():
                async with aiofiles.open(regions_file, 'r') as f:
                    content = await f.read()
                    data = yaml.safe_load(content)
                    for region_id, config_data in data.items():
                        self.regions[region_id] = RegionConfig(**config_data)
                        
            # Load alert rules
            rules_file = self.config_path / "rules.yml"
            if rules_file.exists():
                async with aiofiles.open(rules_file, 'r') as f:
                    content = await f.read()
                    data = yaml.safe_load(content)
                    for group in data.get('groups', []):
                        for rule in group.get('rules', []):
                            if 'alert' in rule:
                                alert_rule = AlertRule(
                                    name=rule['alert'],
                                    expr=rule['expr'],
                                    for_duration=rule.get('for', '5m'),
                                    labels=rule.get('labels', {}),
                                    annotations=rule.get('annotations', {})
                                )
                                self.alert_rules[alert_rule.name] = alert_rule
                                
            logger.info(f"Loaded {len(self.regions)} regions and {len(self.alert_rules)} alert rules")
            
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}") 