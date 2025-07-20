"""
Trading Risk Network Analysis

Implements graph-based risk propagation for trading systems:
- Trader relationship networks
- Risk contagion modeling
- Systemic risk detection
- Copy trading cascade analysis
"""

import logging
from typing import Dict, Any, List, Optional, Set, Tuple
from datetime import datetime, timedelta
from decimal import Decimal
from dataclasses import dataclass
from enum import Enum
import networkx as nx
import numpy as np
from collections import defaultdict
import asyncio

from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import P, T

logger = logging.getLogger(__name__)


class RiskPropagationType(Enum):
    """Types of risk propagation"""
    DIRECT_EXPOSURE = "direct_exposure"
    COPY_TRADING = "copy_trading"
    CORRELATED_POSITIONS = "correlated_positions"
    LIQUIDITY_LINKAGE = "liquidity_linkage"
    MARGIN_CASCADE = "margin_cascade"


class TraderRiskLevel(Enum):
    """Trader risk levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class TraderNode:
    """Trader node in risk network"""
    trader_id: str
    risk_score: float
    exposure: Decimal
    leverage: float
    margin_utilization: float
    position_count: int
    liquidity: Decimal
    last_update: datetime
    metadata: Dict[str, Any]


@dataclass
class RiskEdge:
    """Risk relationship edge"""
    from_trader: str
    to_trader: str
    relationship_type: RiskPropagationType
    strength: float  # 0-1 indicating relationship strength
    exposure_amount: Decimal
    last_interaction: datetime
    metadata: Dict[str, Any]


@dataclass
class RiskPropagationResult:
    """Result of risk propagation analysis"""
    affected_traders: List[str]
    total_exposure: Decimal
    cascade_depth: int
    systemic_risk_score: float
    mitigation_actions: List[Dict[str, Any]]
    propagation_paths: List[List[str]]


class TradingRiskNetwork:
    """Analyzes and manages trading risk propagation networks"""
    
    def __init__(self, gremlin_url: str, event_publisher=None):
        self.gremlin_url = gremlin_url
        self.event_publisher = event_publisher
        self.g = None
        self._connect()
        
        # Risk thresholds
        self.risk_thresholds = {
            TraderRiskLevel.LOW: 0.3,
            TraderRiskLevel.MEDIUM: 0.5,
            TraderRiskLevel.HIGH: 0.7,
            TraderRiskLevel.CRITICAL: 0.9
        }
        
        # Propagation parameters
        self.propagation_damping = 0.85  # Risk reduces as it propagates
        self.min_propagation_strength = 0.1  # Minimum strength to continue propagation
        
    def _connect(self):
        """Connect to JanusGraph"""
        from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
        from gremlin_python.structure.graph import Graph
        
        self.g = Graph().traversal().withRemote(
            DriverRemoteConnection(self.gremlin_url, 'g')
        )
    
    async def update_trader_risk(self, trader_id: str, risk_metrics: Dict[str, Any]):
        """Update trader risk profile in graph"""
        try:
            # Update or create trader vertex
            vertex = self.g.V().has('trader', 'trader_id', trader_id).fold().coalesce(
                __.unfold(),
                __.addV('trader').property('trader_id', trader_id)
            )
            
            # Update properties
            for key, value in risk_metrics.items():
                vertex = vertex.property(key, value)
            
            vertex.property('last_update', datetime.utcnow().isoformat()).iterate()
            
            # Check if risk level changed significantly
            if 'risk_score' in risk_metrics:
                await self._check_risk_threshold_breach(trader_id, risk_metrics['risk_score'])
            
            logger.info(f"Updated risk profile for trader {trader_id}")
            
        except Exception as e:
            logger.error(f"Error updating trader risk: {e}")
    
    async def add_trading_relationship(self, 
                                     from_trader: str,
                                     to_trader: str,
                                     relationship_type: RiskPropagationType,
                                     metadata: Dict[str, Any]):
        """Add or update trading relationship edge"""
        try:
            # Ensure both traders exist
            self.g.V().has('trader', 'trader_id', from_trader).fold().coalesce(
                __.unfold(),
                __.addV('trader').property('trader_id', from_trader)
            ).iterate()
            
            self.g.V().has('trader', 'trader_id', to_trader).fold().coalesce(
                __.unfold(),
                __.addV('trader').property('trader_id', to_trader)
            ).iterate()
            
            # Add or update edge
            edge = self.g.V().has('trader', 'trader_id', from_trader).as_('from').V().has('trader', 'trader_id', to_trader).as_('to').coalesce(
                __.inE(relationship_type.value).where(__.outV().as_('from')),
                __.addE(relationship_type.value).from_('from').to('to')
            )
            
            # Update edge properties
            for key, value in metadata.items():
                edge = edge.property(key, value)
            
            edge.property('last_update', datetime.utcnow().isoformat()).iterate()
            
            logger.info(f"Added {relationship_type.value} relationship: {from_trader} -> {to_trader}")
            
        except Exception as e:
            logger.error(f"Error adding trading relationship: {e}")
    
    async def analyze_risk_propagation(self, 
                                     source_trader: str,
                                     initial_risk_event: Dict[str, Any]) -> RiskPropagationResult:
        """Analyze how risk propagates from a source trader through the network"""
        try:
            affected_traders = set()
            propagation_queue = [(source_trader, 1.0, [source_trader])]  # (trader, risk_strength, path)
            visited = set()
            propagation_paths = []
            total_exposure = Decimal('0')
            max_depth = 0
            
            while propagation_queue:
                current_trader, risk_strength, path = propagation_queue.pop(0)
                
                if current_trader in visited or risk_strength < self.min_propagation_strength:
                    continue
                    
                visited.add(current_trader)
                affected_traders.add(current_trader)
                max_depth = max(max_depth, len(path) - 1)
                
                # Get trader's current state
                trader_data = self._get_trader_data(current_trader)
                if trader_data:
                    exposure = Decimal(str(trader_data.get('exposure', 0)))
                    total_exposure += exposure * Decimal(str(risk_strength))
                
                # Find connected traders
                connections = self._get_risk_connections(current_trader)
                
                for conn in connections:
                    next_trader = conn['trader_id']
                    edge_strength = conn['strength']
                    edge_type = conn['type']
                    
                    # Calculate propagated risk strength
                    propagated_strength = risk_strength * edge_strength * self.propagation_damping
                    
                    # Apply type-specific propagation rules
                    propagated_strength = self._apply_propagation_rules(
                        propagated_strength,
                        edge_type,
                        initial_risk_event
                    )
                    
                    if propagated_strength >= self.min_propagation_strength and next_trader not in visited:
                        new_path = path + [next_trader]
                        propagation_queue.append((next_trader, propagated_strength, new_path))
                        
                        if propagated_strength > 0.5:  # Significant propagation
                            propagation_paths.append(new_path)
            
            # Calculate systemic risk score
            systemic_risk = self._calculate_systemic_risk(
                len(affected_traders),
                float(total_exposure),
                max_depth
            )
            
            # Generate mitigation actions
            mitigation_actions = self._generate_mitigation_actions(
                affected_traders,
                initial_risk_event,
                systemic_risk
            )
            
            result = RiskPropagationResult(
                affected_traders=list(affected_traders),
                total_exposure=total_exposure,
                cascade_depth=max_depth,
                systemic_risk_score=systemic_risk,
                mitigation_actions=mitigation_actions,
                propagation_paths=propagation_paths[:10]  # Top 10 paths
            )
            
            # Publish risk propagation event
            if self.event_publisher and systemic_risk > 0.7:
                await self._publish_systemic_risk_alert(result)
            
            return result
            
        except Exception as e:
            logger.error(f"Error analyzing risk propagation: {e}")
            return RiskPropagationResult(
                affected_traders=[],
                total_exposure=Decimal('0'),
                cascade_depth=0,
                systemic_risk_score=0,
                mitigation_actions=[],
                propagation_paths=[]
            )
    
    async def detect_risk_clusters(self) -> List[Set[str]]:
        """Detect clusters of highly interconnected risky traders"""
        try:
            # Get all high-risk traders
            high_risk_traders = self.g.V().has('trader').has('risk_score', P.gte(0.7)).values('trader_id').toList()
            
            # Build subgraph of high-risk traders
            risk_graph = nx.Graph()
            
            for trader in high_risk_traders:
                connections = self._get_risk_connections(trader)
                for conn in connections:
                    if conn['trader_id'] in high_risk_traders:
                        risk_graph.add_edge(trader, conn['trader_id'], weight=conn['strength'])
            
            # Find communities using Louvain algorithm
            from networkx.algorithms import community
            communities = community.louvain_communities(risk_graph, weight='weight')
            
            # Filter significant clusters
            significant_clusters = []
            for cluster in communities:
                if len(cluster) >= 3:  # At least 3 traders
                    cluster_risk = self._calculate_cluster_risk(cluster)
                    if cluster_risk > 0.6:
                        significant_clusters.append(cluster)
            
            logger.info(f"Detected {len(significant_clusters)} significant risk clusters")
            return significant_clusters
            
        except Exception as e:
            logger.error(f"Error detecting risk clusters: {e}")
            return []
    
    async def calculate_trader_systemic_importance(self, trader_id: str) -> float:
        """Calculate how systemically important a trader is to the network"""
        try:
            # Get trader's connections
            in_degree = self.g.V().has('trader', 'trader_id', trader_id).inE().count().next()
            out_degree = self.g.V().has('trader', 'trader_id', trader_id).outE().count().next()
            
            # Get trader's metrics
            trader_data = self._get_trader_data(trader_id)
            if not trader_data:
                return 0.0
            
            exposure = float(trader_data.get('exposure', 0))
            risk_score = trader_data.get('risk_score', 0)
            
            # Calculate centrality metrics
            centrality_score = self._calculate_centrality_score(trader_id)
            
            # Combine factors
            connectivity_factor = (in_degree + out_degree) / 100  # Normalize
            exposure_factor = min(exposure / 1000000, 1.0)  # Cap at 1M
            risk_factor = risk_score
            
            systemic_importance = (
                0.3 * connectivity_factor +
                0.3 * exposure_factor +
                0.2 * risk_factor +
                0.2 * centrality_score
            )
            
            return min(systemic_importance, 1.0)
            
        except Exception as e:
            logger.error(f"Error calculating systemic importance: {e}")
            return 0.0
    
    async def simulate_cascade_failure(self, 
                                     failing_trader: str,
                                     failure_type: str = "liquidation") -> Dict[str, Any]:
        """Simulate cascade effects of a trader failure"""
        try:
            simulation_results = {
                'initial_failure': failing_trader,
                'failure_type': failure_type,
                'waves': [],
                'total_affected': 0,
                'total_losses': Decimal('0'),
                'simulation_timestamp': datetime.utcnow()
            }
            
            # Wave 0: Initial failure
            current_wave = {failing_trader}
            wave_number = 0
            all_affected = set()
            
            while current_wave and wave_number < 10:  # Max 10 waves
                next_wave = set()
                wave_losses = Decimal('0')
                wave_data = {
                    'wave_number': wave_number,
                    'affected_traders': list(current_wave),
                    'losses': Decimal('0'),
                    'triggers': []
                }
                
                for trader in current_wave:
                    all_affected.add(trader)
                    
                    # Get trader's exposure and connections
                    trader_data = self._get_trader_data(trader)
                    if not trader_data:
                        continue
                    
                    trader_exposure = Decimal(str(trader_data.get('exposure', 0)))
                    wave_losses += trader_exposure * Decimal('0.3')  # Assume 30% loss
                    
                    # Find traders who might be affected
                    connections = self._get_risk_connections(trader)
                    
                    for conn in connections:
                        connected_trader = conn['trader_id']
                        if connected_trader in all_affected:
                            continue
                        
                        # Check if connected trader would fail
                        if self._would_trader_fail(connected_trader, conn, failure_type):
                            next_wave.add(connected_trader)
                            wave_data['triggers'].append({
                                'from': trader,
                                'to': connected_trader,
                                'reason': conn['type']
                            })
                
                wave_data['losses'] = wave_losses
                simulation_results['waves'].append(wave_data)
                simulation_results['total_losses'] += wave_losses
                
                current_wave = next_wave
                wave_number += 1
            
            simulation_results['total_affected'] = len(all_affected)
            
            # Generate recommendations
            simulation_results['recommendations'] = self._generate_cascade_mitigation_recommendations(
                simulation_results
            )
            
            return simulation_results
            
        except Exception as e:
            logger.error(f"Error simulating cascade failure: {e}")
            return {
                'error': str(e),
                'initial_failure': failing_trader,
                'simulation_failed': True
            }
    
    # Helper methods
    def _get_trader_data(self, trader_id: str) -> Optional[Dict[str, Any]]:
        """Get trader vertex data"""
        try:
            trader = self.g.V().has('trader', 'trader_id', trader_id).valueMap().next()
            return {k: v[0] if isinstance(v, list) else v for k, v in trader.items()}
        except:
            return None
    
    def _get_risk_connections(self, trader_id: str) -> List[Dict[str, Any]]:
        """Get risk-relevant connections for a trader"""
        connections = []
        
        # Get incoming connections (traders who follow/copy this trader)
        in_edges = self.g.V().has('trader', 'trader_id', trader_id).inE().project(
            'trader_id', 'type', 'strength', 'exposure'
        ).by(__.outV().values('trader_id')).by(__.label()).by(
            __.values('strength').fold().coalesce(__.unfold(), __.constant(0.5))
        ).by(
            __.values('exposure_amount').fold().coalesce(__.unfold(), __.constant(0))
        ).toList()
        
        # Get outgoing connections (traders this trader follows/depends on)
        out_edges = self.g.V().has('trader', 'trader_id', trader_id).outE().project(
            'trader_id', 'type', 'strength', 'exposure'
        ).by(__.inV().values('trader_id')).by(__.label()).by(
            __.values('strength').fold().coalesce(__.unfold(), __.constant(0.5))
        ).by(
            __.values('exposure_amount').fold().coalesce(__.unfold(), __.constant(0))
        ).toList()
        
        connections.extend(in_edges)
        connections.extend(out_edges)
        
        return connections
    
    def _apply_propagation_rules(self, 
                               strength: float,
                               edge_type: str,
                               risk_event: Dict[str, Any]) -> float:
        """Apply type-specific propagation rules"""
        event_type = risk_event.get('type', 'default')
        
        # Copy trading has strongest propagation
        if edge_type == RiskPropagationType.COPY_TRADING.value:
            return strength * 1.2
        
        # Liquidation events propagate strongly through margin relationships
        elif event_type == 'liquidation' and edge_type == RiskPropagationType.MARGIN_CASCADE.value:
            return strength * 1.5
        
        # Correlated positions propagate based on market conditions
        elif edge_type == RiskPropagationType.CORRELATED_POSITIONS.value:
            market_volatility = risk_event.get('market_volatility', 0.5)
            return strength * (1 + market_volatility)
        
        return strength
    
    def _calculate_systemic_risk(self, 
                               affected_count: int,
                               total_exposure: float,
                               cascade_depth: int) -> float:
        """Calculate overall systemic risk score"""
        # Normalize factors
        count_factor = min(affected_count / 100, 1.0)  # Cap at 100 traders
        exposure_factor = min(total_exposure / 10000000, 1.0)  # Cap at 10M
        depth_factor = min(cascade_depth / 5, 1.0)  # Cap at depth 5
        
        # Weighted combination
        systemic_risk = (
            0.4 * count_factor +
            0.4 * exposure_factor +
            0.2 * depth_factor
        )
        
        return min(systemic_risk, 1.0)
    
    def _generate_mitigation_actions(self,
                                   affected_traders: Set[str],
                                   risk_event: Dict[str, Any],
                                   systemic_risk: float) -> List[Dict[str, Any]]:
        """Generate risk mitigation recommendations"""
        actions = []
        
        if systemic_risk > 0.8:
            actions.append({
                'action': 'HALT_TRADING',
                'scope': 'system-wide',
                'duration': '15 minutes',
                'reason': 'Extreme systemic risk detected'
            })
        
        if systemic_risk > 0.6:
            actions.append({
                'action': 'INCREASE_MARGINS',
                'scope': list(affected_traders)[:20],  # Top 20 affected
                'amount': '50%',
                'reason': 'Risk cascade prevention'
            })
        
        if len(affected_traders) > 50:
            actions.append({
                'action': 'RESTRICT_POSITION_INCREASES',
                'scope': 'all_traders',
                'duration': '1 hour',
                'reason': 'Large-scale risk event'
            })
        
        # Specific actions based on event type
        event_type = risk_event.get('type')
        if event_type == 'liquidation':
            actions.append({
                'action': 'REVIEW_LIQUIDATION_THRESHOLDS',
                'scope': 'risk_management',
                'urgency': 'immediate'
            })
        
        return actions
    
    def _calculate_cluster_risk(self, cluster: Set[str]) -> float:
        """Calculate risk score for a cluster of traders"""
        if not cluster:
            return 0.0
        
        total_risk = 0
        for trader in cluster:
            trader_data = self._get_trader_data(trader)
            if trader_data:
                total_risk += trader_data.get('risk_score', 0)
        
        return total_risk / len(cluster)
    
    def _calculate_centrality_score(self, trader_id: str) -> float:
        """Calculate network centrality score for trader"""
        try:
            # Simple PageRank-style calculation
            # In production, would use more sophisticated algorithm
            connections = len(self._get_risk_connections(trader_id))
            return min(connections / 50, 1.0)  # Normalize to 0-1
        except:
            return 0.0
    
    def _would_trader_fail(self, 
                         trader_id: str,
                         connection: Dict[str, Any],
                         failure_type: str) -> bool:
        """Determine if a trader would fail given a connection failure"""
        trader_data = self._get_trader_data(trader_id)
        if not trader_data:
            return False
        
        margin_util = trader_data.get('margin_utilization', 0)
        risk_score = trader_data.get('risk_score', 0)
        connection_strength = connection.get('strength', 0)
        
        # Different failure types have different contagion rules
        if failure_type == 'liquidation':
            # High margin utilization makes trader vulnerable
            failure_probability = margin_util * connection_strength
        elif failure_type == 'default':
            # High risk score and strong connection increase failure chance
            failure_probability = risk_score * connection_strength * 1.2
        else:
            failure_probability = risk_score * connection_strength
        
        # Random component for simulation
        import random
        return random.random() < failure_probability
    
    def _generate_cascade_mitigation_recommendations(self, 
                                                   simulation_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate recommendations based on cascade simulation"""
        recommendations = []
        
        total_affected = simulation_results['total_affected']
        total_losses = simulation_results['total_losses']
        
        if total_affected > 100:
            recommendations.append({
                'type': 'CIRCUIT_BREAKER',
                'action': 'Implement automatic trading halts when cascade risk exceeds threshold',
                'priority': 'HIGH'
            })
        
        if total_losses > 1000000:
            recommendations.append({
                'type': 'CAPITAL_BUFFER',
                'action': 'Increase minimum capital requirements for systemic traders',
                'priority': 'HIGH'
            })
        
        if len(simulation_results['waves']) > 3:
            recommendations.append({
                'type': 'EXPOSURE_LIMITS',
                'action': 'Implement position limits to reduce interconnectedness',
                'priority': 'MEDIUM'
            })
        
        return recommendations
    
    async def _check_risk_threshold_breach(self, trader_id: str, risk_score: float):
        """Check if trader breached risk threshold and alert if needed"""
        for level, threshold in self.risk_thresholds.items():
            if risk_score >= threshold:
                if self.event_publisher:
                    await self.event_publisher.publish_event({
                        'event_type': 'risk_threshold_breach',
                        'trader_id': trader_id,
                        'risk_level': level.value,
                        'risk_score': risk_score,
                        'threshold': threshold,
                        'timestamp': datetime.utcnow().isoformat()
                    })
                break
    
    async def _publish_systemic_risk_alert(self, result: RiskPropagationResult):
        """Publish systemic risk alert event"""
        if self.event_publisher:
            await self.event_publisher.publish_event({
                'event_type': 'systemic_risk_alert',
                'affected_traders': result.affected_traders[:50],  # Limit size
                'total_affected': len(result.affected_traders),
                'total_exposure': str(result.total_exposure),
                'cascade_depth': result.cascade_depth,
                'systemic_risk_score': result.systemic_risk_score,
                'timestamp': datetime.utcnow().isoformat(),
                'mitigation_required': True
            }) 