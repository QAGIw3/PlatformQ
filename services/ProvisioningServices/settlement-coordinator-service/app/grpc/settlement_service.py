"""gRPC Settlement Coordinator Service Implementation"""

import logging
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime
import grpc
from concurrent import futures
import uuid

# Import generated proto files (these will be generated from proto file)
# For now, we'll define the service interface
from app.models.settlement import (
    Settlement, RiskAssessment, ProviderMetrics, 
    SettlementStatus, RiskLevel, ResourceType
)
from app.risk_engines.probabilistic import ProbabilisticRiskEngine
from app.risk_engines.sa_ccr import SACCRRiskEngine
from app.risk_engines.monte_carlo import MonteCarloRiskEngine
from app.clients.cloudkitty import CloudKittyClient
from app.clients.openmeter import OpenMeterClient
from app.cache.ignite_cache import cache_manager
from app.config import settings

logger = logging.getLogger(__name__)


class SettlementCoordinatorService:
    """gRPC service implementation for settlement coordination"""
    
    def __init__(self):
        # Initialize risk engines
        self.probabilistic_engine = ProbabilisticRiskEngine()
        self.sa_ccr_engine = SACCRRiskEngine()
        self.monte_carlo_engine = MonteCarloRiskEngine()
        
        # Initialize clients
        self.cloudkitty_client = None
        self.openmeter_client = None
        
        # Processing queue
        self.processing_queue = asyncio.Queue(maxsize=1000)
        self.workers = []
        
    async def initialize(self):
        """Initialize service connections"""
        # Connect to Ignite
        await cache_manager.connect()
        
        # Initialize clients
        self.cloudkitty_client = CloudKittyClient()
        self.openmeter_client = OpenMeterClient()
        
        # Start worker tasks
        for i in range(settings.settlement_worker_threads):
            worker = asyncio.create_task(self._process_settlements_worker(i))
            self.workers.append(worker)
        
        logger.info("Settlement Coordinator Service initialized")
    
    async def shutdown(self):
        """Cleanup service resources"""
        # Cancel workers
        for worker in self.workers:
            worker.cancel()
        
        # Wait for workers to complete
        await asyncio.gather(*self.workers, return_exceptions=True)
        
        # Disconnect from services
        await cache_manager.disconnect()
        if self.cloudkitty_client:
            await self.cloudkitty_client.__aexit__(None, None, None)
        if self.openmeter_client:
            await self.openmeter_client.__aexit__(None, None, None)
    
    async def ProcessSettlement(
        self, 
        request: Dict[str, Any], 
        context: grpc.ServicerContext
    ) -> Dict[str, Any]:
        """Process a single settlement"""
        try:
            # Create settlement from request
            settlement = Settlement(
                trade_id=request['trade_id'],
                buyer_id=request['buyer_id'],
                seller_id=request['seller_id'],
                provider_id=request['provider_id'],
                resource_type=ResourceType(request['resource_type']),
                quantity=request['quantity'],
                unit_price=request['unit_price'],
                total_value=request['total_value'],
                trade_timestamp=datetime.fromisoformat(request['trade_timestamp']),
                delivery_start=datetime.fromisoformat(request['delivery_start']),
                delivery_end=datetime.fromisoformat(request['delivery_end']),
                metadata=request.get('metadata', {})
            )
            
            # Save to cache
            await cache_manager.save_settlement(settlement)
            
            # Add to processing queue
            await self.processing_queue.put(settlement)
            
            # Track usage in OpenMeter
            await self.openmeter_client.track_usage(
                settlement_id=settlement.id,
                tenant_id=settlement.buyer_id,
                resource_type=settlement.resource_type.value,
                quantity=settlement.quantity,
                metadata={
                    'trade_id': settlement.trade_id,
                    'provider_id': settlement.provider_id
                }
            )
            
            return {
                'settlement_id': settlement.id,
                'status': settlement.status.value,
                'message': 'Settlement queued for processing'
            }
            
        except Exception as e:
            logger.error(f"Error processing settlement: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return {}
    
    async def CalculateRisk(
        self, 
        request: Dict[str, Any], 
        context: grpc.ServicerContext
    ) -> Dict[str, Any]:
        """Calculate risk for a settlement"""
        try:
            settlement_id = request['settlement_id']
            risk_models = request.get('risk_models', ['all'])
            
            # Get settlement from cache
            settlement = await cache_manager.get_settlement(settlement_id)
            if not settlement:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Settlement {settlement_id} not found")
                return {}
            
            # Get provider metrics
            provider_metrics = await self._get_provider_metrics(settlement.provider_id)
            
            # Check cache for existing risk assessment
            cached_assessment = await cache_manager.get_risk_assessment(settlement_id)
            if cached_assessment and not request.get('force_recalculate', False):
                return {
                    'settlement_id': settlement_id,
                    'risk_score': cached_assessment.final_score,
                    'risk_level': cached_assessment.risk_level.value,
                    'cached': True,
                    'assessment': cached_assessment.model_dump()
                }
            
            # Calculate risk using requested models
            risk_results = {}
            
            if 'probabilistic' in risk_models or 'all' in risk_models:
                prob_result = await self.probabilistic_engine.calculate_risk(
                    settlement, provider_metrics
                )
                risk_results['probabilistic'] = prob_result
            
            if 'sa_ccr' in risk_models or 'all' in risk_models:
                sa_ccr_result = await self.sa_ccr_engine.calculate_risk(
                    settlement, provider_metrics
                )
                risk_results['sa_ccr'] = sa_ccr_result
            
            if 'monte_carlo' in risk_models or 'all' in risk_models:
                mc_result = await self.monte_carlo_engine.calculate_risk(
                    settlement, provider_metrics
                )
                risk_results['monte_carlo'] = mc_result
            
            # Combine results
            final_assessment = self._combine_risk_assessments(
                settlement_id, risk_results, provider_metrics
            )
            
            # Cache the assessment
            await cache_manager.save_risk_assessment(final_assessment)
            
            return {
                'settlement_id': settlement_id,
                'risk_score': final_assessment.final_score,
                'risk_level': final_assessment.risk_level.value,
                'cached': False,
                'assessment': final_assessment.model_dump()
            }
            
        except Exception as e:
            logger.error(f"Error calculating risk: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return {}
    
    async def GetSettlementStatus(
        self, 
        request: Dict[str, Any], 
        context: grpc.ServicerContext
    ) -> Dict[str, Any]:
        """Get settlement status"""
        try:
            settlement_id = request['settlement_id']
            
            # Get from cache
            settlement = await cache_manager.get_settlement(settlement_id)
            if not settlement:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Settlement {settlement_id} not found")
                return {}
            
            # Get risk assessment if available
            risk_assessment = await cache_manager.get_risk_assessment(settlement_id)
            
            # Get billing status from CloudKitty
            billing_status = None
            if settlement.billing_id:
                billing_status = await self.cloudkitty_client.get_rated_data(
                    settlement_id
                )
            
            return {
                'settlement_id': settlement_id,
                'status': settlement.status.value,
                'trade_id': settlement.trade_id,
                'settlement_timestamp': settlement.settlement_timestamp.isoformat() 
                    if settlement.settlement_timestamp else None,
                'risk_score': risk_assessment.final_score if risk_assessment else None,
                'risk_level': risk_assessment.risk_level.value if risk_assessment else None,
                'billing_status': billing_status,
                'escrow_amount': settlement.escrow_amount,
                'escrow_released': settlement.escrow_released
            }
            
        except Exception as e:
            logger.error(f"Error getting settlement status: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return {}
    
    async def StreamSettlements(
        self, 
        request: Dict[str, Any], 
        context: grpc.ServicerContext
    ):
        """Stream settlement updates"""
        try:
            # Create session
            session_id = str(uuid.uuid4())
            filters = request.get('filters', {})
            
            # Save session data
            await cache_manager.save_session_data(session_id, {
                'filters': filters,
                'started_at': datetime.utcnow().isoformat()
            })
            
            # Stream updates
            while not context.is_active():
                # Get settlements matching filters
                settlements = await self._get_filtered_settlements(filters)
                
                for settlement in settlements:
                    # Get latest status
                    risk_assessment = await cache_manager.get_risk_assessment(
                        settlement.id
                    )
                    
                    update = {
                        'settlement_id': settlement.id,
                        'status': settlement.status.value,
                        'timestamp': datetime.utcnow().isoformat(),
                        'risk_score': risk_assessment.final_score 
                            if risk_assessment else None,
                        'risk_level': risk_assessment.risk_level.value 
                            if risk_assessment else None
                    }
                    
                    yield update
                
                # Wait before next update
                await asyncio.sleep(1)
            
        except Exception as e:
            logger.error(f"Error in settlement stream: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
        finally:
            # Cleanup session
            await cache_manager.delete_session_data(session_id)
    
    async def BatchProcessSettlements(
        self, 
        request: Dict[str, Any], 
        context: grpc.ServicerContext
    ) -> Dict[str, Any]:
        """Process multiple settlements in batch"""
        try:
            settlements_data = request['settlements']
            
            settlements = []
            for data in settlements_data:
                settlement = Settlement(
                    trade_id=data['trade_id'],
                    buyer_id=data['buyer_id'],
                    seller_id=data['seller_id'],
                    provider_id=data['provider_id'],
                    resource_type=ResourceType(data['resource_type']),
                    quantity=data['quantity'],
                    unit_price=data['unit_price'],
                    total_value=data['total_value'],
                    trade_timestamp=datetime.fromisoformat(data['trade_timestamp']),
                    delivery_start=datetime.fromisoformat(data['delivery_start']),
                    delivery_end=datetime.fromisoformat(data['delivery_end']),
                    metadata=data.get('metadata', {})
                )
                settlements.append(settlement)
            
            # Save batch to cache
            await cache_manager.save_settlements_batch(settlements)
            
            # Add all to processing queue
            for settlement in settlements:
                await self.processing_queue.put(settlement)
            
            # Track batch usage
            for settlement in settlements:
                await self.openmeter_client.track_usage(
                    settlement_id=settlement.id,
                    tenant_id=settlement.buyer_id,
                    resource_type=settlement.resource_type.value,
                    quantity=settlement.quantity
                )
            
            return {
                'batch_id': str(uuid.uuid4()),
                'settlements_count': len(settlements),
                'total_value': sum(s.total_value for s in settlements),
                'status': 'queued'
            }
            
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return {}
    
    async def GetRiskMetrics(
        self, 
        request: Dict[str, Any], 
        context: grpc.ServicerContext
    ) -> Dict[str, Any]:
        """Get aggregated risk metrics"""
        try:
            provider_id = request.get('provider_id')
            start_time = datetime.fromisoformat(request['start_time'])
            end_time = datetime.fromisoformat(request['end_time'])
            
            # Get settlements in time range
            settlements = await self._get_settlements_in_range(
                provider_id, start_time, end_time
            )
            
            # Calculate aggregate metrics
            total_value = sum(s.total_value for s in settlements)
            
            risk_scores = []
            risk_levels = {'low': 0, 'medium': 0, 'high': 0, 'critical': 0}
            
            for settlement in settlements:
                assessment = await cache_manager.get_risk_assessment(settlement.id)
                if assessment:
                    risk_scores.append(assessment.final_score)
                    risk_levels[assessment.risk_level.value] += 1
            
            avg_risk = sum(risk_scores) / len(risk_scores) if risk_scores else 0
            
            return {
                'provider_id': provider_id,
                'period': {
                    'start': start_time.isoformat(),
                    'end': end_time.isoformat()
                },
                'settlements_count': len(settlements),
                'total_value': total_value,
                'average_risk_score': avg_risk,
                'risk_distribution': risk_levels,
                'high_risk_percentage': (
                    (risk_levels['high'] + risk_levels['critical']) / 
                    len(settlements) * 100 if settlements else 0
                )
            }
            
        except Exception as e:
            logger.error(f"Error getting risk metrics: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return {}
    
    # Helper methods
    
    async def _process_settlements_worker(self, worker_id: int):
        """Worker to process settlements from queue"""
        logger.info(f"Settlement worker {worker_id} started")
        
        while True:
            try:
                # Get settlement from queue
                settlement = await self.processing_queue.get()
                
                # Update status
                await cache_manager.update_settlement_status(
                    settlement.id,
                    SettlementStatus.PROCESSING.value
                )
                
                # Get provider metrics
                provider_metrics = await self._get_provider_metrics(
                    settlement.provider_id
                )
                
                # Calculate risk
                risk_results = {}
                
                # Always calculate probabilistic (fast)
                prob_result = await self.probabilistic_engine.calculate_risk(
                    settlement, provider_metrics
                )
                risk_results['probabilistic'] = prob_result
                
                # Calculate SA-CCR for medium/high value settlements
                if settlement.total_value > 1000:
                    sa_ccr_result = await self.sa_ccr_engine.calculate_risk(
                        settlement, provider_metrics
                    )
                    risk_results['sa_ccr'] = sa_ccr_result
                
                # Calculate Monte Carlo for high value settlements
                if settlement.total_value > 10000:
                    mc_result = await self.monte_carlo_engine.calculate_risk(
                        settlement, provider_metrics
                    )
                    risk_results['monte_carlo'] = mc_result
                
                # Create risk assessment
                assessment = self._combine_risk_assessments(
                    settlement.id, risk_results, provider_metrics
                )
                
                # Save risk assessment
                await cache_manager.save_risk_assessment(assessment)
                
                # Update settlement with risk info
                settlement.risk_score = assessment.final_score
                settlement.risk_level = assessment.risk_level
                settlement.risk_factors = assessment.risk_breakdown
                
                # Apply escrow if needed
                if assessment.require_escrow:
                    settlement.escrow_amount = (
                        settlement.total_value * assessment.escrow_percentage
                    )
                
                # Create billing entry
                billing_result = await self.cloudkitty_client.create_rating_entry(
                    settlement_id=settlement.id,
                    resource_type=settlement.resource_type.value,
                    quantity=settlement.quantity,
                    unit_price=settlement.unit_price * (1 + assessment.risk_premium),
                    start_time=settlement.delivery_start,
                    end_time=settlement.delivery_end,
                    metadata={
                        'risk_score': assessment.final_score,
                        'risk_level': assessment.risk_level.value,
                        'escrow_amount': settlement.escrow_amount
                    }
                )
                
                settlement.billing_id = billing_result.get('rating_id')
                
                # Update settlement status
                await cache_manager.update_settlement_status(
                    settlement.id,
                    SettlementStatus.COMPLETED.value,
                    datetime.utcnow()
                )
                
                # Save updated settlement
                await cache_manager.save_settlement(settlement)
                
                # Emit event
                await self._emit_settlement_event(settlement, assessment)
                
                logger.info(
                    f"Worker {worker_id} completed settlement {settlement.id}"
                )
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    f"Worker {worker_id} error processing settlement: {e}"
                )
                
                # Update status to failed
                if 'settlement' in locals():
                    await cache_manager.update_settlement_status(
                        settlement.id,
                        SettlementStatus.FAILED.value
                    )
    
    async def _get_provider_metrics(
        self, 
        provider_id: str
    ) -> ProviderMetrics:
        """Get or create provider metrics"""
        # Check cache first
        metrics = await cache_manager.get_provider_metrics(provider_id)
        
        if not metrics:
            # Create default metrics for new provider
            metrics = ProviderMetrics(
                provider_id=provider_id,
                measurement_period_days=30,
                uptime_percentage=0.99,  # Default 99%
                average_response_time_ms=100,
                total_incidents=0,
                critical_incidents=0,
                total_capacity={"cpu": 1000, "gpu": 100, "memory": 4000},
                utilized_capacity={"cpu": 0, "gpu": 0, "memory": 0},
                overcommit_ratio=1.0,
                completed_settlements=0,
                failed_settlements=0,
                disputed_settlements=0,
                average_settlement_time_hours=1.0,
                total_value_settled=0.0,
                average_transaction_value=0.0,
                payment_default_rate=0.0
            )
            
            await cache_manager.save_provider_metrics(metrics)
        
        return metrics
    
    def _combine_risk_assessments(
        self,
        settlement_id: str,
        risk_results: Dict[str, Dict[str, Any]],
        provider_metrics: ProviderMetrics
    ) -> RiskAssessment:
        """Combine results from multiple risk engines"""
        
        # Extract scores
        scores = []
        recommendations = {
            'require_escrow': False,
            'escrow_percentage': 0.0,
            'risk_premium': 0.0,
            'mitigation_strategies': []
        }
        
        prob_score = None
        sa_ccr_exposure = None
        mc_var = None
        mc_cvar = None
        
        if 'probabilistic' in risk_results:
            prob = risk_results['probabilistic']
            prob_score = prob['risk_score']
            scores.append(prob_score)
            self._merge_recommendations(recommendations, prob['recommendations'])
        
        if 'sa_ccr' in risk_results:
            sa_ccr = risk_results['sa_ccr']
            sa_ccr_exposure = sa_ccr['exposure']
            scores.append(sa_ccr['risk_score'])
            self._merge_recommendations(recommendations, sa_ccr['recommendations'])
        
        if 'monte_carlo' in risk_results:
            mc = risk_results['monte_carlo']
            mc_var = mc['value_at_risk']
            mc_cvar = mc['conditional_value_at_risk']
            scores.append(mc['risk_score'])
            self._merge_recommendations(recommendations, mc['recommendations'])
        
        # Calculate final score (weighted average)
        if len(scores) == 1:
            final_score = scores[0]
        elif len(scores) == 2:
            final_score = scores[0] * 0.4 + scores[1] * 0.6
        else:
            # All three engines
            final_score = prob_score * 0.3 + scores[1] * 0.3 + scores[2] * 0.4
        
        # Determine risk level
        if final_score < settings.risk_threshold_low:
            risk_level = RiskLevel.LOW
        elif final_score < settings.risk_threshold_medium:
            risk_level = RiskLevel.MEDIUM
        elif final_score < settings.risk_threshold_high:
            risk_level = RiskLevel.HIGH
        else:
            risk_level = RiskLevel.CRITICAL
        
        return RiskAssessment(
            settlement_id=settlement_id,
            probabilistic_score=prob_score,
            sa_ccr_exposure=sa_ccr_exposure,
            monte_carlo_var=mc_var,
            monte_carlo_cvar=mc_cvar,
            final_score=final_score,
            risk_level=risk_level,
            confidence_level=settings.risk_confidence_level,
            sla_uptime=provider_metrics.uptime_percentage,
            provider_reliability_score=self._calculate_reliability_score(
                provider_metrics
            ),
            require_escrow=recommendations['require_escrow'],
            escrow_percentage=recommendations['escrow_percentage'],
            risk_premium=recommendations['risk_premium'],
            diversification_needed='diversification_needed' in recommendations,
            risk_breakdown={
                'engines_used': list(risk_results.keys()),
                'individual_scores': {k: v.get('risk_score') 
                                    for k, v in risk_results.items()}
            },
            mitigation_strategies=recommendations['mitigation_strategies']
        )
    
    def _merge_recommendations(
        self, 
        target: Dict[str, Any], 
        source: Dict[str, Any]
    ):
        """Merge recommendations from different engines"""
        # Take maximum values for financial recommendations
        target['require_escrow'] = target['require_escrow'] or source.get(
            'require_escrow', False
        )
        target['escrow_percentage'] = max(
            target['escrow_percentage'],
            source.get('escrow_percentage', 0.0)
        )
        target['risk_premium'] = max(
            target['risk_premium'],
            source.get('risk_premium', 0.0)
        )
        
        # Merge strategies
        for strategy in source.get('mitigation_strategies', []):
            if strategy not in target['mitigation_strategies']:
                target['mitigation_strategies'].append(strategy)
        
        # Copy other fields
        for key, value in source.items():
            if key not in target and key not in [
                'require_escrow', 'escrow_percentage', 
                'risk_premium', 'mitigation_strategies'
            ]:
                target[key] = value
    
    def _calculate_reliability_score(
        self, 
        metrics: ProviderMetrics
    ) -> float:
        """Calculate provider reliability score"""
        total = (
            metrics.completed_settlements + 
            metrics.failed_settlements + 
            metrics.disputed_settlements
        )
        
        if total == 0:
            return 0.5  # Default for new providers
        
        completion_rate = metrics.completed_settlements / total
        dispute_rate = metrics.disputed_settlements / total
        
        # Weighted score
        score = completion_rate * 0.7 + (1 - dispute_rate) * 0.3
        
        # Penalty for incidents
        if metrics.critical_incidents > 0:
            penalty = min(metrics.critical_incidents * 0.1, 0.3)
            score = max(score - penalty, 0)
        
        return score
    
    async def _get_filtered_settlements(
        self, 
        filters: Dict[str, Any]
    ) -> List[Settlement]:
        """Get settlements matching filters"""
        # For now, get by status
        status = filters.get('status', 'pending')
        return await cache_manager.get_settlements_by_status(status)
    
    async def _get_settlements_in_range(
        self,
        provider_id: Optional[str],
        start_time: datetime,
        end_time: datetime
    ) -> List[Settlement]:
        """Get settlements in time range"""
        # This would query Ignite with SQL
        # For now, return empty list
        return []
    
    async def _emit_settlement_event(
        self,
        settlement: Settlement,
        assessment: RiskAssessment
    ):
        """Emit settlement completion event"""
        # Would publish to Pulsar
        logger.info(
            f"Settlement {settlement.id} completed with risk score "
            f"{assessment.final_score:.2f}"
        ) 