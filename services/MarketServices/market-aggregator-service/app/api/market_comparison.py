"""
Market Comparison and Workload Template API Routes
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional, Dict
from datetime import datetime

from ..models.aggregation import (
    MarketComparisonRequest, MarketComparisonResponse,
    WorkloadTemplate, ResourceType
)
from ..core.market_client import MarketClient
from ..core.dependencies import get_market_client
from ..config import settings


router = APIRouter(prefix="/markets", tags=["Market Comparison"])


@router.post("/compare", response_model=MarketComparisonResponse)
async def compare_markets(
    request: MarketComparisonRequest,
    market_client: MarketClient = Depends(get_market_client)
):
    """Compare prices across different markets for a resource"""
    try:
        # Get prices from all markets
        all_prices = await market_client.get_all_market_prices(
            request.resource_type.value,
            request.specifications
        )
        
        # Process market prices
        market_prices = {}
        quality_adjusted_prices = {}
        
        if request.resource_type == ResourceType.QUANTUM:
            # Process quantum markets
            if 'spot' in all_prices:
                spot_prices = [p['price_per_minute'] for p in all_prices['spot']]
                market_prices['spot'] = sum(spot_prices) / len(spot_prices) if spot_prices else 0
                
            if 'futures' in all_prices:
                futures_prices = [p['price_per_minute'] for p in all_prices['futures']]
                market_prices['futures'] = sum(futures_prices) / len(futures_prices) if futures_prices else 0
            
            # Quality adjustment
            if request.include_quality_adjusted:
                for market, base_price in market_prices.items():
                    # Adjust price based on average quality
                    avg_quality = 85  # Would get from oracle service
                    quality_adjusted_prices[market] = base_price * (100 / avg_quality)
                    
        elif request.resource_type == ResourceType.AI:
            # Process AI markets
            if 'spot' in all_prices:
                spot_prices = [p['price_per_hour'] for p in all_prices['spot']]
                market_prices['spot'] = sum(spot_prices) / len(spot_prices) if spot_prices else 0
                
            if 'reserved' in all_prices:
                # Calculate effective hourly rate for reserved
                reserved_rates = []
                for r in all_prices['reserved']:
                    hours = r.get('reservation_hours', 720)
                    upfront = r.get('upfront_cost', 0)
                    hourly = r.get('hourly_rate', 0)
                    effective = (upfront / hours) + hourly
                    reserved_rates.append(effective)
                market_prices['reserved'] = sum(reserved_rates) / len(reserved_rates) if reserved_rates else 0
            
            # Quality adjustment
            if request.include_quality_adjusted:
                for market, base_price in market_prices.items():
                    avg_quality = 90
                    quality_adjusted_prices[market] = base_price * (100 / avg_quality)
                    
        elif request.resource_type == ResourceType.NETWORK:
            # Process network paths
            if 'paths' in all_prices:
                for path_data in all_prices['paths']:
                    path = path_data['path']
                    qos_pricing = path_data['qos_pricing']
                    
                    for qos in qos_pricing:
                        market_key = f"path_{path['path_id']}_{qos['qos_class']}"
                        market_prices[market_key] = qos['price_per_mbps_hour']
                        
                        if request.include_quality_adjusted:
                            quality_score = path.get('quality_score', 80)
                            quality_adjusted_prices[market_key] = qos['price_per_mbps_hour'] * (100 / quality_score)
        
        # Determine best option
        best_market = min(
            quality_adjusted_prices.items() if quality_adjusted_prices else market_prices.items(),
            key=lambda x: x[1]
        )[0] if market_prices else "none"
        
        # Calculate savings potential
        if market_prices:
            min_price = min(market_prices.values())
            max_price = max(market_prices.values())
            savings_potential = max_price - min_price
        else:
            savings_potential = 0
        
        # Generate recommendations
        recommendations = []
        if request.resource_type == ResourceType.QUANTUM and 'futures' in market_prices:
            if market_prices.get('futures', 0) < market_prices.get('spot', 0) * 0.8:
                recommendations.append("Consider futures contracts for 20%+ savings")
                
        if request.resource_type == ResourceType.AI and 'reserved' in market_prices:
            if request.duration_hours > 168:  # More than a week
                recommendations.append("Reserved instances recommended for long-term workloads")
                
        if request.resource_type == ResourceType.NETWORK:
            recommendations.append("Consider lower QoS with redundancy for cost savings")
        
        return MarketComparisonResponse(
            resource_type=request.resource_type,
            specifications=request.specifications,
            market_prices=market_prices,
            quality_adjusted_prices=quality_adjusted_prices,
            best_option=best_market,
            savings_potential=savings_potential,
            recommendations=recommendations
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/workload-templates", response_model=List[WorkloadTemplate])
async def get_workload_templates():
    """Get available workload templates"""
    try:
        templates = []
        
        for template_id, template_data in settings.WORKLOAD_TEMPLATES.items():
            template = WorkloadTemplate(
                template_id=template_id,
                name=template_id.replace('_', ' ').title(),
                description=f"Template for {template_id.replace('_', ' ')}",
                resource_requirements=template_data,
                typical_duration_hours=24,  # Default
                estimated_cost_range=(100, 1000),  # Would calculate actual range
                use_cases=["research", "production"],
                performance_metrics={
                    "throughput": "high",
                    "latency": "low",
                    "reliability": "99.9%"
                }
            )
            templates.append(template)
        
        return templates
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/workload-templates/{template_id}", response_model=WorkloadTemplate)
async def get_workload_template(
    template_id: str,
    market_client: MarketClient = Depends(get_market_client)
):
    """Get specific workload template with current pricing"""
    try:
        if template_id not in settings.WORKLOAD_TEMPLATES:
            raise HTTPException(status_code=404, detail="Template not found")
        
        template_data = settings.WORKLOAD_TEMPLATES[template_id]
        
        # Calculate cost range based on current market prices
        min_cost = 0
        max_cost = 0
        
        if 'quantum' in template_data:
            quantum_req = template_data['quantum']
            resources = await market_client.search_quantum_resources(
                min_qubit_count=quantum_req.get('qubit_count', 1)
            )
            if resources:
                prices = [r['price_per_minute'] for r in resources]
                duration = quantum_req.get('coherence_window_minutes', 10)
                min_cost += min(prices) * duration
                max_cost += max(prices) * duration
        
        if 'ai' in template_data:
            ai_req = template_data['ai']
            resources = await market_client.search_ai_accelerators(
                accelerator_type=ai_req.get('accelerator_type', 'GPU')
            )
            if resources:
                prices = [r['price_per_hour'] for r in resources]
                duration = ai_req.get('duration_hours', 1)
                count = ai_req.get('count', 1)
                min_cost += min(prices) * duration * count
                max_cost += max(prices) * duration * count
        
        if 'network' in template_data:
            network_req = template_data['network']
            # Simplified network cost calculation
            bandwidth = network_req.get('bandwidth_mbps', 100)
            duration = 24  # Default duration
            min_cost += bandwidth * 0.001 * duration  # Min rate
            max_cost += bandwidth * 0.01 * duration   # Max rate
        
        template = WorkloadTemplate(
            template_id=template_id,
            name=template_id.replace('_', ' ').title(),
            description=f"Template for {template_id.replace('_', ' ')}",
            resource_requirements=template_data,
            typical_duration_hours=24,
            estimated_cost_range=(min_cost, max_cost),
            use_cases=["research", "production", "development"],
            performance_metrics={
                "throughput": "high",
                "latency": "low" if 'real_time' in template_id else "medium",
                "reliability": "99.9%"
            }
        )
        
        return template
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/optimize-workload/{template_id}")
async def optimize_workload(
    template_id: str,
    budget_limit: Optional[float] = None,
    performance_priority: float = Query(0.5, ge=0, le=1, description="0=cost focus, 1=performance focus"),
    market_client: MarketClient = Depends(get_market_client)
):
    """Optimize a workload template for cost/performance"""
    try:
        if template_id not in settings.WORKLOAD_TEMPLATES:
            raise HTTPException(status_code=404, detail="Template not found")
        
        template_data = settings.WORKLOAD_TEMPLATES[template_id]
        
        # Optimization recommendations based on priority
        recommendations = []
        estimated_cost = 0
        resource_selections = {}
        
        # Cost-focused optimization
        if performance_priority < 0.5:
            recommendations.append("Prioritizing cost optimization")
            
            if 'quantum' in template_data:
                recommendations.append("Use smallest sufficient QPU")
                resource_selections['quantum'] = "basic_qpu"
                
            if 'ai' in template_data:
                recommendations.append("Use spot instances for AI workloads")
                resource_selections['ai'] = "spot_gpu"
                
            if 'network' in template_data:
                recommendations.append("Use best-effort QoS with redundancy")
                resource_selections['network'] = "best_effort_redundant"
                
        # Performance-focused optimization
        else:
            recommendations.append("Prioritizing performance optimization")
            
            if 'quantum' in template_data:
                recommendations.append("Use highest fidelity QPU available")
                resource_selections['quantum'] = "premium_qpu"
                
            if 'ai' in template_data:
                recommendations.append("Use dedicated high-performance accelerators")
                resource_selections['ai'] = "dedicated_tpu"
                
            if 'network' in template_data:
                recommendations.append("Use platinum QoS for lowest latency")
                resource_selections['network'] = "platinum_qos"
        
        # Budget constraints
        if budget_limit:
            recommendations.append(f"Optimizing within budget limit of ${budget_limit}")
        
        return {
            "template_id": template_id,
            "optimization_profile": {
                "performance_priority": performance_priority,
                "cost_priority": 1 - performance_priority,
                "budget_limit": budget_limit
            },
            "recommendations": recommendations,
            "resource_selections": resource_selections,
            "estimated_cost": estimated_cost,
            "expected_performance": {
                "completion_time": "2 hours" if performance_priority > 0.7 else "4 hours",
                "accuracy": "99%" if performance_priority > 0.7 else "97%",
                "reliability": "99.99%" if performance_priority > 0.7 else "99.9%"
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/market-stats")
async def get_market_statistics(
    resource_type: Optional[str] = Query(None, description="Filter by resource type"),
    time_window_hours: int = Query(24, description="Time window for statistics"),
    market_client: MarketClient = Depends(get_market_client)
):
    """Get market statistics and trends"""
    try:
        stats = {
            "time_window_hours": time_window_hours,
            "timestamp": datetime.utcnow().isoformat(),
            "markets": {}
        }
        
        # Get stats for each resource type
        resource_types = [resource_type] if resource_type else ["quantum", "ai", "network"]
        
        for rt in resource_types:
            if rt == "quantum":
                spot_prices = await market_client.get_quantum_spot_prices()
                stats["markets"]["quantum"] = {
                    "average_spot_price": sum(p['price_per_minute'] for p in spot_prices) / len(spot_prices) if spot_prices else 0,
                    "price_volatility": 0.15,  # Mock
                    "available_resources": len(spot_prices),
                    "utilization_rate": 0.75,  # Mock
                    "price_trend": "increasing"  # Mock
                }
                
            elif rt == "ai":
                spot_prices = await market_client.get_ai_spot_prices()
                stats["markets"]["ai"] = {
                    "average_spot_price": sum(p['price_per_hour'] for p in spot_prices) / len(spot_prices) if spot_prices else 0,
                    "price_volatility": 0.20,  # Mock
                    "available_resources": len(spot_prices),
                    "utilization_rate": 0.85,  # Mock
                    "price_trend": "stable"  # Mock
                }
                
            elif rt == "network":
                paths = await market_client.get_network_paths()
                stats["markets"]["network"] = {
                    "average_price_per_mbps": 0.005,  # Mock
                    "price_volatility": 0.10,  # Mock
                    "available_paths": len(paths),
                    "average_utilization": 0.60,  # Mock
                    "congestion_level": "low"  # Mock
                }
        
        # Overall market metrics
        stats["overall"] = {
            "market_efficiency": 0.82,  # Mock
            "arbitrage_opportunities": 15,  # Mock
            "total_volume_usd": 125000,  # Mock
            "active_users": 350  # Mock
        }
        
        return stats
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 