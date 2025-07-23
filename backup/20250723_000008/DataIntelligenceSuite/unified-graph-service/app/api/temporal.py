"""Temporal analysis API endpoints"""

from typing import List, Optional, Dict, Any
from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field

from app.core.config import Settings, get_settings
from app.temporal.temporal_analysis import TemporalAnalysisEngine, CausalAlgorithm


router = APIRouter(prefix="/api/v1/temporal", tags=["temporal"])

# Global instance (will be injected)
temporal_engine: Optional[TemporalAnalysisEngine] = None


class SnapshotRequest(BaseModel):
    """Graph snapshot request"""
    timestamp: datetime = Field(..., description="Timestamp for snapshot")
    entity_types: Optional[List[str]] = Field(None, description="Filter by entity types")
    edge_types: Optional[List[str]] = Field(None, description="Filter by edge types")


class CausalDiscoveryRequest(BaseModel):
    """Causal discovery request"""
    entities: List[str] = Field(..., description="Entity IDs to analyze")
    time_window: str = Field("7d", regex="^\\d+[hdm]$", description="Time window (e.g., 7d, 24h, 30m)")
    algorithm: str = Field("pc", description="Causal discovery algorithm")
    significance_level: float = Field(0.05, ge=0.01, le=0.1)


class ScenarioSimulation(BaseModel):
    """Scenario simulation request"""
    interventions: List[Dict[str, Any]] = Field(..., description="Interventions to apply")
    targets: List[str] = Field(..., description="Target entities to track")
    initial_state: Dict[str, Any] = Field({}, description="Initial state values")
    time_steps: int = Field(10, ge=1, le=100)


class TemporalPatternRequest(BaseModel):
    """Temporal pattern detection request"""
    pattern_type: str = Field(..., regex="^(periodic|trending|anomalous|burst)$")
    time_window: str = Field("30d", regex="^\\d+[hdm]$")
    min_support: float = Field(0.1, ge=0.01, le=1.0)


@router.get("/snapshot")
async def get_graph_snapshot(timestamp: datetime = Query(..., description="Snapshot timestamp"),
                           entity_types: Optional[str] = Query(None, description="Comma-separated entity types"),
                           edge_types: Optional[str] = Query(None, description="Comma-separated edge types"),
                           settings: Settings = Depends(get_settings)):
    """Get graph snapshot at a specific point in time"""
    try:
        # Parse filter lists
        entity_type_list = entity_types.split(',') if entity_types else None
        edge_type_list = edge_types.split(',') if edge_types else None
        
        snapshot = await temporal_engine.get_graph_snapshot(
            timestamp,
            entity_type_list,
            edge_type_list
        )
        
        return snapshot
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/evolution/{entity_id}")
async def get_entity_evolution(entity_id: str,
                             start_time: Optional[datetime] = None,
                             end_time: Optional[datetime] = None,
                             settings: Settings = Depends(get_settings)):
    """Get evolution of an entity over time"""
    try:
        evolution = await temporal_engine.get_entity_evolution(
            entity_id,
            start_time,
            end_time
        )
        
        if not evolution:
            raise HTTPException(status_code=404, detail="Entity not found")
            
        return {
            'entity_id': entity_id,
            'evolution': evolution,
            'versions': len(evolution)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/causal/discover")
async def discover_causality(request: CausalDiscoveryRequest,
                           settings: Settings = Depends(get_settings)):
    """Discover causal relationships between entities"""
    try:
        # Validate algorithm
        if request.algorithm not in [a.value for a in CausalAlgorithm]:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid algorithm. Must be one of: {[a.value for a in CausalAlgorithm]}"
            )
            
        result = await temporal_engine.discover_causality(
            request.entities,
            request.time_window,
            request.algorithm,
            request.significance_level
        )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/scenarios/simulate")
async def simulate_scenario(request: ScenarioSimulation,
                          settings: Settings = Depends(get_settings)):
    """Simulate what-if scenarios on the temporal graph"""
    try:
        scenario = {
            'interventions': request.interventions,
            'targets': request.targets,
            'initial_state': request.initial_state
        }
        
        result = await temporal_engine.simulate_scenario(
            scenario,
            request.time_steps
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/patterns/detect")
async def detect_temporal_patterns(request: TemporalPatternRequest,
                                 settings: Settings = Depends(get_settings)):
    """Detect temporal patterns in the graph"""
    try:
        patterns = await temporal_engine.detect_temporal_patterns(
            request.pattern_type,
            request.time_window,
            request.min_support
        )
        
        return {
            'pattern_type': request.pattern_type,
            'patterns': patterns,
            'count': len(patterns)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/time-series/{entity_id}")
async def get_entity_time_series(entity_id: str,
                               property_name: str = Query(..., description="Property to track"),
                               start_time: datetime = Query(..., description="Start time"),
                               end_time: datetime = Query(..., description="End time"),
                               interval: str = Query("1h", regex="^\\d+[hdm]$"),
                               settings: Settings = Depends(get_settings)):
    """Get time series data for an entity property"""
    try:
        # This would retrieve time series data
        # Placeholder implementation
        return {
            'entity_id': entity_id,
            'property': property_name,
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'interval': interval,
            'data_points': []
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/forecast")
async def forecast_trends(entity_ids: List[str] = Query(..., description="Entity IDs to forecast"),
                        horizon: int = Query(7, ge=1, le=90, description="Forecast horizon in days"),
                        confidence_level: float = Query(0.95, ge=0.5, le=0.99),
                        settings: Settings = Depends(get_settings)):
    """Forecast future trends for entities"""
    try:
        # This would run time series forecasting
        # Placeholder implementation
        return {
            'entities': entity_ids,
            'horizon_days': horizon,
            'confidence_level': confidence_level,
            'forecasts': {},
            'message': "Forecasting not yet implemented"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/anomalies")
async def detect_anomalies(time_window: str = Query("24h", regex="^\\d+[hdm]$"),
                         sensitivity: float = Query(0.95, ge=0.5, le=0.99),
                         entity_types: Optional[str] = Query(None),
                         settings: Settings = Depends(get_settings)):
    """Detect temporal anomalies in the graph"""
    try:
        # Parse entity types
        entity_type_list = entity_types.split(',') if entity_types else None
        
        # This would run anomaly detection
        # Placeholder implementation
        return {
            'time_window': time_window,
            'sensitivity': sensitivity,
            'anomalies': [],
            'message': "Anomaly detection not yet implemented"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/timeline")
async def get_graph_timeline(start_time: datetime = Query(...),
                           end_time: datetime = Query(...),
                           granularity: str = Query("day", regex="^(hour|day|week|month)$"),
                           metrics: Optional[str] = Query(None, description="Comma-separated metrics"),
                           settings: Settings = Depends(get_settings)):
    """Get timeline of graph metrics"""
    try:
        # Parse metrics
        metric_list = metrics.split(',') if metrics else ['nodes', 'edges']
        
        # This would aggregate timeline data
        # Placeholder implementation
        return {
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'granularity': granularity,
            'metrics': metric_list,
            'timeline': []
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def set_dependencies(te: TemporalAnalysisEngine):
    """Set global dependencies"""
    global temporal_engine
    temporal_engine = te 