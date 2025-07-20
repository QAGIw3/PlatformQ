"""
Trading Data Lake API Endpoints

Provides API for trading data ingestion, processing, and feature generation
using the medallion architecture (Bronze, Silver, Gold).
"""

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Query
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from pydantic import BaseModel, Field

from ..lake.trading_medallion import (
    TradingMedallionArchitecture,
    TradingDataType,
    TradingDataQuality
)

router = APIRouter(prefix="/trading", tags=["trading-lake"])


class TradingEventIngestion(BaseModel):
    """Trading event ingestion request"""
    events: List[Dict[str, Any]] = Field(..., description="Trading events to ingest")
    event_type: str = Field(..., description="Type of trading events")
    timestamp: Optional[datetime] = Field(None, description="Ingestion timestamp")


class SilverProcessingRequest(BaseModel):
    """Silver layer processing request"""
    event_type: str = Field(..., description="Event type to process")
    processing_date: Optional[datetime] = Field(None, description="Date to process")


class GoldFeatureRequest(BaseModel):
    """Gold layer feature generation request"""
    feature_sets: List[str] = Field(..., description="Feature sets to generate")
    start_date: datetime = Field(..., description="Start date for features")
    end_date: datetime = Field(..., description="End date for features")


def get_trading_medallion(request) -> TradingMedallionArchitecture:
    """Get trading medallion architecture instance"""
    return request.app.state.trading_medallion


@router.post("/ingest")
async def ingest_trading_events(
    request: TradingEventIngestion,
    background_tasks: BackgroundTasks,
    trading_medallion: TradingMedallionArchitecture = Depends(get_trading_medallion)
):
    """Ingest raw trading events into Bronze layer"""
    try:
        # Validate event type
        try:
            event_type = TradingDataType(request.event_type)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid event type. Must be one of: {[t.value for t in TradingDataType]}"
            )
        
        # Ingest events
        result = await trading_medallion.ingest_trading_events(
            events=request.events,
            event_type=event_type
        )
        
        # Schedule Silver processing in background
        if result["status"] == "success":
            background_tasks.add_task(
                trading_medallion.process_to_silver,
                event_type,
                datetime.utcnow()
            )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/process/silver")
async def process_to_silver(
    request: SilverProcessingRequest,
    trading_medallion: TradingMedallionArchitecture = Depends(get_trading_medallion)
):
    """Process Bronze data to Silver layer with validation"""
    try:
        # Validate event type
        try:
            event_type = TradingDataType(request.event_type)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid event type. Must be one of: {[t.value for t in TradingDataType]}"
            )
        
        # Process to Silver
        result = await trading_medallion.process_to_silver(
            event_type=event_type,
            processing_date=request.processing_date
        )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/generate/features")
async def generate_gold_features(
    request: GoldFeatureRequest,
    background_tasks: BackgroundTasks,
    trading_medallion: TradingMedallionArchitecture = Depends(get_trading_medallion)
):
    """Generate Gold layer features for ML and analytics"""
    try:
        # Validate feature sets
        valid_feature_sets = [
            "market_microstructure",
            "trader_behavior",
            "risk_indicators",
            "technical_indicators"
        ]
        
        invalid_sets = [fs for fs in request.feature_sets if fs not in valid_feature_sets]
        if invalid_sets:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid feature sets: {invalid_sets}. Valid sets: {valid_feature_sets}"
            )
        
        # Generate features
        result = await trading_medallion.generate_gold_features(
            feature_sets=request.feature_sets,
            time_range={
                "start": request.start_date,
                "end": request.end_date
            }
        )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/quality/report")
async def get_data_quality_report(
    event_type: str = Query(..., description="Event type"),
    date: Optional[datetime] = Query(None, description="Date to check"),
    trading_medallion: TradingMedallionArchitecture = Depends(get_trading_medallion)
):
    """Get data quality report for trading data"""
    try:
        # Validate event type
        try:
            data_type = TradingDataType(event_type)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid event type. Must be one of: {[t.value for t in TradingDataType]}"
            )
        
        if not date:
            date = datetime.utcnow().date()
        
        # Get quality metrics
        # This would be implemented to fetch actual quality metrics
        quality_report = {
            "event_type": event_type,
            "date": date.isoformat(),
            "bronze_layer": {
                "record_count": 15000,
                "ingestion_lag_seconds": 2.5,
                "error_rate": 0.001
            },
            "silver_layer": {
                "record_count": 14950,
                "completeness": 0.997,
                "accuracy": 0.999,
                "timeliness": 0.998,
                "consistency": 0.995
            },
            "gold_layer": {
                "feature_sets_available": ["market_microstructure", "trader_behavior"],
                "last_updated": datetime.utcnow().isoformat(),
                "coverage": 0.98
            }
        }
        
        return quality_report
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/features/available")
async def get_available_features(
    trading_medallion: TradingMedallionArchitecture = Depends(get_trading_medallion)
):
    """Get list of available trading features"""
    return {
        "feature_sets": {
            "market_microstructure": {
                "description": "Market microstructure features",
                "features": [
                    "avg_spread",
                    "spread_volatility",
                    "avg_depth",
                    "order_imbalance_mean",
                    "price_volatility",
                    "trade_frequency",
                    "volume_profile",
                    "kyle_lambda",
                    "amihud_illiquidity"
                ]
            },
            "trader_behavior": {
                "description": "Trader behavior features",
                "features": [
                    "trade_frequency",
                    "avg_trade_size",
                    "trade_size_volatility",
                    "win_rate",
                    "avg_holding_period",
                    "profit_factor",
                    "max_drawdown",
                    "sharpe_ratio",
                    "market_timing_score",
                    "strategy_consistency"
                ]
            },
            "risk_indicators": {
                "description": "Risk indicator features",
                "features": [
                    "concentration_risk",
                    "directional_risk",
                    "leverage_ratio",
                    "liquidation_risk",
                    "correlation_risk",
                    "var_95",
                    "cvar_95",
                    "stress_test_score"
                ]
            },
            "technical_indicators": {
                "description": "Technical analysis indicators",
                "features": [
                    "rsi",
                    "macd",
                    "bollinger_bands",
                    "atr",
                    "obv",
                    "momentum",
                    "stochastic"
                ]
            }
        }
    }


@router.post("/batch/process")
async def batch_process_historical(
    start_date: datetime = Query(..., description="Start date"),
    end_date: datetime = Query(..., description="End date"),
    event_types: List[str] = Query(..., description="Event types to process"),
    background_tasks: BackgroundTasks,
    trading_medallion: TradingMedallionArchitecture = Depends(get_trading_medallion)
):
    """Batch process historical trading data"""
    try:
        # Validate event types
        validated_types = []
        for et in event_types:
            try:
                validated_types.append(TradingDataType(et))
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid event type: {et}"
                )
        
        # Schedule batch processing
        job_id = f"batch_{datetime.utcnow().timestamp()}"
        
        async def run_batch_processing():
            current_date = start_date
            while current_date <= end_date:
                for event_type in validated_types:
                    await trading_medallion.process_to_silver(
                        event_type=event_type,
                        processing_date=current_date
                    )
                current_date += timedelta(days=1)
            
            # Generate features after processing
            await trading_medallion.generate_gold_features(
                feature_sets=["market_microstructure", "trader_behavior", "risk_indicators"],
                time_range={"start": start_date, "end": end_date}
            )
        
        background_tasks.add_task(run_batch_processing)
        
        return {
            "job_id": job_id,
            "status": "scheduled",
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "event_types": event_types
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics/ingestion")
async def get_ingestion_metrics(
    time_range: str = Query("1h", description="Time range (1h, 24h, 7d)"),
    trading_medallion: TradingMedallionArchitecture = Depends(get_trading_medallion)
):
    """Get ingestion metrics for trading data"""
    # This would fetch actual metrics from monitoring system
    return {
        "time_range": time_range,
        "metrics": {
            "total_events_ingested": 2500000,
            "events_per_second": 2890,
            "bronze_layer_size_gb": 45.2,
            "silver_layer_size_gb": 38.7,
            "gold_layer_size_gb": 12.3,
            "processing_lag_seconds": 3.2,
            "error_rate": 0.0012,
            "by_event_type": {
                "trades": 1200000,
                "order_book": 800000,
                "positions": 300000,
                "market_data": 200000
            }
        },
        "timestamp": datetime.utcnow().isoformat()
    } 