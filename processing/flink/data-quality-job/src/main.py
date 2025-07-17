"""
Flink Job: Event-Driven Data Quality Monitoring

Monitors data quality in real-time during ingestion and triggers
corrective actions based on quality rules.
"""

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.datastream.functions import ProcessFunction, KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor, ListStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataQualityRules:
    """Data quality rule definitions"""
    
    @staticmethod
    def get_rules_for_type(data_type: str) -> List[Dict[str, Any]]:
        """Get quality rules for specific data type"""
        
        base_rules = [
            {
                "rule_id": "null_check",
                "type": "completeness",
                "severity": "high",
                "check": lambda row: all(v is not None for k, v in row.items() if k in ["id", "timestamp"])
            },
            {
                "rule_id": "timestamp_validity",
                "type": "validity",
                "severity": "high",
                "check": lambda row: DataQualityRules._is_valid_timestamp(row.get("timestamp"))
            }
        ]
        
        type_specific_rules = {
            "simulation_metrics": [
                {
                    "rule_id": "metric_range",
                    "type": "validity",
                    "severity": "medium",
                    "check": lambda row: 0 <= row.get("convergence_rate", 0) <= 1
                },
                {
                    "rule_id": "resource_usage",
                    "type": "validity",
                    "severity": "high",
                    "check": lambda row: 0 <= row.get("cpu_usage", 0) <= 100
                }
            ],
            "asset_metadata": [
                {
                    "rule_id": "asset_size",
                    "type": "validity",
                    "severity": "medium",
                    "check": lambda row: row.get("size", 0) > 0
                },
                {
                    "rule_id": "asset_type",
                    "type": "consistency",
                    "severity": "high",
                    "check": lambda row: row.get("asset_type") in ["3d_model", "dataset", "document", "image"]
                }
            ],
            "workflow_events": [
                {
                    "rule_id": "status_transition",
                    "type": "consistency",
                    "severity": "high",
                    "check": lambda row: DataQualityRules._is_valid_status_transition(row)
                },
                {
                    "rule_id": "duration_check",
                    "type": "validity",
                    "severity": "medium",
                    "check": lambda row: row.get("duration", 0) >= 0
                }
            ]
        }
        
        return base_rules + type_specific_rules.get(data_type, [])
    
    @staticmethod
    def _is_valid_timestamp(timestamp: Any) -> bool:
        """Check if timestamp is valid"""
        if not timestamp:
            return False
            
        try:
            if isinstance(timestamp, str):
                dt = datetime.fromisoformat(timestamp)
            elif isinstance(timestamp, (int, float)):
                dt = datetime.fromtimestamp(timestamp)
            else:
                return False
                
            # Check if timestamp is reasonable (not too far in past or future)
            now = datetime.utcnow()
            diff = abs((dt - now).total_seconds())
            
            return diff < 86400 * 365  # Within 1 year
            
        except:
            return False
            
    @staticmethod
    def _is_valid_status_transition(row: Dict[str, Any]) -> bool:
        """Check if workflow status transition is valid"""
        valid_transitions = {
            "pending": ["running", "cancelled"],
            "running": ["completed", "failed", "paused"],
            "paused": ["running", "cancelled"],
            "completed": [],
            "failed": ["pending"],
            "cancelled": []
        }
        
        prev_status = row.get("previous_status")
        new_status = row.get("status")
        
        if not prev_status or not new_status:
            return True  # Can't validate without both statuses
            
        return new_status in valid_transitions.get(prev_status, [])


class DataQualityMonitor(KeyedProcessFunction):
    """Monitors data quality in real-time"""
    
    def __init__(self):
        self.quality_state = None
        self.violation_history = None
        
    def open(self, runtime_context):
        """Initialize state"""
        self.quality_state = runtime_context.get_state(
            ValueStateDescriptor(
                "quality_metrics",
                type_info=DataTypes.MAP(DataTypes.STRING(), DataTypes.FLOAT())
            )
        )
        
        self.violation_history = runtime_context.get_list_state(
            ListStateDescriptor(
                "violations",
                type_info=DataTypes.ROW([
                    DataTypes.FIELD("timestamp", DataTypes.BIGINT()),
                    DataTypes.FIELD("rule_id", DataTypes.STRING()),
                    DataTypes.FIELD("severity", DataTypes.STRING())
                ])
            )
        )
        
    def process_element(self, value, ctx):
        """Process data and check quality"""
        try:
            data_type = ctx.get_current_key()
            record = value
            
            # Get quality rules for data type
            rules = DataQualityRules.get_rules_for_type(data_type)
            
            # Check each rule
            violations = []
            quality_scores = {}
            
            for rule in rules:
                try:
                    passed = rule["check"](record)
                    quality_scores[rule["rule_id"]] = 1.0 if passed else 0.0
                    
                    if not passed:
                        violations.append({
                            "rule_id": rule["rule_id"],
                            "type": rule["type"],
                            "severity": rule["severity"],
                            "record": record
                        })
                        
                        # Add to violation history
                        self.violation_history.add((
                            ctx.timestamp(),
                            rule["rule_id"],
                            rule["severity"]
                        ))
                        
                except Exception as e:
                    logger.error(f"Error checking rule {rule['rule_id']}: {e}")
                    
            # Update quality state
            current_metrics = self.quality_state.value() or {}
            
            for rule_id, score in quality_scores.items():
                current_score = current_metrics.get(rule_id, 0.0)
                # Exponential moving average
                current_metrics[rule_id] = 0.9 * current_score + 0.1 * score
                
            self.quality_state.update(current_metrics)
            
            # Calculate overall quality score
            overall_score = sum(current_metrics.values()) / len(current_metrics) if current_metrics else 0
            
            # Output quality event
            quality_event = {
                "event_type": "DATA_QUALITY_CHECK",
                "data_type": data_type,
                "record_id": record.get("id", "unknown"),
                "timestamp": datetime.utcnow().isoformat(),
                "overall_score": overall_score,
                "rule_scores": quality_scores,
                "violations": violations,
                "action_required": len([v for v in violations if v["severity"] == "high"]) > 0
            }
            
            yield quality_event
            
            # If critical violations, output alert
            if any(v["severity"] == "high" for v in violations):
                yield {
                    "event_type": "DATA_QUALITY_ALERT",
                    "data_type": data_type,
                    "severity": "high",
                    "violations": [v for v in violations if v["severity"] == "high"],
                    "timestamp": datetime.utcnow().isoformat()
                }
                
        except Exception as e:
            logger.error(f"Error in quality monitoring: {e}")


class DataQualityEnforcer(ProcessFunction):
    """Enforces data quality by cleaning or rejecting bad data"""
    
    def __init__(self, enforcement_mode: str = "clean"):
        self.enforcement_mode = enforcement_mode  # "clean", "reject", or "quarantine"
        
    def process_element(self, value, ctx):
        """Process quality check results and enforce rules"""
        try:
            if value["event_type"] != "DATA_QUALITY_CHECK":
                yield value
                return
                
            record = value.get("violations", [{}])[0].get("record", {}) if value.get("violations") else value
            
            if value["action_required"]:
                if self.enforcement_mode == "reject":
                    # Reject bad data
                    yield {
                        "event_type": "DATA_REJECTED",
                        "reason": "quality_violations",
                        "violations": value["violations"],
                        "record_id": value["record_id"],
                        "timestamp": datetime.utcnow().isoformat()
                    }
                    
                elif self.enforcement_mode == "clean":
                    # Attempt to clean data
                    cleaned_record = self._clean_record(record, value["violations"])
                    
                    if cleaned_record:
                        yield {
                            "event_type": "DATA_CLEANED",
                            "original_record_id": value["record_id"],
                            "cleaned_record": cleaned_record,
                            "modifications": self._get_modifications(record, cleaned_record),
                            "timestamp": datetime.utcnow().isoformat()
                        }
                    else:
                        # If cleaning failed, reject
                        yield {
                            "event_type": "DATA_REJECTED",
                            "reason": "cleaning_failed",
                            "record_id": value["record_id"],
                            "timestamp": datetime.utcnow().isoformat()
                        }
                        
                elif self.enforcement_mode == "quarantine":
                    # Send to quarantine for manual review
                    yield {
                        "event_type": "DATA_QUARANTINED",
                        "record": record,
                        "violations": value["violations"],
                        "record_id": value["record_id"],
                        "timestamp": datetime.utcnow().isoformat()
                    }
                    
            else:
                # Data passed quality checks
                yield {
                    "event_type": "DATA_VALIDATED",
                    "record": record,
                    "quality_score": value["overall_score"],
                    "timestamp": datetime.utcnow().isoformat()
                }
                
        except Exception as e:
            logger.error(f"Error enforcing data quality: {e}")
            
    def _clean_record(self, record: Dict[str, Any], violations: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Attempt to clean record based on violations"""
        cleaned = record.copy()
        
        for violation in violations:
            rule_id = violation["rule_id"]
            
            if rule_id == "null_check":
                # Fill nulls with defaults
                for key in ["id", "timestamp"]:
                    if key not in cleaned or cleaned[key] is None:
                        if key == "timestamp":
                            cleaned[key] = datetime.utcnow().isoformat()
                        else:
                            return None  # Can't fix missing ID
                            
            elif rule_id == "metric_range":
                # Clamp values to valid range
                if "convergence_rate" in cleaned:
                    cleaned["convergence_rate"] = max(0, min(1, cleaned["convergence_rate"]))
                    
            elif rule_id == "resource_usage":
                # Clamp resource usage
                if "cpu_usage" in cleaned:
                    cleaned["cpu_usage"] = max(0, min(100, cleaned["cpu_usage"]))
                    
            elif rule_id == "asset_size":
                # Can't fix invalid size
                if cleaned.get("size", 0) <= 0:
                    return None
                    
        return cleaned
        
    def _get_modifications(self, original: Dict[str, Any], cleaned: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Get list of modifications made during cleaning"""
        modifications = []
        
        for key in set(original.keys()) | set(cleaned.keys()):
            if original.get(key) != cleaned.get(key):
                modifications.append({
                    "field": key,
                    "original": original.get(key),
                    "cleaned": cleaned.get(key)
                })
                
        return modifications


class DataQualityAggregator(ProcessFunction):
    """Aggregates quality metrics for reporting"""
    
    def __init__(self):
        self.metrics_buffer = []
        self.last_report_time = datetime.utcnow()
        
    def process_element(self, value, ctx):
        """Aggregate quality metrics"""
        try:
            if value["event_type"] == "DATA_QUALITY_CHECK":
                self.metrics_buffer.append({
                    "data_type": value["data_type"],
                    "score": value["overall_score"],
                    "timestamp": value["timestamp"]
                })
                
            # Report aggregated metrics every minute
            if (datetime.utcnow() - self.last_report_time).seconds >= 60:
                if self.metrics_buffer:
                    # Calculate aggregated metrics
                    df = pd.DataFrame(self.metrics_buffer)
                    
                    aggregated = {
                        "event_type": "QUALITY_METRICS_REPORT",
                        "timestamp": datetime.utcnow().isoformat(),
                        "period_start": self.last_report_time.isoformat(),
                        "metrics": {}
                    }
                    
                    for data_type in df["data_type"].unique():
                        type_df = df[df["data_type"] == data_type]
                        aggregated["metrics"][data_type] = {
                            "count": len(type_df),
                            "avg_score": type_df["score"].mean(),
                            "min_score": type_df["score"].min(),
                            "max_score": type_df["score"].max(),
                            "std_score": type_df["score"].std()
                        }
                        
                    yield aggregated
                    
                    # Clear buffer
                    self.metrics_buffer = []
                    self.last_report_time = datetime.utcnow()
                    
        except Exception as e:
            logger.error(f"Error aggregating quality metrics: {e}")


def create_data_quality_job():
    """Create and configure the data quality monitoring job"""
    
    # Set up execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    
    # Enable checkpointing
    env.enable_checkpointing(30000)  # 30 seconds
    
    # Configure Pulsar source for different data streams
    data_sources = {
        "simulation_metrics": FlinkKafkaConsumer(
            topics=['simulation-metrics-events'],
            deserialization_schema=SimpleStringSchema(),
            properties={
                'bootstrap.servers': 'pulsar://pulsar:6650',
                'group.id': 'quality-monitor-simulation'
            }
        ),
        "asset_metadata": FlinkKafkaConsumer(
            topics=['asset-metadata-events'],
            deserialization_schema=SimpleStringSchema(),
            properties={
                'bootstrap.servers': 'pulsar://pulsar:6650',
                'group.id': 'quality-monitor-asset'
            }
        ),
        "workflow_events": FlinkKafkaConsumer(
            topics=['workflow-events'],
            deserialization_schema=SimpleStringSchema(),
            properties={
                'bootstrap.servers': 'pulsar://pulsar:6650',
                'group.id': 'quality-monitor-workflow'
            }
        )
    }
    
    # Create streams and key by data type
    quality_streams = []
    
    for data_type, source in data_sources.items():
        stream = env.add_source(source) \
            .map(lambda x: json.loads(x)) \
            .map(lambda x, dt=data_type: {**x, "_data_type": dt}) \
            .key_by(lambda x: x["_data_type"])
            
        quality_streams.append(stream)
        
    # Union all streams
    unified_stream = quality_streams[0]
    for stream in quality_streams[1:]:
        unified_stream = unified_stream.union(stream)
        
    # Apply quality monitoring
    quality_checked = unified_stream.process(DataQualityMonitor())
    
    # Enforce quality rules
    enforced_stream = quality_checked.process(DataQualityEnforcer(enforcement_mode="clean"))
    
    # Aggregate metrics
    metrics_stream = quality_checked.process(DataQualityAggregator())
    
    # Configure Pulsar sinks
    quality_sink = FlinkKafkaProducer(
        topic='data-quality-events',
        serialization_schema=SimpleStringSchema(),
        producer_config={
            'bootstrap.servers': 'pulsar://pulsar:6650'
        }
    )
    
    validated_sink = FlinkKafkaProducer(
        topic='validated-data-events',
        serialization_schema=SimpleStringSchema(),
        producer_config={
            'bootstrap.servers': 'pulsar://pulsar:6650'
        }
    )
    
    quarantine_sink = FlinkKafkaProducer(
        topic='quarantine-data-events',
        serialization_schema=SimpleStringSchema(),
        producer_config={
            'bootstrap.servers': 'pulsar://pulsar:6650'
        }
    )
    
    # Route outputs to appropriate sinks
    enforced_stream \
        .filter(lambda x: x["event_type"] in ["DATA_VALIDATED", "DATA_CLEANED"]) \
        .map(lambda x: json.dumps(x)) \
        .add_sink(validated_sink)
        
    enforced_stream \
        .filter(lambda x: x["event_type"] in ["DATA_REJECTED", "DATA_QUARANTINED"]) \
        .map(lambda x: json.dumps(x)) \
        .add_sink(quarantine_sink)
        
    # Send all quality events to quality sink
    quality_checked \
        .union(metrics_stream) \
        .map(lambda x: json.dumps(x)) \
        .add_sink(quality_sink)
    
    # Execute job
    env.execute("Event-Driven Data Quality Monitoring Job")


if __name__ == "__main__":
    create_data_quality_job() 