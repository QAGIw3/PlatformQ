package com.platformq.flink.dataquality;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.client.api.Schema;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.sink.PulsarSink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.pulsar.client.api.SubscriptionType;

import java.time.Instant;
import java.util.*;

/**
 * Real-time data quality monitoring job that processes streaming data
 * and calculates quality metrics with trust-weighted adjustments
 */
public class DataQualityMonitoringJob {
    
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Configure checkpointing
        env.enableCheckpointing(60000); // 1 minute
        
        // Create Pulsar source for data stream
        PulsarSource<GenericRecord> dataSource = PulsarSource.builder()
            .setServiceUrl("pulsar://pulsar:6650")
            .setTopics("persistent://platformq/public/data-stream-.*")
            .setDeserializationSchema(AvroSchema.of(GenericRecord.class))
            .setStartCursor(StartCursor.latest())
            .build();
            
        // Create Pulsar source for trust scores
        PulsarSource<GenericRecord> trustSource = PulsarSource.builder()
            .setServiceUrl("pulsar://pulsar:6650")
            .setTopics("persistent://platformq/public/trust-score-updates")
            .setDeserializationSchema(AvroSchema.of(GenericRecord.class))
            .setStartCursor(StartCursor.latest())
            .build();
            
        // Read data streams
        DataStream<GenericRecord> dataStream = env.fromSource(
            dataSource, 
            org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), 
            "Data Stream"
        );
        
        DataStream<GenericRecord> trustStream = env.fromSource(
            trustSource,
            org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
            "Trust Score Stream"
        );
        
        // Process and calculate quality metrics
        DataStream<DataQualityMetrics> qualityMetrics = dataStream
            .map(new DataQualityMapper())
            .keyBy(record -> record.datasetId)
            .window(TumblingProcessingTimeWindows.of(Time.minutes(5)))
            .aggregate(
                new QualityAggregator(),
                new QualityWindowProcessor()
            );
            
        // Enrich with trust scores
        DataStream<TrustAdjustedMetrics> trustAdjustedMetrics = qualityMetrics
            .connect(trustStream)
            .keyBy(
                metrics -> metrics.datasetId,
                trust -> trust.get("entity_id").toString()
            )
            .flatMap(new TrustEnrichmentFunction());
            
        // Detect quality anomalies
        DataStream<QualityAnomaly> anomalies = trustAdjustedMetrics
            .keyBy(metrics -> metrics.datasetId)
            .flatMap(new AnomalyDetectionFunction());
            
        // Create quality assessment events
        DataStream<GenericRecord> assessmentEvents = trustAdjustedMetrics
            .map(new QualityAssessmentMapper());
            
        // Write results to Pulsar
        PulsarSink<GenericRecord> qualitySink = PulsarSink.builder()
            .setServiceUrl("pulsar://pulsar:6650")
            .setTopics("persistent://platformq/public/data-quality-assessments")
            .setSerializationSchema(new AvroSchema<>(GenericRecord.class))
            .build();
            
        assessmentEvents.sinkTo(qualitySink);
        
        // Write anomalies to separate topic
        PulsarSink<GenericRecord> anomalySink = PulsarSink.builder()
            .setServiceUrl("pulsar://pulsar:6650")
            .setTopics("persistent://platformq/public/data-quality-anomalies")
            .setSerializationSchema(new AvroSchema<>(GenericRecord.class))
            .build();
            
        anomalies
            .map(new AnomalyEventMapper())
            .sinkTo(anomalySink);
            
        env.execute("Data Quality Monitoring Job");
    }
    
    /**
     * Maps raw data to quality metrics
     */
    public static class DataQualityMapper implements MapFunction<GenericRecord, DataQualityRecord> {
        @Override
        public DataQualityRecord map(GenericRecord record) throws Exception {
            DataQualityRecord quality = new DataQualityRecord();
            quality.datasetId = record.get("dataset_id").toString();
            quality.timestamp = Instant.now().toEpochMilli();
            
            // Calculate field-level quality
            Map<String, Double> fieldQuality = new HashMap<>();
            for (org.apache.avro.Schema.Field field : record.getSchema().getFields()) {
                Object value = record.get(field.name());
                double fieldScore = calculateFieldQuality(field, value);
                fieldQuality.put(field.name(), fieldScore);
            }
            
            quality.fieldQualityScores = fieldQuality;
            quality.recordCompleteness = calculateCompleteness(record);
            quality.hasAnomalies = detectRecordAnomalies(record);
            
            return quality;
        }
        
        private double calculateFieldQuality(org.apache.avro.Schema.Field field, Object value) {
            if (value == null) return 0.0;
            
            // Type-specific quality checks
            switch (field.schema().getType()) {
                case STRING:
                    return validateString(value.toString());
                case INT:
                case LONG:
                    return validateNumeric(value);
                case DOUBLE:
                case FLOAT:
                    return validateDecimal(value);
                default:
                    return 1.0;
            }
        }
        
        private double validateString(String value) {
            if (value.isEmpty()) return 0.0;
            if (value.length() > 1000) return 0.5; // Too long
            if (containsInvalidChars(value)) return 0.7;
            return 1.0;
        }
        
        private boolean containsInvalidChars(String value) {
            // Check for common data quality issues
            return value.contains("\0") || value.contains("\uFFFD");
        }
        
        private double validateNumeric(Object value) {
            // Check for reasonable ranges
            long num = ((Number) value).longValue();
            if (num < 0) return 0.8; // Negative when unexpected
            if (num > 1_000_000_000) return 0.9; // Very large
            return 1.0;
        }
        
        private double validateDecimal(Object value) {
            double num = ((Number) value).doubleValue();
            if (Double.isNaN(num) || Double.isInfinite(num)) return 0.0;
            return 1.0;
        }
        
        private double calculateCompleteness(GenericRecord record) {
            int totalFields = record.getSchema().getFields().size();
            int nonNullFields = 0;
            
            for (org.apache.avro.Schema.Field field : record.getSchema().getFields()) {
                if (record.get(field.name()) != null) {
                    nonNullFields++;
                }
            }
            
            return (double) nonNullFields / totalFields;
        }
        
        private boolean detectRecordAnomalies(GenericRecord record) {
            // Simple anomaly detection rules
            return false; // Placeholder for more complex logic
        }
    }
    
    /**
     * Aggregates quality metrics over windows
     */
    public static class QualityAggregator 
        implements AggregateFunction<DataQualityRecord, QualityAccumulator, DataQualityMetrics> {
        
        @Override
        public QualityAccumulator createAccumulator() {
            return new QualityAccumulator();
        }
        
        @Override
        public QualityAccumulator add(DataQualityRecord record, QualityAccumulator acc) {
            acc.recordCount++;
            acc.totalCompleteness += record.recordCompleteness;
            
            if (record.hasAnomalies) {
                acc.anomalyCount++;
            }
            
            // Aggregate field quality
            for (Map.Entry<String, Double> entry : record.fieldQualityScores.entrySet()) {
                acc.fieldScoreSums.merge(entry.getKey(), entry.getValue(), Double::sum);
                acc.fieldCounts.merge(entry.getKey(), 1L, Long::sum);
            }
            
            return acc;
        }
        
        @Override
        public QualityAccumulator merge(QualityAccumulator a, QualityAccumulator b) {
            a.recordCount += b.recordCount;
            a.totalCompleteness += b.totalCompleteness;
            a.anomalyCount += b.anomalyCount;
            
            // Merge field scores
            for (Map.Entry<String, Double> entry : b.fieldScoreSums.entrySet()) {
                a.fieldScoreSums.merge(entry.getKey(), entry.getValue(), Double::sum);
            }
            
            for (Map.Entry<String, Long> entry : b.fieldCounts.entrySet()) {
                a.fieldCounts.merge(entry.getKey(), entry.getValue(), Long::sum);
            }
            
            return a;
        }
        
        @Override
        public DataQualityMetrics getResult(QualityAccumulator acc) {
            DataQualityMetrics metrics = new DataQualityMetrics();
            metrics.recordCount = acc.recordCount;
            metrics.averageCompleteness = acc.totalCompleteness / acc.recordCount;
            metrics.anomalyRate = (double) acc.anomalyCount / acc.recordCount;
            
            // Calculate average field quality
            metrics.fieldQualityAverage = new HashMap<>();
            for (Map.Entry<String, Double> entry : acc.fieldScoreSums.entrySet()) {
                String field = entry.getKey();
                double avg = entry.getValue() / acc.fieldCounts.get(field);
                metrics.fieldQualityAverage.put(field, avg);
            }
            
            return metrics;
        }
    }
    
    /**
     * Enriches quality metrics with trust scores
     */
    public static class TrustEnrichmentFunction 
        extends RichFlatMapFunction<DataQualityMetrics, TrustAdjustedMetrics> {
        
        private ValueState<Double> trustScoreState;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Double> descriptor = 
                new ValueStateDescriptor<>("trustScore", Double.class, 0.5);
            trustScoreState = getRuntimeContext().getState(descriptor);
        }
        
        @Override
        public void flatMap(DataQualityMetrics metrics, Collector<TrustAdjustedMetrics> out) 
            throws Exception {
            
            Double trustScore = trustScoreState.value();
            
            TrustAdjustedMetrics adjusted = new TrustAdjustedMetrics();
            adjusted.datasetId = metrics.datasetId;
            adjusted.originalMetrics = metrics;
            adjusted.trustScore = trustScore;
            
            // Adjust quality scores based on trust
            adjusted.trustAdjustedCompleteness = metrics.averageCompleteness * (0.7 + 0.3 * trustScore);
            adjusted.trustAdjustedQuality = calculateTrustAdjustedQuality(metrics, trustScore);
            
            // Calculate confidence interval based on trust
            adjusted.confidenceInterval = 0.95 * trustScore;
            
            out.collect(adjusted);
        }
        
        private double calculateTrustAdjustedQuality(DataQualityMetrics metrics, double trust) {
            double baseQuality = metrics.averageCompleteness * 0.25 +
                                (1.0 - metrics.anomalyRate) * 0.35 +
                                calculateFieldQualityScore(metrics) * 0.40;
                                
            // Trust adjustment with bounds
            double adjusted = baseQuality * (0.6 + 0.4 * trust);
            return Math.min(trust * 1.1, adjusted); // Can't exceed trust by more than 10%
        }
        
        private double calculateFieldQualityScore(DataQualityMetrics metrics) {
            if (metrics.fieldQualityAverage.isEmpty()) return 0.0;
            
            return metrics.fieldQualityAverage.values().stream()
                .mapToDouble(Double::doubleValue)
                .average()
                .orElse(0.0);
        }
    }
    
    /**
     * Detects anomalies in quality metrics
     */
    public static class AnomalyDetectionFunction 
        extends RichFlatMapFunction<TrustAdjustedMetrics, QualityAnomaly> {
        
        private ValueState<QualityBaseline> baselineState;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<QualityBaseline> descriptor = 
                new ValueStateDescriptor<>("qualityBaseline", QualityBaseline.class);
            baselineState = getRuntimeContext().getState(descriptor);
        }
        
        @Override
        public void flatMap(TrustAdjustedMetrics metrics, Collector<QualityAnomaly> out) 
            throws Exception {
            
            QualityBaseline baseline = baselineState.value();
            if (baseline == null) {
                // Initialize baseline
                baseline = new QualityBaseline();
                baseline.avgQuality = metrics.trustAdjustedQuality;
                baseline.stdDev = 0.1; // Initial assumption
                baselineState.update(baseline);
                return;
            }
            
            // Check for anomalies
            double deviation = Math.abs(metrics.trustAdjustedQuality - baseline.avgQuality);
            if (deviation > 2 * baseline.stdDev) {
                QualityAnomaly anomaly = new QualityAnomaly();
                anomaly.datasetId = metrics.datasetId;
                anomaly.timestamp = Instant.now().toEpochMilli();
                anomaly.expectedQuality = baseline.avgQuality;
                anomaly.actualQuality = metrics.trustAdjustedQuality;
                anomaly.severity = calculateSeverity(deviation, baseline.stdDev);
                anomaly.anomalyType = deviation > 0 ? "QUALITY_IMPROVEMENT" : "QUALITY_DEGRADATION";
                
                out.collect(anomaly);
            }
            
            // Update baseline with exponential moving average
            baseline.avgQuality = 0.9 * baseline.avgQuality + 0.1 * metrics.trustAdjustedQuality;
            baseline.stdDev = 0.9 * baseline.stdDev + 0.1 * deviation;
            baselineState.update(baseline);
        }
        
        private String calculateSeverity(double deviation, double stdDev) {
            if (deviation > 4 * stdDev) return "CRITICAL";
            if (deviation > 3 * stdDev) return "HIGH";
            if (deviation > 2 * stdDev) return "MEDIUM";
            return "LOW";
        }
    }
    
    // Data classes
    public static class DataQualityRecord {
        public String datasetId;
        public long timestamp;
        public Map<String, Double> fieldQualityScores;
        public double recordCompleteness;
        public boolean hasAnomalies;
    }
    
    public static class QualityAccumulator {
        public long recordCount = 0;
        public double totalCompleteness = 0.0;
        public long anomalyCount = 0;
        public Map<String, Double> fieldScoreSums = new HashMap<>();
        public Map<String, Long> fieldCounts = new HashMap<>();
    }
    
    public static class DataQualityMetrics {
        public String datasetId;
        public long recordCount;
        public double averageCompleteness;
        public double anomalyRate;
        public Map<String, Double> fieldQualityAverage;
        public long windowStart;
        public long windowEnd;
    }
    
    public static class TrustAdjustedMetrics {
        public String datasetId;
        public DataQualityMetrics originalMetrics;
        public double trustScore;
        public double trustAdjustedCompleteness;
        public double trustAdjustedQuality;
        public double confidenceInterval;
    }
    
    public static class QualityAnomaly {
        public String datasetId;
        public long timestamp;
        public double expectedQuality;
        public double actualQuality;
        public String severity;
        public String anomalyType;
    }
    
    public static class QualityBaseline {
        public double avgQuality;
        public double stdDev;
    }
    
    /**
     * Window processor that adds window metadata
     */
    public static class QualityWindowProcessor 
        extends ProcessWindowFunction<DataQualityMetrics, DataQualityMetrics, String, TimeWindow> {
        
        @Override
        public void process(
            String key,
            Context context,
            Iterable<DataQualityMetrics> elements,
            Collector<DataQualityMetrics> out
        ) throws Exception {
            for (DataQualityMetrics metrics : elements) {
                metrics.datasetId = key;
                metrics.windowStart = context.window().getStart();
                metrics.windowEnd = context.window().getEnd();
                out.collect(metrics);
            }
        }
    }
    
    /**
     * Maps quality metrics to Avro records for output
     */
    public static class QualityAssessmentMapper implements MapFunction<TrustAdjustedMetrics, GenericRecord> {
        @Override
        public GenericRecord map(TrustAdjustedMetrics metrics) throws Exception {
            // Create Avro record following the schema
            // Implementation depends on actual Avro schema
            return null; // Placeholder
        }
    }
    
    /**
     * Maps anomalies to Avro records
     */
    public static class AnomalyEventMapper implements MapFunction<QualityAnomaly, GenericRecord> {
        @Override
        public GenericRecord map(QualityAnomaly anomaly) throws Exception {
            // Create Avro record for anomaly event
            return null; // Placeholder
        }
    }
} 