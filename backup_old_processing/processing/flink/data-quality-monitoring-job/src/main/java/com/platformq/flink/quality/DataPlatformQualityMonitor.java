package com.platformq.flink.quality;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.platformq.flink.quality.models.*;
import com.platformq.flink.quality.processors.*;
import com.platformq.flink.quality.sinks.*;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Data Quality Monitoring for PlatformQ Data Platform
 * 
 * Monitors data quality in real-time and integrates with:
 * - Data Platform Service for catalog and lineage
 * - Graph Intelligence Service for trust scores
 * - Analytics Service for quality dashboards
 */
public class DataPlatformQualityMonitor {
    
    private static final Logger LOG = LoggerFactory.getLogger(DataPlatformQualityMonitor.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    public static void main(String[] args) throws Exception {
        // Parse parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Configure checkpointing
        env.enableCheckpointing(params.getLong("checkpoint.interval", 60000), CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(120000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        
        // Set state backend
        String stateBackendPath = params.get("state.backend.path", "s3://platformq/flink-state");
        env.setStateBackend(new FsStateBackend(stateBackendPath));
        
        // Configure parallelism
        env.setParallelism(params.getInt("parallelism", 4));
        
        // Create Pulsar source for data events
        String pulsarUrl = params.get("pulsar.url", "pulsar://pulsar:6650");
        
        PulsarSource<String> dataSource = PulsarSource.builder()
            .setServiceUrl(pulsarUrl)
            .setAdminUrl(params.get("pulsar.admin.url", "http://pulsar:8080"))
            .setTopics(Arrays.asList(
                "persistent://platformq/data-lake/ingestion-events",
                "persistent://platformq/catalog/dataset-updates"
            ))
            .setStartCursor(StartCursor.latest())
            .setDeserializationSchema(new SimpleStringSchema())
            .setSubscriptionName("quality-monitor")
            .build();
        
        // Create data stream
        DataStream<String> rawStream = env.fromSource(
            dataSource,
            WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, timestamp) -> {
                    try {
                        ObjectNode node = objectMapper.readValue(event, ObjectNode.class);
                        return node.has("timestamp") ? node.get("timestamp").asLong() : System.currentTimeMillis();
                    } catch (Exception e) {
                        return System.currentTimeMillis();
                    }
                }),
            "Data Events"
        );
        
        // Parse and validate events
        DataStream<DatasetEvent> datasetEvents = rawStream
            .map(new EventParser())
            .filter(event -> event != null && event.getDatasetId() != null);
        
        // Real-time quality assessment
        DataStream<QualityAssessment> assessments = datasetEvents
            .keyBy(DatasetEvent::getDatasetId)
            .process(new RealTimeQualityAssessor());
        
        // Anomaly detection
        DataStream<QualityAnomaly> anomalies = assessments
            .keyBy(QualityAssessment::getDatasetId)
            .process(new AnomalyDetector());
        
        // Quality aggregation by window
        DataStream<QualityReport> reports = assessments
            .keyBy(QualityAssessment::getDatasetId)
            .window(TumblingEventTimeWindows.of(Time.minutes(5)))
            .process(new QualityAggregator());
        
        // Auto-remediation pipeline
        DataStream<RemediationRequest> remediationRequests = anomalies
            .filter(anomaly -> anomaly.getSeverity().equals("CRITICAL") || anomaly.getSeverity().equals("HIGH"))
            .keyBy(QualityAnomaly::getDatasetId)
            .process(new RemediationDecisionMaker());
        
        // Write to various sinks
        
        // 1. Send quality assessments back to data platform service
        assessments.addSink(new DataPlatformServiceSink(
            params.get("data.platform.url", "http://data-platform-service:8000")
        ));
        
        // 2. Store in Elasticsearch for analytics
        anomalies.addSink(new ElasticsearchQualitySink(
            params.get("elasticsearch.host", "elasticsearch:9200")
        ));
        
        // 3. Log critical issues
        anomalies.filter(anomaly -> anomaly.getSeverity().equals("CRITICAL"))
            .map(anomaly -> objectMapper.writeValueAsString(anomaly))
            .print("CRITICAL QUALITY ISSUE");
        
        // 4. Trigger automated remediation workflows
        remediationRequests.addSink(new RemediationWorkflowSink(
            params.get("workflow.service.url", "http://workflow-service:8000")
        ));
        
        // Execute job
        env.execute("Data Platform Quality Monitor");
    }
    
    /**
     * Parses raw events into dataset events
     */
    public static class EventParser implements MapFunction<String, DatasetEvent> {
        @Override
        public DatasetEvent map(String json) throws Exception {
            try {
                return objectMapper.readValue(json, DatasetEvent.class);
            } catch (Exception e) {
                LOG.warn("Failed to parse event: {}", json, e);
                return null;
            }
        }
    }
    
    /**
     * Real-time quality assessment processor
     */
    public static class RealTimeQualityAssessor extends KeyedProcessFunction<String, DatasetEvent, QualityAssessment> {
        
        private transient ValueState<DatasetProfile> profileState;
        private transient ValueState<QualityBaseline> baselineState;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            profileState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("dataset-profile", DatasetProfile.class)
            );
            
            baselineState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("quality-baseline", QualityBaseline.class)
            );
        }
        
        @Override
        public void processElement(DatasetEvent event, Context ctx, Collector<QualityAssessment> out) throws Exception {
            try {
                // Get or create profile
                DatasetProfile profile = profileState.value();
                if (profile == null) {
                    profile = new DatasetProfile(event.getDatasetId());
                }
                
                // Create assessment
                QualityAssessment assessment = new QualityAssessment();
                assessment.setAssessmentId(UUID.randomUUID().toString());
                assessment.setDatasetId(event.getDatasetId());
                assessment.setTimestamp(System.currentTimeMillis());
                
                // Calculate quality dimensions
                QualityDimensions dimensions = calculateQuality(event, profile);
                assessment.setDimensions(dimensions);
                
                // Calculate overall score
                double score = calculateOverallScore(dimensions);
                assessment.setOverallQualityScore(score);
                
                // Identify issues
                List<String> issues = identifyIssues(dimensions, score);
                assessment.setQualityIssues(issues);
                
                // Update profile
                profile.update(event, dimensions);
                profileState.update(profile);
                
                // Update baseline if needed
                updateBaseline(score);
                
                // Emit assessment
                out.collect(assessment);
                
            } catch (Exception e) {
                LOG.error("Error processing dataset event", e);
            }
        }
        
        private QualityDimensions calculateQuality(DatasetEvent event, DatasetProfile profile) {
            QualityDimensions dimensions = new QualityDimensions();
            
            // Completeness - check if critical fields are present
            double completeness = 1.0;
            if (event.getNumSamples() != null && event.getNumSamples() == 0) {
                completeness = 0.0;
            }
            dimensions.setCompleteness(completeness);
            
            // Accuracy - validate data format
            double accuracy = 1.0;
            if (event.getFormat() == null || event.getFormat().isEmpty()) {
                accuracy = 0.5;
            }
            dimensions.setAccuracy(accuracy);
            
            // Consistency - compare with historical profile
            double consistency = 1.0;
            if (profile.getRecordCount() > 0 && event.getSizeBytes() != null) {
                long avgSize = profile.getAverageSize();
                long currentSize = event.getSizeBytes();
                double deviation = Math.abs(currentSize - avgSize) / (double) avgSize;
                consistency = Math.max(0, 1.0 - deviation);
            }
            dimensions.setConsistency(consistency);
            
            // Timeliness - check data freshness
            double timeliness = 1.0;
            if (event.getTimestamp() != null) {
                long age = System.currentTimeMillis() - event.getTimestamp();
                if (age > TimeUnit.HOURS.toMillis(24)) {
                    timeliness = 0.5;
                }
            }
            dimensions.setTimeliness(timeliness);
            
            // Validity and Uniqueness (simplified)
            dimensions.setValidity(0.9);
            dimensions.setUniqueness(0.95);
            
            return dimensions;
        }
        
        private double calculateOverallScore(QualityDimensions dimensions) {
            return (dimensions.getCompleteness() * 0.2 +
                    dimensions.getAccuracy() * 0.2 +
                    dimensions.getConsistency() * 0.15 +
                    dimensions.getTimeliness() * 0.15 +
                    dimensions.getValidity() * 0.15 +
                    dimensions.getUniqueness() * 0.15);
        }
        
        private List<String> identifyIssues(QualityDimensions dimensions, double overallScore) {
            List<String> issues = new ArrayList<>();
            
            if (dimensions.getCompleteness() < 0.8) {
                issues.add("Low completeness: Missing critical data");
            }
            if (dimensions.getAccuracy() < 0.9) {
                issues.add("Accuracy concerns: Data validation failed");
            }
            if (dimensions.getConsistency() < 0.7) {
                issues.add("Inconsistent data: Deviates from historical patterns");
            }
            if (dimensions.getTimeliness() < 0.8) {
                issues.add("Stale data: Not updated recently");
            }
            if (overallScore < 0.7) {
                issues.add("Overall quality below acceptable threshold");
            }
            
            return issues;
        }
        
        private void updateBaseline(double score) throws Exception {
            QualityBaseline baseline = baselineState.value();
            if (baseline == null) {
                baseline = new QualityBaseline();
                baseline.avgQuality = score;
                baseline.stdDev = 0.1;
            } else {
                // Exponential moving average
                baseline.avgQuality = 0.9 * baseline.avgQuality + 0.1 * score;
                double deviation = Math.abs(score - baseline.avgQuality);
                baseline.stdDev = 0.9 * baseline.stdDev + 0.1 * deviation;
            }
            baselineState.update(baseline);
        }
    }
    
    /**
     * Anomaly detection based on quality scores
     */
    public static class AnomalyDetector extends KeyedProcessFunction<String, QualityAssessment, QualityAnomaly> {
        
        private transient ValueState<QualityBaseline> baselineState;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            baselineState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("anomaly-baseline", QualityBaseline.class)
            );
        }
        
        @Override
        public void processElement(QualityAssessment assessment, Context ctx, Collector<QualityAnomaly> out) throws Exception {
            QualityBaseline baseline = baselineState.value();
            if (baseline == null || baseline.avgQuality == 0) {
                // Initialize baseline
                baseline = new QualityBaseline();
                baseline.avgQuality = assessment.getOverallQualityScore();
                baseline.stdDev = 0.1;
                baselineState.update(baseline);
                return;
            }
            
            double score = assessment.getOverallQualityScore();
            double deviation = Math.abs(score - baseline.avgQuality);
            
            if (deviation > 2 * baseline.stdDev) {
                QualityAnomaly anomaly = new QualityAnomaly();
                anomaly.setDatasetId(assessment.getDatasetId());
                anomaly.setTimestamp(System.currentTimeMillis());
                anomaly.setExpectedQuality(baseline.avgQuality);
                anomaly.setActualQuality(score);
                anomaly.setDeviation(deviation);
                
                // Determine severity
                if (deviation > 4 * baseline.stdDev) {
                    anomaly.setSeverity("CRITICAL");
                } else if (deviation > 3 * baseline.stdDev) {
                    anomaly.setSeverity("HIGH");
                } else {
                    anomaly.setSeverity("MEDIUM");
                }
                
                // Determine type
                if (score < baseline.avgQuality) {
                    anomaly.setAnomalyType("QUALITY_DEGRADATION");
                } else {
                    anomaly.setAnomalyType("QUALITY_IMPROVEMENT");
                }
                
                out.collect(anomaly);
            }
            
            // Update baseline
            baseline.avgQuality = 0.95 * baseline.avgQuality + 0.05 * score;
            baseline.stdDev = 0.95 * baseline.stdDev + 0.05 * deviation;
            baselineState.update(baseline);
        }
    }
    
    /**
     * Aggregates quality assessments into reports
     */
    public static class QualityAggregator extends ProcessWindowFunction<QualityAssessment, QualityReport, String, TimeWindow> {
        
        @Override
        public void process(String key, Context context, Iterable<QualityAssessment> elements, Collector<QualityReport> out) throws Exception {
            QualityReport report = new QualityReport();
            report.setDatasetId(key);
            report.setWindowStart(context.window().getStart());
            report.setWindowEnd(context.window().getEnd());
            report.setTimestamp(System.currentTimeMillis());
            
            double totalScore = 0;
            int count = 0;
            Map<String, Integer> issueCounts = new HashMap<>();
            
            for (QualityAssessment assessment : elements) {
                totalScore += assessment.getOverallQualityScore();
                count++;
                
                for (String issue : assessment.getQualityIssues()) {
                    issueCounts.put(issue, issueCounts.getOrDefault(issue, 0) + 1);
                }
            }
            
            if (count > 0) {
                report.setAverageQuality(totalScore / count);
                report.setAssessmentCount(count);
                report.setTopIssues(getTopIssues(issueCounts, 5));
                
                out.collect(report);
            }
        }
        
        private List<String> getTopIssues(Map<String, Integer> issueCounts, int limit) {
            return issueCounts.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(limit)
                .map(Map.Entry::getKey)
                .collect(java.util.stream.Collectors.toList());
        }
    }
    
    /**
     * Sink to send quality data to Data Platform Service
     */
    public static class DataPlatformServiceSink implements SinkFunction<QualityAssessment> {
        
        private final String dataPlatformUrl;
        private transient org.apache.http.impl.nio.client.CloseableHttpAsyncClient httpClient;
        
        public DataPlatformServiceSink(String dataPlatformUrl) {
            this.dataPlatformUrl = dataPlatformUrl;
        }
        
        @Override
        public void invoke(QualityAssessment assessment, Context context) throws Exception {
            if (httpClient == null) {
                httpClient = org.apache.http.impl.nio.client.HttpAsyncClients.createDefault();
                httpClient.start();
            }
            
            try {
                // Send quality assessment to data platform
                String json = objectMapper.writeValueAsString(assessment);
                
                org.apache.http.client.methods.HttpPost request = new org.apache.http.client.methods.HttpPost(
                    dataPlatformUrl + "/api/v1/quality/assessments"
                );
                request.setHeader("Content-Type", "application/json");
                request.setEntity(new org.apache.http.entity.StringEntity(json));
                
                httpClient.execute(request, null);
                
            } catch (Exception e) {
                LOG.error("Failed to send assessment to data platform", e);
            }
        }
    }
    
    /**
     * Elasticsearch sink for quality data
     */
    public static class ElasticsearchQualitySink implements SinkFunction<QualityAnomaly> {
        
        private final String elasticsearchHost;
        private transient RestHighLevelClient client;
        
        public ElasticsearchQualitySink(String elasticsearchHost) {
            this.elasticsearchHost = elasticsearchHost;
        }
        
        @Override
        public void invoke(QualityAnomaly anomaly, Context context) throws Exception {
            if (client == null) {
                RestClientBuilder builder = RestClient.builder(
                    HttpHost.create(elasticsearchHost)
                );
                client = new RestHighLevelClient(builder);
            }
            
            try {
                Map<String, Object> json = new HashMap<>();
                json.put("dataset_id", anomaly.getDatasetId());
                json.put("timestamp", new Date(anomaly.getTimestamp()));
                json.put("anomaly_type", anomaly.getAnomalyType());
                json.put("severity", anomaly.getSeverity());
                json.put("expected_quality", anomaly.getExpectedQuality());
                json.put("actual_quality", anomaly.getActualQuality());
                json.put("deviation", anomaly.getDeviation());
                
                IndexRequest request = new IndexRequest("data-quality-anomalies")
                    .id(anomaly.getDatasetId() + "-" + anomaly.getTimestamp())
                    .source(json);
                
                client.index(request, org.elasticsearch.client.RequestOptions.DEFAULT);
                
            } catch (Exception e) {
                LOG.error("Failed to index anomaly to Elasticsearch", e);
            }
        }
    }
    
    // Supporting classes
    public static class QualityBaseline {
        public double avgQuality;
        public double stdDev;
    }
    
    public static class QualityAnomaly implements java.io.Serializable {
        private String datasetId;
        private Long timestamp;
        private String anomalyType;
        private String severity;
        private Double expectedQuality;
        private Double actualQuality;
        private Double deviation;
        
        // Getters and setters
        public String getDatasetId() { return datasetId; }
        public void setDatasetId(String datasetId) { this.datasetId = datasetId; }
        
        public Long getTimestamp() { return timestamp; }
        public void setTimestamp(Long timestamp) { this.timestamp = timestamp; }
        
        public String getAnomalyType() { return anomalyType; }
        public void setAnomalyType(String anomalyType) { this.anomalyType = anomalyType; }
        
        public String getSeverity() { return severity; }
        public void setSeverity(String severity) { this.severity = severity; }
        
        public Double getExpectedQuality() { return expectedQuality; }
        public void setExpectedQuality(Double expectedQuality) { this.expectedQuality = expectedQuality; }
        
        public Double getActualQuality() { return actualQuality; }
        public void setActualQuality(Double actualQuality) { this.actualQuality = actualQuality; }
        
        public Double getDeviation() { return deviation; }
        public void setDeviation(Double deviation) { this.deviation = deviation; }
    }
    
    public static class QualityReport implements java.io.Serializable {
        private String datasetId;
        private Long windowStart;
        private Long windowEnd;
        private Long timestamp;
        private Double averageQuality;
        private Integer assessmentCount;
        private List<String> topIssues;
        
        // Getters and setters
        public String getDatasetId() { return datasetId; }
        public void setDatasetId(String datasetId) { this.datasetId = datasetId; }
        
        public Long getWindowStart() { return windowStart; }
        public void setWindowStart(Long windowStart) { this.windowStart = windowStart; }
        
        public Long getWindowEnd() { return windowEnd; }
        public void setWindowEnd(Long windowEnd) { this.windowEnd = windowEnd; }
        
        public Long getTimestamp() { return timestamp; }
        public void setTimestamp(Long timestamp) { this.timestamp = timestamp; }
        
        public Double getAverageQuality() { return averageQuality; }
        public void setAverageQuality(Double averageQuality) { this.averageQuality = averageQuality; }
        
        public Integer getAssessmentCount() { return assessmentCount; }
        public void setAssessmentCount(Integer assessmentCount) { this.assessmentCount = assessmentCount; }
        
        public List<String> getTopIssues() { return topIssues; }
        public void setTopIssues(List<String> topIssues) { this.topIssues = topIssues; }
    }
} 