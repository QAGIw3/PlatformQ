package com.platformq.flink.cep;

import com.platformq.flink.cep.models.Alert;
import com.platformq.flink.cep.models.EntityProfile;
import com.platformq.flink.cep.models.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.sink.PulsarSink;
import org.apache.flink.connector.pulsar.sink.PulsarSinkBuilder;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.PulsarSourceBuilder;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Complex Event Processing Job for Real-time Pattern Detection
 * 
 * Detects fraud, security threats, and anomalies using Flink CEP
 */
public class ComplexEventProcessingJob {
    
    private static final Logger LOG = LoggerFactory.getLogger(ComplexEventProcessingJob.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    // Output tags for different alert types
    private static final OutputTag<Alert> FRAUD_ALERTS = new OutputTag<>("fraud-alerts"){};
    private static final OutputTag<Alert> SECURITY_ALERTS = new OutputTag<>("security-alerts"){};
    private static final OutputTag<Alert> ANOMALY_ALERTS = new OutputTag<>("anomaly-alerts"){};
    private static final OutputTag<Alert> CRITICAL_ALERTS = new OutputTag<>("critical-alerts"){};
    
    public static void main(String[] args) throws Exception {
        // Parse parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Configure checkpointing
        env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        
        // Set state backend
        env.setStateBackend(new FsStateBackend("hdfs://namenode:9000/flink/checkpoints"));
        
        // Configure parallelism
        env.setParallelism(params.getInt("parallelism", 8));
        
        // Create Pulsar source
        PulsarSource<String> pulsarSource = PulsarSource.builder()
            .setServiceUrl(params.get("pulsar.url", "pulsar://pulsar:6650"))
            .setAdminUrl(params.get("pulsar.admin.url", "http://pulsar:8080"))
            .setStartCursor(StartCursor.latest())
            .setTopics(Arrays.asList(
                "user-activity-events",
                "transaction-events",
                "system-events",
                "security-events",
                "api-events"
            ))
            .setDeserializationSchema(new SimpleStringSchema())
            .setSubscriptionName("cep-processor")
            .build();
        
        // Create event stream
        DataStream<Event> eventStream = env
            .fromSource(pulsarSource, 
                       WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                           .withTimestampAssigner((event, timestamp) -> {
                               try {
                                   ObjectNode node = objectMapper.readValue(event, ObjectNode.class);
                                   return Instant.parse(node.get("timestamp").asText()).toEpochMilli();
                               } catch (Exception e) {
                                   return System.currentTimeMillis();
                               }
                           }),
                       "Event Source")
            .map(new EventParser())
            .filter(Objects::nonNull);
        
        // Key by entity ID
        KeyedStream<Event, String> keyedStream = eventStream
            .keyBy(event -> event.getEntityId());
        
        // Apply CEP patterns
        SingleOutputStreamOperator<Alert> alertStream = applyCEPPatterns(keyedStream);
        
        // Enrich alerts
        DataStream<Alert> enrichedAlerts = alertStream
            .keyBy(alert -> alert.getEntityId())
            .process(new AlertEnrichmentFunction());
        
        // Route alerts by severity
        enrichedAlerts
            .process(new AlertRouter())
            .name("Alert Router");
        
        // Get side outputs
        DataStream<Alert> fraudAlerts = enrichedAlerts.getSideOutput(FRAUD_ALERTS);
        DataStream<Alert> securityAlerts = enrichedAlerts.getSideOutput(SECURITY_ALERTS);
        DataStream<Alert> anomalyAlerts = enrichedAlerts.getSideOutput(ANOMALY_ALERTS);
        DataStream<Alert> criticalAlerts = enrichedAlerts.getSideOutput(CRITICAL_ALERTS);
        
        // Create sinks
        PulsarSink<String> alertSink = createPulsarSink(params, "cep-alerts");
        PulsarSink<String> criticalSink = createPulsarSink(params, "critical-alerts");
        
        // Write to sinks
        enrichedAlerts
            .map(alert -> objectMapper.writeValueAsString(alert))
            .sinkTo(alertSink)
            .name("Alert Sink");
        
        criticalAlerts
            .map(alert -> objectMapper.writeValueAsString(alert))
            .sinkTo(criticalSink)
            .name("Critical Alert Sink");
        
        // Execute job
        env.execute("Complex Event Processing - Pattern Detection");
    }
    
    /**
     * Apply all CEP patterns to the event stream
     */
    private static SingleOutputStreamOperator<Alert> applyCEPPatterns(KeyedStream<Event, String> keyedStream) {
        
        // Fraud Pattern 1: Velocity Check
        Pattern<Event, ?> velocityPattern = Pattern.<Event>begin("first")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) {
                    return "TRANSACTION".equals(event.getEventType());
                }
            })
            .times(10)
            .within(Time.minutes(5));
        
        PatternStream<Event> velocityPatternStream = CEP.pattern(keyedStream, velocityPattern);
        DataStream<Alert> velocityAlerts = velocityPatternStream.select(
            new VelocityCheckPatternSelect()
        );
        
        // Fraud Pattern 2: Account Takeover
        Pattern<Event, ?> accountTakeoverPattern = Pattern.<Event>begin("login_failure")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) {
                    return "LOGIN_FAILED".equals(event.getEventType());
                }
            })
            .times(3).consecutive()
            .followedBy("login_success")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) {
                    return "LOGIN_SUCCESS".equals(event.getEventType());
                }
            })
            .followedBy("suspicious_action")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) {
                    return Arrays.asList("PASSWORD_CHANGE", "EMAIL_CHANGE", "LARGE_WITHDRAWAL")
                        .contains(event.getEventType());
                }
            })
            .within(Time.minutes(30));
        
        PatternStream<Event> accountTakeoverStream = CEP.pattern(keyedStream, accountTakeoverPattern);
        DataStream<Alert> accountTakeoverAlerts = accountTakeoverStream.select(
            new AccountTakeoverPatternSelect()
        );
        
        // Security Pattern: Brute Force
        Pattern<Event, ?> bruteForcePattern = Pattern.<Event>begin("failed_auth")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) {
                    return Arrays.asList("LOGIN_FAILED", "API_AUTH_FAILED")
                        .contains(event.getEventType());
                }
            })
            .times(10)
            .within(Time.minutes(5));
        
        PatternStream<Event> bruteForceStream = CEP.pattern(keyedStream, bruteForcePattern);
        DataStream<Alert> bruteForceAlerts = bruteForceStream.select(
            new BruteForcePatternSelect()
        );
        
        // Anomaly Pattern: System Resource Exhaustion
        Pattern<Event, ?> resourceExhaustionPattern = Pattern.<Event>begin("high_usage")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) {
                    return "RESOURCE_METRIC".equals(event.getEventType()) &&
                           event.getMetrics().getOrDefault("usage_percent", 0.0) > 80;
                }
            })
            .times(5).consecutive()
            .followedBy("critical")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) {
                    return "RESOURCE_METRIC".equals(event.getEventType()) &&
                           event.getMetrics().getOrDefault("usage_percent", 0.0) > 95;
                }
            })
            .within(Time.minutes(10));
        
        PatternStream<Event> resourceExhaustionStream = CEP.pattern(keyedStream, resourceExhaustionPattern);
        DataStream<Alert> resourceAlerts = resourceExhaustionStream.select(
            new ResourceExhaustionPatternSelect()
        );
        
        // Union all alert streams
        return velocityAlerts
            .union(accountTakeoverAlerts)
            .union(bruteForceAlerts)
            .union(resourceAlerts);
    }
    
    /**
     * Pattern select function for velocity check
     */
    private static class VelocityCheckPatternSelect implements PatternSelectFunction<Event, Alert> {
        @Override
        public Alert select(Map<String, List<Event>> pattern) throws Exception {
            List<Event> events = pattern.get("first");
            
            double totalAmount = events.stream()
                .mapToDouble(e -> e.getMetrics().getOrDefault("amount", 0.0))
                .sum();
            
            return Alert.builder()
                .alertId(UUID.randomUUID().toString())
                .patternName("velocity_check")
                .severity(totalAmount > 100000 ? "CRITICAL" : "HIGH")
                .entityId(events.get(0).getEntityId())
                .timestamp(Instant.now())
                .eventCount(events.size())
                .riskScore(Math.min(1.0, totalAmount / 100000))
                .evidence(events.stream()
                    .limit(5)
                    .map(Event::toMap)
                    .collect(Collectors.toList()))
                .recommendedActions(Arrays.asList("freeze_account", "manual_review"))
                .build();
        }
    }
    
    /**
     * Pattern select function for account takeover
     */
    private static class AccountTakeoverPatternSelect implements PatternSelectFunction<Event, Alert> {
        @Override
        public Alert select(Map<String, List<Event>> pattern) throws Exception {
            List<Event> loginFailures = pattern.get("login_failure");
            Event loginSuccess = pattern.get("login_success").get(0);
            Event suspiciousAction = pattern.get("suspicious_action").get(0);
            
            return Alert.builder()
                .alertId(UUID.randomUUID().toString())
                .patternName("account_takeover")
                .severity("CRITICAL")
                .entityId(loginSuccess.getEntityId())
                .timestamp(Instant.now())
                .eventCount(loginFailures.size() + 2)
                .riskScore(0.95)
                .metadata(Map.of(
                    "failed_attempts", loginFailures.size(),
                    "suspicious_action", suspiciousAction.getEventType(),
                    "ip_address", suspiciousAction.getMetadata().get("ip_address")
                ))
                .recommendedActions(Arrays.asList("force_logout", "reset_password", "block_ip"))
                .build();
        }
    }
    
    /**
     * Alert enrichment function
     */
    private static class AlertEnrichmentFunction extends KeyedProcessFunction<String, Alert, Alert> {
        
        private transient ValueState<Integer> alertCountState;
        private transient IgniteCache<String, EntityProfile> entityCache;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            // Initialize state
            alertCountState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("alert-count", Integer.class)
            );
            
            // Initialize Ignite connection
            IgniteConfiguration cfg = new IgniteConfiguration();
            cfg.setClientMode(true);
            
            TcpDiscoverySpi spi = new TcpDiscoverySpi();
            TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
            ipFinder.setAddresses(Arrays.asList("ignite-0.ignite:47500", "ignite-1.ignite:47500"));
            spi.setIpFinder(ipFinder);
            cfg.setDiscoverySpi(spi);
            
            Ignite ignite = Ignition.start(cfg);
            entityCache = ignite.getOrCreateCache("entity_profiles");
        }
        
        @Override
        public void processElement(Alert alert, Context ctx, Collector<Alert> out) throws Exception {
            // Update alert count
            Integer count = alertCountState.value();
            if (count == null) count = 0;
            count++;
            alertCountState.update(count);
            
            // Get entity profile
            EntityProfile profile = entityCache.get(alert.getEntityId());
            if (profile == null) {
                profile = EntityProfile.defaultProfile();
            }
            
            // Enrich alert
            Alert enrichedAlert = alert.toBuilder()
                .entityProfile(profile)
                .historicalAlertCount(count)
                .enrichmentTimestamp(Instant.now())
                .finalRiskScore(calculateFinalRiskScore(alert, profile, count))
                .build();
            
            out.collect(enrichedAlert);
        }
        
        private double calculateFinalRiskScore(Alert alert, EntityProfile profile, int alertCount) {
            double baseScore = alert.getRiskScore();
            
            // Adjust based on entity trust score
            double trustModifier = (1.0 - profile.getTrustScore()) * 0.2;
            
            // Adjust based on alert history
            double historyModifier = Math.min(0.3, alertCount * 0.02);
            
            return Math.min(1.0, baseScore + trustModifier + historyModifier);
        }
    }
    
    /**
     * Route alerts to different outputs based on type and severity
     */
    private static class AlertRouter extends ProcessFunction<Alert, Alert> {
        @Override
        public void processElement(Alert alert, Context ctx, Collector<Alert> out) throws Exception {
            // Emit to main output
            out.collect(alert);
            
            // Route to side outputs
            if ("CRITICAL".equals(alert.getSeverity())) {
                ctx.output(CRITICAL_ALERTS, alert);
            }
            
            if (alert.getPatternName().contains("fraud") || 
                alert.getPatternName().contains("money_laundering")) {
                ctx.output(FRAUD_ALERTS, alert);
            } else if (alert.getPatternName().contains("brute_force") || 
                       alert.getPatternName().contains("security")) {
                ctx.output(SECURITY_ALERTS, alert);
            } else if (alert.getPatternName().contains("anomaly") || 
                       alert.getPatternName().contains("resource")) {
                ctx.output(ANOMALY_ALERTS, alert);
            }
        }
    }
    
    /**
     * Create Pulsar sink
     */
    private static PulsarSink<String> createPulsarSink(ParameterTool params, String topic) {
        return PulsarSink.builder()
            .setServiceUrl(params.get("pulsar.url", "pulsar://pulsar:6650"))
            .setAdminUrl(params.get("pulsar.admin.url", "http://pulsar:8080"))
            .setTopics(topic)
            .setSerializationSchema(new SimpleStringSchema())
            .build();
    }
}

/**
 * Event parser
 */
class EventParser implements MapFunction<String, Event> {
    private static final ObjectMapper mapper = new ObjectMapper();
    
    @Override
    public Event map(String json) throws Exception {
        try {
            return mapper.readValue(json, Event.class);
        } catch (Exception e) {
            return null;
        }
    }
} 