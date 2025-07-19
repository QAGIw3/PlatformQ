package io.platformq.flink.computefutures.processors;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;

import io.platformq.flink.computefutures.events.*;
import io.platformq.flink.computefutures.state.*;

/**
 * SLA Monitoring Processor
 * 
 * Monitors compute metrics against SLA requirements and detects violations.
 * Handles:
 * - Uptime monitoring
 * - Latency threshold checks
 * - Performance score validation
 * - Throughput requirements
 * - Failover triggering
 */
public class SLAMonitoringProcessor extends CoProcessFunction<ComputeMetrics, SettlementState, SLAMonitoringResult> {
    
    private static final Logger LOG = LoggerFactory.getLogger(SLAMonitoringProcessor.class);
    
    // Output tags
    public static final OutputTag<SLAViolation> SLA_VIOLATION_OUTPUT = 
        new OutputTag<SLAViolation>("sla-violations", TypeInformation.of(SLAViolation.class));
    
    // State handles
    private ValueState<SettlementState> settlementStateHandle;
    private MapState<String, MetricsHistory> metricsHistoryHandle;
    private ValueState<SLAMonitoringState> monitoringStateHandle;
    private ListState<SLAViolation> violationHistoryHandle;
    
    // Configuration
    private static final int METRICS_HISTORY_SIZE = 100;
    private static final long METRICS_TIMEOUT_MS = 60000; // 1 minute
    private static final double UPTIME_THRESHOLD = 99.9;
    private static final double LATENCY_THRESHOLD_MS = 50.0;
    private static final double PERFORMANCE_THRESHOLD = 0.95;
    private static final double THROUGHPUT_THRESHOLD_GBPS = 1.0;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        // Initialize state
        settlementStateHandle = getRuntimeContext().getState(
            new ValueStateDescriptor<>("settlement-state", SettlementState.class));
            
        metricsHistoryHandle = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("metrics-history", 
                TypeInformation.of(String.class),
                TypeInformation.of(MetricsHistory.class)));
                
        monitoringStateHandle = getRuntimeContext().getState(
            new ValueStateDescriptor<>("monitoring-state", SLAMonitoringState.class));
            
        violationHistoryHandle = getRuntimeContext().getListState(
            new ListStateDescriptor<>("violation-history", SLAViolation.class));
    }
    
    @Override
    public void processElement1(ComputeMetrics metrics, Context ctx, Collector<SLAMonitoringResult> out) throws Exception {
        
        SettlementState settlement = settlementStateHandle.value();
        if (settlement == null) {
            LOG.warn("Received metrics for unknown settlement: {}", metrics.getSettlementId());
            return;
        }
        
        // Update metrics history
        updateMetricsHistory(metrics);
        
        // Get or create monitoring state
        SLAMonitoringState monitoringState = monitoringStateHandle.value();
        if (monitoringState == null) {
            monitoringState = new SLAMonitoringState(metrics.getSettlementId());
            monitoringStateHandle.update(monitoringState);
        }
        
        // Check SLA compliance
        List<SLAViolation> violations = checkSLACompliance(metrics, settlement, monitoringState);
        
        // Emit violations
        for (SLAViolation violation : violations) {
            ctx.output(SLA_VIOLATION_OUTPUT, violation);
            violationHistoryHandle.add(violation);
            monitoringState.incrementViolationCount(violation.getViolationType());
        }
        
        // Update monitoring state
        monitoringState.updateLastMetricsTime(metrics.getTimestamp());
        monitoringState.updateAverages(metrics);
        
        // Check if failover is required
        boolean failoverRequired = shouldTriggerFailover(monitoringState, violations);
        
        // Create monitoring result
        SLAMonitoringResult result = new SLAMonitoringResult(
            metrics.getSettlementId(),
            metrics.getTimestamp(),
            monitoringState.getUptimePercent(),
            monitoringState.getAverageLatency(),
            monitoringState.getAveragePerformance(),
            monitoringState.getAverageThroughput(),
            violations,
            failoverRequired
        );
        
        // Update state
        monitoringStateHandle.update(monitoringState);
        
        out.collect(result);
        
        // Set timer for metrics timeout
        ctx.timerService().registerProcessingTimeTimer(
            ctx.timerService().currentProcessingTime() + METRICS_TIMEOUT_MS);
    }
    
    @Override
    public void processElement2(SettlementState settlement, Context ctx, Collector<SLAMonitoringResult> out) throws Exception {
        // Update settlement state
        settlementStateHandle.update(settlement);
        
        // Initialize monitoring state if needed
        if (monitoringStateHandle.value() == null) {
            SLAMonitoringState monitoringState = new SLAMonitoringState(settlement.getSettlementId());
            monitoringState.setStartTime(settlement.getDeliveryStart());
            monitoringStateHandle.update(monitoringState);
        }
    }
    
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<SLAMonitoringResult> out) throws Exception {
        
        SLAMonitoringState monitoringState = monitoringStateHandle.value();
        if (monitoringState == null) {
            return;
        }
        
        // Check for metrics timeout
        long lastMetricsTime = monitoringState.getLastMetricsTime();
        if (timestamp - lastMetricsTime > METRICS_TIMEOUT_MS) {
            
            // Create timeout violation
            SLAViolation timeoutViolation = new SLAViolation(
                monitoringState.getSettlementId(),
                "metrics_timeout",
                "No metrics received for " + METRICS_TIMEOUT_MS + "ms",
                0.0,  // expected
                0.0,  // actual
                timestamp
            );
            
            ctx.output(SLA_VIOLATION_OUTPUT, timeoutViolation);
            
            // Trigger failover for metrics timeout
            SLAMonitoringResult result = new SLAMonitoringResult(
                monitoringState.getSettlementId(),
                timestamp,
                0.0,  // uptime
                9999.0,  // latency
                0.0,  // performance
                0.0,  // throughput
                Arrays.asList(timeoutViolation),
                true  // failover required
            );
            
            out.collect(result);
        }
    }
    
    private void updateMetricsHistory(ComputeMetrics metrics) throws Exception {
        MetricsHistory history = metricsHistoryHandle.get(metrics.getMetricType());
        if (history == null) {
            history = new MetricsHistory(METRICS_HISTORY_SIZE);
        }
        
        history.addMetric(metrics);
        metricsHistoryHandle.put(metrics.getMetricType(), history);
    }
    
    private List<SLAViolation> checkSLACompliance(
            ComputeMetrics metrics, 
            SettlementState settlement,
            SLAMonitoringState monitoringState) {
        
        List<SLAViolation> violations = new ArrayList<>();
        
        // Get SLA requirements
        SLARequirements requirements = settlement.getSlaRequirements();
        if (requirements == null) {
            // Use default requirements
            requirements = getDefaultSLARequirements();
        }
        
        // Check uptime
        if (metrics.getUptimePercent() < requirements.getMinUptimePercent()) {
            violations.add(new SLAViolation(
                metrics.getSettlementId(),
                "uptime",
                "Uptime below threshold",
                requirements.getMinUptimePercent(),
                metrics.getUptimePercent(),
                metrics.getTimestamp()
            ));
        }
        
        // Check latency
        if (metrics.getLatencyMs() > requirements.getMaxLatencyMs()) {
            violations.add(new SLAViolation(
                metrics.getSettlementId(),
                "latency",
                "Latency above threshold",
                requirements.getMaxLatencyMs(),
                metrics.getLatencyMs(),
                metrics.getTimestamp()
            ));
        }
        
        // Check performance score
        if (metrics.getPerformanceScore() < requirements.getMinPerformanceScore()) {
            violations.add(new SLAViolation(
                metrics.getSettlementId(),
                "performance",
                "Performance below threshold",
                requirements.getMinPerformanceScore(),
                metrics.getPerformanceScore(),
                metrics.getTimestamp()
            ));
        }
        
        // Check throughput
        if (metrics.getThroughputGbps() < requirements.getMinThroughputGbps()) {
            violations.add(new SLAViolation(
                metrics.getSettlementId(),
                "throughput",
                "Throughput below threshold",
                requirements.getMinThroughputGbps(),
                metrics.getThroughputGbps(),
                metrics.getTimestamp()
            ));
        }
        
        // Check error rate
        if (metrics.getErrorRate() > requirements.getMaxErrorRate()) {
            violations.add(new SLAViolation(
                metrics.getSettlementId(),
                "error_rate",
                "Error rate above threshold",
                requirements.getMaxErrorRate(),
                metrics.getErrorRate(),
                metrics.getTimestamp()
            ));
        }
        
        return violations;
    }
    
    private boolean shouldTriggerFailover(SLAMonitoringState state, List<SLAViolation> currentViolations) {
        
        // Immediate failover for critical violations
        for (SLAViolation violation : currentViolations) {
            if (isCriticalViolation(violation)) {
                LOG.warn("Critical SLA violation detected: {}", violation);
                return true;
            }
        }
        
        // Failover if too many violations
        int totalViolations = state.getTotalViolationCount();
        if (totalViolations >= 5) {
            LOG.warn("Too many SLA violations: {}", totalViolations);
            return true;
        }
        
        // Failover if sustained poor performance
        if (state.getAverageUptime() < 95.0 && state.getMetricsCount() > 10) {
            LOG.warn("Sustained poor uptime: {}", state.getAverageUptime());
            return true;
        }
        
        // Failover if provider is completely down
        if (state.getAverageUptime() == 0.0 && state.getMetricsCount() > 3) {
            LOG.warn("Provider appears to be down");
            return true;
        }
        
        return false;
    }
    
    private boolean isCriticalViolation(SLAViolation violation) {
        // Define critical violations
        switch (violation.getViolationType()) {
            case "uptime":
                return violation.getActualValue() < 90.0;  // Below 90% uptime
            case "latency":
                return violation.getActualValue() > 200.0;  // Above 200ms
            case "performance":
                return violation.getActualValue() < 0.5;  // Below 50% performance
            case "metrics_timeout":
                return true;  // Always critical
            default:
                return false;
        }
    }
    
    private SLARequirements getDefaultSLARequirements() {
        return new SLARequirements(
            UPTIME_THRESHOLD,
            LATENCY_THRESHOLD_MS,
            PERFORMANCE_THRESHOLD,
            THROUGHPUT_THRESHOLD_GBPS,
            0.01  // 1% error rate
        );
    }
} 