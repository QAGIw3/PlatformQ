package io.platformq.flink.computefutures.processors;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.PatternSelectFunction;

import io.platformq.flink.computefutures.events.*;
import io.platformq.flink.computefutures.models.*;
import io.platformq.flink.computefutures.patterns.SLAViolationPatterns;
import io.platformq.flink.computefutures.state.ProviderHealthState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Enhanced SLA monitoring processor with CEP pattern detection
 */
public class EnhancedSLAMonitoringProcessor extends KeyedProcessFunction<String, ComputeMetrics, SLAMonitoringResult> {
    
    private static final Logger LOG = LoggerFactory.getLogger(EnhancedSLAMonitoringProcessor.class);
    
    // Output tags
    public static final OutputTag<SLAEvent> SLA_EVENT_OUTPUT = new OutputTag<SLAEvent>("sla-events"){};
    public static final OutputTag<ProviderHealthEvent> HEALTH_EVENT_OUTPUT = new OutputTag<ProviderHealthEvent>("health-events"){};
    public static final OutputTag<FailoverTrigger> FAILOVER_TRIGGER_OUTPUT = new OutputTag<FailoverTrigger>("failover-triggers"){};
    
    // State management
    private transient ValueState<ProviderHealthState> providerHealthState;
    private transient MapState<String, Double> recentMetrics;
    private transient ValueState<Long> lastHealthCheckTime;
    
    // Configuration
    private final long healthCheckIntervalMs = TimeUnit.MINUTES.toMillis(1);
    private final double uptimeThreshold = 99.9;
    private final double performanceThreshold = 0.95;
    private final double latencyThresholdMs = 100.0;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        // Initialize state
        providerHealthState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("provider-health", ProviderHealthState.class)
        );
        
        recentMetrics = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("recent-metrics", String.class, Double.class)
        );
        
        lastHealthCheckTime = getRuntimeContext().getState(
            new ValueStateDescriptor<>("last-health-check", Types.LONG)
        );
    }
    
    @Override
    public void processElement(ComputeMetrics metrics, Context ctx, Collector<SLAMonitoringResult> out) throws Exception {
        String settlementId = metrics.getSettlementId();
        
        // Get or create provider health state
        ProviderHealthState healthState = providerHealthState.value();
        if (healthState == null) {
            healthState = new ProviderHealthState(settlementId);
            providerHealthState.update(healthState);
        }
        
        // Update health state with new metrics
        updateHealthState(healthState, metrics);
        
        // Check for SLA violations
        List<SLAViolation> violations = checkSLAViolations(metrics, healthState);
        
        // Emit SLA events for CEP processing
        emitSLAEvents(metrics, violations, ctx);
        
        // Update recent metrics for trend analysis
        updateRecentMetrics(metrics);
        
        // Determine if failover is needed
        boolean failoverRequired = shouldTriggerFailover(healthState, violations);
        
        // Create monitoring result
        SLAMonitoringResult result = new SLAMonitoringResult(
            settlementId,
            healthState.getHealthScore(),
            violations,
            failoverRequired,
            Instant.now()
        );
        
        // Emit result
        out.collect(result);
        
        // Emit failover trigger if needed
        if (failoverRequired) {
            emitFailoverTrigger(settlementId, healthState, violations, ctx);
        }
        
        // Schedule periodic health check
        scheduleHealthCheck(ctx);
        
        // Update state
        providerHealthState.update(healthState);
    }
    
    private void updateHealthState(ProviderHealthState healthState, ComputeMetrics metrics) {
        // Update metrics history
        healthState.addMetrics(metrics);
        
        // Calculate health score based on multiple factors
        double uptimeScore = metrics.getUptimePercent() / 100.0;
        double performanceScore = metrics.getPerformanceScore();
        double latencyScore = Math.max(0, 1.0 - (metrics.getLatencyMs() / 1000.0));
        double errorScore = Math.max(0, 1.0 - metrics.getErrorRate());
        
        // Weighted health score
        double healthScore = (uptimeScore * 0.4) + 
                           (performanceScore * 0.3) + 
                           (latencyScore * 0.2) + 
                           (errorScore * 0.1);
        
        healthState.setHealthScore(healthScore * 100);
        healthState.setLastUpdateTime(Instant.now());
        
        // Determine health status
        if (healthScore >= 0.95) {
            healthState.setStatus("HEALTHY");
        } else if (healthScore >= 0.8) {
            healthState.setStatus("DEGRADED");
        } else if (healthScore >= 0.5) {
            healthState.setStatus("CRITICAL");
        } else {
            healthState.setStatus("DOWN");
        }
    }
    
    private List<SLAViolation> checkSLAViolations(ComputeMetrics metrics, ProviderHealthState healthState) {
        List<SLAViolation> violations = new ArrayList<>();
        
        // Check uptime SLA
        if (metrics.getUptimePercent() < uptimeThreshold) {
            violations.add(new SLAViolation(
                metrics.getSettlementId(),
                "uptime",
                uptimeThreshold,
                metrics.getUptimePercent(),
                "CRITICAL"
            ));
        }
        
        // Check performance SLA
        if (metrics.getPerformanceScore() < performanceThreshold) {
            violations.add(new SLAViolation(
                metrics.getSettlementId(),
                "performance",
                performanceThreshold,
                metrics.getPerformanceScore(),
                "HIGH"
            ));
        }
        
        // Check latency SLA
        if (metrics.getLatencyMs() > latencyThresholdMs) {
            violations.add(new SLAViolation(
                metrics.getSettlementId(),
                "latency",
                latencyThresholdMs,
                metrics.getLatencyMs(),
                "MEDIUM"
            ));
        }
        
        // Check resource utilization
        if (metrics.getCpuUtilization() > 90.0 || metrics.getMemoryUtilization() > 90.0) {
            violations.add(new SLAViolation(
                metrics.getSettlementId(),
                "resource_exhaustion",
                90.0,
                Math.max(metrics.getCpuUtilization(), metrics.getMemoryUtilization()),
                "HIGH"
            ));
        }
        
        return violations;
    }
    
    private void emitSLAEvents(ComputeMetrics metrics, List<SLAViolation> violations, Context ctx) {
        // Emit events for each metric check
        ctx.output(SLA_EVENT_OUTPUT, new SLAEvent(
            metrics.getSettlementId(),
            "provider_" + metrics.getSettlementId(), // Extract provider ID
            "UPTIME_CHECK",
            uptimeThreshold,
            metrics.getUptimePercent(),
            violations.stream().anyMatch(v -> v.getViolationType().equals("uptime")) ? "uptime" : null
        ));
        
        ctx.output(SLA_EVENT_OUTPUT, new SLAEvent(
            metrics.getSettlementId(),
            "provider_" + metrics.getSettlementId(),
            "PERFORMANCE_CHECK",
            performanceThreshold,
            metrics.getPerformanceScore(),
            violations.stream().anyMatch(v -> v.getViolationType().equals("performance")) ? "performance" : null
        ));
        
        // Emit provider health event
        ProviderHealthState healthState = null;
        try {
            healthState = providerHealthState.value();
        } catch (Exception e) {
            LOG.error("Failed to get health state", e);
        }
        
        if (healthState != null) {
            ProviderHealthEvent healthEvent = new ProviderHealthEvent(
                "provider_" + metrics.getSettlementId(),
                "HEARTBEAT",
                healthState.getStatus(),
                "us-east-1" // TODO: Extract region from settlement
            );
            healthEvent.setHealthScore(healthState.getHealthScore());
            healthEvent.setMetrics(Map.of(
                "cpu", metrics.getCpuUtilization(),
                "memory", metrics.getMemoryUtilization(),
                "latency", metrics.getLatencyMs()
            ));
            
            ctx.output(HEALTH_EVENT_OUTPUT, healthEvent);
        }
    }
    
    private void updateRecentMetrics(ComputeMetrics metrics) throws Exception {
        // Store recent metrics for trend analysis
        recentMetrics.put("uptime_" + System.currentTimeMillis(), metrics.getUptimePercent());
        recentMetrics.put("performance_" + System.currentTimeMillis(), metrics.getPerformanceScore());
        recentMetrics.put("latency_" + System.currentTimeMillis(), metrics.getLatencyMs());
        
        // Clean up old metrics (keep last hour)
        long cutoffTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);
        List<String> keysToRemove = new ArrayList<>();
        
        for (Map.Entry<String, Double> entry : recentMetrics.entries()) {
            String key = entry.getKey();
            long timestamp = Long.parseLong(key.substring(key.lastIndexOf('_') + 1));
            if (timestamp < cutoffTime) {
                keysToRemove.add(key);
            }
        }
        
        for (String key : keysToRemove) {
            recentMetrics.remove(key);
        }
    }
    
    private boolean shouldTriggerFailover(ProviderHealthState healthState, List<SLAViolation> violations) {
        // Immediate failover for critical violations
        if (violations.stream().anyMatch(v -> v.getSeverity().equals("CRITICAL"))) {
            return true;
        }
        
        // Failover if health score is critically low
        if (healthState.getHealthScore() < 50.0) {
            return true;
        }
        
        // Failover if multiple high severity violations
        long highSeverityCount = violations.stream()
            .filter(v -> v.getSeverity().equals("HIGH"))
            .count();
        if (highSeverityCount >= 2) {
            return true;
        }
        
        // Failover if sustained degradation (check trend)
        try {
            return isProgressivelyDegrading();
        } catch (Exception e) {
            LOG.error("Error checking degradation trend", e);
            return false;
        }
    }
    
    private boolean isProgressivelyDegrading() throws Exception {
        // Analyze recent metrics for degradation trend
        List<Double> recentPerformance = new ArrayList<>();
        
        for (Map.Entry<String, Double> entry : recentMetrics.entries()) {
            if (entry.getKey().startsWith("performance_")) {
                recentPerformance.add(entry.getValue());
            }
        }
        
        if (recentPerformance.size() < 5) {
            return false; // Not enough data
        }
        
        // Check if performance is consistently declining
        boolean declining = true;
        for (int i = 1; i < recentPerformance.size(); i++) {
            if (recentPerformance.get(i) >= recentPerformance.get(i - 1)) {
                declining = false;
                break;
            }
        }
        
        return declining;
    }
    
    private void emitFailoverTrigger(String settlementId, ProviderHealthState healthState, 
                                    List<SLAViolation> violations, Context ctx) {
        FailoverTrigger trigger = new FailoverTrigger(
            settlementId,
            "provider_" + settlementId,
            healthState.getStatus(),
            violations,
            "Automatic failover triggered due to SLA violations",
            Instant.now()
        );
        
        ctx.output(FAILOVER_TRIGGER_OUTPUT, trigger);
        
        LOG.warn("Failover triggered for settlement {} due to {} violations", 
                settlementId, violations.size());
    }
    
    private void scheduleHealthCheck(Context ctx) throws Exception {
        Long lastCheck = lastHealthCheckTime.value();
        long currentTime = ctx.timerService().currentProcessingTime();
        
        if (lastCheck == null || currentTime - lastCheck > healthCheckIntervalMs) {
            // Schedule next health check
            ctx.timerService().registerProcessingTimeTimer(currentTime + healthCheckIntervalMs);
            lastHealthCheckTime.update(currentTime);
        }
    }
    
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<SLAMonitoringResult> out) throws Exception {
        // Periodic health check
        ProviderHealthState healthState = providerHealthState.value();
        if (healthState != null) {
            // Check if provider is unresponsive (no metrics for 5 minutes)
            long timeSinceLastUpdate = Instant.now().toEpochMilli() - 
                                     healthState.getLastUpdateTime().toEpochMilli();
            
            if (timeSinceLastUpdate > TimeUnit.MINUTES.toMillis(5)) {
                // Provider is unresponsive
                healthState.setStatus("DOWN");
                healthState.setHealthScore(0.0);
                
                // Trigger failover
                FailoverTrigger trigger = new FailoverTrigger(
                    ctx.getCurrentKey(),
                    "provider_" + ctx.getCurrentKey(),
                    "DOWN",
                    List.of(new SLAViolation(ctx.getCurrentKey(), "unresponsive", 1.0, 0.0, "CRITICAL")),
                    "Provider unresponsive for 5 minutes",
                    Instant.now()
                );
                
                ctx.output(FAILOVER_TRIGGER_OUTPUT, trigger);
                
                // Emit provider down event
                ProviderHealthEvent downEvent = new ProviderHealthEvent(
                    "provider_" + ctx.getCurrentKey(),
                    "PROVIDER_DOWN",
                    "DOWN",
                    "us-east-1"
                );
                ctx.output(HEALTH_EVENT_OUTPUT, downEvent);
            }
        }
        
        // Schedule next check
        ctx.timerService().registerProcessingTimeTimer(timestamp + healthCheckIntervalMs);
    }
} 