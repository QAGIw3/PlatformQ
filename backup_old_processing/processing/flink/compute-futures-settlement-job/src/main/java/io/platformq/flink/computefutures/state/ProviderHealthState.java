package io.platformq.flink.computefutures.state;

import io.platformq.flink.computefutures.events.ComputeMetrics;
import java.io.Serializable;
import java.time.Instant;
import java.util.LinkedList;
import java.util.Queue;

/**
 * State class for tracking provider health
 */
public class ProviderHealthState implements Serializable {
    private String settlementId;
    private String providerId;
    private String status; // HEALTHY, DEGRADED, CRITICAL, DOWN
    private double healthScore; // 0-100
    private Instant lastUpdateTime;
    private Queue<ComputeMetrics> metricsHistory;
    private int consecutiveFailures;
    private static final int MAX_HISTORY_SIZE = 100;
    
    public ProviderHealthState() {
        this.metricsHistory = new LinkedList<>();
    }
    
    public ProviderHealthState(String settlementId) {
        this.settlementId = settlementId;
        this.status = "HEALTHY";
        this.healthScore = 100.0;
        this.lastUpdateTime = Instant.now();
        this.metricsHistory = new LinkedList<>();
        this.consecutiveFailures = 0;
    }
    
    public void addMetrics(ComputeMetrics metrics) {
        metricsHistory.offer(metrics);
        if (metricsHistory.size() > MAX_HISTORY_SIZE) {
            metricsHistory.poll();
        }
    }
    
    // Getters and setters
    public String getSettlementId() {
        return settlementId;
    }
    
    public void setSettlementId(String settlementId) {
        this.settlementId = settlementId;
    }
    
    public String getProviderId() {
        return providerId;
    }
    
    public void setProviderId(String providerId) {
        this.providerId = providerId;
    }
    
    public String getStatus() {
        return status;
    }
    
    public void setStatus(String status) {
        this.status = status;
    }
    
    public double getHealthScore() {
        return healthScore;
    }
    
    public void setHealthScore(double healthScore) {
        this.healthScore = healthScore;
    }
    
    public Instant getLastUpdateTime() {
        return lastUpdateTime;
    }
    
    public void setLastUpdateTime(Instant lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }
    
    public Queue<ComputeMetrics> getMetricsHistory() {
        return metricsHistory;
    }
    
    public void setMetricsHistory(Queue<ComputeMetrics> metricsHistory) {
        this.metricsHistory = metricsHistory;
    }
    
    public int getConsecutiveFailures() {
        return consecutiveFailures;
    }
    
    public void setConsecutiveFailures(int consecutiveFailures) {
        this.consecutiveFailures = consecutiveFailures;
    }
    
    public void incrementFailures() {
        this.consecutiveFailures++;
    }
    
    public void resetFailures() {
        this.consecutiveFailures = 0;
    }
} 