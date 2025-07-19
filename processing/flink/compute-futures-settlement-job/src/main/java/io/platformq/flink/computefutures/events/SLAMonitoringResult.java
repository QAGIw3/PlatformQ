package io.platformq.flink.computefutures.events;

import java.io.Serializable;
import java.util.List;

/**
 * Result of SLA monitoring for a settlement
 */
public class SLAMonitoringResult implements Serializable {
    
    private String settlementId;
    private long timestamp;
    private double uptimePercent;
    private double latencyMs;
    private double performanceScore;
    private double throughputGbps;
    private List<SLAViolation> violations;
    private boolean failoverRequired;
    
    // Constructors
    public SLAMonitoringResult() {}
    
    public SLAMonitoringResult(String settlementId, long timestamp, 
                              double uptimePercent, double latencyMs,
                              double performanceScore, double throughputGbps,
                              List<SLAViolation> violations, boolean failoverRequired) {
        this.settlementId = settlementId;
        this.timestamp = timestamp;
        this.uptimePercent = uptimePercent;
        this.latencyMs = latencyMs;
        this.performanceScore = performanceScore;
        this.throughputGbps = throughputGbps;
        this.violations = violations;
        this.failoverRequired = failoverRequired;
    }
    
    // Getters and setters
    public String getSettlementId() {
        return settlementId;
    }
    
    public void setSettlementId(String settlementId) {
        this.settlementId = settlementId;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    
    public double getUptimePercent() {
        return uptimePercent;
    }
    
    public void setUptimePercent(double uptimePercent) {
        this.uptimePercent = uptimePercent;
    }
    
    public double getLatencyMs() {
        return latencyMs;
    }
    
    public void setLatencyMs(double latencyMs) {
        this.latencyMs = latencyMs;
    }
    
    public double getPerformanceScore() {
        return performanceScore;
    }
    
    public void setPerformanceScore(double performanceScore) {
        this.performanceScore = performanceScore;
    }
    
    public double getThroughputGbps() {
        return throughputGbps;
    }
    
    public void setThroughputGbps(double throughputGbps) {
        this.throughputGbps = throughputGbps;
    }
    
    public List<SLAViolation> getViolations() {
        return violations;
    }
    
    public void setViolations(List<SLAViolation> violations) {
        this.violations = violations;
    }
    
    public boolean isFailoverRequired() {
        return failoverRequired;
    }
    
    public void setFailoverRequired(boolean failoverRequired) {
        this.failoverRequired = failoverRequired;
    }
} 