package io.platformq.flink.computefutures.events;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * Compute resource metrics for SLA monitoring
 */
public class ComputeMetrics implements Serializable {
    
    private String settlementId;
    private String metricType;
    private double uptimePercent;
    private double latencyMs;
    private double performanceScore;
    private double throughputGbps;
    private double errorRate;
    private double cpuUtilization;
    private double memoryUtilization;
    private String regionPair;  // For latency futures
    private long timestamp;
    
    // Constructors
    public ComputeMetrics() {}
    
    public ComputeMetrics(String settlementId, String metricType) {
        this.settlementId = settlementId;
        this.metricType = metricType;
        this.timestamp = System.currentTimeMillis();
    }
    
    // Getters and setters
    public String getSettlementId() {
        return settlementId;
    }
    
    public void setSettlementId(String settlementId) {
        this.settlementId = settlementId;
    }
    
    public String getMetricType() {
        return metricType;
    }
    
    public void setMetricType(String metricType) {
        this.metricType = metricType;
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
    
    public double getErrorRate() {
        return errorRate;
    }
    
    public void setErrorRate(double errorRate) {
        this.errorRate = errorRate;
    }
    
    public double getCpuUtilization() {
        return cpuUtilization;
    }
    
    public void setCpuUtilization(double cpuUtilization) {
        this.cpuUtilization = cpuUtilization;
    }
    
    public double getMemoryUtilization() {
        return memoryUtilization;
    }
    
    public void setMemoryUtilization(double memoryUtilization) {
        this.memoryUtilization = memoryUtilization;
    }
    
    public String getRegionPair() {
        return regionPair;
    }
    
    public void setRegionPair(String regionPair) {
        this.regionPair = regionPair;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
} 