package io.platformq.flink.computefutures.state;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import io.platformq.flink.computefutures.events.ComputeMetrics;

/**
 * State for SLA monitoring of a settlement
 */
public class SLAMonitoringState implements Serializable {
    
    private String settlementId;
    private long startTime;
    private long lastMetricsTime;
    private Map<String, Integer> violationCounts;
    
    // Running averages
    private double totalUptime;
    private double totalLatency;
    private double totalPerformance;
    private double totalThroughput;
    private int metricsCount;
    
    // Current values
    private double currentUptime;
    private double currentLatency;
    private double currentPerformance;
    private double currentThroughput;
    
    // Constructors
    public SLAMonitoringState() {
        this.violationCounts = new HashMap<>();
        this.metricsCount = 0;
    }
    
    public SLAMonitoringState(String settlementId) {
        this();
        this.settlementId = settlementId;
        this.startTime = System.currentTimeMillis();
        this.lastMetricsTime = System.currentTimeMillis();
    }
    
    // Update methods
    public void updateAverages(ComputeMetrics metrics) {
        metricsCount++;
        totalUptime += metrics.getUptimePercent();
        totalLatency += metrics.getLatencyMs();
        totalPerformance += metrics.getPerformanceScore();
        totalThroughput += metrics.getThroughputGbps();
        
        currentUptime = metrics.getUptimePercent();
        currentLatency = metrics.getLatencyMs();
        currentPerformance = metrics.getPerformanceScore();
        currentThroughput = metrics.getThroughputGbps();
    }
    
    public void incrementViolationCount(String violationType) {
        violationCounts.put(violationType, violationCounts.getOrDefault(violationType, 0) + 1);
    }
    
    public void updateLastMetricsTime(long timestamp) {
        this.lastMetricsTime = timestamp;
    }
    
    // Getters
    public String getSettlementId() {
        return settlementId;
    }
    
    public long getStartTime() {
        return startTime;
    }
    
    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }
    
    public long getLastMetricsTime() {
        return lastMetricsTime;
    }
    
    public int getTotalViolationCount() {
        return violationCounts.values().stream().mapToInt(Integer::intValue).sum();
    }
    
    public int getViolationCount(String violationType) {
        return violationCounts.getOrDefault(violationType, 0);
    }
    
    public double getAverageUptime() {
        return metricsCount > 0 ? totalUptime / metricsCount : 100.0;
    }
    
    public double getAverageLatency() {
        return metricsCount > 0 ? totalLatency / metricsCount : 0.0;
    }
    
    public double getAveragePerformance() {
        return metricsCount > 0 ? totalPerformance / metricsCount : 1.0;
    }
    
    public double getAverageThroughput() {
        return metricsCount > 0 ? totalThroughput / metricsCount : 0.0;
    }
    
    public double getUptimePercent() {
        return currentUptime;
    }
    
    public int getMetricsCount() {
        return metricsCount;
    }
} 