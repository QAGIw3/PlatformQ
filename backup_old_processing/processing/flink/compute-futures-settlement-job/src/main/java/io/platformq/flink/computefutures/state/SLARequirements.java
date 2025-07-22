package io.platformq.flink.computefutures.state;

import java.io.Serializable;

/**
 * SLA requirements for compute resources
 */
public class SLARequirements implements Serializable {
    
    private double minUptimePercent;
    private double maxLatencyMs;
    private double minPerformanceScore;
    private double minThroughputGbps;
    private double maxErrorRate;
    
    // Constructors
    public SLARequirements() {
        // Default values
        this.minUptimePercent = 99.9;
        this.maxLatencyMs = 50.0;
        this.minPerformanceScore = 0.95;
        this.minThroughputGbps = 1.0;
        this.maxErrorRate = 0.01;
    }
    
    public SLARequirements(double minUptimePercent, double maxLatencyMs, 
                          double minPerformanceScore, double minThroughputGbps,
                          double maxErrorRate) {
        this.minUptimePercent = minUptimePercent;
        this.maxLatencyMs = maxLatencyMs;
        this.minPerformanceScore = minPerformanceScore;
        this.minThroughputGbps = minThroughputGbps;
        this.maxErrorRate = maxErrorRate;
    }
    
    // Getters and setters
    public double getMinUptimePercent() {
        return minUptimePercent;
    }
    
    public void setMinUptimePercent(double minUptimePercent) {
        this.minUptimePercent = minUptimePercent;
    }
    
    public double getMaxLatencyMs() {
        return maxLatencyMs;
    }
    
    public void setMaxLatencyMs(double maxLatencyMs) {
        this.maxLatencyMs = maxLatencyMs;
    }
    
    public double getMinPerformanceScore() {
        return minPerformanceScore;
    }
    
    public void setMinPerformanceScore(double minPerformanceScore) {
        this.minPerformanceScore = minPerformanceScore;
    }
    
    public double getMinThroughputGbps() {
        return minThroughputGbps;
    }
    
    public void setMinThroughputGbps(double minThroughputGbps) {
        this.minThroughputGbps = minThroughputGbps;
    }
    
    public double getMaxErrorRate() {
        return maxErrorRate;
    }
    
    public void setMaxErrorRate(double maxErrorRate) {
        this.maxErrorRate = maxErrorRate;
    }
} 