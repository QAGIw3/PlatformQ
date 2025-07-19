package io.platformq.flink.computefutures.models;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;

/**
 * Event representing provider health status
 */
public class ProviderHealthEvent implements Serializable {
    private String providerId;
    private String eventType; // HEARTBEAT, PROVIDER_DOWN, PROVIDER_UP, CAPACITY_UPDATE
    private String status; // HEALTHY, DEGRADED, CRITICAL, DOWN
    private String region;
    private double healthScore; // 0-100
    private Map<String, Double> metrics; // cpu, memory, network, etc.
    private long availableCapacity;
    private Instant timestamp;
    
    public ProviderHealthEvent() {}
    
    public ProviderHealthEvent(String providerId, String eventType, String status, String region) {
        this.providerId = providerId;
        this.eventType = eventType;
        this.status = status;
        this.region = region;
        this.timestamp = Instant.now();
    }
    
    // Getters and setters
    public String getProviderId() {
        return providerId;
    }
    
    public void setProviderId(String providerId) {
        this.providerId = providerId;
    }
    
    public String getEventType() {
        return eventType;
    }
    
    public void setEventType(String eventType) {
        this.eventType = eventType;
    }
    
    public String getStatus() {
        return status;
    }
    
    public void setStatus(String status) {
        this.status = status;
    }
    
    public String getRegion() {
        return region;
    }
    
    public void setRegion(String region) {
        this.region = region;
    }
    
    public double getHealthScore() {
        return healthScore;
    }
    
    public void setHealthScore(double healthScore) {
        this.healthScore = healthScore;
    }
    
    public Map<String, Double> getMetrics() {
        return metrics;
    }
    
    public void setMetrics(Map<String, Double> metrics) {
        this.metrics = metrics;
    }
    
    public long getAvailableCapacity() {
        return availableCapacity;
    }
    
    public void setAvailableCapacity(long availableCapacity) {
        this.availableCapacity = availableCapacity;
    }
    
    public Instant getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }
} 