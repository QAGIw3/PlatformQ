package io.platformq.flink.computefutures.models;

import java.io.Serializable;
import java.time.Instant;

/**
 * Event representing an SLA check or violation
 */
public class SLAEvent implements Serializable {
    private String settlementId;
    private String providerId;
    private String eventType; // UPTIME_CHECK, PERFORMANCE_CHECK, LATENCY_CHECK
    private double expectedValue;
    private double actualValue;
    private String violationType; // null if no violation
    private Instant timestamp;
    private String region;
    
    public SLAEvent() {}
    
    public SLAEvent(String settlementId, String providerId, String eventType, 
                   double expectedValue, double actualValue, String violationType) {
        this.settlementId = settlementId;
        this.providerId = providerId;
        this.eventType = eventType;
        this.expectedValue = expectedValue;
        this.actualValue = actualValue;
        this.violationType = violationType;
        this.timestamp = Instant.now();
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
    
    public String getEventType() {
        return eventType;
    }
    
    public void setEventType(String eventType) {
        this.eventType = eventType;
    }
    
    public double getExpectedValue() {
        return expectedValue;
    }
    
    public void setExpectedValue(double expectedValue) {
        this.expectedValue = expectedValue;
    }
    
    public double getActualValue() {
        return actualValue;
    }
    
    public void setActualValue(double actualValue) {
        this.actualValue = actualValue;
    }
    
    public String getViolationType() {
        return violationType;
    }
    
    public void setViolationType(String violationType) {
        this.violationType = violationType;
    }
    
    public Instant getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }
    
    public String getRegion() {
        return region;
    }
    
    public void setRegion(String region) {
        this.region = region;
    }
} 