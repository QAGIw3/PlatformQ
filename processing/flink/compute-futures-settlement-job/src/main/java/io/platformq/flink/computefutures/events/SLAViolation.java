package io.platformq.flink.computefutures.events;

import java.io.Serializable;

/**
 * SLA violation event
 */
public class SLAViolation implements Serializable {
    
    private String settlementId;
    private String violationType;
    private String description;
    private double expectedValue;
    private double actualValue;
    private long timestamp;
    
    // Constructors
    public SLAViolation() {}
    
    public SLAViolation(String settlementId, String violationType, String description,
                       double expectedValue, double actualValue, long timestamp) {
        this.settlementId = settlementId;
        this.violationType = violationType;
        this.description = description;
        this.expectedValue = expectedValue;
        this.actualValue = actualValue;
        this.timestamp = timestamp;
    }
    
    // Getters and setters
    public String getSettlementId() {
        return settlementId;
    }
    
    public void setSettlementId(String settlementId) {
        this.settlementId = settlementId;
    }
    
    public String getViolationType() {
        return violationType;
    }
    
    public void setViolationType(String violationType) {
        this.violationType = violationType;
    }
    
    public String getDescription() {
        return description;
    }
    
    public void setDescription(String description) {
        this.description = description;
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
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    
    @Override
    public String toString() {
        return String.format("SLAViolation{settlementId='%s', type='%s', expected=%.2f, actual=%.2f}",
            settlementId, violationType, expectedValue, actualValue);
    }
} 