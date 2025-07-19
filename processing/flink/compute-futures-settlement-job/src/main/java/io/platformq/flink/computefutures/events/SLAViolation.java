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
    private String severity; // CRITICAL, HIGH, MEDIUM, LOW
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
        this.severity = "MEDIUM"; // Default severity
    }
    
    public SLAViolation(String settlementId, String violationType, 
                       double expectedValue, double actualValue, String severity) {
        this.settlementId = settlementId;
        this.violationType = violationType;
        this.expectedValue = expectedValue;
        this.actualValue = actualValue;
        this.severity = severity;
        this.timestamp = System.currentTimeMillis();
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
    
    public String getSeverity() {
        return severity;
    }
    
    public void setSeverity(String severity) {
        this.severity = severity;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    
    @Override
    public String toString() {
        return String.format("SLAViolation{settlementId='%s', type='%s', severity='%s', expected=%.2f, actual=%.2f}",
            settlementId, violationType, severity, expectedValue, actualValue);
    }
} 