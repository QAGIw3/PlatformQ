package io.platformq.flink.computefutures.models;

import io.platformq.flink.computefutures.events.SLAViolation;
import java.io.Serializable;
import java.time.Instant;
import java.util.List;

/**
 * Event representing a failover trigger
 */
public class FailoverTrigger implements Serializable {
    private String settlementId;
    private String providerId;
    private String providerStatus;
    private List<SLAViolation> violations;
    private String reason;
    private Instant timestamp;
    private String recommendedAction;
    
    public FailoverTrigger() {}
    
    public FailoverTrigger(String settlementId, String providerId, String providerStatus, 
                          List<SLAViolation> violations, String reason, Instant timestamp) {
        this.settlementId = settlementId;
        this.providerId = providerId;
        this.providerStatus = providerStatus;
        this.violations = violations;
        this.reason = reason;
        this.timestamp = timestamp;
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
    
    public String getProviderStatus() {
        return providerStatus;
    }
    
    public void setProviderStatus(String providerStatus) {
        this.providerStatus = providerStatus;
    }
    
    public List<SLAViolation> getViolations() {
        return violations;
    }
    
    public void setViolations(List<SLAViolation> violations) {
        this.violations = violations;
    }
    
    public String getReason() {
        return reason;
    }
    
    public void setReason(String reason) {
        this.reason = reason;
    }
    
    public Instant getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }
    
    public String getRecommendedAction() {
        return recommendedAction;
    }
    
    public void setRecommendedAction(String recommendedAction) {
        this.recommendedAction = recommendedAction;
    }
} 