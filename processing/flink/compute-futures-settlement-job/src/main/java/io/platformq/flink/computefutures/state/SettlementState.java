package io.platformq.flink.computefutures.state;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * State for a compute futures settlement
 */
public class SettlementState implements Serializable {
    
    private String settlementId;
    private String contractId;
    private String buyerId;
    private String providerId;
    private String resourceType;
    private BigDecimal quantity;
    private long deliveryStart;
    private int durationHours;
    private String status;
    private SLARequirements slaRequirements;
    private String allocationId;
    private String failoverProvider;
    private boolean failoverUsed;
    private BigDecimal penaltyAmount;
    private int violationCount;
    
    // Constructors
    public SettlementState() {}
    
    public SettlementState(String settlementId) {
        this.settlementId = settlementId;
        this.status = "initiated";
        this.penaltyAmount = BigDecimal.ZERO;
        this.violationCount = 0;
        this.failoverUsed = false;
    }
    
    // Getters and setters
    public String getSettlementId() {
        return settlementId;
    }
    
    public void setSettlementId(String settlementId) {
        this.settlementId = settlementId;
    }
    
    public String getContractId() {
        return contractId;
    }
    
    public void setContractId(String contractId) {
        this.contractId = contractId;
    }
    
    public String getBuyerId() {
        return buyerId;
    }
    
    public void setBuyerId(String buyerId) {
        this.buyerId = buyerId;
    }
    
    public String getProviderId() {
        return providerId;
    }
    
    public void setProviderId(String providerId) {
        this.providerId = providerId;
    }
    
    public String getResourceType() {
        return resourceType;
    }
    
    public void setResourceType(String resourceType) {
        this.resourceType = resourceType;
    }
    
    public BigDecimal getQuantity() {
        return quantity;
    }
    
    public void setQuantity(BigDecimal quantity) {
        this.quantity = quantity;
    }
    
    public long getDeliveryStart() {
        return deliveryStart;
    }
    
    public void setDeliveryStart(long deliveryStart) {
        this.deliveryStart = deliveryStart;
    }
    
    public int getDurationHours() {
        return durationHours;
    }
    
    public void setDurationHours(int durationHours) {
        this.durationHours = durationHours;
    }
    
    public String getStatus() {
        return status;
    }
    
    public void setStatus(String status) {
        this.status = status;
    }
    
    public SLARequirements getSlaRequirements() {
        return slaRequirements;
    }
    
    public void setSlaRequirements(SLARequirements slaRequirements) {
        this.slaRequirements = slaRequirements;
    }
    
    public String getAllocationId() {
        return allocationId;
    }
    
    public void setAllocationId(String allocationId) {
        this.allocationId = allocationId;
    }
    
    public String getFailoverProvider() {
        return failoverProvider;
    }
    
    public void setFailoverProvider(String failoverProvider) {
        this.failoverProvider = failoverProvider;
    }
    
    public boolean isFailoverUsed() {
        return failoverUsed;
    }
    
    public void setFailoverUsed(boolean failoverUsed) {
        this.failoverUsed = failoverUsed;
    }
    
    public BigDecimal getPenaltyAmount() {
        return penaltyAmount;
    }
    
    public void setPenaltyAmount(BigDecimal penaltyAmount) {
        this.penaltyAmount = penaltyAmount;
    }
    
    public int getViolationCount() {
        return violationCount;
    }
    
    public void setViolationCount(int violationCount) {
        this.violationCount = violationCount;
    }
    
    public void incrementViolationCount() {
        this.violationCount++;
    }
    
    public void addPenalty(BigDecimal penalty) {
        this.penaltyAmount = this.penaltyAmount.add(penalty);
    }
} 