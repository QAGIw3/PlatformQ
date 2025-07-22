package io.platformq.flink.computefutures.events;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;

/**
 * Event representing the initiation of a compute futures settlement
 */
public class ComputeSettlementEvent implements Serializable {
    
    private String settlementId;
    private String contractId;
    private String buyerId;
    private String providerId;
    private String resourceType;
    private BigDecimal quantity;
    private long deliveryStart;  // Unix timestamp
    private int durationHours;
    private long timestamp;
    
    // Constructors
    public ComputeSettlementEvent() {}
    
    public ComputeSettlementEvent(String settlementId, String contractId, String buyerId, 
                                 String providerId, String resourceType, BigDecimal quantity,
                                 long deliveryStart, int durationHours) {
        this.settlementId = settlementId;
        this.contractId = contractId;
        this.buyerId = buyerId;
        this.providerId = providerId;
        this.resourceType = resourceType;
        this.quantity = quantity;
        this.deliveryStart = deliveryStart;
        this.durationHours = durationHours;
        this.timestamp = Instant.now().toEpochMilli();
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
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
} 