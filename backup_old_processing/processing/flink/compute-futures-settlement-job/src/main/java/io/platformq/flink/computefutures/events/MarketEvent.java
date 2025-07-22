package io.platformq.flink.computefutures.events;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * Market event for compute futures
 */
public class MarketEvent implements Serializable {
    
    private String eventId;
    private String eventType;  // bid, offer, trade, clearing
    private String resourceType;
    private BigDecimal quantity;
    private BigDecimal price;
    private String userId;
    private String marketId;
    private long timestamp;
    
    // Constructors
    public MarketEvent() {}
    
    public MarketEvent(String eventType, String resourceType, BigDecimal quantity, BigDecimal price) {
        this.eventType = eventType;
        this.resourceType = resourceType;
        this.quantity = quantity;
        this.price = price;
        this.timestamp = System.currentTimeMillis();
    }
    
    // Getters and setters
    public String getEventId() {
        return eventId;
    }
    
    public void setEventId(String eventId) {
        this.eventId = eventId;
    }
    
    public String getEventType() {
        return eventType;
    }
    
    public void setEventType(String eventType) {
        this.eventType = eventType;
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
    
    public BigDecimal getPrice() {
        return price;
    }
    
    public void setPrice(BigDecimal price) {
        this.price = price;
    }
    
    public String getUserId() {
        return userId;
    }
    
    public void setUserId(String userId) {
        this.userId = userId;
    }
    
    public String getMarketId() {
        return marketId;
    }
    
    public void setMarketId(String marketId) {
        this.marketId = marketId;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
} 