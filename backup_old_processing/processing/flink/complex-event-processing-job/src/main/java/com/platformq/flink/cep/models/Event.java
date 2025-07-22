package com.platformq.flink.cep.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Event model for CEP processing
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Event implements Serializable {
    
    @JsonProperty("event_id")
    private String eventId;
    
    @JsonProperty("event_type")
    private String eventType;
    
    @JsonProperty("entity_id")
    private String entityId;
    
    @JsonProperty("entity_type")
    private String entityType;
    
    @JsonProperty("timestamp")
    private String timestamp;
    
    @JsonProperty("tenant_id")
    private String tenantId;
    
    @JsonProperty("user_id")
    private String userId;
    
    @JsonProperty("session_id")
    private String sessionId;
    
    @JsonProperty("source_service")
    private String sourceService;
    
    @JsonProperty("metadata")
    @Builder.Default
    private Map<String, String> metadata = new HashMap<>();
    
    @JsonProperty("metrics")
    @Builder.Default
    private Map<String, Double> metrics = new HashMap<>();
    
    @JsonProperty("tags")
    private String[] tags;
    
    @JsonProperty("ip_address")
    private String ipAddress;
    
    @JsonProperty("user_agent")
    private String userAgent;
    
    @JsonProperty("location")
    private Location location;
    
    @JsonProperty("risk_indicators")
    private String[] riskIndicators;
    
    /**
     * Convert to map for serialization
     */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("event_id", eventId);
        map.put("event_type", eventType);
        map.put("entity_id", entityId);
        map.put("entity_type", entityType);
        map.put("timestamp", timestamp);
        map.put("tenant_id", tenantId);
        map.put("user_id", userId);
        map.put("metadata", metadata);
        map.put("metrics", metrics);
        return map;
    }
    
    /**
     * Get timestamp as Instant
     */
    public Instant getTimestampInstant() {
        try {
            return Instant.parse(timestamp);
        } catch (Exception e) {
            return Instant.now();
        }
    }
    
    /**
     * Check if event has risk indicator
     */
    public boolean hasRiskIndicator(String indicator) {
        if (riskIndicators == null) return false;
        for (String ri : riskIndicators) {
            if (indicator.equals(ri)) return true;
        }
        return false;
    }
    
    /**
     * Get metric value with default
     */
    public double getMetricValue(String key, double defaultValue) {
        return metrics.getOrDefault(key, defaultValue);
    }
    
    /**
     * Location data
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Location implements Serializable {
        private String country;
        private String city;
        private Double latitude;
        private Double longitude;
        private String timezone;
    }
} 