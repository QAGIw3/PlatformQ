package com.platformq.flink.cep.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Alert model for CEP output
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Alert implements Serializable {
    
    @JsonProperty("alert_id")
    private String alertId;
    
    @JsonProperty("pattern_name")
    private String patternName;
    
    @JsonProperty("severity")
    private String severity;  // CRITICAL, HIGH, MEDIUM, LOW
    
    @JsonProperty("entity_id")
    private String entityId;
    
    @JsonProperty("entity_ids")
    private List<String> entityIds;
    
    @JsonProperty("timestamp")
    private Instant timestamp;
    
    @JsonProperty("event_count")
    private int eventCount;
    
    @JsonProperty("risk_score")
    private double riskScore;
    
    @JsonProperty("final_risk_score")
    private double finalRiskScore;
    
    @JsonProperty("evidence")
    private List<Map<String, Object>> evidence;
    
    @JsonProperty("metadata")
    private Map<String, Object> metadata;
    
    @JsonProperty("recommended_actions")
    private List<String> recommendedActions;
    
    @JsonProperty("entity_profile")
    private EntityProfile entityProfile;
    
    @JsonProperty("historical_alert_count")
    private int historicalAlertCount;
    
    @JsonProperty("enrichment_timestamp")
    private Instant enrichmentTimestamp;
    
    @JsonProperty("alert_context")
    private AlertContext alertContext;
    
    /**
     * Alert context with additional information
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AlertContext implements Serializable {
        private String alertTrend;  // INCREASING, STABLE, DECREASING
        private boolean isRepeatPattern;
        private int recentAlerts24h;
        private List<String> relatedAlertIds;
        private Map<String, String> additionalContext;
    }
    
    /**
     * Check if alert is critical
     */
    public boolean isCritical() {
        return "CRITICAL".equals(severity);
    }
    
    /**
     * Check if alert is high risk
     */
    public boolean isHighRisk() {
        return finalRiskScore > 0.8 || riskScore > 0.8;
    }
    
    /**
     * Get primary entity ID
     */
    public String getPrimaryEntityId() {
        if (entityId != null) return entityId;
        if (entityIds != null && !entityIds.isEmpty()) return entityIds.get(0);
        return "unknown";
    }
} 