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
 * Entity profile for risk assessment
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class EntityProfile implements Serializable {
    
    @JsonProperty("entity_id")
    private String entityId;
    
    @JsonProperty("entity_type")
    private String entityType;
    
    @JsonProperty("trust_score")
    @Builder.Default
    private double trustScore = 0.5;
    
    @JsonProperty("risk_level")
    @Builder.Default
    private String riskLevel = "MEDIUM";  // LOW, MEDIUM, HIGH, CRITICAL
    
    @JsonProperty("account_age_days")
    @Builder.Default
    private int accountAgeDays = 0;
    
    @JsonProperty("total_transactions")
    @Builder.Default
    private long totalTransactions = 0;
    
    @JsonProperty("total_volume")
    @Builder.Default
    private double totalVolume = 0.0;
    
    @JsonProperty("failed_login_attempts")
    @Builder.Default
    private int failedLoginAttempts = 0;
    
    @JsonProperty("last_activity")
    private Instant lastActivity;
    
    @JsonProperty("creation_date")
    private Instant creationDate;
    
    @JsonProperty("verification_status")
    @Builder.Default
    private String verificationStatus = "UNVERIFIED";  // UNVERIFIED, PENDING, VERIFIED
    
    @JsonProperty("kyc_level")
    @Builder.Default
    private int kycLevel = 0;  // 0-3
    
    @JsonProperty("country")
    private String country;
    
    @JsonProperty("device_fingerprints")
    @Builder.Default
    private Map<String, DeviceInfo> deviceFingerprints = new HashMap<>();
    
    @JsonProperty("behavior_metrics")
    @Builder.Default
    private BehaviorMetrics behaviorMetrics = new BehaviorMetrics();
    
    @JsonProperty("tags")
    private String[] tags;
    
    /**
     * Device information
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DeviceInfo implements Serializable {
        private String deviceId;
        private String deviceType;
        private String operatingSystem;
        private Instant firstSeen;
        private Instant lastSeen;
        private int usageCount;
    }
    
    /**
     * Behavior metrics
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BehaviorMetrics implements Serializable {
        @Builder.Default
        private double avgTransactionAmount = 0.0;
        
        @Builder.Default
        private double avgTransactionsPerDay = 0.0;
        
        @Builder.Default
        private int uniqueIpAddresses = 0;
        
        @Builder.Default
        private int uniqueDevices = 0;
        
        @Builder.Default
        private double velocityScore = 0.0;
        
        @Builder.Default
        private Map<String, Integer> activityByHour = new HashMap<>();
    }
    
    /**
     * Create default profile for unknown entities
     */
    public static EntityProfile defaultProfile() {
        return EntityProfile.builder()
            .entityId("unknown")
            .entityType("unknown")
            .trustScore(0.3)
            .riskLevel("HIGH")
            .accountAgeDays(0)
            .verificationStatus("UNVERIFIED")
            .build();
    }
    
    /**
     * Check if entity is new
     */
    public boolean isNewEntity() {
        return accountAgeDays < 7;
    }
    
    /**
     * Check if entity is verified
     */
    public boolean isVerified() {
        return "VERIFIED".equals(verificationStatus);
    }
    
    /**
     * Check if entity is high risk
     */
    public boolean isHighRisk() {
        return "HIGH".equals(riskLevel) || "CRITICAL".equals(riskLevel);
    }
} 