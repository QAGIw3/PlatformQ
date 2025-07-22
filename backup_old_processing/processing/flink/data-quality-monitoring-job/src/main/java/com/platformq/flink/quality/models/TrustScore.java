package com.platformq.flink.quality.models;

import java.io.Serializable;
import java.util.Map;

/**
 * Trust score from graph intelligence
 */
public class TrustScore implements Serializable {
    
    private String entityId;
    private String entityType;
    private Double score;
    private Double confidence;
    private Long timestamp;
    private Map<String, Double> dimensionScores;
    private String source;
    
    // Getters and setters
    public String getEntityId() {
        return entityId;
    }
    
    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }
    
    public String getEntityType() {
        return entityType;
    }
    
    public void setEntityType(String entityType) {
        this.entityType = entityType;
    }
    
    public Double getScore() {
        return score;
    }
    
    public void setScore(Double score) {
        this.score = score;
    }
    
    public Double getConfidence() {
        return confidence;
    }
    
    public void setConfidence(Double confidence) {
        this.confidence = confidence;
    }
    
    public Long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
    
    public Map<String, Double> getDimensionScores() {
        return dimensionScores;
    }
    
    public void setDimensionScores(Map<String, Double> dimensionScores) {
        this.dimensionScores = dimensionScores;
    }
    
    public String getSource() {
        return source;
    }
    
    public void setSource(String source) {
        this.source = source;
    }
} 