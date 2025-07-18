package com.platformq.flink.quality.models;

import java.io.Serializable;
import java.util.Map;

/**
 * Data drift detection result
 */
public class DriftResult implements Serializable {
    
    private String datasetId;
    private String driftId;
    private String driftType;
    private Double driftScore;
    private Map<String, Double> featureDrifts;
    private Long windowStart;
    private Long windowEnd;
    private String recommendation;
    
    // Getters and setters
    public String getDatasetId() {
        return datasetId;
    }
    
    public void setDatasetId(String datasetId) {
        this.datasetId = datasetId;
    }
    
    public String getDriftId() {
        return driftId;
    }
    
    public void setDriftId(String driftId) {
        this.driftId = driftId;
    }
    
    public String getDriftType() {
        return driftType;
    }
    
    public void setDriftType(String driftType) {
        this.driftType = driftType;
    }
    
    public Double getDriftScore() {
        return driftScore;
    }
    
    public void setDriftScore(Double driftScore) {
        this.driftScore = driftScore;
    }
    
    public Map<String, Double> getFeatureDrifts() {
        return featureDrifts;
    }
    
    public void setFeatureDrifts(Map<String, Double> featureDrifts) {
        this.featureDrifts = featureDrifts;
    }
    
    public Long getWindowStart() {
        return windowStart;
    }
    
    public void setWindowStart(Long windowStart) {
        this.windowStart = windowStart;
    }
    
    public Long getWindowEnd() {
        return windowEnd;
    }
    
    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }
    
    public String getRecommendation() {
        return recommendation;
    }
    
    public void setRecommendation(String recommendation) {
        this.recommendation = recommendation;
    }
} 