package com.platformq.flink.quality.models;

import java.io.Serializable;

/**
 * Anomaly detection result
 */
public class AnomalyResult implements Serializable {
    
    private String datasetId;
    private String anomalyId;
    private String anomalyType;
    private Double anomalyScore;
    private String description;
    private Long timestamp;
    
    // Getters and setters
    public String getDatasetId() {
        return datasetId;
    }
    
    public void setDatasetId(String datasetId) {
        this.datasetId = datasetId;
    }
    
    public String getAnomalyId() {
        return anomalyId;
    }
    
    public void setAnomalyId(String anomalyId) {
        this.anomalyId = anomalyId;
    }
    
    public String getAnomalyType() {
        return anomalyType;
    }
    
    public void setAnomalyType(String anomalyType) {
        this.anomalyType = anomalyType;
    }
    
    public Double getAnomalyScore() {
        return anomalyScore;
    }
    
    public void setAnomalyScore(Double anomalyScore) {
        this.anomalyScore = anomalyScore;
    }
    
    public String getDescription() {
        return description;
    }
    
    public void setDescription(String description) {
        this.description = description;
    }
    
    public Long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
} 