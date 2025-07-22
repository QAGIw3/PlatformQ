package com.platformq.flink.quality.models;

import java.io.Serializable;

/**
 * Represents a quality anomaly detected in a dataset
 */
public class QualityAnomaly implements Serializable {
    private String anomalyId;
    private String datasetId;
    private String anomalyType;
    private String severity;
    private double anomalyScore;
    private String description;
    private long detectedAt;
    private String affectedColumn;
    private long affectedRecords;
    
    public QualityAnomaly() {}
    
    public QualityAnomaly(String datasetId, String anomalyType, String severity, double anomalyScore) {
        this.anomalyId = "anomaly_" + System.currentTimeMillis();
        this.datasetId = datasetId;
        this.anomalyType = anomalyType;
        this.severity = severity;
        this.anomalyScore = anomalyScore;
        this.detectedAt = System.currentTimeMillis();
    }
    
    // Getters and setters
    public String getAnomalyId() {
        return anomalyId;
    }
    
    public void setAnomalyId(String anomalyId) {
        this.anomalyId = anomalyId;
    }
    
    public String getDatasetId() {
        return datasetId;
    }
    
    public void setDatasetId(String datasetId) {
        this.datasetId = datasetId;
    }
    
    public String getAnomalyType() {
        return anomalyType;
    }
    
    public void setAnomalyType(String anomalyType) {
        this.anomalyType = anomalyType;
    }
    
    public String getSeverity() {
        return severity;
    }
    
    public void setSeverity(String severity) {
        this.severity = severity;
    }
    
    public double getAnomalyScore() {
        return anomalyScore;
    }
    
    public void setAnomalyScore(double anomalyScore) {
        this.anomalyScore = anomalyScore;
    }
    
    public String getDescription() {
        return description;
    }
    
    public void setDescription(String description) {
        this.description = description;
    }
    
    public long getDetectedAt() {
        return detectedAt;
    }
    
    public void setDetectedAt(long detectedAt) {
        this.detectedAt = detectedAt;
    }
    
    public String getAffectedColumn() {
        return affectedColumn;
    }
    
    public void setAffectedColumn(String affectedColumn) {
        this.affectedColumn = affectedColumn;
    }
    
    public long getAffectedRecords() {
        return affectedRecords;
    }
    
    public void setAffectedRecords(long affectedRecords) {
        this.affectedRecords = affectedRecords;
    }
} 