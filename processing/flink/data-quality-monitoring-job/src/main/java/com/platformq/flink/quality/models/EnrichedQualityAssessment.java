package com.platformq.flink.quality.models;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Quality assessment enriched with trust scores
 */
public class EnrichedQualityAssessment implements Serializable {
    
    private String assessmentId;
    private String datasetId;
    private Long timestamp;
    private Double overallQuality;
    private Double trustScore;
    private Double trustAdjustedQuality;
    private QualityDimensions dimensions;
    private List<String> issues;
    private Map<String, Object> metadata;
    
    // Getters and setters
    public String getAssessmentId() {
        return assessmentId;
    }
    
    public void setAssessmentId(String assessmentId) {
        this.assessmentId = assessmentId;
    }
    
    public String getDatasetId() {
        return datasetId;
    }
    
    public void setDatasetId(String datasetId) {
        this.datasetId = datasetId;
    }
    
    public Long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
    
    public Double getOverallQuality() {
        return overallQuality;
    }
    
    public void setOverallQuality(Double overallQuality) {
        this.overallQuality = overallQuality;
    }
    
    public Double getTrustScore() {
        return trustScore;
    }
    
    public void setTrustScore(Double trustScore) {
        this.trustScore = trustScore;
    }
    
    public Double getTrustAdjustedQuality() {
        return trustAdjustedQuality;
    }
    
    public void setTrustAdjustedQuality(Double trustAdjustedQuality) {
        this.trustAdjustedQuality = trustAdjustedQuality;
    }
    
    public QualityDimensions getDimensions() {
        return dimensions;
    }
    
    public void setDimensions(QualityDimensions dimensions) {
        this.dimensions = dimensions;
    }
    
    public List<String> getIssues() {
        return issues;
    }
    
    public void setIssues(List<String> issues) {
        this.issues = issues;
    }
    
    public Map<String, Object> getMetadata() {
        return metadata;
    }
    
    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }
} 