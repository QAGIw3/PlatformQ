package com.platformq.flink.quality.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.Serializable;

/**
 * Quality assessment result model
 */
public class QualityAssessment implements Serializable {
    
    @JsonProperty("assessment_id")
    private String assessmentId;
    
    @JsonProperty("dataset_id")
    private String datasetId;
    
    @JsonProperty("timestamp")
    private Long timestamp;
    
    @JsonProperty("dimensions")
    private QualityDimensions dimensions;
    
    @JsonProperty("overall_quality_score")
    private Double overallQualityScore;
    
    @JsonProperty("quality_issues")
    private ObjectNode qualityIssues;
    
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
    
    public QualityDimensions getDimensions() {
        return dimensions;
    }
    
    public void setDimensions(QualityDimensions dimensions) {
        this.dimensions = dimensions;
    }
    
    public Double getOverallQualityScore() {
        return overallQualityScore;
    }
    
    public void setOverallQualityScore(Double overallQualityScore) {
        this.overallQualityScore = overallQualityScore;
    }
    
    public ObjectNode getQualityIssues() {
        return qualityIssues;
    }
    
    public void setQualityIssues(ObjectNode qualityIssues) {
        this.qualityIssues = qualityIssues;
    }
} 