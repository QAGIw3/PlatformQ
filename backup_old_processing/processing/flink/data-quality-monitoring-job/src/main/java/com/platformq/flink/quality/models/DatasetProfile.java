package com.platformq.flink.quality.models;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Dataset profile for tracking quality history
 */
public class DatasetProfile implements Serializable {
    
    private String datasetId;
    private Long firstSeen;
    private Long lastUpdated;
    private Long recordCount;
    private Map<String, Double> averageQualityScores;
    private Map<String, Object> statistics;
    
    public DatasetProfile() {
        this.averageQualityScores = new HashMap<>();
        this.statistics = new HashMap<>();
    }
    
    public DatasetProfile(String datasetId) {
        this();
        this.datasetId = datasetId;
        this.firstSeen = System.currentTimeMillis();
        this.lastUpdated = this.firstSeen;
        this.recordCount = 0L;
    }
    
    public void update(DatasetEvent event, QualityDimensions dimensions) {
        this.lastUpdated = System.currentTimeMillis();
        this.recordCount++;
        
        // Update average quality scores
        updateAverageScore("completeness", dimensions.getCompleteness());
        updateAverageScore("accuracy", dimensions.getAccuracy());
        updateAverageScore("consistency", dimensions.getConsistency());
        updateAverageScore("timeliness", dimensions.getTimeliness());
        updateAverageScore("validity", dimensions.getValidity());
        updateAverageScore("uniqueness", dimensions.getUniqueness());
    }
    
    private void updateAverageScore(String dimension, Double newScore) {
        if (newScore == null) return;
        
        Double currentAvg = averageQualityScores.getOrDefault(dimension, 0.0);
        Double newAvg = ((currentAvg * (recordCount - 1)) + newScore) / recordCount;
        averageQualityScores.put(dimension, newAvg);
    }
    
    // Getters and setters
    public String getDatasetId() {
        return datasetId;
    }
    
    public void setDatasetId(String datasetId) {
        this.datasetId = datasetId;
    }
    
    public Long getFirstSeen() {
        return firstSeen;
    }
    
    public void setFirstSeen(Long firstSeen) {
        this.firstSeen = firstSeen;
    }
    
    public Long getLastUpdated() {
        return lastUpdated;
    }
    
    public void setLastUpdated(Long lastUpdated) {
        this.lastUpdated = lastUpdated;
    }
    
    public Long getRecordCount() {
        return recordCount;
    }
    
    public void setRecordCount(Long recordCount) {
        this.recordCount = recordCount;
    }
    
    public Map<String, Double> getAverageQualityScores() {
        return averageQualityScores;
    }
    
    public void setAverageQualityScores(Map<String, Double> averageQualityScores) {
        this.averageQualityScores = averageQualityScores;
    }
    
    public Map<String, Object> getStatistics() {
        return statistics;
    }
    
    public void setStatistics(Map<String, Object> statistics) {
        this.statistics = statistics;
    }
} 