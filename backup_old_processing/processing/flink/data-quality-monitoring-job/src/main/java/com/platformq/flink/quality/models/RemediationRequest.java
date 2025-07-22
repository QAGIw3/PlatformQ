package com.platformq.flink.quality.models;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Request for automated data quality remediation
 */
public class RemediationRequest implements Serializable {
    private String requestId;
    private String datasetId;
    private String planId;
    private List<String> actions;
    private String severity;
    private List<Map<String, Object>> qualityIssues;
    private long timestamp;
    private Map<String, Object> metadata;
    
    public RemediationRequest() {}
    
    public RemediationRequest(String datasetId, List<String> actions, String severity) {
        this.requestId = "rem_" + System.currentTimeMillis();
        this.datasetId = datasetId;
        this.actions = actions;
        this.severity = severity;
        this.timestamp = System.currentTimeMillis();
    }
    
    // Getters and setters
    public String getRequestId() {
        return requestId;
    }
    
    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }
    
    public String getDatasetId() {
        return datasetId;
    }
    
    public void setDatasetId(String datasetId) {
        this.datasetId = datasetId;
    }
    
    public String getPlanId() {
        return planId;
    }
    
    public void setPlanId(String planId) {
        this.planId = planId;
    }
    
    public List<String> getActions() {
        return actions;
    }
    
    public void setActions(List<String> actions) {
        this.actions = actions;
    }
    
    public String getSeverity() {
        return severity;
    }
    
    public void setSeverity(String severity) {
        this.severity = severity;
    }
    
    public List<Map<String, Object>> getQualityIssues() {
        return qualityIssues;
    }
    
    public void setQualityIssues(List<Map<String, Object>> qualityIssues) {
        this.qualityIssues = qualityIssues;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    
    public Map<String, Object> getMetadata() {
        return metadata;
    }
    
    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }
} 