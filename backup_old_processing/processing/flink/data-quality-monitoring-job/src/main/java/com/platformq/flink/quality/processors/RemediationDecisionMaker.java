package com.platformq.flink.quality.processors;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.platformq.flink.quality.models.QualityAnomaly;
import com.platformq.flink.quality.models.RemediationRequest;

import java.util.*;

/**
 * Decides on automated remediation actions based on quality anomalies
 */
public class RemediationDecisionMaker extends KeyedProcessFunction<String, QualityAnomaly, RemediationRequest> {
    
    private static final Logger LOG = LoggerFactory.getLogger(RemediationDecisionMaker.class);
    
    private transient ValueState<RemediationHistory> historyState;
    
    // Remediation rules
    private static final Map<String, List<String>> REMEDIATION_RULES = new HashMap<>();
    
    static {
        // Define remediation actions for different anomaly types
        REMEDIATION_RULES.put("missing_values", Arrays.asList("fill_missing", "notify_owner"));
        REMEDIATION_RULES.put("duplicate_records", Arrays.asList("remove_duplicates"));
        REMEDIATION_RULES.put("schema_violation", Arrays.asList("validate_schema", "quarantine_data"));
        REMEDIATION_RULES.put("outliers", Arrays.asList("clean_data"));
        REMEDIATION_RULES.put("data_drift", Arrays.asList("create_derived_dataset", "notify_owner"));
        REMEDIATION_RULES.put("format_inconsistency", Arrays.asList("standardize_format"));
        REMEDIATION_RULES.put("critical_corruption", Arrays.asList("rollback_version", "request_reupload"));
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<RemediationHistory> descriptor = 
            new ValueStateDescriptor<>("remediation-history", RemediationHistory.class);
        historyState = getRuntimeContext().getState(descriptor);
    }
    
    @Override
    public void processElement(QualityAnomaly anomaly, Context ctx, Collector<RemediationRequest> out) throws Exception {
        try {
            String datasetId = anomaly.getDatasetId();
            
            // Get or create history
            RemediationHistory history = historyState.value();
            if (history == null) {
                history = new RemediationHistory(datasetId);
            }
            
            // Check if we should trigger remediation
            if (shouldTriggerRemediation(anomaly, history)) {
                // Determine remediation actions
                List<String> actions = determineRemediationActions(anomaly);
                
                if (!actions.isEmpty()) {
                    // Create remediation request
                    RemediationRequest request = new RemediationRequest(
                        datasetId,
                        actions,
                        anomaly.getSeverity()
                    );
                    
                    // Add quality issues
                    List<Map<String, Object>> issues = new ArrayList<>();
                    Map<String, Object> issue = new HashMap<>();
                    issue.put("type", anomaly.getAnomalyType());
                    issue.put("severity", anomaly.getSeverity());
                    issue.put("score", anomaly.getAnomalyScore());
                    issue.put("description", anomaly.getDescription());
                    issues.add(issue);
                    request.setQualityIssues(issues);
                    
                    // Add metadata
                    Map<String, Object> metadata = new HashMap<>();
                    metadata.put("anomaly_id", anomaly.getAnomalyId());
                    metadata.put("detection_time", anomaly.getDetectedAt());
                    metadata.put("auto_triggered", true);
                    request.setMetadata(metadata);
                    
                    // Emit remediation request
                    out.collect(request);
                    
                    // Update history
                    history.addRemediationAttempt(anomaly.getAnomalyType(), System.currentTimeMillis());
                    historyState.update(history);
                    
                    LOG.info("Triggered remediation for dataset {}: actions={}", datasetId, actions);
                }
            }
            
        } catch (Exception e) {
            LOG.error("Error in remediation decision making", e);
        }
    }
    
    private boolean shouldTriggerRemediation(QualityAnomaly anomaly, RemediationHistory history) {
        // Don't trigger if recently remediated
        Long lastRemediation = history.getLastRemediation(anomaly.getAnomalyType());
        if (lastRemediation != null) {
            long timeSinceLastRemediation = System.currentTimeMillis() - lastRemediation;
            if (timeSinceLastRemediation < 3600000) { // 1 hour cooldown
                LOG.debug("Skipping remediation - too recent: {} minutes ago", 
                    timeSinceLastRemediation / 60000);
                return false;
            }
        }
        
        // Check severity threshold
        if ("LOW".equals(anomaly.getSeverity())) {
            return false; // Don't auto-remediate low severity issues
        }
        
        // Check confidence score
        if (anomaly.getAnomalyScore() < 0.8) {
            return false; // Need high confidence for auto-remediation
        }
        
        return true;
    }
    
    private List<String> determineRemediationActions(QualityAnomaly anomaly) {
        String anomalyType = anomaly.getAnomalyType();
        
        // Get actions from rules
        List<String> actions = REMEDIATION_RULES.get(anomalyType);
        if (actions == null) {
            // Default actions for unknown anomaly types
            actions = Arrays.asList("notify_owner");
        }
        
        // Filter actions based on severity
        if ("CRITICAL".equals(anomaly.getSeverity())) {
            // For critical issues, always include notification
            if (!actions.contains("notify_owner")) {
                actions = new ArrayList<>(actions);
                actions.add("notify_owner");
            }
        }
        
        return actions;
    }
    
    /**
     * Tracks remediation history for a dataset
     */
    public static class RemediationHistory {
        private String datasetId;
        private Map<String, List<Long>> remediationAttempts;
        
        public RemediationHistory() {
            this.remediationAttempts = new HashMap<>();
        }
        
        public RemediationHistory(String datasetId) {
            this.datasetId = datasetId;
            this.remediationAttempts = new HashMap<>();
        }
        
        public void addRemediationAttempt(String anomalyType, long timestamp) {
            remediationAttempts.computeIfAbsent(anomalyType, k -> new ArrayList<>()).add(timestamp);
        }
        
        public Long getLastRemediation(String anomalyType) {
            List<Long> attempts = remediationAttempts.get(anomalyType);
            if (attempts != null && !attempts.isEmpty()) {
                return attempts.get(attempts.size() - 1);
            }
            return null;
        }
        
        // Getters and setters
        public String getDatasetId() {
            return datasetId;
        }
        
        public void setDatasetId(String datasetId) {
            this.datasetId = datasetId;
        }
        
        public Map<String, List<Long>> getRemediationAttempts() {
            return remediationAttempts;
        }
        
        public void setRemediationAttempts(Map<String, List<Long>> remediationAttempts) {
            this.remediationAttempts = remediationAttempts;
        }
    }
} 