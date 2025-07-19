package com.platformq.flink.quality.sinks;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.platformq.flink.quality.models.RemediationRequest;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
import java.time.Duration;

/**
 * Sink that triggers remediation workflows via the workflow service
 */
public class RemediationWorkflowSink extends RichSinkFunction<RemediationRequest> {
    
    private static final Logger LOG = LoggerFactory.getLogger(RemediationWorkflowSink.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    private final String workflowServiceUrl;
    private transient HttpClient httpClient;
    
    public RemediationWorkflowSink(String workflowServiceUrl) {
        this.workflowServiceUrl = workflowServiceUrl;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
            .connectTimeout(Duration.ofSeconds(10))
            .build();
    }
    
    @Override
    public void invoke(RemediationRequest request, Context context) throws Exception {
        try {
            // Prepare workflow trigger payload
            WorkflowTriggerRequest workflowRequest = new WorkflowTriggerRequest();
            workflowRequest.setPlanId(request.getRequestId());
            workflowRequest.setDatasetId(request.getDatasetId());
            workflowRequest.setActions(request.getActions());
            workflowRequest.setSeverity(request.getSeverity());
            workflowRequest.setQualityIssues(request.getQualityIssues());
            
            // Convert to JSON
            String jsonBody = objectMapper.writeValueAsString(workflowRequest);
            
            // Create HTTP request
            HttpRequest httpRequest = HttpRequest.newBuilder()
                .uri(URI.create(workflowServiceUrl + "/api/v1/workflows/data_quality_remediation/trigger"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .timeout(Duration.ofSeconds(30))
                .build();
            
            // Send request
            HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
            
            if (response.statusCode() == 200 || response.statusCode() == 201) {
                LOG.info("Successfully triggered remediation workflow for dataset {}: {}", 
                    request.getDatasetId(), response.body());
            } else {
                LOG.error("Failed to trigger remediation workflow. Status: {}, Body: {}", 
                    response.statusCode(), response.body());
            }
            
        } catch (Exception e) {
            LOG.error("Error triggering remediation workflow for dataset {}", request.getDatasetId(), e);
            // Don't throw - we don't want to fail the entire job
        }
    }
    
    @Override
    public void close() throws Exception {
        super.close();
        // HTTP client doesn't need explicit closing
    }
    
    /**
     * Request object for triggering workflows
     */
    private static class WorkflowTriggerRequest {
        private String planId;
        private String datasetId;
        private java.util.List<String> actions;
        private String severity;
        private java.util.List<java.util.Map<String, Object>> qualityIssues;
        
        // Getters and setters
        public String getPlanId() {
            return planId;
        }
        
        public void setPlanId(String planId) {
            this.planId = planId;
        }
        
        public String getDatasetId() {
            return datasetId;
        }
        
        public void setDatasetId(String datasetId) {
            this.datasetId = datasetId;
        }
        
        public java.util.List<String> getActions() {
            return actions;
        }
        
        public void setActions(java.util.List<String> actions) {
            this.actions = actions;
        }
        
        public String getSeverity() {
            return severity;
        }
        
        public void setSeverity(String severity) {
            this.severity = severity;
        }
        
        public java.util.List<java.util.Map<String, Object>> getQualityIssues() {
            return qualityIssues;
        }
        
        public void setQualityIssues(java.util.List<java.util.Map<String, Object>> qualityIssues) {
            this.qualityIssues = qualityIssues;
        }
    }
} 