package com.platformq.flink.quality.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Map;

/**
 * Dataset event model
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DatasetEvent implements Serializable {
    
    @JsonProperty("dataset_id")
    private String datasetId;
    
    @JsonProperty("data_uri")
    private String dataUri;
    
    @JsonProperty("format")
    private String format;
    
    @JsonProperty("schema_info")
    private Map<String, Object> schemaInfo;
    
    @JsonProperty("size_bytes")
    private Long sizeBytes;
    
    @JsonProperty("num_samples")
    private Long numSamples;
    
    @JsonProperty("timestamp")
    private Long timestamp;
    
    @JsonProperty("metadata")
    private Map<String, Object> metadata;
    
    // Getters and setters
    public String getDatasetId() {
        return datasetId;
    }
    
    public void setDatasetId(String datasetId) {
        this.datasetId = datasetId;
    }
    
    public String getDataUri() {
        return dataUri;
    }
    
    public void setDataUri(String dataUri) {
        this.dataUri = dataUri;
    }
    
    public String getFormat() {
        return format;
    }
    
    public void setFormat(String format) {
        this.format = format;
    }
    
    public Map<String, Object> getSchemaInfo() {
        return schemaInfo;
    }
    
    public void setSchemaInfo(Map<String, Object> schemaInfo) {
        this.schemaInfo = schemaInfo;
    }
    
    public Long getSizeBytes() {
        return sizeBytes;
    }
    
    public void setSizeBytes(Long sizeBytes) {
        this.sizeBytes = sizeBytes;
    }
    
    public Long getNumSamples() {
        return numSamples;
    }
    
    public void setNumSamples(Long numSamples) {
        this.numSamples = numSamples;
    }
    
    public Long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
    
    public Map<String, Object> getMetadata() {
        return metadata;
    }
    
    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }
} 