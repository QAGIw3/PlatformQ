package com.platformq.flink.quality.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import java.util.Map;

/**
 * Data event from the data lake
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataEvent implements Serializable {
    
    private String eventId;
    private String datasetId;
    private String dataUri;
    private String format;
    private Long timestamp;
    private Map<String, Object> data;
    private Map<String, Object> metadata;
    
    public boolean isValid() {
        return datasetId != null && !datasetId.isEmpty() && 
               data != null && !data.isEmpty();
    }
    
    // Getters and setters
    public String getEventId() {
        return eventId;
    }
    
    public void setEventId(String eventId) {
        this.eventId = eventId;
    }
    
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
    
    public Long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
    
    public Map<String, Object> getData() {
        return data;
    }
    
    public void setData(Map<String, Object> data) {
        this.data = data;
    }
    
    public Map<String, Object> getMetadata() {
        return metadata;
    }
    
    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }
} 