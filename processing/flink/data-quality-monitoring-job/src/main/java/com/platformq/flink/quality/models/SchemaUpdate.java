package com.platformq.flink.quality.models;

import java.io.Serializable;
import java.util.Map;
import java.util.List;

/**
 * Schema update event from the catalog
 */
public class SchemaUpdate implements Serializable {
    
    private String schemaId;
    private String datasetId;
    private String version;
    private Long timestamp;
    private Map<String, FieldSchema> fields;
    private List<String> primaryKeys;
    private Map<String, Object> properties;
    
    // Getters and setters
    public String getSchemaId() {
        return schemaId;
    }
    
    public void setSchemaId(String schemaId) {
        this.schemaId = schemaId;
    }
    
    public String getDatasetId() {
        return datasetId;
    }
    
    public void setDatasetId(String datasetId) {
        this.datasetId = datasetId;
    }
    
    public String getVersion() {
        return version;
    }
    
    public void setVersion(String version) {
        this.version = version;
    }
    
    public Long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
    
    public Map<String, FieldSchema> getFields() {
        return fields;
    }
    
    public void setFields(Map<String, FieldSchema> fields) {
        this.fields = fields;
    }
    
    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }
    
    public void setPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }
    
    public Map<String, Object> getProperties() {
        return properties;
    }
    
    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }
    
    /**
     * Field schema definition
     */
    public static class FieldSchema implements Serializable {
        private String name;
        private String type;
        private boolean nullable;
        private Object defaultValue;
        private Map<String, Object> constraints;
        
        // Getters and setters
        public String getName() {
            return name;
        }
        
        public void setName(String name) {
            this.name = name;
        }
        
        public String getType() {
            return type;
        }
        
        public void setType(String type) {
            this.type = type;
        }
        
        public boolean isNullable() {
            return nullable;
        }
        
        public void setNullable(boolean nullable) {
            this.nullable = nullable;
        }
        
        public Object getDefaultValue() {
            return defaultValue;
        }
        
        public void setDefaultValue(Object defaultValue) {
            this.defaultValue = defaultValue;
        }
        
        public Map<String, Object> getConstraints() {
            return constraints;
        }
        
        public void setConstraints(Map<String, Object> constraints) {
            this.constraints = constraints;
        }
    }
} 