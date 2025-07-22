package com.platformq.flink.quality.models;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * Quality dimensions model
 */
public class QualityDimensions implements Serializable {
    
    @JsonProperty("completeness")
    private Double completeness;
    
    @JsonProperty("accuracy")
    private Double accuracy;
    
    @JsonProperty("consistency")
    private Double consistency;
    
    @JsonProperty("timeliness")
    private Double timeliness;
    
    @JsonProperty("validity")
    private Double validity;
    
    @JsonProperty("uniqueness")
    private Double uniqueness;
    
    // Getters and setters
    public Double getCompleteness() {
        return completeness;
    }
    
    public void setCompleteness(Double completeness) {
        this.completeness = completeness;
    }
    
    public Double getAccuracy() {
        return accuracy;
    }
    
    public void setAccuracy(Double accuracy) {
        this.accuracy = accuracy;
    }
    
    public Double getConsistency() {
        return consistency;
    }
    
    public void setConsistency(Double consistency) {
        this.consistency = consistency;
    }
    
    public Double getTimeliness() {
        return timeliness;
    }
    
    public void setTimeliness(Double timeliness) {
        this.timeliness = timeliness;
    }
    
    public Double getValidity() {
        return validity;
    }
    
    public void setValidity(Double validity) {
        this.validity = validity;
    }
    
    public Double getUniqueness() {
        return uniqueness;
    }
    
    public void setUniqueness(Double uniqueness) {
        this.uniqueness = uniqueness;
    }
} 