package io.platformq.flink.computefutures.state;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Queue;

import io.platformq.flink.computefutures.events.ComputeMetrics;

/**
 * Maintains a rolling history of metrics
 */
public class MetricsHistory implements Serializable {
    
    private final int maxSize;
    private final Queue<ComputeMetrics> metrics;
    
    public MetricsHistory(int maxSize) {
        this.maxSize = maxSize;
        this.metrics = new LinkedList<>();
    }
    
    public void addMetric(ComputeMetrics metric) {
        metrics.offer(metric);
        while (metrics.size() > maxSize) {
            metrics.poll();
        }
    }
    
    public Queue<ComputeMetrics> getMetrics() {
        return new LinkedList<>(metrics);
    }
    
    public int size() {
        return metrics.size();
    }
    
    public boolean isEmpty() {
        return metrics.isEmpty();
    }
    
    public ComputeMetrics getLatest() {
        return ((LinkedList<ComputeMetrics>) metrics).peekLast();
    }
    
    public double getAverageUptime() {
        if (metrics.isEmpty()) return 100.0;
        return metrics.stream()
            .mapToDouble(ComputeMetrics::getUptimePercent)
            .average()
            .orElse(100.0);
    }
    
    public double getAverageLatency() {
        if (metrics.isEmpty()) return 0.0;
        return metrics.stream()
            .mapToDouble(ComputeMetrics::getLatencyMs)
            .average()
            .orElse(0.0);
    }
} 