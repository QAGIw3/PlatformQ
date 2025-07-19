package io.platformq.flink.computefutures.patterns;

import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.windowing.time.Time;
import io.platformq.flink.computefutures.events.ComputeMetrics;
import io.platformq.flink.computefutures.models.ProviderHealthEvent;
import io.platformq.flink.computefutures.models.SLAEvent;
import java.util.List;

/**
 * CEP patterns for detecting SLA violations and triggering failovers
 */
public class SLAViolationPatterns {
    
    /**
     * Pattern: Sustained Low Uptime
     * Triggers when uptime drops below SLA threshold for consecutive checks
     */
    public static Pattern<SLAEvent, ?> sustainedLowUptimePattern() {
        return Pattern.<SLAEvent>begin("low_uptime")
            .where(new SimpleCondition<SLAEvent>() {
                @Override
                public boolean filter(SLAEvent event) {
                    return event.getEventType().equals("UPTIME_CHECK") &&
                           event.getActualValue() < event.getExpectedValue() * 0.95; // 5% tolerance
                }
            })
            .times(3).consecutive()
            .within(Time.minutes(15));
    }
    
    /**
     * Pattern: Critical Uptime Failure
     * Immediate failover trigger when uptime drops below critical threshold
     */
    public static Pattern<SLAEvent, ?> criticalUptimeFailurePattern() {
        return Pattern.<SLAEvent>begin("critical_failure")
            .where(new SimpleCondition<SLAEvent>() {
                @Override
                public boolean filter(SLAEvent event) {
                    return event.getEventType().equals("UPTIME_CHECK") &&
                           event.getActualValue() < 90.0; // Below 90% is critical
                }
            });
    }
    
    /**
     * Pattern: Cascading Performance Degradation
     * Detects when performance metrics progressively worsen
     */
    public static Pattern<SLAEvent, ?> cascadingDegradationPattern() {
        return Pattern.<SLAEvent>begin("initial_degradation")
            .where(new SimpleCondition<SLAEvent>() {
                @Override
                public boolean filter(SLAEvent event) {
                    return event.getEventType().equals("PERFORMANCE_CHECK") &&
                           event.getActualValue() < event.getExpectedValue() * 0.9;
                }
            })
            .followedBy("worsening")
            .where(new IterativeCondition<SLAEvent>() {
                @Override
                public boolean filter(SLAEvent event, Context<SLAEvent> ctx) throws Exception {
                    // Check if performance is worse than previous
                    List<SLAEvent> previousEvents = ctx.getEventsForPattern("initial_degradation");
                    if (!previousEvents.isEmpty()) {
                        double previousValue = previousEvents.get(0).getActualValue();
                        return event.getEventType().equals("PERFORMANCE_CHECK") &&
                               event.getActualValue() < previousValue * 0.95;
                    }
                    return false;
                }
            })
            .times(2)
            .within(Time.minutes(30));
    }
    
    /**
     * Pattern: Latency Spike Pattern
     * Detects sudden increases in latency
     */
    public static Pattern<ComputeMetrics, ?> latencySpikePattern() {
        return Pattern.<ComputeMetrics>begin("normal_latency")
            .where(new SimpleCondition<ComputeMetrics>() {
                @Override
                public boolean filter(ComputeMetrics metrics) {
                    return metrics.getLatency() < 100.0; // Normal < 100ms
                }
            })
            .followedBy("spike")
            .where(new SimpleCondition<ComputeMetrics>() {
                @Override
                public boolean filter(ComputeMetrics metrics) {
                    return metrics.getLatency() > 500.0; // Spike > 500ms
                }
            })
            .times(5).consecutive()
            .within(Time.minutes(5));
    }
    
    /**
     * Pattern: Provider Unresponsive
     * Detects when provider stops sending metrics
     */
    public static Pattern<ProviderHealthEvent, ?> providerUnresponsivePattern() {
        return Pattern.<ProviderHealthEvent>begin("last_heartbeat")
            .where(new SimpleCondition<ProviderHealthEvent>() {
                @Override
                public boolean filter(ProviderHealthEvent event) {
                    return event.getEventType().equals("HEARTBEAT");
                }
            })
            .notFollowedBy("next_heartbeat")
            .where(new SimpleCondition<ProviderHealthEvent>() {
                @Override
                public boolean filter(ProviderHealthEvent event) {
                    return event.getEventType().equals("HEARTBEAT");
                }
            })
            .within(Time.minutes(5)); // No heartbeat for 5 minutes
    }
    
    /**
     * Pattern: Resource Exhaustion
     * Detects when provider is running out of resources
     */
    public static Pattern<ComputeMetrics, ?> resourceExhaustionPattern() {
        return Pattern.<ComputeMetrics>begin("high_utilization")
            .where(new SimpleCondition<ComputeMetrics>() {
                @Override
                public boolean filter(ComputeMetrics metrics) {
                    return metrics.getCpuUtilization() > 85.0 ||
                           metrics.getMemoryUtilization() > 85.0;
                }
            })
            .times(3).consecutive()
            .followedBy("critical")
            .where(new SimpleCondition<ComputeMetrics>() {
                @Override
                public boolean filter(ComputeMetrics metrics) {
                    return metrics.getCpuUtilization() > 95.0 ||
                           metrics.getMemoryUtilization() > 95.0;
                }
            })
            .within(Time.minutes(10));
    }
    
    /**
     * Pattern: Intermittent Failures
     * Detects flapping behavior indicating instability
     */
    public static Pattern<SLAEvent, ?> intermittentFailurePattern() {
        return Pattern.<SLAEvent>begin("failure")
            .where(new SimpleCondition<SLAEvent>() {
                @Override
                public boolean filter(SLAEvent event) {
                    return event.getViolationType() != null;
                }
            })
            .followedBy("recovery")
            .where(new SimpleCondition<SLAEvent>() {
                @Override
                public boolean filter(SLAEvent event) {
                    return event.getViolationType() == null;
                }
            })
            .followedBy("failure_again")
            .where(new SimpleCondition<SLAEvent>() {
                @Override
                public boolean filter(SLAEvent event) {
                    return event.getViolationType() != null;
                }
            })
            .times(3) // 3 flip-flops indicate instability
            .within(Time.hours(1));
    }
    
    /**
     * Pattern: Correlated Provider Failures
     * Detects when multiple providers in same region fail
     */
    public static Pattern<ProviderHealthEvent, ?> correlatedFailurePattern() {
        return Pattern.<ProviderHealthEvent>begin("first_failure")
            .where(new SimpleCondition<ProviderHealthEvent>() {
                @Override
                public boolean filter(ProviderHealthEvent event) {
                    return event.getEventType().equals("PROVIDER_DOWN");
                }
            })
            .followedBy("correlated_failures")
            .where(new IterativeCondition<ProviderHealthEvent>() {
                @Override
                public boolean filter(ProviderHealthEvent event, Context<ProviderHealthEvent> ctx) throws Exception {
                    List<ProviderHealthEvent> failures = ctx.getEventsForPattern("first_failure");
                    if (!failures.isEmpty()) {
                        String region = failures.get(0).getRegion();
                        return event.getEventType().equals("PROVIDER_DOWN") &&
                               event.getRegion().equals(region) &&
                               !event.getProviderId().equals(failures.get(0).getProviderId());
                    }
                    return false;
                }
            })
            .times(2) // 3 providers total in same region
            .within(Time.minutes(15));
    }
    
    /**
     * Pattern: SLA Violation Storm
     * Detects when provider has too many concurrent violations
     */
    public static Pattern<SLAEvent, ?> violationStormPattern() {
        return Pattern.<SLAEvent>begin("violations")
            .where(new SimpleCondition<SLAEvent>() {
                @Override
                public boolean filter(SLAEvent event) {
                    return event.getViolationType() != null;
                }
            })
            .times(10) // 10 violations
            .within(Time.minutes(5)); // within 5 minutes
    }
    
    /**
     * Pattern: Progressive Recovery Detection
     * Identifies when a provider is recovering from issues
     */
    public static Pattern<SLAEvent, ?> recoveryPattern() {
        return Pattern.<SLAEvent>begin("low_performance")
            .where(new SimpleCondition<SLAEvent>() {
                @Override
                public boolean filter(SLAEvent event) {
                    return event.getEventType().equals("PERFORMANCE_CHECK") &&
                           event.getActualValue() < event.getExpectedValue() * 0.7;
                }
            })
            .followedBy("improving")
            .where(new IterativeCondition<SLAEvent>() {
                @Override
                public boolean filter(SLAEvent event, Context<SLAEvent> ctx) throws Exception {
                    List<SLAEvent> previous = ctx.getEventsForPattern("low_performance");
                    if (!previous.isEmpty()) {
                        return event.getEventType().equals("PERFORMANCE_CHECK") &&
                               event.getActualValue() > previous.get(0).getActualValue() * 1.1;
                    }
                    return false;
                }
            })
            .times(3).consecutive()
            .within(Time.minutes(20));
    }
} 