package io.platformq.flink.computefutures.processors;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.platformq.flink.computefutures.events.ComputeSettlementEvent;
import io.platformq.flink.computefutures.state.SettlementState;
import io.platformq.flink.computefutures.state.SLARequirements;

/**
 * Processes settlement events and maintains settlement state
 */
public class SettlementStateProcessor extends KeyedProcessFunction<String, ComputeSettlementEvent, SettlementState> {
    
    private static final Logger LOG = LoggerFactory.getLogger(SettlementStateProcessor.class);
    
    private final Ignite ignite;
    private IgniteCache<String, SettlementState> settlementCache;
    private ValueState<SettlementState> settlementStateHandle;
    
    public SettlementStateProcessor(Ignite ignite) {
        this.ignite = ignite;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        // Get Ignite cache
        settlementCache = ignite.getOrCreateCache("settlement-states");
        
        // Initialize state
        settlementStateHandle = getRuntimeContext().getState(
            new ValueStateDescriptor<>("settlement-state", SettlementState.class));
    }
    
    @Override
    public void processElement(
            ComputeSettlementEvent event, 
            Context ctx, 
            Collector<SettlementState> out) throws Exception {
        
        // Get or create settlement state
        SettlementState state = settlementStateHandle.value();
        if (state == null) {
            state = new SettlementState(event.getSettlementId());
            
            // Initialize from event
            state.setContractId(event.getContractId());
            state.setBuyerId(event.getBuyerId());
            state.setProviderId(event.getProviderId());
            state.setResourceType(event.getResourceType());
            state.setQuantity(event.getQuantity());
            state.setDeliveryStart(event.getDeliveryStart());
            state.setDurationHours(event.getDurationHours());
            
            // Set default SLA requirements
            state.setSlaRequirements(createDefaultSLARequirements());
            
            LOG.info("Created new settlement state for {}", event.getSettlementId());
        }
        
        // Update state based on event
        state.setStatus("provisioning");
        
        // Store in state and cache
        settlementStateHandle.update(state);
        settlementCache.put(state.getSettlementId(), state);
        
        // Emit state
        out.collect(state);
        
        // Set timer for settlement expiry
        long expiryTime = state.getDeliveryStart() + (state.getDurationHours() * 3600 * 1000L);
        ctx.timerService().registerEventTimeTimer(expiryTime);
    }
    
    @Override
    public void onTimer(
            long timestamp, 
            OnTimerContext ctx, 
            Collector<SettlementState> out) throws Exception {
        
        SettlementState state = settlementStateHandle.value();
        if (state != null && "active".equals(state.getStatus())) {
            // Settlement expired
            state.setStatus("completed");
            settlementStateHandle.update(state);
            settlementCache.put(state.getSettlementId(), state);
            
            LOG.info("Settlement {} completed at expiry", state.getSettlementId());
            
            out.collect(state);
        }
    }
    
    private SLARequirements createDefaultSLARequirements() {
        return new SLARequirements(
            99.9,   // uptime
            50.0,   // latency
            0.95,   // performance
            1.0,    // throughput
            0.01    // error rate
        );
    }
} 