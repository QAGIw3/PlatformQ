package com.platformq.fraud;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSource;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSink;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Instant;
import java.util.*;

/**
 * Real-time fraud detection for marketplace transactions
 */
public class MarketplaceFraudDetectionJob {
    
    // Output tags for different fraud types
    private static final OutputTag<FraudAlert> WASH_TRADING_TAG = 
        new OutputTag<FraudAlert>("wash-trading"){};
    private static final OutputTag<FraudAlert> PRICE_MANIPULATION_TAG = 
        new OutputTag<FraudAlert>("price-manipulation"){};
    private static final OutputTag<FraudAlert> SYBIL_ATTACK_TAG = 
        new OutputTag<FraudAlert>("sybil-attack"){};
    private static final OutputTag<FraudAlert> MONEY_LAUNDERING_TAG = 
        new OutputTag<FraudAlert>("money-laundering"){};
    
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(10);
        env.enableCheckpointing(60000); // 1 minute
        
        // Configure Pulsar source
        Properties pulsarProps = new Properties();
        pulsarProps.setProperty("service.url", "pulsar://pulsar:6650");
        pulsarProps.setProperty("admin.url", "http://pulsar:8080");
        
        FlinkPulsarSource<MarketplaceEvent> source = new FlinkPulsarSource<>(
            "persistent://public/default/blockchain-events",
            new MarketplaceEventDeserializer(),
            pulsarProps
        );
        
        // Read events from Pulsar
        DataStream<MarketplaceEvent> events = env.addSource(source)
            .name("Marketplace Events");
        
        // 1. Detect wash trading
        DataStream<FraudAlert> washTradingAlerts = detectWashTrading(events);
        
        // 2. Detect price manipulation
        DataStream<FraudAlert> priceManipulationAlerts = detectPriceManipulation(events);
        
        // 3. Detect Sybil attacks
        DataStream<FraudAlert> sybilAttackAlerts = detectSybilAttacks(events);
        
        // 4. Detect money laundering patterns
        DataStream<FraudAlert> moneyLaunderingAlerts = detectMoneyLaundering(events);
        
        // 5. Combine all fraud alerts
        DataStream<FraudAlert> allAlerts = washTradingAlerts
            .union(priceManipulationAlerts)
            .union(sybilAttackAlerts)
            .union(moneyLaunderingAlerts);
        
        // 6. Enrich alerts with risk scores
        DataStream<FraudAlert> enrichedAlerts = allAlerts
            .keyBy(alert -> alert.getUserAddress())
            .process(new RiskScoreEnrichment())
            .name("Risk Score Enrichment");
        
        // 7. Send alerts to different sinks based on severity
        enrichedAlerts
            .filter(alert -> alert.getSeverity() == FraudSeverity.CRITICAL)
            .addSink(createPulsarSink("fraud-alerts-critical"))
            .name("Critical Fraud Alerts");
        
        enrichedAlerts
            .filter(alert -> alert.getSeverity() == FraudSeverity.HIGH)
            .addSink(createPulsarSink("fraud-alerts-high"))
            .name("High Fraud Alerts");
        
        // 8. Update fraud statistics
        enrichedAlerts
            .keyBy(alert -> alert.getFraudType())
            .window(TumblingEventTimeWindows.of(Time.minutes(5)))
            .aggregate(new FraudStatsAggregator())
            .addSink(createStatsSink())
            .name("Fraud Statistics");
        
        // Execute the job
        env.execute("Marketplace Fraud Detection");
    }
    
    /**
     * Detect wash trading patterns
     */
    private static DataStream<FraudAlert> detectWashTrading(DataStream<MarketplaceEvent> events) {
        return events
            .filter(event -> event.getEventType() == EventType.NFT_TRANSFER)
            .keyBy(event -> event.getTokenId())
            .window(TumblingEventTimeWindows.of(Time.hours(24)))
            .process(new ProcessWindowFunction<MarketplaceEvent, FraudAlert, String, TimeWindow>() {
                @Override
                public void process(String tokenId, Context context, 
                                  Iterable<MarketplaceEvent> events, 
                                  Collector<FraudAlert> out) {
                    List<MarketplaceEvent> eventList = new ArrayList<>();
                    events.forEach(eventList::add);
                    
                    // Check for circular trading patterns
                    Map<String, Set<String>> tradingGraph = new HashMap<>();
                    for (MarketplaceEvent event : eventList) {
                        tradingGraph.computeIfAbsent(event.getFromAddress(), k -> new HashSet<>())
                            .add(event.getToAddress());
                    }
                    
                    // Detect cycles
                    for (String address : tradingGraph.keySet()) {
                        if (detectCycle(address, address, tradingGraph, new HashSet<>())) {
                            FraudAlert alert = new FraudAlert(
                                FraudType.WASH_TRADING,
                                address,
                                tokenId,
                                "Circular trading pattern detected",
                                FraudSeverity.HIGH,
                                0.85
                            );
                            out.collect(alert);
                        }
                    }
                }
            })
            .name("Wash Trading Detection");
    }
    
    /**
     * Detect price manipulation
     */
    private static DataStream<FraudAlert> detectPriceManipulation(DataStream<MarketplaceEvent> events) {
        return events
            .filter(event -> event.getEventType() == EventType.NFT_SALE || 
                           event.getEventType() == EventType.AUCTION_BID)
            .keyBy(event -> event.getContractAddress())
            .process(new KeyedProcessFunction<String, MarketplaceEvent, FraudAlert>() {
                private ValueState<Double> avgPriceState;
                private ValueState<Long> countState;
                
                @Override
                public void open(Configuration parameters) {
                    avgPriceState = getRuntimeContext().getState(
                        new ValueStateDescriptor<>("avgPrice", Double.class));
                    countState = getRuntimeContext().getState(
                        new ValueStateDescriptor<>("count", Long.class));
                }
                
                @Override
                public void processElement(MarketplaceEvent event, Context ctx, 
                                         Collector<FraudAlert> out) throws Exception {
                    Double avgPrice = avgPriceState.value();
                    Long count = countState.value();
                    
                    if (avgPrice == null) {
                        avgPrice = 0.0;
                        count = 0L;
                    }
                    
                    double currentPrice = event.getValue();
                    
                    // Check for abnormal price deviation
                    if (count > 10 && currentPrice > avgPrice * 3) {
                        FraudAlert alert = new FraudAlert(
                            FraudType.PRICE_MANIPULATION,
                            event.getFromAddress(),
                            event.getTokenId(),
                            String.format("Price %f is %.2fx average", currentPrice, currentPrice/avgPrice),
                            FraudSeverity.HIGH,
                            0.75
                        );
                        out.collect(alert);
                    }
                    
                    // Update running average
                    avgPrice = (avgPrice * count + currentPrice) / (count + 1);
                    avgPriceState.update(avgPrice);
                    countState.update(count + 1);
                }
            })
            .name("Price Manipulation Detection");
    }
    
    /**
     * Detect Sybil attacks
     */
    private static DataStream<FraudAlert> detectSybilAttacks(DataStream<MarketplaceEvent> events) {
        return events
            .keyBy(event -> event.getFromAddress())
            .window(TumblingEventTimeWindows.of(Time.hours(1)))
            .process(new ProcessWindowFunction<MarketplaceEvent, FraudAlert, String, TimeWindow>() {
                @Override
                public void process(String address, Context context, 
                                  Iterable<MarketplaceEvent> events, 
                                  Collector<FraudAlert> out) {
                    Map<EventType, Integer> eventCounts = new HashMap<>();
                    Set<String> interactedAddresses = new HashSet<>();
                    
                    for (MarketplaceEvent event : events) {
                        eventCounts.merge(event.getEventType(), 1, Integer::sum);
                        interactedAddresses.add(event.getToAddress());
                    }
                    
                    // Check for suspicious patterns
                    int totalEvents = eventCounts.values().stream().mapToInt(Integer::intValue).sum();
                    
                    // High volume of similar transactions
                    if (totalEvents > 100) {
                        // Check if interacting with many new addresses
                        if (interactedAddresses.size() > 50) {
                            FraudAlert alert = new FraudAlert(
                                FraudType.SYBIL_ATTACK,
                                address,
                                null,
                                String.format("Suspicious activity: %d events with %d addresses", 
                                    totalEvents, interactedAddresses.size()),
                                FraudSeverity.CRITICAL,
                                0.9
                            );
                            out.collect(alert);
                        }
                    }
                }
            })
            .name("Sybil Attack Detection");
    }
    
    /**
     * Detect money laundering patterns
     */
    private static DataStream<FraudAlert> detectMoneyLaundering(DataStream<MarketplaceEvent> events) {
        return events
            .filter(event -> event.getValue() > 10000) // High-value transactions
            .keyBy(event -> event.getFromAddress())
            .process(new KeyedProcessFunction<String, MarketplaceEvent, FraudAlert>() {
                private ValueState<List<MarketplaceEvent>> recentTransactionsState;
                
                @Override
                public void open(Configuration parameters) {
                    recentTransactionsState = getRuntimeContext().getState(
                        new ValueStateDescriptor<>("recentTransactions", 
                            Types.LIST(Types.POJO(MarketplaceEvent.class))));
                }
                
                @Override
                public void processElement(MarketplaceEvent event, Context ctx, 
                                         Collector<FraudAlert> out) throws Exception {
                    List<MarketplaceEvent> recentTransactions = recentTransactionsState.value();
                    if (recentTransactions == null) {
                        recentTransactions = new ArrayList<>();
                    }
                    
                    // Look for layering patterns
                    double totalValue = recentTransactions.stream()
                        .mapToDouble(MarketplaceEvent::getValue)
                        .sum() + event.getValue();
                    
                    if (recentTransactions.size() > 5 && totalValue > 100000) {
                        // Check for rapid succession of high-value transfers
                        long timeSpan = event.getTimestamp() - recentTransactions.get(0).getTimestamp();
                        if (timeSpan < 3600000) { // Within 1 hour
                            FraudAlert alert = new FraudAlert(
                                FraudType.MONEY_LAUNDERING,
                                event.getFromAddress(),
                                null,
                                String.format("Rapid high-value transfers: $%.2f in %d minutes", 
                                    totalValue, timeSpan / 60000),
                                FraudSeverity.CRITICAL,
                                0.95
                            );
                            out.collect(alert);
                        }
                    }
                    
                    // Keep only recent transactions
                    recentTransactions.add(event);
                    if (recentTransactions.size() > 20) {
                        recentTransactions.remove(0);
                    }
                    recentTransactionsState.update(recentTransactions);
                }
            })
            .name("Money Laundering Detection");
    }
    
    /**
     * Enrich fraud alerts with risk scores
     */
    private static class RiskScoreEnrichment 
            extends KeyedProcessFunction<String, FraudAlert, FraudAlert> {
        
        private ValueState<UserRiskProfile> riskProfileState;
        
        @Override
        public void open(Configuration parameters) {
            riskProfileState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("riskProfile", UserRiskProfile.class));
        }
        
        @Override
        public void processElement(FraudAlert alert, Context ctx, 
                                 Collector<FraudAlert> out) throws Exception {
            UserRiskProfile profile = riskProfileState.value();
            if (profile == null) {
                profile = new UserRiskProfile(alert.getUserAddress());
            }
            
            // Update risk profile
            profile.addAlert(alert);
            
            // Calculate composite risk score
            double riskScore = calculateRiskScore(profile, alert);
            alert.setRiskScore(riskScore);
            
            // Adjust severity based on history
            if (profile.getAlertCount() > 5 && riskScore > 0.8) {
                alert.setSeverity(FraudSeverity.CRITICAL);
            }
            
            riskProfileState.update(profile);
            out.collect(alert);
        }
        
        private double calculateRiskScore(UserRiskProfile profile, FraudAlert alert) {
            double baseScore = alert.getConfidence();
            double historyMultiplier = 1 + (profile.getAlertCount() * 0.1);
            double recencyBoost = profile.getRecentAlertCount() > 3 ? 1.2 : 1.0;
            
            return Math.min(baseScore * historyMultiplier * recencyBoost, 1.0);
        }
    }
    
    /**
     * Aggregate fraud statistics
     */
    private static class FraudStatsAggregator 
            implements AggregateFunction<FraudAlert, FraudStatistics, FraudStatistics> {
        
        @Override
        public FraudStatistics createAccumulator() {
            return new FraudStatistics();
        }
        
        @Override
        public FraudStatistics add(FraudAlert alert, FraudStatistics accumulator) {
            accumulator.incrementCount(alert.getFraudType());
            accumulator.updateTotalValue(alert.getFraudType(), alert.getValue());
            accumulator.updateAvgRiskScore(alert.getFraudType(), alert.getRiskScore());
            return accumulator;
        }
        
        @Override
        public FraudStatistics getResult(FraudStatistics accumulator) {
            return accumulator;
        }
        
        @Override
        public FraudStatistics merge(FraudStatistics a, FraudStatistics b) {
            return FraudStatistics.merge(a, b);
        }
    }
    
    /**
     * Helper method to detect cycles in trading graph
     */
    private static boolean detectCycle(String start, String current, 
                                     Map<String, Set<String>> graph, 
                                     Set<String> visited) {
        if (visited.contains(current)) {
            return current.equals(start) && visited.size() > 2;
        }
        
        visited.add(current);
        Set<String> neighbors = graph.get(current);
        if (neighbors != null) {
            for (String neighbor : neighbors) {
                if (detectCycle(start, neighbor, graph, visited)) {
                    return true;
                }
            }
        }
        visited.remove(current);
        return false;
    }
    
    /**
     * Create Pulsar sink for fraud alerts
     */
    private static FlinkPulsarSink<FraudAlert> createPulsarSink(String topic) {
        Properties props = new Properties();
        props.setProperty("service.url", "pulsar://pulsar:6650");
        
        return new FlinkPulsarSink<>(
            "persistent://public/default/" + topic,
            new FraudAlertSerializer(),
            props
        );
    }
    
    /**
     * Create sink for fraud statistics
     */
    private static FlinkPulsarSink<FraudStatistics> createStatsSink() {
        Properties props = new Properties();
        props.setProperty("service.url", "pulsar://pulsar:6650");
        
        return new FlinkPulsarSink<>(
            "persistent://public/default/fraud-statistics",
            new FraudStatisticsSerializer(),
            props
        );
    }
} 