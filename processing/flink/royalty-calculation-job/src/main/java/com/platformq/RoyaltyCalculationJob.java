package com.platformq;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSource;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch7.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.RestClientFactory;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class RoyaltyCalculationJob {
    
    private static final ObjectMapper mapper = new ObjectMapper();
    
    public static void main(String[] args) throws Exception {
        
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Configure Pulsar source
        Properties pulsarProps = new Properties();
        pulsarProps.setProperty("topic", "persistent://public/default/marketplace-transactions");
        pulsarProps.setProperty("serviceUrl", "pulsar://pulsar:6650");
        pulsarProps.setProperty("subscriptionName", "royalty-calculator");
        
        FlinkPulsarSource<String> pulsarSource = new FlinkPulsarSource<>(
            "pulsar://pulsar:6650",
            "http://pulsar:8080",
            "persistent://public/default/marketplace-transactions",
            "royalty-calculator",
            new SimpleStringSchema(),
            pulsarProps
        );
        
        // Read transaction events
        DataStream<String> transactionStream = env.addSource(pulsarSource);
        
        // Parse and calculate royalties
        DataStream<RoyaltyEvent> royaltyStream = transactionStream
            .map(new TransactionParser())
            .filter(event -> event != null && event.getRoyaltyAmount() > 0)
            .keyBy(event -> event.getAssetId())
            .window(TumblingEventTimeWindows.of(Time.hours(1)))
            .aggregate(new RoyaltyAggregator());
        
        // Configure Elasticsearch sink
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("elasticsearch", 9200, "http"));
        
        ElasticsearchSink.Builder<RoyaltyEvent> esSinkBuilder = new ElasticsearchSink.Builder<>(
            httpHosts,
            new org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction<RoyaltyEvent>() {
                public IndexRequest createIndexRequest(RoyaltyEvent element) {
                    Map<String, Object> json = new HashMap<>();
                    json.put("asset_id", element.getAssetId());
                    json.put("creator_address", element.getCreatorAddress());
                    json.put("total_royalties", element.getTotalRoyalties());
                    json.put("transaction_count", element.getTransactionCount());
                    json.put("timestamp", element.getTimestamp());
                    json.put("chain", element.getChain());
                    
                    return Requests.indexRequest()
                        .index("royalty-analytics")
                        .source(json);
                }
                
                @Override
                public void process(RoyaltyEvent element, RuntimeContext ctx, RequestIndexer indexer) {
                    indexer.add(createIndexRequest(element));
                }
            }
        );
        
        // Add sink to write results to Elasticsearch
        royaltyStream.addSink(esSinkBuilder.build());
        
        // Execute the job
        env.execute("Royalty Calculation Job");
    }
    
    // Transaction parser
    static class TransactionParser implements MapFunction<String, RoyaltyEvent> {
        @Override
        public RoyaltyEvent map(String value) throws Exception {
            JsonNode node = mapper.readTree(value);
            
            String eventType = node.get("event_type").asText();
            if (!"asset_purchase".equals(eventType) && !"license_purchase".equals(eventType)) {
                return null;
            }
            
            return new RoyaltyEvent(
                node.get("asset_id").asText(),
                node.get("creator_address").asText(),
                node.get("sale_price").asDouble() * node.get("royalty_percentage").asInt() / 10000,
                1,
                System.currentTimeMillis(),
                node.get("chain").asText()
            );
        }
    }
    
    // Royalty aggregator
    static class RoyaltyAggregator implements AggregateFunction<RoyaltyEvent, RoyaltyEvent, RoyaltyEvent> {
        @Override
        public RoyaltyEvent createAccumulator() {
            return new RoyaltyEvent("", "", 0.0, 0, 0L, "");
        }
        
        @Override
        public RoyaltyEvent add(RoyaltyEvent value, RoyaltyEvent accumulator) {
            return new RoyaltyEvent(
                value.getAssetId(),
                value.getCreatorAddress(),
                accumulator.getTotalRoyalties() + value.getRoyaltyAmount(),
                accumulator.getTransactionCount() + 1,
                System.currentTimeMillis(),
                value.getChain()
            );
        }
        
        @Override
        public RoyaltyEvent getResult(RoyaltyEvent accumulator) {
            return accumulator;
        }
        
        @Override
        public RoyaltyEvent merge(RoyaltyEvent a, RoyaltyEvent b) {
            return new RoyaltyEvent(
                a.getAssetId(),
                a.getCreatorAddress(),
                a.getTotalRoyalties() + b.getTotalRoyalties(),
                a.getTransactionCount() + b.getTransactionCount(),
                System.currentTimeMillis(),
                a.getChain()
            );
        }
    }
    
    // Royalty event class
    static class RoyaltyEvent {
        private String assetId;
        private String creatorAddress;
        private double royaltyAmount;
        private double totalRoyalties;
        private int transactionCount;
        private long timestamp;
        private String chain;
        
        public RoyaltyEvent(String assetId, String creatorAddress, double royaltyAmount, 
                           int transactionCount, long timestamp, String chain) {
            this.assetId = assetId;
            this.creatorAddress = creatorAddress;
            this.royaltyAmount = royaltyAmount;
            this.totalRoyalties = royaltyAmount;
            this.transactionCount = transactionCount;
            this.timestamp = timestamp;
            this.chain = chain;
        }
        
        // Getters
        public String getAssetId() { return assetId; }
        public String getCreatorAddress() { return creatorAddress; }
        public double getRoyaltyAmount() { return royaltyAmount; }
        public double getTotalRoyalties() { return totalRoyalties; }
        public int getTransactionCount() { return transactionCount; }
        public long getTimestamp() { return timestamp; }
        public String getChain() { return chain; }
    }
} 