package com.platformq.derivatives;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import org.apache.avro.specific.SpecificRecordBase;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import com.platformq.derivatives.events.*;
import com.platformq.derivatives.analytics.*;
import com.platformq.connectors.ignite.IgniteSink;
import com.platformq.connectors.elasticsearch.ElasticsearchSink;

import java.time.Duration;
import java.util.Properties;

public class DerivativeAnalyticsJob {
    
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Enable checkpointing for fault tolerance
        env.enableCheckpointing(60000); // 1 minute
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        
        // Configure Pulsar source for trade events
        PulsarSource<TradeExecutedEvent> tradeSource = PulsarSource.builder()
            .setServiceUrl("pulsar://pulsar:6650")
            .setAdminUrl("http://pulsar:8080")
            .setStartCursor(StartCursor.earliest())
            .setTopics("persistent://public/default/trade-events")
            .setDeserializationSchema(new AvroDeserializationSchema<>(TradeExecutedEvent.class))
            .setSubscriptionName("derivative-analytics-trades")
            .build();
            
        // Configure Pulsar source for position events
        PulsarSource<PositionOpenedEvent> positionSource = PulsarSource.builder()
            .setServiceUrl("pulsar://pulsar:6650")
            .setAdminUrl("http://pulsar:8080")
            .setStartCursor(StartCursor.earliest())
            .setTopics("persistent://public/default/position-events")
            .setDeserializationSchema(new AvroDeserializationSchema<>(PositionOpenedEvent.class))
            .setSubscriptionName("derivative-analytics-positions")
            .build();
            
        // Configure Pulsar source for liquidation events
        PulsarSource<LiquidationEvent> liquidationSource = PulsarSource.builder()
            .setServiceUrl("pulsar://pulsar:6650")
            .setAdminUrl("http://pulsar:8080")
            .setStartCursor(StartCursor.earliest())
            .setTopics("persistent://public/default/liquidation-events")
            .setDeserializationSchema(new AvroDeserializationSchema<>(LiquidationEvent.class))
            .setSubscriptionName("derivative-analytics-liquidations")
            .build();
            
        // Create data streams
        DataStream<TradeExecutedEvent> trades = env
            .fromSource(tradeSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)), "Trade Events");
            
        DataStream<PositionOpenedEvent> positions = env
            .fromSource(positionSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)), "Position Events");
            
        DataStream<LiquidationEvent> liquidations = env
            .fromSource(liquidationSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)), "Liquidation Events");
        
        // 1. Real-time Market Metrics (1-minute windows)
        DataStream<MarketMetrics> marketMetrics = trades
            .keyBy(trade -> trade.getMarketId())
            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
            .aggregate(new MarketMetricsAggregator(), new MarketMetricsWindowFunction())
            .name("Market Metrics Aggregation");
            
        // 2. User Trading Statistics
        DataStream<UserTradingStats> userStats = trades
            .keyBy(trade -> trade.getTakerUserId())
            .process(new UserTradingStatsProcessor())
            .name("User Trading Statistics");
            
        // 3. Liquidation Risk Monitoring
        DataStream<LiquidationRiskAlert> riskAlerts = positions
            .keyBy(position -> position.getPositionId())
            .connect(trades.keyBy(trade -> trade.getMarketId()))
            .process(new LiquidationRiskMonitor())
            .name("Liquidation Risk Monitor");
            
        // 4. Market Anomaly Detection
        DataStream<MarketAnomaly> anomalies = marketMetrics
            .keyBy(metrics -> metrics.getMarketId())
            .process(new AnomalyDetector())
            .name("Market Anomaly Detection");
            
        // 5. Funding Rate Analysis
        DataStream<FundingRateMetrics> fundingMetrics = trades
            .keyBy(trade -> trade.getMarketId())
            .window(TumblingEventTimeWindows.of(Time.hours(8))) // 8-hour funding periods
            .process(new FundingRateCalculator())
            .name("Funding Rate Calculation");
            
        // 6. Cascade Detection
        DataStream<CascadeAlert> cascadeAlerts = liquidations
            .keyBy(liquidation -> liquidation.getMarketId())
            .window(TumblingEventTimeWindows.of(Time.minutes(5)))
            .process(new CascadeDetector())
            .name("Liquidation Cascade Detection");
        
        // Output to Ignite for real-time queries
        marketMetrics.addSink(new IgniteSink<>("market_metrics_cache"));
        userStats.addSink(new IgniteSink<>("user_stats_cache"));
        riskAlerts.addSink(new IgniteSink<>("risk_alerts_cache"));
        
        // Output to Elasticsearch for analytics
        anomalies.addSink(new ElasticsearchSink<>("market-anomalies"));
        fundingMetrics.addSink(new ElasticsearchSink<>("funding-metrics"));
        cascadeAlerts.addSink(new ElasticsearchSink<>("cascade-alerts"));
        
        // Execute the job
        env.execute("Derivative Analytics Job");
    }
    
    // Market metrics aggregator
    public static class MarketMetricsAggregator implements AggregateFunction<TradeExecutedEvent, MarketMetricsAccumulator, MarketMetricsAccumulator> {
        @Override
        public MarketMetricsAccumulator createAccumulator() {
            return new MarketMetricsAccumulator();
        }
        
        @Override
        public MarketMetricsAccumulator add(TradeExecutedEvent trade, MarketMetricsAccumulator acc) {
            acc.addTrade(trade);
            return acc;
        }
        
        @Override
        public MarketMetricsAccumulator getResult(MarketMetricsAccumulator acc) {
            return acc;
        }
        
        @Override
        public MarketMetricsAccumulator merge(MarketMetricsAccumulator a, MarketMetricsAccumulator b) {
            return a.merge(b);
        }
    }
    
    // Window function to create final metrics
    public static class MarketMetricsWindowFunction extends ProcessWindowFunction<MarketMetricsAccumulator, MarketMetrics, String, TimeWindow> {
        @Override
        public void process(String marketId, Context context, Iterable<MarketMetricsAccumulator> elements, Collector<MarketMetrics> out) {
            MarketMetricsAccumulator acc = elements.iterator().next();
            
            MarketMetrics metrics = new MarketMetrics();
            metrics.setMarketId(marketId);
            metrics.setWindowStart(context.window().getStart());
            metrics.setWindowEnd(context.window().getEnd());
            metrics.setTradeCount(acc.getTradeCount());
            metrics.setVolume(acc.getTotalVolume());
            metrics.setVwap(acc.getVwap());
            metrics.setHigh(acc.getHigh());
            metrics.setLow(acc.getLow());
            metrics.setOpen(acc.getOpen());
            metrics.setClose(acc.getClose());
            
            out.collect(metrics);
        }
    }
    
    // User trading statistics processor
    public static class UserTradingStatsProcessor extends KeyedProcessFunction<String, TradeExecutedEvent, UserTradingStats> {
        private ValueState<UserTradingStats> statsState;
        
        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<UserTradingStats> descriptor = new ValueStateDescriptor<>(
                "user-trading-stats",
                UserTradingStats.class
            );
            statsState = getRuntimeContext().getState(descriptor);
        }
        
        @Override
        public void processElement(TradeExecutedEvent trade, Context ctx, Collector<UserTradingStats> out) throws Exception {
            UserTradingStats stats = statsState.value();
            if (stats == null) {
                stats = new UserTradingStats();
                stats.setUserId(trade.getTakerUserId());
            }
            
            // Update statistics
            stats.incrementTradeCount();
            stats.addVolume(trade.getSize().multiply(trade.getPrice()));
            stats.addFeesPaid(trade.getTakerFee());
            
            // Calculate maker/taker ratio
            if (trade.getMakerUserId().equals(trade.getTakerUserId())) {
                stats.incrementMakerCount();
            }
            
            statsState.update(stats);
            out.collect(stats);
        }
    }
    
    // Liquidation risk monitor
    public static class LiquidationRiskMonitor extends CoProcessFunction<PositionOpenedEvent, TradeExecutedEvent, LiquidationRiskAlert> {
        private ValueState<PositionRiskState> positionState;
        private ValueState<Double> lastPriceState;
        
        @Override
        public void open(Configuration parameters) {
            positionState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("position-risk", PositionRiskState.class)
            );
            lastPriceState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("last-price", Double.class)
            );
        }
        
        @Override
        public void processElement1(PositionOpenedEvent position, Context ctx, Collector<LiquidationRiskAlert> out) throws Exception {
            PositionRiskState riskState = new PositionRiskState();
            riskState.setPositionId(position.getPositionId());
            riskState.setUserId(position.getUserId());
            riskState.setMarketId(position.getMarketId());
            riskState.setSide(position.getSide());
            riskState.setEntryPrice(position.getEntryPrice());
            riskState.setLiquidationPrice(position.getLiquidationPrice());
            riskState.setHealthFactor(position.getInitialHealthFactor());
            
            positionState.update(riskState);
        }
        
        @Override
        public void processElement2(TradeExecutedEvent trade, Context ctx, Collector<LiquidationRiskAlert> out) throws Exception {
            Double lastPrice = Double.parseDouble(trade.getPrice());
            lastPriceState.update(lastPrice);
            
            PositionRiskState position = positionState.value();
            if (position != null) {
                // Calculate updated health factor based on new price
                double healthFactor = calculateHealthFactor(position, lastPrice);
                
                // Generate alert if health factor is low
                if (healthFactor < 1.2) {
                    LiquidationRiskAlert alert = new LiquidationRiskAlert();
                    alert.setPositionId(position.getPositionId());
                    alert.setUserId(position.getUserId());
                    alert.setMarketId(position.getMarketId());
                    alert.setCurrentPrice(lastPrice);
                    alert.setHealthFactor(healthFactor);
                    alert.setLiquidationPrice(position.getLiquidationPrice());
                    alert.setSeverity(healthFactor < 1.05 ? "CRITICAL" : "WARNING");
                    alert.setTimestamp(System.currentTimeMillis());
                    
                    out.collect(alert);
                }
            }
        }
        
        private double calculateHealthFactor(PositionRiskState position, double currentPrice) {
            // Simplified health factor calculation
            double entryPrice = Double.parseDouble(position.getEntryPrice());
            double liquidationPrice = Double.parseDouble(position.getLiquidationPrice());
            
            if (position.getSide().equals("LONG")) {
                return (currentPrice - liquidationPrice) / (entryPrice - liquidationPrice);
            } else {
                return (liquidationPrice - currentPrice) / (liquidationPrice - entryPrice);
            }
        }
    }
    
    // Anomaly detector using statistical methods
    public static class AnomalyDetector extends KeyedProcessFunction<String, MarketMetrics, MarketAnomaly> {
        private ValueState<AnomalyDetectionState> anomalyState;
        
        @Override
        public void open(Configuration parameters) {
            anomalyState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("anomaly-state", AnomalyDetectionState.class)
            );
        }
        
        @Override
        public void processElement(MarketMetrics metrics, Context ctx, Collector<MarketAnomaly> out) throws Exception {
            AnomalyDetectionState state = anomalyState.value();
            if (state == null) {
                state = new AnomalyDetectionState();
            }
            
            // Update moving averages and standard deviations
            state.updateStatistics(metrics);
            
            // Check for anomalies
            if (state.hasEnoughData()) {
                // Volume anomaly
                if (metrics.getVolume() > state.getVolumeThreshold(3.0)) { // 3 standard deviations
                    out.collect(createAnomaly(metrics, "VOLUME_SPIKE", "Unusual trading volume detected"));
                }
                
                // Price movement anomaly
                double priceChange = Math.abs(metrics.getClose() - metrics.getOpen()) / metrics.getOpen();
                if (priceChange > 0.1) { // 10% move
                    out.collect(createAnomaly(metrics, "PRICE_MOVEMENT", "Large price movement detected"));
                }
                
                // Spread anomaly
                if (metrics.getSpread() > state.getSpreadThreshold(2.5)) {
                    out.collect(createAnomaly(metrics, "SPREAD_WIDENING", "Abnormal spread widening"));
                }
            }
            
            anomalyState.update(state);
        }
        
        private MarketAnomaly createAnomaly(MarketMetrics metrics, String type, String description) {
            MarketAnomaly anomaly = new MarketAnomaly();
            anomaly.setMarketId(metrics.getMarketId());
            anomaly.setAnomalyType(type);
            anomaly.setDescription(description);
            anomaly.setTimestamp(System.currentTimeMillis());
            anomaly.setMetrics(metrics);
            return anomaly;
        }
    }
    
    // Cascade detector for liquidation cascades
    public static class CascadeDetector extends ProcessWindowFunction<LiquidationEvent, CascadeAlert, String, TimeWindow> {
        @Override
        public void process(String marketId, Context context, Iterable<LiquidationEvent> liquidations, Collector<CascadeAlert> out) {
            int liquidationCount = 0;
            double totalLiquidatedValue = 0.0;
            
            for (LiquidationEvent liquidation : liquidations) {
                liquidationCount++;
                totalLiquidatedValue += Double.parseDouble(liquidation.getLiquidatedSize()) * 
                                       Double.parseDouble(liquidation.getLiquidationPrice());
            }
            
            // Detect cascade based on liquidation velocity
            if (liquidationCount > 10 || totalLiquidatedValue > 1000000) { // Thresholds
                CascadeAlert alert = new CascadeAlert();
                alert.setMarketId(marketId);
                alert.setLiquidationCount(liquidationCount);
                alert.setTotalValue(totalLiquidatedValue);
                alert.setWindowStart(context.window().getStart());
                alert.setWindowEnd(context.window().getEnd());
                alert.setSeverity(liquidationCount > 20 ? "CRITICAL" : "WARNING");
                alert.setRecommendation("Consider circuit breaker activation");
                
                out.collect(alert);
            }
        }
    }
} 