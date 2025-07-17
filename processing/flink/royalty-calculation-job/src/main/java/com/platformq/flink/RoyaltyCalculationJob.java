package com.platformq.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.pulsar.PulsarSource;
import org.apache.flink.connector.pulsar.PulsarSink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

public class RoyaltyCalculationJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Source for sales events
        DataStream<String> salesStream = env.addSource(new PulsarSource<>("sales-events-topic", new SimpleStringSchema()))
                .name("Sales Events Source");

        // Source for usage events
        DataStream<String> usageStream = env.addSource(new PulsarSource<>("asset-usage-events", new SimpleStringSchema()))
                .name("Usage Events Source");

        // Parse sales
        DataStream<Tuple2<String, Double>> parsedSales = salesStream.map(new MapFunction<String, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(String value) {
                // Parse JSON to get asset_id and sale_amount
                // Placeholder
                return new Tuple2<>("asset1", 100.0);
            }
        });

        // Parse usage
        DataStream<Tuple2<String, Double>> parsedUsage = usageStream.map(new MapFunction<String, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(String value) {
                // Parse JSON to get asset_id and usage_duration
                // Calculate royalty based on duration, e.g., 0.1 per minute
                // Placeholder
                return new Tuple2<>("asset1", 10.0);
            }
        });

        // Union streams
        DataStream<Tuple2<String, Double>> combined = parsedSales.union(parsedUsage);

        // Key by asset_id, window by hour, sum royalties
        DataStream<String> royalties = combined
                .keyBy(tuple -> tuple.f0)
                .timeWindow(Time.hours(1))
                .apply(new WindowFunction<Tuple2<String, Double>, String, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<Tuple2<String, Double>> input, Collector<String> out) throws Exception {
                        double total = 0.0;
                        for (Tuple2<String, Double> tuple : input) {
                            total += tuple.f1;
                        }
                        out.collect("Asset " + key + " royalty: " + total);
                    }
                });

        // Sink to Pulsar royalty topic
        royalties.addSink(new PulsarSink<>("royalty-output-topic", new SimpleStringSchema()));

        env.execute("Royalty Calculation Job");
    }
} 