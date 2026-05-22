package com.ddia.kafkaprocessor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

/**
 * Kafka Streams processor that detects rain conditions.
 * Filters weather messages where humidity > 70% and outputs
 * a special alert message to the 'raining_alerts' topic.
 */
public class RainDetectorProcessor {

    private static final String INPUT_TOPIC = "weather_status";
    private static final String OUTPUT_TOPIC = "raining_alerts";
    private static final int HUMIDITY_THRESHOLD = 70;

    public static void main(String[] args) {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

        System.out.println("Starting Rain Detector Processor...");
        System.out.println("Kafka broker: " + bootstrapServers);
        System.out.println("Humidity threshold: " + HUMIDITY_THRESHOLD + "%");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "rain-detector-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        ObjectMapper mapper = new ObjectMapper();

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> weatherStream = builder.stream(INPUT_TOPIC);

        // Filter for humidity > 70% and create rain alert messages
        weatherStream
                .filter((key, value) -> {
                    try {
                        JsonNode node = mapper.readTree(value);
                        if (node.has("message_dropped") && node.get("message_dropped").asBoolean()) {
                            return false;
                        }
                        if (!node.has("weather") || node.get("weather").isNull()) {
                            return false;
                        }
                        int humidity = node.get("weather").get("humidity").asInt();
                        return humidity > HUMIDITY_THRESHOLD;
                    } catch (Exception e) {
                        System.err.println("Error parsing message: " + e.getMessage());
                        return false;
                    }
                })
                .mapValues(value -> {
                    try {
                        JsonNode node = mapper.readTree(value);
                        if (node.has("message_dropped") && node.get("message_dropped").asBoolean()) {
                            return value;
                        }
                        long stationId = node.get("station_id").asLong();
                        int humidity = node.get("weather").get("humidity").asInt();
                        long timestamp = node.get("status_timestamp").asLong();

                        String alert = String.format(
                                "{\"alert\":\"RAIN_DETECTED\","
                                        + "\"station_id\":%d,"
                                        + "\"humidity\":%d,"
                                        + "\"timestamp\":%d}",
                                stationId, humidity, timestamp);

                        System.out.println("🌧 Rain detected! Station " + stationId
                                + " humidity=" + humidity + "%");
                        return alert;
                    } catch (Exception e) {
                        return value;
                    }
                })
                .to(OUTPUT_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // Graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down Rain Detector...");
            streams.close();
        }));

        streams.start();
        System.out.println("Rain Detector Processor is running.");
    }
}
