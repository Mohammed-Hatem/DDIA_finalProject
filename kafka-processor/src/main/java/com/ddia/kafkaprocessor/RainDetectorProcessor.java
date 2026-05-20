package com.ddia.kafkaprocessor;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class RainDetectorProcessor {
    public static void main(String[] args) {
        System.out.println("Starting Rain Detector Kafka Processor...");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "rain-detector-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Use kafka:9092 in docker

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> weatherStream = builder.stream("weather_status");

        // TODO: Parse JSON value
        // TODO: Filter messages where humidity > 70%
        // TODO: Map to special message
        // TODO: Output to a new topic e.g., 'raining_alerts'
        
        /* Example:
        weatherStream.filter((key, value) -> {
            // logic to check humidity > 70
            return false; 
        }).to("raining_alerts");
        */

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
