package com.ddia.weatherstation;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class WeatherStationApp {
    public static void main(String[] args) {
        System.out.println("Starting Weather Station Mock...");
        
        // TODO: Read station ID and other configs from environment variables
        long stationId = 1L;
        
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Use kafka:9092 in docker
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            // TODO: Implement the loop to generate messages every 1 second
            // TODO: Implement random battery status (30% low, 40% med, 30% high)
            // TODO: Implement 10% message drop rate
            
            // Example message
            // String jsonMessage = "{...}";
            // ProducerRecord<String, String> record = new ProducerRecord<>("weather_status", String.valueOf(stationId), jsonMessage);
            // producer.send(record);
        }
    }
}
