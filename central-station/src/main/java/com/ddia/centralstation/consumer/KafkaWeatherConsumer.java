package com.ddia.centralstation.consumer;

import com.ddia.centralstation.archiver.ParquetArchiver;
import com.ddia.centralstation.bitcask.BitCask;
import com.ddia.centralstation.model.WeatherStatusMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Kafka consumer that reads weather status messages from the 'weather_status' topic.
 * For each message:
 *   1. Updates the BitCask store with the latest status per station.
 *   2. Sends the message to the ParquetArchiver for batch writing.
 */
@Component
public class KafkaWeatherConsumer {

    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServers;

    @Value("${kafka.topic}")
    private String topic;

    @Value("${kafka.group.id}")
    private String groupId;

    private final BitCask bitCask;
    private final ParquetArchiver parquetArchiver;
    private final ObjectMapper mapper = new ObjectMapper();

    private volatile boolean running = true;
    private Thread consumerThread;

    public KafkaWeatherConsumer(BitCask bitCask, ParquetArchiver parquetArchiver) {
        this.bitCask = bitCask;
        this.parquetArchiver = parquetArchiver;
    }

    @PostConstruct
    public void start() {
        consumerThread = new Thread(this::consume, "kafka-weather-consumer");
        consumerThread.setDaemon(true);
        consumerThread.start();
        System.out.println("[KafkaConsumer] Started — topic=" + topic + " broker=" + bootstrapServers);
    }

    @PreDestroy
    public void stop() {
        running = false;
        if (consumerThread != null) consumerThread.interrupt();
        parquetArchiver.flush();
        System.out.println("[KafkaConsumer] Stopped.");
    }

    private void consume() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));

            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        WeatherStatusMessage msg = mapper.readValue(record.value(), WeatherStatusMessage.class);

                        // Archive all messages (including drop markers) to Parquet/ES
                        parquetArchiver.archive(msg);

                        // Drop markers are not latest station readings — skip BitCask update
                        if (msg.isMessageDropped()) {
                            continue;
                        }

                        String key = "station_" + msg.getStationId();
                        bitCask.put(key, record.value());

                    } catch (Exception e) {
                        System.err.println("[KafkaConsumer] Error processing record: " + e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            if (running) {
                System.err.println("[KafkaConsumer] Fatal error: " + e.getMessage());
            }
        }
    }
}
