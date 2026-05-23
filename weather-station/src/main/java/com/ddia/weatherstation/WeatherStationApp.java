package com.ddia.weatherstation;

import com.ddia.weatherstation.model.Weather;
import com.ddia.weatherstation.model.WeatherStatusMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class WeatherStationApp {

    private static final String TOPIC = "weather_status";
    private static final double DROP_RATE = 0.1;

    public static void main(String[] args) throws Exception {
        long stationId = Long.parseLong(System.getenv().getOrDefault("STATION_ID", "1"));
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

        System.out.println("Starting Weather Station #" + stationId);
        System.out.println("Kafka broker: " + bootstrapServers);

        ObjectMapper mapper = new ObjectMapper();

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Ensure messages are sent immediately
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "0");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            long sNo = 0;
            Random random = new Random();

            while (true) {
                sNo++;

                // Randomly drop 10% of messages (publish marker for Parquet/ES/Kibana analytics)
                if (random.nextDouble() < DROP_RATE) {
                    WeatherStatusMessage dropMarker = new WeatherStatusMessage(
                            stationId,
                            sNo,
                            null,
                            System.currentTimeMillis() / 1000,
                            null
                    );
                    dropMarker.setMessageDropped(true);
                    String dropJson = mapper.writeValueAsString(dropMarker);
                    producer.send(new ProducerRecord<>(TOPIC, String.valueOf(stationId), dropJson));
                    System.out.println("[Station " + stationId + "] Dropped message s_no=" + sNo);
                    Thread.sleep(1000);
                    continue;
                }

                // Battery status: 30% low, 40% medium, 30% high
                String batteryStatus = randomBatteryStatus(random);

                // Random weather data
                Weather weather = new Weather(
                        random.nextInt(101),       // humidity 0-100%
                        random.nextInt(61) + 50,   // temperature 50-110 F
                        random.nextInt(100)         // wind speed 0-99 km/h
                );

                WeatherStatusMessage message = new WeatherStatusMessage(
                        stationId,
                        sNo,
                        batteryStatus,
                        System.currentTimeMillis() / 1000,
                        weather
                );
                message.setMessageDropped(false);

                String json = mapper.writeValueAsString(message);
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(TOPIC, String.valueOf(stationId), json);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Error sending message: " + exception.getMessage());
                    }
                });

                System.out.println("[Station " + stationId + "] Sent s_no=" + sNo
                        + " battery=" + batteryStatus
                        + " humidity=" + weather.getHumidity());

                Thread.sleep(1000);
            }
        }
    }

    private static String randomBatteryStatus(Random random) {
        double r = random.nextDouble();
        if (r < 0.3) return "low";
        if (r < 0.7) return "medium";
        return "high";
    }
}
