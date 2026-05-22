package com.ddia.centralstation;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Central Station — the heart of the weather monitoring system.
 *
 * Responsibilities:
 *   1. Consume weather status messages from Kafka
 *   2. Maintain an up-to-date BitCask key-value store (latest status per station)
 *   3. Archive all messages to Parquet files (batched, partitioned by date and station ID)
 *   4. Expose REST API for the BitCask client
 *
 * To run with JFR profiling (for the project report):
 *   java -XX:StartFlightRecording=duration=60s,filename=recording.jfr -jar central-station.jar
 */
@SpringBootApplication
public class CentralStationApp {
    public static void main(String[] args) {
        SpringApplication.run(CentralStationApp.class, args);
    }
}
