package com.ddia.centralstation;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class CentralStationApp {
    public static void main(String[] args) {
        SpringApplication.run(CentralStationApp.class, args);
        
        System.out.println("Starting Central Station...");
        // TODO: Start Kafka Consumer to read from 'weather_status' topic
        // TODO: Integrate BitCask implementation to store latest status per station
        // TODO: Implement batching logic to write records to Parquet files partitioned by time and station ID
        // TODO: Implement JFR (Java Flight Recorder) profiling logic or use JVM flags
    }
}
