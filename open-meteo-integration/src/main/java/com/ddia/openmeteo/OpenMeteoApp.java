package com.ddia.openmeteo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.integration.config.EnableIntegration;

@SpringBootApplication
@EnableIntegration
public class OpenMeteoApp {
    public static void main(String[] args) {
        SpringApplication.run(OpenMeteoApp.class, args);
    }
}
