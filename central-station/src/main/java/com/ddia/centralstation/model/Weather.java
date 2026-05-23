package com.ddia.centralstation.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Weather {
    private int humidity;
    private int temperature;
    @JsonProperty("wind_speed")
    private int windSpeed;

    public Weather() {}

    public Weather(int humidity, int temperature, int windSpeed) {
        this.humidity = humidity;
        this.temperature = temperature;
        this.windSpeed = windSpeed;
    }

    public int getHumidity() { return humidity; }
    public void setHumidity(int humidity) { this.humidity = humidity; }
    public int getTemperature() { return temperature; }
    public void setTemperature(int temperature) { this.temperature = temperature; }
    public int getWindSpeed() { return windSpeed; }
    public void setWindSpeed(int windSpeed) { this.windSpeed = windSpeed; }
}
