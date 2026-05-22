package com.ddia.openmeteo.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class WeatherStatusMessage {
    @JsonProperty("station_id")
    private long stationId;
    @JsonProperty("s_no")
    private long sNo;
    @JsonProperty("battery_status")
    private String batteryStatus;
    @JsonProperty("status_timestamp")
    private long statusTimestamp;
    @JsonProperty("message_dropped")
    private boolean messageDropped;
    private Weather weather;

    public WeatherStatusMessage() {}

    public WeatherStatusMessage(long stationId, long sNo, String batteryStatus, long statusTimestamp, Weather weather) {
        this.stationId = stationId;
        this.sNo = sNo;
        this.batteryStatus = batteryStatus;
        this.statusTimestamp = statusTimestamp;
        this.weather = weather;
    }

    @JsonProperty("station_id")
    public long getStationId() { return stationId; }
    @JsonProperty("station_id")
    public void setStationId(long stationId) { this.stationId = stationId; }

    @JsonProperty("s_no")
    public long getSNo() { return sNo; }
    @JsonProperty("s_no")
    public void setSNo(long sNo) { this.sNo = sNo; }

    @JsonProperty("battery_status")
    public String getBatteryStatus() { return batteryStatus; }
    @JsonProperty("battery_status")
    public void setBatteryStatus(String batteryStatus) { this.batteryStatus = batteryStatus; }

    @JsonProperty("status_timestamp")
    public long getStatusTimestamp() { return statusTimestamp; }
    @JsonProperty("status_timestamp")
    public void setStatusTimestamp(long statusTimestamp) { this.statusTimestamp = statusTimestamp; }

    @JsonProperty("message_dropped")
    public boolean isMessageDropped() { return messageDropped; }
    @JsonProperty("message_dropped")
    public void setMessageDropped(boolean messageDropped) { this.messageDropped = messageDropped; }

    public Weather getWeather() { return weather; }
    public void setWeather(Weather weather) { this.weather = weather; }
}
