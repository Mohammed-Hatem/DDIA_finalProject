# Weather Stations Monitoring System

**Alexandria University — DDIA Course Project (CSE-4E3)**

A distributed weather monitoring system built with Kafka, BitCask Riak, Parquet, ElasticSearch, and Kubernetes.

## Architecture

```
┌─────────────────┐     ┌─────────────┐     ┌─────────────────────────────────┐
│ Weather Station │────▶│             │     │        Central Station          │
│   1..10 (Mock)  │     │    Kafka    │────▶│  ┌──────────┐  ┌────────────┐  │
│  (1 msg/sec)    │     │  (Broker)   │     │  │ BitCask  │  │  Parquet   │  │
└─────────────────┘     │             │     │  │ (Latest) │  │ (Archive)  │  │
                        └──────┬──────┘     │  └──────────┘  └─────┬──────┘  │
                               │            │  REST API (:8080)    │         │
                        ┌──────▼──────┐     └──────────────────────┼─────────┘
                        │    Rain     │                            │
                        │  Detector   │                    ┌───────▼───────┐
                        │ (humidity   │                    │ ElasticSearch │
                        │   > 70%)    │                    │   + Kibana    │
                        └─────────────┘                    └───────────────┘
```

## Project Structure

```
ddia/
├── pom.xml                        # Parent Maven POM
├── docker-compose.yml             # Full stack for local development
├── bitcask_client.sh              # CLI to query BitCask via REST API
│
├── weather-station/               # Module A+B: Weather Station Mock + Kafka Producer
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/main/java/com/ddia/weatherstation/
│       ├── WeatherStationApp.java
│       └── model/
│           ├── Weather.java
│           └── WeatherStatusMessage.java
│
├── kafka-processor/               # Module C: Rain Detection (Kafka Streams DSL)
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/main/java/com/ddia/kafkaprocessor/
│       └── RainDetectorProcessor.java
│
├── central-station/               # Module D: Central Station (BitCask + Parquet + REST)
│   ├── pom.xml
│   ├── Dockerfile
│   ├── src/main/resources/application.properties
│   └── src/main/java/com/ddia/centralstation/
│       ├── CentralStationApp.java
│       ├── bitcask/
│       │   ├── BitCask.java       # Full BitCask Riak implementation
│       │   └── KeyDirEntry.java
│       ├── archiver/
│       │   └── ParquetArchiver.java
│       ├── consumer/
│       │   └── KafkaWeatherConsumer.java
│       ├── controller/
│       │   └── BitCaskController.java
│       └── model/
│           ├── Weather.java
│           └── WeatherStatusMessage.java
│
└── k8s/
    └── deployment.yaml            # Kubernetes manifests for all services
```

## Project Report

Generated report assets (screenshots, samples, write-up): **[report/README.md](report/README.md)**

Regenerate charts while the stack is running:

```bash
cd report && .venv/bin/python generate_assets.py
```

## Quick Start

### Option 1: Docker Compose (Recommended for development)

```bash
# Build and start everything
docker compose up --build -d

# Watch logs
docker compose logs -f central-station
docker compose logs -f weather-station-1

# Stop everything
docker compose down
```

### Option 2: Run locally via IDE

1. Start infrastructure only:
   ```bash
   docker compose up -d zookeeper kafka elasticsearch kibana
   ```
2. Open the project in IntelliJ / VS Code (it will detect the parent `pom.xml`).
3. Run `CentralStationApp.java` (Spring Boot — port 8080)
4. Run `RainDetectorProcessor.java`
5. Run `WeatherStationApp.java` (set `STATION_ID` env variable)

### Option 3: Kubernetes

```bash
# Build Docker images
docker build -t weather-station:latest -f weather-station/Dockerfile .
docker build -t central-station:latest -f central-station/Dockerfile .
docker build -t kafka-processor:latest -f kafka-processor/Dockerfile .

# Deploy
kubectl apply -f k8s/deployment.yaml

# Check pods
kubectl get pods -n weather-monitoring
```

## Components

### A) Weather Station Mock
- Produces 1 message per second per station to the `weather_status` Kafka topic
- Battery status distribution: 30% low, 40% medium, 30% high
- 10% random message drop rate (s_no still increments); drop events are published with `"message_dropped": true` for Kibana/ES
- Station ID configurable via `STATION_ID` environment variable

### B) Kafka Integration
- Uses Java Kafka Producer API (`kafka-clients`)
- Messages are JSON-serialized with station ID as the Kafka key

### C) Rain Detector (Kafka Streams)
- Reads from `weather_status` topic
- Filters messages where `weather.humidity > 70%`
- Outputs alert to the `raining_alerts` topic

### D) Central Station
- **BitCask Riak**: Append-only log with in-memory keyDir, hint files, scheduled compaction
- **Parquet Archiver**: Batches records (10K default) and writes Parquet files partitioned by date and station ID (includes `message_dropped` flag)
- **REST API**: Exposes BitCask data for the bash client

### E) BitCask Client
```bash
# View all station statuses → saves to <timestamp>.csv
./bitcask_client.sh --view-all

# View a specific station
./bitcask_client.sh --view --key=station_1

# Performance test with 100 concurrent threads
./bitcask_client.sh --perf --clients=100
```

### F) JFR Profiling
Run the Central Station with Java Flight Recorder:
```bash
java -XX:StartFlightRecording=duration=60s,filename=recording.jfr \
     -jar central-station/target/central-station-1.0-SNAPSHOT.jar
```
Then open `recording.jfr` in JDK Mission Control to report:
- Top 10 classes with highest total memory
- GC pauses count and max pause duration
- List of I/O operations

## Services & Ports

| Service        | Port  | URL                          |
|----------------|-------|------------------------------|
| Central Station| 8080  | http://localhost:8080         |
| Kafka          | 9092  | localhost:9092               |
| Zookeeper      | 2181  | localhost:2181               |
| ElasticSearch  | 9200  | http://localhost:9200         |
| Kibana         | 5601  | http://localhost:5601         |
