# Weather Stations Monitoring System

**Alexandria University - DDIA Course Project**

A distributed weather monitoring system built with Kafka, BitCask Riak, Parquet, ElasticSearch, and Kubernetes.

## Architecture

```
Weather Stations (1-10) --> Kafka (weather_status) --> Central Station
                              |                          |- BitCask (latest)
                              |                          |- Parquet (archive)
                              v                          |- REST :8080
                         Rain Detector                   |
                         (kafka-processor)               v
                              |                    Parquet Shipper --> ES/Kibana
                              v
                         raining_alerts (console verify)

Open-Meteo (bonus, station 999) --> weather_status
```

## Project Structure

```
DDIA_finalProject/
├── pom.xml
├── docker-compose.yml
├── bitcask_client.sh
├── _helper/
│   ├── getStart.md              # commands, Kibana, report assets
│   ├── enhancement-plan.md
│   └── eip-patterns.md
├── report/
│   ├── README.md
│   ├── generate_assets.py
│   └── assets/
├── weather-station/             # A+B: mock + Kafka producer
├── kafka-processor/             # C: rain detector (Compose service: rain-detector)
├── central-station/             # D: BitCask + Parquet + REST
├── open-meteo-integration/      # Bonus: Open-Meteo + EIP patterns
├── parquet-shipper/             # E: Parquet to Elasticsearch
└── k8s/
    └── deployment.yaml
```

## Service name map

| Compose / K8s | Maven module |
|---------------|--------------|
| `rain-detector` | `kafka-processor` |
| `weather-station-N` | `weather-station` |
| `central-station` | `central-station` |
| `parquet-shipper` | `parquet-shipper` |
| `open-meteo-integration` | `open-meteo-integration` |

## Project Report

Optional ES stats (stack running; index appears after first Parquet batch ~17 min):

```bash
cd report && python3 generate_assets.py --wait 1200
```

Screenshots and samples: `_helper/report-assets/` (see getStart.md).

## Quick Start

### Option 1: Docker Compose (recommended)

```bash
docker compose up --build -d
docker compose logs -f central-station rain-detector
docker compose down
```

### Option 2: Run locally via IDE

1. Start infrastructure only:
   ```bash
   docker compose up -d zookeeper kafka elasticsearch kibana
   ```
2. Open the project in IntelliJ / VS Code (parent `pom.xml`).
3. Run `CentralStationApp.java` (Spring Boot, port 8080)
4. Run `RainDetectorProcessor.java` with `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`
5. Run `WeatherStationApp.java` with `STATION_ID=1`

### Option 3: Kubernetes

```bash
docker build -t weather-station:latest -f weather-station/Dockerfile .
docker build -t central-station:latest -f central-station/Dockerfile .
docker build -t kafka-processor:latest -f kafka-processor/Dockerfile .
docker build -t open-meteo-integration:latest -f open-meteo-integration/Dockerfile .
docker build -t parquet-shipper:latest -f parquet-shipper/Dockerfile parquet-shipper

kubectl apply -f k8s/deployment.yaml
kubectl get pods -n weather-monitoring
```

## Components

### A) Weather Station Mock
- Produces 1 message per second per station to `weather_status`
- Battery: 30% low, 40% medium, 30% high
- 10% drop rate; drop events use `"message_dropped": true`
- `STATION_ID` environment variable

### B) Kafka Integration
- Java Kafka Producer API (`kafka-clients`)
- JSON messages; station ID as key

### C) Rain Detector (Kafka Streams)
- Module: `kafka-processor`; service: `rain-detector`
- Input: `weather_status`; output: `raining_alerts` when humidity > 70%

### D) Central Station
- **BitCask Riak**: segments, hint files, scheduled compaction
- **Parquet Archiver**: 10K batch default; partition `date=` / `station_id=`
- **REST API**: BitCask client endpoints

### E) BitCask Client
```bash
./bitcask_client.sh --view-all
./bitcask_client.sh --view --key=station_1
./bitcask_client.sh --perf --clients=100
```

### F) JFR Profiling
```bash
java -XX:StartFlightRecording=duration=60s,filename=recording.jfr \
     -jar central-station/target/central-station-1.0-SNAPSHOT.jar
```
Report in JDK Mission Control: top 10 classes by memory, GC pause count/max, I/O list.

## Services and Ports

| Service         | Port | URL                    |
|-----------------|------|------------------------|
| Central Station | 8080 | http://localhost:8080  |
| Kafka           | 9092 | localhost:9092       |
| Zookeeper       | 2181 | localhost:2181       |
| ElasticSearch   | 9200 | http://localhost:9200  |
| Kibana          | 5601 | http://localhost:5601  |
