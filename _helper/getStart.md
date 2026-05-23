# Get Started - Weather Stations Monitoring (DDIA Lab)

Lab specification: [_helper/Project.pdf](Project.pdf) (case-sensitive on Linux).

Full enhancement notes: [enhancement-plan.md](enhancement-plan.md). Bonus EIP patterns: [eip-patterns.md](eip-patterns.md).

## Prerequisites

- Docker and Docker Compose
- Java 17+ (Java 21 works; project targets 17 in `pom.xml`)
- Maven 3.8+ (for local IDE runs)
- Python 3 + pip (parquet-shipper, optional `report/generate_assets.py`)
- Optional: `kubectl` and a local cluster (minikube, kind, or Docker Desktop Kubernetes)

### Download dependencies (one script)

From repo root (no `sudo` if Maven is missing: installs to `~/.local/opt`):

```bash
./scripts/setup-deps.sh
```

Or step by step:

```bash
# Maven: system package OR user install
sudo apt install openjdk-17-jdk maven   # if you have sudo

export PATH="$HOME/.local/opt/apache-maven-3.9.6/bin:$PATH"   # if setup script installed Maven
cd DDIA_finalProject
mvn -B dependency:go-offline package -DskipTests

python3 -m pip install --user pandas pyarrow elasticsearch numpy
docker compose pull
```

After setup, Maven cache is under `~/.m2/repository` and JARs under `*/target/`.

## Service name map

| Compose / K8s name | Maven module | Role |
|--------------------|--------------|------|
| `weather-station-1` ... `weather-station-10` | `weather-station` | Mock producers |
| `rain-detector` | `kafka-processor` | Kafka Streams rain alerts |
| `central-station` | `central-station` | BitCask + Parquet + REST |
| `parquet-shipper` | `parquet-shipper` | Parquet to Elasticsearch |
| `open-meteo-integration` | `open-meteo-integration` | Bonus API feed (station 999) |

## Option 1: Docker Compose (recommended)

```bash
cd DDIA_finalProject
docker compose up --build -d
docker compose ps
docker compose logs -f central-station rain-detector
```

Stop:

```bash
docker compose down
```

### Verify Kafka (PDF section B)

After Kafka is healthy:

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic weather_status \
  --from-beginning \
  --max-messages 5
```

Rain alerts topic (no dedicated consumer in stack; console check only):

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic raining_alerts \
  --from-beginning \
  --max-messages 5
```

### BitCask client (PDF section D)

From repo root (central station must be on port 8080):

```bash
./bitcask_client.sh --view-all
./bitcask_client.sh --view --key=station_1
./bitcask_client.sh --perf --clients=100
```

Outputs: `<unix_timestamp>.csv` or `<unix_timestamp>_thread_<N>.csv`.

### REST smoke test

```bash
curl -s http://localhost:8080/api/bitcask/key/station_1
curl -s http://localhost:8080/api/bitcask/all | head -c 500
```

## Option 2: Local IDE

1. Infrastructure only:

```bash
docker compose up -d zookeeper kafka elasticsearch kibana
```

2. Build:

```bash
mvn -q package -DskipTests
```

3. Run (separate terminals):

- `CentralStationApp` (port 8080)
- `RainDetectorProcessor` with `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`
- `WeatherStationApp` with `STATION_ID=1` and `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`

## Option 3: Kubernetes

**Prerequisite:** a running cluster and `kubectl` context. If you see `current-context is not set` or `could not find the requested resource`, no cluster is running.

### Quick setup (kind + Docker)

```bash
./scripts/setup-k8s.sh
kubectl get pods -n weather-monitoring
```

Manual steps:

```bash
# 1) Install kind and create cluster (once)
curl -Lo ~/.local/bin/kind https://kind.sigs.k8s.io/dl/v0.27.0/kind-linux-amd64
chmod +x ~/.local/bin/kind
kind create cluster --name ddia

# 2) Build and load images into kind
docker build -t weather-station:latest -f weather-station/Dockerfile .
docker build -t central-station:latest -f central-station/Dockerfile .
docker build -t kafka-processor:latest -f kafka-processor/Dockerfile .
docker build -t open-meteo-integration:latest -f open-meteo-integration/Dockerfile .
docker build -t parquet-shipper:latest -f parquet-shipper/Dockerfile parquet-shipper
kind load docker-image weather-station:latest central-station:latest kafka-processor:latest open-meteo-integration:latest parquet-shipper:latest --name ddia

# 3) Deploy (kind: use hostPath PV for shared BitCask/Parquet volume)
kubectl apply -f k8s/deployment.yaml
kubectl delete pvc shared-pvc -n weather-monitoring --ignore-not-found
kubectl apply -f k8s/kind-pv.yaml
kubectl get pods -n weather-monitoring
```

## JFR profiling (PDF section G)

Run central station for about 1 minute with **`settings=profile`** (required for File/Socket I/O in JMC):

```bash
./scripts/record-jfr.sh
```

Or manually:

```bash
PARQUET_BATCH_SIZE=100 \
java -XX:StartFlightRecording=duration=60s,filename=recording.jfr,settings=central-station/jfr-io.jfc \
  -jar central-station/target/central-station-1.0-SNAPSHOT.jar
```

Verify I/O was captured:

```bash
jfr summary recording.jfr | grep -E 'FileRead|FileWrite|SocketRead|SocketWrite'
```

Open `recording.jfr` in JDK Mission Control (JMC is separate from the JDK on Linux):

```bash
./scripts/open-jfr.sh recording.jfr
# or: export PATH="$HOME/.local/opt/JDK Mission Control:$PATH" && jmc -open recording.jfr
```

Text summary without GUI:

```bash
./scripts/jfr-report-cli.sh recording.jfr
```

Capture for the report:

1. Top 10 classes by total memory
2. GC pause count
3. GC maximum pause duration
4. I/O operations list

Save screenshots under `_helper/report-assets/` (e.g. `jfr-memory.png`, `jfr-gc.png`, `jfr-io.png`).

## Kibana and Elasticsearch (PDF section E)

Allow 5-10 minutes after `docker compose up` for data to accumulate.

1. Open http://localhost:5601
2. Stack Management > Data Views > create `weather-status` (or `weather-status*`)
3. Time field: `@timestamp` if present, else `status_timestamp`

### Screenshot: battery distribution (30% / 40% / 30%)

- Filter: `message_dropped: false`
- Visualization: pie or bar on `battery_status.keyword` (or `battery_status`)
- Expected rough mix: low ~30%, medium ~40%, high ~30%

Save: `_helper/report-assets/kibana-battery.png`

### Screenshot: dropped messages (~10%)

- Compare count where `message_dropped: true` vs total per `station_id`
- Or metric: `sum(message_dropped) / count` by station

Save: `_helper/report-assets/kibana-drops.png`

### Bonus: Open-Meteo station

Filter `station_id: 999` in Discover to confirm bonus feed.

## Sample deliverables (PDF section 5)

Parquet layout (under shared volume `/data/parquet`):

```
/data/parquet/date=YYYY-MM-DD/station_id=<id>/batch_<timestamp>.parquet
```

BitCask files: `/data/bitcask/segment_*` and `hint_*`

### Copy from running Compose stack

```bash
# List parquet partitions
docker compose exec central-station ls -R /data/parquet

# Example copy (replace container name and path from ls output)
docker compose ps -q central-station
docker cp "$(docker compose ps -q central-station)":/data/parquet/date=2026-05-23/station_id=1/batch_XXXXX.parquet \
  _helper/report-assets/sample.parquet

# BitCask sample directory
docker cp "$(docker compose ps -q central-station)":/data/bitcask \
  _helper/report-assets/bitcask-sample
```

Also see [report/README.md](../report/README.md) for the formal report bundle layout.

## Report asset checklist

| Asset | Action | Path |
|-------|--------|------|
| Source + Docker + K8s | In repo | root |
| Kibana battery chart | Screenshot after 5-10 min | `_helper/report-assets/kibana-battery.png` |
| Kibana drop rate | Screenshot | `_helper/report-assets/kibana-drops.png` |
| Sample Parquet | `docker cp` one batch file | `_helper/report-assets/sample.parquet` |
| Sample BitCask dir | `docker cp` `/data/bitcask` | `_helper/report-assets/bitcask-sample/` |
| JFR findings | Mission Control screenshots | `_helper/report-assets/jfr-*.png` |
| Written report | PDF/doc with above | team choice |

Optional ES stats script (stack running):

```bash
cd report && python3 generate_assets.py
```

## Timing notes

- Weather stations emit 1 msg/sec each (10 stations).
- Parquet batches flush at 10K records or on shutdown; small runs may need a few minutes before files appear.
- `parquet-shipper` indexes new files into Elasticsearch every few seconds.
- Kibana charts need enough indexed documents before percentages look stable.

## Troubleshooting

| Issue | Check |
|-------|-------|
| Empty BitCask | `docker compose logs central-station`; Kafka consumer connected? |
| No Parquet files | Wait longer or stop stack gracefully to flush buffer |
| Kibana empty | `curl http://localhost:9200/weather-status/_count` |
| Rain alerts empty | Humidity must exceed 70%; dropped messages are skipped |
