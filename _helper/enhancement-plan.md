# Enhancement Plan (offline copy)

Reference lab PDF: [Project.pdf](Project.pdf). Runbook: [getStart.md](getStart.md).

## Requirements coverage

| PDF section | Status | Location |
|-------------|--------|----------|
| A Weather mock | Done | `weather-station` |
| B Kafka producer | Done | `weather-station` |
| C Rain processor | Done | `kafka-processor` (service name: `rain-detector`) |
| D Central station | Done | `central-station`, `bitcask_client.sh` |
| E ES/Kibana | Done | `parquet-shipper` indexes Parquet |
| F Kubernetes | Done | `k8s/deployment.yaml` |
| G JFR | Documented | `getStart.md`, README |
| Bonus Open-Meteo + EIP | Done | `open-meteo-integration`, see `eip-patterns.md` |

## Naming convention (minimal churn)

- Maven module stays `kafka-processor`; runtime service is `rain-detector`.
- Document mapping in README and `getStart.md`.
- Shared POJO extraction (`common-models`) deferred to avoid large refactor.

## Changes applied in this pass

1. Helper docs in `_helper/` (`getStart.md`, this file, `eip-patterns.md`)
2. `.gitignore` allows `_helper/*.md` and report skeleton paths
3. Style: no emoji, no em dashes in code/comments; ASCII section headers
4. Open-Meteo URL includes `hourly=relativehumidity_2m` in `application.properties`
5. Rain alert logging uses plain text; optional POJO left as future work
6. `report/` directory with README and optional `generate_assets.py`
7. Root README updated: full module tree, all Docker images, link to getStart

## Known debt (optional later)

- 10 duplicate weather-station blocks in `k8s/deployment.yaml` (Kustomize loop)
- `raining_alerts` topic has no consumer (console verification only)
- Triplicated `Weather` / `WeatherStatusMessage` in three modules

## Report assets

Collect manually per `getStart.md` into `_helper/report-assets/` or `report/assets/`.
