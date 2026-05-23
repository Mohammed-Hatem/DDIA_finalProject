# Enterprise Integration Patterns (Bonus)

Open-Meteo integration module: `open-meteo-integration/src/main/java/com/ddia/openmeteo/config/IntegrationConfig.java`

| Pattern | Implementation |
|---------|----------------|
| Polling consumer | `openMeteoSource` polled every 10s via `Pollers.fixedDelay(10000)` |
| Invalid message channel | `invalidMessageChannel` + `.discardChannel("invalidMessageChannel")` |
| Dead letter channel | `deadLetterChannel` via `ExpressionEvaluatingRequestHandlerAdvice` |
| Idempotent receiver | `IdempotentReceiverInterceptor` keyed by `s_no` |
| Channel adapter | `RestTemplate` HTTP calls to Open-Meteo API |
| Message transformer | `transformToDomain`: `JsonNode` to `WeatherStatusMessage` |

Count: 6 patterns (lab bonus asks for 5 to 6).

Virtual station id: `999`. Topic: `weather_status` (same as mock stations).
