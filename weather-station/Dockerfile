# Build stage
FROM maven:3.9.4-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
COPY weather-station weather-station
COPY central-station central-station
COPY kafka-processor kafka-processor
COPY open-meteo-integration open-meteo-integration
RUN mvn -pl weather-station -am clean package -DskipTests

# Run stage
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/weather-station/target/weather-station-1.0-SNAPSHOT.jar app.jar
# STATION_ID and KAFKA_BOOTSTRAP_SERVERS are injected via environment variables
ENTRYPOINT ["java", "-jar", "app.jar"]
