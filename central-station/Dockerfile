# Build stage
FROM maven:3.9.4-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
COPY weather-station weather-station
COPY central-station central-station
COPY kafka-processor kafka-processor
RUN mvn -pl central-station -am clean package -DskipTests

# Run stage
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/central-station/target/central-station-1.0-SNAPSHOT.jar app.jar
# Run the application with JFR enabled if needed
# ENTRYPOINT ["java", "-XX:StartFlightRecording=duration=60s,filename=myrecording.jfr", "-jar", "app.jar"]
ENTRYPOINT ["java", "-jar", "app.jar"]
