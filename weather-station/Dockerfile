# Build stage
FROM maven:3.9.4-eclipse-temurin-17 AS build
WORKDIR /app
# We can copy the parent pom and modules
COPY pom.xml .
COPY weather-station weather-station
COPY central-station central-station
COPY kafka-processor kafka-processor
RUN mvn -pl weather-station -am clean package -DskipTests

# Run stage
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/weather-station/target/weather-station-1.0-SNAPSHOT.jar app.jar
# Run the application
ENTRYPOINT ["java", "-cp", "app.jar", "com.ddia.weatherstation.WeatherStationApp"]
