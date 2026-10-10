# =========================================================
# STAGE 1: BUILD THE SPRING BOOT APPLICATION
# =========================================================
FROM maven:3.9-eclipse-temurin-21 AS build

WORKDIR /app

# Copy Maven configuration
COPY pom.xml .

# Download dependencies
RUN mvn -B dependency:go-offline

# Copy application source code
COPY src ./src

# Build the executable Spring Boot JAR
RUN mvn -B clean package -DskipTests


# =========================================================
# STAGE 2: RUN THE BRAIN SERVICE
# =========================================================
FROM eclipse-temurin:21-jre

WORKDIR /app

# Application configuration
ENV SERVER_PORT=8080
ENV KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092
ENV REDIS_HOST=host.docker.internal
ENV REDIS_PORT=6379
ENV AI_API_URL=http://host.docker.internal:11434/api/chat

# Copy the generated JAR
COPY --from=build /app/target/*.jar app.jar

# Expose the Spring Boot port
EXPOSE 8080

# Start the Brain service
ENTRYPOINT ["java", "-jar", "app.jar"]