# ---- Stage 1: Build ----
FROM eclipse-temurin:17-jdk AS builder

WORKDIR /app

COPY mvnw .
COPY .mvn .mvn
COPY pom.xml .

RUN ./mvnw dependency:go-offline -B

COPY src ./src
RUN ./mvnw clean package -DskipTests -B

# Show what jar was built (helps debug name mismatches)
RUN ls -la target/*.jar

# ---- Stage 2: Run ----
FROM eclipse-temurin:17-jre

WORKDIR /app

RUN groupadd -r appgroup && useradd -r -g appgroup appuser

# Create log directory with proper permissions
RUN mkdir -p /var/log/delta-ingestion

COPY --from=builder /app/target/*.jar app.jar

RUN chown appuser:appgroup app.jar && chown -R appuser:appgroup /var/log/delta-ingestion

USER appuser

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]
