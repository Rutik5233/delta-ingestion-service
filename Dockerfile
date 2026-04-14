# ---- Stage 1: Build ----
# Use a full JDK image to compile and package the application
FROM eclipse-temurin:17-jdk-alpine AS builder

WORKDIR /app

# Copy maven wrapper and pom first — Docker caches this layer separately.
# If only source code changes (not pom.xml), this layer is reused → faster builds.
COPY mvnw .
COPY .mvn .mvn
COPY pom.xml .

# Download all dependencies (cached as a separate layer)
RUN ./mvnw dependency:go-offline -B

# Now copy source and build
COPY src ./src
RUN ./mvnw clean package -DskipTests -B

# ---- Stage 2: Run ----
# Use a slim JRE-only image — no compiler, no build tools, smaller attack surface.
# Final image is ~180MB instead of ~600MB with a full JDK.
FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

# Create a non-root user — never run your app as root inside a container
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

# Copy only the built jar from the builder stage
COPY --from=builder /app/target/delta-ingestion-service-1.0.0.jar app.jar

# Give ownership to non-root user
RUN chown appuser:appgroup app.jar

USER appuser

# Document which port the app listens on (doesn't actually publish it — that's docker-compose's job)
EXPOSE 8080

# Use exec form (not shell form) so signals like SIGTERM reach the JVM directly.
# This allows graceful shutdown to work correctly.
ENTRYPOINT ["java", "-jar", "app.jar"]
