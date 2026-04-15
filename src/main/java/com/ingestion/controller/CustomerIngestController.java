package com.ingestion.controller;

import com.ingestion.dto.CustomerIngestRequest;
import com.ingestion.dto.IngestResponse;
import com.ingestion.service.CustomerIngestService;
import com.ingestion.service.LookupCacheService;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.PostConstruct;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Validated
@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
@Tag(name = "Customer Ingestion", description = "APIs for ingesting customer data with delta detection — only net-new records are inserted")
public class CustomerIngestController {

    private final CustomerIngestService ingestService;
    private final LookupCacheService    lookupCacheService;
    private final MeterRegistry         meterRegistry;

    // Backing reference for the success-rate Gauge — updated after every batch
    private final AtomicReference<Double> lastSuccessRate = new AtomicReference<>(0.0);

    @Value("${ingestion.max-batch-size:100000}")
    private int maxBatchSize;

    @PostConstruct
    void registerMetrics() {
        Gauge.builder("ingestion.success.rate.percent", lastSuccessRate, AtomicReference::get)
                .description("Success rate (%) of the most recent ingestion batch")
                .register(meterRegistry);
    }

    @Operation(
            summary = "Ingest customer batch",
            description = "Accepts a JSON array of customer records. Computes the delta against existing data and inserts only net-new customers. " +
                    "Existing customers (matched by external_id) are silently skipped. " +
                    "Maximum batch size is 100,000 records per request."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Batch processed successfully",
                    content = @Content(schema = @Schema(implementation = IngestResponse.class))),
            @ApiResponse(responseCode = "400", description = "Request body is empty or a record fails validation",
                    content = @Content(schema = @Schema(type = "string", example = "Batch cannot be empty"))),
            @ApiResponse(responseCode = "413", description = "Batch exceeds the maximum allowed size of 100,000 records",
                    content = @Content(schema = @Schema(type = "string", example = "Batch size 150000 exceeds maximum 100000"))),
            @ApiResponse(responseCode = "401", description = "Missing or invalid Basic Auth credentials")
    })
    @PostMapping("/customers/ingest")
    public ResponseEntity<?> ingest(
            @RequestBody @Valid List<CustomerIngestRequest> customers) {

        if (customers == null || customers.isEmpty()) {
            return ResponseEntity.badRequest().body("Batch cannot be empty");
        }

        // Validate batch size
        if (customers.size() > maxBatchSize) {
            log.warn("Batch size {} exceeds limit {}", customers.size(), maxBatchSize);
            return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE)
                    .body("Batch size " + customers.size() + " exceeds maximum " + maxBatchSize);
        }

        long startTime = System.currentTimeMillis();
        log.info("Received ingest request with {} records", customers.size());

        IngestResponse response = ingestService.ingest(customers);

        long duration = System.currentTimeMillis() - startTime;
        recordMetrics(response, customers.size(), duration);

        return ResponseEntity.ok(response);
    }

    @Operation(
            summary = "Dry-run ingestion (no writes)",
            description = "Runs the full ingestion logic — delta detection, lookup resolution, deduplication — but does NOT write anything to the database. " +
                    "Use this to safely preview how many records would be inserted, skipped, or rejected before committing a real batch. " +
                    "The response shape is identical to the real ingest endpoint."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Simulation complete — no data was written",
                    content = @Content(schema = @Schema(implementation = IngestResponse.class))),
            @ApiResponse(responseCode = "400", description = "Request body is empty or a record fails validation",
                    content = @Content(schema = @Schema(type = "string", example = "Batch cannot be empty"))),
            @ApiResponse(responseCode = "413", description = "Batch exceeds the maximum allowed size of 100,000 records",
                    content = @Content(schema = @Schema(type = "string", example = "Batch size 150000 exceeds maximum 100000"))),
            @ApiResponse(responseCode = "401", description = "Missing or invalid Basic Auth credentials")
    })
    @PostMapping("/customers/ingest/dry-run")
    public ResponseEntity<?> dryRun(
            @RequestBody @Valid List<CustomerIngestRequest> customers) {

        if (customers == null || customers.isEmpty()) {
            return ResponseEntity.badRequest().body("Batch cannot be empty");
        }

        // Validate batch size
        if (customers.size() > maxBatchSize) {
            log.warn("Dry-run batch size {} exceeds limit {}", customers.size(), maxBatchSize);
            return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE)
                    .body("Batch size " + customers.size() + " exceeds maximum " + maxBatchSize);
        }

        long startTime = System.currentTimeMillis();
        log.info("Received dry-run request with {} records", customers.size());

        IngestResponse response = ingestService.dryRun(customers);

        long duration = System.currentTimeMillis() - startTime;
        recordMetrics(response, customers.size(), duration);

        return ResponseEntity.ok(response);
    }

    /**
     * Record custom metrics for ingestion performance
     */
    private void recordMetrics(IngestResponse response, int batchSize, long duration) {
        // Counter: total records processed
        meterRegistry.counter("ingestion.records.received", "status", "all")
                .increment(response.getReceived());

        // Counter: successfully inserted
        meterRegistry.counter("ingestion.records.inserted")
                .increment(response.getInserted());

        // Counter: skipped (already existed)
        meterRegistry.counter("ingestion.records.skipped")
                .increment(response.getSkippedExisting());

        // Counter: failed records
        meterRegistry.counter("ingestion.records.failed")
                .increment(response.getFailed());

        // Timer: ingestion duration
        meterRegistry.timer("ingestion.batch.duration")
                .record(duration, java.util.concurrent.TimeUnit.MILLISECONDS);

        // Gauge: success rate percentage — update the backing reference, Gauge reads it on scrape
        double successRate = response.getReceived() > 0
                ? (100.0 * (response.getInserted() + response.getSkippedExisting())) / response.getReceived()
                : 0;
        lastSuccessRate.set(successRate);

        log.info("Batch metrics - Received: {}, Inserted: {}, Skipped: {}, Failed: {}, Duration: {}ms, SuccessRate: {}%",
                response.getReceived(), response.getInserted(), response.getSkippedExisting(),
                response.getFailed(), duration, String.format("%.2f", successRate));
    }

    @Operation(
            summary = "Force-refresh lookup caches",
            description = "Immediately reloads the in-memory country and customer_status caches from the database. " +
                    "Under normal operation this is not needed as caches auto-refresh every 5 minutes. " +
                    "Use this if lookup tables were manually updated and you need the change to take effect instantly."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Caches refreshed successfully",
                    content = @Content(schema = @Schema(type = "string", example = "Lookup caches refreshed successfully"))),
            @ApiResponse(responseCode = "401", description = "Missing or invalid Basic Auth credentials")
    })
    @PostMapping("/admin/lookup/refresh")
    public ResponseEntity<String> refreshLookups() {
        lookupCacheService.refresh();
        return ResponseEntity.ok("Lookup caches refreshed successfully");
    }
}