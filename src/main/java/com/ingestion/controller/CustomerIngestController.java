package com.ingestion.controller;

import com.ingestion.dto.CustomerIngestRequest;
import com.ingestion.dto.IngestResponse;
import com.ingestion.service.CustomerIngestService;
import com.ingestion.service.LookupCacheService;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Slf4j
@Validated
@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class CustomerIngestController {

    private final CustomerIngestService ingestService;
    private final LookupCacheService    lookupCacheService;
    private final MeterRegistry         meterRegistry;

    @Value("${ingestion.max-batch-size:100000}")
    private int maxBatchSize;

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

        // Gauge: success rate percentage
        double successRate = response.getReceived() > 0
                ? (100.0 * (response.getInserted() + response.getSkippedExisting())) / response.getReceived()
                : 0;
        meterRegistry.gauge("ingestion.success.rate.percent", successRate);

        log.info("Batch metrics - Received: {}, Inserted: {}, Skipped: {}, Failed: {}, Duration: {}ms, SuccessRate: {:.2f}%",
                response.getReceived(), response.getInserted(), response.getSkippedExisting(),
                response.getFailed(), duration, successRate);
    }

    @PostMapping("/admin/lookup/refresh")
    public ResponseEntity<String> refreshLookups() {
        lookupCacheService.refresh();
        return ResponseEntity.ok("Lookup caches refreshed successfully");
    }
}