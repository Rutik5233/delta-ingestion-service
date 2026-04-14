package com.ingestion.controller;

import com.ingestion.dto.CustomerIngestRequest;
import com.ingestion.dto.IngestResponse;
import com.ingestion.service.CustomerIngestService;
import com.ingestion.service.LookupCacheService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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

    @PostMapping("/customers/ingest")
    public ResponseEntity<IngestResponse> ingest(
            @RequestBody @Valid List<CustomerIngestRequest> customers) {

        if (customers == null || customers.isEmpty()) {
            return ResponseEntity.badRequest().build();
        }
        log.info("Received ingest request with {} records", customers.size());
        return ResponseEntity.ok(ingestService.ingest(customers));
    }

    @PostMapping("/customers/ingest/dry-run")
    public ResponseEntity<IngestResponse> dryRun(
            @RequestBody @Valid List<CustomerIngestRequest> customers) {

        if (customers == null || customers.isEmpty()) {
            return ResponseEntity.badRequest().build();
        }
        log.info("Received dry-run request with {} records", customers.size());
        return ResponseEntity.ok(ingestService.dryRun(customers));
    }

    @PostMapping("/admin/lookup/refresh")
    public ResponseEntity<String> refreshLookups() {
        lookupCacheService.refresh();
        return ResponseEntity.ok("Lookup caches refreshed successfully");
    }
}