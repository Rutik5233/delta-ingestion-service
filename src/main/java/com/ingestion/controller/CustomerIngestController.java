package com.ingestion.controller;

import com.ingestion.dto.CustomerIngestRequest;
import com.ingestion.dto.IngestResponse;
import com.ingestion.service.CustomerIngestService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import jakarta.validation.Valid;
import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/customers")
@RequiredArgsConstructor
public class CustomerIngestController {

    private final CustomerIngestService customerIngestService;

    @PostMapping("/ingest")
    public ResponseEntity<IngestResponse> ingestCustomers(
            @Valid @RequestBody List<CustomerIngestRequest> requests) {

        log.info("Received ingestion request with {} customers", requests.size());
        IngestResponse response = customerIngestService.ingest(requests);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/dry-run")
    public ResponseEntity<IngestResponse> dryRunCustomers(
            @Valid @RequestBody List<CustomerIngestRequest> requests) {

        log.info("Received dry-run request with {} customers", requests.size());
        IngestResponse response = customerIngestService.dryRun(requests);
        return ResponseEntity.ok(response);
    }
}
