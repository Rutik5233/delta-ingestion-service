package com.ingestion.service.impl;

import com.ingestion.repository.CustomerRepository;
import com.ingestion.service.LookupCacheService;

import com.ingestion.dto.CustomerIngestRequest;
import com.ingestion.dto.IngestFailure;
import com.ingestion.dto.ResolvedCustomer;
import com.ingestion.repository.CustomerRepository;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class CustomerIngestChunkProcessor {

    private final CustomerRepository customerRepository;
    private final LookupCacheService  lookupCacheService;
    private final MeterRegistry       meterRegistry;

    /**
     * Processes a single chunk within a real database transaction.
     * Must live in its own Spring bean so that @Transactional is applied
     * via the AOP proxy — calling it from within the same class would bypass
     * the proxy and silently skip the transaction boundary.
     */
    @Transactional
    public ChunkResult processChunk(List<CustomerIngestRequest> chunk) {
        long chunkStartTime = System.currentTimeMillis();

        ResolutionResult resolution = resolveChunk(chunk);
        List<IngestFailure>    failures = resolution.failures();
        List<ResolvedCustomer> resolved = resolution.resolved();

        if (resolved.isEmpty()) {
            return new ChunkResult(0, 0, failures);
        }

        Set<String> incomingIds = resolved.stream()
                .map(ResolvedCustomer::getExternalId)
                .collect(Collectors.toSet());
        Set<String> existingIds = customerRepository.findExistingExternalIds(incomingIds);
        log.debug("Chunk: {} incoming, {} already exist", incomingIds.size(), existingIds.size());

        List<ResolvedCustomer> newCustomers = new ArrayList<>();
        int skipped = 0;
        for (ResolvedCustomer c : resolved) {
            if (existingIds.contains(c.getExternalId())) skipped++;
            else newCustomers.add(c);
        }

        if (newCustomers.isEmpty()) {
            log.debug("Chunk: no new customers to insert, skipping bulkInsert");
            return new ChunkResult(0, skipped, failures);
        }

        int inserted = customerRepository.bulkInsert(newCustomers);
        long chunkDuration = System.currentTimeMillis() - chunkStartTime;
        meterRegistry.timer("ingestion.chunk.duration")
                .record(chunkDuration, java.util.concurrent.TimeUnit.MILLISECONDS);
        meterRegistry.counter("ingestion.chunk.records.processed", "status", "inserted")
                .increment(inserted);
        meterRegistry.counter("ingestion.chunk.records.processed", "status", "skipped")
                .increment(skipped);
        log.debug("Chunk inserted={}, skipped={}, failed={}, duration={}ms",
                inserted, skipped, failures.size(), chunkDuration);

        return new ChunkResult(inserted, skipped, failures);
    }

    /**
     * Simulates chunk processing without writing to the database.
     * Performs full lookup resolution and delta detection, but skips bulkInsert.
     */
    @Transactional(readOnly = true)
    public ChunkResult dryRunChunk(List<CustomerIngestRequest> chunk) {
        ResolutionResult resolution = resolveChunk(chunk);
        List<IngestFailure>    failures = resolution.failures();
        List<ResolvedCustomer> resolved = resolution.resolved();

        if (resolved.isEmpty()) {
            return new ChunkResult(0, 0, failures);
        }

        Set<String> incomingIds = resolved.stream()
                .map(ResolvedCustomer::getExternalId)
                .collect(Collectors.toSet());
        Set<String> existingIds = customerRepository.findExistingExternalIds(incomingIds);

        int wouldInsert = 0, wouldSkip = 0;
        for (ResolvedCustomer c : resolved) {
            if (existingIds.contains(c.getExternalId())) wouldSkip++;
            else wouldInsert++;
        }

        log.debug("[DRY RUN] Chunk: wouldInsert={}, wouldSkip={}, failed={}",
                wouldInsert, wouldSkip, failures.size());
        return new ChunkResult(wouldInsert, wouldSkip, failures);
    }

    /**
     * Resolves lookup codes for all records in the chunk.
     * Records with unknown country or status codes are added to failures.
     */
    private ResolutionResult resolveChunk(List<CustomerIngestRequest> chunk) {
        List<IngestFailure>    failures = new ArrayList<>();
        List<ResolvedCustomer> resolved = new ArrayList<>(chunk.size());

        for (CustomerIngestRequest req : chunk) {
            Optional<Long> countryId = lookupCacheService.resolveCountryCode(req.getCountryCode());
            Optional<Long> statusId  = lookupCacheService.resolveStatusCode(req.getStatusCode());

            if (countryId.isEmpty()) {
                failures.add(IngestFailure.builder()
                        .externalId(req.getExternalId())
                        .reason("Unknown country_code: " + req.getCountryCode())
                        .build());
                continue;
            }
            if (statusId.isEmpty()) {
                failures.add(IngestFailure.builder()
                        .externalId(req.getExternalId())
                        .reason("Unknown status_code: " + req.getStatusCode())
                        .build());
                continue;
            }
            resolved.add(ResolvedCustomer.builder()
                    .externalId(req.getExternalId())
                    .name(req.getName())
                    .email(req.getEmail())
                    .countryId(countryId.get())
                    .statusId(statusId.get())
                    .build());
        }
        return new ResolutionResult(resolved, failures);
    }

    public record ChunkResult(int inserted, int skipped, List<IngestFailure> failures) {}

    private record ResolutionResult(List<ResolvedCustomer> resolved, List<IngestFailure> failures) {}
}
