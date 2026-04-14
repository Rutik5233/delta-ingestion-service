package com.ingestion.service;

import com.ingestion.dto.CustomerIngestRequest;
import com.ingestion.dto.IngestFailure;
import com.ingestion.dto.IngestResponse;
import com.ingestion.dto.ResolvedCustomer;
import com.ingestion.repository.CustomerRepository;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class CustomerIngestService {

    private final CustomerRepository customerRepository;
    private final LookupCacheService  lookupCacheService;
    private final MeterRegistry       meterRegistry;

    @Value("${ingestion.chunk-size:1000}")
    private int chunkSize;

    public IngestResponse ingest(List<CustomerIngestRequest> incoming) {
        log.info("Starting ingestion of {} records", incoming.size());

        int received      = incoming.size();
        int totalInserted = 0;
        int totalSkipped  = 0;
        List<IngestFailure> failures = new ArrayList<>();

        List<CustomerIngestRequest> deduplicated = deduplicateIncoming(incoming, failures);
        List<List<CustomerIngestRequest>> chunks  = partition(deduplicated, chunkSize);
        log.info("Processing {} chunks of up to {} records each", chunks.size(), chunkSize);

        for (int i = 0; i < chunks.size(); i++) {
            log.debug("Processing chunk {}/{}", i + 1, chunks.size());
            ChunkResult result = processChunk(chunks.get(i));
            totalInserted += result.inserted();
            totalSkipped  += result.skipped();
            failures.addAll(result.failures());
        }

        log.info("Ingestion complete — received={}, inserted={}, skipped={}, failed={}",
                received, totalInserted, totalSkipped, failures.size());

        return IngestResponse.builder()
                .received(received)
                .inserted(totalInserted)
                .skippedExisting(totalSkipped)
                .failed(failures.size())
                .failures(failures.isEmpty() ? null : failures)
                .build();
    }

    @Transactional
    protected ChunkResult processChunk(List<CustomerIngestRequest> chunk) {
        long chunkStartTime = System.currentTimeMillis();
        List<IngestFailure> failures    = new ArrayList<>();
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

        if (resolved.isEmpty()) {
            return new ChunkResult(0, 0, failures);
        }

        Set<String> incomingIds = new HashSet<>();
        resolved.forEach(c -> incomingIds.add(c.getExternalId()));

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
        log.debug("Chunk inserted={}, skipped={}, failed={}, duration={}ms", inserted, skipped, failures.size(), chunkDuration);

        return new ChunkResult(inserted, skipped, failures);
    }

    @Transactional(readOnly = true)
    public IngestResponse dryRun(List<CustomerIngestRequest> incoming) {
        log.info("[DRY RUN] Simulating ingestion of {} records", incoming.size());

        int received    = incoming.size();
        int wouldInsert = 0;
        int wouldSkip   = 0;
        List<IngestFailure> failures = new ArrayList<>();

        List<CustomerIngestRequest> deduplicated = deduplicateIncoming(incoming, failures);
        List<List<CustomerIngestRequest>> chunks  = partition(deduplicated, chunkSize);

        for (List<CustomerIngestRequest> chunk : chunks) {
            List<ResolvedCustomer> resolved = new ArrayList<>();

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
                        .name(req.getName()).email(req.getEmail())
                        .countryId(countryId.get()).statusId(statusId.get())
                        .build());
            }

            if (!resolved.isEmpty()) {
                Set<String> ids = new HashSet<>();
                resolved.forEach(c -> ids.add(c.getExternalId()));
                Set<String> existingIds = customerRepository.findExistingExternalIds(ids);
                for (ResolvedCustomer c : resolved) {
                    if (existingIds.contains(c.getExternalId())) wouldSkip++;
                    else wouldInsert++;
                }
            }
        }

        return IngestResponse.builder()
                .received(received)
                .inserted(wouldInsert)
                .skippedExisting(wouldSkip)
                .failed(failures.size())
                .failures(failures.isEmpty() ? null : failures)
                .build();
    }

    private List<CustomerIngestRequest> deduplicateIncoming(
            List<CustomerIngestRequest> records,
            List<IngestFailure> failures) {

        Set<String> seen = new LinkedHashSet<>();
        List<CustomerIngestRequest> deduped = new ArrayList<>();

        for (CustomerIngestRequest req : records) {
            if (req.getExternalId() == null) {
                failures.add(IngestFailure.builder()
                        .externalId("null")
                        .reason("external_id is null")
                        .build());
                continue;
            }
            if (!seen.add(req.getExternalId())) {
                failures.add(IngestFailure.builder()
                        .externalId(req.getExternalId())
                        .reason("Duplicate external_id within incoming batch — first occurrence kept")
                        .build());
            } else {
                deduped.add(req);
            }
        }
        return deduped;
    }

    private <T> List<List<T>> partition(List<T> list, int size) {
        List<List<T>> partitions = new ArrayList<>();
        for (int i = 0; i < list.size(); i += size) {
            partitions.add(list.subList(i, Math.min(i + size, list.size())));
        }
        return partitions;
    }

    private record ChunkResult(int inserted, int skipped, List<IngestFailure> failures) {}
}