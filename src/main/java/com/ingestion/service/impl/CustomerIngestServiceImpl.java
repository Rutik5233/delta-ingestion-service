package com.ingestion.service.impl;

import com.ingestion.service.CustomerIngestService;

import com.ingestion.dto.CustomerIngestRequest;
import com.ingestion.dto.IngestFailure;
import com.ingestion.dto.IngestResponse;
import com.ingestion.service.impl.CustomerIngestChunkProcessor.ChunkResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class CustomerIngestServiceImpl implements CustomerIngestService {

    private final CustomerIngestChunkProcessor chunkProcessor;

    @Value("${ingestion.chunk-size:1000}")
    private int chunkSize;

    @Override
    public IngestResponse ingest(List<CustomerIngestRequest> incoming) {
        log.info("Starting ingestion of {} records", incoming.size());

        int received      = incoming.size();
        int totalInserted = 0;
        int totalSkipped  = 0;
        List<IngestFailure> failures = new ArrayList<>();

        DeduplicationResult dedup = deduplicateIncoming(incoming);
        failures.addAll(dedup.lookupFailures());
        List<List<CustomerIngestRequest>> chunks = partition(dedup.records(), chunkSize);
        log.info("Processing {} chunks of up to {} records each (dedup failures: {})",
                chunks.size(), chunkSize, dedup.lookupFailures().size());

        for (int i = 0; i < chunks.size(); i++) {
            log.debug("Processing chunk {}/{}", i + 1, chunks.size());
            ChunkResult result = chunkProcessor.processChunk(chunks.get(i));
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

    @Override
    public IngestResponse dryRun(List<CustomerIngestRequest> incoming) {
        log.info("[DRY RUN] Simulating ingestion of {} records", incoming.size());

        int received    = incoming.size();
        int wouldInsert = 0;
        int wouldSkip   = 0;
        List<IngestFailure> failures = new ArrayList<>();

        DeduplicationResult dedup = deduplicateIncoming(incoming);
        failures.addAll(dedup.lookupFailures());
        List<List<CustomerIngestRequest>> chunks = partition(dedup.records(), chunkSize);

        for (List<CustomerIngestRequest> chunk : chunks) {
            ChunkResult result = chunkProcessor.dryRunChunk(chunk);
            wouldInsert += result.inserted();
            wouldSkip   += result.skipped();
            failures.addAll(result.failures());
        }

        log.info("[DRY RUN] Complete — received={}, wouldInsert={}, wouldSkip={}, failed={}",
                received, wouldInsert, wouldSkip, failures.size());

        return IngestResponse.builder()
                .received(received)
                .inserted(wouldInsert)
                .skippedExisting(wouldSkip)
                .failed(failures.size())
                .failures(failures.isEmpty() ? null : failures)
                .build();
    }

    private DeduplicationResult deduplicateIncoming(List<CustomerIngestRequest> records) {
        Set<String> seen = new LinkedHashSet<>();
        List<CustomerIngestRequest> deduped = new ArrayList<>();
        List<IngestFailure> nullIdFailures = new ArrayList<>();

        for (CustomerIngestRequest req : records) {
            if (req.getExternalId() == null) {
                nullIdFailures.add(IngestFailure.builder()
                        .externalId("null")
                        .reason("external_id is null")
                        .build());
                continue;
            }
            if (!seen.add(req.getExternalId())) {
                nullIdFailures.add(IngestFailure.builder()
                        .externalId(req.getExternalId())
                        .reason("Duplicate external_id within incoming batch — first occurrence kept")
                        .build());
            } else {
                deduped.add(req);
            }
        }
        return new DeduplicationResult(deduped, nullIdFailures);
    }

    private record DeduplicationResult(
            List<CustomerIngestRequest> records,
            List<IngestFailure> lookupFailures) {}

    private <T> List<List<T>> partition(List<T> list, int size) {
        List<List<T>> partitions = new ArrayList<>();
        for (int i = 0; i < list.size(); i += size) {
            partitions.add(list.subList(i, Math.min(i + size, list.size())));
        }
        return partitions;
    }
}
