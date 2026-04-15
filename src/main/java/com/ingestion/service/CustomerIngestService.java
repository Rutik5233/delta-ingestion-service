package com.ingestion.service;

import com.ingestion.dto.CustomerIngestRequest;
import com.ingestion.dto.IngestResponse;

import java.util.List;

public interface CustomerIngestService {

    IngestResponse ingest(List<CustomerIngestRequest> incoming);

    IngestResponse dryRun(List<CustomerIngestRequest> incoming);
}