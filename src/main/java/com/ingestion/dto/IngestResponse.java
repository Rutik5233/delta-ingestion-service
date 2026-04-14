package com.ingestion.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class IngestResponse {
    private int received;
    private int inserted;
    private int skippedExisting;
    private int failed;

    // Only included in response when there are failures
    // Keeps successful responses clean
    private List<IngestFailure> failures;
}