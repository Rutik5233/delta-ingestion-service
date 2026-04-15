package com.ingestion.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema(description = "Result of an ingestion or dry-run operation")
public class IngestResponse {

    @Schema(description = "Total number of records received in the request", example = "10")
    private int received;

    @Schema(description = "Number of net-new records inserted into the database (0 for dry-run)", example = "7")
    private int inserted;

    @JsonProperty("skipped_existing")
    @Schema(description = "Number of records skipped because they already exist (matched by external_id)", example = "3")
    private int skippedExisting;

    @Schema(description = "Number of records that failed due to unknown country_code, status_code, or validation errors", example = "0")
    private int failed;

    @Schema(description = "Details of each failed record — only present when failed > 0")
    private List<IngestFailure> failures;
}