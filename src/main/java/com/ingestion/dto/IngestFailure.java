package com.ingestion.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Details of a single record that failed during ingestion")
public class IngestFailure {

    @Schema(description = "The external_id of the record that failed", example = "cust_042")
    private String externalId;

    @Schema(description = "Human-readable reason for the failure", example = "Unknown country_code: XX")
    private String reason;
}