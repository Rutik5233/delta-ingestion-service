package com.ingestion.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "A single customer record to be ingested")
public class CustomerIngestRequest {

    @Schema(description = "Unique identifier for the customer in the source system", example = "cust_001", requiredMode = Schema.RequiredMode.REQUIRED)
    @NotBlank(message = "external_id is required")
    @Size(max = 255, message = "external_id must not exceed 255 characters")
    private String externalId;

    @Schema(description = "Full name of the customer", example = "Alice Smith", requiredMode = Schema.RequiredMode.REQUIRED)
    @NotBlank(message = "name is required")
    @Size(max = 255, message = "name must not exceed 255 characters")
    private String name;

    @Schema(description = "Email address of the customer", example = "alice@example.com", requiredMode = Schema.RequiredMode.REQUIRED)
    @NotBlank(message = "email is required")
    @Email(message = "email must be a valid email address")
    @Size(max = 255, message = "email must not exceed 255 characters")
    private String email;

    @Schema(description = "ISO country code — must match a value in the countries lookup table", example = "US", requiredMode = Schema.RequiredMode.REQUIRED)
    @NotBlank(message = "country_code is required")
    @Size(max = 10, message = "country_code must not exceed 10 characters")
    private String countryCode;

    @Schema(description = "Customer status code — must match a value in the customer_status lookup table (e.g. ACTIVE, INACTIVE)", example = "ACTIVE", requiredMode = Schema.RequiredMode.REQUIRED)
    @NotBlank(message = "status_code is required")
    @Size(max = 50, message = "status_code must not exceed 50 characters")
    private String statusCode;
}