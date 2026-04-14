package com.ingestion.dto;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CustomerIngestRequest {

    @NotBlank(message = "external_id is required")
    private String externalId;

    @NotBlank(message = "name is required")
    private String name;

    @NotBlank(message = "email is required")
    @Email(message = "email must be a valid email address")
    private String email;

    @NotBlank(message = "country_code is required")
    private String countryCode;

    @NotBlank(message = "status_code is required")
    private String statusCode;
}