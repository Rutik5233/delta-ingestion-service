package com.ingestion.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ResolvedCustomer {
    private String externalId;
    private String name;
    private String email;
    private Long countryId;
    private Long statusId;
}
