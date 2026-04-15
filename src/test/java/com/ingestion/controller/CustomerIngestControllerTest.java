package com.ingestion.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ingestion.dto.CustomerIngestRequest;
import com.ingestion.dto.IngestResponse;
import com.ingestion.exception.GlobalExceptionHandler;
import com.ingestion.service.CustomerIngestService;
import com.ingestion.service.LookupCacheService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@ExtendWith(MockitoExtension.class)
class CustomerIngestControllerTest {

    @Mock
    private CustomerIngestService ingestService;

    @Mock
    private LookupCacheService lookupCacheService;

    private final MeterRegistry meterRegistry = new SimpleMeterRegistry();
    private final ObjectMapper  objectMapper  = new ObjectMapper();

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        CustomerIngestController controller =
                new CustomerIngestController(ingestService, lookupCacheService, meterRegistry);

        // max-batch-size=2 so the 413 test can send 3 items without building a 100k list
        ReflectionTestUtils.setField(controller, "maxBatchSize", 2);

        // @PostConstruct does not run outside of a Spring context — invoke manually
        controller.registerMetrics();

        // Configure Bean Validation so @Valid @RequestBody constraints are enforced
        LocalValidatorFactoryBean validator = new LocalValidatorFactoryBean();
        validator.afterPropertiesSet();

        mockMvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new GlobalExceptionHandler())
                .setValidator(validator)
                .build();
    }

    // ---------------------------------------------------------------
    // POST /api/v1/customers/ingest
    // ---------------------------------------------------------------

    @Test
    void ingest_validRequest_returns200() throws Exception {
        when(ingestService.ingest(any())).thenReturn(
                IngestResponse.builder().received(1).inserted(1).skippedExisting(0).failed(0).build());

        mockMvc.perform(post("/api/v1/customers/ingest")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(List.of(validRequest("cust_001")))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.received").value(1))
                .andExpect(jsonPath("$.inserted").value(1));

        verify(ingestService).ingest(any());
    }

    @Test
    void ingest_emptyList_returns400() throws Exception {
        mockMvc.perform(post("/api/v1/customers/ingest")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("[]"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void ingest_oversizedBatch_returns413() throws Exception {
        // maxBatchSize=2 (set in setUp); 3 items must trigger 413
        mockMvc.perform(post("/api/v1/customers/ingest")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(
                                List.of(validRequest("c1"), validRequest("c2"), validRequest("c3")))))
                .andExpect(status().isPayloadTooLarge());
    }

    @Test
    void ingest_malformedJson_returns400() throws Exception {
        // GlobalExceptionHandler.handleMalformedJson() catches HttpMessageNotReadableException → 400
        mockMvc.perform(post("/api/v1/customers/ingest")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{not: valid json"))
                .andExpect(status().isBadRequest());
    }

    // ---------------------------------------------------------------
    // POST /api/v1/customers/ingest/dry-run
    // ---------------------------------------------------------------

    @Test
    void dryRun_validRequest_returns200() throws Exception {
        when(ingestService.dryRun(any())).thenReturn(
                IngestResponse.builder().received(1).inserted(1).skippedExisting(0).failed(0).build());

        mockMvc.perform(post("/api/v1/customers/ingest/dry-run")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(List.of(validRequest("cust_002")))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.inserted").value(1));

        verify(ingestService).dryRun(any());
    }

    // ---------------------------------------------------------------
    // POST /api/v1/admin/lookup/refresh
    // ---------------------------------------------------------------

    @Test
    void refreshLookups_returns200() throws Exception {
        mockMvc.perform(post("/api/v1/admin/lookup/refresh"))
                .andExpect(status().isOk())
                .andExpect(content().string("Lookup caches refreshed successfully"));

        verify(lookupCacheService).refresh();
    }

    // ---------------------------------------------------------------
    // Helper
    // ---------------------------------------------------------------

    private CustomerIngestRequest validRequest(String externalId) {
        return CustomerIngestRequest.builder()
                .externalId(externalId)
                .name("Test User")
                .email(externalId + "@example.com")
                .countryCode("US")
                .statusCode("ACTIVE")
                .build();
    }
}

