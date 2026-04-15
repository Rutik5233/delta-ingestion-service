package com.ingestion.service;

import com.ingestion.dto.CustomerIngestRequest;
import com.ingestion.dto.ResolvedCustomer;
import com.ingestion.repository.CustomerRepository;
import com.ingestion.service.CustomerIngestChunkProcessor.ChunkResult;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CustomerIngestChunkProcessorTest {

    @Mock
    private CustomerRepository customerRepository;

    @Mock
    private LookupCacheService lookupCacheService;

    private final MeterRegistry meterRegistry = new SimpleMeterRegistry();

    private CustomerIngestChunkProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new CustomerIngestChunkProcessor(customerRepository, lookupCacheService, meterRegistry);
    }

    // ---------------------------------------------------------------
    // Test 1 — Happy path: new record is resolved and inserted
    // ---------------------------------------------------------------
    @Test
    void shouldInsertNewCustomerWhenLookupSucceeds() {
        when(lookupCacheService.resolveCountryCode("US")).thenReturn(Optional.of(1L));
        when(lookupCacheService.resolveStatusCode("ACTIVE")).thenReturn(Optional.of(1L));
        when(customerRepository.findExistingExternalIds(any())).thenReturn(Set.of());
        when(customerRepository.bulkInsert(any())).thenReturn(1);

        ChunkResult result = processor.processChunk(List.of(customer("cust_01", "US", "ACTIVE")));

        assertThat(result.inserted()).isEqualTo(1);
        assertThat(result.skipped()).isEqualTo(0);
        assertThat(result.failures()).isEmpty();
        verify(customerRepository).bulkInsert(any());
    }

    // ---------------------------------------------------------------
    // Test 2 — Existing customer is skipped, not re-inserted
    // ---------------------------------------------------------------
    @Test
    void shouldSkipExistingCustomer() {
        when(lookupCacheService.resolveCountryCode("US")).thenReturn(Optional.of(1L));
        when(lookupCacheService.resolveStatusCode("ACTIVE")).thenReturn(Optional.of(1L));
        when(customerRepository.findExistingExternalIds(any())).thenReturn(Set.of("cust_01"));

        ChunkResult result = processor.processChunk(List.of(customer("cust_01", "US", "ACTIVE")));

        assertThat(result.inserted()).isEqualTo(0);
        assertThat(result.skipped()).isEqualTo(1);
        assertThat(result.failures()).isEmpty();
        verify(customerRepository, never()).bulkInsert(any());
    }

    // ---------------------------------------------------------------
    // Test 3 — Unknown country code results in a failure record
    // ---------------------------------------------------------------
    @Test
    void shouldRecordFailureForUnknownCountryCode() {
        when(lookupCacheService.resolveCountryCode("ZZ")).thenReturn(Optional.empty());
        when(lookupCacheService.resolveStatusCode("ACTIVE")).thenReturn(Optional.of(1L));

        ChunkResult result = processor.processChunk(List.of(customer("cust_02", "ZZ", "ACTIVE")));

        assertThat(result.inserted()).isEqualTo(0);
        assertThat(result.failures()).hasSize(1);
        assertThat(result.failures().get(0).getExternalId()).isEqualTo("cust_02");
        assertThat(result.failures().get(0).getReason()).contains("Unknown country_code: ZZ");
        verify(customerRepository, never()).bulkInsert(any());
    }

    // ---------------------------------------------------------------
    // Test 4 — Unknown status code results in a failure record
    // ---------------------------------------------------------------
    @Test
    void shouldRecordFailureForUnknownStatusCode() {
        when(lookupCacheService.resolveCountryCode("IN")).thenReturn(Optional.of(2L));
        when(lookupCacheService.resolveStatusCode("BOGUS")).thenReturn(Optional.empty());

        ChunkResult result = processor.processChunk(List.of(customer("cust_03", "IN", "BOGUS")));

        assertThat(result.failures()).hasSize(1);
        assertThat(result.failures().get(0).getReason()).contains("Unknown status_code: BOGUS");
        verify(customerRepository, never()).bulkInsert(any());
    }

    // ---------------------------------------------------------------
    // Test 5 — Mixed chunk: some new, some existing, some failing
    // ---------------------------------------------------------------
    @Test
    void shouldHandleMixedChunkCorrectly() {
        when(lookupCacheService.resolveCountryCode("US")).thenReturn(Optional.of(1L));
        when(lookupCacheService.resolveCountryCode("XX")).thenReturn(Optional.empty());
        when(lookupCacheService.resolveStatusCode("ACTIVE")).thenReturn(Optional.of(1L));
        // cust_existing is in DB; cust_new is not
        when(customerRepository.findExistingExternalIds(any()))
                .thenReturn(Set.of("cust_existing"));
        when(customerRepository.bulkInsert(any())).thenReturn(1);

        List<CustomerIngestRequest> chunk = List.of(
                customer("cust_new",      "US", "ACTIVE"),  // should insert
                customer("cust_existing", "US", "ACTIVE"),  // should skip
                customer("cust_badc",     "XX", "ACTIVE")   // should fail
        );

        ChunkResult result = processor.processChunk(chunk);

        assertThat(result.inserted()).isEqualTo(1);
        assertThat(result.skipped()).isEqualTo(1);
        assertThat(result.failures()).hasSize(1);
        assertThat(result.failures().get(0).getExternalId()).isEqualTo("cust_badc");

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<ResolvedCustomer>> captor = ArgumentCaptor.forClass(List.class);
        verify(customerRepository).bulkInsert(captor.capture());
        assertThat(captor.getValue()).extracting(ResolvedCustomer::getExternalId)
                .containsExactly("cust_new");
    }

    // ---------------------------------------------------------------
    // Test 6 — Empty chunk returns zero counts without hitting DB
    // ---------------------------------------------------------------
    @Test
    void shouldHandleEmptyChunkWithoutDbCalls() {
        ChunkResult result = processor.processChunk(List.of());

        assertThat(result.inserted()).isEqualTo(0);
        assertThat(result.skipped()).isEqualTo(0);
        assertThat(result.failures()).isEmpty();
        verify(customerRepository, never()).findExistingExternalIds(any());
        verify(customerRepository, never()).bulkInsert(any());
    }

    // ---------------------------------------------------------------
    // Test 7 — All records fail lookup: bulkInsert never called
    // ---------------------------------------------------------------
    @Test
    void shouldNotCallBulkInsertWhenAllRecordsFailLookup() {
        when(lookupCacheService.resolveCountryCode(any())).thenReturn(Optional.empty());
        when(lookupCacheService.resolveStatusCode(any())).thenReturn(Optional.of(1L));

        ChunkResult result = processor.processChunk(List.of(
                customer("cust_a", "BAD1", "ACTIVE"),
                customer("cust_b", "BAD2", "ACTIVE")
        ));

        assertThat(result.inserted()).isEqualTo(0);
        assertThat(result.failures()).hasSize(2);
        verify(customerRepository, never()).findExistingExternalIds(any());
        verify(customerRepository, never()).bulkInsert(any());
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------
    private CustomerIngestRequest customer(String externalId, String countryCode, String statusCode) {
        return CustomerIngestRequest.builder()
                .externalId(externalId)
                .name("Test User")
                .email(externalId + "@example.com")
                .countryCode(countryCode)
                .statusCode(statusCode)
                .build();
    }
}
