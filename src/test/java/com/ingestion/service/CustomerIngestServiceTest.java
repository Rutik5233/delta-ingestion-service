package com.ingestion.service;

import com.ingestion.dto.CustomerIngestRequest;
import com.ingestion.dto.IngestResponse;
import com.ingestion.dto.ResolvedCustomer;
import com.ingestion.repository.CustomerRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CustomerIngestServiceTest {

    @Mock
    private CustomerRepository customerRepository;

    @Mock
    private LookupCacheService lookupCacheService;

    @InjectMocks
    private CustomerIngestService ingestService;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(ingestService, "chunkSize", 1000);
    }

    // ---------------------------------------------------------------
    // Test 1 — Only new records should be inserted
    // ---------------------------------------------------------------
    @Test
    void shouldInsertOnlyNewRecords() {
        // GIVEN
        List<CustomerIngestRequest> incoming = List.of(
                customer("cust_001", "US", "ACTIVE"),  // already exists
                customer("cust_002", "IN", "ACTIVE")   // new
        );

        when(lookupCacheService.resolveCountryCode("US")).thenReturn(Optional.of(1L));
        when(lookupCacheService.resolveCountryCode("IN")).thenReturn(Optional.of(2L));
        when(lookupCacheService.resolveStatusCode("ACTIVE")).thenReturn(Optional.of(1L));
        when(customerRepository.findExistingExternalIds(any()))
                .thenReturn(Set.of("cust_001"));
        when(customerRepository.bulkInsert(any())).thenReturn(1);

        // WHEN
        IngestResponse response = ingestService.ingest(incoming);

        // THEN
        assertThat(response.getReceived()).isEqualTo(2);
        assertThat(response.getInserted()).isEqualTo(1);
        assertThat(response.getSkippedExisting()).isEqualTo(1);
        assertThat(response.getFailed()).isEqualTo(0);

        // Verify only cust_002 was passed to bulkInsert
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<ResolvedCustomer>> captor =
                ArgumentCaptor.forClass(List.class);
        verify(customerRepository).bulkInsert(captor.capture());
        assertThat(captor.getValue()).hasSize(1);
        assertThat(captor.getValue().get(0).getExternalId()).isEqualTo("cust_002");
    }

    // ---------------------------------------------------------------
    // Test 2 — Unknown country code should be marked as failed
    // ---------------------------------------------------------------
    @Test
    void shouldMarkFailureForUnknownCountryCode() {
        // GIVEN
        when(lookupCacheService.resolveCountryCode("ZZ")).thenReturn(Optional.empty());
        when(lookupCacheService.resolveStatusCode("ACTIVE")).thenReturn(Optional.of(1L));

        // WHEN
        IngestResponse response = ingestService.ingest(
                List.of(customer("cust_003", "ZZ", "ACTIVE"))
        );

        // THEN
        assertThat(response.getReceived()).isEqualTo(1);
        assertThat(response.getInserted()).isEqualTo(0);
        assertThat(response.getFailed()).isEqualTo(1);
        assertThat(response.getFailures().get(0).getReason())
                .contains("Unknown country_code: ZZ");

        // Nothing should be inserted
        verify(customerRepository, never()).bulkInsert(any());
    }

    // ---------------------------------------------------------------
    // Test 3 — Unknown status code should be marked as failed
    // ---------------------------------------------------------------
    @Test
    void shouldMarkFailureForUnknownStatusCode() {
        // GIVEN
        when(lookupCacheService.resolveCountryCode("US")).thenReturn(Optional.of(1L));
        when(lookupCacheService.resolveStatusCode("INVALID")).thenReturn(Optional.empty());

        // WHEN
        IngestResponse response = ingestService.ingest(
                List.of(customer("cust_004", "US", "INVALID"))
        );

        // THEN
        assertThat(response.getFailed()).isEqualTo(1);
        assertThat(response.getFailures().get(0).getReason())
                .contains("Unknown status_code: INVALID");
        verify(customerRepository, never()).bulkInsert(any());
    }

    // ---------------------------------------------------------------
    // Test 4 — Duplicate external_id in same batch
    // ---------------------------------------------------------------
    @Test
    void shouldDeduplicateWithinIncomingBatch() {
        // GIVEN — same external_id appears twice in payload
        when(lookupCacheService.resolveCountryCode("US")).thenReturn(Optional.of(1L));
        when(lookupCacheService.resolveStatusCode("ACTIVE")).thenReturn(Optional.of(1L));
        when(customerRepository.findExistingExternalIds(any())).thenReturn(Set.of());
        when(customerRepository.bulkInsert(any())).thenReturn(1);

        // WHEN
        IngestResponse response = ingestService.ingest(List.of(
                customer("cust_dup", "US", "ACTIVE"),
                customer("cust_dup", "US", "ACTIVE")  // duplicate
        ));

        // THEN — second occurrence reported as failed, first is inserted
        assertThat(response.getReceived()).isEqualTo(2);
        assertThat(response.getInserted()).isEqualTo(1);
        assertThat(response.getFailed()).isEqualTo(1);
        assertThat(response.getFailures().get(0).getReason())
                .contains("Duplicate external_id");
    }

    // ---------------------------------------------------------------
    // Test 5 — Idempotency: running same input twice inserts nothing
    // ---------------------------------------------------------------
    @Test
    void shouldBeIdempotentWhenAllRecordsAlreadyExist() {
        // GIVEN — all records already exist in DB
        when(lookupCacheService.resolveCountryCode("US")).thenReturn(Optional.of(1L));
        when(lookupCacheService.resolveCountryCode("IN")).thenReturn(Optional.of(2L));
        when(lookupCacheService.resolveStatusCode("ACTIVE")).thenReturn(Optional.of(1L));
        when(customerRepository.findExistingExternalIds(any()))
                .thenReturn(Set.of("cust_001", "cust_002"));

        // WHEN
        IngestResponse response = ingestService.ingest(List.of(
                customer("cust_001", "US", "ACTIVE"),
                customer("cust_002", "IN", "ACTIVE")
        ));

        // THEN — nothing inserted, nothing failed
        assertThat(response.getInserted()).isEqualTo(0);
        assertThat(response.getSkippedExisting()).isEqualTo(2);
        assertThat(response.getFailed()).isEqualTo(0);

        // bulkInsert should never be called
        verify(customerRepository, never()).bulkInsert(any());
    }

    // ---------------------------------------------------------------
    // Test 6 — Empty incoming list
    // ---------------------------------------------------------------
    @Test
    void shouldHandleEmptyIncomingList() {
        // WHEN
        IngestResponse response = ingestService.ingest(List.of());

        // THEN
        assertThat(response.getReceived()).isEqualTo(0);
        assertThat(response.getInserted()).isEqualTo(0);
        assertThat(response.getFailed()).isEqualTo(0);

        // No DB calls at all
        verify(customerRepository, never()).findExistingExternalIds(any());
        verify(customerRepository, never()).bulkInsert(any());
    }

    // ---------------------------------------------------------------
    // Test 7 — All records fail lookup — bulkInsert never called
    // ---------------------------------------------------------------
    @Test
    void shouldNotCallBulkInsertWhenAllRecordsFailLookup() {
        // GIVEN — both records have invalid codes
        when(lookupCacheService.resolveCountryCode(any())).thenReturn(Optional.empty());
        when(lookupCacheService.resolveStatusCode(any())).thenReturn(Optional.of(1L));

        // WHEN
        IngestResponse response = ingestService.ingest(List.of(
                customer("cust_005", "XX", "ACTIVE"),
                customer("cust_006", "YY", "ACTIVE")
        ));

        // THEN
        assertThat(response.getInserted()).isEqualTo(0);
        assertThat(response.getFailed()).isEqualTo(2);
        verify(customerRepository, never()).bulkInsert(any());
    }

    // ---------------------------------------------------------------
    // Helper
    // ---------------------------------------------------------------
    private CustomerIngestRequest customer(
            String externalId, String countryCode, String statusCode) {
        return CustomerIngestRequest.builder()
                .externalId(externalId)
                .name("Test User")
                .email(externalId + "@example.com")
                .countryCode(countryCode)
                .statusCode(statusCode)
                .build();
    }
}