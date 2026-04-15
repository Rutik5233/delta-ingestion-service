package com.ingestion.service;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class LookupCacheServiceTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    // Real registry so we can assert counter values
    private final MeterRegistry meterRegistry = new SimpleMeterRegistry();

    private LookupCacheService service;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        service = new LookupCacheService(jdbcTemplate, meterRegistry);

        // @PostConstruct does not run in plain unit tests — invoke manually.
        // The JdbcTemplate mock silently does nothing, so caches load empty.
        // Metrics (Counters + Gauges) are registered against meterRegistry here.
        service.loadCaches();

        // Populate caches via the existing map references (finals — references are stable).
        Map<String, Long> countries = (Map<String, Long>) ReflectionTestUtils.getField(service, "countryCache");
        Map<String, Long> statuses  = (Map<String, Long>) ReflectionTestUtils.getField(service, "statusCache");
        countries.put("US", 1L);
        countries.put("IN", 2L);
        statuses.put("ACTIVE", 1L);
        statuses.put("INACTIVE", 2L);
    }

    // ---------------------------------------------------------------
    // resolveCountryCode
    // ---------------------------------------------------------------

    @Test
    void resolveCountryCode_knownCode_returnsIdAndIncrementsHit() {
        Optional<Long> result = service.resolveCountryCode("US");

        assertThat(result).contains(1L);
        assertThat(meterRegistry.counter("cache.country.hits").count()).isEqualTo(1.0);
        assertThat(meterRegistry.counter("cache.country.misses").count()).isEqualTo(0.0);
    }

    @Test
    void resolveCountryCode_caseInsensitive_resolvesRegardlessOfCase() {
        assertThat(service.resolveCountryCode("us")).contains(1L);
        assertThat(service.resolveCountryCode("Us")).contains(1L);
        assertThat(meterRegistry.counter("cache.country.hits").count()).isEqualTo(2.0);
    }

    @Test
    void resolveCountryCode_unknownCode_returnsEmptyAndIncrementsMiss() {
        Optional<Long> result = service.resolveCountryCode("ZZ");

        assertThat(result).isEmpty();
        assertThat(meterRegistry.counter("cache.country.hits").count()).isEqualTo(0.0);
        assertThat(meterRegistry.counter("cache.country.misses").count()).isEqualTo(1.0);
    }

    @Test
    void resolveCountryCode_null_returnsEmptyAndIncrementsMiss() {
        Optional<Long> result = service.resolveCountryCode(null);

        assertThat(result).isEmpty();
        assertThat(meterRegistry.counter("cache.country.misses").count()).isEqualTo(1.0);
    }

    // ---------------------------------------------------------------
    // resolveStatusCode
    // ---------------------------------------------------------------

    @Test
    void resolveStatusCode_knownCode_returnsIdAndIncrementsHit() {
        Optional<Long> result = service.resolveStatusCode("ACTIVE");

        assertThat(result).contains(1L);
        assertThat(meterRegistry.counter("cache.status.hits").count()).isEqualTo(1.0);
    }

    @Test
    void resolveStatusCode_unknownCode_returnsEmptyAndIncrementsMiss() {
        Optional<Long> result = service.resolveStatusCode("DELETED");

        assertThat(result).isEmpty();
        assertThat(meterRegistry.counter("cache.status.misses").count()).isEqualTo(1.0);
    }

    // ---------------------------------------------------------------
    // refresh
    // ---------------------------------------------------------------

    @Test
    @SuppressWarnings("unchecked")
    void refresh_clearsAndReloadsCache() {
        // Sanity: existing entry is resolvable before refresh
        assertThat(service.resolveCountryCode("US")).isPresent();

        // Refresh — mocked JdbcTemplate does nothing, so caches reload empty
        service.refresh();

        // After refresh the pre-loaded entry is gone
        assertThat(service.resolveCountryCode("US")).isEmpty();
    }
}
