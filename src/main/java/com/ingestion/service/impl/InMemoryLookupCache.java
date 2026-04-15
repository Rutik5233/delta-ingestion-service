package com.ingestion.service.impl;

import com.ingestion.service.LookupCacheService;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.DependsOn;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@DependsOn("flyway")
@RequiredArgsConstructor
public class InMemoryLookupCache implements LookupCacheService {

    private final JdbcTemplate jdbcTemplate;
    private final MeterRegistry meterRegistry;

    // These references are fixed for the lifetime of the bean.
    // refreshCacheMaps() updates them in-place (clear + putAll) so that
    // Gauge state objects captured at registration time remain valid.
    private final Map<String, Long> countryCache = new ConcurrentHashMap<>();
    private final Map<String, Long> statusCache  = new ConcurrentHashMap<>();

    // Counters are registered once at startup and increment monotonically.
    // Use Prometheus rate() to derive hit ratio over a time window:
    //   rate(cache_country_hits_total[5m]) /
    //   (rate(cache_country_hits_total[5m]) + rate(cache_country_misses_total[5m]))
    private Counter countryCacheHits;
    private Counter countryCacheMisses;
    private Counter statusCacheHits;
    private Counter statusCacheMisses;

    @PostConstruct
    public void loadCaches() {
        log.info("Loading lookup caches from database...");
        refreshCacheMaps();
        // Gauges are registered exactly once at startup.
        // They hold references to the fixed map/AtomicLong instances above,
        // so they stay accurate across every subsequent refresh() call.
        registerCacheMetrics();
        log.info("Lookup caches loaded: {} countries, {} statuses",
                countryCache.size(), statusCache.size());
    }

    @Scheduled(fixedRateString = "${ingestion.cache-refresh-interval-ms:300000}")
    public void scheduledRefresh() {
        log.debug("Scheduled lookup cache refresh triggered");
        refresh();
    }

    @Override
    public void refresh() {
        log.info("Refreshing lookup caches...");
        // Counters are intentionally NOT reset — they are monotonically increasing
        // by design so Prometheus rate() functions work correctly across refreshes.
        refreshCacheMaps();
        log.info("Lookup caches refreshed: {} countries, {} statuses",
                countryCache.size(), statusCache.size());
    }

    private void refreshCacheMaps() {
        Map<String, Long> newCountries = loadLookup("SELECT code, id FROM countries");
        Map<String, Long> newStatuses  = loadLookup("SELECT code, id FROM customer_status");
        // Update in-place — keeps the map references stable for already-registered Gauges
        countryCache.clear();
        countryCache.putAll(newCountries);
        statusCache.clear();
        statusCache.putAll(newStatuses);
    }

    @Override
    public Optional<Long> resolveCountryCode(String code) {
        if (code == null) {
            countryCacheMisses.increment();
            return Optional.empty();
        }
        Optional<Long> result = Optional.ofNullable(countryCache.get(code.toUpperCase()));
        if (result.isPresent()) {
            countryCacheHits.increment();
        } else {
            countryCacheMisses.increment();
        }
        return result;
    }

    @Override
    public Optional<Long> resolveStatusCode(String code) {
        if (code == null) {
            statusCacheMisses.increment();
            return Optional.empty();
        }
        Optional<Long> result = Optional.ofNullable(statusCache.get(code.toUpperCase()));
        if (result.isPresent()) {
            statusCacheHits.increment();
        } else {
            statusCacheMisses.increment();
        }
        return result;
    }

    private Map<String, Long> loadLookup(String sql) {
        Map<String, Long> map = new ConcurrentHashMap<>();
        jdbcTemplate.query(sql, rs -> {
            map.put(rs.getString("code").toUpperCase(), rs.getLong("id"));
        });
        return map;
    }

    private void registerCacheMetrics() {
        countryCacheHits   = Counter.builder("cache.country.hits")
                .description("Total country cache hits")
                .register(meterRegistry);
        countryCacheMisses = Counter.builder("cache.country.misses")
                .description("Total country cache misses")
                .register(meterRegistry);
        statusCacheHits    = Counter.builder("cache.status.hits")
                .description("Total status cache hits")
                .register(meterRegistry);
        statusCacheMisses  = Counter.builder("cache.status.misses")
                .description("Total status cache misses")
                .register(meterRegistry);

        Gauge.builder("cache.country.size", countryCache, (Map<String, Long> m) -> (double) m.size())
                .description("Number of entries in country cache")
                .register(meterRegistry);
        Gauge.builder("cache.status.size", statusCache, (Map<String, Long> m) -> (double) m.size())
                .description("Number of entries in status cache")
                .register(meterRegistry);
    }
}
