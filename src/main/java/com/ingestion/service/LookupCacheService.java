package com.ingestion.service;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
@RequiredArgsConstructor
public class LookupCacheService {

    private final JdbcTemplate jdbcTemplate;
    private final MeterRegistry meterRegistry;

    private volatile Map<String, Long> countryCache = new ConcurrentHashMap<>();
    private volatile Map<String, Long> statusCache  = new ConcurrentHashMap<>();

    // Metrics tracking
    private final AtomicLong countryCacheHits = new AtomicLong(0);
    private final AtomicLong countryCacheMisses = new AtomicLong(0);
    private final AtomicLong statusCacheHits = new AtomicLong(0);
    private final AtomicLong statusCacheMisses = new AtomicLong(0);

    @PostConstruct
    public void loadCaches() {
        log.info("Loading lookup caches from database...");
        countryCache = loadLookup("SELECT code, id FROM countries");
        statusCache  = loadLookup("SELECT code, id FROM customer_status");
        log.info("Lookup caches loaded: {} countries, {} statuses",
                countryCache.size(), statusCache.size());

        // Register cache metrics
        registerCacheMetrics();
    }

    public void refresh() {
        log.info("Refreshing lookup caches...");
        countryCacheHits.set(0);
        countryCacheMisses.set(0);
        statusCacheHits.set(0);
        statusCacheMisses.set(0);
        loadCaches();
    }

    public Optional<Long> resolveCountryCode(String code) {
        if (code == null) {
            countryCacheMisses.incrementAndGet();
            return Optional.empty();
        }

        Optional<Long> result = Optional.ofNullable(countryCache.get(code.toUpperCase()));
        if (result.isPresent()) {
            countryCacheHits.incrementAndGet();
        } else {
            countryCacheMisses.incrementAndGet();
        }
        return result;
    }

    public Optional<Long> resolveStatusCode(String code) {
        if (code == null) {
            statusCacheMisses.incrementAndGet();
            return Optional.empty();
        }

        Optional<Long> result = Optional.ofNullable(statusCache.get(code.toUpperCase()));
        if (result.isPresent()) {
            statusCacheHits.incrementAndGet();
        } else {
            statusCacheMisses.incrementAndGet();
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
        // Country cache hit/miss metrics (use Gauge.builder for correctness)
        Gauge.builder("cache.country.hits", countryCacheHits, AtomicLong::doubleValue)
                .description("Total country cache hits")
                .register(meterRegistry);

        Gauge.builder("cache.country.misses", countryCacheMisses, AtomicLong::doubleValue)
                .description("Total country cache misses")
                .register(meterRegistry);

        // Status cache hit/miss metrics
        Gauge.builder("cache.status.hits", statusCacheHits, AtomicLong::doubleValue)
                .description("Total status cache hits")
                .register(meterRegistry);

        Gauge.builder("cache.status.misses", statusCacheMisses, AtomicLong::doubleValue)
                .description("Total status cache misses")
                .register(meterRegistry);

        // Cache sizes
        Gauge.builder("cache.country.size", countryCache, (Map<String, Long> m) -> (double) m.size())
                .description("Number of entries in country cache")
                .register(meterRegistry);

        Gauge.builder("cache.status.size", statusCache, (Map<String, Long> m) -> (double) m.size())
                .description("Number of entries in status cache")
                .register(meterRegistry);
    }
}