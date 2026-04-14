package com.ingestion.service;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class LookupCacheService {

    private final JdbcTemplate jdbcTemplate;

    private volatile Map<String, Long> countryCache = new ConcurrentHashMap<>();
    private volatile Map<String, Long> statusCache  = new ConcurrentHashMap<>();

    @PostConstruct
    public void loadCaches() {
        log.info("Loading lookup caches from database...");
        countryCache = loadLookup("SELECT code, id FROM countries");
        statusCache  = loadLookup("SELECT code, id FROM customer_status");
        log.info("Lookup caches loaded: {} countries, {} statuses",
                countryCache.size(), statusCache.size());
    }

    public void refresh() {
        log.info("Refreshing lookup caches...");
        loadCaches();
    }

    public Optional<Long> resolveCountryCode(String code) {
        if (code == null) return Optional.empty();
        return Optional.ofNullable(countryCache.get(code.toUpperCase()));
    }

    public Optional<Long> resolveStatusCode(String code) {
        if (code == null) return Optional.empty();
        return Optional.ofNullable(statusCache.get(code.toUpperCase()));
    }

    private Map<String, Long> loadLookup(String sql) {
        Map<String, Long> map = new ConcurrentHashMap<>();
        jdbcTemplate.query(sql, rs -> {
            map.put(rs.getString("code").toUpperCase(), rs.getLong("id"));
        });
        return map;
    }
}