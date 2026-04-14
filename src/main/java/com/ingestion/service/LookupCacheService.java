package com.ingestion.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class LookupCacheService {

    private final JdbcTemplate jdbcTemplate;

    private final Map<String, Long> countryCodeToId = new ConcurrentHashMap<>();
    private final Map<String, Long> statusCodeToId  = new ConcurrentHashMap<>();

    @PostConstruct
    public void loadCache() {
        log.info("Loading lookup caches...");

        // Load countries
        jdbcTemplate.query("SELECT id, code FROM countries", (rs, rowNum) -> {
            countryCodeToId.put(rs.getString("code"), rs.getLong("id"));
            return null;
        });

        // Load customer statuses
        jdbcTemplate.query("SELECT id, code FROM customer_status", (rs, rowNum) -> {
            statusCodeToId.put(rs.getString("code"), rs.getLong("id"));
            return null;
        });

        log.info("Loaded {} countries and {} customer statuses into cache",
                countryCodeToId.size(), statusCodeToId.size());
    }

    public Optional<Long> resolveCountryCode(String code) {
        if (code == null) return Optional.empty();
        return Optional.ofNullable(countryCodeToId.get(code.toUpperCase()));
    }

    public Optional<Long> resolveStatusCode(String code) {
        if (code == null) return Optional.empty();
        return Optional.ofNullable(statusCodeToId.get(code.toUpperCase()));
    }

    public void refreshCache() {
        log.info("Refreshing lookup caches...");
        countryCodeToId.clear();
        statusCodeToId.clear();
        loadCache();
    }
}
