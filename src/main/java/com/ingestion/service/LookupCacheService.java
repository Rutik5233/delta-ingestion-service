package com.ingestion.service;

import java.util.Optional;

public interface LookupCacheService {

    Optional<Long> resolveCountryCode(String code);

    Optional<Long> resolveStatusCode(String code);

    void refresh();
}