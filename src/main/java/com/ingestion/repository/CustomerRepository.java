package com.ingestion.repository;

import com.ingestion.dto.ResolvedCustomer;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public interface CustomerRepository {

    Set<String> findExistingExternalIds(Collection<String> externalIds);

    int bulkInsert(List<ResolvedCustomer> customers);
}