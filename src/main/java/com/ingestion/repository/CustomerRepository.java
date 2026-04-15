package com.ingestion.repository;


import com.ingestion.dto.ResolvedCustomer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
@Repository
@RequiredArgsConstructor
public class CustomerRepository {

    private final JdbcTemplate jdbcTemplate;

    public Set<String> findExistingExternalIds(Collection<String> externalIds) {
        if (externalIds.isEmpty()) return new HashSet<>();

        String[] idArray = externalIds.toArray(new String[0]);

        List<String> existing = jdbcTemplate.queryForList(
                "SELECT external_id FROM customers WHERE external_id = ANY(?)",
                String.class,
                (Object) idArray
        );

        return new HashSet<>(existing);
    }

    public int bulkInsert(List<ResolvedCustomer> customers) {
        if (customers.isEmpty()) return 0;

        String sql = """
                INSERT INTO customers (external_id, name, email, country_id, status_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (external_id) DO NOTHING
                """;

        Timestamp now = Timestamp.from(Instant.now());

        int[][] batchResult = jdbcTemplate.batchUpdate(
                sql,
                customers,
                customers.size(),
                (PreparedStatement ps, ResolvedCustomer c) -> {
                    ps.setString(1, c.getExternalId());
                    ps.setString(2, c.getName());
                    ps.setString(3, c.getEmail());
                    ps.setLong(4, c.getCountryId());
                    ps.setLong(5, c.getStatusId());
                    ps.setTimestamp(6, now);
                }
        );

        int inserted = 0;
        for (int[] batch : batchResult) {
            for (int count : batch) {
                if (count == Statement.SUCCESS_NO_INFO) {
                    // PostgreSQL driver returns -2 for successful batch rows — count as 1 insert
                    inserted++;
                } else if (count > 0) {
                    inserted += count;
                }
                // count == 0: ON CONFLICT DO NOTHING skipped this row — correct, don't count
                // count == EXECUTE_FAILED (-3): row failed — don't count
            }
        }
        return inserted;
    }
}