package com.ingestion.repository;

import com.ingestion.dto.ResolvedCustomer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Statement;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CustomerRepositoryTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    private CustomerRepository repository;

    @BeforeEach
    void setUp() {
        repository = new CustomerRepository(jdbcTemplate);
    }

    // ---------------------------------------------------------------
    // findExistingExternalIds
    // ---------------------------------------------------------------

    @Test
    void findExistingExternalIds_emptyInput_returnsEmptySetWithoutDbCall() {
        Set<String> result = repository.findExistingExternalIds(List.of());

        assertThat(result).isEmpty();
        verifyNoInteractions(jdbcTemplate);
    }

    @Test
    void findExistingExternalIds_returnsOnlyMatchedIds() {
        when(jdbcTemplate.queryForList(anyString(), eq(String.class), any()))
                .thenReturn(List.of("cust_001", "cust_002"));

        Set<String> result = repository.findExistingExternalIds(
                Set.of("cust_001", "cust_002", "cust_003"));

        assertThat(result).containsExactlyInAnyOrder("cust_001", "cust_002");
    }

    // ---------------------------------------------------------------
    // bulkInsert
    // ---------------------------------------------------------------

    @Test
    void bulkInsert_emptyList_returnsZeroWithoutDbCall() {
        int result = repository.bulkInsert(List.of());

        assertThat(result).isEqualTo(0);
        verifyNoInteractions(jdbcTemplate);
    }

    @Test
    void bulkInsert_successNoInfo_countsEachRowAsOne() {
        // PostgreSQL JDBC driver returns SUCCESS_NO_INFO (-2) for each row in a batch
        when(jdbcTemplate.batchUpdate(anyString(), anyList(), anyInt(), any()))
                .thenReturn(new int[][]{{Statement.SUCCESS_NO_INFO, Statement.SUCCESS_NO_INFO}});

        int result = repository.bulkInsert(List.of(resolved("a"), resolved("b")));

        assertThat(result).isEqualTo(2);
    }

    @Test
    void bulkInsert_explicitRowCount_sumsCounts() {
        when(jdbcTemplate.batchUpdate(anyString(), anyList(), anyInt(), any()))
                .thenReturn(new int[][]{{1, 1, 1}});

        int result = repository.bulkInsert(List.of(resolved("x"), resolved("y"), resolved("z")));

        assertThat(result).isEqualTo(3);
    }

    @Test
    void bulkInsert_onConflictRow_notCounted() {
        // count=0 means ON CONFLICT DO NOTHING was triggered — correct to not count
        when(jdbcTemplate.batchUpdate(anyString(), anyList(), anyInt(), any()))
                .thenReturn(new int[][]{{1, 0, 1}});

        int result = repository.bulkInsert(List.of(resolved("a"), resolved("b"), resolved("c")));

        assertThat(result).isEqualTo(2);
    }

    @Test
    void bulkInsert_executeFailed_notCounted() {
        // count=-3 (EXECUTE_FAILED) means a row failed in the batch — should not be counted
        when(jdbcTemplate.batchUpdate(anyString(), anyList(), anyInt(), any()))
                .thenReturn(new int[][]{{Statement.EXECUTE_FAILED, 1}});

        int result = repository.bulkInsert(List.of(resolved("a"), resolved("b")));

        assertThat(result).isEqualTo(1);
    }

    // ---------------------------------------------------------------
    // Helper
    // ---------------------------------------------------------------

    private ResolvedCustomer resolved(String externalId) {
        return ResolvedCustomer.builder()
                .externalId(externalId)
                .name("Test User")
                .email(externalId + "@example.com")
                .countryId(1L)
                .statusId(1L)
                .build();
    }
}
