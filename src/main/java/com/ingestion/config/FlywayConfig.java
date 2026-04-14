package com.ingestion.config;

import org.flywaydb.core.Flyway;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import javax.sql.DataSource;

@Configuration
public class FlywayConfig {

    @Bean
    @DependsOn("dataSource")
    public Flyway flyway(DataSource dataSource) {
        Flyway flyway = Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .baselineOnMigrate(false)
                .cleanDisabled(true)
                .validateOnMigrate(true)
                .outOfOrder(false)
                .load();

        flyway.migrate();
        return flyway;
    }
}

