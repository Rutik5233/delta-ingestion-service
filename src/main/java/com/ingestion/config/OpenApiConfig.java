package com.ingestion.config;

import io.swagger.v3.oas.annotations.enums.SecuritySchemeType;
import io.swagger.v3.oas.annotations.security.SecurityScheme;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
@SecurityScheme(
        name = "basicAuth",
        type = SecuritySchemeType.HTTP,
        scheme = "basic",
        description = "Enter your username and password. Credentials are configured via INGESTION_API_USERNAME and INGESTION_API_PASSWORD environment variables."
)
public class OpenApiConfig {

    @Value("${app.server-url:http://localhost:8080")
    private String serverUrl;

    @Bean
    public OpenAPI openAPI() {
        return new OpenAPI()
                .servers(List.of(new Server().url(serverUrl)))
                .info(new Info()
                        .title("Delta Ingestion Service API")
                        .version("1.0")
                        .description("Accepts batches of customer records, computes the delta against existing data, and inserts only net-new customers."))
                .addSecurityItem(new SecurityRequirement().addList("basicAuth"));
    }
}
