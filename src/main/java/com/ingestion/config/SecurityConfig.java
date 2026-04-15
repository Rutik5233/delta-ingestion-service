package com.ingestion.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

import java.util.List;

@Configuration
@EnableWebSecurity
public class SecurityConfig {

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
                // Disable CSRF — not needed for stateless REST APIs
                .csrf(csrf -> csrf.disable())

                // Enable CORS using the bean below
                .cors(cors -> cors.configurationSource(corsConfigurationSource()))

                // Authorization rules
                .authorizeHttpRequests(auth -> auth
                        // Allow health endpoint without authentication (for load balancers / Fly.io health checks)
                        .requestMatchers("/actuator/health/**").permitAll()
                        // Allow metrics and prometheus without authentication (for monitoring tools)
                        // These rules are active when actuator runs on the same port as the app (e.g. Fly.io deployment
                        // sets MANAGEMENT_SERVER_PORT=8080). Locally actuator is on 8081 and outside this filter chain.
                        .requestMatchers("/actuator/metrics/**").permitAll()
                        .requestMatchers("/actuator/prometheus").permitAll()
                        // Allow Swagger UI and OpenAPI spec without authentication
                        .requestMatchers("/swagger-ui.html", "/swagger-ui/**", "/v3/api-docs/**").permitAll()
                        // All other requests must be authenticated
                        .anyRequest().authenticated()
                )

                // Use Basic Auth
                .httpBasic(httpBasic -> {})

                // Stateless — no sessions, every request must send credentials
                // This is correct for a REST API
                .sessionManagement(session -> session
                        .sessionCreationPolicy(SessionCreationPolicy.STATELESS)
                );

        return http.build();
    }

    @Bean
    public CorsConfigurationSource corsConfigurationSource() {
        CorsConfiguration config = new CorsConfiguration();
        // Allow Swagger UI (same Railway domain) and local dev
        config.setAllowedOriginPatterns(List.of(
                "https://*.railway.app",
                "http://localhost:*"
        ));
        config.setAllowedMethods(List.of("GET", "POST", "PUT", "DELETE", "OPTIONS"));
        config.setAllowedHeaders(List.of("*"));
        // Required so the browser sends the Authorization header cross-origin
        config.setAllowCredentials(true);

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", config);
        return source;
    }
}