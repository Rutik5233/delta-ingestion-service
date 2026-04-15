package com.ingestion.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;

/**
 * Servlet filter that assigns a unique traceId to every request and stores it
 * in the SLF4J MDC so it appears in every log line produced during request
 * processing.
 *
 * <p>Precedence: if the caller supplies an {@code X-Request-Id} header that
 * value is reused (useful when a gateway already stamps requests); otherwise a
 * random UUID is generated.  The resolved traceId is also echoed back in the
 * response via the {@code X-Trace-Id} header so callers can correlate their
 * own logs without needing distributed tracing infrastructure.
 */
@Component
public class MdcLoggingFilter extends OncePerRequestFilter {

    private static final String REQUEST_ID_HEADER  = "X-Request-Id";
    private static final String TRACE_ID_HEADER    = "X-Trace-Id";
    private static final String MDC_TRACE_ID_KEY   = "traceId";

    @Override
    protected void doFilterInternal(
            @NonNull HttpServletRequest  request,
            @NonNull HttpServletResponse response,
            @NonNull FilterChain         filterChain
    ) throws ServletException, IOException {

        String traceId = request.getHeader(REQUEST_ID_HEADER);
        if (traceId == null || traceId.isBlank()) {
            traceId = UUID.randomUUID().toString();
        }

        MDC.put(MDC_TRACE_ID_KEY, traceId);
        response.setHeader(TRACE_ID_HEADER, traceId);

        try {
            filterChain.doFilter(request, response);
        } finally {
            // Always clear MDC to prevent leaking into pooled threads
            MDC.remove(MDC_TRACE_ID_KEY);
        }
    }
}
