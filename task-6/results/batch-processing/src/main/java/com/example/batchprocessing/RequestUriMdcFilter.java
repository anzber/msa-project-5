package com.example.batchprocessing;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * Кладёт метод и URI запроса в MDC, чтобы они попадали в каждую строку лога
 * вместе с traceId и spanId. Micrometer Tracing добавляет traceId/spanId в MDC
 * сам, но URI попадает только в атрибуты спана, поэтому добавляем его здесь.
 */
@Component
@Order(Ordered.LOWEST_PRECEDENCE)
public class RequestUriMdcFilter extends OncePerRequestFilter {

	@Override
	protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
			throws ServletException, IOException {
		MDC.put("uri", request.getMethod() + " " + request.getRequestURI());
		try {
			filterChain.doFilter(request, response);
		} finally {
			MDC.remove("uri");
		}
	}
}
