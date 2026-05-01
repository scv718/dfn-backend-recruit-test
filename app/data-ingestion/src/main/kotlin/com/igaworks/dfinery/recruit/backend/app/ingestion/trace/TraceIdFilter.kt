package com.igaworks.dfinery.recruit.backend.app.ingestion.trace

import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.slf4j.MDC
import org.springframework.stereotype.Component
import org.springframework.web.filter.OncePerRequestFilter

@Component
class TraceIdFilter : OncePerRequestFilter() {
    override fun doFilterInternal(
        request: HttpServletRequest,
        response: HttpServletResponse,
        filterChain: FilterChain
    ) {
        val traceId = request.getHeader(TraceContext.HEADER_NAME)
            ?.takeIf { it.isNotBlank() }
            ?: TraceContext.newTraceId()

        MDC.put(TraceContext.MDC_KEY, traceId)
        response.setHeader(TraceContext.HEADER_NAME, traceId)
        try {
            filterChain.doFilter(request, response)
        } finally {
            MDC.remove(TraceContext.MDC_KEY)
        }
    }
}
