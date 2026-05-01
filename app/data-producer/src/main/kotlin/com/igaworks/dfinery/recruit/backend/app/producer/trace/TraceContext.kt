package com.igaworks.dfinery.recruit.backend.app.producer.trace

import org.slf4j.MDC
import java.util.UUID

object TraceContext {
    const val HEADER_NAME = "X-Trace-Id"
    const val MDC_KEY = "traceId"

    fun currentTraceId(): String {
        return MDC.get(MDC_KEY) ?: newTraceId()
    }

    fun newTraceId(): String {
        return UUID.randomUUID().toString()
    }

    inline fun <T> withTraceId(traceId: String, block: () -> T): T {
        val previousTraceId = MDC.get(MDC_KEY)
        MDC.put(MDC_KEY, traceId)
        return try {
            block()
        } finally {
            if (previousTraceId == null) {
                MDC.remove(MDC_KEY)
            } else {
                MDC.put(MDC_KEY, previousTraceId)
            }
        }
    }

    suspend inline fun <T> withTraceIdSuspend(traceId: String, crossinline block: suspend () -> T): T {
        val previousTraceId = MDC.get(MDC_KEY)
        MDC.put(MDC_KEY, traceId)
        return try {
            block()
        } finally {
            if (previousTraceId == null) {
                MDC.remove(MDC_KEY)
            } else {
                MDC.put(MDC_KEY, previousTraceId)
            }
        }
    }
}
