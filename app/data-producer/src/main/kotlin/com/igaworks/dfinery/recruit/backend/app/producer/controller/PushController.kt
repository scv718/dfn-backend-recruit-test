package com.igaworks.dfinery.recruit.backend.app.producer.controller

import com.igaworks.dfinery.recruit.backend.app.producer.service.DataProduceService
import com.igaworks.dfinery.recruit.backend.app.producer.trace.TraceContext
import com.igaworks.dfinery.recruit.backend.model.producer.PushRequestDTO
import com.igaworks.dfinery.recruit.backend.model.producer.PushResponseDTO
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.server.ServerWebExchange

@RestController
class PushController(
    private val dataProduceService: DataProduceService
) {
    @PostMapping("/push")
    suspend fun push(
        @RequestBody request: PushRequestDTO,
        exchange: ServerWebExchange
    ): PushResponseDTO {
        val traceId = exchange.request.headers.getFirst(TraceContext.HEADER_NAME)
            ?.takeIf { it.isNotBlank() }
            ?: TraceContext.newTraceId()

        exchange.response.headers.set(TraceContext.HEADER_NAME, traceId)
        return TraceContext.withTraceIdSuspend(traceId) {
            dataProduceService.push(request.totalRequests, request.concurrency, request.eventsPerRequest)
        }
    }
}
