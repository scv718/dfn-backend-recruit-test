package com.igaworks.dfinery.recruit.backend.app.ingestion.controller

import com.igaworks.dfinery.recruit.backend.app.ingestion.pipeline.IngestionPipeline
import com.igaworks.dfinery.recruit.backend.app.ingestion.trace.TraceContext
import com.igaworks.dfinery.recruit.backend.model.ingestion.DataIngestionRequestDTO
import com.igaworks.dfinery.recruit.backend.model.ingestion.DataIngestionResponseDTO
import org.slf4j.LoggerFactory
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api")
class DataIngestionController(
    private val ingestionPipeline: IngestionPipeline
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @PostMapping("/v1/collect")
    suspend fun collect(@RequestBody request: DataIngestionRequestDTO): DataIngestionResponseDTO {
        val traceId = TraceContext.currentTraceId()
        val common = request.common
        val eventCount = request.events.orEmpty().size
        log.info(
            "수집 요청 수신: serviceId={}, userId={}, deviceId={}, eventCount={}",
            common?.serviceId,
            common?.userId,
            common?.deviceId,
            eventCount
        )

        val accepted = ingestionPipeline.enqueue(traceId, request)
        return DataIngestionResponseDTO(
            success = accepted,
            rowCount = if (accepted) eventCount else 0,
            message = if (accepted) "accepted" else "ingestion queue is full",
            traceId = traceId
        )
    }
}
