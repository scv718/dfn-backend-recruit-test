package com.igaworks.dfinery.recruit.backend.app.ingestion.controller

import com.igaworks.dfinery.recruit.backend.app.ingestion.pipeline.IngestionPipeline
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
        log.info(
            "Received collect request: serviceId={}, userId={}, deviceId={}, eventCount={}",
            request.common.serviceId,
            request.common.userId,
            request.common.deviceId,
            request.events.size
        )

        val accepted = ingestionPipeline.enqueue(request)
        return DataIngestionResponseDTO(
            success = accepted,
            rowCount = if (accepted) request.events.size else 0
        )
    }
}
