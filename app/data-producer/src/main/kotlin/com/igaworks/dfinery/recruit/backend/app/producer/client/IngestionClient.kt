package com.igaworks.dfinery.recruit.backend.app.producer.client

import com.igaworks.dfinery.recruit.backend.app.producer.config.ProducerProperties
import com.igaworks.dfinery.recruit.backend.app.producer.trace.TraceContext
import com.igaworks.dfinery.recruit.backend.model.ingestion.DataIngestionRequestDTO
import com.igaworks.dfinery.recruit.backend.model.ingestion.DataIngestionResponseDTO
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitBody

@Component
class IngestionClient(
    properties: ProducerProperties
) {
    private val webClient = WebClient.builder()
        .baseUrl(properties.targetUrl)
        .codecs { it.defaultCodecs().maxInMemorySize(10 * 1024 * 1024) }
        .build()

    suspend fun sendEvents(body: DataIngestionRequestDTO, traceId: String): DataIngestionResponseDTO {
        return webClient.post()
            .uri("/api/v1/collect")
            .header(TraceContext.HEADER_NAME, traceId)
            .bodyValue(body)
            .retrieve()
            .awaitBody()
    }
}
