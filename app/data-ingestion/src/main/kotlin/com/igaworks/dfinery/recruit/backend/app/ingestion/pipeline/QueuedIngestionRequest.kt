package com.igaworks.dfinery.recruit.backend.app.ingestion.pipeline

import com.igaworks.dfinery.recruit.backend.model.ingestion.DataIngestionRequestDTO
import java.time.Instant

data class QueuedIngestionRequest(
    val traceId: String,
    val receivedAt: Instant,
    val body: DataIngestionRequestDTO
)
