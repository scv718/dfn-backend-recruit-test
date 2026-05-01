package com.igaworks.dfinery.recruit.backend.app.ingestion.exception

class IngestionPipelineUnavailableException(
    message: String,
    cause: Throwable? = null
) : RuntimeException(message, cause)
