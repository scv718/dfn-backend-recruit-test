package com.igaworks.dfinery.recruit.backend.app.ingestion.exception

class IngestionStorageException(
    message: String,
    cause: Throwable? = null
) : RuntimeException(message, cause)
