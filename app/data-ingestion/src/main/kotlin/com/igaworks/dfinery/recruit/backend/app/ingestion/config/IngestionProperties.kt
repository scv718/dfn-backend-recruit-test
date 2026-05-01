package com.igaworks.dfinery.recruit.backend.app.ingestion.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "ingestion.pipeline")
data class IngestionProperties(
    val queueCapacity: Int = 10_000,
    val workerCount: Int = Runtime.getRuntime().availableProcessors().coerceAtLeast(2),
    val maxRowsPerFile: Int = 2_000,
    val outputDir: String = "build/ingestion-output"
)
