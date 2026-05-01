package com.igaworks.dfinery.recruit.backend.app.ingestion.storage

import com.fasterxml.jackson.databind.ObjectMapper
import com.igaworks.dfinery.recruit.backend.app.ingestion.config.IngestionProperties
import com.igaworks.dfinery.recruit.backend.model.event.Event
import com.igaworks.dfinery.recruit.backend.model.ingestion.DataIngestionRequestDTO
import org.springframework.stereotype.Component
import java.nio.file.Path
import java.time.Instant

@Component
class IngestionStorage(
    objectMapper: ObjectMapper,
    properties: IngestionProperties
) : AutoCloseable {
    private val maxRowsPerFile = properties.maxRowsPerFile.coerceAtLeast(1)
    private val basePath = Path.of(properties.outputDir)
    private val validStore = JsonRollingFileStore(
        objectMapper = objectMapper,
        directory = basePath.resolve("valid"),
        filePrefix = "valid-events",
        maxRowsPerFile = maxRowsPerFile
    )
    private val invalidStore = JsonRollingFileStore(
        objectMapper = objectMapper,
        directory = basePath.resolve("invalid"),
        filePrefix = "invalid-events",
        maxRowsPerFile = maxRowsPerFile
    )

    fun storeValid(common: DataIngestionRequestDTO.Common, event: Event) {
        validStore.write(
            mapOf(
                "ingested_at" to Instant.now().toString(),
                "service_id" to common.serviceId,
                "user_id" to common.userId,
                "device_id" to common.deviceId,
                "event_log_id" to event.eventLogId,
                "event_name" to event.eventName,
                "event_datetime" to event.eventDatetime,
                "event_properties" to event.eventProperties
            )
        )
    }

    fun storeInvalid(
        common: DataIngestionRequestDTO.Common,
        event: Event?,
        eventIndex: Int?,
        errors: List<String>
    ) {
        invalidStore.write(
            mapOf(
                "received_at" to Instant.now().toString(),
                "service_id" to common.serviceId,
                "user_id" to common.userId,
                "device_id" to common.deviceId,
                "event_index" to eventIndex,
                "event" to event,
                "errors" to errors
            )
        )
    }

    override fun close() {
        validStore.close()
        invalidStore.close()
    }
}
