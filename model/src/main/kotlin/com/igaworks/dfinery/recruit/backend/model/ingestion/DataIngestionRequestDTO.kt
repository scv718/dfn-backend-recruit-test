package com.igaworks.dfinery.recruit.backend.model.ingestion

import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.annotation.JsonNaming
import com.igaworks.dfinery.recruit.backend.model.event.Event

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
data class DataIngestionRequestDTO(
    val common: Common? = null,
    val events: List<Event?>? = null
) {
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
    data class Common(
        val serviceId: String? = null,
        val userId: String? = null,
        val deviceId: String? = null
    )
}
