package com.igaworks.dfinery.recruit.backend.model.event

import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.annotation.JsonNaming

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
data class Event(
    val eventLogId: String? = null,
    val eventName: String? = null,
    val eventDatetime: String? = null,
    val eventProperties: Map<String, Any?>? = null
)
