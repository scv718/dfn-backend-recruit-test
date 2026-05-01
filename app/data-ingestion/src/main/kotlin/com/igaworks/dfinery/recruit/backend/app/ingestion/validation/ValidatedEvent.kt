package com.igaworks.dfinery.recruit.backend.app.ingestion.validation

import com.igaworks.dfinery.recruit.backend.model.event.Event

data class ValidatedEvent(
    val event: Event?,
    val eventIndex: Int?,
    val errors: List<String>
) {
    val isValid: Boolean = errors.isEmpty()
}
