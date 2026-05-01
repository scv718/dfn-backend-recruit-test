package com.igaworks.dfinery.recruit.backend.app.ingestion.validation

import com.igaworks.dfinery.recruit.backend.model.event.Event
import com.igaworks.dfinery.recruit.backend.model.event.enums.EventName
import com.igaworks.dfinery.recruit.backend.model.event.enums.EventProperty
import com.igaworks.dfinery.recruit.backend.model.event.enums.LoginMethod
import com.igaworks.dfinery.recruit.backend.model.event.enums.PaymentMethod
import com.igaworks.dfinery.recruit.backend.model.ingestion.DataIngestionRequestDTO
import org.springframework.stereotype.Component
import java.time.Instant
import java.util.UUID

@Component
class EventValidator {
    private val loginMethods = LoginMethod.entries.map { it.method }.toSet()
    private val paymentMethods = PaymentMethod.entries.map { it.method }.toSet()

    fun validate(request: DataIngestionRequestDTO): List<ValidatedEvent> {
        val commonErrors = validateCommon(request.common)
        val events = request.events
        if (events.isNullOrEmpty()) {
            return listOf(
                ValidatedEvent(
                    event = null,
                    eventIndex = null,
                    errors = commonErrors + "events must not be empty"
                )
            )
        }

        return events.mapIndexed { index, event ->
            ValidatedEvent(
                event = event,
                eventIndex = index,
                errors = commonErrors + validateEvent(event)
            )
        }
    }

    private fun validateCommon(common: DataIngestionRequestDTO.Common?): List<String> {
        if (common == null) {
            return listOf("common must not be null")
        }

        val errors = mutableListOf<String>()
        requireString("service_id", common.serviceId, maxLength = 50, errors = errors)
        requireUuid("user_id", common.userId, errors)
        requireUuid("device_id", common.deviceId, errors)
        return errors
    }

    private fun validateEvent(event: Event?): List<String> {
        if (event == null) {
            return listOf("event must not be null")
        }

        val errors = mutableListOf<String>()
        validateEventLogId(event.eventLogId, errors)
        val eventName = validateEventName(event.eventName, errors)
        validateEventDatetime(event.eventDatetime, errors)

        if (eventName != null) {
            validateProperties(eventName, event.eventProperties, errors)
        }

        return errors
    }

    private fun validateEventLogId(value: String?, errors: MutableList<String>) {
        if (value.isNullOrBlank()) {
            errors += "event_log_id must not be blank"
            return
        }

        val parts = value.split(":")
        if (parts.size != 2) {
            errors += "event_log_id must match {UUID}:{timestamp}"
            return
        }

        if (!isUuid(parts[0])) {
            errors += "event_log_id uuid part must be UUID"
        }

        if (parts[1].toLongOrNull() == null) {
            errors += "event_log_id timestamp part must be numeric"
        }
    }

    private fun validateEventName(value: String?, errors: MutableList<String>): EventName? {
        if (value.isNullOrBlank()) {
            errors += "event_name must not be blank"
            return null
        }

        val eventName = EventName.from(value)
        if (eventName == null) {
            errors += "event_name is not allowed: $value"
        }
        return eventName
    }

    private fun validateEventDatetime(value: String?, errors: MutableList<String>) {
        if (value.isNullOrBlank()) {
            errors += "event_datetime must not be blank"
            return
        }

        val instant = runCatching { Instant.parse(value) }.getOrElse {
            errors += "event_datetime must be ISO-8601 instant"
            return
        }

        if (instant.isAfter(Instant.now().plusMillis(100))) {
            errors += "event_datetime must not be in the future"
        }
    }

    private fun validateProperties(
        eventName: EventName,
        properties: Map<String, Any?>?,
        errors: MutableList<String>
    ) {
        when (eventName) {
            EventName.START_SESSION -> {
                val map = requireProperties(eventName, properties, errors) ?: return
                requireString(EventProperty.SESSION_ID.key, map, maxLength = 100, errors = errors)
            }
            EventName.END_SESSION -> {
                val map = requireProperties(eventName, properties, errors) ?: return
                requireString(EventProperty.SESSION_ID.key, map, maxLength = 100, errors = errors)
                requireInteger(EventProperty.SESSION_DURATION.key, map, minInclusive = 0, errors = errors)
            }
            EventName.LOGIN -> {
                val map = requireProperties(eventName, properties, errors) ?: return
                requireEnum(EventProperty.LOGIN_METHOD.key, map, loginMethods, errors)
            }
            EventName.LOGOUT -> {
                if (!properties.isNullOrEmpty()) {
                    errors += "df_logout must not have event_properties"
                }
            }
            EventName.PURCHASE -> {
                val map = requireProperties(eventName, properties, errors) ?: return
                requireUuid(EventProperty.ORDER_ID.key, map, errors)
                requireNumber(EventProperty.TOTAL_PURCHASE_AMOUNT.key, map, minExclusive = 0.0, errors = errors)
                requireEnum(EventProperty.PAYMENT_METHOD.key, map, paymentMethods, errors)
            }
            EventName.VIEW_PRODUCT -> {
                val map = requireProperties(eventName, properties, errors) ?: return
                requireString(EventProperty.PRODUCT_ID.key, map, maxLength = 100, errors = errors)
                requireString(EventProperty.PRODUCT_NAME.key, map, maxLength = 200, errors = errors)
                requireNumber(EventProperty.PRICE.key, map, minInclusive = 0.0, errors = errors)
            }
            EventName.ADD_TO_CART -> {
                val map = requireProperties(eventName, properties, errors) ?: return
                requireString(EventProperty.PRODUCT_ID.key, map, maxLength = 100, errors = errors)
                requireString(EventProperty.PRODUCT_NAME.key, map, maxLength = 200, errors = errors)
                requireNumber(EventProperty.PRICE.key, map, minInclusive = 0.0, errors = errors)
                requireInteger(EventProperty.QUANTITY.key, map, minInclusive = 1, errors = errors)
            }
            EventName.SEARCH -> {
                val map = requireProperties(eventName, properties, errors) ?: return
                requireString(EventProperty.SEARCH_KEYWORD.key, map, maxLength = 500, errors = errors)
            }
            EventName.SIGN_UP -> {
                val map = requireProperties(eventName, properties, errors) ?: return
                requireEnum(EventProperty.SIGN_UP_METHOD.key, map, loginMethods, errors)
            }
            EventName.ADD_PAYMENT_INFO -> {
                val map = requireProperties(eventName, properties, errors) ?: return
                requireEnum(EventProperty.PAYMENT_METHOD.key, map, paymentMethods, errors)
            }
        }
    }

    private fun requireProperties(
        eventName: EventName,
        properties: Map<String, Any?>?,
        errors: MutableList<String>
    ): Map<String, Any?>? {
        if (properties == null) {
            errors += "${eventName.eventName} must have event_properties"
        }
        return properties
    }

    private fun requireString(
        field: String,
        value: String?,
        maxLength: Int,
        errors: MutableList<String>
    ) {
        if (value.isNullOrBlank()) {
            errors += "$field must not be blank"
        } else if (value.length > maxLength) {
            errors += "$field length must be <= $maxLength"
        }
    }

    private fun requireString(
        field: String,
        map: Map<String, Any?>,
        maxLength: Int,
        errors: MutableList<String>
    ) {
        val value = map[field]
        if (value !is String) {
            errors += "$field must be string"
            return
        }
        requireString(field, value, maxLength, errors)
    }

    private fun requireUuid(field: String, value: String?, errors: MutableList<String>) {
        if (value.isNullOrBlank()) {
            errors += "$field must not be blank"
        } else if (!isUuid(value)) {
            errors += "$field must be UUID"
        }
    }

    private fun requireUuid(field: String, map: Map<String, Any?>, errors: MutableList<String>) {
        val value = map[field]
        if (value !is String) {
            errors += "$field must be UUID string"
            return
        }
        requireUuid(field, value, errors)
    }

    private fun requireEnum(
        field: String,
        map: Map<String, Any?>,
        allowed: Set<String>,
        errors: MutableList<String>
    ) {
        val value = map[field]
        if (value !is String || value !in allowed) {
            errors += "$field must be one of ${allowed.joinToString(", ")}"
        }
    }

    private fun requireNumber(
        field: String,
        map: Map<String, Any?>,
        minInclusive: Double? = null,
        minExclusive: Double? = null,
        errors: MutableList<String>
    ) {
        val number = map[field] as? Number
        if (number == null) {
            errors += "$field must be number"
            return
        }

        val value = number.toDouble()
        if (minInclusive != null && value < minInclusive) {
            errors += "$field must be >= $minInclusive"
        }
        if (minExclusive != null && value <= minExclusive) {
            errors += "$field must be > $minExclusive"
        }
    }

    private fun requireInteger(
        field: String,
        map: Map<String, Any?>,
        minInclusive: Long,
        errors: MutableList<String>
    ) {
        val number = map[field] as? Number
        if (number == null) {
            errors += "$field must be integer"
            return
        }

        val value = number.toDouble()
        if (value % 1.0 != 0.0) {
            errors += "$field must be integer"
            return
        }

        if (value < minInclusive) {
            errors += "$field must be >= $minInclusive"
        }
    }

    private fun isUuid(value: String): Boolean {
        return runCatching { UUID.fromString(value) }.isSuccess
    }
}
