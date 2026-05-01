package com.igaworks.dfinery.recruit.backend.app.ingestion.validation

import com.igaworks.dfinery.recruit.backend.model.event.Event
import com.igaworks.dfinery.recruit.backend.model.ingestion.DataIngestionRequestDTO
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.UUID

class EventValidatorTest {
    private val validator = EventValidator()

    @Test
    fun `all allowed event types pass validation`() {
        val result = validator.validate(
            DataIngestionRequestDTO(
                common = validCommon(),
                events = listOf(
                    validEvent("df_start_session", mapOf("df_session_id" to "session-1")),
                    validEvent(
                        "df_end_session",
                        mapOf("df_session_id" to "session-1", "df_session_duration" to 30)
                    ),
                    validEvent("df_login", mapOf("df_login_method" to "Google")),
                    validEvent("df_logout", null),
                    validEvent(
                        "df_purchase",
                        mapOf(
                            "df_order_id" to UUID.randomUUID().toString(),
                            "df_total_purchase_amount" to 10_000.0,
                            "df_payment_method" to "Card"
                        )
                    ),
                    validEvent(
                        "df_view_product",
                        mapOf("df_product_id" to "p-1", "df_product_name" to "item", "df_price" to 1_000.0)
                    ),
                    validEvent(
                        "df_add_to_cart",
                        mapOf(
                            "df_product_id" to "p-1",
                            "df_product_name" to "item",
                            "df_price" to 1_000.0,
                            "df_quantity" to 1
                        )
                    ),
                    validEvent("df_search", mapOf("df_search_keyword" to "keyword")),
                    validEvent("df_sign_up", mapOf("df_sign_up_method" to "Apple")),
                    validEvent("df_add_payment_info", mapOf("df_payment_method" to "BankTransfer"))
                )
            )
        )

        assertThat(result).hasSize(10)
        assertThat(result).allMatch { it.isValid }
    }

    @Test
    fun `valid purchase event passes validation`() {
        val request = requestOf(
            Event(
                eventLogId = "${UUID.randomUUID()}:${Instant.now().toEpochMilli()}",
                eventName = "df_purchase",
                eventDatetime = Instant.now().minusSeconds(1).toString(),
                eventProperties = mapOf(
                    "df_order_id" to UUID.randomUUID().toString(),
                    "df_total_purchase_amount" to 10_000.0,
                    "df_payment_method" to "Card"
                )
            )
        )

        val result = validator.validate(request)

        assertThat(result).hasSize(1)
        assertThat(result.first().isValid).isTrue()
    }

    @Test
    fun `invalid event is returned with validation errors`() {
        val request = requestOf(
            Event(
                eventLogId = "not-a-log-id",
                eventName = "df_purchase",
                eventDatetime = Instant.now().plusSeconds(60).toString(),
                eventProperties = mapOf(
                    "df_order_id" to "not-a-uuid",
                    "df_total_purchase_amount" to 0,
                    "df_payment_method" to "Crypto"
                )
            )
        )

        val result = validator.validate(request)

        assertThat(result).hasSize(1)
        assertThat(result.first().isValid).isFalse()
        assertThat(result.first().errors).isNotEmpty()
    }

    @Test
    fun `missing body fields are treated as validation errors`() {
        val request = DataIngestionRequestDTO(
            common = DataIngestionRequestDTO.Common(),
            events = listOf(Event())
        )

        val result = validator.validate(request)

        assertThat(result).hasSize(1)
        assertThat(result.first().isValid).isFalse()
        assertThat(result.first().errors).contains(
            "service_id must not be blank",
            "user_id must not be blank",
            "device_id must not be blank",
            "event_log_id must not be blank",
            "event_name must not be blank",
            "event_datetime must not be blank"
        )
    }

    @Test
    fun `null common and empty events are treated as validation errors`() {
        val request = DataIngestionRequestDTO()

        val result = validator.validate(request)

        assertThat(result).hasSize(1)
        assertThat(result.first().isValid).isFalse()
        assertThat(result.first().errors).contains(
            "common must not be null",
            "events must not be empty"
        )
    }

    @Test
    fun `null event is treated as validation error`() {
        val request = DataIngestionRequestDTO(
            common = validCommon(),
            events = listOf(null)
        )

        val result = validator.validate(request)

        assertThat(result).hasSize(1)
        assertThat(result.first().isValid).isFalse()
        assertThat(result.first().errors).contains("event must not be null")
    }

    private fun validEvent(eventName: String, eventProperties: Map<String, Any?>?): Event {
        return Event(
            eventLogId = "${UUID.randomUUID()}:${Instant.now().toEpochMilli()}",
            eventName = eventName,
            eventDatetime = Instant.now().minusSeconds(1).toString(),
            eventProperties = eventProperties
        )
    }

    private fun validCommon(): DataIngestionRequestDTO.Common {
        return DataIngestionRequestDTO.Common(
            serviceId = "test-svc",
            userId = UUID.randomUUID().toString(),
            deviceId = UUID.randomUUID().toString()
        )
    }

    private fun requestOf(event: Event): DataIngestionRequestDTO {
        return DataIngestionRequestDTO(
            common = validCommon(),
            events = listOf(event)
        )
    }
}
