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

    private fun requestOf(event: Event): DataIngestionRequestDTO {
        return DataIngestionRequestDTO(
            common = DataIngestionRequestDTO.Common(
                serviceId = "test-svc",
                userId = UUID.randomUUID().toString(),
                deviceId = UUID.randomUUID().toString()
            ),
            events = listOf(event)
        )
    }
}
