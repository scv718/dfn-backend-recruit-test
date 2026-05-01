package com.igaworks.dfinery.recruit.backend.app.producer.service

import com.igaworks.dfinery.recruit.backend.app.producer.client.IngestionClient
import com.igaworks.dfinery.recruit.backend.library.generator.EventDataGenerator
import com.igaworks.dfinery.recruit.backend.model.producer.PushResponseDTO
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.concurrent.atomic.AtomicInteger

@Service
class DataProduceServiceImpl(
    private val ingestionClient: IngestionClient
) : DataProduceService {

    private val log = LoggerFactory.getLogger(javaClass)

    override suspend fun push(totalRequests: Int, concurrency: Int, eventsPerRequest: Int): PushResponseDTO {
        val successCount = AtomicInteger(0)
        val failCount = AtomicInteger(0)
        val startTime = System.currentTimeMillis()
        val requestCount = totalRequests.coerceAtLeast(0)
        val concurrencyLimit = concurrency.coerceAtLeast(1)
        val eventCountPerRequest = eventsPerRequest.coerceAtLeast(1)

        log.info(
            "Push started: totalRequests={}, concurrency={}, eventsPerRequest={}",
            requestCount,
            concurrencyLimit,
            eventCountPerRequest
        )

        coroutineScope {
            (1..requestCount).chunked(concurrencyLimit).forEach { batch ->
                batch.map { idx ->
                    async(Dispatchers.IO) {
                        try {
                            val body = EventDataGenerator.generateRequest(eventCountPerRequest)
                            ingestionClient.sendEvents(body)
                            successCount.incrementAndGet()
                        } catch (e: Exception) {
                            failCount.incrementAndGet()
                            log.error("[{}] failed: {}", idx, e.message)
                        }
                    }
                }.awaitAll()
            }
        }

        val elapsed = System.currentTimeMillis() - startTime
        log.info(
            "Push completed: requests(success={}, fail={}), totalEvents={}, elapsed={}ms",
            successCount.get(),
            failCount.get(),
            successCount.get() * eventCountPerRequest,
            elapsed
        )

        return PushResponseDTO(
            success = successCount.get(),
            fail = failCount.get(),
            elapsedMs = elapsed
        )
    }
}
