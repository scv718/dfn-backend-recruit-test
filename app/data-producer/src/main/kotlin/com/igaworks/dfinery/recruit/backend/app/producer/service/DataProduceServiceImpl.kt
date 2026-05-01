package com.igaworks.dfinery.recruit.backend.app.producer.service

import com.igaworks.dfinery.recruit.backend.app.producer.client.IngestionClient
import com.igaworks.dfinery.recruit.backend.app.producer.trace.TraceContext
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
        val pushTraceId = TraceContext.currentTraceId()
        val successCount = AtomicInteger(0)
        val failCount = AtomicInteger(0)
        val startTime = System.currentTimeMillis()
        val requestCount = totalRequests.coerceAtLeast(0)
        val concurrencyLimit = concurrency.coerceAtLeast(1)
        val eventCountPerRequest = eventsPerRequest.coerceAtLeast(1)

        TraceContext.withTraceId(pushTraceId) {
            log.info(
                "데이터 전송 시작: totalRequests={}, concurrency={}, eventsPerRequest={}",
                requestCount,
                concurrencyLimit,
                eventCountPerRequest
            )
        }

        coroutineScope {
            (1..requestCount).chunked(concurrencyLimit).forEach { batch ->
                batch.map { idx ->
                    async(Dispatchers.IO) {
                        val requestTraceId = TraceContext.newTraceId()
                        try {
                            val body = EventDataGenerator.generateRequest(eventCountPerRequest)
                            val response = ingestionClient.sendEvents(body, requestTraceId)
                            if (!response.success) {
                                throw IllegalStateException(response.message ?: "ingestion request was rejected")
                            }
                            successCount.incrementAndGet()
                        } catch (e: Exception) {
                            failCount.incrementAndGet()
                            TraceContext.withTraceId(requestTraceId) {
                                log.error("[{}] 데이터 전송 실패: {}", idx, e.message)
                            }
                        }
                    }
                }.awaitAll()
            }
        }

        val elapsed = System.currentTimeMillis() - startTime
        TraceContext.withTraceId(pushTraceId) {
            log.info(
                "데이터 전송 완료: requests(success={}, fail={}), totalEvents={}, elapsed={}ms",
                successCount.get(),
                failCount.get(),
                successCount.get() * eventCountPerRequest,
                elapsed
            )
        }

        return PushResponseDTO(
            success = successCount.get(),
            fail = failCount.get(),
            elapsedMs = elapsed
        )
    }
}
