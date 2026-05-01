package com.igaworks.dfinery.recruit.backend.app.ingestion.pipeline

import com.igaworks.dfinery.recruit.backend.app.ingestion.config.IngestionProperties
import com.igaworks.dfinery.recruit.backend.app.ingestion.exception.IngestionPipelineUnavailableException
import com.igaworks.dfinery.recruit.backend.app.ingestion.storage.IngestionStorage
import com.igaworks.dfinery.recruit.backend.app.ingestion.trace.TraceContext
import com.igaworks.dfinery.recruit.backend.app.ingestion.validation.EventValidator
import com.igaworks.dfinery.recruit.backend.model.ingestion.DataIngestionRequestDTO
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong

@Service
class IngestionPipeline(
    private val properties: IngestionProperties,
    private val validator: EventValidator,
    private val storage: IngestionStorage
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val channel = Channel<QueuedIngestionRequest>(capacity = properties.queueCapacity.coerceAtLeast(1))
    private val acceptedRequests = AtomicLong(0)
    private val rejectedRequests = AtomicLong(0)
    private val failedRequests = AtomicLong(0)

    @PostConstruct
    fun start() {
        val workerCount = properties.workerCount.coerceAtLeast(1)
        repeat(workerCount) { workerId ->
            scope.launch {
                for (request in channel) {
                    process(request, workerId)
                }
            }
        }
        log.info(
            "Ingestion pipeline started: workers={}, queueCapacity={}, maxRowsPerFile={}, outputDir={}",
            workerCount,
            properties.queueCapacity,
            properties.maxRowsPerFile,
            properties.outputDir
        )
    }

    fun enqueue(traceId: String, request: DataIngestionRequestDTO): Boolean {
        val queuedRequest = QueuedIngestionRequest(
            traceId = traceId,
            receivedAt = Instant.now(),
            body = request
        )
        val result = channel.trySend(queuedRequest)
        return if (result.isSuccess) {
            acceptedRequests.incrementAndGet()
            true
        } else {
            result.exceptionOrNull()?.let { cause ->
                throw IngestionPipelineUnavailableException("Failed to enqueue collect request", cause)
            }

            rejectedRequests.incrementAndGet()
            log.warn(
                "Ingestion queue is full. rejectedRequests={}, eventCount={}",
                rejectedRequests.get(),
                request.events.size
            )
            false
        }
    }

    private suspend fun process(queuedRequest: QueuedIngestionRequest, workerId: Int) {
        runCatching {
            processWithTrace(queuedRequest, workerId)
        }.onFailure { error ->
            TraceContext.withTraceId(queuedRequest.traceId) {
                failedRequests.incrementAndGet()
                log.error("Pipeline worker failed to process request: workerId={}", workerId, error)
            }
        }
    }

    private suspend fun processWithTrace(queuedRequest: QueuedIngestionRequest, workerId: Int) {
        val request = queuedRequest.body
        val validatedEvents = validator.validate(request)
        val validEvents = validatedEvents
            .filter { it.isValid }
            .mapNotNull { it.event }
        val invalidEvents = validatedEvents.filterNot { it.isValid }

        withContext(Dispatchers.IO) {
            storage.storeValidBatch(queuedRequest.traceId, request.common, validEvents)
            storage.storeInvalidBatch(queuedRequest.traceId, request.common, invalidEvents)
        }

        TraceContext.withTraceId(queuedRequest.traceId) {
            log.debug(
                "Processed collect request: workerId={}, eventCount={}, valid={}, invalid={}, queuedMs={}",
                workerId,
                request.events.size,
                validEvents.size,
                invalidEvents.size,
                Instant.now().toEpochMilli() - queuedRequest.receivedAt.toEpochMilli()
            )
        }
    }

    @PreDestroy
    fun shutdown() {
        channel.close()
        runBlocking {
            withTimeoutOrNull(5_000) {
                scope.coroutineContext[Job]?.children?.toList()?.joinAll()
            }
        }
        scope.cancel()
        storage.close()
        log.info(
            "Ingestion pipeline stopped: acceptedRequests={}, rejectedRequests={}, failedRequests={}",
            acceptedRequests.get(),
            rejectedRequests.get(),
            failedRequests.get()
        )
    }
}
