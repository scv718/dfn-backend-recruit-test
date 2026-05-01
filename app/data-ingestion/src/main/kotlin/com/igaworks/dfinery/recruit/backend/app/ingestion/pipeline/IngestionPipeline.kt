package com.igaworks.dfinery.recruit.backend.app.ingestion.pipeline

import com.igaworks.dfinery.recruit.backend.app.ingestion.config.IngestionProperties
import com.igaworks.dfinery.recruit.backend.app.ingestion.storage.IngestionStorage
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
import java.util.concurrent.atomic.AtomicLong

@Service
class IngestionPipeline(
    private val properties: IngestionProperties,
    private val validator: EventValidator,
    private val storage: IngestionStorage
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val channel = Channel<DataIngestionRequestDTO>(capacity = properties.queueCapacity.coerceAtLeast(1))
    private val acceptedRequests = AtomicLong(0)
    private val rejectedRequests = AtomicLong(0)

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

    fun enqueue(request: DataIngestionRequestDTO): Boolean {
        val result = channel.trySend(request)
        return if (result.isSuccess) {
            acceptedRequests.incrementAndGet()
            true
        } else {
            rejectedRequests.incrementAndGet()
            log.warn(
                "Ingestion queue is full. rejectedRequests={}, eventCount={}",
                rejectedRequests.get(),
                request.events.size
            )
            false
        }
    }

    private suspend fun process(request: DataIngestionRequestDTO, workerId: Int) {
        runCatching {
            val validatedEvents = validator.validate(request)
            withContext(Dispatchers.IO) {
                validatedEvents.forEach { result ->
                    if (result.isValid) {
                        storage.storeValid(request.common, result.event!!)
                    } else {
                        storage.storeInvalid(request.common, result.event, result.eventIndex, result.errors)
                    }
                }
            }
            log.debug(
                "Processed collect request: workerId={}, eventCount={}, valid={}, invalid={}",
                workerId,
                request.events.size,
                validatedEvents.count { it.isValid },
                validatedEvents.count { !it.isValid }
            )
        }.onFailure { error ->
            log.error("Pipeline worker failed to process request: workerId={}", workerId, error)
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
            "Ingestion pipeline stopped: acceptedRequests={}, rejectedRequests={}",
            acceptedRequests.get(),
            rejectedRequests.get()
        )
    }
}
