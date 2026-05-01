package com.igaworks.dfinery.recruit.backend.app.ingestion.controller

import com.igaworks.dfinery.recruit.backend.app.ingestion.exception.IngestionPipelineUnavailableException
import com.igaworks.dfinery.recruit.backend.app.ingestion.trace.TraceContext
import com.igaworks.dfinery.recruit.backend.model.ingestion.DataIngestionResponseDTO
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.http.converter.HttpMessageNotReadableException
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice

@RestControllerAdvice
class DataIngestionExceptionHandler {
    private val log = LoggerFactory.getLogger(javaClass)

    @ExceptionHandler(HttpMessageNotReadableException::class)
    fun handleInvalidBody(error: HttpMessageNotReadableException): ResponseEntity<DataIngestionResponseDTO> {
        log.warn("Invalid collect request body: {}", error.message)
        return response(HttpStatus.BAD_REQUEST, "invalid request body")
    }

    @ExceptionHandler(IngestionPipelineUnavailableException::class)
    fun handlePipelineUnavailable(error: IngestionPipelineUnavailableException): ResponseEntity<DataIngestionResponseDTO> {
        log.error("Ingestion pipeline is unavailable", error)
        return response(HttpStatus.SERVICE_UNAVAILABLE, "ingestion pipeline is unavailable")
    }

    @ExceptionHandler(Exception::class)
    fun handleUnexpected(error: Exception): ResponseEntity<DataIngestionResponseDTO> {
        log.error("Unexpected ingestion API error", error)
        return response(HttpStatus.INTERNAL_SERVER_ERROR, "unexpected ingestion error")
    }

    private fun response(
        status: HttpStatus,
        message: String
    ): ResponseEntity<DataIngestionResponseDTO> {
        return ResponseEntity
            .status(status)
            .body(
                DataIngestionResponseDTO(
                    success = false,
                    rowCount = 0,
                    message = message,
                    traceId = TraceContext.currentTraceId()
                )
            )
    }
}
