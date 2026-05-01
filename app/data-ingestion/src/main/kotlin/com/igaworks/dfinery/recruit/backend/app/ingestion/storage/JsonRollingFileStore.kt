package com.igaworks.dfinery.recruit.backend.app.ingestion.storage

import com.fasterxml.jackson.databind.ObjectMapper
import java.io.BufferedWriter
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

class JsonRollingFileStore(
    private val objectMapper: ObjectMapper,
    private val directory: Path,
    private val filePrefix: String,
    private val maxRowsPerFile: Int
) : AutoCloseable {
    private val timestampFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
        .withZone(ZoneOffset.UTC)
    private var writer: BufferedWriter? = null
    private var currentRows = 0
    private var fileSequence = 0

    fun write(row: Map<String, Any?>) {
        writeAll(listOf(row))
    }

    @Synchronized
    fun writeAll(rows: List<Map<String, Any?>>) {
        if (rows.isEmpty()) {
            return
        }

        rows.forEach { row ->
            if (writer == null || currentRows >= maxRowsPerFile) {
                openNextFile()
            }

            writer!!.write(objectMapper.writeValueAsString(row))
            writer!!.newLine()
            currentRows += 1
        }
        writer!!.flush()
    }

    @Synchronized
    override fun close() {
        writer?.flush()
        writer?.close()
        writer = null
    }

    private fun openNextFile() {
        close()
        Files.createDirectories(directory)
        val timestamp = timestampFormatter.format(Instant.now())
        var path: Path
        do {
            val fileName = "$filePrefix-$timestamp-${fileSequence.toString().padStart(5, '0')}.jsonl"
            path = directory.resolve(fileName)
            fileSequence += 1
        } while (Files.exists(path))

        writer = Files.newBufferedWriter(
            path,
            StandardOpenOption.CREATE_NEW,
            StandardOpenOption.WRITE
        )
        currentRows = 0
    }
}
