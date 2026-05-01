# AI-Assisted Development Notes

## Goal

Implement the `data-ingestion` pipeline so `POST /api/v1/collect` returns immediately and processes incoming event rows asynchronously.

## Design

- API thread only enqueues the request into a bounded in-memory channel and returns HTTP 200 with the accepted row count.
- Worker coroutines consume the channel in parallel.
- Each event is validated against `ValidationRule.md`.
- Valid and invalid rows are stored separately as newline-delimited JSON files.
- Output files roll over after 2,000 rows.
- Queue capacity, worker count, file row limit, and output directory are configurable in `application.properties`.

## Performance Notes

- Pipeline workers group validation results by request and write valid/invalid rows in batches.
- The file store acquires its write lock once per batch and flushes once per batch, instead of flushing every row.
- File rolling is still enforced inside the batch writer, so a large batch can safely span multiple 2,000-row output files.
- Further scaling can move the current channel boundary to an external queue and the file writer boundary to shard writers or object storage.

## Validation Coverage

- Common fields: `service_id`, `user_id`, `device_id`.
- Event fields: `event_log_id`, `event_name`, `event_datetime`.
- Event-specific `event_properties` for all producer event names.
- Invalid rows keep the original event and a list of validation errors under the `invalid` output directory.

## Backpressure Strategy

The ingestion queue is bounded. When it is full, the API still responds immediately with HTTP 200, but the response body has `success=false` and `rowCount=0`. This keeps the HTTP contract predictable while making overload visible to the caller and logs.

## Verification Log

```bash
./gradlew.bat clean build
```

Result: `BUILD SUCCESSFUL`.

Manual runtime checks:

```bash
POST http://localhost:8081/push
{
  "totalRequests": 10,
  "concurrency": 2,
  "eventsPerRequest": 3
}
```

Result: `success=10`, `fail=0`, and 30 valid rows stored.

```bash
POST http://localhost:8081/push
{
  "totalRequests": 410,
  "concurrency": 20,
  "eventsPerRequest": 5
}
```

Result: `success=410`, `fail=0`, and valid output rolled into files with 2,000 and 80 rows.

An invalid direct request to `POST /api/v1/collect` produced one invalid JSONL row with explicit validation errors.
