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
- Missing or null structured fields are accepted into the async pipeline and stored as invalid rows, instead of being rejected before validation.
- Invalid rows keep the original event and a list of validation errors under the `invalid` output directory.

## Backpressure Strategy

The ingestion queue is bounded. When it is full, the API still responds immediately with HTTP 200, but the response body has `success=false` and `rowCount=0`. This keeps the HTTP contract predictable while making overload visible to the caller and logs.

## Error Handling

- Invalid JSON or malformed request bodies return a structured failure response.
- Queue saturation returns `success=false` with an explicit message instead of throwing from the controller.
- Closed or unavailable pipeline failures are surfaced as a service-unavailable response.
- Worker/storage exceptions are isolated inside the worker scope and logged without stopping the whole pipeline.
- The producer treats `success=false` ingestion responses as failed sends instead of counting them as successful HTTP calls.

## Traceability

- The ingestion API accepts `X-Trace-Id` and generates one when the header is absent.
- The trace id is returned in the API response and written to valid/invalid JSONL rows.
- Logs use `[traceId][prefix] message` format so controller and worker logs can be correlated.
- The producer sends a unique `X-Trace-Id` for each collect request.
- The producer also accepts/returns `X-Trace-Id` on `/push` and uses it for push-level logs.

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

Additional edge-case verification:

- Sent valid direct collect requests for all 10 allowed event names.
- Sent 29 invalid-but-parseable collect requests, including missing `common`, null `common`, empty `common`, missing/null/empty `events`, null event item, empty event object, invalid UUIDs, bad `event_log_id`, invalid/future datetime, missing properties, wrong property types, enum errors, range errors, logout properties, and over-length keyword.
- Confirmed all 29 invalid structured-data requests returned HTTP 200 and were persisted under `invalid`.
- Sent 4 malformed or structurally incompatible JSON requests and confirmed they returned HTTP 400.
- Re-ran rolling verification after nullable DTO hardening: 2,181 valid rows were split into 2,000 and 181 row files.
- Ran a constrained backpressure test with `queue-capacity=1` and `worker-count=1`: 120 concurrent producer requests yielded 2 successes and 118 controlled failures, with matching queue-full logs.
