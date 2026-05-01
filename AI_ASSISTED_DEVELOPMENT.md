# AI 활용 개발 기록

## 목표

`POST /api/v1/collect` 요청에 대해 API는 즉시 응답하고, 전달된 이벤트 row는 내부 파이프라인에서 비동기로 처리하도록 구현했습니다.

## 설계

- API 스레드는 요청을 bounded in-memory channel에 넣고, 수락된 row 수와 함께 HTTP 200을 즉시 반환합니다.
- worker coroutine이 channel에서 요청을 가져와 병렬로 처리합니다.
- 각 이벤트는 `ValidationRule.md` 기준으로 검증합니다.
- 정상 row와 검증 실패 row는 newline-delimited JSON(JSONL) 파일로 분리 저장합니다.
- 저장 파일은 2,000 row 단위로 rolling 합니다.
- queue capacity, worker count, file row limit, output directory는 `application.properties`에서 설정할 수 있습니다.

## 성능 개선 포인트

- 요청 단위로 검증 결과를 묶어 valid/invalid row를 batch write 합니다.
- 파일 저장소는 row마다 lock/flush 하지 않고, batch 단위로 lock을 잡고 flush 합니다.
- batch 안에서도 2,000 row rolling 규칙은 유지되므로 큰 요청이 들어와도 여러 파일로 안전하게 나뉩니다.
- 추가 확장이 필요하면 현재 channel 경계를 외부 MQ로, 파일 writer 경계를 shard writer 또는 object storage로 확장할 수 있습니다.

## 검증 범위

- 공통 필드: `service_id`, `user_id`, `device_id`
- 이벤트 필드: `event_log_id`, `event_name`, `event_datetime`
- producer가 생성하는 모든 이벤트명에 대한 `event_properties`
- 누락/null 같은 구조화된 데이터 오류는 파싱 단계에서 바로 거절하지 않고, 비동기 validation 후 invalid row로 저장합니다.
- invalid row에는 원본 이벤트와 검증 오류 목록을 함께 저장합니다.

## 역압 처리 전략

수집 queue는 bounded queue로 제한했습니다. queue가 가득 찬 경우에도 API는 즉시 응답하지만, 응답 body에 `success=false`, `rowCount=0`과 명시적인 메시지를 담습니다. 이를 통해 HTTP 응답 흐름은 예측 가능하게 유지하면서도 overload 상황을 호출자와 로그에서 확인할 수 있게 했습니다.

## 예외 처리

- JSON 문법 오류 또는 구조적으로 파싱할 수 없는 요청은 구조화된 실패 응답으로 반환합니다.
- queue 포화 상황은 controller 예외가 아니라 `success=false` 응답으로 처리합니다.
- 닫혔거나 사용할 수 없는 pipeline 오류는 service unavailable 응답으로 노출합니다.
- worker/storage 예외는 worker scope 내부에서 격리하고 로그로 남겨 전체 pipeline이 중단되지 않게 했습니다.
- producer는 ingestion의 `success=false` 응답을 HTTP 성공으로만 보지 않고 전송 실패로 집계합니다.

## 추적성

- ingestion API는 `X-Trace-Id`를 수신하며, 헤더가 없으면 trace id를 생성합니다.
- trace id는 API 응답과 valid/invalid JSONL row에 함께 기록됩니다.
- 로그는 `[traceId][prefix] 내용` 형식으로 출력해 controller, worker, producer 로그를 연결해서 볼 수 있습니다.
- producer는 collect 요청마다 고유한 `X-Trace-Id`를 전달합니다.
- producer의 `/push` 요청도 `X-Trace-Id`를 수신/응답하고 push 단위 로그에 사용합니다.

## 검증 기록

```bash
./gradlew.bat clean build
```

결과: `BUILD SUCCESSFUL`

실행 중 수동 검증:

```bash
POST http://localhost:8081/push
{
  "totalRequests": 10,
  "concurrency": 2,
  "eventsPerRequest": 3
}
```

결과: `success=10`, `fail=0`, valid row 30건 저장 확인

```bash
POST http://localhost:8081/push
{
  "totalRequests": 410,
  "concurrency": 20,
  "eventsPerRequest": 5
}
```

결과: `success=410`, `fail=0`, valid output이 2,000 row와 80 row 파일로 rolling 되는 것 확인

invalid direct request를 `POST /api/v1/collect`로 전송했을 때 명시적인 validation error와 함께 invalid JSONL row가 1건 생성되는 것을 확인했습니다.

추가 edge case 검증:

- 허용된 이벤트명 10종에 대해 direct collect 정상 요청을 모두 전송했습니다.
- missing `common`, null `common`, empty `common`, missing/null/empty `events`, null event item, empty event object, invalid UUID, 잘못된 `event_log_id`, invalid/future datetime, missing properties, wrong property types, enum error, range error, logout properties, over-length keyword를 포함한 invalid-but-parseable 요청 29종을 전송했습니다.
- 위 29종의 구조화된 invalid 요청은 모두 HTTP 200으로 수락된 뒤 `invalid` 디렉터리에 저장되는 것을 확인했습니다.
- malformed JSON 또는 구조적으로 파싱할 수 없는 요청 4종은 HTTP 400으로 반환되는 것을 확인했습니다.
- nullable DTO 보강 이후 rolling 검증을 다시 수행해 valid row 2,181건이 2,000 row와 181 row 파일로 분리되는 것을 확인했습니다.
- `queue-capacity=1`, `worker-count=1` 조건에서 backpressure 테스트를 수행했고, 120개 동시 producer 요청 중 2건 성공, 118건 controlled failure 및 queue-full 로그가 남는 것을 확인했습니다.
