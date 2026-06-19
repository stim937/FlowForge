# Failure / Retry / DLQ

이 문서는 FlowForge의 실패 처리, retry, DLQ 동작을 설명한다.

## 현재 구현 범위

구현된 기능:

- Worker 처리 실패 감지
- `retryCount` 기반 retry topic publish
- 최대 retry 초과 시 DLQ topic publish
- `failed_events` 저장
- `processed_events` 기반 중복 이벤트 방지
- 관리자 DLQ 조회 API
- 관리자 DLQ 재처리 API의 기본 상태 변경

아직 개선 대상:

- outbox pattern
- retry backoff
- Kafka transaction
- DLQ 재처리 시 Kafka 재발행 자동화
- failure event 전용 notification service

## 실패 주입

요청 payload에 `forceFail: true`를 넣으면 Worker가 의도적으로 실패한다.

```bash
curl -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "requester": "test-user",
    "dataType": "CSV",
    "itemCount": 1000,
    "idempotencyKey": "dlq-demo-key-1",
    "payload": {
      "source": "mock",
      "forceFail": true
    }
  }'
```

Worker는 top-level payload와 nested `payload` 모두에서 `forceFail`을 확인한다.

## Retry Flow

```text
data.process.requested
  |
  v
Worker process()
  |
  |-- success
  |     -> jobs.status = COMPLETED
  |     -> processed_events insert
  |     -> Redis progress = 100
  |
  |-- failure and retryCount < maxRetryCount
  |     -> jobs.status = RETRYING
  |     -> data.process.retry publish with retryCount + 1
  |
  |-- failure and retryCount >= maxRetryCount
        -> jobs.status = DLQ
        -> failed_events upsert-like save
        -> data.process.dlq publish
        -> Redis progress = 0
```

기본 retry 한도:

```text
APP_WORKER_MAX_RETRY_COUNT=3
```

## DLQ 상태 확인

job 상태:

```bash
curl http://localhost:8080/api/v1/jobs/{jobId}
```

DLQ 목록:

```bash
curl http://localhost:8080/api/v1/admin/dlq-events
```

Kafka topic:

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic data.process.dlq \
  --from-beginning
```

## 재처리 API

```bash
curl -X POST http://localhost:8080/api/v1/admin/dlq-events/{eventId}/reprocess
```

현재 단계에서 이 API는 실패 이벤트를 재처리 대상으로 관리하기 위한 최소 API다. 운영 수준의 재처리는 다음 순서로 확장한다.

```text
1. failed_events payload deserialize
2. retryCount reset or increment policy 결정
3. data.process.retry 또는 data.process.requested 재발행
4. failed_events status REPROCESSED / REPROCESS_FAILED 기록
5. audit log 저장
```

## 중복 이벤트 방지

Worker는 처리 전 `processed_events.event_id`를 확인한다.

```text
if processed_events.existsByEventId(event.eventId)
  -> skip
else
  -> process
  -> insert processed_events on success
```

이 방식은 성공 이벤트의 중복 처리를 줄인다. 실패 중복 이벤트에 대한 더 강한 보장은 retry/DLQ 상태와 event table unique 정책을 함께 강화해야 한다.

## 관측 지표

Worker Micrometer counters:

```text
flowforge_worker_jobs_completed_total
flowforge_worker_jobs_retried_total
flowforge_worker_jobs_dlq_total
```

Kafka lag:

```text
kafka_consumergroup_lag
```

Prometheus query:

```bash
curl "http://localhost:9090/api/v1/query?query=flowforge_worker_jobs_dlq_total"
curl "http://localhost:9090/api/v1/query?query=kafka_consumergroup_lag"
```

## 운영 기준

- 일반 부하 테스트에서 DLQ 발생 건수는 0을 목표로 한다.
- 장애 주입 테스트에서는 DLQ 이벤트가 생성되어야 한다.
- retry topic에 이벤트가 쌓이면 Worker replica, Kafka lag, DB latency를 함께 확인한다.
- DLQ가 발생하면 payload, errorMessage, retryCount를 기준으로 재처리 가능 여부를 판단한다.
