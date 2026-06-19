# API Spec

Base URL:

```text
Local: http://localhost:8080
Kubernetes Ingress: http://localhost:8080 with Host: flowforge.local
```

모든 API 응답은 공통 response format을 사용한다.

## Response Format

성공:

```json
{
  "success": true,
  "data": {},
  "message": "success",
  "timestamp": "2026-06-18T00:00:00Z"
}
```

실패:

```json
{
  "success": false,
  "error": {
    "code": "INVALID_REQUEST",
    "message": "requester must not be blank"
  },
  "timestamp": "2026-06-18T00:00:00Z"
}
```

## POST /api/v1/jobs

데이터 처리 job을 생성하고 Kafka `data.process.requested` 이벤트를 발행한다. API는 job을 접수한 뒤 실제 처리는 Worker에 위임한다.

Status:

```text
202 Accepted
```

Request body:

```json
{
  "requester": "test-user",
  "dataType": "CSV",
  "itemCount": 10000,
  "idempotencyKey": "demo-key-1",
  "payload": {
    "source": "mock",
    "description": "large data processing test"
  }
}
```

Validation:

| Field | Rule |
| --- | --- |
| `requester` | required, not blank |
| `dataType` | required, not blank |
| `itemCount` | minimum 1 |
| `idempotencyKey` | optional |
| `payload` | optional object |

Response:

```json
{
  "success": true,
  "data": {
    "jobId": "JOB-20260618-550e8400-e29b-41d4-a716-446655440000",
    "status": "REQUESTED",
    "message": "Data processing job has been accepted."
  },
  "message": "success",
  "timestamp": "2026-06-18T00:00:00Z"
}
```

Idempotency:

`idempotencyKey`가 기존 job과 같으면 새 job을 만들지 않고 기존 job 정보를 반환한다.

```json
{
  "success": true,
  "data": {
    "jobId": "JOB-20260618-existing",
    "status": "REQUESTED",
    "message": "Data processing job has already been accepted."
  },
  "message": "success",
  "timestamp": "2026-06-18T00:00:00Z"
}
```

curl:

```bash
curl -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "requester": "test-user",
    "dataType": "CSV",
    "itemCount": 10000,
    "idempotencyKey": "demo-key-1",
    "payload": {
      "source": "mock"
    }
  }'
```

## GET /api/v1/jobs/{jobId}

job 상태와 처리 건수를 조회한다.

Status:

```text
200 OK
```

Response:

```json
{
  "success": true,
  "data": {
    "jobId": "JOB-20260618-550e8400-e29b-41d4-a716-446655440000",
    "requester": "test-user",
    "status": "COMPLETED",
    "dataType": "CSV",
    "itemCount": 10000,
    "processedCount": 10000,
    "failedCount": 0,
    "createdAt": "2026-06-18T00:00:00Z",
    "updatedAt": "2026-06-18T00:00:03Z"
  },
  "message": "success",
  "timestamp": "2026-06-18T00:00:04Z"
}
```

curl:

```bash
curl http://localhost:8080/api/v1/jobs/{jobId}
```

없는 job을 조회하면 `400 INVALID_REQUEST`를 반환한다.

## GET /api/v1/admin/dlq-events

DLQ로 이동한 실패 이벤트 목록을 조회한다.

Response:

```json
{
  "success": true,
  "data": [
    {
      "eventId": "EVT-550e8400-e29b-41d4-a716-446655440000",
      "jobId": "JOB-20260618-550e8400-e29b-41d4-a716-446655440000",
      "topicName": "data.process.dlq",
      "errorMessage": "Forced failure for test event",
      "retryCount": 3,
      "status": "DLQ",
      "createdAt": "2026-06-18T00:00:00Z",
      "updatedAt": "2026-06-18T00:00:05Z"
    }
  ],
  "message": "success",
  "timestamp": "2026-06-18T00:00:06Z"
}
```

curl:

```bash
curl http://localhost:8080/api/v1/admin/dlq-events
```

## POST /api/v1/admin/dlq-events/{eventId}/reprocess

DLQ 이벤트를 재처리 대상으로 되돌린다.

현재 구현은 `failed_events.status`를 재처리 상태로 변경하는 최소 API다. Kafka 재발행 기반의 완전한 reprocess workflow는 이후 개선 대상이다.

curl:

```bash
curl -X POST http://localhost:8080/api/v1/admin/dlq-events/{eventId}/reprocess
```

## Health / Metrics

```bash
curl http://localhost:8080/actuator/health
curl http://localhost:8080/actuator/health/readiness
curl http://localhost:8080/actuator/health/liveness
curl http://localhost:8080/actuator/prometheus
```
