# Event-Driven Data Processing Platform

Kafka 기반 비동기 데이터 처리 플랫폼이다. Phase 1은 Docker Compose 환경에서 API, Worker, PostgreSQL, Redis, Kafka를 실행하고 `POST /api/v1/jobs` 요청이 Kafka 이벤트로 발행된 뒤 Worker가 consume하여 job 상태를 `COMPLETED`로 갱신하는 최소 기능을 제공한다.

## 아키텍처

```text
Client
  |
  v
data-api-service
  |  save job REQUESTED
  |  publish data.process.requested
  v
Kafka
  |
  v
data-worker-service
  |  consume event
  |  update job COMPLETED
  v
PostgreSQL

Redis: job progress cache
```

## 기술 스택

- Java 17
- Spring Boot 3
- Spring Web, Validation, Data JPA, Kafka, Redis, Actuator
- PostgreSQL
- Redis
- Kafka
- Docker Compose

## 로컬 실행

Docker가 필요하다.

```bash
docker compose up -d --build
```

또는 Makefile을 사용할 수 있다.

```bash
make up
```

서비스 확인:

```bash
docker compose ps
curl http://localhost:8080/actuator/health
curl http://localhost:8081/actuator/health
```

## 검증 curl

job 생성:

```bash
curl -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "requester": "test-user",
    "dataType": "CSV",
    "itemCount": 10000,
    "idempotencyKey": "demo-key-1",
    "payload": {
      "source": "mock",
      "description": "large data processing test"
    }
  }'
```

응답의 `data.jobId` 값을 사용해 상태를 조회한다.

```bash
curl http://localhost:8080/api/v1/jobs/{jobId}
```

Worker가 Kafka 이벤트를 처리한 뒤 `data.status`가 `COMPLETED`, `data.processedCount`가 요청한 `itemCount`와 같아야 한다.

로그 확인:

```bash
docker compose logs -f data-api-service data-worker-service
```

Kafka topic 확인:

```bash
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list
```

Redis progress 확인:

```bash
docker compose exec redis redis-cli GET job:{jobId}:progress
```

## API 명세

### POST /api/v1/jobs

요청:

```json
{
  "requester": "test-user",
  "dataType": "CSV",
  "itemCount": 10000,
  "idempotencyKey": "demo-key-1",
  "payload": {
    "source": "mock"
  }
}
```

응답:

```json
{
  "success": true,
  "data": {
    "jobId": "JOB-20260618-...",
    "status": "REQUESTED",
    "message": "Data processing job has been accepted."
  },
  "message": "success",
  "timestamp": "2026-06-18T00:00:00Z"
}
```

### GET /api/v1/jobs/{jobId}

job 상태와 처리 건수를 조회한다.

## Kafka topic

- `data.process.requested`: API가 발행하고 Worker가 consume한다.

Phase 1에서는 Kafka topic auto-create를 사용한다. Phase 3에서 Strimzi `KafkaTopic` 리소스로 관리한다.

## Retry/DLQ 전략

Phase 1의 범위는 정상 처리 경로다. 실패 이벤트 retry/DLQ, 중복 이벤트 방지는 Phase 4에서 `processed_events`, `failed_events`, retry topic, DLQ topic 기반으로 확장한다.

## 종료

```bash
docker compose down
```

볼륨까지 삭제하려면:

```bash
docker compose down -v
```
