# Event-Driven Data Processing Platform

Kafka 기반 비동기 데이터 처리 플랫폼이다. 현재는 Docker Compose 기반 API/Worker/Kafka/PostgreSQL/Redis 실행, retry/DLQ/idempotency 처리, Kubernetes 기본 배포 manifest까지 제공한다.

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
- Kubernetes
- k3d 또는 kind

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

## Kubernetes 배포

Phase 2 범위의 기본 Kubernetes 배포 manifest는 `infra/k8s` 아래에 있다. Strimzi/KEDA/Monitoring은 다음 단계에서 별도로 추가한다.

사전 준비:

- Docker
- kubectl
- k3d

클러스터 생성:

```bash
make k3d-create
```

이미지 빌드:

```bash
docker compose build data-api-service data-worker-service
```

k3d 클러스터에 로컬 이미지 import:

```bash
make k3d-import-images
```

Kubernetes 리소스 적용:

```bash
make k8s-apply
```

Pod 상태 확인:

```bash
kubectl get pods -n event-platform
kubectl get ingress -n event-platform
```

모든 Pod가 `Running` 또는 `Ready`가 된 뒤 API health 확인:

```bash
curl -H "Host: flowforge.local" http://localhost:8080/actuator/health
```

Kubernetes 환경 job 생성:

```bash
curl -X POST http://localhost:8080/api/v1/jobs \
  -H "Host: flowforge.local" \
  -H "Content-Type: application/json" \
  -d '{
    "requester": "test-user",
    "dataType": "CSV",
    "itemCount": 10000,
    "idempotencyKey": "k8s-demo-key-1",
    "payload": {
      "source": "mock"
    }
  }'
```

상태 조회:

```bash
curl -H "Host: flowforge.local" http://localhost:8080/api/v1/jobs/{jobId}
```

리소스 삭제:

```bash
make k8s-delete
make k3d-delete
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
- `data.process.retry`: Worker가 재시도 대상 이벤트를 발행하고 다시 consume한다.
- `data.process.dlq`: 재시도 한도를 초과한 이벤트가 이동한다.

Phase 1에서는 Kafka topic auto-create를 사용한다. Phase 3에서 Strimzi `KafkaTopic` 리소스로 관리한다.

## Retry/DLQ 전략

Worker는 이벤트 처리 전에 `processed_events` 테이블에서 `eventId` 중복 여부를 확인한다. 이미 처리된 이벤트는 다시 실행하지 않는다.

실패 이벤트는 다음 규칙으로 처리한다.

```text
retryCount < maxRetryCount: job 상태 RETRYING, data.process.retry 발행
retryCount >= maxRetryCount: job 상태 DLQ, failed_events 저장, data.process.dlq 발행
```

로컬 기본값:

```text
APP_WORKER_MAX_RETRY_COUNT=3
```

장애 주입은 요청 payload에 `forceFail: true`를 넣어서 확인한다.

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

잠시 후 job 상태를 조회하면 재시도 한도 초과 뒤 `DLQ`가 된다.

```bash
curl http://localhost:8080/api/v1/jobs/{jobId}
```

DLQ 이벤트 조회:

```bash
curl http://localhost:8080/api/v1/admin/dlq-events
```

DLQ 이벤트 재처리:

```bash
curl -X POST http://localhost:8080/api/v1/admin/dlq-events/{eventId}/reprocess
```

## 종료

```bash
docker compose down
```

볼륨까지 삭제하려면:

```bash
docker compose down -v
```
