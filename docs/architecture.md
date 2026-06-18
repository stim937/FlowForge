# FlowForge Architecture

FlowForge는 HTTP API에서 무거운 데이터 처리를 직접 수행하지 않고 Kafka 이벤트로 넘긴 뒤 Worker가 비동기로 처리하는 데이터 처리 플랫폼이다. 현재 구현은 로컬 Docker Compose와 Kubernetes 배포를 모두 지원하며, Kubernetes 환경에서는 Strimzi Kafka, KEDA, Prometheus, Grafana까지 포함한다.

## 목표

- API 서버는 job 접수와 상태 조회에 집중한다.
- Worker는 Kafka consumer group 기반으로 비동기 처리를 담당한다.
- PostgreSQL은 job 상태, 처리 이벤트, 실패 이벤트를 저장한다.
- Redis는 job progress 같은 임시 상태를 캐시한다.
- Kafka lag를 기준으로 Worker를 자동 확장한다.
- Prometheus/Grafana로 API, JVM, Worker 처리 결과, Kafka lag를 관측한다.

## 논리 아키텍처

```text
Client / k6
  |
  v
data-api-service
  | 1. validate request
  | 2. save jobs row as REQUESTED
  | 3. cache progress=0 in Redis
  | 4. publish DATA_PROCESS_REQUESTED after DB commit
  v
Kafka topic: data.process.requested
  |
  v
data-worker-service
  | 1. consume requested/retry event
  | 2. check processed_events by eventId
  | 3. mark PROCESSING
  | 4. simulate processing
  | 5. mark COMPLETED or RETRYING/DLQ
  v
PostgreSQL / Redis / Kafka retry or DLQ topic
```

## 런타임 구성

| 영역 | 로컬 Docker Compose | Kubernetes |
| --- | --- | --- |
| API | `data-api-service` container | `Deployment/data-api-service` |
| Worker | `data-worker-service` container | `Deployment/data-worker-service` |
| Kafka | Confluent Kafka KRaft container | Strimzi `Kafka`, `KafkaNodePool`, `KafkaTopic` |
| PostgreSQL | `postgres:16-alpine` | `StatefulSet/postgres` |
| Redis | `redis:7-alpine` | `Deployment/redis` |
| Monitoring | Prometheus/Grafana containers | Prometheus/Grafana Deployments |
| Autoscaling | 없음 | HPA for API, KEDA for Worker |

## Job 생성 흐름

```text
POST /api/v1/jobs
  |
  v
JobService.createJob()
  |
  |-- idempotencyKey가 있으면 기존 job 조회
  |-- jobs row 저장: REQUESTED
  |-- Redis progress 초기화: 0
  |-- transaction commit 이후 Kafka event 발행
  v
data.process.requested
```

API는 DB transaction이 commit된 뒤 Kafka 이벤트를 발행한다. 이 방식은 job row 없이 이벤트만 발행되는 상황을 줄인다. 완전한 원자성은 outbox pattern이 더 적합하지만, 현재 단계에서는 단순 transaction synchronization으로 시작한다.

## Worker 처리 흐름

```text
data.process.requested or data.process.retry
  |
  v
DataProcessRequestedListener.handle()
  |
  |-- processed_events 중복 확인
  |-- job PROCESSING 변경
  |-- Redis progress 50
  |-- 처리 성공: COMPLETED, processed_events 저장, progress 100
  |-- 처리 실패: retryCount 기준 retry 또는 DLQ
```

Worker는 같은 `eventId`가 다시 들어오면 `processed_events` 기준으로 중복 처리를 건너뛴다. 성공 처리 시 `flowforge.worker.jobs.completed` Micrometer counter를 증가시킨다.

## Retry / DLQ

처리 실패는 `forceFail: true` payload로 재현할 수 있다.

```text
retryCount < maxRetryCount
  -> job RETRYING
  -> data.process.retry 발행
  -> flowforge.worker.jobs.retried 증가

retryCount >= maxRetryCount
  -> job DLQ
  -> failed_events 저장
  -> data.process.dlq 발행
  -> flowforge.worker.jobs.dlq 증가
```

관리 API는 DLQ 이벤트 조회와 재처리를 제공한다.

```text
GET  /api/v1/admin/dlq-events
POST /api/v1/admin/dlq-events/{eventId}/reprocess
```

## 데이터 저장소

현재 구현의 주요 테이블은 다음 역할을 가진다.

| 테이블 | 역할 |
| --- | --- |
| `jobs` | job 상태, 요청자, 데이터 타입, 처리 수 저장 |
| `processed_events` | Kafka eventId 중복 처리 방지 |
| `failed_events` | DLQ 이벤트와 재처리 대상 관리 |

Redis key는 다음 형태를 사용한다.

```text
job:{jobId}:progress
```

## Kafka Topic

Kubernetes에서는 Strimzi `KafkaTopic` 리소스로 topic을 관리한다.

| Topic | Producer | Consumer | Partitions |
| --- | --- | --- | ---: |
| `data.process.requested` | API | Worker | 6 |
| `data.process.retry` | Worker | Worker | 6 |
| `data.process.completed` | Worker later | Notification later | 3 |
| `data.process.failed` | Worker later | Notification later | 3 |
| `data.process.dlq` | Worker | Admin API / 운영자 | 1 |
| `notification.requested` | Worker/API later | Notification later | 3 |

현재 Worker는 `data.process.requested`, `data.process.retry`를 consume하고, 실패 한도 초과 시 `data.process.dlq`를 publish한다.

## Kubernetes 배포

Kubernetes 리소스는 `infra/k8s`에 있다.

```text
Namespace: event-platform
Ingress host: flowforge.local
Kafka bootstrap: flowforge-kafka-kafka-bootstrap:9092
API replicas: 2
Worker replicas: 1
```

API와 Worker Deployment에는 readiness/liveness probe와 resource requests/limits가 설정되어 있다. PostgreSQL은 StatefulSet과 PVC를 사용하고, Redis/Prometheus/Grafana는 Deployment로 배포한다.

## Autoscaling

API는 Kubernetes HPA로 확장한다.

```text
minReplicas: 2
maxReplicas: 10
CPU averageUtilization: 60
Memory averageUtilization: 75
```

Worker는 KEDA Kafka scaler로 확장한다.

```text
topic: data.process.requested
consumerGroup: data-worker-group
lagThreshold: 100
minReplicaCount: 1
maxReplicaCount: 10
```

기대 흐름은 job 요청 증가, Kafka lag 증가, Worker replica 증가, lag 해소, replica 감소 순서다.

## Monitoring

Spring Boot Actuator와 Micrometer Prometheus registry를 사용한다.

Prometheus scrape 대상:

```text
data-api-service:8080/actuator/prometheus
data-worker-service:8081/actuator/prometheus
flowforge-kafka-kafka-exporter:9404
```

Grafana 기본 대시보드 `FlowForge Overview`는 다음 지표를 보여준다.

- HTTP request rate
- HTTP 평균 응답시간
- JVM memory
- Worker completed/retried/DLQ counter
- Kafka consumer lag

## Helm / GitOps 준비

Helm chart는 `infra/helm/platform`에 있다.

```text
values.yaml
values-local.yaml
values-dev.yaml
templates/platform.yaml
```

현재 chart는 플랫폼 전체를 한 번에 렌더링한다. 이후에는 API, Worker, platform dependency를 별도 chart로 분리하고 Argo CD Application을 추가하면 GitOps 흐름으로 확장할 수 있다.

## Load Test

k6 스크립트는 `load-test/k6`에 있다.

| 스크립트 | 목적 |
| --- | --- |
| `create-jobs.js` | 50 VU, 3분 기본 부하 |
| `spike-test.js` | 10 VU에서 300 VU까지 증가 |
| `soak-test.js` | 10,000건 job 생성 |

결과 기록은 `docs/load-test-result.md`에 남긴다.

## 현재 한계와 개선 방향

- DB 저장과 Kafka publish의 완전한 원자성은 아직 없다. 다음 단계에서 outbox pattern을 적용할 수 있다.
- DLQ 재처리는 기본 API만 제공하며, 운영 UI는 없다.
- Notification service는 topic과 구조만 준비되어 있고 아직 구현되지 않았다.
- Helm chart는 단일 platform chart다. 운영 수준에서는 service chart와 dependency chart로 분리하는 편이 낫다.
- Grafana dashboard는 최소 지표 중심이다. Kafka lag, HPA/KEDA replica 변화, DLQ 추이를 더 상세히 분리할 수 있다.
- 실제 k6 결과 수치는 환경별 실행 후 `docs/load-test-result.md`에 갱신해야 한다.
