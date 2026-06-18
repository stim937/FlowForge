# Event-Driven Data Processing Platform

Kafka 기반 비동기 데이터 처리 플랫폼이다. 현재는 Docker Compose 기반 API/Worker/Kafka/PostgreSQL/Redis/Prometheus/Grafana 실행, retry/DLQ/idempotency 처리, Kubernetes 배포, Strimzi Kafka, autoscaling/monitoring manifest까지 제공한다.

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
Prometheus/Grafana: API, JVM, worker outcome metrics
```

상세 설계 문서는 `docs/architecture.md`에 있다.

## 문서

- `docs/architecture.md`: 전체 아키텍처와 운영 흐름
- `docs/api-spec.md`: API 요청/응답 명세
- `docs/kafka-topics.md`: Kafka topic과 event schema
- `docs/database-schema.md`: DB schema와 Redis key
- `docs/failure-retry-dlq.md`: retry/DLQ 처리 전략
- `docs/load-test-result.md`: k6 결과 기록
- `docs/troubleshooting.md`: 실행/운영 문제 해결

## 기술 스택

- Java 17
- Spring Boot 3
- Spring Web, Validation, Data JPA, Kafka, Redis, Actuator
- PostgreSQL
- Redis
- Kafka
- Docker Compose
- Kubernetes
- Strimzi Kafka Operator
- KEDA
- Prometheus
- Grafana
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
curl http://localhost:8080/actuator/prometheus
curl http://localhost:8081/actuator/prometheus
```

로컬 모니터링:

- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000
- Grafana 계정: `admin` / `admin`
- 기본 대시보드: `FlowForge Overview`

## Kubernetes 배포

Kubernetes 배포 manifest는 `infra/k8s` 아래에 있다. 현재 범위에는 기본 배포, Strimzi Kafka, autoscaling, monitoring 리소스가 포함된다.

사전 준비:

- Docker
- kubectl
- k3d
- Helm

클러스터 생성:

```bash
make k3d-create
```

HPA/KEDA 준비:

```bash
make metrics-install
make keda-install
```

Strimzi Operator 설치:

```bash
make strimzi-install
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
kubectl get kafka,kafkanodepool,kafkatopic -n event-platform
kubectl get hpa -n event-platform
kubectl get scaledobject -n event-platform
kubectl get svc prometheus grafana -n event-platform
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

Prometheus/Grafana 접근:

```bash
make k8s-prometheus-forward
make k8s-grafana-forward
```

각 명령은 터미널을 점유하므로 별도 터미널에서 실행한다.

- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000
- Grafana 계정: `admin` / `admin`

## Autoscaling 전략

API는 Kubernetes HPA로 확장한다.

```text
minReplicas: 2
maxReplicas: 10
CPU averageUtilization: 60
Memory averageUtilization: 75
```

Worker는 KEDA Kafka scaler로 확장한다.

```text
minReplicaCount: 1
maxReplicaCount: 10
topic: data.process.requested
consumerGroup: data-worker-group
lagThreshold: 100
```

상태 확인:

```bash
kubectl describe hpa data-api-service -n event-platform
kubectl describe scaledobject data-worker-service -n event-platform
kubectl get deploy data-api-service data-worker-service -n event-platform
```

대량 job 생성으로 Kafka lag를 만들면 KEDA가 `data-worker-service` replica를 늘린다. 부하가 줄고 lag가 해소되면 cooldown 이후 replica가 감소한다.

## Strimzi Kafka

Kubernetes 환경의 Kafka는 Strimzi Operator가 관리한다.

```text
Kafka cluster: flowforge-kafka
Bootstrap service: flowforge-kafka-kafka-bootstrap:9092
Node pool: dual-role
Mode: KRaft
Storage: ephemeral
```

Kafka topic은 `KafkaTopic` 리소스로 관리한다.

| Topic | Partitions | Replicas |
| --- | ---: | ---: |
| `data.process.requested` | 6 | 1 |
| `data.process.retry` | 6 | 1 |
| `data.process.completed` | 3 | 1 |
| `data.process.failed` | 3 | 1 |
| `data.process.dlq` | 1 | 1 |
| `notification.requested` | 3 | 1 |

상태 확인:

```bash
make kafka-status
kubectl get svc flowforge-kafka-kafka-bootstrap -n event-platform
kubectl logs deploy/strimzi-cluster-operator -n event-platform
```

## Helm 배포

Helm chart는 `infra/helm/platform` 아래에 있다. 초기 chart는 전체 플랫폼을 한 번에 배포하는 `flowforge-platform` chart이며, 환경별 values를 분리한다.

```text
infra/helm/platform/values.yaml
infra/helm/platform/values-local.yaml
infra/helm/platform/values-dev.yaml
```

렌더링 확인:

```bash
make helm-template
```

설치:

```bash
make strimzi-install
make keda-install
make helm-install
```

삭제:

```bash
make helm-delete
```

Helm chart는 Strimzi CRD와 KEDA CRD가 이미 설치되어 있다는 전제로 `Kafka`, `KafkaNodePool`, `KafkaTopic`, `ScaledObject` 리소스를 렌더링한다.

dev values로 렌더링하거나 설치할 때는 변수로 namespace와 values 파일을 바꾼다.

```bash
make helm-template HELM_VALUES=infra/helm/platform/values-dev.yaml
make strimzi-install STRIMZI_NAMESPACE=event-platform-dev
make helm-install HELM_NAMESPACE=event-platform-dev HELM_VALUES=infra/helm/platform/values-dev.yaml
```

## Monitoring 대시보드

Prometheus는 다음 endpoint를 scrape한다.

```text
data-api-service:8080/actuator/prometheus
data-worker-service:8081/actuator/prometheus
flowforge-kafka-kafka-exporter:9404
```

Grafana 기본 대시보드 `FlowForge Overview`는 다음 지표를 보여준다.

- HTTP request count/rate
- HTTP 평균 응답시간
- JVM memory 사용량
- Worker job 처리 결과
  - `flowforge_worker_jobs_completed_total`
  - `flowforge_worker_jobs_retried_total`
  - `flowforge_worker_jobs_dlq_total`
- Kafka consumer lag
  - `kafka_consumergroup_lag`

Prometheus에서 직접 확인:

```bash
curl "http://localhost:9090/api/v1/query?query=up"
curl "http://localhost:9090/api/v1/query?query=flowforge_worker_jobs_completed_total"
curl "http://localhost:9090/api/v1/query?query=kafka_consumergroup_lag"
```

## Load test 방법

k6 설치 후 로컬 Docker Compose 환경에서 실행한다.

```bash
make load-test
make load-test-spike
make load-test-soak
```

Kubernetes Ingress를 대상으로 실행할 때는 Host 헤더를 지정한다.

```bash
make load-test-k8s
```

직접 환경변수를 지정할 수도 있다.

```bash
BASE_URL=http://localhost:8080 HOST_HEADER=flowforge.local k6 run load-test/k6/create-jobs.js
```

스크립트:

- `load-test/k6/create-jobs.js`: 50 VU, 3분 기본 부하 테스트
- `load-test/k6/spike-test.js`: 10 VU에서 300 VU까지 증가하는 5분 스파이크 테스트
- `load-test/k6/soak-test.js`: 10,000건 job 생성 대량 메시지 테스트

## Load test 결과

결과 기록 문서는 `docs/load-test-result.md`에 둔다. 실제 환경에서 실행한 뒤 다음 값을 갱신한다.

- 총 요청 수
- 성공률
- 평균 응답시간
- P95 응답시간
- 최대 Kafka lag
- lag 해소 시간
- worker replica 변화
- DLQ 발생 건수
- 재처리 성공 건수

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

Docker Compose 환경에서는 Kafka topic auto-create를 사용한다. Kubernetes 환경에서는 Strimzi `KafkaTopic` 리소스로 관리한다.

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
