# Event-Driven Data Processing Platform

## 1. 프로젝트 개요

이 프로젝트는 대용량 데이터 처리 요청을 HTTP API에서 직접 처리하지 않고, Kafka 기반 비동기 파이프라인으로 분산 처리하는 시스템이다.

목표는 다음과 같다.

* Kubernetes 환경에서 Spring Boot 기반 API/Worker 서비스를 배포한다.
* Kafka를 이용해 대량 요청을 이벤트 기반으로 분산 처리한다.
* Worker Pod를 수평 확장하여 처리량을 높인다.
* 실패 이벤트는 retry topic과 dead-letter topic으로 분리한다.
* PostgreSQL에는 작업 상태와 처리 결과를 저장한다.
* Redis에는 진행률과 임시 상태를 캐싱한다.
* Prometheus/Grafana를 통해 API 응답시간, Kafka lag, 처리량, 실패율을 관측한다.
* HPA/KEDA를 이용해 API 서버와 Worker를 자동 확장한다.
* k6를 이용해 부하 테스트를 수행하고 결과를 README에 기록한다.

이 프로젝트는 단순 CRUD가 아니라 다음 역량을 보여주는 것을 목표로 한다.

* Kubernetes 배포/운영 이해
* Kafka 기반 비동기 아키텍처 설계
* 대용량 요청 처리
* 장애 대응 및 재처리 설계
* 모니터링/메트릭 기반 운영
* Helm/GitOps 확장 가능 구조

---

## 2. 최종 아키텍처

```text
[Client / k6 Load Test]
        |
        v
[Ingress NGINX]
        |
        v
[data-api-service]
        |
        | 1. job 생성
        | 2. DB에 job 상태 저장
        | 3. Kafka event 발행
        v
[Kafka: data.process.requested]
        |
        v
[data-worker-service replicas N]
        |
        | 처리 성공
        v
[Kafka: data.process.completed]
        |
        | 처리 실패
        v
[Kafka: data.process.retry]
        |
        | 재시도 초과
        v
[Kafka: data.process.dlq]

[Storage]
- PostgreSQL: job, task, result, failure_event
- Redis: job progress, temporary status
- MinIO: optional file/object storage

[Ops]
- Kubernetes
- Helm
- Strimzi Kafka Operator
- KEDA
- Prometheus
- Grafana
- Loki optional
- Argo CD optional
```

---

## 3. 기술 스택

### Backend

* Java 17
* Spring Boot 3.x
* Spring Web
* Spring Validation
* Spring Data JPA 또는 MyBatis
* Spring Kafka
* Spring Boot Actuator
* Micrometer Prometheus Registry
* PostgreSQL
* Redis

### Infra

* Docker
* Kubernetes
* k3d 또는 kind
* Strimzi Kafka Operator
* Kafka UI
* Helm
* KEDA
* Prometheus
* Grafana
* k6

### Optional

* MinIO
* Loki
* Argo CD
* RabbitMQ
* Testcontainers

---

## 4. Repository 구조

```text
event-driven-data-platform/
  README.md
  AGENTS.md
  docker-compose.yml
  Makefile

  services/
    data-api-service/
      build.gradle
      Dockerfile
      src/main/java/...
      src/test/java/...

    data-worker-service/
      build.gradle
      Dockerfile
      src/main/java/...
      src/test/java/...

    notification-service/
      build.gradle
      Dockerfile
      src/main/java/...
      src/test/java/...

  libs/
    common-event/
    common-domain/

  infra/
    k8s/
      namespaces/
      configmaps/
      secrets/
      ingress/
      postgres/
      redis/
      kafka/
      keda/
      monitoring/

    helm/
      data-api-service/
      data-worker-service/
      notification-service/
      platform/

    argocd/
      applications/

  load-test/
    k6/
      create-jobs.js
      spike-test.js
      soak-test.js

  docs/
    architecture.md
    api-spec.md
    kafka-topics.md
    database-schema.md
    failure-retry-dlq.md
    load-test-result.md
    troubleshooting.md
```

---

## 5. 주요 서비스

## 5.1 data-api-service

역할:

* 데이터 처리 job 생성 API 제공
* job 상태 조회 API 제공
* Kafka로 처리 요청 이벤트 발행
* PostgreSQL에 job 상태 저장
* Redis에 job progress 캐싱
* 관리자용 실패 이벤트 재처리 API 제공

주요 API:

```http
POST /api/v1/jobs
GET /api/v1/jobs/{jobId}
GET /api/v1/jobs/{jobId}/progress
POST /api/v1/jobs/{jobId}/retry
GET /api/v1/admin/dlq-events
POST /api/v1/admin/dlq-events/{eventId}/reprocess
```

`POST /api/v1/jobs` 요청 예시:

```json
{
  "requester": "test-user",
  "dataType": "CSV",
  "itemCount": 10000,
  "payload": {
    "source": "mock",
    "description": "large data processing test"
  }
}
```

응답 예시:

```json
{
  "jobId": "JOB-20260618-000001",
  "status": "REQUESTED",
  "message": "Data processing job has been accepted."
}
```

중요 구현 원칙:

* API는 무거운 작업을 직접 처리하지 않는다.
* API는 job 접수 후 빠르게 응답한다.
* 실제 처리는 Kafka consumer worker가 담당한다.
* jobId는 모든 이벤트와 로그에 포함한다.
* 중복 요청 방지를 위해 idempotencyKey를 지원한다.

---

## 5.2 data-worker-service

역할:

* Kafka topic `data.process.requested` consume
* 데이터 처리 시뮬레이션
* 처리 성공 시 `data.process.completed` publish
* 처리 실패 시 retry topic 또는 DLQ publish
* PostgreSQL에 처리 결과 저장
* Redis에 진행률 업데이트
* consumer group 기반 병렬 처리

처리 흐름:

```text
1. data.process.requested 이벤트 수신
2. eventId 중복 처리 여부 확인
3. job 상태 PROCESSING 변경
4. itemCount 기준으로 처리 루프 수행
5. 진행률 Redis 업데이트
6. 성공 시 COMPLETED 처리
7. 실패 시 retryCount 확인
8. retryCount 초과 시 DLQ 이동
```

중요 구현 원칙:

* consumer 처리는 멱등성을 보장해야 한다.
* 같은 eventId가 중복 수신되어도 결과가 중복 저장되면 안 된다.
* Kafka offset commit 전략을 명확히 한다.
* DB 저장과 이벤트 발행 사이의 불일치를 줄이기 위해 outbox pattern을 고려한다.
* 첫 구현에서는 단순 트랜잭션 + 상태 체크로 시작하고, 이후 outbox pattern을 추가한다.

---

## 5.3 notification-service

역할:

* 처리 완료/실패 이벤트 consume
* 알림 로그 저장
* 추후 Telegram/Slack/Email 연동 가능

초기 구현에서는 실제 외부 발송 대신 DB 또는 로그로만 기록한다.

---

## 6. Kafka Topic 설계

| Topic                  | Producer            | Consumer             | 설명        |
| ---------------------- | ------------------- | -------------------- | --------- |
| data.process.requested | data-api-service    | data-worker-service  | 데이터 처리 요청 |
| data.process.completed | data-worker-service | notification-service | 처리 완료     |
| data.process.failed    | data-worker-service | notification-service | 처리 실패     |
| data.process.retry     | data-worker-service | data-worker-service  | 재시도 대상    |
| data.process.dlq       | data-worker-service | admin API            | 최종 실패 이벤트 |
| notification.requested | worker/api          | notification-service | 알림 요청     |

초기 partition 권장값:

```text
data.process.requested: 6
data.process.retry: 6
data.process.completed: 3
data.process.failed: 3
data.process.dlq: 1
notification.requested: 3
```

Consumer group:

```text
data-worker-group
notification-group
```

Kafka event 공통 필드:

```json
{
  "eventId": "EVT-uuid",
  "eventType": "DATA_PROCESS_REQUESTED",
  "jobId": "JOB-uuid",
  "idempotencyKey": "user-key-or-generated-key",
  "retryCount": 0,
  "occurredAt": "2026-06-18T00:00:00Z",
  "payload": {}
}
```

---

## 7. Database Schema 초안

### jobs

```sql
CREATE TABLE jobs (
  id BIGSERIAL PRIMARY KEY,
  job_id VARCHAR(100) NOT NULL UNIQUE,
  requester VARCHAR(100) NOT NULL,
  status VARCHAR(30) NOT NULL,
  data_type VARCHAR(30) NOT NULL,
  item_count INT NOT NULL,
  processed_count INT NOT NULL DEFAULT 0,
  failed_count INT NOT NULL DEFAULT 0,
  idempotency_key VARCHAR(200),
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
);
```

### job_results

```sql
CREATE TABLE job_results (
  id BIGSERIAL PRIMARY KEY,
  job_id VARCHAR(100) NOT NULL,
  result_type VARCHAR(30) NOT NULL,
  result_message TEXT,
  created_at TIMESTAMP NOT NULL
);
```

### processed_events

```sql
CREATE TABLE processed_events (
  id BIGSERIAL PRIMARY KEY,
  event_id VARCHAR(100) NOT NULL UNIQUE,
  job_id VARCHAR(100) NOT NULL,
  event_type VARCHAR(100) NOT NULL,
  processed_at TIMESTAMP NOT NULL
);
```

### failed_events

```sql
CREATE TABLE failed_events (
  id BIGSERIAL PRIMARY KEY,
  event_id VARCHAR(100) NOT NULL,
  job_id VARCHAR(100) NOT NULL,
  topic_name VARCHAR(200) NOT NULL,
  error_message TEXT,
  payload TEXT NOT NULL,
  retry_count INT NOT NULL,
  status VARCHAR(30) NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
);
```

---

## 8. Job Status

```text
REQUESTED
PROCESSING
COMPLETED
FAILED
PARTIAL_FAILED
RETRYING
DLQ
CANCELLED
```

상태 변경 규칙:

```text
REQUESTED -> PROCESSING
PROCESSING -> COMPLETED
PROCESSING -> FAILED
PROCESSING -> PARTIAL_FAILED
FAILED -> RETRYING
RETRYING -> PROCESSING
FAILED -> DLQ
DLQ -> RETRYING
```

---

## 9. Kubernetes 리소스 요구사항

각 서비스는 다음 리소스를 가져야 한다.

```text
Deployment
Service
ConfigMap
Secret
HorizontalPodAutoscaler
ServiceMonitor optional
Ingress only for API service
```

공통 요구사항:

* namespace는 `event-platform` 사용
* 모든 Deployment에 readinessProbe 추가
* 모든 Deployment에 livenessProbe 추가
* 모든 container에 resources.requests / resources.limits 설정
* Spring Boot Actuator endpoint 노출
* `/actuator/health/readiness`
* `/actuator/health/liveness`
* `/actuator/prometheus`

예시 리소스 기준:

```yaml
resources:
  requests:
    cpu: "250m"
    memory: "512Mi"
  limits:
    cpu: "1000m"
    memory: "1Gi"
```

---

## 10. Autoscaling 요구사항

### API Service

API Service는 HPA로 확장한다.

기준:

```text
minReplicas: 2
maxReplicas: 10
CPU averageUtilization: 60
Memory averageUtilization: 75
```

### Worker Service

Worker Service는 KEDA Kafka scaler로 확장한다.

기준:

```text
minReplicaCount: 1
maxReplicaCount: 10
Kafka lagThreshold: 100
topic: data.process.requested
consumerGroup: data-worker-group
```

기대 시나리오:

```text
1. 대량 job 생성
2. Kafka lag 증가
3. KEDA가 worker replica 증가
4. 처리량 증가
5. lag 감소
6. worker replica 감소
```

---

## 11. Monitoring 요구사항

Prometheus/Grafana에서 다음 지표를 확인할 수 있어야 한다.

### API

* HTTP request count
* HTTP request duration
* HTTP 4xx/5xx count
* JVM memory
* JVM GC
* DB connection pool usage

### Kafka/Worker

* Kafka consumer lag
* 처리 성공 수
* 처리 실패 수
* DLQ 이벤트 수
* retry 이벤트 수
* worker 처리 시간
* worker replica count

### Kubernetes

* Pod CPU usage
* Pod memory usage
* Pod restart count
* Deployment replica count
* HPA status
* KEDA ScaledObject status

---

## 12. Load Test 요구사항

k6 스크립트를 작성한다.

### 기본 부하 테스트

```text
동시 사용자: 50
기간: 3분
목표: 정상적으로 job 생성 가능
```

### 스파이크 테스트

```text
동시 사용자: 10 -> 300 급증
기간: 5분
목표: API HPA scale-out 확인
```

### 대량 메시지 테스트

```text
job 요청 수: 10,000건
itemCount: 1,000~10,000
목표: Kafka lag 증가 후 worker scale-out 확인
```

README에 기록할 지표:

```text
- 총 요청 수
- 성공률
- 평균 응답시간
- P95 응답시간
- 최대 Kafka lag
- lag 해소 시간
- worker replica 변화
- DLQ 발생 건수
- 재처리 성공 건수
```

---

## 13. 구현 단계

## Phase 1. Local Docker Compose

목표:

* Spring Boot API/Worker 기본 구현
* PostgreSQL, Redis, Kafka Compose 구성
* Kafka publish/consume 확인
* job 생성/조회 가능
* worker 처리 가능

완료 조건:

* `docker compose up`으로 전체 실행 가능
* `POST /api/v1/jobs` 호출 시 Kafka 이벤트 발행
* worker가 이벤트를 consume하고 job 상태를 COMPLETED로 변경
* 실패 이벤트를 DLQ로 이동 가능

---

## Phase 2. Kubernetes 기본 배포

목표:

* k3d 또는 kind 클러스터 구성
* API/Worker/PostgreSQL/Redis/Kafka 배포
* Ingress로 API 접근
* ConfigMap/Secret 분리

완료 조건:

* `kubectl get pods -n event-platform`에서 모든 Pod Running
* Ingress를 통해 API 호출 가능
* Kafka consumer 정상 동작
* readiness/liveness probe 적용

---

## Phase 3. Kafka on Kubernetes

목표:

* Strimzi로 Kafka 구성
* KafkaTopic 리소스로 topic 관리
* Kafka UI 배포 optional

완료 조건:

* Strimzi Kafka cluster Running
* KafkaTopic으로 topic 생성
* API/Worker가 내부 Kafka bootstrap service로 통신

---

## Phase 4. Retry / DLQ / Idempotency

목표:

* retryCount 기반 재시도 구현
* 최대 재시도 초과 시 DLQ 이동
* processed_events 테이블 기반 중복 이벤트 방지
* 관리자 재처리 API 구현

완료 조건:

* 강제 실패 이벤트가 retry topic으로 이동
* retry 초과 이벤트가 DLQ로 이동
* DLQ 이벤트를 관리자 API로 재처리 가능
* 같은 eventId 중복 consume 시 중복 처리되지 않음

---

## Phase 5. Autoscaling

목표:

* API HPA 적용
* Worker KEDA 적용
* k6 부하 테스트로 scale-out 확인

완료 조건:

* API 부하 증가 시 API Pod replica 증가
* Kafka lag 증가 시 Worker Pod replica 증가
* 부하 감소 후 replica 감소
* Grafana에서 replica 변화 확인

---

## Phase 6. Monitoring

목표:

* Prometheus/Grafana 구성
* Spring Actuator Prometheus metric 수집
* Kafka/Worker 지표 대시보드 구성

완료 조건:

* Grafana dashboard에서 API latency 확인
* Kafka lag 확인
* worker 처리량 확인
* DLQ count 확인

---

## Phase 7. Helm / GitOps

목표:

* 각 서비스를 Helm chart로 패키징
* values-local.yaml / values-dev.yaml 분리
* Argo CD optional 구성

완료 조건:

* Helm으로 전체 서비스 설치 가능
* 설정값을 values로 분리
* README에 배포 명령 정리

---

## 14. Coding Agent 작업 규칙

이 프로젝트를 구현하는 AI Coding Agent는 다음 규칙을 따른다.

1. 한 번에 전체 프로젝트를 완성하려고 하지 말고 Phase 단위로 구현한다.
2. 각 Phase마다 실행 가능한 상태를 만든다.
3. 구현 후 반드시 실행 방법과 검증 방법을 README에 갱신한다.
4. 임의로 기술 스택을 바꾸지 않는다.
5. 불필요하게 복잡한 패턴을 먼저 넣지 않는다.
6. 초기 구현은 단순하게 만들고 이후 개선한다.
7. Kafka event schema는 공통 모듈로 분리한다.
8. 모든 API 응답은 일관된 response format을 사용한다.
9. 모든 로그에는 jobId, eventId를 포함한다.
10. 실패 처리, 재시도, DLQ는 반드시 테스트 가능해야 한다.
11. Kubernetes manifest는 infra/k8s 아래에 둔다.
12. Helm chart는 infra/helm 아래에 둔다.
13. 부하 테스트 결과는 docs/load-test-result.md에 기록한다.
14. 실제 비밀번호/API Key는 커밋하지 않는다.
15. Secret은 샘플 파일만 제공한다.

---

## 15. 우선 구현할 최소 기능

가장 먼저 다음 순서로 구현한다.

```text
1. Gradle multi-module 또는 단일 repo 구조 생성
2. data-api-service 생성
3. data-worker-service 생성
4. PostgreSQL 연동
5. Kafka 연동
6. Redis 연동
7. Dockerfile 작성
8. docker-compose.yml 작성
9. job 생성 API 구현
10. Kafka publish 구현
11. Worker consume 구현
12. job 상태 업데이트 구현
13. 실패 시 DLQ publish 구현
14. README 실행 방법 작성
```

---

## 16. Initial API Response Format

성공 응답:

```json
{
  "success": true,
  "data": {},
  "message": "success",
  "timestamp": "2026-06-18T00:00:00Z"
}
```

실패 응답:

```json
{
  "success": false,
  "error": {
    "code": "INVALID_REQUEST",
    "message": "Invalid request."
  },
  "timestamp": "2026-06-18T00:00:00Z"
}
```

---

## 17. 테스트 요구사항

### Unit Test

* Job 생성 로직
* Event 생성 로직
* Retry count 판단
* DLQ 이동 판단
* Idempotency 체크

### Integration Test

* API 호출 후 DB 저장 확인
* API 호출 후 Kafka event 발행 확인
* Worker consume 후 job 상태 변경 확인
* 실패 이벤트 DLQ 이동 확인

### E2E Test

* Docker Compose 환경에서 job 생성부터 완료까지 확인
* Kubernetes 환경에서 Ingress API 호출부터 worker 처리까지 확인

---

## 18. README에 반드시 포함할 내용

README는 다음 항목을 포함해야 한다.

```text
1. 프로젝트 소개
2. 아키텍처 다이어그램
3. 기술 스택
4. 로컬 실행 방법
5. Docker Compose 실행 방법
6. Kubernetes 배포 방법
7. Kafka topic 설명
8. API 명세
9. Retry/DLQ 전략
10. Autoscaling 전략
11. Monitoring 대시보드
12. Load test 방법
13. Load test 결과
14. 장애 주입 테스트 결과
15. 트러블슈팅
16. 향후 개선 사항
```

---

## 19. Makefile 명령 예시

```makefile
up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

build:
	./gradlew clean build

test:
	./gradlew test

k3d-create:
	k3d cluster create event-platform --agents 2 -p "8080:80@loadbalancer"

k3d-delete:
	k3d cluster delete event-platform

k8s-apply:
	kubectl apply -f infra/k8s/

k8s-delete:
	kubectl delete -f infra/k8s/

load-test:
	k6 run load-test/k6/create-jobs.js
```

---

## 20. 최종 Definition of Done

이 프로젝트는 다음 조건을 만족하면 1차 완료로 본다.

* 로컬 Docker Compose로 전체 시스템 실행 가능
* Kubernetes k3d 환경에서 전체 시스템 실행 가능
* API를 통해 대량 job 생성 가능
* Kafka를 통해 worker가 비동기 처리 가능
* retry/DLQ 처리 가능
* 중복 이벤트 방지 가능
* API HPA scale-out 확인 가능
* Worker KEDA scale-out 확인 가능
* Prometheus/Grafana에서 주요 지표 확인 가능
* k6 부하 테스트 결과 문서화
* README만 보고 제3자가 실행 가능
* 포트폴리오 설명용 architecture.md 작성 완료
