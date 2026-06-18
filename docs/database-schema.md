# Database Schema

현재 구현은 Spring Data JPA `ddl-auto: update`로 schema를 생성한다. 운영 환경에서는 Flyway 또는 Liquibase migration으로 고정하는 것이 다음 개선 대상이다.

## jobs

API와 Worker가 함께 사용하는 job 상태 테이블이다. API service와 Worker service가 같은 테이블을 각자의 JPA entity로 매핑한다.

```sql
CREATE TABLE jobs (
  id BIGSERIAL PRIMARY KEY,
  job_id VARCHAR(100) NOT NULL UNIQUE,
  requester VARCHAR(100) NOT NULL,
  status VARCHAR(30) NOT NULL,
  data_type VARCHAR(30) NOT NULL,
  item_count INT NOT NULL,
  processed_count INT NOT NULL,
  failed_count INT NOT NULL,
  idempotency_key VARCHAR(200),
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
);
```

Status:

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

현재 구현에서 실제로 사용하는 상태:

```text
REQUESTED -> PROCESSING -> COMPLETED
PROCESSING -> RETRYING
PROCESSING -> DLQ
```

## processed_events

Kafka event 중복 처리를 방지한다.

```sql
CREATE TABLE processed_events (
  id BIGSERIAL PRIMARY KEY,
  event_id VARCHAR(100) NOT NULL UNIQUE,
  job_id VARCHAR(100) NOT NULL,
  event_type VARCHAR(100) NOT NULL,
  processed_at TIMESTAMP NOT NULL
);
```

Worker는 이벤트 처리 시작 전 `event_id` 존재 여부를 확인한다. 이미 처리된 이벤트면 job 상태를 다시 변경하지 않는다.

## failed_events

DLQ로 이동한 이벤트와 재처리 대상 이벤트를 저장한다.

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

현재 status:

```text
DLQ
```

관리 API의 reprocess 호출은 이 status를 재처리 상태로 바꾸는 확장 지점이다.

## Redis Keys

job progress 캐시:

```text
job:{jobId}:progress
```

값:

```text
0   job accepted
50  processing
100 completed
0   DLQ/failure path
```

## Idempotency

`jobs.idempotency_key`는 동일 요청 중복 생성을 막기 위해 사용한다.

현재 entity에는 unique constraint가 명시되어 있지 않다. 단일 API instance에서는 repository 조회로 중복 생성을 줄일 수 있지만, 다중 instance 경쟁 상황까지 막으려면 DB unique constraint 추가가 필요하다.

권장 migration:

```sql
CREATE UNIQUE INDEX ux_jobs_idempotency_key
ON jobs (idempotency_key)
WHERE idempotency_key IS NOT NULL;
```

## 다음 개선

- Flyway/Liquibase migration 도입
- `job_results` 테이블 추가
- `failed_events.event_id` unique 또는 eventId+status 기준 index 추가
- outbox table 추가
- idempotency key partial unique index 추가
