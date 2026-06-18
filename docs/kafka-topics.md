# Kafka Topics

FlowForge는 Kafka topic을 통해 API와 Worker를 분리한다. Docker Compose 환경에서는 Kafka auto-create를 사용하고, Kubernetes 환경에서는 Strimzi `KafkaTopic` 리소스로 topic을 관리한다.

## Bootstrap Servers

| Environment | Bootstrap servers |
| --- | --- |
| Docker Compose internal | `kafka:9092` |
| Docker host access | `localhost:29092` |
| Kubernetes Strimzi | `flowforge-kafka-kafka-bootstrap:9092` |

## Topic 목록

| Topic | Producer | Consumer | Partitions | Status |
| --- | --- | --- | ---: | --- |
| `data.process.requested` | data-api-service | data-worker-service | 6 | implemented |
| `data.process.retry` | data-worker-service | data-worker-service | 6 | implemented |
| `data.process.completed` | data-worker-service | notification-service later | 3 | reserved |
| `data.process.failed` | data-worker-service | notification-service later | 3 | reserved |
| `data.process.dlq` | data-worker-service | admin/operator | 1 | implemented |
| `notification.requested` | worker/api later | notification-service later | 3 | reserved |

## Event Schema

현재 공통 이벤트 record는 `libs/common-event`에 있다.

```json
{
  "eventId": "EVT-550e8400-e29b-41d4-a716-446655440000",
  "eventType": "DATA_PROCESS_REQUESTED",
  "jobId": "JOB-20260618-550e8400-e29b-41d4-a716-446655440000",
  "idempotencyKey": "demo-key-1",
  "retryCount": 0,
  "occurredAt": "2026-06-18T00:00:00Z",
  "payload": {
    "requester": "test-user",
    "dataType": "CSV",
    "itemCount": 10000,
    "payload": {
      "source": "mock"
    }
  }
}
```

## Publish / Consume Flow

```text
POST /api/v1/jobs
  -> data.process.requested
  -> worker consumes
  -> success: jobs.status COMPLETED
  -> failure below retry limit: data.process.retry
  -> failure at retry limit: data.process.dlq
```

Worker consumer group:

```text
data-worker-group
```

## Strimzi 관리

Kubernetes topic은 `infra/k8s/12-kafka.yaml`의 `KafkaTopic` 리소스로 정의한다.

확인 명령:

```bash
kubectl get kafka,kafkanodepool,kafkatopic -n event-platform
kubectl get svc flowforge-kafka-kafka-bootstrap -n event-platform
```

Kafka exporter scrape target:

```text
flowforge-kafka-kafka-exporter:9404
```

대표 Prometheus query:

```text
kafka_consumergroup_lag
```

## 운영 메모

- `data.process.requested`와 `data.process.retry`는 Worker 병렬 처리를 위해 partition을 6개로 둔다.
- `data.process.dlq`는 운영자가 순서대로 확인하기 쉽도록 partition을 1개로 둔다.
- 현재 구현은 local phase에서 단순성을 위해 exactly-once 처리를 사용하지 않는다.
- 중복 이벤트 방지는 `processed_events.event_id` unique constraint와 Worker 체크로 처리한다.
