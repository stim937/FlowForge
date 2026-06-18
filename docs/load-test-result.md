# Load Test Result

이 문서는 k6 부하 테스트 실행 결과를 기록한다. 현재 저장소에는 실행 가능한 스크립트와 측정 항목 템플릿을 제공하며, 실제 수치는 테스트 환경에서 실행한 뒤 갱신한다.

## 실행 환경

| 항목 | 값 |
| --- | --- |
| 실행일 | 미측정 |
| 대상 환경 | Docker Compose 또는 k3d |
| API URL | 미측정 |
| Worker replicas | 미측정 |
| Kafka | Docker Compose Kafka 또는 Strimzi Kafka |
| Prometheus/Grafana | 미측정 |

## 기본 부하 테스트

명령:

```bash
make load-test
```

목표:

```text
동시 사용자: 50
기간: 3분
정상적으로 job 생성 가능
```

| 지표 | 결과 |
| --- | --- |
| 총 요청 수 | 미측정 |
| 성공률 | 미측정 |
| 평균 응답시간 | 미측정 |
| P95 응답시간 | 미측정 |
| HTTP 실패율 | 미측정 |
| 최대 Kafka lag | 미측정 |
| DLQ 발생 건수 | 미측정 |

## 스파이크 테스트

명령:

```bash
make load-test-spike
```

목표:

```text
동시 사용자: 10 -> 300 급증
기간: 5분
API HPA scale-out 확인
```

| 지표 | 결과 |
| --- | --- |
| 총 요청 수 | 미측정 |
| 성공률 | 미측정 |
| 평균 응답시간 | 미측정 |
| P95 응답시간 | 미측정 |
| API replica 변화 | 미측정 |
| 최대 Kafka lag | 미측정 |
| DLQ 발생 건수 | 미측정 |

## 대량 메시지 테스트

명령:

```bash
make load-test-soak
```

목표:

```text
job 요청 수: 10,000건
itemCount: 1,000~10,000
Kafka lag 증가 후 Worker KEDA scale-out 확인
```

| 지표 | 결과 |
| --- | --- |
| 총 요청 수 | 미측정 |
| 성공률 | 미측정 |
| 평균 응답시간 | 미측정 |
| P95 응답시간 | 미측정 |
| 최대 Kafka lag | 미측정 |
| lag 해소 시간 | 미측정 |
| worker replica 변화 | 미측정 |
| DLQ 발생 건수 | 미측정 |
| 재처리 성공 건수 | 미측정 |

## 측정 명령

Prometheus:

```bash
curl "http://localhost:9090/api/v1/query?query=kafka_consumergroup_lag"
curl "http://localhost:9090/api/v1/query?query=flowforge_worker_jobs_completed_total"
curl "http://localhost:9090/api/v1/query?query=flowforge_worker_jobs_dlq_total"
```

Kubernetes:

```bash
kubectl get hpa data-api-service -n event-platform --watch
kubectl get deploy data-api-service data-worker-service -n event-platform --watch
kubectl describe scaledobject data-worker-service -n event-platform
```

## 결과 해석 기준

- 기본 부하 테스트는 HTTP 실패율 1% 미만을 목표로 한다.
- 스파이크 테스트는 API replica 증가 여부를 확인한다.
- 대량 메시지 테스트는 Kafka lag 증가, Worker replica 증가, lag 해소 순서를 확인한다.
- DLQ는 장애 주입 테스트가 아닌 일반 부하 테스트에서는 0건을 목표로 한다.
