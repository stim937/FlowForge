# Troubleshooting

FlowForge 실행 중 자주 만나는 문제와 확인 명령을 정리한다.

## Docker Compose

### 서비스 상태 확인

```bash
docker compose ps
docker compose logs -f data-api-service data-worker-service
```

### API health가 내려오지 않을 때

확인:

```bash
curl http://localhost:8080/actuator/health
docker compose logs data-api-service
```

주요 원인:

- PostgreSQL healthcheck 실패
- Kafka healthcheck 실패
- Redis 연결 실패
- 이전 volume에 남은 데이터와 schema 불일치

초기화:

```bash
docker compose down -v
docker compose up -d --build
```

### Worker가 job을 처리하지 않을 때

Kafka topic 확인:

```bash
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list
```

Worker log 확인:

```bash
docker compose logs -f data-worker-service
```

job 상태 확인:

```bash
curl http://localhost:8080/api/v1/jobs/{jobId}
```

Redis progress 확인:

```bash
docker compose exec redis redis-cli GET job:{jobId}:progress
```

## Kubernetes

### Pod 상태 확인

```bash
kubectl get pods -n event-platform
kubectl describe pod -n event-platform <pod-name>
kubectl logs -n event-platform deploy/data-api-service
kubectl logs -n event-platform deploy/data-worker-service
```

### Ingress 호출이 실패할 때

현재 k3d 명령은 load balancer 80을 host 8080에 매핑한다.

```bash
curl -H "Host: flowforge.local" http://localhost:8080/actuator/health
```

확인:

```bash
kubectl get ingress -n event-platform
kubectl describe ingress data-api-service -n event-platform
```

Ingress controller가 없으면 별도로 설치해야 한다.

### 로컬 이미지 Pull 실패

k3d에 로컬 이미지를 import한다.

```bash
docker compose build data-api-service data-worker-service
make k3d-import-images
kubectl rollout restart deploy/data-api-service deploy/data-worker-service -n event-platform
```

### Strimzi Kafka가 Ready가 아닐 때

확인:

```bash
kubectl get kafka,kafkanodepool,kafkatopic -n event-platform
kubectl get pods -n event-platform | findstr kafka
kubectl logs deploy/strimzi-cluster-operator -n event-platform
```

CRD/operator가 없으면 먼저 설치한다.

```bash
make strimzi-install
```

### KEDA ScaledObject가 동작하지 않을 때

확인:

```bash
kubectl get scaledobject -n event-platform
kubectl describe scaledobject data-worker-service -n event-platform
kubectl get pods -n keda
```

KEDA가 설치되어 있지 않으면:

```bash
make keda-install
```

Kafka bootstrap 주소가 Strimzi service와 맞는지도 확인한다.

```bash
kubectl get svc flowforge-kafka-kafka-bootstrap -n event-platform
```

### HPA metric이 unknown일 때

metrics-server 설치 여부를 확인한다.

```bash
kubectl get apiservice v1beta1.metrics.k8s.io
kubectl top pods -n event-platform
```

설치:

```bash
make metrics-install
```

## Monitoring

### Prometheus target 확인

Port-forward:

```bash
make k8s-prometheus-forward
```

Query:

```bash
curl "http://localhost:9090/api/v1/query?query=up"
```

### Grafana 접근

```bash
make k8s-grafana-forward
```

접속:

```text
http://localhost:3000
admin / admin
```

### Worker 지표가 안 보일 때

먼저 실제 job을 생성한다.

```bash
curl -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "requester": "test-user",
    "dataType": "CSV",
    "itemCount": 1000,
    "idempotencyKey": "metric-demo-key",
    "payload": {
      "source": "mock"
    }
  }'
```

Prometheus query:

```bash
curl "http://localhost:9090/api/v1/query?query=flowforge_worker_jobs_completed_total"
```

## Helm

현재 로컬 작업 환경에 Helm CLI가 없으면 `make helm-template`은 실패한다. Helm 설치 환경에서 다음 명령으로 검증한다.

```bash
make helm-template
make helm-install
```

Strimzi/KEDA CRD가 먼저 설치되어 있어야 한다.

```bash
make strimzi-install
make keda-install
```

## k6

k6가 설치되어 있지 않으면 load test 명령은 실패한다.

```bash
k6 version
```

로컬 기본 부하 테스트:

```bash
make load-test
```

Kubernetes Ingress 대상:

```bash
make load-test-k8s
```

## 자주 쓰는 초기화 명령

Docker Compose:

```bash
docker compose down -v
docker compose up -d --build
```

k3d:

```bash
make k8s-delete
make k3d-delete
make k3d-create
```
