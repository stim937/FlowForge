HELM_NAMESPACE ?= event-platform
HELM_VALUES ?= infra/helm/platform/values-local.yaml
STRIMZI_NAMESPACE ?= event-platform

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f

build:
	docker run --rm -v "$$(pwd):/workspace" -w /workspace gradle:8.10.2-jdk17 gradle clean build --no-daemon

test:
	docker run --rm -v "$$(pwd):/workspace" -w /workspace gradle:8.10.2-jdk17 gradle test --no-daemon

k3d-create:
	k3d cluster create event-platform --agents 2 -p "8080:80@loadbalancer"

k3d-delete:
	k3d cluster delete event-platform

k3d-import-images:
	k3d image import flowforge-data-api-service:latest flowforge-data-worker-service:latest -c event-platform

strimzi-install:
	kubectl create namespace $(STRIMZI_NAMESPACE) --dry-run=client -o yaml | kubectl apply -f -
	kubectl apply -f "https://strimzi.io/install/latest?namespace=$(STRIMZI_NAMESPACE)" -n $(STRIMZI_NAMESPACE)
	kubectl rollout status deployment/strimzi-cluster-operator -n $(STRIMZI_NAMESPACE) --timeout=300s

k8s-apply:
	kubectl apply -k infra/k8s/

k8s-delete:
	kubectl delete -k infra/k8s/

metrics-install:
	kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml

keda-install:
	helm repo add kedacore https://kedacore.github.io/charts
	helm repo update
	helm upgrade --install keda kedacore/keda --namespace keda --create-namespace

k8s-prometheus-forward:
	kubectl port-forward svc/prometheus 9090:9090 -n event-platform

k8s-grafana-forward:
	kubectl port-forward svc/grafana 3000:3000 -n event-platform

kafka-status:
	kubectl get kafka,kafkanodepool,kafkatopic -n event-platform

helm-template:
	helm template flowforge-platform infra/helm/platform -f $(HELM_VALUES)

helm-install:
	helm upgrade --install flowforge-platform infra/helm/platform -f $(HELM_VALUES) --namespace $(HELM_NAMESPACE) --create-namespace

helm-delete:
	helm uninstall flowforge-platform --namespace $(HELM_NAMESPACE)

load-test:
	k6 run load-test/k6/create-jobs.js

load-test-spike:
	k6 run load-test/k6/spike-test.js

load-test-soak:
	k6 run load-test/k6/soak-test.js

load-test-k8s:
	HOST_HEADER=flowforge.local BASE_URL=http://localhost:8080 k6 run load-test/k6/create-jobs.js
