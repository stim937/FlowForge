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
