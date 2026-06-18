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
	kubectl apply -f infra/k8s/

k8s-delete:
	kubectl delete -f infra/k8s/
