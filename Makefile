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
