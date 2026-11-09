# Separate database: the integration test truncates its tables, and the
# running mailq container must not pick up the test's messages.
TEST_DB ?= postgres://mailq:mailq@localhost:5432/mailq_test?sslmode=disable

.PHONY: up down logs test itest demo build

up: ## start postgres, mailpit, fakesmtp and mailq
	docker compose up -d --build

down: ## stop everything and drop the database
	docker compose down -v

logs:
	docker compose logs -f mailq fakesmtp

build:
	go build ./...

test: ## unit tests (no database needed)
	go test ./...

itest: ## unit + integration tests against the compose postgres
	docker compose exec -T postgres psql -U mailq -tc "SELECT 1 FROM pg_database WHERE datname='mailq_test'" | grep -q 1 \
		|| docker compose exec -T postgres createdb -U mailq mailq_test
	MAILQ_TEST_DATABASE_URL="$(TEST_DB)" go test -count=1 ./...

demo:
	./scripts/demo.sh
