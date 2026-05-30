# Dev-loop ergonomics for the local Docker stack. Optional convenience —
# every target maps to a one-liner you can run directly.

.PHONY: up down reset logs ps psql worker-build worker-shell worker-run worker

# Bring up backing services (Postgres + Temporal + Temporal UI) in the
# background. The `worker` service has a profile so it stays out unless
# explicitly requested — see `make worker` below.
up:
	docker compose up -d

# Stop services, keep volumes (Postgres data + Temporal history survive).
down:
	docker compose down

# Stop services AND drop volumes — fresh-state reset. Use this between
# experiments where you want Temporal/Postgres state nuked.
reset:
	docker compose down -v

# Tail logs from all running services.
logs:
	docker compose logs -f

# List the compose-managed containers and their states.
ps:
	docker compose ps

# Open a `psql` shell against the structural-DB database.
psql:
	docker compose exec postgres psql -U jarvis -d jarvis_structural

# Build the worker image without starting it. Exercises the multi-stage
# Dockerfile; useful in CI and before pushing to verify build health.
worker-build:
	docker compose --profile container-worker build worker

# Start the worker as a container against the rest of the stack. The
# default dev loop runs it natively (see README); use this only when
# verifying the production-shape path.
worker:
	docker compose --profile container-worker up worker
