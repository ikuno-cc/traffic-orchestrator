#!/bin/sh
# entrypoint_worker.sh
# 1. Queries Postgres for all enabled services to build the explicit -Q queue list.
# 2. Sums their worker_count to set --concurrency, overriding Celery's CPU-core default.
# This prevents tasks stalling due to queue-subscription races or concurrency caps.

set -e

# Always include the default queue so tasks dispatched before services exist aren't lost.
QUEUES="celery"

# Celery defaults --concurrency to CPU count; we need it to match the total
# configured worker_count across all services.  Floor at 1.
CONCURRENCY="${WORKER_CONCURRENCY:-}"   # allow hard override via env

if [ -n "$DATABASE_URL" ]; then
    # Run a single Python snippet that returns two space-separated lines:
    #   line 1: space-separated service IDs
    #   line 2: total worker_count sum
    PY_OUTPUT=$(python3 - <<'PYEOF'
import os, sys
try:
    import psycopg
    from psycopg.rows import dict_row
    url = os.environ.get("DATABASE_URL", "").strip().strip('"').strip("'")
    url = url.replace("postgres://", "postgresql://", 1)
    schema = os.environ.get("PG_SCHEMA", "public")
    table  = os.environ.get("SERVICES_TABLE", "orch_services")
    with psycopg.connect(url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f'SELECT id, worker_count FROM "{schema}"."{table}" WHERE enabled = true'
            )
            rows = cur.fetchall()
    ids   = [str(r["id"]) for r in rows if r.get("id")]
    total = sum(max(1, int(r.get("worker_count") or 1)) for r in rows)
    print(" ".join(ids))   # line 1
    print(str(total))      # line 2
except Exception as exc:
    print(f"[QUEUE-INIT] Could not query services: {exc}", file=sys.stderr)
    print("")   # line 1 empty  → no service queues added
    print("4")  # line 2 default
PYEOF
    )

    SVC_IDS=$(echo "$PY_OUTPUT"  | sed -n '1p')
    DB_CONCURRENCY=$(echo "$PY_OUTPUT" | sed -n '2p')

    for sid in $SVC_IDS; do
        QUEUES="$QUEUES,svc.$sid"
    done

    # Use DB value unless operator already set WORKER_CONCURRENCY env var.
    if [ -z "$CONCURRENCY" ]; then
        CONCURRENCY="$DB_CONCURRENCY"
    fi
fi

# Final safety floor
CONCURRENCY="${CONCURRENCY:-4}"
if [ "$CONCURRENCY" -lt 1 ] 2>/dev/null; then
    CONCURRENCY=1
fi

echo "[QUEUE-INIT] Starting worker | queues=$QUEUES | concurrency=$CONCURRENCY"

exec celery \
    -A workers.celery_app:celery_app \
    worker \
    --loglevel=info \
    --concurrency="$CONCURRENCY" \
    -Q "$QUEUES"
