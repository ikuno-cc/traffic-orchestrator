#!/bin/sh
# entrypoint_worker.sh
# Builds an explicit Celery queue list from all enabled services in Postgres,
# then starts the worker subscribed to every queue from boot — no add_consumer race.

set -e

# Always include the default queue so tasks dispatched before services exist aren't lost.
QUEUES="celery"

# Try to fetch service IDs from Postgres. Failures are non-fatal — we fall back to
# the default queue and let add_consumer() handle the rest at runtime.
if [ -n "$DATABASE_URL" ]; then
    # Normalise postgres:// -> postgresql://
    PG_URL=$(echo "$DATABASE_URL" | sed 's|^postgres://|postgresql://|')
    SCHEMA="${PG_SCHEMA:-public}"
    TABLE="${SERVICES_TABLE:-orch_services}"

    SVC_IDS=$(python3 - <<'PYEOF'
import os, sys
try:
    import psycopg
    from psycopg.rows import dict_row
    url = os.environ.get("DATABASE_URL", "").strip().strip('"').strip("'")
    url = url.replace("postgres://", "postgresql://", 1)
    schema = os.environ.get("PG_SCHEMA", "public")
    table = os.environ.get("SERVICES_TABLE", "orch_services")
    with psycopg.connect(url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f'SELECT id FROM "{schema}"."{table}" WHERE enabled = true'
            )
            rows = cur.fetchall()
    ids = [str(r["id"]) for r in rows if r.get("id")]
    print(" ".join(ids))
except Exception as exc:
    print(f"[QUEUE-INIT] Could not query services: {exc}", file=sys.stderr)
    print("")
PYEOF
    )

    for sid in $SVC_IDS; do
        QUEUES="$QUEUES,svc.$sid"
    done
fi

echo "[QUEUE-INIT] Starting worker consuming queues: $QUEUES"

exec celery \
    -A workers.celery_app:celery_app \
    worker \
    --loglevel=info \
    -Q "$QUEUES"
