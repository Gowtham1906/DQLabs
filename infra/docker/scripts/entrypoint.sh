#!/bin/bash
# =============================================================================
# DQLabs Airflow — Container Entrypoint
# =============================================================================
# Handles startup sequencing for all Airflow components:
#   webserver | scheduler | worker | triggerer | init | flower
#
# The component is selected by the CMD passed to the container,
# which maps to AIRFLOW_COMPONENT or the first argument to this script.
# =============================================================================

set -euo pipefail

AIRFLOW_HOME="${AIRFLOW_HOME:-/opt/airflow}"
POSTGRES_HOST="${POSTGRES_HOST:-postgres}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"

# -----------------------------------------------------------------------------
# Utility: wait for PostgreSQL to accept connections
# -----------------------------------------------------------------------------
wait_for_db() {
    echo "[entrypoint] Waiting for PostgreSQL at ${POSTGRES_HOST}:${POSTGRES_PORT} ..."
    local attempts=0
    local max_attempts=30

    until nc -z "${POSTGRES_HOST}" "${POSTGRES_PORT}" 2>/dev/null; do
        attempts=$((attempts + 1))
        if [ "${attempts}" -ge "${max_attempts}" ]; then
            echo "[entrypoint] ERROR: PostgreSQL not reachable after $((max_attempts * 2))s. Aborting."
            exit 1
        fi
        echo "[entrypoint]   Attempt ${attempts}/${max_attempts} — retrying in 2s ..."
        sleep 2
    done

    echo "[entrypoint] PostgreSQL is ready."
}

# -----------------------------------------------------------------------------
# Utility: wait for Redis broker to accept connections
# -----------------------------------------------------------------------------
wait_for_redis() {
    local redis_host="${REDIS_HOST:-redis}"
    local redis_port="${REDIS_PORT:-6379}"

    echo "[entrypoint] Waiting for Redis at ${redis_host}:${redis_port} ..."
    local attempts=0
    local max_attempts=30

    until nc -z "${redis_host}" "${redis_port}" 2>/dev/null; do
        attempts=$((attempts + 1))
        if [ "${attempts}" -ge "${max_attempts}" ]; then
            echo "[entrypoint] ERROR: Redis not reachable after $((max_attempts * 2))s. Aborting."
            exit 1
        fi
        echo "[entrypoint]   Attempt ${attempts}/${max_attempts} — retrying in 2s ..."
        sleep 2
    done

    echo "[entrypoint] Redis is ready."
}

# -----------------------------------------------------------------------------
# Component dispatch
# -----------------------------------------------------------------------------
COMPONENT="${1:-webserver}"

echo "[entrypoint] Starting component: ${COMPONENT}"

case "${COMPONENT}" in

    # ------------------------------------------------------------------
    # init — Run once to initialise the DB and create the admin user.
    # In Compose: depends_on ensures DB is healthy first.
    # In K8s: run as an initContainer or a Job before other pods start.
    # ------------------------------------------------------------------
    init)
        wait_for_db
        echo "[entrypoint] Running DB migrations ..."
        airflow db upgrade

        echo "[entrypoint] Creating admin user (skipped if already exists) ..."
        airflow users create \
            --username "${AIRFLOW_ADMIN_USER:-admin}" \
            --password "${AIRFLOW_ADMIN_PASSWORD:-admin}" \
            --firstname "DQLabs" \
            --lastname "Admin" \
            --role "Admin" \
            --email "${AIRFLOW_ADMIN_EMAIL:-admin@dqlabs.io}" \
            2>/dev/null || echo "[entrypoint] Admin user already exists — skipping."

        echo "[entrypoint] Airflow initialisation complete."
        ;;

    # ------------------------------------------------------------------
    webserver)
        wait_for_db
        exec airflow webserver
        ;;

    # ------------------------------------------------------------------
    scheduler)
        wait_for_db
        exec airflow scheduler
        ;;

    # ------------------------------------------------------------------
    worker)
        wait_for_db
        wait_for_redis
        exec airflow celery worker
        ;;

    # ------------------------------------------------------------------
    triggerer)
        wait_for_db
        exec airflow triggerer
        ;;

    # ------------------------------------------------------------------
    flower)
        wait_for_redis
        exec airflow celery flower
        ;;

    # ------------------------------------------------------------------
    # Pass-through: allows running arbitrary airflow commands or shells
    # e.g. docker run dqlabs-airflow:latest airflow dags list
    # ------------------------------------------------------------------
    *)
        exec "$@"
        ;;

esac