#!/usr/bin/env bash
# =============================================================================
# DQLabs Airflow – Shared Entrypoint Script
# =============================================================================
# PURPOSE : Runs pre-flight checks and initialization before handing off
#           control to the Airflow component command.
#
# BEHAVIOR PER COMPONENT:
#   scheduler   → waits for DB → runs db migrate → seeds Variables → exec
#   webserver   → waits for DB → waits for db migrate → exec
#   triggerer   → waits for DB → exec
#   worker      → waits for DB → exec
#
# ENVIRONMENT VARIABLES (injected via K8s Secrets / ConfigMaps):
#   AIRFLOW__CORE__SQL_ALCHEMY_CONN   — required for DB readiness check
#   AIRFLOW_VARIABLES_FILE            — optional path to variables JSON
#                                       (defaults to /opt/airflow/config/airflow-variables.json)
# =============================================================================

set -euo pipefail

AIRFLOW_VARIABLES_FILE="${AIRFLOW_VARIABLES_FILE:-/opt/airflow/config/airflow-variables.json}"
DB_CHECK_RETRIES="${DB_CHECK_RETRIES:-30}"
DB_CHECK_INTERVAL="${DB_CHECK_INTERVAL:-5}"
MIGRATE_LOCK_RETRIES="${MIGRATE_LOCK_RETRIES:-60}"
MIGRATE_LOCK_INTERVAL="${MIGRATE_LOCK_INTERVAL:-5}"

# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------
log()  { echo "[entrypoint] $(date -u +%Y-%m-%dT%H:%M:%SZ) INFO  $*"; }
warn() { echo "[entrypoint] $(date -u +%Y-%m-%dT%H:%M:%SZ) WARN  $*"; }
err()  { echo "[entrypoint] $(date -u +%Y-%m-%dT%H:%M:%SZ) ERROR $*" >&2; }

# ---------------------------------------------------------------------------
# wait_for_db: Poll until Airflow can connect to the metadata database.
# Relies on AIRFLOW__CORE__SQL_ALCHEMY_CONN being set.
# ---------------------------------------------------------------------------
wait_for_db() {
    log "Waiting for database to be ready..."
    local retries=0
    until airflow db check >/dev/null 2>&1; do
        retries=$((retries + 1))
        if [ "${retries}" -ge "${DB_CHECK_RETRIES}" ]; then
            err "Database not ready after ${DB_CHECK_RETRIES} attempts. Exiting."
            exit 1
        fi
        warn "Database not ready (attempt ${retries}/${DB_CHECK_RETRIES}). Retrying in ${DB_CHECK_INTERVAL}s..."
        sleep "${DB_CHECK_INTERVAL}"
    done
    log "Database is ready."
}

# ---------------------------------------------------------------------------
# run_db_migrate: Run Airflow database migrations (scheduler only).
# Idempotent — safe to run on every scheduler startup.
# ---------------------------------------------------------------------------
run_db_migrate() {
    log "Running Airflow database migrations..."
    airflow db migrate
    log "Database migration complete."
}

# ---------------------------------------------------------------------------
# wait_for_migrations: Used by non-scheduler components to wait until the
# scheduler has completed its db migrate before accepting traffic.
# ---------------------------------------------------------------------------
wait_for_migrations() {
    log "Waiting for database migrations to complete..."
    local retries=0
    until airflow db check-migrations --migration-wait-timeout 5 >/dev/null 2>&1; do
        retries=$((retries + 1))
        if [ "${retries}" -ge "${MIGRATE_LOCK_RETRIES}" ]; then
            err "Migrations not complete after ${MIGRATE_LOCK_RETRIES} attempts. Exiting."
            exit 1
        fi
        warn "Migrations pending (attempt ${retries}/${MIGRATE_LOCK_RETRIES}). Retrying in ${MIGRATE_LOCK_INTERVAL}s..."
        sleep "${MIGRATE_LOCK_INTERVAL}"
    done
    log "Migrations confirmed complete."
}

# ---------------------------------------------------------------------------
# seed_variables: Import Airflow Variables from a JSON file (scheduler only).
# The JSON file is provided via a Kubernetes ConfigMap mounted at:
#   /opt/airflow/config/airflow-variables.json
# Format: {"key": "value", ...}
# ---------------------------------------------------------------------------
seed_variables() {
    if [ -f "${AIRFLOW_VARIABLES_FILE}" ]; then
        log "Seeding Airflow Variables from ${AIRFLOW_VARIABLES_FILE}..."
        airflow variables import "${AIRFLOW_VARIABLES_FILE}"
        log "Airflow Variables seeded successfully."
    else
        warn "Variables file not found at ${AIRFLOW_VARIABLES_FILE}. Skipping variable seeding."
        warn "Ensure the airflow-variables ConfigMap is mounted if the DQLabsPlugin requires it."
    fi
}

# ---------------------------------------------------------------------------
# Main entrypoint logic
# Detect which Airflow component is being started from the CMD arguments.
# ---------------------------------------------------------------------------
COMPONENT="${2:-unknown}"  # $1 is "airflow", $2 is the subcommand

case "${COMPONENT}" in

    scheduler)
        log "Starting Airflow Scheduler..."
        wait_for_db
        run_db_migrate
        seed_variables
        ;;

    webserver)
        log "Starting Airflow Webserver..."
        wait_for_db
        wait_for_migrations
        ;;

    triggerer)
        log "Starting Airflow Triggerer..."
        wait_for_db
        wait_for_migrations
        ;;

    worker | celery)
        # For "airflow celery worker", $2 is "celery" and $3 is "worker"
        log "Starting Airflow Celery Worker..."
        wait_for_db
        wait_for_migrations
        ;;

    *)
        log "Starting component: ${COMPONENT}"
        wait_for_db
        ;;
esac

log "Launching: $*"
exec "$@"
