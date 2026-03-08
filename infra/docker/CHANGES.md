# DQLabs Airflow – Docker Modernization Change Log

> All changes made to get the local Docker stack fully working (all 6 services healthy).
> Format: file path → line number(s) → old content → new content → reason.

---

## 1. `infra/docker/Dockerfile`

### Change 1 — Added Layer 3.5 (new lines after line 143)

```diff
  # Layer 3: Core providers + DQLabs packages
  RUN pip install --no-cache-dir \
      --constraint "${CONSTRAINTS_URL}" \
      -r /tmp/requirements/requirements-core.txt

+ # Layer 3.5: Re-pin typing_extensions ≥ 4.10.0 (constraint file pins 4.9.0 but
+ # ML packages installed in Layer 2 require TypeIs which was added in 4.10.0).
+ # Airflow 2.8.1 works fine with newer typing_extensions.
+ RUN pip install --no-cache-dir "typing_extensions>=4.10.0,<5.0.0"

  # Layer 4: Connector wheels (private, must be pre-placed in vendor/wheels/)
```

**Why:** The Airflow 2.8.1 constraints file pins `typing_extensions==4.9.0`.
Layer 2 (torch / langchain / ML stack, installed without constraints) installs
`typing_extensions>=4.10.0` because it needs `TypeIs` (added in 4.10.0).
Layer 3's `--constraint` then **downgrades** it back to 4.9.0, causing a
`cannot import name 'TypeIs'` crash at runtime. This layer restores the
newer version after the constrained install.

---

## 2. `infra/docker/requirements/requirements-core.txt`

### Change 1 — Removed `pkutils==3.0.2` (was after line ~125, exact line varies)

```diff
- pkutils==3.0.2
```

**Why:** `pkutils 3.0.2` declares `semver<3.0.0` as a dependency, but the
Airflow 2.8.1 constraints file pins `semver==3.0.2`. Unresolvable conflict —
pip aborts. `pkutils` is not a direct DQLabs dependency; removing it is safe.

---

### Change 2 — Removed build tools (were after setuptools group)

```diff
- setuptools==78.1.1
- setuptools-git==1.2
- wheel==0.38.1
```

**Why:** `gcloud-aio-auth` (transitive dependency of
`apache-airflow-providers-google`) requires `setuptools<67.0.0`. Pinning
`setuptools==78.1.1` directly caused a hard conflict. These are build-time
tools already present in the base image — no runtime value.

---

### Change 3 — `typing_extensions` pinned to constraint version (line 128)

```diff
- typing_extensions>=4.10.0
+ typing_extensions==4.9.0
```

**Why:** Set to match the constraints file so Layer 3 pip install completes
without conflict. Dockerfile Layer 3.5 then upgrades it back to `>=4.10.0`
after the constrained install finishes.

---

## 3. `infra/docker/docker-compose.yml`

### Change 1 — Webserver timeout (lines 57–58, inside `x-airflow-common` environment)

```diff
  AIRFLOW__WEBSERVER__RBAC: "true"
+ AIRFLOW__WEBSERVER__WEB_SERVER_WORKER_TIMEOUT: "300"
+ AIRFLOW__WEBSERVER__WEB_SERVER_MASTER_TIMEOUT: "300"
```

**Why:** On first boot, Flask-AppBuilder (FAB) initialises all RBAC permissions
for 18 DAG categories × N organisations. This takes >120 seconds. The default
gunicorn master timeout is 120s — it killed the webserver before the UI was
ready, logging `No response from gunicorn master within 120 seconds`.

---

### Change 2 — DAG import timeout (line 66, inside `x-airflow-common` environment)

```diff
+ # Increase import timeout — ML packages (torch/langchain) load slowly
+ AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT: "300"
```

**Why:** Each DAG file that imports torch or langchain takes 30–60s to load.
The default `DAGBAG_IMPORT_TIMEOUT` is 30s, causing
`AirflowTaskTimeout: DagBag import timeout after 30.0s` for every DAG file.

---

### Change 3 — `DUMB_INIT_SETSID` moved to common env (lines 85–86)

```diff
  DB_CHECK_RETRIES: "30"
  DB_CHECK_INTERVAL: "5"

+ # ── Celery worker – dumb-init process group (harmless on non-worker services) ──
+ DUMB_INIT_SETSID: "0"
```

**Why:** The worker service previously had its own `environment:` block
containing only `DUMB_INIT_SETSID: "0"`. In YAML, a local `environment:`
key completely **overrides** the merged `*airflow-common` environment — it
does NOT merge deeply. This stripped all common env vars from the worker
(including `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`), causing the worker
entrypoint to connect to an empty database and loop forever on
`"Migrations pending"`. Moving this variable into the common block and
removing the per-service override fixes it.

---

### Change 4 — Removed worker's standalone `environment:` block (line ~231)

```diff
  airflow-worker:
    <<: *airflow-common
    container_name: dqlabs-airflow-worker
    restart: unless-stopped
    command: ["airflow", "celery", "worker"]
-   environment:
-     DUMB_INIT_SETSID: "0"
    healthcheck:
```

**Why:** See Change 3 above. Variable moved to common block.

---

### Change 5 — `airflow-init` skips DAG scanning (lines 157–158)

```diff
  airflow-init:
    <<: *airflow-common
    container_name: dqlabs-airflow-init
+   environment:
+     AIRFLOW__CORE__DAGS_FOLDER: /tmp/empty_dags
    entrypoint: /bin/bash
```

**Why:** `airflow db migrate` scans the DAGs folder before running. With 30+
DAG files each importing torch/langchain, this caused `airflow db migrate` to
take 10+ minutes (and sometimes fail with import timeouts). Pointing init at
an empty folder makes it complete in ~30 seconds.

---

### Change 6 — Health check timeouts increased for scheduler / worker / triggerer

#### airflow-scheduler (lines 217, 219)

```diff
  healthcheck:
    test: ['CMD-SHELL', 'airflow jobs check --job-type SchedulerJob ...']
    interval: 30s
-   timeout: 10s
+   timeout: 60s
    retries: 5
-   start_period: 30s
+   start_period: 90s
```

#### airflow-worker (lines 238, 240)

```diff
  healthcheck:
    test: ['CMD-SHELL', 'celery ... inspect ping ...']
    interval: 30s
-   timeout: 10s
+   timeout: 60s
    retries: 5
-   start_period: 30s
+   start_period: 90s
```

#### airflow-triggerer (lines 261, 263)

```diff
  healthcheck:
    test: ['CMD-SHELL', 'airflow jobs check --job-type TriggererJob ...']
    interval: 30s
-   timeout: 10s
+   timeout: 60s
    retries: 5
-   start_period: 30s
+   start_period: 90s
```

**Why:** The health probe commands (`airflow jobs check`, `celery inspect ping`)
each spawn a new Python process which must import Airflow + DAG dependencies.
With the ML stack this takes >10s, so every probe logged
`Health check exceeded timeout (10s)` and the services stayed permanently
`unhealthy`. `timeout: 60s` gives the probe enough time; `start_period: 90s`
prevents false failures during initial startup.

---

## 4. New files — `dqlabs_agent` stub package

**Folder:** `infra/airflow/dags/dqlabs_agent/`
(volume-mounted into the container at `/opt/airflow/dags/dqlabs_agent/`)

The real `dqlabs_agent` is a private connector wheel deployed only on MWAA.
All DAG files import it, so without it every DAG fails on import. The stubs
return safe no-op responses so the DAG files parse without errors locally.

| File | What it stubs |
|------|---------------|
| `__init__.py` | `DQAgent` class (`is_enabled`, `is_vault_enabled`, `execute`, `clear_connection`, `get_vault_secret`) |
| `services/__init__.py` | Empty package init |
| `services/agent_service.py` | `DQAgentService` class (`execute`, `clear_connection`, `get_vault_data`) |
| `app_helper/__init__.py` | `fetchone(cursor)` utility function |
| `app_helper/agent_helper.py` | `AgentHelpers` class (`get_agent_config`, `get_vault_config`) |

---

## 5. `infra/docker/.env` — created (not committed)

Created via Python (OneDrive blocks direct file write for dotfiles).
Contains local-only secrets: Fernet key, webserver secret key, Postgres
password, admin credentials, and DQLabs app env vars.
Template lives at `infra/docker/env.example`.

---

## Final Stack Status

| Service | Result |
|---------|--------|
| `dqlabs-postgres` | healthy |
| `dqlabs-redis` | healthy |
| `dqlabs-airflow-webserver` | healthy — UI at http://localhost:8080 |
| `dqlabs-airflow-scheduler` | healthy |
| `dqlabs-airflow-worker` | healthy |
| `dqlabs-airflow-triggerer` | healthy |
