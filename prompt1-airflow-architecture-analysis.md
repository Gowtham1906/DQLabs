# Prompt 1 — Airflow Codebase Understanding & Scope Alignment

---

## 1. Codebase Architecture Summary

### Overall Architecture Pattern

DQLabs-Airflow is a **multi-tenant, dynamic DAG platform** built on top of Apache Airflow 2.8.1. It does not contain pre-authored static DAGs. Instead, a single factory module reads organization/tenant configuration from a PostgreSQL backend at runtime and generates DAGs programmatically — one DAG per tenant per task category.

```
GitHub (source of truth)
        │
        ▼
dqlabs wheel (Python package)
        │
        ├──► dq_dags.py                ← Airflow DAG entry point (loaded by scheduler)
        │         │
        │         ▼
        │    DAG Factory               ← Queries PostgreSQL, builds DAG objects per org
        │         │
        │         ▼
        │    Task Factory              ← Routes to 18 task category handlers
        │         │
        │         ▼
        │    Task Implementations      ← 45+ modules: profile, reliability, semantic, lineage...
        │
        ├──► dqlabs_airflow_plugin.py  ← Airflow Listener (metadata push to DQLabs API)
        │
        └──► Custom Operators/Callbacks ← BaseDQLabsOperator, CircuitBreaker, 6 callbacks
```

### Repository Layout

```
infra/airflow/
├── dags/
│   ├── dq_dags.py                  ← Scheduler entry point
│   ├── requirements.txt            ← 60+ Python packages
│   ├── setup.py                    ← Builds dqlabs-3.0-py3-none-any.whl
│   └── dqlabs/                     ← Core Python package
│       ├── dags/                   ← DAG factory
│       ├── tasks/                  ← 45+ task implementations
│       ├── app_helper/             ← 18 shared helper modules
│       ├── drivers/                ← Query, schema, asset, Iceberg, dedup
│       ├── models/                 ← ML/statistical models
│       ├── enums/                  ← Shared type definitions
│       ├── app_constants/          ← Task categories, constants
│       ├── services/               ← Storage abstraction (S3, local)
│       ├── dq_package/
│       │   ├── operators/          ← BaseDQLabsOperator, CircuitBreakerOperator
│       │   └── callbacks/          ← 6 lifecycle callbacks
│       └── file_crypto.py          ← .env encryption/decryption
├── plugins/
│   └── dqlabs_airflow_plugin.py    ← Airflow Listener plugin
├── jars/                           ← 7 JDBC driver binaries
├── scripts/
│   └── mwaa_startup.sh             ← System-level setup (MWAA only)
├── airflow.cfg                     ← Airflow configuration (dev-tuned)
├── airflow-eks.cfg                 ← EKS-specific overrides
└── tests/                          ← Test suite
```

---

## 2. Responsibilities of Airflow

| Component | Location | Responsibility |
|-----------|----------|----------------|
| **DAG Entry Point** | `dags/dq_dags.py` | Loaded by the Airflow scheduler; delegates to the DAG factory |
| **DAG Factory** | `dqlabs/dags/dq_dags.py` | Queries PostgreSQL for tenants/orgs; instantiates one DAG per org per category at runtime |
| **Task Factory** | `dqlabs/tasks/__init__.py` | Routes each task category enum to the correct task module |
| **Task Implementations** | `dqlabs/tasks/` (45+ files) | Core DQ logic per measure type: profile, statistics, behavioral, semantic, lineage, export, catalog, notification, metadata |
| **App Helpers** | `dqlabs/app_helper/` | Shared utilities: connection management, DAG defaults, lineage tracking, catalog sync, crypto, reporting, S3 Select |
| **Drivers** | `dqlabs/drivers/` | Low-level execution layer: query execution (Spark), schema discovery, asset extraction, Apache Iceberg, deduplication |
| **ML Models** | `dqlabs/models/` | Statistical/ML anomaly detection: univariate, behavioral, threshold, semantic |
| **Enums / Constants** | `dqlabs/enums/`, `dqlabs/app_constants/` | Shared type definitions: connection types (31+), task categories (18), schedule types, trigger types |
| **Services** | `dqlabs/services/` | Storage abstraction: S3 and local filesystem |
| **Plugin (Listener)** | `plugins/dqlabs_airflow_plugin.py` | Captures every DAG run event; pushes metadata and lineage to the DQLabs API |
| **Custom Operators** | `dqlabs/dq_package/operators/` | `BaseDQLabsOperator`, `DQLabsCircuitBreakerOperator` |
| **Callbacks** | `dqlabs/dq_package/callbacks/` | DAG/task lifecycle hooks: success, failure, retry, SLA miss, execute |
| **Crypto** | `dqlabs/file_crypto.py` | Encrypts `.env` → `.env.enc` for deployment; decrypts at startup |

### Airflow Execution Model

- **Executor:** CeleryExecutor
- **Message Broker:** Redis
- **Metadata Database:** PostgreSQL 14
- **Components deployed:** Webserver, Scheduler, Worker(s), Triggerer, Flower (Celery UI)
- **DAG distribution:** dqlabs wheel installed via `plugins.zip`; DAGs loaded from the `dags/` folder

### Supported Task Categories (18 types)

| Category Group | Categories |
|---------------|-----------|
| Core DQ | `SEMANTICS`, `RELIABILITY`, `PROFILE`, `STATISTICS`, `BEHAVIORAL`, `CUSTOM` |
| Data Management | `EXPORT_FAILED_ROWS`, `CATALOG_UPDATE`, `CATALOG_SCHEDULE`, `METADATA`, `SYNCASSET` |
| Operational | `NOTIFICATION`, `USERACTIVITY`, `USAGE_QUERY` |
| Workflow | `WORKFLOW`, `PROCESS`, `EXTRACT`, `OBSERVE` |

### Supported Data Sources (31+ connection types)

| Category | Sources |
|----------|---------|
| Relational | PostgreSQL, MySQL, Oracle, MSSQL, Synapse, Teradata, SAP HANA, AlloyDB, DB2 |
| Cloud Warehouses | Snowflake, Redshift, Redshift Spectrum, BigQuery, Databricks, Athena |
| Cloud Storage | AWS S3, S3 Select, Azure Data Lake (ADLS), Azure Blob Storage |
| Data Integration | Fivetran, Airbyte, dbt, Talend, Coalesce |
| BI / Catalog | Tableau, Power BI, Sigma, Atlan, Purview |
| Other | Denodo, MongoDB, Hive, Spark, Salesforce, File |

---

## 3. Custom Hooks and Plugins Identified

### Custom Operators

| Operator | Description |
|----------|-------------|
| `BaseDQLabsOperator` | Abstract base for all DQLabs task operators. Establishes shared context, DQLabs client authentication, and common execution patterns. |
| `DQLabsCircuitBreakerOperator` | Conditional execution gate. Evaluates asset/measure/threshold conditions at runtime. Skips all downstream tasks if the circuit breaker condition is not satisfied. Validates: `asset_id`, `measure_id`, `connection`, `condition`, `threshold`. |

### Custom Callbacks (6)

All callbacks communicate back to the DQLabs API to report execution state in real time:

| Callback | Trigger |
|----------|---------|
| `dq_dag_success_callback` | DAG run completed successfully |
| `dq_dag_failure_callback` | DAG run failed |
| `dq_task_success_callback` | Individual task succeeded |
| `dq_task_failure_callback` | Individual task failed |
| `dq_task_retry_callback` | Task is being retried |
| `dq_task_execute_callback` | Task execution started |
| `dq_sla_miss_callback` | SLA window missed |

### Custom Plugin — `DQLabsAirflowPlugin`

This is an **Airflow Listener** (not a traditional operator plugin) registered via the Airflow plugin system. It:

- Intercepts every DAG run start, success, and failure event system-wide
- Extracts task instances, execution times, DAG source code (configurable), and lineage metadata
- Pushes all captured data to the DQLabs control-plane API via authenticated HTTP
- Supports DAG allowlist/denylist filtering via glob patterns
- Handles sub-DAG traversal and detailed task metadata capture

**Plugin is configured entirely through Airflow Variables — no hardcoding — making it portable across environments.**

| Airflow Variable | Purpose |
|-----------------|---------|
| `dq_endpoint_url` | DQLabs API base URL |
| `dq_client_id` | Client authentication ID |
| `dq_client_secret` | Client authentication secret |
| `dq_connection_name` | Logical connection name in DQLabs |
| `dq_allowed_dags` | Comma-separated glob patterns for DAGs to include |
| `dq_excluded_dags` | Comma-separated glob patterns for DAGs to exclude |
| `dq_extract_source_code` | Boolean — whether to include DAG Python source in metadata |

### No Custom Hooks

There are no custom Airflow Hook subclasses. Database connectivity is handled directly inside helper modules (`connection_helper.py`) using `SQLAlchemy`, `pyodbc`, and `jaydebeapi` (JDBC), bypassing the Airflow Connections UI for DQLabs-managed data sources. Airflow Connections are used only for Airflow's own metadata database.

---

## 4. Dependencies and Runtime Requirements

### Layer 1 — Operating System & System Packages

| Dependency | Version | Purpose |
|------------|---------|---------|
| `msodbcsql17` | 17.x | Microsoft SQL Server ODBC driver |
| `unixODBC` / `unixODBC-dev` | Latest | ODBC framework required by pyodbc |
| Oracle Instant Client | 21.7.0 | Oracle database connectivity |
| OpenJDK | 17 | Required for all JDBC-based connectors |
| Apache Ant | Latest | Java build tooling required by some JDBC drivers |
| Google Chrome + Chromedriver | Latest stable | Selenium-based report rendering (PDF export) |
| `libpq-dev` | Latest | PostgreSQL C libraries for psycopg2 |
| `curl`, `gnupg2`, `apt-transport-https` | — | Package management tooling |

### Layer 2 — JDBC Driver JARs (Binary Assets)

These are static binary files checked into `infra/airflow/jars/`. They are not available via PyPI and must be managed separately:

| JAR File | Target Database |
|----------|----------------|
| `DatabricksJDBC42.jar` | Databricks |
| `db2jcc4.jar` | IBM DB2 |
| `denodo-vdp-jdbcdriver.jar` | Denodo VDP |
| `jt400-20.0.7.jar` | IBM i / AS400 |
| `ngdbc-2.16.11.jar` | SAP HANA |
| `terajdbc4.jar` | Teradata |
| `denodo-vdp-odbcdriver-linux.tar.gz` | Denodo ODBC |

### Layer 3 — Python Dependencies (requirements.txt, 60+ packages)

| Category | Key Packages | Image Size Impact |
|----------|-------------|------------------|
| Apache Airflow | `apache-airflow==2.8.1`, providers, Celery 5.3.1 | Medium |
| LLM / AI | `langchain 0.3.26`, `langchain-openai 0.3.25`, `langgraph-checkpoint-postgres` | Large |
| Deep Learning | `torch==2.6.0`, `transformers>=4.50.0`, `sentence-transformers 4.0.1` | **Very Large (~3 GB)** |
| Statistical / Forecasting | `prophet 1.1.5`, `statsmodels 0.14.2`, `scikit-learn 1.5.2`, `pmdarima 2.0.3`, `cmdstanpy` | Large |
| NLP | `spacy 3.8.0`, `gensim 4.3.3` | Large |
| Data | `pandas 1.5.2`, `numpy 1.26.4`, `SQLAlchemy 1.4.51` | Medium |
| AWS | `boto3 1.33.13` | Small |
| Azure | `azure-storage-blob 12.21.0`, `azure-identity`, `azure-mgmt-datafactory` | Small |
| Catalog Integration | `pyatlan 6.2.0` | Medium |
| Notifications | `sendgrid`, `pymsteams`, `slackclient` | Small |
| Crypto | `pycryptodome` | Small |
| Reporting | `xhtml2pdf`, `selenium` | Small |

### Layer 4 — First-Party Artifacts

| Artifact | Build Source | Distribution |
|----------|-------------|-------------|
| `dqlabs-3.0-py3-none-any.whl` | `infra/airflow/dags/setup.py` | Installed via `plugins.zip` into Airflow |
| 15+ connector wheels | Separate repositories (undocumented) | Installed on top of base image |

---

## 5. Configuration Areas That Must Be Externalized

For the container image to be environment-agnostic across AWS, Azure, and on-prem Kubernetes deployments, the following must never be baked into the image.

### 5.1 — Airflow Core Configuration

All Airflow settings are overridable via `AIRFLOW__<SECTION>__<KEY>` environment variables. The committed `airflow.cfg` must be treated as a development default only.

| Setting | Current Location | Externalization Target |
|---------|-----------------|----------------------|
| `sql_alchemy_conn` | `airflow.cfg` | Kubernetes Secret → `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` |
| `broker_url` (Redis) | `airflow.cfg` | Kubernetes Secret → `AIRFLOW__CELERY__BROKER_URL` |
| `result_backend` | `airflow.cfg` | Kubernetes Secret → `AIRFLOW__CELERY__RESULT_BACKEND` |
| `fernet_key` | `airflow.cfg` | Kubernetes Secret → `AIRFLOW__CORE__FERNET_KEY` |
| `secret_key` (webserver) | `airflow.cfg` | Kubernetes Secret → `AIRFLOW__WEBSERVER__SECRET_KEY` |
| `parallelism` | `airflow.cfg` (value: 2) | Helm `values.yaml` |
| `max_active_tasks_per_dag` | `airflow.cfg` (value: 2) | Helm `values.yaml` |
| DAGs folder path | `airflow.cfg` | Helm `values.yaml` |

### 5.2 — DQLabs Plugin Configuration

These Airflow Variables must be injected at deployment time, not set manually in the Airflow UI:

| Variable | Sensitivity | Target |
|----------|-------------|--------|
| `dq_endpoint_url` | Low | Helm `values.yaml` / ConfigMap |
| `dq_client_id` | Medium | Kubernetes Secret |
| `dq_client_secret` | **High** | Kubernetes Secret / Secrets Manager |
| `dq_connection_name` | Low | Helm `values.yaml` |
| `dq_allowed_dags` | Low | Helm `values.yaml` |
| `dq_excluded_dags` | Low | Helm `values.yaml` |
| `dq_extract_source_code` | Low | Helm `values.yaml` |

### 5.3 — DQLabs Application Environment (`.env`)

The current `.env.enc` pattern (symmetric encryption, decrypted at container startup) must be retired for containerized deployments and replaced with proper secrets injection:

| Variable | Target |
|----------|--------|
| `POSTGRESQL_DB_NAME` | Kubernetes Secret |
| `POSTGRESQL_USER_NAME` | Kubernetes Secret |
| `POSTGRESQL_PASSWORD` | **Kubernetes Secret / Secrets Manager** |
| `POSTGRESQL_HOST` | Helm `values.yaml` / ConfigMap |
| `POSTGRESQL_PORT` | Helm `values.yaml` |
| `SECRET_KEY` | **Kubernetes Secret** |
| `AIRFLOW_HOME` | Dockerfile `ENV` |
| All data source credentials | **Kubernetes Secret / Secrets Manager** |

### 5.4 — Data Source Credential Decryption Key

DQLabs encrypts all data source credentials (Oracle, MSSQL, Snowflake, etc.) stored in PostgreSQL using `pycryptodome`. The decryption key used by `crypto_helper.py` must be:

- Stored in the secrets backend (not in the image or environment file)
- Rotatable without rebuilding the container image

### 5.5 — Resource Limits

Currently hardcoded in `docker-compose.yaml` at `2 CPU / 5 GB memory` per service. In Kubernetes these become Helm-parameterized per component:

| Component | Helm Parameter |
|-----------|---------------|
| Webserver | `webserver.resources.requests/limits` |
| Scheduler | `scheduler.resources.requests/limits` |
| Worker | `worker.resources.requests/limits` |
| Triggerer | `triggerer.resources.requests/limits` |

---

## 6. Risks and Design Recommendations

### Risk 1 — Image Size (Critical)

**Problem:** `torch==2.6.0` alone adds ~2–3 GB. Combined with `transformers`, `spacy`, `sentence-transformers`, `prophet`, and `gensim`, the total image will exceed 10 GB if built naively.

**Recommendation:**
- Use multi-stage Docker builds: separate `build` stage (wheel compilation) from `runtime` stage
- Evaluate whether CPU-only PyTorch (`torch+cpu`) is sufficient — it is significantly smaller (~700 MB vs ~3 GB GPU)
- Consider a split image strategy: a lean `airflow-base` image for the webserver/scheduler, and a heavier `airflow-worker` image with the full ML stack

---

### Risk 2 — JDBC JARs Are Binary Assets Without a Registry

**Problem:** Seven JAR files are currently committed directly to Git in `infra/airflow/jars/`. This is not a scalable or secure practice for production artifact management.

**Recommendation:**
- Store JARs in a private artifact store (S3 bucket, Azure Blob, Nexus, or Artifactory)
- Fetch them during the Docker image build via `COPY --from` a download stage or a `wget`/`curl` step with checksum verification
- Version each JAR explicitly and track in a manifest file

---

### Risk 3 — `.env.enc` Pattern Is Not Container-Native

**Problem:** The current flow (encrypt `.env` → ship `.env.enc` → decrypt at container startup) is opaque to Kubernetes and bypasses native secrets management.

**Recommendation:**
- Retire `.env.enc` entirely for containerized deployments
- Migrate to Kubernetes Secrets with an Airflow secrets backend (`SecretsManagerBackend` or `VaultBackend`)
- For cloud environments: AWS Secrets Manager, Azure Key Vault, or GCP Secret Manager
- For on-prem: HashiCorp Vault

---

### Risk 4 — Connector Wheels Are Undocumented and Unversioned

**Problem:** 15+ connector wheels referenced in `agent_requirements_dev.txt` have no visible registry, versioning scheme, or documented provenance in this repository.

**Recommendation:**
- Before Dockerfile design begins, produce a complete connector wheel inventory: package name, version, source repository, and current hosting location
- Publish connector wheels to a private PyPI index (AWS CodeArtifact, Azure Artifacts, or a self-hosted Nexus/Artifactory)
- Pin all connector wheel versions explicitly

---

### Risk 5 — `airflow.cfg` Contains Development-Only Tuning

**Problem:** The committed `airflow.cfg` has `parallelism = 2` and `max_active_tasks_per_dag = 2`, tuned for a developer laptop. These values will cause severe throughput problems in production.

**Recommendation:**
- Strip `airflow.cfg` of all environment-sensitive values
- Deliver all production overrides exclusively via `AIRFLOW__` environment variables in the Helm chart
- Document all tunable parameters in the Helm `values.yaml` with production-recommended defaults

---

### Risk 6 — Google Chrome in Worker Containers

**Problem:** Chrome and Chromedriver are installed in the Airflow worker image for Selenium-based PDF report generation. This significantly increases image size, attack surface, and maintenance burden.

**Recommendation:**
- Isolate PDF report generation into a dedicated `report-worker` image variant or a sidecar container
- The base Airflow worker image should not carry a browser
- Consider replacing Selenium-based rendering with a headless service (e.g., Puppeteer microservice, wkhtmltopdf, or a dedicated rendering sidecar)

---

### Risk 7 — SQLAlchemy 1.4.x Pinned

**Problem:** `SQLAlchemy==1.4.51` is a legacy version. Apache Airflow 2.8.x supports SQLAlchemy 1.4 but recommends 2.x for new deployments. Several connector packages may pull in conflicting SQLAlchemy versions.

**Recommendation:**
- Audit `requirements.txt` for SQLAlchemy version conflicts during the dependency resolution phase
- Plan for a SQLAlchemy 2.x migration as part of a future patch cycle

---

## Human-in-the-Loop Checkpoint

Before proceeding to **Prompt 2 (Containerization Design)**, confirmation is required on the following:

| # | Question |
|---|---------|
| **Q1** | Where are the 15+ connector wheels currently hosted? (S3 bucket, internal PyPI, Nexus, etc.) This is a hard dependency for Dockerfile design. |
| **Q2** | Does the `torch`/`transformers`/`sentence-transformers` stack need GPU-enabled workers, or is CPU-only inference acceptable for the Airflow task workload? |
| **Q3** | Should Chrome/Selenium remain in the main Airflow worker image, or be isolated into a separate image variant? |
| **Q4** | For the initial target environment, which secrets mechanism is the primary: **Kubernetes Secrets**, **AWS Secrets Manager**, **Azure Key Vault**, or **HashiCorp Vault**? |
| **Q5** | Should all Airflow components (webserver, scheduler, worker, triggerer) share a **single image** with role determined at startup, or is a **split image** strategy preferred (lighter webserver + heavier worker)? |
| **Q6** | Should the `dqlabs` wheel be built **inside** the Docker multi-stage build, or **pre-built and versioned externally** before the image build begins? |

---

*Document generated as part of the DQLabs containerization and deployment planning initiative.*
*Aligned with: Scope of Work §1.1 through §1.10 | AWS Well-Architected Framework*
