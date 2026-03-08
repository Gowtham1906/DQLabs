# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**DQLabs-Airflow** is a multi-tenant enterprise data quality and governance platform built on Apache Airflow. It dynamically generates Airflow DAGs for 18 categories of data quality tasks across multiple client organizations. The packaged dqlabs module (`dqlabs-3.0-py3-none-any.whl`) is deployed to AWS MWAA.

## Development Setup

### Docker (Recommended)
```bash
cd infra
sudo sh start-airflow.sh 1.0   # Build and start all containers
# Airflow UI available at http://localhost:8080
```

### Local Environment
```bash
python -m venv <env_path>
source <env_path>/bin/activate
pip install -r infra/airflow/dags/requirements.txt
pre-commit install
```

Required environment variables (copy from `environments/<env>.env` to `.env`):
```
AIRFLOW_HOME, POSTGRESQL_DB_NAME, POSTGRESQL_USER_NAME, POSTGRESQL_PASSWORD,
POSTGRESQL_HOST, POSTGRESQL_PORT, DEBUG, LOG_BACKUP_COUNT, SECRET_KEY, CLIENT_NAME
```

Airflow database setup:
```bash
airflow db init
airflow webserver   # port 8080
airflow scheduler
```

## Build & Deployment

### Build Wheel (for MWAA)
```bash
cd infra/airflow/dags
rm -rf dist/ build/
python setup.py bdist_wheel
cd dist/
chmod -R 755 ./dqlabs-3.0-py3-none-any.whl
zip -r plugins.zip ./dqlabs-3.0-py3-none-any.whl
# Upload plugins.zip to S3 and update the MWAA instance
```

For MWAA deployments, use `infra/airflow/mwaa_requirements_2.8.1.txt` (not `requirements.txt`).

### CI/CD
- **Jenkins** (`Jenkinsfile`): Checkout → Build Docker image → Push to ECR
- **AWS CodeBuild** (`buildspec-<env>.yml`): Builds wheel, uploads to S3, deploys DAGs to MWAA
- Environments: `dev`, `qa`, `uat`, `prod`, `staging`

## Code Style

Black formatter with line length 88:
```bash
black .
```
Excludes: `.git`, `build`, `dist`, `.venv`, `.env`, `migrations`, `logs`.

## Architecture

### DAG Factory Pattern

The core of the system is a dynamic DAG factory. The entry point is `infra/airflow/dags/dq_dags.py`, which:
1. Loads all organization configs
2. Iterates through each org + task category combination
3. Calls `create_dag(dag_info, category)` to register DAGs into Airflow's global namespace

This means DAGs are not defined statically — they are created programmatically at scheduler startup.

### DAG Task Categories (18 types)

`SEMANTICS`, `RELIABILITY`, `PROFILE`, `STATISTICS`, `CUSTOM`, `BUSINESS_RULES`, `BEHAVIORAL`, `EXPORT_FAILED_ROWS`, `CATALOG_UPDATE`, `NOTIFICATION`, `USERACTIVITY`, `CATALOG_SCHEDULE`, `METADATA`, `SYNCASSET`, `USAGE_QUERY`, `OBSERVE`, `WORKFLOW`, `PROCESS`, `EXTRACT`

### Key Module Layout

```
infra/airflow/dags/
├── dq_dags.py                  # DAG factory entry point
├── setup.py                    # Wheel build configuration
├── requirements.txt            # Production dependencies
└── dqlabs/                     # Core package
    ├── app_constants/          # Shared constants
    ├── app_helper/             # 18 helper modules (business logic)
    │   ├── dag_helper.py       # DAG config & default args
    │   ├── lineage_helper.py   # SQL lineage tracking
    │   ├── pipeline_helper.py  # Pipeline orchestration
    │   ├── report_helper.py    # Report generation & distribution
    │   ├── connection_helper.py# Database connection management
    │   ├── storage_helper.py   # S3/local storage abstraction
    │   ├── notification_helpers/ # Email, Slack, Teams alerts
    │   └── ...
    ├── dags/                   # DAG creation logic
    ├── dq_package/             # Core data quality algorithms
    ├── drivers/                # Database drivers
    ├── enums/                  # Task category enums
    ├── models/                 # Data models
    ├── services/               # Storage services (S3, local)
    ├── tasks/                  # Task implementations (one file per category)
    │   ├── reliability.py      # Largest task file (~176KB)
    │   ├── catalog_schedule.py
    │   ├── export_failed_rows.py
    │   ├── check_alerts.py
    │   └── ...
    └── utils/                  # Shared utilities
```

### Multi-Tenant Design

Each client organization is a tenant. DAG IDs are namespaced per org. Environment-specific configs live in `environments/<env>.env` and are copied to `.env` for deployment.

### Supported Data Sources (30+)

AWS (Athena, S3), GCP (BigQuery, GCS), Azure (Synapse, ADLS), Databricks, Snowflake, Redshift, PostgreSQL, MySQL, Oracle, MSSQL, Denodo, Teradata, DB2, dbt, Tableau, PowerBI, Talend, Fivetran, and more. Connectors are specified in `mwaa_requirements_2.8.1.txt` as dqlabs connector packages.

### Deployment Modes

| Mode | Executor | Use Case |
|------|----------|---------|
| Local (Docker Compose) | LocalExecutor | Development |
| AWS MWAA | CeleryExecutor | Production (managed) |
| Kubernetes (EKS) | KubernetesExecutor | Self-hosted production |

K8s manifests are in `infra/k8s/airflow/` — covers PostgreSQL, webserver, scheduler, workers, triggerer, RabbitMQ, and Nginx ingress.

## Environment Configuration

When working on a specific deployment target, copy the corresponding env file:
```bash
cp environments/qa.env .env     # for QA
cp environments/prod.env .env   # for production
```

The `connection_file/` directory holds service account credentials (e.g., BigQuery JSON keys) — treat these as secrets.
