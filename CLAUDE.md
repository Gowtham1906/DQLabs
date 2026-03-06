# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DQLabs-V1 is a data quality and orchestration platform built on Apache Airflow. The core application is a Python package (`dqlabs`) that implements data quality checks, statistical/ML models, and integrations with 20+ data sources, deployed via Docker or Kubernetes.

## Repository Structure

```
infra/
  airflow/
    dags/
      dqlabs/          # Core Python package (the main application)
      setup.py         # Builds dqlabs wheel distribution
      requirements.txt # Production dependencies
      .env             # Local environment variables (encrypted to .env.enc for deployment)
    Dockerfile         # Airflow image (Python 3.11, amd64, includes ODBC/Oracle/Java/Chrome)
    airflow.cfg        # Airflow config (CeleryExecutor + PostgreSQL)
  airfloweks/          # EKS-specific Airflow variant
  k8s/                 # Kubernetes Helm templates for multi-tenant deployment
  hosted-airflow/      # MWAA-specific configs
  docker-compose.yaml  # Local dev stack
slackbot/              # Flask-based Slack bot for deployment automation
buildspec-*.yml        # AWS CodeBuild specs per environment (dev/qa/uat/prod/sandbox/demo)
Jenkinsfile            # CI/CD: build Docker image → push to ECR
pyproject.toml         # Black formatter config (line-length: 88)
```

## Commands

### Local Development (Docker — Recommended)

```bash
cd infra
sudo sh start-airflow.sh 1.0        # Build and start all containers
# OR manually:
sudo docker-compose up airflow-init  # Initialize Airflow DB (first time)
sudo docker-compose up -d            # Start all services
```

- Airflow UI: http://localhost:8080
- Flower (Celery monitoring): http://localhost:5555

### Local Development (Native Python)

```bash
# Python 3.8+, create venv, then:
export AIRFLOW_HOME=~/airflow
pip install "apache-airflow==2.2.5" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.2.5/constraints-3.8.txt"
pip install -r infra/airflow/dags/requirements.txt
airflow db init
airflow webserver &
airflow scheduler
```

### Building the dqlabs Package

```bash
cd infra/airflow/dags
rm -rf dist/ build/
python setup.py bdist_wheel           # Outputs dqlabs-3.0-py3-none-any.whl in dist/
```

### Deploying to MWAA

```bash
cd infra/airflow/dags
# Copy environment-specific vars from environments/<env>.env into .env
python setup.py bdist_wheel
cd dist/
zip -r plugins.zip ./dqlabs-3.0-py3-none-any.whl
# Upload plugins.zip and requirements_mwaa_2.8.1.txt to S3 MWAA bucket
```

### Docker Image Build & Push (Jenkins/Manual)

```bash
docker build -t dqlabs-airflow:latest infra/airflow/
aws ecr get-login-password | docker login --username AWS --password-stdin <registry>
docker tag dqlabs-airflow:latest <registry>/dqlabs-airflow:$VERSION
docker push <registry>/dqlabs-airflow:$VERSION
```

ECR registry: `655275087384.dkr.ecr.us-east-1.amazonaws.com/dqlabs-airflow`

### Code Formatting

```bash
black infra/airflow/dags/dqlabs/     # Format with Black (line-length: 88)
pre-commit install                    # Install pre-commit hooks (run once)
```

### Testing

```bash
cd infra/airflow
pytest tests/
```

## Core Architecture

### `dqlabs` Package (at `infra/airflow/dags/dqlabs/`)

The heart of the application. Organized into:

- **`tasks/`** — 21 Airflow task implementations covering: `process`, `reliability`, `semantic_analysis`, `update_threshold`, `health`, `observe`, `catalog_schedule`, `catalog_update`, `export_failed_rows`, `metadata`, `snapshot`, `sync_asset`, `check_alerts`, `extract_advanced_measures`, `profile`, `scoring`, `user_activity`. Subdirs include `connector/` (per-source tasks) and `behavioral/`.
- **`app_helper/`** — 18 helper modules. Key ones: `dag_helper.py` (DAG creation, 89KB), `lineage_helper.py` (data lineage, 200KB largest), `connection_helper.py` (DB connections), `dq_helper.py` (DQ operations), `sql_group_parse.py` (SQL parsing, 59KB).
- **`models/`** — Statistical/ML models: `univariate_models.py`, `behavioral_models.py`, `enhanced_threshold_model.py`, `semantic_model.py`.
- **`drivers/`** — Execution layer: `query_driver.py`, `schema_driver.py`, `asset_driver.py`, `dedupe_driver.py`, `iceberg_driver.py`.
- **`enums/`** — Shared enumerations (connection types, schedule types, trigger types, etc.).
- **`services/`** — Storage: `s3_storage_service.py`, `local_storage_service.py`.
- **`dq_package/`** — Custom Airflow callbacks/operators and example DAGs.
- **`file_crypto.py`** — Encrypts `.env` → `.env.enc` for deployment.

### Airflow Deployment Model

- **Executor:** CeleryExecutor with Redis broker
- **Backend:** PostgreSQL 14
- **DAG entry point:** `infra/airflow/dags/dq_dags.py`
- The `dqlabs` wheel is installed as an Airflow plugin via `plugins.zip`

### Multi-Environment Configuration

Environment-specific `.env` files live in `infra/airflow/dags/environments/` (mwaa.env, qa.env, uat.env, prod.env). Copy the target env file to `.env` before building/deploying.

### Connector Architecture

Data source connectors are distributed as separate wheels (15+ specialized packages) installed on top of the base Airflow image. Each connector provides a task implementation under `dqlabs/tasks/connector/`.

### Kubernetes Deployment

`infra/k8s/` contains Helm templates for multi-tenant deployments supporting AWS EKS, Azure AKS, GCP GKE, and on-prem. Each tenant gets an isolated namespace with dedicated PostgreSQL schema/DB, PgBouncer connection pooling, and autoscaling (HPA).

## Key Technologies

| Layer | Technology |
|-------|-----------|
| Orchestration | Apache Airflow 2.8.1, Celery 5.3.1 |
| ML/Analytics | Prophet, statsmodels, scikit-learn, PyTorch, transformers, LangChain |
| Data | pandas 1.5.2, numpy 1.26.4, SQLAlchemy 1.4.51 |
| Cloud | AWS (S3, MWAA, ECR, Secrets Manager), Azure (ADLS, Key Vault), GCP (BigQuery) |
| Infra | Docker, Kubernetes, PostgreSQL 14, Redis |
| Code Quality | Black (formatter), pre-commit |

## Environment Variables

The `.env` file at `infra/airflow/dags/.env` controls all runtime configuration. It is encrypted to `.env.enc` by `file_crypto.py` during the buildspec deploy process. Never commit the unencrypted `.env` file.

Required variables include: `POSTGRESQL_DB_NAME`, `POSTGRESQL_USER_NAME`, `POSTGRESQL_PASSWORD`, `POSTGRESQL_HOST`, `POSTGRESQL_PORT`, `AIRFLOW_HOME`, `SECRET_KEY`.
