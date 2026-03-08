# =============================================================================
# DQLabs Airflow – Modernized Container Image
#
# Base image : apache/airflow:2.8.1-python3.11 (Debian Bullseye)
# Build ctx  : repository root  →  docker build -f infra/docker/Dockerfile .
# Pattern    : Root for system deps, airflow user for Python, non-root runtime
#
# Build args:
#   AIRFLOW_VERSION     default: 2.8.1
#   PYTHON_VERSION      default: 3.11
#   PYTORCH_INDEX_URL   default: https://download.pytorch.org/whl/cpu
#                       override for air-gap / private mirror
#   INSTALL_ML_STACK    default: true  — set false for lightweight webserver image
#
# System stack:
#   Microsoft ODBC Driver 17   (SQL Server / Azure Synapse)
#   Oracle Instant Client 21.7 (Oracle DB)
#   Denodo ODBC driver         (extracted from infra/airflow/jars/)
#   unixODBC / libaio1
#
# Python stack layers (ordered for Docker layer cache efficiency):
#   Layer 1  torch (CPU-only, ~900 MB, cached unless version changes)
#   Layer 2  requirements-ml.txt      (NLP / semantic stack)
#   Layer 3  requirements-core.txt    (Airflow + providers + utilities)
#   Layer 4  requirements-connectors.txt  (private wheels from vendor/wheels/)
#
# Airflow assets baked in:
#   dqlabs Python package  → /opt/airflow/dags/dqlabs/
#   DAG entry points       → /opt/airflow/dags/
#   Custom Airflow plugin  → /opt/airflow/plugins/
#   Entrypoint             → /entrypoint.sh
# =============================================================================

ARG AIRFLOW_VERSION=2.8.1
ARG PYTHON_VERSION=3.11

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# ── 1. System packages (root required) ───────────────────────────────────────
USER root
ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        wget \
        gnupg2 \
        apt-transport-https \
        ca-certificates \
        unixodbc \
        unixodbc-dev \
        gcc \
        g++ \
        build-essential \
        libpq-dev \
        libaio1 \
        libaio-dev \
        unzip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ── 2. Microsoft ODBC Driver 17  (SQL Server / Azure Synapse / MSSQL) ────────
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
        | gpg --dearmor -o /usr/share/keyrings/microsoft.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft.gpg] \
        https://packages.microsoft.com/debian/12/prod bookworm main" \
        > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
        msodbcsql17 \
        mssql-tools \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="${PATH}:/opt/mssql-tools/bin"

# ── 3. Oracle Instant Client 21.7 ────────────────────────────────────────────
RUN wget -q \
    "https://download.oracle.com/otn_software/linux/instantclient/217000/instantclient-basiclite-linux.x64-21.7.0.0.0dbru.zip" \
    -O /tmp/oracle-client.zip \
    && unzip -q /tmp/oracle-client.zip -d /opt/oracle \
    && rm /tmp/oracle-client.zip \
    && echo "/opt/oracle/instantclient_21_7" > /etc/ld.so.conf.d/oracle.conf \
    && ldconfig

ENV ORACLE_HOME=/opt/oracle/instantclient_21_7
ENV ORACLE_DRIVER_PATH=/opt/oracle/instantclient_21_7
ENV LD_LIBRARY_PATH="/opt/oracle/instantclient_21_7:${LD_LIBRARY_PATH}"

# ── 4. Denodo ODBC Driver (from local jars/) ─────────────────────────────────
# Extracted path: /opt/denodo/denodo-vdp-odbcdriver-linux/lib/unixodbc_x64/
# Must match Driver= path in infra/docker/odbcinst.ini
COPY infra/airflow/jars/denodo-vdp-odbcdriver-linux.tar.gz /tmp/denodo-odbc.tar.gz
RUN mkdir -p /opt/denodo \
    && tar -xzf /tmp/denodo-odbc.tar.gz -C /opt/denodo \
    && rm /tmp/denodo-odbc.tar.gz \
    && ldconfig

# ── 5. JDBC Drivers (Spark / Databricks / DB2 / Teradata / Denodo) ───────────
RUN mkdir -p /opt/airflow/jars
COPY infra/airflow/jars/*.jar /opt/airflow/jars/
RUN chown -R airflow:root /opt/airflow/jars && chmod -R 755 /opt/airflow/jars

# ── 6. Register Denodo ODBC driver ───────────────────────────────────────────
# We APPEND to /etc/odbcinst.ini rather than replacing it.
# The MSSQL ODBC apt package writes its own entry to that file — overwriting it
# would silently break SQL Server connectivity.
RUN printf '\n[DenodoODBCDriver]\nDescription=ODBC driver of Denodo\nDriver=/opt/denodo/denodo-vdp-odbcdriver-linux/lib/unixodbc_x64/denodoodbc.so\nUsageCount=1\n' \
    >> /etc/odbcinst.ini

# ── 7. Entrypoint script ──────────────────────────────────────────────────────
COPY infra/docker/scripts/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# ── 8. Python dependencies (airflow user) ────────────────────────────────────
USER airflow

# Copy all requirement files
COPY --chown=airflow:root infra/docker/requirements/ /tmp/requirements/

# Layer 1: torch — large, cache it first so it isn't re-downloaded on every build
# Override PYTORCH_INDEX_URL for air-gap or private mirror.
ARG PYTORCH_INDEX_URL=https://download.pytorch.org/whl/cpu
RUN pip install --no-cache-dir \
    --extra-index-url "${PYTORCH_INDEX_URL}" \
    torch==2.6.0

# Layer 2: ML / NLP stack (sentence-transformers, spacy, langchain, etc.)
ARG INSTALL_ML_STACK=true
RUN if [ "${INSTALL_ML_STACK}" = "true" ]; then \
        pip install --no-cache-dir \
            --extra-index-url "${PYTORCH_INDEX_URL}" \
            -r /tmp/requirements/requirements-ml.txt; \
    fi

# Layer 3: Core providers + DQLabs packages
# --constraint uses the official Airflow 2.8.1 pre-resolved versions, which:
#   1. Prevents pip backtracking (57-min resolution → <5 min)
#   2. Resolves gcloud-aio-auth/cryptography conflict automatically
ARG CONSTRAINTS_URL=https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.11.txt
RUN pip install --no-cache-dir \
    --constraint "${CONSTRAINTS_URL}" \
    -r /tmp/requirements/requirements-core.txt

# Layer 3.5: Re-pin typing_extensions ≥ 4.10.0 (constraint file pins 4.9.0 but
# ML packages installed in Layer 2 require TypeIs which was added in 4.10.0).
# Airflow 2.8.1 works fine with newer typing_extensions.
RUN pip install --no-cache-dir "typing_extensions>=4.10.0,<5.0.0"

# Layer 4: Connector wheels (private, must be pre-placed in vendor/wheels/)
# If the wheel files are absent the build will SKIP this layer and log a warning.
# Populate vendor/wheels/ from your artifact store before building for production.
COPY --chown=airflow:root infra/docker/vendor/wheels /tmp/wheels/
RUN if [ "$(ls /tmp/wheels/*.whl 2>/dev/null | wc -l)" -gt 0 ]; then \
        echo "==> Installing connector wheels from vendor/wheels/..." \
        && pip install --no-cache-dir \
            --find-links /tmp/wheels \
            --no-index \
            -r /tmp/requirements/requirements-connectors.txt; \
    else \
        echo "==> WARNING: vendor/wheels/ is empty. Connector wheels NOT installed."; \
        echo "    Populate infra/docker/vendor/wheels/ and rebuild for connector support."; \
    fi

# ── 9. DAGs and dqlabs package ───────────────────────────────────────────────
# Baked in at build time for production.
# In local dev these paths are overridden by volume mounts in docker-compose.yml.
COPY --chown=airflow:root infra/airflow/dags/dq_dags.py       /opt/airflow/dags/dq_dags.py
COPY --chown=airflow:root infra/airflow/dags/connections.py   /opt/airflow/dags/connections.py
COPY --chown=airflow:root infra/airflow/dags/task_manager.py  /opt/airflow/dags/task_manager.py
COPY --chown=airflow:root infra/airflow/dags/dqlabs            /opt/airflow/dags/dqlabs

# ── 10. Airflow Plugin (DAG-run listener → pushes metadata to DQLabs) ─────────
COPY --chown=airflow:root infra/airflow/plugins/dqlabs_airflow_plugin.py \
    /opt/airflow/plugins/dqlabs_airflow_plugin.py
COPY --chown=airflow:root infra/airflow/plugins/dq_variables.json \
    /opt/airflow/plugins/dq_variables.json

# ── 11. Runtime environment ───────────────────────────────────────────────────
ENV AIRFLOW_HOME=/opt/airflow
# dqlabs package must be importable by all Airflow components
ENV PYTHONPATH="/opt/airflow/dags:${PYTHONPATH}"

# Non-root runtime — airflow user (UID 50000 in the official image)
USER airflow

ENTRYPOINT ["/entrypoint.sh"]
CMD ["airflow", "webserver"]
