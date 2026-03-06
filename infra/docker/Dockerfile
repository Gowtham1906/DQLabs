# syntax=docker/dockerfile:1
# =============================================================================
# DQLabs Airflow — Multi-Stage Dockerfile
# =============================================================================
# Platform : linux/amd64
# Python   : 3.11
# Airflow  : 2.8.1
# Strategy : Two-stage build — python-builder → runtime
#            Build tools, dev headers, and pip cache never reach the final image.
#
# Build (from repo root infra/ directory as context):
#   docker build -f docker/Dockerfile \
#                --build-arg AIRFLOW_VERSION=2.8.1 \
#                -t dqlabs-airflow:3.0.0 \
#                ./infra
#
# Or via Compose (recommended):
#   docker compose -f infra/docker/docker-compose.yml build
#
# Optional build args:
#   INSTALL_JAVA=true   Include OpenJDK 17 (Hive/Spark JDBC connectors)
#   IMAGE_TAG           Version label applied to the image
# =============================================================================


# =============================================================================
# Stage 1: python-builder
# =============================================================================
# Installs ALL Python dependencies into /opt/venv.
# This stage is thrown away — only the compiled venv crosses into runtime.
#
# BuildKit cache mounts (--mount=type=cache) keep the pip download cache on
# the host between builds. Invalidating a layer does NOT re-download packages
# that pip already cached, cutting rebuild times from 30+ min to ~2 min.
# =============================================================================
FROM --platform=linux/amd64 python:3.11-slim-bookworm AS python-builder

ARG AIRFLOW_VERSION=2.8.1
ARG PYTHON_VERSION=3.11

# Airflow provider extras — add or remove to control installed providers.
# Removing unused providers reduces both install time and final image size.
ARG AIRFLOW_EXTRAS="celery,postgres,redis,amazon,microsoft.azure,google,slack,openssl,http,statsd"

ENV VIRTUAL_ENV=/opt/venv
ENV PATH="${VIRTUAL_ENV}/bin:$PATH"
# Suppress interactive prompts from apt/debconf (eliminates the debconf warnings)
ENV DEBIAN_FRONTEND=noninteractive

# ---- Build-time system libraries --------------------------------------------
# Only what is required to compile Python C-extensions from source.
# cargo (Rust) is intentionally excluded — all packages used here ship
# pre-built binary wheels, so no Rust compilation is needed.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    libffi-dev \
    libsasl2-dev \
    libpq-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# ---- Virtual environment + pip toolchain ------------------------------------
RUN python -m venv ${VIRTUAL_ENV} && \
    pip install --upgrade \
        pip==24.3.1 \
        setuptools==75.8.0 \
        wheel==0.45.1

# ---- Step 1: Apache Airflow (with official constraints) ---------------------
# Installed first and separately so this expensive layer is cached
# independently of application dependency changes.
#
# --prefer-binary: always pick a pre-built wheel over compiling from source.
# --mount=type=cache: pip's HTTP and wheel cache persists between builds on
#   the host, so packages are only downloaded once even if this layer is
#   invalidated.
RUN --mount=type=cache,target=/root/.cache/pip,sharing=locked \
    pip install \
        --prefer-binary \
        "apache-airflow[${AIRFLOW_EXTRAS}]==${AIRFLOW_VERSION}" \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# ---- Step 2: PyTorch CPU-only -----------------------------------------------
# Installed with --index-url (not --extra-index-url) so pip looks ONLY at the
# PyTorch CPU index. This guarantees the ~800 MB CPU wheel is selected instead
# of the ~2.5 GB CUDA wheel from PyPI.
# Must run before requirements.txt so the CPU build is already present when
# pip processes the torch==2.6.0 line in requirements.txt.
RUN --mount=type=cache,target=/root/.cache/pip,sharing=locked \
    pip install \
        --prefer-binary \
        --index-url https://download.pytorch.org/whl/cpu \
        torch==2.6.0

# ---- Step 3: dqlabs application dependencies --------------------------------
# Copied separately from app code so this layer is only invalidated when
# requirements.txt itself changes (not on every code change).
COPY airflow/dags/requirements.txt /tmp/requirements.txt

RUN --mount=type=cache,target=/root/.cache/pip,sharing=locked \
    pip install \
        --prefer-binary \
        -r /tmp/requirements.txt

# ---- Step 4: Build and install dqlabs wheel ---------------------------------
# pkutils and python-dotenv are needed by setup.py.
# No .env file is present at build time so the encryption step is skipped.
COPY airflow/dags/ /build/dags/
WORKDIR /build/dags

RUN --mount=type=cache,target=/root/.cache/pip,sharing=locked \
    pip install --prefer-binary pkutils python-dotenv && \
    python setup.py bdist_wheel --quiet && \
    pip install --prefer-binary dist/*.whl


# =============================================================================
# Stage 2: runtime
# =============================================================================
# Clean slim image — receives only /opt/venv from the builder.
# No compilers, no dev headers, no pip cache, no build artifacts.
# =============================================================================
FROM --platform=linux/amd64 python:3.11-slim-bookworm AS runtime

ARG AIRFLOW_VERSION=2.8.1
ARG IMAGE_TAG=latest
ARG AIRFLOW_UID=50000
ARG AIRFLOW_GID=50000

# Set to "true" at build time to include OpenJDK 17.
# Required for: Apache Hive (JDBC), Apache Spark (JDBC), Apache Iceberg.
# Effect: adds ~280 MB to the final image.
ARG INSTALL_JAVA=false

LABEL maintainer="DQLabs Platform Team" \
      org.opencontainers.image.title="dqlabs-airflow" \
      org.opencontainers.image.version="${IMAGE_TAG}" \
      org.opencontainers.image.description="DQLabs data quality platform — Apache Airflow ${AIRFLOW_VERSION}" \
      org.opencontainers.image.base.name="python:3.11-slim-bookworm"

ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW_VERSION=${AIRFLOW_VERSION}
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="${VIRTUAL_ENV}/bin:/opt/mssql-tools/bin:${PATH}"
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive

# Pre-declared — only active if INSTALL_JAVA=true
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Oracle client lib path — no-op unless Oracle Instant Client is added separately
ENV LD_LIBRARY_PATH=/usr/lib/oracle/21/client64/lib:${LD_LIBRARY_PATH}

# ---- Runtime system packages ------------------------------------------------
# gnupg + apt-transport-https: required to import the Microsoft GPG key and
# access packages.microsoft.com over HTTPS (must come before the MSSQL step).
RUN apt-get update && apt-get install -y --no-install-recommends \
    gnupg \
    apt-transport-https \
    unixodbc \
    freetds-bin \
    tdsodbc \
    libsasl2-2 \
    libsasl2-modules \
    libsasl2-modules-gssapi-mit \
    curl \
    wget \
    netcat-traditional \
    gosu \
    ca-certificates \
    openssl \
    && rm -rf /var/lib/apt/lists/*

# ---- Microsoft ODBC Driver 17 for SQL Server --------------------------------
# Covers: MSSQL, Azure Synapse, Azure SQL
# Separate RUN ensures the GPG keyring is fully flushed to disk before
# the subsequent apt-get update reads it.
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
      | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
    curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list \
      > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
      msodbcsql17 \
      mssql-tools \
    && rm -rf /var/lib/apt/lists/*

# ---- Register FreeTDS ODBC driver -------------------------------------------
RUN printf "[FreeTDS]\nDescription=FreeTDS Driver\nDriver=/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so\nSetup=/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so\nFileUsage=1\n" \
    >> /etc/odbcinst.ini

# ---- Optional: OpenJDK 17 ---------------------------------------------------
# Enable with: --build-arg INSTALL_JAVA=true
# Required for: Hive (JDBC), Spark (JDBC), Iceberg
RUN if [ "${INSTALL_JAVA}" = "true" ]; then \
      apt-get update && \
      apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
      rm -rf /var/lib/apt/lists/*; \
    fi

# ---- Optional: Denodo ODBC driver -------------------------------------------
# Place the driver archive in infra/airflow/jars/ then uncomment both lines.
#
# COPY airflow/jars/denodo-vdp-odbcdriver-linux.tar.gz /tmp/denodo-odbc.tar.gz
# RUN mkdir -p /usr/lib/denodo && \
#     tar -xzf /tmp/denodo-odbc.tar.gz --directory /usr/lib/denodo && \
#     printf "[DenodoODBCDriver]\nDescription=ODBC driver of Denodo\nDriver=/usr/lib/denodo/denodo-vdp-odbcdriver-linux/lib/unixodbc_x64/denodoodbc.so\nUsageCount=1\n" \
#       >> /etc/odbcinst.ini && \
#     rm /tmp/denodo-odbc.tar.gz

# ---- Non-root user ----------------------------------------------------------
RUN groupadd --gid ${AIRFLOW_GID} airflow && \
    useradd --uid ${AIRFLOW_UID} \
            --gid airflow \
            --shell /bin/bash \
            --home-dir ${AIRFLOW_HOME} \
            --no-create-home \
            airflow

# ---- Directory structure ----------------------------------------------------
RUN mkdir -p \
      ${AIRFLOW_HOME}/dags \
      ${AIRFLOW_HOME}/logs \
      ${AIRFLOW_HOME}/plugins \
      ${AIRFLOW_HOME}/data \
      ${AIRFLOW_HOME}/csv \
      ${AIRFLOW_HOME}/resources \
    && chown -R airflow:airflow ${AIRFLOW_HOME}

# ---- Copy Python environment from builder -----------------------------------
COPY --from=python-builder /opt/venv /opt/venv

# ---- Copy application files -------------------------------------------------
COPY --chown=airflow:airflow airflow/dags/ ${AIRFLOW_HOME}/dags/
COPY --chown=airflow:airflow airflow/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

# ---- Entrypoint -------------------------------------------------------------
COPY docker/scripts/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

WORKDIR ${AIRFLOW_HOME}

USER airflow

HEALTHCHECK --interval=30s --timeout=15s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

EXPOSE 8080 8793 5555

ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"]