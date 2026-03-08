This directory is populated by the GitHub Actions CI workflow before `docker build`.

Contents required:
  - dqlabs-3.0-py3-none-any.whl            (built from infra/airflow/dags/)
  - dqlabs_connectors-adf-1.0.0-*.whl
  - dqlabs-connectors-airflow-1.0.0-*.whl
  - dqlabs-connectors-athena-1.0.0-*.whl
  - ... (all 30+ connector wheels)

Source:
  Wheels are retrieved from S3 (or Git LFS) as part of the CI prepare-wheels job.
  See .github/workflows/build-and-publish.yml for details.

Do NOT commit .whl files to this directory.
