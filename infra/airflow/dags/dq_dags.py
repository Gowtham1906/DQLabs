import json
from airflow.models import DAG
from pathlib import Path

from dqlabs.utils import (
    get_all_organizations,
    load_env,
    get_organization,
    get_active_channel,
)
from dqlabs.dags.dq_dags import create_dag
from dqlabs.app_constants.dq_constants import (
    task_categories,
    SEMANTICS,
    EXPORT_FAILED_ROWS,
    CATALOG_UPDATE,
    CATALOG_SCHEDULE,
    METADATA,
)

# load the env variables
base_path = str(Path(__file__).parents[0])
load_env(base_path)


def create_dags() -> list:
    dags = []
    organizations = get_all_organizations()
    for organization in organizations:
        if not organization:
            continue

        dq_organization = get_organization(organization)
        if not dq_organization:
            continue

        is_active_org = dq_organization.get("is_active")
        if not is_active_org:
            continue
        organization.update(dq_organization)
        organization.update({"schedule_interval": None})

        for category in task_categories:
            organization.update({"dag_category": category})
            dag: DAG = None
            if category == SEMANTICS:
                # render semantic dag
                semantic_settings = organization.get("semantics", {})
                semantic_settings = semantic_settings if semantic_settings else {}
                semantic_settings = (
                    json.loads(semantic_settings, default=str)
                    if isinstance(semantic_settings, str)
                    else semantic_settings
                )
                organization.update({"semantic_settings": semantic_settings})
                dag = create_dag(organization, category)
            elif category in [EXPORT_FAILED_ROWS, METADATA]:
                # render semantic dag
                general_settings = organization.get("settings", {})
                general_settings = general_settings if general_settings else {}
                general_settings = (
                    json.loads(general_settings, default=str)
                    if isinstance(general_settings, str)
                    else general_settings
                )

                report_settings = general_settings.get("reporting")
                report_settings = report_settings if report_settings else {}
                report_settings = (
                    json.loads(report_settings, default=str)
                    if isinstance(report_settings, str)
                    else report_settings
                )
                organization.update({"report_settings": report_settings})
                dag = create_dag(organization, category)
            elif category in [CATALOG_UPDATE, CATALOG_SCHEDULE]:
                channels = get_active_channel(organization)
                organization.update({"catalog": channels})
                dag = create_dag(organization, category)
            else:
                # render dq categorical dags
                dag = create_dag(organization, category)

            if dag:
                if isinstance(dag, list):
                    dags.extend(dag)
                else:
                    dags.append(dag)
    return dags


# register dags
dags = create_dags()
for dag in dags:
    globals()[dag.dag_id] = dag
