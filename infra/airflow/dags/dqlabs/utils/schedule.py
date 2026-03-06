"""
Migration Notes From V2 to V3:
Migrations Completed
"""

import os

from dqlabs.enums.trigger_types import TriggerType
from dqlabs.enums.schedule_list import ScheduleTypes
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.app_helper.db_helper import execute_query, fetchone
from dqlabs.enums.connection_types import ConnectionType
from dqlabs.app_constants.dq_constants import DEFAULT_AIRFLOW_HOSTNAME
from datetime import datetime
import pytz
import json
from uuid import uuid4
from dqlabs.app_helper.dag_helper import (
    get_postgres_connection,
    get_external_storage_config,
)


def create_request_queue(
    level: str,
    asset_id: str,
    attribute_id: str = None,
    measure_id: str = None,
    report_id: str = None,
    job_type: str = "",
    trigger_type: TriggerType = TriggerType.Once,
    config: dict = {},
):
    try:
        asset = None
        measure = None
        report = None
        job_input = {}
        connection = get_postgres_connection(config)

        if job_type == ScheduleTypes.Semantic.value:
            if asset_id:
                with connection.cursor() as cursor:
                    query_string = (
                        f"""SELECT * FROM core.asset WHERE id = '{asset_id}'"""
                    )
                    cursor = execute_query(connection, cursor, query_string)
                    asset = fetchone(cursor)
                    asset = asset if asset else {}
            job_input = prepare_semantic_dag_info(config, asset, connection)
        elif job_type == ScheduleTypes.Measure.value:
            if measure_id:
                with connection.cursor() as cursor:
                    query_string = (
                        f"""SELECT * FROM core.measure WHERE id = '{measure_id}'"""
                    )
                    cursor = execute_query(connection, cursor, query_string)
                    measure = fetchone(cursor)
                    measure = measure if measure else {}
                    connection_string = f"select * from core.connection where id='{measure.get('connection_id')}'"
                    cursor = execute_query(connection, cursor, connection_string)
                    connection_detail = fetchone(cursor)
                    connection_detail = connection_detail if connection_detail else {}
                    if connection_detail:
                        measure.update({"connection": connection_detail})
                job_input = prepare_measure_dag_info(config, measure, connection)
        elif job_type == ScheduleTypes.Report.value:
            if report_id:
                with connection.cursor() as cursor:
                    query_string = (
                        f"""SELECT * FROM core.widget WHERE id='{report_id}'"""
                    )
                    cursor = execute_query(connection, cursor, query_string)
                    report = fetchone(cursor)
                    report = report if report else {}
                job_input = prepare_report_notification_dag_info(
                    config, report, connection
                )
        elif job_type == ScheduleTypes.Metadata.value:
            job_input = prepare_metadata_dag_info(config, connection)
        else:
            with connection.cursor() as cursor:
                query_string = f"""SELECT * FROM core.asset WHERE id = '{asset_id}'"""
                cursor = execute_query(connection, cursor, query_string)
                asset = fetchone(cursor)
                connection_string = f"select * from core.connection where id='{asset.get('connection_id')}'"
                cursor = execute_query(connection, cursor, connection_string)
                connection_detail = fetchone(cursor)
                connection_detail = connection_detail if connection_detail else {}
                if connection_detail:
                    asset.update({"connection": connection_detail})
            job_input = prepare_dag_info(config, asset, connection)

        airflow_hostname = DEFAULT_AIRFLOW_HOSTNAME
        # if self.mwaa_env:
        #     hostname = self.mwaa_service.get_host_name()
        #     airflow_hostname = hostname if hostname else airflow_hostname
        airflow_config = {"host": airflow_hostname}

        connection_id = str(asset.get("connection_id")) if asset else None
        if not connection_id and measure:
            connection_id = str(measure.get("connection_id"))

        request_queue = {
            "id": str(uuid4()),
            "job_type": job_type if job_type else ScheduleTypes.Asset.value,
            "level": level,
            "trigger_type": trigger_type.value,
            "status": ScheduleStatus.Pending.value,
            "schedule_time": datetime.utcnow().replace(tzinfo=pytz.utc),
            "asset_id": str(asset.get("id")) if asset else None,
            "connection_id": str(asset.get("connection_id")) if asset else None,
            "measure_id": None,
            "job_input": json.dumps(job_input),
            "airflow_config": json.dumps(airflow_config),
            "attributes": [],
            "measures": [],
            "report_id": None,
        }

        if level.lower() == "attribute" and asset:
            with connection.cursor() as cursor:
                query_string = f"""SELECT * FROM core.attribute WHERE asset_id = '{asset.get('id')}' AND id = '{attribute_id}'"""
                cursor = execute_query(connection, cursor, query_string)
                attribute = fetchone(cursor)
                attribute = attribute if attribute else {}
            attributes = []
            if attribute:
                attributes.append({"id": str(attribute.get(attribute_id))})
            request_queue.update({"attributes": attributes})

        if level.lower() == "measure":
            with connection.cursor() as cursor:
                query_string = (
                    f"""SELECT * FROM core.measure WHERE id = '{measure_id}'"""
                )
                cursor = execute_query(connection, cursor, query_string)
                measure = fetchone(cursor)
                measure = measure if measure else {}
            attributes = []
            measures = []
            if measure:
                if measure.get("attribute_id"):
                    attributes.append({"id": str(measure.get("attribute_id"))})
                measures.append({"id": str(measure.get("id"))})
            request_queue.update(
                {
                    "connection_id": (
                        str(measure.get("connection_id")) if measure else None
                    ),
                    "measure_id": str(measure.get("id")) if measure else None,
                    "attributes": json.dumps(attributes),
                    "measures": json.dumps(measures),
                }
            )

        if level.lower() == "report":
            request_queue = {
                "id": str(uuid4()),
                "job_type": "notification",
                "level": level,
                "trigger_type": trigger_type.value,
                "status": ScheduleStatus.Pending.value,
                "schedule_time": datetime.utcnow().replace(tzinfo=pytz.utc),
                "asset_id": None,
                "connection_id": None,
                "measure_id": None,
                "job_input": json.dumps(job_input),
                "airflow_config": json.dumps(airflow_config),
                "attributes": [],
                "measures": [],
                "report_id": report_id if report_id else None,
            }

        with connection.cursor() as cursor:
            query_input = tuple(request_queue.values())
            input_literals = ", ".join(["%s"] * len(query_input))
            query_param = cursor.mogrify(
                f"({input_literals},CURRENT_TIMESTAMP)",
                query_input,
            ).decode("utf-8")

            query_string = f"""
                        insert into core.request_queue(
                            id, job_type, level, trigger_type, status, schedule_time,
                            asset_id, connection_id, measure_id, job_input, airflow_config, attributes, measures, report_id, created_date
                        ) values {query_param}  RETURNING *
                    """
            cursor = execute_query(connection, cursor, query_string)
            queue = fetchone(cursor)
            queue = queue if queue else {}

            if queue:
                if level.lower() == "report":
                    create_report_notification_request_queue_detail(
                        queue, connection, job_input.get("organization_id")
                    )
                else:
                    create_request_queue_detail(queue, connection, job_type)

            update_query = ""
            if level.lower() == "attribute" and asset:
                update_query = f"""
                    update core.attribute set run_status='{ScheduleStatus.Pending.value}'
                    where id='{attribute_id}'
                """
            elif level.lower() == "measure":
                update_query = f"""
                    update core.measure set run_status='{ScheduleStatus.Pending.value}'
                    where id='{measure_id}'
                """
            elif job_type == ScheduleTypes.Report.value:
                update_query = f"""
                    update core.widget set run_status='{ScheduleStatus.Pending.value}'
                    where id='{report_id}'
                """
            elif level.lower() == "asset" and asset:
                update_query = f"""
                    update core.asset set run_status='{ScheduleStatus.Pending.value}'
                    where id='{asset_id}'
                """
            if update_query:
                execute_query(connection, cursor, update_query)
    except Exception as e:
        raise e


def create_report_notification_request_queue_detail(
    request_queue: dict, connection, organization_id
):
    try:
        dag_id = f"{str(organization_id)}_notification"
        request_queue = {
            "id": str(uuid4()),
            "category": "notification",
            "dag_id": dag_id,
            "task_id": "",
            "status": ScheduleStatus.Pending.value,
            "queue": str(request_queue.get("id")),
            "is_submitted": False,
        }

        with connection.cursor() as cursor:
            query_input = tuple(request_queue.values())
            input_literals = ", ".join(["%s"] * len(query_input))
            query_param = cursor.mogrify(
                f"({input_literals},CURRENT_TIMESTAMP)",
                query_input,
            ).decode("utf-8")

            query_string = f"""
                        insert into core.request_queue_detail(
                            id, category, dag_id, task_id, status, queue_id, is_submitted, created_date
                        ) values {query_param} 
                    """
            cursor = execute_query(connection, cursor, query_string)

    except Exception as e:
        raise e


def create_request_queue_detail(
    request_queue: dict, connection: dict, job_type: str = ""
):
    try:
        dag_id = ""
        category = "reliability"
        organization_id = request_queue.get("job_input", {}).get("organization_id")
        admin_organization = request_queue.get("job_input", {}).get(
            "admin_organization"
        )
        organization_id = admin_organization if admin_organization else organization_id
        attribute_id = None
        measure_id = None
        if request_queue.get("level").lower() == "asset":
            category = "reliability"
            dag_id = f"{str(organization_id)}_reliability"
        elif request_queue.get("level").lower() == "attribute" and request_queue.get(
            "attributes"
        ):
            category = "profile"
            dag_id = f"{str(organization_id)}_profile"
            attribute = (
                request_queue.get("attributes")[0]
                if request_queue.get("attributes")
                else None
            )
            if attribute:
                attribute_id = attribute.get("id")
                attribute_id = attribute_id if attribute_id else None
        elif request_queue.get("level").lower() == "measure" and request_queue.get(
            "measures"
        ):
            attribute = (
                request_queue.get("attributes")[0]
                if request_queue.get("attributes")
                else None
            )
            if attribute:
                attribute_id = attribute.get("id")
                attribute_id = attribute_id if attribute_id else None
            measure = (
                request_queue.get("measures")[0]
                if request_queue.get("measures")
                else None
            )
            if measure:
                measure_id = measure.get("id")
                measure_id = measure_id if measure_id else None

            with connection.cursor() as cursor:
                query_string = (
                    f"""SELECT * FROM core.measure WHERE id = '{measure_id}'"""
                )
                cursor = execute_query(connection, cursor, query_string)
                measure = fetchone(cursor)
                connection_string = f"select * from core.base_measure where id='{measure.get('base_measure_id')}'"
                cursor = execute_query(connection, cursor, connection_string)
                base_measure_detail = fetchone(cursor)
                if base_measure_detail:
                    measure.update({"base_measure": base_measure_detail})

            category = measure.get("base_measure", {}).get("type")
            if (
                measure.get("base_measure", {}).get("type") == "custom"
                and measure.get("base_measure", {}).get("category") == "behvioral"
            ):
                category = "behvioral"
            dag_id = f"{str(organization_id)}_{category}"
        else:
            dag_id = ""

        if job_type in [ScheduleTypes.Semantic.value, ScheduleTypes.Metadata.value]:
            category = job_type
            dag_id = f"{str(organization_id)}_{job_type}"

        if not dag_id:
            return

        request_queue = {
            "id": str(uuid4()),
            "category": category,
            "dag_id": dag_id,
            "task_id": "",
            "status": ScheduleStatus.Pending.value,
            "queue": str(request_queue.get("id")),
            "attribute": attribute_id,
            "measure": measure_id,
            "is_submitted": False,
        }

        with connection.cursor() as cursor:
            query_input = tuple(request_queue.values())
            input_literals = ", ".join(["%s"] * len(query_input))
            query_param = cursor.mogrify(
                f"({input_literals},CURRENT_TIMESTAMP)",
                query_input,
            ).decode("utf-8")

            query_string = f"""
                        insert into core.request_queue_detail(
                            id, category, dag_id, task_id, status, queue_id,
                            attribute_id, measure_id, is_submitted, created_date
                        ) values {query_param} 
                    """
            cursor = execute_query(connection, cursor, query_string)

    except Exception as e:
        raise e


def prepare_semantic_dag_info(config: dict, asset: dict, connection: dict) -> dict:
    dag_info = {}
    with connection.cursor() as cursor:
        query_string = f"""SELECT * FROM core.connection WHERE is_default = true"""
        cursor = execute_query(connection, cursor, query_string)
        default_connection = fetchone(cursor)
    postgres_connection_id = (
        default_connection.get("airflow_connection_id")
        if default_connection and default_connection.get("airflow_connection_id")
        else None
    )
    if not postgres_connection_id:
        return dag_info

    organization_id = str(default_connection.get("organization_id", ""))
    admin_organization = (
        str(default_connection.get("organization", {}).get("admin_organization"))
        if default_connection.get("organization", {}).get("admin_organization")
        else None
    )
    with connection.cursor() as cursor:
        query_string = f"""SELECT * FROM core.settings WHERE organization_id = '{organization_id}'"""
        cursor = execute_query(connection, cursor, query_string)
        organization_settings = fetchone(cursor)
    window = 7
    if organization_settings and organization_settings.get("anomaly"):
        window = organization_settings.get("anomaly").get("minimum")
        window = int(window) if window else 3

    dag_config = {
        "organization_id": str(organization_id),
        "core_connection_id": postgres_connection_id,
        "admin_organization": admin_organization,
        "window": window,
        "frequency": 1,
    }

    asset_id = None
    connection_id = None
    if asset:
        asset_id = str(asset.get("id"))
        connection_id = str(asset.get("connection_id"))
        dag_config.update(
            {
                "type": asset.get(type),
                "asset_id": asset_id,
                "connection_id": connection_id,
            }
        )

    dag_info = {
        "input_config": dag_config,
        "asset_id": asset_id,
        "connection_id": connection_id,
        "admin_organization": admin_organization,
        "organization_id": organization_id,
    }
    return dag_info


def prepare_measure_dag_info(config: dict, measure: dict, connection: dict) -> dict:
    dag_info = {}
    organization_id: str = str(measure.get("organization_id"))
    connection_id: str = str(measure.get("connection_id"))
    measure_id: str = str(measure.get("id"))
    with connection.cursor() as cursor:
        query_string = f"""SELECT * FROM core.connection WHERE is_default = true"""
        cursor = execute_query(connection, cursor, query_string)
        default_connection = fetchone(cursor)
    postgres_connection_id = (
        default_connection.get("airflow_connection_id")
        if default_connection and default_connection.get("airflow_connection_id")
        else None
    )
    if not postgres_connection_id:
        return dag_info

    admin_organization = (
        str(default_connection.get("organization", {}).get("admin_organization"))
        if default_connection.get("organization", {}).get("admin_organization")
        else None
    )

    with connection.cursor() as cursor:
        query_string = f"""SELECT * FROM core.settings WHERE organization_id = '{organization_id}'"""
        cursor = execute_query(connection, cursor, query_string)
        organization_settings = fetchone(cursor)
    window = 7
    if organization_settings and organization_settings.get("anomaly"):
        window = organization_settings.get("anomaly").get("minimum")
        window = int(window) if window else 3

    dag_config = {
        "technical_name": measure.get("technical_name"),
        "measure_id": measure_id,
        "connection_id": str(connection_id),
        "organization_id": str(organization_id),
        "core_connection_id": postgres_connection_id,
        "connection_type": measure.get("connection").get("type"),
        "source_connection_id": measure.get("connection").get("airflow_connection_id"),
        "admin_organization": admin_organization,
        "window": window,
        "frequency": 1,
    }

    dag_info = {
        "input_config": dag_config,
        "measure_id": measure_id,
        "connection_id": str(connection_id),
        "admin_organization": admin_organization,
        "organization_id": str(organization_id),
    }
    return dag_info


def prepare_report_notification_dag_info(
    config: dict, report: dict, connection: dict
) -> dict:
    dag_info = {}
    organization_id: str = str(report.get("organization_id"))
    report_id: str = str(report.get("id"))

    with connection.cursor() as cursor:
        query_string = f"""SELECT * FROM core.connection WHERE is_default = true"""
        cursor = execute_query(connection, cursor, query_string)
        default_connection = fetchone(cursor)
    postgres_connection_id = (
        default_connection.get("airflow_connection_id")
        if default_connection and default_connection.get("airflow_connection_id")
        else None
    )
    if not postgres_connection_id:
        return dag_info

    admin_organization = (
        str(default_connection.get("organization", {}).get("admin_organization"))
        if default_connection.get("organization", {}).get("admin_organization")
        else None
    )

    dag_config = {
        "technical_name": report.get("name"),
        "report_id": report_id,
        "organization_id": str(organization_id),
        "core_connection_id": postgres_connection_id,
        "admin_organization": admin_organization,
    }

    dag_info = {
        "input_config": dag_config,
        "report_id": report_id,
        "admin_organization": admin_organization,
        "organization_id": str(organization_id),
    }
    return dag_info


def prepare_dag_info(config: dict, asset: dict, connection: dict) -> dict:
    dag_info = {}
    organization_id: str = str(asset.get("organization_id"))
    connection_id: str = str(asset.get("connection_id"))
    asset_id: str = str(asset.get("id"))

    with connection.cursor() as cursor:
        query_string = f"""SELECT * FROM core.connection WHERE is_default = true"""
        cursor = execute_query(connection, cursor, query_string)
        default_connection = fetchone(cursor)
    postgres_connection_id = (
        default_connection.get("airflow_connection_id")
        if default_connection and default_connection.get("airflow_connection_id")
        else None
    )
    if not postgres_connection_id:
        return dag_info

    admin_organization = (
        str(asset.get("organization", {}).get("admin_organization"))
        if asset.get("organization", {}).get("admin_organization")
        else None
    )
    with connection.cursor() as cursor:
        query_string = f"""SELECT * FROM core.settings WHERE organization_id = '{organization_id}'"""
        cursor = execute_query(connection, cursor, query_string)
        organization_settings = fetchone(cursor)
    window = 7
    if organization_settings and organization_settings.get("anomaly"):
        window = organization_settings.get("anomaly").get("minimum")
        window = int(window) if window else 3
    dag_config = {
        "table_technical_name": get_asset_technical_name(asset),
        "technical_name": asset.get("technical_name"),
        "table_name": get_table_name(config, asset),
        "schema": get_schema_name(config, asset),
        "type": asset.get("type"),
        "asset_id": asset_id,
        "connection_id": str(connection_id),
        "organization_id": str(organization_id),
        "core_connection_id": postgres_connection_id,
        "connection_type": asset.get("connection", {}).get("type"),
        "source_connection_id": asset.get("connection", {}).get(
            "airflow_connection_id"
        ),
        "admin_organization": admin_organization,
        "window": window,
        "frequency": 1,
    }

    dag_info = {
        "input_config": dag_config,
        "asset_id": asset_id,
        "connection_id": str(connection_id),
        "admin_organization": admin_organization,
        "organization_id": str(organization_id),
    }
    return dag_info


def prepare_metadata_dag_info(config: dict, connection: dict) -> dict:
    dag_info = {}
    with connection.cursor() as cursor:
        query_string = f"""SELECT * FROM core.connection WHERE is_default = true"""
        cursor = execute_query(connection, cursor, query_string)
        default_connection = fetchone(cursor)

    postgres_connection_id = (
        default_connection.get("airflow_connection_id")
        if default_connection and default_connection.get("airflow_connection_id")
        else None
    )

    organization_id = default_connection.get("organization_id")
    if not postgres_connection_id:
        return dag_info

    admin_organization = (
        str(default_connection.get("organization", {}).get("admin_organization"))
        if default_connection.get("organization", {}).get("admin_organization")
        else None
    )

    with connection.cursor() as cursor:
        query_string = f"""SELECT * FROM core.settings WHERE organization_id = '{organization_id}'"""
        cursor = execute_query(connection, cursor, query_string)
        organization_settings = fetchone(cursor)

    report_settings = (
        organization_settings.get("reporting") if organization_settings else {}
    )
    connection_id = report_settings.get("connection", {}).get("id", None)
    if not connection_id:
        return dag_info

    with connection.cursor() as cursor:
        query_string = f"""SELECT * FROM core.connection WHERE id = '{connection_id}'"""
        cursor = execute_query(connection, cursor, query_string)
        metadata_connection = fetchone(cursor)

    if not metadata_connection:
        return dag_info

    dag_config = {
        "organization_id": str(organization_id),
        "core_connection_id": postgres_connection_id,
        "admin_organization": admin_organization,
        "connection_id": str(connection_id),
        "connection_type": metadata_connection.get("type"),
        "source_connection_id": metadata_connection.get("airflow_connection_id"),
        "frequency": 1,
    }

    dag_info = {
        "input_config": dag_config,
        "admin_organization": admin_organization,
        "organization_id": str(organization_id),
    }
    return dag_info


def get_table_name(config: dict, asset: dict) -> str:
    """
    Returns the table name for the given asset
    """
    connection_type: str = asset.get("connection", {}).get("type", "").lower()
    asset_type: str = asset.get("type", "")
    if connection_type == ConnectionType.Snowflake.value:
        table = asset.get("name")
        if str(asset_type).lower() == "query":
            table = asset.get("technical_name")
        schema = get_schema_name(config, asset)
        table_name = f'"{schema}"."{table}"'
        db_name = get_db_name(config, asset)
        if db_name:
            table_name = f'"{db_name}"."{schema}"."{table}"'
        return table_name
    elif connection_type == ConnectionType.MSSQL.value:
        table = asset.get("name")
        if str(asset_type).lower() == "query":
            table = asset.get("technical_name")
        schema = get_schema_name(config, asset)
        table_name = f"[{schema}].[{table}]"
        db_name = get_db_name(config, asset)
        if db_name:
            table_name = f'"{db_name}"."{schema}"."{table}"'
        return table_name
    elif connection_type == ConnectionType.Synapse.value:
        table = asset.get("technical_name")
        schema = get_schema_name(config, asset)
        table_name = f"[{schema}].[{table}]"
        return table_name
    elif connection_type == ConnectionType.AWS.value:
        table = asset.get("technical_name")
        schema = get_schema_name(config, asset)
        table_name = f'"{schema}"."{table}"'
        db_name = get_db_name(config, asset)
        if db_name:
            table_name = f'"{db_name}"."{schema}"."{table}"'
        return table_name
    elif connection_type == ConnectionType.Redshift.value:
        table = asset.get("technical_name")
        schema = get_schema_name(config, asset)
        table_name = f"[{schema}].[{table}]"
        db_name = get_db_name(config, asset)
        if db_name:
            table_name = f'"{db_name}"."{schema}"."{table}"'
        return table_name
    elif connection_type == ConnectionType.Redshift_Spectrum.value:
        table = asset.get("technical_name")
        schema = get_schema_name(config, asset)
        table_name = f"[{schema}].[{table}]"
        db_name = get_db_name(config, asset)
        if db_name:
            table_name = f'"{db_name}"."{schema}"."{table}"'
        return table_name
    elif connection_type == ConnectionType.Oracle.value:
        table = asset.get("technical_name")
        schema = get_schema_name(config, asset)
        table_name = f'"{schema}"."{table}"'
        return table_name
    elif connection_type == ConnectionType.MySql.value:
        table = asset.get("technical_name")
        schema = get_schema_name(config, asset)
        table_name = f"{schema}.{table}"
        return table_name
    elif connection_type in [
        ConnectionType.Postgres.value,
        ConnectionType.AlloyDB.value,
    ]:
        table = asset.get("technical_name")
        schema = get_schema_name(config, asset)
        table_name = f'"{schema}"."{table}"'
        db_name = get_db_name(config, asset)
        if db_name:
            table_name = f'"{db_name}"."{schema}"."{table}"'
        return table_name
    elif connection_type == ConnectionType.MongoDB.value:
        table = asset.get("technical_name")
        schema = get_schema_name(config, asset)
        table_name = f"{schema}.{table}"
        return table_name
    elif connection_type == ConnectionType.BigQuery.value:
        table = asset.get("name")
        if str(asset_type).lower() == "query":
            table = asset.get("technical_name")
        schema = get_schema_name(config, asset)
        table_name = f"`{schema}`.`{table}`"
        db_name = get_db_name(config, asset)
        if db_name:
            table_name = f'"{db_name}"."{schema}"."{table}"'
        return table_name
    elif connection_type == ConnectionType.Denodo.value:
        table = asset.get("technical_name")
        asset_type = asset.get("type")
        schema = get_schema_name(config, asset)
        table_name = f'"{schema}"."{table}"'
        asset_type = asset.get("type") if asset else ""
        asset_type = asset_type.lower() if asset_type else ""
        is_query_mode = bool(asset_type.lower() == "query")
        if is_query_mode:
            table_name = table
        return table_name
    elif connection_type == ConnectionType.Databricks.value:
        table = asset.get("technical_name")
        schema = get_schema_name(config, asset)
        database = get_database_name(config, asset)
        table_name = f"`{database}`.`{schema}`.`{table}`"
        db_name = get_db_name(config, asset)
        if db_name:
            table_name = f'"{db_name}"."{schema}"."{table}"'
        return table_name
    elif (connection_type == ConnectionType.Db2.value) or (
        connection_type == ConnectionType.DB2IBM.value
    ):
        table = asset.get("technical_name")
        schema = get_schema_name(config, asset)
        table_name = f'"{schema}"."{table}"'
        return table_name
    elif connection_type == ConnectionType.SapHana.value:
        table = asset.get("technical_name")
        schema = get_schema_name(config, asset)
        table_name = f'"{schema}"."{table}"'
        return table_name
    elif connection_type == ConnectionType.SapEcc.value:
        table = asset.get("technical_name")
        table_name = f'"{table}"'
        return table_name
    elif connection_type == ConnectionType.Athena.value:
        table = asset.get("technical_name")
        schema = get_schema_name(config, asset)
        table_name = f'"{schema}"."{table}"'
        return table_name
    elif connection_type == ConnectionType.EmrSpark.value:
        table = asset.get("technical_name")
        schema = get_schema_name(config, asset)
        table_name = f'"{schema}"."{table}"'
        return table_name
    elif connection_type == ConnectionType.Teradata.value:
        table = asset.get("technical_name")
        schema = get_schema_name(config, asset)
        table_name = (
            f'"{schema.strip()}"."{table.strip()}"' if schema else f'"{table.strip()}"'
        )
        return table_name
    elif connection_type == ConnectionType.Hive.value:
        table = asset.get("technical_name")
        schema = get_schema_name(config, asset)
        table_name = f"`{schema}`.`{table}`"
        return table_name
    elif connection_type == ConnectionType.AlloyDB.value:
        table = asset.get("technical_name")
        schema = get_schema_name(config, asset)
        table_name = f'"{schema}"."{table}"'
        db_name = get_db_name(config, asset)
        if db_name:
            table_name = f'"{db_name}"."{schema}"."{table}"'
        return table_name
    elif connection_type in [ConnectionType.ADLS.value, ConnectionType.File.value, ConnectionType.S3.value]:
        table = f"{asset.get('technical_name')}_{asset.get('id')}"

        # Get external storage configuration
        external_storage_config = get_external_storage_config(config)
        spark_catalog = external_storage_config.get("spark_catalog", "")
        spark_namespace = external_storage_config.get("spark_namespace", "")

         # Get Livy Spark configuration
        dag_info = config.get("dag_info", {})
        livy_spark_config = dag_info.get("livy_spark_config", {})
        livy_spark_config = livy_spark_config if livy_spark_config else {}
        iceberg_catalog = livy_spark_config.get("catalog")
        iceberg_namespace = livy_spark_config.get("namespace")

        # Use Spark catalog and namespace if available, otherwise use Iceberg catalog and namespace
        iceberg_catalog = spark_catalog if spark_catalog else iceberg_catalog
        iceberg_namespace = spark_namespace if spark_namespace else iceberg_namespace

        table_name = f"{iceberg_catalog}.{iceberg_namespace}.{table}"
        return table_name
    else:
        return None


def get_database_name(config: dict, asset: dict) -> str:
    """
    Retruns the database name for the given asset
    """
    connection_type: str = asset.get("connection", {}).get("type", "").lower()
    database = asset["connection"]["credentials"].get("database")
    if connection_type == ConnectionType.Databricks.value:
        database = database if database else "main"
        return database
    else:
        return None


def get_schema_name(config: dict, asset: dict) -> str:
    """
    Returns the schema name for the given asset
    """
    connection_type: str = asset.get("connection", {}).get("type", "").lower()
    schema = asset.get("properties", {}).get("schema")
    if connection_type == ConnectionType.Snowflake.value:
        schema = schema if schema else "public"
        return schema
    elif connection_type == ConnectionType.MSSQL.value:
        schema = schema if schema else "dbo"
        return schema
    elif connection_type == ConnectionType.Synapse.value:
        schema = schema if schema else "dbo"
        return schema
    elif connection_type == ConnectionType.AWS.value:
        schema = (
            schema if schema else "myspectrum_schema"
        )  # this is defined because redshift is going to work on catalogued tables
        return schema
    elif connection_type == ConnectionType.Redshift.value:
        schema = schema if schema else "public"
        return schema
    elif connection_type == ConnectionType.Redshift_Spectrum.value:
        schema = schema if schema else "public"
        return schema
    elif connection_type == ConnectionType.Oracle.value:
        schema = schema if schema else "SYS"
        return schema
    elif connection_type == ConnectionType.MySql.value:
        schema = schema if schema else "mydb"
        return schema
    elif connection_type in [
        ConnectionType.Postgres.value,
        ConnectionType.AlloyDB.value,
    ]:
        schema = schema if schema else "public"
        return schema
    elif connection_type == ConnectionType.MongoDB.value:
        schema = schema if schema else "public"
        return schema
    elif connection_type == ConnectionType.BigQuery.value:
        schema = schema if schema else "public"
        return schema
    elif connection_type == ConnectionType.Denodo.value:
        schema = schema if schema else "admin"
        return schema
    elif connection_type == ConnectionType.Databricks.value:
        schema = schema if schema else "default"
        return schema
    elif (connection_type == ConnectionType.Db2.value) or (
        connection_type == ConnectionType.DB2IBM.value
    ):
        schema = schema if schema else ""
        return schema
    elif connection_type == ConnectionType.SapHana.value:
        schema = schema if schema else ""
        return schema
    elif connection_type == ConnectionType.Athena.value:
        schema = schema if schema else "public"
        return schema
    elif connection_type == ConnectionType.EmrSpark.value:
        schema = schema if schema else "public"
        return schema
    elif connection_type == ConnectionType.Teradata.value:
        schema = asset.get("properties", {}).get("database")
        schema = schema.strip() if schema else ""
        return schema
    elif connection_type == ConnectionType.Hive.value:
        schema = schema if schema else "default"
        return schema
    elif connection_type == ConnectionType.SapEcc.value:
        schema = schema if schema else ""
        return schema
    else:
        return None


def get_db_name(config: dict, asset: dict) -> str:
    """
    Returns the schema name for the given asset
    """
    connection = asset.get("connection", {})
    connection_type: str = asset.get("connection", {}).get("type", "").lower()
    asset_type: str = asset.get("type", "").lower()
    properties = asset.get("properties", {})
    properties = json.loads(properties) if isinstance(properties, str) else properties
    properties = properties if properties else {}
    database = connection.get("credentials", {}).get("database", "")
    if asset_type == "query":
        general_settings = config.get("settings", {})
        if not general_settings:
            dag_info = config.get("dag_info", {})
            dag_info = dag_info if dag_info else {}
            general_settings = dag_info.get("settings", {})
        general_settings = general_settings if general_settings else {}
        general_settings = (
            json.loads(general_settings, default=str)
            if isinstance(general_settings, str)
            else general_settings
        )

        profile_settings = general_settings.get("profile")
        profile_settings = profile_settings if profile_settings else {}
        profile_settings = (
            json.loads(profile_settings, default=str)
            if isinstance(profile_settings, str)
            else profile_settings
        )
        profile_settings = profile_settings if profile_settings else {}

        profile_database_name = profile_settings.get("database")
        profile_database_name = profile_database_name if profile_database_name else ""

        database_name = properties.get("database_name")
        database_name = (
            profile_database_name if profile_database_name else database_name
        )
        database = database_name if database_name else database

    database = database if database else ""
    if connection_type in [
        ConnectionType.Teradata.value,
        ConnectionType.Denodo.value,
        ConnectionType.MongoDB.value,
    ]:
        return None
    return database


def get_asset_technical_name(asset: dict) -> str:
    technical_name: str = asset.get("technical_name", "")
    connection_type: str = asset.get("connection", {}).get("type", "").lower()
    asset_type: str = asset.get("type", "").lower()
    asset_name: str = asset.get("name", "")
    if (
        connection_type
        in [
            ConnectionType.Snowflake.value,
            ConnectionType.MSSQL.value,
            ConnectionType.BigQuery.value,
        ]
        and asset_type != "query"
    ):
        technical_name = asset_name
    return technical_name
