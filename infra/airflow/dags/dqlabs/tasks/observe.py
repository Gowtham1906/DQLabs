import json
from copy import deepcopy
from dqlabs.app_helper.dag_helper import (
    execute_native_query,
    get_native_connection,
    get_postgres_connection,
)
from dqlabs.enums.connection_types import ConnectionType
from dqlabs.utils.tasks import get_task_config, check_task_status
from dqlabs.utils.request_queue import (
    update_queue_detail_status,
    update_queue_status,
)
from dqlabs.utils.extract_workflow import (
    get_queries,
)
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.utils.observe import (
    get_databases,
    save_observe_metrics,
    get_selected_assets,
)
from dqlabs.tasks.check_alerts import check_alerts
from dqlabs.tasks.update_threshold import update_threshold
from dqlabs.app_constants.dq_constants import OBSERVE

from dqlabs.app_helper.db_helper import (
    fetchall,
)


def extract_metadata(
    config: dict,
    database: str,
    source_connection: object,
    observe_queries: dict,
    is_reliability: bool = False,
    **kwargs,
) -> None:
    metadata = []
    metadata_query = observe_queries.get("metadata", "")
    metadata_query = metadata_query if metadata_query else ""
    schema_filter_query = kwargs.get("schema_filter_query")
    schema_filter_query = schema_filter_query if schema_filter_query else ""
    database_filter_query = kwargs.get("database_filter_query")
    database_filter_query = database_filter_query if database_filter_query else ""
    selected_assets = kwargs.get("selected_assets")
    selected_assets = selected_assets if selected_assets else []
    last_observed = kwargs.get("last_observed")
    last_observed = last_observed if last_observed else ""
    if not metadata_query:
        return metadata

    metadata_query = (
        metadata_query.replace("<database>", database)
        .replace("<database_filter>", database_filter_query)
        .replace("<schema_filter>", schema_filter_query)
    )
    response, native_connection = execute_native_query(
        config, metadata_query, source_connection, is_list=True
    )
    metadata = response if response else []
    if not source_connection and native_connection:
        source_connection = native_connection
    is_processed = save_observe_metrics(
        config,
        metadata,
        is_reliability=is_reliability,
        selected_assets=selected_assets,
        observe_queries=observe_queries,
    )
    return metadata, is_processed


def extract_observe_measures(config: dict, **kwargs) -> None:
    is_completed = check_task_status(config, kwargs)
    if is_completed:
        return

    task_config = get_task_config(config, kwargs)
    update_queue_detail_status(
        config, ScheduleStatus.Running.value, task_config=task_config
    )
    update_queue_status(config, ScheduleStatus.Running.value, True)
    connection_type = config.get("connection_type")
    default_queries = get_queries(config)
    if default_queries:
        config.update({"default_queries": default_queries})
    observe_queries = default_queries.get("observe", {})
    observe_queries = observe_queries if observe_queries else {}
    if not observe_queries:
        error_message = (
            f"Observability not supported for connection type: {connection_type}"
        )
        raise ValueError(error_message)

    connection = config.get("connection")
    connection = connection if connection else {}
    last_observed = connection.get("last_observed")
    last_observed = str(last_observed) if last_observed else None
    last_observed_time = ""
    if last_observed:
        last_observed_time = last_observed.replace("Z", "+00:00")
    if not connection_type and connection:
        connection_type = connection.get("type")

    credentials = connection.get("credentials")
    credentials = (
        json.loads(credentials)
        if credentials and isinstance(credentials, str)
        else credentials
    )
    credentials = credentials if credentials else {}
    is_enabled_selected_assets = bool(credentials.get("enable_selected_assets", False))
    is_observe_enabled = bool(credentials.get("observe", False))
    if not is_observe_enabled:
        error_message = "Observability is not enabled for this connection. Please check the connection configuration to enable observability."
        raise ValueError(error_message)

    source_connection = get_native_connection(config)
    source_connection = (
        source_connection
        if source_connection
        and connection_type.lower() in [ConnectionType.Snowflake.value]
        else None
    )

    config.update(
        {
            "is_enabled_selected_assets": is_enabled_selected_assets,
            "is_observe_enabled": is_observe_enabled,
            "last_observed": last_observed_time,
        }
    )
    is_processed = False
    if is_enabled_selected_assets:
        selected_asset_schema_filter = observe_queries.get(
            "selected_asset_schema_filter"
        )
        selected_assets = get_selected_assets(config)
        grouped_assets = {}
        for asset in selected_assets:
            asset_properties = asset.get("properties")
            asset_properties = (
                json.loads(asset_properties)
                if asset_properties and isinstance(asset_properties, str)
                else asset_properties
            )
            asset_properties = asset_properties if asset_properties else {}
            database = asset_properties.get("database")
            schema = asset_properties.get("schema")
            table_name = asset.get("name")
            if not database or not schema:
                continue
            if database not in grouped_assets:
                grouped_assets[database] = {}
            if schema not in grouped_assets[database]:
                grouped_assets[database][schema] = []
            grouped_assets[database][schema].append(table_name)

        # Iterate through the grouped assets and extract metadata
        for database, schemas in grouped_assets.items():
            if not database:
                continue
            for schema, tables in schemas.items():
                tables = list(set(tables)) if tables else []
                if not tables:
                    continue

                table_names = ", ".join(f"'{table}'" for table in tables if table)
                schema_filter_query = deepcopy(selected_asset_schema_filter)
                schema_filter_query = (
                    schema_filter_query.replace("<schema_name>", schema)
                    .replace("<selected_tables>", table_names)
                    .replace("<database_name>", database)
                )
                schema_filter_query = (
                    f" AND {schema_filter_query}" if schema_filter_query else ""
                )
                _, is_observed = extract_metadata(
                    config,
                    database,
                    source_connection,
                    observe_queries,
                    schema_filter_query=schema_filter_query,
                    selected_assets=selected_assets,
                )
                if not is_processed and is_observed:
                    is_processed = is_observed
    else:
        databases, schema_filter_query = get_databases(
            config,
            credentials,
            observe_queries,
            source_connection,
        )

        if connection_type in [ConnectionType.Snowflake.value]:
            for database in databases:
                if not database:
                    continue

                _, is_observed = extract_metadata(
                    config,
                    database,
                    source_connection,
                    observe_queries,
                    schema_filter_query=schema_filter_query,
                    last_observed=last_observed_time,
                )
                if not is_processed and is_observed:
                    is_processed = is_observed
        elif databases:
            database = ""
            database_values = databases if databases else []
            database_values = list(set(database_values)) if database_values else []
            database_values = ", ".join(
                f"'{database}'" for database in database_values if database
            )
            database_values = database_values if database_values else ""

            database_filter_query = observe_queries.get("database_filter")
            database_filter_query = (
                database_filter_query if database_filter_query else ""
            )
            database_filter_query = database_filter_query.replace(
                "<values>", database_values
            )
            _, is_processed = extract_metadata(
                config,
                database,
                source_connection,
                observe_queries,
                database_filter_query=database_filter_query,
                schema_filter_query=schema_filter_query,
                last_observed=last_observed_time,
            )
        else:
            is_processed = False

    if is_processed:
        connection_id = config.get("connection_id")
        connection = get_postgres_connection(config)
        processed_assets = []
        with connection.cursor() as cursor:
            query_string = f"""
                select id from core.asset
                where connection_id='{connection_id}' and is_active=True and is_delete=false
            """
            cursor.execute(query_string)
            processed_assets = fetchall(cursor)
            processed_assets = [str(item['id']) for item in processed_assets]
            processed_assets = processed_assets if processed_assets else []
        # Check for the alerts for the current run
        for asset_id in processed_assets:
            config.update({"is_asset": True, "job_type": OBSERVE, "measure_id": None, "asset_id": asset_id})
            check_alerts(config)
            # # Update the threshold value based on the drift config
            update_threshold(config)
    update_queue_detail_status(config, ScheduleStatus.Completed.value)
    update_queue_status(config, ScheduleStatus.Completed.value)
