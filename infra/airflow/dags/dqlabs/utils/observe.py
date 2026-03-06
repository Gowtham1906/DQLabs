import json
import fnmatch
import re
import pytz
from datetime import datetime, timezone
from uuid import uuid4
from copy import deepcopy
from dqlabs.app_helper.dag_helper import (
    execute_native_query,
    execute_query,
    get_postgres_connection,
    get_native_connection,
    delete_metrics,
)
from dqlabs.app_helper.db_helper import (
    fetchall,
    fetchone,
    split_queries,
)
from dqlabs.app_helper.dq_helper import (
    get_derived_type,
)
from dqlabs.app_constants.dq_constants import PASSED, RELIABILITY
from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.enums.connection_types import ConnectionType
from dqlabs.enums.approval_status import ApprovalStatus


def filter_databases(databases: list, database_config: dict):
    """
    Filters a list of databases based on inclusion and exclusion criteria
    specified in the database configuration.

    Args:
        databases (list): A list of available database names.

    Returns:
        list: A list of database names that match the inclusion criteria and
        do not match the exclusion criteria.
    """
    included_databases = database_config.get("included_databases", [])
    included_databases = included_databases if included_databases else []
    excluded_databases = database_config.get("excluded_databases", [])
    configured_dbs = database_config.get("configured_dbs", [])
    included_databases.extend(configured_dbs)
    included_databases = list(set(included_databases)) if included_databases else []

    selected_databases = []
    if included_databases:
        for included_db in included_databases:
            if included_db in databases:
                selected_databases.append(included_db)
            elif "*" in included_db:
                selected_databases.extend(
                    list(
                        filter(
                            lambda db: fnmatch.fnmatch(db, included_db),
                            databases,
                        )
                    )
                )

    if excluded_databases:
        for excluded_db in excluded_databases:
            if excluded_db in selected_databases:
                selected_databases.remove(excluded_db)
            elif "*" in excluded_db:
                selected_databases = list(
                    filter(
                        lambda db: not fnmatch.fnmatch(db, excluded_db),
                        selected_databases,
                    )
                )

    if not selected_databases and not included_databases and len(databases) > 0:
        selected_databases.extend(databases)
    selected_databases = list(set(selected_databases)) if selected_databases else []
    return selected_databases


def prepare_schema_filters(
    observe_queries: dict, schema_names: list, not_condition: str = ""
) -> list:
    """
    Prepares a list of schema filters based on the provided schema names and query templates.

    Args:
        observe_queries (dict): A dictionary containing query templates for schema filtering.
            Expected keys:
                - "schema_in_check": Template for "IN" condition queries.
                - "schema_like_check": Template for "LIKE" condition queries.
        schema_names (list): A list of schema names to filter. Wildcard patterns (e.g., "*")
            are supported and will be converted to SQL "LIKE" patterns.
        not_condition (str, optional): A string to prepend to the condition (e.g., "NOT").
            Defaults to an empty string.

    Returns:
        list: A list of schema filter queries constructed based on the input schema names
        and query templates.
    """
    schema_filters = []
    in_condition_query = observe_queries.get("schema_in_check")
    like_condition_query = observe_queries.get("schema_like_check")

    included_schema = []
    for schema in schema_names:
        if not schema:
            continue

        if "*" in schema:
            schema_name = schema.replace("*", "%")
            query = deepcopy(like_condition_query)
            query = query.replace("<not_condition>", not_condition).replace(
                "<value>", schema_name
            )
            schema_filters.append(query)
        else:
            included_schema.append(schema)

    included_schema = list(set(included_schema)) if included_schema else []
    schema_to_filter = ", ".join([f"'{schema}'" for schema in included_schema])
    schema_to_filter = f"({schema_to_filter})" if schema_to_filter else ""
    if schema_to_filter:
        schema_include_condition = deepcopy(in_condition_query)
        schema_include_condition = schema_include_condition.replace(
            "<not_condition>", not_condition
        ).replace("<value>", schema_to_filter)
        schema_filters.append(schema_include_condition)
    return schema_filters


def get_schema_filters(credentials: dict, observe_queries: dict):
    """
    Generate schema filters based on included and excluded schemas.

    Args:
        credentials (dict): A dictionary containing schema inclusion and exclusion
            details. Keys include:
            - "included_schema" (list): A list of schemas to include.
            - "excluded_schema" (list): A list of schemas to exclude.
        observe_queries (dict): A dictionary containing query templates or
            configurations used to prepare schema filters.

    Returns:
        list: A list of schema filters generated based on the included and excluded
        schemas. Each filter is prepared using the `observe_queries` and may include
        "NOT" conditions for exclusions.
    """
    schema_filters = []
    included_schema = credentials.get("included_schema", [])
    included_schema = (
        included_schema if included_schema else []
    )
    included_schema = list(set(included_schema)) if included_schema else []

    excluded_schema = credentials.get("excluded_schema", [])
    excluded_schema = (
        excluded_schema if excluded_schema else []
    )
    excluded_schema = list(set(excluded_schema)) if excluded_schema else []

    if included_schema:
        include_filters = prepare_schema_filters(observe_queries, included_schema)
        if include_filters:
            include_filters_query = " OR ".join(include_filters)
            include_filters_query = f"({include_filters_query})"
            schema_filters.append(include_filters_query)

    if excluded_schema:
        exclude_filters = prepare_schema_filters(
            observe_queries, excluded_schema, "NOT"
        )
        if exclude_filters:
            exclude_filters_query = " OR ".join(exclude_filters)
            exclude_filters_query = f"({exclude_filters_query})"
            schema_filters.append(exclude_filters_query)
    return schema_filters


def get_database_key(connection_type: str) -> str:
    """
    Returns the database key based on the connection type.

    Args:
        connection_type (str): The type of the connection (e.g., Snowflake, Postgres).

    Returns:
        str: The database key corresponding to the connection type.
    """
    db_key = "name"
    if connection_type in [
        ConnectionType.Databricks.value.lower(),
    ]:
        db_key = "catalog"
    return db_key


def get_databases(
    config: dict,
    credentials: dict,
    observe_queries: dict,
    source_connection: object,
    **kwargs,
) -> tuple:
    """
    Retrieves and filters a list of databases based on the provided configuration,
    credentials, and query definitions. Additionally, constructs a schema filter query.

    Args:
        config (dict): Configuration dictionary containing connection and query details.
        credentials (dict): Credentials dictionary used for filtering databases and schemas.
        observe_queries (dict): Dictionary containing query templates for database operations.
        source_connection (object): Optional source connection object. If not provided,
            a native connection will be created.
        **kwargs: Additional keyword arguments.

    Returns:
        tuple: A tuple containing:
            - filtered_databases (list): A list of filtered database names.
            - schema_filter_query (str): A query string for schema filtering.
    """
    connection_type = config.get("connection_type")
    connection_type = str(connection_type).lower() if connection_type else ""
    if not source_connection:
        source_connection = get_native_connection(config)
    kwargs = kwargs if kwargs else {}
    fetch_database_query = observe_queries.get("fetch_databases")
    response, native_connection = execute_native_query(
        config,
        fetch_database_query,
        source_connection,
        is_list=True,
    )
    if not source_connection and native_connection:
        source_connection = native_connection

    databases = response if response else []
    db_key = get_database_key(connection_type)
    databases = [database.get(db_key, "") for database in databases]
    filtered_databases = filter_databases(databases, credentials)
    filtered_databases = filtered_databases if filtered_databases else []

    schema_filters = get_schema_filters(credentials, observe_queries)
    schema_filter_query = " AND ".join(schema_filters) if schema_filters else ""
    schema_filter_query = f"AND ({schema_filter_query}) " if schema_filter_query else ""
    return filtered_databases, schema_filter_query


def update_observe_config(config: dict) -> None:
    """
    Updates the observe configuration in the database by setting the
    `last_observed` timestamp and `last_observe_run_id` for a specific connection.

    Args:
        config (dict): A dictionary containing the configuration details.
    """
    connection = config.get("connection")
    connection = connection if connection else {}
    connection_id = connection.get("id")
    run_id = config.get("run_id")
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            update core.connection
            set last_observed=CURRENT_TIMESTAMP, last_observe_run_id={run_id}
            where id='{connection_id}'
        """
        cursor.execute(query_string)


def get_assets_by_connection(config: dict) -> None:
    """
    Fetches the assets associated with a specific connection from the database.

    Args:
        config (dict): A dictionary containing the configuration details.

    Returns:
        list: A list of assets associated with the connection.
    """
    connection = config.get("connection")
    connection = connection if connection else {}
    connection_id = connection.get("id")
    connection = get_postgres_connection(config)
    response = []
    with connection.cursor() as cursor:
        query_string = f"""
            select id, name, properties
            from core.asset
            where connection_id='{connection_id}' and is_active=true and is_delete=false
        """
        cursor.execute(query_string)
        response = fetchall(cursor)
        response = response if response else []
    return response


def get_technical_name(
    name: str,
    include_integer: bool = True,
    include_dollar: bool = False,
    include_special_char: bool = False,
) -> str:
    """
    Returns the technical name for the given name
    """
    technical_name = ""
    if not name:
        return technical_name

    if include_dollar:
        regex = "[^A-Za-z_0-9_$]" if include_integer else "[^A-Za-z_$]"
    elif include_special_char:
        regex = (
            "[^A-Za-z_0-9_$.!@#$%^&*/']"
            if include_integer
            else "[^A-Za-z_$.!@#$%^&*/']"
        )
    else:
        regex = "[^A-Za-z_0-9]" if include_integer else "[^A-Za-z_]"
    technical_name = re.sub(regex, "_", name.strip()).strip()
    return str(technical_name)


def get_asset_unique_id(connection_id: str, connection_type: str, row: dict) -> str:
    """
    Generates a unique asset ID based on the connection type and row data.
    Args:
        connection_type (str): The type of the connection (e.g., Snowflake, Postgres).
        row (dict): A dictionary containing the row data.
    Returns:
        str: A unique asset ID.
    """
    asset_unique_id = ""
    database = row.get("database")
    database = database.lower() if database else ""
    schema = row.get("schema")
    schema = schema.lower() if schema else ""
    table_name = row.get("table_name")
    table_technical_name = get_technical_name(table_name.strip()) if table_name else ""
    table_technical_name = table_technical_name.lower() if table_technical_name else ""
    if connection_type in [
        ConnectionType.Snowflake.value.lower(),
        ConnectionType.Databricks.value.lower(),
    ]:
        asset_unique_id = f"{database}_{schema}_{table_technical_name}"
    asset_unique_id = (
        f"{str(connection_id)}_{asset_unique_id}" if asset_unique_id else ""
    )
    return asset_unique_id


def get_asset(config: dict, asset_unique_id: str) -> dict:
    """
    Fetches the asset details for a specific asset unique ID from the database.

    Args:
        config (dict): A dictionary containing the configuration details.
        asset_unique_id (str): The unique ID of the asset.

    Returns:
        dict: A dictionary containing the asset details.
    """
    response = {}
    connection_id = config.get("connection_id")
    connection = get_postgres_connection(config)

    with connection.cursor() as cursor:
        query_string = f"""
            select asset.*, data.id as data_id from core.asset
            join core.data on data.asset_id=asset.id
            where asset.is_active=true and asset.is_delete=false
            and asset.unique_id='{asset_unique_id}' and asset.connection_id='{connection_id}'
        """
        cursor.execute(query_string)
        response = fetchone(cursor)
        response = response if response else {}
    return response


def get_observe_measures(config: dict, asset_id: str, is_fingerprint_asset: bool = False) -> None:
    """
    Fetches the observability measures for a specific connection type from the database.

    Args:
        config (dict): A dictionary containing the configuration details.
        **kwargs: Additional keyword arguments.

    Returns:
        list: A list of observability measures.
    """
    connection = get_postgres_connection(config)
    measures = []
    if is_fingerprint_asset:
        measure_filter= f"""and LOWER(base.name) in ('schema')"""
    else:
        measure_filter = f"""and LOWER(base.name) in ('schema', 'volume', 'freshness')"""  
    with connection.cursor() as cursor:
        query_string = f"""
            select mes.id as id, base.id as base_measure_id, base.technical_name as name, base.query, base.properties, base.type, base.level, base.category, base.derived_type,
            mes.allow_score, mes.is_drift_enabled, mes.attribute_id, mes.asset_id, mes.is_positive, mes.drift_threshold, base.term_id, mes.semantic_measure, mes.semantic_query,
            mes.weightage, mes.enable_pass_criteria, mes.pass_criteria_threshold, mes.pass_criteria_condition
            from core.measure as mes
            join core.base_measure as base on base.id = mes.base_measure_id and base.is_visible=true
            where mes.is_active=True and mes.is_delete=false
            and base.type='{RELIABILITY}' {measure_filter} and mes.asset_id='{str(asset_id)}'
        """
        cursor.execute(query_string)
        measures = fetchall(cursor)
        measures = measures if measures else []
    return measures


def get_schema_update_history(
    config: dict, previous_schema: dict, current_schema: dict, **kwargs
) -> dict:
    """
    Compares two schema dictionaries and identifies changes between them.

    Args:
        previous_schema (dict): The schema dictionary representing the previous state.
                                Each entry should contain details such as "column_name",
                                "datatype", and "position".
        current_schema (dict): The schema dictionary representing the current state.
                               Each entry should contain details such as "column_name",
                               "datatype", and "position".

    Returns:
        dictionary: A tuple containing a dictionary with the following keys:
            - "added": A list of columns that were added in the current schema.
            - "removed": A list of columns that were removed from the previous schema.
            - "renamed": A list of columns whose names were changed.
            - "datatype_changed": A list of columns whose data types were changed.
    """

    is_first_run: bool = kwargs.get("is_first_run", False)
    asset_id: bool = kwargs.get("asset_id", False)

    update_history = {
        "added": [],
        "removed": [],
        "renamed": [],
        "datatype_changed": [],
    }

    if previous_schema:
        previous_schema = previous_schema if previous_schema else []
        previous_schema = sorted(previous_schema, key=lambda x: x.get("position", 0))
        current_schema = current_schema if current_schema else []
        current_schema = sorted(current_schema, key=lambda x: x.get("position", 0))
        renamed_columns = []
        for column in current_schema:
            previous_column = next(
                (
                    prev_column
                    for prev_column in previous_schema
                    if (prev_column.get("position") == column.get("position"))
                ),
                None,
            )
            previous_column = previous_column if previous_column else {}

            previous_column_name = previous_column.get("column_name")
            current_column_name = column.get("column_name")
            previous_column_type = previous_column.get("datatype")
            current_column_type = column.get("datatype")
            previous_column_position = previous_column.get("position")
            current_column_position = column.get("position")
            if (
                previous_column_position == current_column_position
                and previous_column_name != current_column_name
            ):
                renamed_columns.extend([previous_column_name, current_column_name])
                update_history["renamed"].append(
                    {
                        "type": "renamed",
                        "previous": previous_column,
                        "current": column,
                    }
                )
                continue

            if not previous_column or (
                previous_column_position != current_column_position
                and previous_column_name == current_column_name
            ):
                update_history["added"].append(
                    {
                        "type": "added",
                        "previous": {},
                        "current": column,
                    }
                )
                continue

            if (
                previous_column_position == current_column_position
                and previous_column_name == current_column_name
                and previous_column_type != current_column_type
            ):
                update_history["datatype_changed"].append(
                    {
                        "type": "datatype_changed",
                        "previous": previous_column,
                        "current": column,
                    }
                )

        for column in previous_schema:
            current_column = next(
                (
                    curr_column
                    for curr_column in current_schema
                    if curr_column.get("position") == column.get("position")
                ),
                None,
            )
            if current_column:
                continue

            update_history["removed"].append({"type": "removed", "previous": column})
    else:
        is_first_run = True
        for column in current_schema:
            update_history["added"].append(
                {
                    "type": "added",
                    "previous": {},
                    "current": column,
                }
            )
        config = config if config else {}
        first_observed_assets = config.get("first_observed_assets", [])
        first_observed_assets = first_observed_assets if first_observed_assets else []
        first_observed_assets.append(asset_id)
        config.update({"first_observed_assets": first_observed_assets})

    updated_metrics = {
        f"{str(key).lower()}_count": len(value) if value else 0
        for key, value in update_history.items()
    }
    updated_metrics.update({"is_first_run": is_first_run})
    return update_history, updated_metrics


def check_is_allowed_asset(config: dict, row: dict, asset: dict) -> dict:
    """
    Checks if the asset is allowed to be processed based on the last observed metrics.
    """
    is_allowed = True
    asset = asset if asset else {}
    observe_metrics = asset.get("last_observed_metrics")
    observe_metrics = (
        json.loads(observe_metrics)
        if observe_metrics and isinstance(observe_metrics, str)
        else observe_metrics
    )
    observe_metrics = observe_metrics if observe_metrics else {}
    date_format = "%Y-%m-%d %H:%M:%S.%f%z"
    previous_last_altered = observe_metrics.get("last_altered")
    previous_last_altered = previous_last_altered if previous_last_altered else ""
    previous_last_altered = previous_last_altered.replace("Z", "+00:00")
    previous_last_altered = previous_last_altered.replace("T", " ").replace("t", " ")
    previous_last_altered_time = (
        datetime.strptime(previous_last_altered, date_format)
        if previous_last_altered
        else None
    )
    previous_last_altered = (
        datetime.strptime(previous_last_altered, date_format)
        if previous_last_altered
        else datetime.now(tz=pytz.UTC)
    )
    current_last_altered = row.get("last_altered")
    current_last_altered = str(current_last_altered) if current_last_altered else ""
    current_last_altered = current_last_altered.replace("Z", "+00:00")
    current_last_altered = current_last_altered.replace("T", " ").replace("t", " ")
    current_last_altered_time = (
        datetime.strptime(current_last_altered, date_format)
        if current_last_altered
        else None
    )
    current_last_altered = datetime.strptime(current_last_altered, date_format)

    is_enabled_selected_assets = config.get("is_enabled_selected_assets")
    if (
        is_enabled_selected_assets
        and previous_last_altered_time
        and current_last_altered_time
    ):
        is_allowed = not (
            is_enabled_selected_assets
            and previous_last_altered_time
            and current_last_altered_time
            and (current_last_altered_time <= previous_last_altered_time)
        )
    elif (
        not is_enabled_selected_assets
        and previous_last_altered_time
        and current_last_altered_time
    ):
        is_allowed = not (
            previous_last_altered_time
            and current_last_altered_time
            and (current_last_altered_time <= previous_last_altered_time)
        )

    return is_allowed


def prepare_metadata(config: dict, row: dict, asset: dict) -> dict:
    """
    Prepares the metadata for observability measures by extracting relevant
    information from the row and asset data.

    Args:
        config (dict): A dictionary containing the configuration details.
        row (dict): A dictionary containing the row data.
        asset (dict): A dictionary containing the asset data.

    Returns:
        dict: A dictionary containing the prepared metadata.
    """
    metadata = {}
    observe_metrics = asset.get("last_observed_metrics")
    asset_id = asset.get("id")
    observe_metrics = (
        json.loads(observe_metrics)
        if observe_metrics and isinstance(observe_metrics, str)
        else observe_metrics
    )
    observe_metrics = observe_metrics if observe_metrics else {}
    date_format = "%Y-%m-%d %H:%M:%S.%f%z"
    previous_last_altered = observe_metrics.get("last_altered")
    previous_last_altered = previous_last_altered if previous_last_altered else ""
    previous_last_altered = previous_last_altered.replace("Z", "+00:00")
    previous_last_altered = previous_last_altered.replace("T", " ").replace("t", " ")
    previous_last_altered = (
        datetime.strptime(previous_last_altered, date_format)
        if previous_last_altered
        else datetime.now(tz=pytz.UTC)
    )

    volume = row.get("row_count")
    current_last_altered = row.get("last_altered")
    current_last_altered = str(current_last_altered) if current_last_altered else ""
    current_last_altered = current_last_altered.replace("Z", "+00:00")
    current_last_altered = current_last_altered.replace("T", " ").replace("t", " ")
    current_last_altered = datetime.strptime(current_last_altered, date_format)

    current_time = datetime.now(timezone.utc)
    freshness = abs((current_time - current_last_altered).total_seconds())

    previous_schema = observe_metrics.get("columns")
    previous_schema = previous_schema if previous_schema else []
    current_schema = row.get("columns")
    current_schema = current_schema if current_schema else []

    is_first_run = not previous_last_altered
    update_history, update_metrics = get_schema_update_history(
        config,
        previous_schema,
        current_schema,
        is_first_run=is_first_run,
        asset_id=asset_id,
    )
    update_metrics = update_metrics if update_metrics else {}
    update_history = update_history if update_history else {}
    schema_count = 0
    if update_history:
        for _, value in update_history.items():
            if not value:
                continue
            schema_count += len(value)

    metadata.update(
        {
            **row,
            **update_metrics,
            "volume": volume,
            "freshness": freshness,
            "schema_count": schema_count,
            "update_history": update_history,
        }
    )
    return metadata


def get_asset_config(connection: dict, asset: dict) -> dict:
    """
    Generate a Snowflake asset configuration dictionary based on the provided connection and row data.

    Args:
        connection (dict): A dictionary containing connection details. Expected keys include:
        row (dict): A dictionary containing row data. Expected keys include:

    Returns:
        dict: A dictionary representing the Snowflake asset configuration. The dictionary includes:
    """
    connection_id = connection.get("id")
    organization_id = connection.get("organization_id")
    asset_config = {
        "id": str(uuid4()),
        "organization_id": organization_id,
        "connection_id": connection_id,
        "name": asset.get("table_name"),
        "technical_name": asset.get("table_name"),
        "type": asset.get("table_type"),
        "properties": {
            "database": asset.get("database"),
            "schema": asset.get("schema"),
            "table_name": asset.get("table_name"),
        },
        "is_incremental": False,
        "run_now": False,
        "days_percentage": {"type": "days", "value": "1"},
    }
    return asset_config


def prepare_asset_config(
    config: dict, connection_type: str, row: dict, unique_id: str
) -> dict:
    """
    Prepares the asset configuration for observability measures by extracting
    relevant information from the row data.

    Args:
        config (dict): A dictionary containing the configuration details.
        connection_type (str): The type of the connection (e.g., Snowflake, Postgres).
        row (dict): A dictionary containing the row data.

    Returns:
        dict: A dictionary containing the prepared asset configuration.
    """
    asset_config = {}
    connection = config.get("connection")
    connection = connection if connection else {}
    attributes = row.get("columns", [])
    attributes = attributes if attributes else []
    if connection_type in [
        ConnectionType.Snowflake.value.lower(),
        ConnectionType.Databricks.value.lower(),
    ]:
        asset_config = get_asset_config(connection, row)

    asset_config.update(
        {
            "group": "data",
            "is_valid": True,
            "is_active": True,
            "is_delete": False,
            "is_header": False,
            "unique_id": unique_id,
            "attributes": attributes,
        }
    )
    return asset_config


def get_default_measures(config: dict) -> list:
    """
    Fetches the default measures from the database.

    Args:
        config (dict): A dictionary containing the configuration details.

    Returns:
        list: A list of default measures.
    """
    connection = get_postgres_connection(config)
    measures = []
    with connection.cursor() as cursor:
        query_string = f"""
            select * from core.base_measure
            where is_active=True and is_delete=false and is_default=True and term_id is null
        """
        cursor.execute(query_string)
        measures = fetchall(cursor)
        measures = measures if measures else []
    return measures


def get_measure_count(config: dict, asset_id: str) -> dict:
    """
    Fetches the count of measures for a specific asset from the database.

    Args:
        config (dict): A dictionary containing the configuration details.
        asset_id (str): The ID of the asset.

    Returns:
        dict: A dictionary containing the count of measures.
    """
    connection = get_postgres_connection(config)
    measure_count = {}
    with connection.cursor() as cursor:
        query_string = f"""
            select asset_id, attribute_id, count(*) as measure_count
            from core.measure
            where is_active=True and is_delete=false and asset_id='{asset_id}'
            group by asset_id, attribute_id
        """
        cursor.execute(query_string)
        measure_count = fetchall(cursor)
        measure_count = measure_count if measure_count else []
        measure_count = {
            row.get("attribute_id"): row.get("measure_count") for row in measure_count
        }
    return measure_count


def create_default_measures(
    config: dict,
    asset_config: dict,
    include_asset_measures: bool = True,
    include_attribute_measures: bool = True,
    measure_type: str = "all",
) -> list:
    connection_id = config.get("connection_id")
    attributes = asset_config.get("new_attributes", [])
    attributes = attributes if attributes else []
    organization_id = config.get("organization_id")
    organization_id = organization_id if organization_id else ""
    asset_id = asset_config.get("id")
    asset_id = asset_id if asset_id else ""

    default_measures = get_default_measures(config)
    asset_level_measures = list(
        filter(lambda measure: measure.get("level") == "asset", default_measures)
    )
    attribute_level_measures = list(
        filter(lambda measure: measure.get("level") == "attribute", default_measures)
    )
    asset = config.get("asset")
    asset = asset if asset else {}
    if not asset:
        log_info("No asset found")
        return

    organization_settings = config.get("settings", {})
    organization_settings = organization_settings if organization_settings else {}
    profile_settings = organization_settings.get("profile", {})
    profile_settings = (
        json.loads(profile_settings)
        if isinstance(profile_settings, str)
        else profile_settings
    )
    profile_settings = profile_settings if profile_settings else {}
    check_numerics = profile_settings.get("check_numerics", False)

    # Removing the Duplicate Measure for ADLS as it is not implemented in 2.4.6
    measure_count = get_measure_count(config, asset_id)

    # load default measures for asset
    input_measures = []
    weightage = 100
    enable_pass_criteria = False
    pass_criteria_threshold = 100
    pass_criteria_condition = ">="
    is_validated = True
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        has_asset_measures = measure_count.get("null", 0)
        if include_asset_measures and not has_asset_measures:
            if measure_type != "all":
                asset_level_measures = list(
                    filter(
                        lambda measure: measure.get("type") == measure_type,
                        asset_level_measures,
                    )
                )

            for measure in asset_level_measures:
                attribute_id = None
                threshold_constraints = None
                measure_id = str(uuid4())
                technical_name = measure.get("technical_name")
                category = measure.get("category")
                category = category if category else ""
                allow_export = measure.get("allow_export")
                is_active = measure.get("is_active")
                is_active = bool(is_active)
                properties = measure.get("properties")
                properties = (
                    json.loads(properties)
                    if properties and isinstance(properties, str)
                    else properties
                )
                properties = properties if properties else {}
                is_auto = True
                query_input = (
                    measure_id,
                    organization_id,
                    connection_id,
                    asset_id,
                    attribute_id,
                    technical_name,
                    measure.get("threshold"),
                    measure.get("is_positive"),
                    measure.get("is_drift_enabled"),
                    measure.get("allow_score"),
                    weightage,
                    bool(allow_export),
                    enable_pass_criteria,
                    pass_criteria_threshold,
                    pass_criteria_condition,
                    is_active,
                    is_validated,
                    measure.get("is_delete"),
                    is_auto,
                    measure.get("is_aggregation_query"),
                    measure.get("id"),
                    measure.get("dimension_id"),
                    threshold_constraints,
                    ApprovalStatus.Verified.value,
                )
                input_literals = ", ".join(["%s"] * len(query_input))
                query_param = cursor.mogrify(
                    f"({input_literals}, CURRENT_TIMESTAMP)",
                    query_input,
                ).decode("utf-8")
                input_measures.append(query_param)

        # load default measures for attributes
        if include_attribute_measures:
            if not attributes:
                log_info("No attributes defined for the asset")
                return
            attribute_ids = []
            selected_attributes = list(
                filter(
                    lambda attribute: (attribute.get("is_selected")),
                    attributes,
                )
            )
            for attribute in selected_attributes:
                attribute_id = attribute.get("id")
                has_attribute_measures = measure_count.get("attribute_id", 0)
                if has_attribute_measures:
                    continue
                derived_type = attribute.get("derived_type", "")
                derived_type = derived_type.lower() if derived_type else ""
                if not derived_type:
                    continue

                for measure in attribute_level_measures:
                    measure_id = str(uuid4())
                    measure_derived_types = measure.get("derived_type")
                    measure_derived_types = (
                        measure_derived_types if measure_derived_types else []
                    )
                    if ("all" not in measure_derived_types) and (
                        derived_type not in measure_derived_types
                    ):
                        continue
                    technical_name = measure.get("technical_name")
                    category = measure.get("category")
                    category = category if category else ""
                    allow_export = measure.get("allow_export")
                    is_active = measure.get("is_active")
                    is_active = bool(is_active)
                    properties = measure.get("properties")
                    properties = (
                        json.loads(properties)
                        if properties and isinstance(properties, str)
                        else properties
                    )
                    properties = properties if properties else {}

                    if (
                        not check_numerics
                        and technical_name in ["min_value", "max_value"]
                        and derived_type.lower() == "text"
                    ):
                        is_active = False

                    is_auto = True
                    threshold_constraints = None
                    if category and category.lower() == "range":
                        measure_properties = properties if properties else {}
                        measure_properties = (
                            json.loads(measure_properties)
                            if measure_properties
                            and isinstance(measure_properties, str)
                            else measure_properties
                        )
                        measure_properties = (
                            measure_properties if measure_properties else {}
                        )
                        is_auto = bool(measure_properties.get("is_auto"))
                        threshold_constraints = measure_properties.get(
                            "threshold_constraints"
                        )
                        threshold_constraints = (
                            threshold_constraints if threshold_constraints else {}
                        )
                        threshold_constraints = json.dumps(
                            threshold_constraints, default=str
                        )

                    query_input = (
                        measure_id,
                        organization_id,
                        connection_id,
                        asset_id,
                        attribute_id,
                        technical_name,
                        measure.get("threshold"),
                        measure.get("is_positive"),
                        measure.get("is_drift_enabled"),
                        measure.get("allow_score"),
                        weightage,
                        bool(allow_export),
                        enable_pass_criteria,
                        pass_criteria_threshold,
                        pass_criteria_condition,
                        is_validated,
                        is_active,
                        measure.get("is_delete"),
                        is_auto,
                        measure.get("is_aggregation_query"),
                        measure.get("id"),
                        measure.get("dimension_id"),
                        threshold_constraints,
                        ApprovalStatus.Verified.value,
                    )
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals}, CURRENT_TIMESTAMP)",
                        query_input,
                    ).decode("utf-8")
                    input_measures.append(query_param)
                attribute_ids.append(attribute_id)
            # if attribute_ids:
            #     update_active_measures(attribute_ids)

        measure_params = split_queries(input_measures, 1000)
        for measure_param in measure_params:
            try:
                query_input = ",".join(measure_param)
                attributes_insert_query = f"""
                    insert into core.measure (id, organization_id, connection_id, asset_id, attribute_id,
                    technical_name, threshold, is_positive, is_drift_enabled, allow_score, weightage, is_export,
                    enable_pass_criteria, pass_criteria_threshold, pass_criteria_condition, is_validated, is_active, is_delete,
                    is_auto, is_aggregation_query, base_measure_id, dimension_id, threshold_constraints, status, created_date
                    ) values {query_input}
                """
                cursor = execute_query(connection, cursor, attributes_insert_query)
            except Exception as e:
                log_error("create default measures : inserting new measures", e)


def create_attributes(config: dict, asset_config: dict) -> tuple:
    """
    Creates the attributes in both postgres when asset has list of attributes
    """
    organization_id = config.get("organization_id")
    connection_id = config.get("connection_id")
    asset_id = asset_config.get("id")
    connection = config.get("connection")
    connection = connection if connection else {}
    attributes = asset_config.get("attributes", [])
    attributes = attributes if attributes else []
    asset_config.update({"attributes": attributes})
    if not attributes:
        return

    default_queries = config.get("default_queries", {})
    default_queries = default_queries if default_queries else {}
    dq_datatypes = default_queries.get("dq_datatypes")
    is_selected = True
    attribute_response = []
    primary_columns = []
    new_attributes = []

    attribute_idx = 1
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        for attribute in attributes:
            attribute_name = attribute.get("column_name")
            datatype = attribute.get("datatype")
            is_primary_key = attribute.get("is_primary_key")
            derived_type = get_derived_type(dq_datatypes, datatype)
            if (
                derived_type
                and str(derived_type).lower() in ["integer"]
                and "scale" in attribute
            ):
                scale = attribute.get("scale")
                derived_type = "Numeric" if scale and int(scale) > 0 else "Integer"
            derived_type = derived_type if derived_type else "Text"
            attribute_id = str(uuid4())

            query_input = (
                attribute_id,
                organization_id,
                connection_id,
                asset_id,
                attribute_name,
                attribute_name,
                attribute.get("description", ""),
                datatype,
                derived_type,
                is_selected,
                attribute.get("is_primary_key", False),
                attribute_idx,
                "Pending",
                True,
                False,
                False,
                True,
            )
            input_literals = ", ".join(["%s"] * len(query_input))
            query_param = cursor.mogrify(
                f"({input_literals}, CURRENT_TIMESTAMP)",
                query_input,
            ).decode("utf-8")
            new_attributes.append(query_param)
            attribute_idx += 1

            attribute.update({"id": attribute_id})
            if is_primary_key:
                primary_columns.append(
                    {
                        "id": attribute_id,
                        "name": attribute_name,
                        "derived_type": derived_type,
                    }
                )
            attribute.update(
                {
                    "organization_id": organization_id,
                    "connection_id": connection_id,
                    "asset_id": asset_id,
                    "name": attribute_name,
                    "derived_type": derived_type,
                    "is_selected": is_selected,
                }
            )
            attribute_response.append(attribute)

        attribute_params = split_queries(new_attributes, 1000)
        for attribute_param in attribute_params:
            try:
                query_input = ",".join(attribute_param)
                attributes_insert_query = f"""
                    insert into core.attribute (id, organization_id, connection_id, asset_id, name, technical_name,
                    description, datatype, derived_type, is_selected, is_primary_key, attribute_index, status, is_active,
                    is_delete, is_semantic_enabled, profile, created_date) values {query_input}
                """
                cursor = execute_query(connection, cursor, attributes_insert_query)
            except Exception as e:
                log_error("create attributes : inserting new attributes", e)

    response = list(
        map(
            lambda attribute: {
                "id": attribute.get("id"),
                "name": attribute.get("name"),
                "derived_type": attribute.get("derived_type"),
                "is_selected": attribute.get("is_selected"),
            },
            attribute_response,
        )
    )
    return response, primary_columns


def create_asset(config: dict, asset_config: dict) -> dict:
    """
    Creates an asset in the database using the provided asset configuration.

    Args:
        asset_config (dict): A dictionary containing the asset configuration details.

    Returns:
        dict: A dictionary containing the created asset details.
    """
    attributes = asset_config.get("attributes", [])
    attributes = attributes if attributes else []
    asset_config = asset_config if asset_config else {}
    run_id = config.get("run_id")

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        organization_id = asset_config.get("organization_id")
        connection_id = asset_config.get("connection_id")
        asset_id = asset_config.get("id")
        unique_id = asset_config.get("unique_id")
        properties = asset_config.get("properties")
        properties = (
            json.loads(properties)
            if properties and isinstance(properties, str)
            else properties
        )
        properties = properties if properties else {}
        properties = json.dumps(properties, default=str)

        query_input = (
            asset_id,
            organization_id,
            connection_id,
            asset_config.get("name"),
            asset_config.get("technical_name"),
            asset_config.get("type"),
            properties,
            "Pending",
            "Completed",
            0,
            asset_config.get("run_now"),
            "data",
            run_id,
            run_id,
            True,
            True,
            False,
            unique_id,
        )
        input_literals = ", ".join(["%s"] * len(query_input))
        query_param = cursor.mogrify(
            f"({input_literals}, CURRENT_TIMESTAMP)",
            query_input,
        ).decode("utf-8")

        query_string = f"""
            insert into core.asset (
                id, organization_id, connection_id, name, technical_name, type, properties,
                status, run_status, ratings, run_now, "group", last_run_id, last_observe_run_id,
                is_valid, is_active, is_delete, unique_id, created_date
            ) values {query_param}
        """
        cursor.execute(query_string)
        new_asset = get_asset(config, unique_id)
        new_asset = new_asset if new_asset else {}
        asset_config.update(**new_asset)

        # create data object for the asset
        data_id = str(uuid4())
        query_input = (
            data_id,
            asset_id,
            connection_id,
            False,
            False,
            True,
            False,
        )
        input_literals = ", ".join(["%s"] * len(query_input))
        query_param = cursor.mogrify(
            f"({input_literals})",
            query_input,
        ).decode("utf-8")
        query_string = f"""
            insert into core.data (
                id, asset_id, connection_id, is_incremental, reset_failed_rows_table, is_active, is_delete
            ) values {query_param}
        """
        cursor.execute(query_string)

        # create attributes for the asset
        if not attributes:
            return
        attribute_response, primary_columns = create_attributes(config, asset_config)
        asset_config.update({"new_attributes": attribute_response})
        if primary_columns:
            primary_columns = json.dumps(primary_columns, default=str)
            query_string = f"""
                update core.data set primary_columns='{primary_columns}'
                where id='{asset_id}'
            """
            cursor.execute(query_string)

    # create default measures for the asset
    create_default_measures(
        config,
        asset_config,
        True,
        True,
    )


def update_asset_observed_config(
    config: dict, observe_metrics: list, observe_data: list
) -> None:
    """
    Updates the last observed timestamp for the asset in the database.

    Args:
        config (dict): A dictionary containing the configuration details.
    """
    if not observe_metrics:
        return

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        update_observe_metrics = split_queries(observe_metrics, 1000)
        for update_observe_metric in update_observe_metrics:
            try:
                query_input = ",".join(update_observe_metric)
                update_asset_query = f"""
                    update core.asset set last_observed_metrics=payload.observed_metrics::jsonb,
                    last_observe_run_id=payload.run_id::uuid,
                    data_size=payload.data_size,
                    modified_date=CURRENT_TIMESTAMP
                    from (
                        values {query_input}
                    ) as payload (asset_id, run_id, data_size, observed_metrics)
                    where asset.id::text = payload.asset_id::text
                """
                cursor = execute_query(connection, cursor, update_asset_query)
            except Exception as e:
                log_error("Update last observe metrics : updating observe metrics", e)

        if observe_data:
            update_observe_data_input = split_queries(observe_data, 1000)
            for update_observe_data in update_observe_data_input:
                try:
                    query_input = ",".join(update_observe_data)
                    update_asset_query = f"""
                        update core.data set
                        row_count= (case when payload.row_count is not null and payload.row_count > 0 then payload.row_count else data.row_count end),
                        column_count= (case when payload.column_count is not null and payload.column_count > 0 then payload.column_count else data.column_count end),
                        freshness= (case when payload.freshness is not null then payload.freshness::bigint else data.freshness end)
                        from (
                            values {query_input}
                        ) as payload (id, asset_id, row_count, column_count, freshness)
                        where data.id::text = payload.id::text and data.asset_id::text = payload.asset_id::text
                    """
                    cursor = execute_query(connection, cursor, update_asset_query)
                except Exception as e:
                    log_error("Update observe metadata: updating observe metadata", e)


def update_connection_observed_config(config: dict, metadata: dict) -> None:
    """
    Updates the last observed timestamp for the asset in the database.

    Args:
        config (dict): A dictionary containing the configuration details.
    """
    connection_id = config.get("connection_id")
    run_id = config.get("run_id")
    last_altered = metadata.get("last_altered")
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        try:
            update_asset_query = f"""
                update core.connection set last_observed='{last_altered}', last_observe_run_id='{str(run_id)}'
                where id='{str(connection_id)}'
            """
            cursor = execute_query(connection, cursor, update_asset_query)
        except Exception as e:
            log_error("Update last observe metrics : updating observe metrics", e)


def get_selected_assets(config: dict):
    """
    Retrieves a list of selected assets based on the provided configuration.

    Args:
        config (dict): A dictionary containing configuration details.

    Returns:
        list: A list of selected assets.
    """
    connection_id = config.get("connection_id")
    selected_assets = []
    if not connection_id:
        return selected_assets

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            select asset.*, data.id as data_id from core.asset
            join core.data on data.asset_id=asset.id
            where asset.is_active=True and asset.is_delete=False and asset.connection_id='{connection_id}'
        """
        cursor.execute(query_string)
        selected_assets = fetchall(cursor)
        selected_assets = selected_assets if selected_assets else []
    return selected_assets


def is_observe_measure_mapped(config: dict, asset_id: str) -> bool:
    """
    Checks if the observe measures are mapped in the database.

    Args:
        config (dict): A dictionary containing the configuration details.

    Returns:
        bool: True if observe measures are mapped, False otherwise.
    """
    connection = get_postgres_connection(config)
    measure_count = 0
    with connection.cursor() as cursor:
        query_string = f"""
            select count(*) as total_measures from core.measure
            join core.base_measure on base_measure.id=measure.base_measure_id
            where base_measure.type='observe' and measure.asset_id='{asset_id}'
        """
        cursor.execute(query_string)
        measures = fetchone(cursor)
        measures = measures if measures else {}
        total_measures = measures.get("total_measures", 0)
        measure_count = int(total_measures) if total_measures else 0
    return measure_count > 0


def get_row_count(config: dict, row: dict, observe_queries: dict) -> dict:
    """
    Retrieves the row count for the given row data.

    Args:
        config (dict): A dictionary containing the configuration details.
        row (dict): A dictionary containing the row data.
        observe_queries (dict): A dictionary containing the observe queries.

    Returns:
        dict: A dictionary containing the row count.
    """
    connection = config.get("connection")
    connection = connection if connection else {}
    connection_type = connection.get("type")
    connection_type = connection_type.lower() if connection_type else ""
    if connection_type != ConnectionType.Databricks.value.lower():
        return row

    row_count_query = observe_queries.get("row_count")
    if not row_count_query:
        return row

    database = row.get("database")
    database = database if database else ""
    schema = row.get("schema")
    schema = schema if schema else ""
    table_name = row.get("table_name")
    table_name = table_name if table_name else ""
    row_count_query = (
        row_count_query.replace("<database_name>", database)
        .replace("<schema_name>", schema)
        .replace("<table_name>", table_name)
    )
    response, _ = execute_native_query(
        config,
        row_count_query,
    )
    response = response if response else {}
    volume = response.get("row_count", 0)
    row_count = volume if volume else 0
    row.update({"row_count": row_count})
    return row

def get_metric_id(config: dict, measure_id: str) -> str:
    run_id = config.get("run_id")
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            select id from core.metrics
            where measure_id='{measure_id}' and run_id='{run_id}'
        """
        cursor.execute(query_string)
        metric_id = fetchone(cursor)
        return metric_id.get("id") if metric_id else ""
    
def get_is_fingerprint_asset(config: dict, asset_id: str) -> bool:
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            SELECT 
                CASE 
                    WHEN COUNT(*) > 0 THEN TRUE 
                    ELSE FALSE 
                END AS result
            FROM core.asset a 
            JOIN core.data d ON d.asset_id = a.id 
            WHERE a.id = '{asset_id}'
            AND (
                d.is_incremental = 'true' 
                OR a.is_filtered = 'true' 
                OR a.is_custom_fingerprint = 'true'
            );
        """
        cursor.execute(query_string)
        result = fetchone(cursor)
        return result.get("result", False) if result else False


def save_observe_metrics(config: dict, metadata: list, is_reliability: bool = False, **kwargs) -> None:
    """
    Saves the observability metrics to the database.

    Args:
        config (dict): A dictionary containing the configuration details.
        metadata (list): A list of metadata to be saved.
    """
    is_processed = False
    if not metadata:
        return is_processed

    selected_assets = kwargs.get("selected_assets")
    selected_assets = selected_assets if selected_assets else []
    observe_queries = kwargs.get("observe_queries")
    observe_queries = observe_queries if observe_queries else {}
    connection = config.get("connection")
    connection = connection if connection else {}
    connection_type = connection.get("type")
    connection_type = connection_type.lower() if connection_type else ""
    connection_id = connection.get("id")
    organization_id = connection.get("organization_id")
    run_id = config.get("run_id")
    airflow_run_id = config.get("airflow_run_id")

    total_count = 0
    valid_count = 0
    invalid_count = 0
    valid_percentage = 0
    invalid_percentage = 0
    score = None
    is_archived = False

    measure_ids = []
    observe_metrics = []
    update_observe_metrics_input = []
    update_observed_data_input = []
    pg_connection = get_postgres_connection(config)
    with pg_connection.cursor() as cursor:
        for row in metadata:
            try:
                asset_type = row.get("table_type")
                asset_type = str(asset_type).lower() if asset_type else ""
                fetch_row_count = (
                    connection_type != ConnectionType.Databricks.value.lower()
                    or (
                        connection_type == ConnectionType.Databricks.value.lower()
                        and asset_type != "view"
                    )
                )
                attributes = row.get("columns", [])
                attributes = (
                    json.loads(attributes)
                    if attributes and isinstance(attributes, str)
                    else attributes
                )
                attributes = attributes if attributes else []
                row.update({"columns": attributes})

                # populate observe metrics

                unique_id = get_asset_unique_id(connection_id, connection_type, row)
                asset = None
                is_data_changed = False
                if selected_assets:
                    asset = next(
                        (
                            asset
                            for asset in selected_assets
                            if asset.get("unique_id") == unique_id
                        ),
                        None,
                    )
                    asset = asset if asset else {}
                    is_data_changed = check_is_allowed_asset(config, row, asset)

                    if fetch_row_count:
                        row = get_row_count(config, row, observe_queries)
                else:
                    asset = get_asset(config, unique_id)
                    if fetch_row_count:
                        row = get_row_count(config, row, observe_queries)

                if not asset:
                    asset_config = prepare_asset_config(
                        config, connection_type, row, unique_id
                    )
                    asset_config = asset_config if asset_config else {}
                    if not asset_config:
                        log_info("No asset_config found")
                        continue
                    asset = create_asset(config, asset_config)
                    asset = get_asset(config, unique_id)

                asset = asset if asset else {}
                asset_id = asset.get("id")
                asset_metadata = prepare_metadata(config, row, asset)
                if not asset_metadata:
                    log_info("No asset_metadata found to process")
                    continue
                if asset_metadata.get("is_first_run") == True:
                    is_data_changed = True
                is_fingerprint_asset = get_is_fingerprint_asset(config, asset_id)
                has_schema_changes = any(asset_metadata.get("update_history")[key] for key in asset_metadata.get("update_history"))
                if is_fingerprint_asset:
                    if has_schema_changes:
                        is_data_changed = True
                    else:
                        is_data_changed = False
                run_config = {}
                if asset_metadata:
                    run_config = {
                        "run_id": str(run_id),
                        "volume": asset_metadata.get("row_count"),
                        "is_first_run": asset_metadata.get("is_first_run"),
                        "freshness": asset_metadata.get("freshness"),
                        "column_count": asset_metadata.get("column_count"),
                        "schema_count": asset_metadata.get("schema_count"),
                        "update_history": asset_metadata.get("update_history"),
                        "added_count": asset_metadata.get("added_count"),
                        "removed_count": asset_metadata.get("removed_count"),
                        "renamed_count": asset_metadata.get("renamed_count"),
                        "datatype_changed_count": asset_metadata.get(
                            "datatype_changed_count"
                        ),
                        "is_data_changed": is_data_changed,
                    }
                run_config = json.dumps(run_config, default=str) if run_config else {}
                measures = get_observe_measures(config, asset_id, is_fingerprint_asset)

                # fetch observe measures for the given asset
                for measure in measures:
                    if is_reliability:
                        metric_id = get_metric_id(config, measure.get("id"))
                        query_input = (run_config, str(metric_id))
                        query_param = cursor.mogrify("(%s::jsonb, %s::uuid)", query_input).decode("utf-8")
                        observe_metrics.append(query_param)
                    else:
                        executed_query = ""
                        measure_id = measure.get("id")
                        allow_score = measure.get("allow_score", False)
                        is_drift_enabled = measure.get("is_drift_enabled", False)
                        measure_name = measure.get("name")
                        level = measure.get("level")
                        measure_key = measure_name
                        if measure_key == "freshness_observability":
                            measure_key = "freshness"
                        elif measure_key == "schema":
                            measure_key = "schema_count"
                        value = asset_metadata.get(measure_key)
                        value = value if value else 0
                        weightage = measure.get("weightage", 100)
                        weightage = int(weightage) if weightage else 100

                        query_input = (
                            str(uuid4()),
                            organization_id,
                            connection_id,
                            asset_id,
                            None,
                            measure_id,
                            run_id,
                            airflow_run_id,
                            measure_name,
                            level,
                            str(value),
                            weightage,
                            total_count,
                            valid_count,
                            invalid_count,
                            valid_percentage,
                            invalid_percentage,
                            score,
                            PASSED,
                            is_archived,
                            executed_query,
                            allow_score,
                            is_drift_enabled,
                            run_config,
                            True,
                            True,
                            False,
                        )
                        input_literals = ", ".join(["%s"] * len(query_input))
                        query_param = cursor.mogrify(
                            f"({input_literals}, CURRENT_TIMESTAMP)",
                            query_input,
                        ).decode("utf-8")
                        observe_metrics.append(query_param)
                        if measure_id:
                            measure_ids.append(f"'{str(measure_id)}'")

                # # Update asset metrics for the current run
                if asset and asset_metadata:
                    data_size = None
                    if (
                        connection_type == ConnectionType.Databricks.value.lower()
                        and fetch_row_count
                    ):
                        data_size = asset_metadata.get("row_count")
                        data_size = data_size if data_size else 0

                    query_input = (
                        asset_id,
                        run_id,
                        data_size,
                        json.dumps(asset_metadata, default=str),
                    )
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals}, CURRENT_TIMESTAMP)",
                        query_input,
                    ).decode("utf-8")
                    update_observe_metrics_input.append(query_param)

                # prepare query input to update asset metadata
                data_id = asset.get("data_id") if asset else None
                if data_id:
                    row_count = asset_metadata.get("row_count")
                    row_count = row_count if row_count else 0
                    if connection_type == ConnectionType.Databricks.value.lower():
                        row_count = 0
                    column_count = asset_metadata.get("column_count")
                    column_count = column_count if column_count else 0
                    freshness = asset_metadata.get("freshness")
                    freshness = freshness if freshness else None
                    query_input = (
                        data_id,
                        asset_id,
                        row_count,
                        column_count,
                        freshness,
                    )
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals})",
                        query_input,
                    ).decode("utf-8")
                    update_observed_data_input.append(query_param)

                if not is_processed:
                    is_processed = True
            except Exception as e:
                log_error(f"extract metadata : inserting observe metrics - {str(e)}", e)

        # delete the metrics for the same run_id
        if measure_ids and run_id:
            measure_id_filter = ", ".join(measure_ids)
            delete_metrics(
                config,
                run_id=run_id,
                measure_ids=measure_id_filter,
            )

        observe_metrics = split_queries(observe_metrics, 1000)
        for observe_metrics_input in observe_metrics:
            try:
                query_input = ",".join(observe_metrics_input)
                if is_reliability:
                    values_clause = ",\n".join(observe_metrics_input)
                    update_metrics_query = f"""
                        UPDATE core.metrics
                        SET 
                            run_config = payload.run_config::jsonb
                        FROM (
                            VALUES {values_clause}
                        ) AS payload (run_config, id)
                        WHERE core.metrics.id::text = payload.id::text
                    """
                    cursor = execute_query(pg_connection, cursor, update_metrics_query)
                else:
                    metrics_insert_query = f"""
                        insert into core.metrics (id, organization_id, connection_id, asset_id, attribute_id,
                        measure_id, run_id, airflow_run_id, measure_name, level, value, weightage, total_count,
                        valid_count, invalid_count, valid_percentage, invalid_percentage, score, status, is_archived,
                        query, allow_score, is_drift_enabled, run_config, is_measure, is_active, is_delete, created_date)
                        values {query_input}
                    """
                    cursor = execute_query(pg_connection, cursor, metrics_insert_query)
            except Exception as e:
                log_error("Save observe metrics : inserting new metrics", e)

        if measure_ids:
            measure_id_filter = ", ".join(measure_ids)
            query_string = f"""
                update core.measure set last_run_id='{run_id}', last_run_date=CURRENT_TIMESTAMP
                where id in ({measure_id_filter})
            """
            cursor.execute(query_string)

    # update last observe metrics for the current run
    update_asset_observed_config(
        config, update_observe_metrics_input, update_observed_data_input
    )
    update_connection_observed_config(config, metadata[0])
    return is_processed
