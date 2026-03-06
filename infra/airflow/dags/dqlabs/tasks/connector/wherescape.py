import json
from uuid import uuid4
import datetime

from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone, split_queries
from dqlabs.app_helper.connection_helper import decrypt_connection_config
from dqlabs.app_helper.dag_helper import (
    get_postgres_connection,
)
from dqlabs.app_helper.lineage_helper import map_asset_with_lineage, save_lineage, get_asset_metric_count, update_asset_metric_count, handle_alerts_issues_propagation, update_pipeline_propagations
from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.utils.tasks import get_task_config, check_task_status
from dqlabs.utils.request_queue import (
    update_queue_detail_status,
    update_queue_status,
    create_queue_detail,
)
from dqlabs.app_helper.dag_helper import (
    execute_native_query,
    get_postgres_connection,
    get_native_connection
)
from dqlabs.utils.extract_workflow import (
    get_queries,
    update_asset_run_id
)
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.app_helper import agent_helper
from dqlabs.app_helper.pipeline_helper import get_pipeline_last_run_id, get_pipeline_id, update_pipeline_last_runs
from dqlabs.app_helper.dq_helper import get_pipeline_status, extract_table_name
from dqlabs.enums.connection_types import ConnectionType
from dqlabs.utils.pipeline_measure import execute_pipeline_measure, create_pipeline_task_measures, get_pipeline_tasks

TASK_CONFIG = None
LATEST_RUN = False
ALL_RUNS = []

def extract_wherescape_data(config, **kwargs):
    try:
        is_completed = check_task_status(config, kwargs)
        if is_completed:
            return

        source_connection = None
        # Update the asset state
        run_id = config.get("queue_id")
        asset = config.get("asset")
        default_queries = get_queries(config)
        asset = asset if asset else {}
        asset_properties = asset.get("properties", {})
        asset_properties = asset_properties if asset_properties else {}
        asset_database = config.get("database_name")
        asset_schema = config.get("schema")
        if not asset_database and asset_properties:
            asset_database = asset_properties.get("database")
        if not asset_schema and asset_properties:
            asset_schema = config.get("connection", {}).get(
                "credentials", {}).get("schema", '')
        config.update(
            {"database_name": asset_database, "schema": asset_schema})

        global TASK_CONFIG
        global LATEST_RUN
        global ALL_RUNS
        task_config = get_task_config(config, kwargs)
        update_queue_detail_status(
            config, ScheduleStatus.Running.value, task_config=task_config
        )
        update_queue_status(config, ScheduleStatus.Running.value, True)
        update_asset_run_id(run_id, config)

        connection = config.get("connection", {})
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        credentials = connection.get("credentials")
        connection_type = connection.get("type", "")
        credentials = decrypt_connection_config(credentials, connection_type)
        source_connection = get_native_connection(config)
        metadata_pull_config = credentials.get("metadata", {})
        runs_pull = metadata_pull_config.get("runs", False)
        tasks_pull = metadata_pull_config.get("tasks", False)

        TASK_CONFIG = config

        data = {}
        job_id = asset_properties.get("job_id")
        data, runs, LATEST_RUN = get_ws_job_by_id(config, job_id, data,
                                default_queries, source_connection)

        if tasks_pull:
            new_tasks = __save_models(config, data.get('models', []))
            if new_tasks:
                create_pipeline_task_measures(config, new_tasks)

        __prepare_lineage(config, job_id, data,
                          default_queries, source_connection)

        # Update Job Run Stats
        __update_pipeline_stats(config)

        # Get Description and Prepare Search Key
        description = __prepare_description(data)
        description = description.replace("'", "''")

        search_keys = __prepare_search_key(data)
        search_keys = search_keys.replace("'", "''")

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                update core.asset set description='{description}', search_keys='{search_keys}'
                where id = '{asset_id}' and is_active=true and is_delete = false
            """
            cursor = execute_query(connection, cursor, query_string)
        if runs and LATEST_RUN:
            # Get the latest run from database to ensure we have the most recent run_id
            db_connection = get_postgres_connection(config)
            with db_connection.cursor() as cursor:
                query_string = f"""
                    select source_id
                    from core.pipeline_runs
                    where asset_id = '{asset_id}'
                    order by run_end_at desc
                    limit 1
                """
                cursor = execute_query(db_connection, cursor, query_string)
                latest_run_result = fetchone(cursor)
                latest_run_id = latest_run_result.get("source_id") if latest_run_result else ""
                if latest_run_id:
                    handle_alerts_issues_propagation(config, latest_run_id)

        # save Propagation Values
        update_pipeline_propagations(config, asset, connection_type)
        # update request queue status
        is_triggered = config.get("is_triggered")
        if not is_triggered:
            create_queue_detail(config)
        update_queue_detail_status(config, ScheduleStatus.Completed.value)
        update_queue_status(config, ScheduleStatus.Completed.value, True)
        update_pipeline_last_runs(config)

        propagate_alerts = credentials.get("propagate_alerts", "table")
        metrics_count = None
        asset_id = config.get("asset_id")
        if propagate_alerts == "table":
            metrics_count = get_asset_metric_count(config, asset_id)
        if LATEST_RUN:
            extract_pipeline_measure(ALL_RUNS, tasks_pull)

        if propagate_alerts == "table" or propagate_alerts == "pipeline":
            update_asset_metric_count(config, asset_id, metrics_count, LATEST_RUN, propagate_alerts=propagate_alerts)
    except Exception as e:
        log_error("Wherescape Pull / Push Failed", e)
        update_queue_detail_status(config, ScheduleStatus.Failed.value, str(e))


def get_ws_job_by_id(config: dict, job_id: str, data: dict, default_queries, source_connection: object = None) -> dict:
    try:
        job_query = default_queries.get('job_by_id')
        credentials = config.get("connection", {}).get("credentials", {})
        database = credentials.get("database", '')
        schema = credentials.get("schema", '')
        job_query = job_query.replace('<job_id>', str(job_id)).replace(
            "<database_name>", database).replace("<schema_name>", schema)
        job_data, _ = execute_native_query(
            config, job_query, source_connection)
        if job_data:
            data.update(
                {
                    "job_id": job_data.get("job_id", ""),
                    "id": job_data.get("job_id", ""),
                    "job_name": job_data.get("job_name", ""),
                    "name": job_data.get("job_name", ""),
                    "description": job_data.get('description', ''),
                    "project_id": job_data.get("project_id", ""),
                    "project_name": job_data.get('project_name', ''),
                    "group_id": job_data.get("group_id", ""),
                    "group_name": job_data.get('group_name', ''),
                    "created_at": job_data.get('created_at', '')
                }
            )

            more_info = get_tasks_by_job_id(
                config, job_id, default_queries, source_connection)
            if more_info:
                data.update(more_info)

            __save_wherescape_pipeline_jobs(config, data)

            run_id = more_info.get("runId")
            run_id = int(run_id) if run_id else 0
            last_run_id = get_pipeline_last_run_id(config)
            last_run_id = int(last_run_id) if last_run_id else 0

            if last_run_id == 0:
                run_params = {"initial": True}
                runs, LATEST_RUN = __get_runs_by_job_id(
                    config, job_id, default_queries, source_connection, run_params, data)
            else:
                last_run_id = int(last_run_id) if last_run_id else 0
                last_run_id = last_run_id if last_run_id < run_id else run_id
                run_params = {
                    "run_id": last_run_id,
                    "condition": "eq" if run_id == last_run_id else "gt",
                }
                runs, LATEST_RUN = __get_runs_by_job_id(
                    config, job_id, default_queries, source_connection, run_params, data)

            return data, runs, LATEST_RUN
        else:
            raise Exception(
                f"Wherescape server return empty job details for the {job_id}")
    except Exception as e:
        raise e


def __get_runs_by_job_id(
    config: dict, job_id: str, default_queries, source_connection, params: dict = None, data: dict = None
) -> dict:
    try:
        runs_query = default_queries.get('runs_by_job')
        credentials = config.get("connection", {}).get("credentials", {})
        database = credentials.get("database", '')
        schema = credentials.get("schema", '')
        runs_query = runs_query.replace('<job_id>', str(job_id)).replace(
            "<database_name>", database).replace("<schema_name>", schema)

        metadata_pull_config = credentials.get("metadata", {})
        runs_pull = metadata_pull_config.get("runs", False)
        no_of_runs = credentials.get("no_of_runs", "30")

        asset = config.get("asset", {})
        asset_id = asset.get("id")

        status_filter = credentials.get("status", "all")
        status_filter_query = ""
        runs = ""
        LATEST_RUN = False
        if status_filter != "all":
            status_filter_query = "and wjl.wjl_status = 'C'" if status_filter == "success" else "and wjl.wjl_status = 'F'"
        if params:
            run_id = params.get("run_id", None)
            initial = params.get("initial", None)
            filter_query = ''
            if initial:
                end = datetime.datetime.today()
                start = end - datetime.timedelta(days=30)
                dates = [
                    start.strftime("%Y-%m-%d %H:%M:%S"),
                    end.strftime("%Y-%m-%d %H:%M:%S"),
                ]
                filter_query = f" and wjl_started > DATEADD(DAY, -{no_of_runs}, GETDATE()) "
            elif run_id:
                condition = params.get("condition", None)
                if run_id and condition:
                    if condition == "eq":
                        filter_query = f" and wjl_sequence = {run_id}"
                    elif condition == "gt":
                        filter_query = f" and wjl_sequence > {run_id}"
            runs_query = runs_query.replace('<run_id_filter>', filter_query).replace(
                "<database_name>", database).replace("<schema_name>", schema).replace("<status_filter>", status_filter_query)
            run_data, native_connection = execute_native_query(
                config, runs_query, source_connection, is_list=True)
            run_data = run_data if run_data else {}
            if run_data and runs_pull:
                runs, LATEST_RUN = __save_pipeline_runs(
                    config, run_data, data, default_queries, source_connection)
                __get_runs_details(config, runs)
        return runs, LATEST_RUN
    except Exception as e:
        raise e


def __get_runs_details(config: dict, job_runs: str):
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        pipeline_id = get_pipeline_id(config)

        # Get Runs Details
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select pipeline_runs.id, pipeline_runs.source_id from core.pipeline_runs
                left join core.pipeline_runs_detail on pipeline_runs_detail.run_id = pipeline_runs.source_id
                where pipeline_runs.asset_id = '{asset_id}'
                    and pipeline_runs.pipeline_id = '{pipeline_id}'
                    and pipeline_runs_detail.run_id is null
            """
            cursor = execute_query(connection, cursor, query_string)
            runs = fetchall(cursor)

            for run in runs:
                run_id = run.get('source_id', '')
                data = next((obj for obj in job_runs if str(
                    obj.get("run_id")) == str(run_id)), None)
                if data:
                    __save_runs_details(config, run, data)
    except Exception as e:
        log_error("Wherescape Connector - Get Runs Details By Job ID Failed ", e)
        raise e


def __save_runs_details(config: dict, run: dict,  data: list):
    try:
        connection_obj = config.get("connection", {})
        connection_id = connection_obj.get('id')
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        pipeline_id = get_pipeline_id(config)
        pipeline_run_id = run.get("id")

        models = data.get('models', [])
        insert_objects = []
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:

            # Save Tasks Which Runs / Status
            for model in models:
                if model.get('status'):
                    query_input = (
                        uuid4(),
                        model.get("runId", ''),
                        model.get('uniqueId'),
                        'task',
                        model.get('name'),
                        get_pipeline_status(model.get('status')),
                        model.get('error', ''),
                        (
                            model.get("rawSql", "").replace("'", "''")
                            if model.get("rawSql")
                            else ""
                        ),
                        (
                            model.get("compiledSql", "").replace("'", "''")
                            if model.get("compiledSql")
                            else ""
                        ),
                        model.get('executeStartedAt') if model.get(
                            'executeStartedAt') else None,
                        model.get('executeCompletedAt') if model.get(
                            'executeCompletedAt') else None,
                        model.get('executionTime') if model.get(
                            'executionTime') else None,
                        True,
                        False,
                        pipeline_run_id,
                        pipeline_id,
                        asset_id,
                        connection_id,
                    )
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals})", query_input
                    ).decode("utf-8")
                    insert_objects.append(query_param)

            insert_objects = split_queries(insert_objects)
            for input_values in insert_objects:
                try:
                    query_input = ",".join(input_values)
                    query_string = f"""
                        insert into core.pipeline_runs_detail(
                            id, run_id, source_id, type, name, status, error, source_code, compiled_code,
                            run_start_at, run_end_at, duration, is_active, is_delete,
                            pipeline_run_id, pipeline_id, asset_id, connection_id
                        ) values {query_input} 
                    """
                    cursor = execute_query(
                        connection, cursor, query_string)
                except Exception as e:
                    log_error('DBT Runs Details Insert Failed  ', e)

    except Exception as e:
        log_error(f"DBT Connector - Save Runs Details By Run ID Failed ", e)
        raise e


def __save_models(config: dict, models: list):
    new_pipeline_tasks = []
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")

        connection = config.get("connection", {})
        connection_id = connection.get("id")

        pipeline_id = get_pipeline_id(config)
        source_type = 'model'
        tasks = []

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select pipeline_tasks.id, pipeline_tasks.source_id from core.pipeline_tasks
                join core.pipeline on pipeline.asset_id = pipeline_tasks.asset_id
                where pipeline_tasks.asset_id = '{asset_id}' and is_selected = true and pipeline.is_delete = false
            """
            cursor = execute_query(connection, cursor, query_string)
            existings_models = fetchall(cursor)

            for model in models:
                existing_model = next((x for x in existings_models if str(x.get(
                    "source_id")) == str(model.get("uniqueId"))), None)
                if existing_model:
                    status = get_pipeline_status(model.get(
                        'status') if model.get('status') else "")

                    query_string = f"""
                        update core.pipeline_tasks set
                            status = '{status}',
                            source_type = '{source_type}',
                            error = '{model.get('error') if model.get('error') else ""}',
                            source_code = '{
                                (
                                    model.get("rawSql", "").replace(
                                        "'", "''")
                                    if model.get("rawSql")
                                    else ""
                                )
                            }',
                            compiled_code = '{
                                (
                                    model.get("compiledSql", "").replace(
                                        "'", "''")
                                    if model.get("compiledSql")
                                    else ""
                                )
                            }',
                            run_id = '{model.get("lastRunID", '')}',
                            run_start_at = {f"'{model.get('executeStartedAt')}'" if model.get('executeStartedAt') else "NULL"},
                            run_end_at = {f"'{model.get('executeCompletedAt')}'" if model.get('executeCompletedAt') else "NULL"},
                            duration =  {f"'{model.get('executionTime')}'" if model.get('executionTime') else "NULL"},
                            properties = '{json.dumps({}, default=str).replace("'", "''")}',
                            tests = '{json.dumps(model.get('tests', []), default=str).replace("'", "''")}'
                        where asset_id = '{asset_id}' and source_id = '{model.get("source_id")}'
                    """
                    cursor = execute_query(connection, cursor, query_string)

                    # Save Model Columsn
                    __save_columns(config, model, existing_model.get("id"))
                else:
                    pipeline_task_id = str(uuid4())
                    query_input = (
                        pipeline_task_id,
                        model.get("uniqueId"),
                        model.get("name"),
                        model.get("owner", ""),
                        json.dumps({}, default=str).replace(
                            "'", "''"),
                        model.get("executeStartedAt", ""),
                        model.get("executeCompletedAt", ""),
                        source_type,
                        model.get('executionTime', ''),
                        get_pipeline_status(
                            model.get('status') if model.get('status') else ""),
                        model.get("lastRunID", ''),
                        model.get("schema", ''),
                        str(pipeline_id),
                        str(asset_id),
                        str(connection_id),
                        True,
                        True,
                        False
                    )
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals})", query_input).decode("utf-8")
                    tasks.append(query_param)
                    new_pipeline_tasks.append(pipeline_task_id)

            # create each tasks
            tasks_input = split_queries(tasks)
            for input_values in tasks_input:
                try:
                    query_input = ",".join(input_values)
                    tasks_insert_query = f"""
                        insert into core.pipeline_tasks (id, source_id, name, owner, properties, run_start_at,
                        run_end_at, source_type, duration, status, run_id, schema, pipeline_id, asset_id,connection_id,
                        is_selected, is_active, is_delete)
                        values {query_input} 
                        RETURNING id, source_id
                    """
                    cursor = execute_query(
                        connection, cursor, tasks_insert_query)
                    pipeline_tasks = fetchall(cursor)

                    for pipeline_task in pipeline_tasks:
                        new_model = next((x for x in models if str(
                            x.get('uniqueId')) == str(pipeline_task.get('source_id'))), None)

                        # Save Model Columns
                        __save_columns(config, new_model,
                                       pipeline_task.get('id'))
                except Exception as e:
                    log_error(
                        "extract properties: inserting tasks level", e)

    except Exception as e:
        log_error(f"DBT Connector - Save Models By Job ID Failed ", e)
        raise e
    finally:
        return new_pipeline_tasks


def __save_columns(config: dict, model: dict, pipeline_task_id: str):
    try:
        connection_obj = config.get("connection", {})
        connection_id = connection_obj.get('id')
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        pipeline_id = get_pipeline_id(config)

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                delete from core.pipeline_columns
                where asset_id = '{asset_id}' and pipeline_task_id = '{pipeline_task_id}'
            """
            cursor = execute_query(connection, cursor, query_string)

            insert_objects = []
            columns = model.get('columns')
            if columns:
                for column in columns:
                    query_input = (
                        uuid4(),
                        column.get('name'),
                        column.get('description', ''),
                        column.get('comment', ''),
                        column.get('type'),
                        column.get("tags") if column.get("tags") else [],
                        True,
                        False,
                        pipeline_task_id,
                        pipeline_id,
                        asset_id,
                        connection_id,
                    )
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals})", query_input
                    ).decode("utf-8")
                    insert_objects.append(query_param)

                insert_objects = split_queries(insert_objects)
                for input_values in insert_objects:
                    try:
                        query_input = ",".join(input_values)
                        query_string = f"""
                            insert into core.pipeline_columns(
                                id, name, description,  comment, data_type, tags,
                                is_active, is_delete, pipeline_task_id, pipeline_id, asset_id, connection_id
                            ) values {query_input} 
                        """
                        cursor = execute_query(
                            connection, cursor, query_string)
                    except Exception as e:
                        log_error(
                            'DBT Jobs Tasks Columns Insert Failed  ', e)
    except Exception as e:
        log_error(f"DBT Connector - Save Tasks Columns By Job ID Failed ", e)
        raise e


def __prepare_lineage(config, job_id, data, default_queries, source_connection) -> dict:
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        lineage_config = {"tables": [], "relations": []}

        if job_id and data:
            # extra_params = {"credentials": credentials, "job_id": job_id}
            lineage_config, task_mapping = __get_job_lineage(
                config, data, lineage_config, job_id, default_queries, source_connection)
            lineage_config = __prepare_lineage_relations(
                config, lineage_config, default_queries, source_connection, task_mapping)

            # Removing the tables which don't populate data
            unique_ids = {rel["srcTableId"] for rel in lineage_config["relations"] if rel["srcTableId"] != rel["tgtTableId"]} | \
                {rel["tgtTableId"] for rel in lineage_config["relations"]
                    if rel["srcTableId"] != rel["tgtTableId"]}

            lineage_config["tables"] = [
                table for table in lineage_config["tables"] if table["id"] in unique_ids]
            lineage_config["relations"] = [rel for rel in lineage_config["relations"] if rel["srcTableId"]
                                           != rel["tgtTableId"] and rel["srcTableId"] != '' and rel["tgtTableId"] != '']
            map_asset_with_lineage(config, lineage_config, "pipeline")
            # Save lineage
            save_lineage(config, "pipeline", lineage_config, asset_id)

    except Exception as e:
        log_error(f"Wherescape Connector - Get Model Lineage ", e)


def __get_job_lineage(config, lineageSrcData: dict, lineage_config: dict, job_id, default_queries, source_connection) -> dict:
    try:
        models = lineageSrcData.get('models', [])
        lineage_tables = []
        table_task_mapping = {}
        for model in models:
            current_model, mapping = __map_lineage_data(
                config, model, {"level": 2}
            )
            lineage_tables += [current_model]
            table_task_mapping.update(mapping)
        lineage_config.update({"tables": lineage_tables})
    except Exception as e:
        log_error(f"Wherescape Connector - Get Job Lineage ", e)
    finally:
        return lineage_config, table_task_mapping


def __prepare_lineage_relations(config, lineage_config: dict, default_queries, source_connection, task_mapping) -> dict:
    try:
        tables = lineage_config.get("tables", [])
        relations = []
        credentials = config.get("connection", {}).get("credentials", {})
        database = credentials.get("database", '')
        schema = credentials.get("schema", '')
        for table in tables:
            table_type = table.get("table_type", "")
            if table_type:
                table_type = table_type.lower().replace(' ', '_')
                table_id = table.get("table_id")
                lineage_query = default_queries.get(table_type+'_lineage', '')
                lineage_query = lineage_query.replace('<table_id>', str(table_id)).replace(
                    "<database_name>", database).replace("<schema_name>", schema)
                if lineage_query:
                    fields, _ = execute_native_query(
                        config, lineage_query, source_connection, is_list=True)
                    if fields:
                        for i, field in enumerate(fields):
                            src_id = str(field.get("srctableid"))
                            tgt_id = str(field.get("tgttableid"))
                            fields[i] = {
                                "srcTableId": str(task_mapping.get(src_id, '')),
                                "tgtTableId": str(task_mapping.get(tgt_id, '')),
                                "srcTableColName": field.get("srctablecolname"),
                                "tgtTableColName": field.get("tgttablecolname")
                            }
                        relations += fields

        lineage_config.update(
            {
                "relations": relations,
                "tables": tables
            }
        )
    except Exception as e:
        log_error(f"Wherescape Connector - Prepare Lineage Relations ", e)
    finally:
        return lineage_config


def __prepare_search_key(data: dict) -> dict:
    search_keys = ""
    try:
        keys = {
            "id": data.get("id", ""),
            "name": data.get("name", ""),
            "description": data.get("description", "")
        }
        keys = keys.values()
        keys = [str(x) for x in keys if x]

        if len(keys) > 0:
            search_keys = " ".join(keys)
    except Exception as e:
        log_error(f"Wherescape Connector - Prepare Search Keys ", e)
    finally:
        return search_keys


def __prepare_description(properties):
    name = properties.get("name", "")
    project_name = properties.get("project_name", "")
    group_name = properties.get("group_name", "")
    owner_name = properties.get("owner_name", "")
    runGeneratedAt = properties.get("runGeneratedAt", "")

    generated_description = f"""This Wherescape job {name} is part of Project {project_name} and group {group_name}. 
Owned by {owner_name} and runs generated at {runGeneratedAt}"""
    job_description = properties.get('description')
    job_description = f"{job_description}".strip()
    if job_description:
        return job_description
    return f"{generated_description}".strip()


def get_tasks_by_job_id(config, job_id, default_queries, source_connection) -> dict:
    try:
        task_query = default_queries.get('task_by_job')
        credentials = config.get("connection", {}).get("credentials", {})
        database = credentials.get("database", '')
        schema = credentials.get("schema", '')
        task_query = task_query.replace('<job_id>', str(job_id)).replace(
            "<database_name>", database).replace("<schema_name>", schema)
        task_data, _ = execute_native_query(
            config, task_query, source_connection, is_list=True)
        if task_data:
            for i, task in enumerate(task_data):
                task_data[i] = {
                    "uniqueId": task.get('uniqueid', ''),
                    "name": task.get('name', ''),
                    "table_type": task.get('table_type', ''),
                    "job_id": task.get('job_id', ''),
                    "task_action": task.get('task_action', ''),
                    "table_id": task.get('table_id', ''),
                    "status": task.get('status', ''),
                    "wtc_audit_status": task.get('wtc_audit_status', ''),
                    "executeStartedAt": task.get('executestartedat', ''),
                    "executeCompletedAt": task.get('executecompletedat', ''),
                    "runGeneratedAt": task.get('rungeneratedat', ''),
                    "executionTime": task.get('executiontime', ''),
                    "lastRunID": task.get('lastrunid', None),
                    "schema": task.get('oo_schema', ''),
                    "source_table_name": task.get('source_table_name', ''),
                    "stored_procedure": task.get('stored_procedure')
                }
            p_tasks = list(filter(lambda x: x.get(
                "status") == "success", task_data))
            last_run_id = task_data[0].get("lastRunID", '')
            task_data = get_task_columns(
                config, task_data, default_queries, source_connection)
            response = {
                "models": task_data,
                "runGeneratedAt": task_data[0].get("runGeneratedAt"),
                "tot_task": len(task_data),
                "p_tasks": len(p_tasks),
                "passed": len(p_tasks),
                "failed": len(task_data) - len(p_tasks),
                "last_run_id": last_run_id,
                "runId": last_run_id
            }
        return response
    except Exception as e:
        log_error(f"Wherescape Connector - Get Tasks ", e)


def get_tasks_by_run_id(config, job_id: str, run_id: str, default_queries, source_connection) -> dict:
    try:
        runs_query = default_queries.get('tasks_by_run_id')
        credentials = config.get("connection", {}).get("credentials", {})
        database = credentials.get("database", '')
        schema = credentials.get("schema", '')
        runs_query = runs_query.replace('<job_id>', str(job_id)).replace('<run_id>', str(run_id)).replace(
            "<database_name>", database).replace("<schema_name>", schema)
        response, _ = execute_native_query(
            config, runs_query, source_connection, is_list=True)
        response = response if response else {}
        if response:
            for i, task in enumerate(response):
                response[i] = {
                    "runId": run_id,
                    "uniqueId": task.get("uniqueid", ""),
                    "name": task.get("name", ""),
                    "status": task.get("status", ""),
                    "executeStartedAt": task.get("executestartedat", ""),
                    "executeCompletedAt": task.get("executecompletedat", ""),
                    "compiledSql": task.get("compiledsql", ""),
                    "executionTime": task.get("executiontime", ""),
                    "runGeneratedAt": task.get("rungeneratedat", ""),
                }
        # tasks = {"models": response}
        return response
    except Exception as e:
        log_error(f"Wherescape Connector - Get Jobs Tests and Models ", e)
        raise e

def __save_wherescape_pipeline_jobs(config: dict, data: dict) -> None:
    """
    Save / Update Jobs For Pipeline
    """
    try:
        connection = get_postgres_connection(config)
        asset = config.get("asset", {})
        asset_id = asset.get("id")

        if data:
            data = json.dumps(data, default=str).replace("'", "''")
            connection = get_postgres_connection(config)
            with connection.cursor() as cursor:
                query_string = f"""
                    update core.pipeline set properties='{data}'
                    where asset_id = '{asset_id}'
                """
                cursor = execute_query(connection, cursor, query_string)
    except Exception as e:
        log_error(str(e), e)


def __save_pipeline_runs(config: dict, runs: list, data: dict, default_queries, source_connection) -> None:
    """
    Save / Update Runs For Pipeline
    """
    LATEST_RUN = False
    try:
        connection_data = config.get("connection", {})
        connection_id = connection_data.get("id")
        connection = get_postgres_connection(config)
        asset = config.get("asset", {})
        asset_id = asset.get("id")

        pipeline_id = get_pipeline_id(config)

        if connection_id and runs:
            for i, item in enumerate(runs):
                task_by_run = get_tasks_by_run_id(
                    config,
                    item.get("job_id"),
                    item.get("run_id"),
                    default_queries,
                    source_connection
                )
                runs[i] = {
                    "id": item.get("run_id"),
                    "uniqueId": item.get("run_id"),
                    "project_id": item.get("project_id"),
                    "project_name": data.get("project_name"),
                    "group_id": item.get("group_id"),
                    "group_name": data.get("group_name"),
                    "status": item.get("status"),
                    "status_humanized": item.get("status"),
                    "status_message": item.get('error_msg', ''),
                    "created_at": item.get("created_at"),
                    "started_at": item.get("started_at"),
                    "finished_at": item.get("finished_at"),
                    "job": item.get("job_name"),
                    "in_progress": True if item.get("status") == "running" else False,
                    "is_complete": True if item.get("status") == "success" else False,
                    "is_success": True if item.get("status") == "success" else False,
                    "is_error": True if item.get("status") != 'success' else False,
                    "duration": item.get("duration"),
                    "run_duration": item.get("run_duration"),
                    "job_id": item.get("job_id", ""),
                    "run_id": item.get("run_id", ""),
                    "is_running": True if item.get("status") == "running" else False,
                    "run_type": item.get("run_type", ""),
                    "models": task_by_run
                }

            connection = get_postgres_connection(config)

        ALL_RUNS = runs
        with connection.cursor() as cursor:
            insert_objects = []
            for run in runs:
                run_id = run.get('id')

                # Clear Existing Runs Details
                query_string = f"""
                    delete from core.pipeline_runs_detail
                    where
                        asset_id = '{asset_id}'
                        and pipeline_id = '{pipeline_id}'
                        and run_id = '{run_id}'
                """
                cursor = execute_query(connection, cursor, query_string)

                properties = {}

                # Validating existing runs
                query_string = f"""
                    select id from core.pipeline_runs
                    where
                        asset_id = '{asset_id}'
                        and pipeline_id = '{pipeline_id}'
                        and source_id = '{run_id}'
                """
                cursor = execute_query(connection, cursor, query_string)
                existing_run = fetchone(cursor)
                existing_run = existing_run.get('id') if existing_run else None

                if existing_run:
                    query_string = f"""
                        update core.pipeline_runs set 
                            status = '{get_pipeline_status(run.get('status_humanized'))}', 
                            error = '{run.get('error', '')}',
                            run_start_at = {f"'{run.get('started_at')}'" if run.get('started_at') else 'NULL'},
                            run_end_at = {f"'{run.get('finished_at')}'" if run.get('finished_at') else 'NULL'},
                            duration = '{run.get('duration', '')}' ,
                            properties = '{json.dumps(properties, default=str).replace("'", "''")}'
                        where id = '{existing_run}'
                    """
                    cursor = execute_query(connection, cursor, query_string)

                else:
                    query_input = (
                        uuid4(),
                        run_id,
                        run_id,
                        get_pipeline_status(run.get('status_humanized')),
                        run.get('error', ''),
                        run.get('started_at') if run.get(
                            'started_at') else None,
                        run.get('finished_at') if run.get(
                            'finished_at') else None,
                        run.get('duration', ''),
                        json.dumps(properties, default=str).replace("'", "''"),
                        True,
                        False,
                        pipeline_id,
                        asset_id,
                        connection_id,
                    )
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals})", query_input
                    ).decode("utf-8")
                    insert_objects.append(query_param)
                    if not LATEST_RUN:
                        LATEST_RUN = True

            insert_objects = split_queries(insert_objects)
            for input_values in insert_objects:
                try:
                    query_input = ",".join(input_values)
                    query_string = f"""
                        insert into core.pipeline_runs(
                            id, source_id, technical_id, status, error,  run_start_at, run_end_at, duration,
                            properties, is_active, is_delete, pipeline_id, asset_id, connection_id
                        ) values {query_input}
                    """
                    cursor = execute_query(
                        connection, cursor, query_string)
                except Exception as e:
                    log_error('DBT Jobs Runs Insert Failed  ', e)
        return runs, LATEST_RUN
    except Exception as e:
        log_error(str(e), e)
        raise e


def __map_lineage_data(
    config, data: dict, params: dict = None
) -> dict:
    try:
        level = params.get("level", 0)
        table_id = data.get("table_id", None)
        id = data.get("uniqueId", "")

        table_task_mapping = {
            str(table_id): id
        }

        fields = data.get("columns", [])
        fields = __prepare_columns_list_data(fields)
        source_table_name = data.get("source_table_name", "")
        if source_table_name and " " in source_table_name:
            source_table_name = source_table_name.split(" ")[0]
        schema, database, table = extract_table_name(str(source_table_name), schema_only=True)
        obj = {
            "id": str(id),
            "table_id": str(table_id),
            "name": table if table else data.get("name", ""),
            "src_id": str(id),
            "type": "Pipeline",
            # "target_id": target_id,
            "database": database,
            "schema": schema,
            "alias": data.get("name", ""),
            # "identifier": data.get("identifier", ""),
            # "sourceName": data.get("sourceName", ""),
            # "sourceDescription": data.get("sourceDescription", ""),
            "connection_type": "wherescape",
            "runGeneratedAt": data.get("runGeneratedAt", ""),
            "runElapsedTime": data.get("executiontime", ""),
            "table_type": data.get("table_type", ""),
            "owner": data.get("owner", ""),
            "level": level,
            "fields": fields if fields else [],
            "status": data.get("status", ""),
            "overall_status": data.get("status", ""),
        }
        return obj, table_task_mapping
    except Exception as e:
        log_error("Wherescape Map Lineage By Type Failed ", e)


def __prepare_columns_list_data(columns: list = []) -> list:
    m_columns = []
    try:
        if columns:
            for column in columns:
                obj = {
                    "name": column.get("name", ""),
                    "datatype": column.get("type", ""),
                    "description": column.get("description", ""),
                    "comment": column.get("comment", ""),
                    "type": "column"
                }
                m_columns.append(obj)
    except Exception as e:
        log_error(f"Wherescape Connector - Prepare Columns List ", e)
    finally:
        return m_columns


def get_task_columns(config, models, default_queries, source_connection):
    for model in models:
        table_type = model.get("table_type", "")
        table_id = model.get("table_id")
        credentials = config.get("connection", {}).get("credentials", {})
        database = credentials.get("database", '')
        schema = credentials.get("schema", '')
        if table_type:
            table_type = table_type.lower().replace(' ', '_')
            column_query = default_queries.get(table_type+'_columns', '')
            column_query = column_query.replace('<table_id>', str(table_id)).replace(
                "<database_name>", database).replace("<schema_name>", schema)
            if column_query:
                column_data, _ = execute_native_query(
                    config, column_query, source_connection, is_list=True)
                model.update({
                    'columns': column_data
                })
    return models


def __update_pipeline_stats(config: dict) -> str:
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:

            # Get Last Run Info
            query_string = f"""
                select
                    id,
                    source_id,
                    run_end_at,
                    duration,
                    status
                from
                    core.pipeline_runs
                where asset_id = '{asset_id}'
                order by run_end_at desc
                limit 1
            """
            cursor = execute_query(connection, cursor, query_string)
            last_run = fetchone(cursor)
            last_run = last_run if last_run else {}

            # Get Pipeline Stats
            query_string = f"""
                select 
                    pipeline.id,
                    count(distinct pipeline_tasks.id) as tot_tasks,
                    count(distinct pipeline_tests.id) as tot_tests,
                    count(distinct pipeline_columns.id) as tot_columns,
                    count(distinct pipeline_runs.id) as tot_runs
                from pipeline
                left join core.pipeline_tasks  on pipeline_tasks.asset_id = pipeline.asset_id
                left join core.pipeline_tests  on pipeline_tests.asset_id = pipeline.asset_id
                left join core.pipeline_columns on pipeline_columns.asset_id = pipeline.asset_id
                left join core.pipeline_runs on pipeline_runs.asset_id = pipeline.asset_id
                where pipeline.asset_id ='{asset_id}'
                group by pipeline.id
            """
            cursor = execute_query(connection, cursor, query_string)
            pipeline_stats = fetchone(cursor)
            pipeline_stats = pipeline_stats if pipeline_stats else {}

            # Update Pipeline Status
            query_string = f"""
                select id, properties from core.pipeline
                where asset_id='{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            report = fetchone(cursor)
            properties = report.get('properties', {})
            properties.update({
                "tot_tasks": pipeline_stats.get('tot_tasks', 0),
                "tot_tests": pipeline_stats.get('tot_tests', 0),
                "tot_columns":  pipeline_stats.get('tot_columns', 0),
                "tot_runs":  pipeline_stats.get('tot_runs', 0)
            })

            run_id = last_run.get('source_id', '')
            status = last_run.get('status', '')
            last_run_at = last_run.get('run_end_at')
            query_string = f"""
                update core.pipeline set 
                    run_id='{run_id}', 
                    status='{status}', 
                    last_run_at= {f"'{last_run_at}'" if last_run_at else 'NULL'},
                    properties = '{json.dumps(properties, default=str).replace("'", "''")}'
                where asset_id = '{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
    except Exception as e:
        log_error(f"Wherescape Connector - Update Run Stats to Job Failed ", e)
        raise e


def extract_pipeline_measure(run_history: list, tasks_pull: bool):
    if not run_history:
        return
    
    # Fetch pipeline_name (job_name) from pipeline table
    asset = TASK_CONFIG.get("asset", {})
    asset_id = asset.get("id") if asset else TASK_CONFIG.get("asset_id")
    if not asset_id:
        asset_id = TASK_CONFIG.get("asset_id")
    
    connection = get_postgres_connection(TASK_CONFIG)
    pipeline_name = None
    if asset_id:
        with connection.cursor() as cursor:
            pipeline_query = f"""
                select p.name as pipeline_name
                from core.pipeline p
                where p.asset_id = '{asset_id}'
                limit 1
            """
            cursor = execute_query(connection, cursor, pipeline_query)
            pipeline_result = fetchone(cursor)
            pipeline_name = pipeline_result.get("pipeline_name") if pipeline_result else None
    
    run_history = sorted(run_history, key=lambda x: x.get("started_at"), reverse=True)[:2]
    latest_run = run_history[0]
    previous_run = run_history[1] if len(run_history) > 1 else {}
    job_run_detail = {
        "duration": latest_run.get("duration"),
        "last_run_date": latest_run.get("started_at"),
        "previous_run_date": previous_run.get("started_at") if previous_run.get("started_at") else None,
    }
    # Pass pipeline_name (job_name) for asset level measures
    execute_pipeline_measure(TASK_CONFIG, "asset", job_run_detail, job_name=pipeline_name)

    # Get Tasks and execute measure
    if not tasks_pull:
        return
    tasks = get_pipeline_tasks(TASK_CONFIG)
    if not tasks:
        return
    latest_run_task = latest_run.get("models", [])
    previous_run_task = previous_run.get("models", [])
    for task in tasks:
        task_name = task.get("name")
        last_task_run_detail = next(
            (t for t in latest_run_task if t.get("name") == task_name), None
        )
        previous_run_detail = next(
            (t for t in previous_run_task if t.get("name") == task_name), None
        )
        if not last_task_run_detail:
            continue
        run_detail = {
            "duration": last_task_run_detail.get("duration") / 1000,
            "last_run_date": last_task_run_detail.get("executeStartedAt"),
            "previous_run_date": previous_run_detail.get("executeStartedAt") if previous_run_detail else None,
        }
        # run_detail['is_row_written'] = last_task_run_detail.get("is_row_written")
        # run_detail["rows_affected"] = last_task_run_detail.get("rows_count")
        # run_detail["rows_inserted"] = last_task_run_detail.get("rows_inserted")
        # run_detail["rows_updated"] = last_task_run_detail.get("rows_updated")
        # run_detail["rows_deleted"] = last_task_run_detail.get("rows_deleted")
        # Pass pipeline_name (job_name) and task_name for task level measures
        execute_pipeline_measure(TASK_CONFIG, "task", run_detail, task_info=task, job_name=pipeline_name, task_name=task_name)