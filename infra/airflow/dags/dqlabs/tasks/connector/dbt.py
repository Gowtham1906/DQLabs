"""
    Migration Notes From V2 to V3:
    Migrations Completed
"""

import json
from uuid import uuid4
import re
import datetime
import sqlglot
from sqlglot import parse_one, exp

from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone, split_queries
from dqlabs.app_helper.dq_helper import get_pipeline_status
from dqlabs.app_helper.connection_helper import decrypt_connection_config
from dqlabs.app_helper.dag_helper import (
    get_postgres_connection,
)
from dqlabs.utils.extract_workflow import update_asset_run_id
from dqlabs.app_helper.lineage_helper import (
    save_lineage,  update_pipeline_propagations, map_asset_with_lineage, handle_alerts_issues_propagation,
    get_asset_metric_count, update_asset_metric_count, update_materializes_asset_id, save_lineage_entity,
    get_task_ids_by_source_ids, dbt_exposures_tag_users_mapping, map_exposure_with_lineage
    )
from dqlabs.app_helper.log_helper import log_error
from dqlabs.utils.tasks import get_task_config, check_task_status
from dqlabs.utils.request_queue import (
    update_queue_detail_status,
    update_queue_status,
    create_queue_detail,
)
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.app_helper import agent_helper
from dqlabs.app_helper.pipeline_helper import pipeline_auto_tag_mapping, update_pipeline_last_runs
from dqlabs.utils.pipeline_measure import execute_pipeline_measure, get_pipeline_tasks

TASK_CONFIG = None
ARTIFACTS = None
CATALOG_ARTIFACT = None


def extract_dbt_data(config, **kwargs):
    try:
        is_completed = check_task_status(config, kwargs)
        if is_completed:
            return

        global TASK_CONFIG
        global ARTIFACTS
        task_config = get_task_config(config, kwargs)
        run_id = config.get("run_id")
        update_queue_detail_status(
            config, ScheduleStatus.Running.value, task_config=task_config
        )
        update_queue_status(config, ScheduleStatus.Running.value, True)
        update_asset_run_id(run_id, config)
        connection = config.get("connection", {})
        asset = config.get("asset", {})
        asset_id = asset.get("id")

        asset_properties = asset.get("properties")
        credentials = connection.get("credentials")
        connection_type = connection.get("type", "")
        credentials = decrypt_connection_config(credentials, connection_type)
        TASK_CONFIG = config
        latest_run = False

        is_valid = __validate_connection_establish()
        if is_valid:
            job_id = asset_properties.get("id")

            # Get and Save Job Informations
            __get_job_by_id(config, credentials, job_id)
            adapter_type = __get_adapter_type_from_environment(config, credentials,job_id)

            # Get and Save Pipeline Task Informations
            latest_run = __get_models_and_tests_by_job_id(config, credentials, job_id, adapter_type, connection_type)

            # Update Job Run Stats
            __update_pipeline_stats(config)

            # Save Propagation Values
            update_pipeline_propagations(config, asset)

            description = __prepare_description(asset_properties)
            description = description.replace("'", "''")

            # Get Description and Prepare Search Key
            search_keys = __prepare_search_key(asset_properties)
            search_keys = search_keys.replace("'", "''")

            connection = get_postgres_connection(config)
            with connection.cursor() as cursor:
                query_string = f"""
                    update core.asset set description='{description}', search_keys='{search_keys}'
                    where id = '{asset_id}' and is_active=true and is_delete = false
                """
                cursor = execute_query(connection, cursor, query_string)

        # update request queue status
        is_triggered = config.get("is_triggered")
        if not is_triggered:
            create_queue_detail(config)
        update_queue_detail_status(config, ScheduleStatus.Completed.value)
        update_queue_status(config, ScheduleStatus.Completed.value, True)
        update_pipeline_last_runs(config)
        
        propagate_alerts = credentials.get("propagate_alerts", "table")
        metrics_count = None
        if propagate_alerts == "table":
            metrics_count = get_asset_metric_count(config, asset_id)
        if latest_run:
            extract_pipeline_measure(config, job_id, credentials)
        if propagate_alerts == "table" or propagate_alerts == "pipeline":
            update_asset_metric_count(config, asset_id, metrics_count, latest_run, propagate_alerts=propagate_alerts)
    except Exception as e:
        log_error("Dbt Pull / Push Failed", e)
        update_queue_detail_status(config, ScheduleStatus.Failed.value, str(e))


def __get_response(
    url,
    method_type: str = "get",
    is_metadata=False,
    params=None,
    api_version: str = "v2",
    is_artifact=False
):
    try:
        global TASK_CONFIG
        url = f"{api_version}/{url}" if url else ""
        pg_connection = get_postgres_connection(TASK_CONFIG)
        api_response = agent_helper.execute_query(
            TASK_CONFIG,
            pg_connection,
            "",
            method_name="execute",
            parameters=dict(
                request_url=url,
                request_type=method_type,
                request_params=params,
                is_metadata=is_metadata,
                is_artifact=is_artifact
            ),
        )
        api_response = api_response if api_response else {}
        return api_response
    except Exception as e:
        raise e


def __validate_connection_establish() -> tuple:
    try:
        is_valid = False
        response = __get_response(f"accounts/")
        is_valid = bool(response)
        return (bool(is_valid), "")
    except Exception as e:
        log_error(f"DBT Connector - Validate Connection Failed ", e)
        return (is_valid, str(e))


def __get_pipeline_id(config: dict) -> str:
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select id from core.pipeline
                where asset_id = '{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            pipeline = fetchone(cursor)
            if pipeline:
                return pipeline.get('id')
    except Exception as e:
        log_error(
            f"DBT Connector - Get Pipeline Primary Key Information By Asset ID Failed ", e)
        raise e


def __get_last_run_id(config: dict) -> str:
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select id, source_id from core.pipeline_runs
                where asset_id = '{asset_id}'
                order by source_id desc
                limit 1
            """
            cursor = execute_query(connection, cursor, query_string)
            runs = fetchone(cursor)
            return runs.get('source_id') if runs and runs.get('source_id') else None
    except Exception as e:
        log_error(
            f"DBT Connector - Get Last Run Id By Asset ID Failed ", e)
        raise e


def __prepare_description(properties) -> str:
    name = properties.get("name", "")
    account = properties.get("account", "")
    project = properties.get("project", "")
    environment = properties.get("environment", "")
    description = f"""This Job {name} is part of Account {account} and under this Project {project} and Environment {environment}. """
    return f"{description}".strip()


def __prepare_search_key(properties: dict) -> dict:
    keys = properties.values()
    keys = [str(x)for x in keys if x]

    if len(keys) > 0:
        search_keys = " ".join(keys)
    return search_keys


def __get_job_by_id(config: dict, credentials: dict, job_id: str):
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        account_id = credentials.get("account_id", "")
        request_url = f"accounts/{account_id}/jobs/{job_id}"
        response = __get_response(request_url)
        response = response if response else {}
        response = response.get("data", None)
        if response:
            response = json.dumps(response, default=str).replace("'", "''")
            connection = get_postgres_connection(config)
            with connection.cursor() as cursor:
                query_string = f"""
                    update core.pipeline set properties='{response}'
                    where asset_id = '{asset_id}'
                """
                cursor = execute_query(connection, cursor, query_string)
    except Exception as e:
        log_error(f"DBT Connector - Get Job Information By Job ID Failed ", e)
        raise e


def __get_models_and_tests_by_job_id(config: dict, credentials: dict, job_id: str, adapter_type: str, connection_type: str):
    try:
        meta_query_params = {
            "query": """
            {
                job(id: """ + str(job_id) + """)
                {
                    runId
                    models {
                        uniqueId
                        jobId
                        runId
                        name
                        description
                        owner
                        dependsOn
                        childrenL1
                        rawSql
                        compiledSql
                        tags
                        error
                        status
                        executionTime
                        executeStartedAt
                        executeCompletedAt
                        columns {
                            name
                            type
                            comment
                            description
                            tags
                        }
                        database
                        schema
                        tests {
                            uniqueId
                            name
                            status
                            error
                        }
                    }
                    tests {
                        uniqueId
                        jobId
                        runId
                        name
                        description
                        status
                        tags
                        rawSql
                        compiledSql
                        columnName
                        error
                        executeStartedAt
                        executeCompletedAt
                        executionTime
                        dependsOn
                    }
                }
            }"""
        }
        response = __get_response("", "post", True, meta_query_params)
        response = response if response else {}
        response = response.get("data", {}).get(
            "job", {}) if response else None
        run_id = response.get('runId') if response else None

        # Config
        metadata_pull_config = credentials.get("metadata", {})
        runs_pull = metadata_pull_config.get("runs", False)
        tasks_pull = metadata_pull_config.get("tasks", False)
        tests_pull = metadata_pull_config.get("tests", False)
        auto_tag_mapping = credentials.get("auto_mapping_tags", False)
        latest_run = False

        # Save Job Runs
        if run_id and runs_pull:
            latest_run =__get_job_runs(config, credentials, job_id,
                           run_id, auto_tag_mapping)

        # Save Job Models
        models = response.get("models", None) if response else None
        if models:
            if tasks_pull:
                
                __save_models(config, models, auto_tag_mapping, adapter_type)
            # Fetch catalog.json artifact from the latest successful job run
            __get_job_artifact_catalog(credentials, job_id)
            __prepare_lineage(config, credentials, job_id, models, CATALOG_ARTIFACT, adapter_type, connection_type)

        # Save Job Tests
        if tests_pull:
            tests = response.get("tests", None) if response else None
            if tests:
                __save_tests(config, tests, auto_tag_mapping)
        return latest_run
    except Exception as e:
        log_error(
            f"DBT Connector - Get Models List Information By Job ID Failed ", e)
        raise e


def __get_job_runs(config: dict, credentials: dict, job_id: str, run_id: str, auto_tag_mapping: bool):
    try:
        account_id = credentials.get("account_id", "")
        no_of_runs = credentials.get("no_of_runs", "30")
        status_filter = credentials.get("status", "all")
        latest_run = False

        # Pull last Runs History
        last_run_id = __get_last_run_id(config)
        last_run_id = int(last_run_id) if last_run_id else 0
        if last_run_id == 0:
            run_params = {"initial": True}
        else:
            run_id = int(run_id) if run_id else 0
            last_run_id = run_id if run_id > last_run_id else last_run_id
            run_params = {
                "run_id": last_run_id,
                "condition": "eq" if run_id == last_run_id else "gt",
            }

        account_id = credentials.get("account_id", "")
        request_url = f"accounts/{account_id}/runs/?job_definition_id={job_id}"
        if status_filter != "all":
            status_filter = 10 if status_filter == "success" else 20
            request_url = f"{request_url}&status={status_filter}"
        if run_params.get("initial", None):
            no_of_runs = int(no_of_runs) if type(
                no_of_runs) == str else no_of_runs
            end = datetime.datetime.today()
            start = end - datetime.timedelta(days=no_of_runs)
            dates = [
                start.strftime("%Y-%m-%d %H:%M:%S"),
                end.strftime("%Y-%m-%d %H:%M:%S"),
            ]
            request_url = f"{request_url}&created_at__range={dates}"
        else:
            run_id = run_params.get("run_id", None)
            condition = run_params.get("condition", None)
            if run_id and condition:
                if condition == "eq":
                    request_url = f"{request_url}&pk={run_id}"
                elif condition == "gt":
                    request_url = f"{request_url}&id__gt={run_id}"

        response = __get_response(request_url)
        response = response if response else {}
        response = response.get("data", None)
        if response:
            latest_run = __save_runs(config, response)
            __get_runs_details(config, job_id)
        return latest_run
    except Exception as e:
        log_error(
            f"DBT Connector - Get Runs List By Job ID Failed ", e)
        raise e


def __save_runs(config: dict, runs: list):
    try:
        connection_obj = config.get("connection", {})
        connection_id = connection_obj.get('id')

        asset = config.get("asset", {})
        asset_id = asset.get("id")
        latest_run = False

        pipeline_id = __get_pipeline_id(config)

        connection = get_postgres_connection(config)
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

                properties = {
                    "git_branch": run.get('git_branch'),
                    "git_sha": run.get('git_sha'),
                    "artifact_s3_path": run.get('artifact_s3_path'),
                    "href": run.get('href')
                }

                # Get Durations
                duration = None
                if run.get('duration'):
                    datm = datetime.datetime.strptime(
                        run.get('duration'), "%H:%M:%S")
                    a_timedelta = datm - datetime.datetime(1900, 1, 1)
                    duration = a_timedelta.total_seconds()

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
                            error = '{
                                (
                                    run.get("error", "").replace(
                                        "'", "''")
                                    if run.get("error")
                                    else ""
                                )
                            }',
                            run_start_at = {f"'{run.get('started_at')}'" if run.get('started_at') else 'NULL'},
                            run_end_at = {f"'{run.get('finished_at')}'" if run.get('finished_at') else 'NULL'},
                            duration = '{duration}' ,
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
                        duration,
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
                    if not latest_run:
                        latest_run = True

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
        return latest_run
    except Exception as e:
        log_error(f"DBT Connector - Save Runs By Job ID Failed ", e)
        raise e


def __get_runs_details(config: dict, job_id: str):
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        pipeline_id = __get_pipeline_id(config)

        # Get Runs Details
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select pipeline_run.id, pipeline_run.source_id
                from core.pipeline_runs as pipeline_run
                left join core.pipeline_runs_detail as pipeline_run_detail
                on pipeline_run_detail.run_id = pipeline_run.source_id
                and pipeline_run_detail.asset_id = pipeline_run.asset_id
                and pipeline_run_detail.pipeline_id = pipeline_run.pipeline_id
                where pipeline_run.asset_id = '{asset_id}'
                and pipeline_run.pipeline_id = '{pipeline_id}'
                and pipeline_run_detail.run_id is null
            """
            cursor = execute_query(connection, cursor, query_string)
            runs = fetchall(cursor)

            for run in runs:
                meta_query_params = {
                    "query": """
                        {
                            job(id: """
                    + str(job_id)
                    + """, runId: """
                    + str(run.get('source_id'))
                    + """) {
                                tests {
                                    uniqueId
                                    status
                                    executeCompletedAt
                                    executeStartedAt
                                    executionTime
                                    name
                                    rawSql
                                    compiledSql
                                    error,
                                    runId
                                }
                                models {
                                    uniqueId
                                    name
                                    status
                                    executeStartedAt
                                    executeCompletedAt
                                    rawSql
                                    compiledSql
                                    executionTime
                                    runId,
                                    error
                                }
                        }
                    }"""
                }
                response = __get_response("", "post", True, meta_query_params)
                response = response if response else {}
                response = response.get("data", {}).get(
                    "job", {}) if response else None
                if response:
                    __save_runs_details(config, run, response)

    except Exception as e:
        log_error(
            f"DBT Connector - Get Runs Details By Job ID Failed ", e)
        raise e


def __save_runs_details(config: dict, run: dict,  data: list):
    try:
        connection_obj = config.get("connection", {})
        connection_id = connection_obj.get('id')
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        pipeline_id = __get_pipeline_id(config)
        pipeline_run_id = run.get("id")

        models = data.get('models', [])
        tests = data.get('tests', [])

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

             # Save Tests Which Runs / Status
            for test in tests:
                if test.get('status'):
                    query_input = (
                        uuid4(),
                        test.get("runId", ''),
                        test.get('uniqueId'),
                        'test',
                        test.get('name'),
                        get_pipeline_status(test.get('status')),
                        test.get('error', ''),
                        (
                            model.get("rawSql", "").replace("'", "''")
                            if model.get("rawSql")
                            else ""
                        ),
                        (
                            test.get("compiledSql", "").replace("'", "''")
                            if test.get("compiledSql")
                            else ""
                        ),
                        test.get('executeStartedAt') if test.get(
                            'executeStartedAt') else None,
                        test.get('executeCompletedAt') if test.get(
                            'executeCompletedAt') else None,
                        test.get('executionTime') if test.get(
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


def __save_models(config: dict, models: list, auto_tag_mapping: bool, adapter_type: str):
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        mapping_tags = {}

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select pipeline_tasks.id, pipeline_tasks.source_id from core.pipeline_tasks
                join core.pipeline on pipeline.asset_id = pipeline_tasks.asset_id
                where pipeline_tasks.asset_id = '{asset_id}' and is_selected = true and pipeline.is_delete = false
            """
            cursor = execute_query(connection, cursor, query_string)
            existings_models = fetchall(cursor)

            for model in existings_models:
                new_model = next((x for x in models if x.get(
                    "uniqueId") == model.get("source_id")), None)
                if new_model:
                    properties = {
                        "dependsOn": new_model.get("dependsOn"),
                        "childrenL1": new_model.get("childrenL1"),
                        "materializes": {
                            "adapter_type":adapter_type,
                            "database": new_model.get("database", ""),
                            "schema": new_model.get("schema", ""),
                            "name": new_model.get("name", ""),
                            "fqn": (
                                f"{new_model.get('database','')}.{new_model.get('schema','')}.{new_model.get('name','')}"
                            ).strip(".")
                        }
                    }
                    properties["materializes"] = update_materializes_asset_id(
                        config, properties.get("materializes", {}),  model_name=new_model.get("name"), task_id=model.get("id")
                    )
                    status = get_pipeline_status(new_model.get(
                        'status') if new_model.get('status') else "")
                    tags = new_model.get(
                        "tags") if new_model.get("tags") else []

                    if tags:
                        mapping_tags.update({model.get("id"): tags})
                    query_string = f"""
                        update core.pipeline_tasks set
                            status = '{status}',
                            source_type = 'model',
                            error = '{
                                (
                                    new_model.get("error", "").replace(
                                        "'", "''")
                                    if new_model.get("error")
                                    else ""
                                )
                            }',
                            source_code = '{
                                (
                                    new_model.get("rawSql", "").replace(
                                        "'", "''")
                                    if new_model.get("rawSql")
                                    else ""
                                )
                            }',
                            compiled_code = '{
                                (
                                    new_model.get("compiledSql", "").replace(
                                        "'", "''")
                                    if new_model.get("compiledSql")
                                    else ""
                                )
                            }',
                            run_id ='{new_model.get("runId", '')}',
                            tags = '{json.dumps(tags, default=str).replace("'", "''")}',
                            run_start_at = {f"'{new_model.get('executeStartedAt')}'" if new_model.get('executeStartedAt') else "NULL"},
                            run_end_at = {f"'{new_model.get('executeCompletedAt')}'" if new_model.get('executeCompletedAt') else "NULL"},
                            duration =  {f"'{new_model.get('executionTime')}'" if new_model.get('executionTime') else "NULL"},
                            properties = '{json.dumps(properties, default=str).replace("'", "''")}',
                            tests = '{json.dumps(new_model.get('tests', []), default=str).replace("'", "''")}',
                            description = '{new_model.get('description', '')}'
                        where asset_id = '{asset_id}' and source_id = '{model.get("source_id")}'
                    """
                    cursor = execute_query(connection, cursor, query_string)

                    # Save Model Columsn
                    __save_columns(config, new_model, model.get("id"))

        if auto_tag_mapping:
            __dbt_tag_mapping(config, mapping_tags, "task", asset_id)
    except Exception as e:
        log_error(f"DBT Connector - Save Models By Job ID Failed ", e)
        raise e


def __save_columns(config: dict, model: dict, pipeline_task_id: str):
    try:
        connection_obj = config.get("connection", {})
        connection_id = connection_obj.get('id')
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        pipeline_id = __get_pipeline_id(config)

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
                        column.get('comment'),
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


def __save_tests(config: dict, tests: list, auto_tag_mapping: bool):
    try:
        connection_obj = config.get("connection", {})
        connection_id = connection_obj.get('id')

        asset = config.get("asset", {})
        asset_id = asset.get("id")

        pipeline_id = __get_pipeline_id(config)

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            insert_objects = []
            mapping_tags = {}
            for test in tests:
                test_id = test.get('uniqueId')
                tags = test.get("tags") if test.get("tags") else []
                test_uuid = uuid4()

                # Validating Existing Tests
                query_string = f"""
                    select id from core.pipeline_tests
                    where pipeline_tests.asset_id = '{asset_id}' and source_id ='{test_id}'
                """
                cursor = execute_query(connection, cursor, query_string)
                existing_test = fetchone(cursor)
                existing_test_id = existing_test.get(
                    'id') if existing_test else None
                if tags:
                    mapping_tags.update(
                        {str(existing_test_id if existing_test_id else test_id): tags})

                if existing_test_id:
                    query_string = f"""
                        update core.pipeline_tests set 
                            status = '{get_pipeline_status(test.get('status'))}', 
                            error = '{
                                (
                                    test.get("error", "").replace(
                                        "'", "''")
                                    if test.get("error")
                                    else ""
                                )
                            }',
                            description = '{test.get('description', '')}',
                            column_name = '{test.get('columnName')}',
                            tags = '{json.dumps(tags, default=str).replace("'", "''")}',
                            run_id = '{test.get("runId", '')}',
                            run_start_at = {f"'{test.get('executeStartedAt')}'" if test.get('executeStartedAt') else 'NULL'},
                            run_end_at = {f"'{test.get('executeCompletedAt')}'" if test.get('executeCompletedAt') else 'NULL'},
                            duration = '{test.get('executionTime', '')}',
                            depends_on = '{json.dumps(test.get('dependsOn', []), default=str).replace("'", "''")}'
                        where id = '{existing_test_id}'
                    """
                    cursor = execute_query(connection, cursor, query_string)

                else:
                    query_input = (
                        test_uuid,
                        test.get('uniqueId'),
                        test.get('name'),
                        test.get('description', ''),
                        test.get('columnName'),
                        get_pipeline_status(test.get('status')),
                        test.get('error', ''),
                        (
                            test.get("rawSql", "").replace("'", "''")
                            if test.get("rawSql")
                            else ""
                        ),
                        (
                            test.get("compiledSql", "").replace("'", "''")
                            if test.get("compiledSql")
                            else ""
                        ),
                        json.dumps(tags,
                                   default=str).replace("'", "''"),
                        json.dumps(test.get('dependsOn', []),
                                   default=str).replace("'", "''"),
                        test.get('executeStartedAt') if test.get(
                            'executeStartedAt') else None,
                        test.get('executeCompletedAt') if test.get(
                            'executeCompletedAt') else None,
                        test.get('executionTime') if test.get(
                            'executionTime') else None,
                        test.get("runId", ''),
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

            insert_objects = split_queries(insert_objects)
            for input_values in insert_objects:
                try:
                    query_input = ",".join(input_values)
                    query_string = f"""
                        insert into core.pipeline_tests(
                            id, source_id, name, description, column_name, status, error, source_code, compiled_code, tags, depends_on,
                            run_start_at, run_end_at, duration, run_id, is_active, is_delete, pipeline_id, asset_id, connection_id
                        ) values {query_input} 
                    """
                    cursor = execute_query(
                        connection, cursor, query_string)
                except Exception as e:
                    log_error('DBT Jobs Tests Insert Failed  ', e)
            if auto_tag_mapping:
                __dbt_tag_mapping(config, mapping_tags, "test", asset_id)
    except Exception as e:
        log_error(f"DBT Connector - Save Tests By Job ID Failed ", e)
        raise e


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

            # Update Propagations Alerts and Issues Creation and Notifications
            handle_alerts_issues_propagation(config, run_id)

    except Exception as e:
        log_error(f"DBT Connector - Update Run Stats to Job Failed ", e)
        raise e


def __prepare_lineage(config: dict, credentials: dict, job_id: str, models: list, catalog: dict, adapter_type: str, connection_type: str):
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        lineage_config = {"tables": [], "relations": []}
        catalog_nodes = catalog.get("nodes", {}) if catalog else {}
        catalog_sources = catalog.get("sources") if  catalog else {}
        lineage_entity_config = []
        for model in models:
            extra_params = {"credentials": credentials, "job_id": job_id}
            
            model_unique_id = model.get("uniqueId", "")
            
            lineage_config = __get_current_node_lineage(model, lineage_config)
            lineage_config = __get_depends_on_node_lineage(
                model, lineage_config, extra_params
            )
            lineage_config = __get_childrens_node_lineage(
                model, lineage_config, extra_params
            )
            depends_on = model.get("dependsOn", [])
            
            if not depends_on:
                compiled_table_info = __compile_model_sql_and_extract_tables(config, model, adapter_type)
                if compiled_table_info:
                    all_tables = compiled_table_info.get("all_tables", [])
                        
                    for table_info in all_tables:
                        # Check if table exists in database
                        existing_asset = __check_table_exists_in_db(config, table_info)
                        
                        table_entity = {
                            "database": table_info.get("database", ""),
                            "schema": table_info.get("schema", ""),
                            "name": table_info.get("name", ""),
                            "entity_name": table_info.get("name", ""),
                            "entity": table_info.get("name", ""),
                            "fields": [],
                            "columns": json.dumps([]),
                            "connection_type": adapter_type,
                            "group": "pipeline"
                        }
                        if existing_asset:
                          table_entity["fields"] = existing_asset.get("attributes")
                        lineage_entity_config.append(table_entity)
                        lineage_config["tables"].append(table_entity)
                        
                        # If table exists in DB, create associated asset entry
                        if existing_asset:
                            __create_associated_asset_entry(config, asset_id, existing_asset["id"], table_entity)
                        
                        model_table_relation = __create_model_to_table_relation(
                            model, table_entity, lineage_config, is_source=True
                        )
                        if model_table_relation:
                            lineage_config["relations"].extend(model_table_relation)
            
            for depends in depends_on:
                if depends.startswith("source."):
                    # Call GraphQL to get source information
                    source_response = get_source_by_job_id(config, job_id, depends)
                    
                    # Use GraphQL response directly instead of catalog_sources
                    if source_response and source_response.get("job", {}).get("source"):
                        source_info = __prepare_source_from_graphql_response(
                            model, source_response, adapter_type
                        )
                        existing_asset = __check_table_exists_in_db(config, source_info)
                        if existing_asset:
                            __create_associated_asset_entry(config, asset_id, existing_asset["id"], source_info)
                        lineage_entity_config.append(source_info)
                        lineage_config["tables"].append(source_info)
                        model_source_relation = __create_model_to_table_relation(
                            model, source_info, lineage_config, is_source=True
                        )
                        if model_source_relation:
                            lineage_config["relations"].extend(model_source_relation)
            
                
            if model_unique_id in catalog_nodes:
                 catalog_table_info = __prepare_catalog_table_for_model(
                     model, catalog_nodes[model_unique_id], adapter_type
                 )
                 lineage_entity_config.append(catalog_table_info)
                 lineage_config["tables"].append(catalog_table_info)
                
                 # Create relation from model to catalog table
                 model_table_relation = __create_model_to_table_relation(
                     model, catalog_table_info, lineage_config
                 )
                 if model_table_relation:
                     lineage_config["relations"].extend(model_table_relation)
            else:
                model_table_info = __prepare_model_table_without_catalog(model, adapter_type)
                lineage_entity_config.append(model_table_info)
                lineage_config["tables"].append(model_table_info)
                
                 # Create relation from model to model table
                model_table_relation = __create_model_to_table_relation(
                     model, model_table_info, lineage_config
                 )
                if model_table_relation:
                     lineage_config["relations"].extend(model_table_relation)
            lineage_config = __prepare_lineage_relations(lineage_config)
        
        # Step 2: Insert all matched catalog tables in bulk to lineage_entity table
        map_asset_with_lineage(config, lineage_config, "pipeline")
        
        # Get associated asset mappings after mapping
        associated_assets = __get_associated_assets_by_source_asset_id(config, asset_id)
        
        # Process lineage config with associated assets
        lineage_config, lineage_entity_config = __process_lineage_with_associated_assets(
            lineage_config, lineage_entity_config, associated_assets
        )
        
        # Process exposures and add to lineage_entity_config
        __process_exposures_for_lineage(config, credentials, asset_id, lineage_entity_config, lineage_config)
        save_lineage_entity(config, lineage_entity_config, asset_id)
        save_lineage(config, "pipeline", lineage_config, asset_id)

        # Generate Auto Mapping

    except Exception as e:
        log_error(f"DBT Connector - Prepare Lineage Failed", e)
        raise e


def __process_exposures_for_lineage(config: dict, credentials: dict, asset_id: str, lineage_entity_config: list, lineage_config: dict):
    """
    Process exposures and add them to lineage_entity_config
    This method handles all exposure processing logic directly
    """
    try:
        # Get last run ID
        last_run_id = __get_last_run_id(config)
        asset = config.get("asset", {})
        asset_name = asset.get("name", "")
        connection_id = config.get("connection", {}).get("id", "") if config.get("connection", {}) else ""
        connection_name = config.get("connection", {}).get("name", "") if config.get("connection", {}) else ""
        auto_tag_mapping = credentials.get("auto_mapping_tags", False)
        if not last_run_id:
            return
        
        # Get exposures from manifest
        account_id = credentials.get("account_id", "")
        request_url = f"accounts/{account_id}/runs/{last_run_id}/artifacts/manifest.json"
        response = __get_response(request_url, is_artifact=True)
        exposures = response.get("exposures", {}) if response else {}
        
        if not exposures:
            return
        
        # Get existing models from database
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select id, source_id, name
                from core.pipeline_tasks 
                where asset_id = '{asset_id}' and run_id = '{last_run_id}'
                """
            cursor = execute_query(connection, cursor, query_string)
            existing_models = fetchall(cursor)
            
            # Create a mapping of source_id to model info for quick lookup
            model_source_mapping = {}
            for model in existing_models:
                source_id = model.get('source_id')
                if source_id:
                    model_source_mapping[source_id] = {
                        'id': model.get('id'),
                        'name': model.get('name')
                    }
            
            # Process each exposure
            for exposure_key, exposure_data in exposures.items():
                exposure_name = exposure_data.get("name", "")
                exposure_description = exposure_data.get("description", "")
                exposure_url = exposure_data.get("url", "")
                depends_on = exposure_data.get("depends_on", {})
                nodes = depends_on.get("nodes", [])
                
                # Check if any of the dependency nodes match our models
                matching_models = []
                for node in nodes:
                    if node in model_source_mapping:
                        model_info = model_source_mapping[node]
                        matching_models.append({
                            'source_id': node,
                            'model_id': model_info['id'],
                            'model_name': model_info['name']
                        })
                
                # If we found matching models, print the combined data and create exposure entity
                if matching_models:
                    for match in matching_models:
                        print(f"model_id: {match['model_id']},model_source_id: {match['source_id']}, matched_model: {match['model_name']}, exposure_name: {exposure_name}, exposure_description: {exposure_description}")
                    # Prepare comprehensive exposure properties
                    exposure_properties = {
                        "resource_type": exposure_data.get("resource_type", ""),
                        "package_name": exposure_data.get("package_name", ""),
                        "path": exposure_data.get("path", ""),
                        "original_file_path": exposure_data.get("original_file_path", ""),
                        "unique_id": exposure_data.get("unique_id", ""),
                        "fqn": exposure_data.get("fqn", []),
                        "type": exposure_data.get("type", ""),
                        "owner": exposure_data.get("owner", {}),
                        "description": exposure_data.get("description", ""),
                        "label": exposure_data.get("label", ""),
                        "maturity": exposure_data.get("maturity", ""),
                        "meta": exposure_data.get("meta", {}),
                        "tags": exposure_data.get("tags", []),
                        "config": exposure_data.get("config", {}),
                        "unrendered_config": exposure_data.get("unrendered_config", {}),
                        "url": exposure_data.get("url", ""),
                        "depends_on": exposure_data.get("depends_on", {}),
                        "refs": exposure_data.get("refs", []),
                        "sources": exposure_data.get("sources", []),
                        "metrics": exposure_data.get("metrics", []),
                        "created_at": exposure_data.get("created_at", ""),
                        "matching_models": matching_models,
                        "dbt_tags": None,
                        "user_names": None,
                        "source_asset_id": asset_id,
                        "connection_id": connection_id,
                        "connection_name": connection_name,
                        "asset_name": asset_name,
                        "exposure_entity_id": exposure_data.get("name", "")
                    }
                    if exposure_properties:
                        task_ids = get_task_ids_by_source_ids(config, asset_id, nodes)
                        dbt_exposures_tag_users_mapping(config, credentials, exposure_properties, asset_id, task_ids)
                    
                    # Create exposure entity with all details stored in properties
                    exposure_entity = {
                        "database": "",
                        "schema": "",
                        "name": exposure_name,
                        "entity_name": exposure_name,
                        "entity": exposure_key,  # Use the full exposure key as entity
                        "connection_type": "dbt",
                        "fields": [],
                        "properties": json.dumps(exposure_properties, default=str).replace("'", "''")
                    }
                    
                    lineage_entity_config.append(exposure_entity)
                    
                    # Also add exposure to lineage_config for save_lineage method
                    exposure_lineage_entry = {
                        "id": exposure_key,  # Use full exposure key as ID
                        "name": exposure_name,
                        "database": "",
                        "schema": "",
                        "entity_name": exposure_name,
                        "entity": exposure_key,
                        "connection_type": "dbt",
                        "fields": [],
                        "level": 1,
                        "type": "exposure",
                        "properties": json.dumps(exposure_properties, default=str).replace("'", "''")
                    }
                    lineage_config["tables"].append(exposure_lineage_entry)
                    linage_config_tables = lineage_config.get("tables", [])
                    
                    # Add relations between exposure and matching models
                    for match in matching_models:
                        associtaed_asset_query = f"""
                            select aa.associated_asset_id, a.connection_id from core.associated_asset aa join core.asset a on a.id = aa.associated_asset_id
                            where aa.source_id = '{match['source_id']}' and aa.source_asset_id = '{asset_id}'
                        """
                        cursor = execute_query(connection, cursor, associtaed_asset_query)
                        associated_asset = fetchone(cursor)
                        if associated_asset:
                            associated_asset_id = associated_asset.get("associated_asset_id")
                            associated_asset_connection_id = associated_asset.get("connection_id")
                        else:
                            associated_asset_id = None

                        if associated_asset_id and associated_asset_connection_id:
                            relation = {
                                "srcTableId": associated_asset_id,
                                "source_asset_id": associated_asset_id,
                                "source_connection_id": associated_asset_connection_id,
                                "tgtTableId": exposure_name,        # Target is the exposure
                                "is_source": False
                            }
                            lineage_config["relations"].append(relation)

                        
                        # Find the source table ID for this specific match
                        if not associated_asset_id:
                            sourc_table_id = None
                            for table in linage_config_tables:
                                if table.get("entity") == match['source_id']:
                                    sourc_table_id = table.get("entity_name")
                                    break
                            
                            # Create relation from model to exposure
                            if sourc_table_id:
                                relation = {
                                    "srcTableId": sourc_table_id,  # Source is the model
                                    "tgtTableId": exposure_name,        # Target is the exposure
                                    "is_source": False
                                }
                                lineage_config["relations"].append(relation)
                    
                    associated_asset_ids = map_exposure_with_lineage(config, exposure_url, asset_id)
                    if associated_asset_ids:
                        # Insert all associated assets
                        for associated_asset_id in associated_asset_ids:
                            query_string = f"""
                                insert into core.associated_asset (id, source_id, source_asset_id, associated_asset_id, associate_id, modified_date,is_auto) 
                                values ('{uuid4()}', '{exposure_name}', '{asset_id}', '{associated_asset_id}', '{associated_asset_id}', NOW(), true) """
                            cursor = execute_query(connection, cursor, query_string)
        
    except Exception as e:
        log_error(f"DBT Connector - Process Exposures For Lineage Failed", e)
        raise e





def __get_current_node_lineage(model: dict, lineage_config: dict) -> dict:
    try:
        tables = lineage_config.get("tables", [])
        current_model = __map_lineage_data_by_type(
            "model", lineage_config, model, {"level": 2}
        )
        lineage_config.update({"tables": tables + [current_model]})
        return lineage_config
    except Exception as e:
        log_error(f"DBT Connector - Get Current Node Lineage ", e)
        raise e


def __get_depending_mobel_by_id(job_id, model_id: str, data) -> dict:
    try:
        meta_query_params = {
            "query": """
                    {
                        job(id: """
            + str(job_id)
            + """) {
                    model(uniqueId: """
            + '"'
            + str(model_id)
            + '"'
            + """) 
                    {
                        uniqueId
                        name
                        database
                        schema
                        alias
                        runGeneratedAt
                        description
                        owner
                        type
                        dependsOn
                        materializedType
                        resourceType
                        packageName
                        childrenL1
                        status
                        columns {
                                name
                                type
                                comment
                                description
                            }
                        }
                    }
                }
            """
        }
        response = __get_response("", "post", True, meta_query_params)
        response = response if response else {}
        response = response.get("data", {}).get(
            "job", {}) if response else None
        response = response.get("model", None) if response else None
        if response:
            data.update(
                {
                    "name": response.get("name", ""),
                    "description": response.get("description", ""),
                    "type": "Pipeline",
                    "id": response.get("uniqueId", ""),
                    "runGeneratedAt": response.get("runGeneratedAt", ""),
                    "database": response.get("database", ""),
                    "schema": response.get("schema", ""),
                    "owner_name": response.get("owner", ""),
                    "model_type": response.get("type", ""),
                    "materializedType": response.get("materializedType", ""),
                    "resourceType": response.get("resourceType", ""),
                    "columns": response.get("columns", []),
                    "dependsOn": response.get("dependsOn", ""),
                    "childrenL1": response.get("childrenL1", ""),
                    "status": response.get("status", "")
                }
            )
        return data
    except Exception as e:
        log_error(f"DBT Connector - Get Depending Mobel By Model ID ", e)
        raise e


def __check_model_exists(lineage_config: dict, model_id: str) -> dict:
    model = None
    try:
        tables = lineage_config.get("tables", [])
        model = next((x for x in tables if x.get("id") == model_id), None)
        return model
    except Exception as e:
        log_error(f"DBT Connector - Check Model Already Exists ", e)
        raise e


def __get_depends_on_node_lineage(lineageSrcData: dict, lineage_config: dict, extra_params: dict) -> dict:
    try:
        dependsOn = lineageSrcData.get("dependsOn", [])
        target_id = lineageSrcData.get("uniqueId", "")
        if len(dependsOn) > 0:
            dependsOnModel = list(
                map(
                    lambda x: __map_lineage_data_by_type(
                        "upstream",
                        lineage_config,
                        x,
                        {"level": 3, "target_id": target_id, **extra_params},
                    ),
                    filter(lambda x: x.startswith("model."), dependsOn)
                )
            )

            dependsOnModel = list(
                filter(lambda x: x.get("id"), dependsOnModel))
            if len(dependsOnModel) > 0:
                tables = lineage_config.get("tables", [])
                lineage_config.update({"tables": tables + dependsOnModel})

                for d_value in dependsOnModel:
                    __get_depends_on_node_lineage(
                        d_value, lineage_config, extra_params)

        return lineage_config
    except Exception as e:
        log_error(f"DBT Connector - Get Depends On Node Lineage ", e)
        raise e


def __get_childrens_node_lineage(lineageSrcData: dict, lineage_config: dict, extra_params: dict) -> dict:
    try:
        childrens = lineageSrcData.get("childrenL1", [])
        src_id = lineageSrcData.get("uniqueId", "")
        if len(childrens) > 0:
            childrenModel = list(
                map(
                    lambda x: __map_lineage_data_by_type(
                        "downstream",
                        lineage_config,
                        x,
                        {"level": 3, "src_id": src_id, **extra_params},
                    ),
                    filter(lambda x: x.startswith("model."), childrens)
                )
            )

            childrenModel = list(filter(lambda x: x.get("id"), childrenModel))
            if len(childrenModel) > 0:
                tables = lineage_config.get("tables", [])
                lineage_config.update({"tables": tables + childrenModel})
        return lineage_config
    except Exception as e:
        log_error(f"DBT Connector - Get Childresn Node Lineage ", e)
        raise e


def __map_lineage_data_by_type(type: str, lineage_config: dict, data: dict, params: dict = None) -> dict:
    try:
        level = params.get("level", 0)
        src_id = params.get("src_id", "")
        target_id = params.get("target_id", "")

        if type == "upstream" or type == "downstream":
            exist_model = __check_model_exists(lineage_config, data)

            if not exist_model:
                job_id = params.get("job_id", "")
                data = __get_depending_mobel_by_id(job_id, data, {})
                fields = data.get("columns", [])
                fields = __prepare_columns_list_data(fields)

                obj = {
                    "id": data.get("id", ""),
                    "name": data.get("name", ""),
                    "database": data.get("database", ""),
                    "schema": data.get("schema", ""),
                    "src_id": src_id,
                    "target_id": target_id,
                    "level": level,
                    "runGeneratedAt": data.get("runGeneratedAt", ""),
                    "type": data.get("type", ""),
                    "node_type": type,
                    "owner": data.get("owner", ""),
                    "dependsOn": data.get("dependsOn", ""),
                    "childrenL1": data.get("childrenL1", ""),
                    "fields": fields if fields else [],
                    "status": data.get("status", ""),
                    "overall_status": data.get("overall_status", ""),
                }
                return obj
            else:
                exist_model = {**exist_model}
                exist_model.update(
                    {"src_id": src_id, "target_id": target_id, "duplicate": True}
                )
                return exist_model
        elif type == "model":
            id = data.get("id", None)
            if not id:
                id = data.get("uniqueId", "")
            if not id:
                id = data.get("name", "")

            fields = data.get("columns", [])
            fields = __prepare_columns_list_data(fields)

            obj = {
                "id": id,
                "name": data.get("name", ""),
                "src_id": src_id,
                "target_id": target_id,
                "database": data.get("database", ""),
                "schema": data.get("schema", ""),
                "runGeneratedAt": data.get("runGeneratedAt", ""),
                "type": data.get("type", ""),
                "owner": data.get("owner", ""),
                "level": level,
                "fields": fields if fields else [],
                "status": data.get("status", ""),
                "overall_status": data.get("overall_status", ""),
            }
            return obj
    except Exception as e:
        log_error("Dbt Map Lineage By Type Failed ", e)
        raise e


def __prepare_columns_list_data(columns: list = []) -> list:
    m_columns = []
    try:
        for column in columns:
            obj = {
                "name": column.get("name", ""),
                "datatype": column.get("type", ""),
                "description": column.get("description", ""),
                "comment": column.get("comment", ""),
                "type": "column",
            }
            m_columns.append(obj)
        return m_columns
    except Exception as e:
        log_error(f"DBT Connector - Prepare Columns List ", e)
        raise e


def __prepare_lineage_relations(lineage_config: dict) -> dict:
    try:
        tables = lineage_config.get("tables", [])
        src_relations = lineage_config.get("relations", [])
        relations = []

        source_based_tables = list(
            filter(lambda table: table.get("src_id"), tables))
        # source_based_tables.sort(key=lambda x: x.get('src_id'))
        for table in source_based_tables:
            obj = {"srcTableId": table.get(
                "src_id"), "tgtTableId": table.get("id")}
            field_relations = __prepare_field_lineage_relations_models(
                lineage_config, obj)
            relations = relations + field_relations

        targe_based_tables = list(
            filter(lambda table: table.get("target_id"), tables))
        # targe_based_tables.sort(key=lambda x: x.get('target_id'))
        for table in targe_based_tables:
            obj = {"srcTableId": table.get(
                "id"), "tgtTableId": table.get("target_id")}
            field_relations = __prepare_field_lineage_relations_models(
                lineage_config, obj)
            relations = relations + field_relations

        lineage_config.update(
            {
                "relations": src_relations + relations,
                "tables": list(
                    filter(lambda table: table.get(
                        "duplicate", False) != True, tables)
                ),
            }
        )
        return lineage_config
    except Exception as e:
        log_error(f"DBT Connector - Prepare Lineage Relations ", e)
        raise e

def __prepare_field_lineage_relations(lineage_config: dict, relation_config: dict) -> dict:
    relations = []
    try:
        tables = lineage_config.get("tables", [])
        src_id = relation_config.get("srcTableId", "")
        target_id = relation_config.get("tgtTableId", "")

        if src_id and target_id:
            source_based_table = list(
                filter(lambda table: table.get("id") == src_id, tables)
            )
            target_based_table = list(
                filter(lambda table: table.get("entity_name") == target_id, tables)
            )
            if len(source_based_table) > 0 and len(target_based_table) > 0:
                source_based_table = source_based_table[0]
                target_based_table = target_based_table[0]
                source_based_table_fields = source_based_table.get(
                    "fields", [])
                target_based_table_fields = target_based_table.get(
                    "fields", [])
                for field in source_based_table_fields:
                    target_field = list(
                        filter(
                            lambda table: table.get(
                                "name").lower() == field.get("name").lower(),
                            target_based_table_fields,
                        )
                    )
                    if target_field and len(target_field) > 0:
                        obj = {
                            "srcTableId": src_id,
                            "tgtTableId": target_based_table.get("entity_name"),
                            "srcTableColName": field.get("name", ""),
                            "tgtTableColName": target_field[0].get("name", ""),
                            "is_source": False
                        }
                        relations.append(obj)
                if len(relations) == 0:
                    obj = {"srcTableId": src_id, "tgtTableId": target_based_table.get("entity_name"), "is_source": False}
                    relations.append(obj)
        return relations
    except Exception as e:
        log_error(f"DBT Connector - Prepare Field Relations ", e)
        raise e


def __prepare_field_lineage_relations_sources(lineage_config: dict, relation_config: dict) -> dict:
    relations = []
    try:
        tables = lineage_config.get("tables", [])
        src_id = relation_config.get("srcTableId", "")
        target_id = relation_config.get("tgtTableId", "")

        if src_id and target_id:
            source_based_table = list(
                filter(lambda table: table.get("entity") == src_id, tables)
            )
            target_based_table = list(
                filter(lambda table: table.get("id") == target_id, tables)
            )
            if len(source_based_table) > 0 and len(target_based_table) > 0:
                source_based_table = source_based_table[0]
                target_based_table = target_based_table[0]
                source_based_table_fields = source_based_table.get(
                    "fields", [])
                target_based_table_fields = target_based_table.get(
                    "fields", [])
                for field in source_based_table_fields:
                    target_field = list(
                        filter(
                            lambda table: table.get(
                                "name") == field.get("name"),
                            target_based_table_fields,
                        )
                    )
                    if target_field and len(target_field) > 0:
                        obj = {
                            "srcTableId": source_based_table.get("entity_name"),
                            "tgtTableId": target_id,
                            "srcTableColName": field.get("name", ""),
                            "tgtTableColName": target_field[0].get("name", ""),
                            "is_source": True
                        }
                        relations.append(obj)
                if len(relations) == 0:
                    obj = {"srcTableId": source_based_table.get("entity_name"), "tgtTableId": target_id,  "is_source": True}
                    relations.append(obj)
        return relations
    except Exception as e:
        log_error(f"DBT Connector - Prepare Field Relations ", e)
        raise e
    
def __prepare_field_lineage_relations_models(lineage_config: dict, relation_config: dict) -> dict:
    relations = []
    try:
        tables = lineage_config.get("tables", [])
        src_id = relation_config.get("srcTableId", "")
        target_id = relation_config.get("tgtTableId", "")

        if src_id and target_id:
            source_based_table = list(
                filter(lambda table: table.get("entity") == src_id, tables)
            )
            target_based_table = list(
                filter(lambda table: table.get("id") == target_id, tables)
            )
            if len(source_based_table) > 0 and len(target_based_table) > 0:
                source_based_table = source_based_table[0]
                target_based_table = target_based_table[0]
                source_based_table_fields = source_based_table.get(
                    "fields", [])
                target_based_table_fields = target_based_table.get(
                    "fields", [])
                for field in source_based_table_fields:
                    target_field = list(
                        filter(
                            lambda table: table.get(
                                "name").lower() == field.get("name").lower(),
                            target_based_table_fields,
                        )
                    )
                    if target_field and len(target_field) > 0:
                        obj = {
                            "srcTableId": source_based_table.get("entity_name"),
                            "tgtTableId": target_id,
                            "srcTableColName": field.get("name", ""),
                            "tgtTableColName": target_field[0].get("name", ""),
                            "is_source": True,
                            "dependOn": source_based_table.get("entity")
                        }
                        relations.append(obj)
                if len(relations) == 0:
                    obj = {"srcTableId": source_based_table.get("entity_name"), "tgtTableId": target_id,  "is_source": True}
                    relations.append(obj)
        return relations
    except Exception as e:
        log_error(f"DBT Connector - Prepare Field Relations ", e)
        raise e

def __dbt_tag_mapping(config: dict, tags: dict, level: str, asset_id: str):
    for info in dict.keys(tags):
        tags_info = tags[info]
        params = {
            "asset_id": asset_id,
            "level": level,
            "id": info,
            "source": "dbt"
        }
        pipeline_auto_tag_mapping(config, tags_info, params)

def extract_pipeline_measure(config: dict, job_id: str, credentials: dict):
    # Fetch pipeline_name (job_name) from pipeline table
    asset = config.get("asset", {})
    asset_id = asset.get("id") if asset else config.get("asset_id")
    connection = get_postgres_connection(config)
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
    
    metadata_config = credentials.get("metadata", {})
    tasks_pull = metadata_config.get("tasks", False)

    run_history, task_runs_history = __get_model_runs_measure(job_id, credentials, tasks_pull)
    if not run_history:
        return
    run_history.sort(key=lambda x: x.get("started_at"))
    latest_run = run_history[-1]
    previous_run = run_history[0] if len(run_history) > 1 else {}
    job_run_detail = {
        "duration": latest_run.get("duration"),
        "last_run_date": latest_run.get("started_at"),
        "previous_run_date": previous_run.get("started_at") if previous_run.get("started_at") else None,
    }
    # Pass pipeline_name (job_name) for asset level measures
    execute_pipeline_measure(config, "asset", job_run_detail, job_name=pipeline_name)
    if not tasks_pull:
        return
    tasks = get_pipeline_tasks(config)
    if not tasks:
        return
    row_metrics = __get_run_artifact_result(credentials, latest_run.get("id"))
    for task in tasks:
        source_id = task.get("source_id")
        task_name = task.get("name")
        task_runs = [r for r in task_runs_history if r.get("uniqueId") == source_id and r.get("executeStartedAt")]
        if not task_runs:
            continue
        task_runs.sort(key=lambda x: x.get("executeStartedAt"))
        task_latest_run = task_runs[-1]
        task_previous_run = task_runs[0] if len(task_runs) > 1 else {}

        task_run_detail = {
            "duration": task_latest_run.get("executionTime"),
            "last_run_date": task_latest_run.get("executeStartedAt"),
            "previous_run_date": task_previous_run.get("executeStartedAt") if task_previous_run else None,
        }
        task_metric = row_metrics.get(source_id, {})
        task_run_detail["is_row_written"] = task_metric.get("is_row_written")
        task_run_detail["rows_affected"] = task_metric.get("row_count")

        # Pass pipeline_name (job_name) and task_name for task level measures
        execute_pipeline_measure(config, "task", task_run_detail, task_info=task, job_name=pipeline_name, task_name=task_name)

def __get_model_runs_measure(job_id: str, credentials: dict, tasks_pull: bool):
    model_runs = []
    account_id = credentials.get("account_id", "")
    no_of_runs = credentials.get("no_of_runs", "30")
    status_filter = credentials.get("status", "all")
    request_url = f"accounts/{account_id}/runs/?job_definition_id={job_id}"
    if status_filter != "all":
        status_filter = 10 if status_filter == "success" else 20
        request_url = f"{request_url}&status={status_filter}"
    no_of_runs = int(no_of_runs) if type(
        no_of_runs) == str else no_of_runs
    end = datetime.datetime.today()
    start = end - datetime.timedelta(days=no_of_runs)
    dates = [
        start.strftime("%Y-%m-%d %H:%M:%S"),
        end.strftime("%Y-%m-%d %H:%M:%S"),
    ]
    request_url = f"{request_url}&created_at__range={dates}&limit=2&order_by=-id"
    response = __get_response(request_url)
    response = response if response else {}
    response = response.get("data", None)
    if response and tasks_pull:
        response = response
        for run in response:
            run_id = run.get('id')
            datm = datetime.datetime.strptime(run.get('duration'), "%H:%M:%S")
            a_timedelta = datm - datetime.datetime(1900, 1, 1)
            duration = a_timedelta.total_seconds()
            run['duration'] = duration
            meta_query_params = {
                    "query": """
                        {
                            job(id: """
                    + str(job_id)
                    + """, runId: """
                    + str(run_id)
                    + """) {
                                models {
                                    uniqueId
                                    name
                                    status
                                    executeStartedAt
                                    executeCompletedAt
                                    rawSql
                                    compiledSql
                                    executionTime
                                    runId,
                                    error
                                }
                        }
                    }"""
                }
            models = __get_response("", "post", True, meta_query_params)
            models = models if models else {}
            models = models.get("data", {}).get(
                "job", {}) if models else None
            model_runs.extend(models.get("models", []))
    return response, model_runs

def __get_run_artifact_result(credentials: dict, run_id: str):
    global ARTIFACTS
    row_metric = {}
    try:
        if not ARTIFACTS:
            account_id = credentials.get("account_id", "")
            request_url = f"accounts/{account_id}/runs/{run_id}/artifacts/run_results.json"
            response = __get_response(request_url, is_artifact=True)
            results = response.get("results", []) if response else []
            ARTIFACTS = results

        ARTIFACTS = ARTIFACTS if ARTIFACTS else []
        for result in ARTIFACTS:
            row_metric[result.get("unique_id")] = {
                "row_count": result.get("adapter_response").get("rows_affected") if result.get("adapter_response") else None,
                "is_row_written": result.get("adapter_response").get("code") == "SUCCESS" if result.get("adapter_response") else False
            }
    except Exception as e:
        log_error(f"DBT Connector - Get Run Artifact Result Failed", e)
    finally:
        return row_metric



def __get_adapter_type_from_environment(config: dict, credentials: dict, job_id: str) -> str:
    """Get the adapter type from the environment credentials using REST API"""
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        account_id = credentials.get("account_id", "")
        request_url = f"accounts/{account_id}/jobs/{job_id}"
        response = __get_response(request_url)
        response = response if response else {}
        response = response.get("data", None)
        environment_id = response.get("environment_id")
        env_request_url = f"accounts/{account_id}/environments/{environment_id}"
        env_response = __get_response(env_request_url)
        data = env_response.get("data", {})
        connection_data = data.get("connection", {})
        
        if connection_data:
            adapter_type = connection_data.get("type", "unknown")
            return adapter_type
            
    except Exception as e:
        log_error(f"DBT Connector - Get Job Information By Job ID Failed ", e)
        raise e

def __compile_model_sql_and_extract_tables(config: dict, model: dict, adapter_type: str, compiled_sql: str = None) -> dict:
    """
    Extract table information from compiled SQL for models with empty dependsOn
    """
    try:
        if compiled_sql:
            sql_to_parse = compiled_sql
        else:
            sql_to_parse = model.get("compiledSql", "")
            
        if not sql_to_parse:
            return None
            
        all_tables = __extract_all_tables_from_sql_simple(sql_to_parse)
        if not all_tables:
            return None
        first_table = all_tables[0]
        entity = {
            "database": first_table.get("database", ""),
            "schema": first_table.get("schema", ""),
            "name": first_table.get("name", ""),
            "entity_name": first_table.get("name", ""),
            "entity": first_table.get("name", ""),
            "fields": [],
            "columns": json.dumps([]),
            "level": 2,
            "connection_type":  adapter_type,
            "group": "pipeline",
            "all_tables": all_tables
        }
        return entity
        
    except Exception as e:
        log_error(f"DBT Connector - Compile Model SQL and Extract Tables Failed", e)
        return None

def __extract_all_tables_from_sql_simple(sql_query: str) -> list:
    """
    Extract ALL tables from SQL query - returns list of all tables found
    Handles CTEs, JOINs, aliases - returns all real tables
    """
    try:
        cleaned_sql = __clean_sql_text(sql_query)
        
        try:
            parsed = parse_one(cleaned_sql)
            
            tables = []
            for node in parsed.walk():
                if isinstance(node, exp.Table):
                    table_name = node.sql().strip().strip('"').strip("'").strip('`')
                    
                    if ' AS ' in table_name.upper():
                        table_name = table_name.split(' AS ')[0].strip()
                    elif ' ' in table_name:
                        parts = table_name.split()
                        if len(parts) > 1 and '.' in parts[0] and parts[0].count('.') >= 2:
                            table_name = parts[0]
                    
                    if '.' in table_name:
                        table_info = __parse_table_reference(table_name)
                        if table_info:
                            tables.append(table_info)
            
            return tables
            
        except Exception as sqlglot_error:
            return __extract_all_tables_with_string_parsing(cleaned_sql)
        
    except Exception as e:
        log_error(f"DBT Connector - Extract All Tables From SQL Simple Failed", e)
        return []

def __extract_all_tables_with_string_parsing(sql_text: str) -> list:
    """
    Fallback method to extract ALL tables using string parsing when sqlglot fails
    """
    try:
        tables = []
        from_pattern = r'FROM\s+([^\s,;]+)'
        join_pattern = r'JOIN\s+([^\s,;]+)'
        
        from_match = re.search(from_pattern, sql_text, re.IGNORECASE)
        if from_match:
            table_name = from_match.group(1).strip().strip('"').strip("'").strip('`').rstrip(';')
            table_info = __process_table_name(table_name)
            if table_info:
                tables.append(table_info)
        
        join_matches = re.findall(join_pattern, sql_text, re.IGNORECASE)
        for table_name in join_matches:
            table_name = table_name.strip().strip('"').strip("'").strip('`').rstrip(';')
            table_info = __process_table_name(table_name)
            if table_info:
                tables.append(table_info)
        
        return tables
        
    except Exception as e:
        log_error(f"DBT Connector - Extract All Tables With String Parsing Failed", e)
        return []

def __process_table_name(table_name: str) -> dict:
    """
    Process table name to remove aliases and check format
    """
    try:
        if ' AS ' in table_name.upper():
            table_name = table_name.split(' AS ')[0].strip()
        elif ' ' in table_name:
            parts = table_name.split()
            if len(parts) > 1 and '.' in parts[0]:
                table_name = parts[0]
        
        if '.' in table_name:
            return __parse_table_reference(table_name)
        
        return None
        
    except Exception as e:
        log_error(f"DBT Connector - Process Table Name Failed", e)
        return None


def __clean_sql_text(sql_text: str) -> str:
    """
    Clean SQL text by removing Git merge conflict markers, comments, and other non-SQL content
    """
    try:
        sql_text = re.sub(r'<<<<<<<.*?=======', '', sql_text, flags=re.DOTALL)
        sql_text = re.sub(r'=======.*?>>>>>>>', '', sql_text, flags=re.DOTALL)
        sql_text = re.sub(r'<<<<<<<.*?>>>>>>>', '', sql_text, flags=re.DOTALL)
        
        sql_text = re.sub(r'--.*$', '', sql_text, flags=re.MULTILINE)
        
        sql_text = re.sub(r'/\*.*?\*/', '', sql_text, flags=re.DOTALL)
        
        sql_text = sql_text.replace('`', '"')
        
        sql_text = re.sub(r'\s+', ' ', sql_text)
        sql_text = sql_text.strip()
        
        return sql_text
        
    except Exception as e:
        log_error(f"DBT Connector - Clean SQL Text Failed", e)
        return sql_text


def __parse_table_reference(table_ref: str) -> dict:
    """
    Parse table reference into database, schema, name components
    """
    try:
        table_ref = table_ref.strip().strip('"').strip("'")
        
        parts = table_ref.split('.')
        
        if len(parts) == 3:
            return {
                "database": parts[0],
                "schema": parts[1], 
                "name": parts[2]
            }
        elif len(parts) == 2:
            return {
                "database": "",
                "schema": parts[0],
                "name": parts[1]
            }
        elif len(parts) == 1:
            return {
                "database": "",
                "schema": "",
                "name": parts[0]
            }
        else:
            return None
            
    except Exception as e:
        log_error(f"DBT Connector - Parse Table Reference Failed", e)
        return None

def __prepare_source_from_graphql_response(model: dict, source_response: dict, adapter_type: str) -> dict:
    """
    Prepare source information directly from GraphQL response
    """
    try:
        source_data = source_response.get("job", {}).get("source", {})
        model_unique_id = model.get("uniqueId", "")
        
        entity = {
            "database": source_data.get("database", ""),
            "schema": source_data.get("schema", ""),
            "name": source_data.get("name", ""),
            "entity_name": f"{source_data.get('name', '')}",
            "entity": source_data.get("name", ""),
            "connection_type": adapter_type,
            "table_type": "source",
            "model_name": model.get("name", ""),
            "has_catalog_entry": True,
            "fields": []
        }
        
        # Add column information from GraphQL response
        source_columns = source_data.get("columns", [])
        for col_info in source_columns:
            field_info = {
                "name": col_info.get("name", ""),
                "datatype": col_info.get("type", ""),
                "comment": "",
                "index": 0,
                "type": "column"
            }
            entity["fields"].append(field_info)
        
        # Set columns as JSON string to ensure compatibility
        entity["columns"] = json.dumps(entity["fields"])
        return entity
        
    except Exception as e:
        log_error(f"DBT Connector - Prepare Source From GraphQL Response Failed", e)
        return {}


def __prepare_catalog_table_for_model(model: dict, catalog_model: dict, adapter_type: str, is_source=False, id_dbt_core = False, source_response=None) -> dict:
    """
    Prepare catalog table information for a specific model
    """
    try:
        metadata = catalog_model.get("metadata", {})
        columns = catalog_model.get("columns", {})
        model_unique_id = model.get("uniqueId", "") if not id_dbt_core else model.get("id", "")
        entity = {
            "id": f"{model_unique_id}.{metadata.get('name', '')}",
            "database": metadata.get("database", ""),
            "schema": metadata.get("schema", ""),
            "name": metadata.get("name", ""),
            "entity_name": f"{model_unique_id}.{metadata.get('name', '')}",
            "entity":  metadata.get("name", "") if is_source else model_unique_id,
            "connection_type": adapter_type,
            "table_type": metadata.get("type", ""),
            "level": 2,
            "model_name": model.get("name", ""),
            "has_catalog_entry": True,
            "fields": []
        }
        
        # Add column information
        for col_name, col_info in columns.items():
            field_info = {
                "name": col_info.get("name", col_name),
                "datatype": col_info.get("type", ""),
                "comment": col_info.get("comment", ""),
                "index": col_info.get("index", 0),
                "type": "column"
            }
            entity["fields"].append(field_info)
        
        # Set columns as JSON string to ensure compatibility
        entity["columns"] = json.dumps(entity["fields"])
        return entity
        
    except Exception as e:
        log_error(f"DBT Connector - Prepare Catalog Table For Model Failed", e)
        return {}


def __prepare_model_table_without_catalog(model: dict, adapter_type: str, id_dbt_core = False) -> dict:
    """
    Prepare table information for a model that doesn't have a catalog entry
    """
    try:
        model_unique_id = model.get("uniqueId", "") if not id_dbt_core else model.get("id", "")
        model_name = model.get("name", "")
        database = model.get("database", "")
        schema = model.get("schema", "")
        columns = model.get("columns", [])
        entity = {
            "database": database,
            "schema": schema,
            "name": model_name,
            "entity_name": f"{model_unique_id}.{model_name}",
            "entity": model_unique_id,
            "connection_type": adapter_type,
            "table_type": "model",
            "model_name": model_name,
            "has_catalog_entry": False,
            "fields": []
        }
        
        # Add column information from model data
        for column in columns:
            field_info = {
                "name": column.get("name", ""),
                "datatype": column.get("type", ""),
                "comment": column.get("comment", ""),
                "description": column.get("description", ""),
                "type": "column"
            }
            entity["fields"].append(field_info)
        
        # Set columns as JSON string to ensure compatibility
        entity["columns"] = json.dumps(entity["fields"])
        return entity
            
    except Exception as e:
        log_error(f"DBT Connector - Prepare Model Table Without Catalog Failed", e)
        return {}


def __create_model_to_table_relation(model: dict, table_info: dict, lineage_config: dict, is_source=False, id_dbt_core = False) -> list:
    """
    Create relations between model and the table we're creating (catalog or model table)
    """
    try:
        model_unique_id = model.get("uniqueId", "") if not id_dbt_core else model.get("id", "")
        table_entity = table_info.get("entity", "") if is_source else table_info.get("entity_name", "")
        relation_config = {
            "srcTableId": table_entity if is_source else model_unique_id,
            "tgtTableId": model_unique_id if is_source else table_entity
        }
        if is_source:
            field_relations = __prepare_field_lineage_relations_sources(lineage_config, relation_config)
        else:
            field_relations = __prepare_field_lineage_relations(lineage_config, relation_config)
        return field_relations
        
    except Exception as e:
        log_error(f"DBT Connector - Create Model To Table Relation Failed", e)
        return []

def __get_job_artifact_catalog(credentials: dict, job_id: str) -> dict:
    try:
        global CATALOG_ARTIFACT
        account_id = credentials.get("account_id", "")
        request_url = f"accounts/{account_id}/jobs/{job_id}/artifacts/catalog.json"
        response = __get_response(request_url, is_artifact=True)
        CATALOG_ARTIFACT = response or {}
        return CATALOG_ARTIFACT
    except Exception as e:
        log_error("DBT Connector - Get Job Artifact Catalog Failed", e)
        return {}


def __get_associated_assets_by_source_asset_id(config: dict, source_asset_id: str) -> list:
    """
    Get associated assets by source asset ID from core.associated_asset table
    """
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                SELECT 
                    aa.associated_asset_id, 
                    aa.source_id,
                    a.connection_id
                FROM core.associated_asset aa
                LEFT JOIN core.asset a ON a.id = aa.associated_asset_id 
                    AND a.is_active = true 
                    AND a.is_delete = false
                WHERE aa.source_asset_id = '{source_asset_id}'
            """
            
            cursor = execute_query(connection, cursor, query_string)
            results = fetchall(cursor)
            
            associated_assets = []
            for result in results:
                asset_info = {
                    "associated_asset_id": result.get("associated_asset_id"),
                    "source_id": result.get("source_id"),
                    "connection_id": result.get("connection_id")
                }
                associated_assets.append(asset_info)
            
            return associated_assets
            
    except Exception as e:
        log_error(f"DBT Connector - Get Associated Assets By Source Asset ID Failed", e)
        return []


def __process_lineage_with_associated_assets(lineage_config: dict, lineage_entity_config: list, associated_assets: list) -> tuple:
    try:
        # Create mapping of source_id to associated_asset_id
        source_to_asset_mapping = {}
        for asset in associated_assets:
            source_to_asset_mapping[asset["source_id"]] = asset["associated_asset_id"]
        
        
        # Get relations and find target entities to remove
        relations = lineage_config.get("relations", [])
        target_entities_to_remove = set()
        updated_relations = []
        
        # Step 1: Find relations where source_entity matches associated asset source_id
        for relation in relations:
            source_entity = relation.get("srcTableId", "")
            target_entity = relation.get("tgtTableId", "")
            if source_entity in source_to_asset_mapping and not relation.get("is_source"):
                # Add target_entity to removal list
                target_entities_to_remove.add(target_entity)
                
                # Add source_entity to associated_assets for later comparison
                for asset in associated_assets:
                    is_source = relation.get("is_source")
                    if asset["source_id"] == source_entity and not is_source:
                        asset["source_entity"] = target_entity
                        relation["tgtTableId"] = str(asset["associated_asset_id"])
                        if asset.get("connection_id"):
                            relation["target_asset_id"] = asset["associated_asset_id"]
                            relation["target_connection_id"] = asset.get("connection_id")
                        updated_relations.append(relation)
                        break
            else:
                updated_relations.append(relation)
        
        for relation in updated_relations:
            source_entity = relation.get("srcTableId", "")
            for asset in associated_assets:
                if (asset.get("source_id") == source_entity) and relation.get("is_source"):
                    target_entities_to_remove.add(source_entity)
                    relation["srcTableId"] = str(asset["associated_asset_id"])
                    if asset.get("connection_id"):
                        relation["source_asset_id"] = asset["associated_asset_id"]
                        relation["source_connection_id"] = asset.get("connection_id")
            depend_on = relation.get("dependOn")
            if depend_on:
                for asset in associated_assets:
                    if asset.get("source_id") == depend_on:
                        relation["srcTableId"] = str(asset["associated_asset_id"])
                        relation["source_asset_id"] = asset.get("associated_asset_id")
                        relation["source_connection_id"] = asset.get("connection_id")
                        break
                
                            
        final_relations = []
        for relation in updated_relations:
            target_entity = relation.get("tgtTableId", "")
            if target_entity not in target_entities_to_remove:
                final_relations.append(relation)
        
        tables = lineage_config.get("tables", [])
        filtered_tables = []
        for table in tables:
            entity = table.get("entity", "")
            if entity not in target_entities_to_remove:
                filtered_tables.append(table)
        
        # Step 5: Remove tables from lineage_entity_config
        filtered_entity_config = []
        for entity in lineage_entity_config:
            entity_name = entity.get("entity_name", "")
            if entity_name not in target_entities_to_remove:
                filtered_entity_config.append(entity)
        
        # Update lineage_config
        lineage_config["tables"] = filtered_tables
        lineage_config["relations"] = final_relations
        
        return lineage_config, filtered_entity_config
        
    except Exception as e:
        log_error(f"DBT Connector - Process Lineage With Associated Assets Failed", e)
        return lineage_config, lineage_entity_config


def get_source_by_job_id(config: dict, job_id: str, source_unique_id: str) -> dict:
    """
    Get source information by job ID and source unique ID from GraphQL
    """
    try:
        return __get_source_by_job_id_from_graphql(config, job_id, source_unique_id)
    except Exception as e:
        log_error(f"DBT Connector - Get Source By Job ID Failed", e)
        return {}


def __check_table_exists_in_db(config: dict, table_info: dict) -> dict:
    """
    Check if table exists in database using database, schema, and name
    """
    try:
        database = table_info.get("database", "")
        schema = table_info.get("schema", "")
        name = table_info.get("name", "")
        
        if not database or not schema or not name:
            return None
            
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                SELECT asset.id, asset.name
                FROM core.asset
                JOIN core.data ON data.asset_id = asset.id
                JOIN core.connection ON connection.id = asset.connection_id
                WHERE asset.is_active = true 
                AND asset.is_delete = false
                AND lower(asset.name) = lower('{name}')
                AND lower(asset.properties->>'schema') = lower('{schema}')
                AND lower(asset.properties->>'database') = lower('{database}')
                ORDER BY asset.id ASC
                LIMIT 1
            """
            cursor = execute_query(connection, cursor, query_string)
            result = fetchone(cursor)
            
            if result:
                asset_id = result.get("id")
                attributes_query = f"""
                        SELECT name, technical_name, id as attribute_id
                        FROM core.attribute 
                        WHERE asset_id = '{asset_id}' 
                        AND is_active = true 
                        AND is_delete = false
                        AND parent_attribute_id IS NULL
                    """
                cursor.execute(attributes_query)
                attributes = fetchall(cursor)
                return {
                    "id": result.get("id"),
                    "name": result.get("name"),
                    "database": result.get("database"),
                    "schema": result.get("schema_name"),
                    "attributes": attributes
                }
            return None
            
    except Exception as e:
        log_error(f"DBT Connector - Check Table Exists In DB Failed", e)
        return None


def __create_associated_asset_entry(config: dict, source_asset_id: str, target_asset_id: str, table_info: dict):
    """
    Create entry in associated_asset table
    """
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            # Check if association already exists
            check_query = f"""
                SELECT id FROM core.associated_asset 
                WHERE source_asset_id = '{source_asset_id}' 
                AND associated_asset_id = '{target_asset_id}'
            """
            cursor = execute_query(connection, cursor, check_query)
            existing = fetchone(cursor)
            
            if not existing:
                # Create new association
                insert_query = f"""
                    INSERT INTO core.associated_asset (
                        id, source_asset_id, source_id, associated_asset_id, associate_id, is_auto, modified_date, direction
                    ) VALUES (
                        '{uuid4()}', '{source_asset_id}', '{table_info.get("entity_name", "")}', '{target_asset_id}', 
                        '{target_asset_id}', false, NOW(), 'downstream'
                    )
                    RETURNING id
                """
                cursor = execute_query(connection, cursor, insert_query)
                
    except Exception as e:
        log_error(f"DBT Connector - Create Associated Asset Entry Failed", e)


def __get_source_by_job_id_from_graphql(config: dict, job_id: str, source_unique_id: str) -> dict:
    """
    Execute GraphQL query to get source information by job ID, excluding columns field
    """
    try:
        query = f"""
        {{
            job(id: {job_id}) {{
                source(uniqueId: "{source_unique_id}") {{
                    uniqueId
                    sourceName
                    name
                    state
                    maxLoadedAt
                    database
                    schema
                    columns {{
                        name
                        type
                    }}
                    criteria {{
                        warnAfter {{
                            period
                            count
                        }}
                        errorAfter {{
                            period
                            count
                        }}
                    }}
                    maxLoadedAtTimeAgoInS
                }}
            }}
        }}
        """

        meta_query_params = {
            "query": query
        }

        response = __get_response("", "post", True, meta_query_params)

        if response:
            return response.get("data", {})
        else:
            return {}

    except Exception as e:
        log_error("DBT Connector - GraphQL Source Query Failed", e)
        return {}




