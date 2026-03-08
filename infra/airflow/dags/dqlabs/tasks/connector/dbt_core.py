import json
import tempfile
from uuid import uuid4
from datetime import datetime
import os
import requests
import copy

from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone, split_queries
from dqlabs.app_helper.dq_helper import get_pipeline_status, get_server_endpoint, delete_target_file
from dqlabs.app_helper.connection_helper import decrypt_connection_config
from dqlabs.app_helper.dag_helper import (
    get_postgres_connection,
)
from dqlabs.utils.extract_workflow import update_asset_run_id
from dqlabs.app_helper.pipeline_helper import (
    pipeline_auto_tag_mapping, update_pipeline_last_runs, get_pipeline_job_input,
    get_run_history, get_task_previous_run_history
)
from dqlabs.app_helper.lineage_helper import (
    save_lineage,  update_pipeline_propagations, map_asset_with_lineage, handle_alerts_issues_propagation,
    get_asset_metric_count, update_asset_metric_count,  update_materializes_asset_id, save_lineage_entity,
    get_task_ids_by_source_ids, dbt_exposures_tag_users_mapping, map_exposure_with_lineage
    )
from dqlabs.app_helper.log_helper import log_error
from dqlabs.utils.tasks import get_task_config, check_task_status
from dqlabs.utils.request_queue import (
    update_queue_detail_status,
    update_queue_status,
    create_queue_detail,
)
from dqlabs.utils.pipeline_measure import execute_pipeline_measure, create_pipeline_task_measures, get_pipeline_tasks
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.tasks.connector.dbt import (
    __prepare_catalog_table_for_model,
    __create_model_to_table_relation,
    __prepare_model_table_without_catalog,
    __prepare_field_lineage_relations_sources,
    __get_associated_assets_by_source_asset_id,
    __process_lineage_with_associated_assets,
    __extract_all_tables_from_sql_simple,
    __check_table_exists_in_db,
    __create_associated_asset_entry,
    __prepare_field_lineage_relations_models
)

CATALOG = {}
METADATA = {}
RESULTS = []
RUNS = []
DBT_MODELS = {}
CATALOG_ARTIFACT = None
def extract_dbt_core(config, **kwargs):
    target_path = None
    try:
        is_completed = check_task_status(config, kwargs)
        if is_completed:
            return
        
        global CATALOG
        global METADATA
        global RESULTS
        global RUNS

        task_config = get_task_config(config, kwargs)
        run_id = config.get("run_id")
        update_queue_detail_status(
            config, ScheduleStatus.Running.value, task_config=task_config
        )
        update_queue_status(config, ScheduleStatus.Running.value, True)
        update_asset_run_id(run_id, config)

        job_input = get_pipeline_job_input(config, run_id)
        target_path = job_input.get("target", [])
        asset = config.get("asset")
        asset_name = asset.get("name")
        connection = config.get("connection", {})
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        credentials = connection.get("credentials")
        metadata_config = credentials.get("metadata", {})
        auto_tag_mapping = credentials.get("auto_tag_mapping", False)
        status_filter = credentials.get("status", "all")
        

        metadata = get_core_data(target_path, "manifest.json")
        metadata_nodes = metadata.get("nodes") if metadata else {}
        METADATA = metadata.get("metadata") if metadata else {}
        adapter_type = METADATA.get('adapter_type')

        runs = get_core_data(target_path, "run_results.json")
        RUNS = runs
        RESULTS = runs.get("results") if runs else []

        catalog = get_core_data(target_path, "catalog.json")
        global CATALOG_ARTIFACT
        CATALOG_ARTIFACT = catalog
        CATALOG = catalog.get("nodes", {}) if catalog else {}

        nodes = metadata_nodes
        all_raw_models = []
        raw_tests = []
        global DBT_MODELS

        project_id = metadata.get("project_id", "")
        project_name = metadata.get("project_name") or asset_name or project_id
        for key, value in nodes.items():
            if value.get("resource_type") == "model":
                all_raw_models.append(value)
            elif value.get("resource_type") == "test":
                raw_tests.append(value)
                
        for model in all_raw_models:
            model["childrenL1"] = []
            model["project_name"] = project_name

        # Populate the children list based on depends_on key
        for model in all_raw_models:
            DBT_MODELS.update(
                {model.get("unique_id"): process_model_data(model)}
            )
            for dependency in model.get("depends_on", {}).get("nodes", []):
                for parent_model in all_raw_models:
                    if parent_model["unique_id"] == dependency:
                        parent_model["childrenL1"].append(model["unique_id"])
                        break
        delete_lineage(config, asset_id)
        pipeline_id = __get_pipeline_id(config)
        latest_run = False
        if metadata_config.get("tasks"):
            new_tasks = save_models(config, asset_id,credentials, pipeline_id, all_raw_models, auto_tag_mapping,  adapter_type, metadata)
            # Create Pipeline Task level Measure
            if new_tasks:
                create_pipeline_task_measures(config, new_tasks)

        if metadata_config.get("tests"):
            save_tests(config, asset_id, pipeline_id, raw_tests, auto_tag_mapping)

        if metadata_config.get("runs"):
            latest_run = save_runs(config, asset_id, pipeline_id, nodes, status_filter)
        update_pipeline(config)
        # Update Job Run Stats
        __update_pipeline_stats(config)

        # Save Propagation Values
        update_pipeline_propagations(config, asset)

        # update request queue status
        is_triggered = config.get("is_triggered")
        if not is_triggered:
            create_queue_detail(config)
        update_queue_detail_status(config, ScheduleStatus.Completed.value)
        update_queue_status(config, ScheduleStatus.Completed.value, True)
        update_pipeline_last_runs(config)
        metrics_count = None
        propagate_alerts = credentials.get("propagate_alerts", "table")
        if propagate_alerts == "table":
            metrics_count = get_asset_metric_count(config, asset_id)
        if latest_run:
            extract_pipeline_measure(config, metadata_config.get("tasks"))
        if propagate_alerts == "table" or propagate_alerts == "pipeline":
            update_asset_metric_count(config, asset_id, metrics_count, latest_run, propagate_alerts=propagate_alerts)
    except Exception as e:
        log_error("Dbt Pull / Push Failed", e)
        update_queue_detail_status(config, ScheduleStatus.Failed.value, str(e))
    finally:
        delete_target_file(target_path)


def get_core_data(target_path: list, file_name: str):
    url = next((p for p in target_path if p.endswith("/" + file_name)), None)
    # Determine base directory for temp files
    base_dir = str(os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")
    ))
    tmp_dir = os.path.join(base_dir, "infra/airflow/tests/temp")
    # Create temp directory if it doesn't exist
    os.makedirs(tmp_dir, exist_ok=True)

    # Stream the file in chunks to avoid memory spike
    with requests.get(url, stream=True) as response:
        response.raise_for_status()

        # Write chunks directly to a temporary file
        with tempfile.NamedTemporaryFile(mode="wb+", dir=tmp_dir, delete=True) as tmp:
            for chunk in response.iter_content(chunk_size=1024 * 1024):  # 1 MB chunks
                if chunk:
                    tmp.write(chunk)
            tmp.flush()
            tmp.seek(0)
            data = json.load(tmp)
    return data

def delete_lineage(config: dict, asset_id: str):
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query = f"delete from core.lineage where source_asset_id='{asset_id}' and target_asset_id='{asset_id}' and is_auto=true"
        execute_query(connection, cursor, query)

def get_manifest_description(model, key):
    columns = model.get("columns", {})
    for name, value in columns.items():
        if name == key:
            return value.get("description")   

def process_model_data(model):
    formatted_columns = []
    asset_model = {}
    runs = []
    tests = []
    columns = {}
    model_metadata = {}

    try:
        model_id = model.get("unique_id", "")

        model_catalog = CATALOG.get(model_id, {})
        if model_catalog:
            columns = model_catalog.get("columns")
            model_metadata = model_catalog.get("metadata")
            for key, value in columns.items():
                formatted_columns.append(
                    {
                        "name": value.get("name"),
                        "type": value.get("type"),
                        "description": value.get("comment") if value.get("comment") else get_manifest_description(model, key) ,
                    }
                )
        else:
            columns = model.get("columns", {})
            for key, value in columns.items():
                formatted_columns.append(
                    {
                        "name": value.get("name"),
                        "type": value.get("data_type"),
                        "description": value.get("description"),
                    }
                )
        latest_model_run = None

        run = next(
            (run for run in RESULTS if run.get("unique_id") == model_id), None
        )
        if run:
            timing = run.get("timing", [])
            if timing:
                runs.append(
                    {
                        "name": model.get("name", ""),
                        "uniqueId": model_id,
                        "executionTime": run.get("execution_time", 0),
                        "threadId": run.get("thread_id", ""),
                        "runGeneratedAt": timing[0].get("started_at"),
                        "runElapsedTime": run.get("execution_time"),
                        "error": run.get("message", ""),
                        "message": run.get("message", ""),
                        "status": run.get("status", ""),
                        "compileStartedAt": timing[0].get("started_at"),
                        "compileCompletedAt": timing[0].get("completed_at"),
                        "executeStartedAt": timing[1].get("started_at"),
                        "executeCompletedAt": timing[1].get("completed_at"),
                    }
                )
                latest_model_run = run

        run_failed = list(
            filter(lambda x: x.get("status", "success") != "success", runs)
        )
        asset_model.update(
            {
                "id": model.get("unique_id"),
                "name": model.get("name"),
                "alias": model.get("alias"),
                "description": model.get("description"),
                "type": "Pipeline",
                "table_type": model_metadata.get("type", ""),
                "job_id": model.get("project_name", ""),
                "job_name": model.get("project_name", ""),
                "runGeneratedAt": (
                    latest_model_run.get("timing", [])[0].get("started_at")
                    if latest_model_run
                    else ""
                ),
                "executeStartedAt": (
                    latest_model_run.get("timing", [])[1].get("started_at")
                    if latest_model_run
                    else ""
                ),
                "executeCompletedAt": (
                    latest_model_run.get("timing", [])[1].get("completed_at")
                    if latest_model_run
                    else ""
                ),
                "executionTime": (
                    latest_model_run.get("execution_time", 0)
                    if latest_model_run
                    else ""
                ),
                "runElapsedTime": (
                    latest_model_run.get("execution_time", 0)
                    if latest_model_run
                    else ""
                ),  # dbt-core doesn't provide elapsed time, hence tagging execution time
                "database": model.get("database"),
                "schema": model.get("schema"),
                "owner_name": model_metadata.get("owner", ""),
                "model_type": model.get("config").get("materialized"),
                "materializedType": model.get("config").get("materialized"),
                "resourceType": model.get("resource_type", ""),
                "rawSql": (
                    model.get("raw_code", "").replace("'", "''")
                    if model.get("raw_code")
                    else ""
                ),
                "columns": formatted_columns,
                "tags": model.get("tags", []),
                "runResults": runs,
                "upstream": model.get("depends_on", {}).get("nodes", []),
                "downstream": model.get("childrenL1", []),
                "project_name": model.get("project_name", ""),
                "compiledSql": (
                    latest_model_run.get("compiled_code")
                    if latest_model_run
                    else ""
                ),
                "status": (
                    latest_model_run.get("status", "") if latest_model_run else ""
                ),
                "overall_status": ("failed" if len(run_failed) else "success"),
                "tests": tests,
            }
        )

    except Exception as e:
        log_error(
            f"DBT core Connector - model processing failed for {model_id}", e
        )
    finally:
        return asset_model

def update_pipeline(config: dict) -> str:
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                update core.pipeline set 
                created_at='{METADATA.get("generated_at", "")}',
                updated_at='{METADATA.get("generated_at", "")}',
                project='{METADATA.get("project_name")}'
                where asset_id = '{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)

    except Exception as e:
        log_error(f"DBT Connector - Update Run Stats to Job Failed ", e)
        raise e
    
def save_models(
        config, asset_id: str,credentials: dict, pipeline_id: str, models: list, auto_tag_mapping: bool, adapter_type: str, manifest_metadata: dict
    ):
    new_tasks = []
    try:
        # Runs Info
        runs = RUNS.get("results", [])
        metadata = RUNS.get("metadata", {})
        run_id = metadata.get("invocation_id")
        mapping_tags = {}

        runs_copy = copy.deepcopy(runs)
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select source_id, id, is_selected from core.pipeline_tasks where asset_id='{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            existing_models = fetchall(cursor)
            existing_models = existing_models if existing_models else []
            
            # Prepare a set of existing source_ids for quick lookup
            insert_objects = []
            delete_columns(config, asset_id)
            column_objects = {}
            for model in models:
                source_data = next(
                    (
                        run
                        for run in runs_copy
                        if run["unique_id"] == model.get("unique_id")
                    ),
                    None,
                )

                start_time, end_time, duration, status, error = None, None, None, None, None
                if source_data:
                    runs_copy.remove(source_data)
                    start_time = (
                        source_data.get("timing")[0].get("started_at")
                        if source_data.get("timing")
                        else None
                    )
                    end_time = (
                        source_data.get("timing")[1].get("completed_at")
                        if source_data.get("timing")
                        else None
                    )
                    duration = calculate_duration(start_time, end_time)
                    status = source_data.get("status", None) if source_data else None
                    error = source_data.get("message", None) if source_data else None
                    error = error.replace("'", "''")  if error else None

                properties = {
                    "path": model.get("path" ""),
                    "original_file_path": model.get("original_file_path", ""),
                    "patch_path": model.get("patch_path" ""),
                    "build_path": model.get("build_path" ""),
                    "compiled_path": model.get("compiled_path" ""),
                    "access": model.get("access" ""),
                    "group": model.get("group" ""),
                    "checksum": model.get("checksum" ""),
                    "dependsOn": model.get("dependsOn" ""),
                    "childrenL1": model.get("childrenL1" ""),
                    "project": model.get("project_name", ""),
                }
                properties.update({
                    "materializes": {
                        "adapter_type":adapter_type,
                        "database": model.get("database", ""),
                        "schema": model.get("schema", ""),
                        "name": model.get("name", ""),
                        "fqn": f"{model.get('database','')}.{model.get('schema','')}.{model.get('name','')}".strip(".")
                    }
                })
                tags = model.get("tags") if model.get("tags") else []
                save_data = {
                    "asset": asset_id,
                    "pipeline": pipeline_id,
                    "connection": config.get("connection_id"),
                    "source_id": model.get("unique_id"),
                    "run_id": run_id,
                    "name": model.get("name", ""),
                    "description": model.get("description", ""),
                    "database": model.get("database", ""),
                    "schema": model.get("schema", ""),
                    "source_code": (
                        model.get("raw_code", "").replace("'", "''")
                        if model.get("raw_code")
                        else ""
                    ),
                    "compiled_code": (
                        model.get("compiled_code", "").replace("'", "''")
                        if model.get("compiled_code")
                        else ""
                    ),
                    "status": get_pipeline_status(status),
                    "duration": duration,
                    "run_start_at": start_time,
                    "run_end_at": end_time,
                    "created_at": (
                        datetime.fromtimestamp(model.get("created_at"))
                        if model.get("created_at")
                        else None
                    ),
                    "tags": tags,
                    "properties": json.dumps(properties, default=str).replace(
                        "'", "''"
                    ),
                    "error": error,
                }
                task_id = None
                existing_model = next((m for m in existing_models if m["source_id"] == model.get("unique_id")), None)
                if existing_model:
                    task_id = existing_model.get("id")
                    properties["materializes"] = update_materializes_asset_id(
                        config, properties.get("materializes", {}),  model_name=model.get("name"), task_id=task_id
                    )
                    is_selected = existing_model.get('is_selected', False)
                    if not is_selected:
                        continue
                    update_query = f"""
                        UPDATE core.pipeline_tasks
                        SET
                            name = '{save_data.get("name", "").replace("'", "''")}',
                            description = '{save_data.get("description", "").replace("'", "''")}',
                            database = '{save_data.get("database", "").replace("'", "''")}',
                            schema = '{save_data.get("schema", "").replace("'", "''")}',
                            source_code = '{save_data.get("source_code", "")}',
                            compiled_code = '{save_data.get("compiled_code", "")}',
                            status = '{save_data.get("status", "")}',
                            duration = {save_data.get("duration") if save_data.get("duration") is not None else 'NULL'},
                            run_start_at = {f"'{save_data.get('run_start_at')}'" if save_data.get("run_start_at") is not None else 'NULL'},
                            run_end_at = {f"'{save_data.get('run_end_at')}'" if save_data.get("run_end_at") is not None else 'NULL'},
                            tags = '{json.dumps(tags).replace("'", "''")}',
                            properties = '{save_data.get("properties", "")}',
                            error = '{save_data.get("error", "")}'

                        WHERE asset_id = '{asset_id}' AND source_id = '{model.get("unique_id")}'
                    """
                    execute_query(connection, cursor, update_query)
                else: 
                    task_id = str(uuid4())
                    properties["materializes"] = update_materializes_asset_id(
                        config, properties.get("materializes", {}),  model_name=model.get("name"), task_id=task_id
                    )
                    query_input = (
                        task_id,
                        asset_id,
                        pipeline_id,
                        config.get("connection_id"),
                        model.get("unique_id"),
                        run_id,
                        save_data.get("name"),
                        save_data.get("description"),
                        save_data.get("database"),
                        save_data.get("schema"),
                        save_data.get("source_code"),
                        save_data.get("compiled_code"),
                        save_data.get("status"),
                        save_data.get("duration"),
                        save_data.get("run_start_at") if save_data.get("run_start_at") else None,
                        save_data.get("run_end_at") if save_data.get("run_end_at") else None,
                        json.dumps(save_data.get("tags")),
                        save_data.get("properties"),
                        save_data.get("created_at"),
                        True,
                        True,
                        False,
                        save_data.get("error"),
                    )
                    new_tasks.append(task_id)
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals})", query_input
                    ).decode("utf-8")
                    insert_objects.append(query_param)
                processed_model = DBT_MODELS.get(model.get("unique_id"))
                columns = processed_model.get('columns')
                column_objects.update({task_id: columns})

                if tags:
                    mapping_tags.update({task_id: tags})


            insert_objects = split_queries(insert_objects)
            for input_values in insert_objects:
                try:
                    query_input = ",".join(input_values)
                    query_string = f"""
                        insert into core.pipeline_tasks(
                            id, asset_id, pipeline_id, connection_id, source_id, run_id, name, description,
                            database, schema, source_code, compiled_code, status, duration,
                            run_start_at, run_end_at, tags, properties, created_at,
                            is_selected, is_active, is_delete, error
                        ) values {query_input} 
                    """
                    cursor = execute_query(
                        connection, cursor, query_string)
                except Exception as e:
                    log_error('DBT Jobs Task Insert Failed  ', e)
            
            save_columns(config, asset_id, pipeline_id, column_objects)
            if auto_tag_mapping:
                __dbt_tag_mapping(config, mapping_tags, "task", asset_id)
            __prepare_lineage(config,credentials, CATALOG_ARTIFACT , adapter_type, manifest_metadata)
    except Exception as e:
        log_error(f"DBT Connector - Save Models By Job ID Failed ", e)
    finally:
        return new_tasks

def save_tests(
        config, asset_id: str, pipeline_id: str, tests: list, auto_tag_mapping: bool
    ):
    try:
        connection_id = config.get("connection_id")

        # Runs Info
        runs = RUNS.get("results", [])
        metadata = RUNS.get("metadata", {})
        run_id = metadata.get("invocation_id")
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select source_id, id from core.pipeline_tests where asset_id='{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            existings_tests = fetchall(cursor)
            existings_tests = existings_tests if existings_tests else []
            insert_objects = []
            mapping_tags = {}
            for test in tests:
                source_data = next(
                    (col for col in runs if col["unique_id"] == test.get("unique_id")),
                    None,
                )

                start_time = (
                    source_data.get("timing")[0].get("started_at")
                    if source_data and source_data.get("timing")
                    else None
                )
                end_time = (
                    source_data.get("timing")[1].get("completed_at")
                    if source_data and source_data.get("timing")
                    else None
                )
                duration = calculate_duration(start_time, end_time)
                status = source_data.get("status", None) if source_data else None
                error = source_data.get("message", None) if source_data else None
                properties = {
                    "path": test.get("path" ""),
                    "original_file_path": test.get("original_file_path", ""),
                    "patch_path": test.get("patch_path" ""),
                    "build_path": test.get("build_path" ""),
                    "compiled_path": test.get("compiled_path" ""),
                    "access": test.get("access" ""),
                    "group": test.get("group" ""),
                    "checksum": test.get("checksum" ""),
                    "dependsOn": test.get("dependsOn" ""),
                    "childrenL1": test.get("childrenL1" ""),
                    "file_key_name": test.get("file_key_name" ""),
                    "attached_node": test.get("attached_node" ""),
                }
                tags = test.get("tags") if test.get("tags") else []
                save_data = {
                    "asset": asset_id,
                    "pipeline": pipeline_id,
                    "connection": connection_id,
                    "source_id": test.get("unique_id"),
                    "run_id": run_id,
                    "name": test.get("name", ""),
                    "description": test.get("description", ""),
                    "column_name": test.get("column_name", ""),
                    "source_code": (
                        test.get("raw_code", "").replace("'", "''")
                        if test.get("raw_code")
                        else ""
                    ),
                    "depends_on": test.get("depends_on", {}).get("nodes", []),
                    "compiled_code": (
                        test.get("compiled_code", "").replace("'", "''")
                        if test.get("compiled_code")
                        else ""
                    ),
                    "status": get_pipeline_status(status),
                    "duration": duration,
                    "run_start_at": start_time,
                    "run_end_at": end_time,
                    "created_at": (
                        datetime.fromtimestamp(test.get("created_at"))
                        if test.get("created_at")
                        else None
                    ),
                    "tags": tags,
                    "properties": json.dumps(properties, default=str).replace(
                        "'", "''"
                    ),
                    "error": error,
                }
                existing_test = next((t for t in existings_tests if t["source_id"] == test.get("unique_id")), None)
                test_id = None
                if existing_test:
                    test_id = existing_test.get("id")
                    update_query = f"""
                        UPDATE core.pipeline_tests
                        SET
                            name = '{save_data.get("name", "").replace("'", "''")}',
                            description = '{save_data.get("description", "").replace("'", "''")}',
                            column_name = '{save_data.get("column_name", "").replace("'", "''")}',
                            source_code = '{save_data.get("source_code", "")}',
                            compiled_code = '{save_data.get("compiled_code", "")}',
                            status = '{save_data.get("status", "")}',
                            duration = {save_data.get("duration") if save_data.get("duration") is not None else 'NULL'},
                            run_start_at = {f"'{save_data.get('run_start_at', '')}'" if save_data.get("run_start_at") else 'NULL'},
                            run_end_at = {f"'{save_data.get('run_end_at', '')}'" if save_data.get("run_end_at") else 'NULL'},
                            tags = '{json.dumps(tags).replace("'", "''")}',
                            properties = '{save_data.get("properties", "")}',
                            depends_on = '{json.dumps(save_data.get("depends_on", [])).replace("'", "''")}',
                            updated_at = NOW(),
                            error = '{save_data.get("error", "")}'
                        WHERE id='{test_id}'
                    """
                    execute_query(connection, cursor, update_query)
                else:
                    test_id = str(uuid4())
                    query_input = (
                        test_id,
                        asset_id,
                        pipeline_id,
                        connection_id,
                        test.get("unique_id"),
                        run_id,
                        save_data.get("name", ""),
                        save_data.get("description", ""),
                        save_data.get("column_name"),
                        save_data.get("source_code"),
                        save_data.get("compiled_code"),
                        save_data.get("status"),
                        save_data.get("error"),
                        save_data.get("duration"),
                        save_data.get("run_start_at") if save_data.get("run_start_at") else None,
                        save_data.get("run_end_at") if save_data.get("run_end_at") else None,
                        json.dumps(tags),
                        save_data.get("properties"),
                        json.dumps(save_data.get("depends_on")),
                        save_data.get("created_at"),
                        True,
                        False
                    )
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals})", query_input
                    ).decode("utf-8")
                    insert_objects.append(query_param)
                    if tags:
                        mapping_tags.update({test_id: tags})
            insert_objects = split_queries(insert_objects)
            for input_values in insert_objects:
                try:
                    query_input = ",".join(input_values)
                    query_string = f"""
                        insert into core.pipeline_tests(
                            id, asset_id, pipeline_id, connection_id, source_id, run_id, name, description,
                            column_name, source_code, compiled_code, status, error, duration,
                            run_start_at, run_end_at, tags, properties, depends_on, created_at,
                            is_active, is_delete
                        ) values {query_input}
                    """
                    execute_query(connection, cursor, query_string)
                except Exception as e:
                    log_error('DBT Jobs Tests Insert Failed  ', e)
                
            if auto_tag_mapping:
                __dbt_tag_mapping(config, mapping_tags, "test", asset_id)
    except Exception as e:
        log_error(f"DBT Connector - Save Tests By Job ID Failed ", e)
    
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
    
def calculate_duration(started_at, completed_at):
    seconds = None
    if started_at and completed_at:
        # Converting strings to datetime objects
        fmt = "%Y-%m-%dT%H:%M:%S.%fZ"
        start_time = datetime.strptime(started_at, fmt)
        end_time = datetime.strptime(completed_at, fmt)

        # Calculating the duration
        duration = end_time - start_time

        # Calculate the duration in seconds
        seconds = duration.total_seconds()
    return seconds

def delete_columns(config: dict, asset_id: str):
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            delete from core.pipeline_columns
            where asset_id = '{asset_id}'
        """
        cursor = execute_query(connection, cursor, query_string)

def save_columns(config: dict, asset_id: str, pipeline_id: str, column_list : dict):
    connection_id = config.get("connection_id")
    insert_objects = []
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        for task in column_list.keys():
            task_id = task
            columns = column_list[task]
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
                    task_id,
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

def __prepare_lineage(config: dict, credentials: dict, catalog: dict, adapter_type: str, manifest_metadata: dict):
    try:
        models = list(DBT_MODELS.values())
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        lineage_config = {"tables": [], "relations": []}
        catalog_nodes = catalog.get("nodes", {}) if catalog else {}
        catalog_sources = catalog.get("sources") if  catalog else {}
        lineage_entity_config = []

        for model in models:
            extra_params = {"project_id": model.get("project_name", "")}
            model_unique_id = model.get("id", "")
            lineage_config = __get_current_node_lineage(model, lineage_config)
            lineage_config = __get_depends_on_node_lineage(
                model, lineage_config, extra_params
            )
            lineage_config = __get_childrens_node_lineage(
                model, lineage_config, extra_params
            )
            upstream = model.get("upstream", [])
            
            if not upstream:
                compiled_sql = model.get("compiledSql", "")
                if compiled_sql:
                    all_tables = __extract_all_tables_from_sql_simple(compiled_sql)
                    if all_tables:
                        for table_info in all_tables:
                            existing_asset = __check_table_exists_in_db(config, table_info)
                            table_entity = {
                                "database": table_info.get("database", ""),
                                "schema": table_info.get("schema", ""),
                                "name": table_info.get("name", ""),
                                "entity_name": table_info.get("name", ""),
                                "entity": table_info.get("name", ""),
                                "fields": [],
                                "columns": json.dumps([]),
                                "level": 2,
                                "connection_type": adapter_type,
                                "group": "pipeline"
                            }
                            if existing_asset:
                                table_entity["fields"] = existing_asset.get("attributes")
                            lineage_entity_config.append(table_entity)
                            lineage_config["tables"].append(table_entity)
                            if existing_asset:
                                __create_associated_asset_entry(config, asset_id, existing_asset["id"], table_entity)
                            model_table_relation = __create_model_to_table_relation(
                                model, table_entity, lineage_config, is_source=True, id_dbt_core=True
                            )
                            if model_table_relation:
                                lineage_config["relations"].extend(model_table_relation)
            
            for depends in upstream:
                if depends.startswith("source."):
                    if depends in catalog_sources:
                        catalog_source_info = __prepare_catalog_table_for_model(
                            model, catalog_sources.get(depends), adapter_type, is_source=True, id_dbt_core = True
                        )
                        existing_asset = __check_table_exists_in_db(config, catalog_source_info)
                        if existing_asset:
                            __create_associated_asset_entry(config, asset_id, existing_asset["id"], catalog_source_info)
                        lineage_entity_config.append(catalog_source_info)
                        lineage_config["tables"].append(catalog_source_info)
                        model_sorce_relation = __create_model_to_table_relation(
                            model, catalog_source_info, lineage_config, is_source=True, id_dbt_core = True
                        )
                        if  model_sorce_relation:
                            lineage_config["relations"].extend(model_sorce_relation)
            
                
            if model_unique_id in catalog_nodes:
                catalog_table_info = __prepare_catalog_table_for_model(
                    model, catalog_nodes[model_unique_id], adapter_type, is_source=False, id_dbt_core = True
                )
                lineage_entity_config.append(catalog_table_info)
                lineage_config["tables"].append(catalog_table_info)
                
                # Create relation from model to catalog table
                model_table_relation = __create_model_to_table_relation(
                    model, catalog_table_info, lineage_config, is_source=False, id_dbt_core = True
                )
                if model_table_relation:
                    lineage_config["relations"].extend(model_table_relation)
            else:
                model_table_info = __prepare_model_table_without_catalog(model, adapter_type, id_dbt_core = True)
                lineage_entity_config.append(model_table_info)
                lineage_config["tables"].append(model_table_info)
                
                # Create relation from model to model table
                model_table_relation = __create_model_to_table_relation(
                    model, model_table_info, lineage_config, is_source=False, id_dbt_core = True
                )
                if model_table_relation:
                    lineage_config["relations"].extend(model_table_relation)
        
        # Step 2: Insert all matched catalog tables in bulk to lineage_entity table
        if lineage_entity_config:
            lineage_config = __prepare_lineage_relations(lineage_config)

        # Generate Auto Mapping
        map_asset_with_lineage(config, lineage_config, "pipeline")
        associated_assets = __get_associated_assets_by_source_asset_id(config, asset_id)
        
        # Process lineage config with associated assets
        lineage_config, lineage_entity_config = __process_lineage_with_associated_assets(
            lineage_config, lineage_entity_config, associated_assets
        )
        __process_exposures_for_lineage(config, credentials, lineage_entity_config, lineage_config, manifest_metadata)
        save_lineage_entity(config, lineage_entity_config, asset_id)
        save_lineage(config, "pipeline", lineage_config, asset_id)


    except Exception as e:
        log_error(f"DBT Connector - Prepare Lineage Failed", e)
        raise e


def __process_exposures_for_lineage(config: dict,credentials: dict, lineage_entity_config: list, lineage_config: dict, manifest_metadata: dict):
    try:
        auto_tag_mapping = credentials.get("auto_mapping_tags", False)
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        asset_name = asset.get("name", "")
        connection_id = config.get("connection_id", "")
        connection_name = config.get("connection_name", "")
        exposures = manifest_metadata.get("exposures", {})

        if not exposures:
            return
        
        # Get existing models from database
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select id, source_id, name
                from core.pipeline_tasks 
                where asset_id = '{asset_id}'
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
                        # Get task_ids for all dependency nodes
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
        dependsOn = lineageSrcData.get("upstream", [])
        target_id = lineageSrcData.get("id", "")
        if len(dependsOn) > 0:
                dependsOnModel = list(
                    map(
                        lambda x: __map_lineage_data_by_type(
                            "upstream",
                            lineage_config,
                            x,
                            {
                                "level": 3,
                                "target_id": target_id,
                                "raw_models": lineageSrcData,
                                **extra_params,
                            },
                        ),
                        dependsOn,
                    )
                )

                dependsOnModel = list(filter(lambda x: x.get("id"), dependsOnModel))
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
        """
        Get childrens node's lineage
        """
        childrens = lineageSrcData.get("downstream", [])
        src_id = lineageSrcData.get("id", "")
        if len(childrens) > 0:
            childrenModel = list(
                map(
                    lambda x: __map_lineage_data_by_type(
                        "downstream",
                        lineage_config,
                        x,
                        {"level": 3, "src_id": src_id, **extra_params},
                    ),
                    childrens,
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
                data = DBT_MODELS.get(data, {})
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
            if isinstance(column, str):
                column = columns[column]
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
                            "srcTableId": src_id,
                            "tgtTableId": target_id,
                            "srcTableColName": field.get("name", ""),
                            "tgtTableColName": target_field[0].get("name", ""),
                        }
                        relations.append(obj)
                if len(relations) == 0:
                    obj = {"srcTableId": src_id, "tgtTableId": target_id}
                    relations.append(obj)
        return relations
    except Exception as e:
        log_error(f"DBT Connector - Prepare Field Relations ", e)
        raise e
    
def save_runs(
        config:dict , asset_id: str, pipeline_id: str, models_tests: list, status_filter: str
    ) -> None:
    """
    Save / Update Runs For Pipeline
    """
    latest_run = False
    try:
        runs = RUNS.get("results", [])
        metadata = RUNS.get("metadata", {})
        run_id = metadata.get("invocation_id")
        connection_id = config.get("connection_id")

        start_time = min(
            x.get("timing")[0].get("started_at") for x in runs if x.get("timing")
        )
        end_time = max(
            x.get("timing")[1].get("completed_at") for x in runs if x.get("timing")
        )
        duration = RUNS.get("elapsed_time", None)
        overall_status = "success"
        for obj in runs:
            if obj.get("status") != "success":
                overall_status = "failed"
                break
        if status_filter != "all" and status_filter != overall_status:
            return
        save_data = {
            "asset": asset_id,
            "pipeline": str(pipeline_id),
            "connection": connection_id,
            "source_id": run_id,
            "technical_id": run_id,
            "run_start_at": start_time if start_time else None,
            "run_end_at": end_time if end_time else None,
            "duration": duration,
            "status": get_pipeline_status(overall_status),
        }

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            # Delete Existing Runs Details Data
            query_string = f"""
                delete from core.pipeline_runs_detail
                where
                    asset_id = '{asset_id}'
                    and pipeline_id = '{pipeline_id}'
                    and run_id = '{run_id}'
            """
            cursor = execute_query(connection, cursor, query_string)

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
                        status = '{save_data.get('status')}', 
                        run_start_at = {f"'{save_data.get('run_start_at')}'" if save_data.get('run_start_at') else 'NULL'},
                        run_end_at = {f"'{save_data.get('run_end_at')}'" if save_data.get('run_end_at') else 'NULL'},
                        duration = '{save_data.get('duration')}'
                    where id = '{existing_run}'
                """
                cursor = execute_query(connection, cursor, query_string)
            else:
                run_id = str(uuid4())
                existing_run = run_id
                query_string = f"""
                    insert into core.pipeline_runs (
                        id, asset_id, pipeline_id, connection_id, source_id, technical_id,
                        run_start_at, run_end_at, duration, status, is_active, is_delete
                    ) values (
                        '{run_id}',
                        '{save_data.get('asset')}',
                        '{save_data.get('pipeline')}',
                        '{save_data.get('connection')}',
                        '{save_data.get('source_id')}',
                        '{save_data.get('technical_id')}',
                        {f"'{save_data.get('run_start_at')}'" if save_data.get('run_start_at') else 'NULL'},
                        {f"'{save_data.get('run_end_at')}'" if save_data.get('run_end_at') else 'NULL'},
                        {save_data.get('duration') if save_data.get('duration') is not None else 'NULL'},
                        '{save_data.get('status')}',
                        {True},
                        {False}
                    )
                """
                cursor = execute_query(connection, cursor, query_string)
                latest_run = True
            save_runs_detail(
                config, existing_run, asset_id, pipeline_id, models_tests
            )

            # Update Pipeline Jobs Last Runs Details
            job_update = {
                "status": get_pipeline_status(overall_status),
                "duration": duration,
                "last_run_at": start_time,
            }
            # Build and execute a raw SQL update query for the pipeline record
            update_query = f"""
                UPDATE core.pipeline
                SET
                    status = '{job_update.get('status', '')}',
                    last_run_at = {f"'{job_update.get('last_run_at')}'" if job_update.get('last_run_at') else 'NULL'},
                    updated_at = NOW()
                WHERE asset_id = '{asset_id}'
            """
            cursor = execute_query(connection, cursor, update_query)
    except Exception as e:
        log_error(f"DBT Connector - Save Runs By Job ID Failed ", e)
    finally:
        return latest_run

def save_runs_detail(
    config: dict, pipeline_run_id: str, asset_id: str, pipeline_id: str, models_tests: list
) -> None:
    """
    Save / Update Runs For Pipeline
    """
    try:
        runs = RUNS.get("results", [])
        metadata = RUNS.get("metadata", {})
        run_id = metadata.get("invocation_id")
        connection_id = config.get("connection_id")

        m_t_data = []
        for key, value in models_tests.items():
            m_t_data.append(value)

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            insert_objects = []
            for run in runs:
                start_time = (
                    run.get("timing")[0].get("started_at", None)
                    if run.get("timing") and run.get("timing")[0]
                    else None
                )
                end_time = (
                    run.get("timing")[1].get("completed_at", None)
                    if run.get("timing") and run.get("timing")[1]
                    else None
                )
                properties = {
                    "relation_name": run.get("relation_name" ""),
                    "failures": run.get("failures" ""),
                    "message": run.get("message" ""),
                    "thread_id": run.get("thread_id" ""),
                    "adapter_response": run.get("adapter_response", None),
                }
                source_data = next(
                    (
                        col
                        for col in m_t_data
                        if col["unique_id"] == run.get("unique_id")
                    ),
                    None,
                )

                save_data = {
                    "id": str(uuid4()),
                    "asset": asset_id,
                    "pipeline": str(pipeline_id),
                    "connection": connection_id,
                    "run_id": run_id,
                    "pipeline_run": str(pipeline_run_id),
                    "type": (
                        "test" if source_data.get("resource_type") == "test" else "task"
                    ),
                    "name": (
                        source_data.get("name") if source_data else run.get("unique_id")
                    ),
                    "source_id": run.get("unique_id"),
                    "run_start_at": start_time if start_time else None,
                    "run_end_at": end_time if end_time else None,
                    "duration": run.get("execution_time", None),
                    "status": get_pipeline_status(run.get("status")),
                    "compiled_code": (
                        run.get("compiled_code", "").replace("'", "''")
                        if run.get("compiled_code")
                        else ""
                    ),
                    "properties": json.dumps(properties, default=str).replace(
                        "'", "''"
                    ),
                    "error": run.get("message", ""),
                }

                query_input = (
                    save_data["id"],
                    save_data["asset"],
                    save_data["pipeline"],
                    save_data["connection"],
                    save_data["run_id"],
                    save_data["pipeline_run"],
                    save_data["type"],
                    save_data["name"],
                    save_data["source_id"],
                    save_data["run_start_at"],
                    save_data["run_end_at"],
                    save_data["duration"],
                    save_data["status"],
                    save_data["compiled_code"],
                    save_data["properties"],
                    save_data["error"],
                    True,
                    False
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
                        insert into core.pipeline_runs_detail (
                            id, asset_id, pipeline_id, connection_id, run_id, pipeline_run_id,
                            type, name, source_id, run_start_at, run_end_at, duration, status,
                            compiled_code, properties, error, is_active, is_delete
                        ) values {query_input}
                    """
                    execute_query(connection, cursor, query_string)
                except Exception as e:
                    log_error('DBT Runs Details Insert Failed  ', e)

    except Exception as e:
        log_error(
            f"DBT Connector - Save Runs Details By Run ID Failed ", e
        )

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


def extract_pipeline_measure(config: dict, tasks_pull: bool):
    asset_id = config.get("asset_id")
    
    # Fetch pipeline_name (job_name) from pipeline table
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
    
    run_history = get_run_history(config, asset_id)
    if not run_history:
        return
    latest_run = run_history[0]
    previous_run = run_history[1] if len(run_history) > 1 else {}
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
    previous_task_history = []
    if previous_run:
       previous_task_history = get_task_previous_run_history(config, previous_run.get("id"))
    if not tasks:
        return
    
    for task in tasks:
        source_id = task.get("source_id")
        task_name = task.get("name")
        previous_task_run = next((item for item in previous_task_history if item.get("source_id") == source_id), None)
        latest_task_run = next((item for item in RESULTS if item.get("unique_id") == source_id), None)
        if not latest_task_run:
            continue
        timing = latest_task_run.get("timing", [])
        if not timing:
            continue
        latest_started_at = timing[0].get("started_at")
        adapter_response = latest_task_run.get("adapter_response", {})
        is_row_written = adapter_response.get("code") == "SUCCESS"
        task_run_detail = {
            "duration": latest_task_run.get("execution_time"),
            "last_run_date": latest_started_at,
            "previous_run_date": previous_task_run.get("started_at") if previous_task_run else None,
            "is_row_written": is_row_written,
            "rows_affected": adapter_response.get('rows_affected')
        }
        # Pass pipeline_name (job_name) and task_name for task level measures
        execute_pipeline_measure(config, "task", task_run_detail, task_info=task, job_name=pipeline_name, task_name=task_name)

    

