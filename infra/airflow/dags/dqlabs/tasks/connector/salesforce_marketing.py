import json
from math import log
from uuid import uuid4
from datetime import datetime, timedelta
import uuid

from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone, split_queries
from dqlabs.app_helper.connection_helper import decrypt_connection_config
from dqlabs.app_helper.dag_helper import (
    get_postgres_connection,
)
from dqlabs.app_helper.lineage_helper import save_lineage, save_asset_lineage_mapping, save_lineage_entity, update_pipeline_propagations, update_reports_propagations, handle_alerts_issues_propagation
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
from dqlabs.app_helper.dq_helper import get_pipeline_status
from dqlabs.enums.connection_types import ConnectionType
from dqlabs.utils.pipeline_measure import execute_pipeline_measure, create_pipeline_task_measures, get_pipeline_tasks
from dqlabs.utils.catalog_update  import extract_vault_credentials

TASK_CONFIG = None
LATEST_RUN = False
ALL_RUNS = []

def extract_salesforce_marketing_data(config, **kwargs):
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
        asset_id = asset.get("id")
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
        credentials = connection.get("credentials")
        if credentials.get("is_vault_enabled",False):
            credentials = extract_vault_credentials(config,credentials)

        connection_type = connection.get("type", "")
        credentials = decrypt_connection_config(credentials, connection_type)
        source_connection = get_native_connection(config)
        metadata_pull_config = credentials.get("metadata", {})
        runs_pull = metadata_pull_config.get("runs", False)
        tasks_pull = metadata_pull_config.get("tasks", False)
        pipeline_id = __get_pipeline_id(config)
        TASK_CONFIG = config

        data = {}
        pipeline_info = {}
        pipeline_runs = []
        all_runs = []
        runs_detail_list = []

        if runs_pull:
            # save runs
            pipeline_runs = __get_pipeline_runs(config, asset, credentials)
            if pipeline_runs:
                latest_run, all_runs, runs_detail_list = __save_pipeline_runs(
                    config,
                    pipeline_info,
                    asset_id,
                    pipeline_id,
                    "pipeline",
                    pipeline_runs
                )
        # get pipeline details
        pipeline_info = __get_pipeline_detail(config, asset_properties, credentials)
        if tasks_pull:
            if pipeline_info:
                # save tasks in pipeline_task table
                new_tasks = __save_pipeline_tasks(
                    config,
                    pipeline_info,
                    pipeline_id,
                    asset_id,
                    asset_properties,
                    "task",
                    all_runs,
                    runs_detail_list
                )
        
            # Update Propagations Alerts and Issues Creation and Notifications
            __update_last_run_stats(config, "pipeline")

        # prepare lineage
        __prepare_lineage(config, pipeline_info, asset)

        # # Update Job Run Stats
        __update_pipeline_stats(config)
        update_pipeline_propagations(config, asset, connection_type)
        # # Get Description and Prepare Search Key
        description = __prepare_description(asset)
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

        # # update request queue status
        is_triggered = config.get("is_triggered")
        if not is_triggered:
            create_queue_detail(config)
        update_queue_detail_status(config, ScheduleStatus.Completed.value)
        # if LATEST_RUN:
        #     extract_pipeline_measure(ALL_RUNS, tasks_pull)
    except Exception as e:
        log_error("Salesforce Marketing Pull / Push Failed", e)
        update_queue_detail_status(config, ScheduleStatus.Failed.value, str(e))
    finally:
        update_queue_status(config, ScheduleStatus.Completed.value)

def __update_last_run_stats(config: dict, type: str, last_run_id: str = "") -> str:
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select source_id as run_id,status, run_start_at, run_end_at, duration from core.pipeline_runs
                where asset_id = '{asset_id}' order by run_start_at desc
                limit 1
            """
            cursor = execute_query(connection, cursor, query_string)
            last_run = fetchone(cursor)
            if last_run:
                run_id = last_run.get('run_id') 
                status = last_run.get('status')
                last_run_at = last_run.get('run_start_at') if type == "pipeline" else last_run.get('run_end_at')
                pipeline_query_string = ""
                pipeline_query_string = f"""
                    update core.pipeline set run_id='{run_id}', status='{status}', last_run_at='{last_run_at}'
                    where asset_id = '{asset_id}'
                """
                execute_query(connection, cursor, pipeline_query_string)
                asset = config.get("asset", {})
                last_run_start_at = last_run.get('run_start_at')
                last_run_end_at = last_run.get('run_end_at')
                duration = last_run.get('duration')
                update_task_query = f""" update core.pipeline_tasks set run_start_at='{last_run_start_at}', run_end_at='{last_run_end_at}', duration='{duration}' where asset_id = '{asset_id}'"""
                execute_query(connection, cursor, update_task_query)
                handle_alerts_issues_propagation(config, run_id)
    except Exception as e:
        log_error(f"Salesforce Marketing Pipeline Connector - Update Run Stats to Job Failed ", e)
        raise e

def __get_pipeline_runs(config, asset, credentials):
    try:
        no_of_days = credentials.get("no_of_runs", 30)
        # Calculate timestamp for n days ago
        days_ago_date = datetime.now() - timedelta(days=int(no_of_days))
        status = credentials.get("status")
        url_groups = (
            f"""https://{credentials.get("sub_domain")}.soap.marketingcloudapis.com/Service.asmx"""
        )
        api_response = __get_response(
            config,
            url_groups,
            "post",
            { 
                "after_date": str(days_ago_date),
                "status": status,
                "name": asset.get("name").strip()
            },
            "get_pipeline_runs"
        )
        if api_response.get("is_fault"):
            return []
        
         # Navigate through the nested structure
        results = (api_response
                  .get("data", {})
                  .get("Body", {})
                  .get("RetrieveResponseMsg", {})
                  .get("Results", []))
        
        if results and isinstance(results, list):
            # - 0: Error
            # - 1: Completed
            # - 2: Active (Running)
            # - 3: Waiting
            # - 4: Cancelled
            # - 5: Validating
            # - 6: Ready
            # - 7: Initializing

            is_filter_applied = False if status.lower() == "all" else True
            filter_status = 1
            if status.lower() == "failure":
                filter_status = -1
            
            def _parse_dt(value):
                try:
                    if isinstance(value, str):
                        currentDate = datetime.fromisoformat(value.replace('Z', '+00:00')).replace(tzinfo=None)
                        return datetime.fromisoformat(value.replace('Z', '+00:00')).replace(tzinfo=None)
                    if isinstance(value, (int, float)):
                        ts = value / 1000 if value > 1e12 else value
                        return datetime.fromtimestamp(ts)
                except Exception:
                    return None

            cutoff_date = days_ago_date
            filtered_results = [
                item
                for item in results
                if (
                    item.get("CreatedDate")
                    and _parse_dt(item.get("CreatedDate")) >= cutoff_date
                    and (
                        (is_filter_applied and item.get("Status") == filter_status)
                        or (
                            is_filter_applied == False
                            and str(item.get("Status")) in ["-1", "1"]
                        )
                    )
                )
            ]
            return filtered_results
        else:
            return []
        pass
    except Exception as e:
        log_error("Salesforce Marketing Runs Failed", e)

def __prepare_lineage(config, pipeline_info, asset):
    """
    Helper function to prepare Lineage
    """
    try:
        lineage = {
            "tables": [],
            "relations": []
        }

        lineage = __prepare_lineage_tables(lineage, pipeline_info)
        lineage = __prepare_lineage_relations(lineage, pipeline_info, asset.get("technical_name"))
        # Save Lineage
        save_lineage(config, "pipeline", lineage, asset.get("id"))

        # Save Propagation Values
        update_reports_propagations(config, config.get("asset", {}))

        # Save Associated Asset Map (will not happen as we don't get source details)
        #map_asset_with_lineage(config, lineage, "report")
    except Exception as e:
        log_error(f"Salesforce Marketing Connector - Prepare Lineage Table Failed ", e)
        raise e

def __prepare_lineage_tables(lineage: dict, pipeline_info) -> dict:
    """
    Helper function to prepare  Lineage Tables
    """
    try:
        steps = pipeline_info.get("steps", [])
        for step in steps:
            step.update({
                "name": f"""Step {step.get("step")}"""
            })
        lineage.update({
            "tables": steps
        })
        return lineage
    except Exception as e:
        log_error(f"Salesforce Marketing Connector  - Prepare Lineage Table Failed ", e)
        raise e


def __prepare_lineage_relations(lineage: dict, pipeline_info: dict, current_source_id: str) -> dict:
    """
    Helper function to prepare Lineage Relations
    """
    try:
        relations = []
        items = pipeline_info.get("steps")
        items = items if items else []
        if len(items) == 1:
            next_source_id = f"""task.{items[0].get('id').lower()}.step_{items[0].get('step')}"""
            relation = {
                "srcTableId": current_source_id,
                "tgtTableId": next_source_id
            }
            lineage["relations"].append(relation)
            
        for index, item in enumerate(items):
            current_source_id = f"""task.{item.get('id').lower()}.step_{item.get('step')}"""
            # Build relation to next item's source_id (skip last item)
            if index < len(items) - 1:
                next_item = items[index + 1]
                next_source_id = ""
                if next_item and next_item.get('id') and next_item.get('name'):
                    next_source_id = f"""task.{next_item.get('id').lower()}.step_{next_item.get('step')}"""

                relation = {
                    "srcTableId": current_source_id,
                    "tgtTableId": next_source_id
                }
                lineage["relations"].append(relation)
        return lineage
    except Exception as e:
        log_error(f"Salesforce Marketing Connector - Prepare Lineage Relation Failed ", e)
        raise e


def __get_response(config: dict, url: str = "", method_type: str = "get", params=None, method_name="execute"):
    api_response = None
    try:
        pg_connection = get_postgres_connection(config)
        api_response = agent_helper.execute_query(
            config,
            pg_connection,
            "",
            method_name="execute",
            parameters=dict(
                method_name=method_name,
                request_url=url,
                request_type=method_type,
                request_params=params
            ),
        )
        api_response = api_response if api_response else {}
        return api_response
    except Exception as e:
        raise e

def __save_pipeline_runs(
    config: dict,
    data: dict,
    asset_id: str,
    pipeline_id: str,
    pipeline_type: str = "pipeline",
    runs: list = []
) -> None:
    """
    Save / Update Runs For Pipeline
    """
    all_runs = []
    all_run_details = []
    latest_run = False
    try:
        connection_data = config.get("connection", {})
        connection_id = connection_data.get("id")
        connection = get_postgres_connection(config)
        
        if not connection_id or not runs:
            return
        pipeline_runs = []
        all_pipeline_runs = []
        with connection.cursor() as cursor:
            # Clear Existing Runs Details
            query_string = f"""
                delete from core.pipeline_runs_detail
                where
                    asset_id = '{asset_id}'
                    and pipeline_id = '{pipeline_id}'
            """
            cursor = execute_query(connection, cursor, query_string)

            query_string = f"""
                delete from core.pipeline_runs
                where
                    asset_id = '{asset_id}'
                    and pipeline_id = '{pipeline_id}'
            """
            cursor = execute_query(connection, cursor, query_string)

            insert_objects = []
            for i, item in enumerate(runs):
                started_at = item.get("CreatedDate") if item.get("CreatedDate") else None
                finished_at = item.get("ModifiedDate") if item.get("ModifiedDate") else item.get("CreatedDate")
                if pipeline_type == "job":
                    started_at = datetime.fromtimestamp(item.get("CreatedDate") / 1000) if item.get("CreatedDate") else None
                    finished_at = datetime.fromtimestamp(item.get("ModifiedDate") / 1000) if item.get("ModifiedDate") else None

                run_id =  str(uuid4())
                source_id = f"""{pipeline_type}.{run_id.lower()}.{item.get('Name').lower().replace(" ","_")}"""
                status_humanized = item.get("StatusMessage", "").lower() if item.get("StatusMessage") else ""
                if pipeline_type != 'job':
                    status_message = item.get("StatusMessage", "").lower()
                    status_humanized = "success" if any(x in status_message for x in ["success", "complete"]) else "failed"
                error_message = item.get("StatusMessage", "").replace("'", "''").lower()
                duration = None
                if started_at and finished_at:
                    # Parse the string timestamps to datetime objects
                    started_datetime = datetime.fromisoformat(started_at.replace('Z', '+00:00'))
                    finished_datetime = datetime.fromisoformat(finished_at.replace('Z', '+00:00'))
                    
                    # Now you can subtract them
                    time_delta = finished_datetime - started_datetime
                    duration = time_delta.total_seconds()
                
                individual_run = {
                    "id": run_id,
                    "runGeneratedAt":started_at,
                    "status": item.get("StatusMessage", item.get("StatusMessage", "")),
                    "status_humanized": status_humanized,
                    "status_message": item.get("StatusMessage", ""),
                    "created_at": item.get("CreatedDate", datetime.now()),
                    "updated_at": item.get("ModifiedDate", ""),
                    "started_at": started_at,
                    "finished_at": finished_at,
                    "job": item.get("job", ""),
                    "environment": item.get("environment", ""),
                    "run_steps": item.get("run_steps", ""),
                    "in_progress": item.get("in_progress", ""),
                    "is_complete": True if item.get("StatusMessage", "").lower() == "complete" else False,
                    "is_success": True if status_humanized == "success" else False, 
                    "is_error": True if status_humanized == "failed" else False,
                    "is_cancelled": item.get("is_cancelled", ""),
                    "duration": duration,
                    "run_duration":duration,
                    "job_id": source_id,
                    "unique_id": source_id,
                    "is_running": item.get("is_running", ""),
                    "href": item.get("href", ""),
                    "uniqueId": source_id,
                    "unique_job_id": item.get("job_id", ""),
                    "models": [
                        {**each_activity, "uniqueId": source_id}
                        for each_activity in item.get("dataflow_activities", [])
                    ],
                }

                # make properties
                properties = individual_run

                # Validating existing runs
                query_string = f"""
                    select id from core.pipeline_runs
                    where
                        asset_id = '{asset_id}'
                        and pipeline_id = '{pipeline_id}'
                        and source_id = '{individual_run.get('job_id')}'
                """
                cursor = execute_query(connection, cursor, query_string)
                existing_run = fetchone(cursor)
                existing_run = existing_run.get("id") if existing_run else None
                
                if existing_run:
                    query_string = f"""
                        update core.pipeline_runs set 
                            status = '{status_humanized}', 
                            error = '{error_message}',
                            run_start_at = {f"'{started_at}'" if started_at else 'NULL'},
                            run_end_at = {f"'{finished_at}'" if finished_at else 'NULL'},
                            duration = '{duration}' ,
                            properties = '{json.dumps(properties, default=str).replace("'", "''")}'
                        where id = '{existing_run}'
                    """
                    cursor = execute_query(connection, cursor, query_string)
                    pipeline_runs.append(
                        {
                            "id": existing_run,
                            "source_id": individual_run.get("job_id"),
                            "is_update": True,
                        }
                    )
                    all_pipeline_runs.append(
                        {
                            "id": existing_run,
                            "source_id": individual_run.get("job_id"),
                            "is_update": True,
                        }
                    )
                else:
                    query_input = (
                        uuid4(),
                        individual_run.get("job_id"),
                        run_id,
                        status_humanized,
                        error_message,
                        individual_run.get('started_at') if individual_run.get('started_at') else None,
                        individual_run.get('finished_at') if individual_run.get('finished_at') else None,
                        duration,
                        json.dumps(properties, default=str).replace("'", "''"),
                        True,
                        False,
                        pipeline_id,
                        asset_id,
                        connection_id,
                    )
                    all_pipeline_runs.append(query_input)
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals})", query_input
                    ).decode("utf-8")
                    insert_objects.append(query_param)
                    if not latest_run:
                        latest_run = True
                all_runs.append(individual_run)

            insert_objects = split_queries(insert_objects)

            for input_values in insert_objects:
                try:
                    query_input = ",".join(input_values)
                    query_string = f"""
                        insert into core.pipeline_runs(
                            id, source_id, technical_id, status, error,  run_start_at, run_end_at, duration,
                            properties, is_active, is_delete, pipeline_id, asset_id, connection_id
                        ) values {query_input} 
                        RETURNING id, source_id;
                    """
                    cursor = execute_query(connection, cursor, query_string)
                    pipeline_runs = fetchall(cursor)
                except Exception as e:
                    log_error("Salesforce Marketing Runs Insert Failed  ", e)
            # save individual run detail
            all_run_details = __save_runs_details(config, all_runs, pipeline_id, pipeline_runs)
    except Exception as e:
        log_error(f"Salesforce Marketing Pipeline Connector - Saving Runs Failed ", e)
    finally:
        return latest_run, all_runs, all_run_details


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
                return pipeline.get("id")
    except Exception as e:
        log_error(
            f"Salesforce Marketing Connector - Get Pipeline Primary Key Information By Asset ID Failed ",
            e,
        )
        raise e

def __save_pipeline_tasks(config: dict, pipeline_info: dict, pipeline_id:str, asset_id: str, pipeline_properties: dict = {},  pipeline_type: str = "task", all_runs = [], runs_detail_list= []):
    """
    Save / Update Task For Pipeline
    """
    new_pipeline_tasks = []
    try:
        connection_data = config.get("connection", {})
        connection_id = connection_data.get("id")
        connection = get_postgres_connection(config)
        pipeline_tasks = pipeline_info.get("steps", [])
        source_type = "task"
        tasks = []
        all_tasks = []
        if not pipeline_tasks:
            return

        last_run = all_runs[-1] if len(all_runs) else {}
        last_run_status = last_run.get("status_humanized").lower() if last_run else ""
        with connection.cursor() as cursor:
            for task in pipeline_tasks:
                task_id = f"""{pipeline_type}.{task.get('id').lower()}.step_{task.get('step')}"""
                task_start_datetime = None
                task_end_datetime = None
                if pipeline_info.get("type") == "scheduled":
                    task_start_datetime = pipeline_info.get('schedule').get('startDate') if pipeline_info.get('schedule').get('startDate') else None
                    task_end_datetime = pipeline_info.get('schedule').get('endDate') if pipeline_info.get('schedule').get('endDate') else None

                # Validate Existing Task Information
                query_string = f""" 
                    select id from core.pipeline_tasks 
                    where asset_id = '{asset_id}' and source_id = '{task_id}' 
                """
                cursor = execute_query(connection, cursor, query_string)
                existing_task = fetchone(cursor)
                existing_task = existing_task.get("id") if existing_task else None
                properties = {
                    **pipeline_properties,
                    "description": task.get("description", ""),
                    "step": task.get("step", {}),
                    "task_id": task.get("id", {}),
                    "activities": task.get("activities", {}),
                    "activities_count": len(task.get("activities")) if task.get("activities") else 0
                }
                if existing_task:
                    query_string = f"""
                        update core.pipeline_tasks set 
                            run_start_at = {'NULL'},
                            run_end_at = {'NULL'},
                            properties = '{json.dumps(properties, default=str).replace("'", "''")}',
                            updated_at = '{datetime.now()}',
                            source_type = '{source_type}',
                            status = '{last_run_status}'
                        where id = '{existing_task}'
                    """
                    cursor = execute_query(connection, cursor, query_string)
                else:
                    pipeline_task_id = str(uuid4())
                    query_input = (
                        pipeline_task_id,
                        task_id,
                        f"""Step {task.get('step')}""",
                        task.get("owner", ""),
                        json.dumps(properties, default=str).replace("'", "''"),
                        task_start_datetime,
                        task_end_datetime,
                        source_type,
                        str(pipeline_id),
                        str(asset_id),
                        str(connection_id),
                        True,
                        True,
                        False,
                        datetime.now(),
                        (
                            task.get("description")
                            if task.get("description")
                            else __prepare_description(
                                {"task_name": task.get("name"), **pipeline_properties}
                            )
                        ),
                        last_run_status
                    )
                    all_tasks.append(query_input)
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals})", query_input
                    ).decode("utf-8")
                    tasks.append(query_param)
                    new_pipeline_tasks.append(pipeline_task_id)

            # create each tasks
            tasks_input = split_queries(tasks)
            for input_values in tasks_input:
                try:
                    query_input = ",".join(input_values)

                    tasks_insert_query = f"""
                        insert into core.pipeline_tasks (id, source_id, name, owner, properties, run_start_at,
                        run_end_at, source_type, pipeline_id, asset_id,connection_id, is_selected, is_active, is_delete,
                        created_at, description, status)
                        values {query_input}
                        RETURNING id, source_id;
                    """
                    cursor = execute_query(connection, cursor, tasks_insert_query)
                    pipeline_tasks = fetchall(cursor)
                    # Save Task Runs Columns
                    __save_task_run_details(config, runs_detail_list, pipeline_id, all_tasks)
                except Exception as e:
                    log_error("extract properties: inserting tasks level", e)
    except Exception as e:
        log_error(f"Salesforce Marketing Pipeline Connector - Saving tasks ", e)
        raise e
    finally:
        return new_pipeline_tasks

def __save_task_run_details(config:dict, runs_detail_list: list, pipeline_id: str, tasks: list):
    if runs_detail_list is None:
        return []
    
    task_run_details = []
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        for task in tasks:
            for run in runs_detail_list:
                data_list = list(run)
                data_list[0] = str(uuid4())  # new run_detail id
                data_list[2] = task[1]  # new source_id
                data_list[3] = "task"  # task type
                new_data = tuple(data_list)
                input_literals = ", ".join(["%s"] * len(new_data))
                query_param = cursor.mogrify(
                    f"({input_literals})", new_data
                ).decode("utf-8")
                task_run_details.append(query_param)

        if task_run_details is None:
            return []
        
        # make connection
        splitted_insert_objects = split_queries(task_run_details)
        for input_values in splitted_insert_objects:
            try:
                # insert into run_details table
                query_input = ",".join(input_values)
                query_string = f"""
                    insert into core.pipeline_runs_detail(
                        id, run_id, source_id, type, name, status, error, source_code, compiled_code,
                        run_start_at, run_end_at, duration, is_active, is_delete,
                        pipeline_run_id, pipeline_id, asset_id, connection_id
                    ) values {query_input} 
                """
                cursor = execute_query(connection, cursor, query_string)
            except Exception as e:
                log_error("Salesforce Marketing Pipeline Task Runs Details Insert Failed  ", e)    
    

def __get_pipeline_detail(config: dict, asset_properties: dict, credentials: dict):
    try:
        if asset_properties.get("unique_id") == "":
            return {}
        # automation detail url
        url_groups = (
            f"""https://{credentials.get('sub_domain')}.rest.marketingcloudapis.com/automation/v1/automations/{asset_properties.get('unique_id')}"""
        )
        api_response = __get_response(
            config,
            url_groups,
            "get",
            {},
            "get_pipeline_detail"
        )
        return api_response
    except Exception as e:
        raise e

def get_data_by_key(data, value, get_key="id", filter_key="source_id"):
    return next((item for item in data if item[filter_key] == value), None)

def __save_runs_details(
    config: dict, data: list, pipeline_id: str, pipeline_runs: list = None
):
    try:
        connection_obj = config.get("connection", {})
        connection_id = connection_obj.get("id")
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        insert_objects = []
        raw_insert_objects = []
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            for run in data:
                pipeline_run = get_data_by_key(
                    pipeline_runs, run.get("uniqueId")
                )
                query_input = (
                    str(uuid4()),
                    run.get("id", ""),
                    pipeline_run.get("source_id", "") if pipeline_run else "",
                    "job",
                    run.get("job_id"),
                    get_pipeline_status(run.get("status")),
                    run.get("status", ""),
                    (
                        run.get("rawSql", "").replace("'", "''")
                        if run.get("rawSql")
                        else ""
                    ),
                    (
                        run.get("compiledSql", "").replace("'", "''")
                        if run.get("compiledSql")
                        else ""
                    ),
                    run.get("started_at", None),
                    run.get("finished_at", None),
                    run.get("duration", None),
                    True,
                    False,
                    pipeline_run.get("id", "") if pipeline_run else None,
                    pipeline_id,
                    asset_id,
                    connection_id,
                )
                raw_insert_objects.append(query_input)
                input_literals = ", ".join(["%s"] * len(query_input))
                query_param = cursor.mogrify(
                    f"({input_literals})", query_input
                ).decode("utf-8")
                insert_objects.append(query_param)
            splitted_insert_objects = split_queries(insert_objects)
            for input_values in splitted_insert_objects:
                try:
                    query_input = ",".join(input_values)
                    query_string = f"""
                        insert into core.pipeline_runs_detail(
                            id, run_id, source_id, type, name, status, error, source_code, compiled_code,
                            run_start_at, run_end_at, duration, is_active, is_delete,
                            pipeline_run_id, pipeline_id, asset_id, connection_id
                        ) values {query_input} 
                    """
                    cursor = execute_query(connection, cursor, query_string)
                except Exception as e:
                    log_error("Salesforce Marketing Pipeline Runs Details Insert Failed  ", e)
            return raw_insert_objects
    except Exception as e:
        log_error(f"Salesforce Marketing Pipeline Connector - Save Runs Details By Run ID Failed ", e)
        raise e

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
        log_error(f"Salesforce Marketing Connector - Prepare Search Keys ", e)
    finally:
        return search_keys


def __prepare_description(asset):
    job_description = asset.get('description')
    job_description = f"{job_description}".strip()
    if job_description:
        return job_description
    
    properties = asset.get("properties")
    name = properties.get("name", "")
    project_name = properties.get("project_name", "")
    group_name = properties.get("group_name", "")
    owner_name = properties.get("owner_name", "")
    runGeneratedAt = properties.get("runGeneratedAt", "")

    generated_description = f"""This Salesforce pipeline {name} is part of Project {project_name} and group {group_name}. 
Owned by {owner_name} and runs generated at {runGeneratedAt}"""
    
    return f"{generated_description}".strip()

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
                WITH task_agg AS (
                    SELECT
                        pt.asset_id,
                        COUNT(*) AS tot_tasks,
                        COALESCE(SUM(
                        CASE
                            WHEN pt.properties ? 'activities_count'
                                AND (pt.properties->>'activities_count') ~ '^\d+$'
                            THEN (pt.properties->>'activities_count')::int
                            ELSE 0
                        END
                        ), 0) AS total_activities
                    FROM core.pipeline_tasks pt
                    GROUP BY pt.asset_id
                    ),
                    run_agg AS (
                    SELECT
                        pr.asset_id,
                        COUNT(*) AS tot_runs
                    FROM core.pipeline_runs pr
                    GROUP BY pr.asset_id
                    )
                    SELECT
                    p.id,
                    COALESCE(t.tot_tasks, 0)       AS tot_tasks,
                    COALESCE(r.tot_runs, 0)        AS tot_runs,
                    COALESCE(t.total_activities,0) AS total_activities
                    FROM pipeline p
                    LEFT JOIN task_agg t ON t.asset_id = p.asset_id
                    LEFT JOIN run_agg  r ON r.asset_id = p.asset_id
                    WHERE p.asset_id ='{asset_id}'
            """
            # need to check this query
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
                "total_activities":  pipeline_stats.get('total_activities', 0),
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
        log_error(f"Salesforce Marketing Connector - Update Run Stats to Job Failed ", e)
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
            return
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