import uuid
import boto3
import json
from datetime import datetime, timezone
import math
import gc

from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone, split_queries
from dqlabs.app_helper.dag_helper import (
    get_postgres_connection,
)
from dqlabs.utils.extract_workflow import get_metadata_query, get_queries
from dqlabs.app_helper.dq_helper import convert_to_lower, get_aws_credentials
from dqlabs.app_helper.dag_helper import execute_native_query
from dqlabs.utils.request_queue import update_queue_detail_status, update_queue_status
from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.enums.connection_types import ConnectionType
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.utils.tasks import clear_duplicate_task
from dqlabs.app_constants.dq_constants import USAGE_QUERY


def get_usage_query_days(config: dict) -> str:
    days = "7"
    try:
        """
        Returns the list of range measures for the given attribute
        """
        asset_id = config.get("asset_id")
        connection = get_postgres_connection(config)
        connection_type = config.get("connection_type")
        with connection.cursor() as cursor:
            query_string = f"""
                SELECT end_time FROM core.request_queue_detail where category= 'usage_query'and  queue_id in 
                (SELECT id FROM core.request_queue where asset_id = '{asset_id}')
                and status='Completed' order by end_time desc limit 1
            """
            cursor = execute_query(connection, cursor, query_string)
            result = fetchone(cursor)
            if result:
                last_run_date = result['end_time']
                current_date = datetime.now(timezone.utc)
                time_difference = current_date - last_run_date
                days = f"{math.ceil(time_difference.total_seconds() / (24 * 3600))}"
        return days
    except Exception as e:
        log_error("Failed to get Usage Query Count for Asset", e)


def update_asset_usage_queries(asset_id: str, usage_days: str, config: dict) -> None:
    print("Executing Update Asset Usage Queries")
    try:
        connection = get_postgres_connection(config)
        connection_type = config.get("connection_type")

        try:
            with connection.cursor() as cursor:
                # Get Existing Usage Info
                existing_usage_query = f"""
                        select 
                            date(end_time) as run_date,
                            count(query_id) as total_queries,
                            round(avg(duration::numeric),2) as avg_duration,
                            min(duration) as min_duration,
                            max(duration) as max_duration,
                            round(sum(duration::numeric),2) as total_duration
                        from core.usage_detail
                        where asset_id ='{asset_id}'
                        and date(end_time) >= date(current_date - interval '{usage_days} days')
                        group by date(end_time)
                        order by date(end_time) asc
                    """
                cursor = execute_query(connection, cursor, existing_usage_query)
                existing_query_data = fetchall(cursor)

                # Delete Existing Usage Data
                query_string = f"""
                    delete from core.usage 
                    where asset_id = '{asset_id}' and date(run_date) >= date(current_date - interval '{usage_days} days')
                """
                cursor = execute_query(connection, cursor, query_string)

                if connection_type == ConnectionType.Athena.value:
                    pass

                new_query_data = []
                # Prepare Usage Stats
                for item in existing_query_data:
                    query_input = (
                        str(uuid.uuid4()),
                        str(asset_id),
                        int(item.get("total_queries", 0)),
                        str(item.get("run_date")),
                        str(item.get("run_date")),
                        float(item.get("min_duration")),
                        float(item.get("max_duration")),
                        float(item.get("avg_duration")),
                        float(item.get("total_duration")),
                    )
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals})",
                        query_input,
                    ).decode("utf-8")
                    new_query_data.append(query_param)

                # Insert New Usage Data with optimized batch size for memory management
                batch_size = min(500, len(new_query_data))  # Reduce batch size to prevent memory issues
                new_query_data = split_queries(new_query_data, batch_size)
                for batch_idx, input_values in enumerate(new_query_data):
                    try:
                        query_input = ",".join(input_values)
                        insert_query_string = f"""
                            INSERT INTO core.usage (id, asset_id, total_queries, run_date, end_date, min_duration, max_duration, avg_duration, total_duration)
                            VALUES {query_input}
                        """
                        cursor = execute_query(connection, cursor, insert_query_string)
                        
                        # Force garbage collection every 10 batches to free memory
                        if batch_idx % 10 == 0:
                            gc.collect()
                    except Exception as e:
                        log_error("Insert New Usage Queries", e)
                        raise e
        except Exception as e:
            raise e

    except Exception as e:
        log_error("Failed to Insert or Update Usage Query for Asset", e)


def update_asset_usage_detail_queries(
    asset_id: str, query_data: list, config: dict
) -> None:
    try:
        if not query_data:
            return
            
        # Process large datasets in smaller chunks to prevent memory issues
        if len(query_data) > 10000:
            log_info(f"Large dataset detected ({len(query_data)} rows), processing in smaller chunks")
            chunk_size = 5000
            for i in range(0, len(query_data), chunk_size):
                chunk = query_data[i:i+chunk_size]
                update_asset_usage_detail_queries(asset_id, chunk, config)
                gc.collect()  # Force cleanup between chunks
            return
            
        connection_type = config.get("connection_type")
        is_teradata = connection_type == ConnectionType.Teradata.value
        
        # Process data in smaller batches to reduce memory footprint
        batch_size = 1000  # Process 1000 records at a time
        
        for batch_start in range(0, len(query_data), batch_size):
            batch_end = min(batch_start + batch_size, len(query_data))
            batch_data = query_data[batch_start:batch_end]
            
            # Pre-process batch data more efficiently
            updated_query_data = []
            query_ids = []
            
            for obj in batch_data:
                query_id = str(int(obj.get("query_id", "")) if is_teradata else obj.get("query_id", ""))
                query_ids.append(query_id)
                
                new_obj = {
                    "asset_id": asset_id,
                    "query_id": query_id,
                    "query": obj.get("query_text", "")[:10000],  # Truncate long queries to prevent memory issues
                    "start_time": obj.get("start_time", None),
                    "end_time": obj.get("end_time", None),
                    "duration": obj.get("runtime", 0),
                    "status": obj.get("status", "UNKNOWN"),
                    "user_name": obj.get("user_name", ""),
                    "role_name": obj.get("role_name", ""),
                }
                
                if not is_teradata:
                    new_obj.update({
                        "warehouse_name": obj.get("warehouse_name", ""),
                        "rows_deleted": obj.get("rows_deleted", 0),
                        "rows_inserted": obj.get("rows_inserted", 0),
                        "rows_updated": obj.get("rows_updated", 0),
                        "query_type": obj.get("query_type", ""),
                    })
                
                updated_query_data.append(new_obj)
            
            # Process this batch
            _process_usage_detail_batch(asset_id, updated_query_data, query_ids, config)
            
            # Clean up memory after each batch
            del updated_query_data, query_ids, batch_data
            gc.collect()
            
            log_info(f"Processed batch {batch_start//batch_size + 1}")
            
    except Exception as e:
        log_error("Failed to Insert Usage Detail Query for Asset", e)

def _process_usage_detail_batch(asset_id: str, updated_query_data: list, query_ids: list, config: dict):
    """Process a single batch of usage detail data"""

    connection = get_postgres_connection(config)
    try:
        with connection.cursor() as cursor:
            # Use parameterized query for delete operation with smaller batches
            if query_ids:
                # Process deletes in smaller batches to avoid large IN clauses
                delete_batch_size = 500
                for i in range(0, len(query_ids), delete_batch_size):
                    batch_ids = query_ids[i:i+delete_batch_size]
                    placeholders = ",".join(["%s"] * len(batch_ids))
                    delete_existing_data_query = f"""
                        DELETE from core.usage_detail
                        WHERE asset_id = %s AND query_id IN ({placeholders})
                    """
                    cursor = execute_query(connection, cursor, delete_existing_data_query, [asset_id] + batch_ids)

            # Prepare batch insert data with memory optimization
            insert_batch_size = 200  # Smaller batch size to reduce memory usage
            
            for batch_start in range(0, len(updated_query_data), insert_batch_size):
                batch_end = min(batch_start + insert_batch_size, len(updated_query_data))
                batch_items = updated_query_data[batch_start:batch_end]
                
                new_query_data = []
                for item in batch_items:
                    query_input = (
                        str(uuid.uuid4()),
                        str(item["asset_id"]),
                        str(item["query_id"]),
                        item.get("query", ""),
                        item.get("start_time", None),
                        item.get("end_time", None),
                        item.get("duration", 0),
                        item.get("status", "UNKNOWN"),
                        item.get("user_name", ""),
                        item.get("role_name", ""),
                        item.get("warehouse_name", ""),
                        item.get("rows_deleted", 0),
                        item.get("rows_inserted", 0),
                        item.get("rows_updated", 0),
                        item.get("query_type", ""),
                    )
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(f"({input_literals})", query_input).decode("utf-8")
                    new_query_data.append(query_param)

                # Insert this batch
                if new_query_data:
                    try:
                        query_input = ",".join(new_query_data)
                        query_string = f"""
                            INSERT INTO CORE.USAGE_DETAIL (
                                ID, ASSET_ID, QUERY_ID, QUERY, START_TIME, END_TIME,
                                DURATION, STATUS, USER_NAME, ROLE_NAME, WAREHOUSE_NAME,
                                ROWS_DELETED, ROWS_INSERTED, ROWS_UPDATED, QUERY_TYPE
                            ) VALUES {query_input}
                        """
                        cursor = execute_query(connection, cursor, query_string)
                        
                        # Clean up after each insert batch
                        del new_query_data
                        gc.collect()
                        
                    except Exception as e:
                        log_error("Failed to Insert Usage Query", e)
                        raise e
    except Exception as e:
        log_error("Failed to Insert Usage Detail Query for Asset", e)


def usage_detail_parse_datetime(date_obj, connection_type: str = None):
    """
    Parses different types of datetime values (strings, floats, etc.)
    and returns a datetime object that can be directly inserted into
    a PostgreSQL TIMESTAMP column.

    :param value: The datetime value to be parsed (str, float, or int)
    :connection_type: To handle different date formats of different connectors (optional)
    :return: A datetime object or None if the input is None or invalid
    """
    if connection_type in [ConnectionType.MSSQL.value, ConnectionType.Snowflake.value]:
        if date_obj:
            if date_obj and isinstance(date_obj, str):
                date_obj = date_obj.replace("T", " ")
                return date_obj
            return date_obj.strftime("%Y-%m-%d %H:%M:%S.%f")
        else:
            return None

    if isinstance(date_obj, str):
        # Handle common string formats with and without microseconds
        try:
            # Try parsing with date and time, optionally with microseconds
            return datetime.strptime(date_obj, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            try:
                # Fallback to parsing without microseconds
                return datetime.strptime(date_obj, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                try:
                    # Handle date only
                    return datetime.strptime(date_obj, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    return None

    elif isinstance(date_obj, (float, int)):
        # If the input is a float or int, treat it as a Unix timestamp
        return datetime.fromtimestamp(date_obj)

    else:
        # If the input is None or an unsupported type, return None
        return None


def save_usage_queries_info(config: dict, **kwargs) -> None:
    clear_duplicate_task(config, USAGE_QUERY)
    update_queue_detail_status(config, ScheduleStatus.Running.value)
    update_queue_status(config, ScheduleStatus.Running.value)
    # Get Asset Query Usage Details
    source_connection = ""
    default_queries = get_queries(config)
    usage_days = get_usage_query_days(config)
    asset_id = config.get("asset_id")

    # Save Usage Queries
    usage_detail_query = get_metadata_query(
        "usage_detail_query", default_queries, config
    )

    usage_detail_queries_count = get_metadata_query(
        "usage_detail_queries_count", default_queries, config
    )
    connection_type = config.get("connection_type")
    if connection_type == ConnectionType.Athena.value:
        usage_detail_query_metadata = get_athena_queries_data(config)
        update_asset_usage_detail_queries(asset_id, usage_detail_query_metadata, config)

    if usage_detail_query:
        try:
            usage_detail_query = usage_detail_query.replace("<days>", usage_days)
            usage_detail_queries_count = usage_detail_queries_count.replace("<days>", usage_days)

            if connection_type == ConnectionType.Snowflake.value:
                # Reduce batch size for memory optimization
                batch_size = 5000  # Reduced from 10000
                total_rows_result, _ = execute_native_query(
                    config, usage_detail_queries_count, source_connection
                )
                total_count = total_rows_result.get('total_count',0)
                
                # Add memory check before processing large datasets
                
                offset = 0
                if total_count > 0:
                    batch_num = 0
                    while offset <= total_count:
                        paged_query = f"{usage_detail_query} LIMIT {batch_size} OFFSET {offset}"
                        usage_detail_query_metadata, _ = execute_native_query(
                            config, paged_query, source_connection, is_list=True
                        )
                        if usage_detail_query_metadata:
                            usage_detail_query_metadata = convert_to_lower(
                                usage_detail_query_metadata
                            )
                            update_asset_usage_detail_queries(
                                asset_id, usage_detail_query_metadata, config
                            )
                            
                            # Clean up after each batch
                            del usage_detail_query_metadata
                            gc.collect()
                            
                            batch_num += 1
                            if batch_num % 5 == 0:  # Log every 5 batches
                                log_info(f"Processed Snowflake batch {batch_num}")
                        
                        offset += batch_size

            else:
                # Add memory-aware processing for non-Snowflake connections
                log_info(("usage_detail_query", usage_detail_query))
                
                # Process with chunking for large datasets
                
                usage_detail_query_metadata, native_connection = execute_native_query(
                    config, usage_detail_query, source_connection, is_list=True
                )
                if usage_detail_query_metadata:
                    # Process in chunks if result set is large
                    if len(usage_detail_query_metadata) > 10000:
                        log_info(f"Large result set ({len(usage_detail_query_metadata)} rows), processing in chunks")
                        chunk_size = 5000
                        for i in range(0, len(usage_detail_query_metadata), chunk_size):
                            chunk = usage_detail_query_metadata[i:i+chunk_size]
                            chunk = convert_to_lower(chunk)
                            update_asset_usage_detail_queries(asset_id, chunk, config)
                            del chunk
                            gc.collect()
                    else:
                        usage_detail_query_metadata = convert_to_lower(
                            usage_detail_query_metadata
                        )
                        update_asset_usage_detail_queries(
                            asset_id, usage_detail_query_metadata, config
                        )
                    
                    # Clean up large result set
                    del usage_detail_query_metadata
                    gc.collect()
        except Exception as e:
            log_error("Failed to get Asset Usage Detail Query", e)

    print("Executed Detail Query Successfully")
    dag_info = config.get("dag_info")
    general_settings = dag_info.get("settings")
    storage_settings = general_settings.get(
        "storage") if general_settings else {}
    storage_settings = json.loads(storage_settings) if storage_settings and isinstance(
        storage_settings, str) else storage_settings
    storage_settings = storage_settings if storage_settings else {}

    retention_days = storage_settings.get("days", 60)

    # Clean usage Query History based on Retention
    delete_usage_history_by_retention(config, asset_id, retention_days)

    # Save Usage Query Stats
    update_asset_usage_queries(asset_id, retention_days, config)

    # Update Asset Usage Query Count
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        update_query = f"""
            UPDATE core.asset SET queries = tbc.count
            FROM (
                SELECT asset_id, sum(total_queries) AS count
                FROM core.usage where asset_id ='{asset_id}'
                GROUP BY asset_id
            ) AS tbc
            WHERE id = tbc.asset_id and id ='{asset_id}'
        """
        log_info(("update_query", update_query))
        execute_query(connection, cursor, update_query)

    update_queue_detail_status(config, ScheduleStatus.Completed.value)
    update_queue_status(config, ScheduleStatus.Completed.value)


def get_athena_queries_data(config: dict) -> list:
    """
    Retrieves Athena query execution history for a specific table.

    This function connects to AWS Athena using provided credentials,
    paginates through recent query executions, and filters queries
    that reference a specific table.

    Args:
        config (dict): Dictionary containing AWS credentials and configuration.

    Returns:
        list: A list of dictionaries containing query execution details, including:
              - query_text (str): The SQL query executed.
              - start_time (datetime): Timestamp of when the query was submitted.
              - runtime_ms (int): Execution time in milliseconds.
              - query_execution_id (str): Athena query execution ID.
    """

    # Retrieve AWS credentials
    athena_credentials = get_aws_credentials(config)

    # Initialize Athena client
    athena_client = boto3.client(
        "athena",
        region_name=athena_credentials.get("region_name"),
        aws_access_key_id=athena_credentials.get("aws_access_key_id"),
        aws_secret_access_key=athena_credentials.get("aws_secret_access_key"),
    )

    # Table name to filter queries
    table_name = config.get("table_technical_name")
    execution_ids = []
    athena_query_results = []

    # Paginate through query execution history with memory optimization
    paginator = athena_client.get_paginator("list_query_executions")
    response_iterator = paginator.paginate(
        PaginationConfig={"MaxItems": 100, "PageSize": 25}  # Reduced from 200/50 to save memory
    )

    # Process pages one at a time instead of collecting all IDs
    page_count = 0
    for page in response_iterator:
        page_execution_ids = page["QueryExecutionIds"]
        
        # Process this page's execution IDs immediately
        for query_execution_id in page_execution_ids:
            try:
                response = athena_client.get_query_execution(
                    QueryExecutionId=query_execution_id
                )
                query_execution = response["QueryExecution"]

                # Convert query text to lowercase for case-insensitive search
                query_text = query_execution.get("Query", "").lower()

                if table_name.lower() in query_text:
                    athena_query_results.append(
                        {
                            "query_text": query_execution["Query"][:10000],  # Truncate to prevent memory issues
                            "start_time": query_execution["Status"].get("SubmissionDateTime"),
                            "duration": query_execution["Statistics"].get(
                                "EngineExecutionTimeInMillis", 0
                            ),
                            "query_id": query_execution_id,
                        }
                    )
            except Exception as e:
                log_error(f"Error processing Athena query {query_execution_id}", e)
                continue
        
        # Clean up after processing each page
        del page_execution_ids
        page_count += 1
        
        # Force garbage collection every few pages
        if page_count % 3 == 0:
            gc.collect()
            log_info(f"Processed Athena page {page_count}")
        
        # Limit total results to prevent memory overflow
        if len(athena_query_results) > 1000:
            log_info("Athena query results limit reached (1000), stopping pagination")
            break

    return athena_query_results


def delete_usage_history_by_retention(config, asset_id, retention_days):
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            delete_query = f"""
                DELETE FROM core.usage_detail
                WHERE asset_id = '{asset_id}'
                AND date(end_time) < date(current_date - interval '{retention_days} days')
            """
            cursor = execute_query(connection, cursor, delete_query)
    except Exception as e:
        log_error("Failed to delete usage_detail history by retention", e)