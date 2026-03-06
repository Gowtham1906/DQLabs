"""
Check alerts module for data quality monitoring.

This module handles alert checking based on threshold values for various measures
including range measures with context-aware feedback enhancement and reset system.
"""

import json
import os
from dotenv import load_dotenv
import math
from uuid import uuid4
from datetime import datetime
from langchain_openai import AzureChatOpenAI


from dqlabs.utils import get_general_settings
from dqlabs.utils.notifications import create_alert_notification_log
from dqlabs.utils.drift import (
    check_manual_alert_status,
    get_alert_message,
    get_auto_alert_status,
    get_latest_metrics,
    get_metrics,
    get_total_measure_runs,
    get_default_threshold,
    get_measure_threshold,
    percent_change,
    get_previous_value,
    get_previous_percent_metrics,
    percent_model,
    calculate_deviation,
    schema_change,
    get_current_measure_dqscore,
)
from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.db_helper import (
    execute_query,
    fetchall,
    split_queries,
    fetchone
)
from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.app_constants.dq_constants import (
    ALERT_OK,
    ALERT_HIGH,  # Keep ALERT_HIGH constant
    BEHAVIORAL,
    DRIFT_DAYS,
    DRIFT_PERIOD,
    LENGTH,
    ENUM,
    STATISTICS,
)
from dqlabs.app_helper.dq_helper import get_attribute_label, format_freshness
from dqlabs.utils.behavioral import get_value_column_name
from dqlabs.utils.event_capture import save_alert_event, save_groupby_alerts_event
from dqlabs.utils.integrations.bigpanda import send_bigpanda_alerts
from dqlabs.utils.integrations.servicenow import send_servicenow_alerts, update_servicenow_incident
from dqlabs.utils.integrations.jira import update_jira_issue


# def detect_reset_for_alerts(config, measure_id):
#     """
#     Detect reset scenarios for alert checking based on action_history in core.metrics table.
#     Only checks the immediately previous run to avoid perpetual reset detection.
    
#     Args:
#         config: Configuration dictionary
#         measure_id: ID of the measure to check
        
#     Returns:
#         bool: True if reset detected in previous run, False otherwise
#     """
#     try:
#         run_id = config.get("run_id")
#         connection = get_postgres_connection(config)
        
#         with connection.cursor() as cursor:
#             # Get the immediately previous run for this measure
#             previous_run_query = f"""
#                 SELECT action_history
#                 FROM core.metrics
#                 WHERE measure_id = '{measure_id}'
#                 AND run_id != '{run_id}'
#                 AND action_history !='{{}}' 
#                 ORDER BY created_date desc 
#                 LIMIT 1"""
            
#             cursor = execute_query(connection, cursor, previous_run_query)
#             action_history_result = fetchone(cursor)
            
#             if action_history_result:
#                 action_history = action_history_result.get("action_history", {})
                
#                 # Parse JSON if it's a string
#                 if isinstance(action_history, str):
#                     try:
#                         action_history = json.loads(action_history)
#                     except json.JSONDecodeError:
#                         action_history = {}
                
#                 is_reset_run = action_history.get("is_changed", False)
                
#                 if is_reset_run:
#                     action = action_history.get("action", "unknown")
#                     log_info(f"RESET DETECTED for measure_id {measure_id} in previous run. "
#                             f"Action: {action}. Enforcing grace period.")
#                     return True
        
#         return False
        
#     except Exception as e:
#         log_error(f"Error in reset detection for alerts measure {measure_id}: {e}", e)
#         return False
def generate_llm_based_alert_message(alert_metadata_string: str, job_name: str = None, task_name: str = None):
    """
    Generate a LLM-based alert message for the given alert metadata.
    
    Args:
        alert_metadata_string: A string containing all alert metadata fields from core.metrics
        job_name: Optional job/pipeline name for pipeline alerts
        task_name: Optional task name for pipeline alerts
        
    Returns:
        str: Generated alert message from LLM
    """
    try:
        llm = AzureChatOpenAI(
            deployment_name="gpt-4.1-mini-dqlabs-dev-converse",
            model_name="gpt-4.1-nano",
            azure_endpoint="https://dqlabs-azureai-dev1.cognitiveservices.azure.com/",
            openai_api_version="2024-12-01-preview",
            openai_api_key= os.getenv("OPENAI_AZURE_API_KEY"),
            temperature=0,
        )
        
        prompt = f"""
## System Message
Below is the metadata of an alert. carefully extract meaning of the alert from it
Only answer in one line.
Dont say what it indicates
Follow the examples provided.
FOR ALERTS: Keep the alert name unchanged! Capitalise the first letter of words
FOR ASSET/TABLE NAME:
    - If "Asset name" is provided in the metadata, use that EXACTLY as provided (convert to uppercase)
    - If "Asset name" is NOT provided, extract from query or other metadata:
        - Convert to uppercase
        - Extract ONLY the table name, NOT the fully qualified name
        - For quoted table names like "DBADMIN"."SAP/HANA/TABLE01", extract the content within the last quoted section: "SAP/HANA/TABLE01" → SAP/HANA/TABLE01
        - For unquoted names like "DATABASE.SCHEMA.TABLE" or "SCHEMA.TABLE", use only the last part after the final dot: "TABLE"
        - If you see names like "CUSTOMERAI.MANAGEMENT_R" or "DQLABS_QA.CUSTOMERAI.MANAGEMENT_R", use only "MANAGEMENT_R"
        - Priority: First look for quoted table names, then fall back to dot-separated extraction
FOR JOB/PIPELINE NAME:
    - Use the COMPLETE job/pipeline name exactly as provided in the metadata
    - DO NOT truncate, abbreviate, or shorten the job name
    - If the job name is "Microsoft SQL Server (MSSQL) → MS SQL Server", use the ENTIRE name "MICROSOFT SQL SERVER (MSSQL) → MS SQL SERVER"
    - Preserve all special characters, arrows, and formatting exactly as provided
    - Convert to uppercase but keep the complete name intact
FOR TASK NAME:
    - Use the COMPLETE task name exactly as provided in the metadata
    - DO NOT truncate, abbreviate, or shorten the task name
    - Preserve the entire task name as given
    - Convert to uppercase but keep the complete name intact
FOR ATTRIBUTE NAME:
    - If "Attribute name" is provided in the metadata, use that EXACTLY as provided (convert to uppercase)
    - If "Attribute name" is NOT provided, check the attribute_name field in the metadata
    - ONLY use attribute name if it is explicitly present (either as "Attribute name" or in attribute_name field)
    - If attribute_name is empty, null, or missing, DO NOT mention attribute in the message
    - DO NOT assume or guess attribute names
    - DO NOT use placeholder words like "ATTRIBUTE" or generic terms
    - If no attribute name is available, structure the message without mentioning attribute
FOR ACTUAL VALUES (ALL MEASURE TYPES):
    - ALWAYS include the actual value in the alert message for ALL measure types (time-based and non-time-based)
    - For time-based measures (freshness, delay, latency, file_on_time_arrival):
        - Use formatted values (Formatted value, Formatted lower_threshold, Formatted upper_threshold) if provided
        - DO NOT use raw numeric values (like 67, 80, 1233) - use formatted values (like "1m 7s", "1m 20s", "20m 33s")
        - Include: actual formatted value, formatted lower_threshold, and formatted upper_threshold
    - For non-time-based measures:
        - Use the actual value from "Actual value" field if provided
        - Include the actual value in the message (e.g., "with value 150", "with count 25", "with percentage 85.5%")
        - Also include threshold information if available (lower_threshold and upper_threshold)
FOR MESSAGE:
    - If the alert is about a constraint, mention the constraint in the message.
    - ALWAYS include the actual value in the message for ALL measure types.
    - For non-pipeline alerts WITH attribute:
        "<ASSET> table has "<Alert Name>" with <ATTRIBUTE> attribute with value <ACTUAL_VALUE> violating <constraint/details>."
    - For non-pipeline alerts WITHOUT attribute:
        "<ASSET> table has "<Alert Name>" with value <ACTUAL_VALUE> violating <constraint/details>."
    - For pipeline alerts:
        "<JOB_NAME> job's <TASK_NAME> task has "<Alert Name>" with value <ACTUAL_VALUE> <constraint/details>."
    - For time-based measures: use formatted values (e.g., "with latency value 1m 36s exceeding the threshold of 1m 7s to 1m 20s")
    - For non-time-based measures: use actual numeric values (e.g., "with value 150 exceeding the threshold of 100 to 200")
    - Asset/Table/Job/Pipeline name must always appear first.
    - Attribute/Task name must come after the alert name (only if attribute exists).
    - Do NOT place the attribute directly after the asset name.
    - Avoid repeating any word in the message.

Use the query to extract more details on what the alert is about
If job name or task name is provided in the metadata, use them to build the contextful and meaningful alert message.
If Asset name or Attribute name is provided in the metadata, use them EXACTLY as provided - do not extract from query.
IMPORTANT: Always use the COMPLETE job name and task name as provided and job name should come ahead of task name as mentioned in the example below - never truncate or abbreviate them.

Example:
## Alert Metadata

SELECT T.ROW_COUNT,  COUNT(DISTINCT COL.COLUMN_NAME) AS COLUMN_COUNT,
            T.BYTES AS TABLE_SIZE, DATEDIFF(second, T.LAST_ALTERED, CURRENT_TIMESTAMP) AS FRESHNESS
            FROM ""DQLABS_QA"".INFORMATION_SCHEMA.TABLES  AS T
            JOIN ""DQLABS_QA"".INFORMATION_SCHEMA.COLUMNS AS COL ON COL.TABLE_NAME = T.TABLE_NAME AND COL.TABLE_SCHEMA = T.TABLE_SCHEMA
            WHERE UPPER(T.TABLE_NAME) = UPPER('CUSTOMERAI') AND UPPER(T.TABLE_SCHEMA) = UPPER('CUSTOMERAI')
            GROUP BY T.TABLE_NAME, T.TABLE_SCHEMA, T.ROW_COUNT, T.BYTES, T.LAST_ALTERED
        "	"c3102e3e-73d1-47e3-9876-213fdd29ff0e"	"freshness"			"asset"	"3252614"	100	0	0	0	0	0		"passed"		true		"3066744.0"	"6.0608"	"185870.0"	"4.06"	"High"	"The Freshness failed for value 37d 15h 30m because it exceeds the auto constraint of 22d 1h 10m to 29d 22h 9m"		"{{}}"	"{{""lt_percent"": -7.58, ""ut_percent"": 35.09, ""lower_threshold"": 1905018, ""upper_threshold"": 2585363}}"	"{{""one_sigma"": [1656713.38, 2833667.62], ""two_sigma"": [1607052.46, 2883328.54], ""previous_metric_values"": [{{""value"": ""2153790"", ""created_date"": ""2025-09-26T16:04:38.589164+00:00""}}, {{""value"": ""1993060.273718"", ""created_date"": ""2025-09-24T19:25:29.157582+00:00""}}, {{""value"": ""1996621.23611"", ""created_date"": ""2025-09-24T20:24:50.129291+00:00""}}, {{""value"": ""2000229.886356"", ""created_date"": ""2025-09-24T21:24:58.768712+00:00""}}, {{""value"": ""2003849.505353"", ""created_date"": ""2025-09-24T22:25:18.387568+00:00""}}, {{""value"": ""2007479.194528"", ""created_date"": ""2025-09-24T23:25:48.08323+00:00""}}, {{""value"": ""2011121.254043"", ""created_date"": ""2025-09-25T00:26:30.137546+00:00""}}, {{""value"": ""2014795.221477"", ""created_date"": ""2025-09-25T01:27:44.111727+00:00""}}, {{""value"": ""2018396.203458"", ""created_date"": ""2025-09-25T02:27:45.119649+00:00""}}, {{""value"": ""2022012.894831"", ""created_date"": ""2025-09-25T03:28:01.776301+00:00""}}, {{""value"": ""2025722.83676"", ""created_date"": ""2025-09-25T04:29:51.828488+00:00""}}, {{""value"": ""2029330.00619"", ""created_date"": ""2025-09-25T05:29:58.901948+00:00""}}, {{""value"": ""2032905.986356"", ""created_date"": ""2025-09-25T06:29:34.940347+00:00""}}, {{""value"": ""2036532.149838"", ""created_date"": ""2025-09-25T07:30:02.204731+00:00""}}, {{""value"": ""2116711"", ""created_date"": ""2025-09-26T05:46:28.819464+00:00""}}, {{""value"": ""2216933"", ""created_date"": ""2025-09-27T09:37:00.230456+00:00""}}, {{""value"": ""3066744"", ""created_date"": ""2025-10-07T05:40:22.082916+00:00""}}]}}"		"d7f0c30b-eef4-4dda-92f4-fa467d35de53"	"manual__2025-10-09T09:17:30+00:00"	false	"2025-10-09 14:48:11.621582+05:30"	"2025-10-09 14:48:14.345625+05:30"	"123eb322-4f69-4fcc-9b66-6a2f60b6dce1"		"c3300c7e-c018-4f3f-90d0-279186040c50"	"4f349659-248e-4b7c-8884-9fefa54f1066"	"b08fe8e5-1a93-4d2c-935f-02ae5c1d880b"	false	true	"
            SELECT T.ROW_COUNT,  COUNT(DISTINCT COL.COLUMN_NAME) AS COLUMN_COUNT,
            T.BYTES AS TABLE_SIZE, DATEDIFF(second, T.LAST_ALTERED, CURRENT_TIMESTAMP) AS FRESHNESS
            FROM ""DQLABS_QA"".INFORMATION_SCHEMA.TABLES  AS T
            JOIN ""DQLABS_QA"".INFORMATION_SCHEMA.COLUMNS AS COL ON COL.TABLE_NAME = T.TABLE_NAME AND COL.TABLE_SCHEMA = T.TABLE_SCHEMA
            WHERE UPPER(T.TABLE_NAME) = UPPER('CUSTOMERAI') AND UPPER(T.TABLE_SCHEMA) = UPPER('CUSTOMERAI')
            GROUP BY T.TABLE_NAME, T.TABLE_SCHEMA, T.ROW_COUNT, T.BYTES, T.LAST_ALTERED
        "	true	true	false					

								
## Answer: CUSTOMERAI table has "Freshness Alert" and is not updated for 37.6 days (25.8% over threshold)

## Alert Metadata
"Job name: SAMPLE_PIPELINE"
"Task name: PROCESS_RECORDS"
"Formatted value: 5d 5h"
"Formatted lower_threshold: 4d 15h 6m 40s"
"Formatted upper_threshold: 4d 15h 6m 40s"
"123abc45-0000-1111-2222-333444555666"
"delay"   "Delay"   "asset"
"450000.0"   100 0 0 0 0 0   "passed"   true
"{{}}"   "{{""lt_percent"": 0, ""ut_percent"": 0, ""lower_threshold"": 400000, ""upper_threshold"": 400000}}"   "{{}}"
"88888888-9999-aaaa-bbbb-cccccccccccc"
false   "2025-11-21 06:37:07.419273+00"
"2025-11-21 06:37:07.552488+00"
"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
"ffffffff-1111-2222-3333-444444444444"
"55555555-6666-7777-8888-999999999999"
"b08fe8e5-1a93-4d2c-935f-02ae5c1d880b"
false true true true false


## Answer: SAMPLE_PIPELINE job's PROCESS_RECORDS task has "Delay Alert" with delay value 5d 5h exceeding the threshold of 4d 15h 6m 40s to 4d 15h 6m 40s.

## Alert Metadata
"Job name: Microsoft SQL Server (MSSQL) → MS SQL Server"
"Task name:"
"Formatted value: 1m 36s"
"Formatted lower_threshold: 1m 7s"
"Formatted upper_threshold: 1m 20s"
"123abc45-0000-1111-2222-333444555666"
"latency"   "Latency"   "asset"
"96.0"   100 0 0 0 0 0   "passed"   true
"{{}}"   "{{""lt_percent"": 0, ""ut_percent"": 0, ""lower_threshold"": 67, ""upper_threshold"": 80}}"   "{{}}"
"88888888-9999-aaaa-bbbb-cccccccccccc"
false   "2025-11-21 06:37:07.419273+00"
"2025-11-21 06:37:07.552488+00"
"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
"ffffffff-1111-2222-3333-444444444444"
"55555555-6666-7777-8888-999999999999"
"b08fe8e5-1a93-4d2c-935f-02ae5c1d880b"
false true true true false

## Answer: MICROSOFT SQL SERVER (MSSQL) → MS SQL SERVER job has "Latency Alert" with latency value 1m 36s exceeding the threshold of 1m 7s to 1m 20s.

## Alert Metadata
"Actual value: 150"
"123abc45-0000-1111-2222-333444555666"
"row_count"   "Row Count"   "asset"
"150"   100 0 0 0 0 0   "passed"   true
"{{}}"   "{{""lt_percent"": 0, ""ut_percent"": 0, ""lower_threshold"": 100, ""upper_threshold"": 200}}"   "{{}}"
"88888888-9999-aaaa-bbbb-cccccccccccc"
false   "2025-11-21 06:37:07.419273+00"
"2025-11-21 06:37:07.552488+00"
"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
"ffffffff-1111-2222-3333-444444444444"
"55555555-6666-7777-8888-999999999999"
"b08fe8e5-1a93-4d2c-935f-02ae5c1d880b"
false true true true false

## Answer: CUSTOMERS table has "Row Count Alert" with value 150 exceeding the threshold of 100 to 200.

## Alert Metadata
"Job name: DATA_PIPELINE"
"Task name: TRANSFORM_DATA"
"Actual value: 85.5"
"123abc45-0000-1111-2222-333444555666"
"completeness"   "Completeness"   "attribute"
"85.5"   100 0 0 0 0 0   "passed"   true
"{{}}"   "{{""lt_percent"": 0, ""ut_percent"": 0, ""lower_threshold"": 90, ""upper_threshold"": 100}}"   "{{}}"
"88888888-9999-aaaa-bbbb-cccccccccccc"
false   "2025-11-21 06:37:07.419273+00"
"2025-11-21 06:37:07.552488+00"
"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
"ffffffff-1111-2222-3333-444444444444"
"55555555-6666-7777-8888-999999999999"
"b08fe8e5-1a93-4d2c-935f-02ae5c1d880b"
false true true true false

## Answer: DATA_PIPELINE job's TRANSFORM_DATA task has "Completeness Alert" with value 85.5% below the threshold of 90 to 100.

## Alert Metadata

{alert_metadata_string}

## Answer: 
"""
        
        response = llm.invoke(prompt)
        return response.content
    except Exception as e:
        log_error("Generate LLM-based alert message failed", e)
        return None


def update_alerts_with_llm_messages(config: dict, metric_ids: list):
    """
    Fetch alerts from core.metrics by specific metric IDs and update them with LLM-generated messages.
    For pipeline alerts, uses job_name and task_name from config (passed from execute_pipeline_measure).
    
    Args:
        config: Configuration dictionary containing pipeline_job_name and pipeline_task_name for pipeline alerts
        metric_ids: List of metric IDs to process
        
    Returns:
        int: Number of alerts updated
    """
    try:
        if not metric_ids or len(metric_ids) == 0:
            return 0
            
        connection = get_postgres_connection(config)
        updated_count = 0
        category = config.get("category", "").lower()
        if category == "comparison":
            return 0
        
        # Get pipeline context from config (passed from execute_pipeline_measure)
        pipeline_job_name = config.get("pipeline_job_name")
        pipeline_task_name = config.get("pipeline_task_name")
        
        with connection.cursor() as cursor:
            # Convert metric_ids list to SQL IN clause format
            # Escape single quotes in IDs and format for SQL IN clause
            escaped_ids = [str(mid).replace("'", "''") for mid in metric_ids]
            metric_ids_str = "', '".join(escaped_ids)
            
            # Fetch alerts that need LLM-based messages by specific IDs
            fetch_query = f"""
                SELECT query, message, *
                FROM core.metrics 
                WHERE id IN ('{metric_ids_str}')
                AND drift_status IN ('High','Medium','Low')
            """
            
            cursor = execute_query(connection, cursor, fetch_query)
            alerts = fetchall(cursor)

            if not alerts:
                log_info(f"No alerts found for metric_ids: {metric_ids}. Query may have been filtered out by additional conditions.")
                return 0
            
            # Build metrics_context_map from config values (passed from execute_pipeline_measure)
            # This avoids querying the database and uses the values already available
            metrics_context_map = {}
            if pipeline_job_name or pipeline_task_name:
                # For pipeline alerts, use job_name and task_name from config
                # All alerts in this batch share the same pipeline context
                for alert in alerts:
                    metric_id = alert.get("id")
                    if metric_id:
                        metrics_context_map[metric_id] = {
                            "job_name": pipeline_job_name,
                            "task_name": pipeline_task_name
                        }
            
            # For non-pipeline alerts (no job_name and no task_name), fetch asset and attribute names
            if not pipeline_job_name and not pipeline_task_name:
                # Collect unique asset_ids and attribute_ids from alerts
                asset_ids = set()
                attribute_ids = set()
                for alert in alerts:
                    asset_id = alert.get("asset_id")
                    attribute_id = alert.get("attribute_id")
                    if asset_id:
                        asset_ids.add(str(asset_id).replace("'", "''"))
                    if attribute_id:
                        attribute_ids.add(str(attribute_id).replace("'", "''"))
                
                # Batch fetch asset names
                asset_name_map = {}
                if asset_ids:
                    asset_ids_str = "', '".join(asset_ids)
                    asset_query = f"""
                        SELECT id, name
                        FROM core.asset
                        WHERE id IN ('{asset_ids_str}')
                        AND is_active = true
                        AND is_delete = false
                    """
                    cursor = execute_query(connection, cursor, asset_query)
                    asset_results = fetchall(cursor)
                    if asset_results:
                        for asset in asset_results:
                            asset_name_map[asset.get("id")] = asset.get("name")
                
                # Batch fetch attribute names
                attribute_name_map = {}
                if attribute_ids:
                    attribute_ids_str = "', '".join(attribute_ids)
                    attribute_query = f"""
                        SELECT id, name
                        FROM core.attribute
                        WHERE id IN ('{attribute_ids_str}')
                        AND is_active = true
                        AND is_delete = false
                    """
                    cursor = execute_query(connection, cursor, attribute_query)
                    attribute_results = fetchall(cursor)
                    if attribute_results:
                        for attribute in attribute_results:
                            attribute_name_map[attribute.get("id")] = attribute.get("name")
                
                # Build context map for non-pipeline alerts
                for alert in alerts:
                    metric_id = alert.get("id")
                    if metric_id:
                        asset_id = alert.get("asset_id")
                        attribute_id = alert.get("attribute_id")
                        asset_name = asset_name_map.get(asset_id) if asset_id else None
                        attribute_name = attribute_name_map.get(attribute_id) if attribute_id else None
                        
                        if asset_name or attribute_name:
                            if metric_id not in metrics_context_map:
                                metrics_context_map[metric_id] = {}
                            if asset_name:
                                metrics_context_map[metric_id]["asset_name"] = asset_name
                            if attribute_name:
                                metrics_context_map[metric_id]["attribute_name"] = attribute_name
            
            # Process all alerts and collect LLM messages for batch update
            update_values = []
            for alert in alerts:
                try:
                    # Check if this is a time-based measure that needs formatting
                    measure_name = alert.get("measure_name", "").lower() if alert.get("measure_name") else ""
                    is_time_based_measure = measure_name in ["freshness", "delay", "latency", "file_on_time_arrival", "freshness_observability"]
                    # Format time-based values and thresholds if needed
                    formatted_value = None
                    formatted_lower_threshold = None
                    formatted_upper_threshold = None
                    threshold_dict = alert.get("threshold", {})
                    if isinstance(threshold_dict, str):
                        try:
                            threshold_dict = json.loads(threshold_dict)
                        except:
                            threshold_dict = {}
                    
                    if is_time_based_measure:
                        # Format value
                        value = alert.get("value")
                        if value is not None:
                            try:
                                formatted_value = format_freshness(float(value))
                            except (ValueError, TypeError):
                                formatted_value = str(value)
                        
                        # Format thresholds
                        if threshold_dict:
                            lower_threshold = threshold_dict.get("lower_threshold")
                            upper_threshold = threshold_dict.get("upper_threshold")
                            if lower_threshold is not None:
                                try:
                                    formatted_lower_threshold = format_freshness(float(lower_threshold))
                                except (ValueError, TypeError):
                                    formatted_lower_threshold = str(lower_threshold)
                            if upper_threshold is not None:
                                try:
                                    formatted_upper_threshold = format_freshness(float(upper_threshold))
                                except (ValueError, TypeError):
                                    formatted_upper_threshold = str(upper_threshold)
                    
                    # Format alert metadata as a single string (tab-separated like in the example)
                    # Get all column values in the order they appear and join them with tabs
                    alert_metadata_parts = []
                    for key, value in alert.items():
                        if value is None:
                            alert_metadata_parts.append("")
                        elif isinstance(value, (dict, list)):
                            # Convert dict/list to JSON string
                            alert_metadata_parts.append(json.dumps(value, default=str))
                        else:
                            # Convert to string and escape tabs/newlines
                            str_value = str(value).replace("\t", " ").replace("\n", " ").replace("\r", " ")
                            alert_metadata_parts.append(str_value)
                    
                    # Add actual value and formatted values for all measures
                    value_context_parts = []
                    
                    # Get the actual value from the alert
                    actual_value = alert.get("value")
                    if actual_value is not None:
                        if is_time_based_measure:
                            # For time-based measures, add formatted value
                            if formatted_value is not None:
                                value_context_parts.append(f"Formatted value: {formatted_value}")
                            else:
                                value_context_parts.append(f"Actual value: {actual_value}")
                        else:
                            # For non-time-based measures, add actual value
                            value_context_parts.append(f"Actual value: {actual_value}")
                    
                    # Add formatted thresholds for time-based measures
                    if is_time_based_measure:
                        if formatted_lower_threshold is not None:
                            value_context_parts.append(f"Formatted lower_threshold: {formatted_lower_threshold}")
                        if formatted_upper_threshold is not None:
                            value_context_parts.append(f"Formatted upper_threshold: {formatted_upper_threshold}")
                    
                    # Prepend value context to alert metadata
                    if value_context_parts:
                        alert_metadata_parts = value_context_parts + alert_metadata_parts
                    
                    # Get context info from context map (job_name/task_name for pipeline alerts, asset_name/attribute_name for non-pipeline alerts)
                    metric_id = alert.get("id")
                    context_info = metrics_context_map.get(metric_id, {})
                    job_name_context = context_info.get("job_name")
                    task_name_context = context_info.get("task_name")
                    asset_name_context = context_info.get("asset_name")
                    attribute_name_context = context_info.get("attribute_name")
                    
                    # Determine if this is a pipeline alert (has job_name or task_name) or non-pipeline alert
                    is_pipeline_alert = bool(job_name_context or task_name_context)
                    
                    # Prepend context information to alert_metadata_string
                    # For pipeline alerts: "Job name: {job_name}"	"Task name: {task_name}"
                    # For non-pipeline alerts: "Asset name: {asset_name}"	"Attribute name: {attribute_name}"
                    context_prefix_parts = []
                    if is_pipeline_alert:
                        # Pipeline alert context
                        if job_name_context:
                            context_prefix_parts.append(f"Job name: {job_name_context}")
                        else:
                            context_prefix_parts.append("")
                        if task_name_context:
                            context_prefix_parts.append(f"Task name: {task_name_context}")
                        else:
                            context_prefix_parts.append("")
                    else:
                        # Non-pipeline alert context
                        if asset_name_context:
                            context_prefix_parts.append(f"Asset name: {asset_name_context}")
                        else:
                            context_prefix_parts.append("")
                        if attribute_name_context:
                            context_prefix_parts.append(f"Attribute name: {attribute_name_context}")
                        else:
                            context_prefix_parts.append("")
                    
                    # Build the complete alert_metadata_string with context prepended
                    alert_metadata_parts_with_context = context_prefix_parts + alert_metadata_parts
                    alert_metadata_string = "\t".join(alert_metadata_parts_with_context)
                    
                    # Generate LLM-based message
                    llm_message = generate_llm_based_alert_message(alert_metadata_string)
                    
                    if llm_message:
                        if metric_id:
                            # Prepare tuple for batch update: (metric_id, llm_message)
                            update_values.append((metric_id, llm_message))
                            log_info(f"Generated LLM message for metric_id: {metric_id}")
                    else:
                        log_info(f"LLM message generation returned None for metric_id: {alert.get('id')}")
                    
                except Exception as e:
                    log_error(f"Error processing alert for metric_id {alert.get('id')}: {e}", e)
                    continue
            
            # Perform batch update if we have any messages to update
            if update_values:
                # Build batch update query using UPDATE FROM VALUES pattern
                update_params = []
                for metric_id, llm_message in update_values:
                    query_input = (metric_id, llm_message)
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals})",
                        query_input,
                    ).decode("utf-8")
                    update_params.append(query_param)
                
                # Split into batches if needed (using split_queries helper)
                update_batches = split_queries(update_params)
                
                for batch_values in update_batches:
                    try:
                        query_input = ",".join(batch_values)
                        batch_update_query = f"""
                            UPDATE core.metrics
                            SET message = update_data.message
                            FROM (
                                VALUES {query_input}
                            ) AS update_data(id, message)
                            WHERE metrics.id::text = update_data.id::text
                        """
                        cursor = execute_query(connection, cursor, batch_update_query)
                        updated_count += len(batch_values)
                        log_info(f"Batch updated {len(batch_values)} alerts with LLM-generated messages")
                    except Exception as e:
                        log_error(f"Error in batch update for LLM messages: {e}", e)
                        # Continue with next batch even if one fails
                        continue
            
        return updated_count
        
    except Exception as e:
        log_error("Update alerts with LLM messages failed", e)
        return 0

def check_sql_measure_manual_alerts(config: dict, measure: dict) -> None:
    """
    Check and create alerts for sql_measure type with manual thresholds.
    
    This function processes sql_measure results from summary tables and creates alerts
    based on attribute_aggregation_pairs configuration when enable_manual_threshold is true.
    
    Args:
        config (dict): Configuration dictionary containing run and measure details
        measure (dict): Measure dictionary containing properties with sql_measure configuration
    """
    try:
        # Check if this is a sql_measure with manual thresholds enabled
        properties = measure.get("properties", {})
        if isinstance(properties, str):
            properties = json.loads(properties)
        
        group_measure_type = properties.get("group_measure_type", "")
        enable_manual_threshold = properties.get("enable_manual_threshold", False)
        attribute_aggregation_pairs = properties.get("attribute_aggregation_pairs", [])
        group_by_attributes = properties.get("group_by_attributes", [])
        
        if group_measure_type != "sql_measure" or not enable_manual_threshold:
            return
        
        if not attribute_aggregation_pairs or not group_by_attributes:
            log_info(f"sql_measure {measure.get('id')} has enable_manual_threshold but missing attribute_aggregation_pairs or group_by_attributes")
            return
        
        measure_id = measure.get("id")
        run_id = config.get("run_id")
        connection_id = config.get("connection_id")
        organization_id = config.get("organization_id")
        asset_id = config.get("asset_id")
        asset_id = asset_id if asset_id else None
        connection = get_postgres_connection(config)
        
        # Get the summary table name (same pattern as in grouping.py)
        table_name = measure_id.replace("-", "_")
        detail_table_name = f"{table_name}_detail"
        group_by_clause = group_by_attributes[0].get("name", "") if group_by_attributes else ""
        
        # Get summary results from the table 
        with connection.cursor() as cursor:
            # Fetch all rows from summary table in grouping schema
            fetch_detail_query = f"""
                SELECT * FROM grouping."{detail_table_name}"
            """
            cursor = execute_query(connection, cursor, fetch_detail_query)
            detail_results = fetchall(cursor)
            
            if not detail_results:
                log_info(f"No results found in detail table {detail_table_name} for measure {measure_id}")
                return
            
            # Process each row - check all attribute_aggregation_pairs and create one alert per row if any condition matches
            alert_input_values = []
            all_measure_ids = []
            
            for row in detail_results:
                # Try both lowercase and original case for group_by_clause
                group_by_value = ""
                if group_by_clause:
                    group_by_value = row.get(group_by_clause.lower(), "") or row.get(group_by_clause, "")
                
                # Collect all matching conditions for this row
                matching_conditions = []
                highest_priority_status = ALERT_OK
                priority_order = {"high": 1, "medium": 2, "low": 3, "": 4}  # Lower number = higher priority
                current_highest_priority = 4
                
                # Check all attribute_aggregation_pairs for this row
                for pair in attribute_aggregation_pairs:
                    attribute_name = pair.get("attribute", {}).get("name", "")
                    aggregation_type = pair.get("aggregation", {}).get("value", "")
                    threshold_value = pair.get("value", "")
                    threshold_value2 = pair.get("value2", "")
                    condition = pair.get("condition", "")
                    priority = pair.get("priority", "")
                    
                    if not attribute_name or not aggregation_type or not threshold_value or not condition:
                        log_error("missing attribute_name or aggregation_type or threshold_value or condition", attribute_name, aggregation_type, threshold_value, condition)
                        continue
                    
                    # Get the aggregated value from the row
                    # Format: {attribute_name}__{aggregation_type} (e.g., "PRODUCT__COUNT")
                    # Try both lowercase and original case
                    agg_key_lower = f"{attribute_name}__{aggregation_type}".lower()
                    agg_key_original = f"{attribute_name}__{aggregation_type}"
                    current_value = row.get(agg_key_lower) or row.get(agg_key_original)
                    
                    if current_value is None:
                        log_error("current_value is None", current_value)
                        continue
                    
                    try:
                        current_value = float(current_value)
                        threshold_value = float(threshold_value)
                    except (ValueError, TypeError):
                        log_error(f"Invalid numeric value for attribute {attribute_name} with aggregation {aggregation_type}", Exception("Invalid numeric value"))
                        continue
                    
                    # Check condition
                    is_alert = False
                    condition_lower = condition.lower()
                    if condition_lower == "isgreaterthan":
                        is_alert = current_value > threshold_value
                    elif condition_lower == "islessthan":
                        is_alert = current_value < threshold_value
                    elif condition_lower == "isgreaterthanorequalto":
                        is_alert = current_value >= threshold_value
                    elif condition_lower == "islessthanorequalto":
                        is_alert = current_value <= threshold_value
                    elif condition_lower == "isequalto":
                        is_alert = current_value == threshold_value
                    elif condition_lower == "isbetween":
                        threshold_value2 = float(threshold_value2)
                        is_alert = current_value >= threshold_value and current_value <= threshold_value2
                    elif condition_lower == "isnotbetween":
                        threshold_value2 = float(threshold_value2)
                        is_alert = current_value < threshold_value or current_value > threshold_value2
                    else:
                        log_error(f"Unknown condition: {condition} for sql_measure alert", Exception("Unknown condition"))
                        continue
                    
                    # If condition matches, collect it and determine highest priority
                    if is_alert:
                        condition_dict = {
                            "attribute_name": attribute_name,
                            "aggregation_type": aggregation_type,
                            "current_value": current_value,
                            "threshold_value": threshold_value,
                            "condition": condition,
                            "priority": priority
                        }
                        # Store threshold_value2 for isbetween and isnotbetween conditions
                        if condition_lower in ["isbetween", "isnotbetween"]:
                            condition_dict["threshold_value2"] = threshold_value2
                        matching_conditions.append(condition_dict)
                        
                        # Determine status based on priority
                        priority_lower = priority.lower() if priority else ""
                        priority_num = priority_order.get(priority_lower, 4)
                        
                        # Track highest priority (lowest number)
                        if priority_num < current_highest_priority:
                            current_highest_priority = priority_num
                            if priority_lower == "high":
                                highest_priority_status = "High"
                            elif priority_lower == "medium":
                                highest_priority_status = "Medium"
                            elif priority_lower == "low":
                                highest_priority_status = "Low"
                            else:
                                highest_priority_status = "OK"
                
                # Create a single alert for this row if any condition matched
                if matching_conditions:
                    # Build comprehensive message with all matching conditions
                    condition_messages = []
                    all_threshold_data = []
                    all_alert_metrics = []
                    
                    for match in matching_conditions:
                        attr_name = match["attribute_name"]
                        agg_type = match["aggregation_type"]
                        curr_val = match["current_value"]
                        thresh_val = match["threshold_value"]
                        cond = match["condition"]
                        prio = match["priority"]
                        thresh_val2 = match.get("threshold_value2")
                        
                        cond_lower = cond.lower()
                        if cond_lower == "isbetween":
                            condition_text = f"is between {thresh_val} and {thresh_val2}"
                            condition_messages.append(f"{attr_name} {agg_type} is {curr_val}, which {condition_text}")
                        elif cond_lower == "isnotbetween":
                            condition_text = f"is not between {thresh_val} and {thresh_val2}"
                            condition_messages.append(f"{attr_name} {agg_type} is {curr_val}, which {condition_text}")
                        else:
                            condition_text = cond.replace("is", "").replace("Than", " than").replace("OrEqualTo", " or equal to")
                            condition_messages.append(f"{attr_name} {agg_type} is {curr_val}, which {condition_text} {thresh_val}")
                        
                        threshold_data_item = {
                            "threshold_value": thresh_val,
                            "condition": cond,
                            "priority": prio,
                            "attribute": attr_name,
                            "aggregation": agg_type
                        }
                        if thresh_val2 is not None:
                            threshold_data_item["threshold_value2"] = thresh_val2
                        all_threshold_data.append(threshold_data_item)
                        
                        alert_metrics_item = {
                            "current_value": curr_val,
                            "threshold_value": thresh_val,
                            "measure_name": f"{attr_name} {agg_type}",
                            "group_by_value": group_by_value
                        }
                        if thresh_val2 is not None:
                            alert_metrics_item["threshold_value2"] = thresh_val2
                        all_alert_metrics.append(alert_metrics_item)
                    
                    # Create combined message
                    message = f"Group '{group_by_value}' has violations: " + "; ".join(condition_messages)
                    
                    # Use first matching condition's attribute/aggregation for measure_name and behavioral_key
                    first_match = matching_conditions[0]
                    measure_name = f"{first_match['attribute_name']} {first_match['aggregation_type']}"
                    groupby_key = f"{group_by_value}__{measure_id}" if group_by_value else f"{measure_id}"
                    
                    # Prepare threshold data with all matching conditions
                    threshold_data = {
                        "matching_conditions": all_threshold_data,
                        "total_violations": len(matching_conditions)
                    }
                    
                    # Prepare alert metrics with all matching conditions
                    alert_metrics = {
                        "matching_conditions": all_alert_metrics,
                        "group_by_value": group_by_value,
                        "total_violations": len(matching_conditions)
                    }
                    
                    # Prepare alert percentage (use first match for calculation)
                    first_current = matching_conditions[0]["current_value"]
                    first_threshold = matching_conditions[0]["threshold_value"]
                    alert_percentage = {
                        "current": first_current,
                        "threshold": first_threshold,
                        "percent_change": ((first_current - first_threshold) / first_threshold * 100) if first_threshold != 0 else 0
                    }
                    
                    # Create query input - one alert per row
                    query_input = (
                        str(uuid4()),
                        connection_id,
                        asset_id,
                        None,
                        measure_id,
                        measure_name,
                        groupby_key,
                        run_id,
                        False,  # is_auto (manual threshold)
                        str(first_current),  
                        "0",  
                        "0",  
                        "0", 
                        "NA",
                        message,
                        highest_priority_status,
                        "passed",
                        json.dumps(threshold_data, default=str),
                        json.dumps(alert_percentage, default=str),
                        json.dumps(alert_metrics, default=str),
                    )
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals},CURRENT_TIMESTAMP)",
                        query_input,
                    ).decode("utf-8")
                    
                    alert_input_values.append(query_param)
            
            # Insert alerts into groupby_metrics table
            if alert_input_values:
                drift_alerts_input = split_queries(alert_input_values)
                for input_values in drift_alerts_input:
                    try:
                        query_input = ",".join(input_values)
                        attribute_insert_query = f"""
                            insert into core.groupby_metrics (id,connection_id, asset_id, attribute_id,
                            measure_id, measure_name, groupby_key, run_id, is_auto, value, previous_value,
                            percent_change, value_change, deviation, message, drift_status, status, threshold, alert_percentage, alert_metrics,
                            created_date)
                            values {query_input}
                            RETURNING id
                        """
                        cursor = execute_query(connection, cursor, attribute_insert_query)
                        
                        # Get inserted alert IDs
                        drift_alerts_id = fetchall(cursor)
                        if drift_alerts_id and len(drift_alerts_id) > 0:
                            drift_alerts_id = [id.get("id") for id in drift_alerts_id]
                            
                            # Update alerts with LLM-based messages
                            try:
                                updated_count = update_alerts_with_llm_messages(config, drift_alerts_id)
                                if updated_count > 0:
                                    log_info(f"Updated {updated_count} sql_measure alerts with LLM-generated messages")
                            except Exception as e:
                                log_error("Failed to update sql_measure alerts with LLM messages", e)
                            
                            # Save alert event
                            try:
                                save_groupby_alerts_event(config, drift_alerts_id, {"type": "new_alert"})
                            except Exception as e:
                                log_error("Error sending sql_measure alert event: ", e)
                            
                            all_measure_ids.append(measure_id)
                            
                    except Exception as e:
                        log_error(f"check sql_measure alerts: inserting new alerts for measure {measure_id}", e)
                        raise e
                
                # Send integration alerts
                send_bigpanda_alerts(config, drift_alerts_input)
                send_servicenow_alerts(config, drift_alerts_input)
                
                log_info(f"Created {len(alert_input_values)} sql_measure manual alerts for measure {measure_id}")
            
    except Exception as e:
        log_error(f"Error checking sql_measure manual alerts for measure {measure.get('id', 'unknown')}: {e}", e)


def detect_reset_for_alerts(config, measure_id):
    """
    Detect reset scenarios by checking if a reset occurred since the grace period started.
    Only detects each reset once to allow grace period to properly expire.
    
    Args:
        config: Configuration dictionary
        measure_id: ID of the measure to check
        
    Returns:
        bool: True if currently in grace period due to recent reset, False otherwise
    """
    try:
        run_id = config.get("run_id")
        connection = get_postgres_connection(config)
        
        # Get drift_period from settings for grace period calculation
        general_settings = get_general_settings(config)
        drift_settings = general_settings.get("anomaly", {}) if general_settings else {}
        drift_period = int(drift_settings.get("minimum", DRIFT_PERIOD)) if drift_settings else DRIFT_PERIOD
        drift_period = max(drift_period, 1)  # Ensure minimum value
        
        with connection.cursor() as cursor:
            # Step 1: Find the most recent reset event timestamp
            last_reset_query = f"""
                SELECT MAX(created_date) as last_reset
                FROM core.metrics
                WHERE measure_id = '{measure_id}'
                  AND action_history->>'is_changed' = 'true';
            """
            cursor = execute_query(connection, cursor, last_reset_query)
            reset_result = fetchone(cursor)
            last_reset_timestamp = reset_result.get("last_reset") if reset_result else None
            
            if not last_reset_timestamp:
                # No reset ever recorded for this measure
                return False
            
            # Step 2: Count completed runs since the reset timestamp
            runs_since_reset_query = f"""
                SELECT COUNT(*) as run_count
                FROM core.metrics
                WHERE measure_id = '{measure_id}'
                  AND run_id != '{run_id}'
                  AND created_date > '{last_reset_timestamp}';
            """
            cursor = execute_query(connection, cursor, runs_since_reset_query)
            count_result = fetchone(cursor)
            runs_since_reset = count_result.get("run_count", 0) if count_result else 0
            
            # Step 3: Grace period logic
            # If we have fewer runs than drift_period since reset, we're still in grace period
            is_in_grace_period = runs_since_reset < drift_period
            
            if is_in_grace_period:
                return True
            else:
                return False
                
    except Exception as e:
        log_error(f"Error in reset detection for alerts measure {measure_id}: {e}", e)
        # On error, assume no reset to maintain normal behavior
        return False

def update_alert_count(config: dict) -> None:
    """
    Update the alert count in asset and attribute level in metadata tables.
    
    Args:
        config (dict): Configuration dictionary containing asset and connection details
    """
    asset_id = config.get("asset_id")
    connection = get_postgres_connection(config)
    asset_group = config.get("asset", {}).get("group")
    
    with connection.cursor() as cursor:
        try:
           
            if asset_group == "pipeline":
                query_string = f"""
                    SELECT 
                        metrics.asset_id,
                        metrics.attribute_id,
                        COUNT(*) FILTER (
                            WHERE LOWER(metrics.drift_status) IN ('high', 'medium', 'low')
                        ) AS alert_count
                    FROM core.metrics
                    WHERE metrics.asset_id = '{asset_id}'
                    AND (
                            metrics.measure_id IS NULL
                            OR EXISTS (
                                SELECT 1
                                FROM core.measure
                                WHERE measure.id = metrics.measure_id
                                AND measure.last_run_id = metrics.run_id
                            )
                        )
                    GROUP BY metrics.asset_id, metrics.attribute_id;
                """
            else:
                query_string = f"""
                    select 
                        metrics.asset_id,
                        metrics.attribute_id, 
                        count(case when lower(metrics.drift_status) in ('high', 'medium', 'low') then 1 end) as alert_count
                    from core.metrics
                    join core.measure on measure.id = metrics.measure_id and measure.last_run_id  = metrics.run_id
                    where metrics.asset_id = '{asset_id}'
                    group by metrics.asset_id, metrics.attribute_id
                """

            cursor = execute_query(connection, cursor, query_string)
            alerts_count = fetchall(cursor)
            
            query_string = f"""
                update core.attribute set alerts=0
                where asset_id='{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)

            attribute_counts = []
            asset_alert_count = 0
            
            for alert_count in alerts_count:
                asset_id = alert_count.get("asset_id")
                attribute_id = alert_count.get("attribute_id")
                count = alert_count.get("alert_count")
                count = count if count else 0

                # Get Asset level count
                if not attribute_id:
                    asset_alert_count = asset_alert_count + count
                    continue

                asset_alert_count = asset_alert_count + count
                if attribute_id:
                    query_input = (attribute_id, count)
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals})",
                        query_input,
                    ).decode("utf-8")
                    attribute_counts.append(query_param)

            if attribute_counts:
                attribute_count = ", ".join(attribute_counts)
                query_string = f"""
                    update core.attribute as meta set alerts=temp.alerts
                    from (values {attribute_count}) as temp (id, alerts)
                    where meta.id::Text=temp.id
                    and meta.asset_id='{asset_id}'
                """
                cursor = execute_query(connection, cursor, query_string)

            query_string = f"""
                update core.asset set alerts={asset_alert_count}
                where id='{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)

        except Exception as e:
            log_error("check alerts : update alert count failed - ", e)


def update_last_run_alerts(config: dict) -> None:
    """
    Update the last run alerts count in measure table.
    
    Args:
        config (dict): Configuration dictionary containing measure and connection details
    """
    measure_id = config.get("measure_id")
    asset_id = config.get("asset_id")
    connection_id = config.get("connection_id")
    measure = config.get("measure", {})
    category = measure.get("category")
    group_type = measure.get("properties", {}).get("group_measure_type")

    if category == "grouping" and group_type == "sql_measure":
        metrics_table = "core.groupby_metrics"
    else:
        metrics_table = "core.metrics"

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        try:
            # Update last run alerts count on measures table
            condition_query = ""
            if measure_id:
                condition_query = f"where measure.id='{measure_id}'"
            elif asset_id:
                condition_query = f"where measure.asset_id='{asset_id}'"
            elif connection_id:
                condition_query = f"where measure.connection_id='{connection_id}'"
                
            update_query_string = f"""
                update core.measure
                set alerts = (
                select count(*) from {metrics_table} as drift
                where drift.measure_id = measure.id
                and drift.run_id = measure.last_run_id
                and lower(drift.drift_status) in ('high','medium','low'))
                {condition_query}
            """
            cursor = execute_query(connection, cursor, update_query_string)

        except Exception as e:
            log_error("check alerts : update last run alerts count failed - ", e)


def update_last_runs_for_measure(config: dict, last_runs_measures: dict) -> None:
    """
    Update last runs information for measures.
    
    Args:
        config (dict): Configuration dictionary
        last_runs_measures (dict): Dictionary containing last run information for measures
    """
    connection = get_postgres_connection(config)
    last_runs = []
    
    if not last_runs_measures:
        return
        
    with connection.cursor() as cursor:
        last_run_values = []
        for measure_id, value in last_runs_measures.items():
            if not value:
                continue
                
            status, run_id = value
            query_string = f"""
                    select last_runs from core.measure where id = '{measure_id}'
                """
            cursor = execute_query(connection, cursor, query_string)
            last_runs = fetchone(cursor)
            last_runs = last_runs.get("last_runs", []) if last_runs else []
            last_runs = (
                json.loads(last_runs) if isinstance(last_runs, str) else last_runs
            )
            last_runs = last_runs if last_runs else []
            last_runs = list(filter(lambda x: x.get("run_id") != run_id, last_runs))
            last_runs.insert(
                0, {"run_id": run_id, "status": status, "last_run_date": datetime.now()}
            )

            if len(last_runs) > 7:
                last_runs = last_runs[:7]

            query_input = (
                measure_id,
                json.dumps(last_runs, default=str),
            )
            input_literals = ", ".join(["%s"] * len(query_input))
            query_param = cursor.mogrify(
                f"({input_literals})",
                query_input,
            ).decode("utf-8")

            last_run_values.append(query_param)

        last_run_values = split_queries(last_run_values)
        for input_values in last_run_values:
            try:
                query_input = ",".join(input_values)
                last_run_insert_query = f"""
                    update core.measure as measure set last_runs = to_json(c.last_runs::text)
                    from (values {query_input}) as c(measure_id, last_runs)
                    where c.measure_id = measure.id::text;
                """
                cursor = execute_query(connection, cursor, last_run_insert_query)
            except Exception as e:
                log_error("check alerts: update last run", e)
                raise e


def check_alerts(config: dict, **kwargs) -> list:
    """
    Check for alerts based on threshold values.
    
    This function processes metrics and determines alert status based on configured
    thresholds, with enhanced handling for ALL measure types that may have enhanced
    thresholds from context-aware feedback (not just range measures) and reset system.
    
    Args:
        config (dict): Configuration dictionary containing run and measure details
        **kwargs: Additional keyword arguments
        
    Returns:
        list: List of processed alerts
    """
    # Pull the latest metrics
    job_name = config.get("pipeline_job_name")
    task_name = config.get("pipeline_task_name") 
    run_id = config.get("run_id")
    measure = config.get("measure")
    measure_drift_threshold = measure.get("drift_threshold") if measure else {}
    measure_drift_threshold = measure_drift_threshold if measure_drift_threshold else {}
    measure_drift_threshold = (
        json.loads(measure_drift_threshold)
        if isinstance(measure_drift_threshold, str)
        else measure_drift_threshold
    )
    asset_id = config.get("asset_id")
    job_type = config.get("job_type")
    is_behavioral = (job_type == BEHAVIORAL) or (
        measure and measure.get("category") == BEHAVIORAL
    )
    config.update({"is_behavioral": is_behavioral})
    general_settings = get_general_settings(config)
    general_settings = general_settings if general_settings else {}
    last_runs_measures = {}

    # Getting the minimum number of days the drift should be calculated
    drift_settings = general_settings.get("anomaly") if general_settings else {}
    drift_settings = drift_settings if drift_settings else {}
    drift_settings = (
        json.loads(drift_settings, default=str)
        if isinstance(drift_settings, str)
        else drift_settings
    )
    is_enabled = drift_settings.get("is_active")
    if not is_enabled:
        return
        
    drift_days = drift_settings.get("maximum", DRIFT_DAYS)
    drift_days = int(drift_days) if drift_days else DRIFT_DAYS
    drift_days = drift_days if drift_days > 0 else DRIFT_DAYS
    config.update({"window": drift_days})
    
    # Getting the anomaly period for minimum no of runs before alert is shown
    drift_period = drift_settings.get("minimum", DRIFT_PERIOD)
    drift_period = int(drift_period) if drift_period else DRIFT_PERIOD
    drift_period = drift_period if drift_period > 0 else DRIFT_PERIOD
    if job_type != STATISTICS:
        total_runs = get_total_measure_runs(config, is_behavioral)
    latest_metrics = get_latest_metrics(config, run_id, is_behavioral)
    measure_thresholds = get_measure_threshold(config)
    previous_value_metrics = get_metrics(config, True, is_behavioral)
    first_observed_assets = config.get("first_observed_assets")
    first_observed_assets = first_observed_assets if first_observed_assets else []
    first_observed_assets = (
        list(set(first_observed_assets)) if first_observed_assets else []
    )

    # Depth change logic
    asset = config.get("asset", {})
    asset = asset if asset else {}
    incremental_config = config.get("incremental_config", {})
    current_depth = None
    previous_depth = None
    
    if incremental_config and is_behavioral:
        depth = incremental_config.get("depth", {})
        depth = depth if depth else {}
        depth_value = depth.get("value", 0)
        current_depth = int(depth_value) if depth_value else 0

    if not latest_metrics:
        return

    connection = get_postgres_connection(config)
    
    # Check for sql_measure manual alerts if applicable
    if measure:
        properties = measure.get("properties", {})
        if isinstance(properties, str):
            properties = json.loads(properties)
        group_measure_type = properties.get("group_measure_type", "")
        enable_manual_threshold = properties.get("enable_manual_threshold", False)
        
        if group_measure_type == "sql_measure" and enable_manual_threshold:
            try:
                check_sql_measure_manual_alerts(config, measure)
            except Exception as e:
                log_error(f"Error checking sql_measure manual alerts: {e}", e)
    attribute_alert_count = {}
    processed_assets = []
    asset_query = f"and alert.asset_id='{asset_id}'" if asset_id else ""
    all_measure_ids = []  # Initialize to collect all measure IDs
    
    with connection.cursor() as cursor:
        # Delete the alerts which is not linked with the issues for the same run
        query_string = f"""
            select alert.measure_id, alert.behavioral_key from core.metrics as alert
            join core.issues as issue on issue.metrics_id=alert.id
            where alert.run_id='{run_id}' and issue.metrics_id is not null {asset_query}
        """
        cursor = execute_query(connection, cursor, query_string)
        measure_with_issues = fetchall(cursor)
        measure_issues = [issue.get("measure_id") for issue in measure_with_issues]
        
        if is_behavioral:
            measure_issues = [
                issue.get("behavioral_key") for issue in measure_with_issues
            ]
        measure_issues = [measure_id for measure_id in measure_issues if measure_id]

        for metrics in latest_metrics:
            alert_input_values = []
            measure_ids = list(
                set(
                    [
                        metric.get("measure_id")
                        for metric in metrics
                        if metric.get("measure_id")
                    ]
                )
            )
            if is_behavioral:
                measure_ids = list(
                    set([metric.get("key") for metric in metrics if metric.get("key")])
                )
                
            measure_ids = [
                f"""'{str(measure_id)}'"""
                for measure_id in measure_ids
                if measure_id not in measure_issues
            ]
            
            if is_behavioral:
                measure_id = measure.get("id")
                
            asset_query = f"and asset_id='{asset_id}'" if asset_id else ""

            value_column_name = ""
            default_threshold = None
            
            if is_behavioral:
                value_column_name = get_value_column_name(measure)
                default_threshold = get_default_threshold(metrics, value_column_name)

            for metric in metrics:
                threshold_constraints = {}
                measure_name = metric.get("measure_name")
                if not asset_id and metric.get("asset_id"):
                    asset_id = metric.get("asset_id")
                    processed_assets.append(asset_id)

                slice_key = metric.get("slice_key")
                slice_key = slice_key if slice_key else None
                existing_threshold = metric.get("threshold")
                existing_threshold = (
                    json.loads(existing_threshold)
                    if existing_threshold and isinstance(existing_threshold, str)
                    else existing_threshold
                )
                existing_threshold = existing_threshold if existing_threshold else {}
                measure_name = metric.get("key") if is_behavioral else measure_name

                measure_category = metric.get("category")
                metric_key = (
                    metric.get("key") if is_behavioral else metric.get("measure_name")
                )
                
                if not measure_name:
                    continue
                    
                behavioral_key = measure_name if is_behavioral else None
                measure_label = get_attribute_label(metric_key, False)
                measure_id = metric.get("measure_id")
                
                drift_config = next(
                    (
                        measure_threshold
                        for measure_threshold in measure_thresholds
                        if measure_threshold.get("id") == measure_id
                    ),
                    None,
                )
                drift_config = drift_config if drift_config else {}
                key = f"{str(measure_id).lower()}___{str(measure_label).lower()}"
                if job_type == STATISTICS:
                    total_stat_runs = get_total_measure_runs(config, measure_id)
                    total_measure_runs = total_stat_runs.get(key)
                else:
                    total_measure_runs = total_runs.get(key)

                total_measure_runs = total_measure_runs if total_measure_runs else 0
                
                # --- START OF NEW RESET LOGIC ---
                # RESET DETECTION: Check if this measure has been reset
                is_reset_detected = detect_reset_for_alerts(config, measure_id)

                if is_reset_detected:
                    
                    # GRACE PERIOD OVERRIDE: Reset run count to 1 to ensure grace period
                    # is re-applied and alerts are not generated prematurely
                    total_measure_runs = 1
                # --- END OF NEW RESET LOGIC ---
                
                # Existing alert check logic continues unchanged
                is_alerts_check_enabled = total_measure_runs > drift_period

                run_id = metric.get("run_id")
                attribute_id = metric.get("attribute_id")
                current_value = 0
                
                try:
                    current_value = float(metric.get("value", 0))
                except:
                    current_value = 0
                    
                total_count = int(metric.get("total_count", 0))
                
                if is_behavioral:
                    current_value = metric.get(value_column_name)
                    current_value = float(current_value) if current_value else 0

                    asset = config.get("asset", {})
                    total_rows = asset.get("row_count")
                    total_count = total_rows if total_rows else 0

                current_value = round(current_value, 2) if current_value else 0

                # Get thresholds from drift configuration
                threshold = drift_config.get("threshold", {})
                
                if is_behavioral:
                    threshold = measure_drift_threshold.get(behavioral_key, {})
                    threshold = threshold if threshold else {}
                    if not threshold and default_threshold:
                        threshold = default_threshold.get(behavioral_key, {})
                        threshold = threshold if threshold else {}

                if (
                    measure_category
                    and threshold
                    and measure_category.lower()
                    in ("length", "enum", "pattern", "long_pattern", "short_pattern", "parameter")
                ):
                    measure_name = measure_name.replace("'", "")
                    threshold = threshold.get(measure_name, {})
                    threshold = threshold if threshold else {}

                # Enhanced handling for ALL measure types with enhanced thresholds
                measure_name_lower = measure_name.lower() if measure_name else ""
                is_range_measure = (
                    "value_range" in measure_name_lower or 
                    "length_range" in measure_name_lower or
                    (measure_category and measure_category.lower() == "range")
                )
                
                if is_range_measure:
                    # EXISTING: Range measures logic (FIXED)                    
                    # Get the is_auto flag from the metric
                    is_auto = metric.get("is_auto", True)
                    if threshold is None:
                        threshold = {}
                    
                    if is_auto and not is_reset_detected:
                        # FIXED: Check if ANY existing thresholds are available (enhanced or baseline)
                        existing_lower = existing_threshold.get("lower_threshold", 0)
                        existing_upper = existing_threshold.get("upper_threshold", 0)
                        
                        if existing_lower != 0 or existing_upper != 0:
                            # Use existing thresholds (both enhanced and baseline)
                            lower_threshold = float(existing_lower)
                            upper_threshold = float(existing_upper)
                        else:
                            # FIXED: Use same source as profile.py - drift_threshold table with fallback chain
                            # Step 1: Try drift_threshold table first (same as profile.py)
                            drift_threshold_query = f"""
                                SELECT lower_threshold, upper_threshold
                                FROM core.drift_threshold
                                WHERE measure_id = '{measure_id}'
                                AND attribute_id = '{attribute_id}'
                                ORDER BY created_date DESC
                                LIMIT 1
                            """
                            cursor = execute_query(connection, cursor, drift_threshold_query)
                            drift_result = fetchone(cursor)

                            if drift_result and (drift_result.get('lower_threshold') is not None and drift_result.get('upper_threshold') is not None):
                                # Found in drift_threshold table
                                lower_threshold = float(drift_result.get('lower_threshold', 0))
                                upper_threshold = float(drift_result.get('upper_threshold', 0))
                            else:
                                # Step 2: First-run fallback - use core.attribute table (same as profile.py                                
                                if "length_range" in measure_name_lower:
                                    attr_query = f"""
                                        SELECT min_length, max_length 
                                        FROM core.attribute 
                                        WHERE id = '{attribute_id}'
                                    """
                                    cursor = execute_query(connection, cursor, attr_query)
                                    attr_result = fetchone(cursor)
                                    
                                    if attr_result and attr_result.get("min_length") is not None and attr_result.get("max_length") is not None:
                                        lower_threshold = float(attr_result.get("min_length", 0))
                                        upper_threshold = float(attr_result.get("max_length", 0))
                                    else:
                                        # Step 3: Final fallback to configuration thresholds
                                        lower_threshold = float(threshold.get("lower_threshold", 0))
                                        upper_threshold = float(threshold.get("upper_threshold", 0))
                                               
                                elif "value_range" in measure_name_lower:
                                    attr_query = f"""
                                        SELECT min_value, max_value 
                                        FROM core.attribute 
                                        WHERE id = '{attribute_id}'
                                    """
                                    cursor = execute_query(connection, cursor, attr_query)
                                    attr_result = fetchone(cursor)
                                    
                                    if attr_result and attr_result.get("min_value") is not None and attr_result.get("max_value") is not None:
                                        lower_threshold = float(attr_result.get("min_value", 0))
                                        upper_threshold = float(attr_result.get("max_value", 0))
        
                                    else:
                                        lower_threshold = float(threshold.get("lower_threshold", 0))
                                        upper_threshold = float(threshold.get("upper_threshold", 0))
                                        
                                else:
                                    # Unknown range type - fallback to configuration
                                    lower_threshold = float(threshold.get("lower_threshold", 0))
                                    upper_threshold = float(threshold.get("upper_threshold", 0))
                                    
                    else:
                        if is_reset_detected:
                            
                            # FIXED: Use same drift_threshold-first logic for reset scenarios
                            drift_threshold_query = f"""
                                SELECT lower_threshold, upper_threshold
                                FROM core.drift_threshold
                                WHERE measure_id = '{measure_id}'
                                AND attribute_id = '{attribute_id}'
                                ORDER BY created_date DESC
                                LIMIT 1
                            """
                            cursor = execute_query(connection, cursor, drift_threshold_query)
                            drift_result = fetchone(cursor)

                            if drift_result and (drift_result.get('lower_threshold') is not None and drift_result.get('upper_threshold') is not None):
                                # Found in drift_threshold table
                                lower_threshold = float(drift_result.get('lower_threshold', 0))
                                upper_threshold = float(drift_result.get('upper_threshold', 0))
                                
                            else:
                                # Fallback to attribute table for reset scenarios
                                if "length_range" in measure_name_lower:
                                    attr_query = f"""
                                        SELECT min_length, max_length 
                                        FROM core.attribute 
                                        WHERE id = '{attribute_id}'
                                    """
                                    cursor = execute_query(connection, cursor, attr_query)
                                    attr_result = fetchone(cursor)
                                
                                    if attr_result and attr_result.get("min_length") is not None and attr_result.get("max_length") is not None:
                                        lower_threshold = float(attr_result.get("min_length", 0)) if attr_result.get("min_length") else 0
                                        upper_threshold = float(attr_result.get("max_length", 0)) if attr_result.get("max_length") else 0
                                        
                                    else:
                                        # Fallback to configuration if attribute not found
                                        lower_threshold = float(threshold.get("lower_threshold", 0)) if threshold.get("lower_threshold") else 0
                                        upper_threshold = float(threshold.get("upper_threshold", 0)) if threshold.get("upper_threshold") else 0
                                        
                                    
                                elif "value_range" in measure_name_lower:
                                    attr_query = f"""
                                        SELECT min_value, max_value 
                                        FROM core.attribute 
                                        WHERE id = '{attribute_id}'
                                    """
                                    cursor = execute_query(connection, cursor, attr_query)
                                    attr_result = fetchone(cursor)
                                    
                                    if attr_result and attr_result.get("min_value") is not None and attr_result.get("max_value") is not None:
                                        lower_threshold = float(attr_result.get("min_value", 0))
                                        upper_threshold = float(attr_result.get("max_value", 0))
                                        
                                    else:
                                        # Fallback to configuration if attribute not found
                                        lower_threshold = float(threshold.get("lower_threshold", 0))
                                        upper_threshold = float(threshold.get("upper_threshold", 0))
                                        
                                else:
                                    # Unknown range type - fallback to configuration
                                    lower_threshold = float(threshold.get("lower_threshold", 0))
                                    upper_threshold = float(threshold.get("upper_threshold", 0))
                                    
                        else:
                            # Manual mode - FIXED: Use same drift_threshold-first logic
                            drift_threshold_query = f"""
                                SELECT lower_threshold, upper_threshold
                                FROM core.drift_threshold
                                WHERE measure_id = '{measure_id}'
                                AND attribute_id = '{attribute_id}'
                                ORDER BY created_date DESC
                                LIMIT 1
                            """
                            cursor = execute_query(connection, cursor, drift_threshold_query)
                            drift_result = fetchone(cursor)

                            if drift_result and (drift_result.get('lower_threshold') is not None and drift_result.get('upper_threshold') is not None):
                                # Found in drift_threshold table
                                lower_threshold = float(drift_result.get('lower_threshold', 0))
                                upper_threshold = float(drift_result.get('upper_threshold', 0))
                                
                            else:
                                # Fallback to attribute table for manual mode
                                if "length_range" in measure_name_lower:
                                    attr_query = f"""
                                        SELECT min_length, max_length 
                                        FROM core.attribute 
                                        WHERE id = '{attribute_id}'
                                    """
                                    cursor = execute_query(connection, cursor, attr_query)
                                    attr_result = fetchone(cursor)
                                    
                                    if attr_result and attr_result.get("min_length") is not None and attr_result.get("max_length") is not None:
                                        lower_threshold = float(attr_result.get("min_length", 0))
                                        upper_threshold = float(attr_result.get("max_length", 0))
                                        
                                    else:
                                        # Fallback to configuration if attribute not found
                                        lower_threshold = float(threshold.get("lower_threshold", 0))
                                        upper_threshold = float(threshold.get("upper_threshold", 0))
                                        
                                        
                                elif "value_range" in measure_name_lower:
                                    attr_query = f"""
                                        SELECT min_value, max_value 
                                        FROM core.attribute 
                                        WHERE id = '{attribute_id}'
                                    """
                                    cursor = execute_query(connection, cursor, attr_query)
                                    attr_result = fetchone(cursor)
                                    
                                    if attr_result and attr_result.get("min_value") is not None and attr_result.get("max_value") is not None:
                                        lower_threshold = float(attr_result.get("min_value", 0))
                                        upper_threshold = float(attr_result.get("max_value", 0))
                                        
                                    else:
                                        # Fallback to configuration if attribute not found
                                        lower_threshold = float(threshold.get("lower_threshold", 0))
                                        upper_threshold = float(threshold.get("upper_threshold", 0))
                                        
                                else:
                                    # Unknown range type - fallback to configuration
                                    lower_threshold = float(threshold.get("lower_threshold", 0))
                                    upper_threshold = float(threshold.get("upper_threshold", 0))
                                    
                    
                    # Round thresholds
                    lower_threshold = int(math.floor(lower_threshold))
                    upper_threshold = int(math.ceil(upper_threshold))
                    
                    # Only check for alerts if we have enough runs
                    if is_alerts_check_enabled:
                        # For range measures, create a high alert if value > 0
                        if current_value > 0:
                            status = ALERT_HIGH
                            # Create more meaningful message that explains the number of violations
                            if "value_range" in measure_name_lower:
                                message = f"Value Range check detected {int(current_value)} values outside the acceptable range ({lower_threshold} to {upper_threshold})"
                            else:
                                message = f"Length Range check detected {int(current_value)} values with lengths outside the acceptable range ({lower_threshold} to {upper_threshold})"
                        else:
                            status = ALERT_OK
                            message = ""
                    else:
                        # Not enough runs yet, set status to None to represent "NA"
                        status = None
                        message = ""
                    
                    # Set alert metrics for visualization - use actual threshold values
                    alert_metrics = {
                        "current_value": current_value,
                        "threshold_value": 0,  # Use 0 as the violation threshold
                        "lower_threshold": lower_threshold,  # Store actual lower threshold
                        "upper_threshold": upper_threshold,  # Store actual upper threshold
                        "measure_name": measure_name
                    }
                    
                    # Set alert percentage info
                    alert_percentage = {
                        "current": current_value,
                        "threshold": 0,
                        "percent_change": 100 if current_value > 0 else 0
                    }
                    
                    # Make sure all required variables are defined
                    standard_deviation = 0
                    value_change = 0
                    __percent_change = 0
                    previous_value = 0
                    
                else:
                    # ENHANCED: Non-range measures - now with enhanced threshold support for ALL measure types
                    
                    # Get the is_auto flag from the metric
                    is_auto = metric.get("is_auto", True)
                    try:
                        if is_behavioral and isinstance(current_value, float):
                            current_value = current_value
                        else:
                            current_value = int(current_value)
                    except ValueError:
                        current_value = 0

                    lower_threshold = current_value
                    upper_threshold = current_value

                    if is_auto:
                        # ENHANCED: Check if enhanced thresholds are available for ALL measure types
                        if existing_threshold.get("feedback_enhanced"):
                            # Use the enhanced thresholds that profile.py calculated
                            lower_threshold = float(existing_threshold.get("lower_threshold", 0))
                            upper_threshold = float(existing_threshold.get("upper_threshold", 0))
                            
                        elif threshold:
                            # Fallback to configuration thresholds if no enhancement available
                            lower_threshold = float(threshold.get("lower_threshold", 0))
                            upper_threshold = float(threshold.get("upper_threshold", 0))
                            

                    # Rounding off all numbers to nearest integers
                    lower_threshold = int(math.floor(lower_threshold))
                    upper_threshold = int(math.ceil(upper_threshold))

                    # Get the percentage Change comparing the current value from the previous value
                    __percent_change = 0
                    previous_value = 0
                    value_change = 0
                    
                    try:
                        previous_value_dict = get_previous_value(
                            config, measure_id, metric, is_behavioral
                        )

                        if previous_value_dict:
                            previous_value = float(previous_value_dict.get("value") or 0.0)
                            previous_value = (
                                round(previous_value, 2) if previous_value else 0
                            )
                            value_change = current_value - previous_value
                            __percent_change = percent_change(previous_value, current_value)
                        else:
                            __percent_change = 0
                    except ValueError:
                        __percent_change = 0

                    status = None
                    alert_metrics = {}
                    standard_deviation = 0
                    message = ""

                    if is_auto:
                        if is_alerts_check_enabled:
                            status, alert_metrics, standard_deviation = (
                                get_auto_alert_status(
                                    lower_threshold,
                                    upper_threshold,
                                    current_value,
                                    previous_value_metrics,
                                    attribute_id,
                                    measure_id,
                                    is_behavioral,
                                    behavioral_key,
                                )
                            )
                            if measure_name == "column_count":
                                status, lower_threshold, upper_threshold = schema_change(
                                    config,
                                    status,
                                    lower_threshold,
                                    upper_threshold,
                                    current_value,
                                )
                    else:
                        # Manual Threshold Constraints
                        threshold_constraints = metric.get("threshold_constraints", {})
                        threshold_constraints = (
                            threshold_constraints if threshold_constraints else {}
                        )
                        threshold_constraints = (
                            json.loads(threshold_constraints)
                            if threshold_constraints
                            and isinstance(threshold_constraints, str)
                            else threshold_constraints
                        )
                        threshold_constraints = (
                            threshold_constraints if threshold_constraints else {}
                        )
                        base_measure_name = metric.get("base_measure_name")
                        base_measure_name = (
                            base_measure_name if base_measure_name else measure_name
                        )
                        
                        # Fetch the latest dqscore manual threshold calculation
                        current_dqscore = get_current_measure_dqscore(
                            config, run_id, measure_id
                        )

                        threshold_data = threshold_constraints.get("threshold_data", None)
                        threshold_data_selected = (
                            threshold_data[0]
                            if threshold_data and len(threshold_data) > 0
                            else None
                        )
                        is_first_run = (
                            "asset_id" in metric
                            and metric.get("asset_id")
                            and first_observed_assets
                            and metric.get("asset_id") in first_observed_assets
                        )
                        skip_first_run = (
                            threshold_data_selected.get("skip_first_run", False)
                            if threshold_data_selected
                            else False
                        )
                        threshold_type = (
                            threshold_data_selected.get("type", "")
                            if threshold_data_selected
                            else ""
                        )
                        total_rows = asset.get("row_count", 0)
                        total_rows = total_rows if total_rows else 0
                        
                        if measure and measure_category != "comparison":
                            level = measure.get("level", "")
                            if (level == "measure") or (
                                level != "measure"
                                and not (threshold_type == "score" and total_rows == 0)
                            ):
                                status, message = check_manual_alert_status(
                                    base_measure_name,
                                    threshold_constraints,
                                    current_value,
                                    __percent_change,
                                    current_dqscore,
                                )
                        else:
                            status, message = check_manual_alert_status(
                                base_measure_name,
                                threshold_constraints,
                                current_value,
                                __percent_change,
                                current_dqscore,
                            )

                        if skip_first_run and is_first_run:
                            status = ALERT_OK
                            message = ""

                        if measure_category == "comparison" and status and status != ALERT_OK:
                            th = metric.get("threshold") or {}
                            lower = th.get("lower_threshold", 0)
                            upper = th.get("upper_threshold", 0)
                            message = (
                                f"1 out of 1 Measures failed in the comparison.\n"
                                f"{measure_name} did not meet the expected result. Difference: {current_value}\n "
                                f"Source Count: {lower} | Target Count: {upper}"
                            )
                current_alert = 1 if status and status != ALERT_OK else 0
                
                if attribute_id:
                    attribute_alert = attribute_alert_count.get(attribute_id, 0)
                    attribute_alert_count.update(
                        {attribute_id: (attribute_alert + current_alert)}
                    )
                    
                if status and status != ALERT_OK:
                    create_alert_notification_log(
                        config, status, attribute_id, measure_name
                    )
                    
                # Calculating the percentage change
                alert_percentage = {}
                
                if status and status != ALERT_OK and not is_range_measure:
                    base_measure_name = metric.get("base_measure_name")
                    base_measure_name = (
                        base_measure_name if base_measure_name else measure_name
                    )
                    if measure_category in [LENGTH, ENUM, BEHAVIORAL]:
                        base_measure_name = metric_key
                        
                    if is_auto and not threshold_constraints:
                        message = ""
                        message, alert_percentage = get_alert_message(
                            base_measure_name,
                            is_auto,
                            total_count,
                            current_value,
                            lower_threshold,
                            upper_threshold,
                            threshold_constraints,
                            measure_category,
                            __percent_change,
                        )

                    measure_alert_percentage = json.dumps(alert_percentage, default=str)
                    asset_query = f" and asset_id='{asset_id}'" if asset_id else ""
                    query_string = f"""
                        update core.measure set alert_percentage = '{measure_alert_percentage}'
                        where id='{measure_id}' {asset_query}
                    """
                    cursor = execute_query(connection, cursor, query_string)

                # ENHANCED: For ALL measures, preserve enhanced thresholds if they exist
                if existing_threshold.get("feedback_enhanced"):
                    # Keep the enhanced thresholds and all existing metadata for ALL measure types
                    current_threshold = {
                        **existing_threshold,
                        # Don't overwrite the enhanced lower_threshold and upper_threshold
                    }
                else:
                    # For measures without enhancement, update with current calculated thresholds
                    current_threshold = {
                        **existing_threshold,
                        "lower_threshold": lower_threshold,
                        "upper_threshold": upper_threshold,
                    }
                    
                if not is_auto:
                    current_threshold = {**existing_threshold, **threshold_constraints}

                alert_metrics = alert_metrics if alert_metrics else {}

                # DQL-309 Decimal point issue resolution
                if measure_name and measure_name.lower() in ["freshness", "latency", "delay", "file_on_time_arrival"]:
                    current_value = current_value
                else:
                    frac, _ = math.modf(current_value)
                    if frac == 0:
                        current_value = int(current_value)

                # Calculate percentage threshold for change percentage for viewing in frontend chart
                percent_metrics_dict = get_previous_percent_metrics(
                    config, asset_id, measure_id, attribute_id
                )
                lt_percent, ut_percent = 0, 0
                
                if percent_metrics_dict:
                    percent_metrics = [
                        lst.get("pct_change") for lst in percent_metrics_dict
                    ]
                    if percent_metrics:
                        percent_metrics = list(
                            map(
                                lambda x: 0 if not x or x == "NA" else float(x),
                                percent_metrics,
                            )
                        )
                        if len(percent_metrics) > 1:
                            threshold = percent_model(percent_metrics)
                            lt_percent, ut_percent = threshold[0], threshold[1]
                            
                current_threshold.update({"lt_percent": round(lt_percent, 2)})
                current_threshold.update({"ut_percent": round(ut_percent, 2)})

                # Depth change reset anomaly for incremental logic
                if (current_depth and previous_depth) and (
                    is_behavioral and incremental_config
                ):
                    status = status if (current_depth == previous_depth) else None

                # Calculate the standard deviation for the values from the threshold generated
                deviation = "NA"
                deviation = calculate_deviation(
                    status,
                    lower_threshold,
                    upper_threshold,
                    current_value,
                    standard_deviation,
                )

                # Get the query prepared for behavioral and univariate data
                if is_behavioral:
                    connection_id = config.get("connection_id")
                    query_input = (
                        str(uuid4()),
                        connection_id,
                        asset_id,
                        attribute_id,
                        measure_id,
                        metric_key,
                        behavioral_key,
                        run_id,
                        slice_key,
                        is_auto,
                        str(current_value),
                        str(previous_value),
                        str(__percent_change),
                        str(value_change),
                        str(deviation),
                        message,
                        status,
                        json.dumps(current_threshold, default=str),
                        json.dumps(alert_percentage, default=str),
                        json.dumps(alert_metrics, default=str),
                    )
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals},CURRENT_TIMESTAMP)",
                        query_input,
                    ).decode("utf-8")

                    alert_input_values.append(query_param)
                    last_runs_measures.update({measure_id: (status, run_id)})

                else:
                    # Non-behavioral metrics
                    query_input = (
                        measure_id,
                        run_id,
                        measure_name,
                        behavioral_key,
                        is_auto,
                        slice_key,
                        str(previous_value),
                        str(__percent_change),
                        str(value_change),
                        str(deviation),
                        status,
                        message,
                        json.dumps(alert_percentage, default=str),
                        json.dumps(current_threshold, default=str),
                        json.dumps(alert_metrics, default=str),
                    )
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals},CURRENT_TIMESTAMP)",
                        query_input,
                    ).decode("utf-8")
                    alert_input_values.append(query_param)
                    last_runs_measures.update({measure_id: (status, run_id)})

            if is_behavioral:
                drift_alerts_input = split_queries(alert_input_values)
                for input_values in drift_alerts_input:
                    try:
                        query_input = ",".join(input_values)
                        attribute_insert_query = f"""
                            insert into core.behavioral_metrics (id,connection_id, asset_id, attribute_id,
                            measure_id, measure_name, behavioral_key, run_id, slice_key, is_auto, value, previous_value,
                            percent_change, value_change, deviation, message, status, threshold, alert_percentage, alert_metrics,
                            created_date)
                            values {query_input}
                            RETURNING id
                        """
                        cursor = execute_query(
                            connection, cursor, attribute_insert_query
                        )

                        # Manage New Alerts for Notifications
                        drift_alerts_id = fetchall(cursor)
                        if drift_alerts_id and len(drift_alerts_id) > 0:
                            drift_alerts_id = [id.get("id") for id in drift_alerts_id]
                            # Try to send alert event, but don't fail if it doesn't work
                            
                            # Update alerts with LLM-based messages for the newly updated alerts
                            try:
                                updated_count = update_alerts_with_llm_messages(config, drift_alerts_id)
                                if updated_count > 0:
                                    log_info(f"Updated {updated_count} alerts with LLM-generated messages")
                            except Exception as e:
                                log_error("Failed to update alerts with LLM messages", e)
                                # Don't fail the entire check_alerts process if LLM update fails

                            try:
                                save_alert_event(config, drift_alerts_id, {"type": "new_alert"})
                            except Exception as e:
                                log_error("Error sending alert event: ", e)

                    except Exception as e:
                        log_error("check alerts: inserting new alerts", e)
                        raise e
                        
                send_bigpanda_alerts(config, drift_alerts_input)
                send_servicenow_alerts(config, drift_alerts_input)
                
            else:
                drift_alerts_input = split_queries(alert_input_values)
                for input_values in drift_alerts_input:
                    try:
                        query_input = ",".join(input_values)
                        attribute_insert_query = f"""
                            update core.metrics
                        set
                            measure_name = update_payload.measure_name,
                            behavioral_key = update_payload.behavioral_key,
                            is_auto=update_payload.is_auto,
                            slice_key=update_payload.slice_key::uuid,
                            previous_value=update_payload.previous_value,
                            percent_change=update_payload.percent_change,
                            value_change=update_payload.value_change,
                            deviation=update_payload.deviation,
                            drift_status=update_payload.status,
                            message=update_payload.message,
                            alert_percentage=update_payload.alert_percentage::jsonb,
                            threshold=update_payload.threshold::jsonb,
                            alert_metrics=update_payload.alert_metrics::jsonb,
                            modified_date=update_payload.modified_date
                        from (
                            values {query_input}
                        ) as update_payload(measure_id,run_id,measure_name,behavioral_key,is_auto,slice_key,previous_value,percent_change,value_change,deviation,status,message,alert_percentage,threshold,alert_metrics,modified_date)
                        where
                            metrics.measure_id::text = update_payload.measure_id::text
                            and metrics.run_id::text = update_payload.run_id::text
                            and metrics.measure_name = update_payload.measure_name
                        RETURNING
                        metrics.id, metrics.measure_id
                        """
                        cursor = execute_query(
                            connection, cursor, attribute_insert_query
                        )

                        # Manage New Alerts for Notifications
                        drift_alerts_ids = fetchall(cursor)
                        drift_alerts_id = []
                        if drift_alerts_ids and len(drift_alerts_ids) > 0:
                            drift_alerts_id = list(map(lambda item: item['id'], drift_alerts_ids))
                            batch_measure_ids = list(map(lambda item: item['measure_id'], drift_alerts_ids))
                            all_measure_ids.extend(batch_measure_ids)  # Collect all measure IDs
                            
                            if drift_alerts_id and len(drift_alerts_id) > 0:
                                
                                
                                # Update alerts with LLM-based messages for the newly updated alerts
                                try:
                                    updated_count = update_alerts_with_llm_messages(config, drift_alerts_id)
                                    if updated_count > 0:
                                        log_info(f"Updated {updated_count} alerts with LLM-generated messages")
                                except Exception as e:
                                    log_error("Failed to update alerts with LLM messages", e)
                                    # Don't fail the entire check_alerts process if LLM update fails
                                # Try to send alert event, but don't fail if it doesn't work
                                try:
                                    save_alert_event(config, drift_alerts_id, {"type": "new_alert"})
                                except Exception as e:
                                    log_error("Error sending alert event: ", e)
                                    
                    except Exception as e:
                        log_error("check alerts: inserting new alerts", e)
                        raise e
                        
                send_bigpanda_alerts(config, drift_alerts_input)
                send_servicenow_alerts(config, drift_alerts_input)
                

    if asset_id:
        update_alert_count(config)
            
    update_last_run_alerts(config)
    update_last_runs_for_measure(config, last_runs_measures)
    # Remove duplicates and pass unique measure IDs
    unique_measure_ids = list(set(all_measure_ids)) if all_measure_ids else []
    update_issues_status(config, unique_measure_ids if unique_measure_ids else None)
    map_corelated_alerts(config, unique_measure_ids if unique_measure_ids else None)
    


def map_corelated_alerts(config, measure_ids):
    measure_ids = measure_ids or []
    # Filter out invalid measure_ids
    valid_measure_ids = [mid for mid in measure_ids if mid]
    if not valid_measure_ids:
        return
    
    # Create connection once and reuse
    connection = get_postgres_connection(config)
    
    try:
        with connection.cursor() as cursor:
            # Get latest issues for all measure_ids at once
            measure_ids_str = "','".join(valid_measure_ids)
            latest_issues_query = f"""
                SELECT DISTINCT ON (issues.measure_id) 
                    issues.id, issues.issue_id, issues.measure_id, measure.technical_name as technical_name, metrics.measure_name as measure_name,
                    metrics.run_id as issue_run_id, metrics.created_date as issue_date, metrics.drift_status as issue_drift_status, measure.last_run_id
                FROM core.issues
                JOIN core.metrics metrics ON metrics.id = issues.metrics_id
                JOIN core.measure measure ON measure.id = issues.measure_id and measure.is_active = true
                WHERE lower(issues.status) IN ('new', 'inprogress')
                AND issues.is_active = true
                AND issues.is_delete = false
                AND lower(metrics.drift_status) IN ('low', 'medium', 'high')
                AND measure.id IN ('{measure_ids_str}')
                ORDER BY issues.measure_id, issues.created_date DESC
            """
            cursor = execute_query(connection, cursor, latest_issues_query)
            latest_issues = fetchall(cursor)

            # Collect all valid issues and their metrics
            all_associated_data = []
            
            for issue in latest_issues:
                measure_id = issue.get("measure_id")
                latest_run_id = issue.get("last_run_id")
                
                # Check if latest_run_id is within next 3 runs after issue creation
                future_runs_query = f"""
                    SELECT run_id, drift_status, created_date
                    FROM (
                        SELECT 
                            run_id, 
                            drift_status, 
                            created_date,
                            ROW_NUMBER() OVER (PARTITION BY run_id ORDER BY created_date ASC) as rn
                        FROM core.metrics
                        WHERE measure_id = '{measure_id}' and measure_name = '{issue["measure_name"]}'
                        AND run_id IS NOT NULL
                        AND created_date > '{issue["issue_date"]}'::timestamp
                    ) ranked
                    WHERE rn = 1
                    ORDER BY created_date ASC
                    LIMIT 3
                """
                cursor = execute_query(connection, cursor, future_runs_query)
                future_runs = fetchall(cursor)
                future_run_ids = {run["run_id"] for run in future_runs}
                if latest_run_id not in future_run_ids:
                    continue
                
                # Check if any future run has drift_status = 'ok'
                # has_ok_drift = any(
                #     (run.get("drift_status", "").lower() in ("ok", "")) 
                #     for run in future_runs
                # )
                # if has_ok_drift:
                #     continue
                
                # Get metrics for this run_id and measure_id
                metrics_query = f"""
                    SELECT id, asset_id, attribute_id, connection_id, drift_status
                    FROM core.metrics 
                    WHERE run_id = '{latest_run_id}' AND measure_id = '{measure_id}' AND measure_name = '{issue["measure_name"]}'
                    AND lower(drift_status) IN ('high', 'medium', 'low') AND created_date > '{issue["issue_date"]}'::timestamp
                """
                cursor = execute_query(connection, cursor, metrics_query)
                metrics_results = fetchall(cursor)
                if metrics_results:
                    log_info(f"Found {len(metrics_results)} metrics for run_id: {latest_run_id}, measure_id: {measure_id}")
                    # Ensure we have at least one metric result before accessing index 0
                    if len(metrics_results) > 0:
                        first_metric = metrics_results[0]
                        all_associated_data.append({
                            "issue": issue,
                            "metric_data": {
                                "measure_id": measure_id,
                                "attribute_id": first_metric.get("attribute_id"),
                                "asset_id": first_metric.get("asset_id"),
                                "metrics_ids": metrics_results,
                                "connection_id": first_metric.get("connection_id")
                            }
                        })
                    else:
                        log_error(f"No valid metrics found for run_id: {latest_run_id}, measure_id: {measure_id}")
            
            # Call add_associated_issue once with all data
            if all_associated_data:
                add_associated_issues_batch(all_associated_data, connection)
                        
    except Exception as e:
        log_error(f"Error processing measure_ids: {valid_measure_ids}", e)


def add_associated_issues_batch(all_associated_data, connection):
    """
    Adds associated issues to the database for multiple issues and metrics in batch.
    """
    query = """
        INSERT INTO associated_issues (
            id,
            issue_number, 
            issue_id, 
            measure_id, 
            attribute_id, 
            metrics_id, 
            asset_id, 
            connection_id, 
            created_date
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
    """
    
    all_insert_params = []
    
    for data in all_associated_data:
        issue = data["issue"]
        metric_data = data["metric_data"]
        metrics_results = metric_data.get("metrics_ids", [])
        
        insert_params = [
            (
                str(uuid4()), 
                issue.get('issue_id'), 
                issue.get('id'), 
                metric_data.get("measure_id"), 
                metric_data.get("attribute_id"),
                result["id"], 
                metric_data.get("asset_id"), 
                metric_data.get("connection_id"))
            for result in metrics_results
        ]
        all_insert_params.extend(insert_params)
    
    try:
        with connection.cursor() as cursor:
            cursor.executemany(query, all_insert_params)
            log_info(f"Successfully inserted {len(all_insert_params)} associated issues")
    except Exception as e:
        log_error(f"Failed to add associated issues in batch. Error: {str(e)}", e)


def update_issues_status(config: dict, measure_ids=None):
    """
    Update issues status based on current measure status.
    
    Args:
        config (dict): Configuration dictionary
        measure_ids (list, optional): List of measure IDs to check for issue resolution
    """
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            is_incremental = config.get("is_incremental", False)
            if is_incremental:
                log_info("Auto issue resolving is not applicable for incremental assets.")
                return
            if not measure_ids:
                # Pipeline failed run alert issues - resolve all unresolved issues for the asset
                asset_id = config.get("asset", {}).get("id")
                if not asset_id:
                    log_error("No asset_id found in config for pipeline issue resolution", Exception("No asset_id"))
                    return
                    
                query_string = f"""
                        SELECT status
                        FROM core.pipeline_runs
                        WHERE asset_id = '{asset_id}'
                        ORDER BY run_end_at DESC
                        LIMIT 1
                """
                cursor = execute_query(connection, cursor, query_string)
                last_pipeline_run_status = fetchone(cursor)
                
                if last_pipeline_run_status and last_pipeline_run_status.get('status', '').lower() == 'success':
                    query_string = f"""
                        UPDATE core.issues
                        SET status = 'Resolved',
                        issue_resolved_date = CURRENT_TIMESTAMP
                        WHERE asset_id = '{asset_id}'
                        AND LOWER(status) != 'resolved'
                        RETURNING id
                    """
                    cursor.execute(query_string)
                    updated_issue_ids = fetchall(cursor)
                    log_info(f"Auto-resolved {len(updated_issue_ids)} issues for pipeline success")
                    create_auto_resolution_worklog(config, updated_issue_ids)
                    update_servicenow_incident(config, updated_issue_ids)
                    update_jira_issue(config, updated_issue_ids)
            else:
                # Use parameterized query to prevent SQL injection
                placeholders = ', '.join(['%s'] * len(measure_ids))
                current_run_id = config.get("run_id")
                
                # Single query to resolve issues for measures that are now OK and have unresolved issues
                update_query = f"""
                    UPDATE core.issues
                    SET status = 'Resolved',
                    issue_resolved_date = CURRENT_TIMESTAMP
                    WHERE measure_id IN (
                        SELECT mt.measure_id
                        FROM core.measure m
                        JOIN core.metrics mt ON m.last_run_id = mt.run_id AND m.id = mt.measure_id
                        WHERE m.id IN ({placeholders})
                        AND LOWER(mt.drift_status) = 'ok'
                    )
                    AND LOWER(status) != 'resolved'
                    AND (
                        metrics_id IS NULL 
                        OR metrics_id NOT IN (
                            SELECT id FROM core.metrics 
                            WHERE run_id = %s
                        )
                    )
                    AND metrics_id IN (
                        SELECT id FROM core.metrics 
                        WHERE drift_status IS NULL
                        OR LOWER(drift_status) IN ('high', 'medium', 'low')
                    )
                    RETURNING id
                """
                
                # Execute the update with measure_ids + current_run_id
                update_params = (measure_ids or []) + [current_run_id]
                cursor.execute(update_query, update_params)
                updated_issue_ids = fetchall(cursor)
                
                if updated_issue_ids:
                    log_info(f"Auto-resolved {len(updated_issue_ids)} issues for measures that are now OK")
                    create_auto_resolution_worklog(config, updated_issue_ids)
                    update_servicenow_incident(config, updated_issue_ids)
                    update_jira_issue(config, updated_issue_ids)
                else:
                    log_info("No measures found that need issue resolution")
                    
    except Exception as e:
        raise e


def create_auto_resolution_worklog(config, updated_issue_ids):
    connection = get_postgres_connection(config)
    try:
        with connection.cursor() as cursor:
            insert_objects = []
            for issue_dict in updated_issue_ids:
                issue_id = issue_dict['id']
                query_input = (
                    str(uuid4()), #id
                    "auto_status", #log_type
                    "", #old_status
                    "auto resolved because there is no alert in the last run", #status
                    True,
                    False,
                    None,  # created_by_id
                    issue_id,  # issue_id
                    config.get("organization_id"),
                    None,  # updated_by_id
                )
                input_literals = ", ".join(["%s"] * len(query_input))
                query_param = cursor.mogrify(f"({input_literals}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)", query_input).decode("utf-8")
                insert_objects.append(query_param)

            insert_objects = split_queries(insert_objects)
            for input_values in insert_objects:
                query_input = ",".join(input_values)
                query_string = f"""
                    INSERT INTO core.issue_work_log (
                        id, log_type, old_status, status,
                        is_active, is_delete, 
                        created_by_id, issue_id, organization_id, updated_by_id,
                        created_date, modified_date
                    ) VALUES {query_input}
                """
                cursor.execute(query_string)


    except Exception as e:
        log_error(f"Error inserting issue work log: {e}", e)
