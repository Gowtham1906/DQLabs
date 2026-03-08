"""
    Migration Notes From V2 to V3:
    Migrations Completed 
"""

import os
import re
import json
import time
from datetime import datetime
import tempfile
import logging
import threading
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy.sql import True_

from dqlabs.utils.connections import get_dq_connections, get_dq_connections_database
from dqlabs.app_helper.storage_helper import get_storage_service
from dqlabs.app_helper.dag_helper import execute_native_query
from dqlabs.utils.extract_workflow import get_queries
from dqlabs.app_helper.log_helper import log_error
from dqlabs.app_helper.dq_helper import  get_client_origin
from dqlabs.tasks.notifications import send_mail_notification, send_slack_notification, send_teams_notification, send_google_notification
from dqlabs.enums.connection_types import ConnectionType
from dqlabs.app_helper.report_helper import (generate_metadata_report, prepare_csv_report, prepare_pdf_report, __get_measure_active_filter_query,
                                              __get_date_filter_query, __get_semantics_restrictions_query, validate_report_attachment, prepare_report_masking_data, prepare_predefined_report)
from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.db_helper import execute_query, fetchall
from dqlabs.utils.request_queue import update_queue_status
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.utils.extract_failed_rows import get_export_failed_rows_job_config
from dqlabs_agent.services.livy_service import LivyService

from dqlabs.app_constants.dq_constants import (
    EMAIL_NOTIFICATION, SLACK_NOTIFICATION, TEAMS_NOTIFICATION, EXCEPTION_DASHBOARD_ID
)

logger = logging.getLogger(__name__)

# Semaphore to limit concurrent Selenium WebDriver instances
# Chrome/WebDriver can be resource-intensive, so we limit to 2 concurrent instances
SELENIUM_SEMAPHORE = threading.Semaphore(2)


def send_report_notification(config:dict, channels:list, report:dict, is_share:bool = False):
    """
    Send Report Notification
    """
    if not is_share:
        update_queue_status(config, ScheduleStatus.Running.value, True)
    
    # Check if this is an exception dashboard widget that needs per-user filtering
    report_properties = report.get('properties', {})
    selected_tab = report_properties.get("selectedTab", "default")
    tab_properties = report_properties.get(f"{selected_tab}", {})
    tables = tab_properties.get("tables", [])
    
    # If widget has 'exception' table and we have multiple emails, send per-user filtered reports
    config_properties = config.get("properties", {})
    user_specific_report = config_properties.get("user_specific_report", False)
    is_exception_widget = config.get("dashboard_id", "") == EXCEPTION_DASHBOARD_ID
    if is_exception_widget and user_specific_report:
        # Exception dashboard widget - send separate filtered report to each user
        __send_exception_widget_per_user(config, channels, report, is_share)
        return
    
    # Standard report notification flow (no per-user filtering)
    __send_standard_report_notification(config, channels, report, is_share)


def __send_standard_report_notification(config:dict, channels:list, report:dict, is_share:bool = False):
    """
    Standard Report Notification (without per-user filtering)
    """
    report_type = report.get('report_type', 'metadata')
    metadata_report_type = report.get('metadata_report_type', '')
    dag_info = config.get("dag_info")
    selected_tab = report.get('properties').get("selectedTab", "default")
    widget_tab = report.get('properties').get("widget_tab", "")
    properties = report.get('properties').get(f"{selected_tab}", {})
    properties = properties.get("properties")
    config_properties = config.get("properties", {})
    report_format = properties.get('report_format', 'pdf')
    report_id = report.get("id")
    report_name = report.get("name")
    file_url = ""
    empty_report = False
    try:
        if report_type == "metadata" and metadata_report_type == "pre-defined":
            file_name = __prepare_file_name(report, report_format)
            data = __get_predefined_data(config, report, is_share)
            prepare_predefined_report(config, data, report, file_name)

        elif report_type == "metadata":
            report_format = config_properties.get('report_format', 'pdf')
            if report_format == "image":
                report_format = "png"
            file_name = __prepare_file_name(report, report_format)
            if report_format == "csv":
                data = __get_metadata_data(config, report, is_share)
                prepare_csv_report(data, file_name)
            else:
                client_link = get_client_origin(dag_info)
                if client_link.endswith('/'):
                    client_link = client_link.rsplit('/', 1)[0]
                report_url = f"{client_link}/widget/{report_id}"
                type = report.get("chart_type", "bar")
                generate_metadata_report(report_url, file_name, report_format, class_name="loaderContainer", type=type)
        else:
            file_name = __prepare_file_name(report, report_format)
            data = __get_remediation_data(config, report, is_share)
            schedule_properties = report.get("schedule_properties", {})
            empty_report = schedule_properties.get("empty_report", False)
            if len(data) == 0 and not empty_report:
                return
            empty_report = empty_report and not data
            if report_format == "csv":
                prepare_csv_report(data, file_name)
            else:
                prepare_pdf_report(config, data, report, file_name)
        file_url = __save_report(dag_info, report, file_name, report_format)
        send_notification(config, channels, file_url, is_share, file_name, report_name, empty_report, widget_tab)
        os.remove(file_name)
    except Exception as e:
        log_error(" error to send report", e)
    finally:
        if not is_share:
            __update_report_status(config, report_id, file_url)


def __process_single_widget_user(config: dict, channels: list, report: dict, email: str, 
                                  dag_info: dict, report_format: str, report_id: str, 
                                  report_name: str, type: str, is_share: bool):
    """
    Process a single user's widget report (worker function for multi-threading)
    Uses semaphore to limit concurrent Selenium WebDriver instances
    """
    file_url = ""
    max_retries = 2
    retry_count = 0
    
    while retry_count <= max_retries:
        try:
            # Look up user ID from email
            user_id = __get_user_id_from_email(config, email)
            
            # Prepare unique file name for this user
            safe_email = re.sub("[^A-Za-z0-9]", "_", email.split("@")[0])
            file_name = __prepare_file_name_with_suffix(report, report_format, safe_email)
            
            # Generate the report with target_user_id in URL for filtering
            client_link = get_client_origin(dag_info)
            if client_link.endswith('/'):
                client_link = client_link.rsplit('/', 1)[0]
            
            # Pass target_user_id as URL parameter for per-user filtering
            report_url = f"{client_link}/widget/{report_id}"
            if user_id:
                report_url = f"{report_url}?target_user_id={user_id}"
            
            # Use semaphore to limit concurrent Selenium operations
            SELENIUM_SEMAPHORE.acquire()
            try:
                generate_metadata_report(report_url, file_name, report_format, class_name="loaderContainer", type=type)
            finally:
                SELENIUM_SEMAPHORE.release()
            
            # Save report
            file_url = __save_report(dag_info, report, file_name, report_format)
            
            # Send notification to this specific user only
            __send_notification_to_single_user(config, channels, file_url, is_share, file_name, report_name, email)
            
            # Clean up
            if os.path.exists(file_name):
                os.remove(file_name)
            
            return {"success": True, "email": email, "file_url": file_url}
        except Exception as e:
            retry_count += 1
            error_msg = str(e).lower()
            is_selenium_error = "webdriver" in error_msg or "tab crashed" in error_msg or "selenium" in error_msg
            
            # Clean up file on error
            if 'file_name' in locals() and os.path.exists(file_name):
                try:
                    os.remove(file_name)
                except:
                    pass
            
            if retry_count > max_retries:
                log_error(f"Error sending exception widget report to user {email} after {max_retries} retries", e)
                return {"success": False, "email": email, "error": str(e)}
            elif is_selenium_error:
                # Wait before retrying Selenium errors
                time.sleep(5 * retry_count)  # Exponential backoff: 5s, 10s
                log_error(f"Retry {retry_count}/{max_retries} for Selenium error sending widget report to user {email}", e)
            else:
                # Non-Selenium errors, don't retry
                log_error(f"Error sending exception widget report to user {email}", e)
                return {"success": False, "email": email, "error": str(e)}
    
    return {"success": False, "email": email, "error": "Max retries exceeded"}


def __send_exception_widget_per_user(config:dict, channels:list, report:dict, is_share:bool = False):
    """
    Send Exception Dashboard Widget with per-user filtering
    Each user receives a report filtered to show only their data
    Uses URL parameter to pass target_user_id for filtering
    Uses multi-threading to process multiple users in parallel
    """
    dag_info = config.get("dag_info")
    config_properties = config.get("properties", {})
    report_format = config_properties.get('report_format', 'pdf')
    if report_format == "image":
        report_format = "png"
    
    report_id = report.get("id")
    report_name = report.get("name")
    type = report.get("chart_type", "bar")
    
    # Get emails from properties
    emails_str = config_properties.get("email", "")
    emails = [email.strip() for email in emails_str.split(",") if email.strip()]
    
    if not emails:
        return
    
    # Use multi-threading to process users in parallel
    # Limit to 2 workers for Selenium operations to avoid resource exhaustion
    max_workers = min(2, len(emails))  # Limit to 2 workers or number of emails, whichever is smaller
    file_url = ""
    results = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        futures = {
            executor.submit(
                __process_single_widget_user,
                config, channels, report, email, dag_info, report_format,
                report_id, report_name, type, is_share
            ): email for email in emails
        }
        # Collect results as they complete
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
                if result.get("success") and result.get("file_url"):
                    file_url = result.get("file_url")
            except Exception as e:
                email = futures[future]
                log_error(f"Unexpected error processing widget report for user {email}", e)
                results.append({"success": False, "email": email, "error": str(e)})
    
    # Update status after all users processed
    if not is_share:
        update_queue_status(config, ScheduleStatus.Completed.value, True)
        __update_report_status(config, report_id, file_url)


def __get_user_id_from_email(config: dict, email: str) -> str:
    """
    Look up user ID from email address
    """
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            SELECT id FROM core.users 
            WHERE lower(email) = lower('{email}')
            LIMIT 1
        """
        cursor = execute_query(connection, cursor, query_string)
        result = fetchall(cursor)
        
        if result and len(result) > 0:
            return str(result[0].get("id"))
    return None


def __prepare_file_name_with_suffix(report: dict, format: str, suffix: str):
    """
    Prepare file name with a suffix (e.g., user identifier)
    """
    on_premise_env = os.environ.get("ON_PREMISE", False)
    name = re.sub("[^A-Za-z0-9]", "_", report.get("name").strip())
    report_id = report.get("id")
    
    file_name = f"{name}_{suffix}_{report_id}.{format}"
    if on_premise_env:
        fileobj_or_path = tempfile.gettempdir()
        file_name = f"{fileobj_or_path}/{name}_{suffix}_{report_id}.{format}"
    return file_name


def __send_notification_to_single_user(config: dict, channels: list, report_url: str, is_share: bool, file_name: str, report_name: str, user_email: str):
    """
    Send notification to a single user only (for exception dashboard widgets)
    """
    for channel in channels:
        if channel.get("technical_name") == EMAIL_NOTIFICATION:
            __send_email_to_single_recipient(config, channel, report_url, is_share, file_name, report_name, user_email)
        else:
            channel_name = channel.get("technical_name")
            send_share_report_notification(config, channel_name, channel, report_url)


def __send_email_to_single_recipient(config: dict, channel: dict, report_url: str, is_share: bool, file_name: str, report_name: str, user_email: str):
    """
    Send email notification to a single recipient
    """
    properties = config.get("properties", {})
    dag_info = config.get("dag_info")
    
    message = properties.get("message")
    subject = properties.get("subject", '')
    title = properties.get("title", "")
    configuration = channel.get("configuration", {})
    is_attachment = validate_report_attachment(file_name, config)
    
    # Template
    template = channel.get("template")
    if subject:
        template.update({'subject': subject})
    
    if is_share:
        link = properties.get("link", "")
        sender_name = properties.get("sender_name", "")
        report_link = f""""<a target="_blank" href="{report_url}" class="btn"
                            style="padding:10px;font-size: 16px;font-family: Plus Jakarta Sans, sans-serif; color: #FFFFFF; font-weight: 500; text-decoration: none; white-space: nowrap; display: inline-block; text-transform: uppercase; background: #E60000; text-decoration: none;">
                            Download Report
                        </a>
        """ if not is_attachment else ""
        params = {"title": title, "message": message, "report_link": report_link, "link": link, "sender_name": sender_name}
    else:
        if is_attachment:
            report_link = f"The {report_name} has been shared via schedule, please download the attachment to access the report."
        else:
            report_link = f"""Please <a target="_blank"
                    href="{report_url}" class="link"
                    style="color:rgb(33, 22, 22); text-decoration: none; padding-left: 5px;">
                    click here</a> to download Report
                    """
        params = {"message_body": message, "report_link": report_link}
    
    # Prepare Attachment
    on_premise = os.environ.get("ON_PREMISE", False)
    attachments = [file_name if on_premise else f"./{file_name}"] if is_attachment else []

    # Send to SINGLE user only
    send_mail_notification(
        {
            "channel_config": configuration,
            "dag_info": dag_info,
            "content": {
                "to": [user_email],  # Single user only
                "params": params
            },
            "attached_files": attachments,
            "is_report": True,
            **template,
        },
        config
    )

def send_dashboard_notification(config:dict, channels:list, dashboard:dict, is_share:bool= False):
    """
    Send Dashboard Notification
    """
    if not is_share:
        update_queue_status(config, ScheduleStatus.Running.value, True)
    dag_info = config.get("dag_info")
    config_properties = config.get("properties", {})
    report_format = config_properties.get('report_format', 'pdf')
    dashboard_id = dashboard.get("id")
    dashboard_name = dashboard.get("name")
    user_specific_report = config_properties.get('user_specific_report', False)
    is_data_observability_dashboard = dashboard_name and dashboard_name.lower() == 'data observability dashboard'
    is_exception_dashboard = dashboard_name and dashboard_name.lower() == 'exception dashboard'
    if is_data_observability_dashboard:
        send_data_observability_dashboard_notification(config, channels, dashboard, is_share)
    elif is_exception_dashboard and user_specific_report:
        send_exception_dashboard_notification(config, channels, dashboard, is_share)
    else:
        if report_format == "image":
            report_format = "png"
        file_name = __prepare_file_name(dashboard, report_format)
        file_url = ""
        try:
            client_link = get_client_origin(dag_info)
            if client_link.endswith('/'):
                client_link = client_link.rsplit('/', 1)[0]
            report_url = f"{client_link}/dashboard/{dashboard_id}"
            generate_metadata_report(report_url, file_name, report_format, class_name="dashboardLoading", wait_time=60, is_image=(report_format=="png"))
            file_url = __save_report(dag_info, dashboard, file_name, report_format)
            if not is_share:
                update_queue_status(config, ScheduleStatus.Completed.value, True)
            send_notification(config, channels, file_url, is_share, file_name, dashboard_name)
            os.remove(file_name)
        except Exception as e:
            log_error(" error to send report", e)
            if not is_share:
                update_queue_status(config, ScheduleStatus.Failed.value, True)
        finally:
            if not is_share:
                __update_dashboard_status(config, dashboard_id, file_url)


def __update_report_status(config: dict, report_id: str, report_url: str):
    """
    Update Report Status
    """
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            update core.widget
            set report_url='{report_url}', last_run=CURRENT_TIMESTAMP
            where id='{report_id}'
        """
        cursor = execute_query(connection, cursor, query_string)

def send_data_observability_dashboard_notification(config:dict, channels:list, dashboard:dict, is_share:bool= False):
    """
    Send Data Observability Dashboard Notification for each tab separately
    """
    dag_info = config.get("dag_info")
    config_properties = config.get("properties", {})
    report_format = config_properties.get('report_format', 'pdf')
    dashboard_id = dashboard.get("id")
    dashboard_name = dashboard.get("name")
    
    if report_format == "image":
        report_format = "png"
    
    # Define the tabs for Data Observability Dashboard
    tabs = [
        {"key": "issues", "label": "Issues"},
        {"key": "observability", "label": "Observability"},
        {"key": "quality", "label": "Quality"},
        {"key": "pipeline", "label": "Pipeline"}
    ]
    
    # Send separate notification for each tab
    for tab in tabs:
        try:
            tab_file_name = __prepare_file_name_for_tab(dashboard, report_format, tab["key"])
            file_url = ""
            
            client_link = get_client_origin(dag_info)
            if client_link.endswith('/'):
                client_link = client_link.rsplit('/', 1)[0]
            
            report_url = f"{client_link}/dashboard/{dashboard_id}?tab={tab['key']}"
            
            generate_metadata_report(report_url, tab_file_name, report_format, class_name="dashboardLoading", wait_time=90, is_image=(report_format=="png"))
            file_url = __save_report(dag_info, dashboard, tab_file_name, report_format)
            
            if not is_share:
                update_queue_status(config, ScheduleStatus.Completed.value, True)
            
            send_notification(config, channels, file_url, is_share, tab_file_name, f"{dashboard_name} - {tab['label']}", widget_tab=tab["key"])
            os.remove(tab_file_name)
            
        except Exception as e:
            log_error(f"Error sending notification for tab {tab['key']}", e)
            if not is_share:
                update_queue_status(config, ScheduleStatus.Failed.value, True)
            continue
    
    if not is_share:
        try:
            __update_dashboard_status(config, dashboard_id, file_url)
        except:
            pass

def __process_single_dashboard_user(config: dict, channels: list, dashboard: dict, user_email: str,
                                     dag_info: dict, report_format: str, dashboard_id: str,
                                     dashboard_name: str, is_share: bool):
    """
    Process a single user's dashboard report (worker function for multi-threading)
    Uses semaphore to limit concurrent Selenium WebDriver instances
    """
    file_url = ""
    max_retries = 2
    retry_count = 0
    file_name = __prepare_file_name(dashboard, report_format)
    while retry_count <= max_retries:
        try:
            # Look up user ID from email
            user_id = __get_user_id_from_email(config, user_email)
            
            client_link = get_client_origin(dag_info)
            if client_link.endswith('/'):
                client_link = client_link.rsplit('/', 1)[0]
            
            # Pass target_user_id as URL parameter for per-user filtering
            report_url = f"{client_link}/dashboard/{dashboard_id}"
            if user_id:
                report_url = f"{report_url}?target_user_id={user_id}"
            
            # Use semaphore to limit concurrent Selenium operations
            SELENIUM_SEMAPHORE.acquire()
            try:
                generate_metadata_report(report_url, file_name, report_format, class_name="dashboardLoading", wait_time=90, is_image=(report_format=="png"))
            finally:
                SELENIUM_SEMAPHORE.release()
            
            file_url = __save_report(dag_info, dashboard, file_name, report_format)
            
            # Send notification to this specific user only (reuse existing function)
            __send_notification_to_single_user(config, channels, file_url, is_share, file_name, dashboard_name, user_email)
            
            # Clean up
            if os.path.exists(file_name):
                os.remove(file_name)
            
            return {"success": True, "email": user_email, "file_url": file_url}
        except Exception as e:
            retry_count += 1
            error_msg = str(e).lower()
            is_selenium_error = "webdriver" in error_msg or "tab crashed" in error_msg or "selenium" in error_msg
            
            # Clean up file on error
            if 'file_name' in locals() and os.path.exists(file_name):
                try:
                    os.remove(file_name)
                except:
                    pass
            
            if retry_count > max_retries:
                log_error(f"Error sending exception dashboard report to user {user_email} after {max_retries} retries", e)
                return {"success": False, "email": user_email, "error": str(e)}
            elif is_selenium_error:
                # Wait before retrying Selenium errors
                time.sleep(5 * retry_count)  # Exponential backoff: 5s, 10s
                log_error(f"Retry {retry_count}/{max_retries} for Selenium error sending dashboard report to user {user_email}", e)
            else:
                # Non-Selenium errors, don't retry
                log_error(f"Error sending exception dashboard report to user {user_email}", e)
                return {"success": False, "email": user_email, "error": str(e)}
    
    return {"success": False, "email": user_email, "error": "Max retries exceeded"}


def send_exception_dashboard_notification(config:dict, channels:list, dashboard:dict, is_share:bool= False):
    """
    Send Exception Dashboard Notification
    Each user receives a filtered report showing only their data
    Uses multi-threading to process multiple users in parallel
    """
    dag_info = config.get("dag_info")
    config_properties = config.get("properties", {})
    report_format = config_properties.get('report_format', 'pdf')
    dashboard_id = dashboard.get("id")
    dashboard_name = dashboard.get("name")
    
    if report_format == "image":
        report_format = "png"
    
    # Get emails from properties (comma-separated)
    emails_str = config_properties.get("email", "")
    emails = [email.strip() for email in emails_str.split(",") if email.strip()]
    
    if not emails:
        return
    
    # Use multi-threading to process users in parallel
    # Limit to 2 workers for Selenium operations to avoid resource exhaustion
    max_workers = min(2, len(emails))  # Limit to 2 workers or number of emails, whichever is smaller
    file_url = ""
    results = []
    has_failure = False
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        futures = {
            executor.submit(
                __process_single_dashboard_user,
                config, channels, dashboard, user_email, dag_info, report_format,
                dashboard_id, dashboard_name, is_share
            ): user_email for user_email in emails
        }
        
        # Collect results as they complete
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
                if result.get("success") and result.get("file_url"):
                    file_url = result.get("file_url")
                elif not result.get("success"):
                    has_failure = True
            except Exception as e:
                user_email = futures[future]
                log_error(f"Unexpected error processing dashboard report for user {user_email}", e)
                results.append({"success": False, "email": user_email, "error": str(e)})
                has_failure = True
    
    # Update status after all users have been processed
    if not is_share:
        if has_failure:
            update_queue_status(config, ScheduleStatus.Failed.value, True)
        else:
            update_queue_status(config, ScheduleStatus.Completed.value, True)
        __update_dashboard_status(config, dashboard_id, file_url)



def __prepare_file_name_for_tab(dashboard: dict, format: str, tab_key: str):
    """
    Prepare File Name for specific tab
    """
    on_premise_env = os.environ.get("ON_PREMISE", False)
    name = re.sub("[^A-Za-z0-9]", "_", dashboard.get("name").strip())
    dashboard_id = dashboard.get("id")
    
    file_name = f"{name}_{tab_key}_{dashboard_id}.{format}"
    if on_premise_env:
        fileobj_or_path = tempfile.gettempdir()
        file_name = f"{fileobj_or_path}/{name}_{tab_key}_{dashboard_id}.{format}"
    return file_name

def __update_dashboard_status(config: dict, dashboard_id: str, report_url: str):
    """
    Update Dashboard Status
    """
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            update core.dashboard
            set report_url='{report_url}', last_run=CURRENT_TIMESTAMP
            where id='{dashboard_id}'
        """
        cursor = execute_query(connection, cursor, query_string)


def __get_metadata_data(config:dict, report_info:dict, is_share:bool):
    """
    Get Metadata Widget Data
    """
    metadata = []
    connection = get_postgres_connection(config)
    selected_tab = report_info.get('properties').get("selectedTab", "default")
    query = report_info.get("report_query", {})
    if is_share:
        query = report_info.get("query", {})
    query_string = query[selected_tab]
    search_filters = report_info.get('properties').get("search", {})
    if query_string:
        query_string = __get_measure_active_filter_query(report_info.get('properties'), query_string)
        query_string = __get_date_filter_query(report_info.get('properties'), query_string)
        user = {
            "association":report_info.get("association"),
            "associated_type":report_info.get("associated_type")
        }
        query_string = __get_semantics_restrictions_query(config, report_info.get('properties'), query_string, user)
        if search_filters:
                filters = [f"{key} >= {value[0]} and {key} <= {value[1]}" if isinstance(value, list) else f"lower({key}::text) like '%{value}%'" for key, value in search_filters.items()]
                search_filters = " and ".join(filters)
                query_string = f"""
                    with search_query as ({query_string})
                        select * from search_query
                        where {search_filters}
                """

        with connection.cursor() as cursor:
            cursor = execute_query(connection, cursor, query_string)
            metadata = fetchall(cursor)
            if metadata:
                metadata = [{k: v for k, v in d.items() if "_id" not in k and "id" !=k} for d in metadata]
    return metadata


def __get_predefined_data(config:dict, report_info:dict, is_share:bool):
    """
    Get Metadata Widget Data
    """
    response = []
    connection = get_postgres_connection(config)
    selected_tab = report_info.get('properties').get("selectedTab", "default")
    query_obj = report_info.get("query", {})
    if is_share:
        query = report_info.get("query", {})

    predefined_sections = query_obj[selected_tab]

    for section in predefined_sections:
        table_heading = section.get("heading", "")
        table_query = section.get("tableContent", "")

        if table_query:
            table_query = __get_measure_active_filter_query(report_info.get('properties'), table_query)
            table_query = __get_date_filter_query(report_info.get('properties'), table_query)

            with connection.cursor() as cursor:
                cursor = execute_query(connection, cursor, table_query)
                list = fetchall(cursor)
                if list:
                    response.append({ "heading": table_heading, "tableContent": list})
                    
    return response

def __get_remediation_data(config: dict, report_info: dict, is_share: bool):
    """
    Get Remediation Data
    """
    dag_info = config.get("dag_info")
    report_settings = dag_info.get("report_settings")
    report_settings = (
        json.loads(report_settings, default=str)
        if report_settings and isinstance(report_settings, str)
        else report_settings
    )
    report_settings = report_settings if report_settings else {}
    connection = report_settings.get("connection")
    schema_name = report_settings.get("schema")
    schema_name = schema_name if schema_name else ""
    if not schema_name:
        raise Exception("Please define the schema name in settings -> platform -> configuration -> remediate -> push down metrics section to export the data.")
    
    data_base_name = report_settings.get("database")
    data_base_name = data_base_name if data_base_name else ""
    connection_type = config.get("connection_type")
    if connection_type == ConnectionType.DB2IBM.value and not data_base_name: 
        data_base_name = 'sample'
    config.update({"schema": schema_name, "database": data_base_name})

    # Get Destination Connection
    connection = report_settings.get("connection")
    connection = connection if connection else {}
    connection = connection.get("id", None)
    source_connection = connection

    if connection != "external_storage":
        connection_object = get_dq_connections(config, source_connection)

        if not connection_object or len(connection_object) == 0:
            raise Exception("Missing Connection Details.")
        connection_object = connection_object[0]
        connection_object = connection_object if connection_object else {}
        connection_config = {}
        if connection_object:
            destination_conn_credentials = connection_object.get("credentials")
            destination_conn_database = destination_conn_credentials.get(
                "database")
            report_settings_database = report_settings.get("database")

            if connection_type == ConnectionType.DB2IBM.value and not destination_conn_database:
                destination_conn_database = 'sample'
            if connection_type == ConnectionType.Snowflake.value and not destination_conn_database:
                destination_connection_database = get_dq_connections_database(config, connection.get("id", None))
                destination_conn_database = destination_connection_database.get('name')

            if not report_settings_database:
                report_settings.update(
                    {
                        "database": destination_conn_database
                        if destination_conn_database
                        else ""
                    }
                )
                if destination_conn_database:
                    config.update({"database":destination_conn_database})
            connection_config = {
                "connection_type": connection_object.get("type"),
                "source_connection_id": connection_object.get("airflow_connection_id"),
                "connection": {**connection_object},
            }
            config.update({"connection_type": connection_object.get("type")})
    else:
        spark_external_config = get_export_failed_rows_job_config(config)
        database = spark_external_config.get("iceberg_catalog")
        schema = spark_external_config.get("iceberg_schema")
        connection_config = {
            "connection_type": "spark",
        }
        config.update({
            "connection_type": "spark", 
            "is_spark": True, 
            "database": database,
            "schema": schema
        })

    rows = __get_failed_rows_tables_data(
        config, report_info, connection_config, is_share)
    if rows:
        report_properties = report_info.get("properties", {}).get("default", {})
        properties = report_properties.get("properties")
        assets = properties.get("remediation_assets", [])
        role_id = report_info.get("role_id")
        rows = prepare_report_masking_data(config, rows, assets, role_id, is_exception=True)
    return rows if rows else []

def __get_failed_rows_last_run(config: dict, assets: list, connection: dict, is_spark: bool, connection_type: str, is_summarized: bool = False):
    queries = get_queries(config)
    reports_query = queries.get("report")
    database = config.get("database")
    schema = config.get("schema", "")
    select_data_query = []
    last_run_query = reports_query.get("last_run_query")
    if is_summarized:
        last_run_query = reports_query.get("summary_last_run_query")
    date_range_last_run_query = reports_query.get("date_range").get("last_run")
    if is_summarized:
        date_range_last_run_query = reports_query.get("date_range").get("summary_last_run")

    index = 0
    rows = []
    for asset in assets:
        table_name = asset.get('failed_rows_table_name')
        if is_spark:
            table_name = table_name.lower()
        alias_name = f"source_{index}"
        measure_alias_name = f"md_{index}"
        index = index+1
        query = (
                last_run_query.replace("<database>", database)
                .replace("<schema>", schema)
                .replace("<failed_row_table>", table_name)
                .replace("<alias_name>", alias_name)
                .replace("<measure_alias_name>", measure_alias_name)
            )
        date_range_query = (
            date_range_last_run_query.replace("<database>", database)
            .replace("<schema>", schema)
            .replace("<failed_row_table>", table_name)
            .replace("<measure_alias_name>", measure_alias_name)
            .replace("<alias_name>", alias_name)
        )
        where_condition_query = f"where {date_range_query}"
        query = query.replace("<where_condition>", where_condition_query)
        select_data_query.append(query)
    if select_data_query:
        select_data_query = (
                    f"""{' UNION ALL'.join(f"{w}" for w in select_data_query)}"""
                )
        temp_query = "temp" if connection_type == "oracle" else "as temp"
        select_data_query = (
            f"select * from ({select_data_query}) {temp_query}"
        )
        if is_spark:
            rows = get_spark_iceberg_data(select_data_query, config)
        else:
            config = {**config, **connection}
            rows, native_connection = execute_native_query(
                config, select_data_query, is_list=True)
        
        items = []
        for row in rows:
            items.append({k.lower(): v for k, v in row.items()})
        rows = items
    return rows


def __get_last_runs(config: dict, runs: list):
    assets = [asset.get("asset_id") for asset in runs if asset.get("asset_id")]
    measures = [measure.get("measure_id") for measure in runs if not measure.get("asset_id") and measure.get("measure_id")]
    run_list = [run.get("run_id") for run in runs]
    run_list = f"""({','.join(f"'{i}'" for i in run_list)})"""

    run_data = []
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        if assets:
            assets = f"""({','.join(f"'{i}'" for i in assets)})"""
            query_string = f"""
                select distinct last_run_id from core.asset where id in {assets} and last_run_id in {run_list}
            """
            cursor = execute_query(connection, cursor, query_string)
            asset_runs = fetchall(cursor)
            asset_runs = [run.get("last_run_id") for run in asset_runs] if asset_runs else []
            run_data = [*asset_runs]
        if measures:
            measures = f"""({','.join(f"'{i}'" for i in measures)})"""
            query_string = f"""
                select distinct measure.last_run_id from core.measure 
                join core.base_measure on base_measure.id=measure.base_measure_id
                where measure.id in {measures} and measure.last_run_id in {run_list} and base_measure.level='measure'
            """
            cursor = execute_query(connection, cursor, query_string)
            measure_runs = fetchall(cursor)
            measure_runs = [run.get("last_run_id") for run in measure_runs] if measure_runs else []
            if measure_runs:
                run_data = [*run_data, *measure_runs]
        return run_data


def __get_failed_rows_tables_data(config: dict, report: dict, connection: dict, is_share: bool):
    """
    Get Data from Selected Failes Rows Tables and Columns
    """
    rows = []
    try:
        queries = get_queries(config)
        is_spark = config.get("is_spark", False)
        reports_query = queries.get("report")
        database = config.get("database")
        schema = config.get("schema", "")
        connection_type = connection.get("connection_type", "")
        connection_type = connection_type.lower()
        report_properties = report.get("properties", {}).get("default", {})
        filters = report_properties.get("filters", {})
        properties = report_properties.get("properties")
        assets = properties.get("remediation_assets")
        columns = properties.get("remediation_attributes")
        columns = [column.get("name") for column in columns]
        group_by = properties.get("group_by", [])
        group_by_columns = [x.get("column") for x in group_by]
        group_by_columns = [x.get("name") for x in group_by_columns]

        reporting =config.get("dag_info", {}).get("settings", {}).get("reporting", {})
        reporting = reporting if reporting else {}
        reporting = (
            json.loads(reporting, default=str)
            if reporting and isinstance(reporting, str)
            else reporting
        )
        is_summarized = reporting.get("summarized_report", False)

        if connection_type not in [
            ConnectionType.Spark.value, 
            ConnectionType.Databricks.value,
            ConnectionType.SapHana.value, 
            ConnectionType.MSSQL.value, 
            ConnectionType.Oracle.value,
            ConnectionType.Redshift.value, 
            ConnectionType.Snowflake.value
        ] and is_summarized:
            is_summarized = False
        
        # Schedule Config
        config_properties = config.get("properties", {})
        is_filter = True if is_share else (config_properties.get("filter") == "with_filter")

        if columns:
            if is_filter:
                date_range = filters.get("date_range", {})
                date_range = date_range.get("selected") if date_range.get("selected") else "All"
                if date_range == "Last Run":
                    runs = __get_failed_rows_last_run(config, assets, connection, is_spark, connection_type, is_summarized)
                    runs = __get_last_runs(config, runs)
                    if not runs:
                        return rows
                    report.update({"last_runs": runs})

            select_columns = columns + group_by_columns
            select_columns = __unique(select_columns)
            select_data_query = []
            data_query = reports_query.get("data")
            result_query = reports_query.get("result_query")
            group_by_query = reports_query.get("group_by")
            aggregate_query = reports_query.get("aggregates")
            summary_data_query = reports_query.get("summary_data")
            index = 0
            for asset in assets:
                table_name = asset.get('failed_rows_table_name')
                if is_spark:
                    table_name = table_name.lower()
                alias_name = f"source_{index}"
                measure_alias_name = f"md_{index}"
                index = index+1

                if is_summarized:
                    select_columns = prepare_summary_columns(select_columns, connection.get('connection_type', ''), alias_name, measure_alias_name)
                    asset_columns = f"""{','.join(f'{w}' for w in select_columns)}""" 
                else:
                    asset_columns = f"""{','.join(f'{w}' for w in select_columns)}""" if connection.get('connection_type', '') in ["bigquery", "spark", "databricks"] else f"""{','.join(f'"{w}"' for w in select_columns)}"""
                query = summary_data_query if is_summarized else data_query
                query = (query.replace("<columns>", asset_columns)
                    .replace("<database>", database)
                    .replace("<schema>", schema)
                    .replace("<failed_row_table>", table_name)
                    .replace("<alias_name>", alias_name)
                    .replace("<measure_alias_name>", measure_alias_name)
                )
                
                if is_filter:
                    where_condition_query, conditional_where_conditions = __prepare_failed_rows_data_where_condition(
                        report, reports_query, connection_type, is_summarized=is_summarized)
                else:
                    where_condition_query = ""
                    conditional_where_conditions = []

                query = query.replace(
                    "<where_condition>", where_condition_query)
                select_data_query.append(query)

            # Query Aggercate and Union
            if select_data_query and len(select_data_query):
                agg_by_columns = []
                select_data_query = f"""{' UNION ALL'.join(f"{w}" for w in select_data_query)}"""
                for x in group_by:
                    group_by_col_name = x.get('column').get('name')
                    group_by_agg_query = aggregate_query.get(
                        x.get('aggregate'))
                    group_by_agg_query = group_by_agg_query.replace(
                        "<agg_column>", group_by_col_name)
                    agg_by_columns.append(group_by_agg_query)

                if agg_by_columns:
                    agg_by_columns = f""",{','.join(f'{w}' for w in agg_by_columns)}"""
                    select_columns = f"""{','.join(f'{w}' for w in columns)}""" if connection.get('connection_type', '') in ["bigquery", "spark", "databricks"] else f"""{','.join(f'"{w}"' for w in columns)}"""
                    order_by = columns[0]
                    select_data_query = group_by_query.replace(
                        "<agg_columns>", agg_by_columns).replace(
                        "<failed_row_table_query>", select_data_query).replace(
                        "<columns>", select_columns).replace(
                        "<order_by>", order_by)
                select_data_query = result_query.replace("<select_data_query>", select_data_query)

                if conditional_where_conditions and len(conditional_where_conditions):
                    conditional_where_clause = " and ".join(conditional_where_conditions)
                    select_data_query = f"""select * from ({select_data_query}) where {conditional_where_clause}"""

                config = {**config, **connection}
                if is_spark:
                    rows = get_spark_iceberg_data(select_data_query, config)
                else:
                    rows, native_connection = execute_native_query(
                        config, select_data_query, is_list=True)
                
                items = []
                for row in rows:
                    items.append({k.lower(): v for k, v in row.items()})
                rows = items
    except Exception as e:
        log_error("send_report : error to send report", e)
        raise e
    finally:
        return rows
    
def __unique(sequence):
    seen = set()
    return [x for x in sequence if not (x in seen or seen.add(x))]

def prepare_summary_columns(select_columns: list, connection_type: str, alias_name: str, measure_alias_name: str):
    columns = []
    measure_columns = ["measure_id", "measure_name", "attribute_name", "attribute_id"]
    metadata_columns = ["connection_id", "connection_name", "asset_id", "asset_name", "run_id", "identifier_key", "exportrow_id", "created_date", "is_summarised", "domains", "products", "applications", "terms", "tags", "measure_condition"]
    for column in select_columns:
        if column.lower() in metadata_columns:
            columns.append(f"{alias_name}.{column}")
        elif column.lower() in measure_columns:
            if connection_type in ConnectionType.Snowflake.value:
                columns.append(f"{measure_alias_name}.value:\"{column}\"::STRING as {column}")
            elif connection_type in [ConnectionType.SapHana.value, ConnectionType.Oracle.value]:
                columns.append(f"{measure_alias_name}.{column} as \"{column}\"")
            elif connection_type in ConnectionType.Redshift.value:
                columns.append(f"JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT({alias_name}.\"MEASURE_DATA\", numbers.i), '{column}') as \"{column}\"")
            else:
                columns.append(f"{measure_alias_name}.{column}")                  
        else:
            if connection_type == ConnectionType.Spark.value:
                if re.match(r'^[A-Za-z0-9_]+$', column):
                    columns.append(f"get_json_object({alias_name}.FAILED_ROW_DATA, '$.{column}') as {column}")
                else:
                    columns.append(f"get_json_object({alias_name}.FAILED_ROW_DATA, '$.`{column}`') as {column}")
            elif connection_type == ConnectionType.Snowflake.value:
                columns.append(f"PARSE_JSON({alias_name}.FAILED_ROW_DATA):\"{column}\"::STRING as {column}")
            elif connection_type == ConnectionType.MSSQL.value: 
                columns.append(f"JSON_VALUE({alias_name}.[FAILED_ROW_DATA], '$.\"{column}\"') as [{column}]")
            elif connection_type == ConnectionType.Databricks.value:
                columns.append(f"get_json_object({alias_name}.FAILED_ROW_DATA, '$.{column}') as `{column}`")
            elif connection_type in ConnectionType.Redshift.value:
                columns.append(f"JSON_EXTRACT_PATH_TEXT({alias_name}.\"FAILED_ROW_DATA\", '{column}') as \"{column}\"")
            else:
                columns.append(f"JSON_VALUE({alias_name}.FAILED_ROW_DATA, '$.{column}') as \"{column}\"")
    return columns


def __prepare_failed_rows_data_where_condition(report, reports_query, connection_type: str, is_summarized: bool = False):
    """
    Prepare Failed rows condition
    """
    where_conditions = []
    conditional_where_conditions = []  # Separate list for conditional query in Spark

    # Report Properties
    report_properties = report.get("properties", {}).get("default", {})
    filters = report_properties.get("filters", {})
    date_range = filters.get("date_range", {})
    measures = filters.get('remediation_measures', [])
    domains = filters.get('remediation_domain', [])
    applications = filters.get('remediation_application', [])
    assets = filters.get('remediation_assets', [])
    conditional_query = report.get('preview_query', '')

    if date_range.get("selected") == "Last Run":
        date_range = 'last_run'
    elif date_range.get("selected") == "Today":
        date_range = 'today'
    else:
        date_range = date_range.get('days', 'All')
    # Date Range Filters
    if date_range != 'All':
        where_condition_query = ''
        match_query = reports_query.get("match")
        date_range_query = reports_query.get("date_range")
        if date_range == 'last_run':
            last_runs = report.get("last_runs", [])
            where_condition_query = f"""run_id in ({','.join(f"'{i}'" for i in last_runs)})"""
        elif date_range == 'today':
            where_condition_query = date_range_query.get(
                "today")
        else:
            date_range = str(date_range) if isinstance(date_range, int) else date_range
            where_condition_query = date_range_query.get(
                "between")
            where_condition_query = where_condition_query.replace(
                "<date_range>", date_range)
        if where_condition_query:
            where_conditions.append(where_condition_query)

    if assets and len(assets):
        assets = f"""({','.join(f"'{x.get('id')}'" for x in assets)})"""
        where_conditions.append(
            f""" ASSET_ID IN {assets} """)

    if measures and len(measures):
        measures = f"""({','.join(f"'{x.get('name')}'" for x in measures)})"""
        where_conditions.append(
            f""" MEASURE_NAME IN {measures} """)

    if domains and len(domains):
        domains = ' or '.join([match_query.replace("<attribute>", "DOMAINS").replace("<value>", x.get('label')) for x in domains])
        where_conditions.append(f""" ({domains}) """)

    if applications and len(applications):
        applications = ' or '.join([match_query.replace("<attribute>", "APPLICATIONS").replace("<value>", x.get('label')) for x in applications])
        where_conditions.append(f""" ({applications}) """)

    # For summarized data, conditional query references columns from SELECT clause
    # So we need to handle it separately (will be applied in outer query)
    if conditional_query:
        if is_summarized:
            conditional_where_conditions.append(f"""({conditional_query}) """)
        else:
            where_conditions.append(f"""({conditional_query}) """)

    if is_summarized:
        condition = "is_summarised = true"
        if connection_type in [ConnectionType.MSSQL.value, ConnectionType.Oracle.value]:
            condition = "is_summarised = 1"
        where_conditions.append(condition)
    elif connection_type in ["spark", "mssql", "snowflake", "databricks", "sap_hana"]:
        condition = "(is_summarised = false or is_summarised is null)"
        if connection_type in [ConnectionType.MSSQL.value, ConnectionType.Oracle.value]:
            condition = "(is_summarised = 0 or is_summarised is null)"
        where_conditions.append(condition)

    if where_conditions and len(where_conditions):
        where_conditions = " and ".join(where_conditions)
        where_conditions = f""" where {where_conditions} """
    else:
        where_conditions = ''

    # Return both regular conditions and conditional query conditions separately for Spark
    return where_conditions, conditional_where_conditions


def __prepare_file_name(report: dict, format: str):
    """
    Prepare File Name
    """
    on_premise_env = os.environ.get("ON_PREMISE", False)
    name = re.sub("[^A-Za-z0-9]", "_", report.get("name").strip())
    report_id = report.get("id")

    file_name = f"{name}_{report_id}.{format}"
    if on_premise_env:
        fileobj_or_path = tempfile.gettempdir()
        file_name = f"{fileobj_or_path}/{name}_{report_id}.{format}"
    return file_name


def __prepare_file_name_for_s3(name: str, report_id: str):
    """
    Prepare File Name S3
    """
    file_name = re.sub("[^A-Za-z0-9]", "_", name.strip())
    file_name = f"{file_name}_{report_id}"
    return file_name

def __save_report(dag_info: dict, report: dict, file: str, format: str):
    """
    Save Report in Storage
    """
    report_url = ''
    try:
        report_id = report.get("id")
        name = report.get("name")
        storage_service = get_storage_service(dag_info)
        report_name = __prepare_file_name_for_s3(name, report_id)
        file_path = f"reports/{report_name}.{format.lower()}"
        with open(file, 'rb') as data:
            report_url = storage_service.upload_file(data, file_path, dag_info)
    except Exception as e:
        log_error('Save report failed ', e)
    finally:
        return report_url
    
def send_notification(config:dict, channels:list, report_url:str, is_share:bool, file_name:str, report_name:str, empty_report:bool = False, widget_tab:str = ""):
    """
    Send Notification
    """
    for channel in channels:
        if channel.get("technical_name") == EMAIL_NOTIFICATION:
            send_mail_report_notification(config, channel, report_url, is_share, file_name, report_name, empty_report, widget_tab)
        else:
            channel_name = channel.get("technical_name")
            send_share_report_notification(config, channel_name, channel, report_url, widget_tab)

def send_mail_report_notification(config:dict, channel:dict, report_url:str, is_share:bool, file_name:str, report_name:str, empty_report:bool = False, widget_tab:str = ""):
    """
    Send Email Notification
    """
    properties = config.get("properties", {})
    dag_info = config.get("dag_info")
    
    # Mail Config
    users = properties.get("email")
    users = users.split(",")
    message = properties.get("message")
    subject = properties.get("subject", '')
    if empty_report:
        subject = "EMPTY REPORT"
        
    title = properties.get("title", "")
    configuration = channel.get("configuration", {})
    is_attachment = validate_report_attachment(file_name, config)
    # Template
    template = channel.get("template")
    if subject:
        template.update({ 'subject':  subject })
    if is_share:
        link = properties.get("link", "")
        # Add tab parameter to the link if widget_tab is available
        if widget_tab:
            link = f"{link}&tab={widget_tab}"
            
        sender_name = properties.get("sender_name", "")
        report_link = f""""<a target="_blank" href="{report_url}" class="btn"
                            style="padding:10px;font-size: 16px;font-family: Plus Jakarta Sans, sans-serif; color: #FFFFFF; font-weight: 500; text-decoration: none; white-space: nowrap; display: inline-block; text-transform: uppercase; background: #E60000; text-decoration: none;">
                            Download Report
                        </a>
        """ if not is_attachment else ""
        params = { "title": title, "message": message, "report_link": report_link, "link": link, "sender_name": sender_name}
    else:
        if is_attachment:
            report_link = f"The {report_name} has been shared via schedule, please download the attachment to access the report."
        else:
            report_link = f"""Please <a target="_blank"
                    href="{report_url}" class="link"
                    style="color:rgb(33, 22, 22); text-decoration: none; padding-left: 5px;">
                    click here</a> to download Report
                    """
        params = { "message_body": message, "report_link": report_link}
    # Prepare Attachment
    on_premise = os.environ.get("ON_PREMISE", False)
    attachments = [file_name if on_premise else f"./{file_name}"] if is_attachment else []

    send_mail_notification(
        {
            "channel_config": configuration,
            "dag_info": dag_info,
            "content": {
                "to": users,
                "params": params
            },
            "attached_files": attachments,
            "is_report": True,
            **template,
        },
        config
    )

def send_share_report_notification(config:dict,channel_name:str, channel:dict, report_url:str, widget_tab:str = ""):
    """
    Send Slack Notification
    """
    properties = config.get("properties", {})
    dag_info = config.get("dag_info")
    
    # Mail Config
    message = properties.get("message")
    title = properties.get("title", "")
    link = properties.get("link", "")
    sender_name = properties.get("sender_name", "")
    configuration = channel.get("configuration", {})
    
    # Add tab parameter to the link if widget_tab is available
    if widget_tab and link:
        link = f"{link}&tab={widget_tab}"
    
    params = { "title": title, "message": message, "report_url": report_url, "link": link, "sender_name": sender_name}
    # Template
    template = channel.get("template")
    if channel_name == SLACK_NOTIFICATION:
        send_slack_notification(
            {
                "channel_config": configuration,
                "dag_info": dag_info,
                "content": {
                    "params": params
                },
                "is_report": True,
                **template,
            }
        )
    elif channel_name == TEAMS_NOTIFICATION:
        send_teams_notification(
            {
                "channel_config": configuration,
                "dag_info": dag_info,
                "content": {
                    "params": params
                },
                "is_report": True,
                **template,
            }
        )
    else:
        send_google_notification(
            {
                "channel_config": configuration,
                "dag_info": dag_info,
                "content": {
                    "params": params
                },
                "is_report": True,
                **template,
            }
        )

def get_spark_iceberg_data(query_string: str, config: dict):
    # Get Livy Spark configuration
    dag_info = config.get("dag_info", {})
    livy_spark_config = dag_info.get("livy_spark_config", {})
    livy_spark_config = livy_spark_config if livy_spark_config else {}
    livy_server_url = livy_spark_config.get("livy_url")
    livy_driver_file_path = livy_spark_config.get("drivers_path")
    spark_conf = livy_spark_config.get("spark_config", {})
    spark_conf = (json.loads(spark_conf) if spark_conf and isinstance(spark_conf, str) else spark_conf)

    external_job_config = get_export_failed_rows_job_config(config)
    job_config = {
        "external_credentials": external_job_config,
        "query_string": query_string,
        "is_list": True
    }

    livy_service = LivyService(livy_server_url, livy_driver_file_path, logger=logger)
    response = livy_service.run_external_query(spark_conf, job_config)
    response = response.get("response", {}) if response else {}
    response = response.get("data", []) if response else []
    if response:
        response = [
            {k: None if v == "None" else v for k, v in row.items()}
            for row in response
        ]
    return response