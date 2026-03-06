"""
    Migration Notes From V2 to V3:
    Pending:True
    Need to work on below methods and Notification URLS
    get_report_notification
"""

from uuid import uuid4
import json
import os
from datetime import datetime

from dqlabs.app_helper.db_helper import execute_query, fetchone, fetchall, split_queries
from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_constants.dq_constants import (
    SUCCESS,
    TEAMS_NOTIFICATION,
    SCHEDULE
)
from dqlabs.app_helper.log_helper import log_error
from dqlabs.app_helper.dq_helper import format_freshness, get_client_origin
from dqlabs.enums.schedule_types import ScheduleStatus


def get_notification(config: dict, notification_id: str, module: str = "") -> list:
    """
    Returns the tasks for the given category
    """
    notifications = []
    template_join = "join core.templates temp ON temp.type = noty.type and temp.channel_id=channels.id"
    if module:
        module = f"{module}_summary"
        template_join = f"join core.templates temp ON temp.channel_id=channels.id and temp.type = '{module}'"
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
             select distinct
                noty.content,				
                noty.is_batch,
                TRIM(CONCAT(users.first_name, ' ', users.last_name)) as sender_name,
                temp.subject,
                temp.body,
                (
                    select jsonb_agg(json_build_object('attachment',file)) from core.notifications_attachments where notification_id = noty.id
                ) as attachments,
                temp.json_data,
                channels.technical_name as channel_name,
                channels.id as channel_id,
                noty.organization_id as organization_id,
                integrations.config as channel_config,
                (select theme.logo from core.theme) as logo,
                noty.notification_channel_config,
                noty.type,
                noty.is_summary
            from core.notifications as noty
            join core.notifications_channel ON notifications_channel.notifications_id = noty.id
            join core.integrations ON integrations.channel_id = notifications_channel.channels_id
            join core.channels ON channels.id = integrations.channel_id
            {template_join}
            left join core.notifications_status ON notifications_status.notification_id = noty.id 
                and notifications_status.channel_id = notifications_channel.channels_id
            left join core.users ON users.id::text = noty.trigger_by::text
            where integrations.is_active = true and integrations.is_delete = false 
            and channels.noty = true and noty.id = '{notification_id}'
        """
        cursor = execute_query(connection, cursor, query_string)
        notifications = fetchall(cursor)
        notifications = notifications if notifications else []
    return notifications


def update_notification_status(config: dict, status: str = SUCCESS, error: str = ""):
    """
    Update Notification Status
    """
    error = str(error).replace("'", "''") if error else ""
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_input = (
            str(uuid4()),
            config.get("notification_id"),
            config.get("channel_id"),
            status,
            error,
        )
        input_literals = ", ".join(["%s"] * len(query_input))
        query_param = cursor.mogrify(
            f"({input_literals},CURRENT_TIMESTAMP)",
            query_input,
        ).decode("utf-8")

        query = f"""
                insert into core.notifications_status (id, notification_id, channel_id, status, error, created_date)
                values {query_param}            
            """
        cursor = execute_query(connection, cursor, query)


def get_notifications_settings(config: dict):
    """
    Get Notifications Settings
    """
    notifications_setting = None
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query = """ select notifications from core.settings """
        cursor = execute_query(connection, cursor, query)
        notifications_setting = fetchone(cursor)
        notifications_setting = notifications_setting.get(
            "notifications", None)
    return notifications_setting



def get_client_details(config: dict):
    logo = config.get("logo", None)
    client_name = os.environ.get("CLIENT_NAME")
    client_name = client_name if client_name else "DQLabs"

    dag_info = config.get("dag_info")
    dag_info = dag_info if dag_info else {}
    organization_detail = dag_info.get("organization", None)
    organization_detail = organization_detail if organization_detail else {}
    if not logo:
        theme = organization_detail.get("theme")
        theme = theme if theme else {}
        logo = theme.get("logo")
    website = organization_detail.get("website")
    website = website if website else ""
    website_name = str(website).lower().replace("www.", "") if website else ""
    support_email = organization_detail.get("support_email")
    support_email = support_email if support_email else ""
    website = f"https://{website}/" if website else ""

    client_config = {
        "client_name": client_name,
        "website_name": website_name,
        "website_url": website,
        "support_email": support_email,
    }
    if logo:
        timestamp = str(int(datetime.now().timestamp()))
        client_config.update({"logo": f"{logo}?timestamp={timestamp}"})
    if not website:
        client_config.update({"website_icon": ""})
    if not support_email:
        client_config.update({"mail_icon": ""})
    return client_config


def get_dqlabs_default_images() -> dict:
    timestamp = str(int(datetime.now().timestamp()))
    server_base_url = os.environ.get("DQLABS_SERVER_ENDPOINT")
    if server_base_url and server_base_url.endswith("/"):
        server_base_url = "/".join(server_base_url.split("/")[:-1])

    static_image_path = f"""{server_base_url}/media/dqlabs/"""
    icons = {
        "income_mail_icon": f"""{static_image_path}mailBox.png""",
        "mail_icon": f"""{static_image_path}emailIcon.png""",
        "website_icon": f"""{static_image_path}websiteIcon.png""",
        "lock_icon": f"""{static_image_path}lock.png""",
        "logo": f"""{static_image_path}logo.png?timestamp={timestamp}""",
    }
    return icons


def get_teams_config(notification_info) -> dict:
    """
    Get Teams Configurations
    """
    connection = get_postgres_connection(notification_info)
    with connection.cursor() as cursor:
        query = f"""
            select integrations.id, config from core.integrations
            join core.channels ON channels.id = integrations.channel_id
            where technical_name ='{TEAMS_NOTIFICATION}' 
            and integrations.is_active = true and integrations.is_delete = false
        """
        cursor = execute_query(connection, cursor, query)
        config = fetchone(cursor)
        return config


def update_teams_config(
    notification_info,
    id,
    config,
) -> dict:
    """
    Get Teams Configurations
    """
    connection = get_postgres_connection(notification_info)
    with connection.cursor() as cursor:
        u_config = json.dumps(config, default=str)
        query = f"""
            update core.integrations set config = '{u_config}', modified_date=CURRENT_TIMESTAMP where id = '{id}'
        """
        cursor = execute_query(connection, cursor, query)


def create_notification_request_queue(config: dict, notifications: list, module: str):
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        request_queue_input = []
        for notification in notifications:
            query_input = (
                str(uuid4()),
                "notification",
                module,
                "once",
                ScheduleStatus.Pending.value,
                notification,
            )
            input_literals = ", ".join(["%s"] * len(query_input))
            query_param = cursor.mogrify(
                f"({input_literals}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)", query_input
            ).decode("utf-8")
            request_queue_input.append(query_param)

        request_queue_input = split_queries(request_queue_input)
        for input_values in request_queue_input:
            try:
                query_input = ",".join(input_values)
                query_string = f"""
                    insert into core.request_queue(
                        id, job_type, level, trigger_type, status, notification_id, schedule_time, created_date
                    ) values {query_input} 
                    RETURNING id
                """
                cursor = execute_query(connection, cursor, query_string)
                request_queues_ids = fetchall(cursor)
                request_queues_ids = [id.get("id")
                                      for id in request_queues_ids]
                if request_queues_ids:
                    create_notification_request_queue_detail(
                        config, request_queues_ids)
            except Exception as e:
                log_error("Error in insert request queue for notification : ", e)


def create_notification_request_queue_detail(config: dict, request_queue_ids: list):
    organization_id = config.get("organization_id")
    dag_id = f"{str(organization_id)}_notification"
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        request_queue_detail_input = []
        for request_queue_id in request_queue_ids:
            query_input = (
                str(uuid4()),
                "notification",
                dag_id,
                "",
                ScheduleStatus.Pending.value,
                request_queue_id,
                False,
            )
            input_literals = ", ".join(["%s"] * len(query_input))
            query_param = cursor.mogrify(
                f"({input_literals}, CURRENT_TIMESTAMP)", query_input
            ).decode("utf-8")
            request_queue_detail_input.append(query_param)

        request_queue_detail_input = split_queries(request_queue_detail_input)
        for input_values in request_queue_detail_input:
            try:
                query_input = ",".join(input_values)
                query_string = f"""
                    insert into core.request_queue_detail(
                        id, category, dag_id, task_id, status, queue_id, is_submitted, created_date
                    ) values {query_input} 
                    RETURNING id
                """
                cursor = execute_query(connection, cursor, query_string)
                request_queues_ids = fetchall(cursor)
                request_queues_ids = [id.get("id")
                                      for id in request_queues_ids]
            except Exception as e:
                log_error(
                    "Error in insert request queue detail for notification : ", e)


def get_theme(config: dict) -> list:
    """
    Returns list of theme
    """
    theme = {}
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query = "select theme, logo, reporting from core.theme"
        cursor = execute_query(connection, cursor, query)
        theme = fetchone(cursor)
    return theme


def get_report_notification(config: dict, is_share: bool = False) -> list:
    """
    Returns the details of report or dashboard
    """
    notifications = []
    report_id = config.get("report_id")
    dashboard_id = config.get("dashboard_id")
    module_name = config.get("module_name")
    properties = config.get("properties", {})
    user = properties.get("user", {})
    channels = "('email')"
    if is_share:
        channels = properties.get("channels", [])
        channels = f"""({','.join(f"'{i}'" for i in channels)})"""
    if not report_id and module_name == "share_widget":
        report_id = config.get("notification_id")
    if not dashboard_id and module_name == "share_dashboard":
        dashboard_id = config.get("notification_id")

    if module_name == "report":
        detail_query = f"""
            select users.user_preference->'widget' as preference, users.associated_type, users.role_id,
            users.association, widget.*, schedules_mapping.properties as schedule_properties
            from core.widget 
            left join core.schedules_mapping on schedules_mapping.report_id = widget.id
            left join core.users on users.id = schedules_mapping.created_by::uuid
            where widget.id = '{report_id}'
        """
    elif module_name == "share_widget":
        detail_query = f"select *  from core.widget where id = '{report_id}'"
    else:
        detail_query = f"select * from core.dashboard where id = '{dashboard_id}'"
    connection = get_postgres_connection(config)
    detail = {}
    with connection.cursor() as cursor:
        query_string = f"""
            select channels.technical_name,integrations.config as configuration,
            jsonb_build_object('subject',templates.subject,'body',templates.body,'json_data',templates.json_data) as template
            from core.integrations
            join core.channels on channels.id=integrations.channel_id
            join core.templates on templates.channel_id=channels.id and templates.type='{module_name}'
            where channels.technical_name in {channels} and integrations.is_active=true
        """
        cursor = execute_query(connection, cursor, query_string)
        notifications = fetchall(cursor)
        notifications = notifications if notifications else []
        cursor = execute_query(connection, cursor, detail_query)
        detail = fetchone(cursor)
        detail = detail if detail else {}
        if report_id:
            user_preference = detail.get("preference", {})
            role_id = detail.get("role_id")
            if is_share:
                user_preference = user.get(
                    "user_preference", {}).get("widget", {})
                role_id = user.get("role_id")
            user_preference = user_preference if user_preference else {}
            properties = detail.get("properties")
            widget_id = detail.get("id")
            widget_preference = (
                user_preference[widget_id] if widget_id in user_preference else {}
            )
            if widget_preference:
                properties = {**properties, **
                              widget_preference.get("properties", {})}
            if is_share:
                detail.update(
                    {
                        "properties": properties,
                        "associated_type": user.get("associated_type", "full"),
                        "association": user.get("association", []),
                        "preference": user_preference,
                    }
                )
            else:
                detail.update({"properties": properties})
            detail.update({"role_id": role_id})
    return notifications, detail


def create_alert_notification_log(
    config: dict = {}, status: str = "", attribute_id: str = "", measure_name: str = ""
):
    organization_id = config.get("organization_id")
    asset = config.get("asset", {})
    asset = asset if asset else {}
    asset_name = asset.get("name").replace(
        "'", "''") if asset.get("name") else []
    asset_name = f"for asset {asset_name}" if asset_name else ""
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        attribute_name = ""
        if attribute_id:
            attribute_name_query = (
                f"SELECT id, name FROM core.attribute where id='{attribute_id}' limit 1"
            )
            cursor = execute_query(connection, cursor, attribute_name_query)
            attribute_name = fetchone(cursor)
            attribute_name = attribute_name.get("name", None)
            attribute_name = attribute_name.replace(
                "'", "''") if attribute_name else ""
            measure_name = measure_name.replace(
                "'", "''") if measure_name else ""

        attribute_name = f"in the attribute {attribute_name}" if attribute_name else ""
        notification_insert_query = f"""
        insert into core.notification_logs 
        (id, title, description, level, action, is_delete,organization_id, created_date ) 
        values('{uuid4()}', 'Alert is Triggered for an Asset', 
        '"A {status} priority alert has been triggerd for the measure {measure_name} {attribute_name} {asset_name}', 'Alert', 'created', false,'{organization_id}', CURRENT_TIMESTAMP)
        """
        cursor = execute_query(connection, cursor, notification_insert_query)

def get_notification_summary_data(config: dict, notification_id: str) -> dict:
    """
    Returns the summary data for the notification
    """
    summary_data = {}
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            select distinct id,created_date, content, notification_channel_config
            from core.notifications where id = '{notification_id}' or parent_id = '{notification_id}'
            order by created_date asc
        """
        cursor = execute_query(connection, cursor, query_string)
        response = fetchall(cursor)
        response = response if response else []
        if response:
            users = []
            notification_channel_config = {}
            notification_data = []
            for data in response:
                content = data.get("content", {})
                notification_data.append(content.get("params", {}))
                userList = content.get("to", [])
                if userList:
                    users.extend(userList)
                # Collect all integrated_ids for each channel_id
                channel_configuration = data.get("notification_channel_config", {})
                for channel_id, integrated_ids in channel_configuration.items():
                    if channel_id not in notification_channel_config:
                        notification_channel_config[channel_id] = []
                    notification_channel_config[channel_id].extend(integrated_ids)
        
            # Remove duplicates in integrated_ids
            for channel_id in notification_channel_config:
                notification_channel_config[channel_id] = list(set(notification_channel_config[channel_id]))
            summary_data = {
                "notification_data": notification_data,
                "users": list(set(users)),
                "notification_channel_config": notification_channel_config,
            }
    return summary_data

def get_summary_template(module_name: str, channel_type: str):
    summary_template = ""
    notification_summary_template = {
        "email": {
            "schedule": """
                <tr> 
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[connection_name]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[asset_name]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[attribute_name]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[measure_name]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[job_name]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[job_level]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[status]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[execution_time]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">
                        <a href="[link]" style="color: #007bff; text-decoration: none;">View Log</a>
                    </td>
                </tr>
            """,
            "issue": """
                <tr>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[owner_name]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[connection_name]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[asset_name]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[attribute_name]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[task_name]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[measure_name]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[priority]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[status]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[message]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">
                        <a href="[link]" style="color: #007bff; text-decoration: none;">View Issue</a>
                    </td>
                </tr>
            """,
            "alert": """
                <tr>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[connection_name]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[asset_name]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[attribute_name]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[task_name]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[measure_name]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[measure_description]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[priority]</td>
                     <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[measure_type]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[trigger_type]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[message]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[expected]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[actual]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">
                        <a href="[link]&action=mark_as_normal" style="color: #007bff; text-decoration: none;">Mark As Normal</a>
                    </td>
                     <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">
                        <a href="[link]&action=mark_as_anomaly" style="color: #007bff; text-decoration: none;">Mark As Anomaly</a>
                    </td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">
                        <a href="[link]" style="color: #007bff; text-decoration: none;">View Alert</a>
                    </td>
                </tr>
            """,
            "issue_update": """
                <tr>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[connection_name]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[asset_name]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[attribute_name]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[task_name]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[measure_name]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[message]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[owner_name]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">
                        <a href="[link]" style="color: #007bff; text-decoration: none;">View Issue</a>
                    </td>
                </tr>
            """,
        },
        "teams": {  
            "schedule": """
                <tr> 
                <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[connection_name]</td>
                <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">
                    <a href="[asset_url]" style="color: #007bff; text-decoration: none;">[asset_name]</a>
                </td>
                <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">
                    <a href="[attribute_url]" style="color: #007bff; text-decoration: none;">[attribute_name]</a>
                </td>
                <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">
                    <a href="[attribute_url]" style="color: #007bff; text-decoration: none;">[measure_name]</a>
                </td>
                <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[job_name]</td>
                <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[job_level]</td>
                <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[status]</td>
                <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[execution_time]</td>
                <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">
                    <a href="[link]" style="color: #007bff; text-decoration: none;">View Log</a>
                </td>
            </tr>
            """,
            "issue": """
                <tr> 
                <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[owner_name]</td>
                <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[connection_name]</td>
                <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">
                    <a href="[asset_url]" style="color: #007bff; text-decoration: none;">[asset_name]</a>
                </td>
                <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">
                    <a href="[attribute_url]" style="color: #007bff; text-decoration: none;">[attribute_name]</a>
                </td>
                 <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">
                    <a href="[attribute_url]" style="color: #007bff; text-decoration: none;">[task_name]</a>
                </td>
                <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">
                    <a href="[attribute_url]" style="color: #007bff; text-decoration: none;">[measure_name]</a>
                </td>
                <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[priority]</td>
                <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[status]</td>
                <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[message]</td>
                <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">
                    <a href="[link]" style="color: #007bff; text-decoration: none;">View Issue</a>
                </td>
            </tr>
            """,
            "alert": """
                <tr>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[connection_name]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[asset_name]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[attribute_name]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[task_name]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[measure_name]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[measure_description]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[priority]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[measure_type]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[trigger_type]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[message]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[expected]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[actual]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">
                        <a href="[link]&action=mark_as_normal" style="color: #007bff; text-decoration: none;">Mark As Normal</a>
                    </td>
                     <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">
                        <a href="[link]&action=mark_as_anomaly" style="color: #007bff; text-decoration: none;">Mark As Anomaly</a>
                    </td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">
                        <a href="[link]" style="color: #007bff; text-decoration: none;">View Alert</a>
                    </td>
                </tr>
            """,
            "issue_update": """
                <tr>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[connection_name]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[asset_name]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[attribute_name]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[task_name]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[measure_name]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[message]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">[owner_name]</td>
                    <td style="font-family: Inter, sans-serif; padding: 10px; border: 1px solid #eee;">
                        <a href="[link]" style="color: #007bff; text-decoration: none;">View Issue</a>
                    </td>
                </tr>
            """,
        },
        "teams_native": {
            "schedule": """
                {
                "type": "TableRow",
                "cells": [
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[connection_name]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[asset_name]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[attribute_name]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[measure_name]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[job_name]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[job_level]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[status]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[execution_time]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[View Log]([link])", "wrap": true}]}
                ]
            }
            """,
            "issue": """
                {
                "type": "TableRow",
                "cells": [
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[owner_name]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[connection_name]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[asset_name]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[attribute_name]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[task_name]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[measure_name]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[priority]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[status]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[message]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[View Issue]([link])", "wrap": true}]}
                ]
            }
            """,
            "alert": """
                {
                "type": "TableRow",
                "cells": [
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[connection_name]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[asset_name]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[attribute_name]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[task_name]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[measure_name]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[measure_description]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[priority]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[measure_type]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[trigger_type]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[message]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[expected]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[actual]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[Mark As Normal]([link]&action=mark_as_normal)", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[Mark As Anomaly]([link]&action=mark_as_anomaly)", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[View Issue]([link])", "wrap": true}]}
                ]
            }
            """,
            "issue_update": """
                {
                "type": "TableRow",
                "cells": [
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[connection_name]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[asset_name]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[attribute_name]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[task_name]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[measure_name]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[message]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[owner_name]", "wrap": true}]},
                    {"type": "TableCell", "items": [{"type": "TextBlock", "text": "[View Issue]([link])", "wrap": true}]}
                ]
            }
            """,
        },
        "slack": {
            "schedule": """
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": "*Connection Name:* [connection_name]"},
                        {"type": "mrkdwn", "text": "*Asset Name:* [asset_name]"},
                        {"type": "mrkdwn", "text": "*Attribute Name:* [attribute_name]"},
                        {"type": "mrkdwn", "text": "*Measure Name:* [measure_name]"},
                        {"type": "mrkdwn", "text": "*Job Name:* [job_name]"},
                        {"type": "mrkdwn", "text": "*Job Level:* [job_level]"},
                        {"type": "mrkdwn", "text": "*Job Status:* [status]"},
                        {"type": "mrkdwn", "text": "*Execution Time:* [execution_time]"},
                        {"type": "mrkdwn", "text": "*Log:* <[link]|View log>"}
                    ]
                },
                {"type": "divider"},
            """,
            "issue": """
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": "*Issue Owner:* [connection_name]"},
                        {"type": "mrkdwn", "text": "*Connection Name:* [owner_name]"},
                        {"type": "mrkdwn", "text": "*Asset Name:* [asset_name]"},
                        {"type": "mrkdwn", "text": "*Attribute Name:* [attribute_name]"},
                        {"type": "mrkdwn", "text": "*Task Name:* [task_name]"},
                        {"type": "mrkdwn", "text": "*Measure Name:* [measure_name]"},
                        {"type": "mrkdwn", "text": "*Priority:* [priority]"},
                        {"type": "mrkdwn", "text": "*Status:* [status]"},
                        {"type": "mrkdwn", "text": "*Message:* [message]"},
                        {"type": "mrkdwn", "text": "*Log:* <[link]|View Issue>"},
                    ]
                },
                {"type": "divider"},
            """,
            "alert": """
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": "*Connection Name:* [connection_name]"},
                        {"type": "mrkdwn", "text": "*Asset Name:* [asset_name]"},
                        {"type": "mrkdwn", "text": "*Attribute Name:* [attribute_name]"},
                        {"type": "mrkdwn", "text": "*Task Name:* [task_name]"},
                        {"type": "mrkdwn", "text": "*Measure Name:* [measure_name]"},
                        {"type": "mrkdwn", "text": "*Measure Description:* [measure_description]"},
                        {"type": "mrkdwn", "text": "*Priority:* [priority]"},
                        {"type": "mrkdwn", "text": "*Measure Type:* [measure_type]"},
                        {"type": "mrkdwn", "text": "*Trigger Type:* [trigger_type]"},  
                        {"type": "mrkdwn", "text": "*Message:* [message]"}
                    ]
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": "*Expected:* [expected]"},
                        {"type": "mrkdwn", "text": "*Actual:* [actual]"},
                        {"type": "mrkdwn", "text": "*Log:* <[link]&action=mark_as_normal|Mark As Normal>"},
                        {"type": "mrkdwn", "text": "*Log:* <[link]&action=mark_as_anomaly|Mark As Anomaly>"},
                        {"type": "mrkdwn", "text": "*Log:* <[link]|View Alert>"}
                    ]
                },
                {"type": "divider"},
            """,
            "issue_update": """
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": "*Issue Owner:* [connection_name]"},
                        {"type": "mrkdwn", "text": "*Asset Name:* [asset_name]"},
                        {"type": "mrkdwn", "text": "*Attribute Name:* [attribute_name]"},
                        {"type": "mrkdwn", "text": "*Task Name:* [task_name]"},
                        {"type": "mrkdwn", "text": "*Measure Name:* [measure_name]"},
                        {"type": "mrkdwn", "text": "*Message:* [message]"},
                        {"type": "mrkdwn", "text": "*Connection Name:* [owner_name]"},
                        {"type": "mrkdwn", "text": "*Log:* <[link]|View Issue>"},
                    ]
                },
                {"type": "divider"},
            """,
        },
        "google_chat": {
            "schedule": """
                {
                    "widgets": [
                        {"textParagraph":{"text":"<b>Connection Name:</b> [connection_name]"}},
                        {"textParagraph":{"text":"<b>Asset:</b> [asset_name]"}},
                        {"textParagraph":{"text":"<b>Attribute:</b> [attribute_name]"}},
                        {"textParagraph":{"text":"<b>Measure:</b> [measure_name]"}},
                        {"textParagraph":{"text":"<b>Job:</b> [job_name]"}},
                        {"textParagraph":{"text":"<b>Level:</b> [job_level]"}},
                        {"textParagraph":{"text":"<b>Status:</b> [status]"}},
                        {"textParagraph":{"text":"<b>Time:</b> [execution_time]"}},
                        {"textParagraph":{"text":"<b>Error:</b> [execution_time]"}},
                        {"textParagraph":{"text":"<a href='[link]'>View Log</a>"}},
                        {"divider":{}}
                    ]
                }
            """,
            "issue": """
                {
                    "widgets": [
                        {"textParagraph":{"text":"<b>Issue Owner:</b> [owner_name]"}},
                        {"textParagraph":{"text":"<b>Connection Name:</b> [connection_name]"}},
                        {"textParagraph":{"text":"<b>Asset:</b> [asset_name]"}},
                        {"textParagraph":{"text":"<b>Attribute:</b> [attribute_name]"}},
                        {"textParagraph":{"text":"<b>Task:</b> [task_name]"}},
                        {"textParagraph":{"text":"<b>Measure:</b> [measure_name]"}},
                        {"textParagraph":{"text":"<b>Priority:</b> [priority]"}},
                        {"textParagraph":{"text":"<b>Status:</b> [status]"}},
                        {"textParagraph":{"text":"<b>Message:</b> [message]"}},
                        {"textParagraph":{"text":"<a href='[link]'>View Issue</a>"}},
                        {"divider":{}}
                    ]
                }
            """,
            "alert": """
                {
                    "widgets": [
                        {"textParagraph":{"text":"<b>Connection Name:</b> [connection_name]"}},
                        {"textParagraph":{"text":"<b>Asset:</b><a href='[asset_link]'>[asset_name]</a>"}},
                        {"textParagraph":{"text":"<b>Attribute:</b><a href='[attribute_link]'>[attribute_name]</a>"}},
                        {"textParagraph":{"text":"<b>Task:</b>[task_name]"}},
                        {"textParagraph":{"text":"<b>Measure:</b>[measure_name]"}},
                        {"textParagraph":{"text":"<b>Measure Description:</b>[measure_description]"}},
                        {"textParagraph":{"text":"<b>Priority:</b>[priority]"}},
                        {"textParagraph":{"text":"<b>Measure Type:</b>[measure_type]"}},
                        {"textParagraph":{"text":"<b>Trigger Type:</b>[trigger_type]"}},
                        {"textParagraph":{"text":"<b>Expected:</b>[expected]"}},
                        {"textParagraph":{"text":"<b>Actual:</b>[actual]"}},
                        {"textParagraph":{"text":"<a href='[link]&action=mark_as_normal'>Mark As Normal</a>"}},
                        {"textParagraph":{"text":"<a href='[link]&action=mark_as_anomaly'>Mark As Anomaly</a>"}},
                        {"textParagraph":{"text":"<a href='[link]'>View Alert</a>"}},
                        {"divider":{}}
                    ]
                }
            """,
            "issue_update": """
                {
                    "widgets": [
                        {"textParagraph":{"text":"<b>Connection Name:</b> [connection_name]"}},
                        {"textParagraph":{"text":"<b>Asset:</b> [asset_name]"}},
                        {"textParagraph":{"text":"<b>Attribute:</b> [attribute_name]"}},
                        {"textParagraph":{"text":"<b>Task:</b> [task_name]"}},
                        {"textParagraph":{"text":"<b>Measure:</b> [measure_name]"}},
                        {"textParagraph":{"text":"<b>Message:</b> [message]"}},
                        {"textParagraph":{"text":"<b>Issue Owner:</b> [owner_name]"}},
                        {"textParagraph":{"text":"<a href='[link]'>View Issue</a>"}},
                        {"divider":{}}
                    ]
                }
            """,
        }
    }
    notification_summary_template = notification_summary_template.get(channel_type, {})
    summary_template = notification_summary_template[module_name] if module_name in notification_summary_template else ""
    return summary_template