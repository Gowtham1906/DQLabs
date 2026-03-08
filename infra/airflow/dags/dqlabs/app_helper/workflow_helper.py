import json
import math
import requests
import os

from dqlabs.app_helper.db_helper import execute_query, fetchone, fetchall
from dqlabs.app_helper.dag_helper import get_postgres_connection

# Import Constant
from dqlabs.app_constants.dq_constants import CREATE_ISSUE

def get_workflow_tasks(config: dict):
    workflow_id = config.get("workflow_id")
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            select workflow_tasks.id, workflow_tasks.parent_id, workflow_tasks.name,
            workflow_tasks.configuration, workflow_actions.technical_name, workflow_tasks.is_trigger,
            workflow_actions.configuration as action_configuration
            from core.workflow_tasks 
            join core.workflow_actions on workflow_actions.id = workflow_tasks.workflow_action_id
            where workflow_tasks.workflow_id = '{workflow_id}'
        """
        cursor = execute_query(connection, cursor, query_string)
        tasks = fetchall(cursor)
        for task in tasks:
            workflow_configuration = task.get("configuration") if task.get("configuration") else {} 
            action_configuration = task.get("action_configuration") if task.get("action_configuration") else {}
            configuration = {**action_configuration, **workflow_configuration}
            task.update({"configuration": configuration})
        return tasks

def get_workflow_api_token(config: dict):
    query_string = f"select client_id, client_key as client_secret from core.api_token where is_workflow=true"
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        cursor = execute_query(connection, cursor, query_string)
        return fetchone(cursor)
    
def prepare_workflow_payload(task_name: str, data: dict, columns: list, config: dict):
    """
    Prepare Workflow Payload
    """
    payload = data
    if columns:
        payload = {
            col: data.get(col) or next((v for k, v in data.items() if col in k), "")
            for col in columns
        }
    if task_name == CREATE_ISSUE:
        created_by = config.get("created_by")
        sla = config.get("sla")
        users = config.get("users")
        priority = config.get("priority")
        if priority:
            payload.update({"priority": priority})
        if users:
            payload.update({"users": users})
        if created_by:
            payload.update({"created_by": created_by.get("id")})
        if sla:
            payload.update({"sla": sla})
    return payload

def get_integration_config(config: dict, channel_name: str):
    """
    Get Integration Config
    """
    query_string = f"""
        select integrations.config from core.integrations
        join core.channels on channels.id = integrations.channel_id
        where integrations.is_active=true and channels.technical_name='{channel_name}'
        and channels.is_active=true and integrations.is_delete=false
    """
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        cursor = execute_query(connection, cursor, query_string)
        channel = fetchone(cursor)
        return channel
    

def get_user(config: dict, user_id: str):
    """
    Get User Detail
    """
    query_string = f"select first_name,last_name from core.users where id='{user_id}'"
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        cursor = execute_query(connection, cursor, query_string)
        user = fetchone(cursor)
        return user
    
def update_issue_integration(config: dict, payload: dict):
    if payload.get("external_id"):
        connection = get_postgres_connection(config)
        external_list = payload.get("external_list", {})
        jira_id = external_list.get("jira")
        servicenow_id = external_list.get("servicenow")
        description = payload.get("external_description",)
        if jira_id:
            description += f"JIRA ID: '{jira_id}'"
        if servicenow_id:
            description = description.rstrip() + f"\n   SERVICE NOW ID: '{servicenow_id}'"
        with connection.cursor() as cursor:
            query_string = """
                update core.issues set external_id=%s, description=%s where issues.id =%s
            """
            cursor.execute(query_string, [
                json.dumps(payload.get("external_id"), default=str),
                description,
                payload.get("id")
            ])

def date_formatter(num, detailed=False):
    if num:
        seconds = int(num)
        d = math.floor(seconds / (3600 * 24))
        hour = math.floor(seconds % (3600 * 24) / 3600)
        m = math.floor(seconds % 3600 / 60)
        s = math.floor(seconds % 60)
        h = (d * 24) + hour

        dDisplay = f"{d}d"
        hDisplay = f"{h}h"
        mDisplay = f"{m}m"
        sDisplay = f"{s}s"
        h_Display = f"{hour}h"

        if detailed:
            dDisplay = f"{d} day, " if d == 1 else f"{d} days, " if d > 0 else ""
            hDisplay = f"{h} hour, " if h == 1 else f"{h} hours, " if h > 0 else ""
            h_Display = (
                f"{hour} hour, " if hour == 1 else f"{hour} hours, " if hour > 0 else ""
            )
            mDisplay = f"{m} minutes, " if m == 1 else f"{m} minutes, " if m > 0 else ""
            sDisplay = f"{s} second, " if s == 1 else f"{s} seconds, " if s > 0 else ""

        if d > 3:
            if h:
                return f"{dDisplay} {h_Display} {mDisplay}"
            return dDisplay
        elif h > 0 and h <= 95:
            if mDisplay:
                return f"{hDisplay} {mDisplay} {sDisplay}"
            return f"{hDisplay}"
        elif m > 0 and m <= 59:
            if sDisplay:
                return f"{mDisplay} {sDisplay}"
            return f"{mDisplay}"

        return sDisplay
    if num == 0:
        return f"{num}s"
    return "NA"

def get_value(field: str, item: dict = {}):
    value = item.get(field)
    if field == "measure":
        value = item.get("measure_name")
    if not value:
        if field == "connection":
            value = str(item.get("connection_id")) if item.get("connection_id") else ""
        elif field == "asset_name":
            value = item.get("asset")
        elif field == "measure":
            value = item.get("measure_name")
        elif field in ["domain", "product", "application", "tags"]:
            value = item.get(f"{field}s")
    return value

def get_notification_channel_template(config: dict, channel_name: str, template_name: str):
    """
    Get Notification Channel Template
    """
    configuration = None
    query_string = f"""
        select templates.subject, 
        templates.body,templates.json_data, integrations.config as channel_config, (select theme.logo from core.theme) as logo
        from core.integrations
        join core.channels on channels.id = integrations.channel_id
        join core.templates on templates.channel_id=channels.id
        where channels.technical_name='{channel_name}' and templates.type='{template_name}'
        and channels.is_active = true and integrations.is_active = true
    """
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        cursor = execute_query(connection, cursor, query_string)
        configuration = fetchone(cursor)
    return configuration
    

def get_user_emails(config: dict, users: list = []):
    """
    Get User Detail
    """
    emails = []
    try:
        users = f"""({','.join(f"'{user}'" for user in users)})"""
        query_string = f"select email from core.users where id in {users}"
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            cursor = execute_query(connection, cursor, query_string)
            emails = fetchall(cursor)
            emails = [email.get("email") for email in emails]
            return emails
    except:
        return emails


def get_connection_assets(config: dict, connection_id: str, asset_name: str):
    """
    Get Connection Assets
    """
    assets = []
    asset_condition = f"name = '{asset_name}'"
    if "*" in asset_name:
        asset_name = asset_name.replace("*", "")
        asset_condition = f"name ilike '%{asset_name}%'"
    query_string = f"""
        select distinct id from core.asset where connection_id='{connection_id}' and {asset_condition}
    """
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        cursor = execute_query(connection, cursor, query_string)
        assets = fetchall(cursor)
        assets = [asset.get("id") for asset in assets] if assets else []
    return assets


def make_request(config: dict, url: str, method: str, data: dict = None):
    api_token = config.get("api_token")
    if not api_token:
        api_token = get_workflow_api_token(config)
    server_endpoint = os.environ.get("DQLABS_SERVER_ENDPOINT")
    if server_endpoint and server_endpoint.endswith("/"):
        server_endpoint = "/".join(server_endpoint.split("/")[:-1])
    headers = {
        "client-id": api_token.get("client_id"),
        "client-secret": api_token.get("client_secret"),
        "Content-Type": "application/json"
    }
    endpoint = f"{server_endpoint}{url}"
    if method == "post":
        response = requests.post(endpoint, headers=headers, data=json.dumps(data))
    elif method == "put":
        response = requests.put(endpoint, headers=headers, data=json.dumps(data))
    else:
        response = requests.get(endpoint, headers=headers)
    
    if response.ok:
        response = json.loads(response.text)
        if "response" in response:
            response = response.get("response")
        if "data" in response:
            response = response.get("data")
    else:
        response.raise_for_status()
    return response

def get_semantic_users(config: dict, type: str, data: list):
    """
    Get Semantic Users
    """
    data = f"""({','.join(f"'{i}'" for i in data)})"""
    condition = f"domain_id in {data} and level='domain'"
    if type == "application":
        condition = f"application_id in {data} and level='application'"
    elif type == "product":
        condition = f"product_id in {data} and level='product'"
    elif type == "tag":
        condition = f"tags_id in {data} and level='tag'"
    query_string = f"""
        select distinct user_id from core.user_mapping where {condition}
    """

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        cursor = execute_query(connection, cursor, query_string)
        users = fetchall(cursor)
        users = [user.get("user_id") for user in users]
    return users

def get_semantics(config: dict, asset_id: str):
    """
    Get Existing Semantics
    """
    semantics = []
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            select 
                coalesce(json_agg(domain_mapping.domain_id) filter(where domain_mapping.domain_id is not null) , '[]') as domain, 
                coalesce(json_agg(application_mapping.application_id) filter(where application_mapping.application_id is not null) , '[]') as application,
                coalesce(json_agg(product_mapping.product_id) filter (where product_mapping.product_id is not null), '[]') as product, 
                coalesce(json_agg(tags_mapping.tags_id) filter (where tags_mapping.tags_id is not null), '[]') as tags
            from core.asset
            left join core.domain_mapping on domain_mapping.asset_id = asset.id
            left join core.application_mapping on application_mapping.asset_id = asset.id
            left join core.product_mapping on product_mapping.asset_id = asset.id
            left join core.tags_mapping on tags_mapping.asset_id = asset.id
            where asset.id ='{asset_id}'
        """
        cursor = execute_query(connection, cursor, query_string)
        semantics = fetchone(cursor)
        semantics = semantics if semantics else {}
    return semantics