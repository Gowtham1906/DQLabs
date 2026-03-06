import requests
import os
import json
import re
import base64
from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.db_helper import execute_query, fetchone
from dqlabs.app_helper.log_helper import log_error
from dqlabs.app_helper.crypto_helper import decrypt



def get_jira_response(url="", method_type="post", params="", channel_config: dict = ""):
    api_response = None
    response = ""

    try:
        username = decrypt(channel_config.get("username"))
        api_token = decrypt(channel_config.get("apikey", ""))
        authentication_key = f"{username}:{api_token}"
        encoded_authentication = authentication_key.encode("utf-8")
        authentication = base64.b64encode(encoded_authentication)
        authentication = authentication.decode("utf-8")
        endpoint = channel_config.get("url")

        # Prepare Headers and Params
        api_headers = {
            "Content-Type": "application/json",
            "Accept": "*/*",
            "Authorization": "Basic {}".format(authentication),
        }

        if not api_token:
            raise Exception("Missing Api Key")

        if url:
            endpoint = f"{endpoint}/{url}"

        if method_type == "post":
            data = {}
            if params:
                data = json.dumps(params)
            response = requests.post(url=endpoint, headers=api_headers, data=data)
        elif method_type == "put":
            data = {}
            if params:
                data = json.dumps(params)
            response = requests.put(url=endpoint, headers=api_headers, data=data)
        elif method_type == "delete":
            response = requests.delete(url=endpoint, headers=api_headers)
        else:
            response = requests.get(url=endpoint, headers=api_headers)

        if response and response.status_code in [200, 201, 204]:
            api_response = response.json()
    except Exception as e:
        log_error(f"Jira Get Response Failed - {str(e)}", exc_info=True)
    finally:
        return api_response


def get_jira_config(config):
    is_active = False
    channel_config = {}
    connection = get_postgres_connection(config)
    organization_id = config.get("organization_id")
    with connection.cursor() as cursor:
        query_string = f"""
            select ch.technical_name, ic.is_active, ic.config from core.channels ch
            join core.integrations ic ON ic.channel_id = ch.id
            where ic.organization_id = '{organization_id}' 
            and ch.type = 'jira'
        """
        cursor = execute_query(connection, cursor, query_string)
        channel_config = fetchone(cursor)
        if channel_config:
            is_active = channel_config.get("is_active")
            channel_config = channel_config.get("config")
    return is_active, channel_config


def update_jira_issue(config, issue_ids):
    is_active, channel_config = get_jira_config(config)
    if not is_active:
        return
    if not channel_config.get('web_hook_enabled'):
        return
    connection = get_postgres_connection(config)
    for issue_id in issue_ids:
        issue_id = issue_id.get("id")
        
        with connection.cursor() as cursor:
            query_string = f"""
                select * from core.issues where id = '{issue_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            issue = fetchone(cursor)
        external_id = issue.get("external_id")
        jira_id = external_id.get("jira_id")
        
        url = f"rest/api/3/issue/{jira_id}/transitions"
        params = {}
        response = get_jira_response(
            url, "get", params, channel_config
        )
        response = response if response else {}
        transition_data = response.get("transitions", [])
        transitions_dict = {}
        for transition in transition_data:
            transition_id = transition.get("id")
            transition_name = transition.get(
                "name").lower().replace(" ", "")
            if transition_id is not None and transition_name is not None:
                transitions_dict[transition_name] = transition_id
        second_key = list(transitions_dict.keys())[1] if transitions_dict else ""
        status_code = (
            transitions_dict.get("done", "31")
        )

        params = {"transition": {"id": status_code}}
        get_jira_response(
            url, "post", params, channel_config)
