import base64
import json
import requests

# Import Helpers
from dqlabs.app_helper.workflow_helper import get_integration_config, get_user
from dqlabs.app_helper.crypto_helper import decrypt
from dqlabs.app_helper.dag_helper import  get_postgres_connection
from dqlabs.app_helper.db_helper import execute_query

def execute(config: dict, task: dict, input: dict, previous_task: str):
    jira_config = get_integration_config(config, "jira")
    workflow_project_id = task.get("configuration", {}).get("config", {}).get("project_id")
    workflow_issue_type = task.get("configuration", {}).get("config", {}).get("issue_type", "Task")
    workflow_priority_mapping = task.get("configuration", {}).get("config", {}).get("priority_mapping", {
        "High": "2",    
        "Medium": "3",
        "Low": "4"
    })
    workflow_priority_code = workflow_priority_mapping.get(input.get("priority"), "4")
    previous_task_name = previous_task.get("technical_name")
    if not jira_config:
        raise Exception("No Integration Found")
    jira_config = jira_config.get("config")
    jira_config.update({
    "workflow_project_id": workflow_project_id,
    "workflow_issue_type": workflow_issue_type,
    "workflow_priority_code": workflow_priority_code,
    })
    
    if previous_task_name not in ["create_issue"]:
        raise Exception("Invalid Configuration")

    if previous_task_name == "create_issue":
        create_issue(config, jira_config, input)
    return input

    

def create_issue(config: dict, jira_config: dict, input: dict, is_pipeline: bool = False):
 
    # Get User
    user = {}
    if input.get("created_by"):
        user = get_user(config, input.get("created_by"))

    # Extract Data
    if is_pipeline and not jira_config.get("workflow_project_id"):
        project_key = jira_config.get("project_id", {}) if jira_config.get("project_id") else ''
    else:
        project_key = jira_config.get("workflow_project_id", {}) if jira_config.get("workflow_project_id") else ''
    project_key = project_key.get("key") if project_key else ''
    priority = input.get("priority")
    applications = input.get("applications", [])
    domains = input.get("domains", [])
    products = input.get("products", [])
    description = input.get("description")

    # Prepare List to string
    applications = ",".join([application.get("name") for application in applications])
    domains = ",".join([domain.get("name") for domain in domains])
    products = ",".join([product.get("name") for product in products])
    # Prepare Payload
    
    priority_code = jira_config.get("workflow_priority_code", "4")
    if "ISSUE ID" in description:
        jira_description = description
    else:
        jira_description = f"""
        ASSET: {input.get('asset_name')}
        ATTRIBUTE: {input.get('attribute_name')}
        MEASURE: {input.get('measure_name')}
        CONNECTION TYPE: {input.get('connection_type')}
        CONNECTION NAME: {input.get('connection_name')}
        CREATED BY: {user.get('first_name')} {user.get('last_name')}
        ISSUE KEY : {input.get('issue_id')}
        ASSET ID: {input.get('asset_id')}
        ATTRIBUTE ID: {input.get('attribute_id')}
        MEASURE ID: {input.get('measure_id')}
        APPLICATION: {applications}
        DOMAIN: {domains}
        PRODUCT: {products}
        ISSUE ID: {input.get("id")}
        CREATOR USER ID: {input.get('created_by')}
        """
    issue_type = jira_config.get("workflow_issue_type", "Task")
    payload = {
        "fields": {
            "project": {"key": project_key},
            "summary": input.get("name"),
            "description": jira_description,
            "issuetype": {"name": issue_type},
            "priority": {"id": priority_code},
        }
    }
    url = "rest/api/2/issue"
    response = make_api_call(jira_config, url, "post", payload)
    if response:
        jira_id = response.get("id")
        jira_key = response.get("key")
        if is_pipeline:
            update_jira_detail(config, jira_description, response, input.get("id"))
        else:
            external_id = input.get("external_id", {})
            external_description = input.get("external_description")
            if not external_description:
                external_description = jira_description
            external_id["jira_id"] = jira_id
            external_list = input.get("external_list", {})
            external_list["jira"] = jira_key
            input.update({"external_description": jira_description, "external_list": external_list})


def make_api_call(jira_config: dict, url: str, method: str, payload: dict):
    try:
        username = decrypt(jira_config.get("username"))
        api_token = decrypt(jira_config.get("apikey", ""))
        authentication_key = f"{username}:{api_token}"
        encoded_authentication = authentication_key.encode("utf-8")
        authentication = base64.b64encode(encoded_authentication)
        authentication = authentication.decode("utf-8")
        endpoint = jira_config.get("url")

        # Prepare Headers and Params
        api_headers = {
            "Content-Type": "application/json",
            "Accept": "*/*",
            "Authorization": "Basic {}".format(authentication),
        }
        endpoint = f"{endpoint}/{url}"
        if not api_token:
            raise Exception("Missing Api Key")
        
        # Make Call
        if method == "post":
            response = requests.post(url=endpoint, headers=api_headers, data=json.dumps(payload))
        elif method == "put":
            response = requests.put(url=endpoint, headers=api_headers, data=json.dumps(payload))
        
        if response.ok:
            response = response.json()
        else:
            response.raise_for_status()
        return response
    except Exception as e:
        raise e
    

def update_jira_detail(config: dict, description: str, response: dict, issue_id: str):
    jira_id = response.get("id")
    jira_key = response.get("key")
    if "JIRA ID:" not in description:
        description += f"JIRA ID: '{jira_key}'"
        external_id ={}
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            external_id["jira_id"] = jira_id
            external_id_str = json.dumps(external_id)
            external_id_str = external_id_str.replace("'", "''")
            jira_description = description.replace("'", "''")
            query_string = f""" UPDATE core.issues SET external_id ='{external_id_str}' , description = '{jira_description}' WHERE id = '{issue_id}' """
            cursor = execute_query(connection, cursor, query_string)
