import json
import requests

# Import Helpers
from dqlabs.app_helper.workflow_helper import get_integration_config, get_user
from dqlabs.app_helper.crypto_helper import decrypt
from dqlabs.app_helper.log_helper import log_error
from dqlabs.app_helper.dq_helper import get_client_domain, get_client_origin
from dqlabs.utils.workflow import get_alert_metric
from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.db_helper import execute_query

# Import Constant
from dqlabs.app_constants.dq_constants import CREATE_ALERT, CREATE_ISSUE

def execute(config: dict, task: dict, input: dict, previous_task: str):
    servicenow_config = get_integration_config(config, "servicenow")
    
    if not servicenow_config:
        raise Exception("No Integration Found")

    servicenow_config = servicenow_config.get("config")
    previous_task_name = previous_task.get("technical_name")
    
    if previous_task_name not in [CREATE_ISSUE, CREATE_ALERT]:
        raise Exception("Invalid Configuration")
    
    if previous_task_name == CREATE_ISSUE:
        create_issue(config, servicenow_config, input)
    elif previous_task_name == CREATE_ALERT:
        create_alert(config, servicenow_config, input)
    return input

def create_issue(config: dict, servicenow_config: dict, input: dict, is_pipeline: bool = False):
    # Extract Data
    priority = input.get("priority")
    applications = input.get("applications", [])
    domains = input.get("domains", [])
    products = input.get("products", [])
    description = input.get("description")

    # Get User
    user = {}
    if input.get("created_by"):
        user = get_user(config, input.get("created_by"))
    
    # Prepare List to string
  
    applications = ",".join([application.get("name") for application in applications])
    domains = ",".join([domain.get("name") for domain in domains])
    products = ",".join([product.get("name") for product in products])
 
    priority_code = "1" if priority == "High" else "2" if priority == "Medium" else "3"
    input_prefix = servicenow_config.get("input_prefix", "")
    if "ISSUE ID" in description:
        service_now_description = description
    else:
        service_now_description = f"""
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
    payload = {

        f"{input_prefix}short_description":  input.get("name"),
        f"{input_prefix}description": service_now_description,
        f"{input_prefix}urgency": priority_code,
        f"{input_prefix}category": "inquiry"
    }
    url = servicenow_config.get("issue_url") if servicenow_config.get("issue_url") else "/api/now/table/incident"
    response = make_api_call(servicenow_config, url, "post", payload)
    if response:
        servicenow_id = response.get("number")
        servicenow_id = servicenow_id if servicenow_id else response.get("sys_target_sys_id",{}).get("display_value","")
        if ":" in servicenow_id:
            servicenow_id = servicenow_id.split(":")[1].strip()
        sys_id = response.get("sys_id")
        if is_pipeline:
            update_issue_description(config, service_now_description, response, input)
        else:
            external_id = input.get("external_id", {})
            external_id["servicenow_id"] = servicenow_id
            external_list = input.get("external_list", {})
            external_list["servicenow"] = sys_id
            input.update({"external_description": service_now_description, "external_list": external_list})


def create_alert(config: dict, servicenow_config: dict, input: dict):
    alert = get_alert_metric(config, input.get("metrics"))
    if not alert:
        raise Exception("Alert not found")
    
    # Prepare Payload
    client_name = get_client_domain(config)
    client_origin = get_client_origin(dag_info)
    dag_info = config.get("dag_info")
    link = f"{client_origin}/measure/{alert.get('Measure_ID','')}/detail"
    servicenow_code = {
        "critical": "1", "warning": "2", "unknown": "3"
    }
   
    # Prepare servicenow Alert Name
    asset_name = alert.get("Asset_Name", None)
    attribute_name = alert.get("Attribute_Name", None)
    measure_name = alert.get("Measure_Name", None)
    alert_name = ''
    if asset_name:
        if attribute_name:
            alert_name = f"{asset_name}_{attribute_name}_{measure_name}"
        else:
            alert_name = f"{asset_name}_{measure_name}"
    else:
        alert_name = measure_name
    alert.update({"Alert_ID": alert_name})
    input_prefix = servicenow_config.get("input_prefix", '')
    payload = {
            f"{input_prefix}Type": "Alert",
            f"{input_prefix}primary_property": "Alert_ID",
            f"{input_prefix}secondary_property": "Alert_Description",
            f"{input_prefix}source": client_name, 
            f"{input_prefix}alert_type": 'Alert',
            f"{input_prefix}severity": '3',
            f"{input_prefix}state": '1',
            f"{input_prefix}Alert_ID": alert_name,
            f"{input_prefix}description": alert.get('Alert_Description'),
            f"{input_prefix}severity": servicenow_code.get(alert.get('status'), "3"),
            f"{input_prefix}Alert_Link": link,
            **alert
        }
    url = servicenow_config.get("alert_url","") if servicenow_config.get("alert_url") else  "/api/now/table/em_alert"
    response = make_api_call(config, url, "post", payload)
    if response:
        servicenow_id = response.get("number")
        servicenow_id = servicenow_id if servicenow_id else response.get("sys_target_sys_id",{}).get("display_value","")
        if ":" in servicenow_id:
            servicenow_id = servicenow_id.split(":")[1].strip()
        if servicenow_id:
            external_id = f'{{"servicenow_id": "{servicenow_id}"}}'
            update_query = f"""
                UPDATE core.metrics 
                SET external_id = '{external_id}'
                WHERE id = '{alert.get("Metrics_Id")}'
            """
            connection = get_postgres_connection(config)
            with connection.cursor() as cursor:
                execute_query(connection, cursor, update_query)


def make_api_call(config, endpoint, method, params):
    # from requests.auth import HTTPBasicAuth
    api_response = None
    try:
        if config.get("authentication_type") == 'OAuth Authentication':
            api_response = oauth_response(config, endpoint, method, params)
        else :
            api_response = service_password_response(config, endpoint, method, params)

        # Check the response
        if api_response.ok:
            response = api_response.json()
            api_response = response.get("result")
        else:
            response.raise_for_status()
    except Exception as exp:
        log_error(
            f"ServiceNow Get Response Failed - {str(exp)}", exc_info=True
        )  
        raise Exception(exp)
    finally:
        return api_response
    
def service_password_response(config, url, method_type, data):
    instance_url = config.get("url")
    username = decrypt(config.get("username"))
    password = decrypt(config.get("password"))

    if not username or not password or not instance_url:
        raise Exception("Missing credentials")
    
    # API endpoint (e.g., fetching sys_user records)
    if instance_url and instance_url.endswith("/"):
        instance_url = "/".join(instance_url.split("/")[:-1])
    endpoint = f"{instance_url}{url}"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    # Make the API request
    data = json.dumps(data)
    if method_type == 'post':
        response = requests.post(endpoint, auth=requests.auth.HTTPBasicAuth(username, password), headers=headers, data=data)
    elif method_type == 'put':
        response = requests.put(endpoint, auth=requests.auth.HTTPBasicAuth(username, password), headers=headers, data=data)
    else:
        response = requests.get(endpoint, auth=requests.auth.HTTPBasicAuth(username, password), headers=headers)

    return response


def oauth_response(config, url, method, params):
    try:
        # Retrieve the access token from refresh token
        access_token = get_access_token_from_refresh_token(config)
        instance_url = config.get("url")
        if not access_token:
            raise Exception("Unable to get access token using refresh token.")
        
        # API endpoint (e.g., fetching sys_user records)
        if instance_url and instance_url.endswith("/"):
            instance_url = "/".join(instance_url.split("/")[:-1])
        endpoint = f"{instance_url}{url}"
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}"  # Using the access token here
        }

        # Make the API request based on the method type
        data = json.dumps(params) if params else {}
        if method == 'post':
            response = requests.post(endpoint, headers=headers, data=data)
        elif method == 'put':
            response = requests.put(endpoint, headers=headers, data=data)

        return response
    except Exception as e:
        log_error(
            f"ServiceNow Get Response Failed - {str(e)}", exc_info=True
        )
        raise Exception(e)
        
    
def get_access_token_from_refresh_token(config: dict):
    # Token endpoint URL
    instance_url = config.get("url")
    client_id = decrypt(config.get("client_id"))
    client_secret = decrypt(config.get("client_secret"))
    refresh_token = decrypt(config.get("refresh_token"))
    token_url = f"{instance_url}/oauth_token.do"

    # Prepare the data to exchange refresh token for a new access token
    data = {
        "grant_type": "refresh_token",
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token
    }

    try:
        # Request to get the new access token
        response = requests.post(token_url, data=data)
        if response.ok:
            token_data = response.json()
            access_token = token_data.get("access_token")
            return access_token
        else:
            log_error(f"Failed to get access token. Status code: {response.status_code}")
            raise Exception(e)
    except Exception as e:
        log_error(f"Error getting access token: {str(e)}")
        raise Exception(e)
        

def update_issue_description(config: dict, description: str, response: dict, input: dict):
    service_now_description  = description
    servicenow_id = response.get("number")
    servicenow_id = servicenow_id if servicenow_id else response.get("sys_target_sys_id",{}).get("display_value","")
    if ":" in servicenow_id:
        servicenow_id = servicenow_id.split(":")[1].strip()
    sys_id = response.get("sys_id")
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f""" select external_id from core.issues  WHERE id = '{input.get("id")}' """
        cursor = execute_query(connection, cursor, query_string)
        external_id_jira = cursor.fetchone()
        external_id_value = external_id_jira[0] if external_id_jira and external_id_jira[0] else None
        external_id = external_id_value if external_id_value  else {}
        external_id["servicenow_id"] = servicenow_id
        external_list = input.get("external_list", {})
        external_list["servicenow"] = sys_id
        external_id_str = json.dumps(external_id)
        external_id_str = external_id_str.replace("'", "''")
        connection = get_postgres_connection(config)
        query_string = f"""SELECT description FROM core.issues WHERE id = '{input.get("id")}'"""
        cursor = execute_query(connection, cursor, query_string)
        result = cursor.fetchone()
        if "JIRA ID:"  in result[0]:
            service_now_description = result[0]
        if "SERVICE NOW ID:" not in service_now_description:
            service_now_description = service_now_description.rstrip() + f"\n       SERVICE NOW ID: '{sys_id}'"
        if "SERVICENOW INCIDENT NUMBER:" not in service_now_description:
            service_now_description = service_now_description.rstrip() + f"\n       SERVICENOW INCIDENT NUMBER: '{servicenow_id}'"
        service_now_description =service_now_description.replace("'", "''")
        query_string = f""" UPDATE core.issues SET external_id ='{external_id_str}' , description = '{service_now_description}' WHERE id = '{input.get("id")}' """
        cursor = execute_query(connection, cursor, query_string)