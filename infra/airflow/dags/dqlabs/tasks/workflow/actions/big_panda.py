import json
import requests

# Import Helpers
from dqlabs.app_helper.workflow_helper import get_integration_config, date_formatter
from dqlabs.app_helper.crypto_helper import decrypt
from dqlabs.app_helper.dq_helper import get_client_domain, get_client_origin
from dqlabs.utils.workflow import get_bigpanda_issue, get_alert_metric

# Import Constant
from dqlabs.app_constants.dq_constants import CREATE_ALERT, CREATE_ISSUE

def execute(config: dict, task: dict, input: dict, previous_task: str):
    bigpanda_config = get_integration_config(config, "bigpanda")
    
    if not bigpanda_config:
        raise Exception("No Integration Found")

    bigpanda_config = bigpanda_config.get("config")
    previous_task_name = previous_task.get("technical_name")
    
    if previous_task_name not in [CREATE_ISSUE, CREATE_ALERT]:
        raise Exception("Invalid Configuration")
    
    if previous_task_name == CREATE_ISSUE:
        manage_bigpanda_issue(config, bigpanda_config, input)
    elif previous_task_name == CREATE_ALERT:
        manage_bigpanda_alert(config, bigpanda_config, input)

    return input


def manage_bigpanda_issue(config: dict, bigpanda_config: dict, input: dict):
    issue = get_bigpanda_issue(config, input.get("id"))
    if issue.get("Measure_Name", "") == "Freshness":
        lt = date_formatter(issue.get("Lower_Threshold", "0"))
        ut = date_formatter(issue.get("Upper_Threshold", "0"))
        formatted_data = {
            "Lower_Threshold": lt,
            "Upper_Threshold": ut,
            "Expected": f"{lt} - {ut}",
            "Actual": date_formatter(issue.get("Actual", "0")),
        }
        issue = {**issue, **formatted_data}
    client_name = get_client_domain(config)
    bigpanda_params = {
        "app_key": decrypt(bigpanda_config.get("appkey")),
        "Source": client_name,
        "Type": "Issue",
        "primary_property": "Issue_ID",
        "secondary_property": "Issue_Description",
    }
    client_base_url = get_client_origin(config)
    issue_link = {"Issue_Link": f"""{client_base_url}/remediate/issues/{input.get("id")}"""}
    data = {**bigpanda_params, **issue, **issue_link}
    bigpanda_url = "data/v2/alerts"
    make_api_call(bigpanda_config, bigpanda_url, "post", data)


def manage_bigpanda_alert(config: dict, bigpanda_config: dict, input: dict):
    alert = get_alert_metric(config, input.get("metrics"))
    if alert:
        client_name = get_client_domain(config)
        client_origin = get_client_origin(dag_info)
        dag_info = config.get("dag_info")
        link = f"{client_origin}/measure/{alert.get('Measure_ID','')}/detail"

        # Prepare big Panda Alert Name
        asset_name = alert.get("Asset_Name", None)
        attribute_name = alert.get("Attribute_Name", None)
        measure_name = alert.get("Measure_Name", None)
        alert_name = ''
        if asset_name is not None:
            if attribute_name is not None:
                alert_name = f"{asset_name}_{attribute_name}_{measure_name}"
            else:
                alert_name = f"{asset_name}_{measure_name}"
        else:
            alert_name = measure_name
        payload = {
            "app_key": decrypt(bigpanda_config.get("appkey")),
            "Source": client_name,
            "Type": "Alert",
            "primary_property": "Alert_ID",
            "secondary_property": "Alert_Description",
            "Alert_ID": alert_name,
            "Alert_Link": link,
            **alert,
        }
        bigpanda_url = "data/v2/alerts"
        make_api_call(bigpanda_config, bigpanda_url, "post", payload)
    else:
        raise Exception("Alert Not Found")



def make_api_call(bigpanda_config: dict, url: str, method: str, payload: dict):
    try:
        api_token = decrypt(bigpanda_config.get("org_token", ""))
        endpoint = bigpanda_config.get("url")

        # Prepare Headers and Params
        api_headers = {
            "Content-Type": "application/json",
            "Accept": "*/*",
            "Authorization": "Bearer {}".format(api_token),
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
    