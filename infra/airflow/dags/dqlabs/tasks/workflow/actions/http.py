# Import Helpers
from dqlabs.app_helper.workflow_helper import get_workflow_api_token, prepare_workflow_payload, make_request

def execute(config: dict, task: dict, input: dict, previous_task: dict):
    api_token = config.get("api_token")
    if not api_token:
        api_token = get_workflow_api_token(config)
        config.update({"api_token": api_token})

    # Prepare API Configuration
    task_name = task.get("technical_name")
    configuration = task.get("configuration", {})
    task_config = configuration.get("config", {})
    http_configuration = configuration.get("configuration", {})
    api_url = http_configuration.get("url")
    method = http_configuration.get("method", "post")
    is_fetch_result = http_configuration.get("is_fetch_result")
    payload_columns = http_configuration.get("payload_columns", [])
    output = {}
    payload = prepare_workflow_payload(task_name, input, payload_columns, task_config)
    payload.update({"is_workflow": True})

    # Update Worflow Configuration
    workflow_configuration = configuration.get("config", {})
    if workflow_configuration:
        payload.update({"workflow_configuration": workflow_configuration})
    result = make_request(config, api_url, method, payload)
    if is_fetch_result and result:
        output = fetch_result(config, http_configuration, result)
    return output

def fetch_result(config: dict, configuration: dict, data: dict):
    id = data.get("id")
    api_url = configuration.get("input_url", "").replace("<id>", id)
    method = configuration.get("fetch_method", "get")
    result = make_request(config, api_url, method)
    return result
