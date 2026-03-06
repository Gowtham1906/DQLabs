import requests
import json
from copy import deepcopy

from dqlabs.utils.notifications import update_notification_status, get_client_details
from dqlabs.app_helper.dq_helper import get_client_domain
from dqlabs.app_constants.dq_constants import SUCCESS, FAILED
from dqlabs.app_helper.log_helper import log_error
from dqlabs.utils.notifications import get_summary_template


def send_google_notification(notification_info: dict, is_report:bool= False):
    try:
        channel_config = notification_info.get("channel_config", None)
        is_report = notification_info.get("is_report", False)
        is_workflow = notification_info.get("is_workflow", False)
        channels = channel_config.get("channels", [])
        channel_id = notification_info.get("channel_id", False)
        notification_channel_config = notification_info.get("notification_channel_config", {})
        is_summary_notification = notification_info.get("is_summary", False)

        if isinstance(notification_channel_config, str):
            notification_channel_config = json.loads(notification_channel_config)
        active_notification_channels = notification_channel_config[channel_id] if channel_id in notification_channel_config else []

        if not is_workflow and not active_notification_channels:
            channels = [channel for channel in channels if channel.get("is_notify")]
        
        if active_notification_channels:
            channels = [channel for channel in channels if channel.get("id") in active_notification_channels]

        overall_status = SUCCESS
        for channel in channels:
            webhook_url = channel.get("webhook_url")
            if not webhook_url:
                continue
            payload = prepare_payload(notification_info, is_summary_notification)
            for data in payload:
                try:
                    # Parse JSON string to Python object if data is a string
                    if isinstance(data, str):
                        data = json.loads(data)
                    response = requests.post(webhook_url, json=data)
                    if response.status_code != 200:
                        overall_status = FAILED
                        log_error(f"Failed to send notification to {webhook_url}, {response.text}", error=response.text)
                except json.JSONDecodeError as e:
                    overall_status = FAILED
                    log_error(f"Invalid JSON in payload for {webhook_url}. JSON error: {str(e)}", e)
                except Exception as e:
                    overall_status = FAILED
                    log_error(f"Failed to send notification to {webhook_url}", e)
        
        if not is_report and not is_workflow:
            update_notification_status(notification_info, overall_status)

    except Exception as e:
        log_error("send_notification : error to send google chat notification", e)
        if not is_report:
            update_notification_status(notification_info, FAILED, str(e))
        raise e


def prepare_payload(notification_info: dict, is_summary_notification: bool):
    try:
        sender_name = notification_info.get("sender_name", '')
        module_name = notification_info.get("module_name", "")
        payload = notification_info.get("body")
        content = notification_info.get("content", {})
        summary_notification_data = deepcopy(content.get("summary_notification_data", []))
        notitication_type = notification_info.get("notification_type", "")
        if notitication_type == "issue_update":
            module_name = notitication_type
        notification_params = content.get("params", {})
        image = content.get("image", None)
        owner_name = content.get("owner_name", None)
        client_details = get_client_details(notification_info)

        attachments = notification_info.get("attachments", [])

        dag_info = notification_info.get("dag_info")
        client_domain = get_client_domain(dag_info)

        def escape_json_value(value):
            if value is None:
                return ""
            str_value = str(value)
            escaped = json.dumps(str_value)
            return escaped[1:-1]

        for key in notification_params:
            value = notification_params[key] if notification_params[key] else ""
            if key == "error":
                value = json.dumps(value)
                value = value.replace('"', "")
            payload = payload.replace(f"[{key}]", escape_json_value(value))

        for key in client_details:
            value = client_details[key] if client_details[key] else ""
            payload = payload.replace(f"[{key}]", escape_json_value(value))
            

        if owner_name:
            payload = payload.replace("[sender_name]", escape_json_value(owner_name))
        else:
            payload = payload.replace("[sender_name]", escape_json_value(sender_name))

        payload = payload.replace("[measure_name]", escape_json_value(sender_name))

        if image:
            payload = payload.replace(f"[attachment]", escape_json_value(image))

        if attachments and len(attachments) > 0:
            payload = payload.replace(
                f"[attachment]", escape_json_value(attachments[0].get("attachment", '')))

        if client_domain:
            payload = payload.replace(f"[domain]", escape_json_value(client_domain))

        if is_summary_notification and summary_notification_data:
            summary_template = get_summary_template(module_name, "google_chat")
            google_contents = []
            batch_size = 9
            for i in range(0, len(summary_notification_data), batch_size):
                batch = summary_notification_data[i:i+batch_size]
                google_content = ""
                for params in batch:
                    template_content = summary_template
                    for key in params:
                        value = params[key] if params[key] else ""
                        template_content = template_content.replace(f"[{key}]", value)
                    google_content += template_content
                    if params != batch[-1]:
                        google_content += ","
                google_contents.append(payload.replace("[summary_template]", google_content))
            payload = google_contents if google_contents else payload
        else:
            payload = [payload]
        
        # Parse and re-serialize JSON to ensure proper escaping
        result = []
        for item in payload:
            if isinstance(item, str):
                try:
                    parsed = json.loads(item)
                    result.append(json.dumps(parsed, ensure_ascii=False))
                except json.JSONDecodeError:
                    result.append(item)
            else:
                result.append(item)
        
        return result
    except Exception as e:
        log_error("send_google_chat_notification : prepare_payload", e)
        raise e
