"""
    Migration Notes From V2 to V3:
    No Migration Changes
"""

from slack import WebClient
import json
from copy import deepcopy

from dqlabs.utils.notifications import update_notification_status, get_client_details
from dqlabs.app_helper.dq_helper import get_client_domain
from dqlabs.app_constants.dq_constants import SUCCESS, FAILED
from dqlabs.app_helper.log_helper import log_error
from dqlabs.utils.notifications import get_summary_template


def send_slack_notification(notification_info: dict):
    try:
        channel_config = notification_info.get("channel_config", None)
        is_report = notification_info.get("is_report", False)
        is_workflow = notification_info.get("is_workflow", False)
        token = channel_config.get("access_token")
        channel_id = notification_info.get("channel_id", False)
        notification_channel_config = notification_info.get("notification_channel_config", {})
        is_summary_notification = notification_info.get("is_summary", False)

        if isinstance(notification_channel_config, str):
            notification_channel_config = json.loads(notification_channel_config)
        active_notification_channels = notification_channel_config[channel_id] if channel_id in notification_channel_config else []

        channels = channel_config.get("channels", [])
        if not is_workflow and not active_notification_channels:
            channels = [channel for channel in channels if channel.get("is_notify")]
        
        if active_notification_channels:
            channels = [channel for channel in channels if channel.get("id") in active_notification_channels]
    
        # Send Notification
        overall_status = SUCCESS
        slack_client = WebClient(token=token)
        for channel in channels:
            channel_id = channel.get("channel_id")
            if not channel_id:
                continue
            message = notification_info.get('content', {}).get('params', {}).get('message', "")
            if message:
                updated_message = message.replace('\r\n', '\n')
                notification_info['content']['params']['message'] = updated_message
            payload = prepare_payload(notification_info, is_summary_notification)
            for data in payload:
                try:
                    # Parse JSON string to Python object if data is a string
                    if isinstance(data, str):
                        data = json.loads(data)
                    slack_client.chat_postMessage(
                        channel=channel_id,
                        blocks=data
                    )
                except Exception as e:
                    overall_status = FAILED
                    log_error(f"Failed to send notification to {channel_id}:", e)
        
        if not is_report and not is_workflow:
            update_notification_status(notification_info, overall_status)
    except Exception as e:
        if not is_report:
            update_notification_status(notification_info, FAILED, str(e))
        log_error("send_notification : error to send slack notification", e)
        raise e


def prepare_payload(notification_info: dict, is_summary_notification: bool):
    try:
        sender_name = notification_info.get("sender_name", '')
        module_name = notification_info.get("module_name", "")
        payload = notification_info.get("body")
        content = notification_info.get("content", {})
        notification_params = content.get("params", {})
        attachments = notification_info.get("attachments", [])
        image = content.get("image", None)
        owner_name = content.get("owner_name", None)
        summary_notification_data = deepcopy(content.get("summary_notification_data", []))
        notitication_type = notification_info.get("notification_type", "")
        if notitication_type == "issue_update":
            module_name = notitication_type
       
        dag_info = notification_info.get("dag_info")
        client_domain = get_client_domain(dag_info)
        client_details = get_client_details(notification_info)

        def escape_json_value(value):
            if value is None:
                return ""
            str_value = str(value)
            escaped = json.dumps(str_value)
            return escaped[1:-1]

        for key in notification_params:
            value = notification_params[key] if notification_params[key] else ""
            payload = payload.replace(f"[{key}]", escape_json_value(value))

        for key in client_details:
            value = client_details[key] if client_details[key] else ""
            payload = payload.replace(f"[{key}]", escape_json_value(value))

        

        if owner_name:
            payload = payload.replace("[sender_name]", escape_json_value(owner_name))
        else:
            payload = payload.replace("[sender_name]", escape_json_value(sender_name))

        if image:
            payload = payload.replace(f"[attachment]", escape_json_value(image))

        if attachments and len(attachments) > 0:
            payload = payload.replace(
                f"[attachment]", escape_json_value(attachments[0].get("attachment", '')))

        if client_domain:
            payload = payload.replace(f"[domain]", escape_json_value(client_domain))

        if is_summary_notification and summary_notification_data:
            summary_template = get_summary_template(module_name, "slack")
            slack_contents = []
            batch_size = 20
            for i in range(0, len(summary_notification_data), batch_size):
                batch = summary_notification_data[i:i+batch_size]
                slack_content = ""
                for params in batch:
                    template_content = summary_template
                    for key in params:
                        value = params[key] if params[key] else ""
                        template_content = template_content.replace(f"[{key}]", value)
                    slack_content += template_content
                slack_contents.append(payload.replace("[summary_template]", slack_content))
            payload = slack_contents if slack_contents else payload
        else:
            payload = [payload]
        
        # Parse and re-serialize JSON to properly escape newlines and control characters
        result = []
        for item in payload:
            if isinstance(item, str):
                try:
                    parsed = json.loads(item)
                    result.append(json.dumps(parsed, ensure_ascii=False))
                except json.JSONDecodeError:
                    # Escape control characters inside string values only
                    fixed = []
                    in_string = False
                    escape_next = False
                    for char in item:
                        if escape_next:
                            fixed.append(char)
                            escape_next = False
                        elif char == '\\':
                            fixed.append(char)
                            escape_next = True
                        elif char == '"' and not escape_next:
                            in_string = not in_string
                            fixed.append(char)
                        elif in_string and char in ['\n', '\r', '\t']:
                            fixed.append('\\' + ('n' if char == '\n' else 'r' if char == '\r' else 't'))
                        else:
                            fixed.append(char)
                    
                    try:
                        parsed = json.loads(''.join(fixed))
                        result.append(json.dumps(parsed, ensure_ascii=False))
                    except (json.JSONDecodeError, Exception):
                        result.append(item)
            else:
                result.append(item)
        
        return result
    except Exception as e:
        log_error("send_slack_notification : prepare_payload", e)
        raise e
