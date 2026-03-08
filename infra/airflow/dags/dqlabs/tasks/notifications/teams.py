"""
    Migration Notes From V2 to V3:
    No Migration Changes
"""

import json
import os
import requests
import pymsteams
from copy import deepcopy

from dqlabs.utils.notifications import update_notification_status, get_client_details
from dqlabs.app_helper.dq_helper import get_client_domain, log_info
from dqlabs.app_constants.dq_constants import SUCCESS, FAILED
from dqlabs.app_helper.log_helper import log_error
from dqlabs.utils.notifications import get_summary_template


def send_teams_notification(notification_info: dict, is_report:bool= False):
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
        if channel_config.get("authentication_type") == "WebHook Url":
                for channel in channels:
                    webhook_url = channel.get("webhook_url")
                    if not webhook_url:
                        continue
                    try:
                        payload = prepare_payload(notification_info, is_summary_notification)
                        teams = pymsteams.connectorcard(webhook_url)
                        for data in payload:
                            teams.text(data)
                            teams.send()
                    except Exception as e:
                        overall_status = FAILED
                        log_error(f"Failed to send notification to {webhook_url}: {e}")
        elif channel_config.get("authentication_type") == "Native App":
            try:
                for channel in channels:
                    payload = prepare_payload(notification_info, is_summary_notification)
                    channel_id = channel.get("channel_id")
                    
                    if not channel_id:
                        raise ValueError("channel_id is missing in the payload")
                    
                    # Prepare the POST request
                    url = os.environ.get("TEAMSNATIVEAPP_ENDPOINT")
                    headers = {
                        "Content-Type": "application/json"
                    }

                    for data in payload:
                        data = data.replace('True', 'true').replace('False', 'false')
                        data = data.replace('\\', '').replace('\n', '').replace('\r', '').replace('\t', '')
                        data = json.loads(data)
                        
                        post_payload = {
                            "channel_id": channel_id,
                            "message": data
                        }
                        
                        
                        # Send the POST request
                        response = requests.post(url, json=post_payload, headers=headers)
                    
                        # Check the response status
                        if response.status_code in [200, 201]:
                            log_info("Notification sent successfully")
                        else:
                            raise Exception(f"Failed to send notification: {response.text}")

            except Exception as e:
                overall_status = FAILED
                log_error("Failed to send notification", e)

        if not is_report and not is_workflow:
            update_notification_status(notification_info, overall_status)

    except Exception as e:
        log_error("send_notification : error to send teams notification", e)
        if not is_report:
            update_notification_status(notification_info, FAILED, e)
        raise e


def prepare_payload(notification_info: dict, is_summary_notification: bool):
    try:
        sender_name = notification_info.get("sender_name", '')
        module_name = notification_info.get("module_name", "")
        channel_name = "teams"
        if notification_info.get("channel_config", {}).get("authentication_type") == "Native App":
            payload = notification_info.get("json_data", {})
            channel_name = "teams_native"
        else:
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

        for key in notification_params:
            value = notification_params[key] if notification_params[key] else ""
            payload = payload.replace(f"[{key}]", value)

        for key in client_details:
            value = client_details[key] if client_details[key] else ""
            payload = payload.replace(f"[{key}]", value)

        

        if owner_name:
            payload = payload.replace("[sender_name]", owner_name)
        else:
            payload = payload.replace("[sender_name]", sender_name)

        payload = payload.replace("[measure_name]", sender_name)

        if image:
            payload = payload.replace(f"[attachment]", image)

        if attachments and len(attachments) > 0:
            payload = payload.replace(
                f"[attachment]", f"""{attachments[0].get("attachment", '')}""")

        if client_domain:
            payload = payload.replace(f"[domain]", client_domain)

        if is_summary_notification and summary_notification_data:
            summary_template = get_summary_template(module_name, channel_name)
            teams_contents = []
            batch_size = 15
            for i in range(0, len(summary_notification_data), batch_size):
                batch = summary_notification_data[i:i+batch_size]
                teams_content = ""
                for params in batch:
                    template_content = summary_template
                    for key in params:
                        value = params[key] if params[key] else ""
                        template_content = template_content.replace(f"[{key}]", value)
                    teams_content += template_content
                    if params != batch[-1] and channel_name == "teams_native":
                        teams_content += ","
                teams_contents.append(payload.replace("[summary_template]", teams_content))
            payload = teams_contents if teams_contents else payload
        else:
            payload = [payload]
        return payload
    except Exception as e:
        log_error("send_teams_notification : prepare_payload", e)
        raise e
