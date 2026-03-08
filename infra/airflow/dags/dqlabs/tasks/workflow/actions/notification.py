# Import Helpers
from dqlabs.app_helper.workflow_helper import get_notification_channel_template, get_user_emails
from dqlabs.utils.workflow import prepare_notification_payload
from dqlabs.tasks.notifications.email import send_mail_notification
from dqlabs.tasks.notifications.slack import send_slack_notification
from dqlabs.tasks.notifications.teams import send_teams_notification
from dqlabs.tasks.notifications.google_chat import send_google_notification

# Import Constant
from dqlabs.app_constants.dq_constants import (
    EMAIL_NOTIFICATION, SLACK_NOTIFICATION, TEAMS_NOTIFICATION, CREATE_ALERT, CREATE_ISSUE
)


def execute(config: dict, task: dict, input: dict, previous_task: dict):
    """
    Send Notification Via Workflow
    """

    previous_task_name = previous_task.get("technical_name")
    task_name = task.get("technical_name")
    workflow_name = config.get("workflow_name")
    configuration = task.get("configuration", {})
    task_config = configuration.get("config", {})
    users = task_config.get("users", [])
    channel_name = task_config.get("channel_name")
    
    if task_name == EMAIL_NOTIFICATION and users:
        users = get_user_emails(config, users)
        if not users:
            raise Exception("No Users Found")
    elif not channel_name and task_name != EMAIL_NOTIFICATION:
        raise Exception("No Channel Configure")
    
    template_name = get_template_name(previous_task_name, input)
    notification_configuration = get_notification_channel_template(config, task_name, template_name)
    if not notification_configuration:
        raise Exception("No Channel Configure")
    
    payload = prepare_notification_payload(config, input, previous_task_name)
    if not payload:
        raise Exception("Invalid Node Selection")
    payload.update({"to": users})
    
    if task_name != EMAIL_NOTIFICATION:
        channel_config = notification_configuration.get("channel_config", {})
        channels = channel_config.get("channels", [])
        channels = [channel for channel in channels if channel.get("channel_name") == channel_name]
        if not channels:
            raise Exception("Channel not found")
        notification_configuration.update({
            "channel_config": {
                **channel_config,
                "channels": channels
            }
        })

    dag_info = config.get("dag_info")
    notification_payload = {
        **notification_configuration,
        "sender_name": f"Workflow - {workflow_name}",
        "content": payload,
        "dag_info": dag_info,
        "is_workflow": True
    }
    if EMAIL_NOTIFICATION == task_name:
        send_mail_notification(notification_payload, config)
    elif SLACK_NOTIFICATION == task_name:
        send_slack_notification(notification_payload)
    elif TEAMS_NOTIFICATION ==  task_name:
        send_teams_notification(notification_payload)
    else:
        send_google_notification(notification_payload)
    return input


def get_template_name(task_name: str, input: dict):
    """
    Get Template Name
    """
    if task_name == CREATE_ALERT:
        if input.get("task_name") or input.get("task_id"):
            task_name = "new_alert_pipeline"
        else:
            task_name = "new_alert"
    elif task_name == CREATE_ISSUE:
        task_name =f"workflow_create_issue"
    return task_name