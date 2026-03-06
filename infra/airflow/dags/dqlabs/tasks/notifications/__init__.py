from copy import deepcopy
from dqlabs.utils.tasks import get_task_config
from dqlabs.utils.request_queue import update_queue_detail_status, update_queue_status
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.app_helper.log_helper import log_error

from dqlabs.app_constants.dq_constants import (
    EMAIL_NOTIFICATION, SLACK_NOTIFICATION, TEAMS_NOTIFICATION, SHARE_DASHBOARD, SHARE_WIDGET,
    REPORT, DASHBOARD, GOOGLE_CHAT_NOTIFICATION
)

from dqlabs.utils import get_organization_detail
from dqlabs.utils.notifications import get_notification, get_report_notification, get_notification_summary_data
from dqlabs.tasks.notifications.email import send_mail_notification
from dqlabs.tasks.notifications.slack import send_slack_notification
from dqlabs.tasks.notifications.teams import send_teams_notification
from dqlabs.tasks.notifications.google_chat import send_google_notification
from dqlabs.tasks.notifications.report_notification import send_report_notification, send_dashboard_notification


def handle_notification_tasks(config: dict, **kwargs):
    try:
        task_config = get_task_config(config, kwargs)
        update_queue_detail_status(
            config, ScheduleStatus.Running.value, task_config=task_config)
        update_queue_status(config, ScheduleStatus.Running.value, True)
        dag_info = config.get("dag_info")
        dag_info = dag_info if dag_info else {}
        organization_id = dag_info.get("organization_id")
        organization = get_organization_detail(organization_id, config)
        if organization and dag_info:
            dag_info.update({"organization": organization})
        config.update({"dag_info": dag_info})
        module_name = config.get('module_name', None)
        if module_name in [REPORT, DASHBOARD, SHARE_DASHBOARD, SHARE_WIDGET]:
            is_share = module_name in [SHARE_DASHBOARD, SHARE_WIDGET]
            channels, data = get_report_notification(config, is_share=is_share)
            if channels and data:
                if module_name in [REPORT, SHARE_WIDGET]:
                    send_report_notification(config, channels, data, is_share = (module_name == SHARE_WIDGET))
                elif module_name in [DASHBOARD, SHARE_DASHBOARD]:
                    send_dashboard_notification(config, channels, data, is_share = (module_name == SHARE_DASHBOARD))
            else:
                raise Exception("No Configuration Found")
        else:
            notification_id = config.get('notification_id', None)
            is_summary = config.get('is_summary', False)
            notification_type = config.get('notification_type', None)
            if notification_id:
                notification_module = module_name if is_summary else ""
                if notification_type == "issue_update" and is_summary:
                    notification_module = notification_type
                notifications = get_notification(config, notification_id, module=notification_module)
                summary_data = {}
                if is_summary:
                    summary_data = get_notification_summary_data(config, notification_id)
                    summary_data = summary_data if summary_data else {}
                for notification in notifications:
                    handle_notification_channels(config, notification, summary_data)
            else:
                raise Exception(f"Missing Notification Details by Id")

        update_queue_detail_status(config, ScheduleStatus.Completed.value)
    except Exception as e:
        log_error(f"Handle Notification Tasks : {str(e)}", e)
        update_queue_detail_status(
            config, ScheduleStatus.Failed.value, f"Handle Notification Tasks : {str(e)}")
        update_queue_status(config, ScheduleStatus.Failed.value)
    finally:
        update_queue_status(config, ScheduleStatus.Completed.value)


def handle_notification_channels(config: dict, notification: dict, summary_data: dict):
    try:
        channel_type = notification.get('channel_name')
        noty_config = {**config, **notification}
        if summary_data:
            summary_notification_data = deepcopy(summary_data.get('notification_data', []))
            users = summary_data.get('users', [])
            content = noty_config.get('content', {})
            content.update({
                "summary_notification_data": summary_notification_data,
                "to": users
            })
            noty_config.update({
                "cotent": content,
                "notification_channel_config": summary_data.get('notification_channel_config', {})
            })
        if EMAIL_NOTIFICATION == channel_type:
            send_mail_notification(noty_config, config)
        elif SLACK_NOTIFICATION == channel_type:
            send_slack_notification(noty_config)
        elif TEAMS_NOTIFICATION == channel_type:
            send_teams_notification(noty_config)
        elif GOOGLE_CHAT_NOTIFICATION == channel_type:
            send_google_notification(noty_config)
        else:
            raise Exception(
                f"Invalid Channel Type in Notifications. {channel_type}")
    except Exception as e:
        log_error(f"Handle Notification Tasks Channels : {str(e)}", e)
