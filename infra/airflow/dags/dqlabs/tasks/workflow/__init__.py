from dqlabs.utils.request_queue import  update_workflow_queue_status
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.app_helper.workflow_helper import get_workflow_tasks
from dqlabs.app_helper.log_helper import log_error
from dqlabs.tasks.workflow.execution import execute_workflow
from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.db_helper import execute_query, split_queries


def manage_workflow_tasks(config: dict, **kwargs):
    try:
        update_workflow_queue_status(config, ScheduleStatus.Running.value)
        workflow_id = config.get("workflow_id")
        if workflow_id:
            workflow_tasks = get_workflow_tasks(config)
            execute_workflow(config, workflow_tasks)
        else:
            raise Exception("No Configuration Found")
        update_workflow_queue_status(config, ScheduleStatus.Completed.value)
    except Exception as e:
        log_error(f"Handle Workflow Tasks : {str(e)}", e)
        update_workflow_queue_status(config, ScheduleStatus.Failed.value)