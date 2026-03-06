import importlib
from concurrent.futures import ThreadPoolExecutor, wait
import fnmatch

from dqlabs.tasks.workflow.actions import http, notification, jira, service_now, big_panda

from dqlabs.utils.request_queue import update_workflow_task_queue_status
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.app_helper.log_helper import log_error
from dqlabs.app_helper.workflow_helper import get_value, update_issue_integration
from dqlabs.utils.tasks import check_workflow_task_status


def execute_workflow(config: dict, workflow_tasks: list):
    is_completed = check_workflow_task_status(config)
    if is_completed:
        return
    workflow_input = config.get("workflow_input", {})
    trigger_task = next((task for task in workflow_tasks if task.get("is_trigger")), None)
    task_name = trigger_task.get("technical_name")
    inputs = {task_name: workflow_input}
    update_workflow_task_queue_status(config, trigger_task, workflow_input, status=ScheduleStatus.Completed.value)
    # Execute and manage queue of tasks
    manage_task_queue(config, trigger_task, workflow_tasks, inputs)


def check_workflow_condition(input: dict, filter: dict):
    field = filter.get("field")
    filter_value = filter.get("value")
    value = get_value(field, input)
    value = value if value else ""
    value = value.lower() if isinstance(value, str) else value

    if isinstance(value, list):
        if all(isinstance(item, dict) for item in value):
            value = [item.get("id") for item in value]
            
    if isinstance(filter_value, list):
        if all(isinstance(item, dict) for item in filter_value):
            filter_value = [item.get("id") for item in filter_value]
        
        if field == "measure":
            filter_value = [item.lower() if isinstance(item, str) else item for item in filter_value]
        
        if isinstance(value, list):
            return any(val in value for val in filter_value)
        return value in filter_value
    else:
        filter_value = filter_value.lower()
        if "*" in filter_value:
            return fnmatch.fnmatch(value, filter_value)
        return value == filter_value


def filter_workflow_tasks(tasks: list, filters: dict, current_task: str, input: dict):
    next_taks = [task for task in tasks if task.get("parent_id") == current_task.get("id")]
    filter_tasks = []
    for task in next_taks:
        task_id = task.get("id")
        task_filter = filters.get(task_id, [])
        condition = True
        if task_filter:
            condition = all(check_workflow_condition(input, filter) for filter in task_filter)
        if condition:
            filter_tasks.append(task)
    return filter_tasks

def manage_task_queue(config:dict, current_task: dict, tasks: list, inputs: dict):
    configuration = current_task.get("configuration", {})
    workflow_filters = configuration.get("filters", {})
    input = inputs[current_task.get("technical_name")]
    next_tasks = filter_workflow_tasks(tasks, workflow_filters, current_task, input)
    if next_tasks:
        with ThreadPoolExecutor(len(next_tasks)) as executor:
            futures = []
            for task in next_tasks:
                futures.append(executor.submit(execute_task, config, task, tasks, current_task, inputs))
            wait(futures)
            executor.shutdown()
        update_execution_data(config, current_task, next_tasks, input)


def execute_task(config:dict, task: dict, tasks: list, previous_task: dict, inputs: dict):
    task_name = task.get("name")
    previous_task_name = previous_task.get("technical_name")
    task_input = inputs[previous_task_name]
    update_workflow_task_queue_status(config, task, task_input)
    output = {}
    try:
        configuration = task.get("configuration", {})
        action_type = configuration.get("action_type") if "action_type" in configuration else task.get("technical_name")
        module = importlib.import_module(f"dqlabs.tasks.workflow.actions.{action_type}")
        output = module.execute(config, task, task_input, previous_task)
        inputs[task.get("technical_name")] = output
        update_workflow_task_queue_status(config, task, task_input, output=output, status=ScheduleStatus.Completed.value)
        manage_task_queue(config, task, tasks, inputs)
    except Exception as e:
        error = str(e)
        update_workflow_task_queue_status(config, task, task_input, output=output, error=error, status=ScheduleStatus.Failed.value)
        log_error(f"Run workflow task {task_name}", str(e))
    
def update_execution_data(config: dict, task: dict, next_tasks: dict, input: dict):
    try:
        if task.get("technical_name") == "create_issue":
            external_tasks = [item for item in next_tasks if item.get("technical_name") in ["jira", "service_now"]]
            if external_tasks:
                update_issue_integration(config, input)
    except Exception as e:
        log_error(f"Update", str(e))