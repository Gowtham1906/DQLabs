"""
Description:
    The dqlabs_airflow_plugin.py file defines a custom Airflow plugin named DQLabsPlugin.
    This plugin listens for DAG run events and pushes the execution metadata to DQlabs.
    It extracts detailed information about DAGs, tasks, and their lineage,
    and sends this data to a specified endpoint for further analysis and monitoring.

Usage:
    To use the DQLabsPlugin, Add the dqlabs_airflow_plugin.py file to your Airflow plugins directory. Also ensure that the required DQlabs configuration variables are set in your Airflow environment.
    Include the following variables into Admin -> Variables:
        1. dq_endpoint_url
        2. dq_client_id
        3. dq_client_secret
        4. dq_connection_name
        5. dq_allowed_dags
        6. dq_excluded_dags
        7. dq_extract_source_code

    The dq_allowed_dags and dq_excluded_dags variables are the comma separated values which are used to control which DAGs are allowed to push metadata to DQlabs.

    Here is the Sample:
        # Ensure the following variables are set in your Airflow Variables:
        dq_endpoint_url="https://your-dqlabs-endpoint.com"
        dq_client_id="your-client-id"
        dq_client_secret="your-client-secret"
        dq_connection_name="your-delabs-connection-name"
        dq_allowed_dags="dag_id_1,dag_id_*"
        dq_excluded_dags="excluded_dag_ids"
        dq_extract_source_code="True"

Functions:
    push_dq_metadata: Pushes DAG execution metadata to DQlabs.
    on_dag_run_failed: Callback function that is triggered when a DAG run fails.
    on_dag_run_success: Callback function to be executed when a DAG run is successful.

Dependencies:
    No third party libraries are required to be installed.
    The plugin uses the built-in Airflow and Python libraries.
"""

# Import Modules
import os
import json
import fnmatch
import logging
import logging.handlers
import requests
from typing import List
from airflow.settings import Session
from airflow.plugins_manager import AirflowPlugin
from airflow.models import DAG, DagModel, DagRun, Variable, TaskInstance
from airflow.listeners import hookimpl


# Import Variables
LOG_LEVEL = logging.INFO
LOG_BACKUP_COUNT = 10
DQ_ENDPOINT_URL = Variable.get("dq_endpoint_url", default_var="")
DQ_CLIENT_ID = Variable.get("dq_client_id", default_var="")
DQ_CLIENT_SECRET = Variable.get("dq_client_secret", default_var="")
DQ_CONNECTION = Variable.get("dq_connection_name", default_var="")
DQ_ALLOWED_DAGS = Variable.get("dq_allowed_dags", default_var="")
DQ_EXCLUDED_DAGS = Variable.get("dq_excluded_dags", default_var="")
DQ_EXTRACT_SOURCE_CODE = Variable.get("dq_extract_source_code", default_var="")


class DQRunListener:
    """
    A listener class that listens for DAG run events and pushes the execution metadata to DQlabs.
    """

    def __init__(self):
        self.logger: logging = None
        self.__extract_source_code = False

    def get_logger(self):
        """
        Initializes and returns a logger instance for the class.

        Returns:
            logging.Logger: Configured logger instance.
        """
        log_dir = os.path.join(
            os.path.join(os.path.dirname(os.path.dirname(__file__))),
            "logs",
            "dqlabs_plugin",
        )
        log_file_path = os.path.join(log_dir, "dqlabs_plugin.log")
        formatter = logging.Formatter(
            "%(asctime)s — %(name)s — %(levelname)s — %(funcName)s:%(lineno)d — %(message)s"
        )
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        if not self.logger:
            self.logger = logging.getLogger(__name__)
            self.logger.setLevel(LOG_LEVEL)

        print("log_file_path", log_file_path)
        self.logger.handlers.clear()
        log_handler = logging.handlers.TimedRotatingFileHandler(
            filename=log_file_path,
            backupCount=LOG_BACKUP_COUNT,
            when="midnight",
        )
        log_handler.setLevel(LOG_LEVEL)
        log_handler.setFormatter(formatter)
        self.logger.addHandler(log_handler)

    def check_is_allowed(self, dag_id: str) -> bool:
        """
        Checks if a given DAG ID is allowed to run based on predefined variables and conditions.

        Args:
            dag_id (str): The ID of the DAG to check.

        Returns:
            bool: True if the DAG is allowed to run, False otherwise.
        """

        self.__extract_source_code = str(DQ_EXTRACT_SOURCE_CODE).lower() == "true"
        has_all_variables = all(
            [
                DQ_CONNECTION,
                DQ_ENDPOINT_URL,
                DQ_CLIENT_ID,
                DQ_CLIENT_SECRET,
            ]
        )
        allowed_dags = (
            DQ_ALLOWED_DAGS.split(",")
            if DQ_ALLOWED_DAGS and len(str(DQ_ALLOWED_DAGS).strip()) > 0
            else []
        )
        allowed_dags = allowed_dags if allowed_dags else []

        excluded_dags = (
            DQ_EXCLUDED_DAGS.split(",")
            if DQ_EXCLUDED_DAGS and len(str(DQ_EXCLUDED_DAGS).strip()) > 0
            else []
        )
        excluded_dags = excluded_dags if excluded_dags else []

        allow_all_dags = not allowed_dags and not excluded_dags
        has_allowed_dags = any(
            (
                dag_id == allowed_dag
                or ("*" in allowed_dag and fnmatch.fnmatch(dag_id, allowed_dag))
            )
            for allowed_dag in allowed_dags
        )

        has_excluded_dags = any(
            (
                dag_id == excluded_dag
                or ("*" in excluded_dag and fnmatch.fnmatch(dag_id, excluded_dag))
            )
            for excluded_dag in excluded_dags
        )

        is_dag_allowed = allow_all_dags or (
            not allow_all_dags and (has_allowed_dags and not has_excluded_dags)
        )
        is_allowed = has_all_variables and is_dag_allowed

        self.logger.debug(
            f"""
            Check is_allowed:
            DAG ID: {dag_id}
            Allowed DAGs: {allowed_dags}
            Excluded DAGs: {excluded_dags}
            Allow All: {allow_all_dags}
            Has Allowed DAGs: {has_allowed_dags}
            Has Excluded DAGs: {has_excluded_dags}
            Is Dag Allowed: {is_dag_allowed}
            Has All Variables: {has_all_variables}
            Is Allowed: {is_allowed}
            """
        )
        return is_allowed

    def get_flag(self, is_enabled: bool) -> str:
        """
        Returns a string representation of a boolean flag.

        Args:
            is_enabled (bool): The boolean flag to be converted.

        Returns:
            str: "Yes" if the flag is True, otherwise "No".
        """

        return "Yes" if is_enabled else "No"

    def extract_lineage(self, asset: dict) -> dict:
        """
        Extracts lineage information from the given asset dictionary.

        Args:
            asset (dict): A dictionary containing DAG and task information. Expected keys are:

        Returns:
            dict: The updated asset dictionary with added lineage information.
        """

        lineage = {"tables": [], "relations": []}
        dag_id = asset.get("dag_id")
        tasks = asset.get("tasks", [])
        tables = []
        relations = []

        for task in tasks:
            task_id = task.get("task_id")
            tables.append(
                {
                    "id": task_id,
                    "name": task_id,
                    "task_id": task_id,
                    "dag_id": dag_id,
                    "owner": task.get("owner", ""),
                    "start_date": task.get("start_date", ""),
                    "end_date": task.get("end_date", ""),
                    "operator_name": task.get("operator_name", ""),
                    "ui_color": task.get("ui_color", ""),
                    "ui_fgcolor": task.get("ui_fgcolor", ""),
                    "downstream_task_ids": task.get("downstream_task_ids", []),
                }
            )

            downstream_task_ids = task.get("downstream_task_ids", [])
            downstream_task_ids = downstream_task_ids if downstream_task_ids else []
            for downstream_task_id in downstream_task_ids:
                relations.append(
                    {"srcTableId": task_id, "tgtTableId": downstream_task_id}
                )
        lineage.update({"tables": tables, "relations": relations})
        asset.update({"lineage": lineage})
        return asset

    def extract_sub_dag_info(self, sub_dags: list, sub_dag: DAG, session) -> dict:
        """
        Extracts detailed information about a sub-DAG from the provided list of sub-DAGs and session.

        Args:
            sub_dags (list): A list of dictionaries containing sub-DAG information.
            sub_dag (DAG): The sub-DAG object to extract information from.
            session: The SQLAlchemy session to query the database.

        Returns:
            dict: A dictionary containing detailed information about the sub-DAG if found, otherwise None.
        """

        sub_dag_detail = None
        existing_dag = next(
            (dag for dag in sub_dags if dag.get("dag_id") == sub_dag.dag_id), None
        )
        sub_dag_model: DagModel = (
            session.query(DagModel).filter(DagModel.dag_id == sub_dag.dag_id).first()
        )
        if not sub_dag_model:
            return sub_dag_detail

        if sub_dag_model and not existing_dag:
            owners = sub_dag_model.owners or []
            tags = sub_dag_model.tags or []
            tags = [tag.name for tag in tags if tag and tag.name]

            sub_dag_detail = {
                "dag_id": sub_dag_model.dag_id,
                "root_dag_id": sub_dag_model.root_dag_id,
                "default_view": sub_dag_model.default_view,
                "description": sub_dag_model.description,
                "fileloc": sub_dag_model.fileloc if self.__extract_source_code else "",
                "is_active": self.get_flag(sub_dag_model.is_active),
                "is_subdag": self.get_flag(sub_dag_model.is_subdag),
                "is_paused": self.get_flag(sub_dag_model.is_paused),
                "next_dagrun": sub_dag_model.next_dagrun,
                "owners": owners,
                "schedule_interval": str(sub_dag_model.schedule_interval),
                "schedule_interval_type": str(
                    type(sub_dag_model.schedule_interval).__name__
                ),
                "schedule_interval_value": str(sub_dag_model.schedule_interval),
                "tags": ",".join(tags),
                "root_dag_id": sub_dag_model.root_dag_id,
            }
        return sub_dag_detail

    def extract_task_info(
        self, task_instances: List[TaskInstance], asset: dict, session
    ) -> dict:
        """
        Extracts information from a list of Airflow task instances and updates the provided asset dictionary with task details.

        Args:
            task_instances (List[TaskInstance]): A list of Airflow TaskInstance objects.
            asset (dict): A dictionary to be updated with task information.
            session: The SQLAlchemy session to use for database operations.

        Returns:
            dict: The updated asset dictionary containing task details and sub-DAG information.
        """

        tasks = []
        sub_dags = []

        for task_instance in task_instances:
            task = task_instance.task
            if not task:
                continue

            sub_dag = task.subdag or None
            sub_dag_details = None
            sub_dag_info = {}
            if sub_dag:
                sub_dag_details = self.extract_sub_dag_info(sub_dags, sub_dag, session)
                sub_dag_details = sub_dag_details if sub_dag_details else {}

            if sub_dag_details:
                sub_dag_info = {
                    "dag_id": sub_dag_details.get("dag_id", ""),
                    "root_dag_id": sub_dag_details.get("root_dag_id", ""),
                }
                sub_dags.append(sub_dag_details)

            task_instance_detail = {
                "last_run_start_date": (
                    str(task_instance.start_date) if task_instance.start_date else None
                ),
                "last_run_end_date": (
                    str(task_instance.end_date) if task_instance.end_date else None
                ),
                "last_run_duration": (
                    str(task_instance.duration) if task_instance.duration else None
                ),
            }
            trigger_rule = (
                task.trigger_rule.value
                if task.trigger_rule and not isinstance(task.trigger_rule, str)
                else task.trigger_rule
            )
            trigger_rule = str(trigger_rule) if trigger_rule else None
            weight_rule = (
                task.weight_rule.value
                if task.weight_rule and not isinstance(task.weight_rule, str)
                else task.weight_rule
            )
            weight_rule = str(weight_rule) if weight_rule else None

            error_message = task_instance.error if task_instance.error else None
            error_message = str(error_message) if error_message else None
            tasks.append(
                {
                    "task_id": task_instance.task_id,
                    "run_id": task_instance.run_id,
                    "dag_id": task_instance.dag_id,
                    "status": task_instance.state,
                    "owner": task.owner,
                    "start_date": str(task.start_date) if task.start_date else None,
                    "end_date": str(task.end_date) if task.end_date else None,
                    "trigger_rule": trigger_rule,
                    "error": error_message,
                    "is_mapped": hasattr(task, "mapped") and task.mapped,
                    "wait_for_downstream": task.wait_for_downstream,
                    "retries": task.retries,
                    "queue": task.queue,
                    "pool": task.pool,
                    "operator_name": task.operator_name,
                    "priority_weight": task.priority_weight,
                    "weight_rule": weight_rule,
                    "ui_color": task.ui_color,
                    "ui_fgcolor": task.ui_fgcolor,
                    "downstream_task_ids": (
                        list(task.downstream_task_ids)
                        if task.downstream_task_ids
                        else []
                    ),
                    "sub_dag": sub_dag_info if sub_dag_info else None,
                    **task_instance_detail,
                }
            )

        asset.update({"tasks": tasks, "total_tasks": len(tasks), "sub_dags": sub_dags})
        return asset

    def extract_dag_info(self, dag_model: DagModel, dag_run: DagRun) -> dict:
        """
        Extracts information from a given DagModel instance and returns it as a dictionary.

        Args:
            dag_model (DagModel): The DagModel instance from which to extract information.

        Returns:
            dict: A dictionary containing various attributes of the DagModel instance.
        """

        # Extract source code of the dag
        file_location: str = dag_model.fileloc or ""
        source_code: str = ""
        if (
            file_location
            and os.path.exists(file_location)
            and self.__extract_source_code
        ):
            with open(file_location, "r", encoding="utf-8") as file:
                source_code = file.readlines()
                source_code = (
                    "\n".join(source_code)
                    if source_code and isinstance(source_code, list)
                    else source_code
                )
                source_code = source_code if source_code else ""

        source_code = source_code if self.__extract_source_code else ""
        tags = dag_model.tags or []
        tags = [tag.name for tag in tags if tag and tag.name]

        asset = {
            "dag_id": dag_model.dag_id,
            "name": dag_model.dag_id,
            "default_view": dag_model.default_view,
            "description": dag_model.description,
            "fileloc": file_location if self.__extract_source_code else "",
            "source_code": source_code,
            "is_active": self.get_flag(dag_model.is_active),
            "is_subdag": self.get_flag(dag_model.is_subdag),
            "is_paused": self.get_flag(dag_model.is_paused),
            "next_dagrun": (
                str(dag_model.next_dagrun) if dag_model.next_dagrun else None
            ),
            "owners": dag_model.owners,
            "tags": ",".join(tags),
            "schedule_interval": str(dag_model.schedule_interval),
            "schedule_interval_type": str(type(dag_model.schedule_interval).__name__),
            "schedule_interval_value": str(dag_model.schedule_interval),
            "asset_type": "Pipeline",
            "type": "dag",
            "root_dag_id": dag_model.root_dag_id,
            "scheduler_lock": dag_model.scheduler_lock,
            "has_import_errors": dag_model.has_import_errors,
            "next_dagrun_data_interval_start": (
                str(dag_model.next_dagrun_data_interval_start)
                if dag_model.next_dagrun_data_interval_start
                else None
            ),
            "next_dagrun_data_interval_end": (
                str(dag_model.next_dagrun_data_interval_end)
                if dag_model.next_dagrun_data_interval_end
                else None
            ),
            "next_dagrun_create_after": (
                str(dag_model.next_dagrun_create_after)
                if dag_model.next_dagrun_create_after
                else None
            ),
            "run_id": dag_run.run_id,
            "execution_date": str(dag_run.execution_date),
            "start_date": str(dag_run.start_date),
            "end_date": str(dag_run.end_date),
            "state": dag_run.state,
        }
        return asset

    def delete_temp_file(self, file_path: str) -> None:
        """
        Deletes a temporary file if it exists.

        Args:
            file_path (str): The path to the file to be deleted.
        """
        if file_path and os.path.exists(file_path):
            os.remove(file_path)

    def push_metadata(self, dag_id: str, asset: dict) -> None:
        """
        Pushes metadata to DQlabs.

        This function writes the provided metadata to a temporary JSON file and
        uploads it to the DQlabs server using the specified connection details.

        Args:
            dag_id (str): The ID of the DAG.
            asset (dict): The metadata to be pushed to DQlabs.

        Raises:
            Exception: If any of the required environment variables (DQ_ENDPOINT_URL, DQ_CLIENT_ID, DQ_CLIENT_SECRET, DQ_CONNECTION) are not set.
            Exception: If the HTTP request to DQlabs fails.

        """

        file_path = None
        try:
            endpoint_url = DQ_ENDPOINT_URL if DQ_ENDPOINT_URL else ""
            client_id = DQ_CLIENT_ID if DQ_CLIENT_ID else ""
            client_secret = DQ_CLIENT_SECRET if DQ_CLIENT_SECRET else ""
            connection_name = DQ_CONNECTION if DQ_CONNECTION else ""
            connection_type = "airflow"

            if not endpoint_url:
                raise Exception(
                    "DQ_ENDPOINT_URL is not set. Please set the DQ_ENDPOINT_URL variable."
                )
            if not client_id:
                raise Exception(
                    "DQ_CLIENT_ID is not set. Please set the DQ_CLIENT_ID variable."
                )
            if not client_secret:
                raise Exception(
                    "DQ_CLIENT_SECRET is not set. Please set the DQ_CLIENT_SECRET variable."
                )
            if not connection_name:
                raise Exception(
                    "DQ_CONNECTION is not set. Please set the DQ_CONNECTION variable."
                )

            parent_dir = os.path.dirname(os.path.dirname(__file__))
            data_dir = os.path.join(parent_dir, "dq_data")
            if not os.path.exists(data_dir):
                os.makedirs(data_dir)

            # Write the metadata into a temp file:
            file_name = f"{str(dag_id)}.json"
            file_path = os.path.join(data_dir, file_name)
            self.delete_temp_file(file_path)

            assets = [asset] if asset and isinstance(asset, dict) else asset
            with open(file_path, "w") as file:
                json.dump(assets, file)

            # Push the metadata to DQlabs:
            # Use this to connect dev server
            if "localhost" in endpoint_url:
                endpoint_url = endpoint_url.replace(
                    "http://localhost:8000/", "http://host.docker.internal:8000/"
                )

            headers = {
                "Client-Id": client_id,
                "Client-Secret": client_secret,
            }
            data = {
                "connection_name": connection_name,
                "connection_type": connection_type,
                "file_name": file_name,
            }
            with open(file_path, "rb") as file:
                files = {"file": (connection_type, file)}
                response = requests.post(
                    endpoint_url,
                    data=data,
                    files=files,
                    headers=headers,
                )
                if not response.ok:
                    response.raise_for_status()
        except Exception as error:
            self.logger.error(
                "Failed to push metadata to DQlabs. Error: %s",
                str(error),
                exc_info=True,
            )
            raise error
        finally:
            self.delete_temp_file(file_path)

    def push_dq_metadata(self, dag_run: DagRun, message: str) -> None:
        """
        Pushes DAG execution metadata to DQlabs.
        This function checks if the DAG ID is allowed.
        If allowed, it gathers information about the DAG execution, task instances, and
        lineage related metadata and then pushes this data to DQlabs.

        Args:
            dag_run (DagRun): The DAG run instance containing execution details.
            message (str): The status message of the DAG run.

        Raises:
            Exception: If there is an error during the process, it logs the error and raises it.
        """

        session = None
        try:
            self.logger.debug(
                f"""
                Pushing DAG execution data to DQlabs:
                DAG ID: {dag_run.dag_id}
                Execution Date: {str(dag_run.execution_date)}
                Execution State: {message}
                Variables:
                    - DQ_ALLOWED_DAGS: {DQ_ALLOWED_DAGS}
                    - DQ_EXCLUDED_DAGS: {DQ_EXCLUDED_DAGS}
                    - DQ_CONNECTION: {DQ_CONNECTION}
                    - DQ_ENDPOINT_URL: {DQ_ENDPOINT_URL}
                    - DQ_CLIENT_ID: {DQ_CLIENT_ID}
                    - DQ_CLIENT_SECRET: {DQ_CLIENT_SECRET}
                """
            )

            is_allowed = self.check_is_allowed(dag_run.dag_id)
            if not is_allowed:
                self.logger.info(
                    f"DAG ID: {dag_run.dag_id} is not allowed. Skipping metadata push."
                )
                return

            session = Session()
            task_instances = (
                session.query(TaskInstance)
                .filter(
                    TaskInstance.dag_id == dag_run.dag_id,
                    TaskInstance.execution_date == dag_run.execution_date,
                )
                .all()
            )
            dag_model = (
                session.query(DagModel)
                .filter(DagModel.dag_id == dag_run.dag_id)
                .first()
            )

            total_runs = (
                session.query(DagRun)
                .filter(
                    DagRun.dag_id == dag_run.dag_id,
                )
                .count()
            )

            self.logger.info(
                f"Extracting metadata from dag: {dag_run.dag_id} for run: {dag_run.run_id}"
            )
            asset = self.extract_dag_info(dag_model, dag_run)
            asset.update({"total_runs": total_runs, "run_id": dag_run.run_id})
            asset = self.extract_task_info(task_instances, asset, session)
            asset = self.extract_lineage(asset)
            self.logger.debug(f"Extracted asset detail: {asset}")
            self.logger.info("Extraction completed!")

            self.logger.info("Pushing DAG execution metadata to DQlabs")
            self.push_metadata(dag_run.dag_id, asset)
            self.logger.info("Pushing DAG execution completed!")
        except Exception as error:
            self.logger.error(
                "Failed to push DAG execution data. Error: %s",
                str(error),
                exc_info=True,
            )
        finally:
            if session and hasattr(session, "close"):
                session.close()

    @hookimpl
    def on_dag_run_failed(self, dag_run: DagRun, msg: str):
        """
        Callback function that is triggered when a DAG run fails.

        Args:
            dag_run (DagRun): The DAG run instance that has failed.
            msg (str): The failure message or reason for the failure.
        """
        self.get_logger()
        self.push_dq_metadata(dag_run, msg)

    @hookimpl
    def on_dag_run_success(self, dag_run: DagRun, msg: str):
        """
        Callback function to be executed when a DAG run is successful.

        Args:
            dag_run (DagRun): The DAG run instance that has completed successfully.
            msg (str): A message to be pushed along with the DAG run metadata.
        """
        self.get_logger()
        self.push_dq_metadata(dag_run, msg)


class DQLabsPlugin(AirflowPlugin):
    """
    A DQLabs plugin that prepares and pushes the metadata information to DQLabs.

    Note:
        Ensure that the following DQlabs configuration variables are set:
        - DQ_ALLOWED_DAGS
        - DQ_EXCLUDED_DAGS
        - DQ_CONNECTION
        - DQ_ENDPOINT_URL
        - DQ_CLIENT_ID
        - DQ_CLIENT_SECRET
    """

    name = "DQLabsPlugin"
    listeners = [DQRunListener()]
