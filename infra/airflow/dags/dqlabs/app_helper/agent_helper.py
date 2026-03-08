import json
from copy import deepcopy

from dqlabs_agent import DQAgent
from dqlabs_agent.services.agent_service import DQAgentService
from dqlabs_agent.app_helper.agent_helper import AgentHelpers
from dqlabs_agent.app_helper import fetchone
from dqlabs.app_helper.crypto_helper import encrypt


def is_agent_enabled(connection_type: str):
    return DQAgent.is_enabled(connection_type)


def execute_query(
    config: dict,
    pg_connection: object,
    query_string: str,
    commit: bool = False,
    is_list: bool = False,
    is_columns_only: bool = False,
    is_count_only: bool = False,
    no_response: bool = False,
    convert_lower: bool = True,
    limit: int = 0,
    include_connection_id: bool = False,
    method_name: str = "",
    parameters: dict = {},
    **kwargs,
):
    database_name = config.get("database_name")
    if not database_name:
        database_name = (
            config.get("connection", {}).get("credentials", {}).get("database")
        )
    db_config = dict(database=database_name)
    if kwargs:
        db_config.update(**kwargs)
    connection_type = config.get("connection_type")
    with pg_connection.cursor() as cursor:
        query = """select * from core.settings"""
        cursor.execute(query)
        settings = fetchone(cursor)

    agent_input_config = {
        "connection_type": connection_type,
        "settings": settings,
    }
    has_agent = DQAgent.is_enabled(connection_type)
    agent_config = {}
    if has_agent:
        agent_config = AgentHelpers.get_agent_config(
            pg_connection, agent_input_config)
    agent_config = agent_config if agent_config else {}
    is_enabled = agent_config.get("is_enabled")
    agent_url = agent_config.get("agent_url")
    log_level = agent_config.get("log_level")
    log_level = log_level if log_level else "info"
    client_id = agent_config.get("client_id")
    client_secret = agent_config.get("client_secret")

    connection = config.get("connection", {})
    connection = connection if connection else {}
    connection_id = connection.get("id")
    credentials = connection.get("credentials", {})

    vault_config = {}
    is_vault_enabled = credentials.get("is_vault_enabled")
    selected_vault = credentials.get("selected_vault")
    selected_vault = selected_vault if selected_vault else {}
    if is_vault_enabled and selected_vault:
        vault_id = selected_vault.get("id")
        vault_input_config = dict(vault_id=vault_id, include_id=True)
        vault_config = AgentHelpers.get_vault_config(pg_connection, vault_input_config)
        vault_config = vault_config if vault_config else {}

        is_agent_vault_enabled = False
        if vault_config:
            vault_type = vault_config.get("type")
            is_agent_vault_enabled = DQAgent.is_vault_enabled(vault_type)
        vault_config = vault_config if is_agent_vault_enabled and vault_config else {}

    credentials = (
        json.loads(credentials)
        if credentials and isinstance(credentials, str)
        else credentials
    )
    credentials = credentials if credentials else {}
    credentials.update({"db_config": {**db_config}})
    task_id = config.get("queue_detail_id")

    input_data = dict(
        connection_id=connection_id if include_connection_id else None,
        task_id=task_id if include_connection_id else None,
        connection_type=connection_type,
        credentials=deepcopy(credentials),
        vault_config=vault_config,
        query=query_string,
        is_encrypted=True,
        is_list=is_list,
        commit=commit,
        columns_only=is_columns_only,
        is_count_only=is_count_only,
        limit=limit,
        no_response=no_response,
        convert_lower=convert_lower,
        log_level=log_level,
        method_name=method_name,
        parameters=parameters,
        db_config=db_config,
    )
    if has_agent and is_enabled and agent_url:
        if not (client_id and client_secret):
            raise Exception(
                "Agent is not registered with cloud. Please activate the agent!"
            )

        if "localhost" in agent_url:
            # Use this to connect localhost agent
            agent_url = f"http://host.docker.internal:8005/"
        agent_service = DQAgentService(
            agent_url, str(client_id), str(client_secret))
        response = agent_service.execute(input_data)
        print(f"Execution Completed for Query - {query_string}")
        return response

    response = DQAgent().execute(**input_data)
    response_data = response.get("data")
    response_status = response.get("status")
    if response_status and response_status.lower() != "success":
        error = response.get("error")
        if error:
            raise Exception(error)
    print(f"Execution Completed ")
    return response_data


def clear_connection(config: dict, pg_connection: object):
    connection_type = config.get("connection_type")
    with pg_connection.cursor() as cursor:
        query = """select * from core.settings"""
        cursor.execute(query)
        settings = fetchone(cursor)

    task_id = config.get("queue_detail_id")
    agent_input_config = {
        "connection_type": connection_type,
        "settings": settings,
    }
    has_agent = DQAgent.is_enabled(connection_type)
    agent_config = {}
    if has_agent:
        agent_config = AgentHelpers.get_agent_config(
            pg_connection, agent_input_config)
    agent_config = agent_config if agent_config else {}
    is_enabled = agent_config.get("is_enabled")
    agent_url = agent_config.get("agent_url")
    client_id = agent_config.get("client_id")
    client_secret = agent_config.get("client_secret")

    if has_agent and is_enabled and agent_url:
        if not (client_id and client_secret):
            raise Exception(
                "Agent is not registered with cloud. Please activate the agent!"
            )

        if "localhost" in agent_url:
            # Use this to connect localhost agent
            agent_url = f"http://host.docker.internal:8005/"
        agent_service = DQAgentService(
            agent_url, str(client_id), str(client_secret))
        agent_service.clear_connection(task_id)
    else:
        DQAgent().clear_connection(task_id)


def get_vault_data(config, pg_connection, channel_config: dict): 
        with pg_connection.cursor() as cursor:
            query = """select * from core.settings"""
            cursor.execute(query)
            settings = fetchone(cursor)
        connection_type = (channel_config.get("selected_vault") or {}).get("type","")
        agent_input_config = {
            "settings": settings,
            "connection_type": connection_type,
        }
        agent_config = AgentHelpers.get_agent_config(pg_connection, agent_input_config)
        agent_config = agent_config if agent_config else {}

        is_enabled = agent_config.get("is_enabled")
        agent_url = agent_config.get("agent_url")
        log_level = agent_config.get("log_level")
        client_id = agent_config.get("client_id")
        client_secret = agent_config.get("client_secret")

        vault_config = {}
        is_vault_enabled = channel_config.get("is_vault_enabled")
        selected_vault = channel_config.get("selected_vault")
        selected_vault = selected_vault if selected_vault else {}
        if is_vault_enabled and selected_vault:
            vault_id = selected_vault.get("id")
            config = dict(vault_id=vault_id, include_id=False)
            vault_config = AgentHelpers.get_vault_config(pg_connection, config)
            vault_config = vault_config if vault_config else {}

            is_agent_vault_enabled = False
            if vault_config:
                vault_type = vault_config.get("type")
                is_agent_vault_enabled = DQAgent.is_vault_enabled(vault_type)
            vault_config = (
                vault_config if is_agent_vault_enabled and vault_config else {}
            )
            vault_config.update({
                "vault_key": channel_config.get("vault_key"), 
                "log_level": log_level,
                "rename_user_key": False
                })
        response = {}
        if agent_config and is_enabled and agent_url:
            if not (client_id and client_secret):
                raise Exception("AGENT_NOT_REGISTERED_EXCEPTION")
            if "localhost" in agent_url:
                # Use this to connect localhost agent
                agent_url = f"http://host.docker.internal:8005/"
            agent_service = DQAgentService(
                agent_url, str(client_id), str(client_secret)
            )
            print("Running the query on agent mode...")
            response = agent_service.get_vault_data(vault_config)
        else:
            agent_service = DQAgent()
            response = agent_service.get_vault_secret(vault_config)
        if response:
            response = {k: encrypt(v) for k, v in response.items()}
        return response
