import json

# Import Helpers
from dqlabs.app_helper.workflow_helper import get_semantic_users, make_request, get_semantics
from dqlabs.app_helper.db_helper import execute_query, fetchall
from dqlabs.app_helper.dag_helper import get_postgres_connection

# Import Constant
from dqlabs.app_constants.dq_constants import CREATE_ASSET, CREATE_ISSUE, UPDATE_ASSET

def execute(config: dict, task: dict, input: dict, previous_task: str):
    """
    Update User Mapping via workflow.

    Args:
        config (dict): Configuration dictionary containing necessary settings and credentials.
        task (dict): Task dictionary containing task-specific configuration and details.
        input (dict): Input dictionary containing data passed from the previous task.
        previous_task (str): The technical name of the previous task.

    Returns:
        dict: The input dictionary, potentially modified during the execution.
    """
    previous_task_name = previous_task.get("technical_name")
    configuration = task.get("configuration", {})
    task_config = configuration.get("config", {})

    if previous_task_name not in ["create_issue", "update_asset", "create_asset"]:
        raise Exception(f"Invalid Configuration: Unsupported previous_task_name '{previous_task_name}'")

    users = task_config.get("users", [])
    domain = task_config.get("domain")
    application = task_config.get("application")
    product = task_config.get("product")
    tag = task_config.get("tag")
    sources = get_source_id(previous_task_name, input)

    for source in sources:
        users_list = users

        # Get Existing Users
        existing_users = get_existing_users(config, previous_task_name, source)
        if existing_users:
            users_list.extend(existing_users)

        if previous_task_name == UPDATE_ASSET:
            semantics = get_semantics(config, source)
            domains = json.loads(semantics.get("domain", '[]')) if isinstance(semantics.get("domain"), str) else semantics.get("domain")
            applications = json.loads(semantics.get("application", '[]')) if isinstance(semantics.get("application"), str) else semantics.get("application")
            products = json.loads(semantics.get("product", '[]')) if isinstance(semantics.get("product"), str) else semantics.get("product")
            tags = json.loads(semantics.get("tags", '[]')) if isinstance(semantics.get("tags"), str) else semantics.get("tags")
        else:
            domains = json.loads(input.get("domains", '[]')) if isinstance(input.get("domains"), str) else input.get("domains")
            applications = json.loads(input.get("applications", '[]')) if isinstance(input.get("applications"), str) else input.get("applications")
            tags = json.loads(input.get("tags", '[]')) if isinstance(input.get("tags"), str) else input.get("tags")
            products = json.loads(input.get("products", '[]')) if isinstance(input.get("products"), str) else input.get("products")

            domains = [domain.get("id") for domain in domains] if domains else []
            applications = [app.get("id") for app in applications] if applications else []
            tags = [tag.get("id") for tag in tags] if tags else []
            products = [product.get("id") for product in products] if products else []
        
        if domain and domains:
            domain_users = get_semantic_users(config, "domain", domains)
            users_list.extend(domain_users)
        if application and applications:
            application_users = get_semantic_users(config, "application", applications)
            users_list.extend(application_users)
        if product and products:
            product_users = get_semantic_users(config, "product", products)
            users_list.extend(product_users)
        if tag and tags:
            tag_users = get_semantic_users(config, "tag", tags)
            users_list.extend(tag_users)

        users_list = list(set(users_list))

        # Assign Users
        url = "/api/user-mapping/"
        payload = {
            "user": users
        }
        if previous_task_name == CREATE_ISSUE:
            payload.update({
                "level": "issue",
                "issue_id": source
            })
        elif previous_task_name in [CREATE_ASSET, UPDATE_ASSET]:
            payload.update({
                "level": "asset",
                "asset_id": source
            })
        make_request(config, url, "post", data=payload)
    return input

def get_source_id(task_name: str, input: dict):
    """
    Get Source ID List
    """
    source = []
    if task_name == CREATE_ISSUE:
        source = [input.get("id")]
    elif task_name == CREATE_ASSET:
        source = [input.get("asset_id")]
    elif task_name == UPDATE_ASSET:
        if input.get("asset_id"):
            source = [input.get("asset_id")]
        elif input.get("asset_list"):
            source = input.get("asset_list")
    return source

def get_existing_users(config: dict, task_name: str, source: str):
    """
    Get Existing Users
    """
    users = []
    condition_column = ""
    if task_name == CREATE_ISSUE:
        condition_column = "issue_id"
    elif task_name == CREATE_ASSET:
        condition_column = "asset_id"
    elif task_name == UPDATE_ASSET:
        condition_column = "asset_id"

    if source:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"select distinct user_id from core.user_mapping where {condition_column} ='{source}'"
            cursor = execute_query(connection, cursor, query_string)
            users = fetchall(cursor)
            users = [user.get("user_id") for user in users]
    return users

