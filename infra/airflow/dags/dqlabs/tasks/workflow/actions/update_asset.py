import json
from copy import deepcopy

# Import Helpers
from dqlabs.app_helper.workflow_helper import make_request, get_connection_assets, get_semantics

# Import Constant
from dqlabs.app_constants.dq_constants import CREATE_ASSET

def execute(config: dict, task: dict, input: dict, previous_task: dict):
    """
    Update Asset Via Workflow
    """

    previous_task_name = previous_task.get("technical_name")
    configuration = task.get("configuration", {})
    task_config = configuration.get("config", {})
    
    # Prepare Payload
    payload = prepare_payload(task_config)

    # Get Connection Assets
    assets = [input.get("asset_id")] if input.get("asset_id") else []
    if previous_task_name != CREATE_ASSET:
        connection = task_config.get("connection")
        connection = connection.get("id") if connection else ""
        asset_name = task_config.get("asset")
        asset_name = asset_name if asset_name else ""
        if not connection or not asset_name:
            raise Exception("Connection and Asset is required")
        assets = get_connection_assets(config, connection, asset_name)
        
    # Call API
    asset_payload = []
    for asset in assets:
        data_payload = deepcopy(payload)
        data_payload.update({"asset_id": asset, "id": asset})
        # Update Existing Semantics to Asset
        if previous_task_name != CREATE_ASSET:
            semantics = get_semantics(config, asset)
            domains = json.loads(semantics.get("domain", '[]')) if isinstance(semantics.get("domain"), str) else semantics.get("domain")
            applications = json.loads(semantics.get("application", '[]')) if isinstance(semantics.get("application"), str) else semantics.get("application")
            products = json.loads(semantics.get("product", '[]')) if isinstance(semantics.get("product"), str) else semantics.get("product")
            tags = json.loads(semantics.get("tags", '[]')) if isinstance(semantics.get("tags"), str) else semantics.get("tags")

            if "domains" in data_payload and domains:
                domains = [{"id": domain} for domain in domains]
                domains = list({domain["id"]: domain for domain in [*domains, *data_payload.get("domains")]}.values())
                data_payload.update({"domains": domains})

            if "applications" in data_payload and applications:
                applications = [{"id": app} for app in applications]
                applications = list({app["id"]: app for app in [*applications, *data_payload.get("applications")]}.values())
                data_payload.update({"applications": applications})

            if "products" in products and products:
                products = [{"id": product} for product in products]
                products = list({product["id"]: product for product in [*products, *data_payload.get("products")]}.values())
                data_payload.update({"products": products})

            if "tags" in data_payload and tags:
                tags = [{"id": tag} for tag in tags]
                tags = list({tag["id"]: tag for tag in [*tags, *data_payload.get("tags")]}.values())
                data_payload.update({"tags": tags})

        asset_payload.append(data_payload)

    if assets:
        url = f"/api/asset/{assets[0]}/"
        make_request(config, url, "put", data=asset_payload)
    input.update({"asset_list": assets})
    return input

def prepare_payload(task_config: dict):
    """
    Prepare Payload
    """
    payload = {}
    changed_properties = []
    description = task_config.get("description", [])
    domains = task_config.get("domain", [])
    applications = task_config.get("application", [])
    products = task_config.get("product", [])
    tags = task_config.get("tags", [])
    tags = [{**tag, "level":"asset"} for tag in tags]
    
    changed_properties = []
    for value, key in zip([domains, applications, products, description, tags], ['domains', 'applications', 'products', 'description', 'tags']):
        if value:
            changed_properties.append(key)
            payload[key] = value
    payload.update({"changed_properties": changed_properties})
    return payload

