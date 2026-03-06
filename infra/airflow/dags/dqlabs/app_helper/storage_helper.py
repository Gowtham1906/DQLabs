import json
from dqlabs.services.local_storage_service import LocalStorageService
from dqlabs.services.s3_storage_service import S3StorageService


def get_storage_service(dag_info: dict):
    general_settings = dag_info.get("settings")
    storage_settings = general_settings.get(
        "storage") if general_settings else {}
    storage_settings = json.loads(storage_settings) if storage_settings and isinstance(
        storage_settings, str) else storage_settings
    storage_settings = storage_settings if storage_settings else {}

    storage = storage_settings.get("storage")
    storage_config = storage if storage else {}
    storage_type = storage_config.get("type")
    storage_type = str(storage_type).lower() if storage_type else "s3"

    storage_service = None
    if storage_type == "disc":
        storage_service = LocalStorageService(storage_config)
    elif storage_type == "s3":
        storage_service = S3StorageService(storage_config)

    if not storage_service:
        storage_service = S3StorageService(storage_config)
    return storage_service
