"""
    Migration Notes From V2 to V3:
    No Migration Changes
"""

from dqlabs.utils.tasks import get_task_config, check_task_status, clear_duplicate_task
from dqlabs.utils.request_queue import update_queue_detail_status, update_queue_status
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.utils.catalog_update import (
    collibra_catalog_update,
    hitachi_catalog_update,
    get_measures,
    get_collibra_measures,
    get_collibra_asset_details,
    get_collibra_attributes_details,
    check_vault_enabled,
    extract_vault_credentials
)
from dqlabs.utils.integrations.alation import (
    alation_catalog_update,
    check_catalog_schedule,
)

from dqlabs.utils.integrations.hitachi_pdc import hitachipdc_catalog_update
from dqlabs.utils.integrations.atlan import Atlan
from dqlabs.utils.integrations.purview import Purview
from dqlabs.utils.integrations.datadotworld import Datadotworld
from dqlabs.utils.integrations.databricks_uc import DatabricksUC
from dqlabs.utils.integrations.datadotworld import Datadotworld
from dqlabs.utils.integrations.coalesce import CoalesceCatalog
from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.app_constants.dq_constants import CATALOG_UPDATE


def run_catalog_update(config: dict, **kwargs):
    """
    Run catalog update task
    """
    clear_duplicate_task(config, CATALOG_UPDATE)
    is_completed = check_task_status(config, kwargs)
    if is_completed:
        return
    task_config = get_task_config(config, kwargs)
    update_queue_detail_status(
        config, ScheduleStatus.Running.value, task_config=task_config
    )
    update_queue_status(config, ScheduleStatus.Running.value, True)
    channels = config.get("dag_info").get("catalog")
    for channel in channels:
        channel_name = channel.get("technical_name")
        configuration = channel.get("configuration")
        if check_vault_enabled(configuration):
            configuration = extract_vault_credentials(config, configuration)

        if channel_name == "collibra":
            asset = get_collibra_asset_details(config, configuration)
            attributes = get_collibra_attributes_details(config, configuration)
            measures = get_collibra_measures(config, configuration)
            if (measures or asset or attributes) and not check_catalog_schedule(config, channel_name):
                collibra_catalog_update(config, configuration, measures, asset, attributes)
        elif channel_name == "hitachi":
            measures = get_measures(config)
            if measures:
                hitachi_catalog_update(configuration, measures)
        elif channel_name == "alation":
            if not check_catalog_schedule(config, channel_name):
                alation_catalog_update(config, configuration)
        elif channel_name == "atlan":
            if not check_catalog_schedule(config, channel_name):
                log_info(("config",config))
                log_info(("channel",channel))
                atlan_client = Atlan(config, configuration)
                atlan_client.atlan_catalog_update()
        elif channel_name == "purview":
            if not check_catalog_schedule(config, channel_name):
                log_info(("config",config))
                log_info(("channel",channel))
                purview_client = Purview(config, configuration)
                purview_client.purview_catalog_update()
        elif channel_name == "databricks_uc":
            if not check_catalog_schedule(config, channel_name):
                log_info(("config",config))
                log_info(("channel",channel))
                databricks_uc_client = DatabricksUC(config, configuration)
                databricks_uc_client.databricks_uc_catalog_update()
        elif channel_name == "coalesce":
            if not check_catalog_schedule(config, channel_name):
                coalesce_client = CoalesceCatalog(config, configuration)
                coalesce_client.coalesce_catalog_update()
        elif channel_name == "datadotworld":
            if not check_catalog_schedule(config, channel_name):
                log_info(("config",config))
                log_info(("channel",channel))
                datadotworld_client = Datadotworld(config, configuration)
                datadotworld_client.datadotworld_catalog_update()
        elif channel_name == "hitachi_pdc":
            try:
                hitachipdc_catalog_update(config, configuration)
        
            except Exception as e:
                log_error(f"Hitachipdc Catalog Update ", str(e))

    update_queue_detail_status(config, ScheduleStatus.Completed.value)
    update_queue_status(config, ScheduleStatus.Completed.value)
