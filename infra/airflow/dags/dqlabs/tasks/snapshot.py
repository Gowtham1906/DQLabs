
from uuid import uuid4

from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.db_helper import execute_query,  fetchone

# Import Logs
from dqlabs.app_helper.log_helper import log_error


def create_asset_snapshot(config: dict, asset_id: str) -> None:
    """
    Create Asset Snapshot
    into postgres
    """
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select asset.last_run_id from core.asset
                where id = '{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            last_run_id = fetchone(cursor)
            last_run_id = last_run_id.get(
                'last_run_id', None) if last_run_id else None

            if last_run_id:
                query_string = f"""
                    select asset.id from core.asset_snapshot
                    join core.asset on asset.id = asset_snapshot.asset_id and asset.last_run_id = asset_snapshot.last_run_id
                    where asset.id = '{asset_id}' and asset.last_run_id='{last_run_id}'
                """
                cursor = execute_query(connection, cursor, query_string)
                existing_snapshot = fetchone(cursor)
                if not existing_snapshot:
                    snapshot_id = f"'{uuid4()}'"
                    query_string = f"""
                            insert into core.asset_snapshot (id, asset_id,name,view_type,description,technical_name,unique_id,"group",properties, type,run_status,run_now,query,airflow_pool_name,search_keys,views,conversations,ratings,queries,data_size,score,alerts,issues,is_valid,is_active,is_delete,created_by,updated_by,created_date,modified_date,organization_id,connection_id,last_run_id,status,is_header)
                            select {snapshot_id} as id, id as asset_id,name,view_type,description,technical_name,unique_id,"group",properties, type,run_status,run_now,query,airflow_pool_name,search_keys,views,conversations,ratings,queries,data_size,score,alerts,issues,is_valid,is_active,is_delete,created_by,updated_by,created_date,modified_date,organization_id,connection_id,last_run_id,status,is_header
                            from core.asset where asset.id = '{asset_id}' and asset.last_run_id='{last_run_id}'
                    """
                    cursor = execute_query(connection, cursor, query_string)
    except Exception as e:
        raise e
