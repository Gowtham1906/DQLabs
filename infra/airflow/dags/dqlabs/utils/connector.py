from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone, split_queries
from dqlabs.app_helper.log_helper import log_error
from datetime import datetime
from dqlabs.enums.connection_types import ConnectionType


def get_pipeline_job_last_run_id(config: dict, conn_type: str = '') -> str:
    try:
        connection_data = config.get("connection", {})
        connection_id = connection_data.get("id")
        asset = config.get("asset",{})
        asset_id = asset.get("id", '')
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            if conn_type == ConnectionType.Wherescape.value:
                query_string = f"""
                            select
                                x."run_id" as run_id
                            from 
                                core.pipeline_data as pd,
                                jsonb_to_recordset(data)
                            as x("run_id" int)
                            where pd.type = 'runs' and asset_id = '{asset_id}' and pd.connection_id = '{connection_id}'
                            order by x."run_id" desc
                            limit 1
                            """
            else:
                query_string = f"""
                                select
                                    x."original_dag_run_id" as run_id
                                from 
                                    core.pipeline_data as pd,
                                    jsonb_to_recordset(data)
                                as x("id" text, "is_success" bool, "job_id" text, "duration" text, "finished_at" timestamp, "original_dag_run_id" text)
                                where pd.type = 'runs' and asset_id is null and pd.connection_id = '{connection_id}'
                                order by x."id" desc
                                limit 1
                                """
            cursor = execute_query(connection, cursor, query_string)
            last_run_id = fetchone(cursor)
            last_run_id = last_run_id.get('run_id', None) if last_run_id else None
            return last_run_id
    except Exception as e:
        log_error(str(e), e)

def pipeline_parse_datetime(date_str):
    # parse the pipeline time accordingly
    try:
        # Try parsing with microseconds
        return datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%f%z')
    except ValueError:
        # Fallback to parsing without microseconds
        return datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S%z')