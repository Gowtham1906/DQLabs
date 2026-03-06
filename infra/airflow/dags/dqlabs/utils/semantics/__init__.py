from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone, get_connection
from dqlabs.app_helper.dag_helper import get_postgres_connection


def get_default_connection_id(admin_organization:str=None):
    default_connection_id = ""
    with get_connection(admin_organization) as connection:
        with connection.cursor() as cursor:
            query_string = """
                select airflow_connection_id from core.connection
                where is_default=True
            """
            cursor = execute_query(connection, cursor, query_string)
            default_connection = fetchone(cursor)
            if default_connection:
                default_connection_id = default_connection.get("airflow_connection_id")
                default_connection_id = default_connection_id if default_connection_id else ""
    return default_connection_id