"""
    Migration Notes From V2 to V3:
    No Migration Changes
"""

import json
import os
from airflow.models import Connection, Pool
from airflow.settings import Session
from dqlabs.app_helper.db_helper import (
    execute_query,
    get_connection,
    fetchall,
)
from dqlabs.app_helper.log_helper import log_error
from dqlabs.app_constants.dq_constants import (
    DEFAULT_POSTGRES_CONN,
    DEFAULT_POSTGRES_PORT,
    DEFAULT_SLOTS,
    APP_NAME,
)
from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.connection_helper import prepare_decrypt_connection_config


def get_dq_connections(config: dict, connection_id: str = None) -> list:
    """
    Get list of connections to be created / updated
    """
    connections = []
    session = Session()
    core_connection_id = config.get("core_connection_id")
    connection = None
    if core_connection_id:
        connection: Connection = (
            session.query(Connection)
            .where(Connection.conn_id == core_connection_id)
            .scalar()
        )

    db_connection = None
    if connection:
        db_connection = get_postgres_connection(config)
    else:
        admin_organization = config.get("admin_organization")
        db_connection = get_connection(admin_organization)

    if not db_connection:
        return connections

    with db_connection:
        with db_connection.cursor() as cursor:
            query = f"""
                select * from core.connection
                where is_valid=True
                and (airflow_connection_id is null or is_updated=True)
            """
            if connection_id:
                query = f"""
                    select * from core.connection where id='{connection_id}'
                """

            cursor = execute_query(db_connection, cursor, query)
            connections = fetchall(cursor)
    return connections


def get_dq_connections_database(
    config: dict, connection_id: str = None, limit: int = 1
):
    connection_database_details = {}
    connection = get_postgres_connection(config)
    limit = limit if limit else None
    limit_query = f"limit {str(limit)}" if limit else ""

    with connection:
        with connection.cursor() as cursor:
            query = f"""
                select jsonb_array_elements(credentials->'included_databases')->>0 as name 
                from core.connection
                where id='{str(connection_id)}'
                order by name {limit_query}
            """
            cursor = execute_query(connection, cursor, query)
            connection_database_details = fetchall(cursor)
            connection_database_details = (
                connection_database_details if connection_database_details else []
            )
    if limit == 1 and connection_database_details:
        connection_database_details = connection_database_details[0]
    return connection_database_details


def get_assets(config: dict, asset_id: str = None) -> list:
    """
    Get list of connections to be created / updated
    """
    assets = []
    connection = get_postgres_connection(config)
    with connection:
        with connection.cursor() as cursor:
            query = f"""
                select asset.id, asset.airflow_pool_name, con.airflow_connection_id
                from core.asset as asset
                join core.connection as con on con.id=asset.connection_id
                where (asset.airflow_pool_name is null or asset.airflow_pool_name = '')
                and con.airflow_connection_id  is not null
            """
            if asset_id:
                query = f"""
                    select asset.id, asset.airflow_pool_name, con.airflow_connection_id
                    from core.asset as asset
                    join core.connection as con on con.id=asset.connection_id
                    where asset.id='{asset_id}'
                """
            cursor = execute_query(connection, cursor, query)
            assets = fetchall(cursor)
    return assets


def manage_connection(
    dq_connection: dict, task_config: dict, is_delete: bool = False
) -> None:
    """
    Manage the airflow connection object for the given dqlabs connection object
    """
    try:
        new_connection_id = None
        session = Session()

        connection_object = dq_connection.get("airflow_connection_object")
        connection_name = dq_connection.get("name")
        airflow_connection_id = dq_connection.get("airflow_connection_id")
        if not connection_object:
            return

        connection_object.update({"description": connection_name})
        connection_object = prepare_decrypt_connection_config(connection_object)
        connection_id = connection_object.get("connection_id")
        password = connection_object.get("password")
        extra = connection_object.get("extra")
        extra = json.dumps(extra, default=str) if not isinstance(extra, str) else extra
        properties_to_delete = ["connection_id", "password"]
        for key in properties_to_delete:
            if key in connection_object:
                del connection_object[key]

        session.query(Connection).filter(Connection.conn_id == connection_id).delete()
        if "postgres_connection" in connection_object:
            del connection_object["postgres_connection"]
        
        # Filter out invalid fields for Airflow Connection class
        valid_connection_fields = {
            'conn_type', 'host', 'login', 'schema', 'port', 
            'extra', 'description', 'uri', 'is_encrypted'
        }
        
        # Create a clean connection object with only valid fields
        clean_connection_object = {}
        for key, value in connection_object.items():
            if key in valid_connection_fields:
                clean_connection_object[key] = value
        
        connection = Connection(conn_id=connection_id, **clean_connection_object)
        session.add(connection)
        if password:
            connection.set_password(password)
        session.commit()

        new_connection_id = str(connection) if not airflow_connection_id else None
        connection_id = dq_connection.get("id")
        if new_connection_id and connection_id:
            db_connection = get_postgres_connection(task_config)
            with db_connection:
                with db_connection.cursor() as cursor:
                    query_string = f"""
                        update core.connection set airflow_connection_id='{new_connection_id}' where id='{connection_id}'
                    """
                    execute_query(db_connection, cursor, query_string)
        return new_connection_id
    except Exception as e:
        log_error(str(e), e)
    finally:
        if session:
            session.close()


def delete_connection(connection_id: str) -> None:
    """
    Datete connection object from airflow connections
    """
    try:
        session = Session()
        session.query(Connection).filter(Connection.conn_id == connection_id).delete()
        session.commit()

        session.query(Pool).filter(Pool.pool.ilike(f"{connection_id}%")).delete(
            synchronize_session="fetch"
        )
        session.commit()
    except Exception as e:
        log_error(str(e), e)
    finally:
        if session:
            session.close()


def manage_connection_pool(
    asset: dict, task_config: dict, is_delete: bool = False
) -> None:
    """
    Manage the airflow connection pool object for the given asset
    """
    try:
        session = Session()
        airflow_connection_id = asset.get("airflow_connection_id")
        asset_id = asset.get("id")
        asset_pool = asset.get("airflow_pool_name")
        airflow_pool_name = f"{airflow_connection_id}___{asset_id}"
        slots = DEFAULT_SLOTS
        connection_pool: Pool = (
            session.query(Pool).filter(Pool.pool == airflow_pool_name).first()
        )
        if not connection_pool:
            connection_pool = Pool(
                pool=airflow_pool_name, slots=slots, include_deferred=False
            )
            session.add(connection_pool)
        else:
            connection_pool.slots = slots
        session.commit()

        new_pool_name = str(connection_pool) if not asset_pool else None
        if new_pool_name and asset_id:
            db_connection = get_postgres_connection(task_config)
            with db_connection:
                with db_connection.cursor() as cursor:
                    query_string = f"""
                        update core.asset set airflow_pool_name='{new_pool_name}' where id='{asset_id}'
                    """
                    execute_query(db_connection, cursor, query_string)
    except Exception as e:
        log_error(str(e), e)
    finally:
        if session:
            session.close()


def delete_connection_pool(pool_name: str) -> None:
    """
    Datete connection pool object from airflow connection pools
    """
    try:
        session = Session()
        session.query(Pool).filter(Pool.pool == pool_name).delete()
        session.commit()
    except Exception as e:
        log_error(str(e), e)
    finally:
        if session:
            session.close()


def create_default_connection(config: dict) -> None:
    """
    Create the default postgres connection for airflow communication
    """
    try:
        connection_id = config.get("default_connection_id")
        session = Session()

        default_connection_id = None
        connection_object = None
        if connection_id:
            connections = get_dq_connections(config, connection_id)
            default_connection = connections[0] if connections else None

            connection_object = default_connection.get("airflow_connection_object")
            connection_name = default_connection.get("name")
            if connection_object:
                connection_object.update({"description": connection_name})
                default_connection_id = connection_object.get("connection_id")
                extra = connection_object.get("extra")
                extra = (
                    json.dumps(extra, default=str)
                    if not isinstance(extra, str)
                    else extra
                )
                if "connection_id" in connection_object:
                    del connection_object["connection_id"]
                connection_object.update({"extra": extra})

        if not connection_object:
            default_connection_id = DEFAULT_POSTGRES_CONN
            conn_type = "postgres"
            port = os.environ.get("DQLABS_POSTGRES_PORT")
            port = int(port) if port else DEFAULT_POSTGRES_PORT
            extra = {
                "dbname": os.environ.get("DQLABS_POSTGRES_DB"),
                "application_name": APP_NAME,
            }
            connection_object = {
                "conn_type": conn_type,
                "host": os.environ.get("DQLABS_POSTGRES_HOST"),
                "port": int(port) if port else DEFAULT_POSTGRES_PORT,
                "login": os.environ.get("DQLABS_POSTGRES_USER"),
                "password": os.environ.get("DQLABS_POSTGRES_PASSWORD"),
                "schema": os.environ.get("DQLABS_POSTGRES_SCHEMA"),
                "extra": json.dumps(extra, default=str),
            }

        if connection_object and default_connection_id:
            password = connection_object.get("password")
            if "password" in connection_object:
                del connection_object["password"]
            connection: Connection = (
                session.query(Connection)
                .filter(Connection.conn_id == default_connection_id)
                .delete()
            )
            if "postgres_connection" in connection_object:
                del connection_object["postgres_connection"]

            connection = Connection(conn_id=default_connection_id, **connection_object)
            session.add(connection)
            if password:
                connection.set_password(password)
            session.commit()
            config.update({"core_connection_id": default_connection_id})

            db_connection = get_postgres_connection(config)
            with db_connection:
                with db_connection.cursor() as cursor:
                    query_string = f"""
                        update core.connection set airflow_connection_id='{default_connection_id}' where is_default=True
                    """
                    execute_query(db_connection, cursor, query_string)

            admin_organization = config.get("admin_organization")
            if default_connection_id and admin_organization:
                with get_connection() as db_connection:
                    with db_connection.cursor() as cursor:
                        query_string = f"""
                            update core.admin set default_connection_id='{default_connection_id}' where organization='{admin_organization}'
                        """
                        execute_query(db_connection, cursor, query_string)

            # create default connection pool
            if default_connection_id:
                if not session:
                    session = Session()
                connection_pool: Pool = (
                    session.query(Pool)
                    .filter(Pool.pool == default_connection_id)
                    .delete()
                )
                connection_pool = Pool(
                    pool=default_connection_id, slots=200, include_deferred=False
                )
                session.add(connection_pool)
                session.commit()
    except Exception as e:
        log_error(str(e), e)
    finally:
        if session:
            session.close()
        return default_connection_id
