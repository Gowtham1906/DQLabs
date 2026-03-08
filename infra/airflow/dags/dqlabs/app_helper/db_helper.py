import json
import time
import psycopg2
import psycopg2.errors
import os
from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.app_constants.dq_constants import DEFAULT_POSTGRES_PORT
from decimal import *
import decimal

DEADLOCK_MAX_RETRIES = 3
DEADLOCK_BASE_DELAY = 0.5


def fetchone(cursor):
    result = None
    if not cursor:
        return result
    query_result = cursor.fetchone()
    if "mock" in str(cursor).lower():
        column_description = cursor.description
        if query_result:
            if "total_rows" in column_description:
                values = query_result
            else:
                values = query_result[0]
            result = {
                column: value for column, value in zip(column_description, values)
            }
    else:
        if query_result:
            column_description = cursor.description
            result = dict(zip([col[0] for col in column_description], query_result))
    return result


def fetchall(cursor):
    result = []
    if not cursor:
        return result

    query_results = cursor.fetchall()

    # Special condition for s3 select file type cursor
    if "mock" in str(cursor).lower():
        if query_results:
            column_description = cursor.description
            result = [
                {col: row for col, row in zip(column_description, query_result)}
                for query_result in query_results
            ]
    else:
        if query_results:
            column_description = cursor.description
            for query_result in query_results:
                dict_data = dict(
                    zip([col[0] for col in column_description], query_result)
                )
                for col in column_description:
                    if isinstance(dict_data[col[0]], decimal.Decimal):
                        dict_data[col[0]] = remove_exponent(dict_data[col[0]])
                result.append(dict_data)
    return result


def fetchmany(cursor, limit: int = 0):
    max_limit = 10000
    result = []
    fetch_size = max_limit
    if limit:
        fetch_size = limit
        if limit > max_limit:
            fetch_size = max_limit
    if not fetch_size:
        fetch_size = max_limit

    index = 0
    while True:
        if not limit:
            result = []
        query_results = cursor.fetchmany(fetch_size)
        if not query_results:
            if index == 0:
                print("No query_results")
            break

        index = index + 1
        if query_results:
            column_description = cursor.description
            for query_result in query_results:
                dict_data = dict(
                    zip([col[0] for col in column_description], query_result)
                )
                result.append(dict_data)

        if not limit:
            yield result

        if limit and len(result) == limit:
            break
    return result


def fetch_count(cursor):
    fetch_size = 10000
    total_rows = 0
    index = 0
    while True:
        query_results = cursor.fetchmany(fetch_size)
        if not query_results:
            if index == 0:
                pass
                # print("No query_results")
            break
        index = index + 1
        # print(f"Fetching data from cursor iterator index: {index}")
        total_rows = total_rows + len(query_results)
    return total_rows


def split_queries(array, count: int = 100):
    splitted_array = [array[i : i + count] for i in range(0, len(array), count)]
    return splitted_array


def get_default_connection():
    port = os.environ.get("DQLABS_POSTGRES_PORT")
    port = int(port) if port else DEFAULT_POSTGRES_PORT
    connection = psycopg2.connect(
        host=os.environ.get("DQLABS_POSTGRES_HOST"),
        port=port,
        database=os.environ.get("DQLABS_POSTGRES_DB"),
        user=os.environ.get("DQLABS_POSTGRES_USER"),
        password=os.environ.get("DQLABS_POSTGRES_PASSWORD"),
    )
    return connection


def get_connection(organization: str = None):
    is_dev_env = str(os.environ.get("DEV_ENV")).lower() == "true"
    connection = get_default_connection()
    if not organization:
        return connection

    db_config = None
    with connection.cursor() as cursor:
        query_string = f"select * from core.admin where organization='{organization}'"
        cursor.execute(query_string)
        db_config = fetchone(cursor)

    if db_config:
        credentials = db_config.get("credentials", {})
        credentials = (
            json.loads(credentials) if isinstance(credentials, str) else credentials
        )
        credentials = credentials if credentials else {}
        if credentials:
            host = credentials.get("host")
            if is_dev_env:
                host = "postgres" if host == "localhost" else host
            port = credentials.get("port")
            port = int(port) if port else DEFAULT_POSTGRES_PORT
            if is_dev_env:
                port = port if host != "postgres" else DEFAULT_POSTGRES_PORT

            connection = psycopg2.connect(
                host=host,
                port=port,
                database=credentials.get("name"),
                user=credentials.get("user_name"),
                password=credentials.get("password"),
            )
    return connection


def execute_query(connection, cursor, query, data=None):
    for attempt in range(DEADLOCK_MAX_RETRIES + 1):
        try:
            # print("***executing_query***", query)
            if not data:
                cursor.execute(query)
            else:
                cursor.execute(query, data)
            connection.commit()
            return cursor
        except psycopg2.errors.DeadlockDetected as e:
            connection.rollback()
            if attempt < DEADLOCK_MAX_RETRIES:
                delay = DEADLOCK_BASE_DELAY * (2 ** attempt)
                log_info(f"Deadlock detected (attempt {attempt + 1}/{DEADLOCK_MAX_RETRIES}), retrying in {delay}s")
                time.sleep(delay)
                cursor = connection.cursor()
            else:
                log_error("Execute Query - deadlock after max retries", query)
                log_error("Execute Query - deadlock after max retries", e)
                raise e
        except Exception as e:
            log_error("Execute Query", query)
            log_error("Execute Query", e)
            connection.rollback()
            raise e


def remove_exponent(d):
    try:
        return d.quantize(Decimal(1)) if d == d.to_integral() else d.normalize()
    except:
        return
