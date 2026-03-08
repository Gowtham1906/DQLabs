from dqlabs.enums.connection_types import ConnectionType
from dqlabs.app_helper.crypto_helper import decrypt
import json
import pandas as pd
import re
from datetime import datetime


def get_attribute_names(connection_type: str, attributes: list) -> list:
    if connection_type in [
        ConnectionType.Snowflake.value,
        ConnectionType.Oracle.value,
        ConnectionType.Denodo.value,
        ConnectionType.Redshift_Spectrum.value,
        ConnectionType.Redshift.value,
        ConnectionType.SapHana.value
    ]:
        attributes = [f""" "{key}" """ for key in attributes]
    elif connection_type == ConnectionType.Teradata.value:
        attributes = [f""" "{key.strip()}" """ for key in attributes]
    elif connection_type == ConnectionType.MSSQL.value:
        attributes = [f""" [{key}] """ for key in attributes]
    elif connection_type == ConnectionType.Synapse.value:
        attributes = [f""" [{key}] """ for key in attributes]
    elif connection_type in [
        ConnectionType.Databricks.value,
        ConnectionType.BigQuery.value,
        ConnectionType.MySql.value,
        ConnectionType.S3.value, 
        ConnectionType.ADLS.value,
        ConnectionType.Hive.value,
    ]:
        attributes = [f""" `{key}` """ for key in attributes]
    else:
        attributes = [f""" {key} """ for key in attributes]
    return attributes


def get_primary_key(connection_type: str, keys: list, source_attribute: list = []) -> str:
    """
    Returns the formatted primary keys for the given connection type
    """
    primary_keys = []
    for key in keys:
        input_key = key
        if isinstance(key, dict):
            input_key = key.get("name")
        if not input_key:
            continue
        if input_key not in primary_keys and (not source_attribute or input_key in source_attribute):
            primary_keys.append(input_key)

    primary_keys = get_attribute_names(connection_type, primary_keys)
    primary_key = ", ".join(primary_keys)
    return primary_key


def prepare_decrypt_connection_config(connection_config: dict) -> dict:
    """
    Returns a connection config to decrypt config for the given connection_type
    """
    connection_type = connection_config.get("conn_type", "").lower()
    if connection_type == ConnectionType.Wherescape.value:
        connection_type = connection_config.get('metadata_conn_type', 'mssql')
    if connection_type == ConnectionType.Snowflake.value:
        connection_config.update(
            {
                "login": decrypt(connection_config.get("login")),
                "password": decrypt(connection_config.get("password")),
            }
        )
    elif connection_type == ConnectionType.MSSQL.value:
        connection_config.update(
            {
                "login": (
                    decrypt(connection_config.get("login"))
                    if connection_config.get("login")
                    else ""
                ),
                "password": (
                    decrypt(connection_config.get("password"))
                    if connection_config.get("password")
                    else ""
                ),
            }
        )
    elif connection_type == ConnectionType.Synapse.value:
        connection_config.update(
            {
                "login": decrypt(connection_config.get("login")),
                "password": decrypt(connection_config.get("password")),
            }
        )
    elif connection_type == ConnectionType.Redshift.value:
        extraparam = json.loads(connection_config.get("extra", {}))
        if connection_config.get("authentication_type") == "Secret Manager":
            extraparam.update(
                {
                    "access_key_id": decrypt(extraparam.get("access_key_id")),
                    "secret_access_key": decrypt(extraparam.get("secret_access_key")),
                }
            )
            connection_config.update({"extra": extraparam})
        else:
            connection_config.update(
                {
                    "login": decrypt(connection_config.get("login")),
                    "password": decrypt(connection_config.get("password")),
                }
            )
        connection_config.pop("authentication_type", None)
    elif connection_type == ConnectionType.Redshift_Spectrum.value:
        extraparam = json.loads(connection_config.get("extra", {}))
        if connection_config.get("authentication_type") == "Secret Manager":
            extraparam.update(
                {
                    "access_key_id": decrypt(extraparam.get("access_key_id")),
                    "secret_access_key": decrypt(extraparam.get("secret_access_key")),
                }
            )
            connection_config.update({"extra": extraparam})
        else:
            connection_config.update(
                {
                    "login": decrypt(connection_config.get("login")),
                    "password": decrypt(connection_config.get("password")),
                }
            )
        connection_config.pop("authentication_type", None)
    elif connection_type == "gcpbigquery":  # ConnectionType.BigQuery.value:
        extraparam = json.loads(connection_config.get("extra", {}))
        extraparam.update(
            {
                "extra__google_cloud_platform__keyfile_dict": decrypt(
                    extraparam.get("extra__google_cloud_platform__keyfile_dict")
                ),
            }
        )
        connection_config.update(
            {
                "extra": extraparam,
            }
        )
    elif connection_type == ConnectionType.BigQuery.value:
        json_key = connection_config.get("keyjson", "")
        connection_config.update({"keyjson": decrypt(json_key)})
    elif connection_type == ConnectionType.Databricks.value:
        connection_config.update(
            {
                "password": (
                    decrypt(connection_config.get("password"))
                    if connection_config.get("password")
                    else ""
                )
            }
        )
        extraparam = json.loads(connection_config.get("extra", {}))
        extraparam.update(
            {
                "token": (
                    connection_config.get("password")
                    if connection_config.get("password")
                    else ""
                ),
            }
        )
        extraparam.update(
            {
                "client_secret": (
                    connection_config.get("client_secret")
                    if connection_config.get("client_secret")
                    else ""
                ),
            }
        )
        connection_config.update({"extra": extraparam})
    elif connection_type == ConnectionType.Athena.value:
        extraparam = json.loads(connection_config.get("extra", {}))
        extraparam.update(
            {
                "aws_access_key_id": decrypt(extraparam.get("aws_access_key_id")),
            }
        )
        extraparam.update(
            {
                "aws_secret_access_key": decrypt(
                    extraparam.get("aws_secret_access_key")
                ),
            }
        )
        connection_config.update(
            {
                "conn_type": "aws",
            }
        )
        connection_config.update(
            {
                "extra": extraparam,
            }
        )
    elif connection_type == ConnectionType.EmrSpark.value:
        extraparam = json.loads(connection_config.get("extra", {}))
        extraparam.update(
            {
                "aws_access_key_id": decrypt(extraparam.get("aws_access_key_id")),
            }
        )
        extraparam.update(
            {
                "aws_secret_access_key": decrypt(
                    extraparam.get("aws_secret_access_key")
                ),
            }
        )
        connection_config.update(
            {
                "conn_type": "aws",
            }
        )
        connection_config.update(
            {
                "extra": extraparam,
            }
        )
    elif connection_type == ConnectionType.Oracle.value:
        connection_config.update(
            {
                "login": decrypt(connection_config.get("login")),
                "password": decrypt(connection_config.get("password")),
            }
        )
    elif connection_type == ConnectionType.S3Select.value:
        extraparam = json.loads(connection_config.get("extra", {}))
        extraparam.update(
            {
                "aws_access_key_id": decrypt(extraparam.get("aws_access_key_id")),
            }
        )
        extraparam.update(
            {
                "aws_secret_access_key": decrypt(
                    extraparam.get("aws_secret_access_key")
                ),
            }
        )
        connection_config.update(
            {
                "conn_type": "aws",
            }
        )
        connection_config.update(
            {
                "extra": extraparam,
            }
        )
    elif connection_type == ConnectionType.Hive.value:
        connection_config.update(
            {
                "login": decrypt(connection_config.get("login")),
                "password": decrypt(connection_config.get("password")),
            }
        )
        connection_config.update(
            {
                "conn_type": "hiveserver2",
            }
        )
    elif connection_type == ConnectionType.Salesforce.value:
        # Handle Salesforce Marketing Cloud JDBC connections
        if connection_config.get("connection_type") == "jdbc":
            # For JDBC connections, we need to store credentials in extra
            extraparam = json.loads(connection_config.get("extra", "{}"))
            extraparam.update({
                "client_id": decrypt(connection_config.get("client_id")),
                "client_secret": decrypt(connection_config.get("client_secret")),
                "username": decrypt(connection_config.get("username")),
                "password": decrypt(connection_config.get("password")),
                "server": connection_config.get("server"),
                "port": connection_config.get("port"),
                "database": connection_config.get("database"),
                "jdbc_driver": connection_config.get("jdbc_driver"),
                "jdbc_url": connection_config.get("jdbc_url")
            })
            connection_config.update({
                "conn_type": "jdbc",
                "extra": extraparam
            })
            # Remove fields that are not valid for Airflow Connection
            fields_to_remove = ["client_id", "client_secret", "username", "password", 
                              "server", "port", "database", "jdbc_driver", "jdbc_url", 
                              "connection_type", "authentication_type"]
            for field in fields_to_remove:
                connection_config.pop(field, None)
        else:
            # Handle regular Salesforce connections
            connection_config.update(
                {
                    "login": decrypt(connection_config.get("login")),
                    "password": decrypt(connection_config.get("password")),
                }
            )
    elif connection_type == ConnectionType.ADLS.value:
        connection_config.update(
            {
                "client_secret": decrypt(connection_config.get("client_secret")),
                "storage_access_key": decrypt(
                    connection_config.get("storage_access_key")
                ),
                 "storage_account_key": decrypt(connection_config.get("storage_account_key")),
            }
        )
    elif connection_type == ConnectionType.S3.value:
        connection_config.update(
            {
                "aws_secret_access_key": decrypt(connection_config.get("aws_secret_access_key")),
            }
        )
    elif connection_type in [
        ConnectionType.Dbt.value,
        ConnectionType.Tableau.value,
        ConnectionType.Fivetran.value,
        ConnectionType.Airflow.value,
        ConnectionType.Talend.value,
        ConnectionType.ADF.value,
        ConnectionType.PowerBI.value,
    ]:
        pass
    else:
        connection_config.update(
            {
                "login": decrypt(connection_config.get("login")),
                "password": decrypt(connection_config.get("password")),
            }
        )
    return connection_config


def decrypt_connection_config(connection_config: dict, conn_type: str) -> dict:
    """
    Returns a connection config to decrypt config for the given connection_type
    """
    connection_type = conn_type.lower()
    if connection_type == ConnectionType.Wherescape.value:
        connection_type = connection_config.get('metadata_conn_type', 'mssql')
    if connection_type == ConnectionType.Tableau.value:
        connection_config.update(
            {
                "user": decrypt(connection_config.get("user")),
                "password": decrypt(connection_config.get("password")),
            }
        )
    elif connection_type == ConnectionType.MSSQL.value:
        connection_config.update(
            {
                "user": decrypt(connection_config.get("user")),
                "password": decrypt(connection_config.get("password")),
            }
        )
    elif connection_type == ConnectionType.Synapse.value:
        job_type = connection_config.get("job_type", "")
        if connection_config.get("authentication_type", "") == "Service Principal" and job_type in ["failed_rows", "cross_source"]:
            connection_config.update({
                "client_id": decrypt(connection_config.get("client_id")),
                "client_secret": decrypt(connection_config.get("client_secret")),
            })
        else:
            connection_config.update(
                {
                    "user": decrypt(connection_config.get("user")),
                    "password": decrypt(connection_config.get("password")),
                }
            )
    elif connection_type == ConnectionType.Postgres.value:
        connection_config.update(
            {
                "username": decrypt(connection_config.get("username")),
                "password": decrypt(connection_config.get("password")),
            }
        )
    elif connection_type == ConnectionType.Redshift.value:
        connection_config.update(
            {
                "user": decrypt(connection_config.get("user")),
                "password": decrypt(connection_config.get("password")),
            }
        )
    elif connection_type == ConnectionType.Redshift_Spectrum.value:
        connection_config.update(
            {
                "user": decrypt(connection_config.get("user")),
                "password": decrypt(connection_config.get("password")),
            }
        )
    elif connection_type == "gcpbigquery":  # ConnectionType.BigQuery.value:
        extraparam = json.loads(connection_config.get("extra", {}))
        extraparam.update(
            {
                "extra__google_cloud_platform__keyfile_dict": decrypt(
                    extraparam.get("extra__google_cloud_platform__keyfile_dict")
                ),
            }
        )
        connection_config.update(
            {
                "extra": extraparam,
            }
        )
    elif connection_type == ConnectionType.BigQuery.value:
        connection_config.update({"keyjson": json.loads(decrypt(connection_config.get("keyjson", "")))})
    elif connection_type == ConnectionType.Oracle.value:
        connection_config.update(
            {
                "user": decrypt(connection_config.get("user")),
                "password": decrypt(connection_config.get("password")),
            }
        )
    elif connection_type == ConnectionType.Dbt.value:
        connection_config.update(
            {
                "account_id": decrypt(connection_config.get("account_id")),
                "api_key": decrypt(connection_config.get("api_key")),
            }
        )
    elif connection_type == ConnectionType.ADF.value:
        connection_config.update(
            {"client_secret": decrypt(connection_config.get("client_secret"))}
        )
    elif connection_type == ConnectionType.Fivetran.value:
        connection_config.update(
            {
                "api_secret": decrypt(connection_config.get("api_secret")),
                "api_key": decrypt(connection_config.get("api_key")),
            }
        )
    elif connection_type == ConnectionType.Databricks.value:
        if connection_config.get("token"):
            connection_config.update(
                {
                    "password": decrypt(connection_config.get("token")),
                    "token": decrypt(connection_config.get("token")),
                }
            )
        if connection_config.get("client_secret"):
            connection_config.update(
                {
                    "client_secret": decrypt(
                        connection_config.get("client_secret", "")
                    ),
                }
            )

    elif connection_type == ConnectionType.Athena.value:
        job_type = connection_config.get("job_type", "")
        if job_type in ["failed_rows", "cross_source"]:
            connection_config.update(
                {
                    "awsaccesskey": decrypt(connection_config.get("awsaccesskey")),
                    "awssecretaccesskey": decrypt(connection_config.get("awssecretaccesskey")),
                }
            )
        else:
            extraparam = json.loads(connection_config.get("extra", {}))
            extraparam.update(
                {
                    "aws_access_key_id": decrypt(extraparam.get("aws_access_key_id")),
                    "aws_secret_access_key": decrypt( extraparam.get("aws_secret_access_key")),
                }
            )
            
            connection_config.update({"extra": extraparam })
    
    elif connection_type == ConnectionType.Airbyte.value:
        connection_config.update(
            {
                "client_id": decrypt(connection_config.get("client_id")),
                "client_secret": decrypt(connection_config.get("client_secret")),
            }
        )
    elif connection_type == ConnectionType.EmrSpark.value:
        extraparam = json.loads(connection_config.get("extra", {}))
        extraparam.update(
            {
                "aws_access_key_id": decrypt(extraparam.get("aws_access_key_id")),
            }
        )
        extraparam.update(
            {
                "aws_secret_access_key": decrypt(
                    extraparam.get("aws_secret_access_key")
                ),
            }
        )
        connection_config.update(
            {
                "extra": extraparam,
            }
        )
    elif connection_type == ConnectionType.Airflow.value:
        connection_config.update(
            {
                "username": decrypt(connection_config.get("username")),
                "password": decrypt(connection_config.get("password")),
                "base_auth_key": decrypt(connection_config.get("base_auth_key")),
            }
        )
    elif connection_type == ConnectionType.Talend.value:
        connection_config.update(
            {
                "api_key": decrypt(connection_config.get("api_key")),
            }
        )
    elif connection_type == ConnectionType.Hive.value:
        connection_config.update(
            {
                "user": decrypt(connection_config.get("user")),
                "password": decrypt(connection_config.get("password")),
            }
        )
    elif connection_type == ConnectionType.PowerBI.value:
        if connection_config.get("authentication_type") == "Service Principle":
            connection_config.update(
                {
                    "client_secret": decrypt(connection_config.get("client_secret")),
                }
            )
        else:
            connection_config.update(
                {
                    "user": decrypt(connection_config.get("user")),
                    "password": decrypt(connection_config.get("password")),
                }
            )
    elif connection_type == ConnectionType.Salesforce.value:
        # Handle Salesforce Marketing Cloud JDBC connections
        if connection_config.get("connection_type") == "jdbc":
            connection_config.update({
                "client_id": decrypt(connection_config.get("client_id")),
                "client_secret": decrypt(connection_config.get("client_secret")),
                "sub_domain": decrypt(connection_config.get("sub_domain")),
                "username": decrypt(connection_config.get("username")),
                "password": decrypt(connection_config.get("password")),
            })
        else:
            # Handle regular Salesforce connections
            connection_config.update(
                {
                    "user": decrypt(connection_config.get("user")),
                    "password": decrypt(connection_config.get("password")),
                }
            )
    elif connection_type == ConnectionType.SalesforceMarketing.value:
        # Handle Salesforce Marketing Cloud JDBC connections
        if connection_config.get("connection_type") == "jdbc":
            connection_config.update({
                "client_id": decrypt(connection_config.get("client_id")),
                "client_secret": decrypt(connection_config.get("client_secret")),
                "sub_domain": decrypt(connection_config.get("sub_domain")),
                "username": decrypt(connection_config.get("username")),
                "password": decrypt(connection_config.get("password")),
            })
        else:
            # Handle regular Salesforce connections
            connection_config.update(
                {
                    "client_id": decrypt(connection_config.get("client_id")),
                    "client_secret": decrypt(connection_config.get("client_secret")),
                    "sub_domain": decrypt(connection_config.get("sub_domain")),
                    "account_id": decrypt(connection_config.get("account_id")),
                    "username": decrypt(connection_config.get("username")),
                    "password": decrypt(connection_config.get("password")),
                }
            )
    elif connection_type == ConnectionType.ADLS.value:
        connection_config.update(
            {
                "client_secret": decrypt(connection_config.get("client_secret")),
                "storage_access_key": decrypt(
                    connection_config.get("storage_access_key")
                ),
                 "storage_account_key": decrypt(connection_config.get("storage_account_key")),
            }
        )
    elif connection_type == ConnectionType.S3.value:
        connection_config.update(
            {
                "aws_secret_access_key": decrypt(connection_config.get("aws_secret_access_key")),
            }
        )
    elif connection_type == ConnectionType.Sigma.value:
        connection_config.update(
            {
                "client_id": decrypt(connection_config.get("client_id")),
                "client_secret": decrypt(connection_config.get("client_secret")),
            }
        )
    else:
        connection_config.update(
            {
                "user": decrypt(connection_config.get("user")),
                "password": decrypt(connection_config.get("password")),
            }
        )
    return connection_config


def get_databricks_datasize(records: dict):
    """get the datasize of a table in databricks"""
    extracted_number = 0
    df = pd.DataFrame.from_records(records)
    if "Statistics" in df["col_name"]:
        stats_list = list(df[df["col_name"] == "Statistics"]["data_type"])

        # Regular expression pattern to extract the number
        pattern = r"(\d+) bytes"

        # Extract the number using re.search
        match = re.search(pattern, stats_list[0])

        if match:
            extracted_number = match.group(1)

    return int(extracted_number)


def get_synapse_datasize(records: dict):
    """get the datasize of a table in synapse"""

    df = pd.DataFrame.from_records(records)
    datasize = int(df["data"][0].split()[0])

    return int(datasize)


def get_databricks_freshness(records: dict):
    """Calculate the freshness (age) of a Databricks table in seconds"""
    
    try:
        df = pd.DataFrame.from_records(records)
        
        # Check if DataFrame is empty or missing required column
        if df.empty or "lastmodified" not in df.columns:
            return 0
            
        timestamp = df["lastmodified"].iloc[0]
        
        # Handle None or empty timestamp
        if not timestamp:
            return 0
            
        # Convert to pandas Timestamp with timezone awareness
        if isinstance(timestamp, str):
            timestamp = pd.Timestamp(timestamp)
        
        # Ensure timezone awareness
        if timestamp.tz is None:
            timestamp = timestamp.tz_localize('UTC')
        
        # Calculate seconds since epoch
        table_seconds = int(timestamp.timestamp())
        
        # Get current time in seconds
        current_seconds = int(datetime.now().timestamp())
        
        # Calculate freshness (difference in seconds)
        freshness = current_seconds - table_seconds
        
        return max(0, freshness)  # Ensure non-negative
        
    except Exception as e:
        # Log the error in production code
        print(f"Error calculating Databricks freshness: {e}")
        return 0


def check_is_same_source(source: dict, target: dict):
    """
    Return true if both config are same, otherwise false
    """
    source_details = source.get("connection", {}).get("credentials")
    target_details = target.get("credentials")
    is_same_source = False
    if source.get("connection_id") == target.get("connection_id"):
        is_same_source = True
        return is_same_source

    if source.get("connection_type") == target.get("connection_type"):
        if source.get("connection_type") == ConnectionType.Snowflake.value:
            is_same_source = source_details.get("account") == target_details.get(
                "account"
            ) and source_details.get("warehouse") == target_details.get("warehouse")
        elif source.get("connection_type") in [
            ConnectionType.Oracle.value,
            ConnectionType.Db2.value,
            ConnectionType.Databricks.value,
            ConnectionType.MySql.value,
            ConnectionType.Postgres.value,
            ConnectionType.MongoDB.value,
            ConnectionType.SapHana.value,
            ConnectionType.Hive.value,
            ConnectionType.DB2IBM.value,
            ConnectionType.AlloyDB.value,
        ]:
            is_same_source = source_details.get("server") == target_details.get(
                "server"
            ) and source_details.get("port") == target_details.get("port")
        elif source.get("connection_type") in [ConnectionType.Synapse.value,ConnectionType.MSSQL.value]:
            is_same_source = source_details.get("server") == target_details.get(
                "server"
            ) and source_details.get("database") == target_details.get(
                "database"
            )
        else:
            is_same_source = source_details.get("database") == target_details.get(
                "database"
            )
    return is_same_source


def get_aws_freshness(last_alered_time: object):
    """get the freshness of databricks"""

    seconds_since_epoch = last_alered_time.timestamp()

    # Get current time in seconds
    current_time = datetime.now()
    current_seconds = int(current_time.timestamp())

    # Find the freshness with the different between current timestamp and table seconds
    freshness = current_seconds - seconds_since_epoch

    return int(freshness)


def format_attribute_name(connection_type: str, attribute_name: str):
    """format the attribute name"""
    if connection_type in [
        ConnectionType.Snowflake.value,
        ConnectionType.Oracle.value,
        ConnectionType.Denodo.value,
        ConnectionType.Redshift_Spectrum.value,
        ConnectionType.Redshift.value,
        ConnectionType.SapHana.value,
    ]:
        attribute_name = f'"{attribute_name}"'
    elif connection_type == ConnectionType.Teradata.value:
        attribute_name = f'"{attribute_name.strip()}"'
    elif connection_type in [ConnectionType.MSSQL.value, ConnectionType.Synapse.value]:
        attribute_name = f"[{attribute_name}]"
    elif connection_type in [
        ConnectionType.Databricks.value,
        ConnectionType.BigQuery.value,
        ConnectionType.Hive.value,
        ConnectionType.MySql.value
    ]:
        attribute_name = f"`{attribute_name}`"
    return attribute_name