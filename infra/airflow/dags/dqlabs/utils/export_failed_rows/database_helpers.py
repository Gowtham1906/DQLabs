"""
Database helper functions for export_failed_rows.
These helpers provide database-specific operations while maintaining
backward compatibility with existing code.
"""

import json
import logging
from typing import Any, Dict, List, Optional
from enum import Enum

logger = logging.getLogger(__name__)


class DatabaseType(Enum):
    MSSQL = "mssql"
    ORACLE = "oracle"
    DATABRICKS = "databricks"
    SNOWFLAKE = "snowflake"
    SAPHANA = "saphana"
    REDSHIFT = "redshift"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    BIGQUERY = "bigquery"
    HIVE = "hive"
    SYNAPSE = "synapse"
    TERADATA = "teradata"
    DB2 = "db2"
    DB2IBM = "db2ibm"


def get_database_type(connection_type: str) -> Optional[DatabaseType]:
    """Get database type enum from connection type string"""
    if not connection_type:
        return None
    
    connection_type_lower = connection_type.lower()
    
    # Import ConnectionType enum
    try:
        from dqlabs.enums.connection_types import ConnectionType
        
        # Map connection types to our database types
        type_mapping = {
            ConnectionType.MSSQL.value.lower(): DatabaseType.MSSQL,
            "microsoft sql server": DatabaseType.MSSQL,
            "sql server": DatabaseType.MSSQL,
            ConnectionType.Oracle.value.lower(): DatabaseType.ORACLE,
            ConnectionType.Databricks.value.lower(): DatabaseType.DATABRICKS,
            ConnectionType.Snowflake.value.lower(): DatabaseType.SNOWFLAKE,
            ConnectionType.SapHana.value.lower(): DatabaseType.SAPHANA,
            "sap hana": DatabaseType.SAPHANA,
            ConnectionType.Redshift.value.lower(): DatabaseType.REDSHIFT,
            ConnectionType.Redshift_Spectrum.value.lower(): DatabaseType.REDSHIFT,
            ConnectionType.Postgres.value.lower(): DatabaseType.POSTGRESQL,
            ConnectionType.AlloyDB.value.lower(): DatabaseType.POSTGRESQL,
            ConnectionType.MySql.value.lower(): DatabaseType.MYSQL,
            ConnectionType.BigQuery.value.lower(): DatabaseType.BIGQUERY,
            ConnectionType.Hive.value.lower(): DatabaseType.HIVE,
            ConnectionType.Synapse.value.lower(): DatabaseType.SYNAPSE,
            ConnectionType.Teradata.value.lower(): DatabaseType.TERADATA,
            ConnectionType.Db2.value.lower(): DatabaseType.DB2,
            ConnectionType.DB2IBM.value.lower(): DatabaseType.DB2IBM,
        }
        
        return type_mapping.get(connection_type_lower)
    except ImportError:
        # Fallback mapping if ConnectionType enum is not available
        fallback_mapping = {
            "mssql": DatabaseType.MSSQL,
            "microsoft sql server": DatabaseType.MSSQL,
            "sql server": DatabaseType.MSSQL,
            "oracle": DatabaseType.ORACLE,
            "databricks": DatabaseType.DATABRICKS,
            "snowflake": DatabaseType.SNOWFLAKE,
            "saphana": DatabaseType.SAPHANA,
            "sap hana": DatabaseType.SAPHANA,
            "redshift": DatabaseType.REDSHIFT,
            "postgresql": DatabaseType.POSTGRESQL,
            "postgres": DatabaseType.POSTGRESQL,
            "mysql": DatabaseType.MYSQL,
            "bigquery": DatabaseType.BIGQUERY,
            "hive": DatabaseType.HIVE,
            "synapse": DatabaseType.SYNAPSE,
            "teradata": DatabaseType.TERADATA,
            "db2": DatabaseType.DB2,
            "db2ibm": DatabaseType.DB2IBM,
        }
        return fallback_mapping.get(connection_type_lower)


def format_identifier(connection_type: str, identifier: str) -> str:
    """
    Format database identifier based on connection type.
    EXACT LOGIC FROM ORIGINAL handle_new_record function.
    """
    db_type = get_database_type(connection_type)
    
    if db_type == DatabaseType.MSSQL:
        return f'[{identifier}]'
    elif db_type == DatabaseType.ORACLE:
        return f'"{identifier.upper()}"'
    elif db_type in [DatabaseType.DATABRICKS, DatabaseType.HIVE]:
        return f'`{identifier}`'
    elif db_type in [DatabaseType.SNOWFLAKE, DatabaseType.REDSHIFT, DatabaseType.POSTGRESQL]:
        return f'"{identifier}"'
    elif db_type == DatabaseType.SAPHANA:
        return f'"{identifier}"'
    else:
        return identifier  # Default case


def format_boolean_value(connection_type: str, value: bool) -> str:
    """
    Format boolean value for database insertion.
    EXACT LOGIC FROM ORIGINAL handle_new_record function.
    """
    db_type = get_database_type(connection_type)
    
    if db_type in [DatabaseType.MSSQL, DatabaseType.ORACLE]:
        return "1" if value else "0"
    elif db_type in [DatabaseType.DATABRICKS, DatabaseType.HIVE]:
        return "TRUE" if value else "FALSE"
    elif db_type in [DatabaseType.REDSHIFT]:
        return "true" if value else "false"
    else:
        return "true" if value else "false"


def get_current_timestamp(connection_type: str) -> str:
    """
    Get current timestamp expression for database.
    EXACT LOGIC FROM ORIGINAL handle_new_record function.
    """
    db_type = get_database_type(connection_type)
    
    if db_type == DatabaseType.MSSQL:
        return "GETDATE()"
    elif db_type == DatabaseType.ORACLE:
        return "SYSDATE"
    elif db_type in [DatabaseType.DATABRICKS, DatabaseType.HIVE]:
        return "CURRENT_TIMESTAMP()"
    elif db_type == DatabaseType.REDSHIFT:
        return "GETDATE()"
    else:
        return "CURRENT_TIMESTAMP"


def escape_json_value(connection_type: str, json_str: str) -> str:
    """
    Escape JSON string for database insertion.
    EXACT LOGIC FROM ORIGINAL handle_new_record function.
    """
    db_type = get_database_type(connection_type)
    
    if db_type in [DatabaseType.DATABRICKS, DatabaseType.HIVE]:
        return json_str.replace("'", "\\'")
    else:
        return json_str.replace("'", "''")


def format_json_for_database(connection_type: str, data: Any) -> str:
    """
    Format JSON data for specific database type.
    EXACT LOGIC FROM ORIGINAL handle_new_record function.
    """
    db_type = get_database_type(connection_type)
    
    if db_type == DatabaseType.ORACLE:
        # Use Oracle CLOB handling for large data
        return format_oracle_clob(data)
    elif db_type == DatabaseType.MSSQL:
        # For MSSQL, properly escape single quotes in JSON
        try:
            # Use standard json.dumps for clean JSON
            clean_json = json.dumps(data)
            # Fix: Clean up excessive backslash escaping that json.dumps introduces
            clean_json = re.sub(r'\\+"', '"', clean_json)
            # Escape single quotes for MSSQL (double them)
            escaped_json = clean_json.replace("'", "''")
            return f"'{escaped_json}'"
        except Exception:
            # Fallback to safe serialization
            return safe_json_serialize_for_database(data, connection_type)
    elif db_type in [DatabaseType.SAPHANA, DatabaseType.DATABRICKS, DatabaseType.HIVE]:
        escaped_json = safe_json_serialize_for_database(data, connection_type).replace("'", "''")
        return f"'{escaped_json}'"
    elif db_type in [DatabaseType.REDSHIFT]:
        # For Redshift, ensure clean JSON without double escaping
        try:
            clean_json = json.dumps(data)
            # Fix: Clean up excessive backslash escaping
            clean_json = re.sub(r'\\+"', '"', clean_json)
            # Only escape single quotes for SQL, preserve JSON structure
            escaped_json = clean_json.replace("'", "''")
            return f"'{escaped_json}'"
        except Exception:
            return safe_json_serialize_for_database(data, connection_type)
    else:
        return safe_json_serialize_for_database(data, connection_type)


def format_oracle_clob(data: Any, chunk_size: int = 2000) -> str:
    """
    Handle Oracle CLOB data by splitting into chunks and using TO_CLOB().
    EXACT LOGIC FROM ORIGINAL handle_oracle_chunked_clob function.
    """
    if not data:
        return "''"
    
    # Serialize the data
    json_data = safe_json_serialize_for_database(data, "oracle")
    
    # Escape single quotes for Oracle
    escaped_data = json_data.replace("'", "''")
    
    # If data is short enough, return as simple string
    if len(escaped_data) <= chunk_size:
        return f"'{escaped_data}'"
    
    # Split into chunks
    chunks = [escaped_data[i:i+chunk_size] for i in range(0, len(escaped_data), chunk_size)]
    
    # Create TO_CLOB concatenation
    clob_expression = " || ".join([f"TO_CLOB('{chunk}')" for chunk in chunks])
    
    logger.info(f"Oracle: Split data into {len(chunks)} chunks using TO_CLOB()")
    logger.info(f"Oracle: Total length: {len(escaped_data)} characters")
    
    return f"({clob_expression})"


def safe_json_serialize_for_database(data: Any, connection_type: str = "") -> str:
    """
    Safely serialize objects to JSON with fallback for problematic types.
    EXACT LOGIC FROM ORIGINAL safe_json_serialize function.
    """
    import re
    
    try:
        result = json.dumps(data)
        # Fix: Clean up excessive backslash escaping that json.dumps introduces
        result = re.sub(r'\\+"', '"', result)
        return result
    except (TypeError, ValueError):
        # If JSON serialization fails, convert to string representation
        if isinstance(data, dict):
            result = json.dumps({k: str(v) for k, v in data.items()})
        elif isinstance(data, list):
            result = json.dumps([str(item) for item in data])
        else:
            result = json.dumps(str(data))
        # Fix: Clean up excessive backslash escaping that json.dumps introduces
        result = re.sub(r'\\+"', '"', result)
        return result


def build_select_values_query(connection_type: str, values_to_insert: List[str], 
                             table_column_names: List[str]) -> str:
    """
    Build SELECT values query for specific database type.
    EXACT LOGIC FROM ORIGINAL handle_new_record function.
    """
    db_type = get_database_type(connection_type)
    
    if db_type == DatabaseType.MSSQL:
        # For MSSQL, we need to provide column aliases for each value        
        select_parts = []
        for i, value in enumerate(values_to_insert):
            if i < len(table_column_names):
                select_parts.append(f"{value} AS {table_column_names[i]}")
            else:
                select_parts.append(value)
        return f"SELECT {', '.join(select_parts)}"
        
    elif db_type == DatabaseType.SAPHANA:
        # For SAP HANA, we need to add FROM dummy clause
        select_parts = []
        for i, value in enumerate(values_to_insert):
            if i < len(table_column_names):
                select_parts.append(f"{value} AS {table_column_names[i]}")
            else:
                select_parts.append(value)
        return f"SELECT {', '.join(select_parts)} FROM dummy"
        
    elif db_type in [DatabaseType.DATABRICKS, DatabaseType.HIVE]:
        # For Databricks, we need to provide column aliases for each value
        select_parts = []
        for i, value in enumerate(values_to_insert):
            if i < len(table_column_names):
                # Ensure proper column aliasing for Databricks
                column_alias = table_column_names[i].replace('"', '').replace('`', '')  # Remove any quotes
                select_parts.append(f"{value} AS `{column_alias}`")
            else:
                select_parts.append(value)
        return f"SELECT {', '.join(select_parts)}"
        
    elif db_type == DatabaseType.ORACLE:
        # For Oracle, we need to provide column aliases and handle FROM DUAL
        select_parts = []
        for i, value in enumerate(values_to_insert):
            if i < len(table_column_names):
                select_parts.append(f"{value} AS {table_column_names[i]}")
            else:
                select_parts.append(value)
        return f"SELECT {', '.join(select_parts)} FROM DUAL"
        
    elif db_type in [DatabaseType.REDSHIFT]:
        # For Redshift, we need to provide column aliases
        select_parts = []
        for i, value in enumerate(values_to_insert):
            if i < len(table_column_names):
                select_parts.append(f"{value} AS {table_column_names[i]}")
            else:
                select_parts.append(value)
        return f"SELECT {', '.join(select_parts)}"
        
    else:
        # For other databases, use the original approach
        values_str = ", ".join([str(value) if value is not None else "NULL" for value in values_to_insert])
        return f"SELECT {values_str}"


def format_limit_clause(connection_type: str, limit: int) -> str:
    """Format limit clause for database"""
    db_type = get_database_type(connection_type)
    
    if db_type == DatabaseType.MSSQL:
        return f"TOP {limit}"
    elif db_type == DatabaseType.ORACLE:
        return f"ROWNUM <= {limit}"
    else:
        return f"LIMIT {limit}"


# Import re at the top level for use in functions
import re
