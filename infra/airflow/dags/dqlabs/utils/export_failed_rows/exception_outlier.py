import re
import json
from uuid import uuid4
from dqlabs.utils.cross_source import execute_export_failed_rows_query
from dqlabs.utils.extract_failed_rows import (
    get_measures, get_failed_row_query, get_queries, has_limit,
    lookup_processing_query
)
from dqlabs.app_helper.connection_helper import get_attribute_names
from dqlabs.app_helper.dq_helper import check_is_direct_query
from dqlabs.app_helper.dag_helper import execute_native_query, get_postgres_connection, fetchall, fetchone
from dqlabs.utils.export_failed_rows.config_helpers import get_complete_export_settings, process_measure_export_limits
from dqlabs.enums.connection_types import ConnectionType
from dqlabs.app_helper.log_helper import log_error
from dqlabs.app_helper.db_helper import execute_query, split_queries
from dqlabs.app_helper.sql_group_parse import append_columns_to_query
from dqlabs.app_constants.dq_constants import (
    QUERY,
    PARAMETER,
    DIRECT_QUERY_BASE_TABLE_LABEL,
    CROSS_SOURCE
)

def extract_exception_outlier(config: dict):
    """
    Exception outlier for failed rows with optional hashkey and assignment
    
    Args:
        config: Configuration dictionary
        with_hashkey: If True, adds row_hashkey column to the query
        assignment_rules: Optional list of assignment rules for auto-assignment
                         Format: [{"rule": {...}, "assignee": "user_id"}, ...]
    
    Returns:
        measures: List of measures with processed queries
    """
    connection = config.get("connection")
    default_source_queries = get_queries(config)
    is_direct_query_asset = check_is_direct_query(config)
    export_settings = get_complete_export_settings(config)
    limit_condition_query = default_source_queries.get("limit_query")
    connection_type = config.get("connection_type")
    base_table_query = config.get("base_table_query")

    # Get assignment rules for failed rows
    user_assignment_rules = get_assignment_rules(config)
    if not user_assignment_rules:
        return
    assignment_rules = user_assignment_rules.get("user_assignment", [])
    default_assignee = user_assignment_rules.get("user_id")

    measures = get_measures(config, exception_outlier=True)
    if not measures:
        return

    for measure in measures:
        measure_id = measure.get("id")
        measure_category = measure.get("category")
        query_string, _, has_failed_rows_query = (
            get_failed_row_query(config, measure, default_source_queries, is_direct_query_asset)
        )
        if not query_string:
            continue
        export_limits = process_measure_export_limits(
                    measure, export_settings.export_row_limit, export_settings.export_column_limit
                )
        measure_export_row_limit = export_limits.get("measure_export_row_limit")

        if measure_export_row_limit and limit_condition_query:
            if (
                "order by" in query_string
                and "order by (select 1)" in limit_condition_query
            ):
                limit_condition_query = limit_condition_query.replace(
                        "order by (select 1)", ""
                    )
            if not has_limit(connection_type, query_string):
                if (
                    measure_category == "lookup"
                    and "<query_string>" in query_string
                ):
                    query_string = f"{query_string}"
                else:
                    if "distinct" not in query_string.lower():
                        from dqlabs.utils.export_failed_rows.measure_helpers import merge_limit_query
                        query_string = merge_limit_query(
                            query_string, limit_condition_query, connection_type
                        )
            select_query = query_string
            if (
                    measure_category == "lookup"
                    and "<query_string>" in query_string
                ):
                    (
                        select_query,
                        query_string,
                        lookup_group_query,
                    ) = lookup_processing_query(
                        config,
                        measure,
                        select_query,
                        query_string,
                        limit_condition_query,
                        measure_export_row_limit,
                        True,
                    )
            else:
                if measure_category == "lookup":
                    if ("count(*)" in query_string.lower()) and (
                        "count(*) as " not in query_string.lower()
                    ):
                        compiled = re.compile(re.escape("count(*)"), re.IGNORECASE)
                        query_string = compiled.sub(
                            'count(*) as "COUNT(*)"', query_string
                        )

            if (
                (measure_category == QUERY or measure_category == PARAMETER)
                and is_direct_query_asset
                and base_table_query
            ):
                query_string = query_string.replace(
                    DIRECT_QUERY_BASE_TABLE_LABEL, f"({base_table_query})"
                )

            query_string = (query_string.replace("<metadata_attributes>", "")
                .replace("<query_attributes>", "*"))
            failed_rows_query_string = query_string
            query_string = query_string.replace("<count>", "1")
            
            if measure_category == CROSS_SOURCE:
                connection_type = "spark"
                failed_rows_data = execute_export_failed_rows_query(config, measure, query_string)
            else:
                failed_rows_data, _ = execute_native_query(config, query_string, is_list=True, convert_lower=False)
            if not failed_rows_data:
                continue
            failed_rows_data = failed_rows_data[0] if failed_rows_data else []
            hashkey_columns = list(failed_rows_data.keys())
            hashkey_columns = [column for column in hashkey_columns if column.lower() != "row_number"]
            hashkey_query = prepare_hash_key_query(config, hashkey_columns, connection_type)

            failed_rows_query_string = (
                failed_rows_query_string.replace("<count>", str(measure_export_row_limit))
                .replace("<metadata_attributes>", "")
                .replace("<query_attributes>", "*")
            )

            assignment_query = prepare_assignment_query(assignment_rules, default_assignee, hashkey_columns)

            if failed_rows_query_string.lower().startswith("with"):
                failed_rows_query = append_columns_to_query(failed_rows_query_string, [hashkey_query, assignment_query], [])
            else:
                alias_name = "failed_rows" if connection_type == ConnectionType.Oracle.value else "as failed_rows"
                failed_rows_query = f"""
                    select {hashkey_query}, {assignment_query} from ({failed_rows_query_string}) {alias_name}
                """

            if measure_category == CROSS_SOURCE:
                failed_records = execute_export_failed_rows_query(config, measure, failed_rows_query, is_create_table=False)
            else:
                failed_records, _ = execute_native_query(config, failed_rows_query, is_list=True)
            insert_failed_rows(config, measure_id, failed_records, hashkey_columns)



def insert_failed_rows(config: dict, measure_id: str, failed_rows_data: list, columns: list):
    connection_id = config.get("connection_id")
    asset_id = config.get("asset_id")
    run_id = config.get("run_id")
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        results = []
        for row in failed_rows_data:
            query_input = (
                str(uuid4()),
                row.get("hash_key"),
                row.get("user_id"),
                "invalid",
                connection_id,
                asset_id,
                measure_id,
                run_id,
                json.dumps(columns) if isinstance(columns, list) else columns
            )
            input_literals = ", ".join(["%s"] * len(query_input))
            query_param = cursor.mogrify(
                f"({input_literals},CURRENT_TIMESTAMP)",
                query_input,
            ).decode("utf-8")
            results.append(query_param)

        failed_rows_input = split_queries(results)
        for input_values in failed_rows_input:
            try:
                input_value = ",".join(input_values)
                failed_row_insert_query = f"""
                    WITH upsert AS (
                        UPDATE core.exception AS main
                        SET
                            run_id = data.run_id::uuid,
                            columns = data.columns::jsonb,
                            modified_date = CURRENT_TIMESTAMP,
                            user_id = data.user_id::uuid
                        FROM (VALUES
                            {input_value}
                        ) AS data(id, hash_key, user_id, status, connection_id, asset_id, measure_id, run_id, columns)
                        WHERE
                            main.hash_key = data.hash_key
                            AND main.measure_id = data.measure_id::uuid
                        RETURNING main.id
                    )
                    INSERT INTO core.exception (
                        id, hash_key, user_id, status, connection_id, asset_id, measure_id, run_id, columns, created_date, modified_date
                    )
                    SELECT
                        data.id::uuid, data.hash_key, data.user_id::uuid,  status,  data.connection_id::uuid,
                        data.asset_id::uuid, data.measure_id::uuid, data.run_id::uuid, data.columns::jsonb, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    FROM (VALUES {input_value}) AS data(id, hash_key, user_id, status, connection_id, asset_id, measure_id, run_id, columns)
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM core.exception AS main
                        WHERE
                            main.hash_key = data.hash_key
                            AND main.measure_id = data.measure_id::uuid
                    )
                """                
                cursor = execute_query(
                    connection, cursor, failed_row_insert_query)
            except Exception as e:
                log_error("Save Failed Rows: inserting/updating failed row", e)


def extract_columns(condition: str):
    """
    Extract potential column names from a condition string, ignoring SQL keywords,
    numbers, and string literals.
    Supports multiple quoting styles:
    - Backticks: `DEPARTMENT`
    - Square brackets: [DEPARTMENT]
    - Double quotes: "DEPARTMENT" or "/SELL/COSTINRS"
    - Single quotes: 'MAKE_CODE' (when used as identifier, not in value lists)
    Distinguishes between quoted column names and string literal values.
    """
    SQL_KEYWORDS = {
        # Logical
        "AND", "OR", "NOT", "ALL", "ANY", "SOME", "EXISTS",
        # Comparison
        "=", "<>", "!=", "<", ">", "<=", ">=", "BETWEEN", "IN", "LIKE", "SIMILAR", "ILIKE", "IS", "NULL",
        # Case
        "CASE", "WHEN", "THEN", "ELSE", "END", "TRUE", "FALSE",
        # Functions
        "UPPER", "LOWER", "TRIM", "SUBSTRING", "COALESCE", "NVL", "CAST", "CONVERT",
        # Misc
        "AS"
    }
    columns = set()
    
    # Step 1: Extract backtick-quoted identifiers (MySQL style: `column`)
    backtick_pattern = r'`([^`]+)`'
    backtick_matches = re.findall(backtick_pattern, condition)
    for match in backtick_matches:
        col = match.strip()
        if col and col not in SQL_KEYWORDS and not col.isdigit():
            columns.add(col.upper())
    
    # Step 2: Extract square-bracket-quoted identifiers (SQL Server style: [column])
    bracket_pattern = r'\[([^\]]+)\]'
    bracket_matches = re.findall(bracket_pattern, condition)
    for match in bracket_matches:
        col = match.strip()
        if col and col not in SQL_KEYWORDS and not col.isdigit():
            columns.add(col.upper())
    
    # Step 3: Extract double-quoted identifiers (PostgreSQL/Standard SQL: "column")
    double_quoted_pattern = r'"([^"]+)"'
    double_quoted_matches = re.findall(double_quoted_pattern, condition)
    for match in double_quoted_matches:
        col = match.strip()
        if col and col not in SQL_KEYWORDS and not col.isdigit():
            columns.add(col.upper())
    
    # Step 4: Handle single-quoted identifiers vs literals
    # Single quotes are tricky - they can be identifiers or literals
    # Strategy: 
    # - If single-quoted string appears in value lists (IN, BETWEEN, etc.), it's a literal
    # - If it appears before operators/keywords, it's likely an identifier
    # - We need to check context carefully
    
    # Find all single-quoted strings with their positions in original condition
    single_quoted_pattern = r"'([^']+)'"
    all_single_quoted = list(re.finditer(single_quoted_pattern, condition))
    
    # Create a version without other quote types for context analysis
    condition_for_context = condition
    condition_for_context = re.sub(r'`[^`]+`', 'X', condition_for_context)  # Replace with placeholder
    condition_for_context = re.sub(r'\[[^\]]+\]', 'X', condition_for_context)
    condition_for_context = re.sub(r'"[^"]+"', 'X', condition_for_context)
    
    # Check each single-quoted string to see if it's an identifier or literal
    for match in all_single_quoted:
        start_pos = match.start()
        end_pos = match.end()
        col_name = match.group(1).strip()
        
        # Skip if it's a SQL keyword or number
        if not col_name or col_name.upper() in SQL_KEYWORDS or col_name.isdigit():
            continue
        
        # Get context before and after
        before_context = condition_for_context[max(0, start_pos-30):start_pos].strip()
        after_context = condition_for_context[end_pos:end_pos+30].strip()
        
        # Check if it's in a value list (IN, BETWEEN, etc.) - these are literals
        # Pattern: Look for "IN (" or "BETWEEN" before the quote, or inside parentheses after keywords
        is_in_value_list = False
        if re.search(r'\bIN\s*\([^)]*$', before_context, re.IGNORECASE):
            is_in_value_list = True
        if re.search(r'\bBETWEEN\s+', before_context, re.IGNORECASE):
            # Check if this is the second value in BETWEEN (after AND)
            if re.search(r'\bAND\s+', before_context, re.IGNORECASE):
                is_in_value_list = True
        # Check if we're inside parentheses that likely contain values
        if re.search(r'\([^)]*$', before_context) and not re.search(r'\w+\s*\([^)]*$', before_context):
            # Inside parentheses but not a function call - likely a value list
            is_in_value_list = True
        
        # If it's not in a value list, check if it's an identifier
        if not is_in_value_list:
            # Check if followed by operators/keywords that suggest it's an identifier
            if after_context:
                identifier_indicators = r'^\s*(=|<>|!=|<|>|<=|>=|\s+IN\s*\(|\s+LIKE|\s+ILIKE|\s+SIMILAR|\s+IS\s+)'
                if re.match(identifier_indicators, after_context, re.IGNORECASE):
                    columns.add(col_name.upper())
                    continue
            # Check if it's at the start or after logical operators (AND, OR) - likely identifier
            if not before_context or re.search(r'^\s*$|^\s*(AND|OR|WHERE|HAVING)\s+', before_context, re.IGNORECASE):
                if after_context and re.search(r'^\s*(=|<>|!=|<|>|<=|>=|IN|LIKE)', after_context, re.IGNORECASE):
                    columns.add(col_name.upper())
    
    # Step 5: Remove all quoted strings (we've already extracted identifiers)
    # This includes string literals that we want to ignore
    condition_clean = condition
    condition_clean = re.sub(r'`[^`]+`', '', condition_clean)
    condition_clean = re.sub(r'\[[^\]]+\]', '', condition_clean)
    condition_clean = re.sub(r'"[^"]+"', '', condition_clean)
    condition_clean = re.sub(r"'[^']*'", '', condition_clean)
    
    # Step 6: Extract unquoted identifiers (column names without quotes)
    tokens = re.findall(r"\b\w+\b", condition_clean.upper())
    for t in tokens:
        if t not in SQL_KEYWORDS and not t.isdigit():
            columns.add(t)
    
    return columns

def prepare_assignment_query(assignment_rules: list, default_assignee: str, available_columns: list):
    """
    Args:
        assignment_rules: List of assignment rules
        default_assignee: Default assignee
        available_columns: List of available columns
    
    Returns:
        Assignment query
    """
    available_columns_set = {column.upper() for column in available_columns}
    when_clauses = []
    for rule in assignment_rules:
        condition = rule.get('condition', '')
        user = rule.get("user")
        columns_in_condition = extract_columns(condition)
        if columns_in_condition and columns_in_condition <= available_columns_set:
            when_clauses.append(f"WHEN {condition} THEN '{user}'")
    if when_clauses:
        return f"CASE {' '.join(when_clauses)} ELSE '{default_assignee}' END AS user_id"
    else:
        return f"'{default_assignee}' AS user_id"

def prepare_hash_key_query(config: dict, columns: list, connection_type: str, is_condition: bool = False):
    """
    Prepare a reliable hash key query for failed rows, using ConnectionType enum
    and connector-specific simple MD5/hash implementations.
    """
    connection_type = connection_type.lower() if connection_type else connection_type
    columns = get_attribute_names(connection_type, columns) if not is_condition else columns
    delimiter = "|"
    hash_key_query = ""
    alias_name = " as hash_key" if not is_condition else ""

    if connection_type == ConnectionType.MongoDB.value:
        if not columns:
            return ""
        concat_parts = []
        for i, col in enumerate(columns):
            if i > 0:
                concat_parts.append(f'"{delimiter}"')
            # Remove $ prefix if present, then add it back
            field_name = col.replace("$", "")
            concat_parts.append(f'"${field_name}"')
        concat_expr = ", ".join(concat_parts)
        # Construct JSON expression: {"$toHex": {"$md5": {"$concat": [...]}}}
        json_expr = '{"$toHex": {"$md5": {"$concat": [' + concat_expr + ']}}}'
        if is_condition:
            # For $match condition: return expression for use in $expr
            # Note: This is used in prepare_marked_normal_where_clause which returns $match stage
            hash_key_query = json_expr
        else:
            # For $project: return projection expression
            hash_key_query = json_expr
        return hash_key_query

    if connection_type in [ConnectionType.Snowflake.value, ConnectionType.MySql.value]:
        columns = [f"COALESCE(CAST({column} AS VARCHAR), '')" for column in columns]
        hash_key_query = f"MD5(CONCAT_WS('{delimiter}', {', '.join(columns)})) {alias_name}"
    elif connection_type in [ConnectionType.MSSQL.value, ConnectionType.Synapse.value]:
        columns = [f"ISNULL(CAST({column} AS VARCHAR(255)), '')" for column in columns]
        columns = f", '{delimiter}' ,".join(columns)
        hash_key_query = f"CONVERT(VARCHAR(32),HASHBYTES('MD5',CONCAT({columns})), 2) {alias_name}"
    elif connection_type == ConnectionType.Oracle.value:
        columns = " || '{}' || ".format(delimiter).join([f"NVL(TO_CHAR({c}), '')" for c in columns])
        hash_key_query = f"LOWER(STANDARD_HASH({columns}, 'MD5')) {alias_name}"
    elif connection_type == ConnectionType.SapHana.value:
        columns = " || '{}' || ".format(delimiter).join([f"""COALESCE(CAST("{column}" AS NVARCHAR), '')""" for column in columns])
        hash_key_query = f"TO_VARCHAR(HASH_MD5(TO_BINARY({columns}))) {alias_name}"
    elif connection_type == ConnectionType.BigQuery.value:
        columns = [f"IFNULL(CAST({column} AS STRING), '')" for column in columns]
        hash_key_query = f"""TO_HEX(MD5(CONCAT({f", '{delimiter}' ,".join(columns)}))) {alias_name}"""
    elif connection_type == ConnectionType.Databricks.value:
        columns = [f"IFNULL(CAST({column} AS STRING), '')" for column in columns]
        hash_key_query = f"""MD5(CONCAT({f", '{delimiter}' ,".join(columns)})) {alias_name}"""
    elif connection_type == ConnectionType.Athena.value:
        columns = " || '{}' || ".format(delimiter).join([f"COALESCE(CAST({column} AS VARCHAR), '')" for column in columns])
        hash_key_query = f"TO_HEX(MD5(to_utf8({columns}))) {alias_name}"
    elif connection_type in [ConnectionType.Spark.value, ConnectionType.ADLS.value, ConnectionType.S3.value, ConnectionType.File.value]:
        columns = " || '{}' || ".format(delimiter).join([f"COALESCE(CAST({column} AS STRING), '')" for column in columns])
        hash_key_query = f"md5({columns}) {alias_name}" 
    else:
        columns = " || '{}' || ".format(delimiter).join([f"COALESCE(CAST({column} AS VARCHAR), '')" for column in columns])
        hash_key_query = f"md5({columns}) {alias_name}" 
    return hash_key_query


def get_marked_normal_outlier_values(config: dict, measure_id: str):

    # Check Report Settings for Exception Outlier
    dag_info = config.get("dag_info", {})
    dag_info = dag_info if dag_info else {}
    general_settings = dag_info.get("settings", {})
    general_settings = general_settings if general_settings else {}
    general_settings = json.loads(general_settings, default=str) if isinstance(general_settings, str) else general_settings
    report_settings = general_settings.get("reporting", {})
    if isinstance(report_settings, str):
        report_settings = json.loads(report_settings, default=str)
    exception_outlier = report_settings.get("exception_outlier", False)
    if not exception_outlier:
        return []

     
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            select distinct hash_key, columns from core.exception 
            where measure_id = '{measure_id}' and status = 'false_positive'
        """
        cursor = execute_query(connection, cursor, query_string)
        result = fetchall(cursor)
        result = result if result else []
        return result

def prepare_marked_normal_where_clause(config: dict, marked_normal_records: list, connection_type: str):
    if not marked_normal_records:
        return None
    columns = marked_normal_records[0].get("columns")
    columns = json.loads(columns) if isinstance(columns, str) else columns
    columns = get_attribute_names(connection_type, columns)
    hash_keys = [record.get("hash_key") for record in marked_normal_records]
    if connection_type == ConnectionType.MongoDB.value:
        if not hash_keys:
            return None
        # Return JSON string for $match stage: {"$match": {"hash_key": {"$nin": [...]}}}
        hash_keys_json = json.dumps(hash_keys)
        match_stage = json.dumps([{"$match": {"hash_key": {"$nin": hash_keys}}}])
        return match_stage
    hash_keys = "', '".join(hash_keys)
    hash_key_query = prepare_hash_key_query(config, columns, connection_type, is_condition=True)
    where_clause = f"{hash_key_query} not in ('{hash_keys}')"
    return where_clause


def get_assignment_rules(config: dict):
    """
    Get assignment rules for failed rows
    """
    asset_id = config.get("asset_id")
    measure_id = config.get("measure_id")
    connection = get_postgres_connection(config)
    response = {}
    with connection.cursor() as cursor:
        if measure_id:
            query_string = f"""select configuration from core.exception_workflow
             where measure_id='{measure_id}' 
             and is_active=true order by created_date desc limit 1"""
        else:
            query_string = f"""select configuration from core.exception_workflow 
            where asset_id='{asset_id}' and is_active=true order by created_date desc limit 1"""

        cursor = execute_query(connection, cursor, query_string)
        result = fetchone(cursor)
       
        # Connection Level Condition
        if not result:
            condition = f"and asset.id ='{asset_id}'" if asset_id else f"and measure.id = '{measure_id}'"
            query_string = f"""
            select exception_workflow.configuration from core.exception_workflow
            left join core.asset on asset.connection_id = exception_workflow.connection_id
            left join core.measure on measure.asset_id = asset.id
            where exception_workflow.is_active=true and exception_workflow.asset_id is null
            and exception_workflow.measure_id is null {condition}
            order by exception_workflow.created_date desc limit 1
            """
            cursor = execute_query(connection, cursor, query_string)
            result = fetchone(cursor)

        # Semantic Level Condition
        if not result:
            with_clause = f"""
              with base_result as (
                select distinct id as asset_id from core.asset
                where id = '{asset_id}'
              ),
            """
            if measure_id:
                with_clause = f"""
                    with base_result as (
                        select distinct id as measure_id from core.measure
                        where id = '{measure_id}'
                    ),
                """
            join_attribute_name = "asset_id" if asset_id else "measure_id"
            tag_join = f"""
                union all
                select 'tags' as type, tags_id, asset_id, null as measure_id, exception_workflow_id
                from core.tags_mapping where (asset_id is not null and level='asset')
                or exception_workflow_id is not null
            """
            query_string = f"""
               {with_clause}
                mappings as (
                  select 'application' as type, application_id as map_id, asset_id, measure_id, exception_workflow_id
                  from core.application_mapping where asset_id is not null or measure_id is not null
                  or exception_workflow_id is not null
                  union all
                  select 'product' as type, product_id as map_id, asset_id, measure_id, exception_workflow_id
                  from core.product_mapping where asset_id is not null or measure_id is not null
                  or exception_workflow_id is not null
                  union all
                  select 'domain' as type, domain_id as map_id, asset_id, measure_id, exception_workflow_id
                  from core.domain_mapping where asset_id is not null or measure_id is not null
                  or exception_workflow_id is not null
                  {tag_join}
               ),
               exception_workflow as (
                  select distinct workflow.exception_workflow_id
                  from mappings as mapping
                  join mappings as workflow
                    on mapping.type = workflow.type
                    and mapping.map_id = workflow.map_id
                    and workflow.exception_workflow_id is not null
                    where mapping.{join_attribute_name} = (select {join_attribute_name} FROM base_result)
               )
               select exception_workflow.configuration
                from core.exception_workflow
                where exception_workflow.is_active = true
                and exception_workflow.asset_id is null and exception_workflow.measure_id is null and exception_workflow.connection_id is null
                and exception_workflow.id IN (select exception_workflow_id FROM exception_workflow)
                order by exception_workflow.created_date desc
                limit 1;
            """
            cursor = execute_query(connection, cursor, query_string)
            result = fetchone(cursor)

        if not result:
            return {}

        # Default User Assignment
        default_user_query_string = f"""select created_by from core.asset where id='{asset_id}'"""
        if measure_id:
            default_user_query_string = f"""select created_by from core.measure where id='{measure_id}'"""

        cursor = execute_query(connection, cursor, default_user_query_string)
        user_result = fetchone(cursor)
        user_result = user_result if user_result else {}
        user_id = user_result.get("created_by")
        user_assignment = []
        result = result if result else {}
        if result:
            configuration = json.loads(result.get("configuration")) if isinstance(result.get("configuration"), str) else result.get("configuration")
            user_assignment = configuration.get("user_assignment", [])
        response = {
            "user_id": user_id,
            "user_assignment": user_assignment
        }
        return response