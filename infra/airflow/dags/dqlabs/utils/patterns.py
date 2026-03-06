"""
    Migration Notes From V2 to V3:
    Migrations Completed 
"""

import json
from uuid import uuid4
from dqlabs.app_helper.db_helper import execute_query, fetchall
from dqlabs.app_helper.dq_helper import (
    convert_to_lower,
    get_attribute_label,
    calculate_weightage_score,
    check_measure_result
)
from dqlabs.app_helper.dag_helper import (
    execute_native_query,
    get_postgres_connection,
    prepare_query_string,
    get_query_string,
    delete_metrics
)
from dqlabs.app_constants.dq_constants import FAILED, PASSED
from dqlabs.app_helper.log_helper import log_error
from dqlabs.enums.connection_types import ConnectionType
from dqlabs.utils.extract_workflow import get_attribute_metadata
from dqlabs.utils import is_scoring_enabled


def get_existing_patterns(config: dict) -> list:
    """
    Checks the whether the given asset as defaults patterns or not
    """
    asset_id = config.get("asset_id")
    connection = get_postgres_connection(config)
    existing_patterns = {}
    with connection.cursor() as cursor:
        query_string = f"""
            select distinct mes.attribute_id, array_agg(mes.base_measure_id) as measures
            from core.measure as mes
            join core.base_measure as base on base.id=mes.base_measure_id and base.is_visible=true
            where mes.asset_id='{asset_id}' and base.type='pattern' and base.is_active=True
            group by mes.attribute_id
        """
        cursor = execute_query(connection, cursor, query_string)
        patterns = fetchall(cursor)
        for pattern in patterns:
            if not pattern:
                continue
            attribute_id = pattern.get("attribute_id")
            measures = pattern.get("measures", [])
            existing_patterns.update({
                attribute_id: measures
            })
    return existing_patterns


def get_default_patterns(config: dict) -> list:
    """
    Returns the list of default patterns
    """
    connection = get_postgres_connection(config)
    patterns = []
    with connection.cursor() as cursor:
        query_string = """
            select * from core.base_measure
            where is_default=True and is_delete=False and lower(type)='pattern'
            and is_active=True
            and term_id is null
        """
        cursor = execute_query(connection, cursor, query_string)
        patterns = fetchall(cursor)
    return patterns


def check_is_positive(pattern: dict, attribute: dict) -> list:
    """
    Returns the list of patterns for an attribute based on the given datatype
    """
    properties = pattern.get("pattern", {})
    properties = properties if properties else {}
    positive_constraints = properties.get("positive_constraints", {})
    positive_constraints = positive_constraints if positive_constraints else {}
    validations = []
    for key, value in positive_constraints.items():
        constraints = attribute.get(key)
        if isinstance(value, list):
            validations.append(constraints and constraints.lower() in value)
        else:
            validations.append(constraints == value)
    is_positive = any(validations)
    if not is_positive:
        is_positive = pattern.get("is_positive", False)
    return is_positive


def get_pattern_query(config: dict, attribute: dict) -> str:
    """
    Returns the list of default patterns
    """
    connection_type = config.get("connection_type")
    attribute_id = attribute.get("id")
    metadata = get_attribute_metadata(config, attribute_id)
    metadata = metadata if metadata else {}
    patterns = metadata.get("user_defined_patterns")
    patterns = json.loads(patterns) if patterns and isinstance(
        patterns, str) else patterns
    patterns = patterns if patterns else []
    default_queries = config.get("default_queries", {})
    total_records = int(attribute.get("row_count", 0))

    # Prepare pattern query string
    attribute_name = attribute.get("name")
    query_string = ""
    if patterns:
        queries = []
        for pattern in patterns:
            if not pattern:
                continue
            name = pattern.get("name")
            pattern_name = pattern.get("technical_name")
            if name and not pattern_name:
                pattern_name = get_attribute_label(name, False, False)
                pattern.update({
                    "technical_name": pattern_name
                })
            if (connection_type.lower() == ConnectionType.MSSQL.value) or (connection_type.lower() == ConnectionType.Synapse.value):
                query = prepare_sql_patterns_query(
                    attribute_name, pattern, config)
                if query:
                    queries.append(query)
            else:
                query = prepare_posix_patterns_query(
                    attribute_name, pattern, config)
                if query:
                    queries.append(query)
        if queries:
            pattern_query = ', '.join(queries)
            query_string = get_query_string(
                config, default_queries, pattern_query, total_records, attribute_name=attribute_name
            )
    return query_string, patterns


def prepare_posix_patterns_query(attribute_name: str, pattern: dict, config: dict) -> str:
    """
    Prepares the query string for the posix patterns for given attributes
    """
    attribute_label = get_attribute_label(attribute_name)
    default_queries = config.get("default_queries", {})
    frequency_query = default_queries.get("frequency", {})
    pattern_query = frequency_query.get("pattern") if frequency_query else ""
    pattern_query = pattern_query if pattern_query else ""

    query = ""
    if not pattern_query:
        return query

    properties = pattern.get("pattern", {})
    properties = properties if properties else {}
    pattern_name = pattern.get("technical_name")
    pattern_value = properties.get("posix_pattern")
    if (not pattern_name) or (not pattern_value):
        return query
    query = prepare_query_string(query=pattern_query, attribute=attribute_name)
    attribute_label = f"{attribute_label}{pattern_name}"
    query = query.replace("<attribute_label>", attribute_label).replace(
        "<pattern>", pattern_value)
    return query


def save_pattern_metrics(input_patterns: list, result: dict, config: dict):
    """
    Saves the patterns into metrics table and postgres
    """
    attribute = config.get("attribute", {})
    asset_id = config.get("asset_id")
    measure = config.get("measure", {})
    pattern_measure_name = measure.get("name")
    measure_id = measure.get("id")
    allow_score = is_scoring_enabled(config, measure.get("allow_score", False))
    is_drift_enabled = measure.get("is_drift_enabled", False)
    is_archived = False
    is_positive_measure = measure.get("is_positive_measure", False)
    connection_id = config.get("connection_id")
    organization_id = config.get("organization_id")
    run_id = config.get("run_id")
    airflow_run_id = config.get("airflow_run_id")
    executed_query = config.get("query_string")
    executed_query = executed_query if executed_query else ""
    executed_query = executed_query.strip().replace("'", "''")
    if config.get("has_temp_table", False) and config.get("table_name", "") and config.get("temp_view_table_name", ""):
        executed_query = executed_query.replace(
            f'''{config.get("temp_view_table_name", "").strip()}''', f'''{config.get("table_name", "").strip()}'''
        )
    connection = get_postgres_connection(config)

    total_records = int(attribute.get("row_count", 0))
    with connection.cursor() as cursor:
        attribute_name = attribute.get("name")
        attribute_label = get_attribute_label(attribute_name)
        attribute_id = attribute.get("id")

        # Delete metrics for the same run id
        delete_metrics(
            config,
            run_id=run_id,
            measure_id=measure_id,
            attribute_id=attribute_id,
            level="attribute",
        )

        valid_record_counts = []

        pattern_measure_score = 0
        for pattern in input_patterns:
            pattern_name = pattern.get("name")
            measure_name = pattern.get("name")
            technical_name = pattern.get("technical_name")
            is_positive = pattern.get("is_valid")
            pattern_key = f"{attribute_label}{technical_name}".lower()
            pattern_count = result.get(pattern_key, 0)
            pattern_count = pattern_count if pattern_count else 0
            pattern.update(
                {"value_count": pattern_count, "count": pattern_count})
            weightage = measure.get("weightage", 100)
            weightage = int(weightage) if weightage else 100
            valid_count = 0
            invalid_count = 0
            valid_percentage = 0
            invalid_percentage = 0
            score = None

            if allow_score and total_records:
                valid_count = pattern_count if pattern_count else 0
                valid_count = valid_count if is_positive else (total_records - valid_count)
                invalid_count = total_records - valid_count
                valid_percentage = float(valid_count / total_records * 100)
                invalid_percentage = float(100 - valid_percentage)
                score = valid_percentage
                score = score if score <= 100 else 100
                score = calculate_weightage_score(score, weightage)
                score = 100 if score > 100 else score
                score = 0 if score < 0 else score
                # score = round(score, 2)

            valid_record_counts.append(
                valid_count if is_positive else invalid_count)
            if allow_score:
                pattern_measure_score = pattern_measure_score + score

            query_input = (
                str(uuid4()),
                organization_id,
                connection_id,
                asset_id,
                attribute_id,
                measure_id,
                run_id,
                airflow_run_id,
                attribute_name,
                measure_name,
                pattern_name,
                "attribute",
                str(pattern_count),
                weightage,
                total_records,
                valid_count,
                invalid_count,
                valid_percentage,
                invalid_percentage,
                score,
                PASSED if result else FAILED,
                is_archived,
                executed_query,
                allow_score,
                is_drift_enabled,
                False,
                True,
                False,
                attribute.get("parent_attribute_id", None),
            )
            input_literals = ", ".join(["%s"] * len(query_input))
            query_param = cursor.mogrify(
                f"({input_literals},CURRENT_TIMESTAMP)",
                query_input,
            ).decode("utf-8")

            try:
                attribute_insert_query = f"""
                    insert into core.metrics (id, organization_id, connection_id, asset_id, attribute_id,
                    measure_id, run_id, airflow_run_id,attribute_name, measure_name, base_measure_name, level, value, weightage, total_count,
                    valid_count, invalid_count, valid_percentage, invalid_percentage, score, status, is_archived,
                    query, allow_score, is_drift_enabled, is_measure,  is_active, is_delete, parent_attribute_id, created_date)
                    values {query_param}
                """
                cursor = execute_query(
                    connection, cursor, attribute_insert_query)
            except Exception as e:
                log_error("save patterns: inserting new pattern metrics", e)

        score = None
        if allow_score and input_patterns:
            score = float(pattern_measure_score / len(input_patterns))
            score = score if score <= 100 else 100

        pass_criteria_result = check_measure_result(
            measure, score)

        valid_count = 0
        if valid_record_counts and len(valid_record_counts) > 1:
            valid_count = max(valid_record_counts)
        elif valid_record_counts and len(valid_record_counts) == 1:
            valid_count = valid_record_counts[0]
        valid_count = valid_count if valid_count else 0
        valid_count = total_records if valid_count >= total_records else valid_count
        valid_count = 0 if valid_count < 0 else valid_count
        invalid_count = total_records - valid_count


        query_input = (
            str(uuid4()),
            organization_id,
            connection_id,
            asset_id,
            attribute_id,
            measure_id,
            run_id,
            airflow_run_id,
            attribute_name,
            pattern_measure_name,
            "",
            "attribute",
            str(valid_count),
            weightage,
            total_records,
            valid_count,
            invalid_count,
            valid_percentage,
            invalid_percentage,
            score,
            PASSED if result else FAILED,
            is_archived,
            executed_query,
            allow_score,
            is_drift_enabled,
            True,
            True,
            False,
            attribute.get("parent_attribute_id", None), 
        )
        input_literals = ", ".join(["%s"] * len(query_input))
        query_param = cursor.mogrify(
            f"({input_literals},CURRENT_TIMESTAMP)",
            query_input,
        ).decode("utf-8")
        try:
            attribute_insert_query = f"""
                insert into core.metrics (id, organization_id, connection_id, asset_id, attribute_id,
                measure_id, run_id, airflow_run_id,attribute_name, measure_name, base_measure_name, level, value, weightage, total_count,
                valid_count, invalid_count, valid_percentage, invalid_percentage, score, status, is_archived,
                query, allow_score, is_drift_enabled, is_measure,  is_active, is_delete, parent_attribute_id, created_date)
                values {query_param}
            """
            cursor = execute_query(
                connection, cursor, attribute_insert_query)
        except Exception as e:
            log_error("save patterns: inserting new pattern metrics", e)
        
        score = 'null' if score is None else score
        update_measure_score_query = f"""
            update core.measure set last_run_id='{run_id}', last_run_date=CURRENT_TIMESTAMP, score={score}, failed_rows=null, row_count = {total_records}, valid_rows = {valid_count}, invalid_rows = {invalid_count} , result = '{pass_criteria_result}'
            where attribute_id='{attribute_id}' and id='{measure_id}'
        """
        cursor = execute_query(connection, cursor, update_measure_score_query)

    if input_patterns:
        __save_patterns_metadata(attribute_id, input_patterns, config)


def __save_patterns_metadata(attribute_id: str, pattern_metadata: dict, config: dict):
    """
    Save the patterns into metadata table for the given attribute
    """
    asset_id = config.get("asset_id")
    asset = config.get("asset", {})
    asset = asset if asset else {}

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        patterns = [pattern for pattern in pattern_metadata if pattern]
        attribute_patterns = json.dumps(patterns, default=str)
        attribute_patterns = attribute_patterns.replace("'", "''")
        query_string = f"""
            update core.attribute set user_defined_patterns = '{attribute_patterns}'
            where asset_id='{asset_id}' and id='{attribute_id}'
        """
        cursor = execute_query(connection, cursor, query_string)


def execute_patterns(config: dict) -> None:
    """
    Execute attribute level patterns
    """
    attribute = config.get("attribute")
    pattern_query, patterns = get_pattern_query(config, attribute)
    if not (pattern_query and patterns):
        return

    if not (pattern_query and patterns):
        return

    source_connection = None
    pattern_result = {}
    if pattern_query:
        config.update({
            "sub_category": "FREQUENCY",
            "query_string": pattern_query
        })
        pattern_result, native_connection = execute_native_query(
            config, pattern_query, source_connection)
        if not source_connection and native_connection:
            source_connection = native_connection
        pattern_result = convert_to_lower(pattern_result)
    save_pattern_metrics(patterns, pattern_result, config)


def prepare_sql_patterns_query(attribute_name: str, pattern: dict, config: dict) -> str:
    """
    Prepares the query string for the posix patterns for given attributes
    """
    attribute_label = get_attribute_label(attribute_name)
    default_queries = config.get("default_queries", {})
    frequency_query = default_queries.get("frequency", {})
    pattern_query = frequency_query.get("pattern") if frequency_query else ""
    pattern_query = pattern_query if pattern_query else ""

    query = ""
    if not pattern_query:
        return query

    properties = pattern.get("pattern", {})
    properties = properties if properties else {}
    pattern_name = pattern.get("technical_name")
    pattern_value = properties.get("sql_pattern")
    if (not pattern_name) or (not pattern_value):
        return query

    query = process_sql_pattern_rule_group(
        default_queries, pattern_value, query, attribute_name)
    attribute_label = f"{attribute_label}{pattern_name}"
    query = pattern_query.replace("<pattern_condition>", query).replace(
        "<attribute_label>", attribute_label)
    return query


def process_sql_pattern_rule_group(query_config: str, cRule: dict, query: str, attribute_name: str) -> str:
    """
    Prepares pattern Rule Group Query
    """
    custom_rule_query = query_config.get("custom_rule_query")
    ruleQuery = ""
    connector = cRule.get("connector", None)
    not_condition = cRule.get("not", False)
    rules = cRule.get("rules", [])

    if connector and len(rules) > 0:
        connector_query = custom_rule_query.get(connector, None)
        query_condition = []
        if connector_query:
            for rule in rules:
                rule_condition_query = process_sql_pattern_rule(
                    query_config, rule, query, attribute_name
                )
                if rule_condition_query:
                    query_condition.append(rule_condition_query)

            if len(query_condition) > 0:
                ruleQuery = f" {connector_query} ".join(query_condition)
    if query:
        ruleQuery = f"""{query} {ruleQuery}"""

    if not_condition:
        not_query = custom_rule_query.get("not", None)
        ruleQuery = f"""{not_query} ({ruleQuery})"""
    return ruleQuery


def process_sql_pattern_rule(query_config: dict, cRule: dict, query: str, attribute_name: str) -> str:
    """
    Prepares Pattern Rule Query
    """
    conditionQuery = None
    custom_rule_query = query_config.get("custom_rule_query")
    connector = cRule.get("connector", None)
    rules = cRule.get("rules", [])
    operator = cRule.get("operator", None)
    values = cRule.get("values", [])
    ruleQuery = None

    if connector and len(rules) > 0:
        ruleQuery = process_sql_pattern_rule_group(
            query_config, cRule, query, attribute_name)

    if attribute_name and operator:
        operator_id = operator.get("id", "")
        conditionQuery = custom_rule_query.get(operator_id, None)
        if conditionQuery:
            conditionQuery = conditionQuery.replace(
                "<attribute>", attribute_name)
            if len(values) > 0:
                for index, value in enumerate(values):
                    conditionQuery = conditionQuery.replace(
                        f"""<value{index+1}>""", value
                    )
            query = f"""{conditionQuery}"""
    if ruleQuery:
        query = f"""( {ruleQuery} )"""
    return query
