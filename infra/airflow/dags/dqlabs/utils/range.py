"""
    Migration Notes From V2 to V3:
    No Migration Changes
"""

import json
from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone
from dqlabs.app_helper.dag_helper import (
    get_postgres_connection,
)
from dqlabs.app_helper.log_helper import log_info
from dqlabs.app_helper.connection_helper import get_attribute_names

threshold_query = {
    "isGreaterThan": """<attribute> > <value1>""",
    "isLessThan": """<attribute> < <value1>""",
    "isGreaterThanOrEqualTo": """<attribute> >= <value1>""",
    "isLessThanOrEqualTo": """<attribute> <= <value1>""",
    "isEqualTo": """<attribute> <= <value1>""",
    "isBetween": """<attribute> >= <value1> and <attribute> <= <value2>""",
    "isNotBetween": """not(<attribute> >= <value1> and <attribute> <= <value2>)""",
}


def get_range_measures(config: dict, attribute_id: str) -> list:
    """
    Returns the list of range measures for the given attribute
    """
    asset_id = config.get("asset_id")
    connection = get_postgres_connection(config)
    measures = []
    with connection.cursor() as cursor:
        query_string = f"""
            select mes.id as id, base.id as base_measure_id, base.technical_name as name, base.query, base.properties, base.type, base.level, base.category, base.derived_type,
            mes.allow_score, mes.is_drift_enabled, mes.threshold_constraints, mes.attribute_id, mes.asset_id, mes.is_positive, mes.drift_threshold, base.term_id, mes.semantic_measure, mes.semantic_query,
            mes.weightage
            from core.measure as mes
            join core.base_measure as base on base.id=mes.base_measure_id and base.is_visible=true
            where mes.asset_id='{asset_id}' and base.category='range'
            and mes.is_active=True and mes.attribute_id='{attribute_id}'
        """
        cursor = execute_query(connection, cursor, query_string)
        measures = fetchall(cursor)
    return measures


def update_range_measure(config: dict, measure: dict, threshold_constraints: dict) -> list:
    """
    Returns the list of range measures for the given attribute
    """
    measure_id = measure.get("id")
    if not measure_id:
        return
    threshold_constraints = threshold_constraints if threshold_constraints else {}
    log_info(("range-measure-threshold_constraints",threshold_constraints))
    query = ""
    if threshold_constraints:
        condition = threshold_constraints.get("condition")
        value = threshold_constraints.get("value")
        value = value if value else 0
        value2 = threshold_constraints.get("value2")
        value2 = value2 if value2 else 0

        query = threshold_query.get(condition, None)
        if query:
            query = query.replace("<value1>", str(
                value) if not isinstance(value, str) else value)
            query = query.replace("<value2>", str(
                value2) if not isinstance(value2, str) else value2)

    threshold_constraints.update({"query": query})
    threshold_constraints = {
        'threshold_data': [threshold_constraints]
    }
    log_info(("range-measure-threshold_constraints",threshold_constraints))
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        threshold_constraints = json.dumps(threshold_constraints, default=str)
        query_string = f"""
            update core.measure set threshold_constraints='{threshold_constraints}'
            where id='{measure_id}'
        """
        log_info(("range-measure-query_string",query_string))
        execute_query(connection, cursor, query_string)

def get_snowflake_tables(config: dict, asset_id:str):
    """
    Return the list of tables assets for that asset_id for lineage
    """
    connection = get_postgres_connection(config)
    tables = []
    with connection.cursor() as cursor:
        query_string = f"""
            select 
                ast.id as id
                ,ast.id as asset_id
                ,ast.name as name
                ,ast.score as dqscore
                ,ast.alerts as alerts
                ,'table' as "type"
                ,True as is_asset
                ,ast.properties ->> 'database' as database
                ,ast.properties ->> 'schema' as schema
                ,con.type as connection_type
                ,con.credentials ->> 'warehouse' as warehouse
                ,con.name as connection_name
                ,ast.status as status	
            from core.asset ast 
            join core.connection con on con.id = ast.connection_id
            where ast.id = '{asset_id}'
        """
        cursor = execute_query(connection, cursor, query_string)
        tables = fetchone(cursor)

    return tables


def get_snowflake_fields(config: dict, asset_id:str):
    """
    Return the list of fields for that asset_id for lineage
    """
    connection = get_postgres_connection(config)
    fields = []
    with connection.cursor() as cursor:
        query_string = f"""
            select
                attr.id as id
                ,attr.id as attribute_id
                ,attr.id as mapped_attribute_id
                ,attr.score as dqscore
                ,attr.name as name
                ,attr.alerts as alerts
                ,attr.datatype as datatype
                ,'column' as "type"
                ,attr.asset_id as asset_id
                ,True as "is_asset"
                ,attr.status as status
            from core.attribute attr
            where attr.asset_id = '{asset_id}'
        """
        cursor = execute_query(connection, cursor, query_string)
        fields = fetchall(cursor)
        
    return fields

def fetch_semantic_term_measure_attribute_name(
    config: dict, term_name: str, asset_id: str, attribute_name: str) -> list:
    """
    Return the attribute name for the given semantic term measure
    """
    connection = get_postgres_connection(config)
    connection_type = config.get("connection_type")
    semantic_attribute_name = None
    with connection.cursor() as cursor:
        query_string = f"""
        SELECT DISTINCT
            attr.name AS attribute_name,
            terms.name AS term_name
        FROM 
            core.terms_mapping AS term_map
        JOIN 
            core.terms AS terms ON terms.id = term_map.term_id
        JOIN 
            core.attribute AS attr ON term_map.attribute_id = attr.id
        JOIN 
            core.asset AS ast ON attr.asset_id = ast.id
        WHERE 
            lower(terms.name) = lower('{term_name}')
            AND ast.id = '{asset_id}'
        """
        cursor = execute_query(connection, cursor, query_string)
        log_info(("query_string", query_string))
        semantic_attributes = fetchall(cursor)
        log_info(("semantic_attributes", semantic_attributes))
        if semantic_attributes:
            semantic_attributes = [attribute.get("attribute_name") for attribute in semantic_attributes]
            log_info(("semantic_attributes", semantic_attributes))
            if len(semantic_attributes) == 1:
                semantic_attribute_name = semantic_attributes[0]
            else:
                if attribute_name in semantic_attributes:
                    semantic_attribute_name = attribute_name
                else:
                    semantic_attribute_name = semantic_attributes[-1]

    """ Update attribute syntax accodring to connection type """
    if isinstance(semantic_attribute_name, str):
        semantic_attribute_name = [semantic_attribute_name]

    attribute_list = get_attribute_names(connection_type, semantic_attribute_name)
    semantic_attribute_name = attribute_list.pop() #extract the string from the list
    if isinstance(semantic_attribute_name, list):
        semantic_attribute_name = semantic_attribute_name.pop()
    log_info(("semantic_attribute_name", semantic_attribute_name))

    return semantic_attribute_name
