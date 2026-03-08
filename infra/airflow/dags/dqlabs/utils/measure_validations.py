"""
    Migration Notes From V2 to V3:
    Migrations Completed 
"""


import json
from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.db_helper import execute_query, fetchall
from dqlabs.utils.extract_workflow import get_selected_attributes
from dqlabs.app_constants.dq_constants import MAX_DICTINCT_VALUES


def get_attribute_measures(config: dict):
    """
    Get all the unvalidated measures except health measures
    """
    asset = config.get("asset")
    asset = asset if asset else {}
    asset_id = asset.get("id") if asset else ""

    measures = []
    if not asset_id:
        return measures

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            select mes.id, mes.base_measure_id, mes.attribute_id, base.type, base.category,
            attribute.max_length, attribute.metadata, attribute.derived_type, attribute.is_unique, attribute.is_primary_key,
            attribute.name as attribute_name, mes.technical_name as measure_name, mes.weightage
            from core.measure as mes
            join core.base_measure as base on base.id=mes.base_measure_id and base.is_visible=true
            join core.attribute on attribute.id = mes.attribute_id
            left join core.terms_mapping on terms_mapping.attribute_id = attribute.id
            where mes.asset_id='{asset_id}' and mes.attribute_id is not null and mes.is_validated = False
            and mes.is_active=True and terms_mapping.approved_by is null
            and base.type != 'health' and base.is_default=True
        """
        cursor = execute_query(connection, cursor, query_string)
        measures = fetchall(cursor)
    return measures


def update_measure_status(config: dict, measures: list, is_active: bool):
    """
    Update the measure based on the is_active flag
    """
    asset = config.get("asset")
    asset = asset if asset else {}
    asset_id = asset.get("id") if asset else ""
    if not measures:
        return

    measure_ids = ', '.join(
        [f""" '{str(measures_id)}' """ for measures_id in measures if measures_id])
    measure_ids = f" ({measure_ids}) " if measure_ids else ""
    if not measure_ids:
        return

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            update core.measure set is_active={is_active}, is_validated=True
            where asset_id='{asset_id}' and id in {measure_ids}
        """
        execute_query(connection, cursor, query_string)


def get_active_patterns(config: dict, attribute_id: str):
    """
    Get active patterns
    """
    asset = config.get("asset")
    asset = asset if asset else {}
    asset_id = asset.get("id") if asset else ""

    measures = []
    if not asset_id:
        return measures

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            select mes.id, mes.base_measure_id,
            mes.technical_name as measure_name, base.properties
            from core.measure as mes
            join core.base_measure as base on base.id=mes.base_measure_id and base.is_visible=true
            where mes.asset_id='{asset_id}' and mes.attribute_id='{attribute_id}'
            and mes.is_active=True and base.type = 'pattern'
        """
        cursor = execute_query(connection, cursor, query_string)
        measures = fetchall(cursor)
    return measures


def update_patterns(config: dict):
    """
    Update active patterns as metadata patterns
    """
    asset = config.get("asset")
    asset = asset if asset else {}
    asset_id = asset.get("id") if asset else ""

    attributes = get_selected_attributes(config)
    attributes = attributes if attributes else []
    connection = get_postgres_connection(config)
    for attribute in attributes:
        attribute_id = attribute.get("id")
        patterns = get_active_patterns(config, attribute_id)

        active_patterns = []
        for pattern in patterns:
            properties = pattern.get("properties", {})
            pattern_properties = properties if properties else {}
            active_patterns.append({
                "name": pattern.get("measure_name"),
                "pattern": pattern_properties,
                "count": 0,
                "id": pattern.get("id"),
                "measure_id": str(pattern.get("base_measure_id"))
            })

        patterns = [pattern for pattern in patterns if pattern]
        attribute_patterns = json.dumps(patterns, default=str)
        attribute_patterns = attribute_patterns.replace("'", "''")
        with connection.cursor() as cursor:
            query_string = f"""
                update core.attribute set patterns = '{attribute_patterns}'
                where asset_id='{asset_id}' and id='{attribute_id}'
            """
            cursor = execute_query(connection, cursor, query_string)


def validate_measures(config: dict):
    """
    Validate and activate the measures based on the metadata constraints
    """
    measures = get_attribute_measures(config)
    if not measures:
        return

    date_types = ['date', 'time']
    active_measures = []
    inactive_measures = []
    for measure in measures:
        measure_id = measure.get("id")
        is_unique = measure.get("is_unique")
        is_primary_key = measure.get("is_primary_key")
        measure_type = measure.get("type")
        category = measure.get("category")
        derived_type = measure.get("derived_type")
        max_length = measure.get("max_length", 0)
        max_length = int(max_length) if max_length and isinstance(
            max_length, str) else max_length
        max_length = max_length if max_length else 0

        metadata = measure.get("metadata")
        metadata = metadata if metadata else {}
        metadata = json.loads(metadata) if isinstance(
            metadata, str) else metadata
        metadata = metadata if metadata else {}
        if not metadata:
            continue

        distinct_count = metadata.get("distinct_count")
        is_active = False
        is_date = any([(type in derived_type.lower()) for type in date_types])
        if not ((is_primary_key or is_unique) or is_date or (max_length and max_length > 50)):
            if max_length > 0:
                if distinct_count and distinct_count <= MAX_DICTINCT_VALUES:
                    is_active = (
                        (category and category.lower() == "enum")
                        or (
                            derived_type.lower() in ["integer", "numeric"]
                            and measure_type.lower() in ["statistics", "distribution"]
                        )
                        or (category and category.lower() == "range")
                    )
                else:
                    if derived_type.lower() == "text":
                        is_active = (
                            (category and category.lower() in [
                             "pattern", "length", "range"])
                            or (measure_type.lower() == "distribution")
                        )
                    elif derived_type.lower() in ["integer", "numeric"]:
                        is_active = (
                            (measure_type.lower() in [
                             "statistics", "distribution"])
                            or (category and category.lower() == "range")
                        )
        else:
            if max_length > 0 and derived_type.lower() in ["integer", "numeric"]:
                is_active = (
                    (measure_type.lower() in ["statistics", "distribution"])
                    or (category and category.lower() == "range")
                )

        if is_active:
            active_measures.append(measure_id)
        else:
            inactive_measures.append(measure_id)

    update_measure_status(config, active_measures, True)
    update_measure_status(config, inactive_measures, False)
    # update_patterns(config)
