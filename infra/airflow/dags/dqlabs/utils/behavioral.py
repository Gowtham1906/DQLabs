"""
    Migration Notes From V2 to V3:
    No Migration Changes
"""

from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.db_helper import execute_query, fetchall
from dqlabs.utils import get_last_runs
from dqlabs.app_helper.dq_helper import get_attribute_label
from dqlabs.app_constants.dq_constants import DRIFT_DAYS


def get_behavioral_table_name(measure: dict) -> str:
    """
    Returns the behavioral table name
    """
    measure_name = measure.get("name", "")
    measure_name = get_attribute_label(
        measure_name, False) if measure_name else ""
    measure_id = measure.get("id").replace("-", "_")
    behavioral_table_name = f"{measure_name}___{measure_id}"
    return behavioral_table_name


def get_latest_behavioral_data(config: dict, run_id: str) -> list:
    """
    Returns the behavioural data for the last n runs
    """
    measure = config.get("measure")
    table_name = get_behavioral_table_name(measure)

    behavioral_data = []
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            select * from  "behavioral".{table_name} where run_id = '{run_id}'
        """
        cursor = execute_query(connection, cursor, query_string)
        behavioral_data = fetchall(cursor)
    return behavioral_data


def get_behavioral_data(config: dict, exclude_current_run: bool = False) -> list:
    """
    Returns the behavioural data for the last n runs
    """
    asset_id = config.get("asset_id")
    run_id = config.get("run_id")
    measure = config.get("measure")
    window = int(config.get("window", DRIFT_DAYS))
    table_name = get_behavioral_table_name(measure)
    drift_type = config.get("drift_type","runs")
    behavioral_data = []
    connection = get_postgres_connection(config)
    last_runs = get_last_runs(
        connection,
        asset_id,
        limit=window,
        exclude_current_run=exclude_current_run,
        current_run_id=run_id,
        limit_type=drift_type
    )
    with connection.cursor() as cursor:
        if last_runs:
            last_runs = list(
                map(lambda run: f"""'{run.get("run_id")}'""", last_runs)
            )
            run_ids = ", ".join(last_runs)
            run_ids = f"({run_ids})"
            query_string = f"""
                select * from  "behavioral".{table_name}
                where asset_id='{asset_id}' and run_id in {run_ids}
            """
            cursor = execute_query(connection, cursor, query_string)
            behavioral_data = fetchall(cursor)
    return behavioral_data


def get_latest_threshold(config: dict, attribute_id: str, measure_id: str) -> dict:
    """
    Returns the list of measure for the given window frame
    """
    try:
        asset_id = config.get("asset_id")
        connection = get_postgres_connection(config)
        thresholds = {}
        with connection.cursor() as cursor:
            query_string = f"""
                    select distinct on (attribute_id, measure_id, behavioral_key)
                    attribute_id, measure_id, behavioral_key, created_date,
                    lower_threshold, upper_threshold
                    from core.drift_threshold
                    where behavioral_key !='' and behavioral_key is not null and
                    asset_id='{asset_id}' and attribute_id = '{attribute_id}' and measure_id = '{measure_id}'
                    order by attribute_id, measure_id, behavioral_key, created_date desc
                """
            cursor = execute_query(connection, cursor, query_string)
            latest_thresholds = fetchall(cursor)

            for threshold in latest_thresholds:
                behavioral_key = threshold.get("behavioral_key")
                thresholds.update(
                    {
                        behavioral_key: (
                            threshold.get("lower_threshold", 0),
                            threshold.get("upper_threshold", 0),
                            threshold.get("is_auto", True),
                        )
                    }
                )
        return thresholds
    except Exception as e:
        raise e


def get_value_column_name(measure: dict) -> str:
    """
    Returns the numerical column name
    """
    rule_config = measure.get("properties")
    # DQL-2603 - Adding categoriacal and numerical columns seperately for behavioral query
    category_group_attributes = rule_config.get("attributes", [])
    aggregator_attributes = rule_config.get(
        "aggregator_attributes", [])  # only numerical columns allowed
    if category_group_attributes and aggregator_attributes:
        numerical_attributes = list(
            filter(
                lambda attribute: attribute.get("data_type", "").lower()
                in ["integer", "numeric"],
                aggregator_attributes,
            )
        )
    elif category_group_attributes and not aggregator_attributes:
        numerical_attributes = list(
            filter(
                lambda attribute: attribute.get("data_type", "").lower()
                in ["integer", "numeric"],
                category_group_attributes,
            )
        )
    numerical_attributes = [attribute.get(
        "name") for attribute in numerical_attributes]
    numerical_attributes = [
        get_attribute_label(attribute, False) for attribute in numerical_attributes
    ]
    numeric_attribute = numerical_attributes[0] if numerical_attributes else ""
    return numeric_attribute


def get_threshold(
    config: dict, attribute_id: str, measure_id: str, run_id: str
) -> dict:
    """
    Returns the list of measure for the given window frame
    """
    try:
        asset_id = config.get("asset_id")
        connection = get_postgres_connection(config)
        thresholds = {}
        with connection.cursor() as cursor:
            query_string = f"""
                    select distinct on (attribute_id, measure_id, behavioral_key)
                    attribute_id, measure_id, behavioral_key, created_date,
                    lower_threshold, upper_threshold
                    from core.drift_threshold
                    where behavioral_key !='' and behavioral_key is not null and
                    asset_id='{asset_id}' and attribute_id = '{attribute_id}' and measure_id = '{measure_id}'
                    and run_id='{run_id}'
                    order by attribute_id, measure_id, behavioral_key, created_date desc
                """
            cursor = execute_query(connection, cursor, query_string)
            latest_thresholds = fetchall(cursor)

            for threshold in latest_thresholds:
                behavioral_key = threshold.get("behavioral_key")
                thresholds.update(
                    {
                        behavioral_key: (
                            threshold.get("lower_threshold", 0),
                            threshold.get("upper_threshold", 0),
                            threshold.get("is_auto", True),
                        )
                    }
                )
        return thresholds
    except Exception as e:
        raise e


def get_stddev_by_category(config: dict) -> dict:
    """
    Returns the stddev for each category of the behavioural data of the last n runs
    """
    asset_id = config.get("asset_id")
    run_id = config.get("run_id")
    measure = config.get("measure")
    window = int(config.get("window", DRIFT_DAYS))
    table_name = get_behavioral_table_name(measure)
    value_column_name = get_value_column_name(measure)
    value_column_name = get_attribute_label(value_column_name, False)
    drift_type = config.get("drift_type", "runs")
    behavioral_stddev = {}
    connection = get_postgres_connection(config)
    last_runs = get_last_runs(
        connection,
        asset_id,
        limit=window,
        exclude_current_run=True,
        current_run_id=run_id,
        limit_type=drift_type
    )
    with connection.cursor() as cursor:
        if last_runs:
            last_runs = list(
                map(lambda run: f"""'{run.get("run_id")}'""", last_runs)
            )
            run_ids = ", ".join(last_runs)
            run_ids = f"({run_ids})"
            query_string = f"""
                select key, stddev({value_column_name}) as std_dev
                from  "behavioral".{table_name}
                where asset_id='{asset_id}' and run_id in {run_ids}
                group by key
            """
            cursor = execute_query(connection, cursor, query_string)
            behavioral_data = fetchall(cursor)

            if behavioral_stddev:
                for data in behavioral_data:
                    key = data.get("key")
                    std_dev = data.get("std_dev", 0)
                    std_dev = std_dev if std_dev else 0
                    behavioral_stddev.update({key: std_dev})
    return behavioral_stddev
