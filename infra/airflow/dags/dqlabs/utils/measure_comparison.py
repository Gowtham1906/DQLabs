"""
    Migration Notes From V2 to V3:
    No Migration Changes
"""

import json
import operator
import datetime

from dqlabs.app_helper.db_helper import execute_query, fetchone, split_queries, fetchall
from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.log_helper import log_error
from dqlabs.utils.event_capture import save_alert_event


def get_comparison_metrics(connection: object, measure_id: str) -> str:
    """
    Returns the list of latest comparison metrics of an asset
    """
    try:
        with connection.cursor() as cursor:
            query_string = f"""
                select base.id
                ,base.name as measure_name
                ,base.properties ->> 'priority' as priority
                from core.base_measure base
                where category = 'comparison'
                and level = 'measure'
                and base.is_visible=true
                and base.technical_name in (select technical_name from core.measure
                where id = '{measure_id}')
            """
            cursor = execute_query(connection, cursor, query_string)
            metrics = fetchone(cursor)
        return metrics
    except Exception as e:
        raise e


def get_measure_properties(connection: object, base_measure_id: str):

    # Returns the list of latest comparison measure properties
    try:
        with connection.cursor() as cursor:
            query_string = f"""
                select properties from core.base_measure
                where id = '{base_measure_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            response = fetchone(cursor)
            
        return response
    except Exception as e:
        raise e


def get_comparison_threshold_values(connection: object, properties: dict, rule: dict):
    type = properties.get("type", '')
    source_id = properties.get("source").get("id")
    target_id = properties.get("target").get("id")
    measure = rule.get("measure")

    try:
        with connection.cursor() as cursor:
            if type == "asset":
                query_string = f"""
                    with source_asset_table as 
                    (select ast.name, case when met.value<>'' then met.value else '0' end as value, met.created_date, 
                    b_mes.name as measure_name from core.metrics met 
                    join core.asset ast on met.asset_id = ast.id
                    join core.measure mes on mes.asset_id = ast.id
                    join core.base_measure b_mes on b_mes.id = mes.base_measure_id
                    where b_mes.name = '{measure}' and mes.is_active is true and met.measure_name = b_mes.technical_name
                    and met.asset_id='{source_id}'
                    order by met.created_date desc
                    limit 1),
                    target_asset_table as 
                    (select ast.name, case when met.value<>'' then met.value else '0' end as value, met.created_date, 
                    b_mes.name as measure_name from core.metrics met 
                    join core.asset ast on met.asset_id = ast.id
                    join core.measure mes on mes.asset_id = ast.id 
                    join core.base_measure b_mes on b_mes.id = mes.base_measure_id
                    where b_mes.name = '{measure}' and mes.is_active is true and met.measure_name = b_mes.technical_name
                    and met.asset_id='{target_id}'
                    order by met.created_date desc
                    limit 1)
                    select src.name as source_name, tar.name as target_name, src.value as source_count, tar.value as target_count, 
                    abs(cast(src.value as double precision) - cast(tar.value as double precision)) as diff_value
                    from source_asset_table src
                    join target_asset_table tar 
                    on src.measure_name = tar.measure_name;
                """
            elif type == "attribute":
                query_string = f"""
                    with source_attribute_table as 
                    (select ast.technical_name, case when met.value<>'' then met.value else '0' end as value, 
                    met.created_date, b_mes.name as measure_name 
                    from core.metrics met 
                    join core.attribute ast on met.attribute_id = ast.id
                    join core.measure mes on mes.attribute_id = ast.id
                    join core.base_measure b_mes on b_mes.id = mes.base_measure_id
                    where b_mes.name = '{measure}' and mes.is_active is true and met.measure_name = b_mes.technical_name
                    and met.attribute_id='{source_id}'
                    order by met.created_date desc
                    limit 1),
                    target_attribute_table as 
                    (select ast.technical_name, case when met.value<>'' then met.value else '0' end as value, 
                    met.created_date, b_mes.name as measure_name 
                    from core.metrics met 
                    join core.attribute ast on met.attribute_id = ast.id
                    join core.measure mes on mes.attribute_id = ast.id
                    join core.base_measure b_mes on b_mes.id = mes.base_measure_id
                    where b_mes.name = '{measure}' and mes.is_active is true and met.measure_name = b_mes.technical_name
                    and met.attribute_id='{target_id}'
                    order by met.created_date desc
                    limit 1)
                    select src.technical_name as source_name, tar.technical_name as target_name, src.value as source_count, tar.value as target_count, 
                    abs(cast(src.value as double precision) - cast(tar.value as double precision)) as diff_value
                    from source_attribute_table src
                    join target_attribute_table tar 
                    on src.measure_name = tar.measure_name;
                """
            cursor = execute_query(connection, cursor, query_string)
            comparison_metrics = fetchone(cursor)
        return comparison_metrics
    except Exception as e:
        raise e


def time_conversion(value: int, timeline: str) -> int:
    """
    Returns the time conversion into seconds for different time intervals

    Args:
        value (int): The values for freshness
        timeline (str): The timeline(Minutes,Days,Years etc.) for seconds conversion

    Returns:
        _type_: Total Time in seconds
    """
    time_conversion = {'Minutes': value * 60,
                       'Hours': value * 3600,
                       'Days': value * 86400,
                       'Months': value * 2.628e+6,
                       'Years': value * 3.154e+7
                       }
    return int(time_conversion.get(timeline))


""" All python comparison operators used for comparison measure"""

comparison_operator = {'isgreaterthan': operator.gt,
                       'islessthan': operator.lt,
                       'isgreaterthanorequalto': operator.ge,
                       'islessthanorequalto': operator.le,
                       'isequalto': operator.eq
                       }


def update_last_runs_for_comparison_measure(config: dict, last_runs_measures: dict) -> None:
    connection = get_postgres_connection(config)
    last_runs = []
    if not last_runs_measures:
        return
    with connection.cursor() as cursor:
        last_run_values = []
        for measure_id, value in last_runs_measures.items():
            if not value:
                continue
            status, run_id = value
            query_string = f"""
                    select last_runs from core.measure where id = '{measure_id}'
                """
            cursor = execute_query(connection, cursor, query_string)
            last_runs = fetchone(cursor)
            last_runs = last_runs.get('last_runs', []) if last_runs else []
            last_runs = json.loads(last_runs) if isinstance(
                last_runs, str) else last_runs
            last_runs = last_runs if last_runs else []
            last_runs.insert(0, {"run_id": run_id, "status": status, "last_run_date": datetime.datetime.now() })

            if len(last_runs) > 7:
                last_runs = last_runs[:7]

            query_input = (
                measure_id,
                json.dumps(last_runs, default=str),
            )
            input_literals = ", ".join(["%s"] * len(query_input))
            query_param = cursor.mogrify(
                f"({input_literals})",
                query_input,
            ).decode("utf-8")

            last_run_values.append(query_param)

        last_run_values = split_queries(last_run_values)
        for input_values in last_run_values:
            try:
                query_input = ",".join(input_values)
                last_run_insert_query = f"""
                    update core.measure as measure set last_runs = to_json(c.last_runs::text)
                    from (values {query_input}) as c(measure_id, last_runs) 
                    where c.measure_id = measure.id::text;
                """
                cursor = execute_query(
                    connection, cursor, last_run_insert_query)
            except Exception as e:
                log_error("check alerts: update last run", e)
                raise e

def update_comparison_last_run_alerts(config: dict) -> None:
    """
    Updates the last run alerts count in measure table
    """
    measure_id = config.get("measure_id")

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        try:
            # Update last run alerts count on measures table
            condition_query = ""
            if measure_id:
                condition_query = f"where measure.id='{measure_id}'"
            update_query_string = f"""
                update core.measure
                set alerts = (
                select count(*) from core.metrics as drift
                where drift.measure_id = measure.id
                and drift.run_id = measure.last_run_id
                and lower(drift.drift_status) in ('high','medium','low'))
                {condition_query}
            """
            cursor = execute_query(connection, cursor, update_query_string)

        except Exception as e:
            log_error("check alerts : update last run alerts count failed - ", e)

            
def send_event(config: dict, run_id: str, measure_id: str) -> None:
    """
    Send event to the event capture system
    """
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        try:
            query_string = f"""
                select metrics.id
                from core.metrics
                where measure_id = '{measure_id}'
                and run_id = '{run_id}'
                and lower(drift_status) in ('high','medium','low')
            """
            cursor = execute_query(connection, cursor, query_string)
            metrics = fetchall(cursor)
            metrics = metrics if metrics else []
            metrics = [metric.get('id') for metric in metrics]
            if metrics:
                save_alert_event(config, metrics, {"type": "new_alert"})
        except Exception as e:
            log_error("send event: update last run alerts count failed - ", e)
