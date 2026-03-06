"""
Migration Notes From V2 to V3:
Pending:True
Need to validate Metrics Alerts
"""
import json
import numpy as np
import pandas as pd
import math
from statistics import stdev
import datetime
from scipy.stats import norm

from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone
from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.dq_helper import format_freshness, get_attribute_label
from dqlabs.utils import get_last_runs
from dqlabs.app_constants.dq_constants import (
    ALERT_HIGH,
    ALERT_LOW,
    ALERT_MEDIUM,
    ALERT_OK,
    BEHAVIORAL,
    BUSINESS_RULES,
    DRIFT_DAYS,
    ENUM,
    HEALTH,
    FREQUENCY,
    LENGTH,
    PATTERN,
    STATISTICS,
    DISTRIBUTION,
    SEMANTIC_MEASURE,
    RANGE,
    PROFILE,
    DEFAULT_CHUNK_LIMIT,
    OBSERVE,
    RELIABILITY
)
from dqlabs.utils.behavioral import get_behavioral_table_name, get_value_column_name
from dqlabs.app_helper.log_helper import log_info
from dqlabs.app_helper.log_helper import log_error, log_info

import numpy as np
from statsmodels.tsa.stattools import acf
from statsmodels.tsa.seasonal import seasonal_decompose
from scipy.fft import fft, fftfreq

def get_last_reset_timestamp(connection,attribute_id):
    """
    Get the most recent reset timestamp for a measure/attribute combination.
    
    Args:
        connection: Database connection object
        measure_id (str): ID of the measure
        attribute_id (str): ID of the attribute
        
    Returns:
        str or None: Reset timestamp or None if no reset found
    """
    try:
        with connection.cursor() as cursor:
            query = f"""
                SELECT action_history, created_date
                FROM core.metrics 
                WHERE attribute_id = '{attribute_id}'
                AND action_history is not null
                AND jsonb_typeof(action_history) = 'object'
                AND action_history !='{{}}'::jsonb
                ORDER BY created_date DESC
                LIMIT 1
            """
            cursor = execute_query(connection, cursor, query)
            results = fetchall(cursor)
            
            for row in results:
                action_history = row.get("action_history", {})
                if isinstance(action_history, str):
                    try:
                        action_history = json.loads(action_history)
                    except json.JSONDecodeError:
                        continue
                
                if action_history.get("is_changed", False):
                    reset_timestamp = action_history.get("date")
                    if reset_timestamp:
                        log_info(f"RESET_TIMESTAMP: Found reset at {reset_timestamp}")
                        return reset_timestamp
            
            log_info(f"RESET_TIMESTAMP: No reset found for measure")
            return None
            
    except Exception as e:
        log_error(f"Error getting reset timestamp: {e}", e)
        return None

def get_behavioral_metrics(config: dict, exclude_current_run: bool = False) -> list:
    """
    Returns the list of metrics for the given window frame
    """
    try:
        measure = config.get("measure")
        table_name = get_behavioral_table_name(measure)
        value_column_name = get_value_column_name(measure)
        asset_id = config.get("asset_id")
        run_id = config.get("run_id")
        window = int(config.get("window", DRIFT_DAYS))
        drift_type = config.get("drift_type", "runs")
        connection = get_postgres_connection(config)
        metrics = []
        last_runs = get_last_runs(
            connection,
            asset_id,
            limit=window,
            exclude_current_run=exclude_current_run,
            current_run_id=run_id,
            limit_type=drift_type,
        )
        with connection.cursor() as cursor:
            if last_runs:
                if type(last_runs) == dict:
                    last_runs = [last_runs]
                last_runs = list(
                    map(lambda run: f"""'{run.get("run_id")}'""", last_runs)
                )
                run_ids = ", ".join(last_runs)
                run_ids = f"({run_ids})"

                query_string = f"""
                    select source.asset_id, source.attribute_id, source.measure_id, source.key,
                    jsonb_agg(json_build_object('value', source.{value_column_name}, 'created_date', source.slice_end)) as metric_values
                    from "behavioral".{table_name} as source
                    join core.measure as mes on mes.id=source.measure_id
                    join core.base_measure as base ON base.id = mes.base_measure_id and base.is_visible=true
                    where mes.is_drift_enabled = True and source.run_id in {run_ids}
                    group by source.asset_id, source.attribute_id, source.measure_id, source.key
                """
                cursor = execute_query(connection, cursor, query_string)
                metrics = fetchall(cursor)
                return metrics
    except Exception as e:
        raise e


def get_asset_metrics(config: dict, exclude_current_run: bool = False) -> list:
    """
    Returns the list of metrics for the given window frame
    """
    try:
        asset_id = config.get("asset_id")
        attribute = config.get("attribute")
        attribute = attribute if attribute else {}
        attribute_id = attribute.get("id")
        is_attribute_level = config.get("is_attribute_level", False)
        selected_attributes = config.get("selected_attributes")
        is_pipeline_level = config.get("is_pipeline_task", False)
        pipeline_task_id = config.get("pipeline_task_id")
        selected_attributes_query = ""
        if selected_attributes:
            attribute_ids = []
            for attribute in selected_attributes:
                if not attribute:
                    continue

                if isinstance(attribute, dict):
                    attribute_ids.append(f"""'{str(attribute.get("id"))}'""")
                else:
                    attribute_ids.append(f"""'{str(attribute)}'""")
            selected_attributes = attribute_ids
            selected_attributes_query = ", ".join(selected_attributes)
        measure_id = config.get("measure_id")
        run_id = config.get("run_id")
        window = int(config.get("window", DRIFT_DAYS))
        drift_type = config.get("drift_type", "runs")
        job_type = config.get("job_type")
        category = config.get("category")
        is_asset_level = config.get("is_asset", False)
        is_pipeline_level = config.get("is_pipeline_task", False)
        metrics_type_query = f" and base.type = '{job_type}' " if job_type else ""
        attribute_query = ""
        if job_type == OBSERVE:
            measure_id = None
            connection_id = config.get("connection_id")
            metrics_type_query = f" and base.type='{RELIABILITY}'"
            if connection_id:
                metrics_type_query = (
                    f" {metrics_type_query} and mes.connection_id='{connection_id}'"
                )
        if job_type == HEALTH:
            measure_id = None
            metrics_type_query = (
                f" and base.type='{DISTRIBUTION}' and base.category='{HEALTH}' "
            )
            attribute_query = (
                f" and mes.attribute_id in ({selected_attributes_query})"
                if is_attribute_level
                and selected_attributes
                and selected_attributes_query
                else ""
            )
        elif job_type == PROFILE:
            measure_id = None
            metrics_type_query = f" and base.type in ('{DISTRIBUTION}', '{FREQUENCY}') and (base.category != '{HEALTH}' or base.category is null) "
            attribute_query = (
                f" and mes.attribute_id in ({selected_attributes_query})"
                if is_attribute_level
                and selected_attributes
                and selected_attributes_query
                else ""
            )
        elif job_type == STATISTICS:
            measure_id = None
            attribute_query = (
                f" and mes.attribute_id='{attribute_id}'" if attribute_id else ""
            )
        elif job_type in [BUSINESS_RULES, SEMANTIC_MEASURE]:
            metrics_type_query = " and base.type in ('custom', 'semantic') and lower(base.category) in ('conditional', 'query', 'lookup') "
            if category == PATTERN:
                metrics_type_query = " and base.type in ('frequency') and lower(base.category) in ('pattern') "
        elif job_type == BEHAVIORAL:
            metrics_type_query = (
                " and base.type = 'custom' and lower(base.category) = 'behavioral' "
            )

        measure_query = ""
        if measure_id:
            measure_query = f" and met.measure_id='{measure_id}'  "
        pipeline_query = f" and met.task_id='{pipeline_task_id}'" if pipeline_task_id else ""

        metrics_level_query = (
            " and base.level = 'asset' "
            if is_asset_level or is_pipeline_level
            else " and base.level in ('attribute', 'term', 'measure') "
        )

        connection = get_postgres_connection(config)
        metrics = []
        last_runs = get_last_runs(
            connection,
            asset_id,
            limit=window,
            exclude_current_run=exclude_current_run,
            current_run_id=run_id,
            measure_id=measure_id,
            limit_type=drift_type,
        )
        with connection.cursor() as cursor:
            if last_runs:
                if type(last_runs) == dict:
                    last_runs = [last_runs]
                last_runs = list(
                    map(
                        lambda run: f"""'{run.get("run_id")}'""",
                        filter(lambda x: x.get("run_id"), last_runs),
                    )  # Filter out None run_ids)
                )
                run_ids = ", ".join(last_runs)
                run_ids = f"({run_ids})"

                asset_query = f"and met.asset_id='{asset_id}'" if asset_id else ""
                reset_timestamp = None
                reset_filter = ""
                if attribute_id:
                    reset_timestamp = get_last_reset_timestamp(connection,attribute_id)
                    if reset_timestamp:
                        reset_filter = f"AND met.created_date > '{reset_timestamp}'"
                        log_info(f"Using reset timestamp filter: {reset_timestamp}")

                query_string = f"""
                    select distinct met.attribute_id, met.measure_id, met.measure_name, base.type, base.category, met.task_id,
                    jsonb_agg(json_build_object('created_date', met.created_date, 'value', met.value)) as metric_values
                    from core.metrics as met
                    join core.measure as mes on mes.id=met.measure_id
                    join core.base_measure as base on base.id=mes.base_measure_id
                    where mes.is_drift_enabled = True {reset_filter} {asset_query}
                    and met.status = 'passed' and met.run_id in {run_ids}
                    {metrics_type_query} {metrics_level_query} {measure_query} {attribute_query} {pipeline_query}
                    group by met.attribute_id, met.measure_id, met.measure_name, base.type, base.category, met.task_id
                    order by met.attribute_id, met.measure_id, met.measure_name desc
                """
                log_info(("asset_metric query", query_string))
                cursor = execute_query(connection, cursor, query_string)
                metrics = fetchall(cursor)
                return metrics
    except Exception as e:
        raise e


def get_postgres_metrics(config: dict, exclude_current_run: bool = False) -> list:
    """
    Returns the list of metrics for the given window frame
    """
    try:
        asset_id = config.get("asset_id")
        attribute = config.get("attribute")
        attribute = attribute if attribute else {}
        attribute_id = attribute.get("id")
        is_attribute_level = config.get("is_attribute_level", False)
        selected_attributes = config.get("selected_attributes")
        selected_attributes_query = ""
        if selected_attributes:
            attribute_ids = []
            for attribute in selected_attributes:
                if not attribute:
                    continue

                if isinstance(attribute, dict):
                    attribute_ids.append(f"""'{str(attribute.get("id"))}'""")
                else:
                    attribute_ids.append(f"""'{str(attribute)}'""")
            selected_attributes = attribute_ids
            selected_attributes_query = ", ".join(selected_attributes)
        measure_id = config.get("measure_id")
        run_id = config.get("run_id")
        window = int(config.get("window", DRIFT_DAYS))
        drift_type = config.get("drift_type", "runs")
        job_type = config.get("job_type")
        category = config.get("category")
        is_asset_level = config.get("is_asset", False)
        pipeline_task_id = config.get("pipeline_task_id")
        is_pipeline_level = config.get("is_pipeline_task", False)
        metrics_type_query = f" and base.type = '{job_type}' " if job_type else ""
        attribute_query = ""
        if job_type == OBSERVE:
            measure_id = None
            connection_id = config.get("connection_id")
            metrics_type_query = f" and base.type='{RELIABILITY}'"
            if connection_id:
                metrics_type_query = (
                    f" {metrics_type_query} and mes.connection_id='{connection_id}'"
                )
        if job_type == HEALTH:
            measure_id = None
            metrics_type_query = (
                f" and base.type='{DISTRIBUTION}' and base.category='{HEALTH}' "
            )
            attribute_query = (
                f" and mes.attribute_id in ({selected_attributes_query})"
                if is_attribute_level
                and selected_attributes
                and selected_attributes_query
                else ""
            )
        elif job_type == PROFILE:
            measure_id = None
            metrics_type_query = f" and base.type in ('{DISTRIBUTION}', '{FREQUENCY}') and (base.category != '{HEALTH}' or base.category is null) "
            attribute_query = (
                f" and mes.attribute_id in ({selected_attributes_query})"
                if is_attribute_level
                and selected_attributes
                and selected_attributes_query
                else ""
            )
        elif job_type == STATISTICS:
            measure_id = None
            attribute_query = (
                f" and mes.attribute_id='{attribute_id}'" if attribute_id else ""
            )
        elif job_type in [BUSINESS_RULES, SEMANTIC_MEASURE]:
            metrics_type_query = " and base.type in ('custom', 'semantic') and lower(base.category) in ('conditional', 'query', 'lookup') "
            if category == PATTERN:
                metrics_type_query = " and base.type in ('frequency') and lower(base.category) in ('pattern') "
        elif job_type == BEHAVIORAL:
            metrics_type_query = (
                " and base.type = 'custom' and lower(base.category) = 'behavioral' "
            )

        measure_query = ""
        if measure_id:
            measure_query = f" and met.measure_id='{measure_id}'  "

        pipeline_query = ""
        if pipeline_task_id:
            pipeline_query = f" and met.task_id='{pipeline_task_id}'"

        metrics_level_query = (
            " and base.level = 'asset' "
            if is_asset_level or is_pipeline_level
            else " and base.level in ('attribute', 'term', 'measure') "
        )

        connection = get_postgres_connection(config)
        last_runs = get_last_runs(
            connection,
            asset_id,
            limit=window,
            exclude_current_run=exclude_current_run,
            current_run_id=run_id,
            measure_id=measure_id,
            limit_type=drift_type,
        )
        with connection.cursor() as cursor:
            if last_runs:
                if type(last_runs) == dict:
                    last_runs = [last_runs]
                last_runs = [
                    f"""'{run.get("run_id")}'"""
                    for run in last_runs
                    if run.get("run_id")
                ]
                run_ids = ", ".join(last_runs)
                run_ids = f"({run_ids})"

                asset_query = f"and met.asset_id='{asset_id}'" if asset_id else ""

                total_metrics_query = f"""
                    SELECT COUNT(*) AS total_metrics
                    FROM (
                        SELECT 1
                        FROM core.metrics AS met
                        JOIN core.measure AS mes ON mes.id = met.measure_id
                        JOIN core.base_measure AS base ON base.id = mes.base_measure_id
                        WHERE mes.is_drift_enabled = TRUE
                            {asset_query}
                            AND met.status = 'passed'
                            AND met.run_id in {run_ids}
                            {metrics_type_query}
                            {metrics_level_query}
                            {measure_query}
                            {attribute_query}
                            {pipeline_query}
                        GROUP BY met.measure_id, met.measure_name
                    ) AS distinct_measures;
                """
                log_info(("total_metrics_query", total_metrics_query))
                cursor = execute_query(connection, cursor, total_metrics_query)
                total_metrics_count = fetchone(cursor)
                total_metrics = (
                    total_metrics_count.get("total_metrics")
                    if total_metrics_count
                    else 0
                )
                total_metrics = total_metrics if total_metrics else 0

                query_string = f"""
                    select distinct met.attribute_id, met.task_id
                    ,met.measure_id
                    ,met.measure_name
                    ,base.type
                    ,base.category
                    ,ARRAY_AGG(met.value ORDER BY met.created_date) FILTER (WHERE met.value IS NOT NULL AND TRIM(met.value) <> '') AS metric_values
                    from core.metrics as met
                    join core.measure as mes on mes.id=met.measure_id
                    join core.base_measure as base on base.id=mes.base_measure_id
                    where mes.is_drift_enabled = True {asset_query}
                    and met.status = 'passed' AND met.measure_name not in ('enum', 'long_pattern', 'length','short_pattern') and met.run_id in {run_ids}
                    {metrics_type_query} {metrics_level_query} {measure_query} {attribute_query} {pipeline_query}
                    group by met.attribute_id, met.measure_id, met.task_id, met.measure_name, base.type, base.category
                    order by met.attribute_id, met.measure_id, met.measure_name desc
                """
                log_info(("total_metrics_query_stringy", query_string))
                return query_string, total_metrics
    except Exception as e:
        raise e


def get_metrics(
    config: dict, exclude_current_run: bool = False, is_behavioral: bool = False
) -> list:
    """
    Returns the list of metrics for the given window frame
    """
    try:
        if is_behavioral:
            return get_behavioral_metrics(config, exclude_current_run)
        else:
            return get_asset_metrics(config, exclude_current_run)
    except Exception as e:
        raise e


def get_latest_behavioral_metrics(config: dict, run_id: str):
    """
    Returns the list of behavioral metrics for the given run id
    """
    try:
        measure = config.get("measure")
        measure_id = measure.get("id")
        table_name = get_behavioral_table_name(measure)

        behavioral_data = []
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            total_count_query = f"""
                select count(*) as total_metrics from  "behavioral".{table_name} as behavior
                join core.measure as mes on mes.id=behavior.measure_id
                where mes.id ='{measure_id}' and behavior.run_id = '{run_id}'
                and mes.is_active=True and mes.is_delete=False and mes.is_drift_enabled
            """
            cursor = execute_query(connection, cursor, total_count_query)
            total_metrics_count = fetchone(cursor)
            total_metrics = (
                total_metrics_count.get("total_metrics") if total_metrics_count else 0
            )
            total_metrics = total_metrics if total_metrics else 0

            query_string = f"""
                select behavior.*, mes.is_auto, mes.threshold_constraints, mes.drift_threshold
                from  "behavioral".{table_name} as behavior
                join core.measure as mes on mes.id=behavior.measure_id
                where mes.id ='{measure_id}' and behavior.run_id = '{run_id}'
                and mes.is_active=True and mes.is_delete=False and mes.is_drift_enabled
            """
            default_limit = DEFAULT_CHUNK_LIMIT
            if total_metrics > default_limit:
                total_limit = int(total_metrics / default_limit) + (
                    1 if (total_metrics % default_limit) > 0 else 0
                )
                for i in range(total_limit):
                    offset = i * default_limit
                    offset = offset + 1 if offset > 0 else offset
                    limited_query = (
                        f"{query_string} offset {offset} limit {default_limit}"
                    )
                    cursor = execute_query(connection, cursor, limited_query)
                    limited_metrics = fetchall(cursor)
                    if limited_metrics:
                        yield limited_metrics
            else:
                cursor = execute_query(connection, cursor, query_string)
                behavioral_data = fetchall(cursor)
                yield behavioral_data
    except Exception as e:
        raise e


def get_latest_asset_metrics(config: dict, run_id: str):
    """
    Returns the list of metrics for the given run id and task types
    """
    try:
        asset_id = config.get("asset_id")
        attribute = config.get("attribute")
        attribute = attribute if attribute else {}
        attribute_id = attribute.get("id")
        measure_id = config.get("measure_id")
        is_attribute_level = config.get("is_attribute_level", False)
        is_pipeline_level = config.get("is_pipeline_task", False)
        pipeline_task_id = config.get("pipeline_task_id")
        selected_attributes = config.get("selected_attributes")
        selected_attributes_query = ""
        if selected_attributes:
            attribute_ids = []
            for attribute in selected_attributes:
                if not attribute:
                    continue

                if isinstance(attribute, dict):
                    attribute_ids.append(f"""'{str(attribute.get("id"))}'""")
                else:
                    attribute_ids.append(f"""'{str(attribute)}'""")
            selected_attributes = attribute_ids
            selected_attributes_query = ", ".join(selected_attributes)
        connection = get_postgres_connection(config)
        job_type = config.get("job_type")
        category = config.get("category")
        is_asset_level = config.get("is_asset", False)
        is_pipeline_level = config.get("is_pipeline_task")
        metrics_type_query = f" and base.type = '{job_type}' " if job_type else ""
        attribute_query = ""
        if job_type == OBSERVE:
            measure_id = None
            connection_id = config.get("connection_id")
            metrics_type_query = f" and base.type='{RELIABILITY}'"
            if connection_id:
                metrics_type_query = (
                    f" {metrics_type_query} and mes.connection_id='{connection_id}'"
                )
        if job_type == HEALTH:
            measure_id = None
            metrics_type_query = (
                f" and base.type='{DISTRIBUTION}' and base.category='{HEALTH}' "
            )
            attribute_query = (
                f" and mes.attribute_id in ({selected_attributes_query})"
                if is_attribute_level
                and selected_attributes
                and selected_attributes_query
                else ""
            )
        elif job_type == PROFILE:
            measure_id = None
            metrics_type_query = f" and base.type in ('{DISTRIBUTION}', '{FREQUENCY}') and (base.category != '{HEALTH}' or base.category is null) "
            attribute_query = (
                f" and mes.attribute_id in ({selected_attributes_query})"
                if is_attribute_level
                and selected_attributes
                and selected_attributes_query
                else ""
            )
        elif job_type == STATISTICS:
            measure_id = None
            attribute_query = (
                f" and mes.attribute_id='{attribute_id}'" if attribute_id else ""
            )
        elif job_type in [BUSINESS_RULES, SEMANTIC_MEASURE]:
            metrics_type_query = " and base.type in ('custom', 'semantic') and lower(base.category) in ('conditional', 'query', 'lookup') "
            if category == PATTERN:
                metrics_type_query = " and base.type in ('frequency') and lower(base.category) in ('pattern') "
        elif job_type == BEHAVIORAL:
            metrics_type_query = (
                " and base.type = 'custom' and lower(base.category) = 'behavioral' "
            )

        measure_query = ""
        if measure_id:
            measure_query = f" and met.measure_id='{measure_id}'  "
        
        pipeline_query = ""
        if pipeline_task_id:
            pipeline_query = f" and met.task_id='{pipeline_task_id}'"

        metrics_level_query = (
            " and base.level = 'asset' "
            if is_asset_level or is_pipeline_level
            else " and base.level in ('attribute', 'term', 'measure') "
        )
        asset_query = f"and met.asset_id='{asset_id}'" if asset_id else ""

        if run_id:
            with connection.cursor() as cursor:
                total_count_query = f"""
                    select count(*) as total_metrics from core.metrics as met
                    join core.measure as mes on mes.id=met.measure_id
                    join core.base_measure as base on base.id=mes.base_measure_id
                    where  mes.is_drift_enabled = True {asset_query}
                    and met.status = 'passed' and met.run_id = '{run_id}'
                    {metrics_type_query} {metrics_level_query} {measure_query} {attribute_query}
                    {pipeline_query}
                """
                cursor = execute_query(connection, cursor, total_count_query)
                total_metrics_count = fetchone(cursor)
                total_metrics = (
                    total_metrics_count.get("total_metrics")
                    if total_metrics_count
                    else 0
                )
                total_metrics = total_metrics if total_metrics else 0

                query_string = f"""
                    select met.id, met.measure_name, met.asset_id, met.attribute_id, met.measure_id, met.total_count,
                    met.value, met.created_date, met.run_id, met.level, base.type, base.category, met.threshold,
                    mes.threshold_constraints, mes.is_auto, mes.is_drift_enabled, base.name as base_measure_name,
                    base.technical_name as technical_name
                    from core.metrics as met
                    join core.measure as mes on mes.id=met.measure_id
                    join core.base_measure as base on base.id=mes.base_measure_id
                    where mes.is_drift_enabled = True {asset_query}
                    and met.status = 'passed' and met.run_id = '{run_id}' and met.measure_name not in ('length','enum', 'short_pattern', 'pattern', 'long_pattern')
                    {metrics_type_query} {metrics_level_query} {measure_query} {attribute_query} {pipeline_query}
                    order by met.created_date desc
                """
                default_limit = DEFAULT_CHUNK_LIMIT
                if total_metrics > default_limit:
                    total_limit = int(total_metrics / default_limit) + (
                        1 if (total_metrics % default_limit) > 0 else 0
                    )
                    for i in range(total_limit):
                        offset = i * default_limit
                        offset = offset + 1 if offset > 0 else offset
                        limited_query = (
                            f"{query_string} offset {offset} limit {default_limit}"
                        )
                        cursor = execute_query(connection, cursor, limited_query)
                        limited_metrics = fetchall(cursor)
                        if limited_metrics:
                            yield limited_metrics
                else:
                    cursor = execute_query(connection, cursor, query_string)
                    metrics = fetchall(cursor)
                    yield metrics
    except Exception as e:
        raise e


def get_latest_metrics(config: dict, run_id: str, is_behavioral: bool) -> list:
    """
    Returns the list of metrics for the given run id and task types
    """
    try:
        if is_behavioral:
            return get_latest_behavioral_metrics(config, run_id)
        else:
            return get_latest_asset_metrics(config, run_id)
    except Exception as e:
        raise e


def get_total_asset_measure_runs(config: dict, measure_id: str) -> list:
    """
    Returns the total runs count for each metrics
    """
    try:
        asset_id = config.get("asset_id")
        job_type = config.get("job_type")
        measure = config.get("measure")
        measure = measure if measure else {}
        connection = get_postgres_connection(config)
        if job_type == PROFILE: #fix
            measure_id = None
        elif job_type == STATISTICS:
            measure_id = measure_id
        else:
            measure_id = config.get("measure_id")

        queries = []
        if measure_id:
            queries.append(f"measure_id='{measure_id}' ")
        if asset_id:
            queries.append(f"asset_id='{asset_id}' ")
            queries.append(f"is_archived = False ")
        filter_query_string = ""
        if queries:
            filter_query_string = " and ".join(queries)
            filter_query_string = f"where {filter_query_string}"
        total_measure_runs = {}
        with connection.cursor() as cursor:
            query_string = f"""
                select distinct measure_id, measure_name, count(measure_id) as total_runs from core.metrics
                {filter_query_string}
                group by measure_id, measure_name
            """
            cursor = execute_query(connection, cursor, query_string)
            result = fetchall(cursor)
            for row in result:
                measure_id = row.get("measure_id")
                measure_name = row.get("measure_name")
                measure_name = get_attribute_label(measure_name, False)
                total_runs = row.get("total_runs")
                total_runs = total_runs if total_runs else 0
                if not (measure_id and measure_name):
                    continue
                key = f"{str(measure_id).lower()}___{str(measure_name).lower()}"
                total_measure_runs.update({key: total_runs})
        return total_measure_runs
    except Exception as e:
        raise e


def get_total_behavioral_measure_runs(config: dict) -> list:
    """
    Returns the total runs count for each metrics
    """
    try:
        measure = config.get("measure")
        measure_id = measure.get("id")
        table_name = get_behavioral_table_name(measure)

        connection = get_postgres_connection(config)
        total_measure_runs = {}
        with connection.cursor() as cursor:
            query_string = f"""
                select key, count(distinct run_id) as total_runs
                from  "behavioral".{table_name}
                group by key
            """
            cursor = execute_query(connection, cursor, query_string)
            result = fetchall(cursor)
            for row in result:
                measure_name = row.get("key")
                total_runs = row.get("total_runs")
                total_runs = total_runs if total_runs else 0
                if not (measure_id and measure_name):
                    continue
                key = f"{str(measure_id).lower()}___{str(measure_name).lower()}"
                total_measure_runs.update({key: total_runs})
        return total_measure_runs
    except Exception as e:
        raise e


def get_total_measure_runs(config: dict, measure_id: str, is_behavioral: bool = False) -> list:
    """
    Returns the list of metrics for the given run id and task types
    """
    try:
        metrics = []
        if is_behavioral:
            metrics = get_total_behavioral_measure_runs(config)
        else:
            metrics = get_total_asset_measure_runs(config, measure_id)
        return metrics
    except Exception as e:
        raise e


def get_latest_threshold(config: dict, attributes: list, measures: list) -> dict:
    """
    Returns the list of measure for the given window frame
    """
    try:
        asset_id = config.get("asset_id")
        connection = get_postgres_connection(config)
        measure_ids = ", ".join(measures)
        attribute_ids = ", ".join(attributes)
        attribute_query = (
            f" and attribute_id in ({attribute_ids}) " if attribute_ids else ""
        )
        asset_query = f"and asset_id='{asset_id}'" if asset_id else ""

        thresholds = {}
        with connection.cursor() as cursor:
            query_string = f"""
                    select distinct on (attribute_id, measure_id) attribute_id, measure_id, created_date,
                    lower_threshold, upper_threshold
                    from core.drift_threshold
                    where measure_id in ({measure_ids}) {asset_query}  {attribute_query}
                    order by attribute_id, measure_id, created_date desc
                """
            cursor = execute_query(connection, cursor, query_string)
            latest_thresholds = fetchall(cursor)

            for threshold in latest_thresholds:
                attribute_id = threshold.get("attribute_id")
                measure_id = threshold.get("measure_id")
                attribute_measures = thresholds.get(attribute_id, {})
                attribute_measures.update(
                    {
                        measure_id: {
                            "lower_threshold": threshold.get("lower_threshold", 0),
                            "upper_threshold": threshold.get("upper_threshold", 0),
                            "is_auto": threshold.get("is_auto", True),
                        }
                    }
                )
                thresholds.update({attribute_id: attribute_measures})
        return thresholds
    except Exception as e:
        raise e


def get_percentage(current: float, previous: float) -> int:
    """
    Returns the percentage change between two values
    """
    if current == previous:
        return 0
    try:
        return (abs(current - previous) / previous) * 100.0
    except ZeroDivisionError:
        return float("inf")


def calculate_confidence_interval(arr: list) -> tuple:
    """
    Confidence Interval is calcualted using the following formula:
    UI = m + (Z * (s/sqrt.n))
    LI = m - (Z * (s/sqrt.n))

    params:
    m: float - mean of the sample array
    s: float - standard deviation of the sample array
    Z: float - Z-score of 95% interval
    n: int   - number of elements in the array

    returns:
    UI: float - Upper Interval
    LI: float - Lower Interval
    """

    Z = 1.959964  # z-score
    mean = np.mean(arr)
    stddev = np.std(arr)
    n = len(arr)

    UI = mean + (Z * (stddev / math.sqrt(n)))  # upper interval
    LI = mean - (Z * (stddev / math.sqrt(n)))  # lower interval

    return LI, UI


def round_decimals_up(number: float, decimals: int = 2):
    """
    Returns a value rounded up to a specific number of decimal places.
    """
    if not isinstance(decimals, int):
        raise TypeError("decimal places must be an integer")
    elif decimals < 0:
        raise ValueError("decimal places has to be 0 or more")
    elif decimals == 0:
        return math.ceil(number)

    factor = 10**decimals
    return math.ceil(number * factor) / factor


def generate_prophet_dates(num: int) -> list:
    """Generating dates ('ds') for prophet values

    Parameters
    -----------
    num : len of list of values

    Returns:
    -----------
    list : list of dates starting from current date

    """
    start = datetime.date.today()
    periods = num  # generating the num of periods based on list of values
    daterange = []
    for day in range(periods):
        _date = (start + datetime.timedelta(days=day)).isoformat()
        daterange.append(_date)
    return daterange


def get_threshold_percentage(value: float, total_count: int):
    try:
        if not total_count:
            return 0
        return round((value / total_count) * 100, 2)
    except ZeroDivisionError:
        return 0


def generate_manual_threshold_message(
    base_measure_name: str,
    alert_status: str,
    threshold_constraints: list,
    current_value: float,
    percent_change: float,
    dqscore: float,
) -> str:
    """
    Generate manual threshold error message based on condition
    """
    message = ""
    # Create the hash table for query_expression
    query_expression_lookup = {
        "isGreaterThan": "because it exceeds the",
        "isLessThan": "because it falls below the",
        "isGreaterThanOrEqualTo": "because it exceeds the",
        "isLessThanOrEqualTo": "because it falls below the",
        "isEqualTo": "because it's equal to the",
        "isBetween": "because it's between the",
        "isNotBetween": "because it's outside the",
    }

    if alert_status == ALERT_OK:
        return message

    threshold_constraint = next(
        (
            constraint
            for constraint in threshold_constraints
            if (
                constraint
                and str(constraint.get("priority", "")).lower() == alert_status.lower()
            )
        ),
        None,
    )
    expression = threshold_constraint.get("query")
    threshold_type = threshold_constraint.get("type", "value")
    condition = threshold_constraint.get("condition")
    expression = expression if expression else ""
    # Additional expression information for constraint condition
    query_expression = query_expression_lookup.get(condition)

    if threshold_type == "value":
        expression = expression.replace("<attribute>", "value")
        if base_measure_name.lower() in ["value range", "length range"]:
            message = f"The {base_measure_name} failed because {current_value} number of records exceed the allowed range ({expression})."
        else:
            message = f"The {base_measure_name} failed for value {current_value} {query_expression} manual constraint of {expression}"
    elif threshold_type == "percentage":
        expression = expression.replace("<attribute>", "percent")
        message = f"The {base_measure_name} failed for percent {percent_change} {query_expression} manual constraint of {expression}"
    elif threshold_type == "score":
        expression = expression.replace("<attribute>", "score")
        message = f"The {base_measure_name} failed for score {dqscore} {query_expression} manual constraint of {expression}"
    return message


def check_manual_alert_status(
    base_measure_name: str,
    threshold_constraints_object: dict,
    current_value: float,
    percent_change: float,
    dqscore: float,
):
    """
    Returns alert status for the given value in manual mode.

    This function evaluates the current value and percent change against a set of threshold constraints
    provided in the threshold_constraints_object. The constraints are evaluated based on their priority
    ('High', 'Medium', 'Low'), and the highest priority breach determines the alert status.

    Parameters:
    - base_measure_name (str): The name of the base measure being evaluated.
    - threshold_constraints_object (dict): An object containing threshold constraints.
      The expected format is:
      {
          "threshold_data": [
              {"type": "value", "query": "<attribute> > 10", "value": "10", "priority": "High", "condition": "isGreaterThan"},
              {"type": "value", "query": "<attribute> > 20", "value": "20", "priority": "Medium", "condition": "isGreaterThan"},
              {"type": "value", "query": "<attribute> > 30", "value": "30", "priority": "Low", "condition": "isGreaterThan"}
          ]
      }
    - current_value (float): The current value of the attribute being evaluated.
    - percent_change (float): The percent change of the attribute being evaluated.

    Returns:
    - alert_status (str): The alert status based on the priority ('High', 'Medium', 'Low', or 'OK').
    - threshold_message (str): A message explaining the threshold breach.
    """

    alert_status = ALERT_OK
    threshold_message = ""
    threshold_constraints_list = threshold_constraints_object.get("threshold_data", "")
    if threshold_constraints_list:
        # Define the priority order
        priority_order = {"High": 1, "Medium": 2, "Low": 3}
        # Sort the list using the custom order
        sorted_threshold_data = sorted(
            threshold_constraints_list, key=lambda x: priority_order[x.get("priority")]
        )

        for threshold_constraints in sorted_threshold_data:
            expression = threshold_constraints.get("query")
            type = threshold_constraints.get("type", "value")

            if not expression:
                continue

            priority = threshold_constraints.get("priority", "")
            default_value = 0

            if type == "value":
                expression = (
                    expression.replace("<attribute>", str(current_value))
                    .replace("<value>", str(default_value))
                    .replace("<value1>", str(default_value))
                    .replace("<value2>", str(default_value))
                )
            elif type == "percentage":
                expression = (
                    expression.replace("<attribute>", str(percent_change))
                    .replace("<value>", str(default_value))
                    .replace("<value1>", str(default_value))
                    .replace("<value2>", str(default_value))
                )
            elif type == "score":
                expression = (
                    expression.replace("<attribute>", str(dqscore))
                    .replace("<value>", str(default_value))
                    .replace("<value1>", str(default_value))
                    .replace("<value2>", str(default_value))
                )

            # Fix for DQL-1491
            if "is not" in expression:
                expression = expression.replace("is not", "!=")
            if "null" in expression:
                expression = expression.replace("null", "None")

            try:
                status = eval(expression)
            except Exception as e:
                alert_status = priority
                break
            if status:
                alert_status = priority
                break

    threshold_message = generate_manual_threshold_message(
        base_measure_name,
        alert_status,
        threshold_constraints_list,
        current_value,
        percent_change,
        dqscore,
    )
    return alert_status, threshold_message


def get_auto_alert_status(
    lower_threshold: float,
    upper_threshold: float,
    current_value: float,
    previous_value_metrics,
    attribute_id: str,
    measure_id: str,
    is_behavioral: bool,
    behavioral_key: str,
):

    # Compute std dev for identify the alert status
    previous_values = None
    if previous_value_metrics:
        previous_values = next(
            (
                previous_value_metric
                for previous_value_metric in previous_value_metrics
                if (
                    previous_value_metric.get("attribute_id") == attribute_id
                    and previous_value_metric.get("measure_id") == measure_id
                )
            ),
            None,
        )
    if is_behavioral and previous_value_metrics:
        previous_values = next(
            (
                previous_value_metric
                for previous_value_metric in previous_value_metrics
                if previous_value_metric.get("key") == behavioral_key
            ),
            None,
        )
    previous_metric_values = (
        list(previous_values.get("metric_values", [])) if previous_values else []
    )
    previous_metric_value = []
    for metric_value in previous_metric_values:
        value = __get_metric_value(metric_value)
        previous_metric_value.append(value)

    std_dev = np.std(previous_metric_value) if len(previous_metric_value) > 1 else 0
    std_dev = round(std_dev, 2) if std_dev else 0

    current_value = round_decimals_up(current_value)
    value_sign = True if current_value >= 0 else False

    if std_dev == 0:
        if upper_threshold == lower_threshold == 0:
            one_sigma = (lower_threshold - 1.5, upper_threshold + 1.5)
            two_sigma = (lower_threshold - 2.5, upper_threshold + 2.5)
        else:
            if current_value < 10e4:
                one_sigma = (
                    lower_threshold - (0.2 * lower_threshold),
                    upper_threshold + (0.2 * upper_threshold),
                )
                two_sigma = (
                    lower_threshold - (0.4 * lower_threshold),
                    upper_threshold + (0.4 * upper_threshold),
                )
            else:
                one_sigma = (
                    lower_threshold - (0.01 * lower_threshold),
                    upper_threshold + (0.01 * upper_threshold),
                )
                two_sigma = (
                    lower_threshold - (0.02 * lower_threshold),
                    upper_threshold + (0.02 * upper_threshold),
                )

    if 0 < std_dev <= 10:
        one_sigma = (lower_threshold - 3 * std_dev, upper_threshold + 3 * std_dev)
        # log distribution defines the sigma level
        two_sigma = (lower_threshold - 6 * std_dev, upper_threshold + 6 * std_dev)
    elif 10 < std_dev <= 150:
        one_sigma = (lower_threshold - 1.5 * std_dev, upper_threshold + 1.5 * std_dev)
        # log distribution defines the sigma level
        two_sigma = (lower_threshold - 2.5 * std_dev, upper_threshold + 2.5 * std_dev)
    elif 150 < std_dev <= 10e3:
        if lower_threshold < 0 and upper_threshold > 0:
            one_sigma = (lower_threshold - std_dev, upper_threshold + std_dev)
            two_sigma = (
                lower_threshold - 1.2 * std_dev,
                upper_threshold + 1.2 * std_dev,
            )
        else:
            one_sigma = (
                lower_threshold - (1.5 * std_dev),
                upper_threshold + (1.5 * std_dev),
            )
            # mean of the threshold values to get a estimate mean
            two_sigma = (
                lower_threshold - (2.5 * std_dev),
                upper_threshold + (2.5 * std_dev),
            )
    elif std_dev > 10e3:
        one_sigma = (lower_threshold - std_dev, upper_threshold + std_dev)
        two_sigma = (lower_threshold - 1.2 * std_dev, upper_threshold + 1.2 * std_dev)

    one_sigma = tuple([round(value, 2) for value in one_sigma])
    two_sigma = tuple([round(value, 2) for value in two_sigma])

    if value_sign:
        one_sigma = [val for val in one_sigma]
        two_sigma = [val for val in two_sigma]
        one_sigma[0] = one_sigma[0] if one_sigma[0] >= 0 else 0
        two_sigma[0] = two_sigma[0] if two_sigma[0] >= 0 else 0
        one_sigma, two_sigma = tuple(one_sigma), tuple(two_sigma)

    # Identify the alert status based on different criterion
    status = ALERT_OK
    std_dev = round(std_dev, 2)
    alert_metrics = {
        "one_sigma": one_sigma,
        "two_sigma": two_sigma,
        "previous_metric_values": previous_metric_values,
    }

    # condition for freshness
    if (one_sigma[0] == 0) and (two_sigma[0] == 0):
        if lower_threshold <= current_value <= upper_threshold:
            status = ALERT_OK
            return status, alert_metrics, std_dev
        elif one_sigma[0] <= current_value <= one_sigma[1]:
            status = ALERT_LOW
            return status, alert_metrics, std_dev
        elif two_sigma[0] < current_value <= two_sigma[1]:
            status = ALERT_MEDIUM
            return status, alert_metrics, std_dev
        elif current_value > two_sigma[1]:
            status = ALERT_HIGH
            return status, alert_metrics, std_dev

    if (
        (one_sigma[0] > 0 and two_sigma[0] >= 0)
        or (
            (one_sigma[0] < 0 and one_sigma[1] > 0)
            and (two_sigma[0] < 0 and two_sigma[1] > 0)
        )
        or (one_sigma[0] > 0 and two_sigma[0] <= 0)
    ):
        if (one_sigma[0] <= current_value < lower_threshold) or (
            upper_threshold < current_value <= one_sigma[1]
        ):
            status = ALERT_LOW
        elif (two_sigma[0] <= current_value < one_sigma[0]) or (
            one_sigma[1] < current_value <= two_sigma[1]
        ):
            status = ALERT_MEDIUM
        elif (current_value < two_sigma[0]) or (current_value > two_sigma[1]):
            status = ALERT_HIGH
        else:
            status = ALERT_OK
    elif (one_sigma[0] < 0 and one_sigma[1] < 0) and (
        two_sigma[0] < 0 and two_sigma[1] < 0
    ):
        if lower_threshold <= current_value <= upper_threshold:
            status = ALERT_OK
        elif (one_sigma[0] < current_value < lower_threshold) or (
            upper_threshold < current_value < one_sigma[1]
        ):
            status = ALERT_LOW
        elif (two_sigma[0] < current_value < one_sigma[0]) or (
            one_sigma[1] < current_value < two_sigma[1]
        ):
            status = ALERT_MEDIUM
        elif (current_value < two_sigma[0]) or (current_value > two_sigma[1]):
            status = ALERT_HIGH

    return status, alert_metrics, std_dev


def calculate_deviation(
    status: str,
    lower_threshold: float,
    upper_threshold: float,
    current_value: float,
    standard_deviation: float,
):
    """Calculate the standard deviation between value and the expected threshold"""
    deviation_change = 0
    difference = 0
    if lower_threshold <= current_value <= upper_threshold:
        deviation_change = 0
    else:
        if standard_deviation == 0:
            try:
                difference = abs(current_value - upper_threshold) / upper_threshold
            except ZeroDivisionError:
                difference = 0
            if status == ALERT_LOW:
                deviation_change = 1 + round(difference, 2)
            elif status == ALERT_MEDIUM:
                deviation_change = 2 + round(difference, 2)
            else:
                deviation_change = 3 + round(difference, 2)
        else:
            try:
                mean = (
                    lower_threshold + upper_threshold
                ) / 2  # calculate the mean of the lower and upper threshold
                deviation_change = (
                    abs(current_value - mean) / standard_deviation
                )  # calculate the change in mean and current_value divided by standard deviation
                deviation_change = round(
                    deviation_change, 2
                )  # the sigma is calculated to two decimal points
            except ZeroDivisionError:
                deviation_change = 0

    return deviation_change


def get_alert_message(
    measure_name: str,
    is_auto: bool,
    total_count: int,
    current_value: float,
    lower_threshold: float,
    upper_threshold: float,
    threshold_constraints: dict,
    measure_category: str,
    percent_change: float,
):
    message = ""
    alert_percentage = {}
    if is_auto and total_count:
        value_percentage = get_threshold_percentage(current_value, total_count)
        lower_threshold_percentage = get_threshold_percentage(
            lower_threshold, total_count
        )
        upper_threshold_percentage = get_threshold_percentage(
            upper_threshold, total_count
        )
        alert_percentage = {
            "value_percentage": value_percentage,
            "lower_threshold": lower_threshold_percentage,
            "upper_threshold": upper_threshold_percentage,
        }
        # message = f""" {measure_name} value % change {value_percentage}% exceeded the limit of {lower_threshold_percentage} to {upper_threshold_percentage}%"""

    if measure_category:
        if measure_category == LENGTH:
            measure_name = f"""Length of {str(measure_name)}"""
        elif measure_category == ENUM:
            measure_name = f"""{str(measure_name)} Category"""
        elif measure_category == PATTERN:
            measure_name = f"""{str(measure_name)} Pattern"""
    # when both lower and upper threshold are 0, the alert was not displaying. This is the fix for that issue.
    if (lower_threshold == 0 and upper_threshold == 0):
        message_status = "it exceeds"
        current_value = int(current_value)
        if current_value < lower_threshold:
            message_status = "it is below"
        """Adding constraint for int and float message values"""
        if measure_name and measure_name.lower() in ["freshness", "last updated", "sla", "delay", "latency"]:
            if lower_threshold:
                lower_threshold = format_freshness(lower_threshold)
            if upper_threshold:
                upper_threshold = format_freshness(upper_threshold)
            if current_value:
                current_value = format_freshness(current_value)
        message = f"""The {measure_name} failed for value {current_value} because {message_status} the auto constraint of {lower_threshold} to {upper_threshold}"""

    if lower_threshold or upper_threshold:
        message_status = "it exceeds"
        if isinstance(current_value, str):
            current_value = float(current_value)
        if current_value < lower_threshold:
            message_status = "it is below"
        if measure_name and measure_name.lower() in ["freshness", "last updated", "sla", "delay", "latency"]:  
            if lower_threshold:
                lower_threshold = format_freshness(lower_threshold)
            if upper_threshold:
                upper_threshold = format_freshness(upper_threshold)
            if current_value:
                current_value = format_freshness(current_value)
            message = f"""The {measure_name} failed for value {current_value} because {message_status} the auto constraint of {lower_threshold} to {upper_threshold}"""
        else:
            """Adding constraint for int and float message values"""
            frac, _ = math.modf(current_value)
            if frac == 0:
                current_value = int(current_value)
            message = f"""The {measure_name} failed for value {current_value} because {message_status} the auto constraint of {lower_threshold} to {upper_threshold}"""

    if not is_auto and threshold_constraints:
        expression = threshold_constraints.get("query")
        type = threshold_constraints.get("type")
        type = type if type else "value"
        expression = expression if expression else ""
        if type == "value":
            expression = expression.replace("<attribute>", "value")
            message = f""" {measure_name} failed for value {current_value} due to manual constraint of {expression}"""
        elif type == "percentage":
            expression = expression.replace("<attribute>", "percent")
            message = f""" {measure_name} failed for percent {percent_change} due to manual constraint of {expression}"""

    return message, alert_percentage


def get_marked_values(config: dict, is_behavioral: bool = False) -> dict:
    """
    Returns the list of marked values for the given measure
    """
    try:
        asset_id = config.get("asset_id")
        measure_id = config.get("measure_id")
        run_id = config.get("run_id")
        window = int(config.get("window", DRIFT_DAYS))
        drift_type = config.get("drift_type", "runs")
        measure_query = ""
        if measure_id:
            measure_query = f" and alert.measure_id='{measure_id}'  "
        asset_query = f"and alert.asset_id='{asset_id}'" if asset_id else ""

        connection = get_postgres_connection(config)
        marked_values = {}
        last_runs = get_last_runs(
            connection,
            asset_id,
            limit=window,
            exclude_current_run=True,
            current_run_id=run_id,
            measure_id=measure_id,
            limit_type=drift_type,
        )
        with connection.cursor() as cursor:
            if last_runs:
                if type(last_runs) == dict:
                    last_runs = [last_runs]
                last_runs = list(
                    map(lambda run: f"""'{run.get("run_id")}'""", last_runs)
                )
                run_ids = ", ".join(last_runs)
                run_ids = f"({run_ids})"

                behavioral_key = ""
                behavioral_group_key = ""
                if is_behavioral:
                    behavioral_key = "behavioral_key, "
                    behavioral_group_key = ", behavioral_key"

                query_string = f"""
                    select distinct on (alert.measure_id) alert.measure_id, {behavioral_key}
                    json_agg(case when alert.marked_as='normal' then alert.value end) as normal_values,
                    json_agg(case when alert.marked_as='outlier' then alert.value end) as outlier_values
                    from core.metrics as alert
                    where alert.run_id in {run_ids} {measure_query} {asset_query}
                    group by alert.measure_id {behavioral_group_key}
                """
                cursor = execute_query(connection, cursor, query_string)
                result = fetchall(cursor)

                for data in result:
                    measure_id = data.get("measure_id")
                    if is_behavioral:
                        measure_id = data.get("behavioral_key")

                    normal_values = data.get("normal_values")
                    normal_values = normal_values if normal_values else []
                    normal_values = [
                        __get_metric_value({"value": value})
                        for value in normal_values
                        if value
                    ]

                    outlier_values = data.get("outlier_values")
                    outlier_values = outlier_values if outlier_values else []
                    outlier_values = [
                        __get_metric_value({"value": value})
                        for value in outlier_values
                        if value
                    ]

                    marked_values.update(
                        {
                            measure_id: {
                                "normal_values": normal_values,
                                "outlier_values": outlier_values,
                            }
                        }
                    )

        return marked_values
    except Exception as e:
        raise e


def __get_metric_value(metric):
    value = metric.get("value")
    try:
        value = float(value) if value else 0
    except:
        value = 0
    return value


def update_flag_threshold(
    metrics_values: list, mark_passed: list, mark_failed: list
) -> list:
    """
    Updating the threshold based on mark as passed/mark as failed values by the user
    and returns an updated list for threshold to be calculate
    """
    # metrics_values = [
    #     __get_metric_value(metric)
    #     for metric in metrics
    #     if metric.get("value")
    # ]

    """calculate inter quartile regions for the values"""
    q3, q1 = np.percentile(metrics_values, [85, 10]) if metrics_values else (0, 0)
    sorted_values = []

    """ Removing outliers from the inter quartile range"""
    for val in metrics_values:
        if val not in mark_passed:
            if q1 <= val <= q3:
                sorted_values.append(val)
            else:
                sorted_values.append(val)
        else:
            sorted_values.append(val)
    median = np.median(sorted_values)
    passed_weight = [0.5, 1.5]

    """ Assigning weights to different use cases """

    if len(mark_failed) != 0:
        for val in mark_failed:
            for i in range(len(sorted_values)):
                if sorted_values[i] == val:
                    sorted_values[i] = median

    if len(mark_passed) != 0:
        for val in mark_passed:
            if val <= median:
                for i in range(len(sorted_values)):
                    if sorted_values[i] == val:
                        sorted_values[i] = passed_weight[0] * val
            else:
                for i in range(len(sorted_values)):
                    if sorted_values[i] == val:
                        sorted_values[i] = passed_weight[1] * val

    values = [{"value": value} for value in sorted_values]
    return values


def get_default_threshold(metrics: list, value_column_name: str):
    """
    Returns threshold for each behavioral key
    """
    default_threshold = {}
    if not value_column_name:
        return default_threshold

    grouped_metrics = {}
    for metric in metrics:
        key = metric.get("key")
        value = metric.get(value_column_name)
        try:
            value = float(value)
        except:
            value = 0
        if not key in grouped_metrics:
            grouped_metrics.update({key: []})

        metric_values = grouped_metrics.get(key, [])
        metric_values = metric_values if metric_values else []
        metric_values.append(value)
        grouped_metrics.update(
            {key: metric_values, "threshold": metric.get("drift_threshold").get(key)}
        )

    for key, values in grouped_metrics.items():
        if not values:
            continue
        if key == "threshold":
            lower_threshold, upper_threshold = values.get(
                "lower_threshold"
            ), values.get("upper_threshold")
            lower_threshold = int(math.floor(lower_threshold))
            upper_threshold = int(math.ceil(upper_threshold))
            default_threshold.update(
                {
                    key: {
                        "lower_threshold": lower_threshold,
                        "upper_threshold": upper_threshold,
                    }
                }
            )
        else:
            lower_threshold, upper_threshold = min(values), max(values)
            lower_threshold = int(math.floor(lower_threshold))
            upper_threshold = int(math.ceil(upper_threshold))
            default_threshold.update(
                {
                    key: {
                        "lower_threshold": lower_threshold,
                        "upper_threshold": upper_threshold,
                    }
                }
            )

    return default_threshold


def get_timeseries_metrics(metrics_array: list):
    """Get clean array values for metrics for drift analysis"""
    array_list = []
    metrics_values = []
    created_date = []

    for metric in metrics_array:
        value = metric.get("value")
        date_time = metric.get("created_date")
        if value:
            value = __get_metric_value(metric)
        if value and date_time:
            metrics_values.append(value)
            created_date.append((date_time))

    if not metrics_values or not created_date:
        return array_list

    """ Creating an empty dataframe to sort values based on created_date
    and returning the list of values as per the latest datetime"""
    df = pd.DataFrame()
    df["value"] = metrics_values
    if created_date:
        date_values = pd.to_datetime(created_date)
        df["date"] = date_values.date
        df["hour"] = date_values.hour
        df["minute"] = date_values.minute
        df["seconds"] = date_values.second

    """ Concatenating the individual datetime components into one datetime """
    df.loc[:, "created_date"] = pd.to_datetime(
        df["date"].astype(str)
        + " "
        + df["hour"].astype(str)
        + ":"
        + df["minute"].astype(str)
        + ":"
        + df["seconds"].astype(str)
    )

    df = df[["created_date", "value"]].copy(deep=True)
    df = df.sort_values(by=["created_date"])

    array_list = list(df["value"].values)

    return array_list


def get_measure_threshold(config: dict, measure_id: str = "") -> dict:
    """
    Returns the list of behavioral metrics for the given run id
    """
    try:
        asset_id = config.get("asset_id")
        job_type = config.get("job_type")
        category = config.get("category")
        is_asset_level = config.get("is_asset", False)
        is_pipeline_level = config.get("is_pipeline_task", False)
        pipeline_task_id = config.get("pipeline_task_id")
        asset_id = config.get("asset_id")
        attribute = config.get("attribute")
        attribute = attribute if attribute else {}
        attribute_id = attribute.get("id")

        metrics_type_query = f" and base.type = '{job_type}' " if job_type else ""
        attribute_query = ""
        if job_type == OBSERVE:
            connection_id = config.get("connection_id")
            metrics_type_query = f" and base.type='{RELIABILITY}'"
            if connection_id:
                metrics_type_query = (
                    f" {metrics_type_query} and mes.connection_id='{connection_id}'"
                )
        if job_type == HEALTH:
            metrics_type_query = (
                f" and base.type='{DISTRIBUTION}' and base.category='{HEALTH}' "
            )
        elif job_type == PROFILE:
            metrics_type_query = f" and base.type in ('{DISTRIBUTION}', '{FREQUENCY}') and (base.category != '{HEALTH}' or base.category is null) "
        elif job_type == STATISTICS:
            attribute_query = (
                f" and mes.attribute_id='{attribute_id}'" if attribute_id else ""
            )
        elif job_type in [BUSINESS_RULES, SEMANTIC_MEASURE]:
            metrics_type_query = " and base.type in ('custom', 'semantic') and lower(base.category) in ('conditional', 'query', 'lookup', 'parameter') "
            if category == PATTERN:
                metrics_type_query = " and base.type in ('frequency') and lower(base.category) in ('pattern') "
        elif job_type == BEHAVIORAL:
            metrics_type_query = (
                " and base.type = 'custom' and lower(base.category) = 'behavioral' "
            )

        metrics_level_query = (
            " and base.level = 'asset' "
            if is_asset_level or is_pipeline_level
            else " and base.level in ('attribute', 'term', 'measure') "
        )
        measure_query = f" and mes.id='{measure_id}'" if measure_id else ""
        asset_query = f" and mes.asset_id='{asset_id}'" if asset_id else ""
        pipeline_query = f" and mes.task_id='{pipeline_task_id}'" if pipeline_task_id else ""

        measure_thresholds = []
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select mes.id, mes.technical_name, mes.is_auto as is_auto,
                mes.threshold_constraints as constraints,
                mes.drift_threshold as threshold
                from core.measure as mes
                join core.base_measure as base on base.id=mes.base_measure_id
                where mes.is_active=True {asset_query}
                and mes.is_delete=False and mes.is_drift_enabled = True
                {metrics_type_query}{metrics_level_query}{measure_query}{attribute_query} {pipeline_query}
            """
            cursor = execute_query(connection, cursor, query_string)
            measure_thresholds = fetchall(cursor)
        return measure_thresholds
    except Exception as e:
        raise e


def get_previous_value(
    config: dict,
    measure_id: str = "",
    metric: dict = None,
    is_behavioral: str = False,
    is_alert: bool = False,
    is_threshold: bool = False,
    measure_name: str = None,
    is_parameter: bool = False,
) -> dict:
    """
    Returns the latest value for each measure
    """
    previous_value = 0
    measure_name = measure_name.strip("'") if measure_name else ""
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            if is_behavioral:
                # get the second last value for the key in behavioral metrics
                measure = config.get("measure")
                measure_id = measure.get("id")
                table_name = get_behavioral_table_name(measure)
                metric_key = metric.get("key")
                aggregator_col = get_behavioral_numerical_aggregator(config)
                aggregator_col = get_attribute_label(aggregator_col, False)
                query_string = f"""
                    select "{aggregator_col}" as value from behavioral.{table_name}
                    where key = '{metric_key}' and slice_start<'{metric.get("slice_start")}'
                    and measure_id = '{measure_id}'
                    order by slice_start desc
                    limit 1
                """
            elif is_alert:
                measure_name_condition = f"and met.measure_name = '{measure_name}'" if measure_name else ""
                if is_parameter:
                    query_string = f"""
                        select met.value, met.drift_status,met.alert_percentage
                        from core.metrics met
                        where met.measure_id = '{measure_id}' {measure_name_condition}
                        and status = 'passed'
                        and is_archived = False
                        order by created_date desc
                        limit 7;
                    """
                else:
                    query_string = f"""
                        select met.value, met.drift_status,met.alert_percentage
                        from core.metrics met
                        where met.measure_id = '{measure_id}'
                        {measure_name_condition}
                        and status = 'passed'
                        and is_archived = False
                        order by created_date desc
                        limit 7;
                    """
            elif is_threshold:
                measure_name_condition = f"and dr_threshold.measure_name = '{measure_name}'" if measure_name else ""
                if is_parameter:
                    query_string = f"""
                        select dr_threshold.lower_threshold,dr_threshold.upper_threshold
                        from core.drift_threshold dr_threshold
                        where dr_threshold.measure_id = '{measure_id}'
                        {measure_name_condition}
                        order by created_date desc
                        limit 7;    
                    """
                else:
                    query_string = f"""
                        select dr_threshold.lower_threshold,dr_threshold.upper_threshold
                        from core.drift_threshold dr_threshold
                        where dr_threshold.measure_id = '{measure_id}'
                        {measure_name_condition}
                        order by created_date desc
                        limit 7;    
                    """
            else:
                measure_name_condition = f"and met.measure_name = '{measure_name}'" if measure_name else ""
                query_string = f"""
                    select met.value, met.drift_status,met.alert_percentage 
                    from core.metrics met
                    where met.measure_id = '{measure_id}'
                    {measure_name_condition}
                    and status = 'passed'
                    and is_archived = False
                    order by created_date desc
                    limit 1 offset 1;
                """

            cursor = execute_query(connection, cursor, query_string)
            if is_alert or is_threshold:
                previous_value = fetchall(cursor)
            else:
                previous_value = fetchone(cursor)
        return previous_value
    except Exception as e:
        raise e


def get_previous_percent_metrics(
    config: dict,
    asset_id: str = "",
    measure_id: str = "",
    attribute_id: str = "",
    drift_days: int = 3,
) -> dict:
    """
    Returns the last run values for calculating percent threshold
    """
    metrics = []
    try:
        connection = get_postgres_connection(config)
        asset_query = f" and met.asset_id='{asset_id}'" if asset_id else ""
        with connection.cursor() as cursor:
            if attribute_id:
                query_string = f"""
                    select met.percent_change as pct_change from core.metrics met
                    where met.attribute_id = '{attribute_id}'
                    and met.measure_id = '{measure_id}' {asset_query}
                    order by created_date desc
                    limit {drift_days};
                """
            else:
                query_string = f"""
                    select met.percent_change as pct_change from core.metrics met
                    where met.measure_id = '{measure_id}' {asset_query}
                    order by created_date desc
                    limit {drift_days};
                """
            cursor = execute_query(connection, cursor, query_string)
            metrics = fetchall(cursor)
        return metrics
    except Exception as e:
        raise e


def cleanup_metrics(metric_values: list) -> list:
    """Cleans up metric values and returns only numerical values."""
    cleaned_metrics = []
    for metrics_group in metric_values:
        if metrics_group is None:
            cleaned_metrics.append([])  # Handle None values gracefully
            continue
        try:
            cleaned_metrics.append(
                [
                    float(metric)
                    for metric in metrics_group
                    if metric is not None and str(metric).strip() != ""
                ]
            )
        except (ValueError, TypeError) as e:
            # Skip or log invalid metric group if conversion fails
            cleaned_metrics.append([])
    log_info(("Cleaned metrics:", cleaned_metrics))
    return cleaned_metrics


def percent_change(prev_value: float, curr_value: float) -> float:
    """Get the percent change between previous value to current value"""
    curr_value = float(curr_value)
    prev_value = float(prev_value)
    if curr_value == prev_value:
        return 0
    try:
        return round(((curr_value - prev_value) / prev_value) * 100.0, 4)
    except ZeroDivisionError:
        return float(curr_value * 100.0)


def percent_model(metrics: list) -> tuple:
    """Calculate the upper and lower threshold for percent change"""
    metrics = [
        0 if np.isnan(value) else value for value in metrics
    ]  # handle nan values
    avg = np.average(metrics)
    std = stdev(metrics)
    threshold = [0, 0]
    if avg and std:
        threshold = [avg - std, avg + std]
    return threshold


def schema_change(
    config: dict,
    status: str,
    lower_threshold: float,
    upper_threshold: float,
    current_value: float,
):
    # custom alert for schema
    connection = get_postgres_connection(config)
    asset_id = config.get("asset_id")
    asset_query = f" and met.asset_id='{asset_id}'" if asset_id else ""
    with connection.cursor() as cursor:
        query_string = f"""
            select met.value as previous_value from core.metrics met
            where met.measure_name = 'column_count'
            and status = 'passed'
            {asset_query}
            order by met.created_date desc
            limit 1 offset 1;
            """
        cursor = execute_query(connection, cursor, query_string)
        metrics = fetchone(cursor)
    previous_value = int(metrics.get("previous_value")) if metrics else ""
    if previous_value:
        # status = ALERT_HIGH if status is not ALERT_OK else status
        lower_threshold = previous_value
        upper_threshold = previous_value
        if current_value == previous_value:
            status = ALERT_OK
        else:
            status = ALERT_HIGH

    return status, lower_threshold, upper_threshold


def get_current_run_value(config: dict, measure_id: str, run_id: str):
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select met.value from core.metrics met
                where met.measure_id = '{measure_id}'
                and met.status = 'passed' and met.run_id = '{run_id}'
                order by created_date desc
                limit 1
            """

            cursor = execute_query(connection, cursor, query_string)
            metrics = fetchone(cursor)
            return metrics
    except Exception as e:
        raise e


def get_previous_depth(config: dict, asset_id: str):
    try:
        previous_depth = None
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select ast.previous_depth from core.asset ast
                where ast.id = '{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            metrics = fetchone(cursor)
            if metrics:
                previous_depth = metrics.get("previous_depth", None)
                if previous_depth:
                    previous_depth = int(previous_depth)
            return previous_depth
    except Exception as e:
        raise e


def update_depth_value(config: dict, asset_id: str, depth_value: int):
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            if depth_value:
                query_string = f"""
                    update core.asset
                    set previous_depth = {depth_value}
                    where id = '{asset_id}'
                """
                execute_query(connection, cursor, query_string)
                log_info(("Updated the depth value for incremental load"))
    except Exception as e:
        raise e


def get_behavioral_numerical_aggregator(config: dict):
    measure = config.get("measure")
    base_measure_id = measure.get("base_measure_id")
    connection = get_postgres_connection(config)
    try:
        with connection.cursor() as cursor:
            query_string = f"""
            select lower(jsonb_array_elements(properties -> 'aggregator_attributes') ->> 'name') AS aggregator_name
            from
                core.base_measure
            where
                id = '{base_measure_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            aggregator = fetchone(cursor)
            aggregator_value = aggregator.get("aggregator_name") if aggregator else None
            return aggregator_value
    except Exception as e:
        raise e


def get_current_measure_dqscore(config: dict, run_id: str, measure_id: str) -> object:
    """Fetch the latest dqscore for the measure"""
    connection = get_postgres_connection(config)
    dqscore = 0  # default value
    try:
        with connection.cursor() as cursor:
            query_string = f"""
            select score as dqscore
            from core.metrics 
            where measure_id = '{measure_id}'
            and run_id = '{run_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            score_metrics = fetchone(cursor)
            dqscore = score_metrics.get("dqscore", 0) if score_metrics else None
            return dqscore
    except Exception as e:
        raise e

def compute_yhat_bounds(values:list, forecast:float, confidence=0.95, n_bootstrap=1000) -> float:
    """
    Compute yhat_lower and yhat_upper bounds using bootstrap residuals.

    Args:
        values (list or np.array): Historical values.
        forecast (float): Forecasted value (yhat).
        confidence (float): Confidence level for intervals.
        n_bootstrap (int): Number of bootstrap samples.

    Returns:
        Tuple (yhat_lower, yhat_upper)
    """
    # Step 1: Convert to numpy array and clean
    try:
        values = np.array(values)
        values = values[~np.isnan(values)]  # Remove NaNs

        # # Step 2: Optional: Winsorize or remove extreme outliers
        # q1, q3 = np.percentile(values, [25, 75])
        # iqr = q3 - q1
        # lower_bound = q1 - 1.5 * iqr
        # upper_bound = q3 + 1.5 * iqr
        # filtered_values = values[(values >= lower_bound) & (values <= upper_bound)]

        # Step 3: Calculate residuals vs forecast
        residuals = values - forecast

        # Step 4: Bootstrap sampling of residuals
        bootstrapped_forecasts = []
        for _ in range(n_bootstrap):
            sampled_residuals = np.random.choice(residuals, size=1)
            bootstrapped_forecast = forecast + sampled_residuals
            bootstrapped_forecasts.append(bootstrapped_forecast[0])

        # Step 5: Compute confidence bounds
        lower_percentile = ((1 - confidence) / 2) * 100
        log_info((f"Lower percentile: {lower_percentile}"))
        log_info((f"Bootstrapped forecasts: {bootstrapped_forecasts}"))
        yhat_lower = np.percentile(bootstrapped_forecasts, lower_percentile)
    except Exception as e:
        log_info((f"Error in computing yhat bounds: {e}"))
        yhat_lower = None

    yhat_lower = float(yhat_lower) if yhat_lower is not None else None

    return yhat_lower


def classify_list(values:list)-> str:
    # Filter out empty strings and non-numeric values
    filtered_values = [v for v in values if isinstance(v, (int, float))]

    if not filtered_values:
        return "List contains no numeric values"

    all_negative_including_zero = all(v <= 0 for v in filtered_values)
    all_positive_including_zero = all(v >= 0 for v in filtered_values)

    if all_negative_including_zero:
        return "negative"
    elif all_positive_including_zero:
        return "positive"
    else:
        return "mixed"


def is_seasonal_series(data, max_period=14, verbose=True):
    """
    Determines if a time series exhibits seasonality using multiple statistical methods.

    Applies three main checks:
    1. Autocorrelation Function (ACF) peak detection.
    2. Seasonal decomposition strength.
    3. Fast Fourier Transform (FFT) dominant frequency detection.

    If at least two of these checks indicate seasonality, the series is classified as seasonal.

    Args:
        data (list or np.ndarray): The time series data.
        max_period (int): The maximum period to check for seasonality.
        verbose (bool): If True, prints diagnostic information.

    Returns:
        bool: True if the series is classified as seasonal, False otherwise.
    """
    data = np.asarray(data)
    n = len(data)
    score = 0

    if n < 3:
        if verbose:
            print("Series too short to evaluate seasonality.")
        return False

    if np.all(data == data[0]):
        if verbose:
            print("Series is constant; skipping seasonality detection.")
        return False

    # --- ACF-based check ---
    try:
        acf_vals = acf(data, nlags=min(n // 2, max_period), fft=True)
        acf_peaks = [
            lag
            for lag in range(1, len(acf_vals) - 1)
            if acf_vals[lag] > 0.3
            and acf_vals[lag] > acf_vals[lag - 1]
            and acf_vals[lag] > acf_vals[lag + 1]
        ]
        if acf_peaks:
            score += 1
            if verbose:
                print(f"ACF peaks detected at lags: {acf_peaks}")
    except Exception as e:
        if verbose:
            print(f"ACF check failed: {e}")

    # --- Seasonal decomposition strength check ---
    for period in range(2, max_period):
        try:
            decomp = seasonal_decompose(data, period=period, extrapolate_trend="freq")
            strength = np.std(decomp.seasonal) / np.std(data)
            if strength > 0.1:
                score += 1
                if verbose:
                    print(
                        f"Decomposition suggests seasonality at period {period}, strength={strength:.2f}"
                    )
                break
        except Exception:
            continue

    # --- FFT peak detection ---
    try:
        freqs = fftfreq(n)
        fft_vals = fft(data)
        power = np.abs(fft_vals) ** 2
        power[0] = 0  # Remove DC component

        half_power = power[: n // 2]
        if len(half_power) > 0 and np.any(half_power > 0):
            dominant_freq = freqs[np.argmax(half_power)]
            if dominant_freq > 0:
                period = int(round(1 / dominant_freq))
                if 2 <= period <= max_period:
                    score += 1
                    if verbose:
                        print(f"FFT detected dominant period: {period}")
        else:
            if verbose:
                print("FFT check skipped due to flat or constant signal")
    except Exception as e:
        if verbose:
            print(f"FFT check failed: {e}")

    # --- Final decision ---
    is_seasonal = score >= 2
    if verbose:
        print(
            f"Final Seasonality Score: {score} -> {'Seasonal' if is_seasonal else 'Non-seasonal'}"
        )

    return is_seasonal
