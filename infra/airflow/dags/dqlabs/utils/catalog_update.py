"""
    Migration Notes From V2 to V3:
    Pending:True
    Need to validate URLS
"""

import json
import base64
import re
import requests
from uuid import uuid4
from datetime import datetime, timedelta
from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone
from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.app_helper.crypto_helper import decrypt
from dqlabs.app_constants.dq_constants import collibra_dq_score_type_id
from dqlabs.app_helper import agent_helper
from dqlabs.app_constants.dq_constants import DEFAULT_SEMANTIC_THRESHOLD


def get_measures(config: dict):
    """
    Get measure for metrics
    """
    asset_id = config.get("asset_id")
    attribute_id = config.get("attribute_id")
    measure_id = config.get("measure_id")
    measures = []
    if asset_id:
        condition = f"measure.asset_id='{asset_id}'"
    elif attribute_id:
        condition = f"measure.attribute_id='{attribute_id}'"
    elif measure_id:
        condition = f"measure.id='{measure_id}'"
    else:
        return measures

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            select 
                measure.id,
                measure.attribute_id,
                base_measure.name,
                metrics.score,
                metrics.total_count, 
                measure.result,
                measure.enable_pass_criteria,
                measure.pass_criteria_threshold,                
                metrics.valid_count,
                metrics.invalid_count,TO_CHAR(metrics.created_date, 'mm/dd/yyyy') as last_update_date,
                metrics.created_date as execution_time,dimension.name as dimension_name,
                case when measure.drift_threshold->>'lower_threshold' ~ '[0-9]' then cast(measure.drift_threshold->>'lower_threshold' as float) else 0 end as lower_threshold,
                case when measure.drift_threshold->>'upper_threshold' ~ '[0-9]' then cast(measure.drift_threshold->>'upper_threshold' as float) else 0 end as upper_threshold
            from core.measure
            join core.base_measure on base_measure.id=measure.base_measure_id and base_measure.type='custom'
            left join core.asset on asset.id=measure.asset_id and asset.is_active=true
            left join core.metrics on metrics.run_id=asset.last_run_id and metrics.measure_id=measure.id
			left join core.dimension on dimension.id=measure.dimension_id
            where measure.allow_score=true and {condition}
        """
        cursor = execute_query(connection, cursor, query_string)
        measures = fetchall(cursor)
    return measures

def get_colibra_push_data(channel_config, type: str = ""):
    """
    Get Collibra push data
    """
    push_data = channel_config.get("push", [])
    push_obj = next(item for item in push_data if type in item) if push_data else {}
    push = push_obj.get(type, False)
    score = push_obj.get("options", {}).get("score", False)
    return push, score

def check_vault_enabled(channel_config):
    """
    Check Vault enabled
    """
    is_vault_enabled  = channel_config.get("is_vault_enabled", False)
    vault_key = channel_config.get("vault_key", "")
    if is_vault_enabled and vault_key:
        return True
    return False

def extract_vault_credentials(config: dict, channel_config: dict):
    """
    Extract vault credentials
    """
    connection = get_postgres_connection(config)
    response = agent_helper.get_vault_data(
        config,
        connection,
        channel_config
    )
    if response:
        channel_config.update(**response)
    return channel_config
    

def get_collibra_asset_details(config, channel_config):
    """
    get asset details
    """
    asset_id = config.get("asset_id")
    measure_id = config.get("measure_id")
    push_assets, score = get_colibra_push_data(channel_config, "asset")
    assets = {}
    if not push_assets or (push_assets and not score) or (not asset_id and measure_id):
        return assets

    asset_condition = ''
    if asset_id:
        asset_condition = f"and asset.id='{asset_id}'"
    
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            with field_data as (
                select * from core.fields f 
                join core.field_property fp 
                on f.id=fp.field_id and level ='Asset'
                where lower(f.name)='collibra asset id'
            )
			select asset.id, asset.name, asset.technical_name, asset.score, asset.properties, 
            coalesce(fd.value, null) as collibra_asset_id 
            from core.asset 
			left join field_data as fd on fd.asset_id = asset.id
            left join core.connection on connection.id=asset.connection_id
			where asset.is_active = true and asset.is_delete = false and connection.is_active = true 
            and connection.is_delete = false and asset.group='data' and asset.score is not null
            {asset_condition}
        """
        cursor = execute_query(connection, cursor, query_string)
        assets = fetchall(cursor)
    return assets

def get_collibra_attributes_details(config, channel_config):
    """
    get attributes details for collibra
    """
    asset_id = config.get("asset_id")
    attribute_id = config.get("attribute_id")
    measure_id = config.get("measure_id")
    push_attributes, attribute_score = get_colibra_push_data(channel_config, "attribute")
    attributes = []
    if not push_attributes or (push_attributes and not attribute_score) or (not asset_id and not attribute_id and measure_id):
        return attributes
    
    attribute_condition = ''
    if asset_id:
        attribute_condition = f"and attribute.asset_id='{asset_id}'"
    elif attribute_id:
        attribute_condition = f"and attribute.id='{attribute_id}'"

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            with field_data as (
                select * from core.fields f 
                join core.field_property fp 
                on f.id=fp.field_id and level ='Attribute'
                where lower(f.name)='collibra asset id'
            )
			select attribute.id, attribute.name, attribute.status, attribute.score, asset.properties,
            asset.name as table_name, coalesce(fd.value, null) as collibra_asset_id 
            from core.attribute 
			left join field_data as fd on fd.attribute_id = attribute.id
			left join core.asset on asset.id=attribute.asset_id
            left join core.connection on connection.id=asset.connection_id
			where asset.is_active = true and asset.is_delete = false and connection.is_active = true 
            and connection.is_delete = false and attribute.is_selected = true
            and asset.group='data' and attribute.score is not null
            {attribute_condition}
        """
        cursor = execute_query(connection, cursor, query_string)
        attributes = fetchall(cursor)
    return attributes


def get_collibra_measures(config: dict, channel_config: dict):
    """
    Check Collibra measures 
    """
    asset_id = config.get("asset_id")
    attribute_id = config.get("attribute_id")
    measure_id = config.get("measure_id")
    measures = []
    push_data = channel_config.get("push", [])
    measure_condition = ""

    push_measures_obj = next(item for item in push_data if "measure" in item) if push_data else {}
    push_measures = push_measures_obj.get("measure", False)
    if push_measures:
        oob_measures = push_measures_obj.get("options", {}).get("oob_measures", False)
        custom_measures = push_measures_obj.get("options", {}).get("custom_measures", False)
        semantic_measures = push_measures_obj.get("options", {}).get("semantic_measures", False)
        if not custom_measures and not semantic_measures and not oob_measures:
            return measures
        else:
            filter_query = []
            if custom_measures:
                filter_query.append("custom")
            if semantic_measures:
                filter_query.append("semantic")
            if oob_measures:
                filter_query.extend(["distribution", "frequency", "observe", "reliability", "statistics"])
            if not filter_query:
                measure_condition = f""""""
            else:
                filter_query = [f"'{i}'" for i in filter_query]
                measure_condition = f"""and base_measure.type in ({",".join(filter_query)})"""
    else:
        return measures
    
    if asset_id:
        condition = f" and measure.asset_id='{asset_id}'"
    elif attribute_id:
        condition = f" and measure.attribute_id='{attribute_id}'"
    elif measure_id:
        condition = f" and measure.id='{measure_id}'"
    else:
        condition = ''

    connection = get_postgres_connection(config)
    if measure_condition:
        with connection.cursor() as cursor:
            query_string = f"""
                with field_data as (
                    select * from core.fields f 
                    join core.field_property fp 
                    on f.id=fp.field_id
                )
                select 
                    measure.id,
                    measure.attribute_id,
                    measure.asset_id,
                    base_measure.name,
                    base_measure.description as measure_description,
                    metrics.query as measure_query,
                    metrics.score,
                    metrics.total_count, 
                    measure.result,
                    measure.enable_pass_criteria,
                    measure.pass_criteria_threshold,
                    metrics.valid_count,
                    metrics.invalid_count,TO_CHAR(metrics.created_date, 'mm/dd/yyyy') as last_update_date,
                    metrics.created_date as execution_time,dimension.name as dimension_name,
                    case when measure.drift_threshold->>'lower_threshold' ~ '[0-9]' then cast(measure.drift_threshold->>'lower_threshold' as float) else 0 end as lower_threshold,
                    case when measure.drift_threshold->>'upper_threshold' ~ '[0-9]' then cast(measure.drift_threshold->>'upper_threshold' as float) else 0 end as upper_threshold,
                    case when measure.asset_id is not null and measure.attribute_id is not null then 'attribute' when measure.asset_id is not null then 'asset' else 'measure' end as measure_level,
                    asset.name as asset_name,
                    attribute.name as attribute_name,
                    asset.properties ->> 'schema' as schema,
                    asset.properties ->> 'database' as database,
                    coalesce(fd.value, null) as collibra_column_id,
                    coalesce(fd1.value, null) as collibra_table_id,
                    coalesce(fd2.value, null) as collibra_measure_id
                from core.measure
                join core.base_measure on base_measure.id=measure.base_measure_id {measure_condition}
                join core.metrics on metrics.measure_id=measure.id and metrics.run_id=measure.last_run_id
                left join core.asset on asset.id=measure.asset_id and asset.is_active=true
                left join core.attribute on attribute.id=measure.attribute_id and attribute.is_active=true
                left join core.dimension on dimension.id=measure.dimension_id
                left join field_data fd on (measure.attribute_id = fd.attribute_id and fd.level='Attribute' and lower(fd.name)='collibra asset id')
                left join field_data fd1 on (measure.asset_id = fd1.asset_id and fd1.level='Asset' and lower(fd1.name)='collibra asset id') 
                left join field_data fd2 on (measure.id = fd2.measure_id and fd2.level='Measure' and lower(fd2.name)='collibra asset id') 
                where measure.allow_score=true and measure.is_active is true {condition}
                """
            cursor = execute_query(connection, cursor, query_string)
            measures = fetchall(cursor)
    return measures

def hitachi_catalog_update(config: dict, measures: list = []):
    try:
        for measure in measures:
            measure_name = measure.get("name")
            configuration = {
                **config,
                "measure_name": measure_name
            }
            rule = find_hitachi_quality_statistics(configuration)
            if rule:
                configuration.update({"id": rule.get("id")})
                dimension_name = measure.get("dimension_name")
                lower_threshold = measure.get("lower_threshold") if measure.get(
                    "lower_threshold") <= 100 else 100
                high_threshold = measure.get("upper_threshold") if measure.get(
                    "lower_threshold") <= 100 else 100
                params = {
                    "qualityDimensionName": dimension_name if dimension_name else rule.get("qualityDimensionName"),
                    "totalRows": measure.get("total_count"),
                    "qualityValue": measure.get("valid_count"),
                    "nonCompliantRows": measure.get("invalid_count"),
                    "executionTime": measure.get("execution_time"),
                    "lowThresholdValue": lower_threshold,
                    "highThresholdValue": high_threshold
                }
                update_hitachi_quality_statistics(configuration, params)
    except Exception as e:
            log_error("Hitachi Catalog Update Failed", str(e))
            raise e

def collibra_catalog_update(config: dict, channel_config: dict, measures: list = [], assets: list = [], attributes: list = []):
    mapping = channel_config.get("mapping", [])
    communities = channel_config.get("communities", [])
    community_ids = [item.get('id') for item in communities] if communities else []

    pull_domains = channel_config.get("pull_domains", False)
    pull_terms = channel_config.get("pull_terms", False)
    pull_data_concepts = channel_config.get("pull_data_concepts", False)

    domains = channel_config.get("domains", [])
    domain_ids = [item.get('id') for item in domains] if domains else []

    data_concept_domains = channel_config.get("data_concept_domains", [])
    data_concept_domain_ids = [item.get('id') for item in data_concept_domains] if data_concept_domains else []

    rule_prefix = 'Rule'
    metric_prefix = 'Result'

    if not domain_ids and community_ids:
        domains_by_community = []
        for id in community_ids:
            response = get_domains_by_community(channel_config, id)
            domains_by_community = domains_by_community + response
        domain_ids = domains_by_community

    catalog_measures = [
        data for data in mapping if data.get("type") == "measure" and data.get('push_pull') == True]
    catalog_attributes = [
        data for data in mapping if data.get("type") == "attribute" and data.get('push_pull') == True]
    # collibra_attribute_types = get_all_attribute_types(channel_config)

    term_mapped_dq_columns = []
    domain_mapped_dq_tables = []

    try:
        if assets:
            for asset in assets:
                collibra_id = asset.get("collibra_asset_id", None)
                log_info(("Processing DQ Score for Asset: ", collibra_id))
                if collibra_id:
                    _, collibra_asset = check_collibra_asset_exists(channel_config, collibra_id, 'asset')
                    log_info(("Identified Collibra Asset: ", collibra_asset))
                    if collibra_asset and domain_ids:
                        collibra_asset = collibra_asset if collibra_asset.get('domain').get('id') in domain_ids and collibra_asset.get('domain').get('resourceType') == 'Domain' else None

                    attribute = get_catalog_attribute(
                        {**channel_config, "asset_id": collibra_id, "collibra_type_id": collibra_dq_score_type_id})
                    attribute_id = attribute.get("id") if attribute else None
                    value = asset.get('score', 0)
                    if attribute_id:
                        update_catalog(
                            {**channel_config, "attribute_id": attribute_id, "value": value})
                    else:
                        create_attribute({**channel_config, "collibra_type_id": collibra_dq_score_type_id,
                                        "asset_id": collibra_id, "value": value})
                else:
                    properties = asset.get("properties", {})
                    database = properties.get("database", '')
                    schema = properties.get('schema', '')
                    dq_table_name = asset.get('name', '')
                    response = get_collibra_assets(channel_config, domain_ids, database, 'database') if database else []
                    log_info(("Identified Collibra Databases for score: ", response))
                    if len(response)>0:
                        for database_asset in response:
                            table = get_table_by_db(channel_config, dq_table_name, database_asset)
                            log_info(("Identified Collibra tables by Database for score: ", table))
                            if table:
                                break
                            schema = get_schema_by_db(channel_config, schema, database_asset)
                            log_info(("Identified Collibra schema by Database for score: ", schema))
                            if schema:
                                table = get_table_by_schema(channel_config, dq_table_name, schema)
                                log_info(("Identified Collibra tables by Schema for score: ", table))
                                if table:
                                    break
                        if table:
                            collibra_id = table.get('collibra_id', '')
                            attribute = get_catalog_attribute(
                                {**channel_config, "asset_id": collibra_id, "collibra_type_id": collibra_dq_score_type_id})
                            attribute_id = attribute.get("id") if attribute else None
                            value = asset.get('score', 0)
                            if attribute_id:
                                update_catalog(
                                    {**channel_config, "attribute_id": attribute_id, "value": value})
                            else:
                                create_attribute({**channel_config, "collibra_type_id": collibra_dq_score_type_id,
                                                "asset_id": collibra_id, "value": value})
                        

        if attributes:
            for attribute in attributes:
                collibra_id = attribute.get("collibra_asset_id", None)
                log_info(("Processing DQ Score for Attribute: ", collibra_id))
                if collibra_id:
                    _, asset = check_collibra_asset_exists(channel_config, collibra_id, 'attribute')
                    log_info(("Identified Collibra Attribute: ", asset))
                    if asset and domain_ids:
                        asset = asset if asset.get('domain').get('id') in domain_ids and asset.get('domain').get('resourceType') == 'Domain' else None

                    collibra_attribute = get_catalog_attribute(
                        {**channel_config, "asset_id": collibra_id, "collibra_type_id": collibra_dq_score_type_id})
                    collibra_attribute_id = collibra_attribute.get("id") if collibra_attribute else None
                    value = attribute.get('score', 0)
                    if collibra_attribute_id:
                        update_catalog(
                            {**channel_config, "attribute_id": collibra_attribute_id, "value": value})
                    else:
                        create_attribute({**channel_config, "collibra_type_id": collibra_dq_score_type_id,
                                        "asset_id": collibra_id, "value": value})
                else:
                    properties = attribute.get("properties", {})
                    database = properties.get("database", '')
                    schema = properties.get('schema', '')
                    dq_table_name = attribute.get('table_name', '')
                    response = get_collibra_assets(channel_config, domain_ids, database, 'database') if database else []
                    log_info(("Identified Collibra Databases for score: ", response))
                    if len(response)>0:
                        for database_asset in response:
                            table = get_table_by_db(channel_config, dq_table_name, database_asset)
                            log_info(("Identified Collibra tables by Database for score: ", table))
                            if table:
                                break
                            schema = get_schema_by_db(channel_config, schema, database_asset)
                            log_info(("Identified Collibra schema by Database for score: ", schema))
                            if schema:
                                table = get_table_by_schema(channel_config, dq_table_name, schema)
                                log_info(("Identified Collibra tables by Schema for score: ", table))
                                if table:
                                    break
                        if table:
                            collibra_table_id = table.get('collibra_id', '')
                            dq_column_name = attribute.get("name")
                            asset_type = table.get('type', {}).get('name','')
                            column = get_collibra_columns(channel_config, collibra_table_id, dq_column_name, asset_type)
                            if column:
                                collibra_column_id = column.get("collibra_id")
                                collibra_attribute = get_catalog_attribute(
                                    {**channel_config, "asset_id": collibra_column_id, "collibra_type_id": collibra_dq_score_type_id})
                                collibra_attribute_id = collibra_attribute.get("id") if collibra_attribute else None
                                value = attribute.get('score', 0)
                                if collibra_attribute_id:
                                    update_catalog(
                                        {**channel_config, "attribute_id": collibra_attribute_id, "value": value})
                                else:
                                    create_attribute({**channel_config, "collibra_type_id": collibra_dq_score_type_id,
                                                    "asset_id": collibra_column_id, "value": value})

        for measure in measures:
            collibra_measure_id = measure.get("collibra_measure_id", None)
            collibra_column_id = measure.get("collibra_column_id", None)
            collibra_table_id = measure.get("collibra_table_id", None)

            dq_attribute_id = measure.get("attribute_id", None)
            dq_asset_id = measure.get("asset_id", None)
            dq_measure_id = measure.get("id", None)

            dq_column_name = measure.get("attribute_name", '')
            dq_table_name = measure.get("asset_name", '')
            dq_measure_name = measure.get("name", '')

            measure_level = measure.get("measure_level", '')

            if measure_level == 'attribute':
                collibra_rule_name = dq_table_name + ' | ' + dq_column_name + ' | ' + dq_measure_name + ' ' + rule_prefix
                collibra_metric_name = dq_table_name + ' | ' + dq_column_name + ' | ' + dq_measure_name + ' ' + metric_prefix
            elif measure_level == 'asset':
                collibra_rule_name = dq_table_name + ' | ' + dq_measure_name + ' ' + rule_prefix
                collibra_metric_name = dq_table_name + ' | ' + dq_measure_name + ' ' + metric_prefix
            else:
                collibra_rule_name = dq_measure_name + ' ' + rule_prefix
                collibra_metric_name = dq_measure_name + ' ' + metric_prefix
            
            collibra_rule_display_name = dq_measure_name
            collibra_metric_display_name = dq_measure_name

            if collibra_measure_id:
                asset_exists, asset = check_collibra_asset_exists(channel_config, collibra_measure_id, 'measure')
                log_info((f"Identified Collibra Measure for {collibra_measure_id}: ", asset))
                collibra_rule_id = get_rule_id_from_measure(channel_config, collibra_measure_id)
                if asset:
                    # Run Push Metrics based on the active tag
                    for catalog_measure in catalog_measures:
                        push_metrics_to_collibra(channel_config, catalog_measure, collibra_measure_id, collibra_rule_id, measure)
                collibra_table_id = get_collibra_table_by_measure(channel_config, collibra_measure_id)
            elif collibra_column_id:
                asset_exists, column = check_collibra_asset_exists(channel_config, collibra_column_id, 'attribute')
                log_info((f"Identified Collibra Column for {collibra_column_id}: ", column))
                if asset_exists:
                    domain_id = column.get('domain', {}).get('id', '')
                    collibra_rule = get_rule_by_column(channel_config, collibra_column_id, collibra_rule_name, domain_id, collibra_rule_display_name)
                    collibra_rule_id = collibra_rule.get("id", '') if collibra_rule else None
                    collibra_metric = get_collibra_metrics_by_dq_rule(channel_config, collibra_rule, collibra_metric_name, domain_id, collibra_metric_display_name) if collibra_rule else None
                    if collibra_metric:
                        collibra_measure_id = collibra_metric.get("id", '')
                        for catalog_measure in catalog_measures:
                            push_metrics_to_collibra(channel_config, catalog_measure, collibra_measure_id, collibra_rule_id, measure)
                collibra_table_id = get_collibra_table_by_column(channel_config, collibra_column_id)
            elif collibra_table_id and not measure_level == 'asset':
                asset_exists, asset = check_collibra_asset_exists(channel_config, collibra_table_id, 'asset')
                log_info((f"Identified Collibra Asset for {collibra_table_id}: ", asset))
                if not asset_exists:
                    continue
                domain_id = asset.get('domain', {}).get('id', '')
                asset_type = asset.get('type', {}).get('name','')
                column = get_collibra_columns(channel_config, collibra_table_id, dq_column_name, asset_type)
                log_info((f"Identified Collibra Column for {dq_column_name}: ", column))
                if column:
                    collibra_column_id = column.get("collibra_id")
                    collibra_rule = get_rule_by_column(channel_config, collibra_column_id, collibra_rule_name, domain_id, collibra_rule_display_name)
                    collibra_rule_id = collibra_rule.get("id", '') if collibra_rule else None
                    collibra_metric = get_collibra_metrics_by_dq_rule(channel_config, collibra_rule, collibra_metric_name, domain_id, collibra_metric_display_name) if collibra_rule else None
                    if collibra_metric:
                        collibra_measure_id = collibra_metric.get("id", '')
                        for catalog_measure in catalog_measures:
                            push_metrics_to_collibra(channel_config, catalog_measure, collibra_measure_id, collibra_rule_id, measure)
            elif measure_level not in ('measure', 'asset'):
                database = measure.get("database", '')
                schema = measure.get('schema', '')
                response = get_collibra_assets(channel_config, domain_ids, database, 'database') if database else []
                log_info(("Identified Collibra Databases for metric push: ", response))
                if len(response)>0:
                    for database_asset in response:
                        table = get_table_by_db(channel_config, dq_table_name, database_asset)
                        log_info(("Identified Collibra tables by Database for metric push: ", table))
                        if table:
                            break
                        schema = get_schema_by_db(channel_config, schema, database_asset)
                        log_info(("Identified Collibra schema by Database for metric push: ", schema))
                        if schema:
                            table = get_table_by_schema(channel_config, dq_table_name, schema)
                            log_info(("Identified Collibra tables by Schema for metric push: ", table))
                            if table:
                                break
                    
                    if table:
                        collibra_table_id = table.get('collibra_id', '')
                        dq_column_name = measure.get("attribute_name")
                        asset_type = table.get('type', {}).get('name','')
                        column = get_collibra_columns(channel_config, collibra_table_id, dq_column_name, asset_type)
                        log_info(("Identified Collibra Column for metric push: ", column))
                        if column:
                            collibra_column_id = column.get("collibra_id")
                            column_exists, column = check_collibra_asset_exists(channel_config, collibra_column_id, 'attribute')
                            log_info((f"Identified Collibra Column for {collibra_column_id}: ", column))
                            if column and column_exists:
                                collibra_column_id = column.get("id")
                                domain_id = column.get('domain', {}).get('id', '')
                                collibra_rule = get_rule_by_column(channel_config, collibra_column_id, collibra_rule_name, domain_id, collibra_rule_display_name)
                                collibra_rule_id = collibra_rule.get("id", '') if collibra_rule else None
                                log_info((f"Identified Collibra Rule for {collibra_rule_name}: ", collibra_rule))
                                collibra_metric = get_collibra_metrics_by_dq_rule(channel_config, collibra_rule, collibra_metric_name, domain_id, collibra_metric_display_name) if collibra_rule else None
                                log_info((f"Identified Collibra Metric for {collibra_metric_name}: ", collibra_metric))
                                if collibra_metric:
                                    collibra_measure_id = collibra_metric.get("id", '')
                                    for catalog_measure in catalog_measures:
                                        if catalog_measure.get("push_pull", False):
                                            push_metrics_to_collibra(channel_config, catalog_measure, collibra_measure_id, collibra_rule_id, measure)
            if collibra_measure_id:
                handle_measure_dimension_tagging(config, channel_config, measure, collibra_measure_id)

            if collibra_column_id and pull_terms:
                if collibra_column_id not in term_mapped_dq_columns:
                    if dq_attribute_id:
                        unmap_collibra_terms(config, dq_attribute_id, dq_asset_id)
                    collibra_business_terms = get_collibra_column_terms(channel_config, collibra_column_id)
                    if collibra_business_terms:
                        for term in collibra_business_terms:
                            term_asset = get_collibra_asset(channel_config, term.get('collibra_id'))
                            domain_name = term_asset.get('domain', {}).get('name', '')
                            domain = insert_collibra_domain(config, domain_name)
                            domain_id = domain.get('id', None)
                            if domain_id:
                                term_id = insert_collibra_term(config, channel_config, term, domain)
                                map_attribute_to_term(config, term_id, dq_attribute_id, dq_asset_id)
                    term_mapped_dq_columns.append(collibra_column_id)
            
            if collibra_table_id and pull_domains:
                if collibra_table_id not in domain_mapped_dq_tables:
                    asset_exists, collibra_table = check_collibra_asset_exists(channel_config, collibra_table_id, 'asset')
                    if asset_exists and collibra_table:
                        domain = collibra_table.get('domain', {}).get('name', '')
                        domain = insert_collibra_domain(config, domain)
                        domain_id = domain.get('id', None)
                        map_collibra_domain_to_asset(config, domain_id, dq_asset_id, dq_measure_id)
                    domain_mapped_dq_tables.append(collibra_table_id)

            if collibra_measure_id:
                for attribute in catalog_attributes:
                    collibra_type_id = attribute.get("collibra_type_id")
                    push_pull = attribute.get('push_pull', False)
                    if push_pull:
                        catalog_attribute = get_catalog_attribute(
                            {**channel_config, "asset_id": collibra_measure_id, "collibra_type_id": collibra_type_id})
                        collibra_value = catalog_attribute.get("value") if catalog_attribute else None
                        if collibra_value:
                            collibra_value = collibra_value[0] if isinstance(collibra_value, list) else collibra_value
                            field_id = attribute.get('dq_field_id', '')
                            if field_id:
                                update_custom_field_value(config, dq_measure_id, field_id, collibra_value)
            
        if pull_data_concepts:
        # Get all data concept type assets from each domain in domain_ids
            try:
                    # Fetch all assets of type "DataConcept" for the domain from Collibra
                data_concept_assets = get_collibra_assets(channel_config, data_concept_domain_ids, '', 'data concept')
                data_concept_assets = data_concept_assets if data_concept_assets else []

                # Insert each data concept asset as a term under the respective domain if not already present
                for data_concept in data_concept_assets:
                    term_name = data_concept.get("name")
                    if not term_name:
                        continue
                    # Use get domain name from Collibra asset
                    domain_name = None
                    # Try to extract domain name from the asset if possible
                    domain_info = data_concept.get("domain", {})
                    domain_name = domain_info.get("name", "")

                    # If not available, try to fetch the domain asset
                    if not domain_name:
                        # Optionally fetch domain info here if API provides it
                        pass

                    # Insert domain to DQLabs if not present, just in case (idempotent)
                    dqlabs_domain = insert_collibra_domain(config, domain_name)
                    dqlabs_domain_id = dqlabs_domain.get("id") if dqlabs_domain else None

                    if not dqlabs_domain_id:
                        continue

                    # Create term if not already present
                    insert_collibra_term(config, channel_config, data_concept, dqlabs_domain)
            except Exception as e:
                log_error("Data Concept Terms Creation Failed", str(e))

        # clean_collibra_domain_and_terms(config)
    except Exception as e:
        raise e

def get_collibra_table_by_column(config, column_id):
    """
    Get collibra Table/View asset for the column
    """
    try:
        url = format_url(config.get("url"))
        url_params = {
            "relationTypeId": "00000000-0000-0000-0000-000000007042",
            "sourceId": column_id
        }
        query_string = prepare_query_params(url_params)
        url = f"{url}/relations?{query_string}"
        response = make_call(config, url, "get")
        response = [{"name": item.get("target", {}).get("name", ""), "collibra_id": item.get("target", {}).get("id")} for item in response] if response else []
        if len(response)>0:
            return response[0].get('collibra_id','')
        return None
    except Exception as e:
        log_error(f"Error getting Collibra table by column {column_id}:", str(e))
        raise e

def get_collibra_table_by_measure(config, measure_id):
    """
    Get collibra Table/View asset for the measure
    """
    try:
        url = format_url(config.get("url"))
        url_params = {
            "relationTypeId": "00000000-0000-0000-0000-000000007016",
            "targetId": measure_id
        }
        query_string = prepare_query_params(url_params)
        url = f"{url}/relations?{query_string}"
        response = make_call(config, url, "get")
        response = response if response else []
        if len(response) > 0:
            rule_id = response[0].get("source", {}).get("id", '')
            url_params = {
                "sourceId": rule_id,
                "typePublicId": "DataQualityRuleGovernsColumn"
            }
            query_string = prepare_query_params(url_params)
            url = f"{url}/relations?{query_string}"
            response = make_call(config, url, "get")
            response = response if response else []
            if len(response) > 0:
                column_id = response[0].get("target", {}).get("id", '')
                table_id = get_collibra_table_by_column(config, column_id)
                return table_id
        return None
    except Exception as e:
        log_error(f"Error getting Collibra table by measure {measure_id}:", str(e))
        raise e

def check_collibra_asset_exists(config: dict, id, type):
    """
    Check if asset exists in Collibra
    """
    try:
        if type == 'measure':
            collibra_type = ['Data Quality Metric']
        elif type == 'asset':
            collibra_type = ['Table', 'Database View', 'Data Set']
        elif type == 'attribute':
            collibra_type = ['Column']
        is_exists = False
        url = format_url(config.get("url"))
        url = f"{url}/assets/{id}"
        response = make_call(config, url, "get")
        if response:
            if response.get('type', {}).get('name','') in collibra_type:
                is_exists = True
        return is_exists, response
    except Exception as e:
        log_error(f"Error checking Collibra asset {id} of type {type}:", str(e))
        raise e

def get_collibra_assets(config: dict, domain_ids, name, type):
    """
    Check if asset exists in Collibra
    """
    try:
        if type == 'database':
            collibra_type = ['Database']
            collibra_type_ids = ['00000000-0000-0000-0000-000000031006']
        elif type == 'table':
            collibra_type = ['Table', 'Database View', 'Data Set']
            collibra_type_ids = ['00000000-0000-0000-0000-000000031007', '00000000-0000-0000-0001-000400000009', '00000000-0000-0000-0001-000400000001']
        elif type == 'schema':
            collibra_type = ['Schema']
            collibra_type_ids = ['00000000-0000-0000-0001-000400000002']
        elif type == 'data concept':
            collibra_type = ['Data Concept']
            collibra_type_ids = ['00000000-0000-0000-0000-000000031113']
        url = format_url(config.get("url"))
        url_params = {
            "name": name if name else "",
            "typeIds": collibra_type_ids,
            "excludeMeta": True,
            "limit": 5000
        }
        query_string = prepare_query_params(url_params)
        url = f"{url}/assets?{query_string}"
        response = make_call(config, url, "get")
        response = [
            item
            for item in response if response
            if item['type']['name'] in collibra_type
            and (clean_collibra_hierarchial_name(item.get('name', '')) == name if name else True)
        ] if response else []
        response = [item for item in response if item['domain']['id'] in domain_ids] if response and domain_ids else response
        return response
    except Exception as e:
        log_error(f"Error getting Collibra assets for {name} of type {type}:", str(e))
        raise e

def get_table_by_db(config, name, database):
    """
    Get collibra Table/View asset for the database
    """
    try:
        database_id = database.get('id', '')
        url = format_url(config.get("url"))
        url_params = {
            "typePublicId": "TableIsPartOfDatabase",
            "targetId": database_id
        }
        query_string = prepare_query_params(url_params)
        url = f"{url}/relations?{query_string}"
        response = make_call(config, url, "get")
        response = [
            item
            for item in response if response
            if clean_collibra_hierarchial_name(item['source']['name']) == name
        ] if response else []
        response = [{"name": clean_collibra_hierarchial_name(item.get("source", {}).get("name", "")), "collibra_id": item.get("source", {}).get("id")} for item in response] if response else []
        if len(response)>0:
            return response[0]
        return None
    except Exception as e:
        log_error(f"Error getting Collibra table by database {name}:", str(e))
        raise e

def get_table_by_schema(config, name, schema):
    """
    Get collibra Table/View asset for the schema
    """
    try:
        schema_id = schema.get('collibra_id', '')
        url = format_url(config.get("url"))
        url_params = {
            "typePublicId": "SchemaContainsTable",
            "sourceId": schema_id
        }
        query_string = prepare_query_params(url_params)
        url = f"{url}/relations?{query_string}"
        response = make_call(config, url, "get")
        response = [
            item
            for item in response if response
            if clean_collibra_hierarchial_name(item['target']['name']) == name
        ] if response else []
        response = [{"name": clean_collibra_hierarchial_name(item.get("target", {}).get("name", "")), "collibra_id": item.get("target", {}).get("id")} for item in response] if response else []
        if len(response)>0:
            return response[0]
        return None
    except Exception as e:
        log_error(f"Error getting Collibra table by schema {name}:", str(e))
        raise e

def get_schema_by_db(config, name, database):
    """
    Get collibra Schema asset for the database
    """
    try:
        database_id = database.get('id', '')
        url = format_url(config.get("url"))
        url_params = {
            "typePublicId": "TechnologyAssetHasSchema",
            "sourceId": database_id
        }
        query_string = prepare_query_params(url_params)
        url = f"{url}/relations?{query_string}"
        response = make_call(config, url, "get")
        response = [
            item
            for item in response if response
            if clean_collibra_hierarchial_name(item['target']['name']) == name
        ] if response else []
        response = [{"name": clean_collibra_hierarchial_name(item.get("target", {}).get("name", "")), "collibra_id": item.get("target", {}).get("id")} for item in response] if response else []
        if len(response)>0:
            return response[0]
        return None
    except Exception as e:
        log_error(f"Error getting Collibra schema by database {name}:", str(e))
        raise e
            
def get_collibra_columns(config: dict, collibra_asset_id, column_name, asset_type=None):
    """
    Get all the columns related to the Table/View asset
    """
    try:
        url = format_url(config.get("url"))
        url_params = {
            "relationTypeId": "00000000-0000-0000-0000-000000007042",
            "targetId": collibra_asset_id
        }
        if asset_type and asset_type.lower() == 'data set':
            url_params = {
                "relationTypeId": "00000000-0000-0000-0000-000000007062",
                "sourceId": collibra_asset_id
            }
        query_string = prepare_query_params(url_params)
        url = f"{url}/relations?{query_string}"
        response = make_call(config, url, "get")
        if asset_type and asset_type.lower() == 'data set':
            response = [
                item
                for item in response if response
                if clean_collibra_hierarchial_name(item['target']['name']) == column_name
            ] if response else []
            response = [{"name": clean_collibra_hierarchial_name(item.get("target", {}).get("name", "")), "collibra_id": item.get("target", {}).get("id")} for item in response] if response else []
        else:
            response = [
                item
                for item in response if response
                if clean_collibra_hierarchial_name(item['source']['name']) == column_name
            ] if response else []
            response = [{"name": clean_collibra_hierarchial_name(item.get("source", {}).get("name", "")), "collibra_id": item.get("source", {}).get("id")} for item in response] if response else []
        if len(response)>0:
            return response[0]
        return None
    except Exception as e:
        log_error(f"Error getting Collibra columns for asset {collibra_asset_id} and column {column_name}:", str(e))
        raise e

def get_collibra_rules_by_column(config: dict, column_id):
    """
    Get the rule related to the column by name.
    """
    try:
        url = format_url(config.get("url"))
        url_params = {
            "typePublicId": "DataQualityRuleGovernsColumn",
            "targetId": column_id
        }
        query_string = prepare_query_params(url_params)
        url = f"{url}/relations?{query_string}"
        response = make_call(config, url, "get")
        rules = [{"name": rule.get("source", {}).get("name", ""), "collibra_id": response.get("source", {}).get("id", '')} for rule in response] if response else {}
        # rules = list(map(lambda x: {**x, 'name': x['name'].split("> ")[1] if "> " in x['name'] else x['name']}, rules))
        return rules
    except Exception as e:
        log_error(f"Error getting Collibra rules for column {column_id}:", str(e))
        raise e

def get_dq_column(config, dq_column_id):
    """
    Get the dq column by id
    """
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select id from core.attribute 
                where id={dq_column_id}'
            """
            cursor = execute_query(
                connection, cursor, query_string)
            dq_column = fetchone(cursor)
            return dq_column
    except Exception as e:
        log_error(f"Error getting DQ column {dq_column_id}:", str(e))
        raise e

def get_dq_attribute_measures(config, dq_column_id):
    """
    Get all dq column measures for metrics
    """
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            select 
                measure.id,
                measure.attribute_id,
                base_measure.name,
                metrics.score,
                metrics.total_count, 
                measure.result,
                measure.enable_pass_criteria,
                measure.pass_criteria_threshold,                
                metrics.valid_count,
                metrics.invalid_count,TO_CHAR(metrics.created_date, 'mm/dd/yyyy') as last_update_date,
                metrics.created_date as execution_time,dimension.name as dimension_name,
                case when measure.drift_threshold->>'lower_threshold' ~ '[0-9]' then cast(measure.drift_threshold->>'lower_threshold' as float) else 0 end as lower_threshold,
                case when measure.drift_threshold->>'upper_threshold' ~ '[0-9]' then cast(measure.drift_threshold->>'upper_threshold' as float) else 0 end as upper_threshold
            from core.measure
            join core.base_measure on base_measure.id=measure.base_measure_id
            left join core.asset on asset.id=measure.asset_id
            left join core.metrics on metrics.measure_id=measure.id
			left join core.dimension on dimension.id=measure.dimension_id
            where (asset.is_active is true or measure.is_active is true) and measure.attribute_id='{dq_column_id}'
        """
        cursor = execute_query(connection, cursor, query_string)
        measures = fetchall(cursor)
    return measures

def get_rule_by_column(config, collibra_column_id, rule_name, domain_id, rule_display_name):
    """
    Get the rule related to the column by name.
    """
    try:
        url = format_url(config.get("url"))
        url_params = {
            "targetId": collibra_column_id,
            "typePublicId": "DataQualityRuleGovernsColumn"
        }
        query_string = prepare_query_params(url_params)
        url = f"{url}/relations?{query_string}"
        response = make_call(config, url, "get")
        response = [item for item in response if item['source']['name'] == rule_name] if response else []
        response = [{"name":item.get("source", {}).get("name", ''), "id":item.get("source", {}).get("id", '')} for item in response]
        if len(response)>0:
            response = response[0]
            if response.get("displayName", '') != rule_display_name:
                update_collibra_display_name(config, response, rule_display_name)
            return response
        else:
            rule_domain = config.get("rule_domain", {})
            rule_domain_id = rule_domain.get("id") if rule_domain else None
            rule_domain_id = rule_domain_id if rule_domain_id else domain_id
            rule = create_collibra_column_rule(config, rule_domain_id, rule_name, rule_display_name)
            if rule:
                map_rule_to_column(config, collibra_column_id, rule.get("id", ''))
                return rule
        return None
    except Exception as e:
        log_error(f"Error getting Collibra rule for column {collibra_column_id} and rule name {rule_name}:", str(e))
        raise e

def create_collibra_column_rule(config, domain_id, rule_name, rule_display_name):
    """
    Create a new Collibra Data Quality Rule for the column.
    """
    try:
        url = format_url(config.get("url"))
        url = f"{url}/assets"
        params = {
            "name": rule_name,
            "displayName": rule_display_name,
            "domainId": domain_id,
            "statusId": "00000000-0000-0000-0000-000000005008",
            "excludedFromAutoHyperlinking": True,
            "typePublicId": "DataQualityRule"
        }
        response = make_call(config, url, params=params, method='post')
        return response
    except Exception as e:
        log_error(f"Error creating Collibra column rule {rule_name}:", str(e))
        raise e

def map_rule_to_column(config, column_id, rule_id):
    """
    Map the rule to the column in Collibra.
    """
    try:
        url = format_url(config.get("url"))
        url = f"{url}/relations"
        params = {
            "sourceId": rule_id,
            "targetId": column_id,
            "typeId": "00000000-0000-0000-0000-090000010022"
        }
        response = make_call(config, url, params=params, method='post')
        if not response:
            raise Exception(f"Failed to map rule to column")
    except Exception as e:
        log_error(f"Error mapping Collibra rule {rule_id} to column {column_id}:", str(e))
        raise e

def map_metric_to_rule(config, rule_id, metric_id):
    """
    Map the metric to the rule in Collibra.
    """
    try:
        url = format_url(config.get("url"))
        url = f"{url}/relations"
        params = {
            "sourceId": rule_id,
            "targetId": metric_id,
            "typeId": "00000000-0000-0000-0000-000000007016"
        }
        response = make_call(config, url, params=params, method='post')
        if not response:
            raise Exception(f"Failed to map metric to rule")
    except Exception as e:
        log_error(f"Error mapping Collibra metric {metric_id} to rule {rule_id}:", str(e))
        raise e

def get_collibra_metrics_by_dq_rule(config, rule, metric_name, domain_id, metric_display_name):
    """
    Get the metric related to the rule by name.
    """
    try:
        rule_id = rule.get("id", '')
        url = format_url(config.get("url"))
        url_params = {
            "relationTypeId": "00000000-0000-0000-0000-000000007016",
            "sourceId": rule_id
        }
        query_string = prepare_query_params(url_params)
        url = f"{url}/relations?{query_string}"
        response = make_call(config, url, "get")
        response = [item for item in response if item['target']['name'] == metric_name] if response else []
        metrics = [{"name":item.get("target", {}).get("name", ''), "id":item.get("target", {}).get("id", '')} for item in response]
        if len(metrics)>0:
            metric = metrics[0]
            if metric.get("displayName", '') != metric_display_name:
                update_collibra_display_name(config, metric, metric_display_name)
            return metric
        else:
            rule_domain = config.get("rule_domain", {})
            rule_domain_id = rule_domain.get("id") if rule_domain else None
            rule_domain_id = rule_domain_id if rule_domain_id else domain_id
            metric = create_collibra_metric(config, rule_domain_id, metric_name, metric_display_name)
            map_metric_to_rule(config, rule_id, metric.get("id", ''))
        return metric
    except Exception as e:
        log_error(f"Error getting Collibra metrics for rule {rule.get('name', '')} and metric name {metric_name}:", str(e))
        raise e

def create_collibra_metric(config, domain_id, metric_name, metric_display_name):
    """
    Create a new Collibra Data Quality Metric.
    """
    try:
        url = format_url(config.get("url"))
        url = f"{url}/assets"
        params = {
            "name": metric_name,
            "displayName": metric_display_name,
            "domainId": domain_id,
            "statusId": "00000000-0000-0000-0000-000000005008",
            "excludedFromAutoHyperlinking": False,
            "typePublicId": "DataQualityMetric"
        }
        response = make_call(config, url, params=params, method='post')
        return response
    except Exception as e:
        log_error(f"Error creating Collibra metric {metric_name}:", str(e))
        raise e

def push_metrics_to_collibra(channel_config, catalog_measure, metric_id, rule_id, measure):
    """
    Push metrics to Collibra based on the channel configuration and measure data.
    """
    try:
        catalog_name = catalog_measure.get("dq_column")
        collibra_type_id = catalog_measure.get("collibra_type_id")
        asset_id = metric_id
        if catalog_name.lower() in ["description", "measure query"]:
            asset_id = rule_id
        if not asset_id:
            return
        attribute = get_catalog_attribute(
            {**channel_config, "asset_id": asset_id, "collibra_type_id": collibra_type_id})
        attribute_id = attribute.get("id") if attribute else None
        value = get_attribute_value(catalog_measure, measure, {
            **channel_config, "asset_id": asset_id})
        if attribute_id:
            update_catalog(
                {**channel_config, "attribute_id": attribute_id, "catalog": catalog_name, "value": value})
        else:
            create_attribute({**channel_config, "collibra_type_id": collibra_type_id,
                            "asset_id": asset_id, "catalog": catalog_name, "value": value})
    except Exception as e:
        log_error(f"Error pushing metrics to Collibra for {asset_id}:", str(e))
        raise e

def get_collibra_column_terms(config, column_id):
    """
    Get all business terms related to the column.
    """
    try:
        url = format_url(config.get("url"))
        url_params = {
            "typePublicId": "BusinessAssetRepresentsDataAsset",
            "targetId": column_id
        }
        query_string = prepare_query_params(url_params)
        url = f"{url}/relations?{query_string}"
        response = make_call(config, url, "get")
        response = [{"name": item.get("source", {}).get("name", ""), "collibra_id": item.get("source", {}).get("id")} for item in response] if response else []
        return response
    except Exception as e:
        log_error(f"Error getting Collibra column terms for column {column_id}:", str(e))
        raise e

def insert_collibra_term(config, channel_config, term, domain):
    """
    Insert a new term into Collibra if it does not already exist.
    """
    try:
        organization_id = config.get('organization_id')
        if not organization_id:
            organization_id = config.get('dag_info', {}).get('organization_id')
        term_id = term.get("id", '')
        term = term.get("name", '')
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:

            # Check term Already Available
            check_existing_query = f"""
                select id from core.terms
                where source = 'collibra'
                and is_active = true and is_delete = false
                and lower(name)='{term.lower()}' and domain_parent_id='{domain.get("id")}'
            """
            cursor = execute_query(
                connection, cursor, check_existing_query)
            existing_term_data = fetchone(cursor)

            if existing_term_data:
                term_id = existing_term_data.get('id')
            else:
                # Insert New term if term Not Available
                term_description = get_catalog_attribute({**channel_config, "asset_id": term_id, "collibra_type_id": "00000000-0000-0000-0000-000000003114"})
                term_description = term_description.get("value", '') if term_description else ''
                term_id = str(uuid4())
                technical_term_name = f"{str(term)}({domain.get('name', '')})"

                query_input = (
                    term_id,
                    term,
                    technical_term_name,
                    term_description,
                    'Pending',
                    'Text',
                    False, False, False, False, '[]', '[]',
                    f'["{term}"]',
                    f'["{term}"]',
                    str(organization_id),
                    domain.get("id", ''),
                    domain.get("id", ''),
                    DEFAULT_SEMANTIC_THRESHOLD,
                    1,
                    'collibra',
                    True,
                    False,
                )
                input_literals = ", ".join(["%s"] * len(query_input))
                query_param = cursor.mogrify(
                    f"({input_literals}, CURRENT_TIMESTAMP)", query_input
                ).decode("utf-8")
                try:
                    query_string = f"""
                        insert into core.terms(
                            id, name, technical_name, description, status, derived_type, 
                            is_null, is_blank, is_unique, is_primary_key, tags, 
                            enum, contains, synonyms, organization_id, domain_parent_id,
                            domain_id, threshold, sensitivity, source, is_active, is_delete, created_date
                        ) values {query_param} 
                        RETURNING id
                    """
                    cursor = execute_query(
                        connection, cursor, query_string)
                    term = fetchone(cursor)
                    term_id = term.get('id')
                    return term_id
                except Exception as e:
                    log_error('Collibra Term Insert Failed  ', e)
            return term_id
    except Exception as e:
        log_error('Collibra Term Insert Failed  ', e)
        raise e

def unmap_collibra_terms(config, dq_attribute_id, dq_asset_id):
    """
    Unmap Collibra Terms from Attribute
    """
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            existing_query = f"""
            DELETE from core.terms_mapping
            where id in (
                select tm.id from core.terms_mapping tm 
                join core.terms on terms.id=tm.term_id 
                where tm.attribute_id='{dq_attribute_id}'
                and tm.asset_id='{dq_asset_id}'
            )
            """
            cursor = execute_query(
                connection, cursor, existing_query)
    except Exception as e:
        log_error(f'Unmap Collibra Terms from Attribute Failed  ', e)
        raise e

def map_attribute_to_term(config, term_id, dq_attribute_id, dq_asset_id):
    """
    Map Collibra Term to Attribute
    """
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            existing_query = f"""
            select id from core.terms_mapping 
            where asset_id='{dq_asset_id}' 
            and attribute_id='{dq_attribute_id}' 
            and term_id='{term_id}'
            """
            cursor = execute_query(
                connection, cursor, existing_query)
            existing_term_mapping_data = fetchone(cursor)

            if not existing_term_mapping_data:
                query_input = (
                    str(uuid4()),
                    str(term_id),
                    str(dq_asset_id),
                    str(dq_attribute_id),
                )
                input_literals = ", ".join(
                    ["%s"] * len(query_input))
                query_param = cursor.mogrify(
                    f"({input_literals}, CURRENT_TIMESTAMP)", query_input
                ).decode("utf-8")
                try:
                    query_string = f"""
                        insert into core.terms_mapping (id,
                        term_id, asset_id, attribute_id, created_date
                        ) values {query_param}
                        """
                    cursor = execute_query(
                        connection, cursor, query_string)
                except Exception as e:
                    log_error(
                        f'Collibra Terms Map to Attribute Insert Failed  ', e)
    except Exception as e:
        log_error(f'Collibra Terms Map to Attribute Insert Failed  ', e)
        raise e

def get_all_attribute_types(config: dict):
    """
    Get all Attribute types
    """
    try:
        url = format_url(config.get("url"))
        url = f"{url}/attributeTypes"
        response = make_call(config, url, "get")
        return response if response else None
    except Exception as e:
        log_error(f"Failed to get all attribute types", str(e))
        raise e

def get_domains_by_community(channel_config, community_id):
    """
    Get domain by community
    """
    try:
        url = format_url(channel_config.get("url"))
        url = f"{url}/domains?communityId={community_id}"
        response = make_call(channel_config, url, "get")
        response = [item['id'] for item in response] if response else []
        return response if response else []
    except Exception as e:
        log_error(f"Failed to get domains by community {community_id}", str(e))
        raise e

def prepare_query_params(params: dict) -> str:
    """
    Return Formatted Query Params
    """
    query_strings = []
    for key, value in params.items():
        if isinstance(value, list):
            for item in value:
                if item:
                    query_strings.append(f"{key}={item}")
        elif value:
            query_strings.append(f"{key}={value}")
    query_string = "&".join(query_strings)
    return f"&{query_string}" if query_string else ""


def get_catalog_assets(config: dict) -> str:
    """
    Return Filtered Assets
    """
    try:
        url = format_url(config.get("url"))
        url_params = {
            "name": config.get("measure_name")
        }
        query_string = prepare_query_params(url_params)
        url = f"{url}/assets?offset=0&limit=1&countLimit=-1&nameMatchMode=ANYWHERE&typeInheritance=true&excludeMeta=true&sortField=NAME&sortOrder=ASC{query_string}"
        response = make_call(config, url, "get")
        return response if response else None
    except Exception as e:
        log_error(f"Failed to get catalog assets for {config.get('measure_name')}", str(e))
        raise e

def format_url(url: str) -> str:
    """
    Return formatted Url
    """
    return url[:-1] if url.endswith("/") else url


def get_catalog_attribute(config: dict):
    """
    Get Catalog Attributes
    """
    try:
        url = format_url(config.get("url"))
        asset_id = config.get("asset_id")
        collibra_type_id = config.get("collibra_type_id")
        url = f"{url}/attributes?assetId={asset_id}&typeIds={collibra_type_id}"
        response = make_call(config, url, "get")
        return response[0] if response else None
    except Exception as e:
        log_error(f"Failed to get catalog attribute for asset {config.get('asset_id')} and type {config.get('collibra_type_id')}", str(e))
        raise e

def get_jwt_token_for_collibra(config: dict) -> str:
    """
    Get JWT token from VMware Identity Manager (ViDM) for Collibra authentication
    """
    try:
        authentication_url = config.get("authentication_url")
        client_id = decrypt(config.get("client_id", ""))
        client_secret = decrypt(config.get("client_secret", ""))
        username = decrypt(config.get("username", ""))
        password = decrypt(config.get("password", ""))
    except:
        authentication_url = config.get("authentication_url")
        client_id = config.get("client_id", "")
        client_secret = config.get("client_secret", "")
        username = config.get("username", "")
        password = config.get("password", "")

    if not authentication_url:
        raise Exception("Authentication URL is required for JWT authentication")

    payload = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'password',
        'username': username,
        'password': password,
        'scope': 'openid profile user'
    }

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    try:
        response = requests.post(authentication_url, data=payload, headers=headers)
        if response.status_code == 200:
            token_data = response.json()
            access_token = token_data.get('access_token')
            if not access_token:
                raise Exception("access_token not found in authentication response")
            return access_token
        else:
            raise Exception(f"Authentication failed with status code {response.status_code}: {response.text}")
    except Exception as e:
        log_error("JWT Authentication failed:", str(e))
        raise Exception(f"JWT Authentication failed: {str(e)}")

def make_call(config: dict, url: str, method: str, params: dict = None, is_result: bool = True):
    """
    Generic API Method
    """
    authentication_type = config.get("authentication_type", "Username/Password")
    is_jwt = authentication_type in ["JWT", "JWT Token"]

    try:
        if is_jwt:
            try:
                access_token = get_jwt_token_for_collibra(config)
                headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {access_token}",
                }
            except Exception as e:
                log_error("JWT authentication failed:", str(e))
                raise Exception(f"JWT authentication failed: {str(e)}")
        else:
            try:
                username = decrypt(config.get("username"))
                password = decrypt(config.get("password"))
            except:
                username = config.get("username")
                password = config.get("password")

            authentication_key = f'{username}:{password}'
            encoded_authentication = authentication_key.encode("utf-8")
            authentication = base64.b64encode(encoded_authentication)
            authentication = authentication.decode('utf-8')
            headers = {
                "Content-Type": "application/json",
                "Authorization": 'Basic {}'.format(authentication)
            }

        if method == "patch":
            response = requests.patch(
                url, headers=headers, data=json.dumps(params))
        elif method == "post":
            response = requests.post(
                url, headers=headers, data=json.dumps(params))
        else:
            response = requests.get(url, headers=headers)
        if (response.status_code == 200 or response.status_code == 201):
            if is_result:
                response = response.text
                if response:
                    response = json.loads(response)
                    if "data" in response:
                        response = response.get(
                            'data') if response.get('data') else None
                    elif "results" in response:
                        response = response.get(
                            'results') if response.get('results') else None
        elif response.status_code == 401:
            log_error(f"Unauthorized access to {url}.", "Please check your credentials.")
            raise Exception("Unauthorized access. Please check your credentials.")
        else:
            response = response if not is_result else {}
        return response
    except requests.exceptions.RequestException as e:
        log_error(f"Error making API call to {url} with method {method}:", str(e))
        raise e


def create_attribute(config: dict):
    """
    Create new attribute
    """
    try:
        url = format_url(config.get("url"))
        asset_id = config.get("asset_id")
        value = config.get("value")
        if isinstance(value, str):
            value = value.replace("''", "'")
        collibra_type_id = config.get("collibra_type_id")
        url = f"{url}/attributes"
        params = {
            "value": value,
            "assetId": asset_id,
            "typeId": collibra_type_id
        }
        response = make_call(config, url, "post", params, is_result=False)
    except Exception as e:
        log_error(f"Failed to create attribute for asset {asset_id} and type {collibra_type_id}", str(e))
        raise e


def update_catalog(config: dict):
    """
    Update Catalog
    """
    try:
        url = format_url(config.get("url"))
        attribute_id = config.get("attribute_id")
        value = config.get("value")
        if isinstance(value, str):
            value = value.replace("''", "'")
        url = f"{url}/attributes/{attribute_id}"
        catalog = config.get("catalog")
        params = {
            "value": value
        }
        response = make_call(config, url, "patch", params, is_result=False)
        if isinstance(response, requests.Response):  
            if response.status_code != 200:
                log_error(f"{catalog} for {attribute_id} failed", response.text)
        else:
            log_error(f"{catalog} for {attribute_id} failed,"," invalid response received.")
    except Exception as e:
        log_error(f"Failed to update catalog for attribute {attribute_id}", str(e))
        raise e

def get_attribute_value(catalog_measure: dict, measure: dict, config: dict):
    """
    Return Attribute Value
    """
    try:
        collibra_column = catalog_measure.get("collibra_column")
        catalog_key = catalog_measure.get("catalog_key")
        if collibra_column == "Result":
            return measure[catalog_key] == 'Pass'
        elif collibra_column == "Last Sync Date":
            update_date = datetime.strptime(measure[catalog_key], '%m/%d/%Y')
            utc_time = datetime(
                update_date.year, update_date.month, update_date.day)
            return (utc_time - datetime(1970, 1, 1)) // timedelta(milliseconds=1)
        return measure[catalog_key]
    except Exception as e:
        log_error(f"Failed to get attribute value for catalog measure {catalog_measure.get('dq_column', '')} and measure {measure.get('name', '')}", str(e))
        raise e


def find_hitachi_quality_statistics(config: dict):
    url = format_url(config.get("url"))
    response = make_call(config, url, "get")
    return response


def update_hitachi_quality_statistics(config: dict, params: dict):
    url = format_url(config.get("url"))
    measure_name = config.get("measure_name")
    response = make_call(config, url, "put", params)
    if response.status_code != 200:
        log_error(f"{measure_name}", response.text)

def check_custom_fields(config: dict, field_name):
    field_name = field_name.lower()
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query = f"""
            select id from core.fields 
            where level = 'Measure' and source='collibra' and is_active is true 
            and is_delete is false and lower(name)='{field_name}'
        """
        cursor = execute_query(connection, cursor, query)
        data = fetchone(cursor)
        existing_field = data.get("id") if data else None
    return existing_field

def create_custom_field(config: dict, field_name):
    organization_id = config.get('organization_id')
    if not organization_id:
        organization_id = config.get('dag_info', {}).get('organization_id')
    created_by = config.get("connection", {}).get("created_by", None)
    if not created_by:
        created_by = get_user_id(config)

    connection = get_postgres_connection(config)
    field_id = ''
    with connection.cursor() as cursor:
        query_input = (
            str(uuid4()),
            field_name,
            "Text",
            "Measure",
            1,
            'collibra',
            str(True),
            str(False),
            created_by,
            organization_id,
            str(False)
        )
        input_literals = ", ".join(["%s"] * len(query_input))
        query_param = cursor.mogrify(
            f"({input_literals}, CURRENT_TIMESTAMP)", query_input
        ).decode("utf-8")
        try:
            query_string = f"""
                insert into core.fields (id, name, data_type, level, "order", source, is_active, 
                is_delete, created_by, organization_id, is_created, created_date) 
                values {query_param} 
                RETURNING id
            """
            cursor = execute_query(connection, cursor, query_string)
            data = fetchone(cursor)
            field_id = data.get("id")
            return field_id
        except Exception as e:
            log_error(f"Error in create custom field {field_name} : ", e)

def update_custom_field_value(config: dict, measure_id, field_id, value):
    organization_id = config.get('organization_id')
    if not organization_id:
        organization_id = config.get('dag_info', {}).get('organization_id')
    created_by = config.get("connection", {}).get("created_by")
    if not created_by:
        created_by = get_user_id(config)
    connection = get_postgres_connection(config)
    if type(value) == list:
        value = str(value)
    with connection.cursor() as cursor:
        check_query = f"""
            select * from core.field_property where measure_id='{measure_id}' and field_id='{field_id}'
        """
        cursor = execute_query(connection, cursor, check_query)
        data = fetchone(cursor)
        if data:
            update_query = f"""
                update core.field_property set value='{value}' where measure_id='{measure_id}' and field_id='{field_id}'
            """
            execute_query(connection, cursor, update_query)
        else:
            query_input = (
                str(uuid4()),
                str(value),
                str(True),
                str(False),
                created_by,
                organization_id,
                field_id,
                measure_id
            )
            input_literals = ", ".join(["%s"] * len(query_input))
            query_param = cursor.mogrify(
                f"({input_literals}, CURRENT_TIMESTAMP)", query_input
            ).decode("utf-8")
            try:
                query_string = f"""
                    insert into core.field_property (id, value, is_active, 
                    is_delete, created_by, organization_id, field_id, measure_id, created_date) 
                    values {query_param} 
                """
                cursor = execute_query(connection, cursor, query_string)
            except Exception as e:
                log_error(f"Error in update custom field : ", e)

def get_collibra_asset(config: dict, asset_id):
    url = format_url(config.get("url"))
    url = f"{url}/assets/{asset_id}"
    response = make_call(config, url, "get", None)
    return response

def insert_collibra_domain(config: dict, domain_name):
    try:
        organization_id = config.get('organization_id')
        if not organization_id:
            organization_id = config.get('dag_info', {}).get('organization_id')
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            level = 1
            domain_type = 'domain'

            # Check Domain Already Available
            check_existing_query = f"""
                select id, name from core.domain 
                where source = 'collibra'
                and is_active = true and is_delete = false
                and lower(name)='{domain_name.lower()}'
            """
            cursor = execute_query(
                connection, cursor, check_existing_query)
            existing_domain_data = fetchone(cursor)

            if existing_domain_data:
                domain = existing_domain_data
            else:
                # Insert New Domain if Domain Not Available
                domain_id = str(uuid4())

                query_input = (
                    domain_id,
                    domain_name,
                    domain_name,
                    '',
                    str(organization_id),
                    None,
                    domain_type,
                    level-1,
                    'collibra',
                    None,
                    True,
                    False,
                    json.dumps(
                        {"type": "collibra"}, default=str),
                )
                input_literals = ", ".join(["%s"] * len(query_input))
                query_param = cursor.mogrify(
                    f"({input_literals}, CURRENT_TIMESTAMP)", query_input
                ).decode("utf-8")
                try:
                    query_string = f"""
                        insert into core.domain(
                            id, name, technical_name, description, organization_id,
                            parent_id, type, level, source, domain, is_active, 
                            is_delete, properties, created_date
                        ) values {query_param} 
                        RETURNING id, name
                    """
                    cursor = execute_query(
                        connection, cursor, query_string)
                    domain = fetchone(cursor)
                    return domain
                except Exception as e:
                    log_error('Collibra Domain Insert Failed  ', e)
            return domain
    except Exception as e:
        log_error('Collibra Domain Insert Failed  ', e)

def map_collibra_domain_to_asset(config, domain_id, asset_id, measure_id):
    try:
        created_by = config.get("connection", {}).get("created_by", '')
        type = 'asset'
        if asset_id:
            condition_query = f"and asset_id = '{asset_id}'"
            mapping_query = f"and domain_mapping.asset_id = '{asset_id}'"
        elif measure_id:
            condition_query = f"and measure_id = '{measure_id}'"
            mapping_query = f"and domain_mapping.measure_id = '{measure_id}'"
            type = 'measure'

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            delete_mapped_domain_query = f"""
                delete from core.domain_mapping where id in (
                    select domain_mapping.id from core.domain 
                    join core.domain_mapping on domain_mapping.domain_id = domain.id 
                    {mapping_query} and domain_mapping.level ='{type}'
                    where (properties->>'type')::text = 'collibra'
                    and domain.id <> '{domain_id}'
                )
            """
            cursor = execute_query(
                connection, cursor, delete_mapped_domain_query)
            check_existing_query = f"""
                    select id from core.domain_mapping 
                    where level='{type}' {condition_query} and domain_id = '{domain_id}'
                """
            cursor = execute_query(
                connection, cursor, check_existing_query)
            existing_map_data = fetchone(cursor)

            if existing_map_data:
                return
            else:
                query_input = (
                    str(uuid4()),
                    type,
                    str(domain_id),
                    str(asset_id) if asset_id else str(measure_id),
                    created_by
                )
                input_literals = ", ".join(
                    ["%s"] * len(query_input))
                query_param = cursor.mogrify(
                    f"({input_literals}, CURRENT_TIMESTAMP)", query_input
                ).decode("utf-8")
                try:
                    query_string = f"""
                        insert into core.domain_mapping (id,
                        level, domain_id, {type}_id, created_by, created_date
                        ) values {query_param}
                        """
                    cursor = execute_query(
                        connection, cursor, query_string)
                except Exception as e:
                    log_error(
                        f'Collibra Domain Map to {type} Insert Failed  ', e)
    except Exception as e:
        log_error(f'Collibra Domain Map to {type} Insert Failed  ', e)

def get_collibra_asset_tags(config, asset_id):
    url = format_url(config.get("url"))
    url = f"{url}/assets/{asset_id}/tags"
    response = make_call(config, url, "get", None)
    return response

def insert_collibra_tag(config: dict, tag):
    tag_name = tag.get("name")
    try:
        organization_id = config.get('organization_id')
        if not organization_id:
            organization_id = config.get(
                'dag_info', {}).get('organization_id')
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            description = ""

            # Check Tag Already Available
            check_existing_query = f"""
                select id from core.tags 
                where source = 'collibra' 
                and lower(name)='{tag_name.lower()}'
            """
            cursor = execute_query(
                connection, cursor, check_existing_query)
            existing_tag_data = fetchone(cursor)

            if existing_tag_data:
                tag_id = existing_tag_data.get("id")
            else:
                # Insert New Tag if Tag Not Avilable
                new_tag_id = str(uuid4())

                query_input = (
                    new_tag_id,
                    tag_name,
                    tag_name,
                    description,
                    str(organization_id),
                    '#64AAEF',
                    'collibra',
                    str(False),
                    str(True),
                    str(False),
                    None,
                    json.dumps(
                        {"type": "collibra"}, default=str),
                )
                input_literals = ", ".join(["%s"] * len(query_input))
                query_param = cursor.mogrify(
                    f"({input_literals}, CURRENT_TIMESTAMP)", query_input
                ).decode("utf-8")

                try:
                    query_string = f"""
                        insert into core.tags(
                            id, name, technical_name, description, organization_id, 
                            color, source, is_mask_data, is_active, is_delete, 
                            parent_id, properties, created_date
                        ) values {query_param} 
                        RETURNING id
                    """
                    cursor = execute_query(
                        connection, cursor, query_string)
                    tag = fetchone(cursor)
                    tag_id = tag.get("id")
                    return tag_id
                except Exception as e:
                    log_error('Collibra Tags Insert Failed  ', e)
            return tag_id
    except Exception as e:
        log_error(
            f"Collibra Tags Insert Failed ", e)

def map_collibra_tag_to_attribute(config: dict, tag_id, tag_name, attribute_id):
    try:
        created_by = config.get("connection", {}).get("created_by")
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_input = (
                str(uuid4()),
                'attribute',
                str(tag_id),
                str(attribute_id),
                created_by
            )
            input_literals = ", ".join(
                ["%s"] * len(query_input))
            query_param = cursor.mogrify(
                f"({input_literals}, CURRENT_TIMESTAMP)", query_input
            ).decode("utf-8")
            try:
                query_string = f"""
                    insert into core.tags_mapping (id,
                    level, tags_id, attribute_id, created_by, created_date
                    ) values {query_param}
                    """
                cursor = execute_query(
                    connection, cursor, query_string)
            except Exception as e:
                log_error(
                    f'Collibra Tags Map to Attribute Insert Failed  ', e)
    except Exception as e:
        log_error(f'Collibra Tags Map to Attribute Insert Failed  ', e)

def get_asset_responsibilities(config: dict, asset_id):
    """
    Fetch asset's Responsibilities
    """
    url = format_url(config.get("url"))
    url = f"{url}/responsibilities?includeInherited=true&resourceIds={asset_id}"
    response = make_call(config, url, "get")
    return response

def get_collibra_user(config: dict, user_id):
    """
    Fetch user by ID
    """
    url = format_url(config.get("url"))
    url = f"{url}/users/{user_id}"
    response = make_call(config, url, "get")
    name = response.get("firstName")+" "+response.get("lastName")
    return name

def clean_attribute_mapped_tags(config: dict, attribute_id):
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        delete_mapped_tag_query = f"""
            delete from core.tags_mapping where id in (
                select tags_mapping.id from core.tags 
                join core.tags_mapping on tags_mapping.tags_id = tags.id 
                and tags_mapping.attribute_id = '{attribute_id}' 
                and tags_mapping.level ='attribute'
                where (properties->>'type')::text = 'collibra'
            )
        """
        cursor = execute_query(
            connection, cursor, delete_mapped_tag_query)

def clean_collibra_domain_and_terms(config: dict):
    """
    Delete the unmapped Collibra domains and terms from the Dqlabs Portal
    """
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            terms_delete_query = f"""
                DELETE FROM core.terms
                WHERE terms.source = 'collibra'
                and id NOT IN (
                    SELECT DISTINCT term_id FROM core.terms_mapping
                );
                """
            cursor = execute_query(connection, cursor, terms_delete_query)
            domain_delete_query = f"""
                DELETE FROM core.domain
                WHERE domain.source = 'collibra'
                and id NOT IN (
                    SELECT DISTINCT domain_id FROM core.domain_mapping
                )
                and id NOT IN (
                    SELECT DISTINCT domain_id FROM core.terms
                );
                """
            cursor = execute_query(connection, cursor, domain_delete_query)
    except Exception as e:
        log_error(f'Collibra Domain and Terms Clean Failed  ', e)

def get_user_id(config):
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        user_query = "select id from core.users limit 1"
        cursor = execute_query(connection, cursor, user_query)
        data = fetchone(cursor)
        id = data.get("id")
    return id

def clean_collibra_hierarchial_name(raw_name: str) -> str:
    last_part = raw_name.split(">")[-1].strip()
    return re.sub(r"\s*\(.*\)$", "", last_part)

def update_collibra_display_name(config, asset, display_name):
    """
    Update Collibra Display Name
    """
    try:
        url = format_url(config.get("url"))
        id = asset.get("id", '')
        url = f"{url}/assets/{id}"
        params = {"displayName": display_name}
        response = make_call(config, url, params=params, method="patch", is_result=False)
        if response.status_code != 200:
            log_error(f"Failed to update display name for rule {asset}", response.text)
    except Exception as e:
        log_error(f"Error updating Collibra display names: {str(e)}")
        raise e

def check_rule_has_dimension(config: dict, rule_id: str, dimension_name: str):
    """
    Check if a Collibra rule has a specific dimension tagged to it
    """
    try:
        url = format_url(config.get("url"))
        url_params = {
            "sourceId": rule_id,
            "typePublicId": "DataQualityRuleClassifiedByDataQualityDimension"
        }
        query_string = prepare_query_params(url_params)
        url = f"{url}/relations?{query_string}"
        response = make_call(config, url, "get")
        
        if response:
            for relation in response:
                target_asset = relation.get("target", {})
                if target_asset.get("name") == dimension_name:
                    return True, target_asset.get("id")
        return False, None
    except Exception as e:
        log_error(f"Error checking if rule {rule_id} has dimension {dimension_name}:", str(e))
        return False, None

def get_data_quality_dimension_by_name(config: dict, dimension_name: str, dimension_domain_id: str):
    """
    Get a data quality dimension asset by name from the specified domain
    """
    try:
        url = format_url(config.get("url"))
        url_params = {
            "name": dimension_name,
            "typePublicId": "DataQualityDimension",
            "domainId": dimension_domain_id,
            "excludeMeta": True
        }
        query_string = prepare_query_params(url_params)
        url = f"{url}/assets?{query_string}"
        response = make_call(config, url, "get")
        
        if response:
            for asset in response:
                if asset.get("name") == dimension_name:
                    return asset
        return None
    except Exception as e:
        log_error(f"Error getting data quality dimension {dimension_name} from domain {dimension_domain_id}:", str(e))
        return None

def create_data_quality_dimension(config: dict, dimension_name: str, dimension_domain_id: str):
    """
    Create a new data quality dimension asset in the specified domain
    """
    try:
        url = format_url(config.get("url"))
        url = f"{url}/assets"
        params = {
            "name": dimension_name,
            "displayName": dimension_name,
            "domainId": dimension_domain_id,
            "statusId": "00000000-0000-0000-0000-000000005008",
            "excludedFromAutoHyperlinking": True,
            "typePublicId": "DataQualityDimension"
        }
        response = make_call(config, url, params=params, method='post')
        return response
    except Exception as e:
        log_error(f"Error creating data quality dimension {dimension_name}:", str(e))
        return None

def get_rule_id_from_measure(config: dict, measure_id: str):
    """
    Get the rule ID associated with a measure (metric)
    """
    try:
        url = format_url(config.get("url"))
        url_params = {
            "relationTypeId": "00000000-0000-0000-0000-000000007016",
            "targetId": measure_id
        }
        query_string = prepare_query_params(url_params)
        url = f"{url}/relations?{query_string}"
        response = make_call(config, url, "get")
        
        if response and len(response) > 0:
            rule_id = response[0].get("source", {}).get("id")
            return rule_id
        return None
    except Exception as e:
        log_error(f"Error getting rule ID for measure {measure_id}:", str(e))
        return None

def tag_rule_with_dimension(config: dict, rule_id: str, dimension_id: str):
    """
    Tag a rule with a data quality dimension
    """
    try:
        url = format_url(config.get("url"))
        url = f"{url}/relations"
        params = {
            "sourceId": rule_id,
            "targetId": dimension_id,
            "typePublicId": "DataQualityRuleClassifiedByDataQualityDimension",
        }
        response = make_call(config, url, params=params, method='post')
        if not response:
            raise Exception(f"Failed to tag rule {rule_id} with dimension {dimension_id}")
        return response
    except Exception as e:
        log_error(f"Error tagging rule {rule_id} with dimension {dimension_id}:", str(e))
        raise e

def handle_measure_dimension_tagging(config: dict, channel_config: dict, measure: dict, collibra_measure_id: str):
    """
    Handle dimension tagging for a measure after update_catalog or create_attribute
    """
    try:
        dimension_name = measure.get("dimension_name")
        dimension_domain = channel_config.get("dimension_domain", {})
        dimension_domain_id = dimension_domain.get("id", '') if dimension_domain else ''
        
        if not dimension_name or not dimension_domain or not collibra_measure_id:
            log_info(f"Skipping dimension tagging for measure {measure.get('name', '')} because dimension name, domain, or measure ID is missing")
            return
        
        # For measures, we need to get the rule ID from the measure
        # The measure ID is the metric, and we need to find the associated rule
        collibra_rule_id = get_rule_id_from_measure(channel_config, collibra_measure_id)
        
        if not collibra_rule_id:
            log_info(f"Could not find rule ID for measure {collibra_measure_id}")
            return
        
        # Check if the rule already has this dimension tagged
        has_dimension, existing_dimension_id = check_rule_has_dimension(channel_config, collibra_rule_id, dimension_name)
        
        if has_dimension:
            log_info(f"Rule {collibra_rule_id} already has dimension {dimension_name} tagged")
            return
        
        # Check if dimension exists in the dimension domain
        dimension_asset = get_data_quality_dimension_by_name(channel_config, dimension_name, dimension_domain_id)
        if not dimension_asset:
            # Create the dimension if it doesn't exist
            dimension_asset = create_data_quality_dimension(channel_config, dimension_name, dimension_domain_id)
            if not dimension_asset:
                log_info(f"Failed to create data quality dimension {dimension_name}")
                return
        
        # Tag the rule with the dimension
        dimension_id = dimension_asset.get("id")
        tag_rule_with_dimension(channel_config, collibra_rule_id, dimension_id)
        log_info(f"Successfully tagged rule {collibra_rule_id} with dimension {dimension_name}")
        
    except Exception as e:
        log_error(f"Error handling dimension tagging for measure {measure.get('name', '')}:", str(e))