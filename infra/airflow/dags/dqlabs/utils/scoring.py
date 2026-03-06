"""
    Migration Notes From V2 to V3:
    Migrations Completed 
    Pending:true
    Validate Scoring Logic
"""

from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone
from dqlabs.enums.approval_status import ApprovalStatus


def get_asset_scores(config: dict) -> list:
    """
    Returns the scores for given asset, for the last run.
    """
    try:
        asset_id = config.get("asset_id")
        run_id = config.get("queue_id")
        connection = get_postgres_connection(config)
        asset_scores = {}
        if run_id:
            with connection.cursor() as cursor:
                query_string = f"""
                    select 
                        distinct measure.asset_id,  
                        ((sum(metrics.score*metrics.weightage)/sum(metrics.weightage))) as score
                    from core.metrics
                    join core.measure on measure.id = metrics.measure_id and measure.last_run_id = metrics.run_id and measure.is_active = True
                    left join core.attribute on attribute.id = measure.attribute_id and attribute.is_selected = true
                    where metrics.asset_id ='{asset_id}'
                    and metrics.is_measure = true and metrics.allow_score = true
                    and lower(measure.status)!='{ApprovalStatus.Deprecated.value.lower()}'
                   	and attribute.parent_attribute_id is null
                    group by measure.asset_id
                """
                cursor = execute_query(connection, cursor, query_string)
                result = fetchall(cursor)
                for score in result:
                    asset_id = score.get("asset_id")
                    dq_score = score.get("score")
                    if dq_score is not None:
                        dq_score = dq_score if dq_score else 0
                        dq_score = 100 if dq_score > 100 else dq_score
                        dq_score = 0 if dq_score < 0 else dq_score
                    asset_scores.update({asset_id: dq_score})
        return asset_scores
    except Exception as e:
        raise e


def get_attribute_scores(config: dict) -> list:
    """
    Returns the list of scores for each attribute
    in a given asset, for the last run.
    """
    try:
        asset_id = config.get("asset_id")
        run_id = config.get("queue_id")
        level = config.get("level")
        connection = get_postgres_connection(config)
        attribute_scores = {}

        # Update JSON Attribute Scores
        update_json_attribute_scores(config)

        if run_id:
            with connection.cursor() as cursor:
                query_string = f"""
                   select 
                        distinct measure.attribute_id,  
                        (
                            sum(case when metrics.allow_score = true and metrics.is_measure = true then metrics.score * metrics.weightage end)
                            /
                            sum(case when metrics.allow_score = true and metrics.is_measure = true then metrics.weightage end)
                        ) as score
                    from core.attribute
                    left join core.measure on measure.attribute_id = attribute.id and measure.is_active=true and lower(measure.status)!='{ApprovalStatus.Deprecated.value.lower()}'
                    left join core.metrics  on metrics.measure_id = measure.id and measure.last_run_id = metrics.run_id
                    and metrics.is_measure = true and metrics.allow_score = true
                    where attribute.asset_id = '{asset_id}'
                    group by measure.attribute_id
                """
                cursor = execute_query(connection, cursor, query_string)
                result = fetchall(cursor)
                for score in result:
                    attribute_id = score.get("attribute_id")
                    dq_score = score.get("score")
                    if dq_score is not None:
                        dq_score = dq_score if dq_score else 0
                        dq_score = 100 if dq_score > 100 else dq_score
                        dq_score = 0 if dq_score < 0 else dq_score
                    attribute_scores.update({attribute_id: dq_score})

                # Set DQScore Zero to attribute where all measure is inactive
                attribute_condition = ""
                if level == "attribute":
                    attribute_id = config.get("attribute_id", None)
                    attribute_condition = (
                        f""" and attribute.id = '{attribute_id}' """
                        if attribute_id
                        else ""
                    )
                set_query_string = f"""
                    update core.attribute set score=null
                    where asset_id='{asset_id}'
                    and lower(status)!='{ApprovalStatus.Deprecated.value.lower()}'
                    and attribute.id in (
                        select 
                            attribute.id 
                        from core.attribute
                        join core.measure on measure.attribute_id = attribute.id
                        where attribute.asset_id='{asset_id}' and attribute.is_active = true
                        {attribute_condition}
                        group by attribute.id
                        having sum(case when measure.is_active=True and measure.allow_score = True then 1 else 0 end) <= 0
                    )
                """
                cursor = execute_query(connection, cursor, set_query_string)
        return attribute_scores
    except Exception as e:
        raise e


def get_rules_statistics(config: dict) -> list:
    """
    Returns the list of scores for each attribute
    in a given asset, for the last run.
    """
    try:
        asset_id = config.get("asset_id")
        run_id = config.get("queue_id")
        connection = get_postgres_connection(config)
        statistics = {}
        if run_id:
            with connection.cursor() as cursor:
                query_string = f"""
                    select distinct met.attribute_id, count(distinct met.measure_id) as total_rules,
                    count(distinct case when met.status='passed' then met.measure_id end) as passed_rules,
                    count(distinct case when met.status='failed' then met.measure_id end) as failed_rules
                    from core.metrics as met
                    join core.measure as mes on mes.id=met.measure_id
                    where met.asset_id='{asset_id}' and met.run_id = '{run_id}'
                    group by met.attribute_id
                """
                cursor = execute_query(connection, cursor, query_string)
                result = fetchall(cursor)
                for stat in result:
                    attribute_id = stat.get("attribute_id")
                    attribute_id = attribute_id if attribute_id else "asset"
                    passed_rules = stat.get("passed_rules")
                    passed_rules = passed_rules if passed_rules else 0
                    failed_rules = stat.get("failed_rules")
                    failed_rules = failed_rules if failed_rules else 0
                    statistics.update(
                        {
                            attribute_id: {
                                "passed_rules": passed_rules,
                                "failed_rules": failed_rules,
                            }
                        }
                    )
        return statistics
    except Exception as e:
        raise e


def update_json_attribute_scores(config: dict):
    """
    Returns the list of scores for each attribute
    in a given asset, for the last run.
    """
    try:
        asset_id = config.get("asset_id")
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select distinct
                  attribute.id as attribute_id,
                  measure.id as measure_id,
                  measure.last_run_id as run_id
                from core.attribute
                join core.measure on measure.attribute_id = attribute.id 
				join core.base_measure on base_measure.id = measure.base_measure_id and base_measure.is_visible = false
                where attribute.asset_id = '{asset_id}' and attribute.is_active=true 
                    and attribute.is_selected=true and lower(attribute.derived_type)='json'
            """
            cursor = execute_query(connection, cursor, query_string)
            result = fetchall(cursor)
            for json_schema_score_measure in result:
                attribute_id = json_schema_score_measure.get("attribute_id")
                measure_id = json_schema_score_measure.get("measure_id")
                run_id = json_schema_score_measure.get("run_id")

                if run_id and measure_id:
                    query_string = f"""
                    select 
                            distinct attribute.parent_attribute_id as attribute_id,  
                            (
                                sum(case when metrics.allow_score = true and metrics.is_measure = true then metrics.score * metrics.weightage end)
                                /
                                sum(case when metrics.allow_score = true and metrics.is_measure = true then metrics.weightage end)
                            ) as score
                        from core.attribute
                        join core.measure on measure.attribute_id = attribute.id 
                        join core.metrics on metrics.measure_id = measure.id and measure.last_run_id = metrics.run_id
                        where 
                            attribute.asset_id = '{asset_id}' and attribute.parent_attribute_id = '{attribute_id}'
                            and attribute.is_active=true and attribute.is_selected=true and attribute.is_delete=false 
                            and lower(attribute.status)!='{ApprovalStatus.Deprecated.value.lower()}'
                            and measure.is_active=true and lower(measure.status)!='{ApprovalStatus.Deprecated.value.lower()}'
                            and metrics.is_measure = true and metrics.allow_score = true
                        group by attribute.parent_attribute_id
                    """
                    cursor = execute_query(connection, cursor, query_string)
                    score = fetchone(cursor)
                    if score:
                        dq_score = score.get("score")
                        dq_score = dq_score if dq_score else 0
                        dq_score = 100 if dq_score > 100 else dq_score
                        dq_score = 0 if dq_score < 0 else dq_score
                    else:
                        dq_score = "null"

                    update_query_string = f"""
                        update core.measure set score={dq_score}
                        where id='{measure_id}' and last_run_id='{run_id}' and attribute_id='{attribute_id}'
                    """
                    cursor = execute_query(connection, cursor, update_query_string)

                    update_query_string = f"""
                        update core.metrics set score={dq_score}
                        where measure_id='{measure_id}' and run_id='{run_id}' and attribute_id='{attribute_id}'
                    """
                    cursor = execute_query(connection, cursor, update_query_string)
    except Exception as e:
        raise e
