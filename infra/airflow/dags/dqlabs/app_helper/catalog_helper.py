from dqlabs.app_helper.log_helper import log_error
from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone

def get_asset_metrics(config, dq_asset_id=None):
    """
    Get Asset Level metrics
    """
    asset_id = config.get("asset_id")
    asset_filter = ""
    if dq_asset_id:
        asset_id = dq_asset_id
    metrics = None
    if asset_id:
        asset_filter = f"and asset.id = '{asset_id}' "
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                with measure_stat as (
                    select
                        measure.asset_id,
                        sum(case when measure.is_active then 1 else 0 end) as active_measures,
                        sum(case when measure.is_drift_enabled then 1 else 0 end) as observed_measures,
                        sum(case when measure.allow_score then 1 else 0 end) as scored_measures
                    from core.asset
                    join core.measure on measure.asset_id = asset.id
                    join core.base_measure on base_measure.id = measure.base_measure_id 
                    where measure.is_active = true and measure.is_delete = false and base_measure.level ='asset' and asset.is_active = true
                    {asset_filter}
                    group by measure.asset_id
                ),
                terms as (
                    select count(distinct terms.id) as terms_count,
                        attribute.asset_id
                    FROM core.asset               
                    join core.attribute on attribute.asset_id=asset.id
                    join core.terms_mapping on terms_mapping.attribute_id = attribute.id
                    join core.terms ON terms.id = terms_mapping.term_id
                    where attribute.is_active=true
                    {asset_filter}
                    group by attribute.asset_id
                )
                select
                    connection.type as connection_type,
                    asset.technical_name as name,
                    asset.id as asset_id,
                    asset.score,
                    data.row_count,
                    data.freshness,
                    data.duplicate_count,
                    asset.alerts,
                    asset.issues,
                    data.valid_rows,
                    data.invalid_rows,
                    data.failed_rules,
                    data.passed_rules,
                    asset.properties as properties,
                    connection.credentials as credentials,
                    case when asset.modified_date is not null then asset.modified_date else asset.created_date end as last_updated_date,
                    stat.active_measures,
                    stat.observed_measures,
                    stat.scored_measures,
                    coalesce(terms.terms_count, 0) as terms
                from core.asset
                join core.data on data.asset_id = asset.id
                join core.connection on connection.id = asset.connection_id
                left join measure_stat as stat on stat.asset_id = asset.id
                left join terms as terms on terms.asset_id = asset.id
                where asset.is_active = true and asset.is_delete = false
                and connection.is_active = true and connection.is_delete = false
                and asset.group = 'data' and asset.status != 'Deprecated'
                {asset_filter}
            """
            cursor = execute_query(connection, cursor, query_string)
            metrics = fetchone(cursor) if asset_id else fetchall(cursor)
        return metrics
    except Exception as e:
        log_error(f"Get Asset Metrics ", str(e))
        raise e


def get_attributes_metrics(config, dq_asset_id=None):
    """
    Get Attribute Level metrics
    """
    asset_id = config.get("asset_id")
    if dq_asset_id:
        asset_id = dq_asset_id
    metrics = []
    asset_filter = ""
    measure_filter = ""
    if asset_id:
        asset_filter = f"and asset.id = '{asset_id}' "
        measure_filter = f"where mes.asset_id = '{asset_id}' "

    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                with measure_stat as (
                    select mes.attribute_id,
                        sum(case when mes.is_active=True then 1 else 0 end) as active_measures,
                        sum(case when mes.is_drift_enabled=True then 1 else 0 end) as observed_measures,
                        sum(case when mes.allow_score=True then 1 else 0 end) as scored_measures,
                        max(case when mes.is_active and mes.allow_score then mes.invalid_rows else 0 end) as invalid_rows,
                        max(mes.row_count) - max(case when mes.is_active and mes.allow_score then mes.invalid_rows else 0 end) as valid_rows,
                        sum(
                            case when mes.is_active=True and
                            mes.technical_name not in (
                                'min_value', 'max_value', 'min_length', 'max_length', 'length_range',
                                'distinct_count', 'null_count', 'zero_values', 'value_range', 'pattern',
                                'space_count', 'blank_count'
                            ) and base.type not in ('custom', 'semantic')
                            then 1 else 0 end
                        ) as active_advanced_measures
                    from core.measure as mes
                    join core.base_measure as base on base.id=mes.base_measure_id
                    {measure_filter}
                    group by mes.attribute_id
                ),
                terms as (
                    select count(distinct terms.id) as terms_count,
                        attribute.id
                    FROM core.asset
                    join core.attribute on attribute.asset_id=asset.id
                    join core.terms_mapping on terms_mapping.attribute_id = attribute.id
                    join core.terms ON terms.id = terms_mapping.term_id
                    where attribute.is_active = true and asset.is_active = true
                    {asset_filter}
                    group by attribute.id
                )
                select
                    attribute.technical_name as name,
                    attribute.alerts,
                    attribute.issues,
                    attribute.total_rules,
                    attribute.passed_rules,
                    attribute.failed_rules,
                    attribute.row_count,
                    attribute.score,
                    case when asset.modified_date is not null then asset.modified_date else asset.created_date end as last_updated_date,
                    stat.active_measures,
                    stat.observed_measures,
                    stat.scored_measures,
                    stat.invalid_rows,
                    stat.valid_rows,
                    attribute.id as attribute_id,
                    asset.id as asset_id,
                    coalesce(terms.terms_count, 0) as terms
                from
                core.attribute
                join core.asset on asset.id = attribute.asset_id
                join core.data on data.asset_id = asset.id
                left join measure_stat as stat on stat.attribute_id = attribute.id
                left join terms as terms on terms.id = attribute.id
                where asset.is_active = true and attribute.is_active = true
                and asset.is_delete = false and attribute.is_delete = false
                {asset_filter}
            """
            cursor = execute_query(connection, cursor, query_string)
            metrics = fetchall(cursor)
        return metrics
    except Exception as e:
        log_error(f"Get Attribute Metrics ", str(e))
        raise e


def get_measure_metrics(config):
    """
    Get Measure Last Run Asset Level metrics
    """
    asset_id = config.get("asset_id")
    metrics = []
    asset_filter = ""
    measure_filter = ""
    if asset_id:
        asset_filter = f"and asset.id = '{asset_id}' "
        measure_filter = f"where asset_id = '{asset_id}' "
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                with last_run as (
                    select * from (
                        select distinct on (run_id) run_id, created_date
                        from core.metrics
                        {measure_filter}
                        order by run_id, created_date desc
                    ) as temp_table
                    order by created_date desc
                    limit 1
                )
                select 
                    distinct on (measure.id)
                    attribute.id as attribute_id,
                    asset.id as asset_id,
                    metrics.measure_id,
                    asset.technical_name as asset_name,
                    attribute.technical_name as attribute_name,
                    attribute.alerts as attribute_alerts,
                    attribute.issues as attribute_issues,
                    asset.alerts as asset_alerts,
                    asset.issues as asset_issues,
                    base_measure.name,
                    base_measure.description,
                    metrics.value,
                    metrics.level,
                    metrics.score,
                    metrics.created_date,
                    metrics.drift_status
                from core.measure
                join core.base_measure on base_measure.id = measure.base_measure_id
                join core.asset on asset.id = measure.asset_id
                join core.data on data.asset_id = measure.asset_id
                join core.metrics on metrics.measure_id = measure.id 
                join last_run as run on run.run_id=metrics.run_id
                left join core.attribute on attribute.id = metrics.attribute_id
                where asset.is_active = true and asset.is_delete = false and measure.is_active = true
                {asset_filter}
            """
            cursor = execute_query(connection, cursor, query_string)
            metrics = fetchall(cursor)
        return metrics
    except Exception as e:
        log_error(f"Get Measure Metrics ", str(e))
        raise e


def get_alerts_metrics(config, id, type='asset'):
    """
    Get Alerts By Last Run 
    """
    asset_id = config.get("asset_id")
    alerts = []
    if not asset_id or not id:
        return alerts

    type_condition = f" and asset.id='{id}' "
    if type == 'attribute':
        type_condition = f" and attr.id='{id}' "
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                WITH alerts_result AS (  
                    select metrics.created_date, metrics.message, metrics.drift_status as status, metrics.value,
                    base.name as measure_name, attr.name as attribute_name, asset.name as asset_name, 
                    metrics.measure_name as measure,
                    metrics.id as alert_id,
                    mes.id as measure_id,
                    (case 
                                    when lower(metrics.drift_status) = 'high'  then 1
                                    when lower(metrics.drift_status) = 'medium'  then 2
                                    else 3
                                end) as alert_priority
                    from core.metrics as metrics
                    join core.measure as mes on mes.id=metrics.measure_id
                    join core.base_measure as base on base.id=mes.base_measure_id
                    join core.asset as asset on asset.id=metrics.asset_id and asset.last_run_id = metrics.run_id
                    join core.connection as connection on connection.id=asset.connection_id
                    left join core.issues as issue on issue.metrics_id=metrics.id and issue.is_delete = false
                    left join core.attribute as attr on attr.id = metrics.attribute_id
                    where lower(metrics.drift_status) != 'ok' and asset.is_active = True and asset.is_delete = false
                    {type_condition}
                )
                select * from alerts_result
                order by alert_priority asc
            """
            cursor = execute_query(connection, cursor, query_string)
            alerts = fetchall(cursor)
        return alerts
    except Exception as e:
        log_error(
            f"Get Alert Stats ", str(e))
        raise e


def get_issues_metrics(config, id, type='asset'):
    """
    Get Issues for Last 3 Days 
    """
    asset_id = config.get("asset_id")
    issues = []
    if not asset_id or not id:
        return issues

    type_condition = f" and asset.id='{id}' "
    if type == 'attribute':
        type_condition = f" and attribute.id='{id}' "
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                WITH issues_result AS (  
                    select 
                        concat(issues.issue_id,' ',issues.name) as name,
                        attribute.name as attribute_name,
                        issues.id,
                        issues.status, 
                        issues.priority, 
                        issues.created_date
                    from core.issues as issues
                    join core.asset as asset on asset.id=issues.asset_id
                    join core.measure as measure on measure.id=issues.measure_id 
                    join core.connection as connection on connection.id=asset.connection_id
                    left join core.attribute on attribute.id=issues.attribute_id
                    where issues.is_delete = false
                    and measure.is_active=true and asset.is_active = true and asset.is_delete = false
                    {type_condition}
                )
                select * from issues_result
                order by created_date desc
            """
            cursor = execute_query(connection, cursor, query_string)
            issues = fetchall(cursor)
        return issues
    except Exception as e:
        log_error(
            f"Get Issue Metrics ", str(e))
        raise e

def get_glossary_stats(config, glossary_id):
    glossary = {}
    try:
        """
        Get domain details by domain id
        """
        if not glossary_id:
            return glossary

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            # get the domain by name
            query_string = f"""
                with recursive domain_hierarchy as (
                    select id, name, parent_id, domain, level, is_active
                    from core.domain where domain.id in ('{glossary_id}')
                    union all
                    select r.id, r.name, r.parent_id, r.domain, r.level, r.is_active
                    from core.domain r
                    join domain_hierarchy dh on r.parent_id = dh.id
                ), unique_domains as (
                    select * from domain_hierarchy
                    group by id, name, parent_id, domain, level, is_active
                    order by level
                ), domain_list as (
                    with recursive sub_domain as (
                        select id, name, parent_id, domain, level, is_active, id as root
                        from unique_domains
                        union all
                        select r.id, r.name, r.parent_id, r.domain, r.level, r.is_active, s.root
                        from unique_domains r
                        join sub_domain s on r.parent_id = s.id
                    ) select * from sub_domain
                ), domains_stats as (
                    select dom.root as id,
                    asset.score,
                    count(distinct metrics.id) as alerts,
                    count(distinct issues.id) as issues,
                    count(distinct asset.id) as assets_count,
                    count(distinct attribute.id) as attributes_count,
                    count(distinct measure.id) as measures_count,
                    asset.id as assets, null::uuid as measures,
                    array_remove(array_agg(distinct case when issues.id is not null then issues.id end), NULL) as issue_ids
                    from core.domain
                    join domain_list as dom on dom.id=domain.id
                    join core.domain_mapping on domain_mapping.domain_id=dom.id
					left join core.asset on asset.id=domain_mapping.asset_id
                    left join core.measure on measure.asset_id=asset.id and measure.is_active=true
                    left join core.attribute on attribute.id=measure.attribute_id and attribute.is_selected=true
                    left join core.metrics on metrics.measure_id=measure.id
                    and metrics.run_id=measure.last_run_id and lower(metrics.drift_status) in ('high','medium','low')
                    and (
                        metrics.attribute_id is null
                        or (metrics.attribute_id is not null and metrics.attribute_id=attribute.id)
                    )
                    left join core.issues on issues.measure_id=measure.id and issues.is_delete=false
                    and (
                        issues.attribute_id is null
                        or (issues.attribute_id is not null and issues.attribute_id=attribute.id)
                    )
                    where asset.is_active=true
                    group by dom.root, asset.id
                ), domain_stats as (
                    select id,
                    avg(score) as score,
                    sum(alerts) as alerts,
                    sum(issues) as issues,
                    sum(assets_count) as assets,
                    sum(attributes_count) as attributes,
                    sum(measures_count) as measures
                    from domains_stats
                    group by id
                    order by score desc 
                ), domain_detail as (
                    select g.id, g.name, g.parent_id, g.domain, g.level, g.is_active, domain_stats.*
                    from core.domain as g
                    join domain_stats on domain_stats.id=g.id
                    order by score desc 
                ) select * from domain_detail
            """
            cursor = execute_query(connection, cursor, query_string)
            glossary = fetchall(cursor)
            glossary = glossary if glossary else []
            if glossary:
                glossary = next(
                    (
                        stat
                        for stat in glossary
                        if str(stat.get("id")) == str(glossary_id)
                    ),
                    {}
                )
            else:
                glossary = {}
        return glossary
    except Exception as e:
        log_error(
            f"Alation Connector: Get Glossary Stats ", str(e))
        raise e

def get_glossary_measure_count(config, glossary_id):
    measures = 0
    try:
        """
        Get domain details by domain id
        """
        if not glossary_id:
            return measures

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            # get the domain by name
            query_string = f"""
                select 
                    count(distinct measure.id)
                from core.measure
                join core.connection on connection.id=measure.connection_id
                left join core.asset on asset.id = measure.asset_id and asset.is_active=true and asset.is_delete=false               
                left join core.attribute on attribute.id = measure.attribute_id and attribute.is_selected=true
                left join core.domain_mapping on (domain_mapping.asset_id = asset.id or  domain_mapping.measure_id = measure.id)
                where domain_mapping.domain_id in  (
                            WITH RECURSIVE tree AS (
                                SELECT id
                                FROM core.domain
                                WHERE id = '{glossary_id}'
                                UNION ALL
                                SELECT r.id
                                FROM core.domain r
                                JOIN tree c ON r.parent_id = c.id
                            )
                            select id from tree
                        )
                    
                and connection.is_active=true and measure.is_active = true and measure.is_delete = false
                and ((measure.attribute_id is null and attribute.id is null) or (measure.attribute_id is not null and attribute.is_selected = true))
            """
            cursor = execute_query(connection, cursor, query_string)
            measures = fetchone(cursor)
            measures = measures.get('count', 0) if measures else 0
        return measures
    except Exception as e:
        log_error(
            f"Get Glossary measures count ", str(e))
        raise e

def get_domain_metrics(config, gl_id):
    metrics = {}
    try:
        """
        Get Domain metrics
        """
        if not gl_id:
            return metrics

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            # get the domain
            query_string = f"""
                WITH RECURSIVE domain_list
                    AS (
                        SELECT id
                            ,parent_id
                            ,name
                            ,technical_name
                        FROM core.domain
                        WHERE id = '{gl_id}'
                        UNION ALL
                        SELECT t.id
                            ,t.parent_id
                            ,t.name
                            ,t.technical_name
                        FROM core.domain t
                        JOIN domain_list ON t.parent_id = domain_list.id
                        )
                        ,asset_count
                    AS (
                        SELECT count(DISTINCT a.id) AS asset_count
                            ,count(DISTINCT CASE 
                                    WHEN lower(a.type) IN (
                                            'base table'
                                            ,'table'
                                            ,'external table'
                                            )
                                        THEN a.id
                                    ELSE NULL
                                    END) AS tables
                            ,count(DISTINCT CASE 
                                    WHEN lower(a.type) = 'view'
                                        THEN a.id
                                    ELSE NULL
                                    END) AS VIEWS
                            ,count(DISTINCT CASE 
                                    WHEN lower(a.GROUP) = 'pipeline'
                                        THEN a.id
                                    ELSE NULL
                                    END) AS pipelines
                            ,count(DISTINCT CASE 
                                    WHEN lower(a.GROUP) = 'report'
                                        THEN a.id
                                    ELSE NULL
                                    END) AS reports
                        FROM domain_list dh
                        JOIN core.domain_mapping dm ON dh.id = dm.domain_id
                        JOIN core.asset a ON dm.asset_id = a.id
                        JOIN core.connection c ON c.id = a.connection_id
                        WHERE a.is_active = true
                            AND a.is_delete = false
                            AND c.is_active = true
                            AND c.is_delete = false
                        )
                        ,attribute_count
                    AS (
                        SELECT count(DISTINCT attribute.id)
                        FROM domain_list dh
                        JOIN core.domain_mapping dm ON dh.id = dm.domain_id
                        JOIN core.asset a ON dm.asset_id = a.id
                        JOIN core.connection c ON c.id = a.connection_id
                        JOIN core.attribute ON attribute.asset_id = a.id
                            AND attribute.is_selected = true
                        WHERE a.is_active = true
                            AND a.is_delete = false
                            AND c.is_active = true
                            AND c.is_delete = false
                        )
                        ,measures_count
                    AS (
                        SELECT count(DISTINCT m.id) AS measure_count
                        FROM domain_list dh
                        JOIN core.domain_mapping dm ON dh.id = dm.domain_id
                        JOIN core.measure m ON (
                                dm.measure_id = m.id
                                OR dm.asset_id = m.asset_id
                                )
                        JOIN core.connection c ON c.id = m.connection_id
                        LEFT JOIN core.asset ON asset.id = dm.asset_id
                        WHERE m.is_active = true
                            AND m.is_delete = false
                            AND (
                                asset.id IS NULL
                                OR asset.is_active = true
                                AND asset.is_delete = false
                                )
                            AND c.is_active = true
                            AND c.is_delete = false
                        )
                        ,domain_score
                    AS (
                        SELECT (sum(metrics.score * metrics.weightage) / sum(metrics.weightage)) AS score
                        FROM domain_list dh
                        JOIN core.domain_mapping dm ON dh.id = dm.domain_id
                        JOIN core.measure m ON (
                                dm.measure_id = m.id
                                OR dm.asset_id = m.asset_id
                                )
                        JOIN core.connection c ON c.id = m.connection_id
                        JOIN core.metrics metrics ON metrics.measure_id = m.id
                            AND m.last_run_id = metrics.run_id
                        WHERE metrics.allow_score = true
                            AND metrics.is_measure = true
                            AND c.is_active = true
                            AND c.is_delete = false
                            AND metrics.measure_id IN (
                                SELECT DISTINCT mes.id
                                FROM core.measure AS mes
                                LEFT JOIN core.attribute AS attr ON attr.id = mes.attribute_id
                                LEFT JOIN core.asset AS ast ON ast.id = mes.asset_id
                                LEFT JOIN core.connection AS conn ON conn.id = mes.connection_id
                                WHERE mes.id = metrics.measure_id
                                    AND mes.is_active = true
                                    AND mes.is_delete = false
                                    AND (
                                        (
                                            ast.is_active = true
                                            AND ast.is_delete = false
                                            AND lower(ast.STATUS) != 'deprecated'
                                            )
                                        OR (
                                            ast.is_active = true
                                            AND ast.is_delete = false
                                            AND lower(ast.STATUS) != 'deprecated'
                                            AND attr.is_active = true
                                            AND attr.is_delete = false
                                            AND lower(attr.STATUS) != 'deprecated'
                                            )
                                        OR (
                                            conn.id IS NOT NULL
                                            AND ast.id IS NULL
                                            AND attr.id IS NULL
                                            )
                                        )
                                )
                        )
                        ,user_counts
                    AS (
                        SELECT count(DISTINCT u.id) AS users
                        FROM domain_list dh
                        JOIN core.user_mapping um ON dh.id = um.domain_id
                        JOIN core.users u ON u.id = um.user_id
                        WHERE u.is_active = true
                        )
                        ,alert_issue_counts
                    AS (
                        SELECT count(DISTINCT (issues.id)) AS issue_count
                            ,count(DISTINCT (
                                    CASE 
                                        WHEN lower(metrics.drift_status) IN (
                                                'high'
                                                ,'medium'
                                                ,'low'
                                                )
                                            THEN metrics.id
                                        ELSE NULL
                                        END
                                    )) AS alert_count
                        FROM domain_list dh
                        JOIN core.domain_mapping dm ON dh.id = dm.domain_id
                        JOIN core.measure ON (
                                dm.measure_id = measure.id
                                OR dm.asset_id = measure.asset_id
                                )
                        JOIN core.connection c ON c.id = measure.connection_id
                        LEFT JOIN core.issues ON issues.measure_id = measure.id
                            AND issues.is_active = true
                        LEFT JOIN core.metrics ON metrics.measure_id = measure.id
                            AND metrics.run_id = measure.last_run_id
                            AND lower(metrics.drift_status) IN (
                                'high'
                                ,'medium'
                                ,'low'
                                )
                        WHERE measure.is_active = true
                            AND measure.is_delete = false
                            AND c.is_active = true
                            AND c.is_delete = false
                        )
                    SELECT dh.name
                        ,ac.asset_count
                        ,mc.measure_count
                        ,ds.score
                        ,ac.tables
                        ,ac.VIEWS
                        ,ac.pipelines
                        ,ac.reports
                        ,attribute.count AS attributes
                        ,uc.*
                        ,alert_issue_counts.*
                    FROM asset_count ac
                    CROSS JOIN measures_count mc
                    CROSS JOIN domain_score ds
                    CROSS JOIN attribute_count attribute
                    CROSS JOIN user_counts uc
                    CROSS JOIN alert_issue_counts
                    CROSS JOIN domain_list dh
            """
            cursor = execute_query(connection, cursor, query_string)
            glossary = fetchone(cursor)
            # glossary stats
            stats = get_glossary_stats(config, gl_id)
            measures = get_glossary_measure_count(config, gl_id)

            users = glossary.get('users', 0) if glossary else 0

            metrics = {
                **stats,
                "name": glossary.get('name'),
                "id": gl_id,
                "tables": glossary.get('tables', 0),
                "views": glossary.get('views', 0),
                "pipeline": glossary.get('pipeline', 0),
                "reports": glossary.get('reports', 0),
                "measures": measures,
                "alerts": glossary.get('alert_count'),
                "score": str(glossary.get('score')),
                "issues": glossary.get('issue_count'),
                "users": users,
                "attributes": glossary.get('attributes', 0) if glossary.get('attributes', 0) else 0
            }

        return metrics
    except Exception as e:
        log_error(
            f"Get Domain Mertics ", str(e))
        raise e

def get_product_metrics(config, product_id):
    """
    Get Product metrics
    """
    metrics = {}
    try:
        if not product_id:
            return metrics

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            # get the domain by name
            query_string = f"""
            WITH RECURSIVE product_list AS (
                SELECT id, parent_id,name,technical_name
                FROM core.product
                WHERE id='{product_id}'
                UNION ALL
                SELECT t.id, t.parent_id,t.name,t.technical_name
                FROM core.product t
                JOIN product_list ON t.parent_id = product_list.id
            )
        
                    ,asset_count as (
                        select
                            count(distinct a.id) as asset_count,
                            count(distinct case when lower(a.type) IN ('base table', 'table', 'external table', 'asset', 'file', 'folder')  then a.id else null end) as tables,
                            count(distinct case when lower(a.type) = 'view' then a.id else null end) as views,
                            count(distinct case when lower(a.group) = 'pipeline' then a.id else null end) as pipelines,
                            count(distinct case when lower(a.group) = 'report' then a.id else null end) as reports,
                            count(distinct case when lower(a.type) = 'query' then a.id else null end) as query
                        from product_list dh
                        join core.product_mapping dm on dh.id = dm.product_id
                        join core.asset a on dm.asset_id = a.id 
                        join core.connection con on con.id = a.connection_id
                         where  
                            con.is_active = true and con.is_delete = false
                            and a.is_active = true and a.is_delete = false and a.status != 'Deprecated'
                    ), attribute_count as (
                        select count(distinct attribute.id) 
                        from product_list dh
                        join core.product_mapping dm on dh.id = dm.product_id
                        join core.asset a on dm.asset_id = a.id 
                        join core.attribute on attribute.asset_id=a.id and attribute.is_selected=true
                        join core.connection con on con.id = a.connection_id
                        where 
                            con.is_active = true and con.is_delete = false
                            and a.is_active = true and a.is_delete = false and a.status != 'Deprecated'
                            and attribute.is_active = true and attribute.is_delete = false and attribute.status != 'Deprecated'
                    ), measures_count as (
                        select count(distinct m.id) AS measure_count
                        from product_list dh
                        join core.product_mapping dm on dh.id = dm.product_id
                        left join core.measure m on (dm.measure_id = m.id or dm.asset_id=m.asset_id)
                        left join core.asset on asset.id=dm.asset_id
                        left join core.connection con on con.id = m.connection_id
                        where 
                            con.is_active = true and con.is_delete = false
                            and m.is_active = true AND m.is_delete = false
                            and (asset.id is null or asset.is_active=true and asset.is_delete=false and asset.status != 'Deprecated')
                    ), product_score AS (
                        select 
                            (sum(metrics.score * metrics.weightage) / sum(metrics.weightage)) as score
                        from  product_list dh
                        join core.product_mapping dm on dh.id = dm.product_id
                        join core.measure m on (dm.measure_id = m.id or dm.asset_id = m.asset_id)
                        join core.metrics metrics on metrics.measure_id = m.id and m.last_run_id = metrics.run_id
                        where  metrics.allow_score = true and metrics.is_measure = true 
                        and metrics.measure_id in (
                        select distinct mes.id from core.measure as mes
                        left join core.attribute as attr on attr.id = mes.attribute_id 
                        left join core.asset as ast on ast.id = mes.asset_id 
                        left join core.connection as conn on conn.id = mes.connection_id
                        where 
                            mes.id = metrics.measure_id and mes.is_active = true and mes.is_delete = false  and mes.status != 'Deprecated'
                            and (conn.is_active = true and conn.is_delete = false )
                            and ((ast.is_delete = false and ast.is_active = true  and ast.status != 'Deprecated') or ast.is_delete is null)
                            and ((attr.is_selected = true and attr.is_delete = false   and attr.status != 'Deprecated') or attr.is_delete is null)
                    )
                    ), user_counts as (
                        select array_remove(array_agg(DISTINCT u.id), NULL) AS users
                        from core.product dh
                        join core.user_mapping um on dh.id = um.product_id
                        join core.users u on u.id = um.user_id
                        where u.is_active = true and dh.id = '{product_id}'
                    ),
                    alert_issue_counts as (
                        select  
                        count(distinct(issues.id)) as issue_count, 
                        count(distinct (case when lower(metrics.drift_status) in ('high', 'medium', 'low') then metrics.id else null end)) as alert_count
                        from product_list dh
                        join core.product_mapping dm on dh.id = dm.product_id
                        join core.measure on (
                            dm.measure_id = measure.id
                            or dm.asset_id = measure.asset_id
                        )
                        left join core.issues on issues.measure_id=measure.id and issues.is_active=true
                        left join core.metrics on metrics.measure_id=measure.id 
                            and metrics.run_id=measure.last_run_id 
                            and lower(metrics.drift_status) in ('high','medium','low')
                        left join core.asset on asset.id=measure.asset_id
                        left join core.connection con on con.id = measure.connection_id
                        where 
                            measure.is_active=true and measure.is_delete=false and measure.status != 'Deprecated'
                            and con.is_active = true and con.is_delete = false
                            and (asset.id is null or asset.is_active=true and asset.is_delete=false and asset.status != 'Deprecated')
                        )
                    select  ac.asset_count, mc.measure_count, ds.score, ac.query, ac.tables, ac.views, ac.pipelines, ac.reports, 
                      attribute.count as attributes, uc.*, alert_issue_counts.*
                    from asset_count ac
                    cross join measures_count mc
                    cross join product_score ds 
                    cross join attribute_count attribute
                    cross join user_counts uc
                    cross join alert_issue_counts
            """
            cursor = execute_query(connection, cursor, query_string)
            metrics = fetchone(cursor)
        return metrics
    except Exception as e:
        log_error(
            f"Get Product Metrics ", str(e))
        raise e