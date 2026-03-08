from uuid import uuid4
from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.db_helper import execute_query, fetchone, fetchall


def get_alert_metrics(config, alerts_data):
    alerts_list = []
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            alerts_data = alerts_data[0]
            for alerts in alerts_data:
                alert_id = alerts.split("'")[1]
                run_id = alerts.split("'")[3]
                query_string = f"""
                select connection.name as "Connection_Name", asset.name as "Asset_Name", attribute.technical_name as "Attribute_Name", 
                string_agg(distinct(application.name),', ') as "Application", string_agg(distinct(domain.name),', ') as "Domain", 
                base_measure.name as "Measure_Name", base_measure.type  as "Measure_Type", base_measure.level as "Measure_Level",
                case when metrics.drift_status='High' then 'critical'
                when metrics.drift_status='Medium' then 'warning'
                when metrics.drift_status='Low' then 'unknown'
                else 'ok' end as status, metrics.message as "Alert_Description", metrics.threshold->>'lower_threshold' as "Lower_Threshold",
                metrics.threshold->>'upper_threshold' as "Upper_Threshold", 
                concat(metrics.threshold->>'lower_threshold',' - ', metrics.threshold->>'upper_threshold') as "Expected", 
                metrics.value as "Actual", metrics.percent_change as "Percent_Change", metrics.attribute_id as "Attribute_Id", metrics.asset_id as "Asset_Id",
                metrics.deviation as "Deviation", metrics.measure_id as "Measure_ID", metrics.id as "Metrics_Id", metrics.created_date as "Created_Date"
                from core.metrics 
                join core.connection on metrics.connection_id=connection.id
                left join core.asset on metrics.asset_id=asset.id
                left join core.attribute on attribute.id=metrics.attribute_id
                left join core.application_mapping on application_mapping.asset_id=asset.id
                left join core.application on application.id=application_mapping.application_id
                left join core.domain_mapping on domain_mapping.asset_id=asset.id
                left join core.domain on domain.id=domain_mapping.domain_id
                left join core.measure on measure.id=metrics.measure_id
                left join core.base_measure on measure.base_measure_id=base_measure.id
                where metrics.measure_id='{alert_id}' and metrics.run_id='{run_id}'
                group by connection.name,asset.name,attribute.technical_name,base_measure.name,
                base_measure.type,base_measure.level,metrics.drift_status,metrics.message,
                metrics.threshold,metrics.threshold,metrics.value,metrics.percent_change,
                metrics.percent_change,metrics.deviation,metrics.measure_id,metrics.id
                """
                cursor = execute_query(connection, cursor, query_string)
                alert_info = fetchone(cursor)
                if alert_info:
                    alerts_list.append(alert_info)
    except Exception as e:
        print(f"Error fetching alert metrics: {e}")
    return alerts_list


def map_corelated_alerts(config, drift_alerts_input):
    """
    Map corelated alerts to their respective alert IDs.

    Args:
        alerts (list): List of alert dictionaries.

    Returns:
        dict: Dictionary mapping alert IDs to their respective corelated alerts.
    """
    check_last_run = 3
    corelated_alerts = get_alert_metrics(config, drift_alerts_input)
    for alert in corelated_alerts:
        recent_alert_created_date = alert.get("Created_Date")
        alert_id = alert.get("Metrics_Id")
        asset_id = alert.get("Asset_Id")
        measure_id = alert.get("Measure_ID")
        attribute_id = alert.get("Attribute_Id")
        if not asset_id or not measure_id or not attribute_id:
            continue
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            # Fetch the related issue for the alert
            related_issue_query = f"""
            select issues.*, metrics.created_date as alert_created
                from 
                core.issues 
                left join core.metrics on metrics.id=issues.metrics_id
                
                where issues.status in ('New', 'Inprogress') 
                and issues.is_active='true' and issues.is_delete='false' and
                issues.asset_id='{asset_id}' and issues.attribute_id='{attribute_id}' and issues.measure_id='{measure_id}'
            ORDER BY 
                created_date DESC
            limit 1
            """
            cursor = execute_query(connection, cursor, related_issue_query)
            related_issue = fetchone(cursor)
            if related_issue:
                alert_created_date = related_issue.get("alert_created")
                # Get last n runs of the metrics with comparable target alert created date
                query_string = f"""
                    WITH ordered_metrics AS (
                        SELECT 
                            id, created_date,
                            ROW_NUMBER() OVER (ORDER BY created_date DESC) as row_num
                        FROM 
                            core.metrics
                        WHERE 
                            measure_id = '{measure_id}'
                    ),
                    target_row AS (
                        SELECT 
                            row_num
                        FROM 
                            ordered_metrics
                        WHERE 
                            id = '{related_issue.get("metrics_id")}' and
                            created_date = '{alert_created_date}'
                    )
                    SELECT 
                        id
                    FROM 
                        ordered_metrics
                    WHERE 
                        row_num BETWEEN (SELECT row_num FROM target_row) - {check_last_run}
                        AND (SELECT row_num FROM target_row) 
                    ORDER BY 
                        created_date DESC;
                """
                cursor = execute_query(connection, cursor, query_string)
                metrics_result = fetchall(cursor)
                # Check if the alert is within the last n runs
                if metrics_result:
                    for metric in metrics_result:
                        metric_id = metric.get("id")
                        if metric_id == alert_id:
                            add_associated_issue(related_issue, metric, connection)
                            break


def add_associated_issue(issue, metric, connection):
    """
    Adds an associated issue to the database.
    """

    query = """
        INSERT INTO associated_issues (
            id,
            issue_number,
            issue_id,
            measure_id,
            attribute_id,
            metrics_id,
            asset_id,
            connection_id,
            created_date
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
    """
    values = (
        str(uuid4()),
        issue.get("issue_id"),
        issue.get("id"),
        metric.get("measure_id"),
        metric.get("attribute_id"),
        metric.get("id"),
        metric.get("asset_id"),
        metric.get("connection_id"),
    )
    try:
        with connection.cursor() as cursor:
            cursor = execute_query(connection, cursor, query, values)
    except Exception as e:
        print(f"Failed to add associated issue: {e}")
