import json

# Import Helpers
from dqlabs.app_helper.dq_helper import get_client_origin, format_freshness
from dqlabs.app_helper.db_helper import execute_query, fetchone
from dqlabs.app_helper.dag_helper import get_postgres_connection

# Import Constant
from dqlabs.app_constants.dq_constants import CREATE_ALERT, CREATE_ISSUE, JOB_EXECUTION

def prepare_notification_payload(config: dict, input: dict, task_name: str) -> dict:
    """
    Prepare Notification Payload
    """
    payload = None
    dag_info = config.get("dag_info")
    client_link = get_client_origin(dag_info)

    is_groupby_alert = False
    if input.get("alert_type") == "groupby_alert":
        is_groupby_alert = True
    if task_name == CREATE_ISSUE:
        link = f"""{client_link}/remediate/issues/{input.get("id") if input.get("id") else input.get("issue_id")}"""
        payload = {
            "to": [],
            "params": {
                "link": link,
                "title": input.get("issue_id"),
                "status": input.get("status"),
                "message": input.get("name"),
                "issue_id": input.get("id") if input.get("id") else input.get("issue_id"),
                "priority": input.get("priority"),
                "asset_name": input.get("asset_name") if input.get("asset_name") else input.get("asset"),
                "attribute_name": input.get("attribute_name") if input.get("attribute_name") else input.get("attribute"),
                "subject_description": "Create the issue"
            },
            "owner_name": "",
            "subject_description": input.get("name")
        }
    elif task_name == CREATE_ALERT:
        expected = ""
        measure_name = input.get("measure_name", "")
        threshold = input.get("threshold", {})
        if isinstance(threshold, str) and threshold:
            threshold = json.loads(threshold)
        actual = input.get("value", "")
        if threshold:
            lThreshold = threshold.get("lower_threshold", "")
            uThreshold = threshold.get("upper_threshold", "")

            if measure_name and measure_name.lower() == "freshness":
                lThreshold = format_freshness(
                    int(float(lThreshold))) if lThreshold else ""
                uThreshold = format_freshness(
                    int(float(uThreshold))) if uThreshold else ""
                actual = format_freshness(int(float(actual))) if actual else ""

            expected = f"""{lThreshold} - {uThreshold}"""

        alert_id = input.get("metrics", "")
        measure_id = input.get("measure", "")
        priority = input.get("priority", "")
        connection_name = input.get("connection_name")
        alert_measure_name = input.get("alert_measure_name", "")

        if "threshold_data" in threshold:
            filtered_expected = next(
                    (
                        val
                        for val in threshold["threshold_data"]
                        if (
                            str(val.get("priority")).lower() == str(priority).lower()
                        )
                    ),
                    {},
                )
            expected = get_expected_manual_threshold(filtered_expected)

        # Pipeline Related Content
        default_na = "NA"
        job_name = input.get("job_name")
        task_name = input.get("task_name") if input.get("task_name") else default_na
        task_id = input.get("pipeline_task_id") if input.get("pipeline_task_id") else input.get("task_id")
        task_id = str(task_id) if task_id else ""
        error_message = input.get("task_error")
        error_message = error_message if error_message else default_na
        task_status = input.get("task_status")
        task_status_title = (
                        "failure"
                        if task_status and str(task_status).lower() == "failed"
                        else task_status
                    )
        tags = input.get("tags_tags", [])
        tags = json.loads(tags) if tags and isinstance(tags, str) else tags
        tags = tags if tags else []
        tag_names = ", ".join(tags) if tags else default_na
        tag_names = tag_names if tag_names else default_na
        run_time = input.get("run_end_at")
        run_time = run_time.strftime("%m-%d-%y %H:%m:%S") if run_time else default_na

        dag_info = config.get("dag_info")
        client_link = get_client_origin(dag_info)
        asset_link =  f"""{client_link}/observe/data/{input.get("asset")}""" if input.get("asset") else ""
        attribute_link =  (f"""{client_link}/observe/data/{input.get("asset")}/attributes/{input.get("attribute")}""" 
                    if input.get("attribute") else "")

        if task_id:
            client_link = f"""{client_link}/alerts/{task_id}?date_filter=All&external=true"""
        else:
            client_link = f"""{client_link}/measure/{measure_id}/detail?measure_name={alert_measure_name}&alert_id={alert_id}"""
        
        asset_name = input.get("asset_name") if input.get("asset_name") else ""
        attribute_name = input.get("attribute_name") if input.get("attribute_name") else default_na
        if is_groupby_alert:
            alert_data = get_groupby_alert_metric(config, alert_id) if alert_id else {}
        else:
            alert_data = get_alert_metric(config, alert_id) if alert_id else {}
        measure_type = alert_data.get("Measure_Type", "")
        measure_category = alert_data.get("Measure_Category", "")
        threshold_type = alert_data.get("Threshold_Type", "")
        database = alert_data.get("Database", "")
        schema = alert_data.get("Schema", "")

        if input.get("asset_name") is None and input.get("attribute_name") is None and measure_name is not None:
            fully_qualified_name = f"{connection_name}.{measure_name}"
            asset_link = client_link
        else:
            fully_qualified_name = f"{connection_name}.{database}.{schema}.{asset_name}"

        asset_name = input.get("asset_name") if input.get("asset_name") else ""
        attribute_name = input.get("attribute_name") if input.get("attribute_name") else default_na
        measure_type = alert_data.get("Measure_Type", "")
        measure_category = alert_data.get("Measure_Category", "")
        threshold_type = alert_data.get("Threshold_Type", "")
        database = alert_data.get("Database", "")
        schema = alert_data.get("Schema", "")

        if input.get("asset_name") is None and input.get("attribute_name") is None and measure_name is not None:
            fully_qualified_name = f"{connection_name}.{measure_name}"
            asset_link = client_link
        else:
            fully_qualified_name = f"{connection_name}.{database}.{schema}.{asset_name}"

        payload = {
            "to": [],
            "params": {
                "title": input.get("name", ""),
                "message": input.get("name", ""),
                "link": client_link,
                "asset_name": asset_name,
                "attribute_name": attribute_name,
                "measure_id": measure_id,
                "alert_id": alert_id,
                "measure_name": measure_name,
                "priority": priority,
                "expected": expected,
                "actual": actual,
                "connection_name": connection_name,
                "job_name": job_name,
                "task_name": task_name,
                "task_status": task_status,
                "task_status_title": task_status_title,
                "run_time": run_time,
                "tags": tag_names,
                "error": error_message,
                "measure_category": measure_category,
                "measure_type": measure_type,
                "trigger_type": threshold_type,
                "asset_link": asset_link,
                "attribute_link": attribute_link,
                "fully_qualified_name": fully_qualified_name,
            },
        }
    elif task_name == JOB_EXECUTION:
        measure_link = f"""{client_link}/measure/{input.get("measure_id")}/detail""" if input.get("measure_id") else ""
        asset_link =  f"""{client_link}/observe/data/{input.get("asset_id")}""" if input.get("asset_id") else ""
        attribute_link =  (f"""{client_link}/observe/data/{input.get("asset_id")}/attributes/{input.get("attribute_id")}""" 
                    if input.get("attribute_id") else "")
        link = f"""{client_link}/settings/connect/logs?queue_id={input.get('queue_id')}"""
        payload = {
            "to": [],
            "params": {
                "job_name": input.get("job_name").upper(),
                "link": link,
                "connection_name": input.get("connection_name"),
                "asset_name": input.get("asset_name", ""),
                "attribute_name": input.get("attribute_name", ""),
                "measure_name": input.get("measure_name", ""),
                "status": input.get("status"),
                "job_type":  input.get("job_type"),
                "error": input.get("error"),
                "asset_link": asset_link,
                "measure_link": measure_link,
                "attribute_link": attribute_link,
                "start_time": input.get("start_time"),
                "end_time": input.get("end_time")
            },
        }
    return payload


def get_bigpanda_issue(config: dict, issue_id: str):
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            select connection.name as "Connection_Name", asset.name as "Asset_Name", attribute.technical_name as "Attribute_Name",  
            base_measure.name as "Measure_Name", base_measure.type  as "Measure_Type", base_measure.level as "Measure_Level",
            case when i.status='Resolved' then 'ok'
            when i.priority='High' then 'critical'
            when i.priority='Medium' then 'warning'
            when i.priority='Low' then 'unknown'
            else 'ok' end as status, metrics.message as "Alert_Description", 
            i.issue_id as "Issue_ID", i.name as "Issue_Name", i.description as "Issue_Description", i.status as "Issue_Status", i.priority as "Issue_Priority",
                string_agg(DISTINCT case when i.created_by_id in (users.id) then concat(users.first_name, ' ',users.last_name) end, ', ') as Assignee, 
            string_agg(DISTINCT case when i.created_by_id in (users.id) then concat(users.first_name, ' ',users.last_name) end, ', ') as Reported_by, 
            metrics.threshold->>'lower_threshold' as "Lower_Threshold",
            metrics.threshold->>'upper_threshold' as "Upper_Threshold", 
            concat(metrics.threshold->>'lower_threshold',' - ', metrics.threshold->>'upper_threshold') as "Expected", 
            metrics.value as "Actual", metrics.percent_change as "Percent_Change",
            metrics.deviation as "Deviation",
            string_agg(distinct(application.name),', ') as "Application", string_agg(distinct(domain.name),', ') as "Domain"
            from core.issues i
            join core.metrics on metrics.id=i.metrics_id
            left join core.measure on measure.id=i.measure_id
            join core.connection on metrics.connection_id=connection.id
            left join core.asset on i.asset_id=asset.id
            left join core.attribute on attribute.id=i.attribute_id
            left join core.version on version.asset_id=asset.id
            left join core.application_mapping on application_mapping.measure_id=measure.id
            left join core.application on application.id=application_mapping.application_id
            left join core.domain_mapping on domain_mapping.measure_id=measure.id
            left join core.domain on domain.id=domain_mapping.domain_id
            left join core.product_mapping on product_mapping.measure_id=measure.id
            left join core.product on product.id=product_mapping.product_id
            left join core.base_measure on measure.base_measure_id=base_measure.id
            left join core.users on users.id=i.created_by_id
            where i.id='{issue_id}'
            group by connection.name,asset.name,attribute.technical_name,base_measure.name,
            base_measure.type,base_measure.level,metrics.status,metrics.message,
            i.issue_id, i.name, i.description, i.status, i.priority,
            metrics.threshold,metrics.threshold,metrics.value,metrics.percent_change,metrics.deviation
        """
        cursor = execute_query(connection, cursor, query_string)
        issue_data = fetchone(cursor)
    return issue_data

def get_alert_metric(config: dict, alert_id: str):
    connection = get_postgres_connection(config)
    response = None
    with connection.cursor() as cursor:
        query_string = f"""
            select connection.name as "Connection_Name", asset.name as "Asset_Name", attribute.technical_name as "Attribute_Name", 
            string_agg(distinct(application.name),', ') as "Application", string_agg(distinct(domain.name),', ') as "Domain", 
            base_measure.name as "Measure_Name", base_measure.type  as "Measure_Type", base_measure.level as "Measure_Level",
            base_measure.category as "Measure_Category", case when measure.is_auto = true then 'auto' else 'manual' end as "Threshold_Type",
            asset.properties->>'schema' as "Schema",asset.properties->>'database' as "Database",
            case when metrics.drift_status='High' then 'critical'
            when metrics.drift_status='Medium' then 'warning'
            when metrics.drift_status='Low' then 'unknown'
            else 'ok' end as status, metrics.message as "Alert_Description", metrics.threshold->>'lower_threshold' as "Lower_Threshold",
            metrics.threshold->>'upper_threshold' as "Upper_Threshold", 
            concat(metrics.threshold->>'lower_threshold',' - ', metrics.threshold->>'upper_threshold') as "Expected", 
            metrics.value as "Actual", metrics.percent_change as "Percent_Change",
            metrics.deviation as "Deviation", metrics.measure_id as "Measure_ID"
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
            where metrics.id='{alert_id}'
            group by connection.name,asset.name,asset.properties, attribute.technical_name,base_measure.name,
            base_measure.type,base_measure.level,base_measure.category,measure.is_auto,metrics.drift_status,
            metrics.message,metrics.threshold,metrics.threshold,metrics.value,metrics.percent_change,
            metrics.percent_change,metrics.deviation,metrics.measure_id
        """
        cursor = execute_query(connection, cursor, query_string)
        alert = fetchone(cursor)
        if alert:
            if alert.get("Measure_Name", '') == 'Freshness':
                lt = format_freshness(
                    alert.get("Lower_Threshold", '0'))
                ut = format_freshness(
                    alert.get("Upper_Threshold", '0'))
                formatted_data = {
                    "Lower_Threshold": lt,
                    "Upper_Threshold": ut,
                    "Expected": f"{lt} - {ut}",
                    "Actual": format_freshness(alert.get("Actual", '0'))
                }
                alert = {**alert, **formatted_data}
            response = alert
    return response

def get_groupby_alert_metric(config: dict, alert_id: str):
    connection = get_postgres_connection(config)
    response = None
    with connection.cursor() as cursor:
        query_string = f"""
            select distinct gbm.id, gbm.message as "Alert_Description", gbm.drift_status as status, gbm.value, 
            gbm.threshold, gbm.measure_id, gbm.attribute_id, gbm.asset_id, mes.organization_id, 
            base.name as "Measure_Name", attr.name as "Attribute_Name", asset.name as "Asset_Name", asset.group as "Asset_Group",
            asset.properties as "Asset_Properties", mes.is_auto,
            gbm.measure_name as "Alert_Measure_Name", gbm.groupby_key,
            base.description as "Measure_Description",
            (CASE 
                WHEN base.type != 'custom' THEN 'Auto'
                WHEN base.type = 'custom' AND mes.asset_id IS NULL THEN 'Standalone'
                WHEN base.type = 'custom' AND mes.asset_id IS NOT NULL THEN 'Custom'
            END) AS "Measure_Type", base.type as "Measure_Category",
            (case
                when base.properties ->> 'enable_manual_threshold' = 'true' then 'Manual'
                else 'ML Based'
            end) as "Trigger_Type",
            (case 
                when lower(gbm.drift_status) = 'high'  then 'critical'
                when lower(gbm.drift_status) = 'medium'  then 'warning'
                when lower(gbm.drift_status) = 'low'  then 'unknown'
                else 'ok'
            end) as status, gbm.drift_status as "Priority",
            connection.name as "Connection_Name",
            gbm.threshold->>'lower_threshold' as "Lower_Threshold",
            gbm.threshold->>'upper_threshold' as "Upper_Threshold", 
            concat(gbm.threshold->>'lower_threshold',' - ', gbm.threshold->>'upper_threshold') as "Expected", 
            gbm.value as "Actual", gbm.percent_change as "Percent_Change",
            gbm.deviation as "Deviation", gbm.measure_id as "Measure_ID"
            from core.groupby_metrics as gbm
            join core.measure as mes on mes.id=gbm.measure_id
            join core.connection on connection.id=mes.connection_id
            join core.base_measure as base on base.id=mes.base_measure_id
            left join core.asset as asset on asset.id=gbm.asset_id and asset.is_active = true and asset.is_delete = false
            left join core.attribute as attr on attr.id = gbm.attribute_id
            where lower(gbm.drift_status) != 'ok'
            and mes.is_notification = true and mes.is_active = true
            and gbm.id = '{alert_id}'
        """
        cursor = execute_query(connection, cursor, query_string)
        alert = fetchone(cursor)
        if alert:
            response = alert
    return response


def get_expected_manual_threshold(threshold_constraints):
    if not threshold_constraints:
        return ""

    condition = threshold_constraints.get("condition")
    value = threshold_constraints.get("value", "")
    value2 = threshold_constraints.get("value2", "")
    condition_map = {
        "isGreaterThan": lambda: f"> {value}",
        "isLessThan": lambda: f"< {value}",
        "isGreaterThanOrEqualTo": lambda: f">= {value}",
        "isLessThanOrEqualTo": lambda: f"<= {value}",
        "isEqualTo": lambda: f"== {value}",
        "isBetween": lambda: f"{value} <> {value2}",
        "isNotBetween": lambda: f"{value} </> {value2}",
    }

    return condition_map.get(condition, lambda: "")()