"""
    Migration Notes From V2 to V3:
    Migrations Completed 
"""


import requests
import os
import json
import re
from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.db_helper import execute_query, fetchone
from dqlabs.app_helper.log_helper import log_error
from dqlabs.app_helper.crypto_helper import decrypt
from dqlabs.app_helper.dq_helper import get_client_origin, format_freshness



def get_servicenow_config(config):
    is_active = False
    channel_config = {}
    connection = get_postgres_connection(config)
    organization_id = config.get("organization_id")
    with connection.cursor() as cursor:
        query_string = f"""
            select ch.technical_name, ic.is_active, ic.config from core.channels ch
            join core.integrations ic ON ic.channel_id = ch.id
            where ic.organization_id = '{organization_id}' 
            and ch.type = 'servicenow'
        """
        cursor = execute_query(connection, cursor, query_string)
        channel_config = fetchone(cursor)
        if channel_config:
            is_active = channel_config.get("is_active")
            channel_config = channel_config.get("config")
    return is_active, channel_config

def get_servicenow_password_response(params: dict = {}, config: dict = {}, endpoint: str = '', methode: str = "post"):
    
    instance_url = config.get("url")
    username = decrypt(config.get("username"))
    password = decrypt(config.get("password"))

    # Prepare Headers and Params
    api_headers = {
        "Content-Type": "application/json",
        "Accept": "*/*",
    }
    if instance_url and instance_url.endswith("/"):
        instance_url = "/".join(instance_url.split("/")[:-1])

    if endpoint:
        endpoint = f"{instance_url}{endpoint}"

    data = {}
    if params:
        data = params
    if methode == 'post':
        response = requests.post(
            url=endpoint, auth=requests.auth.HTTPBasicAuth(username, password), headers=api_headers, json=data)
    elif methode == 'patch':
        response = requests.patch(
            endpoint, auth=requests.auth.HTTPBasicAuth(username, password), headers=api_headers, json=data)
    return response

def get_access_token_from_refresh_token(config: dict):
    # Token endpoint URL
    instance_url = config.get("url")
    client_id = decrypt(config.get("client_id"))
    client_secret = decrypt(config.get("client_secret"))
    refresh_token = decrypt(config.get("refresh_token"))
    token_url = f"{instance_url}/oauth_token.do"

    # Prepare the data to exchange refresh token for a new access token
    data = {
        "grant_type": "refresh_token",
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token
    }

    try:
        # Request to get the new access token
        response = requests.post(token_url, data=data)
        if response.status_code == 200:
            token_data = response.json()
            access_token = token_data.get("access_token")
            return access_token
        else:
            log_error(f"Failed to get access token. Status code: {response.status_code}")
            log_error("Error response: ", response.text)
            return None
    except Exception as e:
        log_error(f"Error getting access token: {str(e)}")
        return None

def get_servicenow_oauth_response(params: dict = {}, config: dict = {}, endpoint: str = '', methode: str = "post"):
    instance_url = config.get("url")

    # Prepare Headers and Params
    access_token = get_access_token_from_refresh_token(config)
    if not access_token:
        raise Exception("Unable to get access token using refresh token.")
            
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token}"  # Using the access token here
    }
    if instance_url and instance_url.endswith("/"):
        instance_url = "/".join(instance_url.split("/")[:-1])
    if endpoint:
        endpoint = f"{instance_url}{endpoint}"

    data = {}
    if params:
        data = params

    if methode == 'post':
        response = requests.post(
            url=endpoint, headers=headers, json=data)
    elif methode == 'patch':
        response = requests.patch(
            endpoint, headers=headers, json=data)
    return response



def get_servicenow_response(params: dict = {}, config: dict = {}, endpoint: str = '', methode: str = "post"):
    api_response = None
    response = ''

    try:
        if config.get("authentication_type").lower() == "oauth authentication":
            response = get_servicenow_oauth_response(params, config, endpoint, methode)
        else:
            response = get_servicenow_password_response(params, config, endpoint, methode)

        if (response and response.status_code in [200, 201, 204]):
            response = response.json()
            api_response = response.get("result")
    except Exception as e:
        log_error(
            f"Servicenow Get Response Failed", e)
    finally:
        return api_response


def get_alert_metrics(config, alerts_data, alerts_filter):
    connection = get_postgres_connection(config)
    servicenow_alerts = []
    alerts_filter_query = ""
    if alerts_filter:
        alerts_filter = '(\'' + '\', \''.join(map(str, alerts_filter)) + '\')'
        alerts_filter_query = f"and lower(metrics.drift_status) in {alerts_filter}"
        
    with connection.cursor() as cursor:
        alerts_data = alerts_data[0]
        for alerts in alerts_data:
            alert_id = alerts.split('\'')[1]
            run_id = alerts.split('\'')[3]
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
            metrics.value as "Actual", metrics.percent_change as "Percent_Change",
            metrics.deviation as "Deviation", metrics.measure_id as "Measure_ID", metrics.id as "Metrics_Id"
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
            where metrics.measure_id='{alert_id}' and metrics.run_id='{run_id}' {alerts_filter_query}
            group by connection.name,asset.name,attribute.technical_name,base_measure.name,
			base_measure.type,base_measure.level,metrics.drift_status,metrics.message,
			metrics.threshold,metrics.threshold,metrics.value,metrics.percent_change,
            metrics.percent_change,metrics.deviation,metrics.measure_id,metrics.id
            """
            cursor = execute_query(connection, cursor, query_string)
            alert_info = fetchone(cursor)
            if alert_info:
                if alert_info.get("Measure_Name", '') == 'Freshness':
                    lt = format_freshness(
                        int(alert_info.get("Lower_Threshold", '0')))
                    ut = format_freshness(
                        int(alert_info.get("Upper_Threshold", '0')))
                    formatted_data = {
                        "Lower_Threshold": lt,
                        "Upper_Threshold": ut,
                        "Expected": f"{lt} - {ut}",
                        "Actual": alert_info.get("Actual", '0')
                    }
                    alert_info = {**alert_info, **formatted_data}
                servicenow_alerts.append(alert_info)
    return servicenow_alerts


def prepare_servicenow_request_data(config, channel_config, drift_alerts):
    try:
        request_data = []
        client_name = os.environ.get("CLIENT_NAME")
        client_name = client_name if client_name else "DQLabs"
        input_prefix = channel_config.get("input_prefix", '')
        base_data = {
            f"{input_prefix}Type": "Alert",
            f"{input_prefix}primary_property": "Alert_ID",
            f"{input_prefix}secondary_property": "Alert_Description",
            f"{input_prefix}source": client_name, 
            f"{input_prefix}alert_type": 'Alert',
            f"{input_prefix}severity": '3',
            f"{input_prefix}state": '1'
        }
        dag_info = config.get("dag_info")
        client_origin = get_client_origin(dag_info)
        for alert in drift_alerts:
            alert_link = f"{client_origin}/measure/{alert.get('Measure_ID','')}/detail"
            alert_link = {"Alert_Link": alert_link}
            servicenow_code = {
                "critical": "1", "warning": "2", "unknown": "3"
            }
            base_data.update(
                {
                    f"{input_prefix}severity": servicenow_code.get(alert.get('status'), "3")
                }
            )

            # Prepare servicenow Alert Name
            asset_name = alert.get("Asset_Name", None)
            attribute_name = alert.get("Attribute_Name", None)
            measure_name = alert.get("Measure_Name", None)
            alert_name = ''
            if asset_name is not None:
                if attribute_name is not None:
                    alert_name = f"{asset_name}_{attribute_name}_{measure_name}"
                else:
                    alert_name = f"{asset_name}_{measure_name}"
            else:
                alert_name = measure_name
            alert.update({"Alert_ID": alert_name})
            data = {**base_data, **alert, **alert_link}
            request_data.append(data)

    except Exception as e:
        log_error("Servicenow - Prepare alert data failed", e)

    finally:
        return request_data

def send_servicenow_alerts(config, drift_alerts):
    try:
        is_active, channel_config = get_servicenow_config(config)
        if not channel_config:
            return
        alert_url = config.get("alert_url","") if config.get("alert_url") else  "/api/now/table/em_alert"

        push_alerts = channel_config.get("push_alerts", '')
        alerts_filter = channel_config.get("alerts_priorities", [])
        if is_active and push_alerts:
            if drift_alerts != []:
                alerts = get_alert_metrics(config, drift_alerts, alerts_filter)
                if alerts:
                    request_data = prepare_servicenow_request_data(
                        config, channel_config, alerts)
                    connection = get_postgres_connection(config)
                    with connection.cursor() as cursor:
                        for alert, request in zip(alerts, request_data):
                            servicenow_response = get_servicenow_response(request, channel_config, alert_url, "post")
                            if servicenow_response:
                                servicenow_id = servicenow_response.get("number")
                                servicenow_sys_id = servicenow_response.get("sys_id")
                                if servicenow_id:
                                    client_name = os.environ.get("CLIENT_NAME")
                                    client_name = client_name if client_name else "DQLabs"
                                    update_payload = {
                                        "source": client_name,
                                        "description": alert.get('Alert_Description')
                                    }
                                    update_url = f"{alert_url}/{servicenow_sys_id}"
                                    get_servicenow_response(update_payload, channel_config, update_url, "patch")
                                    # Update the metrics table with the external_id
                                    external_id = f'{{"servicenow_id": "{servicenow_id}"}}'
                                    update_query = f"""
                                        UPDATE core.metrics 
                                        SET external_id = '{external_id}'
                                        WHERE id = '{alert.get("Metrics_Id")}'
                                    """
                                    execute_query(connection, cursor, update_query)
    except Exception as e:
        log_error(
            f"Servicenow send alerts Failed", e)

def update_servicenow_incident(config, issue_ids):
    """
    Update servicenow incident based on issue_id
    """
    try:
        is_active, channel_config = get_servicenow_config(config)
        if not channel_config:
            return
            
        if not is_active or not channel_config.get('web_hook_enabled'):
            
            return
            
        connection = get_postgres_connection(config)
        if is_active and channel_config.get('web_hook_enabled'):
            for issue_id in issue_ids:
                issue_id = issue_id.get("id")
                with connection.cursor() as cursor:
                    query_string = f"""
                        SELECT * FROM core.issues WHERE id = '{issue_id}'
                    """
                    cursor = execute_query(connection, cursor, query_string)
                    issue = fetchone(cursor)
                external_id = issue.get("external_id")
                if issue and "servicenow_id" in external_id:
                    issue_id = issue.get("id")
                    issue_number = issue.get("issue_number")
                    
                    match = re.search(r"SERVICE NOW ID:\s*'([^']+)'", issue.get("description", ""))
                    service_now_id = match.group(1)
                    match = re.search(r"SERVICENOW INCIDENT NUMBER:\s*'([^']+)'", issue.get("description", ""))
                    
                    endpoint = channel_config.get("issue_url", "") if channel_config.get("issue_url") else f"/api/now/table/incident/{service_now_id}"
                    output_prefix = channel_config.get("output_prefix", "")
                    payload = {
                        f'{output_prefix}state' : '6',
                        f'{output_prefix}close_code' : 'Resolved by caller',
                        f'{output_prefix}close_notes' : f'Issue resolved automatically via DQLabs. Issue ID: {issue_number}'
                    }
                    get_servicenow_response(payload, channel_config, endpoint, "patch")
    except Exception as e:
        log_error(
            f"Servicenow update incident Failed", e)