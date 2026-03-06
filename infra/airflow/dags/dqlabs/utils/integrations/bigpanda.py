"""
    Migration Notes From V2 to V3:
    Migrations Completed 
"""


import requests
import os
from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.db_helper import execute_query, fetchone
from dqlabs.app_helper.log_helper import log_error
from dqlabs.app_helper.crypto_helper import decrypt
from dqlabs.app_helper.dq_helper import get_client_origin, format_freshness


def send_bigpanda_alerts(config, drift_alerts):
    try:
        is_active, channel_config = get_bigpanda_config(config)
        if not channel_config:
            return

        push_alerts = channel_config.get("push_alerts", '')
        alerts_filter = channel_config.get("priorities", [])
        if is_active and push_alerts:
            if drift_alerts != []:
                alerts = get_alert_metrics(config, drift_alerts, alerts_filter)
                if alerts:
                    request_data = prepare_bigpanda_request_data(
                        config, channel_config, alerts)
                    for x in request_data:
                        get_bigpanda_response(x, channel_config)
    except Exception as e:
        log_error(
            f"Bigpanda send alerts Failed", e)


def get_bigpanda_config(config):
    is_active = False
    channel_config = {}
    connection = get_postgres_connection(config)
    organization_id = config.get("organization_id")
    with connection.cursor() as cursor:
        query_string = f"""
            select ch.technical_name, ic.is_active, ic.config from core.channels ch
            join core.integrations ic ON ic.channel_id = ch.id
            where ic.organization_id = '{organization_id}' 
            and ch.type = 'bigpanda'
        """
        cursor = execute_query(connection, cursor, query_string)
        channel_config = fetchone(cursor)
        if channel_config:
            is_active = channel_config.get("is_active")
            channel_config = channel_config.get("config")
    return is_active, channel_config


def get_bigpanda_response(params: dict = {}, config: dict = {}):
    api_response = None
    response = ''

    try:
        api_token = decrypt(config.get('org_token', ''))
        endpoint = config.get("url")
        url = "data/v2/alerts"

        # Prepare Headers and Params
        api_headers = {
            "Content-Type": "application/json",
            "Accept": "*/*",
            "Authorization": 'Bearer {}'.format(api_token)
        }

        if not api_token:
            raise Exception("Missing Api Key")

        if url:
            endpoint = f"{endpoint}/{url}"

        data = {}
        if params:
            data = params
        response = requests.post(
            url=endpoint, headers=api_headers, json=data)

        if (response and response.status_code in [200, 201, 204]):
            api_response = response.json()
    except Exception as e:
        log_error(
            f"Bigpanda Get Response Failed", e)
    finally:
        return api_response


def get_alert_metrics(config, alerts_data, alerts_filter):
    connection = get_postgres_connection(config)
    bigpanda_alerts = []
    alerts_filter_query = ""
    if alerts_filter:
        alerts_filter = '(\'' + '\', \''.join(map(str, alerts_filter)) + '\')'
        alerts_filter_query = f"and lower(metrics.drift_status) in {alerts_filter}"
    with connection.cursor() as cursor:
        alerts_data = alerts_data[0]
        for alerts in alerts_data:
            alert_id = alerts.split('\'')[1]
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
            where metrics.id='{alert_id}' {alerts_filter_query}
            group by connection.name,asset.name,attribute.technical_name,base_measure.name,
			base_measure.type,base_measure.level,metrics.drift_status,metrics.message,
			metrics.threshold,metrics.threshold,metrics.value,metrics.percent_change,
            metrics.percent_change,metrics.deviation,metrics.measure_id
            """
            cursor = execute_query(connection, cursor, query_string)
            alert_info = fetchone(cursor)
            if alert_info:
                if alert_info.get("Measure_Name", '') == 'Freshness':
                    lt = format_freshness(
                        alert_info.get("Lower_Threshold", '0'))
                    ut = format_freshness(
                        alert_info.get("Upper_Threshold", '0'))
                    formatted_data = {
                        "Lower_Threshold": lt,
                        "Upper_Threshold": ut,
                        "Expected": f"{lt} - {ut}",
                        "Actual": alert_info(alert_info.get("Actual", '0'))
                    }
                    alert_info = {**alert_info, **formatted_data}
                bigpanda_alerts.append(alert_info)
    return bigpanda_alerts


def prepare_bigpanda_request_data(config, channel_config, drift_alerts):
    try:
        request_data = []
        client_name = os.environ.get("CLIENT_NAME")
        client_name = client_name if client_name else "DQLabs"
        base_data = {
            "app_key": decrypt(channel_config.get("appkey")),
            "Source": client_name,
            "Type": "Alert",
            "primary_property": "Alert_ID",
            "secondary_property": "Alert_Description"
        }

        dag_info = config.get("dag_info")
        client_origin = get_client_origin(dag_info)
        for alert in drift_alerts:
            alert_link = f"{client_origin}/measure/{alert.get('Measure_ID','')}/detail"
            alert_link = {"Alert_Link": alert_link}

            # Prepare big Panda Alert Name
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
        log_error("BigPanda - Prepare alert data failed", e)

    finally:
        return request_data
