import re
import json
import requests
from uuid import uuid4
import copy

from dqlabs.utils.request_queue import update_queue_detail_status, update_queue_status
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.utils.tasks import get_task_config
from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone, split_queries
from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.app_helper.crypto_helper import decrypt
from dqlabs.app_helper.dq_helper import format_freshness, get_client_origin
from dqlabs.utils.integrations.atlan import Atlan
from dqlabs.utils.integrations.datadotworld import Datadotworld
from dqlabs.utils.integrations.databricks_uc import DatabricksUC
from dqlabs.utils.integrations.purview import Purview
from dqlabs.utils.integrations.coalesce import CoalesceCatalog
from dqlabs.utils.catalog_update import (
    collibra_catalog_update, get_collibra_measures, get_collibra_asset_details, get_collibra_attributes_details
    )
from dqlabs.app_helper.catalog_helper import (get_domain_metrics, get_asset_metrics ,get_attributes_metrics,
                                              get_measure_metrics, get_alerts_metrics, get_issues_metrics)

from dqlabs.app_helper.dag_helper import get_postgres_connection

# HTML Clean up for Description
HTML_TAG_CLEANR = re.compile('<.*?>')

def cleanhtml(raw_html):
    cleantext = re.sub(HTML_TAG_CLEANR, '', raw_html)
    cleantext = cleantext.replace("'", "''")
    return cleantext


def call_api_request(endpoint, method_type, channel, params='', auth_token=''):
    try:
        # Prepare Headers and Params
        api_headers = {'accept': 'application/json'}
        if auth_token:
            api_headers['TOKEN'] = auth_token

        if method_type == "post" and not auth_token:
            response = requests.post(
                url=endpoint, data=params, verify=False)
        elif method_type == "post" and auth_token:
            api_headers["content-type"] = "application/json"
            params_data = json.dumps(params)
            response = requests.post(
                url=endpoint, data=params_data, headers=api_headers, verify=False)
        elif method_type == "put":
            api_headers["content-type"] = "application/json"
            params_data = json.dumps(params)
            response = requests.put(
                url=endpoint, data=params_data, headers=api_headers, verify=False)
        elif method_type == "delete":
            response = requests.delete(
                url=endpoint, headers=api_headers, verify=False)
        else:
            response = requests.get(
                url=endpoint, headers=api_headers, verify=False)

        if (response and response.status_code in [200, 201, 202, 204]):
            if response.status_code == 204:
                return response
            return response.json()

        # Handle 401 or 403 - Try to get a new token and retry once
        elif (response and response.status_code in [401, 403]) and channel:
            log_error("Alation Connector - Auth error, attempting token refresh.")
            new_token = get_alation_token(channel)
            if new_token:
                return call_api_request(endpoint, method_type, channel, params, new_token)
        
        # Raise for other errors
        response.raise_for_status()

    except requests.exceptions.HTTPError as http_err:
        log_error("Alation Connector - HTTP error occurred", http_err)
    except requests.exceptions.RequestException as req_err:
        log_error("Alation Connector - Request failed", req_err)
    except Exception as e:
        log_error("Alation Connector - Unexpected error", e)


def validate_refesh_token(channel, refresh_token):
    api_url = "integration/v1/validateRefreshToken/"
    api_params = {
        'user_id': channel.get("user_id", ''),
        'refresh_token': refresh_token
    }
    response = requests.post(
        url=f"""{channel.get('host', '')}/{api_url}""", data=api_params, verify=False)

    if (response and response.status_code in [200, 201]):
        response = response.json()
        if response.get("token_status", '').lower() == "active":
            return refresh_token
        else:
            api_url = "integration/v1/createRefreshToken/"
            api_params = {
                'username': channel.get("user", ''),
                'password': decrypt(channel.get('password', '')),
                'name': channel.get("name", 'DQLabs_alation')
            }
            response = requests.post(
                url=f"""{channel.get('host', '')}/{api_url}""", data=api_params, verify=False)

            if response:
                return response.get('refresh_token', '')


def create_refesh_token(channel):
    refresh_token = None
    try:
        api_url = "integration/v1/createRefreshToken/"
        api_params = {
            'username': decrypt(channel.get("user", '')),
            'password': decrypt(channel.get('password', '')),
            'name': channel.get("name", 'DQLabs_alation')
        }
        response = requests.post(
            url=f"""{channel.get('host', '')}/{api_url}""", data=api_params, verify=False)

        if response:
            return response.get('refresh_token', '')
        return refresh_token
    except Exception as e:
        raise e


def get_alation_token(channel):
    token = None
    try:
        channel_host_url = channel.get('host', '')
        refresh_token = channel.get('refresh_token', '')
        try:
            refresh_token = decrypt(refresh_token)
        except Exception as e:
            refresh_token = create_refesh_token(channel)
        if channel.get('authentication_type', '') == 'Username and Password':
            refresh_token = validate_refesh_token(channel, refresh_token)
        user_id = channel.get("user_id", '')
        api_url = f"{channel_host_url}/integration/v1/createAPIAccessToken/"
        api_params = {
            'refresh_token': refresh_token,
            'user_id': user_id
        }
        response = call_api_request(api_url, 'post', channel, api_params)
        token = response.get('api_access_token', '')
    except Exception as e:
        log_error(
            f"Alation Connector - Get Token By User Id and Reset Token ", e)
    finally:
        return token


def validate_channel_config_permission(channel, permission_module, permission_task, permission_property=None):
    permission = False
    try:
        if permission_module == 'export':
            permission = False
            if permission_property:
                permission_config = channel.get('export', '')
                permission_config = json.loads(permission_config)
                permission = next((item["options"][permission_task]
                                  for item in permission_config if permission_property in item and item[permission_property]), False)
        else:
            permission_config = channel.get('import', '')
            permission = permission_config.get(permission_task, False)
        permission = True if permission == 'true' or permission == True else False
    except Exception as e:
        log_error(
            f"Validate Channel Config Failed ", e)
    finally:
        return permission


def get_alation_domains(channel, token):
    domains = {}
    try:
        channel_host_url = channel.get('host', '')
        api_url = f"{channel_host_url}/integration/v2/domain"
        domains_list = call_api_request(api_url, 'get', channel, '', token)
        domains_list = domains_list if domains_list else []
        domains_list = [
            p for p in domains_list if p.get('title')]

        if domains_list and len(domains_list) > 0:
            domain_ids = [x.get('id') for x in domains_list]
            api_url = f"{channel_host_url}/integration/v2/domain/membership/view_rules/"
            payload = {
                "domain_id": domain_ids,
                "exclude": False
            }
            domains_table_map_list = call_api_request(api_url,
                                                      'post', channel, payload, token)
            domains_table_map_list = domains_table_map_list if domains_table_map_list else []
            domains_table_map_list = [
                p for p in domains_table_map_list if p.get('otype') == 'table']
            domains.update({
                "list": domains_list,
                "mapped": domains_table_map_list if domains_table_map_list else []
            })
    except Exception as e:
        log_error(
            f"Alation Connector - Get Domains ", e)
    finally:
        return domains


def clean_alation_domains(config, channel):
    try:
        alation_domains = channel.get("domains", None)
        if alation_domains:
            domains_list = alation_domains.get("list", [])
            domains_ids = list(
                map(lambda domain: f"""'{domain.get("id")}'""", domains_list)
            )
            domains_ids = ", ".join(domains_ids)
            domains_ids = f"({domains_ids})"

            connection = get_postgres_connection(config)
            with connection:
                with connection.cursor() as cursor:

                    # Delete Domain and Mapping from DQLabs When if removed from Alation
                    check_existing_query = f"""
                        WITH RECURSIVE tree AS (
                            SELECT id, parent_id
                            FROM core.domain
                            WHERE id in (
                            select id from core.domain
                                where (properties->>'type')::text = 'alation' 
                                and (properties->>'alation_domain_id')::text not in {domains_ids}
                            )
                            UNION ALL
                            SELECT t.id, t.parent_id
                            FROM core.domain t
                            JOIN tree ON t.parent_id = tree.id
                            )
                        SELECT id FROM tree
                    """
                    cursor = execute_query(
                        connection, cursor, check_existing_query)
                    existing_domains = fetchall(cursor)
                    if existing_domains:
                        existing_mapped_domains_ids = list(
                            map(lambda domain: f"""'{domain.get("id")}'""",
                                existing_domains)
                        )
                        existing_mapped_domains_ids = ", ".join(
                            existing_mapped_domains_ids)
                        existing_mapped_domains_ids = f"({existing_mapped_domains_ids})"
                        if existing_mapped_domains_ids:
                            remove_mapped_domains_query = f"""delete from core.domain_mapping where domain_id in {existing_mapped_domains_ids}"""
                            cursor = execute_query(
                                connection, cursor, remove_mapped_domains_query)

                            remove_linked_domains_key_values = f""" delete from core.terms_mapping
                               where term_id in (select id from core.terms where domain_id in {existing_mapped_domains_ids})
                            """
                            cursor = execute_query(
                                connection, cursor, remove_linked_domains_key_values)

                            remove_linked_domains_key_values = f""" update core.measure set is_active=false,is_delete=true
                               where base_measure_id in (select id from core.base_measure where term_id in (select id from core.terms where domain_id in {existing_mapped_domains_ids}))
                            """
                            cursor = execute_query(
                                connection, cursor, remove_linked_domains_key_values)

                            remove_linked_domains_key_values = f""" update core.base_measure set term_id=NULL, is_active=false,is_delete=true
                               where term_id in (select id from core.terms where domain_id in {existing_mapped_domains_ids})
                            """
                            cursor = execute_query(
                                connection, cursor, remove_linked_domains_key_values)

                            remove_linked_domains_key_values = f""" update core.version_history set term_id=NULL
                               where term_id in (select id from core.terms where domain_id in {existing_mapped_domains_ids})
                            """
                            cursor = execute_query(
                                connection, cursor, remove_linked_domains_key_values)

                            remove_mapped_domains_query = f"""delete from core.terms where domain_id in {existing_mapped_domains_ids}"""
                            cursor = execute_query(
                                connection, cursor, remove_mapped_domains_query)

                            remove_domains_query = f"""delete from core.domain where id in {existing_mapped_domains_ids}"""
                            cursor = execute_query(
                                connection, cursor, remove_domains_query)

    except Exception as e:
        log_error(
            f"Alation Domain Validate / Deletion Failed ", e)


def prepare_alation_domains_by_level(alation_domains, parent_domain={"id": None, "level": 0, "technical_name": []}):
    try:
        domains = []
        sub_domains = [p for p in alation_domains if p.get(
            'parent_id') == parent_domain.get('id')]
        for s_domain in sub_domains:
            technical_name = [s_domain.get('title')]
            level = parent_domain.get("level") + 1

            p_technical_name = parent_domain.get("technical_name", [])
            if p_technical_name:
                technical_name = p_technical_name + technical_name

            s_domain.update({
                "level": level,
                "technical_name": technical_name
            })
            domains = domains + \
                [s_domain] + \
                prepare_alation_domains_by_level(alation_domains, s_domain)
        return domains
    except Exception as e:
        log_error(
            f"Alation Connector - Prepare Map Domains ", e)


def insert_alation_domains(config: dict, connection, domains: list, level=1):
    try:
        domains_list = list(filter(lambda x: x.get('level') == level, domains))

        if not domains_list or len(domains_list) == 0:
            return

        domain_input = []
        organization_id = config.get('organization_id')
        if not organization_id:
            organization_id = config.get('dag_info', {}).get('organization_id')
        with connection:
            with connection.cursor() as cursor:
                for domain in domains_list:
                    domain_id = domain.get('id')
                    domain_name = domain.get('title')
                    description = domain.get('description', '')
                    description = cleanhtml(description) if description else ''
                    parent_id = domain.get('parent_id', '')
                    level = domain.get('level')
                    technical_name = domain.get('technical_name')
                    domain_type = 'category' if parent_id else 'domain'
                    parent_domain_id = ''
                    parent_glossary_id = ''

                    parent_domain = list(filter(lambda x: x.get(
                        'id') == parent_id, domains))
                    parent_domain = parent_domain[0] if parent_domain and len(
                        parent_domain) > 0 else None

                    if parent_domain:
                        parent_domain_id = parent_domain.get('domain_id', '')
                        parent_glossary_id = parent_domain.get(
                            'parent_domain_id', '')
                        parent_glossary_id = parent_glossary_id if parent_glossary_id else parent_domain_id

                    # Check Domain Already Avilable
                    check_existing_query = f"""
                        select id, domain from core.domain 
                        where (properties->>'type')::text = 'alation'
                        and (properties->>'alation_domain_id')::text = '{domain_id}'
                        and is_active = true and is_delete = false
                    """
                    cursor = execute_query(
                        connection, cursor, check_existing_query)
                    existing_domain_data = fetchone(cursor)

                    if existing_domain_data:
                        glossary_id = existing_domain_data.get('id')
                        for s_domain in domains:
                            if s_domain.get('id') == domain_id:
                                s_domain.update(
                                    {'domain_id': glossary_id, 'parent_domain_id': existing_domain_data.get('domain', '')})
                                break

                        update_domain_query = f"""
                            update core.domain set name='{domain_name}', technical_name='{technical_name}', description = '{description}'
                            where id = '{glossary_id}'
                        """
                        cursor = execute_query(
                            connection, cursor, update_domain_query)
                        continue

                    # Insert New Domain if Domain Not Avilable
                    glossary_id = str(uuid4())
                    for s_domain in domains:
                        if s_domain.get('id') == domain_id:
                            # Check Domain Already Avilable
                            check_existing_query = f"""
                                select id 
                                from core.domain 
                                where (properties->>'type')::text = 'alation'
                                and (properties->>'alation_domain_id')::text = '{domain.get('mapping_domain_id')}'
                                and is_active = true and is_delete = false
                            """
                            cursor = execute_query(
                                connection, cursor, check_existing_query)
                            existing_domain_data = fetchone(cursor)
                            if existing_domain_data:
                                s_domain.update(
                                    {'mapping_domain_id': existing_domain_data.get('id')})

                            s_domain.update(
                                {'domain_id': glossary_id})
                            break

                    query_input = (
                        glossary_id,
                        domain_name,
                        technical_name,
                        description,
                        str(organization_id),
                        str(parent_domain_id) if parent_domain_id else None,
                        domain_type,
                        level-1,
                        str(domain.get('mapping_domain_id')
                            ) if parent_domain_id else None,
                        True,
                        False,
                        'alation',
                        json.dumps(
                            {"type": "alation", "alation_domain_id": domain_id}, default=str),
                    )
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals}, CURRENT_TIMESTAMP)", query_input
                    ).decode("utf-8")
                    domain_input.append(query_param)

                domain_input = split_queries(domain_input)
                for input_values in domain_input:
                    try:
                        query_input = ",".join(input_values)
                        query_string = f"""
                            insert into core.domain(
                                id, name, technical_name, description, organization_id, parent_id, type, level, domain, is_active, is_delete, source, properties, created_date
                            ) values {query_input} 
                            RETURNING id
                        """
                        cursor = execute_query(
                            connection, cursor, query_string)
                    except Exception as e:
                        log_error('Alation Domain Insert Failed  ', e)

        insert_alation_domains(config, connection, domains, level+1)
    except Exception as e:
        log_error('Alation Domain Insert Failed  ', e)


def prepare_insert_alation_domains(config, channel):
    try:
        alation_domains = channel.get("domains", None)
        if alation_domains:
            domains_list = alation_domains.get("list", [])
            domains_list = prepare_alation_domains_by_level(domains_list)
            domains_list = domains_list if domains_list else []
            domains_list_details = copy.deepcopy(domains_list)

            for domain in domains_list:
                technical_names = domain.get('technical_name', [])
                technical_names = technical_names if technical_names else []
                if len(technical_names) > 1:
                    parent_name = technical_names[0]
                    filter_value = [domains_list_detail for domains_list_detail in domains_list_details if domains_list_detail.get(
                        'title') == parent_name]
                    if filter_value:
                        filter_value = filter_value[0]
                    technical_names = technical_names[:-1]
                    technical_names = ".".join(technical_names)
                    domain.update(
                        {'technical_name': f"""{domain.get('title')}({technical_names})""", 'mapping_domain_id': filter_value.get('id')})
                else:
                    domain.update({'technical_name': domain.get(
                        'title'), 'mapping_domain_id': domain.get('id')})
            connection = get_postgres_connection(config)
            insert_alation_domains(config, connection, domains_list)
    except Exception as e:
        log_error(
            f"Alation Domain Inservt / Deletion Failed ", e)


def get_alation_tags(channel, token):
    tags = []
    try:
        channel_host_url = channel.get('host', '')
        api_url = f"{channel_host_url}/integration/v2/tag"
        tags = call_api_request(api_url, 'get', channel, '', token)
        tags = tags if tags else []
        tags = [p for p in tags if p.get('name')]
    except Exception as e:
        log_error(
            f"Alation Connector - Get Tags ", e)
    finally:
        return tags


def clean_alation_tags(config, channel):
    try:
        alation_tags = channel.get("tags", None)
        if alation_tags:
            tag_ids = list(
                map(lambda tag: f"""'{tag.get("id")}'""", alation_tags)
            )
            tag_ids = ", ".join(tag_ids)
            tag_ids = f"({tag_ids})"

            connection = get_postgres_connection(config)
            with connection:
                with connection.cursor() as cursor:

                    # Delete Tag and Mapping from DQLabs When if removed from Alation
                    check_existing_query = f"""
                        WITH RECURSIVE tree AS (
                            SELECT id, parent_id
                            FROM core.tags
                            WHERE id in (
                            select id from core.tags
                                where (properties->>'type')::text = 'alation' 
                                and (properties->>'alation_tag_id')::text not in {tag_ids}
                            )
                            UNION ALL
                            SELECT t.id, t.parent_id
                            FROM core.tags t
                            JOIN tree ON t.parent_id = tree.id
                            )
                        SELECT id FROM tree
                    """
                    cursor = execute_query(
                        connection, cursor, check_existing_query)
                    existing_tags = fetchall(cursor)
                    if existing_tags:
                        existing_mapped_tags_ids = list(
                            map(lambda tag: f"""'{tag.get("id")}'""",
                                existing_tags)
                        )
                        existing_mapped_tags_ids = ", ".join(
                            existing_mapped_tags_ids)
                        existing_mapped_tags_ids = f"({existing_mapped_tags_ids})"
                        if existing_mapped_tags_ids:
                            remove_mapped_tags_query = f"""delete from core.tags_mapping where tags_id in {existing_mapped_tags_ids}"""
                            cursor = execute_query(
                                connection, cursor, remove_mapped_tags_query)

                            remove_tags_query = f"""delete from core.tags where id in {existing_mapped_tags_ids}"""
                            cursor = execute_query(
                                connection, cursor, remove_tags_query)

    except Exception as e:
        log_error(
            f"Alation Tags Validate / Deletion Failed ", e)


def insert_alation_tags(config, channel):
    try:
        alation_tags = channel.get("tags", None)
        if alation_tags:
            tags_input = []
            organization_id = config.get('organization_id')
            if not organization_id:
                organization_id = config.get(
                    'dag_info', {}).get('organization_id')
            connection = get_postgres_connection(config)
            with connection:
                with connection.cursor() as cursor:
                    for tag in alation_tags:
                        tag_id = tag.get('id')
                        tag_name = tag.get('name')
                        description = tag.get('description', '')
                        description = cleanhtml(
                            description) if description else ''

                        # Check Tag Already Avilable
                        check_existing_query = f"""
                            select id from core.tags 
                            where (properties->>'type')::text = 'alation' 
                            and (properties->>'alation_tag_id')::text = '{tag_id}'
                        """
                        cursor = execute_query(
                            connection, cursor, check_existing_query)
                        existing_tag_data = fetchone(cursor)

                        if existing_tag_data:
                            ex_tag_id = existing_tag_data.get('id')
                            update_domain_query = f"""
                                update core.tags set name='{tag_name}', technical_name='{tag_name}', description = '{description}'
                                where id = '{ex_tag_id}'
                            """
                            cursor = execute_query(
                                connection, cursor, update_domain_query)
                            continue

                        # Insert New Tag if Tag Not Avilable
                        new_tag_id = str(uuid4())

                        query_input = (
                            new_tag_id,
                            tag_name,
                            tag_name,
                            False,
                            True,
                            False,
                            description,
                            str(organization_id),
                            '#64AAEF',
                            None,
                            'alation',
                            json.dumps(
                                {"type": "alation", "alation_tag_id": tag_id}, default=str),
                        )
                        input_literals = ", ".join(["%s"] * len(query_input))
                        query_param = cursor.mogrify(
                            f"({input_literals}, CURRENT_TIMESTAMP)", query_input
                        ).decode("utf-8")
                        tags_input.append(query_param)

                    tags_input = split_queries(tags_input)
                    for input_values in tags_input:
                        try:
                            query_input = ",".join(input_values)
                            query_string = f"""
                                insert into core.tags(
                                    id, name, technical_name, is_mask_data, is_active, is_delete, description, organization_id, color, parent_id, source, properties, created_date
                                ) values {query_input} 
                                RETURNING id
                            """
                            cursor = execute_query(
                                connection, cursor, query_string)
                        except Exception as e:
                            log_error('Alation Tags Insert Failed  ', e)
    except Exception as e:
        log_error(
            f"Alation Tags Insert Failed ", e)


def map_alation_domain_to_dqlabs(config: dict, channel: dict, table: dict, asset_id):
    try:
        domain_input = []
        alation_domains = channel.get("domains", [])

        if alation_domains and table:
            domains_mapped_list = alation_domains.get("mapped", [])
            table_oid = table.get('id')
            domains_table_map_list = [
                p for p in domains_mapped_list if p.get('oid') == table_oid]
            if domains_table_map_list:
                mapped_domains_ids = list(
                    map(lambda domain: f"""'{domain.get("domain_id")}'""",
                        domains_table_map_list)
                )
                mapped_domains_ids = ", ".join(mapped_domains_ids)
                mapped_domains_ids = f"({mapped_domains_ids})"
                connection = get_postgres_connection(config)
                with connection:
                    with connection.cursor() as cursor:
                        delete_mapped_domain_query = f"""
                            delete from core.domain_mapping where id in (
                                select domain_mapping.id from core.domain 
                                join core.domain_mapping on domain_mapping.domain_id = domain.id 
                                and domain_mapping.asset_id = '{asset_id}'
                                where (properties->>'type')::text = 'alation' 
                                and (properties->>'alation_domain_id')::text not in {mapped_domains_ids}
                            )
                        """
                        cursor = execute_query(
                            connection, cursor, delete_mapped_domain_query)

                        check_existing_query = f"""
                                select id, properties->>'alation_domain_id' as alation_domain_id from core.domain
                                where (properties->>'type')::text = 'alation' 
                                and (properties->>'alation_domain_id')::text in {mapped_domains_ids}
                                order by level asc
                            """
                        cursor = execute_query(
                            connection, cursor, check_existing_query)
                        mapped_domains = fetchall(cursor)
                        mapped_domains = mapped_domains if mapped_domains else []

                        for domain in mapped_domains:
                            domain_id = domain.get('id')
                            check_existing_query = f"""
                                    select id from core.domain_mapping 
                                    where level='asset' and asset_id = '{asset_id}' and domain_id = '{domain_id}'
                                """
                            cursor = execute_query(
                                connection, cursor, check_existing_query)
                            existing_map_data = fetchone(cursor)
                            if existing_map_data:
                                continue

                            query_input = (
                                str(uuid4()),
                                'asset',
                                str(domain_id),
                                str(asset_id)
                            )
                            input_literals = ", ".join(
                                ["%s"] * len(query_input))
                            query_param = cursor.mogrify(
                                f"({input_literals}, CURRENT_TIMESTAMP)", query_input
                            ).decode("utf-8")
                            domain_input.append(query_param)

                        domain_input = split_queries(domain_input)
                        for input_values in domain_input:
                            try:
                                query_input = ",".join(input_values)
                                query_string = f"""
                                        insert into core.domain_mapping(
                                            id, level, domain_id, asset_id, created_date
                                        ) values {query_input}
                                    """
                                cursor = execute_query(
                                    connection, cursor, query_string)
                            except Exception as e:
                                log_error(
                                    'Alation Domain Map to Metadata Insert Failed  ', e)
            else:
                connection = get_postgres_connection(config)
                with connection:
                    with connection.cursor() as cursor:
                        delete_mapped_domain_query = f"""
                            delete from core.domain_mapping where id in (
                                select domain_mapping.id from core.domain 
                                join core.domain_mapping on domain_mapping.domain_id = domain.id 
                                and domain_mapping.asset_id = '{asset_id}' and domain_mapping.level ='asset'
                                where (properties->>'type')::text = 'alation'
                            )
                        """
                        cursor = execute_query(
                            connection, cursor, delete_mapped_domain_query)
    except Exception as e:
        log_error('Alation Domain Map to Metadata Insert Failed  ', e)


def bind_alerts_rows(alerts, client_origin):
    alerts_html = ""
    try:
        for alert in alerts:
            measure_id = alert.get('measure_id')
            alert_id = alert.get('alert_id')
            measure_name = alert.get('measure')
            link = f"""{client_origin}/measure/{measure_id}/detail?measure_name={measure_name}&alert_id={alert_id}"""
            alerts_html = f"""
                {alerts_html}
                 <tr>\
                    <td><strong><a href={link} target="_blank" rel="noopener noreferrer">{alert.get('message', '')}</a></strong></td>\
                    <td>{alert.get('attribute_name', '') if alert.get('attribute_name', '') else '-'}</td>\
                    <td>{alert.get('measure_name', '')}</td>\
                    <td>{alert.get('status', '')}</td>\
                    <td>{alert.get('created_date').strftime("%A, %d. %B %Y %I:%M%p %Z")}</td>\
                </tr>\
                """
    except Exception as e:
        log_error(
            f"Alation Connector - Bind Alerts Rows Failed", e)
    finally:
        return alerts_html

def get_domain_alerts_metrics(config, domain_id):
    alerts = []
    try:
        """
        Get domain details by domain id
        """
        if not domain_id:
            return alerts

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            # get the domain by name
            query_string = f"""
                select alert.id, alert.created_date,                    
                    alert.message, alert.drift_status as status, alert.value, alert.threshold, alert.measure_id, alert.attribute_id,
                    alert.asset_id, alert.marked_as, alert.measure_name as measure, alert.percent_change, alert.deviation,
                    connection.name as connection_name, base_measure.name as measure_name, attr.name as attribute_name,
                    asset.name as asset_name,issues.id as issue_id, issues.issue_id as issue_number
                from core.metrics as alert
                join core.measure on measure.id=alert.measure_id and measure.is_active=true
                join core.base_measure on base_measure.id=measure.base_measure_id
                left join core.connection as connection on connection.id=measure.connection_id and connection.is_active=true
                left join core.asset as asset on asset.id=measure.asset_id and asset.is_active=true and asset.is_delete = false               
                left join core.attribute as attr on attr.id=measure.attribute_id and attr.is_selected=true               
                left join core.domain_mapping on (domain_mapping.asset_id=asset.id or domain_mapping.measure_id=measure.id)
                left join core.application_mapping on (application_mapping.asset_id=asset.id or application_mapping.measure_id=measure.id)               
                left join core.issues on issues.metrics_id=alert.id and issues.is_delete = false
                left join core.domain on domain.id=domain_mapping.domain_id
                where lower(alert.drift_status) in ('high', 'medium', 'low')
                and (alert.attribute_id is null or (alert.attribute_id is not null and attr.is_selected = true))
                and (alert.asset_id is null or (alert.asset_id is not null and asset.is_active = true))
                and ( domain.id in (
                            WITH RECURSIVE tree AS (
                                SELECT id
                                FROM core.domain
                                WHERE id = '{domain_id}'
                                UNION ALL
                                SELECT r.id
                                FROM core.domain r
                                JOIN tree c ON r.parent_id = c.id
                            )
                            select id from tree
                        )
                    )
                and alert.run_id=measure.last_run_id         
                group by alert.id, connection.id, asset.id, attr.id, measure.id, base_measure.id, issues.id
            """
            cursor = execute_query(connection, cursor, query_string)
            alerts = fetchall(cursor)
            alerts = alerts if alerts else []
        return alerts
    except Exception as e:
        log_error(
            f"Alation Connector: Get domain alerts ", str(e))
    finally:
        return alerts


def set_alation_alerts_metrics(config, channel, id='', type='asset'):
    alerts_html = ""
    try:
        if type == 'domain':
            alerts = get_domain_alerts_metrics(config, id)
        else:
            alerts = get_alerts_metrics(config, id, type)
        dag_info = config.get("dag_info")
        client_origin = get_client_origin(dag_info)
        if alerts and len(alerts) > 0:
            alerts_html = f"""<table style="border-style: inset;margin-top:30px;">\
                                        <thead>\
                                            <tr>\
                                                <th style="width: 40%;">Alerts</th>\
                                                <th>Column</th>                      
                                                <th>Measure</th>\
                                                <th>Status</th>\
                                                <th>Created Date</th>\
                                            </tr>\
                                        </thead>\
                                        <tbody>
                                            {bind_alerts_rows(alerts, client_origin)}                                         
                                        </tbody>\
                                    </table>"""
    except Exception as e:
        log_error(
            f"Alation Connector - Get {type} Alerts Failed", e)
    finally:
        return alerts_html

def bind_issues_rows(issues, client_origin):
    issues_html = ""
    try:
        for issue in issues:
            issue_id = issue.get('id')
            link = f"{client_origin}/remediate/issues/{issue_id}"
            issues_html = f"""
                {issues_html}
                 <tr>\
                    <td><strong><a href={link} target="_blank" rel="noopener noreferrer">{issue.get('name', '')}</a></strong></td>\
                    <td>{issue.get('attribute_name', '') if issue.get('attribute_name', '') else '-'}</td>\
                    <td>{issue.get('priority', '')}</td>\
                    <td>{issue.get('status', '')}</td>\
                    <td>{issue.get('created_date').strftime("%A, %d. %B %Y %I:%M%p %Z")}</td>\
                </tr>\
                """
    except Exception as e:
        log_error(
            f"Alation Connector - Bind Issues Rows Failed", e)
    finally:
        return issues_html


def get_domain_issues_metrics(config, domain_id):
    issues = []
    try:
        """
        Get domain details by domain id
        """
        if not domain_id:
            return issues

        organization_id = config.get('organization_id')
        if not organization_id:
            organization_id = config.get('dag_info', {}).get('organization_id')
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            # get the domain by name
            query_string = f"""
                        select
                            issues.id, 
                            concat(issues.issue_id,' ',issues.name) as name,
                            asset.name as asset_name,
                            attribute.name as attribute_name,
                            issues.status, 
                            issues.priority, 
                            issues.asset_id as asset_id,
                            issues.created_date as created_date,
                            issues.id,           
                            attribute.id as attribute_id
                        from core.issues as issues
                        join core.measure on measure.id=issues.measure_id and measure.is_active=true
                        left join core.asset as asset on asset.id=issues.asset_id and asset.is_delete=false                      
                        left join core.attribute as attribute on attribute.id=measure.attribute_id and attribute.is_selected=true                      
                        left join core.domain_mapping on (domain_mapping.asset_id=asset.id or domain_mapping.measure_id=measure.id)
                        left join core.application_mapping on (application_mapping.asset_id=asset.id or application_mapping.measure_id=issues.measure_id)
                        left join core.domain on domain.id=domain_mapping.domain_id 
                        where issues.organization_id ='{organization_id}' and issues.is_delete = false
                        and (asset.id is null or (asset.id is not null and asset.is_active = true))
                        and (
                            issues.attribute_id is null
                            or (issues.attribute_id is not null and issues.attribute_id=attribute.id)
                        )
                        and ( domain.id in  (
                            WITH RECURSIVE tree AS (
                                SELECT id
                                FROM core.domain
                                WHERE id = '{domain_id}'
                                UNION ALL
                                SELECT r.id
                                FROM core.domain r
                                JOIN tree c ON r.parent_id = c.id
                            )
                            select id from tree
                            )
                        )
                        group by issues.id,asset.name,attribute.name,attribute.id,date(issues.created_date)
                        """
            cursor = execute_query(connection, cursor, query_string)
            issues = fetchall(cursor)
            issues = issues if issues else []
        return issues
    except Exception as e:
        log_error(
            f"Alation Connector: Get domain issues metrics ", str(e))
    finally:
        return issues


def set_alation_issues_metrics(config, channel, id='', type='asset'):
    issues_html = ""
    try:
        if type == 'domain':
            issues = get_domain_issues_metrics(config, id)
        else:
            issues = get_issues_metrics(config, id, type)
        dag_info = config.get("dag_info")
        client_origin = get_client_origin(dag_info)
        if issues and len(issues) > 0:
            issues_html = f"""<table style="border-style: inset;margin-top:30px;">\
                                        <thead>\
                                            <tr>\
                                                <th style="width: 45%;">Issues</th>\
                                                <th>Column</th>                      
                                                <th>Priority</th>\
                                                <th>Status</th>\
                                                <th>Created Date</th>\
                                            </tr>\
                                        </thead>\
                                        <tbody>
                                            {bind_issues_rows(issues, client_origin)}                                         
                                        </tbody>\
                                    </table>"""
    except Exception as e:
        log_error(
            f"Alation Connector - Get {type} Issues Failed", e)
    finally:
        return issues_html


def get_measures_data(config, _id, _type='asset'):
    measures = []
    try:
        """
        Get asset and attribute measures by domain id
        """
        if not _id:
            return measures

        _condition = f" base.level='{_type}' and mes.asset_id = '{_id}' "
        if _type == 'attribute':
            asset_id = config.get("asset_id")
            _condition = f" base.level in ('{_type}', 'term') and mes.attribute_id='{_id}' and mes.asset_id ='{asset_id}' "

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            # get the domain by name
            query_string = f"""
                select distinct on (mes.id) mes.id, base.name,mes.status, base.level as level, base.technical_name, 
                    base.description, base.is_default, base.type, base.category, dim.name as dimension_name,
                    mes.is_aggregation_query, base.query, mes.is_active, mes.is_drift_enabled, mes.allow_score, 
                    mes.is_positive, mes.score, 
                    mes.last_runs, data.primary_columns,
                    base.default_active, base.disable_scoring,mes.weightage, base.allow_export, mes.result,
                    count(alert.id) as alerts, count(isu.id) as issues,
                    count(case when alert.run_id=mes.last_run_id then alert.id end) as recent_alerts_count,
                    connection.name as connection_name,ast.name as asset,attribute.name as attribute,
                    ast.id as asset_id,attribute.id as attribute_id,
                    connection.id as connection_id,
                    case when ast.modified_date is not null then ast.modified_date else ast.created_date end as cmdate,
                    CAST(mes.drift_threshold->>'lower_threshold' AS NUMERIC) as lower_threshold,
                    CAST(mes.drift_threshold->>'upper_threshold' AS NUMERIC) as upper_threshold,
                    case when mes.is_active and mes.allow_score then mes.row_count else 0 end as total_records,                  
                    mes.invalid_rows as invalid_rows,
                    mes.valid_rows as valid_rows
                from core.measure as mes
                join core.base_measure as base on base.id=mes.base_measure_id
                left join core.connection on connection.id=mes.connection_id and connection.is_active=true
                left join core.asset as ast on ast.id=mes.asset_id
				left join core.data on data.asset_id = ast.id
                left join core.attribute on attribute.asset_id=ast.id and attribute.is_active=true
                left join core.dimension dim on dim.id = mes.dimension_id
                left join core.domain_mapping on (domain_mapping.measure_id=mes.id or domain_mapping.asset_id=ast.id)
                left join core.domain on domain.id=domain_mapping.domain_id
                left join core.application_mapping on (application_mapping.measure_id=mes.id or application_mapping.asset_id=ast.id)
                left join core.application on application.id=application_mapping.application_id
                left join core.metrics as alert on alert.measure_id=mes.id and lower(alert.drift_status) in ('high','medium','low')
                left join core.issues as isu on isu.measure_id=mes.id and lower(isu.status) != 'resolved'
				left join core.terms_mapping on terms_mapping.attribute_id = attribute.id  and terms_mapping.approved_by is not null
                left join core.terms on terms.id=terms_mapping.term_id
                where mes.is_active=true 
				and {_condition}
                group by mes.id, base.id, data.id, dim.name,connection.id,attribute.id,ast.id                
            """
            cursor = execute_query(connection, cursor, query_string)
            measures = fetchall(cursor)
            measures = measures if measures else []
    except Exception as e:
        log_error(
            f"Alation Connector: Get Measures data ", str(e))
    finally:
        return measures


def bind_measures_metrics_rows(metrics, client_origin, _type):
    rows_html = ""
    try:
        for metric in metrics:
            metric_id = metric.get('id')
            link = f"{client_origin}/measure/{metric_id}/detail"
            allow_score = metric.get('allow_score', False)
            rows_html = f"""
                {rows_html}
                 <tr>\
                    <td><strong><a href={link} target="_blank" rel="noopener noreferrer">{metric.get('name', '')}</a></strong></td>\
                    <td>{metric.get('status', '')}</td>\
                    <td>{rounding(metric.get('score', 0)) if allow_score else 'NA'}</td>\
                    <td>{metric.get('invalid_rows', '') if allow_score else 'NA'}</td>\
                    <td>{metric.get('valid_rows', '') if allow_score else 'NA'}</td>\
                    <td>{metric.get('total_records', '') if allow_score else 'NA'}</td>\
                    <td>{metric.get('description', '')}</td>\
                    <td>{metric.get('cmdate').strftime("%A, %d. %B %Y %I:%M%p %Z")}</td>\
                    <td>{metric.get('is_active', '')}</td>\
                    <td>{metric.get('is_positive', '')}</td>\
                    <td>{metric.get('allow_score', '')}</td>\
                    <td>{metric.get('is_drift_enabled', '')}</td>\
                    <td>{metric.get('category', '')}</td>\
                    <td>{metric.get('type', '')}</td>\
                    <td>{metric.get('alerts', '')}</td>\
                    <td>{metric.get('issues', '')}</td>\
                    <td>{metric.get('lower_threshold', '')}</td>\
                    <td>{metric.get('upper_threshold', '')}</td>\
                    <td>{metric.get('weightage', '')}</td>\
                    <td>{metric.get('dimension_name', '')}</td>\
                </tr>\
                """
    except Exception as e:
        log_error(
            f"Alation Connector - Bind {_type.title()} Measures Metrics Rows Failed", e)
    finally:
        return rows_html


def get_domain_measures_data(config, _id, catagory, rule_config):
    measures = []
    try:
        """
        Get domain details by domain id
        """
        if not _id:
            return measures

        custom_rule = rule_config.get("custom", False)
        semantic_rule = rule_config.get("semantics", False)
        auto_rule = rule_config.get("auto", False)
        if custom_rule and semantic_rule and auto_rule:
            filter_query = f""""""
        else:
            filter_query = []
            if custom_rule:
                filter_query.append("custom")
            if semantic_rule:
                filter_query.append("semantic")
            if auto_rule:
                filter_query.append("distribution")
                filter_query.append("frequency")
                filter_query.append("reliability")
                filter_query.append("observe")
            if not filter_query:
                filter_query = f""""""
            else:
                filter_query = [f"""'{str(i)}'""" for i in filter_query]
                filter_query = f"""base_measure.type in ({",".join(filter_query)}) and """

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            # get the domain by name
            query_string = f"""
                WITH asset_measures AS (
                    select 
                        distinct on (measure.id)
                        measure.id, measure.technical_name as technical_name, base_measure.name,
                        measure.is_active,measure.is_positive,measure.allow_score, measure.status, 
                        dimension.name as dimension_name, base_measure.category, base_measure.type,
                        attribute.name as attribute,measure.score,base_measure.description,
                        measure.is_drift_enabled,                        
                        CAST(measure.drift_threshold->>'lower_threshold' AS NUMERIC) as lower_threshold,
                        CAST(measure.drift_threshold->>'upper_threshold' AS NUMERIC) as upper_threshold,
                        count(distinct drift_alert.id) as alerts, 
                        count(distinct issue.id) as issues, measure.weightage, 
                        case when measure.is_active and measure.allow_score then measure.row_count else 0 end as total_records,
                        measure.invalid_rows as invalid_rows,
                        measure.valid_rows as valid_rows,
                        case when measure.modified_date is not null then measure.modified_date else measure.created_date end as cmdate
                    from core.measure 
                    join core.base_measure on base_measure.id=measure.base_measure_id
                    join core.connection on connection.id=measure.connection_id
                    left join core.asset on asset.id=measure.asset_id
                    left join core.attribute as attribute on attribute.id=measure.attribute_id                    
                    left join core.dimension on dimension.id=measure.dimension_id
                    left join core.metrics as drift_alert on drift_alert.measure_id=measure.id and lower(drift_alert.drift_status) in ('high','medium','low')
                    left join core.issues as issue on issue.measure_id=measure.id and issue.is_delete=false and lower(issue.status) != 'resolved'
                    left join core.domain_mapping on (domain_mapping.asset_id=asset.id or domain_mapping.measure_id=measure.id)
                    left join core.domain on domain.id=domain_mapping.domain_id 
                    where (
                        measure.is_delete=false and connection.is_active=true
                        and (
                            (base_measure.level='measure' and asset.id is null)
                            or (base_measure.level!='measure' and asset.is_active=true and asset.is_delete=false)
                        )
                        and ((measure.attribute_id is null and attribute.id is null) or (measure.attribute_id is not null and attribute.is_selected = true))
                        and measure.is_active = True and {filter_query} domain.id = '{_id}'
	                )  
                    group by 
                        measure.id, base_measure.id, asset.id,  connection.id, dimension.id,
                        attribute.id
                    )
                    select * from asset_measures
                    """
            cursor = execute_query(connection, cursor, query_string)
            measures = fetchall(cursor)
            measures = measures if measures else []
    except Exception as e:
        log_error(
            f"Alation Connector: Get Measures data ", str(e))
    finally:
        return measures


def get_channel_metrics(channel, key):
    rule_config = {}
    export_conf = channel.get("export", [])
    if isinstance(export_conf, str):
        export_conf = json.loads(export_conf)
    for export_config in export_conf:
        if export_config.get(key, False):
            rule_config = export_config.get("options", {})

    return rule_config


def set_alation_measures_metrics(config, channel, _id, _type='asset', catagory=None):
    measures_html = ""
    try:
        if _type == 'domain':
            export_conf = channel.get("export", [])
            if isinstance(export_conf, str):
                export_conf = json.loads(export_conf)
            for export_config in export_conf:
                if export_config.get("domain", False):
                    rule_config = export_config.get("options", {})
            measures = get_domain_measures_data(
                config, _id, catagory, rule_config)
        else:
            measures = get_measures_data(config, _id, _type)
        dag_info = config.get("dag_info")
        client_origin = get_client_origin(dag_info)
        if measures and len(measures) > 0:
            measures_html = f"""<table style="border-style: inset;margin-top:30px;">\
                                        <thead>\
                                            <tr>\
                                                <th style="width: 45%;" colspan="18">{"Standalone Measures" if catagory else "Measures"}</th>\
                                            </tr>
                                            <tr>\
                                                <th>Rule Name</th>\
                                                <th>Status</th>\
                                                <th>DQ Score</th>\
                                                <th>Failed Rows</th>\
                                                <th>Passed Rows</th>\
                                                <th>Total Rows</th>\
                                                <th>Description</th>\
                                                <th>Last Update Date</th>\
                                                <th>Active</th>\
                                                <th>Valid</th>\
                                                <th>Scoring</th>\
                                                <th>Monitor</th>\
                                                <th>Category</th>\
                                                <th>Type</th>\
                                                <th>Alerts</th>\
                                                <th>Issues</th>\
                                                <th>Upper Threshold</th>\
                                                <th>Lower Threshold</th>\
                                                <th>Weightage</th>\
                                                <th>Dimension</th>\
                                            </tr>\
                                        </thead>\
                                        <tbody>
                                            {bind_measures_metrics_rows(measures, client_origin, _type)}                                         
                                        </tbody>\
                                    </table>"""
    except Exception as e:
        log_error(
            f"Alation Connector - Get measures metrics Failed", e)
    finally:
        return measures_html


def get_issue_count(config, asset_id):
    try:
        alert_count = 0
        distinct_count = 0
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select measure.asset_id, count(distinct alert.id) as alerts_count, count(distinct issues.id) as issues_count
                    from core.measure
                    join core.base_measure on base_measure.id=measure.base_measure_id
                    join core.connection as connection on connection.id=measure.connection_id and connection.is_active=true
                    join core.asset as asset on asset.id=measure.asset_id and asset.is_active=true and asset.is_delete = false
                    left join core.attribute as attribute on attribute.id=measure.attribute_id and attribute.is_selected=true
                    left join core.metrics as alert on alert.measure_id=measure.id and alert.run_id=measure.last_run_id and lower(alert.drift_status) in ('high', 'medium', 'low')
                    left join core.issues on issues.measure_id=measure.id and issues.is_delete = false
                    where measure.asset_id='{asset_id}' and measure.is_active=true
                    and asset.is_active=true
                    group by measure.asset_id
            """
            cursor = execute_query(connection, cursor, query_string)
            issues = fetchone(cursor)
            if issues:
                alert_count = issues.get('alerts_count', 0)
                distinct_count = issues.get('issues_count', 0)
        return alert_count, distinct_count
    except Exception as e:
        return alert_count, distinct_count


def set_alation_table_metrics(config, channel, metrics, token, schema):
    try:
        asset_id = config.get("asset_id")
        asset_name = metrics.get('name', '')
        if not asset_id:
            asset_id = metrics.get("asset_id")
        channel_host_url = channel.get('host', '')
        schema_id = schema.get('id', '')
        ds_id = schema.get('ds_id', '')
        custom_field_api_url = f"{channel_host_url}/api/v1/bulk_metadata/custom_fields/default/table?create_new=false&replace_values=true"
        dag_info = config.get("dag_info")
        client_origin = get_client_origin(dag_info)
        link = f"""{client_origin}/observe/data/{asset_id}/measures"""
        if schema_id and ds_id:
            api_url = f"{channel_host_url}/integration/v2/table/?name={asset_name}&schema_id={schema_id}&ds_id={ds_id}&limit=1000"

            tables = call_api_request(api_url, 'get', channel, '', token)
            alert_count, distinct_count = get_issue_count(config, asset_id)
            if tables and len(tables) > 0:

                # Map Alation Domain to DQLabs
                if validate_channel_config_permission(channel, 'import', 'domains'):
                    map_alation_domain_to_dqlabs(
                        config, channel, tables[0], asset_id)

                singular_field_name = "DQLABS"  # the custom field name we're setting in alation
                asset_summary_table, asset_alert_table, asset_issues_table, asset_measures_table = '', '', '', ''

                # Check Metrics Export Permission
                if validate_channel_config_permission(channel, 'export', 'summary', 'asset'):
                    asset_summary_table = f"""<table style="border-style: inset;">\
                                                <thead>\
                                                    <tr>\
                                                        <th colspan="2"><strong>Summary</strong></th>\
                                                    </tr>\
                                                    <tr>\
                                                        <th style="width: 25%;">Metrics</th>\
                                                        <th>Values</th>\
                                                    </tr>\
                                                </thead>\
                                                <tbody>\
                                                    <tr>\
                                                        <td>DQ Score</td>\
                                                        <td><strong>{rounding(metrics.get('score', 0))}%</strong></td>\
                                                    </tr>\
                                                    <tr>\
                                                        <td>Total Rows</td>\
                                                        <td><strong>{metrics.get('row_count', ''):,}</strong></td>\
                                                    </tr>\
                                                    <tr>\
                                                        <td>Freshness</td>\
                                                        <td><strong>{format_freshness(metrics.get('freshness', 0))}</strong></td>\
                                                    </tr>\
                                                    <tr>\
                                                        <td>Active Rules</td>\
                                                        <td><strong>{metrics.get('active_measures', '') if metrics.get('active_measures', None) != None else ''}</strong></td>\
                                                    </tr>\
                                                    <tr>\
                                                        <td>Observed Rules</td>\
                                                        <td><strong>{metrics.get('observed_measures', '') if metrics.get('observed_measures', None) != None else ''}</strong></td>\
                                                    </tr>\
                                                    <tr>\
                                                        <td>Scoring Rules</td>\
                                                        <td><strong>{rounding(metrics.get('scored_measures', 0)) if metrics.get('scored_measures', 0) else rounding(0)}</strong></td>\
                                                    </tr>\
                                                    <tr>\
                                                        <td>Duplicate Rows</td>\
                                                        <td><strong>{metrics.get('duplicate_count', ''):,}</strong></td>\
                                                    </tr>\
                                                    <tr>\
                                                        <td>Alerts</td>\
                                                        <td><strong><a href="{client_origin}/remediate/alerts?asset_id={asset_id}" target="_blank" rel="noopener noreferrer">{alert_count}</a></strong></td>\
                                                    </tr>\
                                                    <tr>\
                                                        <td>Issues</td>\
                                                        <td><strong><a href="{client_origin}/remediate/issues?asset_id={asset_id}" target="_blank" rel="noopener noreferrer">{distinct_count}</a></strong></td>\
                                                    </tr>\
                                                    <tr>\
                                                        <td>Terms</td>\
                                                        <td><strong>{metrics.get('terms', ''):,}</strong></td>\
                                                    </tr>\
                                                    <tr>\
                                                        <td>Last Updated Date</td>\
                                                        <td><strong>{metrics.get('last_updated_date').strftime("%A, %d. %B %Y %I:%M%p %Z")}</strong></td>\
                                                    </tr>\
                                                    <tr>\
                                                        <td colspan="2"><div style="margin-left:82%;"><strong>See more <a href={link} target="_blank" rel="noopener noreferrer">click here</a></strong></div></td>\
                                                    </tr>\
                                                </tbody>\
                                            </table>"""

                # Check Alerts Export Permission
                if validate_channel_config_permission(channel, 'export', 'alerts', 'asset'):
                    asset_alert_table = set_alation_alerts_metrics(
                        config, channel, asset_id)

                # Check issues Export Permission
                if validate_channel_config_permission(channel, 'export', 'issues', 'asset'):
                    asset_issues_table = set_alation_issues_metrics(
                        config, channel, asset_id)

                # Check Measures Export Permission
                if validate_channel_config_permission(channel, 'export', 'measures', 'asset'):
                    asset_measures_table = set_alation_measures_metrics(
                        config, channel, asset_id, 'asset')

                custom_field_value = f"""<div>\
                                        {asset_summary_table}
                                        {asset_alert_table}
                                        {asset_issues_table}
                                        {asset_measures_table}
                                    </div>"""
                data = {"key": tables[0].get(
                    'key'), singular_field_name: custom_field_value}
                call_api_request(custom_field_api_url,
                                 'post', channel, data, token)

                # if validate_channel_config_permission(channel, 'export', 'summary', 'attribute') or validate_channel_config_permission(channel, 'export', 'measures') or validate_channel_config_permission(channel, 'import', 'tags'):
                set_alation_columns_metrics(
                    config, channel, metrics, token, tables[0], asset_id)
    except Exception as e:
        log_error(
            f"Alation Connector - Get Tables ", e)

def set_alation_columns_metrics(config, channel, metrics, token, table, asset_id):
    try:
        metrics = get_attributes_metrics(config, asset_id)
        channel_host_url = channel.get('host', '')
        table_id = table.get('id', '')
        schema_id = table.get('schema_id', '')
        ds_id = table.get('ds_id', '')
        custom_field_api_url = f"{channel_host_url}/api/v1/bulk_metadata/custom_fields/default/attribute?create_new=false&replace_values=true"

        dag_info = config.get("dag_info")
        client_origin = get_client_origin(dag_info)

        if metrics and table_id and schema_id and ds_id:
            api_url = f"{channel_host_url}/integration/v2/column/?table_id={table_id}&schema_id={schema_id}&ds_id={ds_id}"
            columns = call_api_request(api_url, 'get', channel, '', token)

            if columns and len(columns) > 0:
                singular_field_name = "DQLABS"  # the custom field name we're setting in alation
                for column in columns:
                    map_matric = next((x for x in metrics if x.get(
                        'name').lower() == column.get('name').lower()), None)
                    if map_matric:
                        asset_id = map_matric.get('asset_id')
                        attr_id = map_matric.get('attr_id')
                        attribute_id = map_matric.get('attribute_id')
                        attribute_summary_table, attribute_alerts_table, attribute_issues_table, attribute_measures_table = '', '', '', ''

                        # Check Summary Export Permission
                        if validate_channel_config_permission(channel, 'export', 'summary', 'attribute'):
                            link = f"""{client_origin}/observe/data/{asset_id}/attributes/{attribute_id}"""
                            attribute_summary_table = f"""<table style="border-style: inset;">\
                                                    <thead>\
                                                        <tr>\
                                                            <th colspan="2"><strong>Summary</strong></th>\
                                                        </tr>\
                                                        <tr>\
                                                            <th style="width: 25%;">Metrics</th>\
                                                            <th>Values</th>\
                                                        </tr>\
                                                    </thead>\
                                                    <tbody>\
                                                        <tr>\
                                                            <td>DQ Score</td>\
                                                            <td><strong>{rounding(map_matric.get('score', 0))}%</strong></td>\
                                                        </tr>\
                                                        <tr>\
                                                            <td>Total Rows</td>\
                                                            <td><strong>{map_matric.get('row_count', ''):,}</strong></td>\
                                                        </tr>\
                                                        <tr>\
                                                            <td>Active Rules</td>\
                                                            <td><strong>{map_matric.get('active_measures', ''):,}</strong></td>\
                                                        </tr>\
                                                        <tr>\
                                                            <td>Observed Rules</td>\
                                                            <td><strong>{map_matric.get('observed_measures', ''):,}</strong></td>\
                                                        </tr>\
                                                        <tr>\
                                                            <td>Scoring Rules</td>\
                                                            <td><strong>{rounding(map_matric.get('scored_measures', 0)):,}</strong></td>\
                                                        </tr>\
                                                        <tr>\
                                                            <td>Valid Rows</td>\
                                                            <td><strong>{map_matric.get('valid_rows', ''):,}</strong></td>\
                                                        </tr>\
                                                        <tr>\
                                                            <td>Invalid Rows</td>\
                                                            <td><strong>{map_matric.get('invalid_rows', ''):,}</strong></td>\
                                                        </tr>\
                                                        <tr>\
                                                            <td>Alerts</td>\
                                                             <td><strong><a href="{client_origin}/remediate/alerts?attribute_id={attribute_id}" target="_blank" rel="noopener noreferrer">{map_matric.get('alerts', '')}</a></strong></td>\
                                                        </tr>\
                                                        <tr>\
                                                            <td>Issues</td>\
                                                            <td><strong><a href="{client_origin}/remediate/issues?attribute_id={attribute_id}" target="_blank" rel="noopener noreferrer">{map_matric.get('issues', '')}</a></strong></td>\
                                                        </tr>\
                                                        <tr>\
                                                            <td>Terms</td>\
                                                            <td><strong>{map_matric.get('terms', ''):,}</strong></td>\
                                                        </tr>\
                                                        <tr>\
                                                            <td>Last Updated Date</td>\
                                                            <td><strong>{map_matric.get('last_updated_date').strftime("%A, %d. %B %Y %I:%M%p %Z")}</strong></td>\
                                                        </tr>\
                                                        <tr>\
                                                            <td colspan="2"><div style="margin-left:82%;"><strong>See more <a href={link} target="_blank" rel="noopener noreferrer">click here</a></strong></div></td>\
                                                        </tr>\
                                                    </tbody>\
                                                </table>"""

                        # Check alerts Export Permission
                        if validate_channel_config_permission(channel, 'export', 'alerts', 'attribute'):
                            attribute_alerts_table = set_alation_alerts_metrics(
                                config, channel, attribute_id, 'attribute')

                        # Check issues Export Permission
                        if validate_channel_config_permission(channel, 'export', 'issues', 'attribute'):
                            attribute_issues_table = set_alation_issues_metrics(
                                config, channel, attribute_id, 'attribute')

                        # Check Measures Export Permission
                        if validate_channel_config_permission(channel, 'export', 'measures', 'attribute'):
                            attribute_measures_table = set_alation_measures_metrics(
                                config, channel, attr_id, 'attribute')

                        # final html for custom field
                        custom_field_value = f"""<div>\
                                        {attribute_summary_table}
                                        {attribute_alerts_table}
                                        {attribute_issues_table}
                                        {attribute_measures_table}
                                    </div>"""

                        # creating payload
                        data = {"key": column.get(
                            'key'), singular_field_name: custom_field_value}
                        call_api_request(custom_field_api_url,
                                         'post', channel, data, token)

                # Check Measures Export Permission
                if validate_channel_config_permission(channel, 'export', 'measures', 'asset') or validate_channel_config_permission(channel, 'export', 'measures', 'attribute'):
                    set_alation_health_measures(
                        config, channel, token, columns, table)

                # Check Tags Import Permission
                if validate_channel_config_permission(channel, 'import', 'tags'):
                    map_alation_tags_to_dqlabs(
                        config, channel, token, columns, metrics)

    except Exception as e:
        log_error(
            f"Alation Connector - Get Columns ", e)


def map_alation_tags_to_dqlabs(config: dict, channel: dict, token, columns: dict, metrics):
    try:
        for column in columns:
            map_matric = next((x for x in metrics if x.get(
                'name').lower() == column.get('name').lower()), None)
            if map_matric:
                get_alation_tags_by_column(
                    config, channel, token, column, map_matric)
    except Exception as e:
        log_error(
            f"Alation Tags Mapping Failed ", e)


def get_alation_tags_by_column(config, channel, token, column, dq_column):
    try:
        column_id = column.get('id')
        channel_host_url = channel.get('host', '')
        api_url = f"{channel_host_url}/integration/v2/tag/?oid={column_id}&otype=attribute"
        tags = call_api_request(api_url, 'get', channel, '', token)
        tags = tags if tags else []
        alation_tags_to_dQColumns_map(config, tags, dq_column)
    except Exception as e:
        log_error(
            f"Alation Connector - Get Tags for Columns ", e)


def alation_tags_to_dQColumns_map(config: dict, tags: list, dq_column: dict):
    try:
        tag_input = []
        attribute_id = dq_column.get('attribute_id')
        connection = get_postgres_connection(config)
        with connection:
            with connection.cursor() as cursor:
                if tags and len(tags) > 0:
                    mapped_tag_ids = list(
                        map(lambda tag: f"""'{tag.get("id")}'""", tags))
                    mapped_tag_ids = ", ".join(mapped_tag_ids)
                    mapped_tag_ids = f"({mapped_tag_ids})"

                    delete_mapped_tags_query = f"""
                        delete from core.tags_mapping where id in (
                            select tags_mapping.id from core.tags 
                            join core.tags_mapping on tags_mapping.tags_id = tags.id 
                            and tags_mapping.attribute_id = '{attribute_id}' 
                            where (properties->>'type')::text = 'alation' 
                            and (properties->>'alation_tag_id')::text not in {mapped_tag_ids}
                        )
                    """
                    cursor = execute_query(
                        connection, cursor, delete_mapped_tags_query)

                    check_existing_query = f"""
                            select id, properties->>'alation_tag_id' as alation_tag_id from core.tags
                            where (properties->>'type')::text = 'alation' 
                            and (properties->>'alation_tag_id')::text in {mapped_tag_ids}
                        """
                    cursor = execute_query(
                        connection, cursor, check_existing_query)
                    mapped_tags = fetchall(cursor)
                    mapped_tags = mapped_tags if mapped_tags else []

                    for tag in mapped_tags:
                        tag_id = tag.get('id')
                        check_existing_query = f"""
                                select id from core.tags_mapping 
                                where attribute_id = '{attribute_id}' and tags_id = '{tag_id}'
                            """
                        cursor = execute_query(
                            connection, cursor, check_existing_query)
                        existing_map_data = fetchone(cursor)

                        if existing_map_data:
                            continue

                        query_input = (
                            str(uuid4()),
                            'attribute',
                            str(tag_id),
                            str(attribute_id)
                        )
                        input_literals = ", ".join(
                            ["%s"] * len(query_input))
                        query_param = cursor.mogrify(
                            f"({input_literals}, CURRENT_TIMESTAMP)", query_input
                        ).decode("utf-8")
                        tag_input.append(query_param)

                    tag_input = split_queries(tag_input)
                    for input_values in tag_input:
                        try:
                            query_input = ",".join(input_values)
                            query_string = f"""
                                    insert into core.tags_mapping(
                                        id, level, tags_id, attribute_id, created_date
                                    ) values {query_input}
                                """
                            cursor = execute_query(
                                connection, cursor, query_string)
                        except Exception as e:
                            log_error(
                                'Alation Tags Map to Metadata Insert Failed  ', e)
                else:
                    delete_mapped_tags_query = f"""
                            delete from core.tags_mapping where id in (
                                select tags_mapping.id from core.tags 
                                join core.tags_mapping on tags_mapping.tags_id = tags.id 
                                and tags_mapping.attribute_id = '{attribute_id}'
                                where (properties->>'type')::text = 'alation'
                            )
                        """
                    cursor = execute_query(
                        connection, cursor, delete_mapped_tags_query)

    except Exception as e:
        log_error(
            f"Alation Connector - Save Tags for Columns ", e)

def set_alation_health_measures(config, channel, token, columns, table):
    try:
        metrics = get_measure_metrics(config)
        channel_host_url = channel.get('host', '')
        api_url = f"{channel_host_url}/integration/v1/data_quality/"

        dag_info = config.get("dag_info")
        client_origin = get_client_origin(dag_info)

        alation_fields = []
        alation_values = []
        flags = get_alation_flags(channel, token)

        if metrics and len(metrics) > 0:
            # checking the permissions for asset measure
            if validate_channel_config_permission(channel, 'export', 'measures', 'asset'):
                asset_measures = list(filter(lambda attribute: attribute.get("level") == 'asset' and attribute.get("asset_name", '').lower(
                ) == table.get('name').lower(), metrics))

                asset_has_alets = list(filter(lambda attribute: attribute and attribute.get(
                    "level") == 'asset' and (attribute.get("asset_alerts") or 0) > 0, metrics))
                asset_has_issues = list(filter(lambda attribute: attribute and attribute.get(
                    "level") == 'asset' and (attribute.get("asset_issues") or 0) > 0, metrics))

                clear_alation_flags(
                    channel, token, table.get('id'), flags)
                if asset_has_alets and len(asset_has_alets) > 0:
                    set_alation_flags(channel, table.get(
                        'id'), 'WARNING', 'table', 'alerts', token, asset_measures[0] if len(asset_measures) > 0 else None, config)

                if asset_has_issues and len(asset_has_issues) > 0:
                    set_alation_flags(channel, table.get(
                        'id'), 'DEPRECATION', 'table', 'issues', token, asset_measures[0] if len(asset_measures) > 0 else None, config)

                for measure in asset_measures:
                    status = measure.get('status', '')
                    status = status.lower() if status else ''
                    if status == 'high':
                        status = "ALERT"
                    elif status in ['low', 'medium']:
                        status = "WARNING"
                    else:
                        status = "GOOD"

                    measure_value = measure.get('value', 0)
                    measure_name = measure.get('name')
                    if measure_name == 'Freshness':
                        m_value = int(measure_value) if measure_value else 0
                        measure_value = format_freshness(m_value)

                    field_key = f"{str(measure.get('asset_id'))}.{str(measure.get('measure_id'))}"
                    alation_fields.append({
                        "field_key": field_key,
                        "name": measure_name,
                        "description": measure.get('description'),
                        "type": "STRING"
                    })

                    alation_values.append({
                        "field_key": field_key,
                        "object_key": table.get('key'),
                        "object_type": "TABLE",
                        "status": status,
                        "value": measure_value,
                        "url": f"{client_origin}/asset/{str(measure.get('asset_id'))}"
                    })

            # checking the permissions for attribute measure
            if validate_channel_config_permission(channel, 'export', 'measures', 'attribute'):
                for column in columns:
                    measures = list(filter(lambda attribute: attribute.get("level") == 'attribute' and attribute.get("attribute_name", '').lower(
                    ) == column.get('name').lower(), metrics))

                    attribute_has_alerts = list(filter(lambda attribute: attribute.get("level") == 'attribute' and attribute.get("attribute_name", '').lower(
                    ) == column.get('name').lower() and (attribute.get("attribute_alerts") or 0) > 0, metrics))

                    attribute_has_issues = list(filter(lambda attribute: attribute.get("level") == 'attribute' and attribute.get("attribute_name", '').lower(
                    ) == column.get('name').lower() and (attribute.get("attribute_issues") or 0) > 0, metrics))

                    for measure in measures:
                        status = measure.get('status', '')
                        status = status.lower() if status else ''
                        if status == 'high':
                            status = "ALERT"
                        elif status in ['low', 'medium']:
                            status = "WARNING"
                        else:
                            status = "GOOD"
                        field_key = f"{str(measure.get('attribute_id'))}.{str(measure.get('measure_id'))}"

                        alation_fields.append({
                            "field_key": field_key,
                            "name": measure.get('name'),
                            "description": measure.get('description'),
                            "type": "STRING"
                        })

                        alation_values.append({
                            "field_key": field_key,
                            "object_key": column.get('key'),
                            "object_type": "ATTRIBUTE",
                            "status": status,
                            "value": measure.get('value'),
                            "url": f"{client_origin}/measure/{str(measure.get('measure_id'))}/detail"
                        })

                    clear_alation_flags(
                        channel, token, column.get('id'), flags)
                    if attribute_has_alerts and len(attribute_has_alerts) > 0:
                        set_alation_flags(channel, column.get(
                            'id'), 'WARNING', 'attribute', 'alerts', token, measures[0] if len(measures) > 0 else None, config)

                    if attribute_has_issues and len(attribute_has_issues) > 0:
                        set_alation_flags(channel, column.get(
                            'id'), 'DEPRECATION', 'attribute', 'alerts', token, measures[0] if len(measures) > 0 else None, config)

            if alation_fields and alation_values:
                payload = {
                    "fields": alation_fields,
                    "values": alation_values
                }
                call_api_request(api_url,
                                 'post',channel, payload, token)
    except Exception as e:
        log_error(
            f"Alation Connector - Update Health Metrics ", e)


def get_alation_flags(channel, token):
    flags = []
    try:
        channel_host_url = channel.get('host', '')
        api_url = f"{channel_host_url}/integration/flag/"
        flags = call_api_request(api_url, 'get', channel, '', token)
    except Exception as e:
        log_error(
            f"Alation Connector - Get Flags ", e)
    finally:
        return flags


def clear_alation_flags(channel, token, id, flags):
    try:
        channel_host_url = channel.get('host', '')
        delete_api_url = f"{channel_host_url}/integration/flag/"
        for flag in flags:
            flag_subject = flag.get('subject')
            obj_id = flag_subject.get('id')

            if obj_id == id:
                flag_id = flag.get('id')
                channel_host_url = channel.get('host', '')
                api_url = f"{delete_api_url}/{flag_id}/"
                call_api_request(api_url, 'delete', channel, '', token)
    except Exception as e:
        log_error(
            f"Alation Connector - Clear Flags ", e)


def set_alation_flags(channel, id, flag_type, otype, type, token, data, config):
    try:
        channel_host_url = channel.get('host', '')
        api_url = f"{channel_host_url}/integration/flag/"

        dag_info = config.get("dag_info")
        client_origin = get_client_origin(dag_info)

        link = ''
        name = ''

        if data:
            if otype == 'table':
                name = data.get('asset_name')
                link = f"{client_origin}/observe/data/{str(data.get('asset_id'))}/measures"
            else:
                link = f"{client_origin}/observe/data/{str(data.get('asset_id'))}/attributes/{str(data.get('attribute_id'))}"
                name = data.get('attribute_name')

        content = f"Identified {type} with this {otype}"
        if link and name:
            content = f"{content} <a href={link}>{name}</href>"

        payload = {
            "flag_type": flag_type,
            "flag_reason": content,
            "subject": {
                "id": id,
                "otype": otype
            }
        }
        call_api_request(api_url,
                         'post', channel, payload, token)
    except Exception as e:
        log_error(
            f"Alation Connector - Set Flags ", e)


def get_alation_schema(config, channel, datasources, token):
    try:
        for datasource in datasources:
            metrics = datasource.get("current_metrics")
            credentials = metrics.get('credentials', None)
            properties = metrics.get('properties', None)
            channel_host_url = channel.get('host', '')

            database_name = credentials.get('database', '')
            database_name = database_name.lower() if database_name else ''

            if not database_name:
                database_name = properties.get('database', '')

            schema_name = properties.get('schema', '')
            schema_name = schema_name.lower() if schema_name else ''

            database_name = f"{database_name}.{schema_name}"
            ds_id = datasource.get("id")

            if schema_name and database_name:
                api_url = f"{channel_host_url}/integration/v2/schema/?ds_id={ds_id}&limit=1000"
                schemas = call_api_request(api_url, 'get', channel, '', token)
                if schemas:
                    schemas = list(filter(lambda schema: schema.get("name").lower(
                    ) == schema_name.lower() or schema.get("name").lower() == database_name.lower() 
                    or schema.get("name").lower() == f"{database_name.lower()}.{schema_name.lower()}", schemas))

                    for schema in schemas:
                        set_alation_table_metrics(
                            config, channel, copy.deepcopy(metrics), token, schema)
    except Exception as e:
        log_error(
            f"Alation Connector - Get Schemas ", e)


def rounding(value):
    try:
        if isinstance(value, int):
            return value
        elif isinstance(value, str):
            try:
                return round((float(value)), 2)
            except:
                return 0
        elif isinstance(value, float):
            return round(value, 2)
        else:
            return value
    except:
        return value


def get_glossary_record(config, domain_id):
    glossary = {}
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select id, properties->>'alation_domain_id' as alation_domain_id from core.domain
                where (properties->>'type')::text = 'alation' 
                and (properties->>'alation_domain_id')::text = '{domain_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            glossary = fetchone(cursor)
            if not glossary:
                glossary = {}
            return glossary
    except Exception as e:
        log_error(
            f"Alation Connector - Get glossary record ", e)
    finally:
        return glossary

def set_alation_domain_metrics(config, channel, token):
    try:
        channel_host_url = channel.get('host', '')
        custom_field_api_url = f"{channel_host_url}/integration/v2/custom_field_value/"
        dag_info = config.get("dag_info")
        client_origin = get_client_origin(dag_info)
        domains_data = get_alation_domains(channel, token)
        domains = domains_data.get('list')

        if domains and len(domains) > 0:
            custom_field_value_url = f"{channel_host_url}/integration/v2/custom_field/?allow_multiple=false&field_type=RICH_TEXT&limit=100&name_singular=DQLABS&skip=0"
            custom_field = call_api_request(
                custom_field_value_url, 'get', channel, '', token)
            singular_field_id = None
            if custom_field:
                singular_field_id = custom_field[0].get("id")
            # singular_field_id = 10010  # the custom field id we're setting in alation for domain
            for domain in domains:
                domain_id = domain.get('id', None)
                glossary = get_glossary_record(config, domain_id)
                glossary_id = glossary.get('id')
                if domain_id and glossary_id:
                    domain_metrices_table, domain_issues_table, domain_alerts_table, domain_measures_table = '', '', '', ''
                    # Check Domain Metrics Export Permission
                    if validate_channel_config_permission(channel, 'export', 'summary', 'domain'):
                        metrics = get_domain_metrics(config, glossary_id)
                        link = f"""{client_origin}/discover/semantics/domains/{glossary_id}/assets"""
                        domain_metrices_table = f"""<table style="border-style: inset;">\
                                                    <thead>\
                                                        <tr>\
                                                            <th colspan="2"><strong>Summary</strong></th>\
                                                        </tr>\
                                                        <tr>\
                                                            <th style="width: 25%;">Metrics</th>\
                                                            <th>Values</th>\
                                                        </tr>\
                                                    </thead>\
                                                    <tbody>\
                                                        <tr>\
                                                            <td>DQ Score</td>\
                                                            <td><strong>{rounding(metrics.get('score', 0))}%</strong></td>\
                                                        </tr>\
                                                        <tr>\
                                                            <td>Tables</td>\
                                                            <td><strong>{metrics.get('tables', '')}</strong></td>\
                                                        </tr>\
                                                        <tr>\
                                                            <td>Views</td>\
                                                            <td><strong>{metrics.get('views', '')}</strong></td>\
                                                        </tr>\
                                                        <tr>\
                                                            <td>Attributes</td>\
                                                            <td><strong>{metrics.get('attributes', '')}</strong></td>\
                                                        </tr>\
                                                        <tr>\
                                                            <td>Reports</td>\
                                                            <td><strong>{metrics.get('reports', '')}</strong></td>\
                                                        </tr>\
                                                        <tr>\
                                                            <td>Pipeline</td>\
                                                            <td><strong>{metrics.get('pipeline', '')}</strong></td>\
                                                        </tr>\
                                                        <tr>\
                                                            <td>Alerts</td>\
                                                            <td><strong><a href="{client_origin}/remediate/alerts?domains={glossary_id}" target="_blank" rel="noopener noreferrer">{metrics.get('alerts', 0)}</a></strong></td>\
                                                        </tr>\
                                                        <tr>\
                                                            <td>Issues</td>\
                                                            <td><strong><a href="{client_origin}/remediate/issues?domains={glossary_id}" target="_blank" rel="noopener noreferrer">{metrics.get('issues', 0)}</a></strong></td>\
                                                        </tr>\
                                                        <tr>\
                                                            <td>Measures</td>\
                                                            <td><strong><a href="{client_origin}/discover/semantics/domains/{glossary_id}/measures" target="_blank" rel="noopener noreferrer">{metrics.get('measures', '')}</a></strong></td>\
                                                        </tr>\
                                                        <tr>\
                                                            <td>Users</td>\
                                                            <td><strong>{metrics.get('users', '')}</strong></td>\
                                                        </tr>\
                                                        <tr>\
                                                            <td colspan="2"><div style="margin-left:82%;"><strong>See more <a href={link} target="_blank" rel="noopener noreferrer">click here</a></strong></div></td>\
                                                        </tr>\
                                                    </tbody>\
                                                </table>"""

                    channel_metrics = get_channel_metrics(channel, 'domain')
                    # Check Domain Issues Export Permission
                    if validate_channel_config_permission(channel, 'export', 'issues', 'domain'):
                        domain_issues_table = ""
                        if channel_metrics.get("issues", False):
                            domain_issues_table = set_alation_issues_metrics(
                                config, channel, glossary_id, 'domain')

                    # Check Domain alerts Export Permission
                    if validate_channel_config_permission(channel, 'export', 'alerts', 'domain'):
                        domain_alerts_table = ""
                        if channel_metrics.get("alerts", False):
                            domain_alerts_table = set_alation_alerts_metrics(
                                config, channel, glossary_id, 'domain')

                    # Check Domain Measure Export Permission
                    if validate_channel_config_permission(channel, 'export', 'measures', 'domain'):
                        domain_measures_table = ""
                        if channel_metrics.get("measures", False):
                            domain_measures_table = set_alation_measures_metrics(
                                config, channel, glossary_id, 'domain', '')

                        # domain_standalone_measures_table = set_alation_measures_metrics(
                        #     config, channel, glossary_id, 'domain', 'standalone')

                    custom_field_value = f"""<div>\
                                                {domain_metrices_table}
                                                {domain_alerts_table}
                                                {domain_issues_table}
                                                {domain_measures_table}
                                            </div>"""
                    if singular_field_id:
                        data = [{
                            "otype": "domain",
                            "oid": str(domain.get('id')),
                            "field_id": singular_field_id,
                            "value": custom_field_value
                        }]
                    else:
                        data = [{
                            "otype": "domain",
                            "oid": str(domain.get('id')),
                            "value": custom_field_value
                        }]
                    res = call_api_request(custom_field_api_url,
                                           'put', channel, data, token)

    except Exception as e:
        log_error(
            f"Alation Connector - Get Domains ", e)


def get_alation_datasource(config, channel, metrics, token):
    try:
        channel_host_url = channel.get('host', '')
        api_url_v1 = f"{channel_host_url}/integration/v1/datasource/?limit=1000"
        datasources_v1 = call_api_request(api_url_v1, 'get', channel, '', token)

        # v2 ocf connector api configuration
        api_url_v2 = f"{channel_host_url}/integration/v2/datasource/"
        datasources_v2 = call_api_request(api_url_v2, 'get', channel, '', token)

        valid_datasources = channel.get("datasource", [])
        # Append the two datasources lists
        all_datasources = []
        if datasources_v1:
            all_datasources.extend(datasources_v1)
        if datasources_v2:
            all_datasources.extend(datasources_v2)
        datasources = list(filter(lambda datasource: datasource.get(
            "title") if datasource.get("title") in valid_datasources else '', all_datasources))
        
        if datasources and valid_datasources:
            datasources = list(filter(lambda datasource: datasource.get(
                "title") if datasource.get("title") in valid_datasources else '', datasources))
        final_filter_datasource = []
        for current_metrics in metrics:
            credentials = current_metrics.get('credentials', None)
            server_host = credentials.get('account', '')

            if not server_host:
                server_host = credentials.get('server', '')
            if not server_host:
                server_host = credentials.get('host', '')
            server_host = server_host.lower() if server_host else ''

            database_name = credentials.get('database', '')
            database_name = database_name.lower() if database_name else ''

            connection_type = current_metrics.get('connection_type', '')
            connection_type = connection_type.lower() if connection_type else ''

            # Get the valid schema from connection
            properties = current_metrics.get('properties', None)
            schema = properties.get('schema', '')
            schema = schema.lower() if schema else ''

            if not database_name:
                database_name = properties.get('database', '')

            if server_host and database_name:
                if datasources:
                    filter_datasources = list(filter(lambda datasource: datasource.get("dbtype").lower() if datasource.get("dbtype") else '' == connection_type
                                                     and server_host in datasource.get("host").lower() if datasource.get("host") else '', datasources))
                    if connection_type == 'databricks':
                        filter_datasources = list(filter(lambda datasource: datasource.get(
                            "dbtype", "").lower() == 'databricks' if datasource.get("dbtype") else '', datasources))

                    if len(filter_datasources) == 0:
                        server_port = credentials.get('port', '')
                        if server_port:
                            server_host = f"{server_host}:{server_port}"
                        api_url = f"{channel_host_url}/integration/v2/datasource/?limit=1000"
                        datasources = call_api_request(
                            api_url, 'get', channel, '', token)
                        filter_datasources = list(filter(lambda datasource: server_host in datasource.get(
                            "uri").lower() if datasource.get("uri") else '', datasources))

                    for filter_datasource in filter_datasources:
                        filter_datasource.update({
                            "current_metrics": copy.deepcopy(current_metrics)
                        })
                        final_filter_datasource.append(
                            copy.deepcopy(filter_datasource))

        if final_filter_datasource:
            get_alation_schema(
                config, channel, final_filter_datasource, token)

        # set alation domain metrics
        set_alation_domain_metrics(config, channel, token)

    except Exception as e:
        log_error(
            f"Alation Connector - Get Datasource ", e)


def run_alation_catalog(config: dict, channel: dict):
    metrics = get_asset_metrics(config)
    if metrics:
        token = get_alation_token(channel)

        if token:
            # Check Domains Import Permission
            if validate_channel_config_permission(channel, 'import', 'domains'):
                domains = get_alation_domains(channel, token)
                channel.update(
                    {"domains": domains if domains else None})
                clean_alation_domains(config, channel)
                prepare_insert_alation_domains(config, channel)

            # Check Tags Import Permission
            if validate_channel_config_permission(channel, 'import', 'tags'):
                tags = get_alation_tags(channel, token)
                channel.update(
                    {"tags": tags if tags else None})
                clean_alation_tags(config, channel)
                insert_alation_tags(config, channel)

            get_alation_datasource(
                config, channel, metrics, token)


def run_catalog_schedule(config: dict, **kwargs):
    """
    Run user activity task
    """
    try:
        update_queue_detail_status(config, ScheduleStatus.Running.value)
        update_queue_status(config, ScheduleStatus.Running.value, True)
        channels = config.get("dag_info").get("catalog")
        for channel_name in channels:
            channel = copy.deepcopy(channel_name.get("configuration"))
            channel_name = channel_name.get("technical_name")
            if channel_name == "alation":
                run_alation_catalog(config, channel)
            elif channel_name == "collibra":
                assets = get_collibra_asset_details(config, channel)
                attributes = get_collibra_attributes_details(config, channel)
                measures = get_collibra_measures(config, channel)
                if (measures or assets or attributes):
                    collibra_catalog_update(config, channel, measures, assets, attributes)
            elif channel_name == "atlan":
                atlan_client = Atlan(config, channel)
                atlan_client.atlan_catalog_update()
            elif channel_name == "purview":
                purview_client = Purview(config, channel)
                purview_client.purview_catalog_update()
            elif channel_name == "databricks_uc":
                databricks_uc_client = DatabricksUC(config, channel)
                databricks_uc_client.databricks_uc_catalog_update()
            elif channel_name == "coalesce":
                coalesce_client = CoalesceCatalog(config, channel)
                coalesce_client.coalesce_catalog_update()
            elif channel_name == "datadotworld":
                datadotworld_client = Datadotworld(config, channel)
                datadotworld_client.datadotworld_catalog_update()
    except Exception as e:
        # update request queue status
        update_queue_detail_status(config, ScheduleStatus.Failed.value, str(e))
    finally:
        update_queue_detail_status(config, ScheduleStatus.Completed.value)
        update_queue_status(config, ScheduleStatus.Completed.value, True)
