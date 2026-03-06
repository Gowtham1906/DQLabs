"""
    Migration Notes From V2 to V3:
    No Migration Changes
"""


from dqlabs.app_helper.crypto_helper import encrypt
import base64
from urllib.parse import urlencode
import json
import msal
import requests

import logging
error_logger = logging.getLogger('error_log')


class PowerBiConnector():
    def __init__(self, input_config: dict):
        self._input_config = input_config

    def create_connection(self):
        try:
            client_id = self._input_config.get('client_id')
            authority_url = 'https://login.microsoftonline.com/organizations'
            scope = ['https://analysis.windows.net/powerbi/api/.default']
            authenticate_mode = self._input_config.get('authentication_type')

            if authenticate_mode.lower() == 'master user':
                # Create a public client to authorize the app with the AAD app
                power_bi_user = self._input_config.get('user')
                power_bi_password = self._input_config.get('password')
                clientapp = msal.PublicClientApplication(
                    client_id, authority=authority_url)
                token = clientapp.acquire_token_by_username_password(
                    power_bi_user, power_bi_password, scopes=scope)
            else:
                # Service Principal auth is recommended by Microsoft to achieve App Owns Data Power BI embedding
                client_secret = self._input_config.get('client_secret')
                tenant_id = self._input_config.get('tenant_id')
                authority = authority_url.replace(
                    'organizations', tenant_id)
                clientapp = msal.ConfidentialClientApplication(
                    client_id, client_secret, authority=authority)

                # Make a client call if Access token is not available in cache
                token = clientapp.acquire_token_for_client(scopes=scope)

            if (token and 'access_token' in token):
                return token['access_token']
            else:
                raise ValueError('Invalid credentials')
        except Exception as e:
            raise e

    def validate_connection(self) ->tuple:
        try:
            is_valid = False
            is_valid = self.create_connection()
            return (bool(is_valid), "")
        except Exception as e:
            return (is_valid, str(e))

    def get_database_schemas(self,  dbname: str) -> list:
        workspaces = []
        try:
            access_token = self.create_connection()
            if bool(access_token):
                url_groups = f"https://api.powerbi.com/v1.0/myorg/admin/groups?%24top=1000&%24filter=state%20eq%20('Active')"
                header = {'Content-Type': 'application/json',
                          'Authorization': f'Bearer {access_token}'}
                response = requests.get(url=url_groups, headers=header)
                if response and response.status_code == 200:
                    response = response.json()
                    workspaces = response.get('value', [])
        except Exception as e:
            error_logger.error(
                f"Power BI Connector - Get Workspaces - {str(e)}", exc_info=True)
        finally:
            return workspaces

    def get_assets(self):
        workspace = self._input_config.get('workspace', [])
        assets = []
        try:
            access_token = self.create_connection()
            if bool(access_token):
                reports = self.get_reports_list(workspace, access_token)
                dashboards = self.get_dashboard_list(
                    workspace, access_token)
                report_dash = reports+dashboards
                allWorkspaces = self.get_database_schemas('')

                for data in report_dash:
                    work_space_name = self.get_workspace_name(
                        allWorkspaces, data)
                    asset = {
                        "name": data.get("name") if data.get("type") == 'Reports' else data.get("displayName"),
                        "report_id": data.get("id"),
                        "workspace_id": data.get("workspaceId")
                    }
                    asset.update({
                        'description': self.__prepare_description(asset)
                    })
                    assets.append(asset)
        except Exception as e:
            error_logger.error(
                f"Power BI Connector - Get Assests - {str(e)}", exc_info=True)
        finally:
            return assets

    def get_reports_list(self, workspace, access_token):
        url_groups = f'https://api.powerbi.com/v1.0/myorg/admin/reports'
        header = {'Content-Type': 'application/json',
                  'Authorization': f'Bearer {access_token}'}
        reports = []
        response = requests.get(url=url_groups, headers=header)
        if response and response.status_code == 200:
            reports = response.json()
            reports = reports.get('value', [])
            if reports and workspace:
                reports = [
                    {**x} for x in reports for y in workspace if y['id'] == x['workspaceId']]
            for r in reports:
                r["type"] = "Reports"
        return reports

    def get_dashboard_list(self, workspace, access_token):
        url_groups = f'https://api.powerbi.com/v1.0/myorg/admin/dashboards'
        header = {'Content-Type': 'application/json',
                  'Authorization': f'Bearer {access_token}'}
        dashboard = []
        response = requests.get(url=url_groups, headers=header)
        if response and response.status_code == 200:
            dashboard = response.json()
            dashboard = dashboard.get('value', [])
            if dashboard and workspace:
                dashboard = [
                    {**x} for x in dashboard for y in workspace if y['id'] == x['workspaceId']]

            for d in dashboard:
                d["type"] = "Dashboard"
        return dashboard

    def get_workspace_name(self, workspace, data):
        workspace = [x for x in workspace if x.get(
            'id') == data.get('workspaceId')]
        return workspace[0].get('name')

    def __prepare_description(self, properties):
        view_name = properties.get('name', '')
        view_type = properties.get('type', '')
        workbook_name = properties.get('workbook', '')

        return f"""This {view_type} {view_name} is part of Workspace {workbook_name}. """


error_logger = logging.getLogger('error_log')


class TableauConnector:
    def __init__(self, input_config: dict):
        self.type = 'tableau'
        self._input_config = input_config

    def create_connection(self):
        decode_token = {}
        try:
            # create connection
            site = ''
            username = self._input_config.get("user", '')
            password = self._input_config.get('password', '')
            personal_access_token_name = self._input_config.get(
                'personal_access_token_name', '')
            personal_access_token_key = self._input_config.get(
                'personal_access_token_key', '')
            sitename = self._input_config.get('site', '')
            authentication_type = self._input_config.get(
                'authentication_type', '')
            if sitename.lower() != 'default' or '':
                site = sitename

            if authentication_type == 'Personal Access Token':
                api_params = {
                    'credentials': {
                        'personalAccessTokenName': personal_access_token_name,
                        'personalAccessTokenSecret': personal_access_token_key,
                        'site': {
                            'contentUrl': site
                        }
                    }
                }

            else:
                api_params = {
                    'credentials': {
                        'name': username,
                        'password': password,
                        'site': {
                            'contentUrl': site
                        }
                    }
                }

            # Tableau Api Request
            url = f"auth/signin"
            response = self.__getResponse(url, 'post', api_params)

            if (response and response.status_code == 200):
                json_response = response.json()
                response_credentials = json_response.get('credentials', {})
                user_object = response_credentials.get('user', {})
                site_object = response_credentials.get('site', {})

                decode_token = {
                    "token_id": response_credentials.get('token', None),
                    "user_id": user_object.get('id', ''),
                    "site_id": site_object.get('id', '')
                }
                return decode_token
            else:
                raise ValueError('Invalid credentials')
        except Exception as e:
            raise e

    def __getResponse(self, url, method_type, params='', auth_token=''):
        response = None
        try:
            ssl = self._input_config.get('ssl', '')
            connection_secure = 'http://'
            if ssl:
                connection_secure = 'https://'
            host = self._input_config.get('host', '')
            port = self._input_config.get('port', '')

            if port:
                endpoint = f"{connection_secure}{host}:{port}/api/3.18/{url}"
            else:
                endpoint = f"{connection_secure}{host}/api/3.18/{url}"

            # Prepare Headers and Params
            api_headers = {'Content-Type': 'application/json',
                           'Accept': 'application/json'}
            if auth_token:
                api_headers['X-Tableau-Auth'] = auth_token

            if method_type == 'post':
                data = json.dumps(params)
                response = requests.post(
                    url=endpoint, headers=api_headers, data=data)
            else:
                response = requests.get(url=endpoint, headers=api_headers)

        except Exception as e:
            error_logger.error(
                f"TableauConnector - Get Response - {str(e)}", exc_info=True)
        finally:
            return response

    def validate_connection(self) ->tuple:
        try:
            is_valid = False
            is_valid = self.create_connection()
            return (bool(is_valid), "")
        except Exception as e:
            return (is_valid, str(e))

    def get_assets(self):
        assets = []
        try:
            # create connection
            tableau_tokens = self.create_connection()
            if tableau_tokens:
                auth_token = tableau_tokens.get('token_id', None)
                site_id = tableau_tokens.get('site_id', None)

                if auth_token and site_id:

                    ssl = self._input_config.get('ssl', '')
                    connection_secure = 'http://'
                    if ssl:
                        connection_secure = 'https://'
                    host = self._input_config.get('host', '')
                    port = self._input_config.get('port', '')

                    if port:
                        endpoint = f"{connection_secure}{host}:{port}/api/metadata/graphql"
                    else:
                        endpoint = f"{connection_secure}{host}/api/metadata/graphql"

                    asset_list = requests.post(
                        url=endpoint,
                        json={"query": """ 
                                {
                                    views{
                                        name
                                        luid
                                        __typename
                                        createdAt
                                        updatedAt
                                        tags {
                                            id,
                                            name
                                        }
                                        workbook {
                                            luid
                                            name
                                            projectName
                                            projectLuid
                                            site {
                                                luid
                                                name
                                            }
                                            owner {
                                                luid
                                                name
                                            }
                                        }
                                    }
                                }"""
                              },
                        headers={
                            "X-Tableau-Auth": auth_token
                        })

                    if (asset_list and asset_list.status_code == 200):
                        asset_list = asset_list.json()['data']['views']
                        for row in asset_list:
                            view_luid = row.get('luid', None)
                            view_luid = view_luid if view_luid else None
                            if view_luid:
                                workbook = row.get('workbook', {})
                                workbook = workbook if workbook else {}
                                asset = {
                                    'name': row.get('name', '').lower(),
                                    'view_id': row.get('luid', '').lower(),
                                    'workbook_id': workbook.get('luid', '').lower(),
                                }
                                assets.append(asset)
        except Exception as e:
            error_logger.error(
                f"TableauConnector - Get Datasets / Workbooks - {str(e)}", exc_info=True)
        finally:
            return assets


error_logger = logging.getLogger('error_log')


class TalendConnector():
    def __init__(self, input_config: dict):
        self.type = input_config.get("connection_type")
        self._input_config = input_config
        self.connection = None

    def __getResponse(self, url='', method_type='post', params=''):
        api_response = None
        try:
            api_key = self._input_config.get("api_key", '')
            endpoint = self._input_config.get("endpoint", '')

            # Prepare Headers and Params
            api_headers = {'Content-Type': 'application/json',
                           'Accept': 'application/json'
                           }
            if not api_key:
                raise Exception("Missing Api Key")

            if url:
                endpoint = f"{endpoint}/{url}"

            api_headers['Authorization'] = f"Bearer {api_key}"

            if method_type == 'post':
                data = {}
                if params:
                    data = json.dumps(params)
                response = requests.post(
                    url=endpoint, headers=api_headers, data=data)
            elif method_type == 'delete':
                response = requests.delete(url=endpoint, headers=api_headers)
            else:
                response = requests.get(url=endpoint, headers=api_headers)

            if (response and response.status_code in [200, 201]):
                api_response = response.json()
        except Exception as e:
            error_logger.error(
                f"Talend Connector - Get Response Failed - {str(e)}", exc_info=True)
        finally:
            return api_response

    def validate_connection(self) ->tuple:
        try:
            is_valid = False
            is_valid = self.__getResponse('subscription', 'get')
            return (bool(is_valid), "")
        except Exception as e:
            error_logger.error(
                f"Talend Connector - Validate Connection Failed - {str(e)}", exc_info=True)
            return (is_valid, 'Validation failed. Please check given credentials is valid')

    def get_assets(self):
        assets = []
        try:
            projects = self._input_config.get("project")
            environment = self._input_config.get("environment")
            workspace = self._input_config.get("workspace")

            query_params = {}
            if workspace:
                query_params.update({
                    "workspaceId": workspace
                })
            if environment:
                query_params.update({
                    "environmentId": environment
                })

            request_url = "artifacts"
            if query_params:
                query_params = urlencode(query_params)
                request_url = f"{request_url}?{query_params}"

            response = self.__getResponse(request_url, 'get')
            response = response.get("items", []) if response else None
            for data in response:
                workspace = data.get("workspace", {})
                environment = workspace.get("environment")
                assets.append({
                    "name": data.get("name"),
                    "type": "job" if data.get("type") == "standard" else data.get("type"),
                    "workspace_name": workspace.get("name"),
                    "workspace_owner": workspace.get("owner"),
                    "workspace_type": workspace.get("type"),
                    "environment_name": environment.get("name"),
                    "pipeline_id": data.get("id"),
                    "workspace_id": workspace.get("id"),
                    "environment_id": environment.get("id"),
                    "asset_type": "pipeline"
                })
        except Exception as e:
            error_logger.error(
                f"Talend Connector - Get Pipeline - {str(e)}", exc_info=True)
        finally:
            return assets

    def get_projects_env(self):
        data = {
            "projects": [],
            "environments": [],
            "workspaces": []
        }
        try:
            projects = []
            environments = []
            workspaces = []

            # Get Project Details List
            request_url = "projects"
            response = self.__getResponse(request_url, 'get')
            response = response.get('items', None) if response else None

            if response:
                for project in response:
                    projects.append({
                        "name": project.get('name', ''),
                        "id": project.get('id', '')
                    })

            # Get Env Details List
            request_url = "environments"
            response = self.__getResponse(request_url, 'get')
            response = response if response else None

            if response:
                for env in response:
                    environments.append({
                        "name": env.get('name', ''),
                        "id": env.get('id', '')
                    })

            request_url = "workspaces"
            response = self.__getResponse(request_url, 'get')
            response = response if response else None

            if response:
                for workspace in response:
                    workspaces.append({
                        "name": workspace.get('name', ''),
                        "id": workspace.get('id', ''),
                        "environment_name": workspace.get('environment').get('name'),
                        "environment_id": workspace.get('environment').get('id')
                    })

            data.update({
                "projects": projects,
                "environments": environments,
                "workspaces": workspaces
            })

        except Exception as e:
            error_logger.error(
                f"Talend Connector - Get Project By Id - {str(e)}", exc_info=True)
        finally:
            return data


error_logger = logging.getLogger('error_log')


def get_server_hosting_url():
    return os.environ.get("SERVER_ENDPOINT")


def get_client_site_domain(request):
    domain_name = "localhost"
    client_host_origin = request.headers.get("Origin", "")
    client_host_origin = client_host_origin.replace("https://", "")
    client_host_origin = client_host_origin.replace("http://", "")
    client_host_origin = client_host_origin.replace("www.", "")
    domain = client_host_origin.split(".")
    if len(domain) > 0:
        domain = domain[0].split(':')
        if len(domain) > 0:
            domain_name = domain[0]

    return domain_name


class DbtConnector():
    def __init__(self, input_config: dict):
        self.type = input_config.get("connection_type")
        self._input_config = input_config
        self.connection = None

    def __getResponse(self, url='', method_type='post', is_meta=False, params=''):
        api_response = None
        try:
            api_key = self._input_config.get("api_key", '')
            endpoint = 'https://cloud.getdbt.com/api'
            meta_endpoint = 'https://metadata.cloud.getdbt.com/graphql'

            if is_meta:
                endpoint = meta_endpoint

            # Prepare Headers and Params
            api_headers = {'Content-Type': 'application/json',
                           'Accept': 'application/json',
                           'X-dbt-partner-source': 'DQLabs'
                           }
            if not api_key:
                raise Exception("Missing Api Key")

            if url:
                endpoint = f"{endpoint}/{url}"

            api_headers['Authorization'] = f"Bearer {api_key}"

            if method_type == 'post':
                data = {}
                if params:
                    data = json.dumps(params)
                response = requests.post(
                    url=endpoint, headers=api_headers, data=data)
            elif method_type == 'delete':
                response = requests.delete(url=endpoint, headers=api_headers)
            else:
                response = requests.get(url=endpoint, headers=api_headers)

            if (response and response.status_code in [200, 201]):
                api_response = response.json()
        except Exception as e:
            error_logger.error(
                f"DBT Connector - Get Response Failed - {str(e)}", exc_info=True)
        finally:
            return api_response

    def validate_connection(self) ->tuple:
        try:
            is_valid = False
            is_valid = self.__getResponse('v2/accounts', 'get')
            return (bool(is_valid), "")
        except Exception as e:
            error_logger.error(
                f"DBT Connector - Validate Connection Failed - {str(e)}", exc_info=True)
            return (is_valid, 'Validation failed. Please check given credentials is valid')

    def get_assets(self):
        assets = []
        try:
            account_id = self._input_config.get("account_id", '')
            projects = self._input_config.get("projects", [])
            environments = self._input_config.get("environments", [])

            project_ids = [x.get('id') for x in projects]
            environments_ids = [x.get('id') for x in environments]

            if not account_id:
                raise Exception("Missing Account ID")

            request_url = f"v2/accounts/{account_id}/jobs"
            response = self.__getResponse(request_url, 'get')
            response = response.get('data', []) if response else []

            if project_ids and len(project_ids) > 0:
                response = list(filter(lambda x: x.get(
                    "project_id") in project_ids, response))

            if environments_ids and len(environments_ids) > 0:
                response = list(filter(lambda x: x.get(
                    "environment_id") in environments_ids, response))

            for job in response:
                job = self.get_project_by_id(job)
                models = self.get_mobels_by_job_id(job)
                assets = assets + models
        except Exception as e:
            error_logger.error(
                f"DBT Connector - Get Jobs - {str(e)}", exc_info=True)
        finally:
            return assets

    def get_project_by_id(self, job):
        try:
            account_id = self._input_config.get("account_id", '')
            project_id = job.get("project_id", '')

            request_url = f"v2/accounts/{account_id}/projects/{project_id}"
            response = self.__getResponse(request_url, 'get')
            response = response.get('data', None)

            if response:
                job.update({
                    "project_name": response.get('name', ''),
                    "project_subdirectory": response.get('dbt_project_subdirectory', '')
                })

        except Exception as e:
            error_logger.error(
                f"DBT Connector - Get Project By Id - {str(e)}", exc_info=True)
        finally:
            return job

    def get_mobels_by_job_id(self, job):
        models = []
        try:
            job_id = job.get("id", '')
            meta_query_params = {"query": """
                                    {
                                        models(jobId: """+str(job_id)+""") 
                                        {
                                            uniqueId
                                            name
                                            database
                                            schema
                                            runGeneratedAt
                                            description
                                            owner
                                            type
                                            language
                                        }
                                    }"""
                                 }
            response = self.__getResponse('', 'post', True, meta_query_params)
            if response:
                response = response.get('data', {})
                response = response.get('models', [])
                for model in response:
                    asset = {
                        'name': model.get('name', ''),
                        'description': model.get('description', ''),
                        'asset_type': 'Pipeline',
                        'model_id': model.get('uniqueId', ''),
                        'job_id': job.get('id', ''),
                        'job_name': job.get('name', ''),
                        'project_id': job.get('project_id', ''),
                        'project_name': job.get('project_name', ''),
                        'account_id': job.get('account_id', ''),
                        'environment_id': job.get('environment_id', ''),
                        'runGeneratedAt': model.get('runGeneratedAt', ''),
                        'database': model.get('database', ''),
                        'schema': model.get('schema', ''),
                        'owner': model.get('owner', ''),
                        'table_type': model.get('type', ''),
                        'language': model.get('language', '')
                    }

                    asset.update({
                        "description": f"{asset.get('description')}  {self.__prepare_description(asset)}".strip()
                    })
                    models.append(asset)
        except Exception as e:
            error_logger.error(
                f"DBT Connector - Get Models By Job - {str(e)}", exc_info=True)
        finally:
            return models

    def __prepare_description(self, properties):
        view_name = properties.get('name', '')
        project_name = properties.get('project_name', '')
        database = properties.get('database', '')
        schema = properties.get('schema', '')
        owner_name = properties.get('owner', '')
        runGeneratedAt = properties.get('runGeneratedAt', '')

        description = f"""This dbt Model {view_name} is part of Project {project_name} and related to Database {database} and Schema {schema}. 
Owned by {owner_name} and runs generated at {runGeneratedAt}"""

        return f"{properties.get('description')}  {description}".strip()

    def get_info_from_connection(self, request: dict = {}, params: dict = {}):
        web_hook = None
        try:
            account_id = self._input_config.get("account_id", '')
            web_hook_enabled = self._input_config.get(
                "web_hook_enabled", False)
            web_hook_id = params.get("web_hook_id", '')
            web_hook_name = self._input_config.get("web_hook_name", '')
            web_hook_name = f"""DQLabs_{web_hook_name.replace(" ", "_")}"""
            client_domain = get_client_site_domain(request)
            web_hook_callback_url = f"{get_server_hosting_url()}api/channel_action/dbt_hook/?domain={client_domain}"

            if not account_id:
                raise Exception("Missing Account ID")

            params = {
                "event_types": [
                    "job.run.completed",
                    "job.run.errored"
                ],
                "name": web_hook_name,
                "client_url": web_hook_callback_url,
                "active": True,
                "description": "A webhook created by DQLabs for when jobs are completed or errored",
                "job_ids": []
            }

            response = None
            if web_hook_enabled and not web_hook_id:
                request_url = f"v3/accounts/{account_id}/webhooks/subscriptions"
                response = self.__getResponse(
                    request_url, 'post', False, params)
                if response:
                    response = response.get('data', [])
                    web_hook = {
                        "web_hook_id": response.get('id'),
                        "hmac_secret": encrypt(response.get('hmac_secret'))
                    }
            if not web_hook_enabled and web_hook_id:
                request_url = f"v3/accounts/{account_id}/webhooks/subscription/{web_hook_id}"
                response = self.__getResponse(request_url, 'delete')
                if response:
                    web_hook = {
                        "web_hook_id": '',
                        "hmac_secret": ''
                    }
        except Exception as e:
            error_logger.error(
                f"DBT Connector - Web Hook Validation - {str(e)}", exc_info=True)
        finally:
            return web_hook

    def get_projects_env(self):
        data = {
            "projects": [],
            "environments": []
        }
        try:
            projects = []
            environments = []
            account_id = self._input_config.get("account_id", '')

            # Get DBT Project Details List
            request_url = f"v2/accounts/{account_id}/projects/"
            response = self.__getResponse(request_url, 'get')
            response = response.get('data', None) if response else None

            if response:
                for project in response:
                    projects.append({
                        "name": project.get('name', ''),
                        "id": project.get('id', '')
                    })

            # Get DBT Env Details List
            request_url = f"v2/accounts/{account_id}/environments/"
            response = self.__getResponse(request_url, 'get')
            response = response.get('data', None) if response else None

            if response:
                for env in response:
                    environments.append({
                        "name": env.get('name', ''),
                        "id": env.get('id', ''),
                        "project_id": env.get('project_id', '')
                    })

            data.update({
                "projects": projects,
                "environments": environments
            })

        except Exception as e:
            error_logger.error(
                f"DBT Connector - Get Project By Id - {str(e)}", exc_info=True)
        finally:
            return data


error_logger = logging.getLogger('error_log')


class AirflowConnector():
    def __init__(self, input_config: dict):
        self.type = input_config.get("connection_type")
        self._input_config = input_config
        self.connection = None

    def create_connection(self):
        try:
            # Airflow Api Request
            url = "pools"
            response = self.__getResponse(url, 'get')

            if (response and response.status_code == 200):
                return True
            else:
                raise ValueError('Invalid credentials')
        except Exception as e:
            raise e

    def __getResponse(self, url, method_type, params=''):
        response = None
        try:
            server = self._input_config.get("server", '')
            username = self._input_config.get("username", '')
            password = self._input_config.get('password', '')
            base_auth_key = self._input_config.get('base_auth_key', '')
            authentication_type = self._input_config.get(
                'authentication_type', '')

            auth_key = f"Basic {base_auth_key})"
            if authentication_type == "Username and Password":
                data_string = username + ":" + password
                data_bytes = data_string.encode("utf-8")
                base64_encode_auth = base64.b64encode(
                    data_bytes).decode('utf8')
                auth_key = f"Basic {base64_encode_auth})"

            # Prepare Headers and Params
            endpoint = f"{server}/api/v1/{url}"
            api_headers = {'Content-Type': 'application/json',
                           'Accept': 'application/json', 'Authorization': auth_key}

            if method_type == 'post':
                data = json.dumps(params)
                response = requests.post(
                    url=endpoint, headers=api_headers, data=data)
            else:
                response = requests.get(url=endpoint, headers=api_headers)

        except Exception as e:
            error_logger.error(
                f"AirflowConnector - Get Response - {str(e)}", exc_info=True)
        finally:
            return response

    def validate_connection(self) ->tuple:
        try:
            is_valid = False
            is_valid = self.create_connection()
            return (bool(is_valid), "")
        except Exception as e:
            return (is_valid, str(e))

    def get_assets(self):
        assets = []
        try:
            url = f"dags"
            response = self.__getResponse(url, 'get')
            response = response.json() if response else []
            response = response.get('dags', []) if response else []
            for asset in response:
                owners = asset.get('owners', [])
                tags = asset.get('tags', [])
                schedule_interval = asset.get('schedule_interval', {})
                schedule_interval = schedule_interval if schedule_interval else {}

                asset = {
                    "dag_id": asset.get('dag_id'),
                    "name": asset.get('dag_id'),
                    "default_view": asset.get('default_view'),
                    "description": asset.get('description'),
                    "fileloc": asset.get('fileloc'),
                    "is_active": 'Yes' if asset.get('is_active', False) else 'No',
                    "is_subdag": 'Yes' if asset.get('is_subdag', False) else 'No',
                    "is_paused": 'Yes' if asset.get('is_paused', False) else 'No',
                    "next_dagrun": asset.get('next_dagrun'),
                    "owners": ','.join(owners),
                    "schedule_interval_type": schedule_interval.get('__type', ''),
                    "schedule_interval_value": schedule_interval.get('value', ''),
                    'asset_type': 'Pipeline',
                    'tags': ','.join(f"'{x.get('name')}'" for x in tags),
                    'type': 'dag'
                }
                assets.append(asset)
        except Exception as e:
            error_logger.error(
                f"Airflow Connector - Get Dags - {str(e)}", exc_info=True)
        finally:
            return assets
