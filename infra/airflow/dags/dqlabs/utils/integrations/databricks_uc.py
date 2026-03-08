import json
import pytz
import uuid
import requests
from typing import List, Dict, Any, Optional, Union
from urllib.parse import urljoin
import urllib.parse 
from dqlabs.app_helper import agent_helper

from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone
from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.app_helper.crypto_helper import decrypt
from dqlabs.app_helper.dq_helper import format_freshness
from dqlabs.enums.connection_types import ConnectionType
from dqlabs.app_helper.catalog_helper import (get_asset_metrics)


class DatabricksUC:    
    def __init__(self, config, channel):
        self.session = requests.Session()
        self.config = config
        log_info(('config for databricks_uc', self.config))
        self.credentials = channel
        log_info(('channel for datbricks_uc',self.credentials))
        self.client_id = decrypt(self.credentials.get('client_id'))
        self.client_secret = decrypt(self.credentials.get('client_secret'))
        self.token = decrypt(self.credentials.get('token'))
        self.tenant_id = decrypt(self.credentials.get('tenant_id'))
        self.connection = get_postgres_connection(self.config)
        self.organization_id = config.get('organization_id')
        if not self.organization_id:
            self.organization_id = config.get('dag_info', {}).get('organization_id')
        
        self.tag_propagation = (
            config.get('dag_info', {})
            .get('settings', {})
            .get('discover', {})
            .get('tag_propagation',{})
            .get('is_active', False)
        )
        log_info(("tag_propagation",self.tag_propagation))
        
    def _get_access_token(self) -> str:
        """Get access token based on authentication type"""

        auth_type = self.credentials.get('authentication_type')
        if not auth_type:
            return "Error: Authentication type not specified"
        
        if auth_type == 'Token':
            if 'token' not in self.credentials:
                return "Error: Token is required for PAT authentication"
            return self.token
            
        elif auth_type == 'OAuth(m2m)':
            return self._get_oauth_m2m_token()
            
        elif auth_type == 'OAuth(Microsoft Entra ID)':
            return self._get_entra_id_token()
            
        else:
            return f"Error: Unsupported authentication type: {auth_type}"

    def _get_oauth_m2m_token(self) -> str:
        """Get OAuth M2M token"""
        required = ['client_id', 'client_secret', 'server']
        if not all(param in self.credentials for param in required):
            return f"Error: Missing required parameters for OAuth M2M: {required}"
            
        try:
            token_url = f"https://{self.credentials['server']}/oidc/v1/token"
            log_info(("token_url in oauth m2m", token_url))
            payload = {
                'grant_type': 'client_credentials',
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'scope': 'all-apis'
            }
            log_info(("payload in oauth m2m", payload))
            response = requests.post(token_url, data=payload, timeout=10)
            
            if response.status_code != 200:
                return f"Error: OAuth M2M token request failed: {response.text}"
                
            access_token = response.json().get('access_token')
            return access_token if access_token else "Error: No access token in response"
            
        except Exception as e:
            return f"Error: {str(e)}"

    def _get_entra_id_token(self) -> str:
        """Get Microsoft Entra ID token"""
        required = ['tenant_id', 'client_id', 'client_secret']
        if not all(param in self.credentials for param in required):
            return f"Error: Missing required parameters: {required}"
            
        try:
            token_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
            log_info(("token_url in entra id", token_url))
            payload = {
                'grant_type': 'client_credentials',
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'scope': '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default'
            }
            log_info(("payload in entra id", payload))
            
            response = requests.post(
                token_url,
                data=payload,
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                timeout=10
            )
            
            if response.status_code != 200:
                error_msg = response.json().get('error_description', 'Unknown error')
                return f"Error: Token request failed: {error_msg}"
                
            access_token = response.json().get('access_token')
            return access_token if access_token else "Error: No access token in response"
            
        except Exception as e:
            return f"Error: {str(e)}"

    def __get_response(
        self, 
        endpoint: str = '/api/2.1/unity-catalog/catalogs',
        method: str = 'get',
        payload: Optional[dict] = None
    ) -> Union[str, dict]:
        """
        Make API call to Databricks UC
        
        Args:
            access_token: The authentication token
            method: HTTP method ('GET' or 'POST')
            endpoint: API endpoint path
            params: Query parameters for GET requests
            payload: JSON payload for POST requests
            
        Returns:
            Union[str, dict]: 
                - API response JSON if successful
                - error message if failed
        """
        access_token = self._get_access_token()
        workspace_url = self.credentials.get("server", "")
        if not workspace_url:
            return "Error: Workspace URL is not configured"

        try:
            api_url = f"https://{workspace_url}/{endpoint}"
            log_info(("api_url in get response", api_url))
            log_info(("access_token in get response", access_token))
            log_info(("payload in get response", payload))
            log_info(("method in get response", method))
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }

            if method == 'get':
                response = requests.get(
                    api_url,
                    headers=headers,
                )
            elif method == 'post':
                response = requests.post(
                    api_url,
                    headers=headers,
                    data=json.dumps(payload),
                )
            else:
                return f"Error: Method not supported: {method}"

            if response.status_code in (200, 201):
                try:
                    return response.json()
                except ValueError:
                    return response.text
            else:
                try:
                    error_msg = response.json()
                except ValueError:
                    error_msg = response.text
                return None
            
        except requests.Timeout:
            return "Error: Request timed out. Workspace is not responding"
        except requests.ConnectionError:
            return "Error: Connection error. Workspace is not responding"
        except Exception as e:
            return f"Error: {str(e)}"

    def __databricks_uc_channel_configuration(self) -> Dict[str, Any]:
            """
            Retrieves and returns the import and export configurations for Databricks Unity Catalog from `self.channel`.
            
            This function logs the channel configuration, extracts the "import" and "export" settings,
            and ensures the "export" configuration is properly parsed from JSON format.
            
            Returns:
                Tuple[Dict[str, Any], Dict[str, Any]]: The import configuration (dict) and export configuration (dict).
            """
            log_info(("databricks_uc_configuration",self.credentials))
            import_config = self.credentials.get("import", {})
            export_config = json.loads(self.credentials.get("export"))
            return import_config, export_config

    def __asset_information(self) -> Dict[str, Any]:
        """
        Retrieves and returns the asset information from the channel configuration.
        
        This function extracts the "asset" information from `self.channel` and returns it as a dictionary.
        
        Returns:
            Dict[str, Any]: The asset information (dict).
        """


        # Fetch DQ Asset Details
        asset = self.config.get("asset", {})
        asset_info = []
        if asset.get("properties"):
            # Fetch DQ Connection details
            connection = self.config.get("connection", {})
            connection_type = connection.get("type", "")
            asset_properties = asset.get("properties", {})
            database_name = self.config.get("database_name")
            asset_type = 'view' if asset.get("type").lower() == 'view' else 'table'
            asset_data = {
                    "asset_id": self.config.get("asset_id"),
                    "asset_name": asset.get("name"),
                    "asset_schema": asset_properties.get("schema"),
                    "asset_database": database_name,
                    "connection_type": connection_type,
                    "asset_properties": asset_properties,
                    "asset_type": asset_type
                    }
            asset_info.append(asset_data)
        else:
            asset_list = self.__get_assets()
            for asset in asset_list:
                if asset:
                    asset_properties = asset.get("properties", {})
                    asset_type = 'view' if asset.get("type").lower() == 'view' else 'table'
                    asset_data = {
                        "asset_id": asset.get("id"),
                        "asset_name": asset.get("name"),
                        "asset_schema": asset_properties.get("schema"),
                        "asset_database": asset_properties.get("database"),
                        "connection_type": asset.get("connection_type"),
                        "asset_properties": asset_properties,
                        "asset_type": asset_type
                    }   
                    asset_info.append(asset_data)
        return asset_info
    
    def __get_assets(self) -> List[Dict[str, Any]]:
        """
        Fetch all assets from the PostgreSQL database.

        This method queries the PostgreSQL database to retrieve all assets and their associated properties.
        It returns a list of dictionaries, each representing an asset with its details.

        Returns
        -------
        List[Dict[str, Any]]
            A list of dictionaries containing asset details fetched from the database.

        Examples
        --------
        >>> obj = YourClass()
        >>> assets = obj.__get_assets()
        >>> print(assets)
        """
        assets = []
        try:
            query_string = """
                SELECT 
                    asset.*, 
                    connection.type AS connection_type 
                FROM 
                    core.asset
                JOIN 
                    core.connection 
                ON 
                    asset.connection_id = connection.id
                WHERE 
                    LOWER(asset.type) IN ('table', 'view', 'base table', 'external table') 
                    AND asset.is_active IS TRUE 
                    AND connection.is_active IS TRUE
                    AND asset.is_delete IS FALSE 
                    AND connection.is_delete IS FALSE
            """
            assets = self.__run_postgres_query(query_string,
                                                query_type='fetchall'
                                                )
        except Exception as e:
            log_error(
                f"Atlan Connector - Get Assets Error", e)
        return assets
    
    def __run_postgres_query(self, query_string: str, query_type: str, output_statement: str = ''):
        """
        Executes a PostgreSQL query and returns the result based on the query type.
        
        This function connects to the PostgreSQL database, executes the query, and fetches the result
        if applicable. It supports various query types, including fetchone, fetchall, insert, update,
        and delete operations.
        
        Args:
            query_string (str): The SQL query to execute.
            query_type (str): The type of query ('fetchone', 'fetchall', 'insert', 'update', 'delete').
            output_statement (str, optional): A log message for tracking query execution.
        
        Returns:
            Any: The result of the query execution (for fetch operations) or None for update/insert/delete.
        
        Raises:
            Exception: If query execution fails.
        """
        connection = get_postgres_connection(self.config)
        records = ''
        with connection.cursor() as cursor:
            try:
                cursor = execute_query(
                        connection, cursor, query_string)
            except Exception as e:
                log_error("Databricks Connector - Query failed Error", e)
            
            if query_type not in ['insert','update','delete']:
                if query_type == 'fetchone':
                    records = fetchone(cursor)
                elif query_type == 'fetchall':
                    records = fetchall(cursor)
                
                return records
            
            log_info(("Query run:", query_string))
            log_info(('Query Executed',output_statement))
    
    def __get_warehouse_id(self, credentials):
        """
        Retrieves the ID of a running Databricks SQL warehouse that matches the specified server credentials.
        
        This method makes an API call to get all available warehouses, then searches for a warehouse that:
        1. Is in a RUNNING state
        2. Has a hostname in its ODBC parameters that matches the server specified in the credentials
        
        Args:
            config (dict): Configuration parameters needed for the API request
            credentials (dict): Authentication credentials containing at least a 'server' field
            
        Returns:
            str: The ID of the matching warehouse if found, otherwise an empty string
            
        Raises:
            Logs any exceptions that occur during the API request or processing
        """
        request_url = f"api/2.0/sql/warehouses"
        try:
            response = self.__get_response(request_url)
            log_info(("response in get warehouses", response))
            warehouse_id = ""
            if response:
                warehouses = response.get("warehouses", [])
                log_info(("warehouses", warehouses))
                for warehouse in warehouses:
                    log_info(("warehouse in if", warehouse))
                    log_info(("warehouse state", warehouse.get("state", "")))
                    log_info(("warehouse odbc_params", warehouse.get("odbc_params")))
                    log_info(("warehouse hostname", warehouse.get("odbc_params").get("hostname", "")))
                    log_info(("credentials server", credentials.get("server")))
                    if warehouse.get("state", "") in (
                        "RUNNING",
                    ) and warehouse.get("odbc_params").get(
                        "hostname", ""
                    ) == credentials.get(
                        "server"
                    ):
                        warehouse_id = warehouse.get("id", "")
                        return warehouse_id
        except Exception as e:
            log_error.error(f"Databricks get warehouses - {str(e)}", exc_info=True)
        finally:
            return warehouse_id
        
    def __check_asset_exists_in_databricks(self, config, asset_info):
        """
        Checks if a specified asset (table) exists in Databricks Unity Catalog.

        This method queries the Databricks Unity Catalog API to verify the existence of a table
        with the given database, schema, and table name from the asset_info.

        Args:
            config (dict): Configuration parameters needed for the API request
            asset_info (dict): Dictionary containing asset information with keys:
                - asset_database (str): The catalog/database name
                - asset_schema (str): The schema name
                - asset_name (str): The table name

        Returns:
            tuple: A tuple containing:
                - response (dict|None): The API response if the asset exists, None otherwise
                - bool: True if the asset exists (response is not None), False otherwise

        Raises:
            Logs any exceptions that occur during the API request
        """
        database_name = asset_info.get("asset_database")
        schema_name = asset_info.get("asset_schema")
        asset_name = asset_info.get("asset_name")
        asset_name = urllib.parse.quote(asset_name, safe='')
        response = None
        try:
            request_url = f"api/2.0/unity-catalog/tables/{database_name}.{schema_name}.{asset_name}"
            log_info(("request_url in asset exists", request_url))
            response = self.__get_response(request_url, "get")
            log_info(("response in check_asset_exists_in_databricks", response))
        except Exception as e:
            log_error.error(f"Databricks get relations - {str(e)}", exc_info=True)
        finally:
            
            return response, True if response else False
        
    def generate_tag_guid(self, hexadecimal:object) -> object:
        """
        Generates a UUID (Universally Unique Identifier) for a given hexadecimal string.
        
        Parameters:
        tag_name (object): The input tag_name string to generate a UUID.
        
        Returns:
        object: A UUID object generated from the input.
        """
        namespace = uuid.NAMESPACE_DNS
        generated_uuid = uuid.uuid5(namespace, hexadecimal)

        return generated_uuid
    
    def get_databricks_tags(self, config, warehouse_id: str, query: str, is_column_query= False) -> List[Dict[str, Any]]:
        """
        Retrieves and processes tags from Databricks for either tables or columns.

        Executes a SQL query against a Databricks warehouse to fetch tag information,
        then processes the results into a standardized format. Can handle both table-level
        and column-level tags based on the is_column_query parameter.

        Args:
            config (dict): Configuration parameters for the API request
            warehouse_id (str): ID of the Databricks SQL warehouse to execute the query against
            query (str): SQL query to execute for fetching tags
            is_column_query (bool, optional): Flag indicating whether the query is for column tags.
                                            Defaults to False (table tags).

        Returns:
            List[Dict[str, Any]]: List of processed tags in the format:
                [
                    {
                        "guid": str,  # Generated GUID for the tag
                        "name": str   # Name of the tag
                    },
                    ...
                ]

        Raises:
            Logs any exceptions that occur during the API request or processing
        """
        
        tags_list = []
        request_url = f"api/2.0/sql/statements/"
        if is_column_query:
            query = query.replace("table_tags", "column_tags")
        params = {
            "warehouse_id": warehouse_id,
            "catalog": "main",
            "schema": "dqlabs",
            "statement": query,
        }
        try:
            response = self.__get_response(request_url, "post", params)
            log_info(("response in get databricks tags", response))
            if response:
                column_relations = response.get("result", {}).get("data_array", [])
                for item in column_relations:
                    tags_list.append(
                        {
                            "catalog_name": item[0],
                            "schema_name": item[1],
                            "table_name": item[2],
                            "tag_name": item[4] if is_column_query else item[3],
                            "tag_value": item[5] if is_column_query else item[4],
                        }
                    )
        except Exception as e:
            log_error.error(f"Databricks get relations - {str(e)}", exc_info=True)
        finally:
            tags_list = [
                {
                    "guid": self.generate_tag_guid(tag.get("tag_name")),
                    "name": tag.get("tag_name"),
                }
                for tag in tags_list
            ]
            return tags_list
    
    def delete_tags_in_dqlabs(self, tags_info:List[Dict[str, Any]]) -> object:
        """
        Deletes tags in DQLabs that are no longer associated with a given asset in Databricks.

        Parameters:
        - asset_id (str): The unique identifier of the asset.
        - tags_info (List[Dict[str, Any]]): A list of dictionaries containing tag details from databricks, including tag GUIDs.

        Process:
        1. Extracts GUIDs of tags currently associated with the asset in Databricks.
        2. Fetches all Databricks tags stored in DQLabs from the `core.tags` table.
        3. Logs the tags information for debugging.
        4. Compares the Datbricks tag GUIDs with those in DQLabs:
            - If a tag exists in DQLabs but is not present in the Databricks tag list, it is considered obsolete.
            - Deletes the mapping of such obsolete tags from the `core.tags_mapping` table, ensuring they are no longer linked to the asset.
        5. Logs the removal of each unmapped tag for tracking purposes.

        The function ensures that only outdated tags are removed, maintaining data consistency between Databricks and DQLabs.

        Returns:
        - None (Logs messages for debugging and tracking deletion operations).
        """

        dqlabs_databricks_tags = []
        databricks_tags_guids = [tag.get("guid") for tag in tags_info] # fetch databricks tags guids
        # fetch all databricks glossary terms in dqlabs
        dqlabs_tags_query = f"""
                            select id from core.tags
                            where source = '{ConnectionType.Databricks.value}' 
                            """
        dqlabs_databricks_tags_response = self.__run_postgres_query(
                                                    query_string = dqlabs_tags_query,
                                                    query_type='fetchall'
                                                       )
        if dqlabs_databricks_tags_response:
            dqlabs_databricks_tags = [tag.get("id") for tag in dqlabs_databricks_tags_response]

        log_info(("tags_info",tags_info))
        if dqlabs_databricks_tags:
            # for tag_id in dqlabs_databricks_tags:
                # if tag_id not in databricks_tags_guids:
            tag_ids = "', '".join(dqlabs_databricks_tags)
            remove_tags_mapping_query = f"""
                                            delete from core.tags_mapping
                                            where tags_id in ('{tag_ids}')
                                        """
            # delete terms mapping for id
            self.__run_postgres_query(
                            query_string = remove_tags_mapping_query,
                            query_type = 'delete',
                            output_statement = f"{dqlabs_databricks_tags} unmapped from tags mapping")
            remove_tags_id = f"""
                                delete from core.tags
                                where id in ('{tag_ids}')
                            """
            # delete the term
            self.__run_postgres_query(
                            query_string = remove_tags_id,
                            query_type = 'delete',
                            output_statement = f"{tag_ids} deleted from tags") 

    
    def get_attribute_id_from_name(self, asset_id: str, attribute_name: str) -> object:

        """
        Fetch the attribute ID corresponding to a given asset ID and attribute name.

        This function performs a SQL query to fetch the attribute ID for a specific attribute name
        associated with a given asset ID from the PostgreSQL database.

        It logs the asset ID and attribute name for debugging purposes and returns the attribute ID
        if found. If an error occurs during the process, it logs the error message.

        Parameters
        ----------
        asset_id : str
            The ID of the asset to which the attribute is associated.
        attribute_name : str
            The name of the attribute for which the ID is being fetched.

        Returns
        -------
        object
            The attribute ID if found, otherwise an empty string.

        Examples
        --------
        >>> obj = YourClass()
        >>> obj.get_attribute_id_from_name("450f606a-484d-4659-bfe5-aff69ff71189", "AF_NAME")
        '1234abcd'  # Example output of attribute ID fetched from the database.

        Notes
        -----
        - The function assumes that the `asset_id` and `attribute_name` exist in the database.
        - The SQL query is case-insensitive, as both `asset_id` and `attribute_name` are compared in lowercase.
        - If no matching attribute is found, the function returns an empty string.
        - Any errors encountered during the query execution are logged for debugging.

        """
        
        """ Get the attribute id from attribute name"""
        attribute_id = ''
        try:
            query_string = f"""
                    SELECT attr.id as attribute_id 
                    from core.attribute attr
                    where lower(attr.name) = lower('{attribute_name}')
                    and asset_id = '{asset_id}'
                """
            attributes = self.__run_postgres_query(query_string,
                                                query_type='fetchone'
                                                )
            attribute_id = attributes.get("attribute_id")
            return attribute_id
        except Exception as e:
            log_error(
                f"databricks Connector - Get Response Error", e)

    def __get_attribute_tags_of_asset(self, config, asset_info, warehouse_id: str):
        """
        Fetches attribute tags for a given asset in Databricks Unity Catalog.

        This function constructs a SQL query to retrieve attribute tags associated with a specific
        asset in the Databricks Unity Catalog. It uses the provided warehouse ID to execute the query
        and returns a list of attribute tags.

        Parameters:
        - config (dict): Configuration settings for the connection.
        - warehouse_id (str): The ID of the warehouse to use for the query.

        Returns:
        - List[Dict[str, Any]]: A list of dictionaries containing attribute tag information.
        """

        catalog_name = asset_info.get("asset_database")
        schema_name = asset_info.get("asset_schema")
        table_name = asset_info.get("asset_name")
        request_url = f"api/2.0/sql/statements/"
        params = {
            "warehouse_id": warehouse_id,
            "catalog": "main",
            "schema": "dqlabs",
            "statement": f"""SELECT * FROM system.information_schema.column_tags where catalog_name = '{catalog_name}' and schema_name = '{schema_name}' and table_name ='{table_name}'""",
        }
        tags_list = []
        try:
            response = self.__get_response(request_url, "post", params)
            log_info(("response in get attribute tags", response))
            if response:
                column_relations = response.get("result", {}).get("data_array", [])
                for item in column_relations:
                    tags_list.append(
                        {
                            "catalog_name": item[0],
                            "schema_name": item[1],
                            "table_name": item[2],
                            "column_name": item[3],
                            "tag_name": item[4],
                            "tag_value": item[5],
                        }
                    )
        except Exception as e:
            log_error.error(f"Databricks get relations - {str(e)}", exc_info=True)
        finally:
            tags_list = [
                {
                    "guid": self.generate_tag_guid(tag.get("tag_name")),
                    "name": tag.get("tag_name"),
                    "column_name": tag.get("column_name"),
                }
                for tag in tags_list
            ]
            return tags_list
        
    def __prepare_attribute_tags_list(self, active_attributes, total_asset_attribute_tags: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Prepares a list of attribute tags from the total asset attribute tags.

        This function processes the total asset attribute tags and extracts relevant information
        to create a list of dictionaries containing attribute tag details.

        Parameters:
        - total_asset_attribute_tags (List[Dict[str, Any]]): A list of dictionaries containing
          total asset attribute tags.

        Returns:
        - List[Dict[str, Any]]: A list of dictionaries containing prepared attribute tag information.
        """

        attribute_specific_tags = {}
        for attribute in active_attributes:
            tags_list = []
            for tag in total_asset_attribute_tags:
                if attribute.get("name") == tag.get("column_name"):
                    tags_list.append(tag)
            attribute_specific_tags.update(
                {attribute.get("name"): tags_list}
            )
        return attribute_specific_tags

    def insert_databricks_tags_to_dqlabs(self, tags_info:List[Dict[str, Any]]) -> object:
        """
        Inserts Databricks tags into the DQLabs system if they do not already exist.
        
        Parameters:
        tags_info (List[Dict[str, Any]]): A list of dictionaries containing tag information, 
                                        where each dictionary has keys 'guid' (tag ID) and 'name' (tag name).
        
        Returns:
        object: Logs the status of each insert operation.
        """

        log_info(("tags_info",tags_info))
        for tag in tags_info:
            tag_id = tag.get("guid")
            log_info(("tag_id",tag_id))
            tag_name = tag.get("name")
            #check if tag id in tags table 
            tag_check_query =f"""
                                select exists (
                                select 1
                                from core."tags"
                                where id = '{tag_id}'
                                and source = '{ConnectionType.Databricks.value}'
                                and is_active = true and is_delete = false
                                );
                            """
            tag_check_flag = self.__run_postgres_query(tag_check_query,
                                                        query_type='fetchone'
                                                        )
            tag_check_flag = tag_check_flag.get("exists",False)
            log_info(("tag_check_flag",tag_check_flag))
            if not tag_check_flag:
                # insert databricks tags
                
                try:
                    with self.connection.cursor() as cursor:
                        # Prepare tags insert data
                        query_input = (
                            tag_id,
                            tag_name,
                            tag_name,
                            None,  # Description
                            "#21598a",  # Color
                            ConnectionType.Databricks.value,  # Database name
                            False,  # Native query run
                            json.dumps({"type": ConnectionType.Databricks.value}, default=str),  # Properties
                            1,  # Order
                            False,  # is_mask_data
                            True,  # is_active
                            False,  # is_delete
                            str(self.organization_id),  # Organization ID
                            ConnectionType.Databricks.value,  # Source
                        )

                        # Generate placeholders for the query using %s
                        input_literals = ", ".join(["%s"] * len(query_input))
                        query_param = cursor.mogrify(
                            f"({input_literals}, CURRENT_TIMESTAMP)", query_input
                        ).decode("utf-8")

                        # Insert tags query
                        insert_tags_query = f"""
                            INSERT INTO core.tags(
                                id, name, technical_name, description, color, db_name, native_query_run, properties,
                                "order", is_mask_data, is_active, is_delete, organization_id, source, created_date
                            )
                            VALUES {query_param}
                            RETURNING id;
                        """
                        log_info(("insert_tags_query", insert_tags_query))

                        # Run query
                        execute_query(
                            self.connection, cursor, insert_tags_query
                        )
                        log_info((f"Glossary Tag tag_id: {tag_id}, tag_name: {tag_name} Inserted into Dqlabs"))
                except Exception as e:
                    log_error(
                        f"Insert domain tag failed", e)

    def __create_metadata(self, asset_flags, asset_id):
        """
        Creates metadata string for a data asset by processing various quality metrics and URLs.

        This function:
        1. Gathers asset quality metrics from DQLabs
        2. Processes time data with proper timezone conversion (to IST)
        3. Maps metric values to their corresponding DQLabs metadata names
        4. Generates URLs for alerts, issues, and measures based on asset_id
        5. Formats all selected metadata into a comma-separated string

        Args:
            asset_flags (dict): Dictionary containing export options with keys:
                - options (dict): Flags indicating which metadata to include:
                    * summary (bool): Include quality metrics
                    * alerts (bool): Include alerts URL
                    * issues (bool): Include issues URL
                    * measures (bool): Include measures URL
            asset_id (str): The unique identifier of the asset used for URL generation

        Returns:
            str: Comma-separated string of metadata key-value pairs in format "'key' = value",
                or None if no metadata was generated

        Notes:
            - Time values are converted from UTC to IST (Asia/Kolkata) timezone
            - Freshness values are specially formatted using format_freshness()
            - URLs are constructed by joining with the DQLabs host URL from the channel config
        """
        
        asset_export_options = asset_flags.get("options", {})
        log_info(("asset_export_options", asset_export_options))
        
        metrics = get_asset_metrics(self.config, asset_id)
        log_info(("metrics 503", metrics))
        
        # Prepare time data
        ist_zone = pytz.timezone('Asia/Kolkata')
        last_updated_time = metrics.get("last_updated_date", "")
        if last_updated_time:
            if last_updated_time.tzinfo is None:
                last_updated_time = pytz.UTC.localize(last_updated_time)
            formatted_time = last_updated_time.astimezone(ist_zone).strftime('%d-%m-%Y %I:%M:%S %p')
        else:
            formatted_time = None

        # Mapping of metric keys to their DQLabs metadata names
        metric_mappings = {
            "score": "DQLabs.DQScore",
            "row_count": "DQLabs.TotalRows",
            "freshness": ("DQLabs.Freshness", lambda x: f"'{format_freshness(x)}'"),
            "active_measures": "DQLabs.ActiveRules",
            "observed_measures": "DQLabs.ObservedRules",
            "scored_measures": "DQLabs.ScoringRules",
            "duplicate_count": "DQLabs.DuplicateRows",
            "alerts": "DQLabs.Alerts",
            "issues": "DQLabs.Issues",
            "last_updated_date": ("DQLabs.LastUpdatedDate", lambda x: f"'{formatted_time}'")
        }

        data = []
        dqlabs_host = self.credentials.get("dq_url")
        
        # Process summary metrics
        if asset_export_options.get("summary"):
            for metric_key, metadata_name in metric_mappings.items():
                metric_value = metrics.get(metric_key)
                if metric_value is not None:
                    if isinstance(metadata_name, tuple):
                        metadata_key, formatter = metadata_name
                        data.append(f"'{metadata_key}' = {formatter(metric_value)}")
                    else:
                        data.append(f"'{metadata_name}' = {metric_value}")

        # Process URL options
        url_options = {
            "alerts": f"remediate/alerts?asset_id={asset_id}&a_type=data",
            "issues": f"remediate/issues?asset_id={asset_id}&a_type=data",
            "measures": f"observe/data/{asset_id}/measures#measures-table"
        }

        for option, path in url_options.items():
            if asset_export_options.get(option):
                url = urljoin(dqlabs_host, path)
                data.append(f"'DQLabs.{option.capitalize()}URL' = '{url}'")

        # Join all items
        if data:
            return ", ".join(data)
        return None

    def __push_asset_metadata(self, asset_info, asset_flags, warehouse_id: str):
        
        asset_id = asset_info.get("asset_id")
        catalog_name = asset_info.get("asset_database")
        schema_name = asset_info.get("asset_schema")
        table_name = asset_info.get("asset_name")
        asset_type = asset_info.get("asset_type")
        type_condition = "TABLE" if asset_type == "table" else "VIEW"
        request_url = f"api/2.0/sql/statements/"
        created_metadata = self.__create_metadata(asset_flags, asset_id)
        query = f""" ALTER {type_condition} `{catalog_name}`.`{schema_name}`.`{table_name}`
            SET TBLPROPERTIES ({created_metadata})"""
        log_info(("query in push asset metadata", query))
        params = {
            "warehouse_id": warehouse_id,
            "catalog": "main",
            "schema": "dqlabs",
            "statement": query,
        }
        log_info(("params in get databricks asset metadata", params))
        try:
            response = self.__get_response(request_url, "post", params)
            log_info(("response in get databricks asset metadata", response))
        except Exception as e:
            log_error.error(f"Databricks get relations - {str(e)}", exc_info=True)
        finally:
            return response
    
    def map_asset_metrics(self, asset_info: List[Dict[str, Any]], warehouse_id, map_tags:bool = False) -> object:
        """
        Maps tags from Microsoft databricks to a DQLabs asset by checking for existing tag mappings
        and inserting new ones if they do not already exist.

        Args:
            asset_info (List[Dict[str, Any]]): Information about the asset.
            databricks_asset_data (dict): Data from Microsoft databricks containing asset details.
            tags_info (List[Dict[str, Any]]): List of tag information.
            map_tags (bool, optional): Flag to determine whether to map tags. Defaults to False.

        Returns:
            object: Logs of the mapping process.
        """

        asset_id = asset_info.get("asset_id")
        catalog_name = asset_info.get("asset_database")
        schema_name = asset_info.get("asset_schema")
        table_name = asset_info.get("asset_name")
        query = f"""SELECT * FROM system.information_schema.table_tags where catalog_name = '{catalog_name}' and schema_name = '{schema_name}' and table_name ='{table_name}'"""
        log_info(("query in map_asset_metrics", query))
        if map_tags:
            # Update the tags for dqlabs asset
            asset_tags_list = self.get_databricks_tags(self.config, warehouse_id, query)
            log_info(("asset_tags_list", asset_tags_list))
            if asset_tags_list:
                for tag_mapped in asset_tags_list:
                    # asset_tag_id = self.generate_tag_guid(tag_mapped)
                    #check if term id in tags table 
                    attribute_tag_mapped_check_query =f"""
                                        select exists (
                                        select 1
                                        from core."tags_mapping"
                                        where tags_id = '{tag_mapped.get("guid")}'
                                        and asset_id = '{asset_id}'
                                        );
                                    """
                    attribute_tag_mapped = self.__run_postgres_query(attribute_tag_mapped_check_query,
                                                                query_type='fetchone'
                                                                )
                    attribute_tag_mapped = attribute_tag_mapped.get("exists",False)
                    log_info(("attribute_tag_mapped for debug",attribute_tag_mapped))
                    if not attribute_tag_mapped:
                        #  map databricks terms to attributes
                        try:
                            with self.connection.cursor() as cursor:
                                # Prepare attribute-tag mapping insert data
                                query_input = (
                                    str(uuid.uuid4()),  # Unique ID
                                    'asset',  # Level
                                    asset_id,  # Asset ID
                                    None,  # Attribute ID
                                    tag_mapped.get("guid"),  # Tag ID
                                )

                                # Generate placeholders for the query using %s
                                input_literals = ", ".join(["%s"] * len(query_input))
                                query_param = cursor.mogrify(
                                    f"({input_literals}, CURRENT_TIMESTAMP)", query_input
                                ).decode("utf-8")

                                # Insert attribute-tag mapping query
                                insert_asset_tag_query = f"""
                                    INSERT INTO core.tags_mapping(
                                        id, level, asset_id, attribute_id, tags_id, created_date
                                    )
                                    VALUES {query_param}
                                    RETURNING id;
                                """
                                log_info(("insert_asset_tag_query", insert_asset_tag_query))

                                # Run query
                                execute_query(
                                    self.connection, cursor, insert_asset_tag_query
                                )
                        except Exception as e:
                            log_error(
                                f"Insert Asset Tag Mapping failed", e)
            return asset_tags_list
                            
    def map_attribute_metrics(self, asset_info: List[Dict[str, Any]], warehouse_id, active_attributes, tags_info: List[Dict[str, Any]], map_domains:bool = False, map_tags:bool = False) -> object:
        """
        Maps terms and tags from Microsoft databricks to attributes in a DQLabs asset.

        Ensures correct terms and tags are linked to attributes and removes outdated mappings.

        Args:
            asset_info (List[Dict[str, Any]]): Information about the asset.
            databricks_asset_data (dict): Data from Microsoft databricks containing asset details.
            tags_info (List[Dict[str, Any]]): List of tag information.
            terms_info (List[Dict[str, Any]]): List of term information.
            map_domains (bool, optional): Flag to determine whether to map domains. Defaults to False.
            map_tags (bool, optional): Flag to determine whether to map tags. Defaults to False.

        Returns:
            object: Logs of the mapping process.
        """
        asset_id = asset_info.get("asset_id")
        # log_info(("term_info for debug",terms_info))
        log_info(("tags-info",tags_info))
        log_info(("active_attributes",active_attributes))

        total_asset_attribute_tags = self.__get_attribute_tags_of_asset(self.config, asset_info, warehouse_id)
        log_info(("total_asset_attribute_tags",total_asset_attribute_tags)) 
        attribute_specific_tags = self.__prepare_attribute_tags_list(active_attributes, total_asset_attribute_tags)

        log_info(("attribute_tags",attribute_specific_tags))
        
        """ Map terms and tags to attributes """
        for attribute in active_attributes:
            attribute_guid = attribute.get("guid")
            log_info(("attribute_guid",attribute_guid))
            attribute_name = attribute.get("name")
            log_info(("attribute_name",attribute_name))
            attribute_id = self.get_attribute_id_from_name(asset_id=asset_id,
                                                                        attribute_name=attribute_name)

            if map_tags:
                log_info(("map_tags for debug",map_tags))
                attribute_tags  = attribute_specific_tags.get(attribute_name) # databricks api way of displaying tags
                log_info(("attribute_tag_classifications for debug",attribute_tags))
                if attribute_tags:
                    for attribute_tag_classification in attribute_tags:
                        attribute_tag_id = attribute_tag_classification.get("guid")
                        #check if term id in tags table 
                        attribute_tag_mapped_check_query =f"""
                                            select exists (
                                            select 1
                                            from core."tags_mapping"
                                            where tags_id = '{attribute_tag_id}'
                                            and attribute_id = '{attribute_id}'
                                            );
                                        """
                        attribute_tag_mapped = self.__run_postgres_query(attribute_tag_mapped_check_query,
                                                                    query_type='fetchone'
                                                                    )
                        attribute_tag_mapped = attribute_tag_mapped.get("exists",False)
                        log_info(("attribute_tag_mapped_check_query",attribute_tag_mapped_check_query))
                        log_info(("attribute_tag_mapped for debug",attribute_tag_mapped))
                        if not attribute_tag_mapped:
                            #  map databricks terms to attributes
                            try:
                                with self.connection.cursor() as cursor:
                                    # Prepare attribute-tag mapping insert data
                                    query_input = (
                                        str(uuid.uuid4()),  # Unique ID
                                        'attribute',  # Level
                                        asset_id,  # Asset ID
                                        attribute_id,  # Attribute ID
                                        attribute_tag_id,  # Tag ID
                                    )

                                    # Generate placeholders for the query using %s
                                    input_literals = ", ".join(["%s"] * len(query_input))
                                    query_param = cursor.mogrify(
                                        f"({input_literals}, CURRENT_TIMESTAMP)", query_input
                                    ).decode("utf-8")

                                    # Insert attribute-tag mapping query
                                    insert_attribute_tag_query = f"""
                                        INSERT INTO core.tags_mapping(
                                            id, level, asset_id, attribute_id, tags_id, created_date
                                        )
                                        VALUES {query_param}
                                        RETURNING id;
                                    """
                                    log_info(("insert_attribute_tag_query", insert_attribute_tag_query))

                                    # Run query
                                    execute_query(
                                        self.connection, cursor, insert_attribute_tag_query
                                    )
                            except Exception as e:
                                log_error(
                                    f"Insert Attribute Tag Mapping failed", e)
    
    def map_asset_tags_to_attributes(self, asset_info, asset_tags_list: List[Dict[str, Any]], active_attributes: List[Dict[str, Any]], map_tags) -> object:
        if map_tags:
            asset_id = asset_info.get("asset_id")
            log_info(("asset_id", asset_id))
            log_info(("asset_tags_list", asset_tags_list))
            for tag in asset_tags_list:
                atlan_tag_qualified_name = tag.get("name")
                log_info(("atlan_tag_qualified_name",atlan_tag_qualified_name))

                # Fetch the tag_id from tags atlan api
                asset_tag_id = tag.get("guid")

                
                attribute_tag_mapped_check_query =f"""
                                    select id
                                    from core.tags_mapping
                                    where tags_id = '{asset_tag_id}'
                                    and asset_id = '{asset_id}'
                                    and level = 'attribute'
                                """
                log_info(("attribute_tag_mapped_check_query inn asset.",attribute_tag_mapped_check_query))
                attribute_tag_mapped = self.__run_postgres_query(attribute_tag_mapped_check_query,
                                                            query_type='fetchall'
                                                            )
                tags_to_remove = [tag_id.get("id") for tag_id in attribute_tag_mapped]
                if tags_to_remove:
                    ids_to_remove = "', '".join(tags_to_remove)
                    remove_tags_mapping_query = f"""
                                                    delete from core.tags_mapping
                                                    where id in ('{ids_to_remove}')
                                                """
                    log_info(("remove_tags_mapping_query",remove_tags_mapping_query))
                    self.__run_postgres_query(
                                    query_string = remove_tags_mapping_query,
                                    query_type = 'delete',
                                    output_statement = f"{tags_to_remove} unmapped from tags mapping")
                    
                with self.connection.cursor() as cursor:
                    get_attribute_query_string = f"""
                        select id from core.attribute where asset_id = '{asset_id}' and is_active = true and is_delete = false"""

                    get_attribute_ids_list = self.__run_postgres_query(get_attribute_query_string,
                                                    query_type='fetchall'
                                                    )
                    insert_attributes = []
                    if get_attribute_ids_list:
                        for attribute_id in get_attribute_ids_list:
                            attribute_id = attribute_id.get("id")
                            insert_attributes.append((
                                str(uuid.uuid4()),  # Unique ID
                                'attribute',  # Level
                                asset_id,  # Asset ID
                                attribute_id,  # Attribute ID
                                asset_tag_id,  # Tag ID
                            ))

                    if insert_attributes:
                        placeholders = ", ".join(["%s"] * len(insert_attributes[0]))
                        query_param = b",".join([
                            cursor.mogrify(f"({placeholders}, CURRENT_TIMESTAMP)", vals)
                            for vals in insert_attributes
                        ]).decode("utf-8")

                        insert_attribute_tag_mapping_query = f"""
                            INSERT INTO core.tags_mapping(
                                id, level, asset_id, attribute_id, tags_id, created_date
                            )
                            VALUES {query_param}
                            RETURNING id;
                        """
                        log_info(("insert_asset_tag_mapping_query", insert_attribute_tag_mapping_query))

                        try:
                            execute_query(self.connection, cursor, insert_attribute_tag_mapping_query)
                        except Exception as e:
                            log_info(("tags_mapping Query Execution Error:", str(e)))


    
    def databricks_uc_catalog_update(self):
        log_info(("databricks_uc_catalog_update"))
        try:
            import_config, export_config = self.__databricks_uc_channel_configuration()
            log_info(("import_config", import_config))
            log_info(("export_config", export_config))
            pull_domains = import_config.get("domains", False)
            pull_tags = import_config.get("tags", False)
            pull_products = import_config.get("products", False)
            metrics = get_asset_metrics(self.config)
            log_info(("pull_domains", pull_domains))
            log_info(("pull_tags", pull_tags))
            log_info(("pull_products", pull_products))

        #     asset_guid = self.fetch_guid_for_asset(asset_info)
            asset_flags = next(
                (item for item in export_config if "asset" in item), None
            )
            log_info(("asset_flags", asset_flags))
            attribute_flags = next(
                (item for item in export_config if "attribute" in item), None
            )
            log_info(("attribute_flags", attribute_flags))
            domain_flags = next(
                (
                    item
                    for item in reversed(export_config)
                    if "domain" in item
                    and "asset" not in item
                    and "attribute" not in item
                ),
                None,
            )
            log_info(("domain_flags", domain_flags))
            credentials_config = self.config.get("connection",{}).get("credentials", {})
            warehouse_id=self.__get_warehouse_id(self.credentials)
            log_info(("warehouse_id for debug", warehouse_id))
            query = f""" SELECT * FROM system.information_schema.table_tags"""
            tags_info = self.get_databricks_tags(self.config, warehouse_id, query)
            attributes_tags_info = self.get_databricks_tags(self.config, warehouse_id, query, is_column_query=True)
            tags_info.extend(attributes_tags_info)
            log_info(("tags_info 782", tags_info))
            distinct_tags_dict = {tag['guid']: tag for tag in tags_info}.values()
            tags_info = list(distinct_tags_dict)
            log_info(("tags_info 784", tags_info))
            if pull_tags:
                # Delete glossary mapping in dqlabs if deleted in databricks
                self.delete_tags_in_dqlabs(tags_info = tags_info) 

                # map databricks tags to dqlabs
                self.insert_databricks_tags_to_dqlabs(tags_info = tags_info)
            assets_info = self.__asset_information()
            for asset_info in assets_info:
                log_info(("asset_info", asset_info))
                asset_id = asset_info.get('asset_id')
                log_info(("asset_id", asset_id))
                databricks_asset_data, asset_exists_in_databricks = self.__check_asset_exists_in_databricks(
                    self.config, asset_info
                )
                if asset_exists_in_databricks:
                    if tags_info:
                        asset_tags_list = self.map_asset_metrics(asset_info, warehouse_id, pull_tags)
                        log_info(("asset_tags_list", asset_tags_list))
                        active_attributes = databricks_asset_data.get("columns", [])
                        self.map_attribute_metrics(asset_info, warehouse_id, active_attributes, tags_info, pull_domains, pull_tags)

                        if self.tag_propagation:
                            self.map_asset_tags_to_attributes(asset_info, asset_tags_list, active_attributes, pull_tags)
                    
                    if asset_flags.get("asset", False) and asset_exists_in_databricks:
                        """ Push Asset Custom Metadata from DQLabs to databricks"""
                        try:
                            if self.__push_asset_metadata(asset_info, asset_flags, warehouse_id) :
                                log_info(
                                    f"Push Asset Custom Metadata from DQLabs to Databricks for asset_id {asset_id} Success"
                                )
                            else:
                                log_info(
                                    f"Push Asset Custom Metadata from DQLabs to Databricks for asset_id {asset_id} Failed"
                                )
                        except Exception as e:
                            log_error(
                                "Push Asset Custom Metadata from DQLabs to Databricks Failed ", e
                            )
                else:
                    log_info((
                        f"Asset {asset_info.get('asset_name','')} does not exist in databricks"
                    ))

        except Exception as e:
            log_error("databricks Catalog Update Failed ", e)
