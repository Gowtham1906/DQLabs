import json
import urllib.parse
import requests
import pytz
import uuid

from typing import List, Dict, Any
from urllib.parse import urljoin

from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone, split_queries
from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.app_helper.crypto_helper import decrypt
from dqlabs.app_helper.dq_helper import format_freshness, get_client_origin
from dqlabs.enums.connection_types import ConnectionType , EntityType
from azure.identity import ClientSecretCredential
from dqlabs.app_helper.catalog_helper import (get_asset_metrics,
                                               get_attributes_metrics
                                               )

from dqlabs.app_constants.dq_constants import DEFAULT_SEMANTIC_THRESHOLD, PURVIEW


class Purview:    
    def __init__(self, config, channel):
        self.config = config
        log_info(('config for debug',self.config))
        self.channel = channel
        log_info(('channel for debug',self.channel))
        self.connection = get_postgres_connection(self.config)


    def get_token(self):
        """
        Retrieves an OAuth 2.0 access token for authenticating API requests to Azure Purview.
        
        This method uses client secret authentication via the `ClientSecretCredential` class from `azure.identity`.
        It fetches the required credentials from `self.channel`, decrypts the client secret, and attempts to obtain
        an access token from Azure.

        Returns:
            str: The access token if authentication is successful, otherwise None.
        """
        
        # Extract authentication credentials from the channel configuration
        tenant_id = self.channel.get("tenant_id")
        client_id = decrypt(self.channel.get("client_id") )if self.channel.get("is_vault_enabled") else self.channel.get("client_id")
        client_secret = decrypt(self.channel.get("client_secret"))
        
        try:
            # Authenticate using client secret credentials
            credential = ClientSecretCredential(tenant_id, client_id, client_secret)

            # Request an access token for Azure Purview API
            token = credential.get_token("https://purview.azure.net/.default")
            return token.token  # Return the access token
        
        except Exception as e:
            log_error(f"Failed to get token: {str(e)}")
            return None

    def __purview_channel_configuration(self) -> Dict[str, Any]:
        """
        Retrieves and returns the import and export configurations for Azure Purview from `self.channel`.
        
        This function logs the channel configuration, extracts the "import" and "export" settings,
        and ensures the "export" configuration is properly parsed from JSON format.
        
        Returns:
            Tuple[Dict[str, Any], Dict[str, Any]]: The import configuration (dict) and export configuration (dict).
        """
        log_info(("purview_configuration",self.channel))
        import_config = self.channel.get("import", {})
        export_config = json.loads(self.channel.get("export"))
        return import_config, export_config

    def check_import_metrics(self):
        """
        Checks and logs the status of import-related metrics from `self.channel`.
        
        This function verifies the presence of certain import settings, such as domains, tags, and products.
        It logs their status as boolean values.
        
        Raises:
            Exception: If any error occurs during the check, it is logged and re-raised.
        """
        try:
            channel_checks = self.channel.get("import", {})
            # domain checks
            self.domain_check = bool(channel_checks.get("domains"))
            log_info(("domain_check", self.domain_check))

            # term checks
            self.tags_check = bool(channel_checks.get("tags"))
            log_info(("tags_check", self.tags_check))

        except Exception as e:
            log_info(f"Error in check_import_metrics: {e}")
            raise Exception(f"Error in check_import_metrics: {e}")

    def __call_api_request(self, endpoint: str, method_type: str, params: str = ''):
        """
        Sends an API request to the specified endpoint using the given HTTP method.
        
        This function retrieves an authentication token, prepares request headers, and sends the request.
        It supports GET, POST, PUT, and DELETE methods.
        
        Args:
            endpoint (str): The API URL.
            method_type (str): The HTTP method ('get', 'post', 'put', 'delete').
            params (str, optional): The request payload (for POST and PUT methods).
        
        Returns:
            dict or requests.Response: The response JSON if successful, otherwise raises an error.
        
        Raises:
            ValueError: If token retrieval fails or if the API request encounters an error.
        """
        try:
        # Prepare Headers and Params
            log_info(("endpoint", endpoint))
            token = self.get_token()
            if not token:
                raise ValueError("Failed to retrieve token")
            
            api_headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
            }

            if method_type == "post":
                response = requests.post(
                    url=endpoint, headers=api_headers, data=json.dumps(params), verify=False)
                if response.status_code in [200, 201, 202, 204]:
                    return response
            elif method_type == "put":
                response = requests.put(
                    url=endpoint, data=params, headers=api_headers, verify=False)
            elif method_type == "delete":
                response = requests.delete(
                    url=endpoint, headers=api_headers, verify=False)
            else:
                response = requests.get(url=endpoint, headers=api_headers, verify=False)

            if (response and response.status_code in [200, 201, 202, 204]):
                if response.status_code in [204]:
                    return response
                return response.json()

            else:
                raise ValueError(response.raise_for_status())
        except Exception as e:
            log_error(
                "Purview Connector - Get Response Error", e)
            
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
                log_error("Purview Connector - Query failed Error", e)
            
            if query_type not in ['insert','update','delete']:
                if query_type == 'fetchone':
                    records = fetchone(cursor)
                elif query_type == 'fetchall':
                    records = fetchall(cursor)
                
                return records
            
            log_info(("Query run:", query_string))
            log_info(('Query Executed',output_statement))

    def __asset_information(self) -> Dict[str, Any]:
        """
        Retrieves and returns asset-related information from the configuration.
        
        This function extracts details such as asset ID, name, schema, database name,
        connection type, and additional asset properties from `self.config`.
        
        Returns:
            Dict[str, Any]: A dictionary containing asset details including ID, name,
            schema, database, connection type, and other properties.
        """
        asset_info = []
        asset = self.config.get("asset", {})
        if asset.get("properties", {}):
            # Fetch DQ Connection details
            connection = self.config.get("connection", {})
            connection_type = connection.get("type", "")

            # Fetch DQ Asset Details
            asset_properties = asset.get("properties", {})
            database_name = self.config.get("database_name")
            
            asset_data = {
                        "asset_id": self.config.get("asset_id"),
                        "asset_name": asset.get("name"),
                        "asset_schema": asset_properties.get("schema"),
                        "asset_database": database_name,
                        "connection_type": connection_type,
                        "asset_properties": asset_properties
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
                    AND connection.type IN ('postgresql', 'snowflake', 'mysql', 'adls')
            """
            assets = self.__run_postgres_query(query_string,
                                                query_type='fetchall'
                                                )
        except Exception as e:
            log_error(
                f"Purview Connector - Get Assets Error", e)
        return assets
    
    def filter_records(self, data, connection_type, asset_name, database_name, asset_properties):
        """
        Filters records based on connection type, asset name, and database name.
        
        This function iterates through the records in the provided data, checking if they match the given
        connection type, asset name, and database name.
        
        Args:
            data (dict): The input data containing records to filter.
            connection_type (str): The expected connection type.
            asset_name (str): The name of the asset.
            database_name (str): The name of the database.
        
        Returns:
            list: A list of filtered records that match the criteria.
        """
        filtered = []
        log_info(("Filtering data", data))
        # asset_name = urllib.parse.quote(asset_name, safe='')
        for record in data.get("value", []):
            qualified_name = record.get("qualifiedName", "")
            if (connection_type.lower() == ConnectionType.ADLS.value and 
                f"/{asset_properties.get('container','')}/{asset_properties.get('file_path', '')}" in qualified_name ):
                    log_info(("Found asset", json.dumps(record, indent=4)))
                    filtered.append(record)
            elif ((record.get("entityType", "").startswith(connection_type.lower()) and
                  record.get("name", "").lower() == asset_name.lower()) and
                  (f"/dbs/{database_name}/" in qualified_name.lower() or
                  f"/databases/{database_name}/" in qualified_name.lower())
                  ):
                    filtered.append(record)
        return filtered

    def fetch_guid_for_asset(self, asset_info):
        """
        Fetches the GUID for a given asset using Azure Purview API.
        
        This function constructs a search query to find the asset within Purview, retrieves matching records,
        and extracts the GUID of the first matching asset.
        
        Args:
            asset_info (dict): The asset information containing asset_name, connection_type, and asset_database.
        
        Returns:
            str: The GUID of the matching asset.
        
        Raises:
            Exception: If the asset is not found or if the API request fails.
        """
        assets_data = []
        try:
            host = self.channel.get("url")
            # Construct the URL for the token request
            url = "/datamap/api/search/query?api-version=2022-03-01-preview"
            endpoint = urljoin(host, url)
            params ={
                "keywords": f'{asset_info.get("asset_name")}',
                "limit": 1000,
                "offset": 0,
            }
            # fetch the attribute information from purview
            assets_data = self.__call_api_request(endpoint, 'post', params) 
            assets_data = assets_data.json()
            asset_name = asset_info.get("asset_name")
            connection_type = asset_info.get("connection_type")
            database_name = asset_info.get("asset_database")
            database_name = database_name.lower() if database_name else ''
            asset_properties = asset_info.get("asset_properties") if connection_type.lower() == ConnectionType.ADLS.value else ''
            asset_desc = self.filter_records(assets_data, connection_type, asset_name, database_name, asset_properties)
            if not asset_desc:
                log_info(f"Asset '{asset_info.get('asset_name')}' not found.")
                raise Exception(f"Asset '{asset_info.get('asset_name')}' not found in Purview.")
            else :
                return asset_desc[0]["id"]
        except Exception as e:
            log_error("Purview API failed while fetching guid for asset", e)

    def fetch_existing_business_metadata(self):
        """
        Fetches existing business metadata from Azure Purview.
        
        This function constructs a request to retrieve metadata definitions from Purview,
        specifically targeting the "DQLabs_Metadata" definition.
        
        Returns:
            dict: The retrieved business metadata if the request is successful.
        """
        business_metadata = []
        try:
            host = self.channel.get("url")
            # Construct the URL for the token request
            url = "/datamap/api/atlas/v2/types/businessmetadatadef/name/DQLabs_Metadata"
            endpoint = urljoin(host, url)

            # fetch the attribute information from purview
            business_metadata = self.__call_api_request(endpoint, 'get') 
        except Exception as e:
            log_error("Fetch Purview Business Metadata API failed", e)
        finally:
            return business_metadata

    def get_applicable_entity_types(self, attribute_name, business_metadata):
        """
        Retrieves the applicable entity types for a given business metadata attribute.
        
        This function searches through the business metadata attributes to find a matching attribute
        and returns the applicable entity types defined in its options.
        
        Args:
            attribute_name (str): The name of the attribute to search for.
            business_metadata (dict): The business metadata containing attribute definitions.
        
        Returns:
            list or None: A list of applicable entity types if found, otherwise None.
        """
        log_info("Getting applicable entity types")
        log_info(("Available attributes:", [attr["name"] for attr in business_metadata.get("attributeDefs", [])]))
        for attr in business_metadata["attributeDefs"]:
            if attr["name"] == attribute_name:
                return json.loads(attr["options"]["applicableEntityTypes"])
        return None

    def designed_payload_for_business_metadata(self, applicableEntityTypes_List=""):
        """
        Constructs a payload for defining business metadata in Azure Purview.
        
        This function generates a structured JSON payload containing metadata definitions,
        including data quality metrics such as DQ Score, Total Rows, Freshness, Active Rules, etc.
        
        Args:
            applicableEntityTypes_List (list, optional): A list of applicable entity types. Defaults to an empty string.
        
        Returns:
            str: JSON-formatted string representing the payload.
        """

        applicableEntityTypes_List = json.dumps(applicableEntityTypes_List) if applicableEntityTypes_List else ''
        true_value = 'true'
        false_value = 'false'
        payload = {
            "businessMetadataDefs": [
                {
                    "category": "BUSINESS_METADATA",
                    "name": "DQLabs_Metadata",
                    "description": "DQLabs data quality metadata",
                    "typeVersion": "1.0",
                    "serviceType": "atlas_core",
                    "attributeDefs": [
                        {
                            "name": "DQ Score",
                            "typeName": "float",
                            "isOptional": true_value,
                            "cardinality": "SINGLE",
                            "valuesMinCount": 0,
                            "valuesMaxCount": 1,
                            "isUnique": false_value,
                            "isIndexable": true_value,
                            "includeInNotification": false_value,
                            "options": {
                                "applicableEntityTypes": applicableEntityTypes_List
                            }
                        },
                        {
                            "name": "Total Rows",
                            "typeName": "int",
                            "isOptional": true_value,
                            "cardinality": "SINGLE",
                            "valuesMinCount": 0,
                            "valuesMaxCount": 1,
                            "isUnique": false_value,
                            "isIndexable": true_value,
                            "includeInNotification": false_value,
                            "options": {
                                "applicableEntityTypes": applicableEntityTypes_List
                            }
                        },
                        {
                            "name": "Freshness",
                            "typeName": "string",
                            "isOptional": true_value,
                            "cardinality": "SINGLE",
                            "valuesMinCount": 0,
                            "valuesMaxCount": 1,
                            "isUnique": false_value,
                            "isIndexable": true_value,
                            "includeInNotification": false_value,
                            "options": {
                                "applicableEntityTypes": applicableEntityTypes_List,
                                "maxStrLength": "2048"
                            }
                        },
                        {
                            "name": "Active Rules",
                            "typeName": "int",
                            "isOptional": true_value,
                            "cardinality": "SINGLE",
                            "valuesMinCount": 0,
                            "valuesMaxCount": 1,
                            "isUnique": false_value,
                            "isIndexable": true_value,
                            "includeInNotification": false_value,
                            "options": {
                                "applicableEntityTypes": applicableEntityTypes_List
                            }
                        },

                        {
                            "name": "Observed Rules",
                            "typeName": "int",
                            "isOptional": true_value,
                            "cardinality": "SINGLE",
                            "valuesMinCount": 0,
                            "valuesMaxCount": 1,
                            "isUnique": false_value,
                            "isIndexable": true_value,
                            "includeInNotification": false_value,
                            "options": {
                                "applicableEntityTypes": applicableEntityTypes_List
                            }
                        },
                        {
                            "name": "Scoring Rules",
                            "typeName": "int",
                            "isOptional": true_value,
                            "cardinality": "SINGLE",
                            "valuesMinCount": 0,
                            "valuesMaxCount": 1,
                            "isUnique": false_value,
                            "isIndexable": true_value,
                            "includeInNotification": false_value,
                            "options": {
                                "applicableEntityTypes": applicableEntityTypes_List
                            }
                        },
                        {
                            "name": "Duplicate Rows",
                            "typeName": "int",
                            "isOptional": true_value,
                            "cardinality": "SINGLE",
                            "valuesMinCount": 0,
                            "valuesMaxCount": 1,
                            "isUnique": false_value,
                            "isIndexable": true_value,
                            "includeInNotification": false_value,
                            "options": {
                                "applicableEntityTypes": applicableEntityTypes_List
                            }
                        },
                        {
                            "name": "Alerts",
                            "typeName": "int",
                            "isOptional": true_value,
                            "cardinality": "SINGLE",
                            "valuesMinCount": 0,
                            "valuesMaxCount": 1,
                            "isUnique": false_value,
                            "isIndexable": true_value,
                            "includeInNotification": false_value,
                            "options": {
                                "applicableEntityTypes": applicableEntityTypes_List
                            }
                        },
                        {
                            "name": "Issues",
                            "typeName": "int",
                            "isOptional": true_value,
                            "cardinality": "SINGLE",
                            "valuesMinCount": 0,
                            "valuesMaxCount": 1,
                            "isUnique": false_value,
                            "isIndexable": true_value,
                            "includeInNotification": false_value,
                            "options": {
                                "applicableEntityTypes": applicableEntityTypes_List
                            }
                        },
                        {
                            "name": "Last Updated Date",
                            "typeName": "string",
                            "isOptional": true_value,
                            "cardinality": "SINGLE",
                            "valuesMinCount": 0,
                            "valuesMaxCount": 1,
                            "isUnique": false_value,
                            "isIndexable": true_value,
                            "includeInNotification": false_value,
                            "options": {
                                "applicableEntityTypes": applicableEntityTypes_List,
                                "maxStrLength": "2048"
                            }
                        },
                        {
                            "name": "DqLabs Alerts Url",
                            "typeName": "string",
                            "isOptional": true_value,
                            "cardinality": "SINGLE",
                            "valuesMinCount": 0,
                            "valuesMaxCount": 1,
                            "isUnique": false_value,
                            "isIndexable": true_value,
                            "includeInNotification": false_value,
                            "options": {
                                "applicableEntityTypes": applicableEntityTypes_List,
                                "maxStrLength": "2048"
                            }
                        },
                        {
                            "name": "DqLabs Issues Url",
                            "typeName": "string",
                            "isOptional": true_value,
                            "cardinality": "SINGLE",
                            "valuesMinCount": 0,
                            "valuesMaxCount": 1,
                            "isUnique": false_value,
                            "isIndexable": true_value,
                            "includeInNotification": false_value,
                            "options": {
                                "applicableEntityTypes": applicableEntityTypes_List,
                                "maxStrLength": "2048"
                            }
                        },
                        {
                            "name": "DqLabs Measures Url",
                            "typeName": "string",
                            "isOptional": true_value,
                            "cardinality": "SINGLE",
                            "valuesMinCount": 0,
                            "valuesMaxCount": 1,
                            "isUnique": false_value,
                            "isIndexable": true_value,
                            "includeInNotification": false_value,
                            "options": {
                                "applicableEntityTypes": applicableEntityTypes_List,
                                "maxStrLength": "2048"
                            }
                        }
                        

                    ]
                }
            ]
        }
    
        return json.dumps(payload, indent=4)


    def send_metadata_request(self, guid, params):
        """
        Sends a request to update business metadata for a specific entity in Azure Purview.
        
        This function constructs the API request URL using the provided GUID and sends the metadata
        update request with the given parameters.
        
        Args:
            guid (str): The globally unique identifier of the entity to update.
            params (dict): The metadata update payload.
        
        Returns:
            bool: True if the request was successful (status code 204), False otherwise.
        """
        url = f"/datamap/api/atlas/v2/entity/guid/{guid}/businessmetadata?isOverwrite=true"
        endpoint = urljoin(self.channel.get("url"), url)
        response = self.__call_api_request(endpoint, 'post', params)
        return response.status_code == 204

    def create_metadata(self):
        """
        Creates a new business metadata definition in Azure Purview.
        
        This function generates a metadata definition payload and sends it to the Purview API
        to create the business metadata type "DQLabs_Metadata". If the metadata already exists,
        the function logs a message and exits successfully.
        
        Returns:
            bool: True if the metadata creation was successful or already exists (status code 200 or 409),
                False if the request failed.
        """
        params = self.designed_payload_for_business_metadata()
        host = self.channel.get("url")
        url = "/catalog/api/atlas/v2/types/typedefs"
        endpoint = urljoin(host, url)
        response = self.__call_api_request(endpoint, 'post', params)
        if response.status_code in [200,409]:
            log_info("Successfully created business metadata in Purview.")
            return True  # Return True if successful, else False in case of failure.
        else:
            log_info(f"Failed to create business metadata in Purview. Response: {response.text}")
            return False  # Return False in case of failure.
        

    def register_asset_metadata(self, asset_guid, asset_info, asset_flags, is_created_metadata):
        """
        Registers business metadata for a given asset in Azure Purview.

        This function checks whether the metadata already exists and if the asset type matches
        the applicable entity types. If needed, it updates the metadata schema before pushing
        the metadata to Purview.

        Args:
            asset_guid (str): The unique identifier for the asset in Purview.
            asset_info (dict): Information about the asset, including connection type and name.
            asset_flags (dict): Additional asset-specific flags for metadata processing.
            is_created_metadata (bool): Indicates whether the metadata has already been created.

        Returns:
            None
        """
        if asset_guid:
            if is_created_metadata:
                count = 0
                connection_type = asset_info.get("connection_type")
                
                existing_business_metadata=self.fetch_existing_business_metadata()
                attribute_name = "DQ Score"
                applied_entities = self.get_applicable_entity_types(attribute_name, existing_business_metadata)
                if connection_type == ConnectionType.Postgres.value:
                    entity_types = EntityType.Postgres.value
                elif connection_type == ConnectionType.Snowflake.value:
                    entity_types = EntityType.Snowflake.value
                elif connection_type == ConnectionType.MySql.value:
                    entity_types = EntityType.MySql.value
                elif connection_type == ConnectionType.Oracle.value:
                    entity_types = EntityType.Oracle.value
                elif connection_type == ConnectionType.SapHana.value:
                    entity_types = EntityType.SapHana.value
                elif connection_type == ConnectionType.MSSQL.value:
                    entity_types = EntityType.MSSQL.value
                elif connection_type == ConnectionType.ADLS.value:
                    entity_types = EntityType.ADLS.value
                for entity in applied_entities:
                    if entity in entity_types:
                        count += 1
                        pass
                if count == len(entity_types):
                    if self.push_custom_metadata(asset_info = asset_info, asset_guid = asset_guid, asset_flags = asset_flags):
                        log_info(f"Successfully pushed business metadata for Purview asset '{asset_info.get('asset_name')}'.")
                    else:
                        log_error(f"Failed to push business metadata for Purview asset ", asset_info.get('asset_name'))

                else:
                    for entity in entity_types:
                        if entity not in applied_entities:
                            applied_entities.append(entity)
                    # json_list_str = json.dumps(applied_entities)
                    # applicableEntityTypes_List = json.dumps(json_list_str)
                    
                    params = self.designed_payload_for_business_metadata(applied_entities)
                    host = self.channel.get("url")
                    url = "/catalog/api/atlas/v2/types/typedefs"
                    endpoint = urljoin(host, url)
                    response = self.__call_api_request(endpoint, 'put', params)
                    if isinstance(response, dict):
                        log_info(f"Successfully associated business metadata to Purview '{asset_info.get('connection_type')}'.")
                        if self.push_custom_metadata(asset_info = asset_info, asset_guid = asset_guid, asset_flags = asset_flags):
                            log_info(f"Successfully pushed business metadata for Purview asset '{asset_info.get('asset_name')}'.")
                        else:
                            log_error(f"Failed to push business metadata for Purview asset", asset_info.get('asset_name'))


                   
    def push_custom_metadata(self, asset_info, asset_guid, active_attributes=[], asset_flags={}, attributes_flags={}):
        """
        Pushes custom business metadata for an asset or its attributes in Azure Purview.
        
        This function retrieves and formats metadata related to an asset or its attributes,
        such as data quality scores, row counts, freshness, and issue details. The formatted
        metadata is then sent to Azure Purview using an API request.
        
        Args:
            asset_info (dict): Information about the asset.
            asset_guid (str): The globally unique identifier of the asset.
            active_attributes (list, optional): List of attributes to process (if applicable).
            asset_flags (dict, optional): Flags for asset-related metadata processing.
            attributes_flags (dict, optional): Flags for attribute-related metadata processing.
        
        Returns:
            bool: True if metadata push was successful, False otherwise.
        """

        dq_score = None
        total_rows = None
        freshness = None
        active_rules = None
        observed_rules = None
        scoring_rules = None
        duplicate_rows = None
        alerts = None
        issues = None
        last_updated_date = None
        dqlabs_alerts_url = None
        dqlabs_measures_url = None
        dqlabs_issues_url = None
        asset_id = asset_info.get("asset_id")

        if active_attributes and attributes_flags:
            dqlabs_host = self.channel.get("dq_url")
            attribute_metrics = get_attributes_metrics(self.config, asset_id)
            attribute_export_options = attributes_flags["options"]

            for attribute in active_attributes:
                attributes_guid = attribute.get("guid")
                log_info(("attribute_guid",attributes_guid))
                attribute_name = attribute.get("name")
                log_info(("attribute_name",attribute_name))
                try:
                    # filter dqlabs attribute based on purview attribute name
                    metrics = next((x for x in attribute_metrics if x["name"].lower() == attribute_name.lower()), None)
                    attribute_guid = metrics.get("attribute_id")
                    log_info(("attribute_id",attribute_guid))
                    if metrics:
                        last_updated_time = metrics.get("last_updated_date", "")
                        # Localize the time to UTC
                        ist_zone = pytz.timezone('Asia/Kolkata')
                        # If the datetime is already timezone-aware, directly convert it to IST
                        if last_updated_time.tzinfo is not None:
                            last_updated_time_ist = last_updated_time.astimezone(ist_zone)
                        else:
                            # If the datetime is naive (no tzinfo), localize it to UTC first
                            last_updated_time = pytz.utc.localize(last_updated_time)
                            last_updated_time_ist = last_updated_time.astimezone(ist_zone)
                        formatted_time = last_updated_time_ist.strftime('%d-%m-%Y %I:%M:%S %p')

                        if attribute_export_options.get("summary"):
                                dq_score = metrics.get("score")
                                total_rows = metrics.get("row_count")
                                freshness = metrics.get('freshness')
                                active_rules = metrics.get("active_measures")
                                observed_rules = metrics.get("observed_measures")
                                scoring_rules = metrics.get("scored_measures")
                                duplicate_rows = metrics.get("duplicate_count")
                                alerts = metrics.get("alerts")
                                issues = metrics.get("issues")
                                last_updated_date = formatted_time

                        if attribute_export_options.get("alerts"):
                            asset_alerts_url = urljoin(dqlabs_host, f"remediate/alerts?asset_id={asset_id}&a_type=data")
                            dqlabs_alerts_url = asset_alerts_url

                        if attribute_export_options.get("issues"): 
                            issue_alerts_url = urljoin(dqlabs_host, f"remediate/issues?asset_id={asset_id}&a_type=data")
                            dqlabs_issues_url = issue_alerts_url

                        if attribute_export_options.get("measures"):    
                            measures_alerts_url = urljoin(dqlabs_host, f"observe/data/{asset_id}/attributes/{attribute_guid}#measures-table")
                            dqlabs_measures_url = measures_alerts_url     

                        # url = f"/datamap/api/atlas/v2/entity/guid/{attribute['guid']}/businessmetadata?isOverwrite=true"
                        # endpoint = urljoin(self.channel.get("url"), url)
                        params = {
                            "DQLabs_Metadata": {k: v for k, v in {
                                "DQ Score": dq_score,
                                "Total Rows": total_rows,
                                "Freshness": freshness,
                                "Active Rules": active_rules,
                                "Observed Rules": observed_rules,
                                "Scoring Rules": scoring_rules,
                                "Duplicate Rows": duplicate_rows,
                                "Alerts": alerts,
                                "Issues": issues,
                                "Last Updated Date": last_updated_date,
                                "DqLabs Alerts Url": dqlabs_alerts_url,
                                "DqLabs Issues Url": dqlabs_issues_url,
                                "DqLabs Measures Url": dqlabs_measures_url,
                            }.items() if v is not None}
                        }
                        
                        # response = self.__call_api_request(endpoint, 'post', params)
                        if self.send_metadata_request(attributes_guid, params):
                            log_info((f"Successfully pushed business metadata for Purview attribute for '{attribute_name}'"))  
                        else:
                            log_info((f"Failed to push business metadata for Purview attribute for '{attribute_name}'"))
        
                            
                except Exception as e:
                    log_error(
                            "Purview Attribute Custom Metadata Update Failed ", e)
            return True
        
        elif asset_flags: 
            asset_export_options = asset_flags.get("options", {})
            log_info(("asset_export_options", asset_export_options))
            dqlabs_host = self.channel.get("dq_url")
            metrics = get_asset_metrics(self.config, asset_id)
            log_info(("metrics 503", metrics))
            params = {}
            if metrics:
                last_updated_time = metrics.get("last_updated_date", "")

                # Convert time to IST
                ist_zone = pytz.timezone('Asia/Kolkata')
                if last_updated_time.tzinfo is None:
                    last_updated_time = pytz.UTC.localize(last_updated_time)

                last_updated_time_ist = last_updated_time.astimezone(ist_zone)
                formatted_time = last_updated_time_ist.strftime('%d-%m-%Y %I:%M:%S %p')

                
                # Extract relevant values
                if asset_export_options.get("summary"):
                    dq_score = metrics.get("score")
                    total_rows = metrics.get("row_count")
                    freshness = format_freshness(metrics.get('freshness'))
                    active_rules = metrics.get("active_measures")
                    scoring_rules = metrics.get("scored_measures")
                    duplicate_rows = metrics.get("duplicate_count")
                    alerts = metrics.get("alerts")
                    issues = metrics.get("issues")

                if asset_export_options.get("alerts"):
                    dqlabs_alerts_url = urljoin(dqlabs_host, f"remediate/alerts?asset_id={asset_id}&a_type=data")

                if asset_export_options.get("issues"):
                    issue_alerts_url = urljoin(dqlabs_host, f"remediate/issues?asset_id={asset_id}&a_type=data")
                    dqlabs_issues_url = issue_alerts_url

                if asset_export_options.get("measures"):
                    dqlabs_measures_url = urljoin(dqlabs_host, f"observe/data/{asset_id}/attributes#measures-table")

                # Remove None values before sending
                params = {
                        "DQLabs_Metadata": {k: v for k, v in {
                            "DQ Score": dq_score,
                            "Total Rows": total_rows,
                            "Freshness": freshness,
                            "Active Rules": active_rules,
                            "Observed Rules": observed_rules,
                            "Scoring Rules": scoring_rules,
                            "Duplicate Rows": duplicate_rows,
                            "Alerts": alerts,
                            "Issues": issues,
                            "Last Updated Date": last_updated_date,
                            "DqLabs Alerts Url": dqlabs_alerts_url,
                            "DqLabs Issues Url": dqlabs_issues_url,
                            "DqLabs Measures Url": dqlabs_measures_url,
                    }.items() if v is not None}
                }

            return True if self.send_metadata_request(asset_guid, params) else False


    def fetch_entity_details(self, asset_guid):
        """
        Fetches details of a specific entity from Azure Purview using its GUID.
        
        Args:
            asset_guid (str): The globally unique identifier of the asset.
        
        Returns:
            list: A list of active attributes associated with the entity.
        """
        try:
            host = self.channel.get("url")
            # Construct the URL for the token request
            url = f"/datamap/api/atlas/v2/entity/guid/{asset_guid}?api-version=2023-09-01"
            endpoint = urljoin(host, url)

            # fetch the attribute information from purview
            entity_details = self.__call_api_request(endpoint, 'get')
            active_attributes = self.extract_active_attributes(entity_details)
            return active_attributes  # Return the active attributes details.
        except Exception as e:
            log_error("Fetch Purview Entity Details API failed", e)

    
    def extract_active_attributes(self, data):
        """
        Extracts active attributes from entity data retrieved from Azure Purview.
        
        Args:
            data (dict): Entity details response from Purview.
        
        Returns:
            list: A list of active attributes with their GUIDs and names.
        """
        active_attributes = []
        
        for guid, entity in data.get("referredEntities", {}).items():
            if entity.get("status") == "ACTIVE":
                table_info = (
                    entity.get("relationshipAttributes", {}).get("table", {})
                    if entity.get("relationshipAttributes", {}).get("table", {})
                    else entity.get("relationshipAttributes", {}).get("view", {})
                )
                if table_info.get("relationshipStatus") == "ACTIVE":
                    active_attributes.append({
                        "guid": guid,
                        "name": entity.get("attributes", {}).get("name", "Unknown")
                    })
        
        return active_attributes


    def register_attribute_metadata(self, asset_guid, asset_info, attribute_flags, is_created_metadata):
        """
        Registers attribute metadata for a given asset in Microsoft Purview. This function determines the applicable 
        entity types for business metadata association, verifies existing metadata, and pushes new metadata if necessary.

        Parameters:
        - asset_guid (str): The unique identifier of the asset.
        - asset_info (dict): Contains asset details such as connection type and asset name.
        - attribute_flags (dict): Flags indicating specific attributes to be applied.
        - is_created_metadata (bool): Specifies whether new metadata is being created.

        Process:
        1. Fetches existing metadata details for the asset.
        2. Determines applicable entity types based on the connection type.
        3. Verifies if the metadata is already applied.
        4. If all entity types are covered, pushes business metadata.
        5. If not all entity types are covered, updates applicable entity types and pushes the metadata.
        6. Logs success or failure messages accordingly.

        The function interacts with an API to associate business metadata and ensures consistency in metadata registration.
        """
        if asset_guid:
            active_attributes = self.fetch_entity_details(asset_guid)
            if is_created_metadata:
                count = 0
                connection_type = asset_info.get("connection_type")
                existing_business_metadata=self.fetch_existing_business_metadata()
                attribute_name = "DQ Score"
                applied_entities = self.get_applicable_entity_types(attribute_name, existing_business_metadata)
                if connection_type == ConnectionType.Postgres.value:
                    entity_types = EntityType.Postgres.value
                elif connection_type == ConnectionType.Snowflake.value:
                    entity_types = EntityType.Snowflake.value
                elif connection_type == ConnectionType.MySql.value:
                    entity_types = EntityType.MySql.value
                elif connection_type == ConnectionType.Oracle.value:
                    entity_types = EntityType.Oracle.value
                elif connection_type == ConnectionType.SapHana.value:
                    entity_types = EntityType.SapHana.value
                elif connection_type == ConnectionType.MSSQL.value:
                    entity_types = EntityType.MSSQL.value
                elif connection_type == ConnectionType.ADLS.value:
                    entity_types = EntityType.ADLS.value
                for entity in applied_entities:
                    if entity in entity_types:
                        count += 1
                        pass
                if count == len(entity_types):
                    if self.push_custom_metadata(asset_info = asset_info, asset_guid = asset_guid, active_attributes = active_attributes, attributes_flags = attribute_flags):
                        log_info(f"Successfully pushed business metadata for Purview asset '{asset_info.get('asset_name')}'.")
                    else:
                        log_error(f"Failed to push business metadata for Purview asset ", {asset_info.get('asset_name')})
                else :
                    for entity in entity_types:
                        if entity not in applied_entities:
                            applied_entities.append(entity)
                    
                    params = self.designed_payload_for_business_metadata(applied_entities)
                    host = self.channel.get("url")
                    url = "/catalog/api/atlas/v2/types/typedefs"
                    endpoint = urljoin(host, url)
                    response = self.__call_api_request(endpoint, 'put', params)
                    if isinstance(response, dict):
                        log_info(f"Successfully associated business metadata to Purview '{asset_info.get('connection_type')}'.")
                        if self.push_custom_metadata(asset_info = asset_info, asset_guid = asset_guid, active_attributes = active_attributes, attributes_flags = attribute_flags):
                            log_info(f"Successfully pushed business metadata for Purview asset '{asset_info.get('asset_name')}'  Attributes")
                        else:
                            log_error(f"Failed to push business metadata for Purview asset ",asset_info.get('asset_name'))

    def fetch_glossary(self) -> List[Dict[str, Any]]:
        """
        Fetch all glossary information from the Purview API.

        This function constructs the full URL for the glossary API request, queries the Purview
        API to retrieve the glossary details, and returns them as a list of dictionaries.

        It handles any exceptions that occur during the API request and logs them for debugging purposes.

        Returns
        -------
        List[Dict[str, Any]]
            A list of dictionaries containing glossary information fetched from Purview.
            Each dictionary represents a glossary term and its associated details.

        Examples
        --------
        >>> obj = YourClass()
        >>> obj.fetch_glossary()
        [{'glossary1_key': 'value1', 'glossary2_key': 'value2'}]  # Example output for fetched glossary

        Notes
        -----
        - The function assumes that the `host` configuration (API base URL) is available in the channel.
        - The glossary data is fetched by calling the `/api/meta/glossary` endpoint from the Purview API.
        - Any exceptions raised during the API request are logged for debugging.

        """
        """ Get all the glossary information"""
        domain_info = []
        try:
            host = self.channel.get("url")
            # Construct the URL for the token request
            url = "/datamap/api/atlas/v2/glossary"
            endpoint = urljoin(host, url)

            # fetch the attribute information from purview
            domain_info = self.__call_api_request(endpoint, "get")
        except Exception as e:
            log_error("Fetch Purview Glossary API failed", e)
        finally:
            return domain_info

    def insert_purview_terms_to_dqlabs(
        self, glossary_info: List[Dict[str, Any]]
    ) -> object:
        """
        Inserts glossary terms from Microsoft Purview into the DQLabs database.

        Parameters:
        - glossary_info (List[Dict[str, Any]]): A list of dictionaries containing glossary details such as terms, 
        domain ID, and domain name.

        Process:
        1. Checks if glossary information is provided; logs and exits if empty.
        2. Iterates through glossary terms, extracting relevant attributes like term ID, name, and categories.
        3. Checks if the term already exists in the DQLabs database by querying core.terms.
        4. If the term does not exist:
            - Determines the appropriate domain ID and constructs a technical name.
            - Prepares an SQL `INSERT` query with term attributes.
            - Executes the query to store the term in DQLabs.
            - Logs the success or failure of the operation.

        The function ensures that only new terms are inserted while avoiding duplication, and handles exceptions 
        gracefully to log errors.

        Returns:
        - None (Logs messages for debugging and tracking insert operations).
        """
        if not glossary_info:
            log_info("No glossary terms found to insert.")
            return

        for glossary in glossary_info:
            terms = glossary.get("terms", [])
            domain_id = glossary.get("guid", "")
            domain_name = glossary.get("name", "")

            for term in terms:
                term_id = term.get("termGuid")
                term_name = term.get("displayText")
                term_categories = term.get("attributes", {}).get("categories", [])
                term_categories = term_categories if term_categories else []

                log_info(("Processing term", term_name, "ID", term_id))

                # Check if term already exists
                term_check_query = f"""
                    SELECT EXISTS (
                        SELECT 1 FROM core.terms
                        WHERE id = '{term_id}' AND source = '{PURVIEW}'
                        AND is_active = TRUE AND is_delete = FALSE
                    );
                """
                term_check_flag = self.__run_postgres_query(
                    term_check_query, query_type="fetchone"
                )
                term_exists = term_check_flag.get("exists", False)

                if not term_exists:
                    term_domain_id = domain_id  # Default to domain ID
                    technical_name = f"{term_name}({domain_name})"

                    if term_categories:
                        category_data = term_categories[0]
                        term_domain_id = category_data.get("guid", domain_id)
                        # category_technical_name = (
                        #     self.__get_term_category_technical_name(term_domain_id)
                        # )
                        technical_name = f"{term_name}"

                    try:
                        with self.connection.cursor() as cursor:
                            query_input = (
                                term_id,
                                term_name,
                                technical_name,
                                None,
                                str(self.config.get("organization_id")),
                                domain_id,
                                term_domain_id,
                                True,
                                False,
                                PURVIEW,
                                "Pending",
                                "Text",
                                67,
                                1,
                                False,
                                False,
                                False,
                                False,
                                json.dumps([]),
                                json.dumps([]),
                                json.dumps([]),
                                json.dumps([]),
                            )
                            input_literals = ", ".join(["%s"] * len(query_input))
                            query_param = cursor.mogrify(
                                f"({input_literals}, CURRENT_TIMESTAMP)", query_input
                            ).decode("utf-8")

                            insert_query = f"""
                                INSERT INTO core.terms(
                                    id, name, technical_name, description, organization_id, domain_parent_id, domain_id,
                                    is_active, is_delete, source, status, derived_type, threshold, sensitivity,
                                    is_null, is_blank, is_unique, is_primary_key,
                                    tags, enum, contains, synonyms, created_date
                                ) VALUES {query_param} RETURNING id;
                            """
                            log_info(("Executing insert query", insert_query))
                            execute_query(self.connection, cursor, insert_query)
                            log_info(
                                (f"Inserted term: {term_name} ({term_id}) into DQLabs")
                            )
                    except Exception as e:
                        log_error(f"Failed to insert term {term_name}", e)

    def __delete_terms(self, purview_term_guids: List[Dict[str, Any]]) -> object:
        """
        Delete terms from DQLabs.

        This function deletes terms from the DQLabs glossary that are not present in the provided list of purview term GUIDs.
        It fetches all purview terms from the DQLabs database, compares them with the provided list of GUIDs, and performs the following actions:
        1. Unmaps terms from the `core.terms_mapping` table.
        2. Deletes associated version history entries from the `core.version_history` table.
        3. Deletes the term itself from the `core.terms` table.

        Args
        ----
        purview_term_guids : List[Dict[str, Any]]
            A list of dictionaries containing the GUIDs of terms that should not be deleted. The function will delete any term not present in this list.

        Returns
        -------
        object
            The function performs several database operations (unmapping, deleting version history, and deleting the term), but does not return a value. It executes the queries and logs output statements.

        Example
        -------
        purview_term_guids = [{"guid": "guid_1"}, {"guid": "guid_2"}]
        __delete_terms(purview_term_guids)

        Notes
        -----
        - The function performs three SQL queries:
        1. A query to fetch the existing terms from the `core.terms` table.
        2. A query to delete the term mappings from the `core.terms_mapping` table.
        3. A query to delete the term version history from the `core.version_history` table.
        4. A query to delete the term itself from the `core.terms` table.
        - The function only deletes terms that are not present in the provided `purview_term_guids` list.
        - For each term, the function logs output statements to indicate what has been deleted or unmapped.

        Errors
        ------
        If an error occurs while executing any query, it is expected that the function will handle and log the error appropriately.
        """
        """ Delete terms from dqlabs"""
        dqlabs_purview_terms = []
        # fetch all purview glossary terms in dqlabs
        dqlabs_terms_query = f"""
                            select id from core.terms
                            where source = '{PURVIEW}' 
                            """
        dqlabs_purview_terms_response = self.__run_postgres_query(
            query_string=dqlabs_terms_query, query_type="fetchall"
        )
        if dqlabs_purview_terms_response:
            dqlabs_purview_terms = [
                term.get("id") for term in dqlabs_purview_terms_response
            ]

        if dqlabs_purview_terms:
            for term_id in dqlabs_purview_terms:
                if term_id not in purview_term_guids:
                    remove_terms_mapping_query = f"""
                                                    delete from core.terms_mapping
                                                    where term_id = '{term_id}'
                                                """
                    # delete terms mapping for id
                    self.__run_postgres_query(
                        query_string=remove_terms_mapping_query,
                        query_type="delete",
                        output_statement=f"{term_id} unmapped from terms mapping",
                    )

                    # delete terms version history
                    remove_terms_version_history = f"""
                                                    delete from core.version_history
                                                    where term_id = '{term_id}'
                                                """
                    self.__run_postgres_query(
                        query_string=remove_terms_version_history,
                        query_type="delete",
                        output_statement=f"{term_id} deleted from version_history",
                    )

                    # delete the term
                    remove_terms_id = f"""
                                        delete from core.terms
                                        where id = '{term_id}'
                                    """
                    self.__run_postgres_query(
                        query_string=remove_terms_id,
                        query_type="delete",
                        output_statement=f"{term_id} deleted from terms",
                    )

    def __delete_glossary(self, purview_glossary_guids: List[Dict[str, Any]]) -> object:
        """
        Delete glossary from DQLabs.

        This function deletes glossaries from the DQLabs glossary system that are not present in the provided list of Purview glossary GUIDs.
        It performs the following actions for glossaries that are not in the provided list:
        1. Deletes any glossary mappings related to the glossary.
        2. Removes all associated categories.
        3. Deletes terms related to the categories.
        4. Unmaps and deletes categories related to the glossary.
        5. Deletes the glossary itself.

        Args
        ----
        purview_glossary_guids : List[Dict[str, Any]]
            A list of dictionaries containing the GUIDs of glossaries that should not be deleted. The function will delete any glossary not present in this list.

        Returns
        -------
        object
            The function does not return any value. It performs several database operations (deleting mappings, categories, terms, etc.) and logs output statements.

        Example
        -------
        purview_glossary_guids = [{"guid": "guid_1"}, {"guid": "guid_2"}]
        __delete_glossary(purview_glossary_guids)

        Notes
        -----
        - The function performs several SQL queries:
        1. Fetching glossaries from `core.domain`.
        2. Removing glossary mappings from `core.domain_mapping`.
        3. Fetching and deleting associated categories for each glossary.
        4. Deleting terms associated with those categories from `core.terms`.
        5. Deleting version history entries for those terms from `core.version_history`.
        6. Unmapping and deleting categories from `core.domain_mapping` and `core.domain`.
        7. Finally, deleting the glossary from `core.domain`.
        - The function only deletes glossaries that are not present in the provided list of GUIDs.
        - For each glossary, category, and term, the function logs output statements to indicate what has been deleted or unmapped.

        Errors
        ------
        If an error occurs while executing any query, it is expected that the function will handle and log the error appropriately.
        """
        """ Delete glossary from dqlabs"""
        dqlabs_purview_glossary = []
        # fetch all purview glossary terms in dqlabs
        dqlabs_glossary_query = f"""
                            select id from core.domain
                            where source = '{PURVIEW}' 
                            and type = 'domain'
                            """
        dqlabs_glossary_query_response = self.__run_postgres_query(
            query_string=dqlabs_glossary_query, query_type="fetchall"
        )
        if dqlabs_glossary_query_response:
            dqlabs_purview_glossary = [
                glossary.get("id") for glossary in dqlabs_glossary_query_response
            ]

        if dqlabs_purview_glossary:
            for glossary_id in dqlabs_purview_glossary:
                if glossary_id not in purview_glossary_guids:
                    """ 
                    Delete Glossary Mapping related to Domain ID
                    """
                    # Get all the categories for the glossary id
                    domain_category_query = f"""
                                        select id from core.domain
                                        where parent_id = '{glossary_id}'
                                        and source = '{PURVIEW}'
                                        """

                    fetch_category_for_domain_response = self.__run_postgres_query(
                        query_string=domain_category_query, query_type="fetchall"
                    )
                    categories_to_be_deleted = [
                        category.get("id")
                        for category in fetch_category_for_domain_response
                    ]

                    for category_id in categories_to_be_deleted:
                        # fetch the terms id mapped to category
                        category_terms_query = f"""
                                            select id from core.terms
                                            where domain_id = '{category_id}'
                                            and source = '{PURVIEW}'
                                            """
                        fetch_terms_for_category_response = self.__run_postgres_query(
                            query_string=category_terms_query, query_type="fetchall"
                        )
                        terms_to_be_deleted = [
                            term.get("id") for term in fetch_terms_for_category_response
                        ]
                        if terms_to_be_deleted:
                            for term_id in terms_to_be_deleted:
                                remove_terms_mapping_query = f"""
                                                                delete from core.terms_mapping
                                                                where term_id = '{term_id}'
                                                            """
                                # delete terms mapping for id
                                self.__run_postgres_query(
                                    query_string=remove_terms_mapping_query,
                                    query_type="delete",
                                    output_statement=f"{term_id} unmapped from tags mapping",
                                )

                                # delete terms version history
                                remove_terms_version_history = f"""
                                                                delete from core.version_history
                                                                where term_id = '{term_id}'
                                                            """
                                self.__run_postgres_query(
                                    query_string=remove_terms_version_history,
                                    query_type="delete",
                                    output_statement=f"{term_id} deleted from version_history",
                                )

                                remove_terms_id = f"""
                                                    delete from core.terms
                                                    where id = '{term_id}'
                                                """
                                # delete the term
                                self.__run_postgres_query(
                                    query_string=remove_terms_id,
                                    query_type="delete",
                                    output_statement=f"{term_id} deleted from terms",
                                )

                        """ 
                        Delete Categories Mapping and Category ID
                        """

                        remove_category_mapping_query = f"""
                                                        delete from core.domain_mapping
                                                        where domain_id = '{category_id}'
                                                    """
                        # delete category mapping for id
                        self.__run_postgres_query(
                            query_string=remove_category_mapping_query,
                            query_type="delete",
                            output_statement=f"{category_id} unmapped from categories mapping",
                        )

                        remove_category_id = f"""
                                            delete from core.domain
                                            where id = '{category_id}'
                                            and type = 'category'
                                        """
                        # delete the term
                        self.__run_postgres_query(
                            query_string=remove_category_id,
                            query_type="delete",
                            output_statement=f"{category_id} deleted from categories",
                        )

                    """ 
                    Delete Domain Mapping and Domain ID
                    """
                    remove_domain_mapping_query = f"""
                                                    delete from core.domain_mapping
                                                    where domain_id = '{glossary_id}'
                                                """
                    # delete category mapping for id
                    self.__run_postgres_query(
                        query_string=remove_domain_mapping_query,
                        query_type="delete",
                        output_statement=f"{glossary_id} unmapped from domain mapping",
                    )

                    remove_domain_id = f"""
                                        delete from core.domain
                                        where id = '{glossary_id}'
                                        and type = 'domain'
                                    """
                    # delete the glossary
                    self.__run_postgres_query(
                        query_string=remove_domain_id,
                        query_type="delete",
                        output_statement=f"{glossary_id} deleted from categories",
                    )

    def delete_glossary_in_dqlabs(self, domains_info: List[Dict[str, Any]]) -> object:
        """
        Delete glossary, categories, and terms mapping in DQLabs if deleted in purview.

        This function handles the deletion of glossaries, categories, and terms in DQLabs if they are marked for deletion in purview.
        The deletion is performed based on the provided `domains_info` list, which contains information about the glossaries, categories, and terms.

        Args
        ----
        domains_info : List[Dict[str, Any]]
            A list of dictionaries containing information about glossaries, categories, and terms that need to be deleted.
            Each dictionary can contain the following keys:
            - 'guid': The glossary GUID.
            - 'categories': A list of categories within the glossary, each containing a 'categoryGuid'.
            - 'terms': A list of terms within the glossary, each containing a 'termGuid'.

        Returns
        -------
        object
            The function does not return any value. It performs several deletion operations in the DQLabs system to remove glossaries, categories, and terms.

        Example
        -------
        domains_info = [
            {
                "guid": "glossary_guid_1",
                "categories": [{"categoryGuid": "category_guid_1"}],
                "terms": [{"termGuid": "term_guid_1"}]
            },
            {
                "guid": "glossary_guid_2",
                "categories": [{"categoryGuid": "category_guid_2"}],
                "terms": [{"termGuid": "term_guid_2"}]
            }
        ]
        delete_glossary_in_dqlabs(domains_info)

        Notes
        -----
        - The function extracts glossary, category, and term GUIDs from the `domains_info` list and deletes them in DQLabs by calling the `__delete_terms`, `__delete_categories`, and `__delete_glossary` functions.
        - If there are no terms, categories, or glossaries to delete, the respective deletion functions will not be called.
        - The deletion process is done in the following order:
        1. Deleting terms using the `__delete_terms` function.
        2. Deleting categories using the `__delete_categories` function.
        3. Deleting glossaries using the `__delete_glossary` function.
        """
        """ Delete glossary and terms mapping in dqlabs if delete in purview"""
        glossary_guids = []
        term_guids = []

        for item in domains_info:
            glossary_guids.append(item["guid"])
            if "terms" in item:
                term_guids.extend(
                    term["termGuid"]
                    for term in item["terms"]
                    if "displayText" in term.keys()
                )

        # update terms  in dqlabs
        if term_guids:
            self.__delete_terms(purview_term_guids=term_guids)

        # update glossary  in dqlabs
        if glossary_guids:
            self.__delete_glossary(purview_glossary_guids=glossary_guids)
    
    def delete_tags_in_dqlabs(self, asset_id, tags_info:List[Dict[str, Any]]) -> object:
        """
        Deletes tags in DQLabs that are no longer associated with a given asset in Microsoft Purview.

        Parameters:
        - asset_id (str): The unique identifier of the asset.
        - tags_info (List[Dict[str, Any]]): A list of dictionaries containing tag details from Purview, including tag GUIDs.

        Process:
        1. Extracts GUIDs of tags currently associated with the asset in Purview.
        2. Fetches all Purview tags stored in DQLabs from the `core.tags` table.
        3. Logs the tags information for debugging.
        4. Compares the Purview tag GUIDs with those in DQLabs:
            - If a tag exists in DQLabs but is not present in the Purview tag list, it is considered obsolete.
            - Deletes the mapping of such obsolete tags from the `core.tags_mapping` table, ensuring they are no longer linked to the asset.
        5. Logs the removal of each unmapped tag for tracking purposes.

        The function ensures that only outdated tags are removed, maintaining data consistency between Purview and DQLabs.

        Returns:
        - None (Logs messages for debugging and tracking deletion operations).
        """

        dqlabs_purview_tags = []
        purview_tags_guids = [tag.get("guid") for tag in tags_info] # fetch purview tags guids
        # fetch all purview glossary terms in dqlabs
        dqlabs_tags_query = f"""
                            select id from core.tags
                            where source = '{PURVIEW}' 
                            """
        dqlabs_purview_tags_response = self.__run_postgres_query(
                                                    query_string = dqlabs_tags_query,
                                                    query_type='fetchall'
                                                       )
        if dqlabs_purview_tags_response:
            dqlabs_purview_tags = [tag.get("id") for tag in dqlabs_purview_tags_response]

        log_info(("tags_info",tags_info))
        if dqlabs_purview_tags:
            for tag_id in dqlabs_purview_tags:
                if tag_id not in purview_tags_guids:
                    remove_tags_mapping_query = f"""
                                                    delete from core.tags_mapping
                                                    where asset_id  = '{asset_id}' and tags_id = '{tag_id}'
                                                """
                    # delete terms mapping for id
                    self.__run_postgres_query(
                                    query_string = remove_tags_mapping_query,
                                    query_type = 'delete',
                                    output_statement = f"{tag_id} unmapped from tags mapping")

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
                f"Purview Connector - Get Response Error", e)

    def insert_purview_domains_to_dqlabs(
        self, domains_info: List[Dict[str, Any]]
    ) -> object:
        """
        Fetch and insert all domain metrics from Atlan to Dqlabs.

        This function processes the domains and associated categories and terms from Atlan.
        It checks whether the domain already exists in Dqlabs, inserts it if necessary,
        and proceeds to handle the associated categories and terms, inserting them into 
        the relevant tables in Dqlabs.

        Parameters
        ----------
        domains_info : list of dict
            A list of dictionaries containing domain information retrieved from Atlan.
            Each dictionary should contain details like `guid`, `name`, `qualifiedName`, and `categories`.

        Returns
        -------
        object
            The result of the insert operation. This can be a confirmation or error message based 
            on the success or failure of the database insertions.

        Notes
        -----
        - This function will insert domains, categories, and terms into the Dqlabs system.
        - Categories are checked for parent-child relationships, and terms are mapped to their 
        respective categories if present.
        - The function ensures that duplicate entries are not created by checking the existence 
        of each item (domain, category, term) before insertion.
        """
        
        """ Fetch and insert all domain metrics from atlan to dqlabs"""

        log_info(("domain_info", domains_info))
        for domain in domains_info:
            # fetch domains from purview
            domain_id = domain.get("guid")
            log_info(("domain_id", domain_id))
            domain_name = domain.get("name")
            log_info(("domain_name for category", domain_name))
            domain_qualified_name = domain.get("qualifiedName")
            log_info(("domain_qualified_name for debug", domain_qualified_name))
            # check if domain_id in domain table
            domain_check_query = f"""
                                select exists (
                                select 1
                                from core."domain"
                                where id = '{domain_id}'
                                and source = '{PURVIEW}'
                                and is_active = true and is_delete = false
                                );
                            """
            domain_check_flag = self.__run_postgres_query(
                domain_check_query, query_type="fetchone"
            )
            domain_check_flag = domain_check_flag.get("exists", False)
            log_info(("domain_check_flag", domain_check_flag))
            if not domain_check_flag:
                # insert purview domain
                try:
                    with self.connection.cursor() as cursor:
                        # Prepare query input
                        query_input = (
                            domain_id,
                            domain_name,
                            domain_name,
                            None,  # Description
                            str(self.config.get("organization_id")),
                            None,  # Parent ID
                            "domain",
                            0,  # Level
                            None,  # Domain
                            True,  # is_active
                            False,  # is_delete
                            PURVIEW,  # Source
                            json.dumps(
                                {"type": PURVIEW, "purview_domain_id": domain_id},
                                default=str,
                            ),
                        )

                        # Generate placeholders for the query using %s
                        input_literals = ", ".join(["%s"] * len(query_input))
                        query_param = cursor.mogrify(
                            f"({input_literals}, CURRENT_TIMESTAMP)", query_input
                        ).decode("utf-8")

                        # Construct the SQL query string
                        insert_domain_query = f"""
                            INSERT INTO core.domain(
                                id, name, technical_name, description, organization_id, parent_id, type, level, domain, is_active, is_delete, source, properties, created_date
                            )
                            VALUES {query_param}
                            RETURNING id
                        """
                        log_info(("insert_domain_query", insert_domain_query))

                        # run query
                        execute_query(self.connection, cursor, insert_domain_query)
                        log_info(
                            (
                                f"Glossary Domain domain_id: {domain_id}, domain_name: {domain_name} Inserted into Dqlabs"
                            )
                        )
                except Exception as e:
                    log_error("Insert domain query failed", e)

            # Fetch categories from Purview
            categories = domain.get("categories", [])
            log_info(("categories", categories))

            # Split categories into parent and non-parent
            category_no_parent = [
                item for item in categories if "parentCategoryGuid" not in item
            ]
            category_parent = [
                item for item in categories if "parentCategoryGuid" in item
            ]

            # Combine both lists for unified processing
            all_categories = [(category, None) for category in category_no_parent] + [
                (category, category.get("parentCategoryGuid"))
                for category in category_parent
            ]

            log_info(("all_categories for debug", all_categories))

            # Process all categories
            for category, parent_id in all_categories:
                log_info(("category", category))
                log_info(("parent_id", parent_id))
                category_id = category.get("categoryGuid")
                category_name = category.get("displayText")
                log_info(("category_name for debug", category_name))
                parent_id = (
                    parent_id if parent_id else domain_id
                )  # update parent id for category

                # Check if category already exists in the domain table
                category_check_query = f"""
                                        select exists (
                                        select 1
                                        from core."domain"
                                        where id = '{category_id}'
                                        and source = '{PURVIEW}'
                                        and is_active = true
                                        and is_delete = false
                                        );
                                    """
                category_check_flag = self.__run_postgres_query(
                    category_check_query, query_type="fetchone"
                )
                category_check_flag = category_check_flag.get("exists", False)
                log_info(("category_check_flag", category_check_flag))
                path = self.get_full_category_path(category_name, all_categories)
                log_info(("path fro debug", path))
                if path == "":
                    term_technical_name = f"{category_name}({domain_name})"
                else:
                    term_technical_name = f"{category_name}({domain_name}.{path})"
                if not category_check_flag:
                    try:
                        with self.connection.cursor() as cursor:
                            # Prepare category insert data
                            query_input = (
                                category_id,
                                category_name,
                                term_technical_name,  # path - appending the complete hierarchy
                                None,  # Description
                                str(self.config.get("organization_id")),
                                parent_id,  # Parent ID
                                "category",
                                1,  # Level
                                domain_id,
                                True,  # is_active
                                False,  # is_delete
                                PURVIEW,
                                json.dumps(
                                    {"type": "purview", "purview_domain_id": domain_id},
                                    default=str,
                                ),
                            )

                            # Generate placeholders for the query using %s
                            input_literals = ", ".join(["%s"] * len(query_input))
                            query_param = cursor.mogrify(
                                f"({input_literals}, CURRENT_TIMESTAMP)", query_input
                            ).decode("utf-8")

                            # Insert category query
                            insert_category_query = f"""
                                INSERT INTO core.domain(
                                    id, name, technical_name, description, organization_id, parent_id, type, level, domain, is_active, is_delete, source, properties, created_date
                                )
                                VALUES {query_param}
                                RETURNING id;
                            """
                            log_info(("insert_category_query", insert_category_query))

                            # Run query
                            execute_query(
                                self.connection, cursor, insert_category_query
                            )
                            log_info(
                                (
                                    f"Glossary Category category_id: {category_id}, category_name: {category_name} for domain_name: {domain_name}Inserted into Dqlabs"
                                )
                            )

                    except Exception as e:
                        log_error("Insert category query failed", e)
        self.insert_purview_terms_to_dqlabs(domains_info)
        

    def map_asset_metrics(self, asset_info: List[Dict[str, Any]], purview_asset_data, tags_info: List[Dict[str, Any]], map_tags:bool = False) -> object:
        """
        Maps tags from Microsoft Purview to a DQLabs asset by checking for existing tag mappings
        and inserting new ones if they do not already exist.

        Args:
            asset_info (List[Dict[str, Any]]): Information about the asset.
            purview_asset_data (dict): Data from Microsoft Purview containing asset details.
            tags_info (List[Dict[str, Any]]): List of tag information.
            map_tags (bool, optional): Flag to determine whether to map tags. Defaults to False.

        Returns:
            object: Logs of the mapping process.
        """

        asset_id = asset_info.get("asset_id")
        if map_tags:
            # Update the tags for dqlabs asset
            asset_tags_list = purview_asset_data.get('entity', {}).get('labels', [])
            log_info(("asset_tags_list", asset_tags_list))
            if asset_tags_list:
                for tag_mapped in asset_tags_list:
                    asset_tag_id = self.generate_tag_guid(tag_mapped)
                    #check if term id in tags table 
                    attribute_tag_mapped_check_query =f"""
                                        select exists (
                                        select 1
                                        from core."tags_mapping"
                                        where tags_id = '{asset_tag_id}'
                                        and asset_id = '{asset_id}'
                                        );
                                    """
                    attribute_tag_mapped = self.__run_postgres_query(attribute_tag_mapped_check_query,
                                                                query_type='fetchone'
                                                                )
                    attribute_tag_mapped = attribute_tag_mapped.get("exists",False)
                    log_info(("attribute_tag_mapped for debug",attribute_tag_mapped))
                    if not attribute_tag_mapped:
                        #  map purview terms to attributes
                        try:
                            with self.connection.cursor() as cursor:
                                # Prepare attribute-tag mapping insert data
                                query_input = (
                                    str(uuid.uuid4()),  # Unique ID
                                    'asset',  # Level
                                    asset_id,  # Asset ID
                                    None,  # Attribute ID
                                    asset_tag_id,  # Tag ID
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
                            

    def map_attribute_metrics(self, asset_info: List[Dict[str, Any]], purview_asset_data, active_attributes, tags_info: List[Dict[str, Any]], terms_info: List[Dict[str, Any]], map_domains:bool = False, map_tags:bool = False) -> object:
        """
        Maps terms and tags from Microsoft Purview to attributes in a DQLabs asset.

        Ensures correct terms and tags are linked to attributes and removes outdated mappings.

        Args:
            asset_info (List[Dict[str, Any]]): Information about the asset.
            purview_asset_data (dict): Data from Microsoft Purview containing asset details.
            tags_info (List[Dict[str, Any]]): List of tag information.
            terms_info (List[Dict[str, Any]]): List of term information.
            map_domains (bool, optional): Flag to determine whether to map domains. Defaults to False.
            map_tags (bool, optional): Flag to determine whether to map tags. Defaults to False.

        Returns:
            object: Logs of the mapping process.
        """
        asset_id = asset_info.get("asset_id")
        log_info(("purview_asset_data", json.dumps(purview_asset_data, indent=4)))
        log_info(("term_info for debug",terms_info))
        log_info(("tags-info",tags_info))
        log_info(("active_attributes",active_attributes))
        
        """ Map terms and tags to attributes """
        for attribute in active_attributes:
            attribute_guid = attribute.get("guid")
            log_info(("attribute_guid",attribute_guid))
            attribute_name = attribute.get("name")
            log_info(("attribute_name",attribute_name))
            attribute_info = purview_asset_data.get('referredEntities', {}).get(f'{attribute_guid}', {})
            log_info(("attribute_info",json.dumps(attribute_info, indent =4)))
            attribute_term_meanings  = attribute_info['relationshipAttributes']['meanings'] # purview api way of displaying terms
            log_info(("attribute_term_meanings",attribute_term_meanings))
            attribute_id = self.get_attribute_id_from_name(asset_id=asset_id,
                                                                        attribute_name=attribute_name)
            # fetch the terms if mapped to the attribute
            if attribute_term_meanings:
                terms_mapped = list(
                                filter(lambda x: x["entityStatus"] == 'ACTIVE' and x["relationshipStatus"]=='ACTIVE' , 
                                       attribute_term_meanings) 
                                    )
                log_info(("terms_mapped",terms_mapped))
            
                if terms_mapped:
                    terms_mapped = terms_mapped[-1]
                    log_info(("terms_mapped after dedcution",terms_mapped))

                    
                    terms_mapped_guid=terms_mapped.get("guid")
                    for terms in terms_info:
                        if terms.get("termGuid") not in terms_mapped_guid:
                            terms_mapping_retrieve_query=f"""select * from core.terms_mapping where attribute_id = '{attribute_id}' and term_id = '{terms.get("termGuid")}'"""
                            terms_mapping_retrieve = self.__run_postgres_query(terms_mapping_retrieve_query,
                                                                        query_type='fetchall'
                                                                        )
                            log_info(("terms_mapping_retrieve_query for debug",terms_mapping_retrieve_query))
                            if terms_mapping_retrieve:
                                delete_terms_mapping_retrieve_query=f"""delete from core.terms_mapping where attribute_id = '{attribute_id}' and term_id = '{terms.get("termGuid")}'"""
                                delete_terms_mapping_retrieve=self.__run_postgres_query(delete_terms_mapping_retrieve_query,
                                                                        query_type='delete'
                                                                        )
                                log_info(("delete_terms_mapping_retrieve_query",delete_terms_mapping_retrieve_query))
                                log_info(("Deleted mapping for",terms.get('displayText')))
                                log_info(("deleted attribute",attribute_id))


                    attribute_term_id = terms_mapped.get("guid")
                    log_info(("attribute_term_id",attribute_term_id))
                    if map_domains:
                        attribute_id = self.get_attribute_id_from_name(asset_id=asset_id,
                                                                    attribute_name=attribute_name)
                        
                        # insert into terms mapping
                        #check if term id in tags table 

                        attribute_term_mapped_check_query =f"""
                                            select exists (
                                            select 1
                                            from core."terms_mapping"
                                            where term_id = '{attribute_term_id}'
                                            and attribute_id = '{attribute_id}'
                                            );
                                        """
                        attribute_term_mapped = self.__run_postgres_query(attribute_term_mapped_check_query,
                                                                    query_type='fetchone'
                                                                    )
                        attribute_term_mapped = attribute_term_mapped.get("exists",False)
                        if not attribute_term_mapped:
                            #  map purview terms to attributes
                            try:
                                with self.connection.cursor() as cursor:
                                    # Prepare attribute-term mapping insert data
                                    query_input = (
                                        str(uuid.uuid4()),  # Unique ID
                                        'Approved',  # Approval status
                                        asset_id,  # Asset ID
                                        attribute_id,  # Attribute ID
                                        attribute_term_id,  # Term ID
                                    )

                                    # Generate placeholders for the query using %s
                                    input_literals = ", ".join(["%s"] * len(query_input))
                                    query_param = cursor.mogrify(
                                        f"({input_literals}, CURRENT_TIMESTAMP)", query_input
                                    ).decode("utf-8")

                                    # Insert attribute-term mapping query
                                    insert_attribute_term_query = f"""
                                        INSERT INTO core.terms_mapping(
                                            id, approval_status, asset_id, attribute_id, term_id, created_date
                                        )
                                        VALUES {query_param}
                                        RETURNING id;
                                    """
                                    log_info(("insert_attribute_term_query", insert_attribute_term_query))

                                    # Run query
                                    execute_query(
                                        self.connection, cursor, insert_attribute_term_query
                                    )
                            except Exception as e:
                                log_error(
                                    f"Insert Attribute Term Mapping failed", e)
                else:
                    terms_mapping_retrieve_query=f"""select * from core.terms_mapping where attribute_id = '{attribute_id}' AND term_id IN (
                            SELECT id
                            FROM core.terms
                            WHERE source = '{PURVIEW}')"""
                    terms_mapping_retrieve = self.__run_postgres_query(terms_mapping_retrieve_query,
                                                                        query_type='fetchall'
                                                                        )
                    log_info(("terms_mapping_retrieve_query for debug",terms_mapping_retrieve_query))
                    if terms_mapping_retrieve:
                        delete_terms_mapping_retrieve_query=f"""DELETE FROM core.terms_mapping
                        WHERE attribute_id = '{attribute_id}' 
                        AND term_id IN (
                            SELECT id
                            FROM core.terms
                            WHERE source = '{PURVIEW}')"""
                        delete_terms_mapping_retrieve=self.__run_postgres_query(delete_terms_mapping_retrieve_query,
                                                                        query_type='delete'
                                                                        )
                        log_info(("delete_terms_mapping_retrieve_query",delete_terms_mapping_retrieve_query))
                        
                        log_info(("deleting the attribute term_mapping if no term has been mapped",attribute_id))


            if map_tags:
                log_info(("map_tags for debug",map_tags))
                # fetch the tags if mapped to the attribute in purview
                log_info(("attribute_info in map tags",attribute_info ))
                attribute_tags  = attribute_info['labels'] # purview api way of displaying tags
                log_info(("attribute_tag_classifications for debug",attribute_tags))
                if attribute_tags:
                    for attribute_tag_classification in attribute_tags:
                        attribute_tag_id = self.generate_tag_guid(attribute_tag_classification)
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
                            #  map purview terms to attributes
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

    def insert_purview_tags_to_dqlabs(self, tags_info:List[Dict[str, Any]]) -> object:
        """
        Inserts Purview tags into the DQLabs system if they do not already exist.
        
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
                                and source = '{PURVIEW}'
                                and is_active = true and is_delete = false
                                );
                            """
            tag_check_flag = self.__run_postgres_query(tag_check_query,
                                                        query_type='fetchone'
                                                        )
            tag_check_flag = tag_check_flag.get("exists",False)
            log_info(("tag_check_flag",tag_check_flag))
            if not tag_check_flag:
                # insert purview tags
                
                try:
                    with self.connection.cursor() as cursor:
                        # Prepare tags insert data
                        query_input = (
                            tag_id,
                            tag_name,
                            tag_name,
                            None,  # Description
                            "#21598a",  # Color
                            PURVIEW,  # Database name
                            False,  # Native query run
                            json.dumps({"type": PURVIEW}, default=str),  # Properties
                            1,  # Order
                            False,  # is_mask_data
                            True,  # is_active
                            False,  # is_delete
                            str(self.config.get("organization_id")),  # Organization ID
                            PURVIEW,  # Source
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

    def fetch_purview_asset_info(self,asset_guid):
        """
        Fetches asset information from Purview, including associated tags.
        
        Parameters:
        asset_guid (str): The GUID of the asset to fetch from Purview.
        
        Returns:
        tuple: A tuple containing asset data and a list of tags mapped with generated GUIDs.
        """
        tags_info = set()
        tags_list = []
        try:
            host = self.channel.get("url")
            # Construct the URL for the token request
            url = f"/datamap/api/atlas/v2/entity/guid/{asset_guid}"
            endpoint = urljoin(host, url)

            # fetch the attribute information from purview
            asset_data = self.__call_api_request(endpoint, "get")
            
            tags_info = set()
            tags_info.update(asset_data["entity"].get("labels", []))
            for referred_entity in asset_data.get("referredEntities", {}).values():
                tags_info.update(referred_entity.get("labels", []))
            log_info(("tags_info for debug", tags_info))
            tags_list = [{"guid": self.generate_tag_guid(tag), "name": tag} for tag in tags_info]

        except Exception as e:
            log_error("Fetch Purview Glossary API failed", e)
            return []
    
        return asset_data, tags_list

        
    def purview_catalog_update(self):
        try:
            import_config, export_config = self.__purview_channel_configuration()
            log_info(("import_config", import_config))
            log_info(("export_config", export_config))
            pull_domains = import_config.get("domains", False)
            pull_tags = import_config.get("tags", False)

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
            glossary_domain_info = (
                self.fetch_glossary()
            )  # need to add the glossary purview API
            log_info(("glossary_domain_info", glossary_domain_info))
            terms_list = []
            # Iterate through each dictionary in the data
            for item in glossary_domain_info:
                # Check if the 'terms' key is present in the dictionary
                if "terms" in item:
                    # If 'terms' is a list, iterate over each term in the list
                    for term in item["terms"]:
                        if "displayText" in term:
                            # Append the term with its 'termGuid' and 'displayText' to the terms_list
                            terms_list.append(
                                {
                                    "termGuid": term["termGuid"],
                                    "displayText": term["displayText"],
                                }
                            )

            if pull_domains:
                # Delete glossary mapping in dqlabs if deleted in purview
                self.delete_glossary_in_dqlabs(domains_info=glossary_domain_info)

                # Map purview domains to dqlabs
                self.insert_purview_domains_to_dqlabs(domains_info=glossary_domain_info)
                log_info(("Dqlabs Glossary Domain Updated"))
            
            asset_list = self.__asset_information()
            for asset_info in asset_list:
                asset_id = asset_info.get('asset_id')
                asset_guid = self.fetch_guid_for_asset(asset_info)
                purview_asset_data, tags_info = self.fetch_purview_asset_info(asset_guid)
                log_info(("tags_info", tags_info))
                if pull_tags and asset_guid:
                    # Delete glossary mapping in dqlabs if deleted in purview
                    self.delete_tags_in_dqlabs(asset_id, tags_info = tags_info) 

                    # map purview tags to dqlabs
                    self.insert_purview_tags_to_dqlabs(tags_info = tags_info)
                
                if asset_guid:
                    if tags_info:
                        self.map_asset_metrics(asset_info, purview_asset_data, tags_info, pull_tags)

                        active_attributes = self.fetch_entity_details(asset_guid)
                        self.map_attribute_metrics(asset_info, purview_asset_data, active_attributes, tags_info, terms_list, pull_domains, pull_tags)
                    
                    is_created_metadata = self.create_metadata()
                    if asset_flags.get("asset", False):
                        """ Push Asset Custom Metadata from DQLabs to Purview"""
                        try:
                            self.register_asset_metadata(asset_guid, asset_info, asset_flags, is_created_metadata)
                        except Exception as e:
                            log_error(
                                "Push Asset Custom Metadata from DQLabs to Purview Failed ", e
                            )

                    if attribute_flags.get("attribute", False):
                        """ Push Attribute Custom Metadata from DQLabs to Purview"""
                        try:
                            self.register_attribute_metadata(asset_guid, asset_info, attribute_flags, is_created_metadata)
                        except Exception as e:
                            log_error(
                                "Push Attribute Custom Metadata from DQLabs to Purview Failed ",
                                e,
                            )

        except Exception as e:
            log_error("Purview Catalog Update Failed ", e)
