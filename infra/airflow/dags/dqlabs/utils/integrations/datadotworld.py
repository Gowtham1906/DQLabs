import re
import copy
import json
import requests
import pytz
import uuid
from concurrent.futures import ThreadPoolExecutor

from uuid import uuid4
from typing import List, Dict, Any
from urllib.parse import urljoin
import urllib.parse

from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone, split_queries
from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.app_helper.crypto_helper import decrypt
from dqlabs.app_helper.dq_helper import format_freshness, get_client_origin, get_max_workers
from dqlabs.enums.connection_types import ConnectionType
from dqlabs.app_constants.dq_constants import DEFAULT_SEMANTIC_THRESHOLD

# get asset/domain/attribute metrics from alation
from dqlabs.app_helper.catalog_helper import (get_asset_metrics,
                                               get_attributes_metrics)

#datadotworld constant
DATADOTWORLD = "datadotworld"
ATLAN = "atlan"

class Datadotworld:    
    def __init__(self, config, channel):

        """
        Initialize the Atlan instance with configuration and channel data.
        
        This method sets up the Atlan object by accepting a configuration dictionary and a channel dictionary. 
        It also decrypts the API key and establishes a connection to a PostgreSQL database using the provided configuration.

        Parameters
        ----------
        config : dict
            The configuration dictionary containing details for the connection and asset information.
        
        channel : dict
            The channel configuration dictionary containing API keys, import/export settings, etc.

        Attributes
        ----------
        config : dict
            The configuration passed during initialization.
        
        channel : dict
            The channel configuration passed during initialization.

        api_key : str
            The decrypted API key retrieved from the channel configuration.
        
        connection : object
            The PostgreSQL connection object created using the `get_postgres_connection` function.

        Examples
        --------
        >>> config = {"connection": {"type": "postgres"}, "asset": {"properties": {"schema": "public"}}}
        >>> channel = {"api_key": "encrypted_api_key", "import": {"tags": True, "domains": True}, "export": "JSON_STRING"}
        >>> atlan_instance = Atlan(config, channel)
        >>> print(atlan_instance.api_key)  # The decrypted API key.
        >>> print(atlan_instance.connection)  # The PostgreSQL connection object.
        """
        """
        Initialize the Atlan instance with configuration and channel data.
        """
        self.config = config
        log_info(('config for debug',self.config))
        self.channel = channel
        log_info(('channel for debug',self.channel))
        self.api_key = decrypt(self.channel.get("api_key"))
        self.owner = self.channel.get("owner", {}).get("id") if self.channel.get("owner", {}) else ""
        self.connection = get_postgres_connection(self.config)
        self.thresholds = self.channel.get("thresholds")
        self.organization_id = config.get('organization_id')
        self.tag_propagation = config.get('dag_info', {}).get('settings', {}).get('discover', {}).get('tag_propagation',{}).get('is_active', False)
        if not self.organization_id:
            self.organization_id = config.get('dag_info', {}).get('organization_id')

    def datadotworld_catalog_update(self):

        """
        Updates the catalog information in DQLabs based on configurations from Atlan. 
        This function performs multiple tasks including the deletion, insertion, and 
        mapping of glossaries, tags, products, and asset attributes between Atlan and DQLabs.

        The update process is governed by import and export configurations, which 
        control which components (domains, tags, products, etc.) are pulled from Atlan 
        and pushed to DQLabs. The function performs the following:

        1. Fetches glossary, tags, and product information from Atlan.
        2. Deletes entries in DQLabs that are no longer present in Atlan.
        3. Maps and inserts new glossary, tags, and product information into DQLabs.
        4. Maps attributes and metrics to assets.
        5. Creates and pushes custom metadata for assets, attributes, and domains between DQLabs and Atlan.

        Returns
        -------
        None
            The function does not return any value, but it performs various database operations.

        Example
        -------
        # Example usage of atlan_catalog_update method:
        
        # Instantiate the class that contains the atlan_catalog_update method
        atlansync = AtlanCatalogSync()

        # Call the atlan_catalog_update method to update the catalog in DQLabs
        atlansync.atlan_catalog_update()
        
        # Explanation:
        # 1. An instance of the AtlanCatalogSync class is created.
        # 2. The atlan_catalog_update method is called on the instance, 
        #    which will trigger the process of synchronizing data between Atlan and DQLabs.
        #    This includes fetching glossary, tags, and product data, deleting outdated entries, 
        #    and inserting new data into DQLabs.

        Notes
        -----
        - The function handles multiple tasks for synchronizing data between Atlan and DQLabs.
        - It makes use of configurations from Atlan for controlling the pulling and pushing of data.
        - The data synchronization includes the following components:
            - **Glossaries (Domains)**
            - **Tags**
            - **Products**
            - **Asset Attributes**
        - The function performs both deletions and insertions based on whether data exists in Atlan but not in DQLabs.
        - It also pushes custom metadata back to Atlan if specified in the export configuration.
        
        Error Handling:
        ---------------
        - Any errors encountered during the update process are caught and logged, but the function does not raise exceptions.
        """
        try:
            """ Fetch all Datadotworld glossaries and tags and map it in DQLabs"""
            import_config, export_config = self.__datadotworld_channel_configuration()
            log_info(("import_config",import_config))
            log_info(("export_config",export_config))
            
            # pull config
            pull_domains = import_config.get("domains", False)
            pull_tags = import_config.get("tags", False)
            pull_products = import_config.get("products", False)

            # push config
            asset_flags = next((item for item in export_config if "asset" in item), {})
            attribute_flags = next((item for item in export_config if "attribute" in item), {})
            product_flags = next((item for item in export_config if "product" in item), {})
            domain_flags = next((item for item in reversed(export_config) if "domain" in item), {})
            log_info({"asset_flags": asset_flags, "attribute_flags": attribute_flags, "domain_flags": domain_flags, "product_flags": product_flags})   

            datadotworld_terms_info = self.fetch_datadotworld_terms()
            search_results = self.search_resources(payload={"owner": self.owner})
            domains, products = self.get_domains_products(search_results)
            if pull_domains:

                self.delete_datadotworld_terms_in_dqlabs(datadotworld_terms_info = datadotworld_terms_info)

                self.insert_datadotworld_terms_to_dqlabs(datadotworld_terms_info = datadotworld_terms_info)

                self.delete_datadotworld_domains_in_dqlabs(domains = domains)

                if domains:
                    self.insert_datadotworld_domains_to_dqlabs(domains = domains)

            if pull_products:
                # Delete glossary mapping in dqlabs if deleted in atlan
                self.delete_datadotworld_products_in_dqlabs(
                                            products = products
                                            )
                
                if products:
                    self.insert_datadotworld_products_to_dqlabs(products = products)
                
            """ Map all the domains,terms,tags and product to the asset"""
            # Get current asset properties
            asset_information = self.__asset_information()
            max_workers = get_max_workers()
            total_assets = len(asset_information) if asset_information else 0
            max_workers = (
                total_assets
                if total_assets and total_assets < max_workers
                else max_workers
            )
            executor = ThreadPoolExecutor(max_workers=max_workers)
            if executor:
                futures = [
                    executor.submit(
                        self.__datadotworld_metadata_push,
                        asset_data,
                        datadotworld_terms_info,
                        pull_tags,
                        pull_domains,
                        pull_products,
                        asset_flags,
                        attribute_flags,
                        domain_flags,
                        product_flags,
                    )
                    for asset_data in asset_information
                ]
                executor.shutdown()
                for future in futures:
                    try:
                        future.result()
                    except Exception as e:
                        if("server responded with a permission error" in str(e).lower()):
                            log_info(("error in exception under metadata push", e))
                            raise e
                        log_error("Exception from thread in __atlan_metadata_push", e)
                        continue
        except Exception as e:
            log_error(f"Atlan Catalog Update Failed ", e)
            raise e
    
    def __asset_information(self) -> Dict[str, Any]:

        """
        Fetch DQ Asset details based on the configuration.

        This method retrieves asset-related information such as asset ID, asset name, schema, 
        and the connection type from the configuration dictionary.

        Returns
        -------
        dict
            A dictionary containing the asset details like `asset_id`, `asset_name`, `asset_schema`, 
            `asset_database`, `connection_type`, and `asset_properties`.

        Examples
        --------
        >>> obj = YourClass()
        >>> asset_info = obj.__asset_information()
        >>> print(asset_info)  # Prints the asset information as a dictionary.
        """
        
        # Fetch DQ Asset Details
        asset = self.config.get("asset", {})
        asset_info = []
        if asset.get("properties"):
            connection = self.config.get("connection", {})
            connection_type = connection.get("type", "")

            asset_properties = asset.get("properties", {})
            database_name = self.config.get("database_name")

            if connection_type == ConnectionType.BigQuery.value:
                credentials = connection.get('credentials')
                decrypted_keyjson = decrypt(credentials.get('keyjson'))
                filtered_key = decrypted_keyjson.replace("\r", "").replace("\n", "")
                filtered_keyjson= json.loads(filtered_key)
                database_name = filtered_keyjson.get("project_id")
            elif connection_type == ConnectionType.Athena.value:
                if database_name.lower() == 'awsdatacatalog':
                    database_name = "AwsDataCatalog"
            elif connection_type == ConnectionType.Oracle.value:
                database_name = database_name.lower()
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
            return assets
        except Exception as e:
            log_error(
                f"Datadotworld Connector - Get Assets Error", e)

    def fetch_datadotworld_terms(self):
        try:
            host = self.channel.get("host")
            if not host:
                raise ValueError("Host is missing in channel configuration.")
            url = f"metadata/glossary/{self.owner}?size=1000&from=0"
            # Construct the full endpoint
            # endpoint = urljoin(host, url)

            # Fetch the tags information from Atlan
            terms_info = self.__call_api_request(host, url, 'get')
            terms_info = terms_info.get('records', []) if terms_info else []
            
            return terms_info
        except Exception as e:
            log_error("Fetch Atlan Tags API failed", e)
            raise e
    
    def search_resources(self, payload: Dict[str, Any]):
        try:
            host = self.channel.get("host")
            if not host:
                raise ValueError("Host is missing in channel configuration.")
            url = f"metadata/resources/search?size=1000&from=0&hydration=catalog"
            # Fetch all paginated data
            all_records = self.__fetch_all_paginated_data(host, url, payload)
            print("Iinside search_resources - total records:", len(all_records))
            return all_records
        except Exception as e:
            log_error("Fetch Datadotworld Domains Failed", e)
            raise e
    
    def __fetch_all_paginated_data(self, host: str, initial_url: str, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Fetches all data from a paginated API endpoint by following the 'next' links.
        
        Args:
            host (str): The base host URL.
            initial_url (str): The initial API endpoint URL.
            payload (dict): The payload to send with POST requests.
        
        Returns:
            List[Dict[str, Any]]: A list of all records from all pages.
        """
        all_records = []
        current_url = initial_url
        
        while True:
            try:
                if 'from=10000' in current_url:
                    break
                response = self.__call_api_request(host, current_url, 'post', payload)
                
                if not response:
                    break
                
                # Extract records from current page
                records = response.get('records', [])
                if records:
                    all_records.extend(records)
                    log_info((f"Fetched {len(records)} records. Total so far: {len(all_records)}"))
                
                # Check if there's a next page
                next_url = response.get('next')
                if not next_url:
                    break
                
                # Update current_url to the next page URL
                # The next_url is already a relative path like "metadata/resources/search?size=10&from=10&hydration=catalog"
                current_url = next_url
                
            except Exception as e:
                # Check if this is the 10,000 item limit error
                is_limit_error = False
                error_str = str(e).lower()
                error_repr = repr(e).lower()
                
                # First, try to get the response object from the exception
                response_obj = None
                if isinstance(e, requests.exceptions.HTTPError):
                    response_obj = getattr(e, 'response', None)
                elif isinstance(e, ValueError) and e.args:
                    # Check if ValueError wraps an HTTPError
                    for arg in e.args:
                        if isinstance(arg, requests.exceptions.HTTPError):
                            response_obj = getattr(arg, 'response', None)
                            break
                        elif isinstance(arg, requests.Response):
                            response_obj = arg
                            break
                
                # Check the response JSON for the error message
                if response_obj is not None:
                    try:
                        error_response = response_obj.json()
                        error_message = error_response.get('message', '').lower()
                        if 'cannot request past 10000 items' in error_message or '10000 items' in error_message:
                            is_limit_error = True
                    except (ValueError, AttributeError, KeyError, json.JSONDecodeError):
                        pass
                
                # Also check exception message for limit error (fallback)
                if not is_limit_error:
                    is_limit_error = (
                        "cannot request past 10000 items" in error_str or 
                        "10000 items" in error_str or
                        "cannot request past 10000 items" in error_repr or
                        "10000 items" in error_repr or
                        "past 10000" in error_str or
                        "past 10000" in error_repr
                    )
                
                if is_limit_error:
                    # Log as info and continue (don't raise exception, just break to return records)
                    log_info((f"Reached API limit of 10,000 items. Returning {len(all_records)} records fetched so far."))
                    break
                else:
                    # For other exceptions, log as error and break (but don't raise)
                    log_error(f"Error fetching paginated data at URL: {current_url}", e)
                    break
        
        log_info((f"Total records fetched: {len(all_records)}"))
        return all_records

    def get_measures_data(self, id, entity_type='asset', asset_id=None, export_options={}):
        measures = []
        try:
            """
            Get asset and attribute measures by domain id
            """
            if not id:
                return measures

            custom_rule = export_options.get("custom", False)
            semantic_rule = export_options.get("semantics", False)
            auto_rule = export_options.get("auto", False)
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
                    filter_query.append("statistics")
                if not filter_query:
                    return measures
                else:
                    filter_query = [f"""'{str(i)}'""" for i in filter_query]
                    filter_query = f"""and base.type in ({",".join(filter_query)}) """

            _condition = f" base.level='{entity_type}' and mes.asset_id = '{id}' "
            if entity_type == 'attribute':
                _condition = f" base.level in ('{entity_type}', 'term') and mes.attribute_id='{id}' and mes.asset_id ='{asset_id}' "

            connection = get_postgres_connection(self.config)
            with connection.cursor() as cursor:
                # get the domain by name
                query_string = f"""
                    select distinct on (mes.id) mes.id, base.name,mes.status, base.level as level, base.technical_name, 
                        base.description, base.is_default, base.type, base.category, dim.name as dimension_name,
                        mes.is_aggregation_query, base.query, mes.is_active, mes.is_drift_enabled, mes.allow_score, 
                        mes.is_positive, mes.score, 
                        mes.last_runs, data.primary_columns, mes.enable_pass_criteria,
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
                    where mes.is_active=true  {filter_query} and mes.allow_score=true and mes.score is not null
                    and {_condition}
                    group by mes.id, base.id, data.id, dim.name,connection.id,attribute.id,ast.id                
                """
                cursor = execute_query(connection, cursor, query_string)
                measures = fetchall(cursor)
                measures = measures if measures else []
        except Exception as e:
            log_error(
                f"Coalesce Connector: Get Measures data ", str(e))
        finally:
            return measures

    def delete_datadotworld_terms_in_dqlabs(self, datadotworld_terms_info):
        try:
            datadotworld_terms_info = [term['id'].split('businessTerm-')[-1] for term in datadotworld_terms_info]
            dqlabs_datadotworld_terms = []
            # fetch all atlan glossary terms in dqlabs
            dqlabs_datadotworld_terms_query = f"""
                                select id from core.terms
                                where source = '{DATADOTWORLD}' 
                                """
            dqlabs_datadotworld_terms_response = self.__run_postgres_query(
                                                        query_string = dqlabs_datadotworld_terms_query,
                                                        query_type='fetchall'
                                                        )
            log_info(("dqlabs_datadotworld_terms_response",dqlabs_datadotworld_terms_response))
            if dqlabs_datadotworld_terms_response:
                dqlabs_datadotworld_terms = [term.get("id") for term in dqlabs_datadotworld_terms_response]
            
            if dqlabs_datadotworld_terms:
                term_to_be_deleted = [term_id for term_id in dqlabs_datadotworld_terms if term_id not in datadotworld_terms_info]
                if term_to_be_deleted:
                    term_ids_to_be_deleted = "', '".join(term_to_be_deleted)
                    remove_terms_mapping_query = f"""
                                                    delete from core.terms_mapping
                                                    where term_id in ('{term_ids_to_be_deleted}')
                                                """
                    # delete terms mapping for id
                    self.__run_postgres_query(
                                    query_string = remove_terms_mapping_query,
                                    query_type = 'delete',
                                    output_statement = f"{term_ids_to_be_deleted} unmapped from terms mapping")
                    
                    # delete terms version history
                    remove_terms_version_history = f"""
                                                    delete from core.version_history
                                                    where term_id in ('{term_ids_to_be_deleted}')
                                                """
                    self.__run_postgres_query(
                                    query_string = remove_terms_version_history,
                                    query_type = 'delete',
                                    output_statement = f"{term_ids_to_be_deleted} deleted from version_history")
                    
                    # delete the term
                    remove_terms_id = f"""
                                        delete from core.terms
                                        where id in ('{term_ids_to_be_deleted}')
                                    """
                    self.__run_postgres_query(
                                    query_string = remove_terms_id,
                                    query_type = 'delete',
                                    output_statement = f"{term_ids_to_be_deleted} deleted from terms")

        except Exception as e:
            log_error("Delete Datadotworld Terms in DQLabs Failed", e)
            raise e
        
    def delete_datadotworld_products_in_dqlabs(self, products):
        products = [product['guid'] for product in products]
        dqlabs_datadotworld_products = []
       
        # fetch all atlan products in dqlabs
        dqlabs_data_product_query = f"""
                            select id from core.product
                            where source = '{DATADOTWORLD}' 
                            and type in ('product')
                            """
        dqlabs_data_productquery_response = self.__run_postgres_query(
                                                    query_string = dqlabs_data_product_query,
                                                    query_type='fetchall'
                                                       )

        log_info(("dqlabs_data_product_query_response",dqlabs_data_productquery_response))
        if dqlabs_data_productquery_response:
            dqlabs_datadotworld_products= [data_product_domain.get("id") for data_product_domain in dqlabs_data_productquery_response]
      
        log_info(("dqlabs_datadotworld_products for debug",dqlabs_datadotworld_products))
        log_info(("Extended dqlabs_datadotworld_products for debug",dqlabs_datadotworld_products))
        log_info(("Extended datadotworld_products for debug",products))

        data_products= [data_product 
                    for data_product in dqlabs_datadotworld_products 
                    if data_product not in products
                    ]
        if data_products:
            data_product_ids = "', '".join(data_products)
            fetching_subproducts_delete_mapping_query = f"""delete from core.product_mapping where product_id in ('{data_product_ids}')"""
            fetch_subproducts_delete_mapping_response = self.__run_postgres_query(
                            query_string= fetching_subproducts_delete_mapping_query,
                            query_type = 'delete'
                            )
            log_info((f'Deleting sub product mappingg id of {data_products}'))
            deleting_sub_product_query = f""" 
                                            delete from core.product 
                                            WHERE (id in ('{data_product_ids}') or parent_id in ('{data_product_ids}'))
                                            and type in ('category', 'product')
                                            AND id NOT IN (SELECT product_id FROM user_mapping WHERE product_id IS NOT NULL)
                                            """
            fetch_deleting_sub_product_query = self.__run_postgres_query(
                                            query_string= deleting_sub_product_query,
                                                query_type = 'delete'
                                                )
            fetching_subproducts_mapping_delete_query = f"""delete from core.product 
                                                            WHERE id in ('{data_product_ids}')
                                                            AND id NOT IN (SELECT product_id FROM user_mapping WHERE product_id IS NOT NULL)
                                                            """
            fetch_subproducts_delete_response = self.__run_postgres_query(
                                            query_string= fetching_subproducts_mapping_delete_query,
                                                query_type = 'delete'
                                                )
            log_info((f'Deleting sub product id of {data_product_ids}'))


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

    def delete_datadotworld_domains_in_dqlabs(self, domains):
        datadotworld_domain_guids = [domain['guid'] for domain in domains]
        dqlabs_datadotworld_domains = []
        # fetch all datadotworld domains in dqlabs
        dqlabs_domain_query = f"""
                            select id from core.domain
                            where source = '{DATADOTWORLD}' 
                            and type in ('domain', 'category')
                            """
        dqlabs_domain_query_response = self.__run_postgres_query(
                                                    query_string = dqlabs_domain_query,
                                                    query_type='fetchall'
                                                       )
        log_info(("dqlabs_glossary_query_response",dqlabs_domain_query_response))
        if dqlabs_domain_query_response:
            dqlabs_datadotworld_domains= [domain.get("id") for domain in dqlabs_domain_query_response]
        log_info(("dqlabs_datadotworld_domains for debug",dqlabs_datadotworld_domains))
        if dqlabs_datadotworld_domains:
            domain_ids = [domain_id for domain_id in dqlabs_datadotworld_domains if domain_id not in datadotworld_domain_guids]
            log_info(("domain_ids to be deleted for debug",domain_ids))
            if domain_ids:
                domain_ids_list = "', '".join(domain_ids)
                remove_domain_mapping_query = f"""
                                                delete from core.domain_mapping
                                                where domain_id in  ('{domain_ids_list}')
                                            """
                # delete category mapping for id
                self.__run_postgres_query(
                                query_string = remove_domain_mapping_query,
                                query_type = 'delete',
                                output_statement = f"{domain_ids_list} unmapped from categories mapping")

                remove_domain_id = f"""
                                    delete from core.domain
                                    where (id in  ('{domain_ids_list}') or parent_id in ('{domain_ids_list}'))
                                    and type in ('category', 'domain')
                                    AND id NOT IN (SELECT domain_id FROM user_mapping WHERE domain_id IS NOT NULL)
                                """
                log_info(("remove_category_id for debug",remove_domain_id))
                # delete the term
                self.__run_postgres_query(
                                query_string = remove_domain_id,
                                query_type = 'delete',
                                output_statement = f"{domain_ids_list} deleted from categories")

    
    def get_domains_products(self, search_results):
        try:
            domains = []
            products = []
            for result in search_results:
                if result.get("resourceDetails", {}).get("typeDetails",{}).get("label") == "Domain Collection":
                    result['guid'] = self.generate_tag_guid(result.get('title'))
                    domains.append(result)
                elif result.get("resourceDetails", {}).get("typeDetails",{}).get("label") == "Data Product":
                    result['guid'] = self.generate_tag_guid(result.get('title'))
                    products.append(result)
            return domains, products
        except Exception as e:
            log_error("Get Datadotworld Domains and Products Failed", e)

    def insert_datadotworld_domains_to_dqlabs(self, domains):
        # domains = list(filter(lambda x: x["attributes"]["qualifiedName"].endswith("/super") and x["status"] == 'ACTIVE', domains_info))
        log_info(("domains in atlan",domains))
        domain_ids = [str(domain['guid']) for domain in domains]
        print("Iinside insert_datadotworld_domains_to_dqlabs domain_ids",domain_ids)
        domain_list = "', '".join(domain_ids)
        domain_check_query = f"""
                            SELECT id FROM core.domain
                            WHERE id IN ('{domain_list}')
                            AND source = '{DATADOTWORLD}' AND is_active = true AND is_delete = false;
                        """
        existing_domains = self.__run_postgres_query(domain_check_query, query_type='fetchall')
        existing_domain_ids = {item["id"] for item in existing_domains}

        new_domain_values = []
        for  domain in domains:
            if domain['guid'] not in existing_domain_ids:
                new_domain_values.append((
                    domain.get('guid'),
                    domain.get('title'),
                    domain.get('title'),
                    None,
                    None,
                    "domain",
                    0,
                    json.dumps({"type": DATADOTWORLD}),
                    DATADOTWORLD,
                    True,
                    False,
                    str(self.organization_id),
                    None
                ))
        if new_domain_values: 
            try:
                with self.connection.cursor() as cursor:
                    placeholders = ", ".join(["%s"] * len(new_domain_values[0]))
                    query_param = b",".join([
                        cursor.mogrify(f"({placeholders}, CURRENT_TIMESTAMP)", vals)
                        for vals in new_domain_values
                    ]).decode("utf-8")

                    # Insert product query
                    insert_domain_query = f"""
                        INSERT INTO core.domain(
                            id, name, technical_name, description, domain, type, level, properties,
                            source, is_active, is_delete, organization_id, parent_id, created_date
                        )
                        VALUES {query_param}
                        RETURNING id;
                    """
                    log_info(("insert_domain_query", insert_domain_query))
                    execute_query(self.connection, cursor, insert_domain_query)
            except Exception as e:
                log_error("Bulk insert products failed", e)
                raise e

    def insert_datadotworld_products_to_dqlabs(self, products):
        log_info(("products of datadotworld",products))
        product_ids = [ str(product['guid']) for product in products]
        product_list = "', '".join(product_ids)
        existing_products = []
        if product_ids:
            product_check_query = f"""
                SELECT id FROM core.product
                WHERE id IN ('{product_list}')
                AND source = '{DATADOTWORLD}' AND is_active = true AND is_delete = false;
            """
            existing_products = self.__run_postgres_query(product_check_query, query_type='fetchall')

        existing_product_ids = {item["id"] for item in existing_products}
        new_product_values = []
        log_info(("products in get products",products))
        for product in products :
            if product.get('guid') not in existing_product_ids:
                new_product_values.append((
                    product.get('guid'),
                    product.get('title'),
                    product.get('title'),
                    None,
                    "product",
                    1,
                    json.dumps({"type": DATADOTWORLD}),
                    DATADOTWORLD,
                    True,
                    False,
                    str(self.organization_id)
                ))

        if new_product_values:
            try:
                with self.connection.cursor() as cursor:
                    placeholders = ", ".join(["%s"] * len(new_product_values[0]))
                    query_param = b",".join([
                        cursor.mogrify(f"({placeholders}, CURRENT_TIMESTAMP)", vals)
                        for vals in new_product_values
                    ]).decode("utf-8")

                    insert_sub_product_query = f"""
                        INSERT INTO core.product(
                            id, name, technical_name, description, type, level, properties,
                            source, is_active, is_delete, organization_id, created_date
                        )
                        VALUES {query_param}
                        RETURNING id;
                    """
                    log_info(("insert_sub_product_query", insert_sub_product_query))
                    execute_query(self.connection, cursor, insert_sub_product_query)
            except Exception as e:
                log_error("Bulk insert sub-products failed", e)
                raise e
    def insert_datadotworld_terms_to_dqlabs(self, datadotworld_terms_info):
        try:
            term_insert_values = []
            if not datadotworld_terms_info:
                return

            term_ids = [term['id'].split('businessTerm-')[-1] for term in datadotworld_terms_info]
            term_check_query = f"""
                SELECT id FROM core.terms
                WHERE id IN ('{"', '".join(term_ids)}')
                AND source = '{DATADOTWORLD}' AND is_active = true AND is_delete = false;
            """
            existing_terms = self.__run_postgres_query(term_check_query, query_type="fetchall")
            existing_term_ids = {item["id"] for item in existing_terms}

            for term in datadotworld_terms_info:
                term_id = term['id'].split('businessTerm-')[-1]
                if term_id in existing_term_ids:
                    continue

                term_insert_values.append((
                    term_id,
                    term.get("title"),
                    term.get("title"),
                    None,
                    str(self.organization_id),
                    True,
                    False,
                    DATADOTWORLD,
                    "Pending",
                    "Text",
                    DEFAULT_SEMANTIC_THRESHOLD,
                    1,
                    False,
                    False,
                    False,
                    False,
                    json.dumps([]),
                    json.dumps([]),
                    json.dumps([]),
                    json.dumps([])
                ))

            if term_insert_values:
                with self.connection.cursor() as cursor:
                    placeholders = ", ".join(["%s"] * len(term_insert_values[0]))
                    query_param = b",".join([
                        cursor.mogrify(f"({placeholders}, CURRENT_TIMESTAMP)", vals)
                        for vals in term_insert_values
                    ]).decode("utf-8")
                    insert_terms_query = f"""
                        INSERT INTO core.terms (
                            id, name, technical_name, description, organization_id,
                            is_active, is_delete, source, status, derived_type, threshold, sensitivity,
                            is_null, is_blank, is_unique, is_primary_key,
                            tags, enum, contains, synonyms, created_date
                        )
                        VALUES {query_param}
                        RETURNING id;
                    """
                    log_info(("insert_terms_query", insert_terms_query))
                    execute_query(self.connection, cursor, insert_terms_query)
        except Exception as e:
            log_error("Insert Datadotworld Terms to DQLabs Failed", e)
            raise e
    
    def __datadotworld_channel_configuration(self) -> Dict[str, Any]:

        """
        Fetch the Atlan Channel Configuration for import and export settings.

        This function retrieves the Atlan channel configuration and extracts the import 
        and export settings from it. It logs the channel configuration and returns 
        both import and export configurations as dictionaries.

        Breakdown of the Atlan Configuration:
        -------------------------------------
        1. **`host`**:
        - The base URL for Atlan: `'https://colpal-sandbox.atlan.com'`.

        2. **`dq_url`**:
        - The Data Quality (DQ) URL, pointing to a local development server: `'http://localhost:3000'`.

        3. **`export`**:
        - A JSON string describing the configuration for exporting data, with settings for:
            - `asset`: Exporting asset details including summaries, measures, alerts, issues, etc.
            - `attribute`: Exporting attribute details such as summaries, measures, alerts, etc.
            - `domain`: Exporting domain details including summaries, measures, alerts, etc.
        - Each category has multiple options such as `summary`, `measures`, `alerts`, `issues`, and others, depending on the export needs.

        4. **`import`**:
        - A dictionary with the import settings. Currently, it enables the import of `tags` and `domains` as `True`.

        5. **`api_key`**:
        - The API key used for authentication with Atlan (not logged for security reasons).

        6. **`mapping`**:
        - A JSON string defining the mapping between Data Quality (DQ) columns and Atlan columns, with an `isNew` flag indicating whether the mapping is new.

        7. **`is_valid`**:
        - A boolean indicating whether the configuration is valid: `True`.

        8. **`datasource`**:
        - A JSON array containing the list of data sources. Each entry represents a specific data source like Synapse, BigQuery, Snowflake, etc., with their unique identifiers.

        Returns
        -------
        Tuple[Dict[str, Any], Dict[str, Any]]
            A tuple containing two dictionaries:
            - The first dictionary represents the import configuration (from `self.channel['import']`).
            - The second dictionary represents the export configuration (parsed from `self.channel['export']`).

        Examples
        --------
        >>> obj = YourClass()
        >>> import_config, export_config = obj.__atlan_channel_configuration()
        >>> print(import_config)  # Prints the import configuration as a dictionary.
        >>> print(export_config)  # Prints the export configuration as a dictionary.
        
        Notes
        -----
        - This function assumes that `self.channel` is a valid configuration object containing keys 'import' and 'export'.
        - The export configuration is expected to be a JSON string that is parsed into a dictionary.
        - The function logs the entire channel configuration for debugging purposes.
        """
        # Fetch Atlan Channel Configuration
        log_info(("atlan_configuration",self.channel))
        import_config = self.channel.get("import", {})
        export_config = json.loads(self.channel.get("export"))
        return import_config, export_config

    def get_asset_data(self, source, schema, name):
        try:
            host = self.channel.get("host")
            if not host:
                raise ValueError("Host is missing in channel configuration.")
            url = f"metadata/data/sources/{self.owner}/{source}/tables/{urllib.parse.quote(name, safe='')}/?schema={schema}"
            asset_data = self.__call_api_request(host, url, 'get')
            return asset_data
        except Exception as e:
            log_error("Get Asset Data Failed", e)
            raise e
    
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
        datadotworld_tags_guids = [tag.get("guid") for tag in tags_info] # fetch datadotworld tags guids
        # fetch all purview glossary terms in dqlabs
        dqlabs_tags_query = f"""
                            select id from core.tags
                            where source = '{DATADOTWORLD}' 
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
                if tag_id not in datadotworld_tags_guids:
                    remove_tags_mapping_query = f"""
                                                    delete from core.tags_mapping
                                                    where asset_id  = '{asset_id}' and tags_id = '{tag_id}'
                                                """
                                        
                    # delete terms mapping for id
                    self.__run_postgres_query(
                                    query_string = remove_tags_mapping_query,
                                    query_type = 'delete',
                                    output_statement = f"{tag_id} unmapped from tags mapping")


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
                                and source = '{DATADOTWORLD}'
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
                            DATADOTWORLD,  # Database name
                            False,  # Native query run
                            json.dumps({"type": DATADOTWORLD}, default=str),  # Properties
                            1,  # Order
                            False,  # is_mask_data
                            True,  # is_active
                            False,  # is_delete
                            str(self.organization_id),  # Organization ID
                            DATADOTWORLD,  # Source
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
    
    def fetch_entity_details(self, asset_database, asset_schema, asset_name):
        try:
            host = self.channel.get("host")
            if not host:
                raise ValueError("Host is missing in channel configuration.")
            url = f"metadata/data/sources/{self.owner}/{asset_database}/tables/{urllib.parse.quote(asset_name, safe='')}/columns?size=1000&from=0&schema={asset_schema}"
            entity_details = self.__call_api_request(host, url, 'get')
            entity_details = entity_details['records']
            return entity_details
        except Exception as e:
            log_error("Fetch Datadotworld Entity Details Failed", e)
            raise e
    
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
            log_info(("query_string of attribute",query_string))
            attributes = self.__run_postgres_query(query_string,
                                                query_type='fetchone'
                                                )
            attribute_id = attributes.get("attribute_id") if attributes else None
            return attribute_id
        except Exception as e:
            log_error(
                f"Datadotworld Connector - Get Response Error", e)

    def map_attribute_metrics(self, asset_data: Dict[str, Any], active_attributes: List[Dict[str, Any]], pull_tags: bool):
        asset_id = asset_data.get("asset_id")
        for attribute in active_attributes:
            attribute_name = attribute.get("title")
            attribute_id = self.get_attribute_id_from_name(asset_id=asset_id,attribute_name=attribute_name)
            if pull_tags:

                log_info(("map_tags for debug",pull_tags))
                # fetch the tags if mapped to the attribute in purview
                log_info(("attribute_info in map tags",attribute ))
                attribute_tags  = attribute['tags']  if 'tags' in attribute else []# purview api way of displaying tags
                attribute_tags = [{"guid": str(self.generate_tag_guid(tag)), "name": tag} for tag in attribute_tags]
                log_info(("attribute_tag_classifications for debug",attribute_tags))
                if attribute_tags and attribute_id:
                    for attribute_tag in attribute_tags:
                        tag_id = attribute_tag.get("guid")
                        tag_name = attribute_tag.get("name")
                        
                        # First, ensure the tag exists in tags table
                        tag_check_query = f"""
                            select exists (
                                select 1
                                from core."tags"
                                where id = '{tag_id}'
                                and source = '{DATADOTWORLD}'
                                and is_active = true and is_delete = false
                            );
                        """
                        tag_exists = self.__run_postgres_query(tag_check_query, query_type='fetchone')
                        tag_exists = tag_exists.get("exists", False) if tag_exists else False
                        
                        # If tag doesn't exist, insert it first
                        if not tag_exists:
                            try:
                                with self.connection.cursor() as cursor:
                                    query_input = (
                                        tag_id,
                                        tag_name,
                                        tag_name,
                                        None,  # Description
                                        "#21598a",  # Color
                                        DATADOTWORLD,  # Database name
                                        False,  # Native query run
                                        json.dumps({"type": DATADOTWORLD}, default=str),  # Properties
                                        1,  # Order
                                        False,  # is_mask_data
                                        True,  # is_active
                                        False,  # is_delete
                                        str(self.organization_id),  # Organization ID
                                        DATADOTWORLD,  # Source
                                    )
                                    
                                    input_literals = ", ".join(["%s"] * len(query_input))
                                    query_param = cursor.mogrify(
                                        f"({input_literals}, CURRENT_TIMESTAMP)", query_input
                                    ).decode("utf-8")
                                    
                                    insert_tag_query = f"""
                                        INSERT INTO core.tags(
                                            id, name, technical_name, description, color, db_name, native_query_run, properties,
                                            "order", is_mask_data, is_active, is_delete, organization_id, source, created_date
                                        )
                                        VALUES {query_param}
                                        RETURNING id;
                                    """
                                    execute_query(self.connection, cursor, insert_tag_query)
                                    log_info((f"Tag inserted: {tag_id}, {tag_name}"))
                            except Exception as e:
                                log_error(f"Failed to insert tag before mapping: {tag_id}", e)
                                continue  # Skip mapping if tag insertion failed
                        
                        # Check if tag is already mapped to this attribute
                        attribute_tag_mapped_check_query = f"""
                            select exists (
                                select 1
                                from core."tags_mapping"
                                where tags_id = '{tag_id}'
                                and attribute_id = '{attribute_id}'
                            );
                        """
                        attribute_tag_mapped = self.__run_postgres_query(attribute_tag_mapped_check_query, query_type='fetchone')
                        attribute_tag_mapped = attribute_tag_mapped.get("exists", False) if attribute_tag_mapped else False
                        
                        log_info(("attribute_tag_mapped for debug", attribute_tag_mapped))
                        if not attribute_tag_mapped:
                            # Map tag to attribute
                            try:
                                with self.connection.cursor() as cursor:
                                    # Prepare attribute-tag mapping insert data
                                    query_input = (
                                        str(uuid.uuid4()),  # Unique ID
                                        'attribute',  # Level
                                        asset_id,  # Asset ID
                                        attribute_id,  # Attribute ID
                                        tag_id,  # Tag ID
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

    def get_entity_score(self, id: str, entity_type: str):
        with self.connection.cursor() as cursor:
            query_string = f"""
                select score from core.{entity_type} where id = '{id}'
            """
            cursor = execute_query(self.connection, cursor, query_string)
            asset_score = fetchone(cursor)
            asset_score = asset_score.get("score", 0) if asset_score else 0  
            return asset_score if asset_score else 0

    def get_badge_iri(self, asset_score: int):
        for threshold in self.thresholds:
            if asset_score >= threshold.get("from") and asset_score <= threshold.get("to") and threshold.get("type") == "poor":
                return "https://dwec.data.world/v0/default-profile/PoorBadge"
            elif asset_score >= threshold.get("from") and asset_score <= threshold.get("to") and threshold.get("type") == "moderate":
                return "https://dwec.data.world/v0/default-profile/ModerateBadge"
            elif asset_score >= threshold.get("from") and asset_score <= threshold.get("to") and threshold.get("type") == "good":
                return "https://dwec.data.world/v0/default-profile/GoodBadge"

    
    def push_data_badge(self, id: str, resource_iri: str, entity_type: str):
        entity_score = self.get_entity_score(id, entity_type)
        badge_iri = self.get_badge_iri(entity_score)
        payload = { 
                    "badges": [
                        {
                            "resource": {
                                "type": "IRI",
                                "iri": resource_iri
                            },
                            "badgeIri": badge_iri
                        }
                    ] 
                }
        host = self.channel.get("host")
        if not host:
            raise ValueError("Host is missing in channel configuration.")
        url = f"dataquality/badges/{self.owner}"
        self.__call_api_request(host, url, 'post', payload)

    def get_check_run_status(self, result, score, enable_pass_criteria):
            print("result for check run status", result)
            if not enable_pass_criteria and score == 100:
                return "PASS"
            if not enable_pass_criteria and score != 100:
                return "FAIL"
            if enable_pass_criteria:
                if result.lower() == "pass":
                    return "PASS"
                elif result.lower() == "fail":
                    return "FAIL"

    def push_check_runs(self, id: str, entity_type: str, resource_iri: str, collection_iri: str, export_options: Dict[str, Any], asset_id: str = None):
        measures = self.get_measures_data(id, entity_type, asset_id, export_options=export_options)
        check_runs = []
        for measure in measures:
            url = urljoin(self.channel.get("dq_url"), f"measure/{measure.get('id')}/detail?date_filter=All")
            score = measure.get("score")
            if score is not None and not score == 'None':
                score = round(float(score), 2)
            check_runs.append({
                "config": {
                    "resource": {
                        "type": "IRI",
                        "iri": resource_iri
                    },
                    "weight": 1,
                    "description": measure.get("description"),
                    "id": measure.get("id"),
                    "lastUpdated": "2025-11-25T05:48:28.089123Z",
                    "source": "DQLabs",
                    "title": f"{measure.get('name')} : {score}",
                    "query": measure.get("query") if measure.get("query") else "Select 1",
                    "dimension": measure.get("dimension_name"),
                    "url": url
                },
                "result": self.get_check_run_status(measure.get("result"), measure.get("score"), measure.get("enable_pass_criteria")),
                "runSuccessful": "True",
                "end": "2025-11-25T05:48:28.089123Z",
                "start": "2025-11-25T05:48:28.089123Z",
                "score": score if score else None
            })
        if check_runs:
            payload = {
                "checkRuns": check_runs
            }
            host = self.channel.get("host")
            if not host:
                raise ValueError("Host is missing in channel configuration.")
            url = f"dataquality/checkruns/{self.owner}?collectionIri={collection_iri}"
            self.__call_api_request(host, url, 'post', payload)

    def __datadotworld_metadata_push(self, asset_data: Dict[str, Any], terms_list: List[Dict[str, Any]], pull_tags: bool, pull_domains: bool, pull_products: bool, asset_flags: Dict[str, Any], attribute_flags: Dict[str, Any], domain_flags: Dict[str, Any], product_flags: Dict[str, Any]):
        asset_id = asset_data.get("asset_id")
        # connection_type = asset_data.get("connection_type")
        asset_schema = asset_data.get("asset_schema")
        asset_name = asset_data.get("asset_name")
        asset_database = asset_data.get("asset_database")

        datadotworld_asset_data = self.get_asset_data(asset_database, asset_schema, asset_name)
        asset_tags = datadotworld_asset_data.get("tags", [])
        tags_list = [{"guid": str(self.generate_tag_guid(tag)), "name": tag} for tag in asset_tags]
        if datadotworld_asset_data:
            if pull_tags and datadotworld_asset_data:
                    # Delete glossary mapping in dqlabs if deleted in purview
                    self.delete_tags_in_dqlabs(asset_id, tags_info = tags_list) 

                    # map purview tags to dqlabs
                    self.insert_purview_tags_to_dqlabs(tags_info = tags_list)
                
            if datadotworld_asset_data:
                if tags_list:
                    self.map_asset_metrics(asset_data, tags_list, pull_tags)

                active_attributes = self.fetch_entity_details(asset_database, asset_schema, asset_name)
                log_info(("active_attributes",active_attributes))
                if active_attributes:
                    self.map_attribute_metrics(asset_data, active_attributes, pull_tags)

            log_info((f"atlan metrics mapped to attribute of asset: {asset_id}"))

            # Push Asset Custom Metadata from DQLabs to DataDotWorld
            if asset_flags and asset_flags.get("options"):
                try:
                    payload = {
                        "type": ["https://dwec.data.world/v0/DatabaseTable"],
                        "owner": self.owner,
                        "query": asset_name
                    }
                    search_results = self.search_resources(payload=payload)
                    asset_resource_iri = search_results[0].get("iri") if search_results else None
                    collection_iri = datadotworld_asset_data.get("collections")[0].get("referent") if datadotworld_asset_data.get("collections") else None
                    if asset_resource_iri:
                        self.push_data_badge(id=asset_id, resource_iri=asset_resource_iri, entity_type="asset")
                        self.push_check_runs(id=asset_id, entity_type="asset", resource_iri=asset_resource_iri, collection_iri=collection_iri, export_options=asset_flags.get("options", {}))
                    print(f"[__atlan_metadata_push] Pushing custom metadata for asset: {asset_name}")
                    self.push_custom_metadata(asset_info=asset_data, asset_flags=asset_flags, attribute_flags=attribute_flags, asset_resource_iri=asset_resource_iri, collection_iri=collection_iri)
                    log_info((f"Successfully pushed custom metadata to DataDotWorld for asset: {asset_id}"))
                except Exception as e:
                    log_error(f"Push Asset Custom Metadata from DQLabs to DataDotWorld Failed for asset: {asset_id}", e)
                    # Don't raise, continue with other assets


    def map_asset_metrics(self, asset_info: Dict[str, Any], tags_info: List[Dict[str, Any]], map_tags: bool):
        asset_id = asset_info.get("asset_id")
        if map_tags:
            # Update the tags for dqlabs asset
            # asset_tags_list = purview_asset_data.get('entity', {}).get('labels', [])
            # log_info(("asset_tags_list", asset_tags_list))
            if tags_info:
                for tag_mapped in tags_info:
                    tag_id = tag_mapped.get("guid")
                    tag_name = tag_mapped.get("name")
                    
                    # First, ensure the tag exists in tags table
                    tag_check_query = f"""
                        select exists (
                            select 1
                            from core."tags"
                            where id = '{tag_id}'
                            and source = '{DATADOTWORLD}'
                            and is_active = true and is_delete = false
                        );
                    """
                    tag_exists = self.__run_postgres_query(tag_check_query, query_type='fetchone')
                    tag_exists = tag_exists.get("exists", False) if tag_exists else False
                    
                    # If tag doesn't exist, insert it first
                    if not tag_exists:
                        try:
                            with self.connection.cursor() as cursor:
                                query_input = (
                                    tag_id,
                                    tag_name,
                                    tag_name,
                                    None,  # Description
                                    "#21598a",  # Color
                                    DATADOTWORLD,  # Database name
                                    False,  # Native query run
                                    json.dumps({"type": DATADOTWORLD}, default=str),  # Properties
                                    1,  # Order
                                    False,  # is_mask_data
                                    True,  # is_active
                                    False,  # is_delete
                                    str(self.organization_id),  # Organization ID
                                    DATADOTWORLD,  # Source
                                )
                                
                                input_literals = ", ".join(["%s"] * len(query_input))
                                query_param = cursor.mogrify(
                                    f"({input_literals}, CURRENT_TIMESTAMP)", query_input
                                ).decode("utf-8")
                                
                                insert_tag_query = f"""
                                    INSERT INTO core.tags(
                                        id, name, technical_name, description, color, db_name, native_query_run, properties,
                                        "order", is_mask_data, is_active, is_delete, organization_id, source, created_date
                                    )
                                    VALUES {query_param}
                                    RETURNING id;
                                """
                                execute_query(self.connection, cursor, insert_tag_query)
                                log_info((f"Tag inserted: {tag_id}, {tag_name}"))
                        except Exception as e:
                            log_error(f"Failed to insert tag before mapping: {tag_id}", e)
                            continue  # Skip mapping if tag insertion failed
                    
                    # Check if tag is already mapped to this asset
                    attribute_tag_mapped_check_query = f"""
                        select exists (
                            select 1
                            from core."tags_mapping"
                            where tags_id = '{tag_id}'
                            and asset_id = '{asset_id}'
                        );
                    """
                    attribute_tag_mapped = self.__run_postgres_query(attribute_tag_mapped_check_query, query_type='fetchone')
                    attribute_tag_mapped = attribute_tag_mapped.get("exists", False) if attribute_tag_mapped else False
                    
                    log_info(("attribute_tag_mapped for debug", attribute_tag_mapped))
                    if not attribute_tag_mapped:
                        # Map tag to asset
                        try:
                            with self.connection.cursor() as cursor:
                                # Prepare asset-tag mapping insert data
                                query_input = (
                                    str(uuid.uuid4()),  # Unique ID
                                    'asset',  # Level
                                    asset_id,  # Asset ID
                                    None,  # Attribute ID
                                    tag_id,  # Tag ID
                                )

                                # Generate placeholders for the query using %s
                                input_literals = ", ".join(["%s"] * len(query_input))
                                query_param = cursor.mogrify(
                                    f"({input_literals}, CURRENT_TIMESTAMP)", query_input
                                ).decode("utf-8")

                                # Insert asset-tag mapping query
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
    
    def push_custom_metadata(self, asset_info: Dict[str, Any], asset_flags: Dict[str, Any] = {}, attribute_flags: Dict[str, Any] = {}, asset_resource_iri: str = None, collection_iri: str = None):
        """
        Pushes custom metadata for asset and/or attributes to DataDotWorld.
        
        This method handles both asset-level and attribute-level metadata pushing independently
        based on the flags provided. Asset metadata is pushed only if asset_flags.options is enabled,
        and attribute metadata is pushed only if attribute_flags.options is enabled.
        
        Args:
            asset_info (dict): Information about the asset containing asset_id, asset_name, etc.
            asset_flags (dict): Flags for asset-related metadata processing. Must have "options" key.
            attribute_flags (dict): Flags for attribute-related metadata processing. Must have "options" key.
        
        Returns:
            bool: True if at least one push was successful, False otherwise.
        """
        try:
            asset_id = asset_info.get("asset_id")
            asset_name = asset_info.get("asset_name")
            asset_schema = asset_info.get("asset_schema")
            asset_database = asset_info.get("asset_database")
            
            print(f"[push_custom_metadata] Starting for asset: {asset_name}, ID: {asset_id}")
            
            if not all([asset_id, asset_name, asset_schema, asset_database]):
                log_error("Missing required asset information for push_custom_metadata", Exception("Missing required asset information"))
                return False
            
            asset_export_options = asset_flags.get("options", {}) if asset_flags else {}
            attribute_export_options = attribute_flags.get("options", {}) if attribute_flags else {}
            
            asset_success = False
            attribute_success = False
            
            # Push Asset-level metadata if asset_flags is enabled
            if asset_export_options:
                try:
                    print(f"[push_custom_metadata] Asset flags enabled, pushing asset-level metadata")
                    asset_success = self.__push_asset_metadata(asset_info, asset_flags)
                except Exception as e:
                    log_error(f"Push Asset Custom Metadata Failed for asset: {asset_id}", e)
                    print(f"[push_custom_metadata] Failed to push asset metadata: {str(e)}")
            
            # Push Attribute-level metadata if attribute_flags is enabled
            if attribute_export_options:
                try:
                    attribute_success = self.__push_attribute_metadata(asset_info, attribute_flags, asset_resource_iri, collection_iri)
                except Exception as e:
                    log_error(f"Push Attribute Custom Metadata Failed for asset: {asset_id}", e)
                    print(f"[push_custom_metadata] Failed to push attribute metadata: {str(e)}")
            
            return asset_success or attribute_success
                
        except Exception as e:
            print(f"[push_custom_metadata] Exception occurred: {str(e)}")
            log_error(f"Push Custom Metadata to DataDotWorld Failed for asset: {asset_info.get('asset_name', 'unknown')}", e)
            return False
    
    def __push_asset_metadata(self, asset_info: Dict[str, Any], asset_flags: Dict[str, Any]) -> bool:
        """
        Pushes asset-level metadata (summary, description, tags) to DataDotWorld.
        
        Args:
            asset_info (dict): Information about the asset.
            asset_flags (dict): Flags for asset-related metadata processing.
        
        Returns:
            bool: True if successful, False otherwise.
        """
        asset_id = asset_info.get("asset_id")
        asset_name = asset_info.get("asset_name")
        asset_schema = asset_info.get("asset_schema")
        asset_database = asset_info.get("asset_database")
        
        asset_export_options = asset_flags.get("options", {})
        dqlabs_host = self.channel.get("dq_url")
        metrics = get_asset_metrics(self.config, asset_id)
        
        if not metrics:
            print("[__push_asset_metadata] No metrics found, skipping asset metadata push")
            return False
        
        # Format last updated time to UTC ISO format
        last_updated_date = None
        last_updated_time = metrics.get("last_updated_date")
        if last_updated_time:
            if last_updated_time.tzinfo is None:
                last_updated_time = pytz.UTC.localize(last_updated_time)
            else:
                last_updated_time = last_updated_time.astimezone(pytz.UTC)
            last_updated_date = last_updated_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        # Extract metrics based on export options
        metric_data = {}
        if asset_export_options.get("summary"):
            freshness_value = metrics.get('freshness')
            dqscore = metrics.get("score")
            if dqscore is not None and not dqscore == 'None':
                dqscore = round(float(dqscore), 2)
            metric_data = {
                "DQ Score": dqscore,
                "Total Rows": metrics.get("row_count"),
                "Freshness": format_freshness(freshness_value) if freshness_value else None,
                "Active Rules": metrics.get("active_measures"),
                "Observed Rules": metrics.get("observed_measures"),
                "Scoring Rules": metrics.get("scored_measures"),
                "Duplicate Rows": metrics.get("duplicate_count"),
                "Alerts": metrics.get("alerts"),
                "Issues": metrics.get("issues"),
                "Last Updated Date": last_updated_date
            }
        
        # Build URLs if needed
        if asset_export_options.get("alerts") and dqlabs_host:
            metric_data["DqLabs Alerts Url"] = urljoin(dqlabs_host, f"remediate/alerts?asset_id={asset_id}&a_type=data")
        if asset_export_options.get("issues") and dqlabs_host:
            metric_data["DqLabs Issues Url"] = urljoin(dqlabs_host, f"remediate/issues?asset_id={asset_id}&a_type=data")
        if asset_export_options.get("measures") and dqlabs_host:
            metric_data["DqLabs Measures Url"] = urljoin(dqlabs_host, f"observe/data/{asset_id}/attributes#measures-table")
        
        # Build summary as markdown table
        if metric_data:
            # Filter out None values
            filtered_metrics = {key: value for key, value in metric_data.items() if value is not None}
            
            if filtered_metrics:
                # Build table rows
                table_rows = []
                # Header row with DQLABS_METADATA (2 columns for proper table format)
                table_rows.append("| DQLABS_METADATA | |")
                # Separator row
                table_rows.append("|--------------------------------------------|---------------------------------------------|")
                # Data rows with key-value pairs
                for key, value in filtered_metrics.items():
                    table_rows.append(f"| {key} | {value} |")
                
                summary = "\n".join(table_rows)
            else:
                summary = ""
        else:
            summary = ""
        # Build payload
        payload = {
            "summary": summary
        }
        
        # Construct API endpoint
        encoded_table_name = urllib.parse.quote(asset_name, safe='')
        encoded_schema = urllib.parse.quote(asset_schema, safe='')
        url = f"metadata/data/sources/{self.owner}/{asset_database}/tables/{encoded_table_name}?schema={encoded_schema}"
        print(f"[__push_asset_metadata] API URL for asset: {url}")
        
        return self.__push_metadata_to_api(url, payload, asset_name, level='asset')
    
    def __push_attribute_metadata(self, asset_info: Dict[str, Any], attribute_flags: Dict[str, Any], asset_resource_iri: str = None, collection_iri: str = None) -> bool:
        """
        Pushes attribute-level metadata (summary, description, tags) for each column to DataDotWorld.
        
        Args:
            asset_info (dict): Information about the asset.
            attribute_flags (dict): Flags for attribute-related metadata processing.
        
        Returns:
            bool: True if at least one attribute was pushed successfully, False otherwise.
        """
        asset_id = asset_info.get("asset_id")
        asset_name = asset_info.get("asset_name")
        asset_schema = asset_info.get("asset_schema")
        asset_database = asset_info.get("asset_database")
        
        attribute_export_options = attribute_flags.get("options", {})
        dqlabs_host = self.channel.get("dq_url")
        
        # Get all attribute metrics
        attribute_metrics_list = get_attributes_metrics(self.config, asset_id)
        
        # Fetch all attributes/columns
        active_attributes = self.fetch_entity_details(asset_database, asset_schema, asset_name)
        
        if not active_attributes:
            print(f"[__push_attribute_metadata] No attributes found for asset: {asset_name}")
            return False
        
        success_count = 0
        
        # Process each attribute/column
        for attribute in active_attributes:
            try:
                attribute_name = attribute.get("title") or attribute.get("name")
                if not attribute_name:
                    continue

                # Get attribute_id from database
                attribute_id = self.get_attribute_id_from_name(asset_id=asset_id, attribute_name=attribute_name)
                if not attribute_id:
                    continue
                payload = {
                        "type": ["https://dwec.data.world/v0/DatabaseColumn"],
                        "owner": self.owner,
                        "query": attribute_name
                    }
                search_results = self.search_resources(payload=payload)
                attribute_resource_iri = next((record.get("iri") for record in search_results if record.get("tableIri") == asset_resource_iri), None)
                if attribute_resource_iri:
                    self.push_data_badge(id=attribute_id, resource_iri=attribute_resource_iri, entity_type="attribute")
                    self.push_check_runs(id=attribute_id, entity_type="attribute", asset_id=asset_id, resource_iri=attribute_resource_iri, collection_iri=collection_iri, export_options=attribute_flags.get("options", {}))
                print(f"[push_custom_metadata] Attribute flags enabled, pushing attribute-level metadata")
                
                
                # Find matching metrics for this attribute (try by attribute_id first, then by name)
                metrics = next((x for x in attribute_metrics_list if x.get("attribute_id") == attribute_id), None)
                if not metrics:
                    # Fallback: match by attribute name
                    metrics = next((x for x in attribute_metrics_list if x.get("name", "").lower() == attribute_name.lower()), None)
                
                # Build summary if metrics exist and summary option is enabled
                summary = ""
                if metrics and attribute_export_options.get("summary"):
                    # Format last updated time to UTC ISO format
                    last_updated_date = None
                    last_updated_time = metrics.get("last_updated_date")
                    if last_updated_time:
                        if last_updated_time.tzinfo is None:
                            last_updated_time = pytz.UTC.localize(last_updated_time)
                        else:
                            last_updated_time = last_updated_time.astimezone(pytz.UTC)
                        last_updated_date = last_updated_time.strftime('%Y-%m-%dT%H:%M:%SZ')

                    dqscore = metrics.get("score")
                    if dqscore is not None and not dqscore == 'None':
                        dqscore = round(float(dqscore), 2)
                    freshness_value = metrics.get('freshness')
                    metric_data = {
                        "DQ Score": dqscore,
                        "Total Rows": metrics.get("row_count"),
                        "Freshness": format_freshness(freshness_value) if freshness_value else None,
                        "Active Rules": metrics.get("active_measures"),
                        "Observed Rules": metrics.get("observed_measures"),
                        "Scoring Rules": metrics.get("scored_measures"),
                        "Duplicate Rows": metrics.get("duplicate_count"),
                        "Alerts": metrics.get("alerts"),
                        "Issues": metrics.get("issues"),
                        "Last Updated Date": last_updated_date
                    }
                    
                    # Build URLs if needed
                    if attribute_export_options.get("alerts") and dqlabs_host:
                        metric_data["DqLabs Alerts Url"] = urljoin(dqlabs_host, f"remediate/alerts?asset_id={asset_id}&a_type=data")
                    if attribute_export_options.get("issues") and dqlabs_host:
                        metric_data["DqLabs Issues Url"] = urljoin(dqlabs_host, f"remediate/issues?asset_id={asset_id}&a_type=data")
                    if attribute_export_options.get("measures") and dqlabs_host:
                        metric_data["DqLabs Measures Url"] = urljoin(dqlabs_host, f"observe/data/{asset_id}/attributes/{attribute_id}#measures-table")
                    
                    # Build summary as markdown table
                    if metric_data:
                        # Filter out None values
                        filtered_metrics = {key: value for key, value in metric_data.items() if value is not None}
                        
                        if filtered_metrics:
                            # Build table rows
                            table_rows = []
                            # Header row with DQLABS_METADATA (2 columns for proper table format)
                            table_rows.append("| DQLABS_METADATA | |")
                            # Separator row
                            table_rows.append("|--------------------------------------------|---------------------------------------------|")
                            # Data rows with key-value pairs
                            for key, value in filtered_metrics.items():
                                table_rows.append(f"| {key} | {value} |")
                            
                            summary = "\n".join(table_rows)
                        else:
                            summary = ""
                    else:
                        summary = ""

                try:
                    host = self.channel.get("host")
                    if host:
                        encoded_table_name = urllib.parse.quote(asset_name, safe='')
                        encoded_column_name = urllib.parse.quote(attribute_name, safe='')
                        encoded_schema = urllib.parse.quote(asset_schema, safe='')
                        url = f"metadata/data/sources/{self.owner}/{asset_database}/tables/{encoded_table_name}/columns/{encoded_column_name}?schema={encoded_schema}"
                except Exception as e:
                    log_error(f"Failed to fetch existing tags from DataDotWorld API for column: {attribute_name}", e)
                # Build payload with summary, description, and tags
                payload = {
                    "summary": summary
                }
                
                # Construct API endpoint for column
                encoded_table_name = urllib.parse.quote(asset_name, safe='')
                encoded_column_name = urllib.parse.quote(attribute_name, safe='')
                encoded_schema = urllib.parse.quote(asset_schema, safe='')
                url = f"metadata/data/sources/{self.owner}/{asset_database}/tables/{encoded_table_name}/columns/{encoded_column_name}?schema={encoded_schema}"
                
                # Push metadata
                if self.__push_metadata_to_api(url, payload, attribute_name, level='attribute'):
                    success_count += 1
                        
            except Exception as e:
                log_error(f"Failed to push metadata for attribute: {attribute_name}", e)
                continue
        
        return success_count > 0

    def get_asset_description(self, asset_id: str) -> str:
        """
        Retrieves the description of an asset from the database.
        
        Args:
            asset_id (str): The unique identifier of the asset.
        
        Returns:
            str: The description of the asset, or empty string if not found.
        """
        try:
            connection = get_postgres_connection(self.config)
            with connection.cursor() as cursor:
                query_string = f"""
                    SELECT description FROM core.asset WHERE id = '{asset_id}'
                """
                cursor = execute_query(connection, cursor, query_string)
                description = fetchone(cursor)
                return description.get("description", "") if description else ""
        except Exception as e:
            log_error(f"Failed to get asset description for asset: {asset_id}", e)
            return ""
    
    def get_attribute_description(self, attribute_id: str) -> str:
        """
        Retrieves the description of an attribute/column from the database.
        
        Args:
            attribute_id (str): The unique identifier of the attribute.
        
        Returns:
            str: The description of the attribute, or empty string if not found.
        """
        try:
            connection = get_postgres_connection(self.config)
            with connection.cursor() as cursor:
                query_string = f"""
                    SELECT description FROM core.attribute WHERE id = '{attribute_id}'
                """
                cursor = execute_query(connection, cursor, query_string)
                description = fetchone(cursor)
                return description.get("description", "") if description else ""
        except Exception as e:
            log_error(f"Failed to get attribute description for attribute: {attribute_id}", e)
            return ""
    
    def __push_metadata_to_api(self, url: str, payload: Dict[str, Any], entity_name: str, level: str = 'asset') -> bool:
        """
        Helper method to push metadata to DataDotWorld API.
        
        Args:
            url (str): The API endpoint URL (relative path).
            payload (dict): The payload to send.
            entity_name (str): Name of the entity (asset or column name) for logging.
            level (str): The level - 'asset' or 'attribute'. Default is 'asset'.
        
        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            host = self.channel.get("host")
            if not host:
                raise ValueError("Host is missing in channel configuration.")
            
            full_url = f"{host}/v0/{url}"
            print(f"[__push_metadata_to_api] API URL for {level} {entity_name}: {full_url}")
            print(f"[__push_metadata_to_api] Making PATCH request for {level}: {entity_name}...")
            
            response = self.__call_api_request(host, url, 'patch', payload)
            
            if response:
                print(f"[__push_metadata_to_api] Successfully pushed metadata for {level}: {entity_name}")
                log_info((f"Successfully pushed metadata to DataDotWorld for {level}: {entity_name}"))
                return True
            else:
                print(f"[__push_metadata_to_api] Failed to push metadata for {level}: {entity_name}")
                log_error(f"Failed to push metadata to DataDotWorld for {level}: {entity_name}", Exception("API request returned no response"))
                return False
        except Exception as e:
            log_error(f"Failed to push metadata for {level}: {entity_name}", e)
            print(f"[__push_metadata_to_api] Exception for {level} {entity_name}: {str(e)}")
            return False
    
    def get_tags_from_db(self, asset_id: str, level: str = 'asset', attribute_id: str = None) -> List[str]:
        try:
            query_string = f"""
                SELECT DISTINCT t.name
                FROM core.tags_mapping tm
                JOIN core.tags t ON tm.tags_id = t.id
                WHERE tm.asset_id = '{asset_id}'
                AND tm.level = '{level}'
                AND t.is_active = true
                AND t.is_delete = false
            """
            if level == 'attribute' and attribute_id:
                query_string += f" AND tm.attribute_id = '{attribute_id}'"
            
            tags_result = self.__run_postgres_query(query_string, query_type='fetchall')
            tags = [tag.get("name") for tag in tags_result if tag.get("name")]
            entity_name = attribute_id if level == 'attribute' else asset_id
            print(f"[get_tags_from_db] Found {len(tags)} tags for {level}: {entity_name}")
            return tags
        except Exception as e:
            entity_name = attribute_id if level == 'attribute' else asset_id
            log_error(f"Failed to get tags from database for {level}: {entity_name}", e)
            return []

    def __call_api_request(self, host: str, url: str, method_type: str, params: str = ''):

        """
        Make an HTTP request (GET, POST, PUT, DELETE) to a given API endpoint.

        This function makes an API request to the specified endpoint using the given HTTP method 
        (GET, POST, PUT, DELETE). It handles sending the appropriate headers, including the 
        authorization token, and returns the response based on the status code. If the response 
        status code indicates success (200, 201, 202, 204), it returns the response content or 
        the raw response object for a 204 status. In case of an error, it raises an exception.
        
        For GET and POST requests, this function also handles pagination automatically. If the response 
        contains a "next" key, it will continue fetching pages until all data is retrieved and 
        combine all records into a single response.

        Parameters
        ----------
        host : str
            The base host URL (e.g., 'https://api.data.world').
        
        url : str
            The relative URL path (e.g., 'metadata/resources/search?size=1000&from=0').
            The function will automatically prepend '/v0/' to construct the full endpoint.

        method_type : str
            The HTTP method to be used for the request. Can be one of 'get', 'post', 'put', 'patch', or 'delete'.
        
        params : str or dict, optional
            The parameters or data to send with the request. For POST, PUT, and PATCH requests, 
            this can be a dict (will be JSON encoded) or a JSON string. Default is an empty string.

        Returns
        -------
        dict or requests.Response
            - For successful responses (status codes 200, 201, 202, 204), returns the parsed JSON 
            response content. For paginated GET/POST requests, returns a combined response with all records.
            - For status code 204 (No Content), returns the raw response object.
            - If an error occurs (non-success status code or exception), an exception is raised.

        Examples
        --------
        >>> obj = YourClass()
        >>> obj.__call_api_request('https://api.data.world', 'metadata/resources/search', 'get')
        {'data': 'value'}

        >>> obj.__call_api_request('https://api.data.world', 'metadata/update', 'post', '{"key": "value"}')
        {'status': 'success'}

        Notes
        -----
        - The function assumes that the `self.api_key` is set with a valid API token for authentication.
        - The function supports five HTTP methods: 'get', 'post', 'put', 'patch', and 'delete'.
        - For GET and POST requests, pagination is handled automatically if the response contains a "next" key.
        - The function automatically prepends '/v0/' to the URL to construct the full endpoint.
        - For POST, PUT, and PATCH requests, if params is a dict, it will be automatically JSON encoded.
        - For POST pagination, the original params are used for the first request, and subsequent requests 
          use the "next" URL provided in the response.
        - Logs the endpoint, method type, and parameters for debugging purposes.
        - Any exceptions during the request process are logged using the `log_error` function.
        """
        host = host + '/v0/'
        endpoint = urljoin(host, url)
        try:

            # Prepare Headers and Params
            api_headers = {
                            "Authorization": f"Bearer {self.api_key}",
                            "Content-Type": "application/json"
                            }
            if method_type == "post":
                response = requests.post(
                    url=endpoint, headers=api_headers, data=json.dumps(params), verify=False, timeout=900)
                # if response.status_code in [200, 201, 202, 204]:
                #     return response
            elif method_type == "put":
                # Use json= parameter for proper JSON encoding
                if isinstance(params, dict):
                    response = requests.put(
                        url=endpoint, json=params, headers=api_headers, verify=False, timeout=900)
                else:
                    # If params is already a string, use data=
                    response = requests.put(
                        url=endpoint, data=params, headers=api_headers, verify=False, timeout=900)
            elif method_type == "patch":
                # Use json= parameter for proper JSON encoding with PATCH
                if isinstance(params, dict):
                    response = requests.patch(
                        url=endpoint, json=params, headers=api_headers, verify=False, timeout=900)
                else:
                    # If params is already a string, use data=
                    response = requests.patch(
                        url=endpoint, data=params, headers=api_headers, verify=False, timeout=900)
            elif method_type == "delete":
                response = requests.delete(
                    url=endpoint, headers=api_headers, verify=False, timeout=900)
            else:
                response = requests.get(url=endpoint, headers=api_headers, verify=False, timeout=900)
            if (response and response.status_code in [200, 201, 202, 204]):
                if response.status_code in [204]:
                    return response
                return response.json()

            else:
                # Log the actual error response from API
                try:
                    error_response = response.json() if response.content else {}
                    log_error(f"API Error Response: {error_response}", Exception(f"Status {response.status_code}"))
                    print(f"[__call_api_request] API Error Response: {error_response}")
                    print(f"[__call_api_request] Response Status: {response.status_code}")
                    print(f"[__call_api_request] Response Headers: {response.headers}")
                except:
                    log_error(f"API Error - Status {response.status_code}, Response: {response.text}", Exception(f"Status {response.status_code}"))
                    print(f"[__call_api_request] API Error - Status {response.status_code}")
                    print(f"[__call_api_request] Response Text: {response.text}")
                raise ValueError(response.raise_for_status())
        except requests.exceptions.Timeout as timeout_error:
            log_error(
                f"Atlan Connector - Request Timeout Error for ", endpoint)
            raise timeout_error
        except Exception as e:
            log_error(
                f"Atlan Connector - Get Response Error", e)


            
    def __run_postgres_query(self, query_string: str, query_type: str, output_statement: str = ''):

        """
        Helper function to run PostgreSQL queries and return the results based on the query type.

        This function is designed to execute various types of PostgreSQL queries such as 
        SELECT (fetching one or all records) or data-modifying queries (INSERT, UPDATE, DELETE). 
        It uses the connection configuration stored in `self.config` to establish a connection 
        to the database and execute the query. The function also logs the query details for debugging purposes.

        Parameters
        ----------
        query_string : str
            The SQL query string that needs to be executed.
        query_type : str
            The type of query being executed. Can be one of the following:
            - 'fetchone' for fetching a single record.
            - 'fetchall' for fetching multiple records.
            - 'insert', 'update', 'delete' for data-modifying queries.
        output_statement : str, optional
            A description or statement to log after the query is executed. Default is an empty string.

        Returns
        -------
        object
            - For 'fetchone': The first record retrieved from the query result.
            - For 'fetchall': A list of all records retrieved from the query result.
            - For data-modifying queries (INSERT, UPDATE, DELETE), returns `None`.

        Examples
        --------
        >>> obj = YourClass()
        >>> obj.__run_postgres_query("SELECT * FROM my_table WHERE id = 1", "fetchone")
        {'id': 1, 'name': 'example'}  # Example output for fetching a single record.

        >>> obj.__run_postgres_query("SELECT * FROM my_table", "fetchall")
        [{'id': 1, 'name': 'example'}, {'id': 2, 'name': 'example2'}]  # Example output for fetching all records.

        >>> obj.__run_postgres_query("INSERT INTO my_table (id, name) VALUES (1, 'example')", "insert")
        None  # No result expected for insert queries.

        Notes
        -----
        - The function assumes that the PostgreSQL connection configuration (`self.config`) is properly set up.
        - It supports different types of queries, such as SELECT, INSERT, UPDATE, and DELETE.
        - Logs are generated for debugging purposes, including the query string, query type, and output statement.
        - Any exceptions that occur during query execution are caught and logged.

        """
        
        """ Helper function to run postgres queries """
        connection = get_postgres_connection(self.config)
        records = ''
        with connection.cursor() as cursor:
            try:
                cursor = execute_query(
                        connection, cursor, query_string)
            except Exception as e:
                log_error(f"Atlan Connector - Query failed Error", e)
            
            if not query_type in ['insert','update','delete']:
                if query_type == 'fetchone':
                    records = fetchone(cursor)
                elif query_type == 'fetchall':
                    records = fetchall(cursor)
                
                return records
            
            log_info(("Query run:", query_string))
            log_info(('Query Executed',output_statement))