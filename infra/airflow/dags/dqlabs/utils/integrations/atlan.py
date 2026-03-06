import re
import copy
import json
import requests
import pytz
from concurrent.futures import ThreadPoolExecutor

from uuid import uuid4
from typing import List, Dict, Any
from urllib.parse import urljoin

from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone, split_queries
from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.app_helper.crypto_helper import decrypt
from dqlabs.app_helper.dq_helper import format_freshness, get_client_origin, get_max_workers
from dqlabs.enums.connection_types import ConnectionType
from dqlabs.app_constants.dq_constants import DEFAULT_SEMANTIC_THRESHOLD, NEW_ATLAN_CUSTOM_METADATA_PROPERTIES, ATLAN_CUSTOM_METADATA_PROPERTIES

# get asset/domain/attribute metrics from alation
from dqlabs.app_helper.catalog_helper import (get_asset_metrics,
                                               get_attributes_metrics,
                                               get_domain_metrics, 
                                               get_product_metrics)

#import atlan SDK
from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import Table, View
from pyatlan.model.custom_metadata import CustomMetadataDict
from pyatlan.model.typedef import AttributeDef, CustomMetadataDef
from pyatlan.model.enums import AtlanCustomAttributePrimitiveType
from pyatlan.cache.custom_metadata_cache import CustomMetadataCache

#atlan constant
ATLAN = "atlan"

class Atlan:    
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
        self.connection = get_postgres_connection(self.config)
        self.organization_id = config.get('organization_id')
        self.tag_propagation = config.get('dag_info', {}).get('settings', {}).get('discover', {}).get('tag_propagation',{}).get('is_active', False)
        if not self.organization_id:
            self.organization_id = config.get('dag_info', {}).get('organization_id')

    def atlan_catalog_update(self):

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
            """ Fetch all Atlan glossaries and tags and map it in DQLabs"""
            import_config, export_config = self.__atlan_channel_configuration()
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
            # domain_flags = next(
            #                     (item for item in reversed(export_config) if "domain" in item and "asset" not in item and "attribute" not in item), 
            #                     None
            #                 )
            domain_flags = next((item for item in reversed(export_config) if "domain" in item), {})
            log_info({"asset_flags": asset_flags, "attribute_flags": attribute_flags, "domain_flags": domain_flags, "product_flags": product_flags})   

            glossary_info = self.fetch_glossary()
            log_info(("glossary_domain_info",glossary_info))
            terms_list = []

            terms_list = [
                {'termGuid': term['guid'], 'displayText': term['displayText']}
                for item in glossary_info if 'terms' in item
                for term in item['terms'] if 'displayText' in term
            ]

            # Fetch Atlan Domains and Data Products
            atlan_domain_info = self.fetch_product_domain_information(product_keyword="DataDomain")
            log_info(("atlan_product_domain", atlan_domain_info))

            data_product_info = self.fetch_product_domain_information(product_keyword="DataProduct")
            log_info(("data_product_info",data_product_info))

            parent_id_dict = self.find_parent_guid(atlan_domain_info + data_product_info)
            log_info(("parent_id_dict",parent_id_dict))

            if pull_domains:
                # Delete glossary mapping in dqlabs if deleted in atlan
                self.delete_domains_in_dqlabs(glossary_info= glossary_info, atlan_domain_info = atlan_domain_info) 

                # Map atlan domains to dqlabs
                self.insert_atlan_domains_to_dqlabs(domains_info = atlan_domain_info, glossary_info = glossary_info, parent_id_dict = parent_id_dict)
                log_info(("Dqlabs Glossary Domain Updated"))

            # Fetch all Atlan Tags and map it in Dqlabs
            tags_info = self.fetch_atlan_tags()
            log_info(("tags_info",tags_info))

            if pull_tags:
                # Delete glossary mapping in dqlabs if deleted in atlan
                self.delete_tags_in_dqlabs(tags_info = tags_info) 

                # map atlan tags to dqlabs
                self.insert_atlan_tags_to_dqlabs(tags_info = tags_info)

            if pull_products:
                # Delete glossary mapping in dqlabs if deleted in atlan
                self.delete_product_in_dqlabs(
                                            data_product_info = data_product_info
                                            )
                
                if isinstance(atlan_domain_info, list) and isinstance(data_product_info, list):
                    # map atlan product into dqlabs only if entities are available
                    self.insert_atlan_product_to_dqlabs(product_domain_info = atlan_domain_info, 
                                                        data_product_info = data_product_info)
                
            """ Map all the domains,terms,tags and product to the asset"""
            # Get current asset properties
            asset_information = self.__asset_information()
            if asset_information:
                self.create_custom_metadata()
                self.update_custom_metadata_properties()

            log_info(("asset_information",asset_information))

            active_dqlabs_custom_properties = self.get_active_custom_properties()
            # Push Atlan metrics for each asset
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
                        self.__atlan_metadata_push,
                        asset_data,
                        active_dqlabs_custom_properties,
                        tags_info,
                        terms_list,
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
    
    def get_dqlabs_asset_domains(self, config, asset_id: str):
        try:
            connection = get_postgres_connection(config)
            with connection.cursor() as cursor:
                query_string = f"""
                SELECT 
                    d.id 
                FROM 
                    core.domain_mapping dm
                    INNER JOIN core.domain d 
                        ON d.id = dm.domain_id
                        AND d.source = '{ATLAN}'
                        AND dm.asset_id = '{asset_id}'
                """
                cursor = execute_query(connection, cursor, query_string)
                domains = fetchall(cursor)
            return domains
        except Exception as e:
            log_error(f"Get Asset domains ", str(e))
            raise e
    
    def get_dqlabs_asset_products(self, config, asset_id: str):

        try:
            connection = get_postgres_connection(config)
            with connection.cursor() as cursor:
                query_string = f"""
                SELECT 
                    p.id 
                FROM 
                    core.product_mapping pm
                    INNER JOIN core.product p 
                        ON p.id = pm.product_id
                        AND p.source = '{ATLAN}'
                        AND pm.asset_id = '{asset_id}'
                """
                cursor = execute_query(connection, cursor, query_string)
                products = fetchall(cursor)
            return products
        except Exception as e:
            log_error(f"Get Asset domains ", str(e))
            raise e
        
    def push_domain_custom_metadata_to_atlan(self, domain_export_config, domain_id, properties):
        domain_export_config = domain_export_config["options"]
        dqlabs_host = self.channel.get("dq_url")

        # Fetch Dq metrics 
        metrics = get_domain_metrics(self.config, domain_id)
        log_info(("domain metrics in product metadata pushing", metrics))
        # generate the atlan client 
        atlan_client = AtlanClient(
                                base_url=self.channel.get("host"), 
                                api_key=self.api_key
                                )
        cm_dqlabs_domain = CustomMetadataDict(name="DQLABS")

        """Build the custom metadata asset"""
        # build the summary report 
        if domain_export_config.get("summary"):
            dqscore = metrics.get("score")
            log_info(("dqscore",dqscore))
            if dqscore is not None and not dqscore == 'None' and "DQ Score" in properties:
                cm_dqlabs_domain["DQ Score"] = round(float(dqscore), 2)
            
            if "Active Rules" in properties:
                cm_dqlabs_domain["Active Rules"] = metrics.get("measures")
            
            if "Tables" in properties:
                cm_dqlabs_domain["Tables"] = metrics.get("tables")
            
            if "Views" in properties:
                cm_dqlabs_domain["Views"] = metrics.get("views")
            
            if "Reports" in properties:
                cm_dqlabs_domain["Reports"] = metrics.get("reports")
            
            if "Pipelines" in properties:
                cm_dqlabs_domain["Pipelines"] = metrics.get("pipeline")
            
            if "Users" in properties:
                cm_dqlabs_domain["Users"] = metrics.get("users")
            
            if "Attributes" in properties:
                cm_dqlabs_domain["Attributes"] = metrics.get("attributes")
            
            if "Alerts" in properties:
                cm_dqlabs_domain["Alerts"] = metrics.get("alerts")
            
            if "Issues" in properties:
                cm_dqlabs_domain["Issues"] = metrics.get("issues")

        # build all url properties
        if domain_export_config.get("alerts") and "DqLabs Alerts Url" in properties:
            asset_alerts_url = urljoin(dqlabs_host, f"remediate/alerts?domains={domain_id}&hierarchy=true")
            cm_dqlabs_domain["DqLabs Alerts Url"] = asset_alerts_url

        if domain_export_config.get("issues") and "DqLabs Issues Url" in properties:
            issue_alerts_url = urljoin(dqlabs_host, f"remediate/issues?domains={domain_id}&hierarchy=true")
            cm_dqlabs_domain["DqLabs Issues Url"] = issue_alerts_url

        if domain_export_config.get("measures") and "DqLabs Measures Url" in properties:
            measures_alerts_url = urljoin(dqlabs_host, f"discover/semantics/domains/{domain_id}/measures")
            cm_dqlabs_domain["DqLabs Measures Url"] = measures_alerts_url

        log_info(("cm_dqlabs_domain metadata",cm_dqlabs_domain))
        try:
            response = atlan_client.asset.replace_custom_metadata(guid = domain_id,
                                                            custom_metadata = cm_dqlabs_domain)
        except Exception as e:
            log_error(
                    f"Atlan Asset Custom Metadata Update Failed ", e)
            raise e

    
    def push_product_custom_metadata_to_atlan(self, product_export_config, product_id, properties):
        product_export_config = product_export_config["options"]
        # asset_id = asset_information.get("asset_id")
        dqlabs_host = self.channel.get("dq_url")

        # Fetch Dq metrics 
        metrics = get_product_metrics(self.config, product_id)
        log_info(("product metrics in product metadata pushing", metrics))
        # generate the atlan client 
        atlan_client = AtlanClient(
                                base_url=self.channel.get("host"), 
                                api_key=self.api_key
                                )
        cm_dqlabs_product = CustomMetadataDict(name="DQLABS")

        """Build the custom metadata asset"""
        # build the summary report 
        if product_export_config.get("summary"):
            dqscore = metrics.get("score")
            log_info(("dqscore in product",dqscore))
            if dqscore is not None and not dqscore == 'None' and "DQ Score" in properties:
                cm_dqlabs_product["DQ Score"] = round(float(dqscore), 2)
            
            if "Active Rules" in properties:
                cm_dqlabs_product["Active Rules"] = metrics.get("measure_count")
            
            if "Tables" in properties:
                cm_dqlabs_product["Tables"] = metrics.get("tables")
            
            if "Views" in properties:
                cm_dqlabs_product["Views"] = metrics.get("views")
            
            if "Reports" in properties:
                cm_dqlabs_product["Reports"] = metrics.get("reports")
            
            if "Pipelines" in properties:
                cm_dqlabs_product["Pipelines"] = metrics.get("pipelines")
            
            if "Users" in properties:
                cm_dqlabs_product["Users"] = metrics.get("users")
            
            if "Attributes" in properties:
                cm_dqlabs_product["Attributes"] = metrics.get("attributes")
            
            if "Alerts" in properties:
                cm_dqlabs_product["Alerts"] = metrics.get("alert_count")
            
            if "Issues" in properties:
                cm_dqlabs_product["Issues"] = metrics.get("issue_count")

        # build all url properties
        if product_export_config.get("alerts") and "DqLabs Alerts Url" in properties:
            asset_alerts_url = urljoin(dqlabs_host, f"remediate/alerts?products={product_id}&hierarchy=true")
            cm_dqlabs_product["DqLabs Alerts Url"] = asset_alerts_url

        if product_export_config.get("issues") and "DqLabs Issues Url" in properties:
            issue_alerts_url = urljoin(dqlabs_host, f"remediate/issues?products={product_id}&hierarchy=true")
            cm_dqlabs_product["DqLabs Issues Url"] = issue_alerts_url

        if product_export_config.get("measures") and "DqLabs Measures Url" in properties:
            measures_alerts_url = urljoin(dqlabs_host, f"discover/semantics/products/{product_id}/measures")
            cm_dqlabs_product["DqLabs Measures Url"] = measures_alerts_url

        log_info(("cm_dqlabs_product metadata",cm_dqlabs_product))
        try:
            response = atlan_client.asset.replace_custom_metadata(guid = product_id,
                                                            custom_metadata = cm_dqlabs_product)
        except Exception as e:
            log_error(
                    f"Atlan Asset Custom Metadata Update Failed ", e)
            raise e

    def __atlan_metadata_push(self, asset_data: Dict[str, Any], properties, tags_info: List[Dict[str, Any]], terms_list: List[Dict[str, Any]], pull_tags: bool, pull_domains: bool, pull_products: bool, asset_flags: Dict[str, Any], attribute_flags: Dict[str, Any], domain_flags: Dict[str, Any], product_flags: Dict[str, Any]):
        asset_id = asset_data.get("asset_id")
        # connection_type = asset_data.get("connection_type")
        asset_schema = asset_data.get("asset_schema")
        asset_name = asset_data.get("asset_name")
        asset_database = asset_data.get("asset_database")

        # Fetch active datasources selected
        user_atlan_datasources = self.channel.get("datasource",[])
        log_info(("user_atlan_datasources",user_atlan_datasources))

        valid_qualified_name = '' 
        if isinstance(user_atlan_datasources, str):
            user_atlan_datasources = json.dumps(user_atlan_datasources)
        if user_atlan_datasources:
            for datasource in user_atlan_datasources:
                # if connection_type.lower() in datasource.lower(): #check for same connection type datasources
                asset_fully_qualified_name = f"{datasource}/{asset_database}/{asset_schema}/{asset_name}"
                log_info(("asset_fully_qualified_name",asset_fully_qualified_name))
                try:
                    asset_guid = self.__fetch_table_guid(asset_data, asset_fully_qualified_name)
                    log_info(("asset_guid",asset_guid))
                except Exception as e:
                    log_error(
                        f"Fetching Table Guid Failed for {asset_fully_qualified_name}", e)
                    if("server responded with a permission error" in str(e).lower()):
                        raise e
                    else:
                        log_error(
                            f"Fetching Table Guid Failed for {asset_fully_qualified_name}", e)
                        continue
                if asset_guid:
                    # get valid qualified name 
                    valid_qualified_name = asset_fully_qualified_name
                    # fetch asset info
                    atlan_asset_info = self.fetch_asset_info(asset_guid=asset_guid)
                    log_info(("atlan_asset_info",atlan_asset_info))
                    if atlan_asset_info:
                        # map asset product
                        self.map_asset_metrics(
                                asset_info = atlan_asset_info,
                                tags_info = tags_info,
                                dq_asset_data = asset_data,
                                map_products = pull_products,
                                map_tags = pull_tags,
                                map_domains = pull_domains
                                )
                        log_info((f"product mapped to asset {asset_id}"))

                        # map attribute 
                        self.map_attribute_metrics(asset_info = atlan_asset_info,
                                                    tags_info = tags_info,
                                                    terms_info=terms_list,
                                                    dq_asset_data = asset_data,
                                                    map_domains = pull_domains,
                                                    map_tags = pull_tags)
                        log_info((f"atlan metrics mapped to attribute of asset: {asset_id}"))
                        log_info(("valid_qualified_name",valid_qualified_name))
                        self.map_tag_for_asset_attributes(
                                asset_info = atlan_asset_info,
                                tags_info = tags_info,
                                dq_asset_data = asset_data,
                                map_tags = pull_tags,
                                )
                        if valid_qualified_name:
                            if asset_flags.get("asset",False):
                                """ Push Asset Custom Metadata from DQLabs to Atlan"""
                                try:
                                    self.push_asset_custom_metadata_to_atlan(
                                                                            qualified_name = valid_qualified_name,
                                                                            asset_information = asset_data,
                                                                            asset_export_config = asset_flags,
                                                                            properties = properties
                                                                            )
                                except Exception as e:
                                    log_error(
                                        f"Push Asset Custom Metadata from DQLabs to Atlan Failed ", e)
                                    raise e


                            if attribute_flags.get("attribute",False):
                                """ Push Attribute Custom Metadata from DQLabs to Atlan"""
                                try:
                                    self.push_attribute_custom_metadata_to_atlan(
                                                                        qualified_name = valid_qualified_name,
                                                                        asset_information = asset_data,
                                                                        attribute_export_config = attribute_flags,
                                                                        properties = properties
                                                                        )
                                except Exception as e:
                                    log_error(
                                        f"Push Attribute Custom Metadata from DQLabs to Atlan Failed ", e)
                                    raise e
                            
                            if domain_flags.get("domain",False):
                                """ Push Product Custom Metadata from DQLabs to Atlan"""
                                atlan_asset_domains = (
                                    atlan_asset_info.get("entity", {})
                                    .get("attributes", {})
                                    .get("domainGUIDs", [])
                                )       
                                dqlabs_asset_domains = self.get_dqlabs_asset_domains(self.config ,asset_id)
                                dqlabs_asset_domains = [product.get("id", "") for product in dqlabs_asset_domains]
                                matched_domain = set(dqlabs_asset_domains).intersection(set(atlan_asset_domains))
                                for domain in matched_domain:
                                    log_info(("domainnn", domain))
                                    try:
                                        self.push_domain_custom_metadata_to_atlan(
                                                                    domain_export_config = domain_flags,
                                                                    domain_id = domain,
                                                                    properties = properties
                                                                            )
                                    except Exception as e:
                                        log_error(
                                            f"Push Attribute Custom Metadata from DQLabs to Atlan Failed ", e)
                                        raise e
                            
                            if product_flags.get("product",False):
                                """ Push Product Custom Metadata from DQLabs to Atlan"""
                                atlan_asset_products = (
                                    atlan_asset_info.get("entity", {})
                                    .get("attributes", {})
                                    .get("productGUIDs", [])
                                )       
                                dqlabs_asset_products = self.get_dqlabs_asset_products(self.config ,asset_id)
                                dqlabs_asset_products = [product.get("id", "") for product in dqlabs_asset_products]
                                matched_prducts = set(dqlabs_asset_products).intersection(set(atlan_asset_products))
                                for product in matched_prducts:
                                    log_info(("product", product))
                                    try:
                                        self.push_product_custom_metadata_to_atlan(
                                                                    product_export_config = product_flags,
                                                                    product_id = product,
                                                                    properties = properties
                                                                            )
                                    except Exception as e:
                                        log_error(
                                            f"Push Attribute Custom Metadata from DQLabs to Atlan Failed ", e)
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

            if asset.get("type").lower() == 'view':
                asset_type = View
            elif asset.get("type").lower() in ['table', 'base table', 'external table']:
                asset_type = Table
            else:
                return []
            
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
                    asset_type = View if asset.get("type").lower() == 'view' else Table
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
                f"Atlan Connector - Get Assets Error", e)
        


    def __atlan_channel_configuration(self) -> Dict[str, Any]:

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
        
    def check_import_metrics(self):

        """
        Check the import metrics for domains, tags, and products based on the channel configuration.

        This function checks the import configuration for different components like domains, tags, and products 
        in the channel configuration. It then updates instance variables `domain_check`, `tags_check`, and 
        `product_check` based on whether these components are included in the import configuration.

        Parameters
        ----------
        None
        
        Returns
        -------
        None
            This function does not return any value. Instead, it updates the instance variables 
            `domain_check`, `tags_check`, and `product_check`.

        Examples
        --------
        >>> obj = YourClass()
        >>> obj.check_import_metrics()
        # Updates the instance variables domain_check, tags_check, and product_check based on the channel configuration.
        
        Notes
        -----
        - This function assumes that `self.channel` contains the configuration data, especially the "import" key.
        - The function logs the result of each check for debugging purposes.
        - If an error occurs, the function logs the error message and raises an exception.

        """
        """
        Check the import metrics for domains, tags, and products.
        Updates instance variables based on the channel configuration.
        """
        try:
            channel_checks = self.channel.get("import", {})
            # domain checks
            self.domain_check = bool(channel_checks.get("domains"))
            log_info(("domain_check", self.domain_check))

            # term checks
            self.tags_check = bool(channel_checks.get("tags"))
            log_info(("tags_check", self.tags_check))

            # product check
            self.product_check = bool(channel_checks.get("product"))
            log_info(("product_check", self.product_check))
        except Exception as e:
            log_info(f"Error in check_import_metrics: {e}")
            raise e
    
    def __call_api_request(self, endpoint: str, method_type: str, params: str = ''):

        """
        Make an HTTP request (GET, POST, PUT, DELETE) to a given API endpoint.

        This function makes an API request to the specified endpoint using the given HTTP method 
        (GET, POST, PUT, DELETE). It handles sending the appropriate headers, including the 
        authorization token, and returns the response based on the status code. If the response 
        status code indicates success (200, 201, 202, 204), it returns the response content or 
        the raw response object for a 204 status. In case of an error, it raises an exception.

        Parameters
        ----------
        endpoint : str
            The URL of the API endpoint to make the request to.

        method_type : str
            The HTTP method to be used for the request. Can be one of 'get', 'post', 'put', or 'delete'.
        
        params : str, optional
            A string containing the parameters or data to send with the request. For POST and PUT requests, 
            this should be a JSON string. Default is an empty string.

        Returns
        -------
        dict or requests.Response
            - For successful responses (status codes 200, 201, 202, 204), returns the parsed JSON 
            response content.
            - For status code 204 (No Content), returns the raw response object.
            - If an error occurs (non-success status code or exception), an exception is raised.

        Examples
        --------
        >>> obj = YourClass()
        >>> obj.__call_api_request('https://example.com/api/data', 'get')
        {'data': 'value'}

        >>> obj.__call_api_request('https://example.com/api/update', 'post', '{"key": "value"}')
        {'status': 'success'}

        Notes
        -----
        - The function assumes that the `self.api_key` is set with a valid API token for authentication.
        - The function supports four HTTP methods: 'get', 'post', 'put', and 'delete'.
        - Logs the endpoint, method type, and parameters for debugging purposes.
        - Any exceptions during the request process are logged using the `log_error` function.
        """
        try:
            # Prepare Headers and Params
            api_headers = {
                            "Authorization": f"Bearer {self.api_key}",
                            "Content-Type": "application/json"
                            }

    
            if method_type == "post":
                response = requests.post(
                    url=endpoint, headers=api_headers, data=json.dumps(params), verify=False, timeout=900)
                if response.status_code in [200, 201, 202, 204]:
                    return response
            elif method_type == "put":
                params = json.dumps(params)
                response = requests.put(
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
                raise ValueError(response.raise_for_status())
        except requests.exceptions.Timeout as timeout_error:
            log_error(
                f"Atlan Connector - Request Timeout Error for ", endpoint)
            raise timeout_error
        except Exception as e:
            log_error(
                f"Atlan Connector - Get Response Error", e)

    def __call_indexsearch_api(self, payload: Dict[str, Any] = {}):

        """
        Perform a GraphQL IndexSearch query to Atlan's Meta API.

        This function makes a POST request to the Atlan API to perform an index search using the provided 
        payload. It constructs the endpoint URL using the `host` configuration and sends the request with 
        the necessary authentication token. The function handles the response, checking for success 
        status codes (200, 201, 202, 204), and returns the parsed JSON response for valid requests. 
        In case of an error, it raises an exception.

        Parameters
        ----------
        payload : Dict[str, Any], optional
            A dictionary containing the request payload for the GraphQL IndexSearch query. The payload 
            should include the query (DSL) and any other relevant parameters such as `attributes`.
            Default is an empty dictionary.

        Returns
        -------
        dict or requests.Response
            - On success, returns the JSON response from the Atlan API.
            - On a 204 status code, returns the raw response object.
            - In case of an error, an exception is raised.

        Examples
        --------
        >>> obj = YourClass()
        >>> obj.__call_indexsearch_api({'dsl': {'size': 100, 'query': {'term': {'__glossary': {'value': 'example'}}}}})
        {'hits': {'total': {'value': 100}, 'hits': []}}  # Example response from the API.

        Notes
        -----
        - The function assumes that `self.channel` contains the `host` configuration and that `self.api_key` 
        is set with a valid authentication token.
        - The function supports a GraphQL query with a query string and parameters like `size`, `from`, 
        and `query`.
        - Logs are generated for debugging, including the request payload, the response status code, 
        and the response content.

        """
        log_info(("payload for debug", payload))
        """ Graph QL Indexsearch for atlan"""
        try:
            host = self.channel.get("host")
            # Construct the URL for the token request
            endpoint = urljoin(host, "/api/meta/search/indexsearch")

            # Define headers, including your authentication token
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }
            log_info(("payload",payload))
            # Make the POST request to the API
            response = requests.post(endpoint, headers=headers, data=json.dumps(payload))
            log_info(("response.status_code",response.status_code))
            if (response and response.status_code in [200, 201, 202, 204]):
                    if response.status_code in [204]:
                        return response
                    return response.json()
            else:
                raise ValueError(response.raise_for_status())
        except Exception as e:
            log_error(
                f"Atlan Connector - Get Response Error", e)
            raise e
            
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
        attribute_name = attribute_name.replace("'", "''")
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
            attribute_id = attributes.get("attribute_id") if attributes else None
            return attribute_id
        except Exception as e:
            log_error(
                f"Atlan Connector - Get Response Error", e)
            raise e
            
    def fetch_atlan_tags(self) -> List[Dict[str, Any]]:

        """
        Fetch all the tags from Atlan.

        This function queries Atlan to retrieve all the tags classified as `CLASSIFICATION`. 
        It constructs the full endpoint URL based on the host configuration and performs 
        an API request to fetch the classification definitions.

        Returns
        -------
        List[Dict[str, Any]]
            A list of dictionaries, each representing a tag in Atlan under the classification type.
            Each dictionary contains the tag's details.

        Examples
        --------
        >>> obj = YourClass()
        >>> obj.fetch_atlan_tags()
        [{'tag1_key': 'value1', 'tag2_key': 'value2'}]  # Example output for fetched tags

        Notes
        -----
        - The function assumes that the host configuration (`host`) is available in the channel.
        If the host is missing, an exception will be raised.
        - If the API request is successful, the function extracts the `classificationDefs` 
        from the response, which contains the tags.

        """
        """Fetch all the tags from Atlan."""
        tags_info = []
        try:
            host = self.channel.get("host")
            if not host:
                raise ValueError("Host is missing in channel configuration.")

            url = "api/meta/types/typedefs/?type=CLASSIFICATION"
            # Construct the full endpoint
            endpoint = urljoin(host, url)

            # Fetch the tags information from Atlan
            tags = self.__call_api_request(endpoint, 'get')
            tags_info = tags['classificationDefs']
            return tags_info
        except Exception as e:
            log_error("Fetch Atlan Tags API failed", e)
            raise e

    def fetch_glossary(self) -> List[Dict[str, Any]]:

        """
        Fetch all active glossary information from the Atlan API using pagination, including terms for each glossary.

        This function uses the indexsearch API to retrieve all active glossary information in batches
        to avoid timeout issues with large datasets. It fetches data in chunks of 100 records
        and continues until all records are retrieved. For each active glossary, it also fetches
        all associated terms in batches for better performance.

        Returns
        -------
        List[Dict[str, Any]]
            A list of dictionaries containing active glossary information fetched from Atlan.
            Each dictionary represents an active glossary entity with its associated details and terms.

        Examples
        --------
        >>> obj = YourClass()
        >>> obj.fetch_glossary()
        [{'glossary1_key': 'value1', 'terms': [{'term1': 'value1'}]}]  # Example output for fetched active glossary with terms

        Notes
        -----
        - The function uses the `/api/meta/search/indexsearch` endpoint with pagination support.
        - Data is fetched in batches of 100 records to prevent timeout issues.
        - The function continues fetching until all records are retrieved.
        - Only glossaries with status 'ACTIVE' are included in the results.
        - For each active glossary, it fetches all associated terms using the enhanced fetch_atlan_glossary_terms method with batching.
        - Terms are fetched in batches to handle large glossaries efficiently.
        - Any exceptions raised during the API request are logged for debugging.

        """
        """ Get all the active glossary information using pagination with terms in batches"""
        
        all_glossary_info = []
        batch_size = 100
        offset = 0
        
        try:
            while True:
                # Construct the payload for the indexsearch API with pagination
                payload = {
                    "dsl": {
                        "from": offset,
                        "size": batch_size,
                        "track_total_hits": True,
                        "query": {
                            "bool": {
                                "filter": [
                                    {
                                        "term": {
                                            "__typeName.keyword": {
                                                "value": "AtlasGlossary"
                                            }
                                        }
                                    }
                                ]
                            }
                        },
                        "sort": [
                            {
                                "__guid": {
                                    "order": "asc"
                                }
                            }
                        ]
                    },
                    "attributes": []
                }

                # Fetch the glossary information from atlan using indexsearch API
                batch_response = self.__call_indexsearch_api(payload=payload)
                log_info(("batch_response",batch_response))
                
                if not batch_response or 'entities' not in batch_response:
                    break
                    
                batch_entities = batch_response['entities']
                if not batch_entities:
                    break
                    
                # Filter for only active glossaries and fetch their terms in batches
                active_glossaries = []
                for glossary in batch_entities:
                    # Double-check that the glossary is active (additional safety check)
                    if glossary.get('status') == 'ACTIVE':
                        try:
                            # Get the qualified name of the glossary
                            glossary_qualified_name = glossary.get('attributes', {}).get('qualifiedName')
                            glossary_name = glossary.get('attributes', {}).get('name', 'Unknown')
                            
                            if glossary_qualified_name:
                                # Fetch terms for this active glossary using batching
                                log_info(f"Fetching terms for active glossary: {glossary_name}")
                                terms = self.fetch_atlan_glossary_terms(glossary_qualified_name, batch_size=100)
                                # Add terms to the glossary data
                                glossary['terms'] = terms
                                active_glossaries.append(glossary)
                                log_info(f"Successfully fetched {len(terms)} terms for active glossary: {glossary_name}")
                            else:
                                glossary['terms'] = []
                                active_glossaries.append(glossary)
                                log_info(f"No qualified name found for active glossary: {glossary_name}, skipping terms fetch")
                        except Exception as term_error:
                            log_error(f"Failed to fetch terms for active glossary {glossary.get('attributes', {}).get('name', 'Unknown')}", term_error)
                            glossary['terms'] = []
                            active_glossaries.append(glossary)
                    else:
                        log_info(f"Skipping inactive glossary: {glossary.get('attributes', {}).get('name', 'Unknown')} (status: {glossary.get('status')})")
                    
                all_glossary_info.extend(active_glossaries)
                
                # Check if we've fetched all records
                if len(batch_entities) < batch_size:
                    break
                    
                offset += batch_size
                
                log_info(f"Fetched {len(batch_entities)} glossary records, {len(active_glossaries)} active, total active so far: {len(all_glossary_info)}")
            
            log_info(f"Successfully fetched {len(all_glossary_info)} total active glossary records with terms")
            return all_glossary_info
            
        except Exception as e:
            log_error(f"Fetch Atlan Glossary API failed", e)
            raise e

    def fetch_atlan_glossary_terms(self, glossary_qualified_name: str, batch_size: int = 100) -> List[Dict[str, Any]]:

        """
        Fetch all the terms in Atlan for a given glossary using pagination.

        This function queries Atlan to retrieve all glossary terms associated with 
        the specified `glossary_qualified_name`. It fetches terms in batches to handle
        large glossaries efficiently. It filters the terms based on their 
        type (`AtlasGlossaryTerm`) and status (`ACTIVE`), and returns a list of 
        active glossary terms.

        Parameters
        ----------
        glossary_qualified_name : str
            The qualified name of the glossary whose terms are to be fetched.
        batch_size : int, optional
            The number of terms to fetch per batch. Default is 100.

        Returns
        -------
        List[Dict[str, Any]]
            A list of dictionaries, each representing a glossary term from Atlan. 
            Only active glossary terms of type `AtlasGlossaryTerm` are included.

        Examples
        --------
        >>> obj = YourClass()
        >>> obj.fetch_atlan_glossary_terms("0xGdCQjwnOKTN2dDLzI2O")
        [{'term1_key': 'value1', 'term2_key': 'value2'}]  # Example output for glossary terms

        Notes
        -----
        - The function constructs a payload for querying the Atlan API to fetch terms associated 
        with the specified glossary qualified name.
        - Only terms of type `AtlasGlossaryTerm` with status `ACTIVE` are included in the results.
        - The function fetches terms in batches to handle large glossaries efficiently.
        - The function handles exceptions and logs any errors encountered during execution.
        """
        """ Fetch all the terms in atlan for that glossary using pagination"""
        all_terms = []
        offset = 0
        
        try:
            while True:
                # Construct the payload for the token request with pagination
                payload = {
                    "dsl": {
                        "size": batch_size,
                        "from": offset,
                        "track_total_hits": True,
                        "query": {
                            "bool": {
                                "filter": [
                                    {
                                        "term": {
                                            "__glossary": {
                                                "value": f"{glossary_qualified_name}"
                                            }
                                        }
                                    },
                                    {
                                        "term": {
                                            "__typeName.keyword": {
                                                "value": "AtlasGlossaryTerm"
                                            }
                                        }
                                    }
                                ]
                            }
                        },
                        "sort": [
                            {
                                "__guid": {
                                    "order": "asc"
                                }
                            }
                        ]
                    },
                    "attributes": [
                        "categories"
                    ]
                }

                # fetch the attribute information from atlan
                term_info = self.__call_indexsearch_api(payload=payload) 
                
                if not term_info or 'entities' not in term_info:
                    break
                    
                batch_terms = term_info['entities']
                log_info((f"batch_terms",batch_terms))
                if not batch_terms:
                    break
                
                # Filter for active terms (additional safety check)
                active_terms = list(filter(lambda x: x['typeName'] == 'AtlasGlossaryTerm' 
                                         and x["status"] == 'ACTIVE', batch_terms))
                
                all_terms.extend(active_terms)
                
                # Check if we've fetched all records
                if len(batch_terms) < batch_size:
                    break
                    
                offset += batch_size
                
                log_info(f"Fetched {len(active_terms)} terms for glossary {glossary_qualified_name}, total so far: {len(all_terms)}")
            
            log_info(f"Successfully fetched {len(all_terms)} total terms for glossary {glossary_qualified_name}")
            return all_terms
            
        except Exception as e:
            log_error(f"Fetch Atlan Terms API failed for qualified name {glossary_qualified_name}", e)
            raise e

    def fetch_product_domain_information(self, product_keyword: str) -> List[Dict[str, Any]]:

        """
        Fetch all the products and domains in Atlan.

        This function retrieves product or domain information from Atlan based on 
        the given `product_keyword`. It constructs a query to fetch data about either 
        a DataProduct or DataDomain and returns a list of entities related to that keyword.

        Parameters
        ----------
        product_keyword : str
            The keyword used to filter the product or domain type. It can be either:
            - "DataProduct" to fetch product information.
            - "DataDomain" to fetch domain information.

        Returns
        -------
        List[Dict[str, Any]]
            A list of dictionaries containing product or domain entities from Atlan.
            Each dictionary contains information about a specific product or domain entity.

        Examples
        --------
        >>> obj = YourClass()
        >>> obj.fetch_product_domain_information("DataProduct")
        [{'entity1_key': 'value1', 'entity2_key': 'value2'}]  # Example output for DataProduct

        >>> obj.fetch_product_domain_information("DataDomain")
        [{'entity1_key': 'value1', 'entity2_key': 'value2'}]  # Example output for DataDomain

        Notes
        -----
        - The function uses a keyword (`product_keyword`) to dynamically select whether to query 
        for product or domain information.
        - It constructs a payload for a query to the Atlan API and processes the response to 
        extract relevant entities.
        - The function handles exceptions and logs any errors encountered during the execution.
        """
        """ Fetch all the products and domain in atlan """
        product_domain_info = {}
        try:
            # Construct the payload for the token request
            keyword = "DataProduct" if product_keyword == "DataProduct" else "DataDomain"
            payload = {
                        "dsl": {
                        "from": 0,
                        "size": 100,
                        "aggregations": {},
                        "track_total_hits": True,
                        "query": {
                            "bool": {
                            "filter": [
                                {
                                "term": {
                                    "__typeName.keyword": {
                                    "value": f"{keyword}"
                                    }
                                }
                                }
                            ]
                            }
                        },
                        "sort": [
                            {
                            "__guid": {
                                "order": "asc"
                            }
                            }
                        ]
                        },
                        "attributes": [
                        "certificateStatus"
                        ]
                    }

            # fetch the attribute information from atlan
            product_domain_info = self.__call_indexsearch_api(payload=payload) 
            if product_domain_info:
                # retrieve only the entities for the product domain
                product_domain_info = product_domain_info.get("entities", [])
            return product_domain_info
        except Exception as e:
            log_error(f"Fetch Atlan Product Domain Information Failed", e)
            raise e
        
    
    def get_full_category_path(self, category_name: str, all_categories: list) -> str:
            
        """
        Get the full hierarchical path of a category in a domain.

        This function constructs the complete hierarchy for a given category 
        name based on the list of all categories and their parent-child 
        relationships.

        Parameters
        ----------
        category_name : str
            The name of the category whose full hierarchy path is required.
        all_categories : list
            A list of tuples where each tuple contains:
            - category : dict
                A dictionary containing:
                - `categoryGuid` : str
                    A unique identifier for the category.
                - `displayText` : str
                    The display name of the category.
                - `relationGuid` : str, optional
                    A relationship identifier for the category.
                - `parentCategoryGuid` : str, optional
                    The `categoryGuid` of the parent category, if applicable.
            - parent_category_guid : str or None
                The `categoryGuid` of the parent category, or `None` if the category 
                has no parent.

        Returns
        -------
        str
            The full hierarchical path of the specified category, starting 
            from the root category, with each level separated by a period (`.`).

            If the category is not found, an empty string is returned.

        Examples
        --------
        >>> all_categories = [
        ...     (
        ...         {
        ...             "categoryGuid": "3c2e41fd-d3ab-47bc-8480-427452a63dbe",
        ...             "relationGuid": "d37d2f99-1cf6-4a4b-a9f7-2543cc54dd07",
        ...             "displayText": "Firstname SubDomain"
        ...         },
        ...         None
        ...     ),
        ...     (
        ...         {
        ...             "categoryGuid": "37fbc09c-337e-47c1-aec6-511376ebd60f",
        ...             "parentCategoryGuid": "3c2e41fd-d3ab-47bc-8480-427452a63dbe",
        ...             "relationGuid": "ead2f75e-c8f8-473d-bc4d-2600eace5793",
        ...             "displayText": "SubSub Domain"
        ...         },
        ...         "3c2e41fd-d3ab-47bc-8480-427452a63dbe"
        ...     ),
        ... ]
        >>> obj = YourClass()
        >>> obj.get_full_category_path("SubSub Domain", all_categories)
        'Firstname SubDomain.SubSub Domain'

        Notes
        -----
        - The function assumes that `all_categories` is well-formed, meaning each category
        and its parent exist in the list. However, circular references or missing parent 
        categories may cause unexpected results.
        - The function trims the first level from the path (assumes a dummy root at index 0).
        """


        category_dict = {}
        for category, parent_category_guid in all_categories:
            category_dict[category['categoryGuid']] = (category['displayText'], parent_category_guid)
        full_path = []
        current_category_guid = None
        for category, _ in all_categories:
            if category['displayText'] == category_name:
                current_category_guid = category['categoryGuid']
                break
        for _ in range(len(all_categories)):  
            display_text, parent_category_guid = category_dict.get(current_category_guid, (None, None))
            
            if display_text is None:
                break 
            full_path.append(display_text)
            if parent_category_guid is None:
                break
            current_category_guid = parent_category_guid
        full_path = full_path[1:] 
        return '.'.join(reversed(full_path))


    def __get_term_category_technical_name(self, category_id: str) -> str:

        """
        Fetch the category technical name used for tree mapping of terms.

        This method retrieves the technical name of a given category, which is used for 
        tree mapping in the context of terms. The technical name may be transformed if 
        it contains a pair of parentheses (i.e., swapping parts of the name). The transformation 
        logic is as follows:
        
        1. If the technical name contains parentheses, the part inside the parentheses will be 
        moved to the front, separated by a dot (`.`).
        2. If there are no parentheses, the technical name remains unchanged.

        The method retrieves the category technical name from the database using the provided 
        `category_id` and processes it accordingly.

        Parameters
        ----------
        category_id : str
            The ID of the category whose technical name is to be fetched.

        Returns
        -------
        str
            The transformed technical name of the category. If no transformation is needed, 
            it returns the original technical name as stored in the database.

        Examples
        --------
        >>> obj = YourClass()
        >>> category_technical_name = obj.__get_term_category_technical_name('aadd5b70-7c2e-44c5-b069-5623ef5f3077')
        >>> print(category_technical_name)  # Outputs: SCP.Cat_1

        Notes
        -----
        - The method assumes the database connection and query execution function `__run_postgres_query` are correctly set up.
        - The `category_id` is used in the SQL query to fetch the corresponding `technical_name` from the `core.domain` table.
        - The transformation logic specifically handles cases where the `technical_name` contains parentheses. If no parentheses are found, the original name is returned.
        - The method logs any errors that occur during the execution.

        """
        log_info(("category_id for debug",category_id))
        """ Fetch the category technical name used for tree mapping of term"""
        category_technical_name = ''
        try:
            input_query = f"""
                SELECT 
                    CASE
                        WHEN POSITION('(' IN technical_name) > 0 AND POSITION(')' IN technical_name) > POSITION('(' IN technical_name) THEN
                            CONCAT(
                                SUBSTRING(technical_name FROM POSITION('(' IN technical_name) + 1 FOR POSITION(')' IN technical_name) - POSITION('(' IN technical_name) - 1),
                                '.',
                                SUBSTRING(technical_name FROM 1 FOR POSITION('(' IN technical_name) - 1)
                            )
                        ELSE
                            technical_name  -- Return the original value if no parentheses are found
                    END AS final_output
                FROM core.domain
                WHERE id = '{category_id}';
            """
            category = self.__run_postgres_query(query_string=input_query,
                                                 query_type="fetchone")
            log_info(("category_result",category))
            category_technical_name = category.get("final_output")
            return category_technical_name
        except Exception as e:
            log_error(f"Fetch Category technical name failed", e)
            raise e
    
    def get_atlan_datasource(self):
        """
        Retrieve a set of data sources from the Atlan API.

        This method constructs a request to the Atlan API endpoint to fetch 
        information about data connections. It processes the response and 
        extracts qualified names for each data source.

        Returns:
            set: A set containing the qualified names of the data sources 
                retrieved from the Atlan API. If an error occurs, an empty 
                set is returned.

        Raises:
            Exception: Logs an error message if an exception occurs during 
                    the API request or response processing.
        """
        datasources = {}
        data = ''
        try:
            host = self.channel.get("host")
            
            # Define the API endpoint
            endpoint =  urljoin(host, "/api/meta/search/indexsearch")

            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }


            # Set up the request body
            request_body = {
                "attributes": [
                    "connectorName",
                    "connectionName",
                    "connectionQualifiedName",
                    "defaultSchemaQualifiedName"
                ],
                "dsl": {
                    "size": 400,
                    "sort": [
                        {"__timestamp.date": {"format": "epoch_second", "order": "desc"}}
                    ],
                    "post_filter": {
                        "bool": {
                            "filter": [
                                {"terms": {"__typeName.keyword": ["Connection"]}}
                            ]
                        }
                    },
                    "aggs": {
                        "group_by_connection": {
                            "terms": {
                                "field": "connectionQualifiedName",
                                "size": 400
                            }
                        }
                    }
                }
            }

            # Make the POST request to the Atlan API
            response = requests.post(endpoint, headers=headers, data=json.dumps(request_body))

            # Check for a successful response
            if response.status_code == 200:
                data = response.json()  # Parse JSON response
            
            # Fetch the datasources from the api list
            if data:
                datasources = {entity["attributes"]["qualifiedName"] for entity in data["entities"]}
            
            return datasources
        except Exception as e:
            log_error(
                f"Atlan Connector - Get Token By User Id and Reset Token - {str(e)}", e)
            raise e
            

    def find_parent_guid(self, domains):
        # Create a mapping from qualifiedName to guid and domain object
        qn_to_guid = {domain['attributes']['qualifiedName']: domain['guid'] for domain in domains}
        qn_to_domain = {domain['attributes']['qualifiedName']: domain for domain in domains}
        
        result = {}
        
        for domain in domains:
            qn = domain['attributes']['qualifiedName']
            parts = qn.split('/')

            # Skip the super domain (has no parent)
            if len(parts) <= 4:  # "default/domain/Wi6EwdWXK9eDW7jSR33vJ/super"
                continue
                
            # Find parent qualified name by removing the last part
            parent_qn_parts = parts[:-2] if parts[-2] in ['domain', 'product'] else parts[:-1]
            parent_qn = '/'.join(parent_qn_parts)
            
            # Get parent domain from our mapping
            parent_domain = qn_to_domain.get(parent_qn)
            
            if parent_domain:
                result[domain['guid']] = {
                    'parent_domain_id': parent_domain['guid'],
                    'parent_domain_name': parent_domain['attributes']['name']
                }
        return result

    def insert_atlan_domains_to_dqlabs(self, domains_info:List[Dict[str, Any]], glossary_info:List[Dict[str, Any]], parent_id_dict) -> object:

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


        try:
            domains = list(filter(lambda x: x["attributes"]["qualifiedName"].endswith("/super") and x["status"] == 'ACTIVE', domains_info))
            log_info(("domains in atlan",domains))
            domain_ids = {domain["guid"]: domain for domain in domains}
            domain_ids_list=list(domain_ids.keys())
            domain_list = "', '".join(domain_ids_list)
            domain_check_query = f"""
                                SELECT id FROM core.domain
                                WHERE id IN ('{domain_list}')
                                AND source = '{ATLAN}' AND is_active = true AND is_delete = false;
                            """
            existing_domains = self.__run_postgres_query(domain_check_query, query_type='fetchall')
            existing_domain_ids = {item["id"] for item in existing_domains}

            new_domain_values = []
            for domain_id, domain in domain_ids.items():
                if domain_id not in existing_domain_ids:
                    new_domain_values.append((
                        domain_id,
                        domain.get("displayText"),
                        domain.get("displayText"),
                        None,
                        None,
                        "domain",
                        0,
                        json.dumps({"type": ATLAN}),
                        ATLAN,
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
            
            sub_domains = [
                    x for x in domains_info
                    if x['status'] == 'ACTIVE'
                    and any(domain["attributes"]["qualifiedName"] in x["attributes"]["qualifiedName"] and x["guid"] != domain["guid"] for domain in domains)
                ]
            log_info(("sub_domains in atlan",sub_domains))  
            sub_domain_map = {x['guid']: x for x in sub_domains}
            sub_domain_ids = list(sub_domain_map.keys())

            sub_domains_list = "', '".join(sub_domain_ids)
            log_info(("sub_domains_list",sub_domains_list))
            sub_domain_check_query = f"""
                SELECT id FROM core.domain
                WHERE id IN ('{sub_domains_list}')
                AND source = '{ATLAN}' AND is_active = true AND is_delete = false and type = 'category';
            """
            existing_sub_domains = self.__run_postgres_query(sub_domain_check_query, query_type='fetchall')
            existing_sub_domain_ids = {item["id"] for item in existing_sub_domains}
            log_info(("existing_sub_domain_ids",existing_sub_domain_ids))
            new_sub_domain_values = []
            for sub_id, sub_domain in sub_domain_map.items():
                if sub_id not in existing_sub_domain_ids:
                    parent_id = parent_id_dict.get(sub_id, {}).get('parent_domain_id',"")
                    parent_name = parent_id_dict.get(sub_id, {}).get('parent_domain_name',"")

                    new_sub_domain_values.append((
                        sub_id,
                        sub_domain.get("displayText"),
                        f"{sub_domain.get('displayText')}({parent_name})",
                        None,
                        parent_id,
                        "category",
                        1,
                        json.dumps({"type": ATLAN}),
                        ATLAN,
                        True,
                        False,
                        str(self.organization_id),
                        parent_id
                    ))

            if new_sub_domain_values:
                try:
                    with self.connection.cursor() as cursor:
                        placeholders = ", ".join(["%s"] * len(new_sub_domain_values[0]))
                        query_param = b",".join([
                            cursor.mogrify(f"({placeholders}, CURRENT_TIMESTAMP)", vals)
                            for vals in new_sub_domain_values
                        ]).decode("utf-8")

                        insert_sub_domain_query = f"""
                            INSERT INTO core.domain(
                                id, name, technical_name, description, domain, type, level, properties,
                                source, is_active, is_delete, organization_id, parent_id, created_date
                            )
                            VALUES {query_param}
                            RETURNING id;
                        """
                        log_info(("insert_sub_domain_query", insert_sub_domain_query))
                        execute_query(self.connection, cursor, insert_sub_domain_query)
                except Exception as e:
                    log_error("Bulk insert sub-domains failed", e)
                    raise e
                
            term_insert_values = []
            for glossary in glossary_info:
                # domain_id = domain["guid"]
                glossary_name = glossary["attributes"]["name"]
                domain_qualified_name = glossary["attributes"]["qualifiedName"]
                terms = self.fetch_atlan_glossary_terms(domain_qualified_name, batch_size=100)

                if not terms:
                    continue

                term_ids = [term["guid"] for term in terms]
                term_check_query = f"""
                    SELECT id FROM core.terms
                    WHERE id IN ('{"', '".join(term_ids)}')
                    AND source = '{ATLAN}' AND is_active = true AND is_delete = false;
                """
                existing_terms = self.__run_postgres_query(term_check_query, query_type="fetchall")
                existing_term_ids = {item["id"] for item in existing_terms}

                for term in terms:
                    term_id = term["guid"]
                    if term_id in existing_term_ids:
                        continue

                    term_name = term.get("displayText")
                    # term_categories = term.get("attributes", {}).get("categories") or []
                    # domain_parent_id = domain_id
                    # term_domain_id = domain_id
                    # technical_name = f"{term_name})"

                    # if term_categories:
                    #     category_info = term_categories[0]
                    #     term_domain_id = category_info["guid"]
                    #     category_technical_name = self.__get_term_category_technical_name(term_domain_id)
                    #     technical_name = f"{term_name}({category_technical_name})"

                    term_insert_values.append((
                        term_id,
                        term_name,
                        term_name,
                        None,
                        str(self.organization_id),
                        True,
                        False,
                        ATLAN,
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
            log_error("Bulk insert for domain/category/term failed", e)
            raise e


    def insert_atlan_tags_to_dqlabs(self, tags_info:List[Dict[str, Any]]) -> object:

        """
        Fetch and insert Atlan tags into dqlabs.

        This method checks if the tags from Atlan already exist in the `core.tags` table 
        in the database. If not, it inserts the tags into the `core.tags` table with 
        relevant details including tag ID, name, description, color, and properties.

        Parameters
        ----------
        tags_info : List[Dict[str, Any]]
            A list of dictionaries containing the tags' information fetched from Atlan. 
            Each dictionary contains the `guid` (tag ID), `displayName` (tag name), and other 
            metadata such as `category`, `description`, `options`, etc.

        Returns
        -------
        object
            This method does not return a specific value. It performs insertion of the tags 
            into the database and logs the success or failure of the operation.

        Examples
        --------
        >>> obj = YourClass()
        >>> tags_info = [
        >>>     {'guid': '3478b47e-d23c-4c9e-938b-f9c9e84c301c', 'displayName': 'Pii', 'category': 'CLASSIFICATION'}
        >>> ]
        >>> obj.insert_atlan_tags_to_dqlabs(tags_info)

        Notes
        -----
        - The method uses the `guid` of each tag to check if it already exists in the 
        `core.tags` table, filtering by active status and deletion status.
        - If the tag doesn't exist, it constructs an SQL query to insert the tag data into the 
        database with placeholders for dynamic fields.
        - The method logs information at each step, such as the tag ID, the query being executed, 
        and the success or failure of the insertion.
        - Exception handling is in place to catch and log any errors that occur during the process.

        SQL Query:
        ----------
        The query first checks if the tag exists in the `core.tags` table:
        
            SELECT EXISTS (
                SELECT 1 
                FROM core."tags" 
                WHERE id = '{tag_id}' 
                AND source = '{ATLAN}' 
                AND is_active = true 
                AND is_delete = false
            );

        If the tag does not exist, the following insert query is used to insert the tag:

            INSERT INTO core.tags (
                id, name, technical_name, description, color, db_name, native_query_run, properties, 
                "order", is_mask_data, is_active, is_delete, organization_id, source, created_date
            ) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        """
        
        """ Fetch all the tags from atlan into dqlabs"""
        log_info(("tags_info",tags_info))
        tag_id_name = {tag.get("guid"): tag.get("displayName") for tag in tags_info if tag.get("guid")}
        tag_ids = list(tag_id_name.keys())
    
        tag_ids_list = "', '".join(tag_ids)
        tag_check_query = f"""
            SELECT id FROM core.tags
            WHERE id IN ('{tag_ids_list}')
            AND source = '{ATLAN}' AND is_active = true AND is_delete = false
        """

        existing_tags = self.__run_postgres_query(tag_check_query, query_type='fetchall')
        existing_tag_ids = {item["id"] for item in existing_tags}

        new_tags = [tag_id for tag_id in tag_ids if tag_id not in existing_tag_ids]
        if not new_tags:
            log_info("No new tags to insert.")
            return

        insert_values = []
        for tag_id in new_tags:
            tag_name = tag_id_name[tag_id]
            insert_values.append((
                tag_id,
                tag_name,
                tag_name,
                None,
                "#21598a",
                ATLAN,
                False,
                json.dumps({"type": ATLAN}),
                1,
                False,
                True,
                False,
                str(self.organization_id),
                ATLAN
            ))

        try:
            with self.connection.cursor() as cursor:
                placeholders = ", ".join(["%s"] * len(insert_values[0]))
                values_sql = b",".join([
                    cursor.mogrify(f"({placeholders}, CURRENT_TIMESTAMP)", vals)
                    for vals in insert_values
                ]).decode("utf-8")

                insert_tags_query = f"""
                    INSERT INTO core.tags (
                        id, name, technical_name, description, color, db_name, native_query_run, properties,
                        "order", is_mask_data, is_active, is_delete, organization_id, source, created_date
                    )
                    VALUES {values_sql}
                    RETURNING id;
                """

                log_info(f"Inserting {insert_tags_query} new tags into core.tags")
                execute_query(self.connection, cursor, insert_tags_query)
        except Exception as e:
            log_error("Bulk insert tags failed", e)
            raise e

    
    def insert_atlan_product_to_dqlabs(self, product_domain_info : List[Dict[str, Any]], data_product_info : List[Dict[str, Any]]) -> object:

        """
        Fetch and insert Atlan products and sub-products into dqlabs.

        This method checks if the products from Atlan already exist in the `core.product` table 
        in the database. If not, it inserts the product and its associated sub-products into 
        the `core.product` table with relevant details including product ID, name, description, 
        type, level, and properties.

        Parameters
        ----------
        product_domain_info : List[Dict[str, Any]]
            A list of dictionaries containing the product domain information fetched from Atlan. 
            Each dictionary contains product details such as `guid`, `displayText`, `attributes` (including 
            `qualifiedName`), `status`, and other metadata.

        data_product_info : List[Dict[str, Any]]
            A list of dictionaries containing data product information fetched from Atlan. Each dictionary 
            contains data product details similar to the `product_domain_info`.

        Returns
        -------
        object
            This method does not return a specific value. It performs insertion of the products and sub-products 
            into the database and logs the success or failure of the operation.

        Examples
        --------
        >>> obj = YourClass()
        >>> product_domain_info = [
        >>>     {'guid': 'abcd1234', 'displayText': 'Product A', 'attributes': {'qualifiedName': 'productA/super'}, 'status': 'ACTIVE'}
        >>> ]
        >>> data_product_info = [
        >>>     {'guid': 'efgh5678', 'displayText': 'Sub Product 1', 'attributes': {'qualifiedName': 'productA/super/sub1'}, 'status': 'ACTIVE'}
        >>> ]
        >>> obj.insert_atlan_product_to_dqlabs(product_domain_info, data_product_info)

        Notes
        -----
        - The method first filters out the active products with the `qualifiedName` ending in `/super`.
        - It then checks if each product already exists in the `core.product` table. If not, it inserts the product.
        - For each product, the method finds its associated sub-products and inserts them as well, ensuring that the 
        `product_id` is assigned as the parent ID for sub-products.
        - The `qualifiedName` of sub-products is used to associate them with their parent product.
        - The method handles any exceptions during the process and logs errors.

        SQL Query:
        ----------
        For checking if a product exists:

            SELECT EXISTS (
                SELECT 1
                FROM core."product"
                WHERE id = '{product_id}' 
                AND source = '{ATLAN}' 
                AND is_active = true 
                AND is_delete = false
            );

        If the product does not exist, the following insert query is used:

            INSERT INTO core.product(
                id, name, technical_name, description, product, type, level, properties,
                source, is_active, is_delete, organization_id, parent_id, created_date
            ) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        
        For sub-products, a similar check and insert operation is performed with the parent product ID included.
        """
        # Filtering domains and sub-domains
        products = list(filter(lambda x: x["attributes"]["qualifiedName"] and x["status"] == 'ACTIVE', product_domain_info))
        log_info(("products of atlan",products))
        sub_products = [
            x for x in data_product_info
            if x['status'] == 'ACTIVE'
            and any(p["attributes"]["qualifiedName"] in x["attributes"]["qualifiedName"] and x["guid"] != p["guid"] for p in products)
        ]
        log_info(("sub_products of atlan",sub_products))
        sub_product_map = {x['guid']: x for x in sub_products}
        sub_product_ids = list(sub_product_map.keys())

        existing_sub_products = []
        if sub_product_ids:
            sub_products_listr = "', '".join(sub_product_ids)

            sub_product_check_query = f"""
                SELECT id FROM core.product
                WHERE id IN ('{sub_products_listr}')
                AND source = '{ATLAN}' AND is_active = true AND is_delete = false;
            """
            existing_sub_products = self.__run_postgres_query(sub_product_check_query, query_type='fetchall')


        existing_sub_product_ids = {item["id"] for item in existing_sub_products}
        new_sub_product_values = []
        log_info(("sub_product_map in get products",sub_product_map))
        for sub_id, sub_product in sub_product_map.items():
            if sub_id not in existing_sub_product_ids:
                # parent_name = parent_id_dict.get(sub_id,{}).get('parent_domain_name',"")

                new_sub_product_values.append((
                    sub_id,
                    sub_product.get("displayText"),
                    sub_product.get("displayText"),
                    None,
                    "product",
                    1,
                    json.dumps({"type": ATLAN}),
                    ATLAN,
                    True,
                    False,
                    str(self.organization_id)
                ))

        if new_sub_product_values:
            try:
                with self.connection.cursor() as cursor:
                    placeholders = ", ".join(["%s"] * len(new_sub_product_values[0]))
                    query_param = b",".join([
                        cursor.mogrify(f"({placeholders}, CURRENT_TIMESTAMP)", vals)
                        for vals in new_sub_product_values
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

                
    def fetch_asset_info(self, asset_guid:str) -> Dict[str, Any]:

        """
        Retrieve all asset information for the given asset GUID.

        This method makes an API request to fetch details of the asset identified by the provided 
        asset GUID. The request is made to the Atlan API to retrieve metadata information, excluding 
        relationships and minimal extended information.

        Parameters
        ----------
        asset_guid : str
            The GUID of the asset for which information needs to be fetched.

        Returns
        -------
        Dict[str, Any]
            A dictionary containing the asset details retrieved from the Atlan API. If an error occurs, 
            an empty dictionary is returned.

        Examples
        --------
        >>> obj = YourClass()
        >>> asset_guid = "abcd1234-efgh5678-ijkl91011"
        >>> asset_info = obj.fetch_asset_info(asset_guid)

        Notes
        -----
        - The method constructs the URL for the Atlan API request using the `asset_guid` and a predefined
        host from the `self.channel` object.
        - The API call is made using the `__call_api_request` method.
        - In case of an error, the exception is caught and logged using the `log_error` method.
        - Finally, the method returns the asset information as a dictionary.

        HTTP Request:
        -------------
        URL: 
            `api/meta/entity/guid/{asset_guid}?ignoreRelationships=false&minExtInfo=false/classification`
        Method: GET
        """
        """ Retrieve all asset information for asset guid"""

        asset_info = {}
        try:
            # Extract the host from the channel dictionary
            host = self.channel.get("host")
            
            # Construct the URL for the token request
            url = f"api/meta/entity/guid/{asset_guid}?ignoreRelationships=false&minExtInfo=false/classification"
            endpoint = urljoin(host,url)
            asset_info = self.__call_api_request(endpoint=endpoint,
                                                method_type='get')
            return asset_info
        except Exception as e:
            log_error(
                f"Atlan Connector - Get Response Error", e)


    def fetch_attributes_for_table(self, asset_info: List[Dict[str, Any]]) -> List:

        """
        Retrieve all the active columns and their properties for the given asset.

        This method processes the asset information to extract the active columns and their 
        properties (GUID and attribute name) from the `relationshipAttributes` of the asset. Only 
        the columns that have an `ACTIVE` status are included in the returned list.

        Parameters
        ----------
        asset_info : List[Dict[str, Any]]
            The list of asset information dictionaries, containing details such as entity status 
            and columns information.

        Returns
        -------
        List
            A list of dictionaries, each containing the `guid` and `attribute_name` of an active column.

        Examples
        --------
        >>> obj = YourClass()
        >>> asset_info = [{"entity": {"relationshipAttributes": {"columns": [{"guid": "1234", "displayText": "column1", "entityStatus": "ACTIVE"}]}}}]
        >>> attribute_details = obj.fetch_attributes_for_table(asset_info)

        Notes
        -----
        - The method accesses `relationshipAttributes` from the asset's `entity` field to fetch the columns.
        - Each column is checked for its `entityStatus`, and only those with the status `ACTIVE` are included.
        - The resulting list contains dictionaries with two keys: `guid` and `attribute_name`.

        """
        """ Retrive all the asset's active columns and its properties"""

        attribute_info = asset_info['entity']['relationshipAttributes']['columns']
        attribute_details = [
                            {
                                "guid": attribute.get("guid"),
                                "attribute_name": attribute.get("displayText"),
                            }
                            for attribute in attribute_info
                            if attribute.get("entityStatus") == 'ACTIVE'
                        ]

        return attribute_details

    def map_asset_metrics(self, asset_info: List[Dict[str, Any]], tags_info: List[Dict[str, Any]], dq_asset_data, map_products:bool = False, map_tags:bool = False, map_domains:bool = False) -> object:
        """
        Map product details and tags to the asset.

        This method maps active data products to the asset and also maps tags to the asset if applicable.
        It uses the `asset_info` and `tags_info` to insert the necessary records into the database.

        Parameters
        ----------
        asset_info : List[Dict[str, Any]]
            A list containing information about the asset, including its data products and classifications (tags).
        
        tags_info : List[Dict[str, Any]]
            A list containing tag information used for mapping tags to the asset.
        
        map_products : bool, optional
            Flag to indicate whether to map products to the asset. Default is False.
        
        map_tags : bool, optional
            Flag to indicate whether to map tags to the asset. Default is False.

        Returns
        -------
        object
            The result of the database operations, typically None if the operation is successful.

        Notes
        -----
        - If `map_products` is True, active data products associated with the asset will be mapped to it.
        - If `map_tags` is True, tags from Atlan will be mapped to the asset.
        - The method performs database insertions for product and tag mappings and ensures no duplicate mappings exist.

        Examples
        --------
        >>> obj = YourClass()
        >>> asset_info = [{"entity": {"relationshipAttributes": {"outputPortDataProducts": [{"guid": "product_guid", "displayText": "product_name", "entityStatus": "ACTIVE"}]}}}]
        >>> tags_info = [{"guid": "tag_guid", "name": "tag_name"}]
        >>> obj.map_asset_metrics(asset_info, tags_info, map_products=True, map_tags=True)

        """
        """ Map the product details to the Asset"""
        asset_id = dq_asset_data.get("asset_id")
        log_info(("dataprodcutasset_info",asset_info))
        data_products_info = asset_info['entity']['relationshipAttributes']['outputPortDataProducts']
        data_products_info = [x['guid'] for x in data_products_info if x['entityStatus'] == 'ACTIVE' and x['typeName'] == 'DataProduct']
        log_info(("data_products_info",data_products_info))
        log_info(("map_tags flag",map_tags))
        data_domains = asset_info['entity']['attributes']['domainGUIDs']
        log_info(("data_domains",data_domains))

        if map_domains:
            if data_domains:
                mapped_domain_ids = []
                for data_domain in data_domains:
                    mapped_domain_ids.append(data_domain)
                    domain_id = str(data_domain)

                    #check if domain id in domain_mapping table 
                    asset_domain_mapped_check_query =f"""
                                        select exists (
                                        select 1
                                        from core."domain_mapping"
                                        where domain_id = '{domain_id}'
                                        and asset_id = '{asset_id}'
                                        );
                                    """
                    asset_domain_mapped = self.__run_postgres_query(asset_domain_mapped_check_query,
                                                                query_type='fetchone'
                                                                )
                    asset_domain_mapped = asset_domain_mapped.get("exists",False)
                    log_info(("asset_domain_mapped_check_query",asset_domain_mapped_check_query))
                    log_info(("asset_domain_mapped for debug",asset_domain_mapped))
                    if not asset_domain_mapped:
                        # Map the asset with the domain
                        try:    
                            with self.connection.cursor() as cursor:
                                # Prepare mapping insert data
                                query_input = (
                                    str(uuid4()),
                                    'asset',
                                    asset_id,
                                    None,
                                    domain_id,
                                )

                                # Generate placeholders for the query using %s
                                input_literals = ", ".join(["%s"] * len(query_input))
                                query_param = cursor.mogrify(
                                    f"({input_literals}, CURRENT_TIMESTAMP)", query_input
                                ).decode("utf-8")

                                # Insert mapping query
                                insert_map_asset_domain = f"""
                                    INSERT INTO core.domain_mapping(
                                        id, level, asset_id, measure_id, domain_id, created_date
                                    )
                                    VALUES {query_param}
                                    RETURNING id;
                                """
                                log_info(("insert_map_asset_domain", insert_map_asset_domain))

                                # Run query
                                execute_query(
                                    self.connection, cursor, insert_map_asset_domain
                                )
                        except Exception as e:
                            log_error(
                                f"Domain with id {domain_id} mapped to asset", e)
                log_info(("mapped_domain_ids for delete",mapped_domain_ids))
                # delete the domains mapping for unmapped domain to the asset
                mapped_domain_ids = "', '".join(mapped_domain_ids)
                delete_domain_mapping_retrieve_query=f"""DELETE FROM core.domain_mapping dm using core.domain d where d.id = dm.domain_id
                and dm.asset_id = '{asset_id}' and dm.level = 'asset'  and d.source = '{ATLAN}' and d.type in ('domain','category') and dm.domain_id not in ('{mapped_domain_ids}')"""
                log_info(("delete_domain_mapping_retrieve_query",delete_domain_mapping_retrieve_query))
                delete_domain_mapping_retrieve=self.__run_postgres_query(delete_domain_mapping_retrieve_query,
                                                                query_type='delete'
                                                                )

            else:
                # delete the domain mapping if no domain has been mapped to the asset in atlan
                delete_domain_mapping_retrieve_query=f"""DELETE FROM core.domain_mapping dm using core.domain d where d.id = dm.domain_id
                and dm.asset_id = '{asset_id}' and dm.level = 'asset'  and d.source = '{ATLAN}' and d.type in ('domain','category')"""
                log_info(("delete_domain_mapping_retrieve_query",delete_domain_mapping_retrieve_query))
                delete_domain_mapping_retrieve=self.__run_postgres_query(delete_domain_mapping_retrieve_query,
                                                                query_type='delete'
                                                                )
            

        if map_products:
            log_info(("inside map_products flag",map_products))
            if data_products_info:
                log_info(("inside data_products_info",data_products_info))
                mapped_product_ids = []
                for data_product_id in data_products_info:
                    mapped_product_ids.append(data_product_id)
                    asset_product_mapped_check_query =f"""
                                        select exists (
                                        select 1
                                        from core."product_mapping"
                                        where product_id = '{data_product_id}'
                                        and asset_id = '{asset_id}'
                                        );
                                    """
                    asset_product_mapped = self.__run_postgres_query(asset_product_mapped_check_query,
                                                                query_type='fetchone'
                                                                )
                    asset_product_mapped = asset_product_mapped.get("exists",False)
                    log_info(("asset_product_mapped_check_query",asset_product_mapped_check_query))
                    log_info(("asset_product_mapped for debug",asset_product_mapped))
                    
                    if not asset_product_mapped:
                        
                        # Map the asset with the product
                        try:    
                            with self.connection.cursor() as cursor:
                                # Prepare mapping insert data
                                query_input = (
                                    str(uuid4()),
                                    'asset',
                                    asset_id,
                                    None,
                                    data_product_id,
                                )

                                # Generate placeholders for the query using %s
                                input_literals = ", ".join(["%s"] * len(query_input))
                                query_param = cursor.mogrify(
                                    f"({input_literals}, CURRENT_TIMESTAMP)", query_input
                                ).decode("utf-8")

                                # Insert mapping query
                                insert_map_asset_product = f"""
                                    INSERT INTO core.product_mapping(
                                        id, level, asset_id, measure_id, product_id, created_date
                                    )
                                    VALUES {query_param}
                                    RETURNING id;
                                """
                                log_info(("insert_map_asset_product", insert_map_asset_product))

                                # Run query
                                execute_query(
                                    self.connection, cursor, insert_map_asset_product
                                )
                        except Exception as e:
                            log_error(
                                f"Product {data_product_id} mapped to asset", e)
                            raise e
                log_info(("mapped_product_ids for delete",mapped_product_ids))
                # delete the products mapping for unmapped product to the asset
                mapped_product_ids = "', '".join(mapped_product_ids)
                delete_products_mapping_retrieve_query=f"""DELETE FROM core.product_mapping pm using core.product p where p.id = pm.product_id
                and pm.asset_id = '{asset_id}' and pm.product_id not in ('{mapped_product_ids}') and pm.level = 'asset'  and p.source = '{ATLAN}'"""
                delete_product_mapping_retrieve=self.__run_postgres_query(delete_products_mapping_retrieve_query,
                                                                query_type='delete'
                                                                )
                log_info(("delete_products_mapping_retrieve_query",delete_products_mapping_retrieve_query))
            else:
                # delete the product mapping if no product has been mapped to the asset
                delete_products_mapping_retrieve_query=f"""DELETE FROM core.product_mapping pm using core.product p where p.id = pm.product_id
                and pm.asset_id = '{asset_id}' and pm.level = 'asset'  and p.source = '{ATLAN}'"""
                delete_product_mapping_retrieve=self.__run_postgres_query(delete_products_mapping_retrieve_query,
                                                                query_type='delete'
                                                                )
                log_info(("delete_products_mapping_retrieve_query",delete_products_mapping_retrieve_query))
        if map_tags:
            # Update the tags for dqlabs asset
            
            if 'classifications' in asset_info['entity'].keys(): # Flag to check if tags are mapped to the asset or not
                asset_tag_classifications  = asset_info['entity']['classifications'] # atlan api way of displaying tags
                mapped_tag_ids = []
                for attribute_tag_classification in asset_tag_classifications:
                    atlan_tag_qualified_name = attribute_tag_classification.get("typeName")
                    log_info(("atlan_tag_qualified_name",atlan_tag_qualified_name))

                    # Fetch the tag_id from tags atlan api
                    tags_mapped = list(filter(lambda x: x['name'] == atlan_tag_qualified_name ,
                                            tags_info)
                                            )
                    log_info(("tags_mapped",tags_mapped))
                    if tags_mapped:
                        for tag_mapped in tags_mapped:
                            asset_tag_id = tag_mapped.get("guid")
                            mapped_tag_ids.append(asset_tag_id)
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
                                #  map atlan terms to attributes
                                try:
                                    with self.connection.cursor() as cursor:
                                        # Prepare attribute-tag mapping insert data
                                        query_input = (
                                            str(uuid4()),  # Unique ID
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
                                    raise e
                # delete the tags mapping for unmapped tags to the asset
                mapped_tag_ids = "', '".join(mapped_tag_ids)
                delete_tags_mapping_retrieve_query=f"""DELETE FROM core.tags_mapping tm using core.tags t where t.id = tm.tags_id
                and tm.asset_id = '{asset_id}' and tm.tags_id not in ('{mapped_tag_ids}') and tm.level = 'asset'  and t.source = '{ATLAN}'"""
                delete_tags_mapping_retrieve=self.__run_postgres_query(delete_tags_mapping_retrieve_query,
                                                                query_type='delete'
                                                                )
                log_info(("delete_tags_mapping_retrieve_query",delete_tags_mapping_retrieve_query))
            else:
                # delete the tags mapping if no tags has been mapped to the asset
                delete_tags_mapping_retrieve_query=f"""DELETE FROM core.tags_mapping tm using core.tags t where t.id = tm.tags_id
                and tm.asset_id = '{asset_id}' and tm.level = 'asset'  and t.source = '{ATLAN}'"""
                delete_tags_mapping_retrieve=self.__run_postgres_query(delete_tags_mapping_retrieve_query,
                                                                query_type='delete'
                                                                )
                log_info(("delete_tags_mapping_retrieve_query",delete_tags_mapping_retrieve_query))
                

    def map_attribute_metrics(self, asset_info: List[Dict[str, Any]], tags_info: List[Dict[str, Any]], terms_info: List[Dict[str, Any]], dq_asset_data, map_domains:bool = False, map_tags:bool = False) -> object:

        """
        Map the attribute terms and tags to the relevant attributes for assets.

        This function maps terms and tags to asset attributes in a system by checking if
        the terms and tags are already mapped. If not, it inserts the mappings into the 
        respective tables. The function can be customized to map either domains, tags, or both.

        Parameters
        ----------
        asset_info : list of dict
            A list of dictionaries containing asset information. Each dictionary should contain details about the asset,
            including its attributes.
            
        tags_info : list of dict
            A list of dictionaries containing tag information. Each dictionary represents a tag that might be mapped to an attribute.
            
        map_domains : bool, optional
            A flag indicating whether to map domains to the attributes. Default is False.
            
        map_tags : bool, optional
            A flag indicating whether to map tags to the attributes. Default is False.

        Returns
        -------
        object
            The result of the mapping operation, which may be a confirmation or error message based on the outcome.

        Notes
        -----
        - If `map_domains` is `True`, terms will be mapped to attributes by inserting them into the `terms_mapping` table.
        - If `map_tags` is `True`, tags will be mapped to attributes by inserting them into the `tags_mapping` table.
        - The function ensures that terms and tags are not mapped multiple times by checking for their existence in the respective tables before performing the insert.

        Example
        -------
        map_attribute_metrics(asset_info, tags_info, map_domains=True, map_tags=True)
            This would map both domains and tags to the attributes of the assets provided in `asset_info`.
        """
         
        """ Map the attribute terms and tags """
        
        asset_id = dq_asset_data.get("asset_id")
        atlan_asset_attributes = self.fetch_attributes_for_table(asset_info)
        log_info(("atlan_asset_attributes",atlan_asset_attributes))
        log_info(("term_info for debug",terms_info))
        log_info(("tags-info",tags_info))
        
        """ Map terms and tags to attributes """
        for attribute in atlan_asset_attributes:
            attribute_guid = attribute.get("guid")
            log_info(("attribute_guid",attribute_guid))
            attribute_name = attribute.get("attribute_name")
            log_info(("attribute_name",attribute_name))

            # Extract the host from the channel dictionary
            host = self.channel.get("host")
            
            # Construct the URL for the token request
            url = f"api/meta/entity/guid/{attribute_guid}?ignoreRelationships=false&minExtInfo=false/classification"
            endpoint = urljoin(host, url)
            # fetch the attribute information from atlan
            attribute_info = self.__call_api_request(endpoint, 'get', params='')
            log_info(("attribute_info",attribute_info))
            attribute_term_meanings  = attribute_info['entity']['relationshipAttributes']['meanings'] # atlan api way of displaying terms
            log_info(("attribute_term_meanings",attribute_term_meanings))
            attribute_id = self.get_attribute_id_from_name(asset_id=asset_id, attribute_name=attribute_name)
            if not attribute_id:
                log_info(("attribute_id not found for attribute_name",attribute_name))
                continue
            # fetch the terms if mapped to the attribute
            terms_mapped = []
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
                term_ids = [term.get("termGuid") for term in terms_info if term.get("termGuid") != terms_mapped_guid]
                if term_ids:
                    term_ids_to_be_deleted = "', '".join(term_ids)
                    delete_terms_mapping_retrieve_query=f"""delete from core.terms_mapping where attribute_id = '{attribute_id}' and term_id in ('{term_ids_to_be_deleted}')"""
                    delete_terms_mapping_retrieve=self.__run_postgres_query(delete_terms_mapping_retrieve_query,
                                                            query_type='delete'
                                                            )
                                            
                # terms_mapped = terms_mapped[-1] # fetch only the latest term mapped to the attribute 

                attribute_term_id = terms_mapped.get("guid")
                log_info(("attribute_term_id",attribute_term_id))
                # get attribute id from attribute name and asset id 

                if map_domains:
                    # attribute_id = self.get_attribute_id_from_name(asset_id=asset_id,
                    #                                             attribute_name=attribute_name)
                    
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
                        #  map atlan terms to attributes
                        try:
                            with self.connection.cursor() as cursor:
                                # Prepare attribute-term mapping insert data
                                query_input = (
                                    str(uuid4()),  # Unique ID
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
                            raise e
            else:
                delete_terms_mapping_retrieve_query=f"""DELETE FROM core.terms_mapping
                WHERE attribute_id = '{attribute_id}' 
                AND term_id IN (
                    SELECT id
                    FROM core.terms
                    WHERE source = '{ATLAN}')"""
                delete_terms_mapping_retrieve=self.__run_postgres_query(delete_terms_mapping_retrieve_query,
                                                                query_type='delete'
                                                                )
                log_info(("delete_terms_mapping_retrieve_query",delete_terms_mapping_retrieve_query))
                
                log_info(("deleting the attribute term_mapping if no term has been mapped",attribute_id))


            if map_tags:
                tag_ids=[tag.get("name") for tag in tags_info]
                log_info(("tag_ids for debug",tag_ids))
                attribute_id = self.get_attribute_id_from_name(asset_id=asset_id,
                                                                    attribute_name=attribute_name)
                log_info(("map_tags for debug",map_tags))
                # fetch the tags if mapped to the attribute in atlan
                log_info(("attribute_info['entity'].keys() for debug",attribute_info['entity'].keys())) 
                if 'classifications' in attribute_info['entity'].keys(): # Flag to check if tags are mapped to the attribute or not
                    attribute_tag_classifications  = attribute_info['entity']['classifications'] # atlan api way of displaying tags
                    log_info(("attribute_tag_classifications for debug",attribute_tag_classifications))
                    for tag in tags_info:
                        if tag.get("name") not in [tag_name.get("typeName") for tag_name in attribute_tag_classifications]:
                            fetch_tag_mappings_query=f"""select id from core.tags_mapping where tags_id= '{tag.get("guid")}' and attribute_id='{attribute_id}'"""
                            fetch_tag=self.__run_postgres_query(fetch_tag_mappings_query,
                                                                            query_type='fetchone'
                                            )
                            log_info(("fetch_tag_mappings_query",fetch_tag_mappings_query))
                            log_info(("fetch_tag",fetch_tag))
                            if fetch_tag:
                                delete_tag_mappings_query=f"""delete from core.tags_mapping where tags_id= '{tag.get("guid")}' and attribute_id='{attribute_id}'"""
                                delete_tag = self.__run_postgres_query(delete_tag_mappings_query,
                                                                                query_type='delete'
                                                                                )
                                log_info(("delete_tag_mappings_query for debug having tags",delete_tag_mappings_query))
                                log_info((f"delete_tag_mappings_query for {attribute_name}"))

                    for attribute_tag_classification in attribute_tag_classifications:
                        atlan_tag_qualified_name = attribute_tag_classification.get("typeName")
                        log_info(("atlan_tag_qualified_name",atlan_tag_qualified_name))

                        # Fetch the tag_id from tags atlan api
                        tags_mapped = list(filter(lambda x: x['name'] == atlan_tag_qualified_name ,
                                                tags_info)
                                                )
                        log_info(("tags_mapped",tags_mapped))
                        
                        if tags_mapped:
                            for tag_mapped in tags_mapped:
                                attribute_tag_id = tag_mapped.get("guid")
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
                                    #  map atlan terms to attributes
                                    try:
                                        with self.connection.cursor() as cursor:
                                            # Prepare attribute-tag mapping insert data
                                            query_input = (
                                                str(uuid4()),  # Unique ID
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
                                        raise e
                else :
                    fetch_tag_mappings_query=f"""select * from core.tags_mapping where attribute_id='{attribute_id}' and tags_id IN (
                        SELECT id
                        FROM core.tags
                        WHERE source = '{ATLAN}')"""
                    fetch_tag=self.__run_postgres_query(fetch_tag_mappings_query,
                                                                    query_type='fetchall'
                                        )
                    
                    if fetch_tag:
                            delete_tag_mappings_query=f"""delete from core.tags_mapping where attribute_id='{attribute_id}' and tags_id IN (
                        SELECT id
                        FROM core.tags
                        WHERE source = '{ATLAN}')"""
                            delete_tag = self.__run_postgres_query(delete_tag_mappings_query,
                                                                        query_type='delete'
                                                                        )
                            log_info(("delete_tag_mappings_indi_query for debug",delete_tag_mappings_query))
                            log_info((f"delete_tag_mappings_query for {attribute_name}"))

    def map_tag_for_asset_attributes(self, asset_info: List[Dict[str, Any]], tags_info: List[Dict[str, Any]], dq_asset_data, map_tags:bool = False) -> object:

        """
        Map the tag for child propagation.

        This function maps tags to the child assets based on the provided asset information and tags. 
        It checks if the tags are already mapped to the child assets and if not, it inserts the mappings.

        Parameters
        ----------
        asset_info : list of dict
            A list of dictionaries containing asset information. Each dictionary should contain details about the asset,
            including its attributes.
            
        tags_info : list of dict
            A list of dictionaries containing tag information. Each dictionary represents a tag that might be mapped to an attribute.
            
        Returns
        -------
        object
            The result of the mapping operation, which may be a confirmation or error message based on the outcome.

        Notes
        -----
        - If `map_tags` is `True`, tags will be mapped to the attributes.
        
        Example
        -------
        map_tag_for_asset_attributes(asset_info, tags_info, map_tags=True)
            This would map tags to the attributes of the assets provided in `asset_info`.
        """
        if self.tag_propagation:
            tags_mapped=[]
            asset_id = dq_asset_data.get("asset_id")
            if map_tags:
                # Update the tags for dqlabs asset
                
                if 'classifications' in asset_info['entity'].keys(): # Flag to check if tags are mapped to the asset or not
                    asset_tag_classifications  = asset_info['entity']['classifications'] # atlan api way of displaying tags
                    
                        
                    for attribute_tag_classification in asset_tag_classifications:
                        atlan_tag_qualified_name = attribute_tag_classification.get("typeName")
                        log_info(("atlan_tag_qualified_name",atlan_tag_qualified_name))

                        # Fetch the tag_id from tags atlan api
                        tags_mapped.extend(filter(lambda x: x['name'] == atlan_tag_qualified_name, tags_info))
                    log_info(("tags_mapped",tags_mapped))
                    if tags_mapped:
                        for tag_mapped in tags_mapped:
                            asset_tag_id = tag_mapped.get("guid")
                            #check if tag id in tags table 
                            attribute_tag_mapped_check_query =f"""
                                                select id
                                                from core.tags_mapping
                                                where tags_id = '{asset_tag_id}'
                                                and asset_id = '{asset_id}'
                                                and level = 'attribute'
                                            """
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
                                self.__run_postgres_query(
                                                query_string = remove_tags_mapping_query,
                                                query_type = 'delete',
                                                output_statement = f"{tags_to_remove} unmapped from tags mapping")
                            if self.tag_propagation:
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
                                                str(uuid4()),  # Unique ID
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

                                        try:
                                            execute_query(self.connection, cursor, insert_attribute_tag_mapping_query)
                                        except Exception as e:
                                            raise e
        else:
            return None   
                                                                                        
    def fetch_valid_atlan_custom_metadata(self) -> List[Dict[str, Any]]:

        """ 
        Fetch all existing custom metadata names from Atlan and their associated properties.

        This function sends a request to Atlan's API to retrieve all custom metadata definitions, filtering
        those that contain business-related metadata types. It processes the response to extract the metadata
        names and their associated properties.

        The function returns a list of dictionaries where each dictionary contains:
        - **custom_metadata_name**: The name of the custom metadata.
        - **properties**: A list of property names associated with the custom metadata.

        Returns
        -------
        List[dict]
            A list of dictionaries, each containing a custom metadata name and a list of associated properties.

        Example
        -------
        If the response from the Atlan API includes the following business metadata definitions:
        ```json
        [
            {
                "displayName": "DQLABS",
                "attributeDefs": [
                    {"displayName": "DQ Score"},
                    {"displayName": "Total Rows"},
                    {"displayName": "Freshness"}
                ]
            }
        ]
        ```
        The function will return:
        ```python
        [
            {
                "custom_metadata_name": "DQLABS",
                "properties": ["DQ Score", "Total Rows", "Freshness"]
            }
        ]
        ```

        """

        """
        Fetch all existing custom metadata names from Atlan and their associated properties.

        Returns:
            List[dict]: A list of dictionaries containing custom metadata names and their properties.
        """
        # Extract the host from the channel dictionary
        host = self.channel.get("host")
        
        # Construct the URL for the token request
        endpoint = urljoin(host, "api/meta/types/typedefs?type=BUSINESS_METADATA")

        # Call the API and parse the response
        response = self.__call_api_request(endpoint=endpoint, method_type='get')
        atlan_custom_metadata = response.get('businessMetadataDefs', [])

        # Extract relevant metadata and properties
        valid_atlan_custom_metadata = [
            {
                "custom_metadata_name": metadata.get("displayName"),
                "properties": [attr.get("displayName") for attr in metadata.get("attributeDefs", [])]
            }
            for metadata in atlan_custom_metadata
        ]

        return valid_atlan_custom_metadata

    def __post_dqlabs_custom_metadata(self, properties: List[Dict[str, Any]], method_type='') -> object:

        """
        Post custom metadata to Atlan's API.

        This function creates and sends custom metadata definitions to Atlan. It takes a list of property definitions,
        where each property contains information about the metadata you want to define, such as its name, description, and datatype.
        
        The function uses this information to create custom metadata in Atlan, allowing you to store and manage
        these properties within the Atlan platform.

        Args
        ----
        properties : list of dict
            A list of dictionaries, where each dictionary represents a property of the custom metadata. 
            Each dictionary contains the following information about the property:
            - `property_name`: The name of the property.
            - `description`: A description of the property (optional).
            - `datatype`: The data type of the property (e.g., "STRING", "INTEGER").
        
        method_type : str, optional
            An optional parameter that specifies the method type for additional configurations. By default, it is empty.

        Returns
        -------
        object
            The response from the Atlan client, which indicates whether the custom metadata creation was successful or not.

        Example
        -------
        properties = [
            {"property_name": "Project Name", "description": "The name of the project", "datatype": "STRING"},
            {"property_name": "Budget", "description": "The budget allocated", "datatype": "DECIMAL"}
        ]
        
        response = __post_dqlabs_custom_metadata(properties)
        This will send a request to Atlan's API to create custom metadata with the properties defined above.

        Notes
        -----
        - The function constructs a custom metadata definition called "DQLABS" and associates it with the provided property definitions.
        - It then sends the data to Atlan through an API request to create this custom metadata in the platform.
        - The supported data types for the properties are: "STRING", "INTEGER", "DECIMAL", and "URL".
        - The function automatically assumes that the attributes will not have multiple values (single-valued attributes).

        Errors
        ------
        - If something goes wrong while posting the metadata to Atlan, the function logs an error message indicating the failure.
        """
        """
        Post the custom metadata API to Atlan.

        Args:
            properties (List[Dict[str, Any]]): List of property definitions.
            method_type (str): Optional method type for additional configurations.

        Returns:
            object: Response from Atlan client.
        """
        # Extract the host and API key from the channel dictionary
        base_url = self.channel.get("host")

        # Initialize the Atlan client
        client = AtlanClient(base_url=base_url, api_key=self.api_key)

        # Create the custom metadata definition
        cm_def = CustomMetadataDef.create(display_name="DQLABS")

        # Iterate through properties and create attribute definitions
        attribute_defs = []
        for property in properties:
            property_name = property.get("property_name")
            property_description = property.get("description", "")
            property_datatype = property.get("datatype").upper()

            # Map property datatype to AtlanCustomAttributePrimitiveType
            if property_datatype == "STRING":
                attribute_type = AtlanCustomAttributePrimitiveType.STRING
            elif property_datatype == "INTEGER":
                attribute_type = AtlanCustomAttributePrimitiveType.INTEGER
            elif property_datatype == "DECIMAL":
                attribute_type = AtlanCustomAttributePrimitiveType.DECIMAL
            elif property_datatype == "URL":
                attribute_type = AtlanCustomAttributePrimitiveType.URL

            # Create the attribute definition
            attribute_defs.append(
                AttributeDef.create(
                    display_name=property_name,
                    attribute_type=attribute_type,
                    multi_valued=False,  # Default to single-valued attributes
                )
            )

        # Assign attribute definitions to the custom metadata definition
        cm_def.attribute_defs = attribute_defs
        log_info(("attribute_defs", attribute_defs))

        # update the dqlabs logo in atlan
        cm_def.options = CustomMetadataDef.Options.with_logo_from_url(
                            url="https://i.imghippo.com/files/rNvR9791YCs.jpeg")
        # Send the API request
        try:
            response = client.typedef.create(cm_def)
            log_info("Custom metadata created successfully!")
        except Exception as e:
            log_info(f"Failed to create custom metadata. Status Code: {e}")
            raise e


    def create_custom_metadata(self):

        """
        Create Atlan Custom Metadata Based on Mapping Attributes.

        This function checks if the "DQLABS" custom metadata already exists in Atlan. If not, it creates the custom metadata 
        by defining a set of properties like "DQ Score", "Total Rows", "Freshness", etc., and then posts this metadata to Atlan.

        Args
        ----
        None

        Returns
        -------
        None
            This function does not return any value. It performs a post request to Atlan if necessary.

        Example
        -------
        create_custom_metadata()
        
        This will check if the "DQLABS" custom metadata exists in Atlan. If not, it will create the metadata with properties 
        such as "DQ Score", "Total Rows", "Freshness", and more, and then post it to Atlan.

        Notes
        -----
        - The function filters the existing custom metadata in Atlan to search for "DQLABS".
        - If "DQLABS" does not exist, the function creates it with several defined properties.
        - The properties include metrics like data quality score, freshness, active rules, duplicate rows, etc.
        - This function uses the helper function `__post_dqlabs_custom_metadata` to create the metadata in Atlan.
        
        Properties:
        - "DQ Score": A decimal representing the overall data quality score.
        - "Total Rows": An integer representing the total count of rows within the entity.
        - "Freshness": A string indicating the recency of the data.
        - "Active Rules": An integer showing the number of active rules applied.
        - "Observed Rules": An integer showing the number of observed rules.
        - "Scoring Rules": An integer representing the rules contributing to the score.
        - "Duplicate Rows": The count of duplicate rows found.
        - "Alerts": Total number of alerts generated for the entity.
        - "Issues": Total number of issues for the entity.
        - "DqLabs Alerts Url": URL linking to the DQLabs alerts page.
        - "DqLabs Issues Url": URL linking to the DQLabs issues page.
        - "DqLabs Measures Url": URL linking to the DQLabs measures page.

        Errors
        ------
        - If the custom metadata creation fails or if there is an issue posting it to Atlan, an error message is logged.
        """

    

        """ Create Atlan Custom Metadata Based on Mapping Attributes"""
        valid_atlan_custom_metadata = self.fetch_valid_atlan_custom_metadata()
        log_info(("valid_atlan_custom_metadata",valid_atlan_custom_metadata))
        
        # fetch only the DQLABS custom metadata 
        dqlabs_custom_metadata = list(filter(lambda x:x["custom_metadata_name"] == 'DQLABS', valid_atlan_custom_metadata))

        # dqlabs custom metadata push properties

        dqlabs_custom_metadata_properties = [
                            {"property_name": "DQ Score", "description": "Data Quality Score representing the overall quality of the entity", "datatype": "decimal"},
                            {"property_name": "Total Rows", "description": "Total count of rows within the entity", "datatype": "integer"},
                            {"property_name": "Freshness", "description": "Indicates the recency of the data in the entity", "datatype": "string"},
                            {"property_name": "Tables", "description": "Number of active data quality rules applied to the entity", "datatype": "integer"},
                            {"property_name": "Views", "description": "Number of active data quality rules applied to the entity", "datatype": "integer"},
                            {"property_name": "Attributes", "description": "Number of active data quality rules applied to the entity", "datatype": "integer"},
                            {"property_name": "Pipelines", "description": "Number of active data quality rules applied to the entity", "datatype": "integer"},
                            {"property_name": "Reports", "description": "Number of active data quality rules applied to the entity", "datatype": "integer"},
                            {"property_name": "Users", "description": "Number of active data quality rules applied to the entity", "datatype": "integer"},
                            {"property_name": "Active Rules", "description": "Number of active data quality rules applied to the entity", "datatype": "integer"},
                            {"property_name": "Observed Rules", "description": "Number of rules observed during data quality evaluation", "datatype": "integer"},
                            {"property_name": "Scoring Rules", "description": "Number of rules contributing to the data quality score", "datatype": "integer"},
                            {"property_name": "Duplicate Rows", "description": "Count of duplicate rows found in the entity", "datatype": "integer"},
                            {"property_name": "Alerts", "description": "Total number of data quality alerts generated for the entity", "datatype": "integer"},
                            {"property_name": "Issues", "description": "Total number of data quality issues generated for the entity", "datatype": "integer"},
                            {"property_name": "Last Updated Date", "description": "The most recent date and time when an asset's metadata was modified", "datatype": "string"},
                            {"property_name": "DqLabs Alerts Url", "description": "URL linking to the DQLabs alerts page for this entity", "datatype": "url"},
                            {"property_name": "DqLabs Issues Url", "description": "URL linking to the DQLabs issues page for this entity", "datatype": "url"},
                            {"property_name": "DqLabs Measures Url", "description": "URL linking to the DQLabs measures page for this entity", "datatype": "url"}
                        ]
        
        # Create property and custom metadata if not created already
        if not dqlabs_custom_metadata:
            self.__post_dqlabs_custom_metadata(properties=dqlabs_custom_metadata_properties,
                                            method_type='post')
            log_info(("DQLABS Custom Metadata Created successfully in Atlan"))

    def get_active_custom_properties(self):
        valid_atlan_custom_metadata = self.fetch_valid_atlan_custom_metadata()
        dqlabs_custom_metadata = list(
            filter(lambda x: x["custom_metadata_name"] == 'DQLABS', valid_atlan_custom_metadata)
        )
        dqlabs_custom_metadata_properties = dqlabs_custom_metadata[0]["properties"]
        if dqlabs_custom_metadata_properties:
            dqlabs_custom_metadata_properties = [item for item in dqlabs_custom_metadata_properties if item in ATLAN_CUSTOM_METADATA_PROPERTIES]
        return dqlabs_custom_metadata_properties

    def push_asset_custom_metadata_to_atlan(self, qualified_name : str, asset_information:Dict[str,Any], asset_export_config:Dict[str,Any], properties) -> object:

        """
        Push configured metrics to asset.

        This function sends custom metadata updates to Atlan for a given asset. It updates metrics like the summary of the asset, 
        alerts, issues, and measures, based on the configuration provided.

        Args
        ----
        qualified_name : str
            The qualified name of the asset (usually the fully qualified name in the system).
        
        asset_information : dict
            A dictionary containing asset-related information, such as the asset ID. 
            Example:
            {
                "asset_id": "asset_1234"
            }

        asset_export_config : dict
            A configuration dictionary specifying what parts of the asset's metadata should be updated. 
            It includes options for exporting summary, alerts, issues, and measures.
            Example:
            {
                "options": {
                    "summary": True,
                    "alerts": True,
                    "issues": False,
                    "measures": True
                }
            }

        Returns
        -------
        object
            The response object from Atlan client, indicating whether the custom metadata update was successful.

        Example
        -------
        asset_export_config = {
            "options": {
                "summary": True,
                "alerts": True,
                "issues": False,
                "measures": True
            }
        }
        asset_information = {"asset_id": "asset_1234"}
        push_asset_custom_metadata_to_atlan(qualified_name="table_qualified_name", 
                                            asset_information=asset_information, 
                                            asset_export_config=asset_export_config)
        
        This will push the asset metadata with the configured metrics to Atlan.

        Notes
        -----
        - The function fetches asset-specific metrics such as "DQ Score", "Total Rows", "Freshness", etc.
        - The custom metadata is then updated to Atlan for the asset specified by the `qualified_name`.
        - URL properties for alerts, issues, and measures are generated if specified in the configuration.
        - If the asset's summary, alerts, issues, or measures are enabled in the `asset_export_config`, the function updates those properties in Atlan.

        Errors
        ------
        - If the API request to update custom metadata fails, an error message is logged.
        """


        """ 
        Push configured metrics to asset
        -: summary of asset
        -: alerts
        -: issues
        -: measures
        """
        table_guid = self.__fetch_table_guid(asset_information, qualified_name)
        asset_export_options = asset_export_config["options"]

        if isinstance(table_guid, str):
            asset_id = asset_information.get("asset_id")
            dqlabs_host = self.channel.get("dq_url")

            # Fetch Dq metrics 
            metrics = get_asset_metrics(self.config, asset_id)

            # generate the atlan client 
            atlan_client = AtlanClient(
                                    base_url=self.channel.get("host"), 
                                    api_key=self.api_key
                                    )
            cm_dqlabs_asset = CustomMetadataDict(name="DQLABS")
            last_updated_time = metrics.get("last_updated_date")
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

            """Build the custom metadata asset"""
            # build the summary report 
            if asset_export_options.get("summary"):
                dqscore = metrics.get("score")
                if dqscore is not None and not dqscore == 'None' and "DQ Score" in properties:
                    cm_dqlabs_asset["DQ Score"] = round(float(dqscore), 2)
                
                if "Total Rows" in properties:
                    cm_dqlabs_asset["Total Rows"] = metrics.get("row_count")
                
                if "Freshness" in properties:
                    cm_dqlabs_asset["Freshness"] = format_freshness(metrics.get('freshness'))
                
                if "Active Rules" in properties:
                    cm_dqlabs_asset["Active Rules"] = metrics.get("active_measures")
                
                if "Observed Rules" in properties:
                    cm_dqlabs_asset["Observed Rules"] = metrics.get("observed_measures")
                
                if "Scoring Rules" in properties:
                    cm_dqlabs_asset["Scoring Rules"] = metrics.get("scored_measures")
                
                if "Duplicate Rows" in properties:
                    cm_dqlabs_asset["Duplicate Rows"] = metrics.get("duplicate_count")
                
                if "Alerts" in properties:
                    cm_dqlabs_asset["Alerts"] = metrics.get("alerts")
                
                if "Issues" in properties:
                    cm_dqlabs_asset["Issues"] = metrics.get("issues")
                
                if "Last Updated Date" in properties:
                    cm_dqlabs_asset["Last Updated Date"] = formatted_time

            # build all url properties
            if asset_export_options.get("alerts") and "DqLabs Alerts Url" in properties:
                asset_alerts_url = urljoin(dqlabs_host, f"remediate/alerts?asset_id={asset_id}&a_type=data")
                cm_dqlabs_asset["DqLabs Alerts Url"] = asset_alerts_url

            if asset_export_options.get("issues") and "DqLabs Issues Url" in properties:
                issue_alerts_url = urljoin(dqlabs_host, f"remediate/issues?asset_id={asset_id}&a_type=data")
                cm_dqlabs_asset["DqLabs Issues Url"] = issue_alerts_url

            if asset_export_options.get("measures") and "DqLabs Measures Url" in properties:
                measures_alerts_url = urljoin(dqlabs_host, f"observe/data/{asset_id}/measures#measures-table")
                cm_dqlabs_asset["DqLabs Measures Url"] = measures_alerts_url

            log_info(("cm_dqlabs_asset",cm_dqlabs_asset))
            try:
                response = atlan_client.asset.replace_custom_metadata(guid = table_guid,
                                                                custom_metadata = cm_dqlabs_asset)
            except Exception as e:
                log_error(
                        f"Atlan Asset Custom Metadata Update Failed ", e)
                raise e


    def push_attribute_custom_metadata_to_atlan(self, qualified_name : str, asset_information:Dict[str,Any], attribute_export_config:Dict[str,Any], properties) -> object:

        """
        Push configured metrics to attributes in Atlan.

        This function pushes custom metadata for attributes to Atlan. It retrieves the relevant metrics and asset attributes,
        then sends custom metadata for each attribute, including data quality scores, row counts, freshness, and more.

        Args
        ----
        qualified_name : str
            The qualified name of the asset, used to fetch the asset's GUID.
        
        asset_information : Dict[str, Any]
            A dictionary containing information about the asset, including the asset ID.
        
        attribute_export_config : Dict[str, Any]
            A dictionary containing configuration options for exporting the attribute data, including:
            - `summary`: Whether to include summary metrics like data quality score and row count.
            - `alerts`: Whether to include alerts URL for the attribute.
            - `issues`: Whether to include issues URL for the attribute.
            - `measures`: Whether to include measures URL for the attribute.

        Returns
        -------
        object
            The response from Atlan client, indicating whether the custom metadata update was successful.

        Example
        -------
        qualified_name = "asset_example"
        asset_information = {"asset_id": "12345"}
        attribute_export_config = {
            "options": {
                "summary": True,
                "alerts": True,
                "issues": True,
                "measures": True
            }
        }
        
        response = push_attribute_custom_metadata_to_atlan(qualified_name, asset_information, attribute_export_config)
        This will push custom metadata for attributes of the asset to Atlan, including data quality metrics and URLs for alerts, issues, and measures.

        Notes
        -----
        - The function fetches asset and attribute details from Atlan, then retrieves data quality metrics for each attribute.
        - If the attribute's name matches a metric, it builds a custom metadata dictionary with summary, alerts, issues, and measure URLs.
        - The function uses the Atlan client to send the custom metadata to the relevant attribute in Atlan.
        - If any issues arise during the process, an error is logged.

        Property Details:
        - "DQ Score": Data quality score for the attribute.
        - "Total Rows": Total number of rows in the attribute.
        - "Freshness": The recency of the data in the attribute.
        - "Active Rules": Number of active data quality rules for the attribute.
        - "Observed Rules": Number of observed rules during evaluation.
        - "Scoring Rules": Number of rules contributing to the data quality score.
        - "Duplicate Rows": Count of duplicate rows in the attribute.
        - "Alerts": Total number of data quality alerts generated for the attribute.
        - "Issues": Total number of data quality issues generated for the attribute.
        - "DqLabs Alerts Url": URL linking to the DQLabs alerts page for this attribute.
        - "DqLabs Issues Url": URL linking to the DQLabs issues page for this attribute.
        - "DqLabs Measures Url": URL linking to the DQLabs measures page for this attribute.

        Errors
        ------
        - If there is an error while posting the metadata to Atlan, the function logs the error and does not return any value.
        """

        """ 
        Push configured metrics to attributes
        -: summary of attributes
        -: alerts
        -: issues
        -: measures
        """
        table_guid = self.__fetch_table_guid(asset_information, qualified_name)
        attribute_export_options = attribute_export_config["options"]

        # fetch asset info
        atlan_asset_info = self.fetch_asset_info(asset_guid=table_guid)

        if isinstance(table_guid, str) and atlan_asset_info:
            asset_id = asset_information.get("asset_id")
            atlan_asset_attributes = self.fetch_attributes_for_table(atlan_asset_info)
            log_info(("atlan_asset_attributes",atlan_asset_attributes))

            dqlabs_host = self.channel.get("dq_url")

            # Fetch Dq attribute metrics 
            attribute_metrics = get_attributes_metrics(self.config, asset_id)

            for attribute in atlan_asset_attributes:
                attributes_guid = attribute.get("guid")
                log_info(("attribute_guid",attributes_guid))
                attribute_name = attribute.get("attribute_name")
                log_info(("attribute_name",attribute_name))

                try:
                    # filter dqlabs attribute based on atlan attribute name
                    metrics = next((x for x in attribute_metrics if x["name"].lower() == attribute_name.lower()), None)
                    attribute_guid = metrics.get("attribute_id")
                    log_info(("attribute_id",attribute_guid))
                    
                    if metrics:
                        # generate the atlan client 
                        atlan_client = AtlanClient(
                                                base_url=self.channel.get("host"), 
                                                api_key=self.api_key
                                                )
                        cm_dqlabs_attribute = CustomMetadataDict(name="DQLABS")
                        last_updated_time = metrics.get("last_updated_date")
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

                        """Build the custom metadata asset"""
                        # build the summary report 
                        if attribute_export_options.get("summary"):
                            dqscore = metrics.get("score")
                            if dqscore is not None and not dqscore == 'None' and "DQ Score" in properties:
                                cm_dqlabs_attribute["DQ Score"] = round(float(dqscore), 2)
                            
                            if "Total Rows" in properties:
                                cm_dqlabs_attribute["Total Rows"] = metrics.get("row_count")
                            
                            if "Freshness" in properties:
                                cm_dqlabs_attribute["Freshness"] = metrics.get("freshness")
                            
                            if "Active Rules" in properties:
                                cm_dqlabs_attribute["Active Rules"] = metrics.get("active_measures")
                            
                            if "Observed Rules" in properties:
                                cm_dqlabs_attribute["Observed Rules"] = metrics.get("observed_measures")
                            
                            if "Scoring Rules" in properties:
                                cm_dqlabs_attribute["Scoring Rules"] = metrics.get("scored_measures")
                            
                            if "Duplicate Rows" in properties:
                                cm_dqlabs_attribute["Duplicate Rows"] = metrics.get("duplicate_count")
                            
                            if "Alerts" in properties:
                                cm_dqlabs_attribute["Alerts"] = metrics.get("alerts")
                            
                            if "Issues" in properties:
                                cm_dqlabs_attribute["Issues"] = metrics.get("issues")
                            
                            if "Last Updated Date" in properties:
                                cm_dqlabs_attribute["Last Updated Date"] = formatted_time


                        # build all url properties
                        if attribute_export_options.get("alerts") and "DqLabs Alerts Url" in properties:
                            asset_alerts_url = urljoin(dqlabs_host, f"remediate/alerts?asset_id={asset_id}&a_type=data")
                            cm_dqlabs_attribute["DqLabs Alerts Url"] = asset_alerts_url

                        if attribute_export_options.get("issues") and "DqLabs Issues Url" in properties:    
                            issue_alerts_url = urljoin(dqlabs_host, f"remediate/issues?asset_id={asset_id}&a_type=data")
                            cm_dqlabs_attribute["DqLabs Issues Url"] = issue_alerts_url

                        if attribute_export_options.get("measures") and "DqLabs Measures Url" in properties:    
                            measures_alerts_url = urljoin(dqlabs_host, f"observe/data/{asset_id}/attributes/{attribute_guid}#measures-table")
                            cm_dqlabs_attribute["DqLabs Measures Url"] = measures_alerts_url                   

                        log_info(("cm_dqlabs_attribute",cm_dqlabs_attribute))
                        response = atlan_client.asset.replace_custom_metadata(
                                                                        guid = attributes_guid,
                                                                        custom_metadata = cm_dqlabs_attribute
                                                                        )
                except Exception as e:
                    log_error(
                            f"Atlan Asset Custom Metadata Update Failed ", e)
                    raise e

    def update_custom_metadata_properties(self):
        """
        Updates custom metadata definitions with valid data sources and glossaries.

        This method performs the following tasks:
        1. Fetches all valid data sources and glossaries.
        2. Retrieves and filters valid custom metadata definitions specific to "DQLABS".
        3. Updates the custom metadata attributes with applicable data sources and glossaries.
        4. Sends the updated metadata definition to the Atlan client for update.

        Steps:
        - Fetches valid Atlan data sources and glossaries using helper methods.
        - Filters valid custom metadata related to "DQLABS".
        - Initializes an Atlan client to update custom metadata definitions.
        - Updates attributes with new data sources and glossaries.
        - Handles errors gracefully with logging for debugging purposes.

        Returns:
            None

        Raises:
            None: All exceptions are logged internally.

        """

        valid_glossary = {}
        valid_datasources = {}

        # Fetch all valid data sources
        try:
            valid_datasources = self.get_atlan_datasource()
        except Exception as e:
            log_error(f"No valid datasources found", e)
            raise e

        # Fetch all valid glossaries
        try:
            glossary_info = self.fetch_glossary()
        except Exception as e:
            log_error(f"No valid glossary found", e)
            raise e

        if glossary_info:
            valid_glossary = {glossary["attributes"]["qualifiedName"] for glossary in glossary_info}

        log_info(("valid datasources", valid_datasources))
        log_info(("valid glossary", valid_glossary))

        # Fetch valid custom metadata definitions
        valid_atlan_custom_metadata = self.fetch_valid_atlan_custom_metadata()
        log_info(("valid_atlan_custom_metadata", valid_atlan_custom_metadata))

        # Filter DQLABS-specific custom metadata
        dqlabs_custom_metadata = list(
            filter(lambda x: x["custom_metadata_name"] == 'DQLABS', valid_atlan_custom_metadata)
        )

        if dqlabs_custom_metadata:
            # Initialize the Atlan client
            base_url = self.channel.get("host")
            atlan_client = AtlanClient(
                base_url=base_url, 
                api_key=self.api_key
            )

            # Fetch the existing custom metadata definition
            metadata_name = "DQLABS"
            cache = CustomMetadataCache(atlan_client)
            existing_metadata = cache.get_custom_metadata_def(name=metadata_name)
            existing_field_names = {attr.display_name for attr in existing_metadata.attribute_defs} if existing_metadata.attribute_defs else set()
            log_info(("existing_field_names in DQLABS metadata", existing_field_names))
            missing_elements = [item for item in NEW_ATLAN_CUSTOM_METADATA_PROPERTIES if item not in existing_field_names]
            log_info(("missing_elements in DQLABS metadata", missing_elements))
            
            if not missing_elements:
                log_info(("Custom metadata already contains both fields. Skipping update."))
            new_attributes = []
            for element in missing_elements:
            # if "Tables" not in existing_field_names:
                new_attributes.append( AttributeDef.create(
                    display_name= element,
                    attribute_type=AtlanCustomAttributePrimitiveType.INTEGER
                ))
            existing_attrs = existing_metadata.attribute_defs if existing_metadata.attribute_defs else []
            existing_metadata.attribute_defs = existing_attrs + new_attributes
            log_info(("updated existing_metadata with new attributes", existing_metadata))

            # Update all attributes with the common values
            for attr in existing_metadata.attribute_defs:
                attr.applicable_connections = valid_datasources
                attr.applicable_glossaries = valid_glossary

            try:
                # Send updated metadata to Atlan client
                response = atlan_client.typedef.update(existing_metadata)
                log_info("Custom metadata updated successfully.")
            except Exception as e:
                log_error(f"Error updating custom metadata", e)
                raise e

    def update_custom_metadata(self, guid: str, metrics: Dict[str,Any], domain_export_options:Dict[str,Any]):

        """
        Update custom metadata for a given asset in Atlan.

        This function updates the custom metadata for a domain or category asset in Atlan based on the provided metrics.
        The metadata is updated for various attributes like Data Quality score, alerts, and issues, depending on the 
        specified export options.

        Args
        ----
        guid : str
            The unique identifier (GUID) of the asset (domain or category) whose metadata needs to be updated.

        metrics : Dict[str, Any]
            A dictionary containing metrics data for the asset, including attributes like:
            - "score": The Data Quality score for the asset.
            - "alerts": The number of data quality alerts for the asset.
            - "issues": The number of data quality issues for the asset.

        domain_export_options : Dict[str, Any]
            A dictionary containing options specifying which metrics to include in the custom metadata update. 
            It can contain keys like:
            - "summary": Whether to include the Data Quality score in the update.
            - "alerts": Whether to include the number of alerts in the update.
            - "issues": Whether to include the number of issues in the update.

        Returns
        -------
        None
            This function does not return any value. It performs the metadata update operation.

        Example
        -------
        domain_export_options = {
            "summary": True,
            "alerts": True,
            "issues": True
        }
        metrics = {
            "score": 90,
            "alerts": 5,
            "issues": 2
        }
        update_custom_metadata(guid="some_guid", metrics=metrics, domain_export_options=domain_export_options)

        Notes
        -----
        - The function only updates the metadata for attributes that are enabled in the `domain_export_options`.
        - If the "summary" option is enabled, it updates the "DQ Score".
        - If the "alerts" option is enabled, it updates the "Alerts".
        - If the "issues" option is enabled, it updates the "Issues".
        - The function uses the Atlan API client to make the update, so the API key and host URL must be provided.

        Errors
        ------
        - If an error occurs while updating custom metadata, it is caught and logged with the asset's GUID for reference.
        """
        """
        Update custom metadata for a given asset.

        :param asset_id: ID of the asset (domain or category).
        :param metrics: Metrics data to update.
        """
        try:
            atlan_client = AtlanClient(
                base_url=self.channel.get("host"),
                api_key=self.api_key
            )
            dqlabs_host = self.channel.get("dq_url")
            log_info(("dqlabs_host for debug",dqlabs_host))

            cm_dqlabs_domain = CustomMetadataDict(name="DQLABS")
            log_info(("metrics for domain debug",metrics))
            if domain_export_options.get("summary"):
                dqscore = metrics.get("score")
                log_info(("dqscore",dqscore))
                if not dqscore == 'None' and "DQ Score" in cm_dqlabs_domain:
                    cm_dqlabs_domain["DQ Score"] = dqscore
            if domain_export_options.get("alerts") and "Alerts" in cm_dqlabs_domain:
                cm_dqlabs_domain["Alerts"] = int(metrics.get("alerts"))
            if domain_export_options.get("issues") and "Issues" in cm_dqlabs_domain:
                cm_dqlabs_domain["Issues"] = int(metrics.get("issues"))
            if domain_export_options.get("measures") and "DqLabs Measures Url" in cm_dqlabs_domain:
                domain_id=metrics.get("id")
                log_info(("domain_id for debug",domain_id))
                measures_url = urljoin(dqlabs_host, f"discover/semantics/domains/{domain_id}/measures")
                cm_dqlabs_domain["DqLabs Measures Url"] = measures_url
                log_info(("cm_dqlabs_domain for measure debug",cm_dqlabs_domain["DqLabs Measures Url"] ))

            log_info(("cm_dqlabs_domain", cm_dqlabs_domain))

            atlan_client.asset.replace_custom_metadata(
                guid=guid,
                custom_metadata=cm_dqlabs_domain
            )
        except Exception as e:
            log_error(f"Custom Metadata Update Failed for asset_id: {guid}", e)
            raise e

    def __fetch_table_guid(self, asset_info, qualified_name:str) -> Dict[str, Any]:

        """
        Fetch a table's GUID and its details based on its fully qualified name.

        This function fetches a table's GUID from Atlan using its fully qualified name.
        It checks if the table is active and returns the GUID if active. If the table is not found or inactive, an empty JSON object is returned.

        Args
        ----
        qualified_name : str
            The fully qualified name of the table to fetch.

        Returns
        -------
        dict
            If the table is found and active, returns the table's GUID.
            If the table is not found or inactive, returns an empty JSON object.

        Example
        -------
        qualified_name = "database.schema.table_name"
        guid = __fetch_table_guid(qualified_name)
        # guid = "12345-67890-abcde" if table found and active, or {}

        Errors
        ------
        If an error occurs during the API call, it is logged, and an empty JSON object is returned.
        """
        """
        Fetch a table's GUID and its details based on its fully qualified name.

        :param client: An instance of AtlanClient.
        :param qualified_name: The fully qualified name of the table.
        :return: JSON representation of the table if found, else an empty JSON object.
        """
        base_url = self.channel.get("host")
        asset_type = asset_info.get("asset_type")
        try:
            # Initialize Atlan client
            client = AtlanClient(base_url=base_url, api_key=self.api_key)
            log_info(("qualified_name",qualified_name))
            # Fetch the table using the qualified name
            table = client.asset.get_by_qualified_name(
                asset_type=asset_type,
                qualified_name=qualified_name
            )
            
            # Check if the table is found
            if table:
                status = str(table.status)
                if status == 'EntityStatus.ACTIVE': #check if status of entity is active 
                    return table.guid # return active guid
                else:
                    return {}
            else:
                return json.dumps({})  # Return an empty JSON object if table is not found

        except Exception as e:
            log_error(f"Error fetching table: {str(e)}",e)
            if("server responded with a permission error" in str(e).lower()):
                raise e
            else:
                return json.dumps({})

    def __delete_terms(self, atlan_term_guids:List[Dict[str, Any]]) -> object:

        """
        Delete terms from DQLabs.

        This function deletes terms from the DQLabs glossary that are not present in the provided list of Atlan term GUIDs.
        It fetches all Atlan terms from the DQLabs database, compares them with the provided list of GUIDs, and performs the following actions:
        1. Unmaps terms from the `core.terms_mapping` table.
        2. Deletes associated version history entries from the `core.version_history` table.
        3. Deletes the term itself from the `core.terms` table.

        Args
        ----
        atlan_term_guids : List[Dict[str, Any]]
            A list of dictionaries containing the GUIDs of terms that should not be deleted. The function will delete any term not present in this list.

        Returns
        -------
        object
            The function performs several database operations (unmapping, deleting version history, and deleting the term), but does not return a value. It executes the queries and logs output statements.

        Example
        -------
        atlan_term_guids = [{"guid": "guid_1"}, {"guid": "guid_2"}]
        __delete_terms(atlan_term_guids)

        Notes
        -----
        - The function performs three SQL queries:
        1. A query to fetch the existing terms from the `core.terms` table.
        2. A query to delete the term mappings from the `core.terms_mapping` table.
        3. A query to delete the term version history from the `core.version_history` table.
        4. A query to delete the term itself from the `core.terms` table.
        - The function only deletes terms that are not present in the provided `atlan_term_guids` list.
        - For each term, the function logs output statements to indicate what has been deleted or unmapped.

        Errors
        ------
        If an error occurs while executing any query, it is expected that the function will handle and log the error appropriately.
        """
        """ Delete terms from dqlabs"""
        dqlabs_atlan_terms = []
        # fetch all atlan glossary terms in dqlabs
        dqlabs_terms_query = f"""
                            select id from core.terms
                            where source = '{ATLAN}' 
                            """
        dqlabs_atlan_terms_response = self.__run_postgres_query(
                                                    query_string = dqlabs_terms_query,
                                                    query_type='fetchall'
                                                       )
        if dqlabs_atlan_terms_response:
            dqlabs_atlan_terms = [term.get("id") for term in dqlabs_atlan_terms_response]
        
        if dqlabs_atlan_terms:
            term_to_be_deleted = [term_id for term_id in dqlabs_atlan_terms if term_id not in atlan_term_guids]
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
        
    def __delete_data_product_info(self, atlan_data_product_guid:List[Dict[str, Any]]) -> object:
        log_info(("atlan_data_product_guid for debug",atlan_data_product_guid))
        dqlabs_atlan_data_products_and_domains = []
       
        # fetch all atlan products in dqlabs
        dqlabs_data_product_and_domain_query = f"""
                            select id from core.product
                            where source = '{ATLAN}' 
                            and type in ('product')
                            """
        dqlabs_data_product_and_domain_query_response = self.__run_postgres_query(
                                                    query_string = dqlabs_data_product_and_domain_query,
                                                    query_type='fetchall'
                                                       )

        log_info(("dqlabs_data_product_and_domain_query_response",dqlabs_data_product_and_domain_query_response))
        if dqlabs_data_product_and_domain_query_response:
            dqlabs_atlan_data_products_and_domains= [data_product_domain.get("id") for data_product_domain in dqlabs_data_product_and_domain_query_response]
      
        log_info(("dqlabs_atlan_data_products for debug",dqlabs_atlan_data_products_and_domains))
        log_info(("Extended dqlabs_atlan_data_products for debug",dqlabs_atlan_data_products_and_domains))
        log_info(("Extended atlan_data_product_guid for debug",atlan_data_product_guid))

        data_products= [data_product 
                    for data_product in dqlabs_atlan_data_products_and_domains 
                    if data_product not in atlan_data_product_guid
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


    def __delete_domains(self, atlan_domain_guids:List[Dict[str, Any]]) -> object:

        """
        Delete glossary from DQLabs.

        This function deletes glossaries from the DQLabs glossary system that are not present in the provided list of Atlan glossary GUIDs.
        It performs the following actions for glossaries that are not in the provided list:
        1. Deletes any glossary mappings related to the glossary.
        2. Removes all associated categories.
        3. Deletes terms related to the categories.
        4. Unmaps and deletes categories related to the glossary.
        5. Deletes the glossary itself.

        Args
        ----
        atlan_glossary_guids : List[Dict[str, Any]]
            A list of dictionaries containing the GUIDs of glossaries that should not be deleted. The function will delete any glossary not present in this list.

        Returns
        -------
        object
            The function does not return any value. It performs several database operations (deleting mappings, categories, terms, etc.) and logs output statements.

        Example
        -------
        atlan_glossary_guids = [{"guid": "guid_1"}, {"guid": "guid_2"}]
        __delete_glossary(atlan_glossary_guids)

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
        log_info(("atlan_domain_guids for debug in delete domains",atlan_domain_guids))   
        dqlabs_atlan_domain = []
        # fetch all atlan glossary terms in dqlabs
        dqlabs_domain_query = f"""
                            select id from core.domain
                            where source = '{ATLAN}' 
                            and type in ('domain', 'category')
                            """
        dqlabs_domain_query_response = self.__run_postgres_query(
                                                    query_string = dqlabs_domain_query,
                                                    query_type='fetchall'
                                                       )
        log_info(("dqlabs_glossary_query_response",dqlabs_domain_query_response))
        if dqlabs_domain_query_response:
            dqlabs_atlan_domain= [domain.get("id") for domain in dqlabs_domain_query_response]
        log_info(("dqlabs_atlan_glossary for debug",dqlabs_atlan_domain))
        if dqlabs_atlan_domain:
            log_info(("inside if second"))
            domain_ids = [domain_id for domain_id in dqlabs_atlan_domain if domain_id not in atlan_domain_guids]
            log_info(("domain_ids to be deleted for debug",domain_ids))
            if domain_ids:
                domain_ids_list = "', '".join(domain_ids)

                """ 
                Delete Domains Mapping and Category ID
                """

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

    def delete_product_in_dqlabs(self, data_product_info:List[Dict[str, Any]]) -> object: 
        log_info(("data_product_info for debug",data_product_info))

        data_product_info_guid=[]

        for item in data_product_info:
            if item['status'] == 'ACTIVE':  # Check if the status is 'ACTIVE'
                data_product_info_guid.append(item['guid'])
            

        if data_product_info_guid:
            self.__delete_data_product_info(
                                atlan_data_product_guid = data_product_info_guid,
                                )  

    def delete_domains_in_dqlabs(self, glossary_info:List[Dict[str, Any]], atlan_domain_info) -> object:

        """
        Delete glossary, categories, and terms mapping in DQLabs if deleted in Atlan.

        This function handles the deletion of glossaries, categories, and terms in DQLabs if they are marked for deletion in Atlan.
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
        """ Delete glossary and terms mapping in dqlabs if delete in atlan"""
        
        category_guids = []
        term_guids = []
        log_info(("domains_info for debug",glossary_info))
        for item in glossary_info:
            if 'terms' in item:
                term_guids.extend(term['guid'] for term in item['terms'] if 'displayText' in term.keys())
        domain_guids = [domain['guid'] for domain in atlan_domain_info if 'guid' in domain.keys() and domain['status'] == 'ACTIVE']
            
        log_info(("domain_guids for debug",domain_guids))   
        log_info(("term_guids for debug",term_guids))
        # update terms  in dqlabs 
        if term_guids:
            self.__delete_terms(
                                atlan_term_guids = term_guids
                                )
    
        # update glossary  in dqlabs 
        if domain_guids:
            self.__delete_domains(
                    atlan_domain_guids = domain_guids
                    )  


    def delete_tags_in_dqlabs(self, tags_info:List[Dict[str, Any]]) -> object:

        """
        Delete tags from DQLabs that are not present in the provided Atlan tags information.

        This function deletes tags in DQLabs that are no longer present in the provided `tags_info` list. It first compares the
        tag GUIDs from DQLabs with the provided Atlan tag GUIDs and deletes those tags from the DQLabs system that are no longer
        present in Atlan.

        Args
        ----
        tags_info : List[Dict[str, Any]]
            A list of dictionaries containing information about the tags to be checked for deletion.
            Each dictionary contains the following keys:
            - 'guid': The GUID of the tag.

        Returns
        -------
        object
            The function does not return any value but performs the deletion operations in DQLabs.

        Example
        -------
        tags_info = [
            {"guid": "tag_guid_1"},
            {"guid": "tag_guid_2"}
        ]
        delete_tags_in_dqlabs(tags_info)

        Notes
        -----
        - The function first extracts tag GUIDs from the `tags_info` list.
        - Then it fetches all tags from the DQLabs system with the source 'ATLAN'.
        - It deletes the tags in DQLabs whose GUIDs are not found in the provided Atlan tag GUIDs.
        - Deletion is performed in two steps:
        1. **Tags Mapping**: The tags are first unmapped by deleting their corresponding entries in the `tags_mapping` table.
        2. **Tag Deletion**: The tags themselves are deleted from the `tags` table.

        Error Handling:
        ---------------
        - Errors during the deletion of tags or mappings will be logged, but the function does not raise exceptions.

        """
        """ Delete tags from dqlabs"""
        dqlabs_atlan_tags = []
        atlan_tags_guids = [tag.get("guid") for tag in tags_info] # fetch atlan tags guids
        # fetch all atlan glossary terms in dqlabs
        dqlabs_tags_query = f"""
                            select id from core.tags
                            where source = '{ATLAN}' 
                            """
        dqlabs_atlan_tags_response = self.__run_postgres_query(
                                                    query_string = dqlabs_tags_query,
                                                    query_type='fetchall'
                                                       )
        if dqlabs_atlan_tags_response:
            dqlabs_atlan_tags = [tag.get("id") for tag in dqlabs_atlan_tags_response]

        log_info(("tags_info",tags_info))
        if dqlabs_atlan_tags:
            tags_to_remove = [tag_id for tag_id in dqlabs_atlan_tags if tag_id not in atlan_tags_guids]
            if tags_to_remove:
                tag_ids = "', '".join(tags_to_remove)
                remove_tags_mapping_query = f"""
                                                delete from core.tags_mapping
                                                where tags_id in ('{tag_ids}')
                                            """
                log_info(("remove_tags_mapping_query",remove_tags_mapping_query))
                self.__run_postgres_query(
                                query_string = remove_tags_mapping_query,
                                query_type = 'delete',
                                output_statement = f"{tags_to_remove} unmapped from tags mapping")
                remove_tags_id = f"""
                                    delete from core.tags
                                    where (id in ('{tag_ids}') or parent_id in ('{tag_ids}'))
                                    AND id NOT IN (SELECT tags_id FROM user_mapping WHERE tags_id IS NOT NULL)
                                """
                # delete the term
                self.__run_postgres_query(
                                query_string = remove_tags_id,
                                query_type = 'delete',
                                output_statement = f"{tag_ids} deleted from tags") 
