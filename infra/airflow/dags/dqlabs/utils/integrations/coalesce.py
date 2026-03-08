import json
import requests
import pytz
from uuid import uuid4

from typing import List, Dict, Any, Optional
from urllib.parse import urljoin

from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone
from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.app_helper.crypto_helper import decrypt
from dqlabs.enums.connection_types import ConnectionType 
from dqlabs.app_helper.crypto_helper import decrypt

from dqlabs.app_constants.dq_constants import COALESCE
from dqlabs.app_helper.catalog_helper import (get_asset_metrics, get_attributes_metrics)
from datetime import datetime, timezone

class CoalesceCatalog:

    def __init__(self, config, channel):
        self.config = config
        log_info(('config for debug',self.config))
        self.channel = channel
        log_info(('channel for debug',self.channel))
        self.dq_url = self.channel.get("dq_url")
        self.connection = get_postgres_connection(self.config)
        self.warehouse_connection_cache = {}
        self.tag_propagation = (
            config.get('dag_info', {})
            .get('settings', {})
            .get('discover', {})
            .get('tag_propagation',{})
            .get('is_active', False)
        )
        log_info(("tag_propagation",self.tag_propagation))
        self.organization_id = config.get('organization_id')
        if not self.organization_id:
            self.organization_id = config.get('dag_info', {}).get('organization_id')
        

    def __call_api_request(self, endpoint: str, method_type: str, payload: json = None) -> Dict[str, Any]:
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
            api_key = decrypt(self.channel.get("api_key"))
            if not api_key:
                raise ValueError("Failed to retrieve API key")

            headers = {
                'Authorization': f'Token {api_key}',
                'Content-Type': 'application/json'
            }

            if method_type == "post":
                response = requests.post(
                    url=endpoint, headers=headers, data=payload)
                if response.status_code in [200, 201, 202, 204]:
                    if "errors" in response.json():
                    # Extract error message if present
                        message = response.json()["errors"][0].get("message", "Unknown error")
                        raise ValueError(f"API Error: {message}")
                    return response
            else:
                response = requests.get(url=endpoint, headers=headers)

            if (response and response.status_code in [200, 201, 202, 204]):
                if response.status_code in [204]:
                    return response
                return response.json()

            else:
                raise ValueError(response.raise_for_status())
        except Exception as e:
            log_info(
                f"Coalesce Connector - Get Response Error, {e}")
            
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
                log_info("coalesce Connector - Query failed Error", e)
            
            if query_type not in ['insert','update','delete']:
                if query_type == 'fetchone':
                    records = fetchone(cursor)
                elif query_type == 'fetchall':
                    records = fetchall(cursor)
                
                return records
            
            log_info(("Query run:", query_string))
            log_info(('Query Executed',output_statement))

    def fetch_alldata(self, url: str, payload: str, operation: str) -> List[Dict]:
        """
        Fetches all data from a paginated API endpoint.
        
        Args:
            page (str): The current page number to fetch.
            url (str): The API endpoint URL.
            payload (str): The GraphQL query payload with pagination placeholders.
        
        Returns:
            List[Dict]: A list of dictionaries containing the fetched data.
        """
        all_data = []
        current_page = 0

        while True:
            response = self.fetch_paginated_data(
                page=current_page, url=url, payload=payload, operation=operation
            )
            data = response
            if not data:
                break

            all_data.extend(data)
            if len(data) < 500 :
                break

            current_page += 1
        return all_data
    

    def fetch_paginated_data(
        self,
        page: str,
        url: str,
        payload: str,
        operation: str,
    ) -> List[Dict]:
        """
        A common method to fetch paginated data from GraphQL API endpoints.
        
        Args:
            operation: The GraphQL operation name (e.g., "getTables")
            query_template: The GraphQL query template with pagination placeholders
            data_path: Path to the data in the response (e.g., ["data", "getTables", "data"])
            variables: Variables to include in the GraphQL query
            page_size: Number of items per page
            max_pages: Maximum number of pages to fetch (None for all available)
            **kwargs: Additional parameters to format into the query template
            
        Returns:
            List of all items from paginated responses
            
        Raises:
            Exception: If API request fails or data parsing fails
        """
        try:
            payload = payload.replace("{page}", str(page))
            response = self.__call_api_request(url, 'post', payload=payload)
            return response.json().get("data", {}).get(operation, {}).get("data", [])


        except Exception as e:
            log_info(f"Failed to fetch paginated data for {url}: {str(e)}")
            raise

    def __coalesce_channel_configuration(self) -> Dict[str, Any]:
                """
                Retrieves and returns the import and export configurations for Coalesce catalog from `self.channel`.
                
                This function logs the channel configuration, extracts the "import" and "export" settings,
                and ensures the "export" configuration is properly parsed from JSON format.
                
                Returns:
                    Tuple[Dict[str, Any], Dict[str, Any]]: The import configuration (dict) and export configuration (dict).
                """
                log_info(("coalesce_configuration",self.channel))
                import_config = self.channel.get("import", {})
                export_config = json.loads(self.channel.get("export"))
                return import_config, export_config

    def  fetch_tags_domains(self) -> List[Dict[str, Any]]:
            """
            Fetches tags and domains from the Coalesce API.
            
            Returns:
                List[Dict[str, Any]]: A list of dictionaries containing tags and domains.
            """
            url = "https://api.castordoc.com/public/graphql?op=getTags"
            page_size = 500  # Number of records per page
            payload = """{{
                "query": "query {{\\n  getTags (\\n    pagination : {{\\n        nbPerPage: {page_size}\\n        page: {page}\\n    }}\\n  ){{\\n    totalCount\\n    data {{\\n      id\\n      label\\n    }}\\n  }}\\n}}",
                "variables": {{}}
            }}""".format(page_size=page_size, page="{page}")
            response = self.fetch_alldata(url, payload, operation ="getTags")

            # Parse JSON response
            try:

                domains = []
                tags = []
                if response :
                    for item in response:
                        tag_id = item["id"]
                        label = item["label"]

                        tag_object = {"id": tag_id, "name": label}

                        if label.lower().startswith("domain:"):
                            domain_name = label.split("domain:")[1].strip().replace(" ", "")
                            domains.append({"id": tag_id, "name": domain_name})
                        elif not label.lower().startswith("dq_score:"):
                            tags.append(tag_object)

                return domains, tags

            except Exception as e:
                log_info("Error parsing response from Coalesce API", e)
                log_info("Raw response:", response.text)

    def fetch_terms (self) -> List[Dict[str, Any]]:
        """
        Fetches terms from the Coalesce API.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing term information.
        """
        url = "https://api.castordoc.com/public/graphql?op=getTerms"
        page_size = 500  # Number of records per page
        payload = f"""{{
            "query": "query {{\\n  getTerms (\\n    pagination:\\n    {{\\n        nbPerPage:{page_size}\\n        page:{{page}}\\n    }}\\n  ){{\\n    totalCount\\n    data {{\\n      id\\n      name\\n      parentTerm {{\\n        id\\n        name\\n      }}\\n      childrenTerms {{\\n        id\\n        name\\n      }}\\n    }}\\n  }}\\n}}",
            "variables": {{}}
        }}"""

        response = self.fetch_alldata(url, payload, operation ="getTerms")
        terms_data = response
    
    # First create a dictionary for quick lookup
        term_dict = {term['id']: term for term in terms_data}
        
        result = []
        
        for term in terms_data:
    # For each term, build its hierarchical name
            current_term = term
            parent_names = []
            
            # Walk up the parent chain
            while current_term.get('parentTerm'):
                parent_id = current_term['parentTerm']['id']
                if parent_id in term_dict:
                    parent_term = term_dict[parent_id]
                    parent_names.append(parent_term['name'])
                    current_term = parent_term
                else:
                    break
            
            # Format as child(parent.grandparent)
            if parent_names:
                hierarchical_name = f"{term['name']}({'.'.join(reversed(parent_names))})"
            else:
                hierarchical_name = term['name']
            
            result.append({
                'id': term['id'],
                'name': term['name'],
                'technical_name': hierarchical_name
            })
        

        return result


    def delete_domains_in_dqlabs(self, coalesce_domain_ids: List[Dict[str, Any]]) -> object:
        """
        Deletes domains in DQLabs that are no longer present in Coalesce.

        Args:
            (List[Dict[str, Any]]): List of dictionaries containing Coalesce domain IDs.

        Returns:
            object: The result of the delete operation.
        """
        dqlabs_coalesce_domains = []
        # fetch all coalesce domains in dqlabs
        dqlabs_domain_query = f"""
                            select id from core.domain
                            where source = '{COALESCE}' 
                            and type = 'domain'
                            """
        dqlabs_domain_query_response = self.__run_postgres_query(
            query_string=dqlabs_domain_query, query_type="fetchall"
        )
        if dqlabs_domain_query_response:
            dqlabs_coalesce_domains = [
                domain.get("id") for domain in dqlabs_domain_query_response
            ]

        if dqlabs_coalesce_domains:
            for domain_id in dqlabs_coalesce_domains:
                if domain_id not in coalesce_domain_ids:
                    """ 
                    Delete Domains Mapping related to Domain ID
                    """
                    # Get all the categories for the domain id
                    domain_category_query = f"""
                                        select id from core.domain
                                        where parent_id = '{domain_id}'
                                        and source = '{COALESCE}'
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
                                            and source = '{COALESCE}'
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
                                                    where domain_id = '{domain_id}'
                                                """
                    # delete category mapping for id
                    self.__run_postgres_query(
                        query_string=remove_domain_mapping_query,
                        query_type="delete",
                        output_statement=f"{domain_id} unmapped from domain mapping",
                    )

                    remove_domain_id = f"""
                                        delete from core.domain
                                        where id = '{domain_id}'
                                        and type = 'domain'
                                    """
                    # delete the domain
                    self.__run_postgres_query(
                        query_string=remove_domain_id,
                        query_type="delete",
                       
                        output_statement=f"{domain_id} deleted from categories",
                    )

    def insert_colesce_domains_to_dqlabs(
        self, domains_info: List[Dict[str, Any]]
    ) -> object:
        """
        Fetch and insert all domain metrics from Coalesce to Dqlabs.

        It checks whether the domain already exists in Dqlabs, inserts it if necessary,
        and proceeds to handle the associated categories and terms, inserting them into 
        the relevant tables in Dqlabs.

        Parameters
        ----------
        domains_info : list of dict
            A list of dictionaries containing domain information retrieved from COALESCE.
            Each dictionary should contain details like `id`, `name`

        Returns
        -------
        object
            The result of the insert operation. This can be a confirmation or error message based 
            on the success or failure of the database insertions.

        Notes
        -----
        - This function will insert domains into the Dqlabs system.
        - The function ensures that duplicate entries are not created by checking the existence 
        of each item (domain) before insertion.

        """
        log_info(("domain_info", domains_info))
        insert_values = []
        domains_info = [domain for domain in domains_info 
                    if domain.get("id") 
                    and domain.get("name") 
                    and domain["name"].strip()
                    and not domain["name"].isspace() ]
        for domain in domains_info:
            domain_id = domain.get("id")
            log_info(("domain_id", domain_id))
            domain_name = domain.get("name")
            domain_name = domain_name.strip()
            log_info(("domain_name for category", domain_name))

            # check if domain_id exists
            domain_check_query = f"""
                select exists (
                    select 1
                    from core."domain"
                    where id = '{domain_id}'
                    and source = '{COALESCE}'
                    and is_active = true and is_delete = false
                );
            """
            domain_check_flag = self.__run_postgres_query(
                domain_check_query, query_type="fetchone"
            )
            domain_check_flag = domain_check_flag.get("exists", False)
            log_info(("domain_check_flag", domain_check_flag))

            if not domain_check_flag:
                # Prepare query input
                query_input = (
                    domain_id,
                    domain_name,
                    domain_name,
                    None,  # Description
                    str(self.organization_id),
                    None,  # Parent ID
                    "domain",
                    0,  # Level
                    None,  # Domain
                    True,  # is_active
                    False,  # is_delete
                    COALESCE,  # Source
                    json.dumps(
                        {"type": COALESCE, "coalesce_domain_id": domain_id},
                        default=str,
                    ),
                )
                insert_values.append(query_input)

        if insert_values:
            try:
                with self.connection.cursor() as cursor:
                    input_literals = ", ".join(["%s"] * len(insert_values[0]))
                    values_sql = b",".join([
                        cursor.mogrify(f"({input_literals}, CURRENT_TIMESTAMP)", vals)
                        for vals in insert_values
                    ]).decode("utf-8")

                    insert_domain_query = f"""
                        INSERT INTO core.domain(
                            id, name, technical_name, description, organization_id, parent_id, type, level, domain,
                            is_active, is_delete, source, properties, created_date
                        )
                        VALUES {values_sql}
                        RETURNING id
                    """
                    log_info(("insert_domain_query", insert_domain_query))

                    execute_query(self.connection, cursor, insert_domain_query)
                    log_info((f"{len(insert_values)} domain(s) inserted into Dqlabs."))
            except Exception as e:
                log_info("Insert domain query failed", e)

    
    def delete_tags_in_dqlabs(self, tags_info:List[Dict[str, Any]]) -> object:

        """
        Delete tags from DQLabs that are not present in the provided COALESCE tags information.

        This function deletes tags in DQLabs that are no longer present in the provided `tags_info` list. It first compares the
        tag GUIDs from DQLabs with the provided COALESCE tag GUIDs and deletes those tags from the DQLabs system that are no longer
        present in COALESCE.

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
        - Then it fetches all tags from the DQLabs system with the source 'COALESCE'.
        - It deletes the tags in DQLabs whose GUIDs are not found in the provided COALESCE tag GUIDs.
        - Deletion is performed in two steps:
        1. **Tags Mapping**: The tags are first unmapped by deleting their corresponding entries in the `tags_mapping` table.
        2. **Tag Deletion**: The tags themselves are deleted from the `tags` table.

        Error Handling:
        ---------------
        - Errors during the deletion of tags or mappings will be logged, but the function does not raise exceptions.

        """
        """ Delete tags from dqlabs"""
        dqlabs_coalesce_tags = []
        coalesce_tags_ids = [tag.get("id") for tag in tags_info] # fetch coalesce tags ids
        # fetch all coalesce glossary terms in dqlabs
        dqlabs_tags_query = f"""
                            select id from core.tags
                            where source = '{COALESCE}' 
                            """
        dqlabs_coalesce_tags_response = self.__run_postgres_query(
                                                    query_string = dqlabs_tags_query,
                                                    query_type='fetchall'
                                                       )
        if dqlabs_coalesce_tags_response:
            dqlabs_coalesce_tags = [tag.get("id") for tag in dqlabs_coalesce_tags_response]

        log_info(("tags_info",tags_info))
        if dqlabs_coalesce_tags:
            tags_to_remove = [tag_id for tag_id in dqlabs_coalesce_tags if tag_id not in coalesce_tags_ids]
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
                                    where id in ('{tag_ids}')
                                """
                # delete the term
                self.__run_postgres_query(
                                query_string = remove_tags_id,
                                query_type = 'delete',
                                output_statement = f"{tag_ids} deleted from tags") 


    def insert_coalesce_tags_to_dqlabs(self, tags_info: List[Dict[str, Any]]) -> None:
        """
        Inserts Coalesce tags into the DQLabs system in bulk if they do not already exist.

        Parameters:
            tags_info (List[Dict[str, Any]]): List of tag dictionaries with keys 'id' and 'name'.
        """
        try:
            log_info(("tags_info", tags_info))
            tags_info =  [tag for tag in tags_info if not tag.get("name", "").startswith("dq_score")  and tag.get("name", "").strip()]

            insert_values = []

            for tag in tags_info:
                tag_id = tag.get("id")
                tag_name = tag.get("name")
                tag_name = tag_name.strip()
                log_info(("Processing tag", tag_id, tag_name))
                # Check if the tag already exists
                tag_check_query = f"""
                    SELECT EXISTS (
                        SELECT 1 FROM core.tags
                        WHERE id = '{tag_id}' AND source = '{COALESCE}'
                        AND is_active = TRUE AND is_delete = FALSE
                    );
                """
                tag_exists = self.__run_postgres_query(tag_check_query, query_type='fetchone').get("exists", False)
                log_info(("tag_exists", tag_exists))

                if not tag_exists:
                    insert_values.append(
                        (
                            tag_id,
                            tag_name,
                            tag_name,
                            None,  # Description
                            "#21598a",  # Color
                            COALESCE,  # db_name
                            False,  # native_query_run
                            json.dumps({"type": COALESCE}, default=str),  # properties
                            1,  # order
                            False,  # is_mask_data
                            True,  # is_active
                            False,  # is_delete
                            str(self.organization_id),  # organization_id
                            COALESCE,  # source
                        )
                    )

            if insert_values:
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
                    log_info(("insert_tags_query", insert_tags_query))
                    execute_query(self.connection, cursor, insert_tags_query)
                    log_info(f"Inserted {len(insert_values)} tag(s) into DQLabs.")
            else:
                log_info("No new tags to insert.")

        except Exception as e:
            log_info("Bulk insert of Coalesce tags failed", e)
            raise e


    def delete_terms_in_dqlabs(self, terms_info: List[Dict[str, Any]]) -> object:
        dqlabs_coalesce_terms = []
        coalesce_terms_ids = [term.get("id") for term in terms_info] # fetch coalesce terms ids
        # fetch all COALESCE glossary terms in dqlabs
        dqlabs_terms_query = f"""
                            select id from core.terms
                            where source = '{COALESCE}' 
                            """
        dqlabs_coalesce_terms_response = self.__run_postgres_query(
                                                    query_string = dqlabs_terms_query,
                                                    query_type='fetchall'
                                                       )
        if dqlabs_coalesce_terms_response:
            dqlabs_coalesce_terms = [term.get("id") for term in dqlabs_coalesce_terms_response]

        if dqlabs_coalesce_terms:
            term_to_be_deleted = [term_id for term_id in dqlabs_coalesce_terms if term_id not in coalesce_terms_ids]
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
                

    def insert_coalesce_terms_to_dqlabs(
    self, terms_info: List[Dict[str, Any]]
) -> object:
        """
        Inserts Knowledge terms from Coalesce into the DQLabs database.
        """
        if not terms_info:
            log_info("No terms found to insert.")
            return
        terms_info =  [term for term in terms_info 
                  if term.get("id") 
                  and term.get("name") 
                  and term["name"].strip() 
                  and not term["name"].isspace()
                  and term.get("technical_name") 
                  and term["technical_name"].strip()
                  and not term["technical_name"].isspace()]
        insert_values = []

        for term in terms_info:
            term_id = term.get("id")
            term_name = term.get("name")
            term_name = term_name.strip()
            term_technical_name = term.get("technical_name", term_name)
            term_technical_name = term_technical_name.strip()

            log_info(("Processing term", term_name, "ID", term_id))

            # Check if term already exists
            term_check_query = f"""
                SELECT EXISTS (
                    SELECT 1 FROM core.terms
                    WHERE id = '{term_id}' AND source = '{COALESCE}'
                    AND is_active = TRUE AND is_delete = FALSE
                );
            """
            term_check_flag = self.__run_postgres_query(
                term_check_query, query_type="fetchone"
            )
            term_exists = term_check_flag.get("exists", False)

            if not term_exists:
                query_input = (
                    term_id,
                    term_name,
                    term_technical_name,
                    None,
                    str(self.organization_id),
                    None,
                    None,
                    True,
                    False,
                    COALESCE,
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
                insert_values.append(query_input)

        if insert_values:
            try:
                with self.connection.cursor() as cursor:
                    input_literals = ", ".join(["%s"] * len(insert_values[0]))
                    values_sql = b",".join([
                        cursor.mogrify(f"({input_literals}, CURRENT_TIMESTAMP)", vals)
                        for vals in insert_values
                    ]).decode("utf-8")

                    insert_query = f"""
                        INSERT INTO core.terms(
                            id, name, technical_name, description, organization_id, domain_parent_id, domain_id,
                            is_active, is_delete, source, status, derived_type, threshold, sensitivity,
                            is_null, is_blank, is_unique, is_primary_key,
                            tags, enum, contains, synonyms, created_date
                        )
                        VALUES {values_sql}
                        RETURNING id;
                    """
                    log_info(("Executing insert query", insert_query))
                    execute_query(self.connection, cursor, insert_query)
                    log_info((f"Inserted {len(insert_values)} terms into DQLabs"))
            except Exception as e:
                log_info("Failed to bulk insert terms", e)


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
        >>> log_info(assets)
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
            log_info(
                f"Coalesce Connector - Get Assets Error", e)
        return assets

    def fetch_id_for_asset(self, asset_info):
        """
        Fetches the ID for a given asset using Azure coalesce API with proper pagination termination
        """
        all_assets = []
        try:
            host = self.channel.get("host")
            asset_name = asset_info.get("asset_name")
            url = f"https://api.castordoc.com/public/graphql?op=getTables"
            # Pagination parameters
            page_size = 500
            
            payload = f"""{{
                "query": "query {{\\n  getTables (scope: {{\\n   nameContains: \\\"{asset_name}\\\"\\n  }}\\n   pagination: {{\\n    nbPerPage: {page_size}\\n    page: {{page}}\\n  }}\\n  ) {{ \\n      totalCount\\n    data  {{\\n      id\\n      name\\n      tagEntities {{\\n        tag {{\\n          id\\n          label\\n        }}\\n      }}\\n      schema {{\\n        id\\n        name\\n        database {{\\n          id\\n          name\\n          warehouse {{\\n            id\\n            name\\n          }}\\n        }}\\n      }}\\n    }}\\n  }}\\n}}",
                "variables": {{}}
                }}"""
            page = 0


            while True:
                data = self.fetch_paginated_data(page, url, payload, operation="getTables")
            
                log_info(f"Completed fetching {len(data)} assets {data}")
            
                if not data:
                    log_info(f"No assets found for: {asset_name}")
                    return None
                connection_type = asset_info.get("connection_type")
                schema = asset_info.get("asset_schema", '')
                database_name = asset_info.get("asset_database", '').lower()
                asset_properties = asset_info.get("asset_properties") if connection_type.lower() == ConnectionType.ADLS.value else ''
                
                filtered_assets = self.filter_records(
                    data, 
                    connection_type, 
                    asset_name, 
                    database_name, 
                    schema, 
                    asset_properties
                )
                

                page += 1
            
                if filtered_assets:
                    return filtered_assets[0]["id"]
            if not filtered_assets:
                    log_info(f"No matching asset found for: {asset_name} with the specified criteria")
            
        except Exception as e:
            log_info(f"Failed to fetch asset ID: {str(e)}")
            raise

    def _fetch_and_cache_warehouse_connections(self):
        """Fetches all sources and caches their connection types."""
        try:
            url = "https://api.castordoc.com/public/graphql?op=getSources"
            page_size = 500
            payload = f"""{{
                    "query": "query {{\\n  getSources (\\n    pagination: {{\\n        nbPerPage: {page_size},\\n        page: {{page}}\\n    }}\\n  ){{\\n    totalCount\\n    greatestId\\n    data {{\\n        id\\n        name\\n        origin\\n        type\\n        technology\\n    }}\\n  }}\\n}}",
                    "variables": {{}}
                }}"""

            response = self.fetch_alldata(url, payload, operation="getSources")

            for source in response:
                tech = source.get("technology", "").lower()
                if tech == "sqlserver":
                    tech = "mssql"  # Normalize to your expected format
                self.warehouse_connection_cache[source["id"]] = tech

        except Exception as e:
            log_info("Failed to fetch warehouse connections", e)

    def get_connection_type_from_warehouse_id(self, warehouse_id):
        """Returns cached connection type (avoids repeated API calls)."""
        if not self.warehouse_connection_cache:
            self._fetch_and_cache_warehouse_connections()  # Lazy-load on first call

        return self.warehouse_connection_cache.get(warehouse_id, "")
    

    def filter_records(self, assets_data, connection_type, asset_name, database_name, schema_name, asset_properties):
        """
        Filters the records based on the provided criteria.

        Args:
            assets_data (list): The list of asset records to filter.
            connection_type (str): The type of connection (e.g., 'mssql', 'adls').
            asset_name (str): The name of the asset to match.
            database_name (str): The name of the database to match.
            asset_properties (dict): Additional properties of the asset to match.

        Returns:
            list: A list of filtered records that match the criteria.
        """
        filtered_records = []
        for record in assets_data:
            db = record.get("schema", {}).get("database", {})
            schema = record.get("schema", {})
            warehouse = db.get("warehouse", {})

            if not all([record.get("name"), db.get("name"), warehouse.get("id")]):
                continue  # Skip incomplete records

            warehouse_id = warehouse["id"]
            actual_conn_type = self.get_connection_type_from_warehouse_id(warehouse_id)
             # Normalize asset name for comparison

            if (
                record.get("name").lower() == asset_name.lower()
                and schema.get("name", "").lower() == schema_name.lower()
                and db.get("name", "").lower() == database_name.lower()
                and actual_conn_type.lower() == connection_type.lower()
            ):
                filtered_records.append(record)

        return filtered_records

    def fetch_coalesce_asset_info(self,asset_guid):
        """
        Fetches asset information from Coalesce, including associated tags.

        Parameters:
        asset_guid (str): The GUID of the asset to fetch from Coalesce.

        Returns:
        tuple: A tuple containing asset data and a list of tags mapped with generated IDs.
        """
        host = self.channel.get("host")
        # Construct the URL for the token request
        url = f"""https://api.castordoc.com/public/graphql?op=getTables\""""
        payload = f"{{\"query\":\"query {{\\n  getTables (scope: {{\\n   ids : \\\"{asset_guid}\\\"\\n  }}\\n  ) {{\\n    data {{\\n      id\\n      name\\n      tagEntities {{\\n        tag {{\\n                id\\n                label\\n              }}\\n      }}\\n      schema {{\\n        id\\n        name\\n        database {{\\n          id\\n          name\\n          warehouse {{\\n            id\\n            name\\n          }}\\n        }}\\n      }}\\n    }}\\n  }}\\n}}\",\"variables\":{{}}}}"

        # fetch the attribute information from coalesce
        asset_data = self.__call_api_request(url, "post", payload)
        try:
            result = asset_data.json()

            tags = []
            domains = []

            if "data" in result and "getTables" in result["data"]:
                for item in result["data"]["getTables"]["data"]:
                    for tag_entry in item.get("tagEntities", []):
                        tag_info = tag_entry.get("tag", {})
                        tag_id = tag_info.get("id")
                        label = tag_info.get("label", "")

                        if not tag_id or not label:
                            continue

                        tag_object = {"id": tag_id, "name": label}

                        if label.lower().startswith("domain:"):
                            domain_name = label.split("domain:")[1].strip().replace(" ", "")
                            domains.append({"id": tag_id, "name": domain_name})
                        else:
                            tags.append(tag_object)

        except Exception as e:
            log_info("Error parsing response from Coalesce API", e)
        return asset_data, domains, tags

    def map_asset_metrics(
        self, asset_info, coalesce_asset_info, tags_info, pull_tags, domains_info, pull_domains
    ):
        """
        Maps asset metrics from Coalesce to DQLabs format.

        Parameters:
        - asset_info (dict): The asset information from DQLabs.
        - coalesce_asset_info (dict): The asset information from Coalesce.
        - tags_info (list): The list of tags associated with the asset.
        - pull_tags (bool): Flag indicating whether to pull tags.
        - domains_info (list): The list of domains associated with the asset.
        - pull_domains (bool): Flag indicating whether to pull domains.
        """
        asset_id = asset_info.get("asset_id")
        if pull_tags:
            # Update the tags for dqlabs asset
            tags_list = [tag for tag in tags_info if not tag.get("name", "").startswith("dq_score")]
            insert_values = []
            mapped_tag_ids = []
            if tags_list:
                for tag_mapped in tags_list:
                    tag_mapped_id = tag_mapped.get("id")
                    mapped_tag_ids.append(tag_mapped_id)
                    #check if tag id in tags table
                    asset_tag_mapped_check_query = f"""
                                        select exists (
                                        select 1
                                        from core."tags_mapping"
                                        where tags_id = '{tag_mapped_id}'
                                        and asset_id = '{asset_id}'
                                        and level = 'asset'
                                        );
                                    """
                    asset_tag_mapped = self.__run_postgres_query(asset_tag_mapped_check_query,
                                                                query_type='fetchone'
                                                                )
                    asset_tag_mapped = asset_tag_mapped.get("exists",False)
                    log_info(("asset_tag_mapped for debug",asset_tag_mapped))
                    if not asset_tag_mapped:
                        #  map coalesce terms to attributes
                        insert_values.append(
                            (
                                str(uuid4()),  # Unique ID
                                'asset',  # Level
                                asset_id,  # Asset ID
                                None,  # Attribute ID
                                tag_mapped_id,  # Tag ID
                            )
                        )
                try:
                    if insert_values:
                        # Prepare attribute-tag mapping insert data
                        log_info(("insert_values for asset_tag_mapping", insert_values))
                        with self.connection.cursor() as cursor:
                            placeholders = ", ".join(["%s"] * len(insert_values[0]))
                            values_sql = b",".join([
                                cursor.mogrify(f"({placeholders}, CURRENT_TIMESTAMP)", vals)
                                for vals in insert_values
                            ]).decode("utf-8")

                            # Insert attribute-tag mapping query
                            insert_asset_tag_query = f"""
                                INSERT INTO core.tags_mapping(
                                    id, level, asset_id, attribute_id, tags_id, created_date
                                )
                                VALUES {values_sql}
                                RETURNING id;
                            """

                            log_info(("insert_asset_tag_query", insert_asset_tag_query))
                            execute_query(
                                self.connection, cursor, insert_asset_tag_query
                            )

                except Exception as e:
                    log_info("Bulk insert tag_mapping failed", e)
                    raise e
                mapped_tag_ids = "', '".join(mapped_tag_ids)
                delete_tags_mapping_retrieve_query=f"""DELETE FROM core.tags_mapping tm using core.tags t where t.id = tm.tags_id
                and tm.asset_id = '{asset_id}' and tm.tags_id not in ('{mapped_tag_ids}') and tm.level = 'asset'  and t.source = '{COALESCE}'"""
                delete_tags_mapping_retrieve=self.__run_postgres_query(delete_tags_mapping_retrieve_query,
                                                                query_type='delete'
                                                                )
                log_info(("delete_tags_mapping_retrieve_query",delete_tags_mapping_retrieve_query))
            else:
                delete_tags_mapping_retrieve_query=f"""DELETE FROM core.tags_mapping tm using core.tags t where t.id = tm.tags_id
                and tm.asset_id = '{asset_id}' and tm.level = 'asset'  and t.source = '{COALESCE}'"""
                delete_tags_mapping_retrieve=self.__run_postgres_query(delete_tags_mapping_retrieve_query,
                                                                query_type='delete'
                                                                )
                log_info(("delete_tags_mapping_retrieve_query",delete_tags_mapping_retrieve_query))

        if pull_domains:
            values_sql = []
            insert_values = []
            domains_ids = []
            domains_info = [domain for domain in domains_info 
                    if domain.get("id") 
                    and domain.get("name") 
                    and domain["name"].strip()
                    and not domain["name"].isspace()]
            if domains_info:
                for data_domain in domains_info:

                    domain_id = data_domain.get("id")
                    domains_ids.append(domain_id)

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
                        # Prepare mapping insert data
                        insert_values.append ((
                        uuid4(),  # Domain ID
                        'asset',
                        asset_id,
                        None,
                        domain_id,
                    ))
                try:
                    if insert_values:
                        with self.connection.cursor() as cursor:
                            placeholders = ", ".join(["%s"] * len(insert_values[0]))
                            values_sql = b",".join([
                                cursor.mogrify(f"({placeholders}, CURRENT_TIMESTAMP)", vals)
                                for vals in insert_values
                            ]).decode("utf-8")

                            # Insert attribute-tag mapping query
                            insert_asset_tag_query = f"""
                                INSERT INTO core.domain_mapping(
                                    id, level, asset_id, measure_id, domain_id, created_date
                                )
                                VALUES {values_sql}
                                RETURNING id;
                            """
                            log_info(("insert_asset_tag_query", insert_asset_tag_query))
                            execute_query(
                                self.connection, cursor, insert_asset_tag_query
                            )
                                # Generate placeholders for the query using %s
                except Exception as e:
                    log_info(
                        f"Domain with id {domain_id} mapped to asset", e)
                mapped_domain_ids = "', '".join(domains_ids)
                delete_domain_mapping_retrieve_query=f"""DELETE FROM core.domain_mapping dm using core.domain d where d.id = dm.domain_id
                and dm.asset_id = '{asset_id}' and dm.domain_id not in ('{mapped_domain_ids}') and dm.level = 'asset'  and d.source = '{COALESCE}'"""
                delete_tags_mapping_retrieve=self.__run_postgres_query(delete_domain_mapping_retrieve_query,
                                                                query_type='delete'
                                                                )
                log_info(("delete_tags_mapping_retrieve_query",delete_tags_mapping_retrieve_query))
            else:
                delete_domain_mapping_retrieve_query=f"""DELETE FROM core.domain_mapping dm using core.domain d where d.id = dm.domain_id
                and dm.asset_id = '{asset_id}' and dm.level = 'asset'  and d.source = '{COALESCE}'"""
                delete_tags_mapping_retrieve=self.__run_postgres_query(delete_domain_mapping_retrieve_query,
                                                                query_type='delete'
                                                                )
                log_info(("delete_tags_mapping_retrieve_query",delete_tags_mapping_retrieve_query))


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
            attribute_id = attributes.get("attribute_id") if attributes else None
            return attribute_id
        except Exception as e:
            log_info(
                f"coalesce Connector - Get Response Error {e}")


    def map_attribute_metrics(self, asset_info: List[Dict[str, Any]], coalesce_info: Dict[str, Any], active_attributes, tags_info: List[Dict[str, Any]], map_tags:bool = False) -> object:
        """
        Maps terms and tags from Coalesce to attributes in a DQLabs asset.

        Ensures correct  tags are linked to attributes and removes outdated mappings.

        Args:
            asset_info (List[Dict[str, Any]]): Information about the asset.
            coalesce_asset_data (dict): Data from Coalesce containing asset details.
            tags_info (List[Dict[str, Any]]): List of tag information.
            terms_info (List[Dict[str, Any]]): List of term information.
            map_domains (bool, optional): Flag to determine whether to map domains. Defaults to False.
            map_tags (bool, optional): Flag to determine whether to map tags. Defaults to False.

        Returns:
            object: Logs of the mapping process.
        """
        try:
            log_info(f"Starting map_attribute_metrics for asset: {asset_info.get('asset_name')}")
            asset_id = asset_info.get("asset_id")
            # log_info(("term_info for debug",terms_info))
            log_info(("tags-info",tags_info))
            log_info(("active_attributes",active_attributes))
            
            """ Map terms and tags to attributes """
            for i, attribute in enumerate(active_attributes):
                try:
                    attribute_guid = attribute.get("id")
                    log_info(("attribute_guid",attribute_guid))
                    attribute_name = attribute.get("name")
                    log_info(("attribute_name",attribute_name))
                    attribute_id = self.get_attribute_id_from_name(asset_id=asset_id,
                                                                            attribute_name=attribute_name)
                    log_info(("attribute_id",attribute_id))
                    log_info(map_tags)

                    if map_tags and attribute_id:
                        log_info(("map_tags for debug",map_tags))
                        attribute_tags  = attribute.get("tagEntities", [])
                        log_info(("attribute_tag_classifications for debug",attribute_tags))
                        if attribute_tags:
                            for tag in tags_info:
                                if tag.get("name") not in [tag_name.get("tag",{}).get("label") for tag_name in attribute_tags]:
                                    fetch_tag_mappings_query=f"""select id from core.tags_mapping where tags_id= '{tag.get("id")}' and attribute_id='{attribute_id}'"""
                                    fetch_tag=self.__run_postgres_query(fetch_tag_mappings_query,
                                                                                    query_type='fetchone'
                                        )
                                    log_info(("fetch_tag_mappings_query",fetch_tag_mappings_query))
                                    log_info(("fetch_tag",fetch_tag))
                                    if fetch_tag:
                                        delete_tag_mappings_query=f"""delete from core.tags_mapping where tags_id= '{tag.get("id")}' and attribute_id='{attribute_id}'"""
                                        delete_tag = self.__run_postgres_query(delete_tag_mappings_query,
                                                                                        query_type='delete'
                                                                                        )
                                        log_info(("delete_tag_mappings_query for debug having tags",delete_tag_mappings_query))
                                        log_info((f"delete_tag_mappings_query for {attribute_name}"))
                            for attribute_tag_classification in attribute_tags:
                                if "domain:" in attribute_tag_classification.get("tag", {}).get("label", "") or "dq_score:" in attribute_tag_classification.get("tag", {}).get("label", ""):
                                    continue  # Skip domain tags
                                attribute_tag_id = attribute_tag_classification.get("tag").get("id")
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
                                    #  map coalesce terms to attributes
                                    try:
                                        with self.connection.cursor() as cursor:
                                            # Prepare attribute-tag mapping insert data
                                            query_input = (
                                                uuid4(),  # Unique ID
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
                                        log_info(
                                            f"Insert Attribute Tag Mapping failed", e)
                        else:
                            fetch_tag_mappings_query=f"""select * from core.tags_mapping where attribute_id='{attribute_id}' and tags_id IN (
                                SELECT id
                                FROM core.tags
                                WHERE source = '{COALESCE}')"""
                            fetch_tag=self.__run_postgres_query(fetch_tag_mappings_query,
                                                                    query_type='fetchall'
                                                )
                            
                            if fetch_tag:
                                    delete_tag_mappings_query=f"""delete from core.tags_mapping where attribute_id='{attribute_id}' and tags_id IN (
                                        SELECT id
                                        FROM core.tags
                                        WHERE source = '{COALESCE}')"""
                                    delete_tag = self.__run_postgres_query(delete_tag_mappings_query,
                                                                                query_type='delete'
                                                                                )
                                    log_info(("delete_tag_mappings_indi_query for debug",delete_tag_mappings_query))
                                    log_info((f"delete_tag_mappings_query for {attribute_name}"))
                    log_info(f"Completed processing attribute: {attribute_name}")
                except Exception as e:
                    log_info(f"Error processing attribute {attribute.get('name')}: {str(e)}")
                    # Continue with next attribute instead of failing completely
                    continue
        except Exception as e:
            log_info(f"Error in map_attribute_metrics: {str(e)}")
            raise e

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
            asset_id = dq_asset_data.get("asset_id")
            if map_tags:
                # Filter out tags that start with "dq_score:"
                filtered_tags = [tag for tag in tags_info if not tag.get("name", "").startswith("dq_score:")]
                for tag_mapped in filtered_tags:
                    asset_tag_id = tag_mapped.get("id")
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
                                    log_info("tags_mapping Query Execution Error:", str(e))
                                    raise e
        else:
            return None   

    def fetch_active_attributes(self, asset_id):
        """
        Fetches active attributes for a given asset from Coalesce.

        Parameters:
        asset_guid (str): The GUID of the asset to fetch attributes for.

        Returns:
        list: A list of active attributes associated with the asset.
        """
        log_info(f"Starting fetch_active_attributes for asset_id: {asset_id}")
        host = self.channel.get("host")
        # Construct the URL for the token request
        url = "https://api.castordoc.com/public/graphql?op=getColumns"
        page_size = 500  # Number of records per page # Start from the first page
        payload = f"""{{
            "query": "query {{\\n  getColumns (scope: {{tableId: \\\"{asset_id}\\\"}}, pagination: {{nbPerPage: {page_size}, page: {{page}}}}) {{\\n totalCount\\n data {{\\n id\\n name\\n dataType\\n description\\n externalId\\n sourceOrder\\n isNullable\\n tagEntities {{ id tag {{ id label }} }} }} }} }}\\n",
            "variables": {{}}
        }}"""

        # fetch the attribute information from coalesce
        # attributes_data = self.__call_api_request(url, "post", payload)
        attributes_data = self.fetch_alldata(url, payload, operation="getColumns")
        try:
            # result = attributes_data.json()
            return attributes_data
        except Exception as e:
            log_info("Error parsing response from Coalesce API", e)
            return []

    def get_status_from_score(self, score: float, threshold_ranges: list[dict]) -> str:
        """
        Determine the status (e.g., ALERT, SUCCESS, WARNING) from a score based on dynamic threshold ranges.

        Example input:
        threshold_ranges = [
            {"from": 0, "to": 33, "color": "#F08080", "type": "warning"},
            {"from": 34, "to": 66, "color": "#FFA500", "type": "success"},
            {"from": 67, "to": 100, "color": "#9ACD32", "type": "failed"}
        ]
        """

        # Round the score safely to nearest integer
        score = round(score) if score is not None else 0

        for threshold in threshold_ranges:
            low = threshold.get("from")
            high = threshold.get("to")
            status_type = threshold.get("type")

            if low is not None and high is not None and low!=high and low <= score <= high:
                # Map "failed" to "ALERT" (as per your earlier logic)
                if status_type == "failed":
                    return "ALERT"
                return status_type.upper()

        return "UNKNOWN"

    def get_measures_data(self, config, asset_info, id, type='asset', export_options={}):
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

            _condition = f" base.level='{type}' and mes.asset_id = '{id}' "
            if type == 'attribute':
                asset_id = asset_info.get("asset_id")
                _condition = f" base.level in ('{type}', 'term') and mes.attribute_id='{id}' and mes.asset_id ='{asset_id}' "

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

    def chunk_list(self, data, chunk_size):
        for i in range(0, len(data), chunk_size):
            yield data[i:i + chunk_size]
            
    def push_asset_custom_metadata_to_coalesce(self, tags_info, asset_id, asset_information, asset_export_options):
        try:
            coalesce_asset_id = asset_id
            asset_id = asset_information.get("asset_id")
            asset_name = asset_information.get("asset_name")
            asset_export_options = asset_export_options["options"]
            metrics = get_asset_metrics(self.config, asset_id)
            measures = self.get_measures_data(self.config, id = asset_id ,asset_info=asset_information, type = "asset", export_options=asset_export_options )
            
            if asset_export_options.get("summary"):
                score = metrics.get('score')
                score = round(score, 2) if score else None
                target_name = f"dq_score:{score}"
                
                other_dq_tags = []
                has_target_tag = False
                entity_type = "TABLE"
                
                # Check existing tags for dq_score tags
                for tag in tags_info:
                    name = tag.get("name", "")
                    if name.startswith("dq_score:"):
                        if name == target_name:
                            has_target_tag = True
                        else:
                            other_dq_tags.append(tag)
                
                # Detach existing dq_score tags that don't match current score
                if other_dq_tags:
                    url = "https://api.castordoc.com/public/graphql?op=detachTags"
                    data_entries_list = []
                    for tag in other_dq_tags:
                        label = tag.get("name")
                        entity_id = coalesce_asset_id
                        entity_type = "TABLE"
                        
                        entry = f'{{ label: "{label}" entityId: "{entity_id}" entityType: {entity_type} }}'
                        data_entries_list.append(entry)
                    
                    data_entries = "\n      ".join(data_entries_list)
                    
                    mutation = f"""
                        mutation {{
                        detachTags(
                            data: [
                            {data_entries}
                            ]
                        )
                        }}
                        """
                    
                    # Final payload for detach
                    payload = json.dumps({
                        "query": mutation,
                        "variables": {}
                    })
                    response = self.__call_api_request(url, 'post', payload=payload)
                    log_info(f"Detached {len(other_dq_tags)} existing dq_score tags for asset {coalesce_asset_id}")
                
                # Attach the new target tag if it doesn't exist
                if not has_target_tag:
                    url = "https://api.castordoc.com/public/graphql?op=attachTags"
                    tag_inputs = [
                        {
                            "label": target_name,
                            "entityId": coalesce_asset_id,
                            "entityType": "TABLE"
                        }
                    ]
                    
                    # Construct the final GraphQL payload for attach
                    payload = {
                        "query": """
                            mutation ($tags: [BaseTagEntityInput!]!) {
                            attachTags(data: $tags)
                            }
                        """,
                        "variables": {
                            "tags": tag_inputs
                        }
                    }
                    
                    # Convert payload to JSON string for sending in a request
                    payload = json.dumps(payload)
                    response = self.__call_api_request(url, 'post', payload=payload)
                    log_info(f"Attached new dq_score tag '{target_name}' for asset {coalesce_asset_id}")

            url = "https://api.castordoc.com/public/graphql?op=upsertDataQualities"
            payload = {
                "qualityChecks": []
            }

                
            if asset_export_options.get("measures"):
                for measure in measures:
                    run_at = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                    score = measure.get("score")
                    measure_url = urljoin(self.dq_url, f"measure/{measure.get('id')}/detail?date_filter=All")
                    payload["qualityChecks"].append({
                        "externalId": f"{measure.get('id')}_{measure.get('name')}",
                        "name": measure.get('name'),
                        "runAt": run_at,
                        "status": self.get_status_from_score(score, self.channel.get("thresholds", {})),
                        "description": f"{measure.get('description')}\nDQ_SCORE:{round(measure.get('score', 0), 2) if score else score}",
                        "url": measure_url

                    })
            quality_checks = payload["qualityChecks"]
            batches = self.chunk_list(quality_checks, 300)
            for batch in batches:
                payload_dict = {
                "query": """
                    mutation ($tableId: String!, $qualityChecks: [BaseQualityCheckInput!]!) {
                    upsertDataQualities(data: { tableId: $tableId, qualityChecks: $qualityChecks }) {
                        id
                        name
                        status
                    }
                    }
                """,
                "variables": {
                    "tableId": coalesce_asset_id,
                    "qualityChecks": batch
                }
                }
                payload_dict = json.dumps(payload_dict)
                try:
                    response = self.__call_api_request(url, 'post', payload=payload_dict)
                except Exception as e:
                    log_error(
                        f"Push Asset Custom Metadata from DQLabs to Coalesce Failed for id {coalesce_asset_id}", e)
        except Exception as e:
            log_error(
                        f"Push Asset Custom Metadata from DQLabs to Coalesce Failed ", e)




        
    def push_attribute_custom_metadata_to_coalesce(self, asset_info, attribute_info, asset_id, attribute_export_options):
        try:
            coalesce_asset_id = asset_id
            attribute_export_options = attribute_export_options["options"]
            url = "https://api.castordoc.com/public/graphql?op=upsertDataQualities"
            payload = {
                "qualityChecks": []
            }
            dq_asset_id = asset_info.get('asset_id')
            
            # Collect batch operations for tags
            tags_to_detach = []
            tags_to_attach = []
            
            if attribute_export_options.get("measures") or attribute_export_options.get("summary"):
                attribute_metrics = get_attributes_metrics(self.config,dq_asset_id)
                for attribute in attribute_info:
                    attribute_name = attribute.get('name')
                    attribute_id = self.get_attribute_id_from_name(asset_id=dq_asset_id,
                                                                            attribute_name=attribute_name)
                    measures = self.get_measures_data(self.config, asset_info, id = attribute_id, type = "attribute", export_options=attribute_export_options )
                    
                    # Handle summary-level dq_score tags for attributes
                    if attribute_export_options.get("summary") and attribute_id:
                        # Get score for this specific attribute
                        attribute_score = next(
                            (attr.get('score') for attr in attribute_metrics 
                             if attr.get('attribute_id') == attribute_id), 
                            None
                        )
                            
                        score = round(attribute_score, 2) if attribute_score else None
                        target_name = f"dq_score:{score}"
                        has_target_tag = False
                        entity_type = "COLUMN"
                            
                            # Get existing tags for this attribute from the attribute_info
                        attribute_tags = attribute.get("tagEntities", [])
                        
                        # Check existing tags for dq_score tags
                        for tag_entity in attribute_tags:
                            tag = tag_entity.get("tag", {})
                            name = tag.get("label", "")
                            if name.startswith("dq_score:"):
                                if name == target_name:
                                    has_target_tag = True
                                else:
                                    # Collect tags to detach
                                    tags_to_detach.append({
                                        "label": name,
                                        "entityId": attribute.get("id"),
                                        "entityType": "COLUMN"
                                    })
                        
                        # Collect tags to attach if target tag doesn't exist
                        if not has_target_tag:
                            tags_to_attach.append({
                                "label": target_name,
                                "entityId": attribute.get("id"),
                                "entityType": "COLUMN"
                            })
                
                    # Handle measures export
                    if attribute_export_options.get("measures"):
                        for measure in measures:
                            score = measure.get("score")
                            run_at = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                            measure_url = urljoin(self.dq_url, f"measure/{measure.get('id')}/detail?date_filter=All")
                            payload["qualityChecks"].append({
                                "externalId": f"{measure.get('id')}_{measure.get('name')}",
                                "name": measure.get('name'),
                                "runAt": run_at,
                                "columnId": attribute.get("id"),
                                "status": self.get_status_from_score(score, self.channel.get("thresholds", {})),
                                "description": f"{measure.get('description')}\nDQ_SCORE:{round(measure.get('score', 0), 2) if score else score}",
                                "url": measure_url
                            })
            
            # Batch process detach operations
            if tags_to_detach:
                batches = self.chunk_list(tags_to_detach, 400)
                for batch in batches:
                    detach_url = "https://api.castordoc.com/public/graphql?op=detachTags"
                    data_entries_list = []
                    for tag in batch:
                        entry = f'{{ label: "{tag["label"]}" entityId: "{tag["entityId"]}" entityType: {tag["entityType"]} }}'
                        data_entries_list.append(entry)
                    
                    data_entries = "\n      ".join(data_entries_list)
                    mutation = f"""
                        mutation {{
                        detachTags(
                            data: [
                            {data_entries}
                            ]
                        )
                        }}
                        """
                    
                    detach_payload = json.dumps({
                        "query": mutation,
                        "variables": {}
                    })
                    response = self.__call_api_request(detach_url, 'post', payload=detach_payload)
                    log_info(f"Batch detached {len(tags_to_detach)} existing dq_score tags from attributes")
            
            # Batch process attach operations
            if tags_to_attach:
                batches = self.chunk_list(tags_to_attach, 400)
                for batch in batches:
                    attach_url = "https://api.castordoc.com/public/graphql?op=attachTags"
                    attach_payload = {
                        "query": """
                        mutation ($tags: [BaseTagEntityInput!]!) {
                        attachTags(data: $tags)
                        }
                    """,
                    "variables": {
                        "tags": batch
                    }
                }
                
                    attach_payload = json.dumps(attach_payload)
                    response = self.__call_api_request(attach_url, 'post', payload=attach_payload)
                    log_info(f"Batch attached {len(batch)} new dq_score tags to attributes")
            
            # Process quality checks in batches
            quality_checks = payload["qualityChecks"]
            batches = self.chunk_list(quality_checks, 300)

            for batch in batches:
                payload_dict = {
                    "query": """
                        mutation ($tableId: String!, $qualityChecks: [BaseQualityCheckInput!]!) {
                            upsertDataQualities(data: { tableId: $tableId, qualityChecks: $qualityChecks }) {
                                id
                                name
                                status
                            }
                        }
                    """,
                    "variables": {
                        "tableId": coalesce_asset_id,
                        "qualityChecks": batch
                    }
                }
                payload_dict = json.dumps(payload_dict)
                try:
                    response = self.__call_api_request(url, 'post', payload=payload_dict)
                except Exception as e:
                    log_error(
                        f"Push Attribute Custom Metadata from DQLabs to Coalesce Failed for id {coalesce_asset_id}", e)
        except Exception as e:
            log_error(
                        f"Push Attribute Custom Metadata from DQLabs to Coalesce Failed", e)

    def delete_existing_data_quality(self, table_id):
        """
        Delete all existing data quality checks for a specific table in Coalesce.
        
        This method fetches all data quality checks for the given table ID and then
        deletes them in batches of 500 (following the API limit).
        
        Args:
            table_id (str): The Coalesce table ID to delete data quality checks for
        """
        try:
            log_info(f"Starting deletion of data quality checks for table: {table_id}")
            
            # Step 1: Fetch all existing data quality checks for the table
            existing_checks = self.fetch_all_data_quality_checks(table_id)
            
            if not existing_checks:
                log_info(f"No existing data quality checks found for table: {table_id}")
                return
            
            log_info(f"Found {len(existing_checks)} data quality checks to delete for table: {table_id}")
            
            # Step 2: Delete the checks in batches
            self.delete_data_quality_checks_in_batches(existing_checks, table_id)
            
            log_info(f"Successfully deleted all data quality checks for table: {table_id}")
            
        except Exception as e:
            log_error(f"Failed to delete existing data quality checks for table {table_id}", e)
            raise e

    def fetch_all_data_quality_checks(self, table_id):
        """
        Fetch all data quality checks for a specific table using pagination.
        
        Args:
            table_id (str): The Coalesce table ID to fetch data quality checks for
            
        Returns:
            list: List of all data quality checks for the table
        """
        try:
            url = "https://api.castordoc.com/public/graphql?op=getDataQualities"
            page_size = 500
            
            # Create the payload template with pagination placeholder
            payload_template = f"""{{
                "query": "query ($tableId: String) {{\\n  getDataQualities(scope: {{ tableId: $tableId }}, pagination: {{\\n            nbPerPage: {page_size},\\n            page: {{page}}\\n\\n        }}) {{\\n    totalCount\\n    data {{\\n      externalId\\n      table {{\\n        id\\n      }}\\n    }}\\n  }}\\n}}",
                "variables": {{"tableId": "{table_id}"}}
            }}"""
            
            # Use the existing fetch_alldata method to handle pagination
            all_checks = self.fetch_alldata(url, payload_template, operation="getDataQualities")
            
            log_info(f"Fetched {len(all_checks)} data quality checks for table: {table_id}")
            return all_checks
            
        except Exception as e:
            log_error(f"Failed to fetch data quality checks for table {table_id}", e)
            raise e

    def delete_data_quality_checks_in_batches(self, checks, table_id):
        """
        Delete data quality checks in batches of 500 (API limit).
        
        Args:
            checks (list): List of data quality checks to delete
            table_id (str): The Coalesce table ID
        """
        try:
            url = "https://api.castordoc.com/public/graphql?op=removeDataQualities"
            
            # Process checks in batches of 500
            batches = self.chunk_list(checks, 500)
            
            for batch_index, batch in enumerate(batches):
                log_info(f"Deleting batch {batch_index + 1} with {len(batch)} checks for table: {table_id}")
                
                # Prepare the checks for deletion
                checks_to_delete = []
                for check in batch:
                    checks_to_delete.append({
                        "externalId": check.get("externalId"),
                        "tableId": table_id
                    })
                
                # Create the mutation payload
                payload = {
                    "query": """
                        mutation ($checks: [QualityCheckInput!]!) {
                            removeDataQualities(data: { qualityChecks: $checks })
                        }
                    """,
                    "variables": {
                        "checks": checks_to_delete
                    }
                }
                
                # Convert to JSON string
                payload_json = json.dumps(payload)
                
                # Make the API call to delete the batch
                response = self.__call_api_request(url, 'post', payload=payload_json)
                
                log_info(f"Successfully deleted batch {batch_index + 1} for table: {table_id}")
                
        except Exception as e:
            log_error(f"Failed to delete data quality checks in batches for table {table_id}", e)
            raise e

    def coalesce_catalog_update(self):
            try:
                import_config, export_config = self. __coalesce_channel_configuration()
                log_info(("import_config", import_config))
                log_info(("export_config", export_config))
                pull_domains = import_config.get("domains", False)
                pull_tags = import_config.get("tags", False)
                pull_terms = import_config.get("terms", False)

                asset_flags = next(
                    (item for item in export_config if "asset" in item), None
                )
                log_info(("asset_flags", asset_flags))
                attribute_flags = next(
                    (item for item in export_config if "attribute" in item), None
                )
                log_info(("attribute_flags", attribute_flags))
                domain_info, tags_info = (
                    self.fetch_tags_domains()
                )
                terms_info = (
                    self.fetch_terms()
                )
                log_info(("domain_info", domain_info))
                log_info(("tags_info", tags_info))
                log_info(("terms_info", terms_info))
                asset_level_tag_info = tags_info

                if pull_domains:
                    domain_ids = []
                    for item in domain_info:
                        domain_ids.append(item["id"])
                    # Delete domain mapping in dqlabs if deleted in coalesce
                    self.delete_domains_in_dqlabs(coalesce_domain_ids=domain_ids)

                    # Map coalesce domains to dqlabs
                    self.insert_colesce_domains_to_dqlabs(domains_info=domain_info)
                    log_info(("Dqlabs Glossary Domain Updated"))
                if pull_tags:
                    # Delete tags mapping in dqlabs if deleted in coalesce
                    self.delete_tags_in_dqlabs(tags_info=tags_info)

                    # Map coalesce tags to dqlabs
                    self.insert_coalesce_tags_to_dqlabs(tags_info=tags_info)
                    log_info(("Dqlabs Tags Updated"))
                
                if pull_terms:
                    # Delete terms mapping in dqlabs if deleted in coalesce
                    self.delete_terms_in_dqlabs(terms_info=terms_info)

                    # Map coalesce terms to dqlabs
                    self.insert_coalesce_terms_to_dqlabs(terms_info=terms_info)
                    log_info(("Dqlabs Terms Updated"))
                
                
                asset_list = self.__asset_information()
                for asset_info in asset_list:
                    asset_id = asset_info.get('asset_id')
                    asset_coalesce_id = self.fetch_id_for_asset(asset_info)
                    if asset_coalesce_id is None:
                        log_info(f"Asset with name {asset_info.get('asset_name')} not found in Coalesce")
                        continue
                    coalesce_asset_info, domains_info, tags_info = self.fetch_coalesce_asset_info(asset_coalesce_id)
                    log_info(("tags_info", tags_info))
                    log_info(("domains_info", domains_info))
                    log_info(("coalesce_asset_info", coalesce_asset_info))
                    coalesce_asset_info = coalesce_asset_info.json()
                    if asset_coalesce_id:
                        active_attributes = self.fetch_active_attributes(asset_coalesce_id)
                        if tags_info or domains_info:
                            self.map_asset_metrics(asset_info, coalesce_asset_info, tags_info, pull_tags, domains_info, pull_domains)

                            # Fetch active attributes for the asset
                            log_info(("active_attributes", active_attributes))
                            # # Map attribute metrics to DQLabs
                            self.map_attribute_metrics(
                                asset_info,
                                coalesce_asset_info,
                                active_attributes,
                                asset_level_tag_info,
                                pull_tags
                            )
                            if self.tag_propagation and tags_info:
                                self.map_tag_for_asset_attributes(
                                    coalesce_asset_info,
                                    tags_info,
                                    asset_info,
                                    pull_tags
                                )
                        asset_export_options = asset_flags["options"]
                        attribute_export_options = asset_flags["options"]
                        if asset_export_options.get("measures",False) or attribute_export_options.get("measures", False):
                            try:
                                self.delete_existing_data_quality(asset_coalesce_id)
                            except Exception as e:
                                log_error(
                                        f"Failed to Delete Data Quality Tab in Coalesce ", e)

                        if asset_flags.get("asset",False):
                                """ Push Asset Custom Metadata from DQLabs to Coalesce"""
                                try:
                                    self.push_asset_custom_metadata_to_coalesce(
                                        tags_info = tags_info,
                                        asset_id = asset_coalesce_id,
                                        asset_information = asset_info,
                                        asset_export_options = asset_flags
                                                                            )
                                except Exception as e:
                                    log_error(
                                        f"Push Asset Custom Metadata from DQLabs to Coalesce Failed ", e)
                                    raise e
                        if attribute_flags.get("attribute", False):
                                """ Push Attribute Custom Metadata from DQLabs to Coalesce"""
                                try:
                                     self.push_attribute_custom_metadata_to_coalesce(
                                         asset_info = asset_info,
                                         attribute_info = active_attributes,
                                         asset_id = asset_coalesce_id,
                                         attribute_export_options = attribute_flags
                                     )
                                except Exception as e:
                                    log_error(
                                        f"Push Asset Custom Metadata from DQLabs to Coalesce Failed ", e)
                                    raise e
                        log_info(f"Completed processing asset: {asset_info.get('asset_name')}")
                            
            

            except Exception as e:
                log_info(f"Coalesce Catalog Update Failed {e}")
