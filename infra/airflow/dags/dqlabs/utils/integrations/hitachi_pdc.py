import json
import requests
import uuid
import re
import random
from uuid import uuid4

from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone, split_queries
from dqlabs.app_helper.log_helper import log_error,log_info
from dqlabs.app_helper.crypto_helper import encrypt,decrypt
from dqlabs.app_helper.dq_helper import format_freshness, get_client_origin

#Global variable to generate seed uuid for tag string
hitach_tag_namespace = uuid.NAMESPACE_DNS 
generated_uuid = uuid.uuid4()

def hitachipdc_catalog_update(config, channel):
    """
    Update the Hitachi PDC (Product Data Catalog) with the latest access token 
    and map PDC data quality metrics.

    Parameters:
    config (dict): Configuration dictionary containing necessary parameters.
    channel (dict): Channel dictionary to be updated with the access token.

    Returns:
    None

    The function performs the following steps:
    1. Retrieves a PDC token using the provided configuration and channel.
    2. Encrypts the token if it exists.
    3. Updates the channel dictionary with the encrypted access token.
    4. Maps PDC data quality metrics.
    5. Logs any exceptions encountered during the process.

    Raises:
    Exception: If any error occurs during the update process.
    """

    try:
        # Retrieve the PDC token
        token = get_pdc_token(config, channel)
        # Encrypt the token if it exists
        access_token = encrypt(token) if token else ""
        # Check for asset connection
        connection = config.get("connection", {})
        if not connection:
            return

        # Update the channel with the encrypted access token
        if access_token:
            channel.update({"access_token": access_token})

            # Map PDC data quality metrics
            map_pdc_dq_metrics(config, channel)

    except Exception as e:
        # Log any exceptions encountered
        log_error(f"Hitachi Catalog Update Failed ", e)


# Helper Functions for Hitachi PDC
def convert_hexa_to_uuid(hexadecimal:object) -> object:
    # Convert the hexadecimal string to a UUID
    # UUID requires 32 hexadecimal characters, so we will pad the string with zeros if necessary
    uuid_hex = hexadecimal.ljust(32, '0')
    generated_uuid = uuid.UUID(uuid_hex)

    return generated_uuid

def generate_unique_int(exclusion_list) -> int:
    # Generate an int id for version domains
    while True:
        rand_int = random.randint(1, 100000)
        if rand_int not in exclusion_list:
            return int(rand_int)


def extract_value_from_jdbc(jdbc_url):
        # Patterns to match various JDBC URL formats
        sql_server_pattern = re.compile(r'//(?P<host>[^:]+):(?P<port>\d+);databaseName=(?P<dbname>[^;]+)')
        oracle_pattern = re.compile(r'@(?P<host>[^:]+):(?P<port>\d+):(?P<dbname>[^;]+)')
        generic_pattern = re.compile(r'//(?P<host>[^:]+):(?P<port>\d+)/(?P<dbname>[^?;]+)')
        sap_hana_pattern = re.compile(r'//(?P<host>[^:/]+):(?P<port>\d+)(?:/|$)')
        log_info(('jdbc URL', jdbc_url))

        # Match the SQL Server pattern
        match = sql_server_pattern.search(jdbc_url)
        if match:
            host = match.group('host')
            port = match.group('port')
            dbname = match.group('dbname')
            return host, port, dbname

        # Match the Oracle pattern
        match = oracle_pattern.search(jdbc_url)
        if match:
            host = match.group('host')
            port = match.group('port')
            dbname = match.group('dbname')
            return host, port, dbname

        # Match the generic pattern
        match = generic_pattern.search(jdbc_url)
        if match:
            host = match.group('host')
            port = match.group('port')
            dbname = match.group('dbname')
            return host, port, dbname

        # Match the SAP HANA pattern
        match = sap_hana_pattern.search(jdbc_url)
        if match:
            host = match.group('host')
            port = match.group('port')
            # For SAP HANA, we return the host, port, and dbname
            return host, port, None
        
        # Return None if no valid match is found
        return None, None, None


# Get dq metrics         
def get_asset_attributes(config:dict):
    """
    Get Attribute Level metrics
    """
    asset_id = config.get("asset_id")
    attributes = []
    if not asset_id:
        return attributes

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            select lower(name) as attribute_name, id as attribute_id 
            from core.attribute 
            where asset_id = '{asset_id}'
            and status != 'Deprecated'
        """
        cursor = execute_query(connection, cursor, query_string)
        attributes = fetchall(cursor)
    return attributes


# def get_attribute_metadataid(config:dict, attribute_id:str):
#     """
#     Get the attribute metadata id for term mapping
#     """
#     asset_id = config.get("asset_id")
#     if not asset_id:
#         return []
    
#     connection = get_postgres_connection(config)
#     with connection.cursor() as cursor:
#         query_string = f"""
#             select id as attributemetadata_id 
#             from core.attribute_metadata
#             where asset_id = '{asset_id}'
#             and attribute_id = '{attribute_id}'
#             and status != 'Deprecated'
#         """
#         cursor = execute_query(connection, cursor, query_string)
#         attribute = fetchone(cursor)
#         attributemetadata_id = attribute.get("attributemetadata_id")
#     return attributemetadata_id

def get_pdc_token(config: dict, channel: dict) -> str:
    """
    Generate an access token for requests to the PDC (Product Data Catalog).

    Parameters:
    config (dict): Configuration dictionary containing necessary parameters.
    channel (dict): Channel dictionary containing host, user, and password information.

    Returns:
    str: The generated access token.

    The function performs the following steps:
    1. Constructs the URL for the token request.
    2. Sets up the headers and data for the request.
    3. Sends a POST request to obtain the access token.
    4. Returns the access token if the request is successful.
    5. Raises an exception if the request fails.

    Raises:
    Exception: If the request to obtain the access token fails.
    """

    # Initialize the access token
    access_token = ''
    
    # Extract the host from the channel dictionary
    host = channel.get("host")
    
    # Construct the URL for the token request
    url = "keycloak/realms/pdc/protocol/openid-connect/token"
    url = f"{host}/{url}" if not '/' in host[-1] else f"{host}{url}"
    
    # Set up the headers for the request
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    
    # Set up the data for the request, decrypting user and password
    data = {
        'username': decrypt(channel.get("user")),
        'password': decrypt(channel.get("password")),
        'grant_type': 'password',
        'client_id': 'pdc-client',
        'scope': 'openid'
    }
    # Send the POST request to obtain the access token
    response = requests.post(url, headers=headers, data=data, verify=False)
    # Check if the request was successful
    if response.status_code == 200:
        access_token = response.json()['access_token']
    else:
        # Raise an exception if the request failed
        raise Exception(f"Failed to get access token: {response.status_code} - {response.text}")
    return access_token


def get_graphql_response(channel: dict, query: str = '', variables: dict = None, resource = False) -> dict:
    """
    Send a GraphQL query to the specified channel and return the response.

    Parameters:
    channel (dict): Channel dictionary containing host and access token information.
    query (str): The GraphQL query string.
    variables (dict, optional): A dictionary of variables for the GraphQL query.

    Returns:
    dict: The JSON response from the GraphQL endpoint.

    The function performs the following steps:
    1. Constructs the GraphQL endpoint URL.
    2. Decrypts the access token from the channel.
    3. Prepares headers and the request body.
    4. Sends a POST request to the GraphQL endpoint.
    5. Returns the JSON response if the request is successful.
    6. Raises an exception if the request fails.

    Raises:
    ValueError: If the request is unauthorized or returns an invalid response.
    Exception: If any other error occurs during the request.
    """

    try:
        # Extract the host URL from the channel dictionary and construct the endpoint
        channel_host_url = channel.get('host', '')
        endpoint = f"{channel_host_url}/api/v1/entities/many" if not '/' in channel_host_url[-1] else f"{channel_host_url}api/v1/entities/many"
        if resource:
            endpoint = f"{channel_host_url}/graphql" if not '/' in channel_host_url[-1] else f"{channel_host_url}graphql"
        # Decrypt the access token
        access_token = channel.get('access_token', '')
        access_token = decrypt(access_token)
        # Prepare headers and the request body
        api_headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        body = {'query': query} if resource else query
        if variables:
            body['variables'] = variables
        # Send the POST request to the GraphQL endpoint
        if resource:
            response = requests.post(endpoint, headers=api_headers, json=body, verify=False)
        else:
            response = requests.post(endpoint, headers=api_headers, data=json.dumps(body), verify=False)
        # Check if the request was successful
        if response and response.status_code in [200, 201]:
            return response.json()
        elif response.status_code in [400, 401]:
            raise ValueError(f"Unauthorized response {response.status_code}")
        else:
            raise ValueError(f"Invalid credentials and response code {response.status_code}")

    except Exception as e:
        # Log any exceptions encountered
        log_error(f"HitachiPDC Connector - Get Response - {str(e)}", e)



def get_hitachi_resource_credentials(channel:dict)-> list:
    """ Get all the resources connected in Hitachi PDC """

    datasources = []
    try:
        # GraphQL to fetch resources 
        api_query = """
                {
            ResourceConnectionMany(filter: {}, skip: 0, limit: 100, sort: _ID_ASC) {
                pId
                databaseName
                jobClasspath
                resourceName
                configMethod
                host
                port
                uri
                databaseType
                description
                status
                userName
                sslType
                trustStoreType
                trustStoreLocation
                trustStorePassword
                keyStoreType
                keyStoreLocation
                keyStorePassword
                cipherSuite
                serverDistinguishedName
                fileSystemType
                path
                followSymLinks
                resourceId
                domain
                shareName
                region
                accountNumber
                iamUsername
                accessKeyID
                secretAccessKey
                assumeRole
                externalID
                connectionType
                oauthClientId
                oauthClientSecret
                hdfsCredType
                accountName
                azureCredType
                azureSharedKey
                azureTenantId
                endpoint
                accessKey
                container
                accessId
                secretKey
                _id
            }
            }
            """
        
        response = get_graphql_response(channel, query=api_query, resource = True)
        
        
        #Fetch the unique pId of resoueces from response
        if response:
            resources = response['data']['ResourceConnectionMany']
            for resource in resources:
                databaseName = resource.get("databaseName")
                config_method = resource.get("configMethod")
                port = ''
                host = ''
                
                if config_method == 'credentials':
                    # Extract resource details
                    port = resource.get("port")
                    host = resource.get("host")
                else:
                    # Read the uri
                    connector_uri = resource.get("uri")
                    if connector_uri:
                        host, port, dbname = extract_value_from_jdbc(connector_uri)
                    host = host if host else resource.get('host', '')
                    port = port if port else resource.get('port', '')
                    dbname = ''
                    databaseName = databaseName if databaseName else dbname 

                resource_details = {"connection_id": resource.get("_id"),
                                    "connection_name": resource.get("resourceName"),
                                    "database": databaseName,
                                    "configMethod": resource.get("configMethod"),
                                    "host": host,
                                    "port": port,
                                    "uri": resource.get("uri"),
                                    "databaseType": resource.get("databaseType"),
                                    "description": resource.get("description"),
                                    "pId": resource.get("pId")
                                    }
                datasources.append(resource_details)
        else:
            raise Exception(f"Response Code {response.status_code}")
        return datasources
    except Exception as e:
        log_error(
            f"HitachiPDC Connector - Get Token By User Id and Reset Token - {str(e)}", e)
        return e   

def get_hitachipdc_metrics(channel:dict, parent_id: str, fetch_type: str = None) -> list:
    """
    Get all the resources connected in Hitachi PDC.

    This function retrieves resources connected in Hitachi PDC based on the provided parent ID 
    and fetch type. It constructs a GraphQL query based on the fetch type to filter the resources.

    Parameters:
    parent_id (str): The ID of the parent resource to filter the resources.
    fetch_type (str, optional): The type of resources to fetch. It can be 'schema', 'table', or None. 
                                If None, no resources will be fetched.

    Returns:
    list: A list of resources connected in Hitachi PDC. If an error occurs, it logs the error 
          and returns the exception.

    Raises:
    Exception: If there is any issue with the GraphQL query or response.

    """
    
    if not fetch_type:
        return None

    try:
        if fetch_type == 'businessTerms':
            api_query = {
            "query": {
                "term": {
                    "parentId": f"""{parent_id}"""
                }
            },
            "limit": 10000
        }
        else :
            api_query = {
                "query": {
                    "term": {
                        "type": f"""{fetch_type.upper()}"""
                    }
                },
                "limit": 10000
            }
        response = get_graphql_response(channel, query=api_query)
        # Return the list of resources if response is successful
        if response:
            return response.get('items',[])

    except Exception as e:
        # Log the error and return the exception
        log_error(
            f"HitachiPDC Connector - Get Token By User Id and Reset Token - {str(e)}", e)
        return e




def run_postgres_query(config:dict, query_string:str, query_type: str,output_statement:str=''):
    """ Helper function to run postgres queries """
    connection = get_postgres_connection(config)
    records = ''
    with connection.cursor() as cursor:
        
        cursor = execute_query(
                connection, cursor, query_string)
        
        if not query_type in ['insert','update','delete']:
            if query_type == 'fetchone':
                records = fetchone(cursor)
            elif query_type == 'fetchall':
                records = fetchall(cursor)
            
            return records
        
        log_info(('Query Executed',output_statement))

# Main Function to map metric
def map_pdc_dq_metrics(config: dict, channel: dict):
    """
    Map all asset level domains from Hitachi PDC (Product Data Catalog) to DQLabs.

    Parameters:
    config (dict): Configuration dictionary containing necessary parameters.
    channel (dict): Channel dictionary containing host and access token information.

    Returns:
    None

    The function performs the following steps:
    1. Fetch DQ connection details and asset details from the configuration.
    2. Fetch PDC resources and filter them based on the DQ connection details.
    3. Map asset domains from PDC to DQLabs.
    4. Map attribute details from PDC to DQLabs.

    The function raises exceptions if any errors occur during the process.
    """
    
    # Fetch DQ Connection details
    connection = config.get("connection", {})
    connection_name = connection.get("name")
    credentials = connection.get("credentials", {})
    db_type = connection.get("type", "")
    organization_id = config.get("organization_id")
    connection = get_postgres_connection(config)

    # Fetch DQ Asset Details
    asset = config.get("asset", {})
    properties = asset.get("properties", {})
    database =properties.get('database','')
    version_id = config.get("version_id", "")

    # Pull DQ Asset Details
    asset_name = asset.get("name", "")
    dq_asset_schema = properties.get("schema", "")

    # Asset Connection details
    dq_conn_port = credentials.get("port", "")
    dq_conn_host = credentials.get("server", "")

    if db_type == 'snowflake':
        dq_conn_host = credentials.get("account", "")
        dq_conn_host = f"{dq_conn_host}.snowflakecomputing.com"
        dq_conn_port = str(443)
    elif db_type == 'postgresql':
        db_type = 'postgres'
    elif db_type == 'saphana':
        db_type = 'sap_hana'
    
    if db_type == 'mysql':
        database = 'MYSQL'


    # Channel Config
    channel_checks = channel.get("import")
    domain_check = True if channel_checks.get("domains") else False
    tags_check = True if channel_checks.get("tags") else False

    # Fetch PDC Resources
    pdc_resources = get_hitachi_resource_credentials(channel)
    # Filter only those values that match the connection details
    filtered_resources = []
    if pdc_resources:
        if db_type.lower() == 'mysql':
            common_conditions = lambda item: (
                (item.get('host') or "").lower() == dq_conn_host.lower() and
                (item.get('port') or "").lower() == dq_conn_port.lower() and
                (item.get('connection_name') or "").lower() == connection_name.lower()
            )
        else:
            common_conditions = lambda item: (
                (item.get('database') or "").lower() == database.lower() and
                (item.get('host') or "").lower() == dq_conn_host.lower() and
                (item.get('databaseType') or "").lower() == db_type.lower() and
                (item.get('connection_name') or "").strip().lower() == connection_name.strip().lower()
            )
        if db_type == 'snowflake':
            filtered_resources = list(filter(common_conditions, pdc_resources))
        else:
            filtered_resources = list(filter(
            lambda item: common_conditions(item) and
                         (item.get('port') or "").lower() == dq_conn_port.lower(),
                pdc_resources
            ))
            
    log_info(("filtered_resources",filtered_resources))

    def map_asset_domains(pId: str, pdc_database: str, pdc_schema: str):
        """
        Fetch and map asset domains for the given parent_id in Hitachi PDC.

        Parameters:
        pId (str): Parent ID in Hitachi PDC.
        pdc_database (str): Database name in Hitachi PDC.
        pdc_schema (str): Schema name in Hitachi PDC.

        Returns:
        None

        The function maps asset domains from Hitachi PDC to DQLabs.
        """
        parent_id = f"{pId}/{pdc_database}/{pdc_schema}"
        
        # Fetch all domains for the parent_id in Hitachi PDC
        api_domain_data = get_hitachipdc_metrics(channel, parent_id, fetch_type='businessTerms')
        
        # Filter only those entities in PDC which have business terms
        filtered_assets = list(filter(lambda item: item.get('businessTerms'), api_domain_data))

        if domain_check:
            # Map only if domains checkbox is active from user
            for asset in filtered_assets:
                if asset.get("name", "").lower() == asset_name.lower():
                    business_terms = asset.get("businessTerms", {})

                    if business_terms:
                        # Insert new glossary terms for Asset
                        for glossary in business_terms:
                            glossary_id = str(convert_hexa_to_uuid(glossary.get("glossaryId", "")))
                            # Check if glossary ID is already available in DQLabs
                            check_glossary_query = f"""
                                select glo.id
                                from core.domain glo
                                where glo.id = '{glossary_id}'
                            """
                            try:
                                glossary_id_check = run_postgres_query(config, check_glossary_query, query_type="fetchone")
                            except Exception as e:
                                log_error(
                                    f"Check glossary query failed", e)
                            glossary_id_check = True if glossary_id_check else False

                            if not glossary_id_check:
                                # Create that glossary ID in the glossary table
                                pass
                            else:
                                # Check if version ID is mapped to an asset
                                asset_id = config.get("asset_id",'')
                                check_version_domain_query = f"""
                                                        select domain_id as glossary_id from core.domain_mapping
                                                        where asset_id = '{asset_id}'
                                                        """
                                all_valid_mapped_domains = run_postgres_query(config, check_version_domain_query, query_type="fetchall")
                                all_glossary_ids_mapped = [glossary.get("glossary_id") for glossary in all_valid_mapped_domains]
                                
                                if domain_check:
                                    if glossary_id not in all_glossary_ids_mapped:
                                        # Generate unique version domain ID
                                        version_domain_id = generated_uuid

                                        # Insert asset domain data into DQ PostgreSQL
                                        insert_asset_domain_query = f"""
                                            insert into core.domain_mapping(id, level,asset_id, domain_id,created_date)
                                            values('{version_domain_id}', 'asset','{asset_id}', '{glossary_id}',CURRENT_TIMESTAMP)
                                        """
                                        log_info(("insert_asset_domain_query", insert_asset_domain_query))

                                        try:    
                                            run_postgres_query(config, insert_asset_domain_query, query_type="insert")
                                        except Exception as e:
                                            log_error(
                                                f"Insert Asset domain query failed", e)


    def map_attribute_details(pId: str, pdc_database: str, pdc_schema: str):
        """
        Fetch and map asset domains for the given parent_id in Hitachi PDC.

        Parameters:
        pId (str): Parent ID in Hitachi PDC.
        pdc_database (str): Database name in Hitachi PDC.
        pdc_schema (str): Schema name in Hitachi PDC.

        Returns:
        None

        The function maps attribute details from Hitachi PDC to DQLabs.
        """
        asset_id = config.get("asset_id")
        # Fetch DQ asset attributes
        dq_asset_attributes = get_asset_attributes(config)
        parent_id = f"{pId}/{pdc_database}/{pdc_schema}"

        # Fetch all domains for the parent_id in Hitachi PDC
        api_table_data = get_hitachipdc_metrics(channel, parent_id, fetch_type='table')
        log_info(("api_table_data34",api_table_data))

        pdc_tables = [table.get("name") for table in api_table_data]

        # Get all DQ asset attributes for matching
        asset_attributes = [attribute.get("attribute_name").lower() for attribute in dq_asset_attributes]
        connection_id = config.get("connection_id", "").replace("-", "")[:-8]
        pdc_glossary_attribute_ids = []
        pdc_glossary_ids = []
        pdc_attributes=[]
        if "/" in asset_name:
            # Replace '/' with '%2F' for as Hitachi PDC uses URL encoding
            dq_asset_name = asset_name.replace("/", "%2F")
        else:
            dq_asset_name = asset_name
        for pdc_table in api_table_data:
            if f"{connection_id}/{database}/{dq_asset_schema}/{dq_asset_name}" in pdc_table.get("fqdn"):
                _id = pdc_table.get('_id')
                api_attribute_data = get_hitachipdc_metrics(channel, parent_id= _id , fetch_type='businessTerms')
                # Attributes from pdc
                pdc_attributes = api_attribute_data
                # mapped glossary domains for attributes
                if pdc_attributes:
                    pdc_glossary_attribute_ids = [str(convert_hexa_to_uuid(term['glossaryId'])) for item in pdc_attributes if item.get('businessTerms') for term in item['businessTerms']]

                """
                Delete domains from asset if term is deleted from pdc
                """
                parent_id = f"{pId}/{pdc_database}/{pdc_schema}"
    
                # Fetch all domains for the parent_id in Hitachi PDC
                api_domain_data = get_hitachipdc_metrics(channel, parent_id, fetch_type='businessTerms')
                # Filter only those entities in PDC which have business terms
                filtered_assets = list(filter(lambda item: item.get('name') == pdc_table, api_domain_data))
                pdc_glossary_ids = []
                for asset in filtered_assets:
                    if asset.get("name", "").lower() == asset_name.lower():
                        business_terms = asset.get("businessTerms", {})
                        
                        """ 
                        Delete mapped dq domains to asset if deleted from pdc
                        
                        """
                        if business_terms:
                            pdc_glossary_ids = list(map(lambda glossary: str(convert_hexa_to_uuid(glossary.get("glossaryId",''))), business_terms))

                        if pdc_glossary_ids or pdc_glossary_attribute_ids:
                            #Combine asset and attribute domains ids for pdc
                            pdc_glossary_ids.extend(pdc_glossary_attribute_ids)

                        
                        # Fetch active glossary ids for asset in dqlabs
                        dq_glossary_query = f"""
                                        select dom.id as glossary_id
                                        from core.domain dom
                                        join core.domain_mapping dom_map
                                        on dom_map.domain_id = dom.id
                                        where dom_map.asset_id = '{asset_id}'
                                        and dom.properties->>'type' like '%HitachiPDC%';

                                        """ 
                        try:
                            dq_glo_ids = run_postgres_query(config, dq_glossary_query, query_type="fetchall")
                        except Exception as e:
                            log_error(
                                f"Fetch dq glossary query failed", e)
                        if dq_glo_ids:
                            dq_glo_ids = [str(glossary.get("glossary_id")) for glossary in dq_glo_ids]
                            for glossary_id in dq_glo_ids:
                                if glossary_id not in pdc_glossary_ids:
                                    delete_version_domain_query = f"""
                                                delete from core.domain_mapping
                                                where domain_id = '{glossary_id}'
                                            """
                                    try:
                                        run_postgres_query(config, delete_version_domain_query, query_type="delete")
                                        log_info((f"Version Domains Deleted for Glossary Id {glossary_id}"))
                                    except:
                                        log_error(
                                        f"Delete Version Domain query failed", e)


                if pdc_attributes:
                    mapped_domains = []
                    for pdc_attribute in pdc_attributes:
                        if pdc_attribute['name'].lower() in asset_attributes:
                            pdc_attr = pdc_attribute['name'].lower()
                            attribute_id = next((attribute['attribute_id'] for attribute in dq_asset_attributes if attribute['attribute_name'] == pdc_attr), None)
                            pdc_terms_ids = []
                            if not attribute_id:
                                return None
                            business_terms = pdc_attribute.get('attributes', {}).get('businessTerms', [])
                            
                            # Changed from term.get('termId') to term.get('id') to match your PDC response format
                            pdc_terms_ids = []
                            for term in business_terms:
                                term_id = term.get('id')
                                if term_id:  # Only process if term_id exists
                                    try:
                                        # Assuming convert_hexa_to_uuid expects a proper UUID string
                                        pdc_terms_ids.append(str(term_id))  # Already in UUID format, no conversion needed
                                    except Exception as e:
                                        log_error(f"Failed to process term ID {term_id}", e)
                                        continue
                                        
                            
                            """
                            Delete all terms mapped for DQ attribute if deleted in Hitachi PDC
                            """
                            
                            # Fetch active glossary ids for asset in dqlabs
                            dq_terms_query = f"""
                                SELECT term.id as term_id
                                FROM core.terms_mapping attr
                                JOIN core.terms term ON attr.term_id = term.id
                                WHERE attr.asset_id = '{asset_id}'
                                AND attr.attribute_id = '{attribute_id}'
                                AND term.source = 'hitachipdc'
                            """ 
                            
                            try:
                                dq_term_ids = run_postgres_query(config, dq_terms_query, query_type="fetchall")
                            except Exception as e:
                                log_error(f"Fetch dq terms query failed", e)
                                dq_term_ids = []
                            
                            if dq_term_ids:
                                dq_term_ids = [str(term.get("term_id")) for term in dq_term_ids if term.get("term_id")]
                                for _term_id in dq_term_ids:
                                    if _term_id not in pdc_terms_ids:
                                        update_term_id_query = f"""
                                            DELETE FROM core.terms_mapping
                                            WHERE term_id = '{_term_id}'
                                            AND attribute_id = '{attribute_id}'
                                            AND asset_id = '{asset_id}'
                                        """
                                        try:
                                            run_postgres_query(config, update_term_id_query, query_type="delete")
                                        except Exception as e:
                                            log_error(f"Failed to delete term mapping for Term ID {_term_id}", e)
                            """ 
                                Map terms to attribute from pdc to dqlabs  
                            """
                            if domain_check:
                                if business_terms:
                                    latest_term = business_terms[-1]
                                    term_id = convert_hexa_to_uuid(latest_term.get("id"))
                                    glossary_id = convert_hexa_to_uuid(latest_term.get("glossaryId"))

                                    if domain_check:
                                        # Check if attribute doesn't already have a term ID
                                        insert_term_query = f"""
                                                    insert into core.terms_mapping (id,approval_status,asset_id,attribute_id,term_id,created_date)
                                                    values('{str(uuid4())}','Pending','{asset_id}','{attribute_id}','{term_id}', CURRENT_TIMESTAMP)        
                                        
                                                """
                                        try:
                                            run_postgres_query(config, insert_term_query, query_type="insert")
                                        except Exception as e:
                                            log_error(
                                            f"Update Term query failed", e)

                                        # Update asset domains
                                        if  glossary_id:
                                            # Update asset version domain
                                            """
                                            Check if row ID exists in version.domains and 
                                            update the domain for the asset if any business terms are mapped to the asset.
                                            """
                                            if_version_domain_exists = False
                                            _domains_query = f""" 
                                                            select domain_id as glossary_id 
                                                            from core.domain_mapping
                                                            where asset_id = '{asset_id}'
                                                            and domain_id = '{glossary_id}'
                                                        """
                                            try:
                                                glossary_ids = run_postgres_query(config, _domains_query, query_type="fetchone")
                                                if_version_domain_exists = True if glossary_ids else False
                                            except Exception as e:
                                                log_error(
                                                    f"Domain ids version domains", e)

                                            if not if_version_domain_exists:
                                                existing_id_query = f""" select id as row_id from core.domain_mapping """
                                                try:
                                                    existing_rows_ids = run_postgres_query(config, existing_id_query, query_type="fetchall")
                                                except Exception as e:
                                                    log_error(
                                                        f"Existing id query failed", e)
                                                if existing_rows_ids:
                                                    valid_rows_ids = [row.get("row_id") for row in existing_rows_ids]
                                                    # Generate unique version domain ID
                                                    version_domain_id = generated_uuid

                                                    # Insert asset domain data into DQ PostgreSQL
                                                    insert_asset_domain_query = f"""
                                                        insert into core.domain_mapping(id, level,asset_id, domain_id, created_date)
                                                        values('{version_domain_id}', 'asset','{asset_id}', '{glossary_id}',CURRENT_TIMESTAMP)
                                                    """ 

                                                    try:    
                                                        run_postgres_query(config, insert_asset_domain_query, query_type="insert")
                                                    except Exception as e:
                                                        log_error(
                                                            f"Insert Asset domain query failed", e)

                        with connection.cursor() as cursor:
                            #Fetch Tags from PDC Attribute

                            tags = pdc_attribute.get("attributes", {}).get("tags", [])

                            """
                            Delete Tags if unmapped from pdc
                            """

                            tags = pdc_attribute.get("attributes", {}).get("tags", [])

                            # Delete Tags if unmapped from pdc
                            dq_tags_ids = []
                            # Extract tag names from the tags list (each tag is a dict with 'name' key)
                            pdc_tags_names = [tag.get("name") for tag in tags if tag.get("name")]
                            pdc_tags_id = []
                            if pdc_tags_names:
                                pdc_tags_id = [str(uuid.uuid5(hitach_tag_namespace, tag_name)) for tag_name in pdc_tags_names]

                            # Fetch Dq tags for attribute
                            dq_tags_query = f"""
                                select tag.id as tags_id
                                from core.tags_mapping attr_tag
                                join core.tags tag on tag.id = attr_tag.tags_id
                                where attr_tag.asset_id = '{asset_id}'
                                and attr_tag.attribute_id = '{attribute_id}'
                                and tag.properties->>'type' LIKE '%HitachiPDC%';
                            """
                            try:
                                dq_tags_ids = run_postgres_query(config, dq_tags_query, query_type="fetchall")
                            except Exception as e:
                                log_error(f"Fetch dq tags query failed", e)


                            if dq_tags_ids:
                                dq_tags_ids = [tag.get('tags_id') for tag in dq_tags_ids]
                                for _tag_id in dq_tags_ids:
                                    if _tag_id not in pdc_tags_id:
                                        delete_term_id_query = f"""
                                            delete from core.tags_mapping
                                            where tags_id = '{_tag_id}'
                                            and attribute_id = '{attribute_id}'
                                            and asset_id = '{asset_id}'
                                            """
                                        try:
                                            run_postgres_query(config, delete_term_id_query, query_type="delete")
                                        except:
                                            log_error(
                                            f"Delete Tags Attribute metadata query failed", e)
                                                
                            """
                            Map Tags to Attribute
                            
                            """
                            if tags:
                                for tag in tags:
                                    # Changed from tag.get("k") to tag.get("name") to match PDC response format
                                    pdc_tag_name = tag.get("name")
                                    
                                    # Skip if tag name is empty or None
                                    if not pdc_tag_name:
                                        continue
                                        
                                    try:
                                        pdc_tag_id = str(uuid.uuid5(hitach_tag_namespace, str(pdc_tag_name)))
                                    except Exception as e:
                                        log_error(f"Failed to generate UUID for tag {pdc_tag_name}", e)
                                        continue

                                    # Check if tag exists in database
                                    check_tag_in_dq_query = f"""
                                        SELECT id FROM core.tags 
                                        WHERE id = '{pdc_tag_id}' 
                                        LIMIT 1
                                    """
                                    try:
                                        check_tags_in_dq = run_postgres_query(config, check_tag_in_dq_query, query_type="fetchall")
                                        tag_exists = len(check_tags_in_dq) > 0
                                    except Exception as e:
                                        log_error(f"Check tag in dq query failed", e)
                                        tag_exists = False

                                    tag_order = 1
                                    if tags_check:
                                        if not tag_exists:
                                            # Create a new tag
                                            hitachi_tag_color = '#6633FF'  # Default color for Hitachi PDC
                                            query_input = (
                                                pdc_tag_id,
                                                f"{pdc_tag_name}",
                                                f"{pdc_tag_name.lower().replace(' ', '_')}",  # technical_name
                                                '',  # description
                                                hitachi_tag_color,
                                                organization_id,
                                                database,
                                                json.dumps({"type": "HitachiPDC"}, default=str),
                                                tag_order,
                                                'hitachipdc',
                                                False,  # is_mask_data
                                                True,   # is_active
                                                False   # is_delete
                                            )
                                            
                                            input_literals = ", ".join(["%s"] * len(query_input))
                                            query_param = cursor.mogrify(f"({input_literals}, CURRENT_TIMESTAMP)", query_input).decode("utf-8")
                                            
                                            try:
                                                insert_tag_dq = f"""
                                                    INSERT INTO core.tags (
                                                        id, name, technical_name, description, color, organization_id, 
                                                        db_name, properties, "order", "source", is_mask_data, 
                                                        is_active, is_delete, created_date
                                                    ) VALUES {query_param}
                                                """
                                                cursor = execute_query(connection, cursor, insert_tag_dq)
                                                log_info(f"HitachiPDC tag '{pdc_tag_name}' inserted successfully.")
                                            except Exception as e:
                                                log_error(f"Failed to insert tag '{pdc_tag_name}'", e)
                                                continue

                                        # Check if tag is already mapped to attribute
                                        check_tag_attribute_query = f"""
                                            SELECT tags_id FROM core.tags_mapping
                                            WHERE attribute_id = '{attribute_id}'
                                            AND tags_id = '{pdc_tag_id}'
                                            LIMIT 1
                                        """
                                        try:
                                            mapped_tags = run_postgres_query(config, check_tag_attribute_query, query_type="fetchall")
                                            is_mapped = len(mapped_tags) > 0
                                        except Exception as e:
                                            log_error(f"Check tag mapping failed", e)
                                            is_mapped = False

                                        if not is_mapped:
                                            # Map tag to attribute
                                            query_input = (
                                                str(uuid4()),
                                                pdc_tag_id,
                                                'attribute',
                                                attribute_id,
                                                asset_id
                                            )
                                            input_literals = ", ".join(["%s"] * len(query_input))
                                            query_param = cursor.mogrify(f"({input_literals}, CURRENT_TIMESTAMP)", query_input).decode("utf-8")
                                            
                                            try:
                                                query_string = f"""
                                                INSERT INTO core.tags_mapping(
                                                    id, tags_id, level, attribute_id, asset_id, created_date
                                                ) VALUES {query_param}
                                                """
                                                cursor = execute_query(connection, cursor, query_string)
                                                log_info(f"Tag '{pdc_tag_name}' mapped to attribute {attribute_id}")
                                            except Exception as e:
                                                log_error(f"Failed to map tag '{pdc_tag_name}' to attribute", e)
                        with connection.cursor() as cursor:
                            query_string = f"""
                            select domain_id from core.terms join
                            core.terms_mapping on terms_mapping.term_id=terms.id and terms.source='hitachipdc' and attribute_id = '{attribute_id}'
                            """
                            cursor = execute_query(connection, cursor, query_string)
                            domain_ids=fetchall(cursor)
                            mapped_domains.extend(dom_id.get("domain_id") for dom_id in domain_ids if domain_ids)
                    with connection.cursor() as cursor:
                        try:
                            if mapped_domains:
                                mapped_domains_list = "', '".join(set(mapped_domains))
                                query_string = f"""
                                select domain_mapping.id from core.domain_mapping join core.domain on domain_mapping.domain_id=domain.id and domain_mapping.domain_id not in ('{mapped_domains_list}') and domain.source='hitachipdc' and domain_mapping.asset_id = '{asset_id}'

                                """
                                cursor = execute_query(connection, cursor, query_string)
                                domain_ids=fetchall(cursor)
                                mapped_domain_ids = [domain_id.get("id") for domain_id in domain_ids if domain_ids]
                                if mapped_domain_ids:
                                    mapped_domain_ids_list = "', '".join(set(mapped_domain_ids))
                                    query_string = f"""
                                    delete from core.domain_mapping where id in ('{mapped_domain_ids_list}')
                                    """
                                    cursor = execute_query(connection, cursor, query_string)
                            else:
                                query_string = f"""
                                select domain_mapping.id from core.domain_mapping join core.domain on domain_mapping.domain_id=domain.id and domain.source='hitachipdc' and domain_mapping.asset_id = '{asset_id}'
                                """
                                cursor = execute_query(connection, cursor, query_string)
                                domain_ids=fetchall(cursor)
                                mapped_domains = [domain_id.get("id") for domain_id in domain_ids if domain_ids]
                                if mapped_domains:
                                    mapped_domains_list = "', '".join(set(mapped_domains))
                                    query_string = f"""
                                    delete from core.domain_mapping where id in ('{mapped_domains_list}')
                                    """
                                    cursor = execute_query(connection, cursor, query_string)
 
                        
                        except Exception as e:
                            log_error(f"Failed to delete domain_mapping for '{asset_id}'", e)
    # If there are filtered resources, proceed with mapping
    if filtered_resources:
        for resource in filtered_resources:
            pId = resource.get("pId")
            pdc_database = resource.get("database")
            resource_name = resource.get("connection_name")
            # Get entity resource details
            entity_resource = get_hitachipdc_metrics(channel, parent_id='', fetch_type='resource')

            #Get the mapped resource name from entities    
            filtered_entities = list(filter(lambda item: item.get('name').lower() == resource_name.lower(), entity_resource))

            # Update the entity id if a custom datasource id is added
            if filtered_entities:
                for entity in filtered_entities:
                    entity_id = entity.get("_id")
                    if str(entity_id) != pId:
                        pId = str(entity_id)

            # List all schemas configured in Hitachi PDC
            api_schema_data = get_hitachipdc_metrics(channel, pId, fetch_type='schema')
            schemas = [schema.get("name", "") for schema in api_schema_data]
            pdc_schema = next((schema for schema in schemas if schema.lower() == dq_asset_schema.lower()), None)
            if pdc_schema:
                # Map asset domains if the DQ asset exists in PDC
                map_asset_domains(pId, pdc_database, pdc_schema)

                # Map attribute domains if the DQ asset exists in PDC
                map_attribute_details(pId, pdc_database, pdc_schema)