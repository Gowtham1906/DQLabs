"""
    Migration Notes From V2 to V3:
    Migrations Completed 
"""

from typing import Dict, List, Any, Optional, Union
from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone


def get_tag_duplicates(connection: object, cursor: object, metadata: dict) -> Dict[str, Any]:
    """
    Get the list of duplicates for semantic tags
    
    Args:
        connection: Database connection object
        cursor: Database cursor object
        metadata: Dictionary containing tag metadata
        
    Returns:
        Dict[str, Any]: Dictionary mapping tag names to their duplicate counts
    """
    name_tags = metadata.get('tag_name')
    name_tags = name_tags if name_tags else []
    tag_duplicate_count = {}
    
    if not isinstance(name_tags, list):
        return tag_duplicate_count
        
    for tag in name_tags:  # list tags
        if not tag:
            continue
            
        tag_name = str(tag)
        query_string = f"""
        select count(*) as duplicate_count from core.tags as tags
        where lower(tags.name) = lower('{tag_name}');
        """
        cursor_query = execute_query(connection, cursor, query_string)
        duplicate_count = fetchall(cursor_query)
        
        if duplicate_count and isinstance(duplicate_count, list):
            for duplicate in duplicate_count:
                if isinstance(duplicate, dict):
                    count = duplicate.get("duplicate_count")
                    if count is not None:
                        tag_duplicate_count[tag_name] = count
                        break
                        
    return tag_duplicate_count


def get_tag_column_list(connection: object, cursor: object, config: dict):
    """
    Get the list of columns already mapped for semantic tags
    """

    column_dict = {}
    query_string = f"""
        select CONCAT(tags.name,'.',attribute.name) concat_tag_name
        from core.attribute
        join core.tags_mapping on tags_mapping.attribute_id = attribute.id
        join core.tags on tags.id = tags_mapping.tags_id;
    """
    cursor_query = execute_query(connection, cursor, query_string)
    columns = fetchall(cursor_query)
    column_dict.update(
        {'concat_tag_name': [col.get("concat_tag_name") for col in columns]})

    return column_dict


def get_selected_attributes(connection: object, cursor: object, column_name: str, asset_id: str):
    """
    Get selected attributes which consist of tags
    """
    query_string = f"""
        select attribute.id from core.attribute
        where lower(attribute.name) = lower('{column_name}') and attribute.asset_id = '{asset_id}';
        """
    cursor_query = execute_query(connection, cursor, query_string)
    attribute_id = fetchone(cursor_query)
    attribute_id = attribute_id.get('id', None) if attribute_id else None
    return attribute_id


def get_selected_asset(connection: object, cursor: object, asset_id: str) -> Optional[str]:
    """
    Get selected asset for parent_id
    
    Args:
        connection: Database connection object
        cursor: Database cursor object
        asset_id: Asset ID to query
        
    Returns:
        Optional[str]: Asset name or None if not found
    """
    query_string = f"""
        WITH asset_table
        AS (
            SELECT asset.id
                ,asset.name AS asset_name
            FROM core.asset AS asset
            LEFT OUTER JOIN core.tags AS tags ON asset.name = tags.name
            WHERE asset.id = '{asset_id}'
            )
        SELECT astbl.asset_name
        FROM asset_table AS astbl;
        """
    cursor_query = execute_query(connection, cursor, query_string)
    asset_results = fetchall(cursor_query)
    
    if asset_results and isinstance(asset_results, list) and len(asset_results) > 0:
        first_result = asset_results[0]
        if isinstance(first_result, dict):
            return first_result.get("asset_name")
    
    return None


def get_parent_id(connection: object, cursor: object, asset_name: str) -> Optional[str]:
    """
    Get selected parent_id for the asset
    
    Args:
        connection: Database connection object
        cursor: Database cursor object
        asset_name: Asset name to query
        
    Returns:
        Optional[str]: Parent ID or None if not found
    """
    query_string = f"""
        select tags.id as tags_id from core.tags as tags
        where lower(tags.name) = lower('{asset_name}');
        """
    cursor_query = execute_query(connection, cursor, query_string)
    parent_results = fetchall(cursor_query)
    
    if parent_results and isinstance(parent_results, list) and len(parent_results) > 0:
        first_result = parent_results[0]
        if isinstance(first_result, dict):
            return first_result.get("tags_id")
    
    return None


def get_source_tags(connection: object, cursor: object, config: object):
    """ 
    Get all the native source tags that need to be pushed from dqlabs to source
    if any new tag added to the asset
    """
    table_name = config.get("technical_name")
    connection_type = config.get("connection_type")
    query_string = f"""
        WITH cte
        AS (
            SELECT 
                tags.name as tag_name
                ,attribute.name as column_name
                ,asset.name as table_name
            FROM core.tags
            JOIN core.tags_mapping ON tags_mapping.tags_id = tags.id
            JOIN core.attribute ON attribute.id = tags_mapping.attribute_id
            JOIN core.asset ON asset.id = attribute.asset_id
            JOIN core.connection ON connection.id = asset.connection_id
            WHERE 
                tags.db_name = 'dqlabs' 
                AND tags.native_query_run = 'False'
                AND lower(asset.technical_name) = lower('{table_name}')
                AND lower(connection.type) = lower('{connection_type}')
            )
        SELECT cte.tag_name
            ,cte.table_name
            ,cte.column_name
        FROM cte
        """
    cursor_query = execute_query(connection, cursor, query_string)
    native_tags = fetchall(cursor_query)
    tag_names, column_names = list(), list()
    tag_details = {}
    if native_tags:
        for _dict in native_tags:
            tag_names.append(_dict.get("tag_name"))
            column_names.append(_dict.get("column_name"))
            tag_details = {'column_name': column_names,
                           'tag_name': tag_names}
    return tag_details


def get_dqlabs_tags(connection: object, cursor: object, config: object, database_name: str):
    """ Get all the tags for the connection type from the backend"""
    connection_type = config.get("connection_type")
    schema_name = config.get("schema")
    schema_name = '"{}"'.format(schema_name)
    asset_name = config.get("technical_name")
    query_string = f"""
        with cte as
        (
            SELECT 
                tags.id as tag_id
                ,tags.name as tag_name
                ,attribute.id as attribute_id
                ,attribute.name as attribute_name
                ,asset.name as asset_name
                ,CAST(asset.properties -> 'schema' as TEXT) as schema_name
                ,CAST(asset.properties -> 'database' as TEXT) as database_name
            FROM core.tags
            JOIN core.tags_mapping ON tags_mapping.tags_id = tags.id
            JOIN core.attribute ON attribute.id = tags_mapping.attribute_id
            JOIN core.asset ON asset.id = attribute.asset_id
            JOIN core.connection ON connection.id = asset.connection_id
            WHERE lower(tags.db_name) = lower('{connection_type}')
        )
        SELECT *,CONCAT(database_name,'.',schema_name,'.',asset_name,'.',attribute_name,'.',tag_name) as tag_key
        FROM cte
        WHERE database_name = lower('{database_name}')  and schema_name = lower('{schema_name}')
        and asset_name = lower('{asset_name}');
        """
    cursor_query = execute_query(connection, cursor, query_string)
    dqlabs_tags = fetchall(cursor_query)
    return dqlabs_tags


def get_col_tags(connection: object, cursor: object, config: object):
    """
    Get the list of column tags from different schemas already mapped for semantic tags
    """

    column_tags_dict = {}
    connection_type = config.get("connection_type")
    query_string = f"""
        SELECT tags.name as tag_name
        FROM core.tags AS tags
    """
    cursor_query = execute_query(connection, cursor, query_string)
    columns = fetchall(cursor_query)
    column_tags_dict.update(
        {'col_tag_name': [col.get("tag_name") for col in columns]})
    return column_tags_dict


def get_unused_tags(connection: object, cursor: object):
    """ Get all the unused(zombie) tags from the dqlabs environment"""
    tags_id = {}
    query_string = f"""
        SELECT tags.id as tags_id
        FROM core.tags as tags
        WHERE tags.id not in(SELECT tags_id FROM core.tags_mapping)
    """
    cursor_query = execute_query(connection, cursor, query_string)
    tags = fetchall(cursor_query)
    tags_id.update({'tag_id': [col.get("tags_id") for col in tags]})
    return tags_id

# should be depreceated in later version if not used


def get_child_tags(connection: object, cursor: object, parent_id: str) -> Optional[str]:
    """ 
    Get all the unused(zombie) tags from the dqlabs environment
    
    Args:
        connection: Database connection object
        cursor: Database cursor object
        parent_id: Parent ID to query
        
    Returns:
        Optional[str]: Child tag ID or None if not found
    """
    query_string = f"""
        select tags.id as tags_id
        from core.tags as tags
        where tags.parent_id = '{parent_id}'
    """
    cursor_query = execute_query(connection, cursor, query_string)
    child_results = fetchall(cursor_query)
    
    if child_results and isinstance(child_results, list) and len(child_results) > 0:
        first_result = child_results[0]
        if isinstance(first_result, dict):
            return first_result.get("tags_id")
    
    return None


def get_tag_name(connection: object, cursor: object, tag_name: str):
    """ Get the correct typecase tag_name"""
    tags_name = {}
    query_string = f"""
        select tags.name as tag_name
        from core.tags as tags
        where UPPER(tags.name) = UPPER('{tag_name}')
    """
    cursor_query = execute_query(connection, cursor, query_string)
    tags = fetchall(cursor_query)
    tags_name.update({'tag_name': [tag.get("tag_name") for tag in tags]})
    return tags_name


def get_attribute_name(connection: object, cursor: object, attribute_name: str):
    """ Get the correct typecase attribute_name"""
    attribute_name_dict = {}
    query_string = f"""
        select attribute.name as attribute_name
        from core.attribute
        where UPPER(attribute.name) = UPPER('{attribute_name}')
    """
    cursor_query = execute_query(connection, cursor, query_string)
    attributes = fetchall(cursor_query)
    attribute_name_dict.update(
        {'attribute_name': [attr.get("attribute_name") for attr in attributes]})
    return attribute_name_dict
