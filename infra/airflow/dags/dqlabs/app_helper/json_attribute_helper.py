"""
JSON Attribute Helper Module

This module provides functionality to flatten JSON attributes in Snowflake queries
using LATERAL FLATTEN operations. It handles both simple JSON paths and complex
nested array structures.

Key Features:
- Parse JSON paths with array notation ([])
- Generate Snowflake LATERAL FLATTEN queries
- Handle nested arrays and static fields
- Support for duplicate removal and grouping
- Automatic data type inference
"""

import re
from typing import List, Dict, Tuple, Optional

from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.db_helper import (
    fetchall,
    execute_query,
)


class JSONFlattener:
    """
    A class to generate Snowflake SQL queries for flattening JSON data with arrays.
    
    This class handles the conversion of JSON paths with array notation into
    Snowflake LATERAL FLATTEN queries that can extract nested data from JSON columns.
    
    Attributes:
        table_name (str): Placeholder for the target table name
        remove_duplicates (bool): Whether to add DISTINCT or GROUP BY clauses
        use_group_by (bool): Use GROUP BY instead of DISTINCT for duplicate removal
        alias_counter (int): Counter for generating unique table aliases
    """
    
    def __init__(self, remove_duplicates: bool = False, use_group_by: bool = False):
        """
        Initialize the JSONFlattener.
        
        Args:
            remove_duplicates: Whether to eliminate duplicate rows in the result
            use_group_by: Use GROUP BY instead of DISTINCT for duplicate removal
        """
        self.table_name = "<table_name>"
        self.remove_duplicates = remove_duplicates
        self.use_group_by = use_group_by
        self.alias_counter = 0
        
    def _get_alias(self, prefix: str = "flatten") -> str:
        """
        Generate a unique alias for flatten operations.
        
        Args:
            prefix: Prefix for the alias name
            
        Returns:
            Unique alias string (e.g., "flatten_1", "arr_2")
        """
        self.alias_counter += 1
        return f"{prefix}_{self.alias_counter}"
    
    def _parse_path(self, path: str) -> Tuple[str, List[str], bool]:
        """
        Parse a JSON path and identify array components.
        
        This method splits a JSON path like "user.projects.[].technologies.[].name"
        into its components and identifies which parts are arrays.
        
        Args:
            path: JSON path string (e.g., "user.projects.[].technologies.[].name")
            
        Returns:
            Tuple containing:
            - base_path: Path without array notation
            - array_paths: List of array paths found
            - has_arrays: Boolean indicating if path contains arrays
        """
        # Split by dots but preserve array notation
        # This regex splits on dots that are not followed by [ or end of string
        parts = re.split(r'\.(?=[^[]|$)', path)
        base_path = ""
        array_paths = []
        has_arrays = False
        
        for part in parts:
            if '[]' in part:
                has_arrays = True
                # Extract the field name before the array notation
                array_part = part.replace('[]', '')
                if base_path:
                    full_array_path = f"{base_path}.{array_part}"
                else:
                    full_array_path = array_part
                array_paths.append(full_array_path)
                base_path = full_array_path
            else:
                if base_path:
                    base_path = f"{base_path}.{part}"
                else:
                    base_path = part
        
        return base_path, array_paths, has_arrays
    
    def _generate_json_path(self, path: str, parent_attribute_technical_name: str) -> str:
        """
        Convert dot notation to Snowflake JSON path syntax.
        
        Args:
            path: JSON path in dot notation (e.g., "user.projects.technologies")
            parent_attribute_technical_name: Parent attribute name (e.g., "JSON_PAYLOAD")
            
        Returns:
            Snowflake JSON path (e.g., "JSON_PAYLOAD:user:projects:technologies")
        """
        return f"{parent_attribute_technical_name}:{path.replace('.', ':')}"
    
    def _get_data_type(self, datatype: str = "string") -> str:
        """
        Get Snowflake data type based on provided datatype or infer from path patterns.
        
        This method prioritizes the provided datatype parameter, falling back to
        heuristics based on common naming patterns in the path.
        
        Args:
            path: JSON path or field name
            datatype: Explicit datatype provided in configuration
            
        Returns:
            Snowflake data type (NUMBER, TIMESTAMP, BOOLEAN, or STRING)
        """
        # If datatype is explicitly provided, use it
        if datatype:
            datatype_lower = datatype.lower()
            
            # Map common datatype names to Snowflake types
            if datatype_lower in ['integer', 'int', 'number', 'numeric', 'decimal', 'float', 'double']:
                return "NUMBER"
            elif datatype_lower in ['timestamp', 'datetime', 'date', 'time']:
                return "TIMESTAMP"
            elif datatype_lower in ['boolean', 'bool']:
                return "BOOLEAN"
            elif datatype_lower in ['string', 'varchar', 'text', 'char']:
                return "STRING"
            elif datatype_lower in ['variant', 'object', 'array']:
                return "VARIANT"
            else:
                # If unknown datatype, default to STRING
                return "STRING"

        # Default to string for everything else
        return "STRING"
    
    def _create_column_alias(self, name: str, path: str = "") -> str:
        """
        Create a clean column alias for SQL queries.
        
        Args:
            name: Preferred name for the alias (usually technical_name)
            path: Fallback path to generate alias from
            
        Returns:
            Clean column alias wrapped in quotes
        """
        if name:
            # Use the provided name as alias
            alias = name.replace(' ', '_').replace('-', '_')
            # Remove special characters except dots and underscores
            alias = re.sub(r'[^a-zA-Z0-9_.]', '', alias)
            return f'"{alias}"'
        else:
            # Fallback to path-based alias
            alias = path.replace('[]', '').replace(':', '_')
            # Keep dots, remove other special characters
            alias = re.sub(r'[^a-zA-Z0-9_.]', '', alias)
            return f'"{alias}"'
    
    def generate_flatten_query(self, path_configs: List[Dict]) -> str:
        """
        Generate a complete Snowflake SQL query to flatten JSON data.
        
        This is the main method that processes path configurations and generates
        a complete SQL query with appropriate LATERAL FLATTEN operations.
        
        Args:
            path_configs: List of dictionaries containing:
                - key_path: JSON path with array notation
                - technical_name: Column alias name
                - parent_attribute_technical_name: Parent JSON column name
                
        Returns:
            Complete Snowflake SQL query string
            
        Example:
            path_configs = [{
                'key_path': 'experience.projects.[].technologies.[].name',
                'technical_name': 'project_technology_name',
                'parent_attribute_technical_name': 'JSON_PAYLOAD'
            }]
        """
        # Reset internal state
        self.alias_counter = 0
        
        # Track array paths and static fields
        array_paths_map = {}
        static_fields = []
        
        # Process each path configuration
        for config in path_configs:
            if not config:
                continue
            key_path = config.get('key_path', '')
            technical_name = config.get('technical_name', '')
            parent_attribute_technical_name = config.get('parent_attribute_technical_name', '')
            datatype = config.get('datatype', '')

            if not key_path:
                static_fields.append(technical_name)
                continue
                
            # Parse the path to determine if it contains arrays
            base_path, array_paths, has_arrays = self._parse_path(key_path)
            
            if not has_arrays:
                # Handle static fields (no arrays)
                json_path = self._generate_json_path(key_path, parent_attribute_technical_name)
                data_type = self._get_data_type(datatype)
                column_alias = self._create_column_alias(technical_name, key_path)
                static_fields.append(f"{json_path}::{data_type} as {column_alias}")
            else:
                # Handle array fields (requires flattening)
                self._process_array_path(key_path, technical_name, parent_attribute_technical_name, array_paths_map)
        
        # Generate the complete query components
        select_clause = self._generate_select_clause(static_fields, array_paths_map)
        from_joins = self._generate_from_joins(array_paths_map)
        distinct_clause, group_by_clause = self._generate_duplicate_removal_clauses(static_fields)
        
        # Combine into final query
        query = f"""
            SELECT {distinct_clause}
            {select_clause}
            FROM {self.table_name}
            {from_joins}{group_by_clause}
        """
        
        return query.strip()
    
    def _process_array_path(self, path: str, technical_name: str, parent_attribute_technical_name: str, array_paths_map: Dict):
        """
        Process JSON paths that contain arrays and build flatten operations.
        
        This method handles complex nested array structures and creates the necessary
        LATERAL FLATTEN operations for each array level.
        
        Args:
            path: JSON path with array notation
            technical_name: Column alias name
            parent_attribute_technical_name: Parent JSON column name
            array_paths_map: Dictionary to store array path information
        """
        # Split path into parts and identify array positions
        parts = path.split('.')
        array_positions = []
        
        for i, part in enumerate(parts):
            if '[]' in part:
                array_positions.append((i, part.replace('[]', '')))
        
        # Process each array level
        current_alias = ""
        parent_alias = ""
        
        for i, (pos, array_field) in enumerate(array_positions):
            # Generate JSON path and alias for this array level
            if i == 0:
                # First array - build path from root
                array_path = '.'.join(parts[:pos]) + ('.' + array_field if array_field else '')
                current_alias = self._get_alias("arr")
                parent_alias = ""
                if array_path:
                    json_path = self._generate_json_path(array_path, parent_attribute_technical_name)
                else:
                    json_path = parent_attribute_technical_name
            else:
                # Nested array - build path from parent
                prev_pos = array_positions[i-1][0]
                between_parts = [p for p in parts[prev_pos+1:pos] if p]
                
                if between_parts:
                    nested_path = ':'.join(between_parts + ([array_field] if array_field else []))
                else:
                    nested_path = array_field
                    
                json_path = f"{parent_alias}.value{':' if nested_path else ''}{nested_path}"
                current_alias = self._get_alias("arr")
            
            # Store or retrieve array information
            array_path_key = json_path
            
            if array_path_key not in array_paths_map:
                array_paths_map[array_path_key] = {
                    'alias': current_alias,
                    'parent_alias': parent_alias,
                    'json_path': json_path,
                    'fields': []
                }
            else:
                # Use existing alias for duplicate paths
                current_alias = array_paths_map[array_path_key]['alias']
            
            # Add fields for the last array level
            if i == len(array_positions) - 1:
                remaining_parts = parts[pos+1:]
                
                if remaining_parts:
                    # Path continues after array - extract specific fields
                    field_path = f"{current_alias}.value:{':'.join(remaining_parts)}"
                    data_type = self._get_data_type()
                    column_alias = self._create_column_alias(technical_name)
                else:
                    # Path ends with array - select entire array value
                    field_path = f"{current_alias}.value"
                    data_type = "VARIANT"  # Use VARIANT for array values
                    column_alias = self._create_column_alias(technical_name)
                
                # Add field to the current array level
                array_paths_map[array_path_key]['fields'].append({
                    'path': field_path,
                    'type': data_type,
                    'alias': column_alias
                })
            
            parent_alias = current_alias
    
    def _generate_select_clause(self, static_fields: List[str], array_paths_map: Dict) -> str:
        """
        Generate the SELECT clause with all fields.
        
        Args:
            static_fields: List of static field expressions
            array_paths_map: Dictionary containing array field information
            
        Returns:
            Formatted SELECT clause string
        """
        select_parts = []
        
        # Add static fields first
        if static_fields:
            select_parts.extend(static_fields)
        
        # Add flattened array fields
        for array_info in array_paths_map.values():
            for field in array_info['fields']:
                select_parts.append(f"{field['path']}::{field['type']} as {field['alias']}")
        
        return ",\n    ".join(select_parts)
    
    def _generate_from_joins(self, array_paths_map: Dict) -> str:
        """
        Generate FROM and LEFT JOIN LATERAL FLATTEN clauses.
        
        Args:
            array_paths_map: Dictionary containing array path information
            
        Returns:
            Formatted JOIN clauses string
        """
        joins = []
        
        for array_info in array_paths_map.values():
            # Create LATERAL FLATTEN join with proper parameters
            join_clause = (
                f"LEFT JOIN LATERAL FLATTEN("
                f"input => {array_info['json_path']}, "
                f"mode => 'ARRAY', "
                f"outer => TRUE"
                f") {array_info['alias']}"
            )
            joins.append(join_clause)
        
        return "\n".join(joins)
    
    def _generate_duplicate_removal_clauses(self, static_fields: List[str]) -> Tuple[str, str]:
        """
        Generate DISTINCT and GROUP BY clauses for duplicate removal.
        
        Args:
            static_fields: List of static field expressions
            
        Returns:
            Tuple of (distinct_clause, group_by_clause)
        """
        if not self.remove_duplicates:
            return "", ""
        
        if self.use_group_by and static_fields:
            # Extract field names for GROUP BY
            static_field_names = []
            for field in static_fields:
                match = re.search(r'as\s+(\w+)', field)
                if match:
                    static_field_names.append(match.group(1))
            
            group_by_clause = f"\nGROUP BY {', '.join(static_field_names)}" if static_field_names else ""
            return "", group_by_clause
        else:
            # Use DISTINCT
            return "DISTINCT ", ""

def extract_attribute_ids_from_rules(config: dict, attributes: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    """
    Extract attribute IDs from a rules input structure.
    
    This method parses a rules configuration object and extracts all unique
    attribute IDs from the rules array. It handles nested structures and
    ensures no duplicate IDs are returned.
    
    Args:
        rules_input: Dictionary containing rules configuration
            Expected format:
            [
                {"id": "attr_123", "name": "user_name"},
                {"id": "attr_456", "name": "user_age"}
            ]
    
    Returns:
        List of unique attribute IDs found in the rules
        
    Example:
        rules_input = {
            "rules": [
                {
                    "attribute": {"id": "attr_123", "name": "user_name"},
                    "operator": {"id": "op_1", "name": "equals"},
                    "values": ["john"],
                    "is_case_sensitive": False,
                    "id": "hXrxRag2yK6EOEvbeZTjZ"
                },
                {
                    "attribute": {"id": "attr_456", "name": "user_age"},
                    "operator": {"id": "op_2", "name": "greater_than"},
                    "values": [25],
                    "is_case_sensitive": False,
                    "id": "B8tBX9xm4uPIXDia0yMEP"
                }
            ]
        }
        
        # Returns: ["attr_123", "attr_456"]
    """
    
    try:
        # Extract attribute IDs from the attributes list
        attribute_ids = [attribute.get("id") for attribute in attributes]
        if attribute_ids:
             # Fetch attribute metadata from database
            connection = get_postgres_connection(config)
            with connection.cursor() as cursor:
                query_string = f"""
                    SELECT 
                        attribute.id, 
                        attribute.technical_name, 
                        attribute.parent_attribute_id, 
                        attribute.key_path, 
                        attribute.datatype,
                        parent_attribute.technical_name as parent_attribute_technical_name
                    FROM core.attribute
                    LEFT JOIN core.attribute as parent_attribute 
                        ON attribute.parent_attribute_id = parent_attribute.id
                    WHERE attribute.id IN ({','.join(f"'{i}'" for i in attribute_ids)})
                    AND attribute.is_active = TRUE AND attribute.is_delete = FALSE
                """
                cursor = execute_query(connection, cursor, query_string)
                attributes = fetchall(cursor)
                is_json_attributes = [attribute for attribute in attributes if attribute.get("parent_attribute_technical_name")]
                return attributes, is_json_attributes
        else:
            return ([], [])
        
    except Exception as e:
        log_error("Error extracting attribute IDs from rules", e)
        return ([], [])


def prepare_json_attribute_flatten_query(
    config: dict,
    attribute: Dict, 
    query: str,
    query_type: str = "sub_query",
    attributes: List[Dict] = []
) -> str:
    """
    Prepare Snowflake LATERAL FLATTEN query for JSON attributes with array data types.
    
    This function takes an existing SQL query and modifies it to handle JSON attributes
    that contain arrays. It fetches attribute metadata from the database and generates
    appropriate LATERAL FLATTEN operations.
    
    Args:
        config: Configuration dictionary
        attribute: Attribute dictionary or string name
        query: Original SQL query to modify
        query_type: Type of query to modify (sub_query or wrap)
        attributes: List of attributes to flatten
        
    Returns:
        Modified SQL query with LATERAL FLATTEN operations
        
    Example:
        # For attribute string
        modified_query = prepare_json_attribute_flatten_query(
            config={},
            attribute="user_projects", 
            query="SELECT * FROM users"
        )
        
        # For attribute dict
        modified_query = prepare_json_attribute_flatten_query(
            config={},
            attribute={"key_path": "projects.[].name"}, 
            query="SELECT * FROM users"
        )
    """
        
    modified_query = query
    
    try:
        connection = get_postgres_connection(config)

        if not attributes:
            # Handle attribute parameter - it can be dict or string
            if isinstance(attribute, str):
                asset_id = config.get("asset_id")
                attribute = attribute.replace("'", "''")
                if not asset_id:
                    log_info(("prepare_json_attribute_flatten_query", "asset_id not found"))
                    return query
                
                # Fetch attribute metadata from database
                with connection.cursor() as cursor:
                    query_string = f"""
                        SELECT 
                            attribute.id, 
                            attribute.technical_name, 
                            attribute.parent_attribute_id, 
                            attribute.key_path, 
                            attribute.datatype,
                            parent_attribute.technical_name as parent_attribute_technical_name
                        FROM core.attribute
                        JOIN core.attribute as parent_attribute 
                            ON attribute.parent_attribute_id = parent_attribute.id
                        WHERE (attribute.name = '{attribute}' or attribute.id::text = '{attribute}')
                        AND attribute.asset_id = '{asset_id}'
                        AND attribute.is_active = TRUE AND attribute.is_delete = FALSE
                    """
                    cursor = execute_query(connection, cursor, query_string)
                    attributes = fetchall(cursor)
            else:
                attributes = [attribute]

        if not attributes:
            return query

        # Generate JSON flatten query
        flattener = JSONFlattener()
        json_query = flattener.generate_flatten_query(attributes)

        if json_query:
            # Define table name format patterns
            # These patterns represent different ways table names can be formatted in queries
            table_name_patterns = [
                '"<database_name>"."<schema_name>"."<table_name>"',  # Quoted format
                '<database_name>.<schema_name>.<table_name>',        # Unquoted format
                '<table_name>'                                        # Simple format
            ]
            
            # Find and replace the appropriate table name pattern
            for pattern in table_name_patterns:
                if pattern in query:
                    # Replace placeholder with actual pattern and wrap in subquery
                    modified_query = json_query.replace('<table_name>', pattern)
                    modified_query = query.replace(pattern, f"({modified_query})")
                    break

        return modified_query
        
    except Exception as e:
        log_error("Error preparing JSON attribute flatten query", e)
        return modified_query