"""
Measure processing helper functions for export_failed_rows.
These helpers eliminate duplication in measure processing logic.
"""

import json
import logging
from typing import Dict, Any, List, Tuple, Optional
from dataclasses import dataclass
from dqlabs.enums.connection_types import ConnectionType

logger = logging.getLogger(__name__)


@dataclass
class MeasureExportLimits:
    """Container for measure-specific export limits"""
    measure_export_row_limit: int
    measure_export_column_limit: int
    is_enabled: bool
    override_config: Dict[str, Any]


def process_measure_export_limits(
    measure: dict, 
    default_row_limit: int, 
    default_column_limit: int
) -> MeasureExportLimits:
    """
    Process measure-specific export limits with consistent logic.
    This function eliminates the duplication of export limit processing.
    """
    export_limit_config = measure.get("export_limit_config")
    export_limit_config = (
        json.loads(export_limit_config)
        if isinstance(export_limit_config, str)
        else export_limit_config
    )
    export_limit_config = export_limit_config if export_limit_config else {}
    
    limit_config = export_limit_config if export_limit_config else {}
    is_enabled = limit_config.get("override", False)
    is_enabled = is_enabled if is_enabled else False
    row_count = limit_config.get("row_count")
    column_count = limit_config.get("column_count")
    
    measure_export_row_limit = (
        int(row_count) if is_enabled and row_count else default_row_limit
    )
    measure_export_row_limit = (
        measure_export_row_limit
        if measure_export_row_limit and measure_export_row_limit > 0
        else default_row_limit
    )
    
    column_count = column_count if column_count else 0
    column_count = int(column_count) if isinstance(column_count, str) else column_count
    measure_export_column_limit = (
        int(column_count)
        if is_enabled and column_count > 0
        else default_column_limit
    )
    measure_export_column_limit = (
        measure_export_column_limit
        if measure_export_column_limit and measure_export_column_limit > 0
        else default_column_limit
    )
    
    return MeasureExportLimits(
        measure_export_row_limit=measure_export_row_limit,
        measure_export_column_limit=measure_export_column_limit,
        is_enabled=is_enabled,
        override_config=export_limit_config
    )


def merge_limit_query(base_query: str, limit_query: str, connection_type: str) -> str:
    """
    Merge limit query into base query. For MongoDB, merges JSON arrays.
    For SQL connectors, uses string concatenation.
    
    Args:
        base_query: The base query string
        limit_query: The limit query string (e.g., "[{\"$limit\": 10}]" for MongoDB)
        connection_type: The connection type string
        
    Returns:
        Merged query string
    """
    if connection_type and connection_type.lower() == ConnectionType.MongoDB.value.lower():
        if not base_query or not limit_query:
            return base_query or limit_query or ""
        
        try:
            base_str = base_query.strip()
            if not base_str.startswith('['):
                return f"{base_query} {limit_query}"
            
            base_pipeline = json.loads(base_str)
            
            limit_str = limit_query.strip()
            if not limit_str.startswith('['):
                return f"{base_query} {limit_query}"
            
            limit_pipeline = json.loads(limit_str)
            
            if isinstance(base_pipeline, list) and isinstance(limit_pipeline, list):
                merged_pipeline = base_pipeline + limit_pipeline
                return json.dumps(merged_pipeline, separators=(',', ':'))
            elif isinstance(base_pipeline, list):
                base_pipeline.extend(limit_pipeline if isinstance(limit_pipeline, list) else [limit_pipeline])
                return json.dumps(base_pipeline, separators=(',', ':'))
            elif isinstance(limit_pipeline, list):
                return json.dumps([base_pipeline] + limit_pipeline, separators=(',', ':'))
            else:
                return json.dumps([base_pipeline, limit_pipeline], separators=(',', ':'))
        except (json.JSONDecodeError, TypeError, ValueError):
            try:
                import re
                combined = f"{base_query.strip()} {limit_query.strip()}"
                if re.search(r'\]\s*\[', combined):
                    parts = re.split(r'\]\s*\[', combined)
                    merged = []
                    for i, part in enumerate(parts):
                        if i == 0:
                            arr_str = part + ']'
                        elif i == len(parts) - 1:
                            arr_str = '[' + part
                        else:
                            arr_str = '[' + part + ']'
                        try:
                            arr = json.loads(arr_str)
                            if isinstance(arr, list):
                                merged.extend(arr)
                            else:
                                merged.append(arr)
                        except (json.JSONDecodeError, TypeError, ValueError):
                            continue
                    if merged:
                        return json.dumps(merged, separators=(',', ':'))
            except Exception:
                pass
            return f"{base_query} {limit_query}"
    else:
        return f"{base_query} {limit_query}"


def apply_query_limits(
    invalid_select_query: str,
    measure: dict,
    default_source_queries: dict,
    measure_limits: MeasureExportLimits,
    connection_type: str
) -> str:
    """
    Apply row and column limits to query with consistent logic.
    This eliminates duplication in query limit application.
    """
    from dqlabs.utils.extract_failed_rows import has_limit, lookup_processing_query
    import re
    
    limit_condition_query = default_source_queries.get("limit_query")
    measure_category = measure.get("category")
    
    if measure_limits.measure_export_row_limit and limit_condition_query:
        if (
            "order by" in invalid_select_query
            and "order by (select 1)" in limit_condition_query
        ):
            limit_condition_query = limit_condition_query.replace(
                "order by (select 1)", ""
            )
        
        if not has_limit(connection_type, invalid_select_query):
            if (
                measure_category == "lookup"
                and "<query_string>" in invalid_select_query
            ):
                invalid_select_query = f"{invalid_select_query}"
            else:
                if "distinct" not in invalid_select_query.lower():
                    invalid_select_query = merge_limit_query(
                        invalid_select_query, limit_condition_query, connection_type
                    )
        
        invalid_select_query = invalid_select_query.replace(
            "<count>", str(measure_limits.measure_export_row_limit)
        )
        
        # Handle lookup measures
        if (
            measure_category == "lookup"
            and "<query_string>" in invalid_select_query
        ):
            # Note: This would need config and other parameters from the calling function
            pass  # Placeholder for lookup processing
        else:
            if measure_category == "lookup":
                if ("count(*)" in invalid_select_query.lower()) and (
                    "count(*) as " not in invalid_select_query.lower()
                ):
                    compiled = re.compile(re.escape("count(*)"), re.IGNORECASE)
                    invalid_select_query = compiled.sub(
                        'count(*) as "COUNT(*)"', invalid_select_query
                    )
    
    return invalid_select_query


def process_measure_metadata(measure: dict, metadata: dict) -> dict:
    """
    Process measure metadata with consistent field extraction.
    This eliminates duplication in metadata processing.
    """
    if not metadata:
        return {}
    
    # Add measure-specific fields to metadata
    enhanced_metadata = {
        **metadata,
        "measure_id": measure.get("id"),
        "measure_name": measure.get("measure_name"),
        "measure_category": measure.get("category"),
        "measure_type": measure.get("type"),
        "measure_level": measure.get("level"),
        "attribute_id": measure.get("attribute_id")
    }
    
    return enhanced_metadata


def get_measure_basic_info(measure: dict) -> Dict[str, Any]:
    """
    Extract basic measure information with consistent field mapping.
    This eliminates duplication in measure info extraction.
    """
    return {
        "measure_id": measure.get("id"),
        "measure_name": measure.get("measure_name"),
        "technical_name": measure.get("technical_name"),
        "category": measure.get("category"),
        "type": measure.get("type"),
        "level": measure.get("level"),
        "attribute_id": measure.get("attribute_id")
    }


def should_skip_measure(
    measure: dict, 
    primary_attributes: List[str], 
    failed_rows_metadata: dict,
    invalid_select_query: str
) -> bool:
    """
    Determine if a measure should be skipped based on consistent validation rules.
    This eliminates duplication in measure validation logic.
    """
    measure_level = measure.get("level")
    measure_name = measure.get("measure_name", "")
    failed_row_columns = list(failed_rows_metadata.keys()) if failed_rows_metadata else []
    
    # Skip if no failed row columns
    if not failed_row_columns:
        return True
    
    # Skip duplicates measure without primary attributes
    if (
        measure_level == "asset"
        and str(measure_name).lower() == "duplicates"
        and not primary_attributes
    ):
        return True
    
    # Skip if no valid query
    if not invalid_select_query:
        return True
    
    return False


def create_temp_table_name(measure: dict) -> str:
    """
    Create consistent temp table names for measures.
    This eliminates duplication in temp table naming.
    """
    measure_category = measure.get("category", "")
    measure_id = measure.get("id", "")
    return f"{measure_category}_{measure_id}"

def get_default_measure_queries(
    config: dict,
    default_queries: dict,
    measure_name: str,
    measure_type: str,
    measure_category: str,
    derived_type: str,
    table_name: str,
    database: str,
    table: str,
    schema: str,
    attribute_name: str,
):
    """
    Returns query string for the health measures (adapted for airflow context).
    This is the airflow-side implementation of the server-side get_default_measure_queries method.
    """
    query_string = ""
    if measure_type in ["distribution"]:
        measure_category = measure_type
    
    default_query = default_queries.get("default_query", "")
    
    # Normalize derived_type
    derived_type = (
        str(derived_type).lower()
        if derived_type and measure_type != "statistics"
        else ""
    )
    
    from dqlabs.utils.extract_workflow import __get_default_queries
    basic_queries = __get_default_queries(
        default_queries, measure_category, derived_type
    )
    
    if default_query and measure_name in basic_queries:
        query = basic_queries.get(measure_name)
        if isinstance(query, dict):
            query = query.get("query")
            query_string = query if query else ""
        else:
            query_string = default_query.replace("<query>", query)
            if measure_type == "reliability":
                query_string = query
    
    # Replace placeholders
    if measure_type == "reliability":
        query_string = (
            query_string.replace("<table_name>", table or "")
            .replace("<table>", table or "")
            .replace("<schema>", schema or "")
            .replace("<schema_name>", schema or "")
            .replace("<database>", database or "")
            .replace("<database_name>", database or "")
            .replace("<attribute>", attribute_name or "")
            .replace("<attribute_label>", attribute_name or "")
        )
    
    query_string = (
        query_string.replace("<table_name>", table_name or "")
        .replace("<table>", table or "")
        .replace("<schema>", schema or "")
        .replace("<schema_name>", schema or "")
        .replace("<database>", database or "")
        .replace("<database_name>", database or "")
        .replace("<attribute>", attribute_name or "")
        .replace("<attribute_label>", attribute_name or "")
    )
    
    return query_string
