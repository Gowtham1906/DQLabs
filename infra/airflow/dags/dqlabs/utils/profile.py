import json
import re
import math
import numpy as np
from copy import deepcopy
from uuid import uuid4
import operator
import time
import math
from datetime import datetime

from dqlabs.enums.distribution_types import DistributionTypes
from dqlabs.app_helper.db_helper import (
    execute_query,
    fetchall,
    fetchmany,
    split_queries,
    fetchone,
)
from dqlabs.utils.range import update_range_measure
from dqlabs.app_constants.dq_constants import (
    PASSED,
    HEALTH,
    DISTRIBUTION,
    FREQUENCY,
    PROPERTIES,
)
from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.app_helper.dq_helper import (
    calculate_weightage_score,
    convert_to_lower,
    get_advanced_measure_names,
    check_measure_result,
    transform_mongodb_field_names,
    get_attribute_label,
)
from dqlabs.app_helper.dag_helper import (
    execute_native_query,
    get_query_string,
    get_postgres_connection,
    delete_metrics
)
from dqlabs.app_helper.connection_helper import get_attribute_names
from dqlabs.enums.connection_types import ConnectionType
from dqlabs.utils import is_scoring_enabled
from dqlabs.utils.value_range_observe import process_historical_values
from dqlabs.app_helper.json_attribute_helper import prepare_json_attribute_flatten_query
from dqlabs.utils.export_failed_rows.measure_helpers import get_default_measure_queries

# Add a timestamp to see exactly when this module is loaded
module_load_time = time.strftime("%Y-%m-%d %H:%M:%S")
log_info(f"==== ENHANCED profile.py module loaded at {module_load_time} ====")

def detect_threshold_oscillation(connection, measure_id, attribute_id, lookback_runs=5):
    """
    Detect oscillation patterns in threshold history to prevent ping-pong effects.
    
    Args:
        connection: Database connection object
        measure_id (str): ID of the measure
        attribute_id (str): ID of the attribute
        lookback_runs (int): Number of recent runs to analyze
        
    Returns:
        dict: Oscillation analysis with metrics and dampening recommendations
    """
    try:
        with connection.cursor() as cursor:
            # Get recent threshold history
            query = f"""
                SELECT 
                    lower_threshold,
                    upper_threshold,
                    run_id,
                    created_date
                FROM core.drift_threshold
                WHERE measure_id = '{measure_id}'
                AND attribute_id = '{attribute_id}'
                ORDER BY created_date DESC
                LIMIT {lookback_runs}
            """
            cursor = execute_query(connection, cursor, query)
            results = fetchall(cursor)
            
            if len(results) < 3:
                return {
                    'oscillating': False,
                    'oscillation_rate': 0.0,
                    'confidence_dampening': 1.0,
                    'change_dampening': 1.0,
                    'reason': 'insufficient_history'
                }
            
            # Extract threshold data
            thresholds = []
            for row in results:
                lower = float(row.get('lower_threshold', 0))
                upper = float(row.get('upper_threshold', 0))
                thresholds.append({
                    'lower': lower,
                    'upper': upper,
                    'range': upper - lower,
                    'midpoint': (lower + upper) / 2,
                    'run_id': row.get('run_id')
                })
            
            # Reverse to get chronological order
            thresholds.reverse()
            
            # Direction Change Analysis
            direction_changes = 0
            range_changes = 0
            
            for i in range(1, len(thresholds)):
                if i > 1:
                    prev_range_change = thresholds[i-1]['range'] - thresholds[i-2]['range']
                    curr_range_change = thresholds[i]['range'] - thresholds[i-1]['range']
                    if (prev_range_change > 0 and curr_range_change < 0) or \
                       (prev_range_change < 0 and curr_range_change > 0):
                        range_changes += 1
                    
                    prev_lower_change = thresholds[i-1]['lower'] - thresholds[i-2]['lower']
                    curr_lower_change = thresholds[i]['lower'] - thresholds[i-1]['lower']
                    prev_upper_change = thresholds[i-1]['upper'] - thresholds[i-2]['upper']
                    curr_upper_change = thresholds[i]['upper'] - thresholds[i-1]['upper']
                    
                    if (prev_lower_change > 0 and curr_lower_change < 0) or \
                       (prev_lower_change < 0 and curr_lower_change > 0):
                        direction_changes += 1
                    if (prev_upper_change > 0 and curr_upper_change < 0) or \
                       (prev_upper_change < 0 and curr_upper_change > 0):
                        direction_changes += 1
            
            # Variance Analysis
            ranges = [t['range'] for t in thresholds]
            import numpy as np
            range_variance = np.var(ranges) if len(ranges) > 1 else 0
            
            # Calculate oscillation metrics
            max_possible_changes = (len(thresholds) - 2) * 2
            direction_change_rate = direction_changes / max_possible_changes if max_possible_changes > 0 else 0
            
            max_possible_range_changes = len(thresholds) - 2
            range_change_rate = range_changes / max_possible_range_changes if max_possible_range_changes > 0 else 0
            
            # Magnitude Analysis
            magnitude_changes = []
            for i in range(1, len(thresholds)):
                prev = thresholds[i-1]
                curr = thresholds[i]
                if prev['range'] > 0:
                    range_change_magnitude = abs(curr['range'] - prev['range']) / prev['range']
                    magnitude_changes.append(range_change_magnitude)
            
            avg_magnitude_change = np.mean(magnitude_changes) if magnitude_changes else 0
            
            # Determine oscillation
            oscillation_indicators = []
            if direction_change_rate > 0.4:
                oscillation_indicators.append(f"high_direction_changes_{direction_change_rate:.2f}")
            if range_change_rate > 0.5:
                oscillation_indicators.append(f"high_range_changes_{range_change_rate:.2f}")
            if avg_magnitude_change > 0.3:
                oscillation_indicators.append(f"high_magnitude_changes_{avg_magnitude_change:.2f}")
            
            oscillation_rate = (direction_change_rate + range_change_rate + min(avg_magnitude_change, 1.0)) / 3
            is_oscillating = len(oscillation_indicators) >= 2 or oscillation_rate > 0.5
            
            if is_oscillating:
                confidence_dampening = max(0.3, 1.0 - (oscillation_rate * 0.5))
                change_dampening = max(0.05, 1.0 - (oscillation_rate * 0.8))
            else:
                confidence_dampening = 1.0
                change_dampening = 1.0
            
            result = {
                'oscillating': is_oscillating,
                'oscillation_rate': oscillation_rate,
                'confidence_dampening': confidence_dampening,
                'change_dampening': change_dampening,
                'indicators': oscillation_indicators,
                'reason': 'oscillation_detected' if is_oscillating else 'stable_pattern'
            }
            
            log_info(f"OSCILLATION_DETECTION: Measure {measure_id} - "
                    f"Oscillating: {is_oscillating}, Rate: {oscillation_rate:.3f}")
            
            return result
            
    except Exception as e:
        log_error(f"Error detecting oscillation for measure {measure_id}: {e}", e)
        return {
            'oscillating': False,
            'oscillation_rate': 0.0,
            'confidence_dampening': 1.0,
            'change_dampening': 1.0,
            'reason': 'detection_error'
        }


def apply_dampening_factor(original_change, oscillation_rate, min_factor=0.05, max_factor=1.0):
    """
    Apply dampening to threshold changes based on oscillation rate.
    
    Args:
        original_change (float): Original threshold change amount
        oscillation_rate (float): Oscillation rate from 0.0 to 1.0
        min_factor (float): Minimum dampening factor
        max_factor (float): Maximum dampening factor
        
    Returns:
        float: Dampened threshold change amount
    """
    try:
        if oscillation_rate <= 0:
            return original_change
        
        dampening_factor = max_factor - (oscillation_rate * (max_factor - min_factor))
        dampening_factor = max(min_factor, min(max_factor, dampening_factor))
        dampened_change = original_change * dampening_factor
        
        log_info(f"DAMPENING: Original: {original_change:.3f}, "
                f"Rate: {oscillation_rate:.3f}, "
                f"Factor: {dampening_factor:.3f}, "
                f"Dampened: {dampened_change:.3f}")
        
        return dampened_change
        
    except Exception as e:
        log_error(f"Error applying dampening factor: {e}", e)
        return original_change


def get_oscillation_aware_confidence_threshold(base_confidence, oscillation_analysis):
    """
    Adjust confidence thresholds based on oscillation analysis.
    
    Args:
        base_confidence (float): Base confidence threshold
        oscillation_analysis (dict): Oscillation analysis from detect_threshold_oscillation
        
    Returns:
        float: Adjusted confidence threshold
    """
    try:
        if not oscillation_analysis.get('oscillating', False):
            return base_confidence
        
        confidence_dampening = oscillation_analysis.get('confidence_dampening', 1.0)
        adjusted_confidence = base_confidence / confidence_dampening
        adjusted_confidence = min(0.95, adjusted_confidence)
        
        log_info(f"CONFIDENCE_ADJUSTMENT: Base: {base_confidence:.3f}, "
                f"Adjusted: {adjusted_confidence:.3f}")
        
        return adjusted_confidence
        
    except Exception as e:
        log_error(f"Error adjusting confidence threshold: {e}", e)
        return base_confidence


def get_previous_run_id(connection, measure_id, attribute_id, current_run_id):
    """Get the previous run ID for context-aware feedback."""
    try:
        with connection.cursor() as cursor:
            query = f"""
                SELECT run_id
                FROM core.metrics 
                WHERE measure_id = '{measure_id}' 
                AND attribute_id = '{attribute_id}'
                AND run_id != '{current_run_id}'
                ORDER BY created_date DESC 
                LIMIT 1
            """
            cursor = execute_query(connection, cursor, query)
            result = fetchone(cursor)
            return result.get('run_id') if result else None
    except Exception as e:
        log_error(f"Error getting previous run ID: {e}", e)
        return None


def get_feedback_from_previous_run(connection, measure_id, attribute_id, previous_run_id, measure_name=None):
    """Get feedback from the previous run only for current run enhancement."""
    try:
        # AND met.run_id = '{previous_run_id}'
        with connection.cursor() as cursor:
            query = f"""
                SELECT 
                    met.value,
                    met.marked_as,
                    met.run_id,
                    met.created_date,
                    met.threshold,
                    met.drift_status
                FROM core.metrics met
                WHERE met.measure_id = '{measure_id}'
                AND met.attribute_id = '{attribute_id}'
                AND met.marked_as IS NOT NULL
                AND met.measure_name = '{measure_name}'
                AND met.value IS NOT NULL
                AND met.drift_status IS NOT NULL
                ORDER BY met.created_date DESC
            """
            
            cursor = execute_query(connection, cursor, query)
            results = fetchall(cursor)
            
            feedback_data = []
            for row in results:
                try:
                    threshold_data = {}
                    if row.get('threshold'):
                        if isinstance(row['threshold'], str):
                            threshold_data = json.loads(row['threshold'])
                        else:
                            threshold_data = row['threshold']
                    
                    feedback_entry = {
                        'value': round(float(row['value']), 2),
                        'marked_as': row['marked_as'],
                        'run_id': row['run_id'],
                        'created_date': row['created_date'],
                        'threshold': threshold_data,
                        'drift_status': row['drift_status']
                    }
                    feedback_data.append(feedback_entry)
                    
                except (ValueError, TypeError, json.JSONDecodeError) as e:
                    log_error(f"Error parsing previous run feedback row: {e}", e)
                    continue
            
            log_info(f"PREVIOUS_RUN_FEEDBACK: Found {len(feedback_data)} feedback entries from previous run {previous_run_id}")
            return feedback_data
    
    except Exception as e:
        log_error("Error getting feedback from previous run", e)
        return []


def analyze_previous_run_feedback_conflicts(feedback_history, measure_name=None):
    """Analyze latest feedback for each value to detect expand/contract/conflict cases."""
    try:
        if not feedback_history:
            log_info("PREVIOUS_RUN_ANALYSIS: No previous run feedback to analyze")
            return {"expand_requests": [], "contract_requests": [], "conflicted": []}

        # Step 1: Group by 'value' and keep only latest feedback per value
        latest_feedback_by_value = {}
        for feedback in feedback_history:
            val = feedback["value"]
            current_date = feedback.get("created_date", datetime.min)
            if (
                val not in latest_feedback_by_value
                or current_date > latest_feedback_by_value[val]["created_date"]
            ):
                latest_feedback_by_value[val] = feedback

        # Step 2: Analyze latest feedback entries
        conflict_analysis = {
            "expand_requests": [],
            "contract_requests": [],
            "conflicted": [],
        }

        for value, feedback in latest_feedback_by_value.items():
            drift_status = feedback.get("drift_status", "").lower()
            marked_as = feedback.get("marked_as", "").lower()

            if (
                drift_status in ["high", "medium", "low", "alert"]
                and marked_as == "normal"
            ):
                conflict_analysis["expand_requests"].append(
                    {
                        "value": value,
                        "confidence": 0.98,
                        "feedback_data": feedback,
                        "reason": "normal_during_alert_previous_run",
                        "action": "mark as normal",
                        "original_threshold": feedback.get("threshold", {}),
                    }
                )
                log_info(
                    f"PREVIOUS_RUN_ANALYSIS: Value {value} → EXPAND REQUEST (normal during alert in latest run)"
                )

            elif drift_status == "ok" and marked_as == "outlier":
                conflict_analysis["contract_requests"].append(
                    {
                        "value": value,
                        "confidence": 0.9,
                        "feedback_data": feedback,
                        "reason": "outlier_during_ok_previous_run",
                        "action": "mark as outlier",
                        "original_threshold": feedback.get("threshold", {}),
                    }
                )
                log_info(
                    f"PREVIOUS_RUN_ANALYSIS: Value {value} → CONTRACT REQUEST (outlier during OK in latest run)"
                )

            else:
                conflict_analysis["conflicted"].append(
                    {
                        "value": value,
                        "confidence": 0.3,
                        "feedback_data": feedback,
                        "reason": f"{marked_as}_during_{drift_status}_previous_run",
                        "action": f"mark as {marked_as}",
                        "original_threshold": feedback.get("threshold", {}),
                    }
                )
                log_info(
                    f"PREVIOUS_RUN_ANALYSIS: Value {value} → CONFLICTED ({marked_as} during {drift_status} in latest run)"
                )

        log_info(
            f"PREVIOUS_RUN_ANALYSIS Summary: expand_requests={len(conflict_analysis['expand_requests'])}, "
            f"contract_requests={len(conflict_analysis['contract_requests'])}, "
            f"conflicted={len(conflict_analysis['conflicted'])}"
        )
        return conflict_analysis

    except Exception as e:
        log_error("Error analyzing previous run feedback conflicts", e)
        return {"expand_requests": [], "contract_requests": [], "conflicted": []}

def _get_previous_run_thresholds_for_discovery(connection, measure_id, attribute_id, current_run_id, measure_name=None):
    """
    SIMPLIFIED & FIXED: Get the actual thresholds from the previous run that caused the feedback.
    
    This version now correctly and exclusively uses the `core.metrics.threshold` column,
    which is the authoritative source for thresholds used during a specific profiling run.
    This avoids any potential mismatch with the long-term learned thresholds in `core.drift_threshold`.
    
    Args:
        connection: Database connection object (POSTGRES connection for metadata)
        measure_id (str): ID of the measure
        attribute_id (str): ID of the attribute
        current_run_id (str): Current run ID to exclude
        
    Returns:
        tuple: (previous_lower, previous_upper) or (None, None) if not found
    """
    try:
        with connection.cursor() as cursor:
            # SQL-FIX: This query is now simplified to only look at core.metrics.
            # It uses ROW_NUMBER() to efficiently get the most recent feedback record.
            # The CASE statement safely casts the JSON text to a float, preventing errors.
            previous_run_query = f"""
                WITH RankedMetrics AS (
                    SELECT
                        met.run_id,
                        met.threshold,
                        ROW_NUMBER() OVER(ORDER BY met.created_date DESC) as rn
                    FROM
                        core.metrics met
                    WHERE
                        met.measure_id = '{measure_id}'
                        AND met.attribute_id = '{attribute_id}'
                        AND met.run_id != '{current_run_id}'
                        AND met.measure_name = '{measure_name}'
                        AND met.marked_as IS NOT NULL
                        AND met.threshold IS NOT NULL
                )
                SELECT
                    run_id,
                    CASE
                        WHEN threshold::jsonb->>'lower_threshold' ~ '^-?[0-9]+(\.[0-9]+)?$'
                        THEN CAST(threshold::jsonb->>'lower_threshold' AS FLOAT)
                        ELSE NULL
                    END as lower_threshold,
                    CASE
                        WHEN threshold::jsonb->>'upper_threshold' ~ '^-?[0-9]+(\.[0-9]+)?$'
                        THEN CAST(threshold::jsonb->>'upper_threshold' AS FLOAT)
                        ELSE NULL
                    END as upper_threshold
                FROM
                    RankedMetrics
                WHERE
                    rn = 1;
            """
            cursor = execute_query(connection, cursor, previous_run_query)
            result = fetchone(cursor)
            
            if result and result.get('lower_threshold') is not None and result.get('upper_threshold') is not None:
                previous_lower = float(result.get('lower_threshold', 0))
                previous_upper = float(result.get('upper_threshold', 0))
                previous_run_id = result.get('run_id')
                
                log_info(f"DATABASE_DISCOVERY: Found previous run {previous_run_id} thresholds "
                         f"from core.metrics that caused feedback: ({previous_lower}, {previous_upper})")
                return previous_lower, previous_upper
            else:
                log_info(f"DATABASE_DISCOVERY: No previous run thresholds with feedback found in core.metrics")
                return None, None
                
    except Exception as e:
        log_error(f"Error getting previous run thresholds for discovery: {e}", e)
        return None, None

def _get_violating_range_boundaries(config, measure_name, feedback_requests, attribute_name, postgres_connection, measure_id, attribute_id):
    """
    FIXED: Database discovery query using PREVIOUS RUN thresholds and proper source connection.
    
    This finds ALL data values that were incorrectly flagged in the previous run,
    returning the actual MIN/MAX of violating values (not violation counts).
    
    ENHANCED: Now discovers COMPLETE data range (all MIN/MAX values) for true data-driven thresholds
    instead of only violation boundaries. This provides real upper and lower data boundaries.
    
    Args:
        config (dict): Configuration containing database connection details
        measure_name (str): Name of the measure ('length_range' or 'value_range')
        feedback_requests (list): List of expand requests with previous run context
        attribute_name (str): Name of the attribute column
        postgres_connection: PostgreSQL connection for threshold lookup
        measure_id (str): The ID of the measure being processed.
        attribute_id (str): The ID of the attribute being processed.
        
    Returns:
        tuple: (actual_min, actual_max) of complete data range or (None, None) if error/no data
    """
    try:
        from dqlabs.app_helper.dag_helper import execute_native_query, get_query_string
        
        log_info(f"DATABASE_DISCOVERY: Building complete data discovery query for {measure_name}")
        
        if not feedback_requests:
            log_info(f"DATABASE_DISCOVERY: No feedback requests provided")
            return None, None
        
        current_run_id = config.get('queue_id', '')
        
        if not measure_id or not attribute_id:
            log_info(f"DATABASE_DISCOVERY: Cannot find measure_id/attribute_id for threshold lookup")
            return None, None
        
        previous_lower, previous_upper = _get_previous_run_thresholds_for_discovery(
            postgres_connection, measure_id, attribute_id, current_run_id, measure_name
        )
        
        if previous_lower is None or previous_upper is None:
            log_info(f"DATABASE_DISCOVERY: Cannot find previous run thresholds, cannot discover data")
            return None, None
        
        log_info(f"DATABASE_DISCOVERY: Previous thresholds for reference: "
                 f"({previous_lower}, {previous_upper})")
        
        table_name = config.get("table_name")
        has_temp_table = config.get("has_temp_table")
        if has_temp_table:
            table_name = config.get("temp_view_table_name")
            
        asset = config.get("asset", {})
        connection_type = config.get("connection_type")
        if str(asset.get("view_type", "")).lower() == "direct query":
            table_name = f"({asset.get('query')}) as direct_query_table"
            if connection_type and str(connection_type).lower() in ["oracle", "bigquery"]:
                table_name = f"({asset.get('query')})"
        
        # CORRECTED: Check column datatype before applying casting
        # Get attribute datatype information
        attribute_datatype = None
        attributes_info = config.get("attributes", [])
        if attributes_info:
            attribute_info = next(
                (attr for attr in attributes_info if attr.get("name") == attribute_name),
                None
            )
            if attribute_info:
                attribute_datatype = attribute_info.get("derived_type", "").lower()
        
        # Determine if casting is needed based on datatype
        is_numeric_column = attribute_datatype in ["integer", "numeric", "decimal", "float", "double"]
        
        # SQL-FIX: Use TRY_CAST for Snowflake to handle non-numeric values gracefully.
        # This prevents the "Numeric value '' is not recognized" error.
        cast_function = "TRY_CAST" if str(connection_type).lower() == "snowflake" else "CAST"
        
        if measure_name == 'length_range':
            # ENHANCED: Get complete data range for length values
            discovery_query = f"""
                SELECT 
                    MIN(LENGTH({attribute_name})) AS violating_min,
                    MAX(LENGTH({attribute_name})) AS violating_max,
                    COUNT(*) AS violating_count
                FROM {table_name}
                WHERE {attribute_name} IS NOT NULL
            """
        elif measure_name == 'value_range':
            if is_numeric_column:
                # Column is already numeric - use directly without casting
                # ENHANCED: Get complete data range for numeric values
                discovery_query = f"""
                    SELECT 
                        MIN({attribute_name}) AS violating_min,
                        MAX({attribute_name}) AS violating_max,
                        COUNT(*) AS violating_count
                    FROM {table_name}
                    WHERE {attribute_name} IS NOT NULL
                """
                log_info(f"DATABASE_DISCOVERY: Using direct numeric column {attribute_name} (type: {attribute_datatype})")
            else:
                # Column might be text - apply casting
                # ENHANCED: Get complete data range for text-based numeric values
                discovery_query = f"""
                    SELECT 
                        MIN({cast_function}({attribute_name} AS NUMERIC)) AS violating_min,
                        MAX({cast_function}({attribute_name} AS NUMERIC)) AS violating_max,
                        COUNT(*) AS violating_count
                    FROM {table_name}
                    WHERE {attribute_name} IS NOT NULL
                    AND {cast_function}({attribute_name} AS NUMERIC) IS NOT NULL
                """
                log_info(f"DATABASE_DISCOVERY: Using casting for non-numeric column {attribute_name} (type: {attribute_datatype})")
        else:
            log_info(f"DATABASE_DISCOVERY: Unsupported measure type {measure_name}")
            return None, None
        
        log_info(f"DATABASE_DISCOVERY: Executing complete data discovery query (not just violations)")
        log_info(f"DATABASE_DISCOVERY: Query: {discovery_query}")
        
        source_connection = config.get("source_connection")
        config_copy = config.copy()
        config_copy.update({"sub_category": "DISCOVERY"})
        result, native_connection = execute_native_query(
            config_copy, discovery_query, source_connection
        )
        
        if not source_connection and native_connection:
            config["source_connection"] = native_connection
        
        if not result:
            log_info(f"DATABASE_DISCOVERY: No results from discovery query")
            return None, None
            
        violating_min = result.get('violating_min')
        violating_max = result.get('violating_max') 
        violating_count = result.get('violating_count', 0)
        
        if violating_count == 0 or violating_min is None or violating_max is None:
            log_info(f"DATABASE_DISCOVERY: No data found in complete data discovery")
            return None, None
            
        violating_min = float(violating_min)
        violating_max = float(violating_max)
        violating_count = result.get('violating_count', 0)
        log_info(f"DATABASE_DISCOVERY: Found COMPLETE data range from {violating_count} records: "
                 f"MIN={violating_min}, MAX={violating_max}")
        
        return violating_min, violating_max
        
    except Exception as e:
        log_error(f"DATABASE_DISCOVERY: Error in discovery query for {measure_name}: {e}", e)
        return None, None

def get_smart_bounds_context(config, measure_name, current_lower, current_upper):
    """
    Get smart bounds context based on measure type and dataset characteristics.
    
    Args:
        config (dict): Configuration containing dataset information
        measure_name (str): Name of the measure  
        current_lower (float): Current lower threshold
        current_upper (float): Current upper threshold
        
    Returns:
        dict: Smart bounds context with appropriate limits
    """
    try:
        # Get dataset context
        total_rows = 1000  # Default fallback
        if config:
            # Try multiple sources for total rows
            attributes = config.get("attributes", [])
            if attributes:
                total_rows = attributes[0].get("row_count", total_rows)
            if total_rows <= 1:
                total_rows = config.get("row_count", 1000)
        
        current_range = abs(current_upper - current_lower)
        
        # Context-aware bounds based on measure type
        if measure_name in ['length_range', 'value_range']:
            # Range measures - different strategy for expansion vs contraction
            return {
                'type': 'range_measure',
                'total_rows': total_rows,
                'current_range': current_range,
                'expansion_strategy': 'data_driven',  # Find actual excluded data
                'contraction_strategy': 'conservative_percentage',  # Reduce violation count
                'expansion_buffer_percent': 0.05,  # Small buffer for data inclusion
                'contraction_percent': 0.08,  # 10% fewer violations
                'max_expansion_ratio': 0.10,  # Max 20% range expansion as fallback
                'max_contraction_ratio': 0.15  # Max 15% range contraction
            }
        else:
            # Count/frequency measures - always percentage-based
            return {
                'type': 'count_measure', 
                'total_rows': total_rows,
                'expansion_percent': 0.15,  # 15% more allowed
                'contraction_percent': 0.10,  # 10% fewer allowed
                'max_expansion_absolute': min(50, total_rows * 0.05),  # Bounded by dataset
                'max_contraction_absolute': min(25, total_rows * 0.02)   # Bounded by dataset
            }
            
    except Exception as e:
        log_error(f"Error getting smart bounds context for {measure_name}: {e}", e)
        # Safe fallback
        return {
            'type': 'fallback',
            'expansion_percent': 0.10,
            'contraction_percent': 0.08,
            'max_expansion_absolute': 20,
            'max_contraction_absolute': 10
        }


def apply_range_enhancement(current_lower, current_upper, conflict_analysis, measure_name, config, measure_id, attribute_id):
    """
    ENHANCED THREE-TIER FALLBACK: Context-aware enhancement for range measures.

    This function now includes a comprehensive three-tier fallback strategy:
    1. Database Discovery (most accurate)
    2. Attribute Table Lookup (reliable fallback) 
    3. Percentage-Based (final fallback)

    ENHANCED: Now uses complete data discovery to set both upper and lower thresholds
    based on actual data boundaries, not just violations. Implements improved buffer
    calculation proportional to discovered data range.

    Args:
        current_lower (float): Current lower threshold value.
        current_upper (float): Current upper threshold value.
        conflict_analysis (dict): Analysis with expand/contract requests.
        measure_name (str): Name of the measure ('length_range' or 'value_range').
        config (dict): Configuration for database discovery queries.
        measure_id (str): The ID of the measure being processed.
        attribute_id (str): The ID of the attribute being processed.

    Returns:
        dict: Dictionary with enhanced thresholds and metadata.
    """

    try:
        enhanced_lower = float(current_lower)
        enhanced_upper = float(current_upper)
        original_lower = enhanced_lower
        original_upper = enhanced_upper

        # ENHANCED: Initialize enhancement_details for auditability
        enhancement_details = {}

        expand_requests = conflict_analysis.get('expand_requests', [])
        contract_requests = conflict_analysis.get('contract_requests', [])

        log_info(f"THREE_TIER_SMART_RANGE: Processing {len(expand_requests)} expand, "
                 f"{len(contract_requests)} contract requests for {measure_name}")

        # === OSCILLATION DETECTION ===
        oscillation_analysis = {'oscillating': False, 'oscillation_rate': 0.0, 'change_dampening': 1.0}
        if config:
            try:
                postgres_connection = config.get('postgres_connection') or get_postgres_connection(config)
                if measure_id and attribute_id and postgres_connection:
                    oscillation_analysis = detect_threshold_oscillation(
                        postgres_connection, measure_id, attribute_id, lookback_runs=5
                    )
                    log_info(f"THREE_TIER_SMART_RANGE: {measure_name} oscillation: "
                             f"oscillating={oscillation_analysis['oscillating']}, "
                             f"rate={oscillation_analysis['oscillation_rate']:.3f}")
            except Exception as e:
                log_error(f"Error detecting oscillation for {measure_name}: {e}", e)

        # Get smart bounds context
        bounds_context = get_smart_bounds_context(config, measure_name, current_lower, current_upper)

        # === OSCILLATION-AWARE CONFIDENCE FILTERING ===
        base_expand_confidence = 0.6 if not oscillation_analysis['oscillating'] else 0.8
        base_contract_confidence = 0.7 if not oscillation_analysis['oscillating'] else 0.9
        expand_confidence_threshold = get_oscillation_aware_confidence_threshold(
            base_expand_confidence, oscillation_analysis
        )
        contract_confidence_threshold = get_oscillation_aware_confidence_threshold(
            base_contract_confidence, oscillation_analysis
        )
        log_info(f"THREE_TIER_SMART_RANGE: Confidence thresholds - "
                 f"expand: {expand_confidence_threshold:.3f}, contract: {contract_confidence_threshold:.3f}")

        # === THREE-TIER EXPANSION LOGIC ===
        database_min, database_max = None, None
        if expand_requests:
            high_confidence_expands = [
                req for req in expand_requests
                if req.get('confidence', 0) >= expand_confidence_threshold
            ]
            if high_confidence_expands:
                log_info(f"THREE_TIER_SMART_RANGE: Starting three-tier expansion strategy")
                
                attribute_name = None
                if config:
                    attributes = config.get("attributes", [])
                    if attributes:
                        # Find attribute by matching attribute_id
                        for attr in attributes:
                            if attr.get("id") == attribute_id:
                                attribute_name = attr.get("name")
                                break
                        # Fallback to first attribute if no match found
                        if not attribute_name and attributes:
                            attribute_name = attributes[0].get("name")
                    if not attribute_name and 'attribute_name' in config:
                        attribute_name = config.get('attribute_name')
                
                postgres_connection = None
                try:
                    from dqlabs.app_helper.dag_helper import get_postgres_connection
                    postgres_connection = get_postgres_connection(config)
                except:
                    pass
                
                # === TIER 1: DATABASE DISCOVERY ===
                if config and attribute_name and postgres_connection:
                    log_info(f"THREE_TIER_SMART_RANGE: TIER 1 - Attempting database discovery")
                    database_min, database_max = _get_violating_range_boundaries(
                        config, measure_name, high_confidence_expands, attribute_name, postgres_connection,
                        measure_id=measure_id,
                        attribute_id=attribute_id
                    )
                
                discovery_success = database_min is not None and database_max is not None
                
                if discovery_success:
                    # === TIER 1 SUCCESS: Use Database Discovery ===
                    log_info(f"THREE_TIER_SMART_RANGE: TIER 1 SUCCESS - Found complete data range: {database_min} to {database_max}")
                    
                    enhancement_details = {
                        'action': high_confidence_expands[0].get('action', 'mark as normal'),
                        'method': 'database_discovery',
                        'tier': 1,
                        'discovery_success': True,
                        'flagged_violation_count': len(high_confidence_expands),
                        'discovered_complete_range': {
                            'min': float(database_min),
                            'max': float(database_max)
                        }
                    }
                    
                    # Set both boundaries to actual data boundaries
                    enhanced_lower = database_min
                    enhanced_upper = database_max
                    
                    # Apply minimal buffer with light dampening
                    discovered_data_range = enhanced_upper - enhanced_lower
                    if discovered_data_range > 0:
                        base_buffer = min(3, max(1, abs(enhanced_lower) * 0.001))
                    else:
                        base_buffer = 1
                    
                    dampened_buffer = apply_dampening_factor(
                        base_buffer, oscillation_analysis['oscillation_rate'],
                        min_factor=0.2, max_factor=1.0
                    )
                    
                    enhanced_lower -= dampened_buffer
                    enhanced_upper += dampened_buffer
                    
                    log_info(f"THREE_TIER_SMART_RANGE: TIER 1 - Applied buffer {dampened_buffer:.2f} - "
                             f"Final: ({enhanced_lower:.2f}, {enhanced_upper:.2f})")
                
                else:
                    # === TIER 2: ATTRIBUTE TABLE LOOKUP ===
                    # CRITICAL: Don't go directly to percentage! Try attribute table first.
                    log_info(f"THREE_TIER_SMART_RANGE: TIER 1 FAILED - Attempting TIER 2: Attribute table lookup")
                    log_info(f"THREE_TIER_SMART_RANGE: Avoiding direct percentage fallback - using stored min/max values")
                    
                    try:
                        with postgres_connection.cursor() as cursor:
                            if measure_name == 'value_range':
                                query = f"""
                                    SELECT min_value, max_value 
                                    FROM core.attribute 
                                    WHERE id = '{attribute_id}'
                                    AND min_value IS NOT NULL 
                                    AND max_value IS NOT NULL
                                """
                                cursor = execute_query(postgres_connection, cursor, query)
                                result = fetchone(cursor)
                                
                                if result and result.get('min_value') is not None and result.get('max_value') is not None:
                                    attr_min = float(result.get('min_value'))
                                    attr_max = float(result.get('max_value'))
                                    
                                    log_info(f"THREE_TIER_SMART_RANGE: TIER 2 SUCCESS - Found attribute value range: ({attr_min}, {attr_max})")
                                    
                                    enhancement_details = {
                                        'action': high_confidence_expands[0].get('action', 'mark as normal'),
                                        'method': 'attribute_table_lookup',
                                        'tier': 2,
                                        'discovery_success': True,
                                        'flagged_violation_count': len(high_confidence_expands),
                                        'attribute_range': {
                                            'min': attr_min,
                                            'max': attr_max
                                        }
                                    }
                                    
                                    # Use attribute table values with small buffer
                                    enhanced_lower = attr_min
                                    enhanced_upper = attr_max
                                    
                                    # Apply small buffer for safety
                                    attr_range = attr_max - attr_min
                                    base_buffer = max(1, attr_range * 0.02)  # 2% buffer
                                    
                                    dampened_buffer = apply_dampening_factor(
                                        base_buffer, oscillation_analysis['oscillation_rate'],
                                        min_factor=0.3, max_factor=1.0
                                    )
                                    
                                    enhanced_lower -= dampened_buffer
                                    enhanced_upper += dampened_buffer
                                    
                                    log_info(f"THREE_TIER_SMART_RANGE: TIER 2 - Used attribute values with buffer {dampened_buffer:.2f} - "
                                             f"Final: ({enhanced_lower:.2f}, {enhanced_upper:.2f})")
                                    
                                else:
                                    log_info(f"THREE_TIER_SMART_RANGE: TIER 2 FAILED - No valid min_value/max_value in attribute table")
                                    attr_min = attr_max = None
                                    
                            elif measure_name == 'length_range':
                                query = f"""
                                    SELECT min_length, max_length 
                                    FROM core.attribute 
                                    WHERE id = '{attribute_id}'
                                    AND min_length IS NOT NULL 
                                    AND max_length IS NOT NULL
                                """
                                cursor = execute_query(postgres_connection, cursor, query)
                                result = fetchone(cursor)
                                
                                if result and result.get('min_length') is not None and result.get('max_length') is not None:
                                    attr_min = float(result.get('min_length'))
                                    attr_max = float(result.get('max_length'))
                                    
                                    log_info(f"THREE_TIER_SMART_RANGE: TIER 2 SUCCESS - Found attribute length range: ({attr_min}, {attr_max})")
                                    
                                    enhancement_details = {
                                        'action': high_confidence_expands[0].get('action', 'mark as normal'),
                                        'method': 'attribute_table_lookup',
                                        'tier': 2,
                                        'discovery_success': True,
                                        'flagged_violation_count': len(high_confidence_expands),
                                        'attribute_range': {
                                            'min': attr_min,
                                            'max': attr_max
                                        }
                                    }
                                    
                                    # Use attribute table values with small buffer
                                    enhanced_lower = attr_min
                                    enhanced_upper = attr_max
                                    
                                    # Apply small integer buffer for lengths
                                    base_buffer = max(1, (attr_max - attr_min) * 0.1)  # 10% buffer for lengths
                                    
                                    dampened_buffer = apply_dampening_factor(
                                        base_buffer, oscillation_analysis['oscillation_rate'],
                                        min_factor=0.3, max_factor=1.0
                                    )
                                    
                                    enhanced_lower = max(0, enhanced_lower - dampened_buffer)  # Lengths can't be negative
                                    enhanced_upper += dampened_buffer
                                    
                                    log_info(f"THREE_TIER_SMART_RANGE: TIER 2 - Used attribute lengths with buffer {dampened_buffer:.2f} - "
                                             f"Final: ({enhanced_lower:.2f}, {enhanced_upper:.2f})")
                                    
                                else:
                                    log_info(f"THREE_TIER_SMART_RANGE: TIER 2 FAILED - No valid min_length/max_length in attribute table")
                                    attr_min = attr_max = None
                            else:
                                log_info(f"THREE_TIER_SMART_RANGE: TIER 2 SKIPPED - Unknown measure type: {measure_name}")
                                attr_min = attr_max = None
                                
                    except Exception as e:
                        log_error(f"THREE_TIER_SMART_RANGE: TIER 2 ERROR - Attribute table lookup failed: {e}", e)
                        attr_min = attr_max = None
                    
                    # === TIER 3: PERCENTAGE-BASED FALLBACK ===
                    if attr_min is None or attr_max is None:
                        log_info(f"THREE_TIER_SMART_RANGE: TIER 2 FAILED - Falling back to TIER 3: Percentage-based expansion")
                        
                        enhancement_details = {
                            'action': high_confidence_expands[0].get('action', 'mark as normal'),
                            'method': 'percentage_fallback',
                            'tier': 3,
                            'discovery_success': False,
                            'flagged_violation_count': len(high_confidence_expands),
                            'fallback_reason': 'database_discovery_and_attribute_lookup_failed'
                        }
                        
                        current_range = enhanced_upper - enhanced_lower
                        max_expansion_ratio = bounds_context.get('max_expansion_ratio', 0.10)
                        
                        if current_range > 0:
                            # Add safety cap to the expansion
                            max_abs_bound = max(abs(original_lower), abs(original_upper))
                            if max_abs_bound == 0: 
                                max_abs_bound = 1000 
                            capped_expansion = max_abs_bound * 0.20 
                            
                            base_expansion = min(current_range * max_expansion_ratio, capped_expansion)
                            log_info(f"THREE_TIER_SMART_RANGE: TIER 3 - Capped base expansion at {base_expansion:.2f}")

                            dampened_expansion = apply_dampening_factor(
                                base_expansion, oscillation_analysis['oscillation_rate'],
                                min_factor=0.05, max_factor=1.0
                            )
                            
                            enhanced_lower -= dampened_expansion / 2
                            enhanced_upper += dampened_expansion / 2
                            
                            log_info(f"THREE_TIER_SMART_RANGE: TIER 3 - Applied percentage expansion {dampened_expansion:.2f} - "
                                     f"Final: ({enhanced_lower:.2f}, {enhanced_upper:.2f})")
            else:
                log_info(f"THREE_TIER_SMART_RANGE: No high-confidence expand requests after oscillation filtering")

        # === CONSERVATIVE CONTRACTION (unchanged) ===
        if contract_requests:
            high_confidence_contracts = [
                req for req in contract_requests
                if req.get('confidence', 0) >= contract_confidence_threshold
            ]
            if high_confidence_contracts:
                log_info(f"THREE_TIER_SMART_RANGE: Applying dampened contraction")
                
                if not enhancement_details:
                    enhancement_details = {
                        'action': high_confidence_contracts[0].get('action', 'mark as outlier'),
                        'method': 'conservative_contraction',
                        'tier': 0,
                        'discovery_success': False,
                        'flagged_metric_value': float(high_confidence_contracts[0].get('value', 0)),
                        'original_range': {
                            'lower': float(original_lower),
                            'upper': float(original_upper)
                        }
                    }
                
                current_range = enhanced_upper - enhanced_lower
                contraction_percent = bounds_context.get('contraction_percent', 0.08)
                max_contraction_ratio = bounds_context.get('max_contraction_ratio', 0.15)
                
                if current_range > 0:
                    base_contraction = min(
                        current_range * contraction_percent,
                        current_range * max_contraction_ratio
                    )
                    dampened_contraction = apply_dampening_factor(
                        base_contraction, oscillation_analysis['oscillation_rate'],
                        min_factor=0.1, max_factor=1.0
                    )
                    
                    if enhancement_details.get('method') == 'conservative_contraction':
                        enhancement_details['contraction_applied_percent'] = (dampened_contraction / current_range) * 100 if current_range > 0 else 0
                    
                    half_contraction = dampened_contraction / 2
                    enhanced_lower += half_contraction
                    enhanced_upper -= half_contraction
                    
                    log_info(f"THREE_TIER_SMART_RANGE: Dampened contraction: "
                             f"base={base_contraction:.2f}, dampened={dampened_contraction:.2f}")
                elif current_range == 0:
                    import random
                    contraction_percent = random.uniform(0.05, 0.07)
                    enhanced_lower *= (1 - contraction_percent)
                    enhanced_upper *= (1 - contraction_percent)

        # === SAFE INTEGER ADJUSTMENT & FINALIZATION ===
        log_info(f"THREE_TIER_SMART_RANGE: Applying safe finalization. Pre-rounding: ({enhanced_lower:.2f}, {enhanced_upper:.2f})")
        
        if measure_name == 'length_range':
            final_lower = max(0, math.floor(enhanced_lower))
            final_upper = math.ceil(enhanced_upper)
            if final_lower >= final_upper:
                final_upper = final_lower + 1
        else:
            final_lower = math.floor(enhanced_lower)
            final_upper = math.ceil(enhanced_upper)
            if final_lower >= final_upper:
                final_upper = final_lower + 1

        # === MINIMUM CHANGE GUARANTEE ===
        if expand_requests or contract_requests:
            if final_lower == int(original_lower) and final_upper == int(original_upper):
                log_info(f"THREE_TIER_SMART_RANGE: No change detected, applying minimum adjustment")
                min_change = 1
                if contract_requests:
                    if measure_name == 'length_range':
                        final_lower = min(final_lower + min_change, final_upper - 1)
                        final_lower = max(0, final_lower)
                    else:
                        final_lower += min_change
                elif expand_requests:
                    if measure_name == 'length_range':
                        final_lower = max(0, final_lower - min_change)
                        final_upper += min_change
                    else:
                        final_lower -= min_change
                        final_upper += min_change

        # Final validation
        if final_lower > final_upper:
            log_info(f"THREE_TIER_SMART_RANGE: Invalid final bounds, reverting to original")
            return {
                'enhanced_lower': math.floor(original_lower),
                'enhanced_upper': math.ceil(original_upper),
                'enhancement_details': {}
            }

        tier_used = enhancement_details.get('tier', 'unknown')
        method_used = enhancement_details.get('method', 'unknown')
        log_info(f"THREE_TIER_SMART_RANGE: FINAL thresholds for {measure_name} using {method_used} (Tier {tier_used}): "
                 f"({final_lower}, {final_upper})")

        return {
            'enhanced_lower': final_lower,
            'enhanced_upper': final_upper,
            'enhancement_details': enhancement_details
        }

    except Exception as e:
        log_error(f"Error applying three-tier range enhancement for {measure_name}: {e}", e)
        return {
            'enhanced_lower': current_lower,
            'enhanced_upper': current_upper,
            'enhancement_details': {}
        }


def apply_other_enhancement(current_lower, current_upper, conflict_analysis, measure_name, config=None):
    """
    CORRECTED SMART BOUNDS with OSCILLATION CONTROL: Context-aware enhancement for count/frequency measures
    with intelligent oscillation detection and selective dampening.
    
    CORRECTED LOGIC:
    - **USER FEEDBACK INTENT > OSCILLATION CONTROL**: When user clearly indicates threshold problems, trust the feedback
    - **Dampening applies to**: Safety margins, fallback adjustments, boundary movements
    - **Light dampening for**: Direct feedback-based adjustments (user showed what was wrong)
    
    STRATEGY:
    - Always percentage-based for both expansion and contraction
    - Bounded by dataset size to prevent unreasonable margins
    - Smart equal bounds handling with context-aware shifting
    - Oscillation control prevents wild swings while respecting user intent
    
    Args:
        current_lower (float): Current lower threshold value
        current_upper (float): Current upper threshold value  
        conflict_analysis (dict): Analysis containing expand/contract requests from feedback
        measure_name (str): Name of the measure for logging and type-specific handling
        config (dict, optional): Configuration for dataset context
        
    Returns:
        tuple: (enhanced_lower, enhanced_upper) - Enhanced threshold values with oscillation control
    """
    try:
        enhanced_lower = float(current_lower)
        enhanced_upper = float(current_upper)
        original_lower = enhanced_lower
        original_upper = enhanced_upper

        expand_requests = conflict_analysis.get('expand_requests', [])
        contract_requests = conflict_analysis.get('contract_requests', [])

        log_info(f"DATA_DRIVEN_COUNT_ENHANCEMENT: Processing {len(expand_requests)} expand, "
                 f"{len(contract_requests)} contract requests for {measure_name}")

        # === OSCILLATION DETECTION ===
        oscillation_analysis = {'oscillating': False, 'oscillation_rate': 0.0, 'change_dampening': 1.0}
        
        if config:
            try:
                postgres_connection = None
                if 'postgres_connection' in config:
                    postgres_connection = config['postgres_connection']
                else:
                    from dqlabs.app_helper.dag_helper import get_postgres_connection
                    postgres_connection = get_postgres_connection(config)
                
                measure_id = config.get('measure_id')
                attribute_id = config.get('attribute_id')
                
                if measure_id and attribute_id and postgres_connection:
                    oscillation_analysis = detect_threshold_oscillation(
                        postgres_connection, measure_id, attribute_id, lookback_runs=5
                    )
                    
                    log_info(f"DATA_DRIVEN_COUNT_ENHANCEMENT: {measure_name} oscillation: "
                            f"oscillating={oscillation_analysis['oscillating']}, "
                            f"rate={oscillation_analysis['oscillation_rate']:.3f}")
                    
            except Exception as e:
                log_error(f"Error detecting oscillation for {measure_name}: {e}", e)

        # Get smart bounds context for count measures
        bounds_context = get_smart_bounds_context(config, measure_name, current_lower, current_upper)
        
        # Modify bounds context based on oscillation
        if oscillation_analysis['oscillating']:
            # DAMPENED PARAMETERS when oscillating
            bounds_context['expansion_percent'] = 0.08      # Reduced from 0.15
            bounds_context['contraction_percent'] = 0.05    # Reduced from 0.10
            bounds_context['max_expansion_absolute'] = min(20, bounds_context.get('max_expansion_absolute', 50))
            bounds_context['max_contraction_absolute'] = min(10, bounds_context.get('max_contraction_absolute', 25))
            log_info(f"DATA_DRIVEN_COUNT_ENHANCEMENT: Applied oscillation dampening to bounds context")

        # === OSCILLATION-AWARE CONFIDENCE FILTERING ===
        base_expand_confidence = 0.6 if not oscillation_analysis['oscillating'] else 0.8
        base_contract_confidence = 0.7 if not oscillation_analysis['oscillating'] else 0.9
        
        expand_confidence_threshold = get_oscillation_aware_confidence_threshold(
            base_expand_confidence, oscillation_analysis
        )
        contract_confidence_threshold = get_oscillation_aware_confidence_threshold(
            base_contract_confidence, oscillation_analysis
        )
        
        log_info(f"DATA_DRIVEN_COUNT_ENHANCEMENT: Confidence thresholds - "
                f"expand: {expand_confidence_threshold:.3f}, contract: {contract_confidence_threshold:.3f}")

        # === CORRECTED: SMART PERCENTAGE-BASED EXPANSION ===
        if expand_requests:
            high_confidence_expands = [
                req for req in expand_requests 
                if req.get('confidence', 0) >= expand_confidence_threshold
            ]
            
            if high_confidence_expands:
                log_info(f"DATA_DRIVEN_COUNT_ENHANCEMENT: Applying smart bounded expansion for "
                         f"{len(high_confidence_expands)} requests")
                
                expansion_percent = bounds_context.get('expansion_percent', 0.15)
                max_expansion_absolute = bounds_context.get('max_expansion_absolute', 50)
                
                # Handle equal bounds case with smart expansion
                if enhanced_lower == enhanced_upper:
                    log_info(f"DATA_DRIVEN_COUNT_ENHANCEMENT: Equal bounds expansion ({enhanced_lower})")
                    feedback_count = float(high_confidence_expands[0]['value'])
                    
                    # Calculate base bounded margin
                    base_margin = min(
                        max(1, feedback_count * expansion_percent),  # Percentage-based
                        max_expansion_absolute  # Absolute limit
                    )
                    
                    # Apply LIGHT dampening for user feedback (trust the user intent)
                    margin = apply_dampening_factor(
                        base_margin, oscillation_analysis['oscillation_rate'],
                        min_factor=0.6, max_factor=1.0  # Light dampening for user feedback
                    )
                    
                    # Create range around the feedback count with dampened margins
                    enhanced_lower = feedback_count - margin
                    enhanced_upper = feedback_count + margin
                    
                    log_info(f"DATA_DRIVEN_COUNT_ENHANCEMENT: Created bounded expanded range with light dampening: "
                             f"feedback={feedback_count}, base_margin={base_margin:.1f}, "
                             f"dampened_margin={margin:.1f}, new range=({enhanced_lower}, {enhanced_upper})")
                
                else:
                    # Normal case: non-equal bounds expansion with smart bounds
                    for req in high_confidence_expands:
                        feedback_count = float(req['value'])
                        
                        # Calculate base bounded margin
                        base_margin = min(
                            max(1, feedback_count * expansion_percent),  # Percentage-based
                            max_expansion_absolute  # Dataset-relative limit
                        )
                        
                        # Apply LIGHT dampening for user feedback
                        margin = apply_dampening_factor(
                            base_margin, oscillation_analysis['oscillation_rate'],
                            min_factor=0.6, max_factor=1.0  # Light dampening for user feedback
                        )
                        
                        # Expand bounds to include the feedback count with dampened margin
                        if feedback_count < enhanced_lower:
                            enhanced_lower = feedback_count - margin
                            log_info(f"DATA_DRIVEN_COUNT_ENHANCEMENT: Bounded expansion of lower bound with light dampening")
                        if feedback_count > enhanced_upper:
                            enhanced_upper = feedback_count + margin  
                            log_info(f"DATA_DRIVEN_COUNT_ENHANCEMENT: Bounded expansion of upper bound with light dampening")

            else:
                log_info(f"DATA_DRIVEN_COUNT_ENHANCEMENT: No high-confidence expand requests after oscillation filtering")

        # === CORRECTED: SMART PERCENTAGE-BASED CONTRACTION ===
        if contract_requests:
            high_confidence_contracts = [
                req for req in contract_requests 
                if req.get('confidence', 0) >= contract_confidence_threshold
            ]
            
            if high_confidence_contracts:
                log_info(f"DATA_DRIVEN_COUNT_ENHANCEMENT: Applying smart bounded contraction")
                
                contraction_percent = bounds_context.get('contraction_percent', 0.10)
                max_contraction_absolute = bounds_context.get('max_contraction_absolute', 25)
                
                # Handle equal bounds with smart shifting
                if enhanced_lower == enhanced_upper:
                    log_info(f"DATA_DRIVEN_COUNT_ENHANCEMENT: Equal bounds contraction ({enhanced_lower})")
                    feedback_count = float(high_confidence_contracts[0]['value'])
                    
                    # Determine base shift amount
                    base_shift = min(
                        max(1, abs(feedback_count) * 0.06),  # 6% shift
                        max_contraction_absolute / 2  # Half of max contraction
                    )
                    
                    # Apply LIGHT dampening for user feedback
                    shift_amount = apply_dampening_factor(
                        base_shift, oscillation_analysis['oscillation_rate'],
                        min_factor=0.6, max_factor=1.0  # Light dampening for user feedback
                    )
                    
                    # Smart shifting strategy to EXCLUDE problematic count with buffer
                    if feedback_count <= enhanced_lower:
                        # User wants to flag low counts - shift range above outlier
                        enhanced_lower = feedback_count + shift_amount
                        enhanced_upper = enhanced_lower + max(1, shift_amount)
                        log_info(f"DATA_DRIVEN_COUNT_ENHANCEMENT: Shifted range above outlier with light dampening")
                    else:
                        # User wants to flag high counts - shift range below outlier
                        enhanced_upper = feedback_count - shift_amount
                        enhanced_lower = max(0, enhanced_upper - max(1, shift_amount))
                        log_info(f"DATA_DRIVEN_COUNT_ENHANCEMENT: Shifted range below outlier with light dampening")
                
                # Standard range contraction with smart bounded margins
                else:
                    log_info(f"DATA_DRIVEN_COUNT_ENHANCEMENT: Applying smart bounded contraction for non-equal bounds")
                    for req in high_confidence_contracts:
                        outlier_count = float(req['value'])
                        
                        # Only contract if the outlier count is currently within acceptable range
                        if enhanced_lower <= outlier_count <= enhanced_upper:
                            # Calculate distances to determine which boundary to move
                            upper_distance = enhanced_upper - outlier_count
                            lower_distance = outlier_count - enhanced_lower
                            
                            # Calculate base bounded margin for exclusion
                            base_exclusion_margin = min(
                                max(1, abs(outlier_count) * contraction_percent),  # Percentage-based
                                max_contraction_absolute  # Absolute limit
                            )
                            
                            # Apply LIGHT dampening for user feedback
                            exclusion_margin = apply_dampening_factor(
                                base_exclusion_margin, oscillation_analysis['oscillation_rate'],
                                min_factor=0.6, max_factor=1.0  # Light dampening for user feedback
                            )

                            # Move the closer boundary with margin to exclude the outlier count
                            if upper_distance <= lower_distance:
                                enhanced_upper = max(enhanced_lower, outlier_count - exclusion_margin)
                                log_info(f"DATA_DRIVEN_COUNT_ENHANCEMENT: Upper bound moved with light dampening")
                            else:
                                enhanced_lower = min(enhanced_upper, outlier_count + exclusion_margin)
                                log_info(f"DATA_DRIVEN_COUNT_ENHANCEMENT: Lower bound moved with light dampening")
            else:
                log_info(f"DATA_DRIVEN_COUNT_ENHANCEMENT: No high-confidence contract requests after oscillation filtering")

        # Final validation to ensure bounds are logical
        if enhanced_lower > enhanced_upper:
            log_info(f"DATA_DRIVEN_COUNT_ENHANCEMENT: Invalid bounds ({enhanced_lower}, {enhanced_upper}), "
                     f"reverting to original")
            return math.floor(original_lower), math.ceil(original_upper)

        # Final type conversion for count/frequency thresholds
        if measure_name in ['distinct_count', 'null_count', 'blank_count', 'length']:
            # Count measures must be non-negative integers
            final_lower = max(0, math.floor(enhanced_lower))
            final_upper = max(final_lower, math.ceil(enhanced_upper))
        else:
            # Other measures can be any appropriate type
            final_lower = math.floor(enhanced_lower)
            final_upper = math.ceil(enhanced_upper)

        # Log results with oscillation context
        if final_lower != math.floor(original_lower) or final_upper != math.ceil(original_upper):
            oscillation_status = "OSCILLATING" if oscillation_analysis['oscillating'] else "STABLE"
            log_info(f"DATA_DRIVEN_COUNT_ENHANCEMENT: Final {oscillation_status} enhanced thresholds for {measure_name}: "
                     f"({original_lower}, {original_upper}) -> ({final_lower}, {final_upper}) "
                     f"[oscillation_rate: {oscillation_analysis['oscillation_rate']:.3f}]")
        else:
            log_info(f"DATA_DRIVEN_COUNT_ENHANCEMENT: No changes needed for {measure_name}")
        return final_lower, final_upper

    except Exception as e:
        log_error(f"Error applying data-driven count enhancement for {measure_name}: {e}", e)
        return current_lower, current_upper


def is_convertible_to_float(value):
    try:
        float(value)
        return True
    except ValueError:
        return False


def get_health_metrics(config: dict):
    """
    Returns the health metrics of the recent run for the given asset
    """
    queue_id = config.get("queue_id")
    asset_id = config.get("asset_id")
    level = config.get("level")
    attribute_id = config.get("attribute_id")
    attribute_filter = ""
    metrics = []
    if level == "attribute":
        attribute_filter = f"and met.attribute_id='{attribute_id}' "

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            select base.technical_name as name, met.value, mes.attribute_id
            from core.metrics as met
            join core.measure as mes on mes.id=met.measure_id
            join core.base_measure as base on base.id=mes.base_measure_id and base.is_visible=true
            left join core.attribute as att on att.id=mes.attribute_id
            where met.asset_id='{asset_id}' and met.run_id='{queue_id}' and base.type='{DISTRIBUTION}'
            and base.category='{HEALTH}'
            {attribute_filter}
            and (att.id is null or (att.id is not null and att.profile = true))
        """
        cursor = execute_query(connection, cursor, query_string)
        metrics = fetchall(cursor)
    return metrics


def get_stored_threshold(config, measure_name, attribute_id, connection=None, specific_value=None):
    """
    Enhanced get_stored_threshold that incorporates context-aware user feedback.
    Now supports both aggregate measures and sub-value measures (patterns, enums, lengths).
    
    Args:
        config (dict): Configuration dictionary containing run and connection details
        measure_name (str): Name of the measure (e.g., 'value_range', 'length_range', 'long_pattern')
        attribute_id (str): ID of the attribute being processed
        connection: Database connection object (optional)
        specific_value (str): For sub-value measures, the specific pattern/enum/length value (optional)
        
    Returns:
        tuple: (lower_threshold, upper_threshold, found, feedback_stats)
    """
    if not connection:
        connection = get_postgres_connection(config)

    feedback_stats = None

    try:
        with connection.cursor() as cursor:
            # Get measure_id
            measure_id_query = f"""
                SELECT id FROM core.measure
                WHERE technical_name = '{measure_name}'
                AND attribute_id = '{attribute_id}'
                AND is_active = True
                LIMIT 1
            """
            cursor = execute_query(connection, cursor, measure_id_query)
            measure_id_result = fetchone(cursor)

            if not measure_id_result:
                log_info(f"No active measure found for {measure_name}, attribute_id={attribute_id}")
                return 0, 0, False, None

            measure_id = measure_id_result.get("id")
            
            # NEW: Check if this measure has sub-values using database-driven detection
            is_sub_value_measure = has_sub_values(measure_id, connection)
            
            # Get baseline threshold from core.drift_threshold or core.metrics
            if is_sub_value_measure:
                specific_value = specific_value.strip("'") if specific_value else ""
                query_string = f"""
                    SELECT lower_threshold, upper_threshold, created_date
                    FROM core.drift_threshold 
                    WHERE measure_id = '{measure_id}' 
                    AND attribute_id = '{attribute_id}'
                    AND measure_name = '{specific_value}'
                    ORDER BY created_date DESC 
                    LIMIT 1
                """
            else:
                measure_name = measure_name.strip("'") if measure_name else ""
                query_string = f"""
                    SELECT lower_threshold, upper_threshold, created_date
                    FROM core.drift_threshold 
                    WHERE measure_id = '{measure_id}' 
                    AND attribute_id = '{attribute_id}'
                    AND measure_name = '{measure_name}'
                    ORDER BY created_date DESC 
                    LIMIT 1
                """
            cursor = execute_query(connection, cursor, query_string)
            result = fetchone(cursor)
            
            original_lower = 0
            original_upper = 0
            found = False

            if result:
                original_lower = float(result.get("lower_threshold", 0))
                original_upper = float(result.get("upper_threshold", 0))
                created_date = result.get("created_date")
                found = True
                
            else:
                # Fallback to core.metrics table
                log_info(f"No stored threshold found in drift_threshold for {measure_name}, trying metrics table")
                
                if is_sub_value_measure:
                    query_string = f"""
                        SELECT threshold, created_date
                        FROM core.metrics 
                        WHERE measure_id = '{measure_id}' 
                        AND attribute_id = '{attribute_id}'
                        AND measure_name = '{specific_value}'
                        AND threshold IS NOT NULL
                        ORDER BY created_date DESC 
                        LIMIT 1
                    """
                else:
                    query_string = f"""
                        SELECT threshold, created_date
                        FROM core.metrics 
                        WHERE measure_id = '{measure_id}' 
                        AND attribute_id = '{attribute_id}'
                        AND measure_name = '{measure_name}'
                        AND threshold IS NOT NULL
                        ORDER BY created_date DESC 
                        LIMIT 1
                    """
                cursor = execute_query(connection, cursor, query_string)
                result = fetchone(cursor)

                if result and result.get("threshold"):
                    threshold_json = result.get("threshold")
                    created_date = result.get("created_date")

                    if isinstance(threshold_json, str):
                        import json
                        try:
                            threshold_data = json.loads(threshold_json)
                        except json.JSONDecodeError:
                            log_error(f"Invalid JSON in threshold column: {threshold_json}")
                            return 0, 0, False, None
                    else:
                        threshold_data = threshold_json

                    original_lower = float(threshold_data.get("lower_threshold", 0))
                    original_upper = float(threshold_data.get("upper_threshold", 0))
                    found = True
                    log_info(f"Found threshold in metrics table for {measure_name}: "
                             f"lower={original_lower}, upper={original_upper}, date={created_date}")
            
            if not found:
                return 0, 0, False, None

            # Apply context-aware feedback enhancement based on measure type
            current_run_id = config.get("queue_id", "")
            previous_run_id = get_previous_run_id(connection, measure_id, attribute_id, current_run_id)
            
            if not previous_run_id:
                return original_lower, original_upper, found, None

            # Get feedback based on measure type
            if is_sub_value_measure and specific_value:
                # Sub-value measure with specific value (pattern/enum/length)
                feedback_history = get_feedback_for_specific_value(
                    connection, measure_id, attribute_id, previous_run_id, specific_value
                )
            elif not is_sub_value_measure:
                # Aggregate measure (existing logic)
                feedback_history = get_feedback_from_previous_run(
                    connection, measure_id, attribute_id, previous_run_id, measure_name
                )
            else:
                # Sub-value measure but no specific value provided - skip enhancement
                return original_lower, original_upper, found, None
            
            if not feedback_history:
                return original_lower, original_upper, found, None

            # Analyze feedback and apply enhancement
            conflict_analysis = analyze_previous_run_feedback_conflicts(feedback_history, measure_name)
            if measure_name in ['length_range', 'value_range']:
                # Range measures use their specific enhancement logic
                # ENHANCED: Handle new dictionary format from apply_range_enhancement
                enhancement_result = apply_range_enhancement(
                    original_lower, original_upper, conflict_analysis, measure_name, config,
                    measure_id=measure_id,
                    attribute_id=attribute_id
                )
                # ENHANCED: Unpack the dictionary result
                enhanced_lower = enhancement_result['enhanced_lower']
                enhanced_upper = enhancement_result['enhanced_upper']
                enhancement_details = enhancement_result['enhancement_details']
            else:
                # Other measures use the updated enhancement logic
                enhanced_lower, enhanced_upper = apply_other_enhancement(
                    original_lower, original_upper, conflict_analysis, measure_name
                )
                # ENHANCED: Create empty enhancement_details for non-range measures
                enhancement_details = {}
            
            # ENHANCED: Prepare feedback metadata for storage with enhancement details
            if current_run_id:
                feedback_stats = {
                    'run_id': current_run_id,
                    'measure_id': measure_id,
                    'attribute_id': attribute_id,
                    'connection': connection,
                    'enhanced_lower': enhanced_lower,
                    'enhanced_upper': enhanced_upper,
                    'stats': {
                        # ENHANCED: Include enhancement_details if available
                        'enhancement_details': enhancement_details if enhancement_details else {
                            'expand_count': len(conflict_analysis.get('expand_requests', [])),
                            'contract_count': len(conflict_analysis.get('contract_requests', [])),
                            'specific_value': specific_value if specific_value else None
                        }
                    }
                }
            return enhanced_lower, enhanced_upper, True, feedback_stats

    except Exception as e:
        log_error(f"Error retrieving threshold for {measure_name}, attribute_id={attribute_id}", e)
        return 0, 0, False, None


def store_feedback_metadata_in_threshold_column(connection, measure_id, attribute_id, run_id, 
                                                enhanced_lower, enhanced_upper, feedback_stats, config):
    """
    Store the ENHANCED thresholds with extensive logging and discovery metadata.
    
    ENHANCED: Now includes discovery success/failure metadata for intelligent threshold management.
    """
    try:
        if connection.closed:
            connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            # ENHANCED: Extract the enhancement_details from the feedback_stats parameter
            enhancement_details = feedback_stats.get('enhancement_details', {})
            
            # ENHANCED: Create an empty list called feedback_triggers
            feedback_triggers = []
            
            # ENHANCED: If enhancement_details is not empty, build a new dictionary representing the trigger
            if enhancement_details:
                trigger = {}
                
                # Build trigger dictionary containing keys like action, flagged_violation_count, discovered_breaching_range, etc.
                trigger['action'] = enhancement_details.get('action')
                
                if 'flagged_violation_count' in enhancement_details:
                    trigger['flagged_violation_count'] = enhancement_details['flagged_violation_count']
                
                if 'discovered_breaching_range' in enhancement_details:
                    trigger['discovered_breaching_range'] = enhancement_details['discovered_breaching_range']
                
                if 'flagged_metric_value' in enhancement_details:
                    trigger['flagged_metric_value'] = enhancement_details['flagged_metric_value']
                
                if 'original_range' in enhancement_details:
                    trigger['original_range'] = enhancement_details['original_range']
                
                if 'contraction_applied_percent' in enhancement_details:
                    trigger['contraction_applied_percent'] = enhancement_details['contraction_applied_percent']
                
                # ENHANCED: Append it to the feedback_triggers list
                feedback_triggers.append(trigger)
            
            # ENHANCED: Replace the current logic for building the threshold_data dictionary
            threshold_data = {
                "lt_percent": 0,
                "ut_percent": 0,
                "lower_threshold": enhanced_lower,  # Use parameter directly
                "upper_threshold": enhanced_upper,  # Use parameter directly
                "feedback_enhanced": True,
                "feedback_metadata": {
                    "enhancement_type": "context_aware_current_run",
                    "enhancement_timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                    # ENHANCED: Add discovery success/failure metadata for intelligent threshold management
                    "method": enhancement_details.get('method', 'unknown'),
                    "discovery_success": enhancement_details.get('discovery_success', False),
                    # ENHANCED: In the final threshold_data object, set the value of the feedback_triggers key
                    "feedback_triggers": feedback_triggers
                }
            }

            # Update threshold column for current run with proper JSON escaping
            # The rest of the function (JSON serialization and database update) remains the same
            threshold_json = json.dumps(threshold_data).replace("'", "''")
            log_info(f"Threshold data inside store feedback metadata thresold column function: {threshold_json}")
            update_query = f"""
                UPDATE core.metrics
                SET threshold = '{threshold_json}'
                WHERE measure_id = '{measure_id}'
                AND attribute_id = '{attribute_id}'
                AND run_id = '{run_id}'
            """
            log_info(f"Update query in store feedback metadata function : {update_query}")
            cursor = execute_query(connection, cursor, update_query)

            if cursor.rowcount > 0:
                log_info(f"ENHANCED_STORAGE: Updated {cursor.rowcount} records with enhanced values: ({enhanced_lower}, {enhanced_upper})")
                log_info(f"ENHANCED_STORAGE: Discovery metadata - method: {enhancement_details.get('method', 'unknown')}, "
                         f"success: {enhancement_details.get('discovery_success', False)}")
            else:
                log_info(f"ENHANCED_STORAGE: No metrics record found to update for run {run_id}")

    except Exception as e:
        log_error(f"ENHANCED_STORAGE: Error storing enhanced threshold metadata: {e}", e)

def store_baseline_metadata_in_threshold_column(connection, measure_id, attribute_id, run_id, 
                                               baseline_lower, baseline_upper, calculation_stats, measure_name, config):
    """
    Store baseline thresholds with extensive debugging and explicit commit.
    
    Args:
        connection: Database connection object
        measure_id (str): ID of the measure
        attribute_id (str): ID of the attribute
        run_id (str): ID of the current run
        baseline_lower (float): Calculated lower threshold
        baseline_upper (float): Calculated upper threshold
        calculation_stats (dict): Metadata about how the threshold was calculated
        measure_name (str): Name of the measure for proper record identification
    """
    
    # Debug the actual parameter values and types
    log_info(f"BASELINE_STORAGE: ENTERING function for {measure_name}")
    log_info(f"DEBUG_CONVERSION: baseline_lower = {baseline_lower} (type: {type(baseline_lower)})")
    log_info(f"DEBUG_CONVERSION: baseline_upper = {baseline_upper} (type: {type(baseline_upper)})")
    log_info(f"DEBUG_CONVERSION: measure_id = {measure_id}")
    log_info(f"DEBUG_CONVERSION: attribute_id = {attribute_id}")
    log_info(f"DEBUG_CONVERSION: run_id = {run_id}")
    log_info(f"DEBUG_CONVERSION: measure_name = {measure_name}")
    log_info(f"DEBUG_CONVERSION: calculation_stats = {calculation_stats}")
    
    try:
        # Check connection status
        log_info(f"DEBUG_CONNECTION: Connection closed = {connection.closed}")
        log_info(f"DEBUG_CONNECTION: Connection autocommit = {connection.autocommit}")
        if connection.closed:
            connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            log_info(f"BASELINE_STORAGE: Cursor created successfully")
            
            # Ensure we have proper float values
            try:
                baseline_lower = float(baseline_lower)
                baseline_upper = float(baseline_upper)
                log_info(f"DEBUG_CONVERSION: After float conversion - lower={baseline_lower}, upper={baseline_upper}")
            except (ValueError, TypeError) as e:
                log_error(f"BASELINE_STORAGE: Error converting to float: {e}", e)
                return
            
            # Create threshold data structure
            threshold_data = {
                "lt_percent": 0,
                "ut_percent": 0,
                "lower_threshold": baseline_lower,
                "upper_threshold": baseline_upper,
                "feedback_enhanced": False,  # Key flag indicating this is baseline, not enhanced
                "calculation_metadata": {
                    "calculation_type": calculation_stats.get('calculation_type', 'fresh_attribute_values'),
                    "method": calculation_stats.get('method', 'first_run'),
                    "source": calculation_stats.get('source', 'core.attribute'),
                    "calculation_timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "is_baseline": True
                }
            }
            
            # Debug the dictionary before JSON conversion
            log_info(f"DEBUG_CONVERSION: threshold_data = {threshold_data}")
            
            # JSON conversion step by step with debugging
            try:
                threshold_json_raw = json.dumps(threshold_data)
                log_info(f"DEBUG_CONVERSION: JSON before replace = {threshold_json_raw}")
                
                threshold_json = threshold_json_raw.replace("'", "''")
                log_info(f"DEBUG_CONVERSION: JSON after replace = {threshold_json}")
                
                # Verify the JSON contains our expected values
                if f'"lower_threshold": {baseline_lower}' not in threshold_json_raw:
                    log_error(f"DEBUG_CONVERSION: WARNING - lower_threshold {baseline_lower} not found in JSON!")
                if f'"upper_threshold": {baseline_upper}' not in threshold_json_raw:
                    log_error(f"DEBUG_CONVERSION: WARNING - upper_threshold {baseline_upper} not found in JSON!")
                    
            except Exception as e:
                log_error(f"BASELINE_STORAGE: JSON conversion error: {e}", e)
                return

            # Build and log the UPDATE query
            update_query = f"""
                UPDATE core.metrics
                SET threshold = '{threshold_json}'
                WHERE measure_id = '{measure_id}'
                AND attribute_id = '{attribute_id}'
                AND run_id = '{run_id}'
                AND (measure_name = '{measure_name}' OR measure_name IS NULL)
                AND is_auto = True
            """
            
            log_info(f"DEBUG_QUERY: About to execute UPDATE query:")
            log_info(f"DEBUG_QUERY: {update_query}")
            
            # Execute the query
            cursor = execute_query(connection, cursor, update_query)
            log_info(f"DEBUG_QUERY: Query executed, rowcount = {cursor.rowcount}")
            
            # Count matching records
            count_query = f"""
                SELECT COUNT(*) as record_count
                FROM core.metrics
                WHERE measure_id = '{measure_id}'
                AND attribute_id = '{attribute_id}'
                AND run_id = '{run_id}'
                AND (measure_name = '{measure_name}' OR measure_name IS NULL)
            """
            cursor = execute_query(connection, cursor, count_query)
            count_result = fetchone(cursor)
            log_info(f"DEBUG_COUNT: Found {count_result.get('record_count', 0)} matching records")

            # Show all matching records
            all_records_query = f"""
                SELECT id, measure_name, threshold, created_date
                FROM core.metrics
                WHERE measure_id = '{measure_id}'
                AND attribute_id = '{attribute_id}'
                AND run_id = '{run_id}'
                AND (measure_name = '{measure_name}' OR measure_name IS NULL)
            """
            cursor = execute_query(connection, cursor, all_records_query)
            all_results = fetchall(cursor)
            for i, record in enumerate(all_results):
                log_info(f"DEBUG_RECORD_{i}: id={record.get('id')}, measure_name='{record.get('measure_name')}', threshold={record.get('threshold')}")
            
            # Explicit commit to ensure persistence
            try:
                if not connection.autocommit:
                    connection.commit()
                    log_info(f"BASELINE_STORAGE: Transaction committed successfully")
                else:
                    log_info(f"BASELINE_STORAGE: Connection in autocommit mode, no manual commit needed")
            except Exception as commit_error:
                log_error(f"BASELINE_STORAGE: Commit failed: {commit_error}", commit_error)
                return
            
            # Report results
            if cursor.rowcount > 0:
                log_info(f"BASELINE_STORAGE: Updated {cursor.rowcount} records with baseline values: ({baseline_lower}, {baseline_upper})")
                log_info(f"BASELINE_STORAGE: Successfully stored baseline thresholds for {measure_name} (measure_id={measure_id})")
                
                # Verify the update by reading back the data (optional verification)
                try:
                    verify_query = f"""
                        SELECT threshold 
                        FROM core.metrics 
                        WHERE measure_id = '{measure_id}'
                        AND attribute_id = '{attribute_id}'
                        AND run_id = '{run_id}'
                        AND (measure_name = '{measure_name}' OR measure_name IS NULL)
                    """
                    cursor = execute_query(connection, cursor, verify_query)
                    verify_result = fetchone(cursor)
                    if verify_result:
                        stored_threshold = verify_result.get('threshold')
                        log_info(f"DEBUG_VERIFY: Stored threshold verification = {stored_threshold}")
                    else:
                        log_info(f"DEBUG_VERIFY: No record found for verification query")
                except Exception as verify_error:
                    log_error(f"DEBUG_VERIFY: Verification query failed: {verify_error}", verify_error)
                    
            else:
                log_info(f"BASELINE_STORAGE: No metrics record found to update for {measure_name}, "
                        f"measure_id={measure_id}, run_id={run_id}")
                
                # Debug: Check if records exist with different criteria
                try:
                    debug_query = f"""
                        SELECT measure_name, threshold 
                        FROM core.metrics 
                        WHERE measure_id = '{measure_id}'
                        AND attribute_id = '{attribute_id}'
                        AND run_id = '{run_id}'
                    """
                    cursor = execute_query(connection, cursor, debug_query)
                    debug_results = fetchall(cursor)
                    log_info(f"DEBUG_NOROWS: Found {len(debug_results)} records without measure_name filter:")
                    for result in debug_results:
                        log_info(f"DEBUG_NOROWS: measure_name='{result.get('measure_name')}', threshold={result.get('threshold')}")
                except Exception as debug_error:
                    log_error(f"DEBUG_NOROWS: Debug query failed: {debug_error}", debug_error)
                
    except Exception as e:
        log_error(f"BASELINE_STORAGE: Error storing baseline threshold metadata for {measure_name} "
                 f"(measure_id={measure_id}): {e}", e)
        try:
            if not connection.autocommit:
                connection.rollback()
                log_info(f"BASELINE_STORAGE: Transaction rolled back due to error")
        except:
            pass  # Ignore rollback errors

def get_distributions(
    values: list, distribution_type: DistributionTypes, language_support: dict = {}
):
    """
    Prepares the univercal patterns for the given values
    """
    distributions = {}
    universal_patterns = {}
    short_universal_patterns = {}
    length_distribution = {}
    space_distribution = {"leading": 0, "trailing": 0, "inner": 0, "outer": 0}
    if not values:
        distributions = {
            "values": values,
            "universal_patterns": [],
            "short_universal_patterns": [],
            "length_distribution": [],
            "space_distribution": space_distribution,
        }
        return distributions

    for value in values:
        enum_value = value.get("enum_value")
        value_count = value.get("value_count")
        value_count = (
            int(float(value_count))
            if value_count and is_convertible_to_float(value_count)
            else 0
        )

        key = str(enum_value)
        key_length = len(key)
        universal_pattern = ""

        european_conversion = re.sub(
            "[\u00c0-\u00ff\u0100-\u017f\u0180-\u024f\u1e00-\u1eff\u0370-\u03ff]",
            "A",
            key,
        )
        key = european_conversion if language_support.get("european", None) else key
        if distribution_type not in [
            DistributionTypes.Pattern,
            DistributionTypes.ShortPattern,
        ]:
            letters_conversion = re.sub("[A-Za-z]", "A", key)
            numerics_conversion = re.sub("[0-9]", "N", letters_conversion)
            quotes_conversion = re.sub("['\"]", "Q", numerics_conversion)
            empty_conversion = re.sub("^$", "E", quotes_conversion)
            universal_pattern = re.sub("[ ]", "S", empty_conversion)
            if enum_value == "":
                universal_pattern = "E"
        else:
            universal_pattern = key

        short_universal_pattern = ""
        short_letters_conversion = re.sub("A{1,}", "A", universal_pattern)
        short_numerics_conversion = re.sub("N{1,}", "N", short_letters_conversion)
        short_quotes_conversion = re.sub("['\"]+", "Q", short_numerics_conversion)
        short_empty_conversion = re.sub("E{1,}", "E", short_quotes_conversion)
        short_universal_pattern = re.sub("S{1,}", "S", short_empty_conversion)
        if enum_value is None:
            key_length = 0
            universal_pattern = "NULL"
            short_universal_pattern = "NULL"
            value.update(
                {
                    "enum_value": short_universal_pattern,
                    "sample_value": short_universal_pattern,
                }
            )

        if enum_value == "":
            universal_pattern = "E"
            short_universal_pattern = "E"
            value.update(
                {
                    "enum_value": short_universal_pattern,
                    "sample_value": short_universal_pattern,
                }
            )

        if universal_pattern:
            universal_pattern_detail = universal_patterns.get(universal_pattern)
            universal_pattern_detail = (
                universal_pattern_detail if universal_pattern_detail else {}
            )
            universal_pattern_count = universal_pattern_detail.get("count")
            universal_pattern_count = (
                int(float(universal_pattern_count))
                if get_numeric_value(universal_pattern_count)
                else 0
            )
            universal_pattern_count = universal_pattern_count + value_count
            universal_patterns.update(
                {
                    universal_pattern: {
                        "count": universal_pattern_count,
                        "short_universal_pattern": short_universal_pattern,
                    }
                }
            )

        if short_universal_pattern:
            short_universal_pattern_detail = short_universal_patterns.get(
                short_universal_pattern
            )
            short_universal_pattern_detail = (
                short_universal_pattern_detail if short_universal_pattern_detail else {}
            )
            short_universal_pattern_count = short_universal_pattern_detail.get("count")
            short_universal_pattern_count = (
                int(float(short_universal_pattern_count))
                if get_numeric_value(short_universal_pattern_count)
                else 0
            )
            short_universal_pattern_count = short_universal_pattern_count + value_count
            short_universal_patterns.update(
                {
                    short_universal_pattern: {
                        "count": short_universal_pattern_count,
                        "short_universal_pattern": short_universal_pattern,
                    }
                }
            )

        length_count = length_distribution.get(str(key_length))
        length_count = length_count if length_count else 0
        length_count = length_count + value_count
        if distribution_type == DistributionTypes.Length:
            length_value = value.get("length_value")
            key_length = length_value
            length_distribution.update({str(length_value): value_count})
        else:
            length_distribution.update({str(key_length): length_count})

        space_distributions = get_space_distributions(short_universal_pattern)
        space_distributions = space_distributions if space_distributions else []
        for space_distribution_type in space_distributions:
            space_distribution_count = space_distribution.get(space_distribution_type)
            space_distribution_count = (
                space_distribution_count if space_distribution_count else 0
            )
            space_distribution_count = space_distribution_count + value_count
            space_distribution.update(
                {space_distribution_type: space_distribution_count}
            )

        value.update(
            {
                "universal_pattern": universal_pattern,
                "short_universal_pattern": short_universal_pattern,
                "length_value": key_length,
                "space_distributions": space_distributions,
            }
        )

    universal_patterns = [
        {
            "pattern": key,
            "enum_value": key,
            "universal_pattern": key,
            "value_count": value.get("count"),
            "short_universal_pattern": value.get("short_universal_pattern"),
        }
        for key, value in universal_patterns.items()
    ]
    short_universal_patterns = [
        {
            "pattern": key,
            "enum_value": key,
            "value_count": value.get("count"),
            "short_universal_pattern": value.get("short_universal_pattern"),
        }
        for key, value in short_universal_patterns.items()
    ]
    length_distribution = [
        {"length_value": key, "enum_value": key, "value_count": value}
        for key, value in length_distribution.items()
    ]

    distributions = {
        "values": values,
        "universal_patterns": universal_patterns,
        "short_universal_patterns": short_universal_patterns,
        "length_distribution": length_distribution,
        "space_distribution": space_distribution,
    }
    return distributions


def get_space_distributions(value: str) -> list:
    """
    Returns space distributions for the given value
    """
    value = str(value) if value else ""
    space_distributions = []
    if not value:
        return space_distributions

    has_leading_space = re.match("^[S]+.*[^S]$", value)
    has_trailing_space = re.match("^[^S].*[S]+$", value)
    has_inner_space = re.match("^[^S].*[S]+.*[^S]$", value)
    has_outter_space = re.match("^[S].*[^S]+.*[^S]+.*[S]$", value)

    if has_leading_space:
        space_distributions.append("leading")
    if has_trailing_space:
        space_distributions.append("trailing")
    if has_inner_space:
        space_distributions.append("inner")
    if has_outter_space:
        space_distributions.append("outer")
    space_distributions = list(set(space_distributions))
    return space_distributions


def get_numeric_value(value: str):
    numeric_value = 0
    if not value or (value is None or value == "NULL"):
        return numeric_value

    try:
        numeric_value = float(value)
        if math.isnan(numeric_value):
            return numeric_value
    except:
        numeric_value = 0
    return numeric_value


def get_range_value(value: str):
    if value is None:
        return value
    try:
        if "." in str(value):
            decimal_point = 0
            if value:
                decimal_point = str(value).split(".")[-1]
                decimal_point = (
                    int(float(decimal_point)) if get_numeric_value(decimal_point) else 0
                )
            value = float(value) if get_numeric_value(value) else 0
            if value and not decimal_point:
                value = int(float(value))
        else:
            value = int(float(value)) if get_numeric_value(value) else 0
    except:
        value = None
    return value


def get_attribute_distributions(
    attribute: dict, distribution: dict, attribute_metrics: dict
):
    """
    Returns all the distributions for the given attribute
    """
    has_numeric_values = attribute.get("has_numeric_values")
    derived_type = attribute.get("derived_type")
    derived_type = str(derived_type).lower() if derived_type else ""
    short_universal_patterns = distribution.get("short_universal_patterns")
    short_universal_patterns = (
        short_universal_patterns if short_universal_patterns else []
    )
    length_distribution = distribution.get("length_distribution")
    length_distribution = length_distribution if length_distribution else []
    value_distribution = distribution.get("value_distribution")
    value_distribution = value_distribution if value_distribution else []
    # added new condition for length range and value range
    range_distribution = distribution.get("range_distribution")
    range_distribution = range_distribution if range_distribution else []

    total_records = 0
    total_values = [
        pattern.get("value_count") for pattern in short_universal_patterns if pattern
    ]
    total_values = [
        int(float(value)) if is_convertible_to_float(value) else 0
        for value in total_values
    ]
    total_records = sum(total_values) if total_values else 0

    # completeness
    null_distribution = 0
    space_distribution = 0
    empty_distribution = 0
    non_empty_distribution = 0

    # uniqueness
    distinct_count = attribute_metrics.get("distinct_count", 0)
    unique_distribution = (
        int(float(distinct_count)) if get_numeric_value(distinct_count) else 0
    )
    repeating_distribution = (
        abs(total_records - unique_distribution) if total_records else 0
    )

    # character
    alphabet_distribution = 0
    digits_distribution = 0
    alphanumeric_distribution = 0
    white_space_distribution = 0
    special_chars_distribution = 0

    # numeric
    positive_distribution = 0
    negative_distribution = 0
    zero_distribution = 0

    for pattern in short_universal_patterns:
        if not pattern:
            continue
        value_count = pattern.get("value_count")
        value_count = int(float(value_count)) if get_numeric_value(value_count) else 0
        pattern_format = str(pattern.get("pattern"))

        if pattern_format in ["NULL", "None", None]:
            null_distribution = value_count
        elif pattern_format == "E":
            empty_distribution = value_count
        elif pattern_format == "S":
            space_distribution = value_count
            white_space_distribution += value_count
        else:
            if pattern_format == "A":
                alphabet_distribution = alphabet_distribution + value_count
            elif pattern_format == "N":
                digits_distribution = digits_distribution + value_count
            elif len(str(re.sub("[ANS]", "", pattern_format))) > 0:
                special_chars_distribution = special_chars_distribution + value_count
                if "S" in pattern_format:
                    white_space_distribution = white_space_distribution + value_count
                if "N" in pattern_format:
                    digits_distribution = digits_distribution + value_count
                if "A" in pattern_format:
                    alphabet_distribution = alphabet_distribution + value_count
            elif re.match("^[AN]+$", pattern_format):
                alphanumeric_distribution = alphanumeric_distribution + value_count
                if "A" in pattern_format:
                    alphabet_distribution = alphabet_distribution + value_count
                if "N" in pattern_format:
                    digits_distribution = digits_distribution + value_count
            elif "S" in pattern_format:
                white_space_distribution = white_space_distribution + value_count
                if "A" in pattern_format:
                    alphabet_distribution = alphabet_distribution + value_count
                if "N" in pattern_format:
                    digits_distribution = digits_distribution + value_count

            if derived_type in ["integer", "numeric"] or has_numeric_values:
                pattern_value = str(re.sub("[N]", "1", pattern_format))
                value = get_numeric_value(pattern_value)
                if value is not None and value < 0:
                    negative_distribution = negative_distribution + value_count
                    digits_distribution = digits_distribution + value_count
                if value is not None and value > 0:
                    positive_distribution = positive_distribution + value_count

    value_range_count = 0
    if derived_type in ["integer", "numeric"] or has_numeric_values:
        # get value range and zero values distribution
        if value_distribution:
            min_value = attribute_metrics.get("min_value")
            min_value = get_range_value(min_value)
            max_value = attribute_metrics.get("max_value")
            max_value = get_range_value(max_value)

            for value in value_distribution:
                if not value:
                    continue
                input_value = value.get("enum_value")
                enum_value = get_numeric_value(input_value)
                value_count = value.get("value_count")
                value_count = (
                    int(float(value_count)) if get_numeric_value(value_count) else 0
                )
                if input_value not in [None, "NULL", "E", ""] and re.match(
                    "^[-+]?[0]+([-.]?[0]+)*$", str(enum_value)
                ):
                    zero_distribution = zero_distribution + value_count

        for key in [
            "zero_values_count",
            "positive_values_count",
            "negative_values_count",
        ]:
            if key in distribution:
                value = distribution.get(key)
                value = int(float(value)) if get_numeric_value(value) else 0
                if key == "zero_values_count":
                    zero_distribution = value
                elif key == "positive_values_count":
                    positive_distribution = value
                else:
                    negative_distribution = value

    non_empty_distribution = (
        abs(
            total_records
            - (null_distribution + space_distribution + empty_distribution)
        )
        if total_records
        else 0
    )
    if "non_empty_distribution" in distribution:
        non_empty_distribution = distribution.get("non_empty_distribution")
        non_empty_distribution = (
            int(float(non_empty_distribution))
            if get_numeric_value(non_empty_distribution)
            else 0
        )

    # prepare all the attribute distributions
    completeness = {
        "null": null_distribution,
        "space": space_distribution,
        "empty": empty_distribution,
        "non_empty": non_empty_distribution,
    }
    uniqueness = {
        "unique": unique_distribution,
        "repeating": repeating_distribution,
    }
    character = {
        "alphabet": alphabet_distribution,
        "digits": digits_distribution,
        "alpha_numeric": alphanumeric_distribution,
        "special": special_chars_distribution,
        "space": white_space_distribution,
    }
    numeric_distribution = {
        "positive": positive_distribution,
        "negative": negative_distribution,
        "zero": zero_distribution,
    }
    if derived_type not in ["integer", "numeric"] and not has_numeric_values:
        numeric_distribution = {}
        range_distribution.update({"value": None})

    basic_profile = {
        "distinct_count": unique_distribution,
        "null_count": null_distribution,
        "space_count": space_distribution,
    }
    if derived_type in ["text"]:
        basic_profile.update(
            {
                "blank_count": empty_distribution,
            }
        )
    if derived_type in ["integer", "numeric"] or has_numeric_values:
        basic_profile.update({"zero_values": zero_distribution})

    distribution.update(
        {
            "completeness": completeness,
            "uniqueness": uniqueness,
            "character": character,
            "numeric_distribution": numeric_distribution,
            "range_distribution": range_distribution,
            "basic_profile": basic_profile,
        }
    )
    distribution = sync_distributions(attribute, distribution)
    return distribution


def sync_distributions(attribute: dict, distribution: dict):
    """
    Sync the new distribution metrics with existing distributions
    """
    has_numeric_values = attribute.get("has_numeric_values")
    derived_type = attribute.get("derived_type")
    derived_type = str(derived_type).lower() if derived_type else ""
    existing_value_distributions = attribute.get("value_distribution")
    value_distribution = distribution.get("value_distribution")
    value_limit = None
    if derived_type.lower() in ["integer", "numeric"] or has_numeric_values:
        min_value = attribute.get("min_value")
        max_value = attribute.get("max_value")
        minimum_value = (
            float(min_value) if min_value and is_convertible_to_float(min_value) else 0
        )
        maximum_value = (
            float(max_value) if max_value and is_convertible_to_float(max_value) else 0
        )
        if derived_type.lower() == "integer":
            minimum_value = (
                int(float(min_value))
                if min_value and is_convertible_to_float(min_value)
                else 0
            )
            maximum_value = (
                int(float(max_value))
                if max_value and is_convertible_to_float(max_value)
                else 0
            )
        derived_type = derived_type if not has_numeric_values else "numeric"
        value_limit = {
            "distribution_type": "value",
            "derived_type": derived_type,
            "min": minimum_value,
            "max": maximum_value,
        }

    value_distribution = update_distribution_values(
        existing_value_distributions, value_distribution, "", value_limit
    )
    value_limit = None

    existing_universal_patterns = attribute.get("universal_patterns")
    universal_patterns = distribution.get("universal_patterns")
    universal_patterns = update_distribution_values(
        existing_universal_patterns, universal_patterns
    )

    existing_short_universal_patterns = attribute.get("short_universal_patterns")
    short_universal_patterns = distribution.get("short_universal_patterns")
    short_universal_patterns = update_distribution_values(
        existing_short_universal_patterns, short_universal_patterns
    )

    existing_length_distribution = attribute.get("length_distribution")
    length_distribution = distribution.get("length_distribution")
    value_limit = None
    if attribute:
        min_value = attribute.get("min_length")
        max_value = attribute.get("max_length")
        value_limit = {
            "distribution_type": "length",
            "derived_type": derived_type,
            "min": (
                int(float(min_value))
                if min_value and is_convertible_to_float(min_value)
                else 0
            ),
            "max": (
                int(float(max_value))
                if max_value and is_convertible_to_float(max_value)
                else 0
            ),
        }
    length_distribution = update_distribution_values(
        existing_length_distribution, length_distribution, "length_value", value_limit
    )
    value_limit = None

    distribution.update(
        {
            "value_distribution": value_distribution,
            "universal_patterns": universal_patterns,
            "short_universal_patterns": short_universal_patterns,
            "length_distribution": length_distribution,
        }
    )
    return distribution


def check_is_valid(enum_value: str, is_valid: bool, value_range: dict):
    """
    Return true if the value is valid, false otherwise.
    """
    derived_type = value_range.get("derived_type")
    distribution_type = value_range.get("distribution_type")
    input_value = enum_value
    if input_value in ["NULL", "E", ""]:
        input_value = (
            None if distribution_type and distribution_type.lower() == "value" else 0
        )
    else:
        if derived_type.lower() == "integer":
            try:
                input_value = (
                    int(float(input_value))
                    if input_value and is_convertible_to_float(input_value)
                    else 0
                )
            except:
                input_value = 0
        elif derived_type.lower() == "numeric":
            try:
                input_value = (
                    float(input_value)
                    if input_value and is_convertible_to_float(input_value)
                    else 0
                )
            except:
                input_value = 0
        elif distribution_type and distribution_type.lower() == "length":
            input_value = (
                int(float(input_value))
                if input_value and is_convertible_to_float(input_value)
                else 0
            )

    if distribution_type and (
        (
            distribution_type.lower() == "value"
            and derived_type.lower() in ["integer", "numeric"]
        )
        or (distribution_type.lower() == "length")
    ):
        min_value = value_range.get("min")
        min_value = min_value if min_value else 0
        max_value = value_range.get("max")
        max_value = max_value if max_value else 0
        is_valid = (input_value is not None) and (
            input_value >= min_value and input_value <= max_value
        )
    return is_valid


def update_distribution_values(
    existing_distribution, new_distribution, key: str = "", value_range: dict = None
) -> list:
    """
    Updates the existing distribution values. Also identify new values as well.
    """
    existing_values = {}
    invalid_values = []

    if not new_distribution:
        return []

    if existing_distribution:
        existing_distribution = (
            json.loads(existing_distribution)
            if isinstance(existing_distribution, str)
            else existing_distribution
        )
        existing_distribution = existing_distribution if existing_distribution else []
        key = key if key else "enum_value"
        existing_values = {value.get(key): value for value in existing_distribution}
        invalid_values = [
            str(value.get(key))
            for value in existing_distribution
            if not value.get("is_valid")
        ]

    for value in new_distribution:
        enum_value = value.get("enum_value")
        if key:
            enum_value = value.get(key)
        value_count = value.get("value_count")
        if existing_values and enum_value in existing_values:
            existing_value = existing_values.get(enum_value)
            if existing_value:
                value.update({**existing_value})
            is_valid = value.get("is_valid")
            if is_valid and invalid_values:
                is_valid = str(enum_value) not in invalid_values
            if value_range:
                is_valid = check_is_valid(enum_value, is_valid, value_range)
            value.update(
                {
                    "is_valid": is_valid,
                    "value_count": value_count,
                    "count": value_count,
                    "is_new": False,
                }
            )
        else:
            is_valid = bool(enum_value not in [None, "NULL", "", "E"])
            if is_valid and invalid_values:
                is_valid = str(enum_value) not in invalid_values
            if value_range:
                is_valid = check_is_valid(enum_value, is_valid, value_range)
            value.update(
                {
                    **value,
                    "is_valid": is_valid,
                    "is_new": bool(existing_values),
                    "is_default": True,
                    "name": enum_value,
                    "count": value_count,
                }
            )
    return new_distribution


def get_profile_measures(config: dict, connection):
    """
    Returns list of all the distribution and frequency measures
    """
    asset = config.get("asset")
    asset = asset if asset else {}
    asset_id = asset.get("id")
    measures = []
    if not asset_id:
        return measures

    with connection.cursor() as cursor:
        query_string = f"""
            select mes.*, base.technical_name as name, base.type, base.category, base.level
            from core.measure as mes
            join core.base_measure as base on base.id=mes.base_measure_id and base.is_visible=true
            where base.type in ('{DISTRIBUTION}', '{FREQUENCY}') and base.is_default=True
            and mes.is_active=True and mes.attribute_id is not null
            and (base.category != '{HEALTH}' or base.category is null)
            and mes.asset_id='{asset_id}'
        """
        cursor = execute_query(connection, cursor, query_string)
        measures = fetchall(cursor)
    return measures


def get_metric_value(measure_name: str, distribution: dict, health_metric: dict):
    """
    Get value for the given measure from the attribute distribution
    """
    uniqueness = distribution.get("uniqueness")
    uniqueness = uniqueness if uniqueness else {}
    character = distribution.get("character")
    character = character if character else {}
    numeric_distribution = distribution.get("numeric_distribution")
    numeric_distribution = numeric_distribution if numeric_distribution else {}
    range_distribution = distribution.get("range_distribution")
    range_distribution = range_distribution if range_distribution else {}

    value = None
    if measure_name in ["null_count", "blank_count", "space_count", "non_empty"]:
        value = health_metric.get(measure_name)
        if measure_name == "non_empty":
            attribute = distribution.get("attribute")
            attribute = attribute if attribute else {}
            total_records = attribute.get("row_count") if attribute else 0
            total_records = (
                int(float(total_records))
                if total_records and is_convertible_to_float(total_records)
                else 0
            )
            space_count = health_metric.get("space_count", 0)
            space_count = (
                int(float(space_count))
                if space_count and is_convertible_to_float(space_count)
                else 0
            )
            null_count = health_metric.get("null_count", 0)
            null_count = (
                int(float(null_count))
                if null_count and is_convertible_to_float(null_count)
                else 0
            )
            blank_count = health_metric.get("blank_count", 0)
            blank_count = (
                int(float(blank_count))
                if blank_count and is_convertible_to_float(blank_count)
                else 0
            )
            value = (
                total_records - (space_count + null_count + blank_count)
                if total_records
                else 0
            )
    elif measure_name == "distinct_count":
        value = uniqueness.get("unique")
    elif measure_name == "duplicate":
        value = uniqueness.get("repeating")
    elif measure_name == "digits_count":
        value = character.get("digits")
    elif measure_name == "whitespace_count":
        value = character.get("space")
    elif measure_name == "special_char_count":
        value = character.get("special")
    elif measure_name == "characters_count":
        value = character.get("alphabet")
    elif measure_name == "alpha_numeric_count":
        value = character.get("alpha_numeric")
    elif measure_name == "positive_count":
        value = numeric_distribution.get("positive")
    elif measure_name == "negative_count":
        value = numeric_distribution.get("negative")
    elif measure_name == "zero_values":
        value = numeric_distribution.get("zero")
    elif measure_name == "long_pattern":
        patterns = []
        universal_patterns = distribution.get("universal_patterns")
        universal_patterns = universal_patterns if universal_patterns else []
        if universal_patterns:
            patterns.extend(universal_patterns)
        value = patterns
    elif measure_name == "short_pattern":
        patterns = []
        short_universal_patterns = distribution.get("short_universal_patterns")
        short_universal_patterns = (
            short_universal_patterns if short_universal_patterns else []
        )
        if short_universal_patterns:
            patterns.extend(short_universal_patterns)
        value = patterns
    elif measure_name == "length":
        length_distribution = distribution.get("length_distribution")
        value = length_distribution if length_distribution else []
    elif measure_name == "enum":
        value_distribution = distribution.get("value_distribution")
        value = value_distribution if value_distribution else []
    elif measure_name == "length_range":
        value = range_distribution.get("length")
    elif measure_name == "value_range":
        value = range_distribution.get("value")
    elif "space" in measure_name:
        space_distribution = distribution.get("space_distribution")
        space_distribution = space_distribution if space_distribution else {}
        if measure_name == "leading_space":
            value = space_distribution.get("leading")
        elif measure_name == "trailing_space":
            value = space_distribution.get("trailing")
        elif measure_name == "outer_space":
            value = space_distribution.get("outer")
        elif measure_name == "inner_space":
            value = space_distribution.get("inner")
    return value


def get_metrics_input(
    config: dict, attribute: dict, measure: dict, cursor, is_metrics: bool = True
):
    """
    Prepares the query input for the given measure
    """
    attribute_id = attribute.get("id")
    attribute_name = attribute.get("name")
    connection_id = config.get("connection_id")
    organization_id = config.get("organization_id")
    run_id = config.get("queue_id")
    asset = config.get("asset", {})
    asset = asset if asset else {}
    asset_id = asset.get("id")
    airflow_run_id = config.get("airflow_run_id")
    executed_query = ""
    measure_level_queries = config.get("measure_level_queries", {})
    measure_level_queries = measure_level_queries if measure_level_queries else {}
    measure_level_query = measure_level_queries.get(attribute_id, {})
    measure_level_query = measure_level_query if measure_level_query else {}
    connection_type = config.get("connection_type")

    measure_name = str(measure.get("name"))
    measure_value = measure.get("measure_value")
    measure_category = measure.get("category")
    measure_id = measure.get("id")
    is_positive = measure.get("is_positive", False)
    allow_score = is_scoring_enabled(config, measure.get("allow_score", False))
    is_drift_enabled = measure.get("is_drift_enabled", False)
    if measure_name in measure_level_query:
        executed_query = measure_level_query.get(measure_name, "")
    if measure_category in measure_level_query:
        executed_query = measure_level_query.get(measure_category, "")
    executed_query = executed_query if executed_query else ""
    if (
        measure_name.lower() not in ["length_range", "space_count"]
        and connection_type == ConnectionType.Oracle.value
    ):
        executed_query = executed_query.strip().replace("'", "''")
    if (
        config.get("has_temp_table", False)
        and config.get("table_name", "")
        and config.get("temp_view_table_name", "")
    ):
        executed_query = executed_query.replace(
            config.get("temp_view_table_name", ""), config.get("table_name", "")
        )
    level = measure.get("level")
    value = measure_value if measure_value else 0
    total_records = attribute.get("row_count") if attribute else 0
    total_records = (
        int(float(total_records))
        if total_records and is_convertible_to_float(total_records)
        else 0
    )

    weightage = measure.get("weightage", 100)
    weightage = (
        int(float(weightage))
        if weightage and is_convertible_to_float(weightage)
        else 100
    )
    valid_count = 0
    invalid_count = 0
    valid_percentage = 0
    invalid_percentage = 0
    score = None
    metrics_score = None
    is_archived = False
    if total_records and allow_score:
        valid_count = value if value else 0
        if measure_category and measure_category.lower() == "range":
            valid_count = total_records - value
        valid_count = valid_count if is_positive else (total_records - valid_count)
        invalid_count = total_records - valid_count
        valid_percentage = float(valid_count / total_records * 100)
        invalid_percentage = float(100 - valid_percentage)
        score = valid_percentage
        score = 100 if score > 100 else score
        score = 0 if score < 0 else score
        metrics_score = score

    if score:
        metrics_score = calculate_weightage_score(score, weightage)
        metrics_score = 100 if metrics_score > 100 else metrics_score
        metrics_score = 0 if metrics_score < 0 else metrics_score

    value = str(value) if value is not None else ""
    value = value.replace("'", "''")
    measure_name = (
        repr(measure_name).replace("\\", "\\\\")
        if "\\x00" in repr(measure_name)
        else measure_name
    )
    query_input = (
        str(uuid4()),
        organization_id,
        connection_id,
        asset_id,
        attribute_id,
        measure_id,
        run_id,
        airflow_run_id,
        attribute_name,
        measure_name,
        level,
        value,
        weightage,
        total_records,
        valid_count,
        invalid_count,
        valid_percentage,
        invalid_percentage,
        metrics_score,
        PASSED,
        is_archived,
        executed_query,
        allow_score,
        is_drift_enabled,
        is_metrics,
        True,
        False,
        attribute.get("parent_attribute_id", None),
    )
    input_literals = ", ".join(["%s"] * len(query_input))
    query_param = cursor.mogrify(
        f"({input_literals}, CURRENT_TIMESTAMP)",
        query_input,
    ).decode("utf-8")
    return query_param, score, total_records, valid_count, invalid_count


def get_metrics(
    config: dict,
    attribute_distribution: dict,
    health_metrics: dict,
    measures: list,
    connection,
    cursor,
):
    """
    Returns the list of metrics for each measure of the attribute
    """
    attribute = attribute_distribution.get("attribute")
    attribute_id = attribute.get("id")
    health_metrics = health_metrics if health_metrics else {}
    attribute_health_metric = health_metrics.get(attribute_id) if attribute_id else {}
    attribute_health_metric = attribute_health_metric if attribute_health_metric else {}
    run_id = config.get("run_id")
    metrics = []
    measure_ids = []
    attribute_measures = list(
        filter(lambda measure: measure.get("attribute_id") == attribute_id, measures)
    )
    row_count = config.get("row_count", 0)
    for measure in attribute_measures:
        measure_id = measure.get("id")
        measure_name = measure.get("name")
        measure_name = str(measure_name).lower() if measure_name else measure_name
        measure_category = measure.get("category")
        weightage = measure.get("weightage", 100)
        weightage = (
            int(float(weightage))
            if weightage and is_convertible_to_float(weightage)
            else 100
        )

        is_positive = measure.get("is_positive", False)
        allow_score = is_scoring_enabled(config, measure.get("allow_score", False))

        measure_value = get_metric_value(
            measure_name, attribute_distribution, attribute_health_metric
        )
        if row_count == 0:
            update_measure_score_query = f"""
                update core.measure set last_run_id='{run_id}', score=null, failed_rows=null, row_count=0, valid_rows=0, invalid_rows=0
                where id='{measure_id}'
            """
            cursor = execute_query(connection, cursor, update_measure_score_query)
            continue
        if measure_value is None:
            continue

        metrics_input = None
        score = None
        total_records = 0
        valid_count = 0
        invalid_count = 0
        if isinstance(measure_value, list):
            total_records = attribute.get("row_count") if attribute else 0
            total_records = (
                int(float(total_records))
                if total_records and is_convertible_to_float(total_records)
                else 0
            )

            metrics_input = []
            valid_count = 0
            for value in measure_value:
                enum_value = value.get("enum_value")
                if measure_name and measure_name.lower() == "length":
                    enum_value = str(value.get("length_value"))
                value_count = value.get("value_count")
                is_valid = value.get("is_valid")
                if is_valid:
                    valid_count = valid_count + int(float(value_count))
                measure.update(
                    {
                        "name": enum_value,
                        "measure_value": value_count,
                        "is_positive": is_valid,
                    }
                )
                measure_input, score, total_records, _, _ = get_metrics_input(
                    config, attribute, measure, cursor, False
                )
                if not measure_input:
                    continue
                metrics_input.append(measure_input)

            valid_percentage = 0
            if total_records:
                valid_count = (
                    valid_count if is_positive else (total_records - valid_count)
                )
                valid_percentage = float(valid_count / total_records * 100)
            invalid_count = total_records - valid_count
            invalid_percentage = float(100 - valid_percentage)
            score = None
            if allow_score:
                score = valid_percentage if is_positive else invalid_percentage
                score = 100 if score > 100 else score
                score = 0 if score < 0 else score
                score = calculate_weightage_score(score, weightage)
                score = 100 if score > 100 else score
                score = 0 if score < 0 else score

            if metrics_input:
                is_archived = False
                attribute_id = attribute.get("id")
                attribute_name = attribute.get("name")
                connection_id = config.get("connection_id")
                organization_id = config.get("organization_id")
                run_id = config.get("queue_id")
                asset = config.get("asset", {})
                asset = asset if asset else {}
                asset_id = asset.get("id")
                airflow_run_id = config.get("airflow_run_id")
                executed_query = ""
                measure_level_queries = config.get("measure_level_queries", {})
                measure_level_queries = (
                    measure_level_queries if measure_level_queries else {}
                )
                measure_level_query = measure_level_queries.get(attribute_id, {})
                measure_level_query = measure_level_query if measure_level_query else {}
                is_positive = measure.get("is_positive", False)
                allow_score = is_scoring_enabled(
                    config, measure.get("allow_score", False)
                )
                is_drift_enabled = measure.get("is_drift_enabled", False)
                if measure_name in measure_level_query:
                    executed_query = measure_level_query.get(measure_name, "")
                if measure_category in measure_level_query:
                    executed_query = measure_level_query.get(measure_category, "")
                executed_query = executed_query if executed_query else ""
                executed_query = executed_query.strip().replace("'", "''")
                if (
                    config.get("has_temp_table", False)
                    and config.get("table_name", "")
                    and config.get("temp_view_table_name", "")
                ):
                    executed_query = executed_query.replace(
                        config.get("temp_view_table_name", ""),
                        config.get("table_name", ""),
                    )
                level = measure.get("level")

                query_input = (
                    str(uuid4()),
                    organization_id,
                    connection_id,
                    asset_id,
                    attribute_id,
                    measure_id,
                    run_id,
                    airflow_run_id,
                    attribute_name,
                    measure_name,
                    level,
                    str(valid_count),
                    weightage,
                    total_records,
                    valid_count,
                    invalid_count,
                    valid_percentage,
                    invalid_percentage,
                    score,
                    PASSED,
                    is_archived,
                    executed_query,
                    allow_score,
                    is_drift_enabled,
                    True,
                    True,
                    False,
                    attribute.get("parent_attribute_id", None),
                )
                input_literals = ", ".join(["%s"] * len(query_input))
                query_param = cursor.mogrify(
                    f"({input_literals}, CURRENT_TIMESTAMP)",
                    query_input,
                ).decode("utf-8")
                metrics_input.append(query_param)
        else:
            measure.update({"measure_value": measure_value})
            metrics_input, score, total_records, valid_count, invalid_count = (
                get_metrics_input(config, attribute, measure, cursor)
            )
            if score:
                score = calculate_weightage_score(score, weightage)
                score = 100 if score > 100 else score
                score = 0 if score < 0 else score

        if not metrics_input:
            continue
        measure_ids.append(measure.get("id"))
        if isinstance(metrics_input, list):
            metrics.extend(metrics_input)
        else:
            metrics.append(metrics_input)

        pass_criteria_result = check_measure_result(measure, score)
        score = "null" if score is None else score
        update_measure_score_query = f"""
            update core.measure set last_run_id='{run_id}', last_run_date=CURRENT_TIMESTAMP, score={score}, failed_rows=null, row_count={total_records}, valid_rows={valid_count}, invalid_rows={invalid_count}, result='{pass_criteria_result}'
            where id='{measure_id}'
        """
        cursor = execute_query(connection, cursor, update_measure_score_query)

    measure_ids = list(set(measure_ids))
    return metrics, measure_ids


def to_list(dict_value: dict):
    """
    Convers the dict distributions into list
    """
    values = []
    if not dict_value:
        return values

    for key, value in dict_value.items():
        values.append(
            {
                "enum_value": key,
                "value_count": value,
            }
        )
    return values


def remove_null_characters(value: str) -> str:
    """
    Remove unicode null characters from the given string value.
    """
    if "u0000" in value:
        while True:
            value = value.replace("\\u0000", "u0000")
            if "\\u0000" not in value:
                break

    if "x00" in value:
        while True:
            value = value.replace("\\x00", "x00")
            if "\\x00" not in value:
                break
    return value


def save_profile_result(config: dict, profile_result: dict, health_metrics: dict):
    """
    update profile result into attribute
    """
    asset = config.get("asset")
    asset = asset if asset else {}
    asset_id = asset.get("id")
    run_id = config.get("queue_id")

    connection = get_postgres_connection(config)
    measures = get_profile_measures(config, connection)
    with connection.cursor() as cursor:
        metrics = []
        measure_ids = []
        attribute_ids = []
        for attribute_id in profile_result:
            updated_fields = []
            attribute_distribution = profile_result.get(attribute_id)
            if not attribute_distribution:
                continue

            attribute_active_measures = attribute_distribution.get("active_measures")
            attribute_active_measures = (
                attribute_active_measures if attribute_active_measures else []
            )

            attribute_ids.append(attribute_id)
            attribute_metrics, attribute_measure_ids = get_metrics(
                config,
                attribute_distribution,
                health_metrics,
                measures,
                connection,
                cursor,
            )
            if attribute_metrics:
                metrics.extend(attribute_metrics)
            if attribute_measure_ids:
                measure_ids.extend(attribute_measure_ids)

            value_distribution = attribute_distribution.get("value_distribution")
            value_distribution = value_distribution if value_distribution else []
            if "enum" in attribute_active_measures:
                value_distribution = json.dumps(value_distribution, default=str)
                value_distribution = value_distribution.replace("'", "''")
                value_distribution = remove_null_characters(value_distribution)
            else:
                value_distribution = "[]"
            updated_fields.append(f"value_distribution='{value_distribution}'")

            length_distribution = attribute_distribution.get("length_distribution")
            length_distribution = length_distribution if length_distribution else []
            if "length" in attribute_active_measures:
                length_distribution = json.dumps(length_distribution, default=str)
                length_distribution = length_distribution.replace("'", "''")
                length_distribution = remove_null_characters(length_distribution)
            else:
                length_distribution = "[]"
            updated_fields.append(f"length_distribution='{length_distribution}'")

            space_distribution = attribute_distribution.get("space_distribution")
            space_distribution = space_distribution if space_distribution else {}
            space_distribution = to_list(space_distribution)
            space_distribution = json.dumps(space_distribution, default=str)
            space_distribution = space_distribution.replace("'", "''")
            updated_fields.append(f"space_distribution='{space_distribution}'")

            universal_patterns = attribute_distribution.get("universal_patterns")
            universal_patterns = universal_patterns if universal_patterns else []
            if "long_pattern" in attribute_active_measures:
                universal_patterns = json.dumps(universal_patterns, default=str)
                universal_patterns = universal_patterns.replace("'", "''")
                universal_patterns = remove_null_characters(universal_patterns)
                try:
                    universal_patterns = re.sub("'.*u0000.*'", "", universal_patterns)
                except Exception as e:
                    universal_patterns = "[]"
                    log_error("save_profile_result : update long pattern", e)
            else:
                universal_patterns = "[]"
            updated_fields.append(f"universal_patterns='{universal_patterns}'")

            short_universal_patterns = attribute_distribution.get(
                "short_universal_patterns"
            )
            short_universal_patterns = (
                short_universal_patterns if short_universal_patterns else []
            )
            if "short_pattern" in attribute_active_measures:
                short_universal_patterns = json.dumps(
                    short_universal_patterns, default=str
                )
                short_universal_patterns = short_universal_patterns.replace("'", "''")
                short_universal_patterns = remove_null_characters(
                    short_universal_patterns
                )
                try:
                    short_universal_patterns = re.sub(
                        "'.*u0000.*'", "", short_universal_patterns
                    )
                except Exception as e:
                    short_universal_patterns = "[]"
                    log_error("save_profile_result : update short pattern", e)
            else:
                short_universal_patterns = "[]"
            updated_fields.append(
                f"short_universal_patterns='{short_universal_patterns}'"
            )

            uniqueness = attribute_distribution.get("uniqueness")
            uniqueness = uniqueness if uniqueness else {}
            uniqueness = to_list(uniqueness)
            uniqueness = json.dumps(uniqueness, default=str)
            uniqueness = uniqueness.replace("'", "''")
            updated_fields.append(f"uniqueness='{uniqueness}'")

            character_distribution = attribute_distribution.get("character")
            character_distribution = (
                character_distribution if character_distribution else {}
            )
            character_distribution = to_list(character_distribution)
            character_distribution = json.dumps(character_distribution, default=str)
            character_distribution = character_distribution.replace("'", "''")
            updated_fields.append(f"character_distribution='{character_distribution}'")

            numeric_distribution = attribute_distribution.get("numeric_distribution")
            numeric_distribution = numeric_distribution if numeric_distribution else {}
            numeric_distribution = to_list(numeric_distribution)
            numeric_distribution = json.dumps(numeric_distribution, default=str)
            numeric_distribution = numeric_distribution.replace("'", "''")
            updated_fields.append(f"numeric_distribution='{numeric_distribution}'")

            basic_profile = attribute_distribution.get("basic_profile")
            basic_profile = basic_profile if basic_profile else {}
            basic_profile = json.dumps(basic_profile, default=str)
            basic_profile = basic_profile.replace("'", "''")
            updated_fields.append(f"basic_profile='{basic_profile}'")

            fields_to_update = ", ".join(updated_fields)
            if fields_to_update:
                query_string = f"""
                    update core.attribute set {fields_to_update}
                    where asset_id='{asset_id}'
                    and id ='{attribute_id}'
                """
                cursor = execute_query(connection, cursor, query_string)

        # delete metrics for the same run_id
        measure_ids = [
            f"""'{measure_id}'""" for measure_id in measure_ids if measure_id
        ]
        measure_id = ", ".join(measure_ids) if measure_ids else ""

        attribute_ids = [
            f"""'{attribute_id}'""" for attribute_id in attribute_ids if attribute_id
        ]
        attribute_id = ", ".join(attribute_ids) if attribute_ids else ""
        if measure_id and attribute_id:
            delete_metrics(
                config,
                run_id=run_id,
                asset_id=asset_id,
                measure_ids=measure_id,
                attribute_ids=attribute_id,
            )

        # insert the distribution metrics into metadata repo
        measures_input = split_queries(metrics, 1000)
        for input_values in measures_input:
            try:
                query_input = ",".join(input_values)
                metrics_insert_query = f"""
                    insert into core.metrics (id, organization_id, connection_id, asset_id, attribute_id,
                    measure_id, run_id, airflow_run_id,attribute_name, measure_name, level, value, weightage, total_count,
                    valid_count, invalid_count, valid_percentage, invalid_percentage, score, status, is_archived,
                    query, allow_score, is_drift_enabled, is_measure,  is_active, is_delete, parent_attribute_id, created_date)
                    values {query_input}
                """
                cursor = execute_query(connection, cursor, metrics_insert_query)
            except Exception as e:
                log_error("profile-measures : inserting new metrics", e)


def get_active_advanced_measures(config, attribute_id):
    """
    Get active advanced measures count
    """
    connection = get_postgres_connection(config)
    advanced_measures = get_advanced_measure_names()
    advanced_measures = [f"'{name}'" for name in advanced_measures]
    advanced_measures_names = ", ".join(advanced_measures)
    active_measures_count = 0
    with connection.cursor() as cursor:
        query_string = f"""
            select count(mes.*) as total_count
            from core.measure as mes
            join core.base_measure as base on base.id=mes.base_measure_id
            where mes.attribute_id='{attribute_id}' and mes.is_active=True
            and mes.technical_name in ({advanced_measures_names})
        """
        cursor = execute_query(connection, cursor, query_string)
        active_measures = fetchone(cursor)
        active_measures = convert_to_lower(active_measures)
        active_measures_count = active_measures.get("total_count")
        active_measures_count = active_measures_count if active_measures_count else 0
    return active_measures_count


def get_active_measures(config: dict, attribute_id: str, measure_names: list):
    """
    Get active advanced measures count
    """
    measure_names = measure_names if measure_names else []
    measure_names = [f"'{name}'" for name in measure_names]
    measure_name_filter = ", ".join(measure_names) if measure_names else ""
    measure_name_filter = (
        f"and lower(mes.technical_name) in ({measure_name_filter})"
        if measure_name_filter
        else ""
    )

    active_measures = []
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            select mes.technical_name
            from core.measure as mes
            join core.base_measure as base on base.id=mes.base_measure_id and base.is_visible=true
            join core.attribute as att on att.id=mes.attribute_id and att.profile = true
            where mes.attribute_id='{attribute_id}' and mes.is_active=True
            {measure_name_filter}
        """
        cursor = execute_query(connection, cursor, query_string)
        active_measures = fetchall(cursor)
        active_measures = active_measures if active_measures else []
        active_measures = [
            str(name.get("technical_name", "")).lower()
            for name in active_measures
            if name and name.get("technical_name", "")
        ]
    return active_measures

# helper function for specific measure type in subclasses measure

def has_sub_values(measure_id, connection):
    """
    Database-driven check to determine if this measure_id has sub-values.
    Returns True for measures like long_pattern, enum, length that have 
    multiple records with different measure_names.
    Returns False for aggregate measures like null_count, distinct_count.
    """
    try:
        with connection.cursor() as cursor:
            query = f"""
                SELECT COUNT(DISTINCT measure_name) as distinct_names,
                       COUNT(*) as total_records
                FROM core.metrics 
                WHERE measure_id = '{measure_id}'
                AND measure_name IS NOT NULL
            """
            cursor = execute_query(connection, cursor, query)
            result = fetchone(cursor)
            
            distinct_names = result.get('distinct_names', 0)
            total_records = result.get('total_records', 0)
            
            # If multiple distinct measure_names exist = sub-value measure
            return distinct_names > 1 and total_records > 1
    except Exception as e:
        log_error(f"Error checking sub-values for measure_id {measure_id}: {e}", e)
        return False


def get_individual_measure_records(connection, measure_id, attribute_id, run_id):
    """
    Get all individual records for a sub-value measure from the current run.
    Used for pattern, enum, and length measures that have multiple records
    per measure with different measure_name values.
    """
    try:
        with connection.cursor() as cursor:
            query = f"""
                SELECT measure_name, value, id
                FROM core.metrics
                WHERE measure_id = '{measure_id}'
                AND attribute_id = '{attribute_id}'
                AND run_id = '{run_id}'
                AND measure_name IS NOT NULL
                ORDER BY measure_name
            """
            cursor = execute_query(connection, cursor, query)
            return fetchall(cursor)
    except Exception as e:
        log_error(f"Error getting individual records for measure_id {measure_id}: {e}", e)
        return []


def get_feedback_for_specific_value(connection, measure_id, attribute_id, previous_run_id, specific_value):
    """
    Get feedback for a specific pattern/enum/length value from previous run.
    This is used for sub-value measures where we need to enhance thresholds
    for only the specific pattern/enum/length that had user feedback.
    """
    try:
        # AND met.run_id = '{previous_run_id}'
        with connection.cursor() as cursor:
            query = f"""
                SELECT 
                    met.value,
                    met.marked_as,
                    met.run_id,
                    met.created_date,
                    met.threshold,
                    met.drift_status,
                    met.measure_name
                FROM core.metrics met
                WHERE met.measure_id = '{measure_id}'
                AND met.attribute_id = '{attribute_id}'
                AND met.measure_name = '{specific_value}'
                AND met.marked_as IS NOT NULL
                AND met.value IS NOT NULL
                AND met.drift_status IS NOT NULL
                ORDER BY met.created_date DESC
            """
            
            cursor = execute_query(connection, cursor, query)
            results = fetchall(cursor)
            
            feedback_data = []
            for row in results:
                try:
                    threshold_data = {}
                    if row.get('threshold'):
                        if isinstance(row['threshold'], str):
                            threshold_data = json.loads(row['threshold'])
                        else:
                            threshold_data = row['threshold']
                    
                    feedback_entry = {
                        'value': round(float(row['value']), 2),
                        'marked_as': row['marked_as'],
                        'run_id': row['run_id'],
                        'created_date': row['created_date'],
                        'threshold': threshold_data,
                        'drift_status': row['drift_status'],
                        'specific_value': specific_value
                    }
                    feedback_data.append(feedback_entry)
                    
                except (ValueError, TypeError, json.JSONDecodeError) as e:
                    log_error(f"Error parsing specific value feedback row for {specific_value}: {e}", e)
                    continue
            
            log_info(f"SPECIFIC_VALUE_FEEDBACK: Found {len(feedback_data)} feedback entries for {specific_value}")
            return feedback_data
    
    except Exception as e:
        log_error(f"Error getting feedback for specific value {specific_value}: {e}", e)
        return []


def update_specific_record_threshold(connection, record_id, measure_id, attribute_id, run_id, 
                                   enhanced_lower, enhanced_upper, feedback_stats, specific_value=None):
    """
    Update the threshold column for a specific metrics record (used for sub-value measures).
    This updates only the individual pattern/enum/length record that had feedback.
    """
    try:
        with connection.cursor() as cursor:
            threshold_data = {
                "lt_percent": 0,
                "ut_percent": 0,
                "lower_threshold": enhanced_lower,
                "upper_threshold": enhanced_upper,
                "feedback_enhanced": True,
                "feedback_metadata": {
                    "enhancement_type": "context_aware_current_run_specific_value",
                    "expand_requests": feedback_stats.get('expand_count', 0),
                    "contract_requests": feedback_stats.get('contract_count', 0),
                    "enhancement_timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "confidence_threshold_used": 0.8,
                    "specific_value": feedback_stats.get('specific_value', '')
                }
            }

            threshold_json = json.dumps(threshold_data).replace("'", "''")
            update_query = f"""
                UPDATE core.metrics
                SET threshold = '{threshold_json}'
                WHERE id = '{record_id}'
                AND measure_id = '{measure_id}'
                AND attribute_id = '{attribute_id}'
                AND measure_name = '{specific_value}'
                AND run_id = '{run_id}'
            """

            cursor = execute_query(connection, cursor, update_query)

            if cursor.rowcount > 0:
                log_info(f"SPECIFIC_RECORD_UPDATE: Updated specific record {record_id} with enhanced thresholds: ({enhanced_lower}, {enhanced_upper})")
            else:
                log_info(f"SPECIFIC_RECORD_UPDATE: No record found to update for {record_id}")

    except Exception as e:
        log_error(f"SPECIFIC_RECORD_UPDATE: Error updating specific record threshold: {e}", e)


def run_deep_profile(
    config,
    default_queries,
    attribute,
    health_metrics,
    source_connection,
    health_metrics_dict,
):
    """
    Run deep profiling for an attribute with enhanced context-aware feedback support.
    
    This function performs comprehensive data profiling including distribution analysis,
    pattern detection, and range validation with intelligent threshold enhancement
    based on user feedback patterns for ALL measure types.
    
    REFACTORED: Fixed 0-0 bug for value_range first runs by using reliable fallback logic
    inspired by the old version. Now uses simple "found vs not found" logic instead of
    complex first-run detection.
    """
    try:
        # Initialize variables to hold metadata that needs to be stored later
        feedback_metadata_to_store = None
        # CORRECTED: Initialize as a list to prevent overwrites
        baseline_metadata_to_store = []

        active_measures = []
        measure_level_queries = {}
        total_records = 0
        asset = config.get("asset")
        asset = asset if asset else {}
        connection = config.get("connection")
        connection = connection if connection else {}

        credentials = connection.get("credentials", {})
        language_support = credentials.get("language", {})

        is_incremental = config.get("is_incremental", False)
        profile_settings = config.get("profile_settings")
        attribute = attribute if attribute else {}
        attribute_id = attribute.get("id")
        attribute_name = attribute.get("name")
        connection_type = config.get("connection_type")
        report_settings = config.get("dag_info").get("report_settings")
        report_settings = (
            json.loads(report_settings, default=str)
            if report_settings and isinstance(report_settings, str)
            else report_settings
        )
        report_settings = report_settings if report_settings else {}
        profiling_schema_name = profile_settings.get("schema")
        profiling_schema_name = profiling_schema_name if profiling_schema_name else ""

        profiling_database = profile_settings.get("database")
        profiling_database = profiling_database if profiling_database else ""
        if source_connection and connection_type == ConnectionType.Denodo.value:
            source_connection = deepcopy(source_connection)

        if not profiling_database:
            credentials = connection.get("credentials")
            credentials = (
                json.loads(credentials)
                if credentials and isinstance(credentials, str)
                else credentials
            )
            credentials = credentials if credentials else {}
            database = credentials.get("database")
            profiling_database = database if database else ""

        if not profiling_schema_name:
            properties = asset.get("properties", {})
            properties = (
                json.loads(properties) if isinstance(properties, str) else properties
            )
            properties = properties if properties else {}
            schema = properties.get("schema")
            profiling_schema_name = schema if schema else ""

        if profiling_database:
            database_names = get_attribute_names(connection_type, [profiling_database])
            database_name = "".join(database_names).strip()
            if database_name:
                profiling_database = f"{database_name}."

        table_name = config.get("table_name")
        if not table_name and ConnectionType.SapEcc.value in connection_type:
            table_name = asset.get("name","")
        has_temp_table = config.get("has_temp_table")
        if has_temp_table:
            table_name = config.get("temp_view_table_name")

        if str(asset.get("view_type", "")).lower() == "direct query":
            table_name = f"""({asset.get("query")}) as direct_query_table"""
            if connection_type and str(connection_type).lower() in [
                ConnectionType.Oracle.value.lower(),
                ConnectionType.BigQuery.value.lower(),
            ]:
                table_name = f"""({asset.get("query")})"""
        profile_settings = config.get("profile_settings")
        profile_settings = (
            json.loads(profile_settings, default=str)
            if profile_settings and isinstance(profile_settings, str)
            else profile_settings
        )
        profile_settings = profile_settings if profile_settings else {}
        frequency_limit = profile_settings.get("count")
        frequency_limit = (
            int(float(frequency_limit))
            if frequency_limit and is_convertible_to_float(frequency_limit)
            else 25
        )

        default_max_length_threshold = 70
        default_max_distinct_value_threshold = 10000
        defualt_distinct_percentage_threshold = 25
        default_pattern_frequency_threshold = 1000
        default_max_row_count = 1000
        long_length_frequency_threshold = 20
        short_length_frequency_threshold = 40

        active_measures = get_active_measures(
            config,
            attribute_id,
            ["distinct_count", "long_pattern", "short_pattern", "enum", "length"],
        )
        max_distinct_value_threshold = profile_settings.get(
            "distinct_values_threshold", default_max_distinct_value_threshold
        )
        max_distinct_value_threshold = (
            int(float(max_distinct_value_threshold))
            if max_distinct_value_threshold
            and is_convertible_to_float(max_distinct_value_threshold)
            else default_max_distinct_value_threshold
        )
        max_distinct_value_threshold = (
            max_distinct_value_threshold
            if max_distinct_value_threshold > 0
            else default_max_distinct_value_threshold
        )

        distinct_percentage_threshold = profile_settings.get(
            "distinct_percentage_threshold", defualt_distinct_percentage_threshold
        )
        distinct_percentage_threshold = (
            int(float(distinct_percentage_threshold))
            if distinct_percentage_threshold
            and is_convertible_to_float(distinct_percentage_threshold)
            else defualt_distinct_percentage_threshold
        )
        distinct_percentage_threshold = (
            distinct_percentage_threshold
            if distinct_percentage_threshold > 0
            else defualt_distinct_percentage_threshold
        )

        pattern_frequency_threshold = profile_settings.get(
            "pattern_frequency_threshold", default_pattern_frequency_threshold
        )
        pattern_frequency_threshold = (
            int(float(pattern_frequency_threshold))
            if pattern_frequency_threshold
            and is_convertible_to_float(pattern_frequency_threshold)
            else default_pattern_frequency_threshold
        )
        pattern_frequency_threshold = (
            pattern_frequency_threshold
            if pattern_frequency_threshold > 0
            else default_pattern_frequency_threshold
        )

        profile_queries = default_queries.get("profile", {})
        value_distribution_query = profile_queries.get("value_distribution")
        length_distribution_query = profile_queries.get("length_distribution")
        pattern_distribution_query = profile_queries.get("pattern_distribution")
        short_pattern_distribution_query = profile_queries.get(
            "short_pattern_distribution"
        )
        value_range_query = profile_queries.get("value_range")
        length_range_query = profile_queries.get("length_range")
        zero_values_query = profile_queries.get("zero_values")
        numeric_value_distribution_query = profile_queries.get(
            "numeric_value_distribution"
        )
        duplicate_values_query = profile_queries.get("distinct_count")
        source_connection = source_connection if source_connection else None
        active_measures_count = get_active_advanced_measures(config, attribute_id)
        distinct_rules_exit = "distinct_count" in active_measures

        datatypes_to_skip = profile_queries.get("datatypes_to_skip")
        skip_deep_profile = (
            check_deep_profile_skip(attribute, datatypes_to_skip)
            if datatypes_to_skip
            else False
        )

        if not active_measures_count or skip_deep_profile:
            return (attribute_id, None)
        derived_type = attribute.get("derived_type")
        derived_type = str(derived_type).lower() if derived_type else ""
        if not total_records:
            total_records = attribute.get("row_count")
            total_records = (
                int(float(total_records))
                if total_records and is_convertible_to_float(total_records)
                else 0
            )
        metrics = list(
            filter(
                lambda metric: metric.get("attribute_id") == attribute_id,
                health_metrics,
            )
        )
        attribute_metrics = (
            dict(map(lambda metric: (metric.get("name"), metric.get("value")), metrics))
            if metrics
            else {}
        )
        # Updating length and value ranges if it is not updated manualy by the user
        updated_properties = attribute.get("updated_properties", {})
        updated_properties = (
            json.loads(updated_properties)
            if isinstance(updated_properties, str)
            else updated_properties
        )
        updated_properties = updated_properties if updated_properties else {}
        if attribute_metrics:
            for category, measure_names in PROPERTIES.items():
                if category == "constraints":
                    continue
                is_updated_mannualy = bool(updated_properties.get(category))
                if is_updated_mannualy:
                    continue

                for measure_name in measure_names:
                    if measure_name not in attribute_metrics:
                        continue
                    attribute.update(
                        {measure_name: attribute_metrics.get(measure_name)}
                    )

        max_length = attribute_metrics.get("max_length", 0)
        max_length = (
            int(float(max_length))
            if max_length and is_convertible_to_float(max_length)
            else 0
        )

        distinct_count = attribute_metrics.get("distinct_count", 0)
        distinct_count = (
            int(float(distinct_count))
            if distinct_count and is_convertible_to_float(distinct_count)
            else 0
        )

        try:
            if not distinct_rules_exit and duplicate_values_query:
                duplicate_values_query = prepare_json_attribute_flatten_query(config, attribute_name, duplicate_values_query, "sub_query")
                query_string = (
                    duplicate_values_query.replace("<attribute>", attribute_name)
                    .replace("<table_name>", table_name)
                    .replace("<distinct_alias_attribute>", "t.")
                    .replace("<distinct_alias>", "t")
                )
                query_string = get_query_string(
                    config, default_queries, query_string, is_full_query=True
                )
                config.update({"sub_category": "PROFILE"})
                distinct_values, native_connection = execute_native_query(
                    config, query_string, source_connection
                )
                if not source_connection and native_connection:
                    source_connection = native_connection
                distinct_values = distinct_values if distinct_values else {}
                distinct_values = convert_to_lower(distinct_values)
                
                if connection_type == ConnectionType.MongoDB.value and distinct_values:
                    distinct_values = transform_mongodb_field_names(distinct_values, attribute_name)
                
                # Use transformed field name for MongoDB, original for others
                distinct_key = f"{get_attribute_label(attribute_name)}distinct_count" if connection_type == ConnectionType.MongoDB.value else "distinct_count"
                distinct_values = distinct_values.get(distinct_key)
                distinct_count = int(distinct_values) if distinct_values else 0
                attribute_metrics.update({"distinct_count": distinct_count})
        except Exception as e:
            log_error(
                f"Deep profile - failed for the attribute = {attribute_id} - {attribute_name} - with Error {str(e)}",
                e,
            )

        distinct_percentage = 0
        if total_records and distinct_count:
            distinct_percentage = (distinct_count / total_records) * 100

        attribute_distribution = {}
        skip_pattern_measure = (
            (distinct_count > 0 and distinct_count <= max_distinct_value_threshold)
            and (
                distinct_percentage > 0
                and distinct_percentage <= distinct_percentage_threshold
            )
        )
        if (
            skip_pattern_measure
            and "enum" in active_measures
        ):
            value_distribution_query = prepare_json_attribute_flatten_query(config, attribute_name, value_distribution_query, "sub_query")
            if connection_type != ConnectionType.SapEcc.value:
                if connection_type == ConnectionType.SalesforceDataCloud.value:
                    table_name = asset.get("name", "")
                query_string = value_distribution_query.replace(
                    "<attribute>", attribute_name
                ).replace("<table_name>", table_name)
                query_string = get_query_string(
                    config, default_queries, query_string, is_full_query=True
                )
                config.update({"sub_category": "DISTRIBUTION"})
                measure_level_queries.update({"enum": query_string})
                result, native_connection = execute_native_query(
                    config, query_string, source_connection, is_list=True
                )
            else:
                config.update({"sub_category": "DISTRIBUTION"})
                measure_level_queries.update({"enum": ""})
                result = []
                native_connection = None
            result = [result] if result else []
            if not source_connection and native_connection:
                source_connection = native_connection
            value_distribution = []
            for rows in result:
                rows = rows if rows else []
                rows = convert_to_lower(rows)
                # Fix column name mapping for Salesforce Marketing
                if connection_type == ConnectionType.SalesforceMarketing.value:
                    if 'enum_value' in query_string.lower() and 'value_count' in query_string.lower():
                        for row in rows:
                            if 'enum_value' not in row:
                                # Find the attribute key and replace it with enum_value
                                for key in list(row.keys()):
                                    if key == attribute_name.lower():
                                        row['enum_value'] = row.pop(key)
                                        break
                value_distribution.extend(rows)

            if value_distribution:
                distributions = get_distributions(
                    value_distribution, DistributionTypes.Value, language_support
                )
                attribute_distribution = {
                    "value_distribution": value_distribution,
                    "universal_patterns": distributions.get("universal_patterns"),
                    "short_universal_patterns": distributions.get(
                        "short_universal_patterns"
                    ),
                    "length_distribution": distributions.get("length_distribution"),
                    "space_distribution": distributions.get("space_distribution"),
                }
        elif connection_type != ConnectionType.S3Select.value:
            if connection_type != ConnectionType.SapEcc.value:
                if connection_type == ConnectionType.Salesforce.value:
                    table_name = asset.get("name", "")
                if connection_type == ConnectionType.SalesforceMarketing.value:
                    table_name = asset.get("name", "")
                if connection_type == ConnectionType.SalesforceDataCloud.value:
                    table_name = asset.get("name", "")
                length_distribution_query = prepare_json_attribute_flatten_query(config, attribute_name, length_distribution_query, "sub_query")
                query_string = length_distribution_query.replace(
                    "<attribute>", attribute_name
                ).replace("<table_name>", table_name)
                query_string = get_query_string(
                    config, default_queries, query_string, is_full_query=True
                )
                config.update({"sub_category": "DISTRIBUTION"})
                measure_level_queries.update({"length": query_string})
                result = []
                if "length" in active_measures:
                    result, native_connection = execute_native_query(
                        config, query_string, source_connection, is_list=True
                    )
                    result = [result] if result else []
                    if not source_connection and native_connection:
                        source_connection = native_connection
                length_frequency = []
                for rows in result:
                    rows = rows if rows else []
                    rows = convert_to_lower(rows)
                    length_frequency.extend(rows)
                length_frequency_count = len(length_frequency) if length_frequency else 0

                filtered_length_frequency = (
                    [length for length in length_frequency if length.get("length_value")]
                    if length_frequency
                    else None
                )
                max_length_object = (
                    max(filtered_length_frequency, key=lambda x: x.get("length_value"))
                    if filtered_length_frequency
                    else None
                )
                has_long_frequency = True
                if max_length_object:
                    length_value = max_length_object.get("length_value")
                    length_value_count = max_length_object.get("value_count")
                    max_length_percentage = (
                        (length_value_count / total_records) * 100
                        if length_value_count and total_records
                        else 0
                    )
                    has_long_frequency = not bool(
                        length_value > default_max_length_threshold
                        or (
                            length_value > default_max_length_threshold
                            and max_length_percentage > distinct_percentage_threshold
                        )
                    )

                # Taking distance between min and max length values using length_frequency_count - 2 (2 means count except the min and max values)
                length_frequency_distance = length_frequency_count - 2
                if (
                    has_long_frequency
                    and "long_pattern" in active_measures
                    and length_frequency_distance <= long_length_frequency_threshold
                    and connection_type != ConnectionType.Salesforce.value
                    and connection_type != ConnectionType.SalesforceMarketing.value
                    and connection_type != ConnectionType.SalesforceDataCloud.value
                    and skip_pattern_measure
                ):
                    long_pattern_frequency = []
                    pattern_distribution_query = prepare_json_attribute_flatten_query(config, attribute_name, pattern_distribution_query, "sub_query")
                    query_string = (
                        pattern_distribution_query.replace("<attribute>", attribute_name)
                        .replace("<table_name>", table_name)
                        .replace("<db_name>.", profiling_database)
                        .replace("<schema_name>", profiling_schema_name)
                    )
                    query_string = get_query_string(
                        config, default_queries, query_string, is_full_query=True
                    )
                    measure_level_queries.update({"pattern": query_string})
                    result, native_connection = execute_native_query(
                        config, query_string, source_connection, is_list=True
                    )
                    result = [result] if result else []
                    if not source_connection and native_connection:
                        source_connection = native_connection
                    short_pattern_frequency = []
                    for rows in result:
                        rows = rows if rows else []
                        rows = convert_to_lower(rows)
                        rows = [
                            {**pattern, "pattern": str(pattern.get("enum_value"))}
                            for pattern in rows
                            if pattern and isinstance(pattern, dict)
                        ]
                        long_pattern_frequency.extend(rows)

                    long_pattern_distribution = get_distributions(
                        long_pattern_frequency, DistributionTypes.Pattern, language_support
                    )
                    attribute_distribution = {
                        "value_distribution": {},
                        "universal_patterns": long_pattern_frequency,
                        "short_universal_patterns": long_pattern_distribution.get(
                            "short_universal_patterns"
                        ),
                        "length_distribution": length_frequency,
                        "space_distribution": long_pattern_distribution.get(
                            "space_distribution"
                        ),
                    }
                else:
                    space_distribution = {}
                    short_pattern_frequency = None
                    if (
                        length_frequency_distance <= short_length_frequency_threshold
                        and "short_pattern" in active_measures
                        and connection_type != ConnectionType.Salesforce.value
                        and connection_type != ConnectionType.SalesforceMarketing.value
                        and connection_type != ConnectionType.SalesforceDataCloud.value
                        and skip_pattern_measure
                    ):
                        short_pattern_distribution_query = prepare_json_attribute_flatten_query(config, attribute_name, short_pattern_distribution_query, "sub_query")
                        query_string = (
                            short_pattern_distribution_query.replace(
                                "<attribute>", attribute_name
                            )
                            .replace("<table_name>", table_name)
                            .replace("<db_name>.", profiling_database)
                            .replace("<schema_name>", profiling_schema_name)
                        )
                        query_string = get_query_string(
                            config, default_queries, query_string, is_full_query=True
                        )
                        config.update({"sub_category": "DISTRIBUTION"})
                        measure_level_queries.update({"pattern": query_string})
                        result, native_connection = execute_native_query(
                            config, query_string, source_connection, is_list=True
                        )
                        if not source_connection and native_connection:
                            source_connection = native_connection

                        short_pattern_frequency = []
                        for rows in result:
                            rows = rows if rows else []
                            rows = convert_to_lower(rows)
                            rows = [
                                {**pattern, "pattern": str(pattern.get("enum_value"))}
                                for pattern in rows
                                if pattern and isinstance(pattern, dict)
                            ]
                            short_pattern_frequency.extend(rows)

                        if (
                            short_pattern_frequency
                            and len(short_pattern_frequency) <= pattern_frequency_threshold
                        ):
                            short_pattern_distribution = get_distributions(
                                short_pattern_frequency,
                                DistributionTypes.ShortPattern,
                                language_support,
                            )
                            space_distribution = short_pattern_distribution.get(
                                "space_distribution"
                            )
                        else:
                            short_pattern_frequency = None

                    if (
                        not short_pattern_frequency
                        and skip_pattern_measure
                        and "short_pattern" in active_measures
                    ):
                        length_frequency_distribution = get_distributions(
                            length_frequency, DistributionTypes.Length, language_support
                        )
                        space_distribution = length_frequency_distribution.get(
                            "space_distribution"
                        )
                        short_pattern_frequency = length_frequency_distribution.get(
                            "short_universal_patterns"
                        )
                        short_pattern_frequency = [
                            {**pattern, "pattern": str(pattern.get("enum_value"))}
                            for pattern in short_pattern_frequency
                            if pattern and isinstance(pattern, dict)
                        ]

                    attribute_distribution = {
                        "value_distribution": {},
                        "universal_patterns": {},
                        "short_universal_patterns": short_pattern_frequency,
                        "length_distribution": length_frequency,
                        "space_distribution": space_distribution,
                    }
            else:
                # For SAP ECC, create a minimal attribute_distribution
                attribute_distribution = {
                    "value_distribution": {},
                    "universal_patterns": {},
                    "short_universal_patterns": [],
                    "length_distribution": {},
                    "space_distribution": {},
                }

        # get range distribution
        if derived_type in ["integer", "numeric"]:
            query_string = ""
            # get zero values distribution
            if zero_values_query:
                zero_values_query = prepare_json_attribute_flatten_query(config, attribute_name, zero_values_query, "sub_query")
                query_string = zero_values_query.replace(
                    "<attribute>", attribute_name
                ).replace("<table_name>", table_name)
                query_string = get_query_string(
                    config, default_queries, query_string, is_full_query=True
                )
                config.update({"sub_category": "PROFILE"})
                measure_level_queries.update({"zero_values_count": query_string})
                zero_values, native_connection = execute_native_query(
                    config, query_string, source_connection
                )
                if not source_connection and native_connection:
                    source_connection = native_connection
                zero_values = zero_values if zero_values else {}
                zero_values = convert_to_lower(zero_values)
                
                if connection_type == ConnectionType.MongoDB.value and zero_values:
                    zero_values = transform_mongodb_field_names(zero_values, attribute_name)
                
                zero_values_key = f"{get_attribute_label(attribute_name)}zero_values_count" if connection_type == ConnectionType.MongoDB.value else "zero_values_count"
                zero_values_count = zero_values.get(zero_values_key)
                zero_values_count = (
                    int(float(zero_values_count))
                    if zero_values_count
                    and is_convertible_to_float(zero_values_count)
                    else 0
                )
                attribute_distribution.update(
                    {"zero_values_count": zero_values_count}
                )
            if numeric_value_distribution_query:
                numeric_value_distribution_query = prepare_json_attribute_flatten_query(config, attribute_name, numeric_value_distribution_query, "sub_query")
                query_string = numeric_value_distribution_query.replace(
                    "<attribute>", attribute_name
                ).replace("<table_name>", table_name)
                query_string = get_query_string(
                    config, default_queries, query_string, is_full_query=True
                )
                query_string = query_string.rstrip()  # First remove any extra whitespace
 
                if query_string.endswith("AND"):
                    query_string = query_string[:-3].rstrip()

                numeric_distribution_values, native_connection = (
                    execute_native_query(config, query_string, source_connection)
                )

                if not source_connection and native_connection:
                    source_connection = native_connection

                if numeric_distribution_values:
                    numeric_distribution_values = convert_to_lower(
                        numeric_distribution_values or {}
                    )
                    
                    if connection_type == ConnectionType.MongoDB.value and numeric_distribution_values:
                        numeric_distribution_values = transform_mongodb_field_names(numeric_distribution_values, attribute_name)
                    
                    # Use transformed field names for MongoDB, original for others
                    if connection_type == ConnectionType.MongoDB.value:
                        attribute_label = get_attribute_label(attribute_name)
                        counts = [f"{attribute_label}positive_values_count", f"{attribute_label}negative_values_count"]
                        zero_key = f"{attribute_label}zero_values_count"
                    else:
                        counts = ["positive_values_count", "negative_values_count"]
                        zero_key = "zero_values_count"
                    positive_values_count, negative_values_count = (
                        (
                            int(float(numeric_distribution_values.get(key, 0)))
                            if numeric_distribution_values.get(key)
                            and is_convertible_to_float(
                                numeric_distribution_values.get(key)
                            )
                            else 0
                        )
                        for key in counts
                    )
                    attribute_distribution.update(
                        {
                            "positive_values_count": positive_values_count,
                            "negative_values_count": negative_values_count,
                        }
                    )
            
            properties = asset.get("properties", {})
            properties = (
                json.loads(properties) if isinstance(properties, str) else properties
            )
            properties = properties if properties else {}
            table = properties.get("table", "")
            table =  table if table else asset.get("name","")
            database = properties.get("database", "")
            schema = properties.get("schema", "")
            # Generate positive_count query
            positive_count_query_string = get_default_measure_queries(
                config=config,
                default_queries=default_queries,
                measure_name="positive_count",
                measure_type="distribution",
                measure_category="distribution",
                derived_type=derived_type,
                table_name=table_name,
                database=database,
                table=table,
                schema=schema,
                attribute_name=attribute_name,
            )
            
            if positive_count_query_string:
                positive_count_query_string = prepare_json_attribute_flatten_query(
                    config, attribute_name, positive_count_query_string, "sub_query"
                )
                query_string = positive_count_query_string.replace(
                    "<attribute>", attribute_name
                ).replace("<table_name>", table_name)
                query_string = get_query_string(
                    config, default_queries, query_string, is_full_query=True
                )
                measure_level_queries.update({"positive_count": query_string})
            
            # Generate negative_count query
            negative_count_query_string = get_default_measure_queries(
                config=config,
                default_queries=default_queries,
                measure_name="negative_count",
                measure_type="distribution",
                measure_category="distribution",
                derived_type=derived_type,
                table_name=table_name,
                database=database,
                table=table,
                schema=schema,
                attribute_name=attribute_name,
            )
            
            if negative_count_query_string:
                negative_count_query_string = prepare_json_attribute_flatten_query(
                    config, attribute_name, negative_count_query_string, "sub_query"
                )
                query_string = negative_count_query_string.replace(
                    "<attribute>", attribute_name
                ).replace("<table_name>", table_name)
                query_string = get_query_string(
                    config, default_queries, query_string, is_full_query=True
                )
                measure_level_queries.update({"negative_count": query_string})
        supported_duplicate_datatypes = ["text", "integer", "numeric", "date", "datetime", "datetimeoffset", "time", "binary", "bit"]
        if derived_type in supported_duplicate_datatypes:
            properties = asset.get("properties", {})
            properties = (
                json.loads(properties) if isinstance(properties, str) else properties
            )
            properties = properties if properties else {}
            table = properties.get("table", "")
            table =  table if table else asset.get("name","")
            database = properties.get("database", "")
            schema = properties.get("schema", "")
            duplicate_query_string = get_default_measure_queries(
                config=config,
                default_queries=default_queries,
                measure_name="duplicate",
                measure_type="distribution",
                measure_category="distribution",
                derived_type=derived_type,
                table_name=table_name,
                database=database,
                table=table,
                schema=schema,
                attribute_name=attribute_name,
            )
            if duplicate_query_string:
                duplicate_query_string = prepare_json_attribute_flatten_query(
                    config, attribute_name, duplicate_query_string, "sub_query"
                )
                query_string = duplicate_query_string.replace(
                    "<attribute>", attribute_name
                ).replace("<table_name>", table_name)
                query_string = get_query_string(
                    config, default_queries, query_string, is_full_query=True
                )
                measure_level_queries.update({"duplicate": query_string})
        # Initialize range_distribution as an empty dictionary
        attribute_distribution["range_distribution"] = {}
        custom_range_distribution = {
            "length": 0,
            "value": 0,
        }  # Default range distribution

        # Fetch all active range measure thresholds
        range_measures = get_active_range_threshold_condition(config, attribute_id)

        # Fetch the attributes data for min/max length/value
        attributes_info = config.get("attributes", [])
        attributes_info = next(
            (
                attribute_info
                for attribute_info in attributes_info
                if attribute_info.get("id") == attribute_id
            ),
            None,
        )

        # Get common configuration for range measures
        asset_id = config.get('asset_id')
        drift_days = config.get('window', 30)
        drift_type = config.get('drift_type', 'runs')
        connection_id = config.get('connection_id')
        current_run_id = config.get("queue_id", "")

        # =============================================================================
        # FIXED: Value Range Processing - Using Fresh Attribute Values for First Run
        # =============================================================================
        if value_range_query:
            value_range_threshold = next(
                (
                    measure
                    for measure in range_measures
                    if measure["measure_name"] == "value_range"
                ),
                None,
            )
            if value_range_threshold:
                measure_id = value_range_threshold.get("measure_id", "")
                is_auto = value_range_threshold.get("is_auto", True)
                
                if not is_auto:
                    # Manual mode - use current min/max from attribute table
                    log_info(f"FIXED: Processing value_range using attribute min/max values (is_auto=False) for attribute_id={attribute_id}")
                    lower_threshold = (
                        float(attribute.get("min_value", 0))
                        if attribute.get("min_value")
                        else 0
                    )
                    upper_threshold = (
                        float(attribute.get("max_value", 0))
                        if attribute.get("max_value")
                        else 0
                    )
                else:
                    # Auto mode - try enhanced threshold first, fallback to fresh attribute values for first run
                    log_info(f"FIXED: Processing value_range using enhanced threshold logic for attribute_id={attribute_id}")

                    # First try to get stored/enhanced thresholds (for subsequent runs)
                    lower_threshold, upper_threshold, found, feedback_stats = get_stored_threshold(
                        config, "value_range", attribute_id, source_connection
                    )

                    # If no stored thresholds found (first run), get fresh values from core.attribute
                    if not found or (lower_threshold == 0 and upper_threshold == 0):
                        log_info(f"FIXED: No stored thresholds found, getting fresh values from core.attribute for value_range")
                        
                        postgres_connection = get_postgres_connection(config)
                        with postgres_connection.cursor() as cursor:
                            query = f"""
                                SELECT min_value, max_value 
                                FROM core.attribute 
                                WHERE id = '{attribute_id}'
                            """
                            cursor = execute_query(postgres_connection, cursor, query)
                            result = fetchone(cursor)
                            
                            if result and result.get("min_value") is not None and result.get("max_value") is not None:
                                try:
                                    lower_threshold = float(result.get("min_value", 0))
                                    upper_threshold = float(result.get("max_value", 0))
                                    log_info(f"FIXED: Used fresh core.attribute values for value_range: ({lower_threshold}, {upper_threshold})")
                                    
                                    # CORRECTED: Append baseline metadata to the list
                                    baseline_metadata_to_store.append({
                                        'connection': postgres_connection,
                                        'measure_id': value_range_threshold.get("measure_id", ""),
                                        'attribute_id': attribute_id,
                                        'run_id': current_run_id,
                                        'baseline_lower': lower_threshold,
                                        'baseline_upper': upper_threshold,
                                        'measure_name': 'value_range',
                                        'stats': {
                                            'calculation_type': 'fresh_attribute_values',
                                            'method': 'first_run',
                                            'source': 'core.attribute'
                                        }
                                    })
                                    
                                except (ValueError, TypeError) as e:
                                    log_error(f"FIXED: Error converting attribute values to float: {e}", e)
                                    # Keep the original 0,0 if conversion fails
                                    pass
                            else:
                                log_info(f"FIXED: No values found in core.attribute for value_range")
                    elif found and feedback_stats:
                        # Enhanced threshold found for subsequent runs
                        log_info(f"FIXED: Found enhanced thresholds for value_range: ({lower_threshold}, {upper_threshold})")
                        feedback_metadata_to_store = feedback_stats
                        feedback_metadata_to_store['enhanced_lower'] = lower_threshold
                        feedback_metadata_to_store['enhanced_upper'] = upper_threshold
                    else:
                        # Found baseline threshold but no enhancement
                        log_info(f"FIXED: Found baseline thresholds for value_range: ({lower_threshold}, {upper_threshold})")

                # Update the SQL query with the retrieved/calculated thresholds
                value_range_query = value_range_query.replace(
                    "<range_condition>", "NOT BETWEEN <value1> AND <value2>"
                ).replace(
                    "<value1>", str(lower_threshold)
                ).replace(
                    "<value2>", str(upper_threshold)
                )
                value_range_query = prepare_json_attribute_flatten_query(config, attribute_name, value_range_query, "sub_query")
                value_range_query = value_range_query.replace("<attribute>", attribute_name)

                query_string = get_query_string(
                    config, default_queries, value_range_query, is_full_query=True
                )
                config.update({"sub_category": "PROFILE"})
                measure_level_queries.update({"value_range": query_string})
                
                if "<range_condition>" not in query_string:
                    value_range, native_connection = execute_native_query(
                        config, query_string, source_connection
                    )
                    source_connection = source_connection or native_connection
                    value_range = convert_to_lower(value_range or {})
                    try:
                        range_count = value_range.get("range_count", 0)
                        custom_range_distribution["value"] = (
                            int(float(range_count))
                            if range_count and is_convertible_to_float(range_count)
                            else 0
                        )
                        log_info(f"FIXED: value_range query executed, violations found: {custom_range_distribution['value']}")
                    except Exception as e:
                        log_error(f"FIXED: Error processing value_range query result: {e}", e)
                        custom_range_distribution["value"] = 0

        # =============================================================================
        # FIXED: Length Range Processing - Using Fresh Attribute Values for First Run
        # =============================================================================
        if length_range_query:
            length_range_threshold = next(
                (
                    measure
                    for measure in range_measures
                    if measure["measure_name"] == "length_range"
                ),
                None,
            )
            if length_range_threshold:
                measure_id = length_range_threshold.get("measure_id", "")
                is_auto = length_range_threshold.get("is_auto", True)
                
                if not is_auto:
                    # Manual mode - use current min/max from attribute table
                    log_info(f"FIXED: Processing length_range using attribute min/max values (is_auto=False) for attribute_id={attribute_id}")
                    lower_threshold = (
                        int(float(attribute.get("min_length", 0)))
                        if attribute.get("min_length")
                        else 0
                    )
                    upper_threshold = (
                        int(float(attribute.get("max_length", 0)))
                        if attribute.get("max_length")
                        else 0
                    )
                else:
                    # Auto mode - try enhanced threshold first, fallback to fresh attribute values for first run
                    log_info(f"FIXED: Processing length_range using enhanced threshold logic for attribute_id={attribute_id}")

                    # First try to get stored/enhanced thresholds (for subsequent runs)
                    lower_threshold, upper_threshold, found, feedback_stats = get_stored_threshold(
                        config, "length_range", attribute_id, source_connection
                    )

                    # If no stored thresholds found (first run), get fresh values from core.attribute
                    if not found or (lower_threshold == 0 and upper_threshold == 0):
                        log_info(f"FIXED: No stored thresholds found, getting fresh values from core.attribute for length_range")
                        
                        postgres_connection = get_postgres_connection(config)
                        with postgres_connection.cursor() as cursor:
                            query = f"""
                                SELECT min_length, max_length 
                                FROM core.attribute 
                                WHERE id = '{attribute_id}'
                            """
                            cursor = execute_query(postgres_connection, cursor, query)
                            result = fetchone(cursor)
                            
                            if result and result.get("min_length") is not None and result.get("max_length") is not None:
                                try:
                                    lower_threshold = float(result.get("min_length", 0))
                                    upper_threshold = float(result.get("max_length", 0))
                                    log_info(f"FIXED: Used fresh core.attribute values for length_range: ({lower_threshold}, {upper_threshold})")
                                    
                                    # CORRECTED: Append baseline metadata to the list
                                    baseline_metadata_to_store.append({
                                        'connection': postgres_connection,
                                        'measure_id': length_range_threshold.get("measure_id", ""),
                                        'attribute_id': attribute_id,
                                        'run_id': current_run_id,
                                        'baseline_lower': lower_threshold,
                                        'baseline_upper': upper_threshold,
                                        'measure_name': 'length_range',
                                        'stats': {
                                            'calculation_type': 'fresh_attribute_values',
                                            'method': 'first_run',
                                            'source': 'core.attribute'
                                        }
                                    })
                                    
                                except (ValueError, TypeError) as e:
                                    log_error(f"FIXED: Error converting attribute lengths to float: {e}", e)
                                    # Keep the original 0,0 if conversion fails
                                    pass
                            else:
                                log_info(f"FIXED: No values found in core.attribute for length_range")
                    elif found and feedback_stats:
                        # Enhanced threshold found for subsequent runs
                        log_info(f"FIXED: Found enhanced thresholds for length_range: ({lower_threshold}, {upper_threshold})")
                        if not feedback_metadata_to_store:
                            feedback_metadata_to_store = feedback_stats
                            feedback_metadata_to_store['enhanced_lower'] = lower_threshold
                            feedback_metadata_to_store['enhanced_upper'] = upper_threshold
                    else:
                        # Found baseline threshold but no enhancement
                        log_info(f"FIXED: Found baseline thresholds for length_range: ({lower_threshold}, {upper_threshold})")

                # Update the SQL query with the retrieved/calculated thresholds
                length_range_query = length_range_query.replace(
                    "<range_condition>", "NOT BETWEEN <value1> AND <value2>"
                ).replace(
                    "<value1>", str(lower_threshold)
                ).replace(
                    "<value2>", str(upper_threshold)
                )
                length_range_query = prepare_json_attribute_flatten_query(config, attribute_name, length_range_query, "sub_query")
                length_range_query = length_range_query.replace("<attribute>", attribute_name)

                query_string = get_query_string(
                    config, default_queries, length_range_query, is_full_query=True
                )
                config.update({"sub_category": "PROFILE"})
                measure_level_queries.update({"length_range": query_string})
                
                if "<range_condition>" not in query_string:
                    length_range, native_connection = execute_native_query(
                        config, query_string, source_connection
                    )
                    source_connection = source_connection or native_connection
                    length_range = convert_to_lower(length_range or {})
                    try:
                        range_count = length_range.get("range_count", 0)
                        custom_range_distribution["length"] = (
                            int(float(range_count))
                            if range_count and is_convertible_to_float(range_count)
                            else 0
                        )
                        log_info(f"FIXED: length_range query executed, violations found: {custom_range_distribution['length']}")
                    except Exception as e:
                        log_error(f"FIXED: Error processing length_range query result: {e}", e)
                        custom_range_distribution["length"] = 0

        # Update non-empty measure
        active_non_empty_measures = get_active_measures(
            config,
            attribute_id,
            ["null_count", "blank_count", "space_count"],
        )
        allow_non_empty_query_execution = len(active_non_empty_measures) < 3
        if allow_non_empty_query_execution:
            non_empty_query = (
                default_queries.get("failed_rows", {})
                .get("attribute", {})
                .get("non_empty")
            )
            if non_empty_query:
                non_empty_query = prepare_json_attribute_flatten_query(config, attribute_name, non_empty_query, "sub_query")
                non_empty_query = (
                    non_empty_query.replace(
                        "<failed_attributes>", "COUNT(*) AS non_empty"
                    )
                    .replace("<not_condition>", "")
                    .replace("<attribute>", attribute_name)
                    .replace("<limit_condition>", "")
                )
                if connection_type == ConnectionType.Salesforce.value and attributes_info and attributes_info.get("datatype", "").lower() in ["boolean"]:
                    return (attribute_id, None)
                query_string = get_query_string(
                    config, default_queries, non_empty_query, is_full_query=True
                )
                non_empty_dist, _ = execute_native_query(
                    config, query_string, source_connection
                )
                non_empty_dist = non_empty_dist if non_empty_dist else {}
                non_empty_count = non_empty_dist.get("non_empty", 0)
                non_empty_count = non_empty_count if non_empty_count else 0
                attribute_distribution.update(
                    {"non_empty_distribution": non_empty_count}
                )

        # Update the latest range distribution
        attribute_distribution["range_distribution"].update(custom_range_distribution)

        if attribute_distribution:
            attribute_distribution = get_attribute_distributions(
                attribute, attribute_distribution, attribute_metrics
            )
            attribute_distribution.update(
                {
                    "attribute": deepcopy(attribute),
                    "attribute_metrics": attribute_metrics,
                }
            )
        log_info(f"attribute_distribution : {attribute_distribution}")
        log_info(f"attribute_distribution for {attribute_id} - {attribute_name} - completed")
        log_info(f"Measure level queries : {measure_level_queries}")
        if active_measures:
            attribute_distribution.update({"active_measures": active_measures})
        profile_metrics = {str(attribute_id): attribute_distribution}
        config.update(
            {"measure_level_queries": {str(attribute_id): measure_level_queries}}
        )

        # This call INSERTS the primary metrics into core.metrics.
        # After this, the record we need to update will exist.
        save_profile_result(config, profile_metrics, health_metrics_dict)

        # =============================================================================
        # ENHANCED: Current-Run Enhancement Pass for ALL measure types (PRESERVED)
        # =============================================================================
        
        # Get all active profile measures for this attribute
        postgres_connection = get_postgres_connection(config)
        all_profile_measures = get_profile_measures(config, postgres_connection)
        attribute_profile_measures = [
            measure for measure in all_profile_measures 
            if measure.get("attribute_id") == attribute_id
        ]
        
        # Loop through each profile measure and apply enhancement based on measure type
        for measure in attribute_profile_measures:
            measure_name = measure.get("name", "")
            measure_id = measure.get("id", "")
            
            #Skip range measures as they are already handled above
            if measure_name in ['length_range', 'value_range']:
                continue
            
            
            try:
                # Database-driven detection to determine if this measure has sub-values
                is_sub_value_measure = has_sub_values(measure_id, postgres_connection)
                
                if is_sub_value_measure:
                    # Handle sub-value measures (patterns, enums, lengths)
                    
                    # Get all individual records for this measure from current run
                    individual_records = get_individual_measure_records(
                        postgres_connection, measure_id, attribute_id, current_run_id
                    )
                    
                    for record in individual_records:
                        specific_value = record.get("measure_name")  # The actual pattern/enum/length value
                        record_id = record.get("id")
                        
                        if not specific_value:
                            continue
                            
                        
                        try:
                            # Get enhanced thresholds for this specific value
                            lower_threshold, upper_threshold, found, feedback_stats = get_stored_threshold(
                                config, measure_name, attribute_id, postgres_connection, specific_value
                            )
                            if found and feedback_stats:                                
                                # Update this specific record with enhanced thresholds
                                update_specific_record_threshold(
                                    postgres_connection, record_id, measure_id, attribute_id, current_run_id,
                                    feedback_stats['enhanced_lower'], feedback_stats['enhanced_upper'], 
                                    feedback_stats['stats'], specific_value
                                )
                                
                            
                                
                        except Exception as e:
                            log_error(f"ENHANCED_FLOW: Error processing {measure_name}[{specific_value}]: {e}", e)
                            continue
                else:
                    # Handle aggregate measures (existing logic)
                    
                    try:
                        # Get enhanced thresholds for the aggregate measure
                        lower_threshold, upper_threshold, found, feedback_stats = get_stored_threshold(
                            config, measure_name, attribute_id, postgres_connection
                        )
                        
                        if found and feedback_stats:
                            
                            # Update the aggregate measure with enhanced thresholds
                            store_feedback_metadata_in_threshold_column(
                                feedback_stats['connection'],
                                feedback_stats['measure_id'],
                                feedback_stats['attribute_id'],
                                feedback_stats['run_id'],
                                feedback_stats['enhanced_lower'],
                                feedback_stats['enhanced_upper'],
                                feedback_stats['stats'],
                                config
                            )
                            
                            
                    except Exception as e:
                        log_error(f"ENHANCED_FLOW: Error processing aggregate measure {measure_name}: {e}", e)
                        continue
                        
            except Exception as e:
                log_error(f"ENHANCED_FLOW: Error processing measure {measure_name}: {e}", e)
                continue

        # =============================================================================
        # REFACTORED: Store Enhanced and Baseline Metadata Consistently
        # =============================================================================
        
        # Store enhanced thresholds (from feedback enhancement)
        if feedback_metadata_to_store:
            log_info(f"REFACTORED: Storing enhanced threshold metadata for range measures on attribute {attribute_id}")
            store_feedback_metadata_in_threshold_column(
                feedback_metadata_to_store['connection'],
                feedback_metadata_to_store['measure_id'],
                feedback_metadata_to_store['attribute_id'],
                feedback_metadata_to_store['run_id'],
                feedback_metadata_to_store['enhanced_lower'],
                feedback_metadata_to_store['enhanced_upper'],
                feedback_metadata_to_store['stats'],
                config
            )

        # CORRECTED: Loop through the list and store all baseline metadata
        for baseline_item in baseline_metadata_to_store:
            log_info(f"DEBUG: About to store baseline for {baseline_item['measure_name']}")
            log_info(f"DEBUG: baseline_lower = {baseline_item['baseline_lower']}")
            log_info(f"DEBUG: baseline_upper = {baseline_item['baseline_upper']}")
            log_info(f"DEBUG: Full baseline_item = {baseline_item}")
            log_info(f"REFACTORED: Storing baseline threshold metadata for {baseline_item['measure_name']} on attribute {attribute_id}")
            store_baseline_metadata_in_threshold_column(
                baseline_item['connection'],
                baseline_item['measure_id'],
                baseline_item['attribute_id'],
                baseline_item['run_id'],
                baseline_item['baseline_lower'],
                baseline_item['baseline_upper'],
                baseline_item['stats'],
                baseline_item['measure_name'],
                config
            )

        log_info(f"FIXED: Completed deep profiling with enhanced functionality for attribute {attribute_id}")
        return (attribute_id, None)
        
    except Exception as e:
        log_error(
            f"run profile - failed for the attribute = {attribute_id} - {attribute_name} - with Error {str(e)}",
            e,
        )
        return (attribute_id, str(e))

def get_temp_table_query(config: dict, default_queries: dict):
    """
    Returns the temp table query for snowflake views
    """
    asset_id = config.get("asset_id")
    asset = config.get("asset", {})
    asset = asset if asset else {}
    connection_type = config.get("connection_type")
    asset_type = asset.get("type") if asset else ""
    asset_type = asset_type.lower() if asset_type else ""
    view_type = asset.get("view_type") if asset else ""
    view_type = str(view_type).lower() if view_type else "table"
    general_settings = config.get("settings", {})
    if not general_settings:
        dag_info = config.get("dag_info", {})
        dag_info = dag_info if dag_info else {}
        general_settings = dag_info.get("settings", {})
    general_settings = general_settings if general_settings else {}
    general_settings = (
        json.loads(general_settings, default=str)
        if isinstance(general_settings, str)
        else general_settings
    )

    profile_settings = general_settings.get("profile")
    profile_settings = (
        json.loads(profile_settings, default=str)
        if profile_settings and isinstance(profile_settings, str)
        else profile_settings
    )
    profile_settings = profile_settings if profile_settings else {}
    is_create_table_enabled = profile_settings.get("allow_create_temp_table")

    create_temp_table_query = ""
    if not (
        (
            asset_type == "view"
            or (asset_type.lower() == "query" and view_type == "view")
        )
        and connection_type == ConnectionType.Snowflake.value
    ):
        return create_temp_table_query

    if not is_create_table_enabled:
        return create_temp_table_query

    table_name = config.get("technical_name")
    schema_name = config.get("schema")
    connection = config.get("connection")
    connection = connection if connection else {}
    connection_type = connection.get("type")
    connection_type = connection_type.lower() if connection_type else ""
    credentials = connection.get("credentials")
    credentials = (
        json.loads(credentials)
        if credentials and isinstance(credentials, str)
        else credentials
    )
    credentials = credentials if credentials else {}
    db_name = credentials.get("database")
    db_name = db_name if db_name else ""

    asset = asset if asset else {}
    profile_database_name = config.get("profile_database_name")
    profile_database_name = profile_database_name if profile_database_name else ""
    profile_schema_name = config.get("profile_schema_name")
    profile_schema_name = profile_schema_name if profile_schema_name else schema_name

    properties = asset.get("properties", {})
    properties = json.loads(properties) if isinstance(properties, str) else properties
    properties = properties if properties else {}
    is_incremental = config.get("is_incremental", False)
    if not db_name:
        db_name = properties.get("database")

    database = properties.get("database")
    database = database if database else db_name
    profile_database_name = profile_database_name if profile_database_name else database
    profile_database_name = profile_database_name if profile_database_name else db_name
    schema_name = properties.get("schema")
    profile_schema_name = profile_schema_name if profile_schema_name else schema_name

    failed_rows_query = default_queries.get("failed_rows", {})
    temp_table_name = f"PROFILE_{str(asset_id)}"
    create_temp_table_query = failed_rows_query.get("create_temp_table")
    source_query = f"""SELECT * FROM "{db_name}"."{schema_name}"."{table_name}" """
    if is_incremental and asset_type and asset_type.lower() == "view":
        source_query = f"""{source_query} <incremental_condition> """
        source_query = get_query_string(
            config, default_queries, source_query, is_full_query=True
        )

    create_temp_table_query = (
        create_temp_table_query.replace("<database_name>", f'"{profile_database_name}"')
        .replace("<schema_name>", profile_schema_name)
        .replace("<temp_table_name>", temp_table_name)
        .replace("<query_string>", source_query)
    )

    config.update(
        {
            "has_temp_table": True,
            "temp_table_name": temp_table_name,
            "temp_schema_name": profile_schema_name,
            "temp_database_name": profile_database_name,
            "temp_view_table_name": f""" "{profile_database_name}"."{profile_schema_name}"."{temp_table_name}" """,
        }
    )

    if str(view_type).lower() == "direct query":
        table_name = f"""({asset.get("query")}) as direct_query_table"""
        if connection_type and str(connection_type).lower() in [
            ConnectionType.Oracle.value.lower(),
            ConnectionType.BigQuery.value.lower(),
        ]:
            table_name = f"""({asset.get("query")})"""
        config.update({"temp_view_table_name": table_name})
    return create_temp_table_query


def check_deep_profile_skip(attribute: dict, datatypes_to_skip: list):
    datatype = attribute.get("derived_type", "")
    if datatype in datatypes_to_skip:
        return True


def get_active_range_threshold_condition(config, attribute_id) -> list:
    """
    Get active range threshold condition for range measures
    """
    connection = get_postgres_connection(config)
    range_measures = []
    with connection.cursor() as cursor:
        query_string = f"""
            select id as measure_id, technical_name as measure_name,is_auto,threshold_constraints from core.measure
            where attribute_id = '{attribute_id}'
            and technical_name in ('length_range','value_range')
            and is_active = True and is_delete = False
        """
        cursor = execute_query(connection, cursor, query_string)
        range_measures = fetchall(cursor)

    return range_measures


def update_range_condition(
    query: str, threshold_data: dict, connection_type: str, measure_type: str
) -> tuple:
    """
    Update the range condition in a query string according to the threshold constraints.

    Args:
        query (str): The input query string containing a placeholder <range_condition>.
        threshold_data (dict): A dictionary containing:
            - "condition" (str): Specifies the condition (e.g., 'isgreaterthan', 'isbetween').
            - "value" (str/int/float): The first value for the condition.
            - "value2" (str/int/float): The second value for 'isbetween' or 'isnotbetween' (optional).

    Returns:
        tupel: The updated query string with the <range_condition> replaced by the appropriate condition,
             or the original query string if an error occurs.
    """
    try:
        # Dictionary of comparison operators and their corresponding SQL-like strings
        query = query or ""  # Ensure query is not None
        range_condition = ""
        comparison_operator = {
            "isgreaterthan": "> <value1>",
            "islessthan": "< <value1>",
            "isgreaterthanorequalto": ">= <value1>",
            "islessthanorequalto": "<= <value1>",
            "isequalto": "= <value1>",
            "isbetween": "BETWEEN <value1> and <value2>",
            "isnotbetween": "NOT BETWEEN <value1> and <value2>",
        }

        # Extract values from threshold_data
        condition = threshold_data.get("condition", "").lower()
        value1 = threshold_data.get("value", 0)
        value2 = threshold_data.get("value2", 0)

        # Validate the condition
        if condition not in comparison_operator:
            raise ValueError(
                f"Invalid condition '{condition}'. Valid options are: {list(comparison_operator.keys())}"
            )

        # Validate the required values for specific conditions
        if condition in ["isbetween", "isnotbetween"]:
            if value1 is None or value2 is None:
                raise ValueError(
                    f"Range Condition '{condition}' requires both 'value' and 'value2'."
                )
        elif value1 is None:
            raise ValueError(f"Range Condition '{condition}' requires 'value'.")

        if connection_type == ConnectionType.BigQuery.value:
            if measure_type == "value_range":
                value1 = f"cast({value1} as string)"
                value2 = f"cast({value2} as string)"

        # Retrieve the corresponding SQL-like string and replace placeholders
        range_condition = comparison_operator.get(condition)
        range_condition = range_condition.replace("<value1>", str(value1))
        range_condition = range_condition.replace(
            "<value2>", str(value2) if value2 is not None else ""
        )
        # Replace <range_condition> in the query string
        query = query.replace("<range_condition>", range_condition)
        return (query, range_condition)

    except Exception as e:
        # Log the error (replace this with actual logging if required)
        log_error(
            f"Error updating range condition: {e}",
            e,
        )
        # Return the original query string as a fallback
        return (query, range_condition)