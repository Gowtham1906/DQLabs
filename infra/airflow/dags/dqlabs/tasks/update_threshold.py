"""
    Migration Notes From V2 to V3:
    Enhanced with Context-Aware Feedback System for Range Measures
    
    NEW ENHANCEMENTS:
    - Time-based confidence decay for all feedback
    - Blending logic extended to non-range measures
    - Consistent threshold evolution across all measure types
    - Config-Driven Alert Reset System
"""

import json
import math
import numpy as np
import random
import time
from datetime import datetime, timezone
from math import isnan
from uuid import uuid4

import pandas as pd

from dqlabs.utils import get_general_settings
from dqlabs.utils.drift import (
    get_marked_values,
    get_metrics,
    update_flag_threshold,
    get_measure_threshold,
    get_postgres_metrics,
    cleanup_metrics,
    get_timeseries_metrics,
    get_current_run_value,
    get_previous_value,
)
from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.db_helper import execute_query, split_queries, fetchone, fetchall
from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.models.univariate_models import UnivariateModel
from dqlabs.models.behavioral_models import BehavioralModel
from dqlabs.app_constants.dq_constants import (
    HEALTH,
    PROFILE,
    BEHAVIORAL,
    CUSTOM,
    RELIABILITY,
    DRIFT_DAYS,
    DEFAULT_CHUNK_LIMIT,
)

# Import ValueRangeObserver and related functions
from dqlabs.utils.value_range_observe import ValueRangeObserver, process_historical_values, get_attribute_historical_boundaries

# Add a timestamp to see exactly when this module is loaded
module_load_time = time.strftime("%Y-%m-%d %H:%M:%S")
log_info(f"==== ENHANCED update_threshold.py module loaded at {module_load_time} ====")


def get_last_reset_timestamp(connection, measure_id, attribute_id):
    """
    Get the most recent reset timestamp for a measure/attribute combination.
    
    Args:
        connection: Database connection object
        measure_id (str): ID of the measure
        attribute_id (str): ID of the attribute
        
    Returns:
        str or None: Reset timestamp or None if no reset found
    """
    try:
        attribute_condition = ""
        if attribute_id:
            attribute_condition = f"AND attribute_id = '{attribute_id}'"
        with connection.cursor() as cursor:
            query = f"""
                SELECT action_history, created_date
                FROM core.metrics
                WHERE measure_id = '{measure_id}'
                {attribute_condition}
                AND action_history is not null
                AND jsonb_typeof(action_history) = 'object'
                AND action_history !='{{}}'::jsonb
                ORDER BY created_date DESC
                LIMIT 1
            """
            cursor = execute_query(connection, cursor, query)
            results = fetchall(cursor)
            
            for row in results:
                action_history = row.get("action_history", {})
                if isinstance(action_history, str):
                    try:
                        action_history = json.loads(action_history)
                    except json.JSONDecodeError:
                        continue
                
                if action_history.get("is_changed", False):
                    reset_timestamp = action_history.get("date")
                    if reset_timestamp:
                        log_info(f"RESET_TIMESTAMP: Found reset at {reset_timestamp} for measure {measure_id}")
                        return reset_timestamp
            
            log_info(f"RESET_TIMESTAMP: No reset found for measure {measure_id}")
            return None
            
    except Exception as e:
        log_error(f"Error getting reset timestamp for measure {measure_id}: {e}", e)
        return None


def detect_and_handle_reset(config, connection, cursor, measure_id, attribute_id, 
                           measure_name, metric_values):
    """
    Detect and handle reset scenarios based on action_history in core.metrics table.
    
    Args:
        config: Configuration dictionary
        connection: Database connection object
        cursor: Database cursor
        measure_id: ID of the measure
        attribute_id: ID of the attribute
        measure_name: Name of the measure
        metric_values: List of metric values
        
    Returns:
        tuple: (is_reset_run, processed_metric_values)
    """
    try:
        run_id = config.get('queue_id')
        attribute_condition = ""
        if attribute_id:
            attribute_condition = f"AND attribute_id = '{attribute_id}'"
        # RESET DETECTION: Check for is_changed flag in action_history from core.metrics
        action_history_query = f"""
            SELECT action_history
            FROM core.metrics
            WHERE measure_id = '{measure_id}'
            {attribute_condition}
            AND run_id = '{run_id}'
            AND action_history is not null
            AND jsonb_typeof(action_history) = 'object'
            AND action_history !='{{}}'::jsonb
            ORDER BY created_date DESC
            LIMIT 1
        """
        cursor = execute_query(connection, cursor, action_history_query)
        action_history_result = fetchone(cursor)
        
        is_reset_run = False
        if action_history_result:
            action_history = action_history_result.get("action_history", {})
            
            # Parse JSON if it's a string
            if isinstance(action_history, str):
                try:
                    action_history = json.loads(action_history)
                except json.JSONDecodeError:
                    action_history = {}
            
            is_reset_run = action_history.get("is_changed", False)
            
            if is_reset_run:
                action = action_history.get("action", "unknown")
                log_info(f"RESET DETECTED for measure_id {measure_id} ({measure_name}). "
                        f"Action: {action}. Executing cleanup and first run behavior.")
                
                # UNIVERSAL CLEANUP: Delete old threshold results for this measure
                attribute_query = f"AND attribute_id = '{attribute_id}'" if attribute_id else ""
                cleanup_query = f"""
                    DELETE FROM core.drift_threshold
                    WHERE measure_id = '{measure_id}' {attribute_query}
                """
                cursor = execute_query(connection, cursor, cleanup_query)
                cleaned_count = cursor.rowcount
                log_info(f"RESET CLEANUP: Cleaned up {cleaned_count} old threshold "
                        f"results for measure {measure_id}")
                
                # FIRST RUN LOGIC: Handle different measure types
                if measure_name in ['length_range', 'value_range']:
                    # Range measures: Cleanup is sufficient, existing process will
                    # fall back to baseline from current data profile
                    log_info(f"RESET: Range measure {measure_name} will use baseline "
                            f"from current data profile")
                    processed_metric_values = metric_values
                    
                else:
                    # Non-range measures: Use only the single most recent value
                    # for "first run" calculation
                    if metric_values:
                        processed_metric_values = [metric_values[-1]]
                        log_info(f"RESET: Using single most recent value for first-run "
                                f"calculation for measure {measure_id} ({measure_name})")
                    else:
                        processed_metric_values = metric_values
                        log_info(f"RESET: No metric values available for measure "
                                f"{measure_id} ({measure_name})")
        
        if not is_reset_run:
            # Normal run: Use full historical data
            processed_metric_values = metric_values
        return is_reset_run, processed_metric_values
        
    except Exception as e:
        log_error(f"Error in reset detection for measure {measure_id}: {e}", e)
        # On error, fall back to normal processing
        return False, metric_values
    
# new helper functions
def evaluate_threshold_priority(profile_context, data_analysis_thresholds, measure_name, connection=None, measure_id=None, attribute_id=None):
    """
    CORRECTED: Proper feedback history detection and age-based blending.
    
    Priority 1: Current feedback (minimal blending)
    Priority 2a: Never had feedback (pure data analysis)  
    Priority 2b: Had feedback before (age-based blending)
    Priority 3: System error (fallback)
    """
    try:
        data_lower, data_upper = data_analysis_thresholds
        
        # Priority 3: System error (rare case)
        if not profile_context or 'lower_threshold' not in profile_context:
            log_info(f"PRIORITY_3: {measure_name} - System error, using pure data analysis")
            return {
                'chosen_lower': data_lower,
                'chosen_upper': data_upper,
                'priority_level': 'system_error_fallback',
                'reasoning': 'Profile.py failed - using pure data analysis'
            }
        
        profile_lower = profile_context['lower_threshold']
        profile_upper = profile_context['upper_threshold']
        # Check for current run feedback (rich metadata)
        feedback_metadata = profile_context.get('feedback_metadata', {})
        feedback_triggers = feedback_metadata.get('feedback_triggers', [])
        has_current_feedback = len(feedback_triggers) > 0

        if has_current_feedback:
            # Priority 1: Current feedback - minimal blending
            blend_factor = 0.15  # 15% data, 85% feedback
            
            blended_lower = profile_lower + blend_factor * (data_lower - profile_lower)
            blended_upper = profile_upper + blend_factor * (data_upper - profile_upper)
            
            final_lower = round(blended_lower)
            final_upper = round(blended_upper)
            
            log_info(f"PRIORITY_1: {measure_name} - Current feedback: "
                    f"({profile_lower}, {profile_upper}) + ({data_lower}, {data_upper}) = ({final_lower}, {final_upper})")
            
            return {
                'chosen_lower': final_lower,
                'chosen_upper': final_upper,
                'priority_level': 'current_user_feedback',
                'reasoning': f'Current user feedback with {blend_factor*100}% data adaptation',
                'use_profile_py': True,
                'has_user_feedback': True
            }
        
        else:
            # Priority 2: No current feedback - check feedback history
            ever_had_feedback = False
            runs_since_feedback = 999
            
            if connection and measure_id and attribute_id:
                try:
                    with connection.cursor() as cursor:
                        # Check if ANY feedback ever existed
                        feedback_history_query = f"""
                            SELECT COUNT(*) as total_feedback
                            FROM core.metrics
                            WHERE measure_id = '{measure_id}'
                            AND measure_name = '{measure_name}'
                            AND attribute_id = '{attribute_id}'
                            AND marked_as IS NOT NULL
                        """
                        cursor = execute_query(connection, cursor, feedback_history_query)
                        result = fetchone(cursor)
                        total_feedback = result.get('total_feedback', 0) if result else 0
                        ever_had_feedback = total_feedback > 0
                        
                        if ever_had_feedback:
                            # Calculate runs since last feedback
                            age_query = f"""
                                SELECT COUNT(*) as runs_since
                                FROM core.drift_threshold
                                WHERE measure_id = '{measure_id}'
                                AND attribute_id = '{attribute_id}'
                                AND created_date > (
                                    SELECT MAX(created_date) 
                                    FROM core.metrics 
                                    WHERE measure_id = '{measure_id}' 
                                    AND measure_name = '{measure_name}'
                                    AND attribute_id = '{attribute_id}'
                                    AND marked_as IS NOT NULL
                                )
                            """
                            cursor = execute_query(connection, cursor, age_query)
                            age_result = fetchone(cursor)
                            runs_since_feedback = age_result.get('runs_since', 5) if age_result else 5
                            
                except Exception as e:
                    log_error(f"Error checking feedback history for {measure_name}: {e}", e)
            
            if not ever_had_feedback:
                # Priority 2a: Never had feedback - pure data analysis
                log_info(f"PRIORITY_2A: {measure_name} - No feedback history, using pure data analysis: ({data_lower}, {data_upper})")
                
                return {
                    'chosen_lower': data_lower,
                    'chosen_upper': data_upper,
                    'priority_level': 'no_feedback_history',
                    'reasoning': 'No feedback history - using pure data analysis',
                    'use_profile_py': False,
                    'has_user_feedback': False
                }
            
            else:
                # Priority 2b: Had feedback before - age-based blending
                
                # Age-based blend factors
                if runs_since_feedback <= 2:
                    blend_factor = 0.2    # Recent: 20% data, 80% feedback
                elif runs_since_feedback <= 5:
                    blend_factor = 0.4    # Medium: 40% data, 60% feedback  
                elif runs_since_feedback <= 15:
                    blend_factor = 0.6    # Old: 60% data, 40% feedback
                else:
                    blend_factor = 0.75   # Very old: 75% data, 25% feedback
                
                blended_lower = profile_lower + blend_factor * (data_lower - profile_lower)
                blended_upper = profile_upper + blend_factor * (data_upper - profile_upper)
                
                final_lower = round(blended_lower)
                final_upper = round(blended_upper)
                
                log_info(f"PRIORITY_2B: {measure_name} - Age-based blending ({runs_since_feedback} runs old): "
                        f"({profile_lower}, {profile_upper}) + ({data_lower}, {data_upper}) = ({final_lower}, {final_upper}) [blend: {blend_factor:.2f}]")
                
                return {
                    'chosen_lower': final_lower,
                    'chosen_upper': final_upper,
                    'priority_level': 'age_based_feedback_blend',
                    'reasoning': f'Previous feedback {runs_since_feedback} runs old - {(1-blend_factor)*100:.0f}% feedback + {blend_factor*100:.0f}% data',
                    'use_profile_py': True,
                    'has_user_feedback': False,
                    'successful_previous_feedback': True
                }
        
    except Exception as e:
        log_error(f"Error in priority evaluation for {measure_name}: {e}", e)
        return {
            'chosen_lower': data_analysis_thresholds[0],
            'chosen_upper': data_analysis_thresholds[1],
            'priority_level': 'error_fallback',
            'reasoning': f'Error in priority evaluation: {e}',
            'use_profile_py': False
        }

def check_signal_alignment(profile_lower, profile_upper, data_lower, data_upper, measure_name):
    """
    NEW: Check if profile.py and data analysis thresholds are reasonably aligned.
    
    Returns:
        dict: Alignment information
    """
    try:
        profile_range = profile_upper - profile_lower
        data_range = data_upper - data_lower
        
        # Calculate differences
        lower_diff = abs(profile_lower - data_lower)
        upper_diff = abs(profile_upper - data_upper)
        
        # Tolerance based on measure type and range size
        if measure_name in ['length_range', 'value_range']:
            # For range measures, use percentage of range
            base_tolerance = 0.15  # 15% tolerance
            if profile_range > 0:
                lower_tolerance = base_tolerance * profile_range
                upper_tolerance = base_tolerance * profile_range
            else:
                lower_tolerance = upper_tolerance = 10  # Absolute tolerance for small ranges
        else:
            # For count measures, use smaller absolute tolerance
            base_tolerance = 0.20  # 20% tolerance
            if profile_range > 0:
                lower_tolerance = base_tolerance * profile_range  
                upper_tolerance = base_tolerance * profile_range
            else:
                lower_tolerance = upper_tolerance = 5  # Absolute tolerance
        
        # Check alignment
        lower_aligned = lower_diff <= lower_tolerance
        upper_aligned = upper_diff <= upper_tolerance
        overall_aligned = lower_aligned and upper_aligned
        
        # Calculate overall tolerance percentage for logging
        max_range = max(profile_range, data_range)
        tolerance_pct = base_tolerance if max_range > 0 else 0
        
        return {
            'aligned': overall_aligned,
            'lower_aligned': lower_aligned,
            'upper_aligned': upper_aligned,
            'lower_diff': lower_diff,
            'upper_diff': upper_diff,
            'tolerance': tolerance_pct,
            'tolerance_values': (lower_tolerance, upper_tolerance)
        }
        
    except Exception as e:
        log_error(f"Error checking signal alignment: {e}", e)
        return {'aligned': False, 'tolerance': 0}


def detect_contaminated_previous_run(previous_lower, previous_upper, baseline_lower, baseline_upper, measure_name):
    """
    NEW: Detect if previous run thresholds are contaminated/extreme outliers.
    
    Args:
        previous_lower, previous_upper: Previous run thresholds
        baseline_lower, baseline_upper: Current baseline thresholds (from priority decision)
        measure_name: Name of the measure
        
    Returns:
        bool: True if previous thresholds should be ignored
    """
    try:
        if previous_lower is None or previous_upper is None:
            return False
            
        # Calculate ranges for comparison
        previous_range = previous_upper - previous_lower
        baseline_range = baseline_upper - baseline_lower
        
        # Detection criteria
        contamination_indicators = []
        
        # 1. Previous range is significantly larger than baseline
        if baseline_range > 0 and previous_range > (baseline_range * 2.5):
            contamination_indicators.append(f"previous_range({previous_range}) >> baseline_range({baseline_range})")
        
        # 2. Previous bounds are very far from baseline
        if abs(previous_lower - baseline_lower) > abs(baseline_range * 1.5):
            contamination_indicators.append(f"previous_lower({previous_lower}) far from baseline_lower({baseline_lower})")
            
        if abs(previous_upper - baseline_upper) > abs(baseline_range * 1.5):
            contamination_indicators.append(f"previous_upper({previous_upper}) far from baseline_upper({baseline_upper})")
        
        # 3. For length_range, check for unreasonable negative values when baseline is reasonable
        if measure_name == "length_range" and previous_lower < -50 and baseline_lower >= -10:
            contamination_indicators.append(f"unreasonable_negative_length({previous_lower}) vs reasonable_baseline({baseline_lower})")
        
        # Contamination detected if multiple indicators
        is_contaminated = len(contamination_indicators) >= 2
        
        if is_contaminated:
            log_info(f"CONTAMINATION_DETECTED: Previous thresholds ({previous_lower}, {previous_upper}) "
                    f"vs baseline ({baseline_lower}, {baseline_upper}). "
                    f"Indicators: {contamination_indicators}")
        else:
            log_info(f"CONTAMINATION_CHECK: Previous thresholds ({previous_lower}, {previous_upper}) "
                    f"appear reasonable vs baseline ({baseline_lower}, {baseline_upper})")
            
        return is_contaminated
        
    except Exception as e:
        log_error(f"Error detecting contaminated previous run: {e}", e)
        return False


def extract_valuerange_thresholds(measure_data, config, connection):
    """
    NEW: Extract ValueRangeObserver thresholds from existing logic.
    Reuses existing process_historical_values functionality.
    
    Returns:
        tuple: (historical_lower, historical_upper) from ValueRangeObserver
    """
    try:
        measure_name = measure_data.get('measure_name')
        measure_id = measure_data.get('measure_id')
        attribute_id = measure_data.get('attribute_id')
        task_id = measure_data.get('task_id')
        drift_days = config.get('window', 30)
        drift_type = config.get('drift_type', 'runs')
        connection_id = config.get('connection_id')

        # Create measure config for ValueRangeObserver (reuse existing logic) 
        measure_config = {
            "measure": {
                "id": measure_id,
                "name": measure_name,
                "attribute_id": attribute_id,
                "task_id": task_id
            },
            "asset": config.get("asset", {}),
            "asset_id": config.get("asset_id"),
            "window": drift_days,
            "drift_type": drift_type,
            "connection_id": connection_id,
            "core_connection_id": config.get("core_connection_id"),
            "postgres_connection": config.get("postgres_connection"),
            "postgres_connection_id": config.get("postgres_connection_id"),
            "queue_id": config.get("queue_id"),
            "job_id": config.get("job_id"),
            "existing_connection": connection
        }

        # Use existing process_historical_values function (unchanged)
        historical_lower, historical_upper, _ = process_historical_values(
            measure_config,
            connection=connection,
            cursor=None,
            method='window',
            window_size=drift_days,
            include_margin=True,
            exclude_latest=True
        )

        log_info(f"VALUERANGE_ANALYSIS: {measure_name} historical analysis: ({historical_lower}, {historical_upper})")
        return historical_lower, historical_upper

    except Exception as e:
        log_error(f"Error extracting ValueRangeObserver thresholds for {measure_name}", e)
        return 0, 0


def process_range_measures_with_priority_framework(measure_data, config, connection):
    """
    NEW: Process range measures using priority framework while preserving existing functionality.
    Falls back to existing logic if priority framework fails.
    """

    #range measures
    try:
        measure_name = measure_data.get('measure_name')
        measure_id = measure_data.get('measure_id')
        attribute_id = measure_data.get('attribute_id')
        task_id = measure_data.get('task_id')
        asset_id = config.get('asset_id')
        run_id = config.get('queue_id')
        connection_id = config.get('connection_id')

        log_info(f"PRIORITY_FRAMEWORK: Processing {measure_name} with priority decision logic")

        # STEP 1: Get current run profile.py context
        profile_context = get_current_run_threshold_context(
            connection, measure_id, attribute_id, run_id
        )

        # STEP 2: Get data analysis (ValueRangeObserver) - ALWAYS run this
        log_info(f"DATA_ANALYSIS: Running ValueRangeObserver for {measure_name}")
        data_thresholds = extract_valuerange_thresholds(measure_data, config, connection)
        
        # STEP 3: Apply priority decision framework
        priority_decision = evaluate_threshold_priority(
            profile_context, data_thresholds, measure_name,
            connection, measure_id, attribute_id
        )

        baseline_lower = priority_decision['chosen_lower']
        baseline_upper = priority_decision['chosen_upper']
        
        log_info(f"PRIORITY_DECISION: {measure_name} - {priority_decision['priority_level']} - "
                f"Chosen: ({baseline_lower}, {baseline_upper}) - {priority_decision['reasoning']}")

        # STEP 4: Check previous run for contamination
        previous_lower, previous_upper = get_previous_run_thresholds(
            connection, measure_id, attribute_id, run_id
        )

        contamination_detected = False
        final_lower, final_upper = baseline_lower, baseline_upper

        if previous_lower is not None and previous_upper is not None:
            contamination_detected = detect_contaminated_previous_run(
                previous_lower, previous_upper, baseline_lower, baseline_upper, measure_name
            )
            
            if not contamination_detected and not priority_decision.get('has_user_feedback', False):
                # Apply light blending only if no contamination and no user feedback
                blend_factor = 0.05  # Very light blending (5% previous, 95% baseline)
                final_lower = baseline_lower + blend_factor * (previous_lower - baseline_lower)
                final_upper = baseline_upper + blend_factor * (previous_upper - baseline_upper)
                log_info(f"LIGHT_BLEND: Applied minimal blending: "
                        f"({baseline_lower}, {baseline_upper}) → ({final_lower}, {final_upper})")
            else:
                log_info(f"CONTAMINATION_OR_FEEDBACK: Skipping blending - contamination: {contamination_detected}, "
                        f"user_feedback: {priority_decision.get('has_user_feedback', False)}")

        # STEP 5: Apply conservative future adjustments (reuse existing feedback logic)
        feedback_history = get_context_aware_feedback_from_metrics(
            connection, measure_id, attribute_id, run_id, measure_name
        )

        enhanced_lower, enhanced_upper = final_lower, final_upper
        if feedback_history and not priority_decision.get('has_user_feedback', False):
            # Only apply additional enhancements if no user feedback was already incorporated
            conflict_analysis = analyze_context_aware_feedback_conflicts(feedback_history)
            stats = get_statistical_context_for_next_run(connection, measure_id, attribute_id)
            
            # Apply existing enhancement logic but with conservative parameters
            enhanced_lower, enhanced_upper = apply_context_aware_enhancement_next_run(
                final_lower, final_upper, conflict_analysis, stats, measure_name,
                connection, measure_id, attribute_id, run_id
            )

        # Convert to appropriate types for range measures
        if measure_name == 'length_range':
            enhanced_lower = max(0, math.floor(enhanced_lower))
            enhanced_upper = math.ceil(enhanced_upper)
        else:
            enhanced_lower = math.floor(enhanced_lower)
            enhanced_upper = math.ceil(enhanced_upper)

        # STEP 6: Store with enhanced metadata
        feedback_metadata = {
            'connection_id': connection_id,
            'priority_framework_used': True,
            'priority_level': priority_decision['priority_level'],
            'reasoning': priority_decision['reasoning'],
            'has_user_feedback': priority_decision.get('has_user_feedback', False),
            'signals_aligned': priority_decision.get('signals_aligned', False),
            'data_analysis_result': data_thresholds,
            'profile_py_result': (profile_context['lower_threshold'], profile_context['upper_threshold']) if profile_context else None,
            'contamination_detected': contamination_detected,
            'previous_run_ignored': contamination_detected,
            'baseline_thresholds': (baseline_lower, baseline_upper),
            'final_thresholds': (enhanced_lower, enhanced_upper)
        }

        store_enhanced_thresholds_with_metadata(
            connection, measure_id, attribute_id, asset_id, task_id,
            enhanced_lower, enhanced_upper, run_id, feedback_metadata, measure_name
        )

        return (enhanced_lower, enhanced_upper)

    except Exception as e:
        log_error(f"Error in priority framework processing for {measure_name}, falling back to existing logic", e)
        # Fallback to existing function
        return process_range_measures_with_context_aware_feedback(measure_data, config, connection)


def process_other_measures_with_priority_framework(measure_data, base_threshold, config, connection):
    """
    NEW: Process other measures using the same priority framework.
    Falls back to existing logic if priority framework fails.
    """
    try:
        measure_name = measure_data.get('measure_name')
        measure_id = measure_data.get('measure_id')
        attribute_id = measure_data.get('attribute_id')
        run_id = config.get('queue_id')

        # STEP 1: Get current run profile.py context
        profile_context = get_current_run_threshold_context(
            connection, measure_id, attribute_id, run_id
        )

        # STEP 2: Use UnivariateModel result as data analysis (base_threshold already calculated)
        data_thresholds = base_threshold
        
        # STEP 3: Apply priority framework
        priority_decision = evaluate_threshold_priority(
            profile_context, data_thresholds, measure_name,
            connection, measure_id, attribute_id
        )

        baseline_lower = priority_decision['chosen_lower']
        baseline_upper = priority_decision['chosen_upper']

        # STEP 4: Apply same contamination detection and minimal processing as range measures
        previous_lower, previous_upper = get_previous_run_thresholds_for_all_measures(
            connection, measure_id, attribute_id, run_id
        )

        contamination_detected = False
        final_lower, final_upper = baseline_lower, baseline_upper

        if previous_lower is not None and previous_upper is not None:
            contamination_detected = detect_contaminated_previous_run(
                previous_lower, previous_upper, baseline_lower, baseline_upper, measure_name
            )
            
            if not contamination_detected and not priority_decision.get('has_user_feedback', False):
                # Apply light blending for other measures too
                blend_factor = 0.05
                final_lower = baseline_lower + blend_factor * (previous_lower - baseline_lower)
                final_upper = baseline_upper + blend_factor * (previous_upper - baseline_upper)
                log_info(f"LIGHT_BLEND_OTHER: Applied minimal blending for {measure_name}")

        # STEP 5: Apply conservative enhancements (similar to existing logic)
        feedback_history = get_context_aware_feedback_from_metrics(
            connection, measure_id, attribute_id, run_id, measure_name
        )

        enhanced_lower, enhanced_upper = final_lower, final_upper
        if feedback_history and not priority_decision.get('has_user_feedback', False):
            conflict_analysis = analyze_context_aware_feedback_conflicts(feedback_history)
            stats = get_statistical_context_for_next_run(connection, measure_id, attribute_id)
            
            enhanced_lower, enhanced_upper = apply_context_aware_enhancement_next_run(
                final_lower, final_upper, conflict_analysis, stats, measure_name,
                connection, measure_id, attribute_id, run_id
            )
        connection_id = config.get('connection_id')
        # Store with metadata (reuse existing store function)
        feedback_metadata = {
            'connection_id': connection_id,
            'priority_framework_used': True,
            'priority_level': priority_decision['priority_level'],
            'has_user_feedback': priority_decision.get('has_user_feedback', False),
            'contamination_detected': contamination_detected
        }

        # Use existing store function
        store_enhanced_thresholds_with_metadata(
            connection, measure_id, attribute_id, config.get('asset_id'), measure_data.get('task_id'),
            enhanced_lower, enhanced_upper, run_id, feedback_metadata, measure_name
        )

        log_info(f"PRIORITY_FRAMEWORK_OTHER_FINAL: {measure_name} - "
                f"UnivariateModel: {base_threshold} → Priority: ({baseline_lower}, {baseline_upper}) → "
                f"Final: ({enhanced_lower}, {enhanced_upper})")

        return (enhanced_lower, enhanced_upper)

    except Exception as e:
        log_error(f"Error in priority framework for other measure {measure_name}, falling back to existing logic", e)
        # Fallback to existing function
        return process_other_measures_with_context_aware_feedback(measure_data, base_threshold, config, connection)


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

def calculate_time_based_confidence_decay(feedback_date, base_confidence=0.8, decay_rate=0.05, min_confidence=0.3):
    """
    NEW ENHANCEMENT: Calculate time-based confidence decay for feedback entries.
    
    Recent feedback gets higher confidence, older feedback gets lower confidence.
    This ensures the system prioritizes current user behavior patterns.
    
    Args:
        feedback_date: Timestamp of the feedback entry
        base_confidence (float): Starting confidence level (default 0.8)
        decay_rate (float): Daily decay rate (default 0.05 = 5% per day)
        min_confidence (float): Minimum confidence floor (default 0.3)
        
    Returns:
        float: Decayed confidence value between min_confidence and base_confidence
    """
    try:
        # Calculate days since feedback was given
        current_time = datetime.now(timezone.utc)
        if isinstance(feedback_date, str):
            # Parse string datetime
            feedback_time = datetime.fromisoformat(feedback_date.replace('Z', '+00:00'))
        else:
            # Handle datetime object
            feedback_time = feedback_date
            if feedback_time.tzinfo is None:
                feedback_time = feedback_time.replace(tzinfo=timezone.utc)
        
        days_old = (current_time - feedback_time).total_seconds() / (24 * 3600)
        days_old = max(0, days_old)  # Ensure non-negative
        
        # Apply exponential decay: confidence = base * (1 - decay_rate)^days_old
        decayed_confidence = base_confidence * ((1 - decay_rate) ** days_old)
        
        # Apply minimum confidence floor
        final_confidence = max(min_confidence, decayed_confidence)
        
        log_info(f"CONFIDENCE_DECAY: Feedback age: {days_old:.1f} days, "
                f"confidence: {base_confidence:.2f} -> {final_confidence:.2f}")
        
        return final_confidence
        
    except Exception as e:
        log_error(f"Error calculating confidence decay: {e}", e)
        # Fallback to base confidence
        return base_confidence


def get_context_aware_feedback_from_metrics(connection, measure_id, attribute_id, current_run_id=None, measure_name=None):
    """
    Get context-aware feedback history from existing core.metrics table for historical patterns.
    Now includes reset timestamp filtering to only get post-reset feedback.
    """
    try:
        # Get reset timestamp to filter feedback
        reset_timestamp = get_last_reset_timestamp(connection, measure_id, attribute_id)
        
        with connection.cursor() as cursor:
            # Build attribute_id condition conditionally
            attribute_condition = ""
            if attribute_id:
                attribute_condition = f"AND met.attribute_id = '{attribute_id}'"
            
            # Build query with optional reset timestamp filter
            reset_filter = ""
            if reset_timestamp:
                reset_filter = f"AND met.created_date > '{reset_timestamp}'"
                log_info(f"FEEDBACK_FILTERING: Using reset timestamp filter: {reset_timestamp}")
            
            query = f"""
                SELECT
                    met.value,
                    met.marked_as,
                    met.run_id,
                    met.created_date,
                    met.threshold,
                    met.drift_status,
                    ROW_NUMBER() OVER (PARTITION BY ROUND(met.value::numeric, 2) ORDER BY met.created_date DESC) as recency_rank
                FROM core.metrics met
                WHERE met.measure_id = '{measure_id}'
                {attribute_condition}
                AND met.marked_as IS NOT NULL
                AND met.value IS NOT NULL
                AND met.measure_name = '{measure_name}'
                AND met.drift_status IS NOT NULL
                {reset_filter}
                ORDER BY met.created_date DESC
                LIMIT 50
            """

            cursor = execute_query(connection, cursor, query)
            results = fetchall(cursor)

            feedback_history = []
            for row in results:
                try:
                    threshold_data = {}
                    if row.get('threshold'):
                        if isinstance(row['threshold'], str):
                            threshold_data = json.loads(row['threshold'])
                        else:
                            threshold_data = row['threshold']

                    feedback_history.append({
                        'value': round(float(row['value']), 2),
                        'marked_as': row['marked_as'],
                        'run_id': row['run_id'],
                        'created_date': row['created_date'],
                        'threshold': threshold_data,
                        'drift_status': row['drift_status'],
                        'recency_rank': row.get('recency_rank', 1)
                    })
                except (ValueError, TypeError, json.JSONDecodeError) as e:
                    log_error(f"Error parsing context-aware feedback row: {e}", e)
                    continue

            reset_info = f" (post-reset from {reset_timestamp})" if reset_timestamp else ""
            log_info(f"NEXT_RUN_FEEDBACK: Retrieved {len(feedback_history)} historical context-aware feedback entries for measure {measure_id}{reset_info}")
            return feedback_history

    except Exception as e:
        log_error("Error getting context-aware feedback from existing tables", e)
        return []


def detect_oscillation_patterns_context_aware(feedback_history):
    """
    Detect oscillation patterns using context-aware data and timestamps
    """
    try:
        if not feedback_history:
            return False

        value_patterns = {}

        # Group feedback by value
        for feedback in feedback_history:
            value = feedback['value']
            if value not in value_patterns:
                value_patterns[value] = []
            value_patterns[value].append(feedback)

        oscillation_detected = False

        for value, patterns in value_patterns.items():
            if len(patterns) >= 3:  # Need at least 3 markings to detect oscillation
                sorted_patterns = sorted(patterns, key=lambda x: x['created_date'])

                # Check for alternating pattern in context-aware feedback
                context_pairs = []
                for pattern in sorted_patterns:
                    drift_status = pattern.get('drift_status', '').lower()
                    marked_as = pattern.get('marked_as', '').lower()
                    context_pairs.append(f"{marked_as}_during_{drift_status}")

                # Look for conflicting context patterns
                alternations = sum(1 for i in range(len(context_pairs)-1) if context_pairs[i] != context_pairs[i+1])
                alternation_rate = alternations / (len(context_pairs) - 1) if len(context_pairs) > 1 else 0

                if alternation_rate > 0.6:  # More than 60% alternations
                    log_info(f"CONTEXT_OSCILLATION_DETECTED: Value {value} shows alternating context pattern: {context_pairs} "
                             f"(alternation_rate: {alternation_rate:.2f})")
                    oscillation_detected = True

                # Check for rapid flip-flopping in recent history
                recent_patterns = sorted_patterns[-3:]  # Last 3 markings
                if len(recent_patterns) >= 3:
                    recent_contexts = [f"{p.get('marked_as', '').lower()}_during_{p.get('drift_status', '').lower()}" for p in recent_patterns]
                    recent_unique = set(recent_contexts)
                    if len(recent_unique) > 1:
                        log_info(f"CONTEXT_OSCILLATION_DETECTED: Value {value} shows recent context flip-flopping: {recent_contexts}")
                        oscillation_detected = True

        return oscillation_detected

    except Exception as e:
        log_error("Error detecting context-aware oscillation patterns", e)
        return False


def analyze_context_aware_feedback_conflicts(feedback_history):
    """
    ENHANCED: Analyze context-aware feedback for conflicts and stability patterns for next run.
    Now includes time-based confidence decay for all feedback entries.
    """
    try:
        value_patterns = {}

        # Group feedback by value
        for feedback in feedback_history:
            value = feedback['value']
            if value not in value_patterns:
                value_patterns[value] = []
            value_patterns[value].append(feedback)

        conflict_analysis = {
            'expand_requests': [],    # User wants fewer alerts (marked normal during alert)
            'contract_requests': [],  # User wants more alerts (marked outlier during ok)
            'conflicted': [],
            'oscillation_detected': False
        }

        total_conflicts = 0

        for value, patterns in value_patterns.items():
            sorted_patterns = sorted(patterns, key=lambda x: x['created_date'], reverse=True)

            # Analyze each feedback for context with time-based confidence decay
            for feedback in sorted_patterns:
                drift_status = feedback.get('drift_status', '').lower()
                marked_as = feedback.get('marked_as', '').lower()
                feedback_date = feedback.get('created_date')
                
                # ENHANCED: Apply time-based confidence decay
                confidence = calculate_time_based_confidence_decay(
                    feedback_date, 
                    base_confidence=0.8,
                    decay_rate=0.05,  # 5% decay per day
                    min_confidence=0.3
                )

                # Context-aware interpretation for next run
                if drift_status in ['high', 'medium', 'low', 'alert'] and marked_as == 'normal':
                    # User says: "These thresholds are too strict" → EXPAND thresholds (fewer alerts)
                    conflict_analysis['expand_requests'].append({
                        'value': value,
                        'confidence': confidence,  # Time-decayed confidence
                        'feedback_data': feedback,
                        'reason': 'normal_during_alert_historical'
                    })

                elif drift_status == 'ok' and marked_as == 'outlier':
                    # User says: "These thresholds are too loose" → CONTRACT thresholds (more alerts)
                    conflict_analysis['contract_requests'].append({
                        'value': value,
                        'confidence': confidence,  # Time-decayed confidence
                        'feedback_data': feedback,
                        'reason': 'outlier_during_ok_historical'
                    })

                else:
                    # Other combinations are less clear or potentially conflicted
                    conflict_analysis['conflicted'].append({
                        'value': value,
                        'confidence': confidence * 0.5,  # Lower confidence for conflicted feedback
                        'feedback_data': feedback,
                        'reason': f'{marked_as}_during_{drift_status}_historical'
                    })
                    total_conflicts += 1

        # Set oscillation flag if conflicts detected or use dedicated oscillation detection
        oscillation_detected = detect_oscillation_patterns_context_aware(feedback_history)
        conflict_analysis['oscillation_detected'] = oscillation_detected or total_conflicts > 0

        log_info(f"NEXT_RUN_ANALYSIS: expand_requests={len(conflict_analysis['expand_requests'])}, "
                 f"contract_requests={len(conflict_analysis['contract_requests'])}, "
                 f"conflicted={len(conflict_analysis['conflicted'])}, "
                 f"oscillation_detected={conflict_analysis['oscillation_detected']}")

        return conflict_analysis

    except Exception as e:
        log_error("Error analyzing context-aware feedback conflicts", e)
        return {'expand_requests': [], 'contract_requests': [], 'conflicted': [], 'oscillation_detected': False}


def get_previous_run_thresholds_for_all_measures(connection, measure_id, attribute_id, current_run_id):
    """
    NEW ENHANCEMENT: Get the actual thresholds used in the previous run for any measure type.
    This extends the blending logic to non-range measures for consistent threshold evolution.
    
    Args:
        connection: Database connection object
        measure_id (str): ID of the measure
        attribute_id (str): ID of the attribute  
        current_run_id (str): Current run ID to exclude from search
        
    Returns:
        tuple: (previous_lower, previous_upper) or (None, None) if not found
    """
    try:
        with connection.cursor() as cursor:
            # Build attribute_id condition conditionally
            attribute_condition = ""
            if attribute_id:
                attribute_condition = f"AND attribute_id = '{attribute_id}'"
            
            # Get previous run ID first
            previous_run_query = f"""
                SELECT run_id FROM core.drift_threshold
                WHERE measure_id = '{measure_id}' 
                {attribute_condition}
                AND run_id != '{current_run_id}'
                ORDER BY created_date DESC
                LIMIT 1
            """
            cursor = execute_query(connection, cursor, previous_run_query)
            previous_run_result = fetchone(cursor)
            
            if not previous_run_result:
                log_info(f"PREVIOUS_THRESHOLDS_ALL: No previous run thresholds found for measure {measure_id}")
                return None, None
                
            previous_run_id = previous_run_result.get('run_id')
            log_info(f"PREVIOUS_THRESHOLDS_ALL: Found previous run ID: {previous_run_id} for measure {measure_id}")
            
            # Get actual thresholds from previous run
            threshold_query = f"""
                SELECT lower_threshold, upper_threshold 
                FROM core.drift_threshold
                WHERE measure_id = '{measure_id}'
                {attribute_condition}
                AND run_id = '{previous_run_id}'
            """
            cursor = execute_query(connection, cursor, threshold_query)
            threshold_result = fetchone(cursor)
            
            if threshold_result:
                previous_lower = float(threshold_result.get('lower_threshold', 0))
                previous_upper = float(threshold_result.get('upper_threshold', 0))
                log_info(f"PREVIOUS_THRESHOLDS_ALL: Found thresholds for measure {measure_id}: ({previous_lower}, {previous_upper})")
                return previous_lower, previous_upper
                
            log_info(f"PREVIOUS_THRESHOLDS_ALL: No threshold data found for previous run of measure {measure_id}")
            return None, None
            
    except Exception as e:
        log_error(f"Error getting previous run thresholds for measure {measure_id}", e)
        return None, None


def process_other_measures_with_context_aware_feedback(measure_data, base_threshold, config, connection):
    """
    CORRECTED: Process non-range measures with data-driven priority and selective oscillation dampening.
    
    CORRECTED LOGIC:
    - **DATA INTEGRITY > OSCILLATION CONTROL**: Trust UnivariateModel analysis and previous run context
    - **Dampening applies to**: Safety margins, fallback blending, final adjustments
    - **Light dampening for**: Historical data insights, previous run blending
    
    This extends the sophisticated threshold evolution logic to all other measure types 
    for consistency and stability while prioritizing data insights.
    
    Args:
        measure_data (dict): Dictionary containing measure information
        base_threshold (tuple): Base threshold from UnivariateModel (lower, upper)
        config (dict): Configuration dictionary
        connection: Database connection object
        
    Returns:
        tuple: (enhanced_lower, enhanced_upper) - Final enhanced threshold values with selective dampening
    """
    try:
        measure_name = measure_data.get('measure_name')
        measure_id = measure_data.get('measure_id')
        attribute_id = measure_data.get('attribute_id')
        task_id = measure_data.get('task_id')
        asset_id = config.get('asset_id')
        run_id = config.get('queue_id')
        connection_id = config.get('connection_id')

        # === OSCILLATION DETECTION ===
        oscillation_analysis = detect_threshold_oscillation(
            connection, measure_id, attribute_id, lookback_runs=5
        )

        # STEP 1: Use base threshold from UnivariateModel as historical insight
        historical_lower, historical_upper = base_threshold

        # STEP 2: Get previous run actual thresholds
        previous_lower, previous_upper = get_previous_run_thresholds_for_all_measures(
            connection, measure_id, attribute_id, run_id
        )
        
        if previous_lower is not None and previous_upper is not None:
            log_info(f"DATA_DRIVEN_OTHER_PROCESSING: {measure_name} previous run used: "
                    f"({previous_lower}, {previous_upper})")
        else:
            log_info(f"DATA_DRIVEN_OTHER_PROCESSING: {measure_name} no previous run thresholds found")

        # STEP 3: CORRECTED - Intelligent blending with LIGHT dampening
        log_info(f"DATA_DRIVEN_OTHER_PROCESSING: CORRECTED blending - prioritizing data insights")
        
        if previous_lower is not None and previous_upper is not None:
            # Get base blending result
            blend_lower, blend_upper = blend_historical_and_previous(
                historical_lower, historical_upper,
                previous_lower, previous_upper,
                measure_name
            )
            
            # Apply LIGHT dampening to blending adjustments when oscillating
            if oscillation_analysis['oscillating']:
                # Calculate blending dampening factor
                blend_factor = apply_dampening_factor(
                    0.5,  # 50% blend normally
                    oscillation_analysis['oscillation_rate'],
                    min_factor=0.7, max_factor=1.0  # Light dampening (70-100%)
                )
                
                # Re-blend with dampened factor
                blended_lower = previous_lower + blend_factor * (historical_lower - previous_lower)
                blended_upper = previous_upper + blend_factor * (historical_upper - previous_upper)
                
                log_info(f"DATA_DRIVEN_OTHER_PROCESSING: Applied light dampening to blending "
                        f"(factor: {blend_factor:.3f})")
            else:
                blended_lower, blended_upper = blend_lower, blend_upper
                
        else:
            # No previous run - use historical directly
            blended_lower, blended_upper = historical_lower, historical_upper
        
        log_info(f"DATA_DRIVEN_OTHER_PROCESSING: {measure_name} intelligent blending result: "
                f"({blended_lower}, {blended_upper})")

        # STEP 4: Apply context-aware feedback enhancement based on historical patterns
        feedback_history = get_context_aware_feedback_from_metrics(
            connection, measure_id, attribute_id, run_id, measure_name
        )

        enhanced_lower, enhanced_upper = blended_lower, blended_upper

        if feedback_history:
            log_info(f"DATA_DRIVEN_OTHER_PROCESSING: {measure_name} applying feedback enhancement "
                    f"on blended thresholds")
            conflict_analysis = analyze_context_aware_feedback_conflicts(feedback_history)
            stats = get_statistical_context_for_next_run(connection, measure_id, attribute_id)
            
            # CORRECTED: Pass oscillation analysis to enhancement function
            enhanced_lower, enhanced_upper = apply_context_aware_enhancement_next_run(
                blended_lower, blended_upper, conflict_analysis, stats, measure_name
            )
        else:
            log_info(f"DATA_DRIVEN_OTHER_PROCESSING: {measure_name} no feedback history found, "
                    f"using blended thresholds")

        # STEP 5: Store enhanced thresholds for next run
        feedback_metadata = {
            'connection_id': connection_id,
            'total_feedback': len(feedback_history),
            'expand_count': len(conflict_analysis.get('expand_requests', [])) if feedback_history else 0,
            'contract_count': len(conflict_analysis.get('contract_requests', [])) if feedback_history else 0,
            'conflict_count': len(conflict_analysis.get('conflicted', [])) if feedback_history else 0,
            'oscillation_detected': oscillation_analysis.get('oscillating', False),
            'oscillation_rate': oscillation_analysis.get('oscillation_rate', 0.0),
            'historical_context': (historical_lower, historical_upper),
            'previous_context': (previous_lower, previous_upper) if previous_lower is not None else None,
            'blended_context': (blended_lower, blended_upper),
            'blending_applied': (blended_lower != historical_lower or blended_upper != historical_upper),
            'dampening_applied': oscillation_analysis.get('oscillating', False)
        }

        store_enhanced_thresholds_with_metadata(
            connection, measure_id, attribute_id, asset_id, task_id,
            enhanced_lower, enhanced_upper, run_id,
            feedback_metadata, measure_name
        )

        oscillation_status = "OSCILLATING" if oscillation_analysis['oscillating'] else "STABLE"
        log_info(f"DATA_DRIVEN_OTHER_PROCESSING: FINAL {oscillation_status} enhanced thresholds for {measure_name}: "
                f"historical=({historical_lower}, {historical_upper}) -> "
                f"previous=({previous_lower}, {previous_upper}) -> "
                f"blended=({blended_lower}, {blended_upper}) -> "
                f"enhanced=({enhanced_lower}, {enhanced_upper}) "
                f"[oscillation_rate: {oscillation_analysis['oscillation_rate']:.3f}]")

        return (enhanced_lower, enhanced_upper)

    except Exception as e:
        log_error(f"Error processing other measure {measure_name} with data-driven feedback", e)
        return base_threshold  # Fallback to original base threshold


def get_statistical_context_for_next_run(connection, measure_id, attribute_id):
    """
    Get comprehensive statistical context for next run threshold calculations.
    Now includes reset timestamp filtering to only analyze post-reset data.
    """
    try:
        # Get reset timestamp to filter statistics
        reset_timestamp = get_last_reset_timestamp(connection, measure_id, attribute_id)
        
        with connection.cursor() as cursor:
            # Build attribute_id condition conditionally
            attribute_condition = ""
            if attribute_id:
                attribute_condition = f"AND attribute_id = '{attribute_id}'"
            
            # Build query with optional reset timestamp filter
            reset_filter = ""
            if reset_timestamp:
                reset_filter = f"AND created_date > '{reset_timestamp}'"
                log_info(f"STATS_FILTERING: Using reset timestamp filter: {reset_timestamp}")
            
            query = f"""
                SELECT value
                FROM core.metrics
                WHERE measure_id = '{measure_id}'
                {attribute_condition}
                AND value IS NOT NULL
                AND value != 'NaN'
                {reset_filter}
                ORDER BY created_date DESC
                LIMIT 200
            """

            cursor = execute_query(connection, cursor, query)
            results = fetchall(cursor)

            values = []
            for row in results:
                try:
                    value = float(row['value'])
                    if not math.isnan(value) and not math.isinf(value):
                        values.append(value)
                except (ValueError, TypeError):
                    continue

            if len(values) < 2:
                return {
                    'mean': 0, 'std_dev': 0, 'median': 0, 'q1': 0, 'q3': 0,
                    'iqr': 0, 'coefficient_of_variation': 0, 'sample_size': len(values),
                    'min_value': 0, 'max_value': 0
                }

            values_array = np.array(values)
            mean = np.mean(values_array)
            std_dev = np.std(values_array)
            median = np.median(values_array)
            q1 = np.percentile(values_array, 25)
            q3 = np.percentile(values_array, 75)
            iqr = q3 - q1
            cv = (std_dev / mean) if mean != 0 else 0

            context = {
                'mean': mean,
                'std_dev': std_dev,
                'median': median,
                'q1': q1,
                'q3': q3,
                'iqr': iqr,
                'coefficient_of_variation': abs(cv),
                'sample_size': len(values),
                'min_value': np.min(values_array),
                'max_value': np.max(values_array)
            }

            reset_info = f" (post-reset from {reset_timestamp})" if reset_timestamp else ""
            log_info(f"NEXT_RUN_STATS: mean={mean:.2f}, std_dev={std_dev:.2f}, cv={cv:.3f}, sample_size={len(values)}{reset_info}")
            return context

    except Exception as e:
        log_error("Error getting statistical context for next run", e)
        return {'mean': 0, 'std_dev': 0, 'median': 0, 'q1': 0, 'q3': 0, 'iqr': 0, 'coefficient_of_variation': 0, 'sample_size': 0}

def get_previous_run_thresholds(connection, measure_id, attribute_id, current_run_id):
    """
    Get the actual thresholds used in the previous run.
    
    Args:
        connection: Database connection object
        measure_id (str): ID of the measure
        attribute_id (str): ID of the attribute
        current_run_id (str): Current run ID to exclude from search
        
    Returns:
        tuple: (previous_lower, previous_upper) or (None, None) if not found
    """
    try:
        with connection.cursor() as cursor:
            # Get previous run ID first
            previous_run_query = f"""
                SELECT run_id FROM core.drift_threshold
                WHERE measure_id = '{measure_id}' 
                AND attribute_id = '{attribute_id}'
                AND run_id != '{current_run_id}'
                ORDER BY created_date DESC
                LIMIT 1
            """
            cursor = execute_query(connection, cursor, previous_run_query)
            previous_run_result = fetchone(cursor)
            
            if not previous_run_result:
                log_info("PREVIOUS_THRESHOLDS: No previous run thresholds found")
                return None, None
                
            previous_run_id = previous_run_result.get('run_id')
            log_info(f"PREVIOUS_THRESHOLDS: Found previous run ID: {previous_run_id}")
            
            # Get actual thresholds from previous run
            threshold_query = f"""
                SELECT lower_threshold, upper_threshold 
                FROM core.drift_threshold
                WHERE measure_id = '{measure_id}'
                AND attribute_id = '{attribute_id}' 
                AND run_id = '{previous_run_id}'
            """
            cursor = execute_query(connection, cursor, threshold_query)
            threshold_result = fetchone(cursor)
            
            if threshold_result:
                previous_lower = float(threshold_result.get('lower_threshold', 0))
                previous_upper = float(threshold_result.get('upper_threshold', 0))
                log_info(f"PREVIOUS_THRESHOLDS: Found thresholds: ({previous_lower}, {previous_upper})")
                return previous_lower, previous_upper
                
            log_info("PREVIOUS_THRESHOLDS: No threshold data found for previous run")
            return None, None
            
    except Exception as e:
        log_error("Error getting previous run thresholds", e)
        return None, None


def blend_historical_and_previous(historical_lower, historical_upper, 
                                 previous_lower, previous_upper, measure_name):
    """
    Intelligently blend historical prediction with previous run actual thresholds.
    
    Uses gap analysis to determine blending strategy:
    - Small gap: Trust previous run more (has learned from experience)
    - Medium gap: Equal blending
    - Large gap: Trust historical more (data patterns may have changed)
    
    Args:
        historical_lower (float): Lower threshold from historical data analysis
        historical_upper (float): Upper threshold from historical data analysis
        previous_lower (float): Lower threshold actually used in previous run
        previous_upper (float): Upper threshold actually used in previous run
        measure_name (str): Name of the measure for logging and type conversion
        
    Returns:
        tuple: (blended_lower, blended_upper) - Intelligently blended thresholds
    """
    try:
        # If no previous thresholds, use historical prediction
        if previous_lower is None or previous_upper is None:
            log_info(f"BLENDING: No previous thresholds for {measure_name}, "
                    f"using historical: ({historical_lower}, {historical_upper})")
            return historical_lower, historical_upper
        
        # Calculate ranges for comparison
        historical_range = historical_upper - historical_lower
        previous_range = previous_upper - previous_lower
        
        # Calculate gap analysis metrics
        if historical_range > 0:
            range_diff_percent = abs(historical_range - previous_range) / historical_range
        else:
            range_diff_percent = 0
            
        # Calculate center point differences
        historical_center = (historical_lower + historical_upper) / 2
        previous_center = (previous_lower + previous_upper) / 2
        center_diff = abs(historical_center - previous_center)
        
        log_info(f"BLENDING: {measure_name} analysis - Range difference: {range_diff_percent:.2%}, "
                f"Center difference: {center_diff:.2f}")
        log_info(f"BLENDING: Historical range: {historical_range:.2f}, "
                f"Previous range: {previous_range:.2f}")
        
        # Determine blending strategy based on gap analysis
        if range_diff_percent < 0.2:  # Small difference (< 20%)
            # Trust previous run more - it contains accumulated learning
            blend_factor = 0.2  # 20% historical, 80% previous
            strategy = "trust_previous"
            log_info(f"BLENDING: Small difference detected for {measure_name}, "
                    f"trusting previous run more (blend_factor: {blend_factor})")
        elif range_diff_percent < 0.5:  # Medium difference (20-50%)
            # Equal blending - balance between experience and fresh data
            blend_factor = 0.5  # 50% historical, 50% previous
            strategy = "equal_blend"
            log_info(f"BLENDING: Medium difference detected for {measure_name}, "
                    f"equal blending (blend_factor: {blend_factor})")
        else:  # Large difference (> 50%)
            # Trust historical more - data patterns may have significantly changed
            blend_factor = 0.7  # 70% historical, 30% previous
            strategy = "trust_historical"
            log_info(f"BLENDING: Large difference detected for {measure_name}, "
                    f"trusting historical more (blend_factor: {blend_factor})")
        
        # Apply blending formula: previous + factor * (historical - previous)
        blended_lower = previous_lower + blend_factor * (historical_lower - previous_lower)
        blended_upper = previous_upper + blend_factor * (historical_upper - previous_upper)
        
        log_info(f"BLENDING: {measure_name} raw blending result: "
                f"({blended_lower:.2f}, {blended_upper:.2f})")
        
        # Ensure valid bounds after blending
        if blended_lower >= blended_upper:
            log_info(f"BLENDING: Invalid bounds after blending {measure_name}, "
                    f"reverting to historical: ({historical_lower}, {historical_upper})")
            return historical_lower, historical_upper
            
        # Convert to appropriate types based on measure type
        if measure_name == 'length_range':
            # Length ranges should be non-negative integers
            blended_lower = max(0, math.floor(blended_lower))
            blended_upper = math.ceil(blended_upper)
        else:
            # Value ranges can be any number, round to integers
            blended_lower = math.floor(blended_lower)
            blended_upper = math.ceil(blended_upper)
        
        log_info(f"BLENDING: {measure_name} final blended thresholds: "
                f"({blended_lower}, {blended_upper}) using strategy '{strategy}'")
        
        return blended_lower, blended_upper
        
    except Exception as e:
        log_error(f"Error blending thresholds for {measure_name}", e)
        log_info(f"BLENDING: Error occurred, falling back to historical thresholds: "
                f"({historical_lower}, {historical_upper})")
        return historical_lower, historical_upper

def get_current_run_threshold_context(connection, measure_id, attribute_id, current_run_id):
    """
    Get current run's threshold context from core.metrics.threshold column.
    
    This provides awareness of what enhanced thresholds and metadata 
    are being saved in the current run.
    
    Args:
        connection: Database connection object
        measure_id (str): ID of the measure
        attribute_id (str): ID of the attribute  
        current_run_id (str): Current run ID
        
    Returns:
        dict: Current run threshold context with metadata
    """
    try:
        with connection.cursor() as cursor:
            # Build attribute_id condition conditionally
            attribute_condition = ""
            if attribute_id:
                attribute_condition = f"AND attribute_id = '{attribute_id}'"
            
            query = f"""
                SELECT 
                    threshold,
                    value,
                    drift_status,
                    marked_as,
                    created_date
                FROM core.metrics
                WHERE measure_id = '{measure_id}'
                {attribute_condition}
                AND run_id = '{current_run_id}'
                AND threshold IS NOT NULL
                ORDER BY created_date DESC
                LIMIT 1
            """
            cursor = execute_query(connection, cursor, query)
            result = fetchone(cursor)
            
            if not result:
                log_info(f"CURRENT_RUN_CONTEXT: No current run threshold found for measure {measure_id}")
                return None
                
            # Parse threshold JSON
            threshold_data = {}
            if result.get('threshold'):
                if isinstance(result['threshold'], str):
                    threshold_data = json.loads(result['threshold'])
                else:
                    threshold_data = result['threshold']
            
            current_context = {
                'lower_threshold': float(threshold_data.get('lower_threshold', 0)),
                'upper_threshold': float(threshold_data.get('upper_threshold', 0)),
                'feedback_enhanced': threshold_data.get('feedback_enhanced', False),
                'feedback_metadata': threshold_data.get('feedback_metadata', {}),
                'calculation_metadata': threshold_data.get('calculation_metadata', {}),
                'current_value': float(result.get('value', 0)),
                'current_drift_status': result.get('drift_status', ''),
                'current_marked_as': result.get('marked_as', ''),
                'created_date': result.get('created_date'),
                'oscillation_applied': threshold_data.get('feedback_metadata', {}).get('oscillation_detected', False),
                'enhancement_type': threshold_data.get('feedback_metadata', {}).get('enhancement_type', 'baseline')
            }
            
            log_info(f"CURRENT_RUN_CONTEXT: Found current run context - "
                    f"thresholds: ({current_context['lower_threshold']}, {current_context['upper_threshold']}), "
                    f"enhanced: {current_context['feedback_enhanced']}, "
                    f"type: {current_context['enhancement_type']}")
            
            return current_context
            
    except Exception as e:
        log_error(f"Error getting current run threshold context: {e}", e)
        return None    


def apply_context_aware_enhancement_next_run(current_lower, current_upper, conflict_analysis, stats, measure_name, connection=None, measure_id=None, attribute_id=None, current_run_id=None):
    """
    CURRENT RUN AWARE: Apply context-aware enhancement with full awareness of current run context.
    
    NOW INCLUDES:
    - Current run threshold metadata awareness
    - Enhancement continuity between runs
    - Oscillation state preservation
    - Feedback pattern consistency
    - Random contraction (5-7% instead of fixed 10%)
    - FIXED: Proper range vs count measure handling
    
    FIXED RANGE MEASURE LOGIC:
    - Range measures: Percentage-based expansion (NOT value-based)
    - Count measures: Value-based expansion (accommodation-based)
    - Current run context: Knows what's being saved right now
    
    Args:
        current_lower (float): Current lower threshold value (blended)
        current_upper (float): Current upper threshold value (blended)
        conflict_analysis (dict): Analysis containing expand/contract requests with threshold context
        stats (dict): Statistical context from historical data
        measure_name (str): Name of the measure for type-specific processing
        connection: Database connection object (for current run awareness)
        measure_id (str): Measure ID for current run lookup
        attribute_id (str): Attribute ID for current run lookup
        current_run_id (str): Current run ID for context lookup
        
    Returns:
        tuple: (enhanced_lower, enhanced_upper) - Enhanced threshold values with current run awareness
    """
    try:
        import random  # For random contraction percentage
        
        enhanced_lower = float(current_lower)
        enhanced_upper = float(current_upper)
        original_lower = enhanced_lower
        original_upper = enhanced_upper

        expand_requests = conflict_analysis.get('expand_requests', [])
        contract_requests = conflict_analysis.get('contract_requests', [])

        log_info(f"CURRENT_RUN_AWARE: Processing {len(expand_requests)} expand and "
                 f"{len(contract_requests)} contract requests for {measure_name}")

        # === CURRENT RUN CONTEXT AWARENESS ===
        current_run_context = None
        if connection and measure_id and attribute_id and current_run_id:
            current_run_context = get_current_run_threshold_context(
                connection, measure_id, attribute_id, current_run_id
            )
            
            if current_run_context:
                log_info(f"CURRENT_RUN_AWARE: Current run context - "
                        f"enhanced: {current_run_context['feedback_enhanced']}, "
                        f"type: {current_run_context['enhancement_type']}, "
                        f"oscillation: {current_run_context['oscillation_applied']}")
                
                # Use current run's enhanced thresholds as starting point if available
                if current_run_context['feedback_enhanced']:
                    log_info(f"CURRENT_RUN_AWARE: Using current run's enhanced thresholds as base")
                    enhanced_lower = current_run_context['lower_threshold']
                    enhanced_upper = current_run_context['upper_threshold']

        # Determine measure type and calculate current range
        is_range_measure = measure_name in ['length_range', 'value_range']
        current_range = enhanced_upper - enhanced_lower
        
        log_info(f"CURRENT_RUN_AWARE: {measure_name} - Type: {'RANGE' if is_range_measure else 'COUNT'}, "
                f"Current bounds: ({enhanced_lower}, {enhanced_upper}), Range: {current_range}")

        # === CURRENT RUN AWARE CONFIDENCE FILTERING ===
        base_expand_confidence = 0.6
        base_contract_confidence = 0.7
        
        # Adjust confidence based on current run context
        if current_run_context and current_run_context['oscillation_applied']:
            base_expand_confidence = 0.8  # Higher confidence needed if current run had oscillation
            base_contract_confidence = 0.9
            log_info(f"CURRENT_RUN_AWARE: Adjusted confidence thresholds due to current run oscillation")
        
        # Apply current run enhancement history
        if current_run_context and current_run_context['feedback_enhanced']:
            enhancement_type = current_run_context['enhancement_type']
            if 'expansion' in enhancement_type.lower():
                base_contract_confidence = 0.8  # Be more careful about contracting after expansion
                log_info(f"CURRENT_RUN_AWARE: Increased contract confidence due to recent expansion")
            elif 'contraction' in enhancement_type.lower():
                base_expand_confidence = 0.8  # Be more careful about expanding after contraction
                log_info(f"CURRENT_RUN_AWARE: Increased expand confidence due to recent contraction")

        # === FIXED EXPANSION LOGIC: RANGE vs COUNT MEASURES ===
        if expand_requests:
            high_confidence_expands = [req for req in expand_requests if req.get('confidence', 0) >= base_expand_confidence]
            if high_confidence_expands:
                log_info(f"CURRENT_RUN_AWARE: Applying expansion for "
                         f"{len(high_confidence_expands)} high-confidence requests")
                
                # Check if current run already applied expansion
                expansion_dampening = 1.0
                if current_run_context and current_run_context['feedback_enhanced']:
                    if 'expansion' in current_run_context['enhancement_type'].lower():
                        expansion_dampening = 0.5  # Reduce expansion if current run already expanded
                        log_info(f"CURRENT_RUN_AWARE: Dampening expansion due to current run expansion")
                
                # === RANGE MEASURES: PERCENTAGE-BASED EXPANSION ===
                if is_range_measure:
                    log_info(f"RANGE_MEASURE_FIX: Using percentage-based expansion for {measure_name}")
                    
                    # For range measures, user feedback means "thresholds were too restrictive"
                    # Apply percentage-based expansion, NOT value-based
                    base_expansion_percentage = random.uniform(0.05, 0.08)  # Random 5-8%
                    expansion_percentage = base_expansion_percentage * expansion_dampening
                    
                    # Apply percentage expansion to both bounds
                    enhanced_lower = enhanced_lower * (1 - expansion_percentage)
                    enhanced_upper = enhanced_upper * (1 + expansion_percentage)
                    
                    # For length_range, ensure non-negative
                    if measure_name == 'length_range':
                        enhanced_lower = max(0, enhanced_lower)
                    
                    log_info(f"RANGE_MEASURE_FIX: Applied {expansion_percentage*100:.1f}% expansion "
                           f"(base: {base_expansion_percentage*100:.1f}%): "
                           f"({original_lower}, {original_upper}) -> ({enhanced_lower:.2f}, {enhanced_upper:.2f})")
                    
                    # Log the feedback values for context (but don't use them for expansion)
                    feedback_values = [float(req['value']) for req in high_confidence_expands]
                    log_info(f"RANGE_MEASURE_FIX: User marked values {feedback_values} as normal - "
                           f"interpreted as 'range thresholds too restrictive'")
                
                # === COUNT MEASURES: VALUE-BASED EXPANSION with UnivariateModel Awareness ===
                else:
                    log_info(f"COUNT_MEASURE: Using value-based expansion for {measure_name}")
                    
                    for req in high_confidence_expands:
                        feedback_value = float(req['value'])
                        
                        # FIXED: Compare against current UnivariateModel thresholds, not previous enhanced thresholds
                        # Get current UnivariateModel result for comparison
                        current_univariate_lower = enhanced_lower  # This is the current UnivariateModel result
                        current_univariate_upper = enhanced_upper  # This is the current UnivariateModel result
                        
                        log_info(f"COUNT_MEASURE: User marked {feedback_value} as normal. "
                               f"Current UnivariateModel: ({current_univariate_lower}, {current_univariate_upper})")
                        
                        # Check if feedback value is outside current UnivariateModel thresholds
                        if feedback_value < current_univariate_lower:
                            # User says this below-UnivariateModel value is normal
                            safety_margin = max(1, min(5, abs(feedback_value) * 0.05)) * expansion_dampening
                            enhanced_lower = min(feedback_value - safety_margin, enhanced_lower)
                            log_info(f"COUNT_MEASURE: Expanded lower threshold to accommodate feedback: {enhanced_lower}")
                            
                        elif feedback_value > current_univariate_upper:
                            # User says this above-UnivariateModel value is normal
                            safety_margin = max(1, min(5, abs(feedback_value) * 0.05)) * expansion_dampening
                            enhanced_upper = max(feedback_value + safety_margin, enhanced_upper)
                            log_info(f"COUNT_MEASURE: Expanded upper threshold to accommodate feedback: {enhanced_upper}")
                            
                        else:
                            # Feedback value is within current UnivariateModel thresholds - no expansion needed
                            log_info(f"COUNT_MEASURE: Feedback value {feedback_value} is within current UnivariateModel "
                                   f"thresholds ({current_univariate_lower}, {current_univariate_upper}) - no expansion needed")

        # === COMPLETE CONTRACTION FIX with UnivariateModel Awareness ===
        if contract_requests:
            high_confidence_contracts = [req for req in contract_requests if req.get('confidence', 0) >= base_contract_confidence]
            if high_confidence_contracts:
                log_info(f"CURRENT_RUN_AWARE: Applying COMPLETE contraction fix with UnivariateModel awareness")
                
                # Check if current run already applied contraction
                contraction_dampening = 1.0
                if current_run_context and current_run_context['feedback_enhanced']:
                    if 'contraction' in current_run_context['enhancement_type'].lower():
                        contraction_dampening = 0.7  # Reduce contraction if current run already contracted
                        log_info(f"CURRENT_RUN_AWARE: Dampening contraction due to current run contraction")
                
                # === VALUE-BASED CONTRACTION FOR OTHER MEASURES ===
                if not is_range_measure:
                    log_info(f"COUNT_MEASURE_CONTRACTION: Using value-based contraction for {measure_name}")
                    
                    for req in high_confidence_contracts:
                        feedback_value = float(req['value'])
                        
                        # FIXED: Compare against current UnivariateModel thresholds
                        current_univariate_lower = enhanced_lower  # Current UnivariateModel result
                        current_univariate_upper = enhanced_upper  # Current UnivariateModel result
                        
                        log_info(f"COUNT_MEASURE_CONTRACTION: User marked {feedback_value} as outlier. "
                               f"Current UnivariateModel: ({current_univariate_lower}, {current_univariate_upper})")
                        
                        # Check if feedback value is within current UnivariateModel thresholds
                        if current_univariate_lower <= feedback_value <= current_univariate_upper:
                            # User says this within-UnivariateModel value is an outlier
                            safety_margin = max(1, min(5, abs(feedback_value) * 0.05)) * contraction_dampening
                            
                            if feedback_value < (current_univariate_lower + current_univariate_upper) / 2:
                                # Value is in lower half - contract upper bound
                                enhanced_upper = min(feedback_value - safety_margin, enhanced_upper)
                                log_info(f"COUNT_MEASURE_CONTRACTION: Contracted upper threshold to exclude outlier: {enhanced_upper}")
                            else:
                                # Value is in upper half - contract lower bound
                                enhanced_lower = max(feedback_value + safety_margin, enhanced_lower)
                                log_info(f"COUNT_MEASURE_CONTRACTION: Contracted lower threshold to exclude outlier: {enhanced_lower}")
                        else:
                            # Feedback value is already outside UnivariateModel thresholds - no contraction needed
                            log_info(f"COUNT_MEASURE_CONTRACTION: Feedback value {feedback_value} is already outside "
                                   f"UnivariateModel thresholds ({current_univariate_lower}, {current_univariate_upper}) - no contraction needed")
                
                # === RANGE MEASURES: PERCENTAGE-BASED CONTRACTION ===
                else:
                    # Detect if this is a pattern measure
                    is_pattern_measure = measure_name.lower() in ['long_pattern', 'short_pattern', 'enum'] or \
                                       'pattern' in measure_name.lower() or 'enum' in measure_name.lower()
                    
                    if is_pattern_measure and enhanced_lower == enhanced_upper:
                        # PATTERN MEASURE EQUAL BOUNDS - Special handling
                        log_info(f"CURRENT_RUN_AWARE: Pattern measure equal bounds contraction")
                        primary_request = max(high_confidence_contracts, key=lambda x: x.get('confidence', 0))
                        outlier_value = float(primary_request['value'])
                        
                        if outlier_value == enhanced_lower:
                            # User marked the exact threshold value as outlier - shift away
                            shift_amount = max(1, int(contraction_dampening))  # At least 1, dampened
                            
                            if enhanced_lower >= 2:
                                # Shift down to exclude the outlier
                                enhanced_lower = enhanced_lower - shift_amount
                                enhanced_upper = enhanced_upper - shift_amount
                                log_info(f"CURRENT_RUN_AWARE: Pattern shifted down by {shift_amount} to exclude outlier: "
                                       f"({enhanced_lower}, {enhanced_upper})")
                            else:
                                # Shift up to exclude the outlier
                                enhanced_lower = enhanced_lower + shift_amount
                                enhanced_upper = enhanced_upper + shift_amount
                                log_info(f"CURRENT_RUN_AWARE: Pattern shifted up by {shift_amount} to exclude outlier: "
                                       f"({enhanced_lower}, {enhanced_upper})")
                        else:
                            # Outlier not exact match - use random percentage contraction (5-7%)
                            base_contraction_percent = random.uniform(0.05, 0.07)  # Random 5-7%
                            contraction_percent = base_contraction_percent * contraction_dampening
                            enhanced_lower = max(0, enhanced_lower * (1 - contraction_percent))
                            enhanced_upper = enhanced_upper * (1 - contraction_percent)
                            log_info(f"CURRENT_RUN_AWARE: Pattern random percentage contraction applied: {contraction_percent*100:.1f}% (base: {base_contraction_percent*100:.1f}%)")
                    
                    else:
                        # REGULAR RANGE MEASURES - Random percentage contraction (5-7%)
                        log_info(f"CURRENT_RUN_AWARE: Regular range measure random percentage contraction")
                        base_contraction_percent = random.uniform(0.05, 0.07)  # Random 5-7%
                        contraction_percent = base_contraction_percent * contraction_dampening
                        
                        # Apply percentage reduction to both bounds
                        enhanced_lower = enhanced_lower * (1 - contraction_percent)
                        enhanced_upper = enhanced_upper * (1 - contraction_percent)
                        
                        # For length_range, ensure non-negative
                        if measure_name == 'length_range':
                            enhanced_lower = max(0, enhanced_lower)
                        
                        log_info(f"CURRENT_RUN_AWARE: Applied {contraction_percent*100:.1f}% contraction (base: {base_contraction_percent*100:.1f}%): "
                               f"({original_lower}, {original_upper}) -> ({enhanced_lower:.1f}, {enhanced_upper:.1f})")

        # ENHANCED: Robust final validation
        if enhanced_lower >= enhanced_upper:
            log_info(f"CURRENT_RUN_AWARE: Invalid bounds detected, applying emergency fix")
            # Use current run context for emergency fix if available
            if current_run_context:
                enhanced_lower = current_run_context['lower_threshold']
                enhanced_upper = current_run_context['upper_threshold']
                log_info(f"CURRENT_RUN_AWARE: Used current run thresholds for emergency fix")
            else:
                # Standard emergency fix
                if is_range_measure and measure_name == 'length_range':
                    enhanced_lower = max(0, math.floor((original_lower + original_upper) / 2))
                    enhanced_upper = enhanced_lower + 2
                else:
                    midpoint = (original_lower + original_upper) / 2
                    enhanced_lower = math.floor(midpoint)
                    enhanced_upper = enhanced_lower + 1

        # === FINAL INTEGER CONVERSION ===
        if measure_name == 'length_range':
            final_lower = max(0, math.floor(enhanced_lower))
            final_upper = max(final_lower, math.ceil(enhanced_upper))
        else:
            final_lower = math.floor(enhanced_lower)
            final_upper = max(final_lower, math.ceil(enhanced_upper))

        # Final validation
        if final_lower >= final_upper:
            log_info(f"CURRENT_RUN_AWARE: Invalid final bounds, applying emergency correction")
            if current_run_context:
                final_lower = int(current_run_context['lower_threshold'])
                final_upper = int(current_run_context['upper_threshold'])
                log_info(f"CURRENT_RUN_AWARE: Used current run context for final fix")
            else:
                if measure_name == 'length_range':
                    final_lower = max(0, int(original_lower))
                    final_upper = max(final_lower + 1, int(original_upper))
                else:
                    final_lower = int(original_lower)
                    final_upper = max(final_lower + 1, int(original_upper))

        # Log final results with current run context
        if final_lower != int(original_lower) or final_upper != int(original_upper):
            measure_type = "RANGE" if is_range_measure else "COUNT/FREQUENCY"
            current_run_info = ""
            if current_run_context:
                current_run_info = f" (current run: {current_run_context['enhancement_type']})"
            
            log_info(f"CURRENT_RUN_AWARE: FINAL enhanced {measure_type} thresholds for {measure_name}: "
                     f"({original_lower}, {original_upper}) -> ({final_lower}, {final_upper}){current_run_info}")
        else:
            log_info(f"CURRENT_RUN_AWARE: No changes applied for {measure_name}")

        return final_lower, final_upper

    except Exception as e:
        log_error(f"Error applying current run aware context enhancement for {measure_name}: {e}", e)
        return math.floor(current_lower), math.ceil(current_upper)


def detect_conflict_oscillation_pattern(expand_requests, contract_requests):
    """
    Detect oscillation patterns in expand/contract requests for measures without direct threshold history.
    """
    try:
        all_requests = expand_requests + contract_requests
        
        if len(all_requests) < 3:
            return 0.0
        
        # Group requests by value to detect conflicting patterns
        value_patterns = {}
        for req in all_requests:
            value = req.get('value', 0)
            if value not in value_patterns:
                value_patterns[value] = []
            value_patterns[value].append(req)
        
        # Detect conflicting patterns for the same values
        conflict_count = 0
        total_value_groups = len(value_patterns)
        
        for value, requests in value_patterns.items():
            if len(requests) > 1:
                # Check for conflicting request types for the same value
                request_types = set()
                for req in requests:
                    if req in expand_requests:
                        request_types.add('expand')
                    if req in contract_requests:
                        request_types.add('contract')
                
                if len(request_types) > 1:
                    conflict_count += 1
        
        # Calculate oscillation rate based on conflicts
        if total_value_groups > 0:
            conflict_rate = conflict_count / total_value_groups
        else:
            conflict_rate = 0.0
        
        # Additional factor: alternating request types in time order
        if len(all_requests) > 1:
            sorted_requests = sorted(all_requests, 
                                   key=lambda x: x.get('feedback_data', {}).get('created_date', ''), 
                                   reverse=False)
            
            alternations = 0
            for i in range(1, len(sorted_requests)):
                prev_type = 'expand' if sorted_requests[i-1] in expand_requests else 'contract'
                curr_type = 'expand' if sorted_requests[i] in expand_requests else 'contract'
                
                if prev_type != curr_type:
                    alternations += 1
            
            alternation_rate = alternations / (len(sorted_requests) - 1) if len(sorted_requests) > 1 else 0
        else:
            alternation_rate = 0.0
        
        # Combine factors
        oscillation_rate = (conflict_rate + alternation_rate) / 2
        
        log_info(f"CONFLICT_OSCILLATION: conflict_rate={conflict_rate:.3f}, "
                f"alternation_rate={alternation_rate:.3f}, "
                f"oscillation_rate={oscillation_rate:.3f}")
        
        return oscillation_rate
        
    except Exception as e:
        log_error(f"Error detecting conflict oscillation pattern: {e}", e)
        return 0.0

def store_enhanced_thresholds_with_metadata(connection, measure_id, attribute_id, asset_id, task_id,
                                            enhanced_lower, enhanced_upper, run_id,
                                            feedback_metadata, measure_name):
    """
    Store enhanced thresholds in the correct table (core.drift_threshold).
    This function NO LONGER modifies core.metrics.
    """
    try:
        with connection.cursor() as cursor:
            # Build attribute_id condition conditionally
            attribute_condition = ""
            if attribute_id:
                attribute_condition = f"AND attribute_id = '{attribute_id}'"
            
            # Update drift_threshold table with enhanced thresholds for the next run
            drift_update_query = f"""
                UPDATE core.drift_threshold
                SET
                    lower_threshold = {enhanced_lower},
                    upper_threshold = {enhanced_upper}
                WHERE measure_id = '{measure_id}'
                {attribute_condition}
                AND run_id = '{run_id}'
            """

            cursor = execute_query(connection, cursor, drift_update_query)

            if cursor.rowcount == 0:
                # Insert new record if update didn't affect any rows
                connection_id = feedback_metadata.get('connection_id', '')
                # Conditionally build columns and values for attribute_id
                columns = [
                    'id', 'connection_id', 'asset_id',
                    'measure_id', 'behavioral_key', 'measure_name',
                    'run_id', 'lower_threshold', 'upper_threshold', 'created_date'
                ]
                values = [
                    f"'{str(uuid4())}'", f"'{connection_id}'", f"'{asset_id}'",
                    f"'{measure_id}'", "''", f"'{measure_name}'",
                    f"'{run_id}'", f"{enhanced_lower}", f"{enhanced_upper}", 'CURRENT_TIMESTAMP'
                ]
                
                # Add task_id if it exists
                if task_id:
                    columns.insert(3, 'task_id')
                    values.insert(3, f"'{task_id}'")
                
                if attribute_id:
                    columns.insert(3, 'attribute_id')
                    values.insert(3, f"'{attribute_id}'")
                    
                insert_query = f"""
                    INSERT INTO core.drift_threshold
                    ({', '.join(columns)})
                    VALUES ({', '.join(values)})
                """
                cursor = execute_query(connection, cursor, insert_query)

            log_info(f"ENHANCED_STORAGE: Stored enhanced thresholds ({enhanced_lower}, {enhanced_upper}) "
                     f"for next run in drift_threshold for measure {measure_id}")

            return True

    except Exception as e:
        log_error("Error storing enhanced thresholds with metadata", e)
        return False


def process_range_measures_with_context_aware_feedback(measure_data, config, connection):
    """
    CORRECTED: Process range measures with data-driven priority and selective oscillation dampening.
    
    CORRECTED LOGIC:
    - **DATA INTEGRITY > OSCILLATION CONTROL**: Trust historical data analysis and previous run context
    - **Dampening applies to**: Safety margins, fallback blending, final adjustments
    - **Light dampening for**: Historical data insights, previous run blending
    
    Args:
        measure_data (dict): Dictionary containing measure information
        config (dict): Configuration dictionary
        connection: Database connection object
        
    Returns:
        tuple: (enhanced_lower, enhanced_upper) - Final enhanced threshold values with selective dampening
    """
    try:
        measure_name = measure_data.get('measure_name')
        measure_id = measure_data.get('measure_id')
        attribute_id = measure_data.get('attribute_id')
        task_id = measure_data.get('task_id')
        asset_id = config.get('asset_id')
        run_id = config.get('queue_id')
        connection_id = config.get('connection_id')
        drift_days = config.get('window', 30)
        drift_type = config.get('drift_type', 'runs')

        log_info(f"DATA_DRIVEN_RANGE_PROCESSING: Processing {measure_name} for measure_id={measure_id}")

        # === OSCILLATION DETECTION ===
        oscillation_analysis = detect_threshold_oscillation(
            connection, measure_id, attribute_id, lookback_runs=5
        )
        
        log_info(f"DATA_DRIVEN_RANGE_PROCESSING: {measure_name} oscillation: "
                f"oscillating={oscillation_analysis['oscillating']}, "
                f"rate={oscillation_analysis['oscillation_rate']:.3f}")

        # Get drift configuration to determine if auto or manual mode
        measure_thresholds = get_measure_threshold(config)
        drift_config = next(
            (threshold for threshold in measure_thresholds if threshold.get("id") == measure_id),
            None,
        )
        drift_config = drift_config if drift_config else {}
        is_auto = drift_config.get("is_auto", True) if drift_config else True

        log_info(f"DATA_DRIVEN_RANGE_PROCESSING: {measure_name} is_auto flag: {is_auto}")

        # STEP 1: Calculate base thresholds from historical data
        if not is_auto:
            # Manual mode - get thresholds from attribute table
            log_info(f"DATA_DRIVEN_RANGE_PROCESSING: Manual mode for {measure_name}")
            try:
                with connection.cursor() as cursor:
                    if measure_name == 'length_range':
                        attr_query = f"""
                            SELECT min_length, max_length
                            FROM core.attribute
                            WHERE id = '{attribute_id}'
                        """
                        cursor = execute_query(connection, cursor, attr_query)
                        attr_result = fetchone(cursor)

                        if attr_result:
                            historical_lower = float(attr_result.get("min_length", 0))
                            historical_upper = float(attr_result.get("max_length", 0))
                        else:
                            historical_lower, historical_upper = 0, 0
                            
                        log_info(f"DATA_DRIVEN_RANGE_PROCESSING: Manual {measure_name} using current "
                               f"length bounds: ({historical_lower}, {historical_upper})")

                    elif measure_name == 'value_range':
                        attr_query = f"""
                            SELECT min_value, max_value
                            FROM core.attribute
                            WHERE id = '{attribute_id}'
                        """
                        cursor = execute_query(connection, cursor, attr_query)
                        attr_result = fetchone(cursor)

                        if attr_result:
                            historical_lower = float(attr_result.get("min_value", 0))
                            historical_upper = float(attr_result.get("max_value", 0))
                        else:
                            historical_lower, historical_upper = 0, 0
                            
                        log_info(f"DATA_DRIVEN_RANGE_PROCESSING: Manual {measure_name} using current "
                               f"value bounds: ({historical_lower}, {historical_upper})")
                    else:
                        historical_lower, historical_upper = 0, 0

                log_info(f"DATA_DRIVEN_RANGE_PROCESSING: Manual {measure_name} base thresholds: "
                        f"({historical_lower}, {historical_upper})")

            except Exception as e:
                log_error(f"Error getting manual thresholds for {measure_name}", e)
                historical_lower, historical_upper = 0, 0
        else:
            # Auto mode - use ValueRangeObserver for historical insight
            log_info(f"DATA_DRIVEN_RANGE_PROCESSING: Auto mode for {measure_name}")

            measure_config = {
                "measure": {
                    "id": measure_id,
                    "name": measure_name,
                    "attribute_id": attribute_id,
                    "task_id": task_id
                },
                "asset": config.get("asset", {}),
                "asset_id": asset_id,
                "window": drift_days,
                "drift_type": drift_type,
                "connection_id": connection_id,
                "core_connection_id": config.get("core_connection_id"),
                "postgres_connection": config.get("postgres_connection"),
                "postgres_connection_id": config.get("postgres_connection_id"),
                "queue_id": config.get("queue_id"),
                "job_id": config.get("job_id"),
                "existing_connection": connection
            }

            try:
                historical_lower, historical_upper, _ = process_historical_values(
                    measure_config,
                    connection=connection,
                    cursor=None,
                    method='window',
                    window_size=drift_days,
                    include_margin=True,
                    exclude_latest=True
                )

                log_info(f"DATA_DRIVEN_RANGE_PROCESSING: Auto {measure_name} base thresholds: "
                        f"({historical_lower}, {historical_upper})")

            except Exception as e:
                log_error(f"Error in ValueRangeObserver for {measure_name}", e)
                historical_lower, historical_upper = 0, 0

        # STEP 2: Get previous run actual thresholds
        previous_lower, previous_upper = get_previous_run_thresholds(
            connection, measure_id, attribute_id, run_id
        )
        
        if previous_lower is not None and previous_upper is not None:
            log_info(f"DATA_DRIVEN_RANGE_PROCESSING: {measure_name} previous run used: "
                    f"({previous_lower}, {previous_upper})")
        else:
            log_info(f"DATA_DRIVEN_RANGE_PROCESSING: {measure_name} no previous run thresholds found")

        # STEP 3: CORRECTED - Intelligent blending with LIGHT dampening
        log_info(f"DATA_DRIVEN_RANGE_PROCESSING: CORRECTED blending - prioritizing data insights")
        
        if previous_lower is not None and previous_upper is not None:
            # Apply LIGHT dampening to blending (trust the data analysis)
            blend_lower, blend_upper = blend_historical_and_previous(
                historical_lower, historical_upper,
                previous_lower, previous_upper,
                measure_name
            )
            
            # Apply minimal dampening to blending adjustments
            if oscillation_analysis['oscillating']:
                # Slightly dampen the blending effect when oscillating
                blend_factor = apply_dampening_factor(
                    0.5,  # 50% blend normally
                    oscillation_analysis['oscillation_rate'],
                    min_factor=0.7, max_factor=1.0  # Light dampening (70-100%)
                )
                
                # Re-blend with dampened factor
                blended_lower = previous_lower + blend_factor * (historical_lower - previous_lower)
                blended_upper = previous_upper + blend_factor * (historical_upper - previous_upper)
                
                log_info(f"DATA_DRIVEN_RANGE_PROCESSING: Applied light dampening to blending "
                        f"(factor: {blend_factor:.3f})")
            else:
                blended_lower, blended_upper = blend_lower, blend_upper
                
        else:
            # No previous run - use historical directly
            blended_lower, blended_upper = historical_lower, historical_upper
        
        log_info(f"DATA_DRIVEN_RANGE_PROCESSING: {measure_name} intelligent blending result: "
                f"({blended_lower}, {blended_upper})")

        # STEP 4: Apply context-aware feedback enhancement based on historical patterns
        feedback_history = get_context_aware_feedback_from_metrics(
            connection, measure_id, attribute_id, run_id, measure_name
        )

        enhanced_lower, enhanced_upper = blended_lower, blended_upper

        if feedback_history:
            log_info(f"DATA_DRIVEN_RANGE_PROCESSING: {measure_name} applying feedback enhancement "
                    f"on blended thresholds")
            conflict_analysis = analyze_context_aware_feedback_conflicts(feedback_history)
            stats = get_statistical_context_for_next_run(connection, measure_id, attribute_id)
            
            # CORRECTED: Pass oscillation analysis to enhancement function
            enhanced_lower, enhanced_upper = apply_context_aware_enhancement_next_run(
                blended_lower, blended_upper, conflict_analysis, stats, measure_name
            )
        else:
            log_info(f"DATA_DRIVEN_RANGE_PROCESSING: {measure_name} no feedback history found, "
                    f"using blended thresholds")

        # STEP 5: Store enhanced thresholds for next run
        feedback_metadata = {
            'connection_id': connection_id,
            'total_feedback': len(feedback_history),
            'expand_count': len(conflict_analysis.get('expand_requests', [])) if feedback_history else 0,
            'contract_count': len(conflict_analysis.get('contract_requests', [])) if feedback_history else 0,
            'conflict_count': len(conflict_analysis.get('conflicted', [])) if feedback_history else 0,
            'oscillation_detected': oscillation_analysis.get('oscillating', False),
            'oscillation_rate': oscillation_analysis.get('oscillation_rate', 0.0),
            'historical_context': (historical_lower, historical_upper),
            'previous_context': (previous_lower, previous_upper) if previous_lower is not None else None,
            'blended_context': (blended_lower, blended_upper),
            'blending_applied': (blended_lower != historical_lower or blended_upper != historical_upper),
            'dampening_applied': oscillation_analysis.get('oscillating', False)
        }

        store_enhanced_thresholds_with_metadata(
            connection, measure_id, attribute_id, asset_id, task_id,
            enhanced_lower, enhanced_upper, run_id,
            feedback_metadata, measure_name
        )

        oscillation_status = "OSCILLATING" if oscillation_analysis['oscillating'] else "STABLE"
        log_info(f"DATA_DRIVEN_RANGE_PROCESSING: FINAL {oscillation_status} enhanced thresholds for {measure_name}: "
                f"historical=({historical_lower}, {historical_upper}) -> "
                f"previous=({previous_lower}, {previous_upper}) -> "
                f"blended=({blended_lower}, {blended_upper}) -> "
                f"enhanced=({enhanced_lower}, {enhanced_upper}) "
                f"[oscillation_rate: {oscillation_analysis['oscillation_rate']:.3f}]")

        return (enhanced_lower, enhanced_upper)

    except Exception as e:
        log_error(f"Error processing range measure {measure_name} with data-driven feedback", e)
        return (0, 0)


def update_threshold(config: dict, is_seasonal: bool = False,  **kwargs) -> list:
    """
    ENHANCED: update_threshold with context-aware feedback system for all measures
    
    Parameters:
        config (dict): Configuration dictionary containing asset_id, queue_id, drift_type, etc.
        is_seasonal (bool): Flag to indicate if the measure is seasonal (default: False)

    NEW ENHANCEMENTS:
    - Time-based confidence decay for all feedback
    - Blending logic extended to non-range measures  
    - Consistent threshold evolution across all measure types
    - Config-Driven Alert Reset System
    - PRIORITY FRAMEWORK: Intelligent decision making between Profile.py and data analysis
    """
    start_time = time.time()
    log_info(f"==== ENHANCED update_threshold function STARTED with config: asset_id={config.get('asset_id')}, "
             f"queue_id={config.get('queue_id')}, drift_type={config.get('drift_type')} ====")
    try:
        general_settings = get_general_settings(config)
        general_settings = general_settings if general_settings else {}
        drift_settings = general_settings.get("anomaly") if general_settings else {}
        drift_settings = drift_settings if drift_settings else {}
        drift_settings = (
            json.loads(drift_settings, default=str)
            if drift_settings and isinstance(drift_settings, str)
            else drift_settings
        )
        drift_settings = drift_settings if drift_settings else {}
        is_enabled = drift_settings.get("is_active")
        if not is_enabled:
            log_info(
                "Please enable observe under settings -> platfrom -> configuration -> observe to enable monitoring for the measures...!"
            )
            return

        drift_days = drift_settings.get("maximum", DRIFT_DAYS)
        drift_days = int(drift_days) if drift_days else DRIFT_DAYS
        drift_days = drift_days if drift_days > 0 else DRIFT_DAYS
        drift_type = drift_settings.get("type", "runs").lower()
        config.update(
            {
                "window": drift_days,
                "drift_type": drift_type,
            }
        )

        measure = config.get("measure")
        measure = measure if measure else {}
        category = measure.get("category")
        measure_drift_threshold = measure.get("drift_threshold", {}) if measure else {}
        measure_drift_threshold = (
            measure_drift_threshold if measure_drift_threshold else {}
        )
        measure_drift_threshold = (
            json.loads(measure_drift_threshold)
            if isinstance(measure_drift_threshold, str)
            else measure_drift_threshold
        )
        job_type = config.get("job_type")
        if job_type and (not is_seasonal):
            # Enable seasonality only for reliability and custom jobs
            is_seasonal = (job_type == RELIABILITY) or (job_type == CUSTOM)
        is_behavioral = (job_type == BEHAVIORAL) or (
            category and category == BEHAVIORAL
        )
        config.update({"is_behavioral": is_behavioral})
        connection_id = config.get("connection_id")
        asset_id = config.get("asset_id")
        run_id = config.get("queue_id")
        metrics_query, total_metrics = get_postgres_metrics(config, False)
        behavioral_metrics = get_metrics(config, False, is_behavioral)
        measure_thresholds = get_measure_threshold(config)
        if not metrics_query:
            return
        if not behavioral_metrics:
            return
        marked_values = get_marked_values(config, is_behavioral)
        connection = get_postgres_connection(config)

        with connection.cursor() as cursor:
            drift_threshold_values = []
            lower_threshold, upper_threshold = 0, 0
            # behavioral model
            if is_behavioral:
                metrics = behavioral_metrics
                for metric in metrics:
                    attribute_id = metric.get("attribute_id")
                    measure_id = metric.get("measure_id")
                    measure_name = metric.get("measure_name")
                    if job_type in [HEALTH, PROFILE]:
                        category = metric.get("category")

                    sorted_metric_values = list(metric.get("metric_values", []))
                    metric_values = get_timeseries_metrics(sorted_metric_values)
                    metric_values = list(metric_values)

                    marked_value = marked_values.get(measure_id)
                    behavioral_key = metric.get("key") if is_behavioral else ""
                    if is_behavioral:
                        marked_value = marked_values.get(behavioral_key)

                    marked_value = marked_value if marked_value else {}
                    normal_values = marked_value.get("normal_values", [])
                    outlier_values = marked_value.get("outlier_values", [])
                    metric_values = update_flag_threshold(
                        metric_values, normal_values, outlier_values
                    )

                    drift_config = next(
                        (
                            measure_threshold
                            for measure_threshold in measure_thresholds
                            if measure_threshold.get("id") == measure_id
                        ),
                        None,
                    )
                    drift_config = drift_config if drift_config else {}
                    is_auto = (
                        drift_config.get("is_auto", True) if drift_config else True
                    )

                    lower_threshold, upper_threshold = 0, 0
                    if is_auto:
                        if is_behavioral:
                            lower_threshold, upper_threshold = BehavioralModel().run(
                                metric_values
                            )

                        if (
                            len(metric_values) > 0
                            and (not lower_threshold)
                            and (not upper_threshold)
                        ):
                            threshold_value = metric_values[0].get("value")
                            threshold_value = (
                                float(threshold_value) if threshold_value else 0
                            )
                            lower_threshold = threshold_value
                            upper_threshold = threshold_value
                    else:
                        measure_threshold = drift_config.get("threshold", {})
                        measure_threshold = (
                            measure_threshold if measure_threshold else {}
                        )
                        lower_threshold = measure_threshold.get("lower_threshold", 0)
                        upper_threshold = measure_threshold.get("upper_threshold", 0)

                    lower_threshold = (
                        lower_threshold if not isnan(lower_threshold) else 0
                    )
                    upper_threshold = (
                        upper_threshold if not isnan(upper_threshold) else 0
                    )
                    lower_threshold = int(math.floor(lower_threshold))
                    upper_threshold = int(math.ceil(upper_threshold))

                    metrics = {
                        "threshold_metrics": metric_values if metric_values else []
                    }
                    # delete the drift threshold for the same run
                    attribute_query = (
                        f""" and attribute_id='{attribute_id}' """
                        if attribute_id
                        else ""
                    )
                    asset_query = f"and asset_id='{asset_id}'" if asset_id else ""
                    query_string = f"""
                        delete from core.drift_threshold
                        where measure_id='{measure_id}' {asset_query}
                        {attribute_query}
                        and run_id='{run_id}'
                    """
                    cursor = execute_query(connection, cursor, query_string)

                    drift_threshold = {
                        "lower_threshold": lower_threshold,
                        "upper_threshold": upper_threshold,
                    }
                    if is_behavioral:
                        behavioral_key = behavioral_key.replace("'", "")
                        measure_drift_threshold.update(
                            {behavioral_key: drift_threshold}
                        )
                    elif category and category.lower() in (
                        "length",
                        "enum",
                        "pattern",
                        "long_pattern",
                        "short_pattern",
                    ):
                        measure_name = measure_name.replace("'", "")
                        measure_drift_threshold.update({measure_name: drift_threshold})
                    else:
                        measure_drift_threshold = drift_threshold
                    drift_threshold_value = json.dumps(
                        measure_drift_threshold, default=str
                    )
                    attribute_condition = (
                        f" and attribute_id='{attribute_id}' " if attribute_id else ""
                    )
                    asset_condition = f" and asset_id='{asset_id}'" if asset_id else ""

                    query_string = f"""
                        update core.measure
                        set drift_threshold='{drift_threshold_value}'
                        where id='{measure_id}' {asset_condition} {attribute_condition}
                    """
                    cursor = execute_query(connection, cursor, query_string)

                    query_input = (
                        str(uuid4()),
                        connection_id,
                        asset_id,
                        attribute_id,
                        measure_name,
                        measure_id,
                        behavioral_key,
                        run_id,
                        float(lower_threshold) if lower_threshold else 0,
                        float(upper_threshold) if upper_threshold else 0,
                        json.dumps(metrics, default=str),
                    )
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals},CURRENT_TIMESTAMP)",
                        query_input,
                    ).decode("utf-8")
                    drift_threshold_values.append(query_param)

                drift_threshold_input = split_queries(drift_threshold_values)

                for input_values in drift_threshold_input:
                    try:
                        query_input = ",".join(input_values)
                        attribute_insert_query = f"""
                            insert into core.drift_threshold (id, connection_id, asset_id, attribute_id, measure_name,
                            measure_id, behavioral_key, run_id, lower_threshold, upper_threshold, threshold_metrics, created_date)
                            values {query_input}
                        """
                        cursor = execute_query(
                            connection, cursor, attribute_insert_query
                        )
                    except Exception as e:
                        log_error("update threshold: inserting new threshold", e)
                        raise e
            else:
                # ENHANCED: Univariate Model processing with context-aware feedback for ALL measures
                # PRIORITY FRAMEWORK: Intelligent decision making between Profile.py and data analysis
                queries = []
                default_limit = DEFAULT_CHUNK_LIMIT
                default_limit = 100
                if total_metrics > default_limit:
                    total_limit = int(total_metrics / default_limit) + (
                        1 if (total_metrics % default_limit) > 0 else 0
                    )
                    for i in range(total_limit):
                        offset = i * default_limit
                        offset = offset + 1 if offset > 0 else offset
                        limited_query = (
                            f"{metrics_query} offset {offset} limit {default_limit}"
                        )
                        queries.append(limited_query)
                else:
                    queries.append(metrics_query)

                for query in queries:
                    drift_threshold_values = []
                    df = pd.read_sql(query, con=connection)
                    if df.empty:
                        continue

                    # Clean up the metrics
                    df["metric_values"] = df["metric_values"].apply(
                        lambda x: x if isinstance(x, list) else []
                    )
                    df["metrics"] = cleanup_metrics(df["metric_values"].tolist())

                    # Filter out rows where the cleaned "metrics" are empty lists
                    df = df[df["metrics"].str.len() > 0]
                    if not list(df["metrics"]):
                        continue

                    try:
                        metrics_array = df["metrics"].tolist()

                        model = UnivariateModel()
                        thresholds = []
                        error_failure = []

                        # ENHANCED: Process each row in metrics_array with context-aware feedback and reset detection
                        for idx, row in enumerate(metrics_array):
                            try:
                                measure_name = df.iloc[idx]["measure_name"]
                                measure_id = df.iloc[idx]["measure_id"]
                                attribute_id = df.iloc[idx]["attribute_id"]
                                task_id = df.iloc[idx]["task_id"]

                                # Before processing measures, check if attribute_id exists
                                # if not attribute_id or attribute_id == "None":
                                #     log_info(f"Skipping {measure_name} - no attribute_id (likely asset-level measure)")
                                #     thresholds.append((0, 0))
                                #     error_failure.append(False)
                                #     continue

                                # ENHANCED: Detect and handle reset scenarios
                                array_rows = row.tolist() if isinstance(row, np.ndarray) else row
                                is_reset_run, metric_values_for_model = detect_and_handle_reset(
                                    config, connection, cursor, measure_id, attribute_id, 
                                    measure_name, array_rows
                                )

                                # =============================================================================
                                # PRIORITY FRAMEWORK INTEGRATION: Main changes are here
                                # =============================================================================
                                
                                # PRIORITY FRAMEWORK: Context-aware processing with priority decision logic
                                measure_name = measure_name.strip("'") if measure_name else ""
                                if measure_name in ['length_range', 'value_range']:
                                    feedback_count_query = f"""SELECT count(*) from core.metrics where measure_id = '{measure_id}' and measure_name = '{measure_name}' and
                                    marked_as is not null limit {drift_days}"""
                                    cursor = execute_query(connection,cursor,feedback_count_query)
                                    result = cursor.fetchone()
                                    count = result[0]
                                    if count > 0:
                                        # NEW: Use priority framework for range measures (with fallback to existing logic)
                                        log_info(f"PRIORITY_FRAMEWORK: Processing {measure_name} with "
                                                f"intelligent decision making between Profile.py and ValueRangeObserver")

                                        # Create measure data structure
                                        measure_data = {
                                            'measure_name': measure_name,
                                            'measure_id': measure_id,
                                            'attribute_id': attribute_id,
                                            'task_id': task_id
                                        }

                                        # Process with priority framework (with fallback to existing logic)
                                        threshold = process_range_measures_with_priority_framework(
                                            measure_data, config, connection
                                        )
                                    else:
                                        # Get drift configuration to check is_auto flag - SAME AS OTHER MEASURES
                                        drift_config = next(
                                            (measure_threshold for measure_threshold in measure_thresholds
                                            if measure_threshold.get("id") == measure_id), None,
                                        )
                                        drift_config = drift_config if drift_config else {}
                                        is_auto = drift_config.get("is_auto", True) if drift_config else True
                                        
                                        log_info(f"{measure_name} is_auto flag: {is_auto}")
                                    
                                        if not is_auto:
                                            # Manual mode - get thresholds from attribute table
                                            log_info(f"Manual mode for {measure_name}: getting thresholds from attribute table")
                                            
                                            try:
                                                if measure_name == 'length_range':
                                                    # Query for length-based thresholds from attribute table
                                                    attr_query = f"""
                                                        SELECT min_length, max_length 
                                                        FROM core.attribute 
                                                        WHERE id = '{attribute_id}'
                                                    """
                                                    cursor_temp = execute_query(connection, cursor, attr_query)
                                                    attr_result = fetchone(cursor_temp)
                                                    
                                                    if attr_result:
                                                        lower_threshold = float(attr_result.get("min_length", 0))
                                                        upper_threshold = float(attr_result.get("max_length", 0))
                                                        log_info(f"Manual {measure_name} thresholds from attribute table: lower={lower_threshold}, upper={upper_threshold}")
                                                    else:
                                                        log_info(f"No attribute data found for {measure_name}, using default thresholds")
                                                        lower_threshold, upper_threshold = 0, 0
                                                        
                                                elif measure_name == 'value_range':
                                                    # Query for value-based thresholds from attribute table
                                                    attr_query = f"""
                                                        SELECT min_value, max_value 
                                                        FROM core.attribute 
                                                        WHERE id = '{attribute_id}'
                                                    """
                                                    cursor_temp = execute_query(connection, cursor, attr_query)
                                                    attr_result = fetchone(cursor_temp)
                                                    
                                                    if attr_result:
                                                        lower_threshold = float(attr_result.get("min_value", 0))
                                                        upper_threshold = float(attr_result.get("max_value", 0))
                                                        log_info(f"Manual {measure_name} thresholds from attribute table: lower={lower_threshold}, upper={upper_threshold}")
                                                    else:
                                                        log_info(f"No attribute data found for {measure_name}, using default thresholds")
                                                        lower_threshold, upper_threshold = 0, 0
                                                        
                                            except Exception as e:
                                                log_error(f"Error fetching manual thresholds for {measure_name} from attribute table", e)
                                                lower_threshold, upper_threshold = 0, 0
                                            
                                            # UPDATE CORE.METRICS TABLE FOR MANUAL MODE (same as auto mode)
                                            try:
                                                threshold_json = json.dumps({
                                                    "lt_percent": 0,
                                                    "ut_percent": 0,
                                                    "lower_threshold": lower_threshold,
                                                    "upper_threshold": upper_threshold
                                                })
                                                
                                                # Update threshold column in metrics table for manual mode
                                                metrics_update_query = f"""
                                                    UPDATE core.metrics
                                                    SET threshold = '{threshold_json}'
                                                    WHERE connection_id = '{connection_id}'
                                                    AND asset_id = '{asset_id}'
                                                    AND attribute_id = '{attribute_id}'
                                                    AND measure_id = '{measure_id}'
                                                    AND run_id = '{run_id}'
                                                """
                                                cursor = execute_query(connection, cursor, metrics_update_query)
                                                log_info(f"Manual mode: Updated threshold column in metrics table: {threshold_json}")
                                                
                                                # If no rows were updated, try to find the specific record
                                                if cursor.rowcount == 0:
                                                    log_info("Manual mode: No records updated. Looking for specific metrics record...")
                                                    
                                                    # Try to find the specific metrics record
                                                    find_query = f"""
                                                        SELECT id FROM core.metrics
                                                        WHERE connection_id = '{connection_id}'
                                                        AND asset_id = '{asset_id}'
                                                        AND attribute_id = '{attribute_id}'
                                                        AND measure_id = '{measure_id}'
                                                        AND run_id = '{run_id}'
                                                        LIMIT 1
                                                    """
                                                    cursor = execute_query(connection, cursor, find_query)
                                                    record = cursor.fetchone()
                                                    
                                                    if record:
                                                        record_id = record[0]
                                                        # Update the specific record by id
                                                        specific_update = f"""
                                                            UPDATE core.metrics
                                                            SET threshold = '{threshold_json}'
                                                            WHERE id = '{record_id}'
                                                        """
                                                        cursor = execute_query(connection, cursor, specific_update)
                                                        log_info(f"Manual mode: Updated specific metrics record (id: {record_id}) with threshold: {threshold_json}")
                                                    else:
                                                        log_info("Manual mode: No existing metrics record found for this run")
                                            except Exception as e:
                                                log_error(f"Manual mode: Error updating metrics table threshold column", e)
                                                
                                            threshold = (lower_threshold, upper_threshold)
                                        else:
                                            # Auto mode - use existing ValueRangeObserver logic
                                            log_info(f"Auto mode for {measure_name}: using ValueRangeObserver")
                                            
                                            # Process with ValueRangeObserver
                                            measure_config = {
                                                "measure": {
                                                    "id": measure_id,
                                                    "name": measure_name,
                                                    "attribute_id": attribute_id
                                                },
                                                "asset": config.get("asset", {}),
                                                "asset_id": asset_id,
                                                "window": drift_days,
                                                "drift_type": drift_type,
                                                # Critical connection parameters
                                                "connection_id": connection_id,
                                                "core_connection_id": config.get("core_connection_id"),
                                                "postgres_connection": config.get("postgres_connection"),
                                                "postgres_connection_id": config.get("postgres_connection_id"),
                                                "queue_id": config.get("queue_id"),
                                                "job_id": config.get("job_id"),
                                                # Use the existing connection if possible
                                                "existing_connection": connection
                                            }
                                            
                                            try:
                                                # Process historical values using ValueRangeObserver
                                                log_info(f"Processing historical values for {measure_name}")
                                                # Update to receive is_first_run flag
                                                lower_threshold, upper_threshold, is_first_run = process_historical_values(
                                                    measure_config, 
                                                    connection=connection,
                                                    cursor=cursor,
                                                    method='window',
                                                    window_size=drift_days,
                                                    include_margin=True,
                                                    exclude_latest = True
                                                )
                                                
                                                log_info(f"ValueRangeObserver result for {measure_name}: "
                                                        f"lower_threshold={lower_threshold}, upper_threshold={upper_threshold}, is_first_run={is_first_run}")
                                                
                                                # Add metrics table update for first run
                                                if is_first_run:
                                                    log_info(f"First run detected - updating metrics table threshold column")
                                                    try:
                                                        # Create threshold JSON structure
                                                        threshold_json = json.dumps({
                                                            "lt_percent": 0,
                                                            "ut_percent": 0,
                                                            "lower_threshold": lower_threshold,
                                                            "upper_threshold": upper_threshold
                                                        })
                                                        
                                                        # Update threshold column in metrics table 
                                                        metrics_update_query = f"""
                                                            UPDATE core.metrics
                                                            SET threshold = '{threshold_json}'
                                                            WHERE connection_id = '{connection_id}'
                                                            AND asset_id = '{asset_id}'
                                                            AND attribute_id = '{attribute_id}'
                                                            AND measure_id = '{measure_id}'
                                                            AND run_id = '{run_id}'
                                                        """
                                                        cursor = execute_query(connection, cursor, metrics_update_query)
                                                        log_info(f"Updated threshold column in metrics table: {threshold_json}")
                                                        
                                                        # If no rows were updated, we may need to find the specific record
                                                        if cursor.rowcount == 0:
                                                            log_info("No records updated. Looking for specific metrics record...")
                                                            
                                                            # Try to find the specific metrics record
                                                            find_query = f"""
                                                                SELECT id FROM core.metrics
                                                                WHERE connection_id = '{connection_id}'
                                                                AND asset_id = '{asset_id}'
                                                                AND attribute_id = '{attribute_id}'
                                                                AND measure_id = '{measure_id}'
                                                                AND run_id = '{run_id}'
                                                                LIMIT 1
                                                            """
                                                            cursor = execute_query(connection, cursor, find_query)
                                                            record = cursor.fetchone()
                                                            
                                                            if record:
                                                                record_id = record[0]
                                                                # Update the specific record by id
                                                                specific_update = f"""
                                                                    UPDATE core.metrics
                                                                    SET threshold = '{threshold_json}'
                                                                    WHERE id = '{record_id}'
                                                                """
                                                                cursor = execute_query(connection, cursor, specific_update)
                                                                log_info(f"Updated specific metrics record (id: {record_id}) with threshold: {threshold_json}")
                                                            else:
                                                                log_info("No existing metrics record found for this run")
                                                    except Exception as e:
                                                        log_error(f"Error updating metrics table threshold column", e)
                                            except Exception as e:
                                                log_error(f"Error in ValueRangeObserver processing for {measure_name}", e)
                                                # Fallback to default thresholds
                                                lower_threshold, upper_threshold = 0, 0
                                                
                                        threshold = (lower_threshold, upper_threshold)
                                        
                                else:
                                    feedback_count_query = f"""SELECT count(*) from core.metrics where measure_id = '{measure_id}' and measure_name = '{measure_name}' and
                                    marked_as is not null limit {drift_days}"""
                                    cursor = execute_query(connection,cursor,feedback_count_query)
                                    result = cursor.fetchone()
                                    count = result[0]
                                    if count > 0:
                                        # NEW: Use priority framework for other measures (with fallback to existing logic)
                                        log_info(f"PRIORITY_FRAMEWORK: Processing other measure {measure_name} "
                                                f"with intelligent decision making between Profile.py and UnivariateModel")
                                        
                                        # Get base threshold from UnivariateModel using processed values
                                        # (which may be single value for reset runs)
                                        base_threshold = model.run(
                                            metric_values_for_model,
                                            job_type_seasonal=is_seasonal,
                                        )
                                        
                                        # Apply priority framework for non-range measures (with fallback to existing logic)
                                        measure_data = {
                                            'measure_name': measure_name,
                                            'measure_id': measure_id,
                                            'attribute_id': attribute_id,
                                            'task_id': task_id
                                        }
                                        
                                        threshold = process_other_measures_with_priority_framework(
                                            measure_data, base_threshold, config, connection
                                        )
                                        
                                        if is_reset_run:
                                            log_info(f"RESET: Enhanced threshold for {measure_name}: "
                                                    f"{base_threshold} -> {threshold}")
                                        else:
                                            log_info(f"PRIORITY_FRAMEWORK: Enhanced other measure "
                                                    f"{measure_name}: {base_threshold} -> {threshold}")
                                    else:
                                        array_rows = row.tolist() if isinstance(row, np.ndarray) else row
                                        
                                        # For reliability measures (is_seasonal=True), ensure proper threshold calculation
                                        # even when no feedback exists
                                        if is_seasonal:
                                            log_info(f"RELIABILITY_MEASURE: Processing {measure_name} with seasonal logic (no feedback)")
                                            # Use the same priority framework but with empty feedback
                                            measure_data = {
                                                'measure_name': measure_name,
                                                'measure_id': measure_id,
                                                'attribute_id': attribute_id,
                                                'task_id': task_id
                                            }
                                            
                                            # Get base threshold from UnivariateModel
                                            base_threshold = model.run(
                                                metric_values_for_model,
                                                job_type_seasonal=is_seasonal,
                                            )
                                            
                                            # Process with priority framework (will handle empty feedback gracefully)
                                            threshold = process_other_measures_with_priority_framework(
                                                measure_data, base_threshold, config, connection
                                            )
                                        else:
                                            # Non-reliability measures - use standard processing
                                            threshold = model.run(
                                                metric_values_for_model,
                                                job_type_seasonal=is_seasonal,
                                            )

                                # =============================================================================
                                # END PRIORITY FRAMEWORK INTEGRATION
                                # =============================================================================
                                thresholds.append(threshold)
                                error_failure.append(False)
                                
                            except Exception as e:
                                log_error(f"Error processing row - {row}: error", e)
                                thresholds.append((0, 0))
                                error_failure.append(True)
                                continue

                        # Add lower and upper thresholds to the DataFrame
                        df["lower_threshold"], df["upper_threshold"] = zip(*thresholds)

                        # Add the error_failure column to the DataFrame
                        df["error_failure"] = error_failure

                        behavioral_key = ""
                        # Process thresholds and store enhanced results
                        for (measure_id, measure_name), group in df.groupby(['measure_id', 'measure_name']):
                            first_match = group.iloc[0]
                            attribute_id = first_match["attribute_id"]
                            task_id = first_match["task_id"]
                            lower_threshold = first_match["lower_threshold"]
                            upper_threshold = first_match["upper_threshold"]
                            category = first_match["category"]
                            measure_name = first_match["measure_name"]
                            metric_values = first_match["metric_values"]

                            if category == "parameter":
                                previous_value_dict = get_previous_value(
                                    config, measure_id, is_alert=True, measure_name=measure_name, is_parameter=True
                                )

                                previous_threshold_dict = get_previous_value(
                                    config, measure_id, is_threshold=True, measure_name=measure_name, is_parameter=True)
                            else:
                                previous_value_dict = get_previous_value(
                                    config, measure_id, is_alert=True, measure_name=measure_name
                                )
                                previous_threshold_dict = get_previous_value(
                                    config, measure_id, is_threshold=True, measure_name=measure_name)

                            # Get the error failure flags as well
                            error_failure_flag = first_match["error_failure"]
                            if isinstance(error_failure_flag, list):
                                error_failure_flag = error_failure_flag[0]

                            # Adjust threshold tolerance based on previous value alert
                            try:
                                if previous_value_dict or previous_threshold_dict:
                                    current_value = metric_values[-1]  # the last item in the list is the current value

                                    previous_value = [
                                        float(value.get("value") or 0.0)
                                        for value in previous_value_dict
                                    ]
                                    try:
                                        # Check condition for both lower and upper thresholds
                                        if previous_threshold_dict:
                                            if (lower_threshold == 0 and upper_threshold == 0) or error_failure_flag:
                                                filtered_previous_alerts = next(
                                                    filter(
                                                        lambda d: not (
                                                            d["lower_threshold"] == 0
                                                            and d["upper_threshold"]
                                                            == 0
                                                        ),
                                                        previous_threshold_dict,
                                                    ),
                                                    previous_threshold_dict.pop(0)
                                                    if previous_threshold_dict
                                                    else None,
                                                )
                                                if filtered_previous_alerts:
                                                    lower_threshold = filtered_previous_alerts.get("lower_threshold", 0)
                                                    upper_threshold = filtered_previous_alerts.get("upper_threshold", 0)

                                    except Exception as e:
                                        lower_threshold = lower_threshold
                                        upper_threshold = upper_threshold

                                    # Skip tolerance adjustments for value_range and length_range (handled by priority framework)
                                    if measure_name not in ['value_range', 'length_range']:
                                        previous_value_alert = next((str(value.get("drift_status", "Ok")) for value in previous_value_dict), "Ok")
                                        #check if value is present in rolling 7 run/day window
                                        if (not previous_value_alert.lower() == "ok") and (current_value in previous_value):
                                            # exponential smoothing of threshold based on previous alert

                                            # Adaptive tolerance based on size of value
                                            if previous_value < 10:
                                                tolerance = random.uniform(0.05, 0.13)
                                            else:
                                                tolerance = random.uniform(0.01, 0.07)

                                            if previous_value < lower_threshold:
                                                lower_threshold = previous_value - (
                                                    previous_value * tolerance
                                                )
                                                lower_threshold = math.floor(lower_threshold)
                                            elif previous_value > upper_threshold:
                                                upper_threshold = previous_value + (
                                                    previous_value * tolerance
                                                )
                                                upper_threshold = math.ceil(upper_threshold)

                            except ValueError:
                                log_info(("Error processing previous value:", previous_value_dict))
                                # Handle ValueError if it occurs
                                lower_threshold = lower_threshold
                                upper_threshold = upper_threshold

                            # Delete the drift threshold for the same run
                            attribute_query = (
                                f" and attribute_id='{attribute_id}'"
                                if attribute_id
                                else ""
                            )
                            asset_query = (
                                f"and asset_id='{asset_id}'" if asset_id else ""
                            )
                            pipeline_task_query = f" and task_id='{task_id}'" if task_id else ""

                            measure_query = (
                                f" and measure_name='{measure_name}'" if category == "parameter" else ""
                            )
                            query_string = f"""
                                delete from core.drift_threshold
                                where measure_id='{measure_id}'
                                {asset_query} {attribute_query} {measure_query} {pipeline_task_query}
                                and run_id='{run_id}'
                            """
                            cursor = execute_query(connection, cursor, query_string)
                            try:
                                current_value_dict = get_current_run_value(
                                    config, measure_id, run_id
                                )
                                previous_value_dict = get_previous_value(
                                    config, measure_id, measure_name=measure_name
                                )
                                current_value_dict = current_value_dict or {}  # if current_value_dict is None, it will default to an empty dictionary {}
                                current_value = float(
                                    current_value_dict.get("value", 0.0)
                                )
                                if previous_value_dict:
                                    previous_value = float(
                                        previous_value_dict.get("value", 0.0)
                                    )
                                else:
                                    previous_value = float(0.0)

                                # Skip direct value adjustment for value_range and length_range measures (handled by priority framework)
                                if measure_name not in ['value_range', 'length_range']:
                                    if current_value == previous_value and not (
                                        lower_threshold <= current_value <= upper_threshold
                                    ):
                                        if current_value < lower_threshold:
                                            lower_threshold = current_value
                                        elif current_value > upper_threshold:
                                            upper_threshold = current_value

                            except ValueError:
                                # Handle ValueError if it occurs
                                lower_threshold = lower_threshold
                                upper_threshold = upper_threshold

                            drift_threshold = {
                                "lower_threshold": lower_threshold,
                                "upper_threshold": upper_threshold,
                            }

                            # Store the threshold data based on the measure_name conditions
                            if category and category.lower() in (
                                "length",
                                "enum",
                                "pattern",
                                "long_pattern",
                                "short_pattern",
                                "parameter",
                            ):
                                measure_name = measure_name.replace("'", "")
                                measure_drift_threshold.update(
                                    {measure_name: drift_threshold}
                                )
                            else:
                                measure_drift_threshold = drift_threshold

                            drift_threshold_value = json.dumps(
                                measure_drift_threshold, default=str
                            )
                            attribute_condition = (
                                f" and attribute_id='{attribute_id}'"
                                if attribute_id
                                else ""
                            )
                            asset_condition = (
                                f" and asset_id='{asset_id}'" if asset_id else ""
                            )
                            task_condition = (
                                f" and task_id='{task_id}'" if task_id else ""
                            )
                            query_string = f"""
                                    update core.measure
                                    set drift_threshold='{drift_threshold_value}'
                                    where id='{measure_id}' {asset_condition} {attribute_condition} {task_condition}
                                """
                            cursor = execute_query(connection, cursor, query_string)
                            query_input = (
                                str(uuid4()),
                                connection_id,
                                asset_id,
                                attribute_id,
                                task_id,
                                measure_id,
                                behavioral_key,
                                measure_name,
                                run_id,
                                float(lower_threshold) if lower_threshold else 0,
                                float(upper_threshold) if upper_threshold else 0,
                            )
                            input_literals = ", ".join(["%s"] * len(query_input))
                            query_param = cursor.mogrify(
                                f"({input_literals}, CURRENT_TIMESTAMP)",
                                query_input,
                            ).decode("utf-8")
                            drift_threshold_values.append(query_param)

                        drift_threshold_input = split_queries(drift_threshold_values)
                        for input_values in drift_threshold_input:
                            try:
                                query_input = ",".join(input_values)
                                attribute_insert_query = f"""
                                    insert into core.drift_threshold (id, connection_id, asset_id, attribute_id, task_id,
                                    measure_id, behavioral_key, measure_name, run_id, lower_threshold, upper_threshold, created_date)
                                    values {query_input}
                                """
                                log_info(f"attribute_insert_query: {attribute_insert_query}")
                                cursor = execute_query(
                                    connection, cursor, attribute_insert_query
                                )
                                execution_time = time.time() - start_time
                            except Exception as e:
                                log_error(
                                    "ENHANCED_PROCESSING: Error inserting enhanced thresholds",
                                    e,
                                )
                                raise e

                    except ValueError as e:
                        execution_time = time.time() - start_time
                        log_error(f"==== ENHANCED_PROCESSING: update_threshold function FAILED after {execution_time:.2f} seconds ====", e)
                        log_error(
                            "ValueError: Cannot set a DataFrame with multiple columns to the single column threshold",
                            e,
                        )
                        raise e
    except Exception as e:
        execution_time = time.time() - start_time
        log_error(f"==== ENHANCED_PROCESSING: update_threshold function FAILED after {execution_time:.2f} seconds ====", e)
        log_error("Failed enhanced update threshold: inserting new threshold ", e)