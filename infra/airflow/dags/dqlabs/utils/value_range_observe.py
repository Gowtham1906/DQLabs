"""
Module for observing and predicting ranges of values over time.

This module provides functionality to track a stream of min/max pairs and predict
expected ranges based on historical data, using either a sliding window approach
or exponentially weighted moving averages (EWMA).

It has been enhanced with capabilities to extract historical value ranges from
attribute data in the database for length_range and value_range measure types.

ENHANCED: Now includes reset timestamp filtering to ensure only post-reset data
is used for threshold calculations.
"""

from collections import deque
import statistics
import math
import json
from typing import Tuple, List, Optional, Union, Dict, Any

# Import logging utilities from the project's logging system 
try:
    from dqlabs.app_helper.log_helper import log_info, log_error
    from dqlabs.app_helper.dag_helper import get_postgres_connection
    from dqlabs.app_helper.db_helper import execute_query, fetchone, fetchall
except ImportError:
    # Fallback logging for standalone usage
    def log_info(msg): print(f"INFO: {msg}")
    def log_error(msg, exc=None): 
        error_msg = f"ERROR: {msg}"
        if exc:
            error_msg += f" - {str(exc)}"
        print(error_msg)
    def get_postgres_connection(config): return None
    def execute_query(connection, cursor, query): return cursor
    def fetchone(cursor): return None
    def fetchall(cursor): return []


def get_last_reset_timestamp_for_boundaries(connection, measure_id, attribute_id):
    """
    Get the most recent reset timestamp for filtering historical boundaries.
    
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
                AND action_history IS NOT NULL
                AND jsonb_typeof(action_history) = 'object'
                AND action_history !='{{}}'::jsonb
                ORDER BY created_date DESC
                LIMIT 1
            """
            cursor = execute_query(connection, cursor, query)
            results = cursor.fetchall()
            
            for row in results:
                action_history = row.get("action_history", {}) if hasattr(row, 'get') else row[0]
                if isinstance(action_history, str):
                    try:
                        action_history = json.loads(action_history)
                    except json.JSONDecodeError:
                        continue
                
                if action_history.get("is_changed", False):
                    reset_timestamp = action_history.get("date")
                    if reset_timestamp:
                        log_info(f"BOUNDARIES_RESET_TIMESTAMP: Found reset at {reset_timestamp} for measure {measure_id}")
                        return reset_timestamp
            
            log_info(f"BOUNDARIES_RESET_TIMESTAMP: No reset found for measure {measure_id}")
            return None
            
    except Exception as e:
        log_error(f"Error getting reset timestamp for boundaries for measure {measure_id}: {e}", e)
        return None


class ValueRangeObserver:
    """
    Observes a stream of (min, max) values and calculates expected range
    using either a sliding window or exponentially weighted moving average (EWMA).
    
    This class is designed for monitoring range-based metrics that consist of 
    minimum and maximum values, such as length_range or value_range measures.
    It provides methods to update the observer with new min/max pairs and to
    retrieve the expected range based on historical data.
    
    Attributes:
        method (str): The algorithm used for calculating expected ranges.
            Either 'window' for sliding window or 'ewma' for exponentially
            weighted moving average.
        window_size (int): The number of recent values to track when using the
            window method.
        alpha (float): The smoothing factor for EWMA method (between 0 and 1).
        include_margin (bool): Whether to include standard deviation as margins
            in the expected range.
    """

    def __init__(self, method: str = 'window', window_size: int = 5, 
                 alpha: float = 0.3, include_margin: bool = False,
                 return_integers: bool = False):
        """
        Initialize the ValueRangeObserver with the specified configuration.

        Args:
            method (str): The algorithm to use, either 'window' or 'ewma'.
            window_size (int): Number of recent values to track (for window method).
                Larger values provide more stability but less responsiveness.
            alpha (float): Smoothing factor for EWMA (between 0 and 1).
                Higher values give more weight to recent observations.
            include_margin (bool): Whether to include standard deviation margin
                in the expected range calculation. This widens the expected range
                to account for variability in the data.
            return_integers (bool): Whether to return integer values instead of floats.
                When True, the lower threshold will be floored and the upper threshold
                will be ceiled to the nearest integer.
        
        Raises:
            ValueError: If method is not 'window' or 'ewma', or if alpha is not 
                between 0 and 1, or if window_size is less than 1.
        """
        # Validate inputs
        if method.lower() not in ['window', 'ewma']:
            raise ValueError("method must be 'window' or 'ewma'")
        if not 0 <= alpha <= 1:
            raise ValueError("alpha must be between 0 and 1")
        if window_size < 1:
            raise ValueError("window_size must be at least 1")
            
        self.method = method.lower()
        self.window_size = window_size
        self.alpha = alpha
        self.include_margin = include_margin
        self.return_integers = return_integers
        
        # Initialize data structures based on method
        if self.method == 'window':
            self.min_values = deque(maxlen=window_size)
            self.max_values = deque(maxlen=window_size)
        elif self.method == 'ewma':
            self.ewma_min = None
            self.ewma_max = None
        
        # Keep track of all observed values for statistics
        self._all_min_values = []
        self._all_max_values = []
        
        log_info(f"ValueRangeObserver initialized with method={method}, "
                f"window_size={window_size}, alpha={alpha}, include_margin={include_margin}, "
                f"return_integers={return_integers}")

    def update(self, min_val: float, max_val: float) -> None:
        """
        Updates the observer with a new (min, max) pair.
        
        This method adds a new observation to the internal data structures
        used for calculating expected ranges.
        
        Args:
            min_val (float): The minimum value in the range.
            max_val (float): The maximum value in the range.
            
        Raises:
            ValueError: If min_val is greater than max_val.
        """
        try:
            # Validate inputs
            min_val = float(min_val)
            max_val = float(max_val)
            
            if min_val > max_val:
                raise ValueError(f"min_val ({min_val}) cannot be greater than max_val ({max_val})")
            
            # Update the appropriate data structure based on method
            if self.method == 'window':
                self.min_values.append(min_val)
                self.max_values.append(max_val)
            elif self.method == 'ewma':
                if self.ewma_min is None:
                    self.ewma_min = min_val
                    self.ewma_max = max_val
                else:
                    self.ewma_min = self.alpha * min_val + (1 - self.alpha) * self.ewma_min
                    self.ewma_max = self.alpha * max_val + (1 - self.alpha) * self.ewma_max
            
            # Store all values for statistics
            self._all_min_values.append(min_val)
            self._all_max_values.append(max_val)
            
            log_info(f"Added new value pair to observer: min={min_val}, max={max_val}")
            
        except Exception as e:
            log_error(f"Error updating ValueRangeObserver with min_val={min_val}, max_val={max_val}", e)
            raise

    def expected_range(self) -> Optional[Tuple[float, float]]:
        """
        Returns the expected (min, max) range based on historical observations.
        
        The expected range represents the prediction of future min/max values
        based on past observations, calculated using the configured method.
        
        Returns:
            Tuple[float, float] or None: A tuple containing (expected_min, expected_max),
                or None if there is insufficient data to calculate the range.
                If return_integers is True, the values will be integers.
        """
        try:
            if self.method == 'window':
                if not self.min_values or not self.max_values:
                    log_info("Insufficient data in window to calculate expected range")
                    return None
                
                # Calculate average min and max values
                avg_min = sum(self.min_values) / len(self.min_values)
                avg_max = sum(self.max_values) / len(self.max_values)
                
                log_info(f"Window method - calculated average min={avg_min}, avg_max={avg_max}")
                
                # Optionally include standard deviation margin
                if self.include_margin and len(self.min_values) > 1:
                    try:
                        std_min = statistics.stdev(self.min_values)
                        std_max = statistics.stdev(self.max_values)
                        
                        log_info(f"Including margin - std_min={std_min}, std_max={std_max}")
                        
                        # Apply the margin to widen the expected range
                        expected_min = avg_min - std_min
                        expected_max = avg_max + std_max
                    except statistics.StatisticsError as e:
                        log_error("Error calculating standard deviation for margin", e)
                        # Fall back to average without margin
                        expected_min = avg_min
                        expected_max = avg_max
                else:
                    expected_min = avg_min
                    expected_max = avg_max
                
                # Convert to integers if requested
                if self.return_integers:
                    expected_min = math.floor(expected_min)
                    expected_max = math.ceil(expected_max)
                    log_info(f"Converting to integers - expected_min={expected_min}, expected_max={expected_max}")
                
                return (expected_min, expected_max)
                
            elif self.method == 'ewma':
                if self.ewma_min is None or self.ewma_max is None:
                    log_info("EWMA values not yet initialized")
                    return None
                
                expected_min = self.ewma_min
                expected_max = self.ewma_max
                
                log_info(f"EWMA method - expected_min={expected_min}, expected_max={expected_max}")
                
                # Convert to integers if requested
                if self.return_integers:
                    expected_min = math.floor(expected_min)
                    expected_max = math.ceil(expected_max)
                    log_info(f"Converting to integers - expected_min={expected_min}, expected_max={expected_max}")
                    
                return (expected_min, expected_max)
                
        except Exception as e:
            log_error("Error calculating expected range", e)
            return None

    def reset(self) -> None:
        """
        Resets the observer to its initial state, clearing all recorded data.
        
        This is useful when starting a new monitoring session or when
        the underlying data distribution has changed significantly.
        """
        try:
            if self.method == 'window':
                self.min_values.clear()
                self.max_values.clear()
            elif self.method == 'ewma':
                self.ewma_min = None
                self.ewma_max = None
            
            self._all_min_values = []
            self._all_max_values = []
            
            log_info("ValueRangeObserver reset to initial state")
        except Exception as e:
            log_error("Error resetting ValueRangeObserver", e)
            raise

    def get_statistics(self) -> Dict[str, Any]:
        """
        Returns descriptive statistics about all observed min/max values.
        
        This method provides insight into the distribution of observed values,
        which can be useful for understanding the data and validating the
        expected ranges.
        
        Returns:
            Dict[str, Any]: A dictionary containing statistics about the
                observed min and max values, including count, mean, median,
                min, max, and standard deviation.
        """
        stats = {
            'count': len(self._all_min_values),
            'min_values': {},
            'max_values': {}
        }
        
        try:
            if self._all_min_values:
                min_vals = self._all_min_values
                stats['min_values'] = {
                    'mean': sum(min_vals) / len(min_vals),
                    'median': statistics.median(min_vals) if len(min_vals) > 0 else None,
                    'min': min(min_vals) if len(min_vals) > 0 else None,
                    'max': max(min_vals) if len(min_vals) > 0 else None,
                    'std_dev': statistics.stdev(min_vals) if len(min_vals) > 1 else None
                }
            
            if self._all_max_values:
                max_vals = self._all_max_values
                stats['max_values'] = {
                    'mean': sum(max_vals) / len(max_vals),
                    'median': statistics.median(max_vals) if len(max_vals) > 0 else None,
                    'min': min(max_vals) if len(max_vals) > 0 else None,
                    'max': max(max_vals) if len(max_vals) > 0 else None,
                    'std_dev': statistics.stdev(max_vals) if len(max_vals) > 1 else None
                }
                
            log_info(f"Generated statistics for {stats['count']} observations")
        except Exception as e:
            log_error("Error calculating statistics", e)
            
        return stats

    def current_method_state(self) -> Dict[str, Any]:
        """
        Returns the current state of the algorithm being used.
        
        This method provides information about the current state of the
        window or EWMA algorithm, which can be useful for debugging or
        understanding how the expected range is being calculated.
        
        Returns:
            Dict[str, Any]: A dictionary containing information about the
                current state of the algorithm.
        """
        state = {
            'method': self.method,
            'include_margin': self.include_margin
        }
        
        try:
            if self.method == 'window':
                state.update({
                    'window_size': self.window_size,
                    'current_window_size': len(self.min_values),
                    'min_values': list(self.min_values),
                    'max_values': list(self.max_values)
                })
            elif self.method == 'ewma':
                state.update({
                    'alpha': self.alpha,
                    'ewma_min': self.ewma_min,
                    'ewma_max': self.ewma_max
                })
                
            log_info(f"Current state: {state}")
        except Exception as e:
            log_error("Error retrieving current method state", e)
            
        return state


def get_attribute_historical_boundaries(config, attribute_id=None, boundary_type=None, measure_id=None, exclude_latest=False):
    """
    ENHANCED: Gets historical attribute boundaries (min_value, max_value, min_length, max_length)
    with reset timestamp filtering to ensure only post-reset data is used.
    
    Args:
        config: Configuration dictionary
        attribute_id: ID of the attribute (if None, will extract from config)
        boundary_type: Type of boundary to fetch ('min_value', 'max_value', 'min_length', 'max_length')
                      (if None, will determine from measure_name in config)
        measure_id: ID of the measure (for logging) (if None, will extract from config)
        exclude_latest: Whether to exclude the most recent value from results (for profile.py)
        
    Returns:
        List of historical values in chronological order (filtered by reset timestamp)
    """
    # Extract values from config if not provided
    if attribute_id is None:
        measure = config.get("measure", {})
        attribute_id = measure.get("attribute_id", "")
    
    if measure_id is None:
        measure = config.get("measure", {})
        measure_id = measure.get("id", "")
    
    # Get attribute and asset details for better logging
    asset = config.get("asset", {}) or {}
    asset_name = asset.get("name", "Unknown")
    
    # Get drift days from config
    drift_days = config.get("window", 30)  # Default to 30 days if not in config
    drift_type = config.get("drift_type", "runs").lower()  # 'runs' or 'days'
    
    context = f"Asset: '{asset_name}', Attribute ID: '{attribute_id}', Measure ID: '{measure_id or 'Unknown'}'"
    log_info(f"{context} - Getting historical {boundary_type} values for {drift_days} {drift_type}")
    
    # Try to use existing connection if provided
    if config.get("existing_connection"):
        log_info(f"{context} - Using existing database connection")
        connection = config.get("existing_connection")
        should_close = False
    else:
        try:
            log_info(f"{context} - Creating new database connection")
            connection = get_postgres_connection(config)
            should_close = True
        except Exception as e:
            log_error(f"{context} - Failed to create database connection", e)
            return []
    
    boundary_values = []
    
    try:
        # ENHANCED: Get reset timestamp for filtering
        reset_timestamp = None
        if measure_id and attribute_id:
            reset_timestamp = get_last_reset_timestamp_for_boundaries(connection, measure_id, attribute_id)
        
        # Query for historical boundary values
        with connection.cursor() as cursor:
            # Build reset filter if timestamp exists
            reset_filter = ""
            if reset_timestamp:
                reset_filter = f"AND created_date > '{reset_timestamp}'"
                log_info(f"{context} - Using reset timestamp filter: {reset_timestamp}")
            
            # First try the metrics table for direct values
            query_string = f"""
                SELECT value FROM core.metrics 
                WHERE attribute_id = '{attribute_id}' 
                AND measure_name = '{boundary_type}' 
                {reset_filter}
                ORDER BY created_date DESC LIMIT {drift_days}
            """
            try:
                log_info(f"{context} - Executing filtered query: {query_string}")
                cursor = execute_query(connection, cursor, query_string)
                results = cursor.fetchall()
                log_info(f"{context} - Query returned {len(results)} results from metrics table")
            except Exception as e:
                log_error(f"{context} - Error querying metrics table", e)
                results = []
            
            # If no results, try attribute table
            if not results:
                if boundary_type in ['min_value', 'max_value', 'min_length', 'max_length']:
                    # For attribute table, apply reset filter if available
                    query_string = f"""
                        SELECT {boundary_type} FROM core.attribute 
                        WHERE id = '{attribute_id}' 
                        {reset_filter}
                        ORDER BY created_date DESC
                        LIMIT {drift_days}
                    """
                    try:
                        log_info(f"{context} - Executing filtered query: {query_string}")
                        cursor = execute_query(connection, cursor, query_string)
                        results = cursor.fetchall()
                        log_info(f"{context} - Query returned {len(results)} results from attribute table")
                    except Exception as e:
                        log_error(f"{context} - Error querying attribute table", e)
                        results = []
            
            # Convert to numeric values
            for row in results:
                try:
                    value = float(row[0]) if row[0] and str(row[0]).strip() else 0
                    boundary_values.append(value)
                except (ValueError, TypeError) as e:
                    log_info(f"{context} - Error parsing boundary value: {str(e)}")
                    continue
        
        # If exclude_latest is True and we have values, remove the most recent one
        if exclude_latest and boundary_values:
            log_info(f"{context} - Excluding most recent value ({boundary_values[0]}) as requested by profile.py")
            boundary_values = boundary_values[1:]  # Skip the first value (most recent)
        
        reset_info = f" (post-reset from {reset_timestamp})" if reset_timestamp else ""
        log_info(f"{context} - Retrieved {len(boundary_values)} historical values for {boundary_type}{reset_info}")
        if boundary_values:
            log_info(f"{context} - {boundary_type} values summary - min: {min(boundary_values)}, "
                     f"max: {max(boundary_values)}, avg: {sum(boundary_values)/len(boundary_values)}")
            
        return boundary_values
    
    except Exception as e:
        log_error(f"{context} - Error fetching {boundary_type} history", e)
        return []

def process_historical_values(config: dict, connection=None, cursor=None, 
                          method: str = 'window', window_size: int = 5, 
                          alpha: float = 0.3, include_margin: bool = True,
                          exclude_latest: bool = False) -> Tuple[float, float, bool]:
    """
    ENHANCED: Process historical min/max values and calculate expected range.
    Now includes reset timestamp filtering to ensure only post-reset data is used.
    
    This function is designed to work with length_range and value_range measure types.
    It retrieves historical boundary values and uses the ValueRangeObserver to
    calculate appropriate thresholds.
    
    Args:
        config (dict): Configuration dictionary containing measure details
        connection: Database connection object (optional)
        cursor: Database cursor object (optional)
        method (str): Algorithm to use - 'window' or 'ewma'
        window_size (int): Size of sliding window
        alpha (float): Smoothing factor for EWMA
        include_margin (bool): Whether to include std dev margin
        exclude_latest (bool): Whether to exclude the most recent value (for profile.py)
        
    Returns:
        Tuple[float, float, bool]: Calculated (lower_threshold, upper_threshold, is_first_run)
    """
    try:
        # Extract necessary values from config
        measure = config.get("measure", {})
        measure_id = measure.get("id", "")
        measure_name = measure.get("name", "")
        asset_id = config.get("asset_id", "")
        attribute_id = measure.get("attribute_id", "")
        task_id = measure.get("task_id", "")
        drift_days = config.get("window", 30)
        
        # Get asset name for logging
        asset = config.get("asset", {}) or {}
        asset_name = asset.get("name", "Unknown")
        context = f"Asset: '{asset_name}', Attribute ID: '{attribute_id}', Measure ID: '{measure_id}'"
        
        log_info(f"{context} - Processing historical values for {measure_name} " + 
                 f"(exclude_latest={exclude_latest})")
        log_info(f"Using method: {method}, window_size: {window_size}, alpha: {alpha}, " +
                 f"include_margin: {include_margin}")
        
        # Pass the existing connection to the config if provided
        if connection:
            log_info(f"{context} - Using provided database connection")
            config["existing_connection"] = connection
        
        # Create observer instance
        observer = ValueRangeObserver(
            method=method,
            window_size=window_size if window_size <= drift_days else drift_days,
            alpha=alpha,
            include_margin=include_margin,
            return_integers=True  # Always return integers for thresholds
        )
        
        # Determine which boundary types to use
        if measure_name == 'length_range':
            min_boundary_type = 'min_length'
            max_boundary_type = 'max_length'
        elif measure_name == 'value_range':
            min_boundary_type = 'min_value'
            max_boundary_type = 'max_value'
        else:
            log_error(f"Unsupported measure_name: {measure_name}. Expected 'length_range' or 'value_range'",
                     ValueError(f"Unsupported measure_name: {measure_name}"))
            return 0, 0, True
            
        # Fetch historical values for min and max boundaries (with reset filtering)
        log_info(f"{context} - Fetching historical {min_boundary_type} and {max_boundary_type} values with reset filtering")
        
        # Use the unified helper function to get historical boundaries (now with reset filtering)
        min_values = get_attribute_historical_boundaries(
            config=config, 
            attribute_id=attribute_id, 
            boundary_type=min_boundary_type, 
            measure_id=measure_id,
            exclude_latest=exclude_latest  # Pass the exclude_latest parameter
        )
        
        max_values = get_attribute_historical_boundaries(
            config=config, 
            attribute_id=attribute_id, 
            boundary_type=max_boundary_type, 
            measure_id=measure_id,
            exclude_latest=exclude_latest  # Pass the exclude_latest parameter
        )
        
        # FIXED: Check if this is a first/early run, but consider if we excluded latest
        # If we excluded latest and have very few values, try to get stored thresholds first
        original_count_min = len(min_values) + (1 if exclude_latest else 0)
        original_count_max = len(max_values) + (1 if exclude_latest else 0)
        is_first_run = original_count_min <= 2 or original_count_max <= 2
        
        # FIXED: If we're in a potential first run scenario but exclude_latest=True,
        # try to get stored thresholds from drift_threshold table first
        if is_first_run and exclude_latest:
            log_info(f"{context} - Few historical values detected (original count: min={original_count_min}, max={original_count_max})")
            log_info(f"{context} - Trying to get stored thresholds from drift_threshold table first")
            
            try:
                # Ensure we have a connection
                if not connection:
                    try:
                        connection = get_postgres_connection(config)
                        should_close = True
                    except Exception as e:
                        log_error(f"{context} - Failed to create database connection", e)
                        return 0, 0, True
                else:
                    should_close = False
                
                with connection.cursor() as cursor:
                    # Get measure_id from measure_name and attribute_id
                    measure_id_query = f"""
                        SELECT id FROM core.measure
                        WHERE technical_name = '{measure_name}'
                        AND attribute_id = '{attribute_id}'
                        AND is_active = True
                        LIMIT 1
                    """
                    cursor = execute_query(connection, cursor, measure_id_query)
                    measure_id_result = cursor.fetchone()
                    
                    if measure_id_result:
                        actual_measure_id = measure_id_result[0] if isinstance(measure_id_result, tuple) else measure_id_result.get("id")
                        log_info(f"{context} - Found measure_id={actual_measure_id} for {measure_name}")
                        
                        # Query drift_threshold table
                        query_string = f"""
                            SELECT lower_threshold, upper_threshold, created_date
                            FROM core.drift_threshold 
                            WHERE measure_id = '{actual_measure_id}' 
                            AND attribute_id = '{attribute_id}'
                            ORDER BY created_date DESC 
                            LIMIT 1
                        """
                        
                        cursor = execute_query(connection, cursor, query_string)
                        result = cursor.fetchone()
                        
                        if result:
                            # Handle both tuple and dict result formats
                            if isinstance(result, tuple):
                                lower_threshold = float(result[0]) if result[0] is not None else 0
                                upper_threshold = float(result[1]) if result[1] is not None else 0
                                created_date = result[2] if len(result) > 2 else "Unknown"
                            else:
                                lower_threshold = float(result.get("lower_threshold", 0))
                                upper_threshold = float(result.get("upper_threshold", 0))
                                created_date = result.get("created_date", "Unknown")
                                
                            log_info(f"{context} - Found stored threshold in drift_threshold table: "
                                     f"lower={lower_threshold}, upper={upper_threshold}, date={created_date}")
                            log_info(f"{context} - Using stored thresholds instead of current attribute values")
                            
                            # Convert to integers if needed
                            lower_threshold = math.floor(lower_threshold)
                            upper_threshold = math.ceil(upper_threshold)
                            
                            return lower_threshold, upper_threshold, False  # Not a first run since we have stored data
                        else:
                            log_info(f"{context} - No stored threshold found in drift_threshold table")
                    else:
                        log_info(f"{context} - No measure found for {measure_name}, attribute_id={attribute_id}")
                        
            except Exception as e:
                log_error(f"{context} - Error retrieving stored thresholds", e)
        
        # If no historical data, get current attribute values directly
        if not min_values or not max_values or is_first_run:
            log_info(f"{context} - First/early run detected (metrics count after exclusion: min={len(min_values)}, max={len(max_values)})")
            log_info(f"{context} - Using current attribute values for thresholds instead of historical data.")
            
            # Ensure we have a connection
            if not connection:
                try:
                    connection = get_postgres_connection(config)
                    should_close = True
                except Exception as e:
                    log_error(f"{context} - Failed to create database connection", e)
                    return 0, 0, True
            else:
                should_close = False
                
            try:
                # Query for current attribute values directly
                with connection.cursor() as cursor:
                    query_string = f"""
                        SELECT {min_boundary_type}, {max_boundary_type} FROM core.attribute 
                        WHERE id = '{attribute_id}'
                    """
                    
                    log_info(f"{context} - Executing query for current values: {query_string}")
                    cursor = execute_query(connection, cursor, query_string)
                    result = fetchone(cursor)
                    
                    if result:
                        # Handle both tuple and dict result formats
                        if isinstance(result, tuple):
                            current_min, current_max = result
                        else:
                            current_min = result.get(min_boundary_type)
                            current_max = result.get(max_boundary_type)
                        
                        # Convert to numeric values
                        try:
                            current_min = float(current_min) if current_min and str(current_min).strip() else 0
                            current_max = float(current_max) if current_max and str(current_max).strip() else 0
                            
                            # Use floor/ceil to get integer thresholds
                            lower_threshold = math.floor(current_min)
                            upper_threshold = math.ceil(current_max)
                            
                            log_info(f"{context} - Using current attribute values as thresholds: "
                                    f"lower={lower_threshold}, upper={upper_threshold}")
                            
                            # Return thresholds along with first_run flag
                            return lower_threshold, upper_threshold, True
                        except (ValueError, TypeError) as e:
                            log_error(f"{context} - Error converting current values to float", e)
                    else:
                        log_info(f"{context} - No current attribute values found")
                        
            except Exception as e:
                log_error(f"{context} - Error fetching current attribute values", e)
                    
            # Fall back to default values if we couldn't get current values
            if measure_name == 'length_range':
                return 0, 100, True  # Default length range
            elif measure_name == 'value_range':
                return -100, 100, True  # Default value range
            return 0, 0, True
        
        if len(min_values) != len(max_values):
            log_info(f"{context} - Warning: Mismatched number of min values ({len(min_values)}) "
                     f"and max values ({len(max_values)})")
            # Adjust to use the smaller set
            min_len = min(len(min_values), len(max_values))
            min_values = min_values[:min_len]
            max_values = max_values[:min_len]
            
        # Process each pair of values
        log_info(f"{context} - Processing {len(min_values)} pairs of historical values")
        for min_val, max_val in zip(min_values, max_values):
            observer.update(min_val, max_val)
            log_info(f"Added value pair to observer: min={min_val}, max={max_val}")
            
        # Get the expected range
        range_result = observer.expected_range()
        
        if range_result is None:
            log_info(f"{context} - Unable to calculate expected range for {measure_name}")
            return 0, 0, False
            
        lower_threshold, upper_threshold = range_result
        
        # Log statistics and results
        stats = observer.get_statistics()
        state = observer.current_method_state()
        log_info(f"Observer statistics: {stats}")
        log_info(f"Current method state: {state}")
        log_info(f"Calculated thresholds: lower={lower_threshold}, upper={upper_threshold}")
        
        # Return thresholds with is_first_run=False
        return lower_threshold, upper_threshold, False
        
    except Exception as e:
        log_error(f"Error processing historical values", e)
        return 0, 0, True