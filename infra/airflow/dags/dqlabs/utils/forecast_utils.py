# -*- coding: utf-8 -*-
"""
Forecasting utility functions for DQLabs
This module contains functions to forecast boundary values based on historical data.
It includes handling for both min/max and length/value boundaries with consistent margin calculation.
"""

import math
from math import isnan
from dqlabs.models.enhanced_threshold_model import EnhancedThresholdModel
from dqlabs.app_helper.log_helper import log_error, log_info

def forecast_boundary_value(config, attribute_id, boundary_type, current_value, 
                           is_min_boundary, measure_id=None, historical_values=None):
    """
    Core forecasting function that handles both min/max and length/value boundaries
    with consistent margin calculation.
    
    Args:
        config: Configuration dictionary
        attribute_id: ID of the attribute
        boundary_type: Type of boundary ('min_value', 'max_value', 'min_length', 'max_length')
        current_value: Current boundary value
        is_min_boundary: Whether this is a minimum boundary (vs maximum)
        measure_id: ID of the measure (for logging)
        historical_values: Historical values for this boundary (REQUIRED)
        
    Returns:
        Forecasted boundary value (integer)
    """
    # Get asset and attribute info for better logging
    asset = config.get("asset", {}) or {}
    asset_name = asset.get("name", "Unknown")
    
    # Get attribute name if available
    attribute_name = None
    if config.get("attributes"):
        for attr in config.get("attributes", []):
            if attr.get("id") == attribute_id:
                attribute_name = attr.get("name")
                break
    
    # Build context string for logs
    measure_context = f", Measure ID: '{measure_id}'" if measure_id else ""
    context = f"Asset: '{asset_name}', Attribute: '{attribute_name or attribute_id}'{measure_context}"
    
    # Determine if this is a length boundary
    is_length_boundary = 'length' in boundary_type.lower()
    
    # Default fallback values - adjusted for length vs value
    default_value = 0 if (is_min_boundary and not is_length_boundary) else 1
    
    # Check for valid historical values - each caller must provide these
    if not historical_values or len(historical_values) < 2:
        log_info(f"{context} - Not enough historical data for {boundary_type}. Using default value: {default_value}")
        return default_value
    
    # Create enhanced model instance
    enhanced_model = EnhancedThresholdModel()
    
    try:
        # Check for constant series (all values are the same)
        is_constant_series = len(set([float(v) for v in historical_values if v is not None])) == 1
        constant_value = float(historical_values[0]) if is_constant_series else None
        
        if is_constant_series:
            log_info(f"{context} - Detected constant series with value {constant_value} for {boundary_type}")
            
            # Always use the original constant value as the forecast
            # Never use ARIMA or other models for constant series
            forecast_value = constant_value
            
            # Detect trend direction for logging
            is_trending_down = False  # Constant series don't trend
            is_trending_up = False
            is_trending = False
            
            log_info(f"{context} - Using constant value {constant_value} directly as forecast (skipping ARIMA)")
        else:
            # Detect trend direction for non-constant series
            is_trending_down = historical_values[-1] < historical_values[0]
            is_trending_up = historical_values[-1] > historical_values[0]
            
            log_info(f"{context} - {boundary_type.capitalize()} trend: " + 
                    ("downward" if is_trending_down else "upward or stable"))
            
            # Use appropriate forecasting model based on data length
            if len(historical_values) >= 3:
                forecast_value, _, _, is_trending = enhanced_model._select_alternative_model(historical_values)
                
                # Validate forecast - ensure it's not incorrectly returning 0 for non-zero data
                if abs(forecast_value) < 0.0001 and any(abs(v) >= 0.0001 for v in historical_values):
                    log_info(f"{context} - WARNING: Model returned near-zero forecast ({forecast_value}) for non-zero data")
                    log_info(f"{context} - Using mean of historical values instead")
                    forecast_value = sum(historical_values) / len(historical_values)
            else:
                # Simple trend for 2 points
                slope = historical_values[1] - historical_values[0]
                forecast_value = historical_values[1] + slope
                is_trending = slope > 0
                
                # If slope is near zero, just use the last value
                if abs(slope) < 0.0001:
                    forecast_value = historical_values[-1]
        
        # Apply margin based on boundary type and trend direction
        if is_min_boundary:
            # For min boundaries, larger margin if trending down
            margin = 0.10 if is_trending_down else 0.05
            
            # Apply percentage-based margin
            adjusted_value = forecast_value * (1 - margin)
            
            # Special handling for length - ensure min is at least 1 unless forecasting 0
            if is_length_boundary:
                # For length, lowest possible is 0 (empty string) or 1 (non-empty)
                # If constant series showing length > 0, don't go below 1
                if is_constant_series and constant_value > 0:
                    adjusted_value = max(1, adjusted_value)
                
                # Ensure non-negative for lengths
                adjusted_value = max(0, adjusted_value)
            else:
                # For regular values, ensure non-negative
                adjusted_value = max(0, adjusted_value)
            
            # Floor the value for min boundaries
            final_value = math.floor(adjusted_value)
            
            log_info(f"{context} - Final {boundary_type} forecast: raw={forecast_value:.2f}, margin={margin*100}%, adjusted={adjusted_value:.2f}, final={final_value}")
        else:
            # For max boundaries, larger margin if trending up
            margin = 0.10 if is_trending_up else 0.05
            
            # Apply percentage-based margin
            adjusted_value = forecast_value * (1 + margin)
            
            # Apply absolute minimum margin for max boundaries to allow some headroom
            # This is especially important for constant series
            if is_constant_series:
                # For length fields, add at least +1 to max (or +2 if value > 10)
                if is_length_boundary:
                    absolute_margin = 2 if constant_value > 10 else 1
                    adjusted_value = max(adjusted_value, constant_value + absolute_margin)
                else:
                    # For numeric values, add at least 10% or +1, whichever is greater
                    pct_margin = constant_value * 0.1
                    absolute_margin = 1 if constant_value > 0 else 1
                    absolute_margin = max(pct_margin, absolute_margin)
                    adjusted_value = max(adjusted_value, constant_value + absolute_margin)
            
            # Ceil the value for max boundaries
            final_value = math.ceil(adjusted_value)
            
            log_info(f"{context} - Final {boundary_type} forecast: raw={forecast_value:.2f}, margin={margin*100}%, adjusted={adjusted_value:.2f}, final={final_value}")
            
        # Final validation - never return 0 as a max threshold for constant series
        # (unless all values are actually 0)
        if not is_min_boundary and final_value == 0 and not all(v == 0 for v in historical_values):
            log_info(f"{context} - WARNING: Zero max threshold detected for non-zero data. Setting to 1.")
            final_value = 1
        
        return final_value
        
    except Exception as e:
        log_error(f"{context} - Error in forecasting: {str(e)}")
        
        # Improved fallback approach for errors
        if is_constant_series and constant_value is not None:
            # If it's a constant series and we know the value, use that with appropriate margins
            if is_min_boundary:
                # For min boundary with constant series, use the constant value as is
                # (slightly reduced for non-length fields)
                if is_length_boundary:
                    # For length, lowest possible is 0 (empty string) or 1 (non-empty)
                    final_value = max(0, math.floor(constant_value))
                else:
                    # For regular values, apply a small reduction but avoid negative
                    final_value = max(0, math.floor(constant_value * 0.95))
            else:
                # For max boundary with constant series, add margin
                if is_length_boundary:
                    # For length, add at least +2 if value > 10, otherwise +1
                    absolute_margin = 2 if constant_value > 10 else 1
                    final_value = math.ceil(constant_value) + absolute_margin
                else:
                    # For regular values, add at least 5%
                    margin = max(1, constant_value * 0.05)
                    final_value = math.ceil(constant_value + margin)
                    
                # Final validation again - never return 0 as a max threshold
                if final_value == 0 and constant_value != 0:
                    log_info(f"{context} - WARNING: Zero max threshold detected in fallback. Setting to 1.")
                    final_value = 1
        else:
            # Fallback for non-constant series or unknown constant
            if is_min_boundary:
                # For min boundary, use minimum with safety margin
                min_val = min(historical_values)
                final_value = max(0, math.floor(min_val * 0.95))
                # For length fields, ensure minimum is appropriate
                if is_length_boundary and min_val > 0:
                    final_value = max(1, final_value)
            else:
                # For max boundary, use maximum with safety margin
                max_val = max(historical_values)
                
                # Special case for length fields - ensure proper absolute margin
                if is_length_boundary:
                    absolute_margin = 2 if max_val > 10 else 1
                    margin = max(max_val * 0.05, absolute_margin)
                else:
                    margin = max(1, max_val * 0.05)  # At least +1 or 5%
                
                final_value = math.ceil(max_val + margin)
                
                # Final validation - never return 0 as a max threshold
                if final_value == 0 and not all(v == 0 for v in historical_values):
                    log_info(f"{context} - WARNING: Zero max threshold detected in non-constant fallback. Setting to 1.")
                    final_value = 1
        
        log_info(f"{context} - Fallback forecasting due to error: final value = {final_value}")
        return final_value