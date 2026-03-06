"""
Enhanced Threshold Model for Value-Based Measures

This module implements an ML-based approach for threshold calculation
that incorporates multiple forecasting models, adaptive lookback periods,
anomaly detection for data cleaning, and trend-aware threshold adjustments.

"""

import pandas as pd
import numpy as np
from scipy import stats
from prophet import Prophet
import pmdarima as pmd
import statsmodels.api as sm
from statsmodels.tsa.ar_model import AutoReg
from statsmodels.tsa.holtwinters import ExponentialSmoothing
import math
from math import isnan
from copy import deepcopy
from sklearn.neighbors import LocalOutlierFactor

from dqlabs.utils.drift import generate_prophet_dates
from dqlabs.app_helper.log_helper import log_error, log_info

class EnhancedThresholdModel:
    """
    Enhanced threshold model that improves handling of constant series
    and applies more intelligent forecasting for various data patterns.
    """
    
    def __init__(self):
        """Initialize the model with default configuration."""
        self.config = {
            "lookback_window": 30,  # Default window size
            "min_length_threshold": 1,  # Minimum non-zero length
            "min_constant_margin": 1,  # Minimum margin for constant series
            "min_value_threshold": 0,  # Minimum value for non-length fields
            "clean_outliers": True,    # Whether to remove outliers
            "min_datapoints": 5,       # Minimum datapoints for outlier detection
            "prophet_params": {        # Prophet configuration
                "changepoint_prior_scale": 0.05,
                "seasonality_prior_scale": 10.0,
                "changepoint_range": 0.9
            }
        }
        log_info("Initializing EnhancedThresholdModel with default configuration.")
    
    def update_config(self, new_config):
        """Update the model configuration."""
        self.config.update(new_config)
    
    def detect_outliers(self, values):
        """
        Detect and clean outliers from the input values
        Returns: cleaned_values, is_outlier_mask
        """
        log_info("Starting outlier detection.")
        if len(values) < self.config.get("min_datapoints", 5):
            log_info("Not enough data points for outlier detection.")
            return values, np.zeros(len(values), dtype=bool)
            
        values_array = np.array(values).reshape(-1, 1)
        
        try:
            lof = LocalOutlierFactor(n_neighbors=min(5, len(values)-1), contamination=0.1)
            outlier_pred = lof.fit_predict(values_array)
            outlier_mask = outlier_pred == -1
            
            cleaned_values = np.array(values).copy()
            if np.any(outlier_mask):
                median_value = np.median(values)
                cleaned_values[outlier_mask] = median_value
                log_info(f"Outliers detected and replaced with median value: {median_value}")
                
            return cleaned_values, outlier_mask
        except Exception as e:
            log_info(f"Outlier detection failed: {str(e)}")
            return values, np.zeros(len(values), dtype=bool)
    
    def _validate_forecast(self, values, forecast_value, lower_bound, upper_bound, method_name="unknown"):
        """
        Validate forecasts for constant series to prevent common issues
        including the zero forecast problem for constant series.
        
        Args:
            values: Original input values
            forecast_value: Forecasted value
            lower_bound: Lower bound from model
            upper_bound: Upper bound from model
            method_name: Name of the forecasting method for logging
            
        Returns:
            tuple: Validated (forecast_value, lower_bound, upper_bound)
        """
        # Check if we have a constant series
        is_constant = len(set([float(v) for v in values if v is not None])) == 1
        if is_constant:
            constant_value = float(values[0])
            log_info(f"Validating {method_name} forecast for constant series with value {constant_value}")
            
            # If the forecast is 0 but the series is not 0, this is the ARIMA (0,0,0) issue
            if abs(forecast_value) < 0.0001 and abs(constant_value) >= 0.0001:
                log_info(f"FORECAST ERROR DETECTED: {method_name} returned {forecast_value} for constant series with value {constant_value}")
                log_info(f"Correcting forecast to use constant value instead")
                forecast_value = constant_value
                
                # UPDATED: Apply different buffer based on value size (consistent with forecast_utils.py)
                # For values > 10, use a larger absolute margin for upper bound
                if constant_value > 10:
                    upper_margin = max(constant_value * 0.05, 2.0)  # At least 2.0 or 5%
                else:
                    upper_margin = max(constant_value * 0.05, 1.0)  # At least 1.0 or 5%
                
                lower_margin = max(constant_value * 0.05, 1.0)  # At least 1.0 or 5%
                lower_bound = constant_value - lower_margin
                upper_bound = constant_value + upper_margin
                
                # Ensure lower bound is non-negative for length or non-negative measures
                if constant_value >= 0:
                    lower_bound = max(0, lower_bound)
                    
            # Verify bounds make sense
            if upper_bound <= constant_value or lower_bound >= constant_value:
                log_info(f"BOUND ERROR DETECTED: {method_name} bounds ({lower_bound}, {upper_bound}) don't contain constant value {constant_value}")
                
                # UPDATED: Recalculate bounds with consistent logic
                # For values > 10, use a larger margin for upper bound
                if constant_value > 10:
                    upper_margin = max(constant_value * 0.05, 2.0)  # At least 2.0 or 5%
                else:
                    upper_margin = max(constant_value * 0.05, 1.0)  # At least 1.0 or 5%
                
                lower_margin = max(constant_value * 0.05, 1.0)  # At least 1.0 or 5%
                lower_bound = constant_value - lower_margin
                upper_bound = constant_value + upper_margin
                
                # Ensure lower bound is non-negative for length or non-negative measures
                if constant_value >= 0:
                    lower_bound = max(0, lower_bound)
        
        # General validation for any forecast
        # If forecast is outside of historical range by a large margin, validate it
        if len(values) > 0:
            min_val = min(float(v) for v in values if v is not None)
            max_val = max(float(v) for v in values if v is not None)
            range_size = max_val - min_val
            
            # If forecast is drastically outside historical range and not in a constant series
            if not is_constant and range_size > 0:
                if forecast_value < min_val - range_size or forecast_value > max_val + range_size:
                    log_info(f"EXTREME FORECAST DETECTED: {method_name} forecast {forecast_value} far outside historical range [{min_val}, {max_val}]")
                    # Use last value with a trend adjustment
                    last_value = float(values[-1])
                    if len(values) > 1:
                        second_last = float(values[-2])
                        trend = last_value - second_last
                        forecast_value = last_value + trend
                    else:
                        forecast_value = last_value
                    
                    # Recalculate bounds
                    buffer = max(abs(forecast_value) * 0.1, range_size * 0.2)
                    lower_bound = forecast_value - buffer
                    upper_bound = forecast_value + buffer
        
        return forecast_value, lower_bound, upper_bound
    
    def _select_alternative_model(self, values):
        """
        Select the best forecasting model based on data characteristics.
        Improved to properly handle constant series.
        
        Args:
            values: List of historical values
            
        Returns:
            Tuple of (forecast_value, lower_bound, upper_bound, is_trending)
        """
        # Check for constant series first (all values are the same)
        if len(set([float(v) for v in values if v is not None])) == 1:
            constant_value = float(values[0])
            log_info(f"Detected constant series with value {constant_value}")
            
            # For constant series, return the constant value itself
            # with calculated confidence intervals - NEVER return 0 for non-zero constant series
            
            # UPDATED: Align with forecast_utils.py logic
            # Apply different margins based on value size
            if constant_value > 10:
                upper_margin = max(constant_value * 0.05, 2.0)  # At least 2.0 or 5%
            else:
                upper_margin = max(constant_value * 0.05, 1.0)  # At least 1.0 or 5%
            
            lower_margin = max(constant_value * 0.05, 1.0)  # At least 1.0 or 5%
            lower_bound = constant_value - lower_margin
            upper_bound = constant_value + upper_margin
            
            # Ensure lower bound is non-negative for non-negative values
            if constant_value >= 0:
                lower_bound = max(0, lower_bound)
                
            log_info(f"Constant series handling: value={constant_value}, bounds=[{lower_bound}, {upper_bound}]")
            return constant_value, lower_bound, upper_bound, False
            
        # If not a constant series, proceed with normal analysis
        series_length = len(values)
        
        # For very short series
        if series_length < 8:
            log_info("Short series (<8 points). Using simple linear regression.")
            forecast, lower, upper, is_trending = self._simple_regression_forecast(values)
            return self._validate_forecast(values, forecast, lower, upper, "simple_regression")
            
        # For medium-length series
        elif series_length < 25:  # Changed from 20 to 25 for Prophet threshold
            log_info("Medium-length series (8-24 points). Attempting ARIMA model.")
            try:
                # First try ARIMA model
                forecast, lower, upper = self._arima_forecast(values)
                forecast, lower, upper = self._validate_forecast(values, forecast, lower, upper, "ARIMA")
                is_trending = forecast > float(values[-1])
                return forecast, lower, upper, is_trending
            except Exception as e:
                log_info(f"ARIMA failed: {str(e)}. Falling back to exponential smoothing.")
                forecast, lower, upper, is_trending = self._exp_smoothing_forecast(values)
                return self._validate_forecast(values, forecast, lower, upper, "exp_smoothing") + (is_trending,)
        
        # For longer series - use Prophet first (25+ points)
        else:
            log_info("Long series (25+ points). Attempting Prophet forecast.")
            try:
                # Try Prophet model first
                forecast, lower, upper, is_trending = self._prophet_forecast(values)
                forecast, lower, upper = self._validate_forecast(values, forecast, lower, upper, "Prophet")
                return forecast, lower, upper, is_trending
            except Exception as e:
                log_info(f"Prophet forecast failed: {str(e)}. Trying Seasonal ARIMA.")
                try:
                    forecast, lower, upper = self._seasonal_arima_forecast(values)
                    forecast, lower, upper = self._validate_forecast(values, forecast, lower, upper, "seasonal_ARIMA")
                    is_trending = forecast > float(values[-1])
                    return forecast, lower, upper, is_trending
                except Exception as e2:
                    log_info(f"Seasonal ARIMA failed: {str(e2)}. Trying standard ARIMA.")
                    try:
                        forecast, lower, upper = self._arima_forecast(values)
                        forecast, lower, upper = self._validate_forecast(values, forecast, lower, upper, "ARIMA")
                        is_trending = forecast > float(values[-1])
                        return forecast, lower, upper, is_trending
                    except Exception as e3:
                        log_info(f"Standard ARIMA failed: {str(e3)}. Using exponential smoothing.")
                        forecast, lower, upper, is_trending = self._exp_smoothing_forecast(values)
                        return self._validate_forecast(values, forecast, lower, upper, "exp_smoothing") + (is_trending,)
    
    def _arima_forecast(self, values):
        """
        ARIMA forecasting with improved handling for constant series.
        """
        import numpy as np
        
        try:
            from pmdarima.arima import auto_arima
            import pandas as pd
            
            # Check for constant series explicitly before using ARIMA
            if len(set([float(v) for v in values if v is not None])) == 1:
                constant_value = float(values[0])
                log_info(f"ARIMA directly handling constant series: {constant_value}")
                # For constant series, return the constant with minimal bounds
                
                # UPDATED: Align with forecast_utils.py logic
                # Apply different margins based on value size
                if constant_value > 10:
                    upper_margin = max(constant_value * 0.05, 2.0)  # At least 2.0 or 5%
                else:
                    upper_margin = max(constant_value * 0.05, 1.0)  # At least 1.0 or 5%
                
                lower_margin = max(constant_value * 0.05, 1.0)  # At least 1.0 or 5%
                return constant_value, constant_value - lower_margin, constant_value + upper_margin
            
            # Convert to pandas Series for auto_arima
            series = pd.Series([float(v) for v in values if v is not None])
            
            # Fit ARIMA model
            model = auto_arima(
                series,
                start_p=0, start_q=0,
                max_p=3, max_q=3,
                d=None,  # Allow auto-differentiation
                seasonal=False,
                stepwise=True,
                suppress_warnings=True,
                error_action='ignore',
                approximation=False if len(values) < 100 else True
            )
            
            # Get forecast with confidence intervals
            forecast, conf_int = model.predict(n_periods=1, return_conf_int=True, alpha=0.05)
            forecast_value = forecast[0]
            lower_bound = conf_int[0][0]
            upper_bound = conf_int[0][1]
            
            # Post-process ARIMA forecast to catch the (0,0,0) ARMA issue
            # CRITICAL CHECK: If ARIMA returned 0 for non-zero data, use the mean instead
            if abs(forecast_value) < 0.0001 and any(abs(float(v)) >= 0.0001 for v in values if v is not None):
                mean_val = np.mean([float(v) for v in values if v is not None])
                log_info(f"ARIMA returned 0 for non-zero data (possible constant series issue). Using mean {mean_val} instead.")
                forecast_value = mean_val
                # Reset bounds
                std_val = np.std([float(v) for v in values if v is not None]) or mean_val * 0.1
                lower_bound = mean_val - 1.96 * std_val
                upper_bound = mean_val + 1.96 * std_val
            
            log_info(f"ARIMA model: forecast = {forecast_value}, lower = {lower_bound}, upper = {upper_bound}")
            return forecast_value, lower_bound, upper_bound
            
        except Exception as e:
            # If ARIMA fails, fall back to simple linear extrapolation
            log_info(f"Error in ARIMA: {str(e)}. Falling back to linear extrapolation.")
            return self._simple_regression_forecast(values)
    
    def _seasonal_arima_forecast(self, values):
        """
        Seasonal ARIMA forecasting with improved handling for constant series.
        """
        # Check for constant series explicitly before using ARIMA
        if len(set([float(v) for v in values if v is not None])) == 1:
            constant_value = float(values[0])
            log_info(f"Seasonal ARIMA directly handling constant series: {constant_value}")
            
            # UPDATED: Align with forecast_utils.py logic
            # Apply different margins based on value size
            if constant_value > 10:
                upper_margin = max(constant_value * 0.05, 2.0)  # At least 2.0 or 5%
            else:
                upper_margin = max(constant_value * 0.05, 1.0)  # At least 1.0 or 5%
            
            lower_margin = max(constant_value * 0.05, 1.0)  # At least 1.0 or 5%
            return constant_value, constant_value - lower_margin, constant_value + upper_margin
            
        # Implementation similar to _arima_forecast but with seasonal=True
        # Constant series handling would be the same
        return self._arima_forecast(values)  # For now, just use regular ARIMA
    
    def _prophet_forecast(self, values):
        """
        Prophet forecasting for longer time series.
        
        Args:
            values: List of historical values
            
        Returns:
            Tuple of (forecast_value, lower_bound, upper_bound, is_trending_up)
        """
        log_info("Starting Prophet forecast.")
        try:
            # Check for constant series
            if len(set([float(v) for v in values if v is not None])) == 1:
                constant_value = float(values[0])
                log_info(f"Prophet handling constant series: {constant_value}")
                
                # UPDATED: Align with forecast_utils.py logic
                # Apply different margins based on value size
                if constant_value > 10:
                    upper_margin = max(constant_value * 0.05, 2.0)  # At least 2.0 or 5%
                else:
                    upper_margin = max(constant_value * 0.05, 1.0)  # At least 1.0 or 5%
                
                lower_margin = max(constant_value * 0.05, 1.0)  # At least 1.0 or 5%
                return constant_value, constant_value - lower_margin, constant_value + upper_margin, False
                
            # Generate dates for Prophet (requires 'ds' and 'y' columns)
            numeric_values = [float(v) for v in values if v is not None]
            dates = generate_prophet_dates(len(numeric_values))
                
            # Scale extremely large values to prevent numerical issues
            max_value = max(abs(float(v)) for v in numeric_values if v is not None)
            scale_factor = 1.0
            scaled_values = numeric_values.copy()
            
            if max_value > 1e6:
                scale_factor = 1e6 / max_value
                scaled_values = [float(v) * scale_factor if v is not None else None for v in numeric_values]
                log_info(f"Scaling values by factor {scale_factor} for numerical stability")
            
            # Create Prophet dataframe
            df = pd.DataFrame({"ds": dates, "y": scaled_values})
            
            # Configure Prophet model
            n_points = len(values)
            # Prophet by default uses 25 changepoints - scale appropriately for shorter series
            n_changepoints = min(25, max(5, n_points // 5))
            log_info(f"Using {n_changepoints} changepoints for time series with {n_points} points")
            
            # Initialize and fit Prophet model
            model = Prophet(
                growth="linear",
                interval_width=0.95,  # 95% confidence interval
                yearly_seasonality="auto" if n_points > 300 else False,
                weekly_seasonality="auto" if n_points > 60 else False,
                daily_seasonality="auto" if n_points > 30 else False,
                seasonality_mode="additive",
                n_changepoints=n_changepoints,
                changepoint_prior_scale=self.config["prophet_params"]["changepoint_prior_scale"],
                seasonality_prior_scale=self.config["prophet_params"]["seasonality_prior_scale"],
                changepoint_range=self.config["prophet_params"]["changepoint_range"]
            )
            
            prophet_model = model.fit(df)
            log_info("Prophet model fitted successfully.")
            
            # Make forecast
            future = prophet_model.make_future_dataframe(periods=1)
            forecast = prophet_model.predict(future)
            
            # Extract forecast values
            next_pred = forecast.iloc[-1]
            forecast_value = float(next_pred["yhat"])
            lower_bound = float(next_pred["yhat_lower"])
            upper_bound = float(next_pred["yhat_upper"])
            
            # Scale results back to original magnitude
            if scale_factor != 1.0:
                forecast_value /= scale_factor
                lower_bound /= scale_factor
                upper_bound /= scale_factor
                log_info(f"Scaled forecast back: value={forecast_value}, bounds=[{lower_bound}, {upper_bound}]")
            
            # Determine if there's a trend
            trend = prophet_model.predict(df)["trend"].values
            is_trending_up = trend[-1] > trend[0] if len(trend) > 1 else False
            
            log_info(f"Prophet forecast: value={forecast_value}, bounds=[{lower_bound}, {upper_bound}], trending_up={is_trending_up}")
            return forecast_value, lower_bound, upper_bound, is_trending_up
            
        except Exception as e:
            log_error(f"Prophet forecast failed: {str(e)}")
            raise  # Re-raise to allow fallback to next model

    def _exp_smoothing_forecast(self, values):
        """
        Exponential smoothing forecasting with improved handling for constant series.
        """
        # Check for constant series explicitly before using exponential smoothing
        if len(set([float(v) for v in values if v is not None])) == 1:
            constant_value = float(values[0])
            log_info(f"Exponential smoothing directly handling constant series: {constant_value}")
            
            # UPDATED: Align with forecast_utils.py logic
            # Apply different margins based on value size
            if constant_value > 10:
                upper_margin = max(constant_value * 0.05, 2.0)  # At least 2.0 or 5%
            else:
                upper_margin = max(constant_value * 0.05, 1.0)  # At least 1.0 or 5%
            
            lower_margin = max(constant_value * 0.05, 1.0)  # At least 1.0 or 5%
            return constant_value, constant_value - lower_margin, constant_value + upper_margin, False
            
        # Implement exponential smoothing for non-constant series
        # This method was referenced but not implemented in the original code
        # Adding a simplified implementation for completeness
        try:
            # Convert to numpy array for easier processing
            numeric_values = np.array([float(v) for v in values if v is not None])
            
            # Create a simple exponential smoothing model
            model = ExponentialSmoothing(numeric_values, trend=None, seasonal=None)
            model_fit = model.fit()
            
            # Forecast next value
            forecast = model_fit.forecast(1)[0]
            
            # Calculate prediction intervals (approximate)
            residuals = model_fit.resid
            std_resid = np.std(residuals)
            lower_bound = forecast - 1.96 * std_resid
            upper_bound = forecast + 1.96 * std_resid
            
            # Determine trend direction
            is_trending = forecast > numeric_values[-1]
            
            return forecast, lower_bound, upper_bound, is_trending
            
        except Exception as e:
            log_info(f"Error in exponential smoothing: {str(e)}. Falling back to simple regression.")
            return self._simple_regression_forecast(values)
    
    def _simple_regression_forecast(self, values):
        """
        Simple linear regression forecasting with improved handling for constant series.
        """
        import numpy as np
        
        # Check for constant series
        if len(set([float(v) for v in values if v is not None])) == 1:
            constant_value = float(values[0])
            log_info(f"Simple regression handling constant series: {constant_value}")
            
            # UPDATED: Align with forecast_utils.py logic
            # Apply different margins based on value size
            if constant_value > 10:
                upper_margin = max(constant_value * 0.05, 2.0)  # At least 2.0 or 5%
            else:
                upper_margin = max(constant_value * 0.05, 1.0)  # At least 1.0 or 5%
            
            lower_margin = max(constant_value * 0.05, 1.0)  # At least 1.0 or 5%
            lower_bound = constant_value - lower_margin
            upper_bound = constant_value + upper_margin
            
            # Ensure lower bound is non-negative for non-negative metrics
            if constant_value >= 0:
                lower_bound = max(0, lower_bound)
                
            return constant_value, lower_bound, upper_bound, False
        
        # For non-constant series with very few points, use simple trend
        numeric_values = [float(v) for v in values if v is not None]
        x = np.arange(len(numeric_values))
        y = np.array(numeric_values)
        
        # Fit linear regression: y = mx + b
        if len(numeric_values) > 1:
            m, b = np.polyfit(x, y, 1)
            
            # Forecast next value
            next_x = len(numeric_values)
            forecast = m * next_x + b
            
            # Determine if trending up
            is_trending = m > 0
            
            # Calculate prediction intervals
            y_pred = m * x + b
            n = len(numeric_values)
            mse = np.sum((y - y_pred) ** 2) / (n - 2) if n > 2 else np.var(y)
            std_err = np.sqrt(mse * (1 + 1/n + (next_x - np.mean(x))**2 / np.sum((x - np.mean(x))**2)))
            
            # 95% confidence interval
            lower_bound = forecast - 1.96 * std_err
            upper_bound = forecast + 1.96 * std_err
        else:
            # Only one point, can't establish trend
            forecast = numeric_values[0]
            lower_bound = forecast * 0.95
            upper_bound = forecast * 1.05
            is_trending = False
            
        # Validate forecast
        forecast, lower_bound, upper_bound = self._validate_forecast(values, forecast, lower_bound, upper_bound, "simple_regression_internal")
            
        return forecast, lower_bound, upper_bound, is_trending
    
    def run(self, values):
        """
        Run the model to determine lower and upper thresholds.
        
        Args:
            values: List of historical values
            
        Returns:
            Tuple of (lower_threshold, upper_threshold)
        """
        try:
            # Handle empty or invalid inputs
            if not values or len(values) == 0:
                return 0, 0
                
            # Extract numeric values from complex data structures if needed
            numeric_values = []
            for val in values:
                if isinstance(val, dict) and 'value' in val:
                    try:
                        num_val = float(val['value'])
                        numeric_values.append(num_val)
                    except (ValueError, TypeError):
                        continue
                else:
                    try:
                        num_val = float(val)
                        numeric_values.append(num_val)
                    except (ValueError, TypeError):
                        continue
            
            if not numeric_values:
                return 0, 0
            
            # Convert to numpy array for easier processing
            numeric_values = np.array(numeric_values)
            log_info(f"Processing {len(numeric_values)} numeric values.")
            
            # NEW: Check for extreme values that might cause numerical issues
            max_val = np.max(np.abs(numeric_values))
            scale_factor = 1.0
            if max_val > 1e6:
                log_info(f"Detected very large values (max: {max_val}). Scaling data for numerical stability.")
                scale_factor = 1e6 / max_val
                numeric_values = numeric_values * scale_factor
                log_info(f"Applied scaling factor of {scale_factor}. New max value: {np.max(np.abs(numeric_values))}")
            
            # NEW: Smoothing for noisy data
            if len(numeric_values) >= 3 and len(set(numeric_values)) > 1:
                # Check if data is very noisy
                variance = np.var(numeric_values)
                mean_val = np.mean(numeric_values)
                if variance > (mean_val * mean_val * 0.5) and mean_val != 0:  # High variance relative to mean
                    log_info(f"High variance detected. Applying smoothing.")
                    # Apply simple moving average for smoothing
                    window_size = min(3, len(numeric_values))
                    smoothed_values = np.convolve(numeric_values, np.ones(window_size)/window_size, mode='valid')
                    # Pad the beginning to keep same length
                    padding = np.full(len(numeric_values) - len(smoothed_values), smoothed_values[0])
                    smoothed_values = np.concatenate((padding, smoothed_values))
                    log_info(f"Applied smoothing. New values: {smoothed_values.tolist()}")
                    numeric_values = smoothed_values
            
            # NEW: Apply outlier detection and cleaning
            if self.config.get("clean_outliers", True):
                log_info("Cleaning outliers from metric values.")
                numeric_values, outlier_mask = self.detect_outliers(numeric_values)
                if np.any(outlier_mask):
                    log_info(f"Cleaned {np.sum(outlier_mask)} outliers from data.")
                
            # Check if this is a length measure (all integers >= 0)
            is_length_measure = all(v == int(v) and v >= 0 for v in numeric_values)
            
            # Check for constant series - CRITICAL FIX FOR ZERO FORECAST ISSUE
            if len(set(numeric_values)) == 1:
                constant_value = numeric_values[0]
                log_info(f"Enhanced model handling constant series with value {constant_value}")
                
                # For constant length series
                if is_length_measure:
                    # For lengths, min bound should be at least 0, max should be at least constant+1
                    if constant_value == 0:
                        # For constant zero lengths, allow values from 0 to 1
                        lower_bound = 0
                        upper_bound = 1  # Always allow at least up to 1 for zero values
                    else:
                        # For non-zero constant lengths, min shouldn't be less than 1, 
                        # max should have some headroom
                        lower_bound = max(0, constant_value - 1) if constant_value > 1 else 0
                        
                        # UPDATED: Match the logic in forecast_utils.py
                        # For length values > 10, use margin of 2, otherwise margin of 1
                        absolute_margin = 2 if constant_value > 10 else 1
                        upper_bound = constant_value + absolute_margin
                        
                        log_info(f"Applied consistent margin for length measure: value={constant_value}, margin={absolute_margin}")
                else:
                    # For regular values, set reasonable bounds around the constant
                    # Use percentage for non-zero values, absolute margin for zero
                    if constant_value == 0:
                        lower_bound = 0
                        upper_bound = 1  # Always allow at least up to 1 for zero values
                    else:
                        margin = max(1, constant_value * 0.05)  # At least 1 or 5%
                        lower_bound = max(0, constant_value - margin)
                        upper_bound = constant_value + margin
                
                log_info(f"Constant series threshold calculation: value={constant_value}, bounds=[{lower_bound}, {upper_bound}]")
                
                # Rescale back if we applied scaling
                if scale_factor != 1.0:
                    lower_bound /= scale_factor
                    upper_bound /= scale_factor
                    log_info(f"Scaled thresholds back: bounds=[{lower_bound}, {upper_bound}]")
                    
                return lower_bound, upper_bound
                
            # For non-constant series, use the enhanced forecasting logic
            # Convert numeric_values back to list for compatibility with existing methods
            numeric_values_list = numeric_values.tolist()
            forecast, lower, upper, _ = self._select_alternative_model(numeric_values_list)
            
            # Get min and max observed values for bounds checking
            min_value = min(numeric_values)
            max_value = max(numeric_values)
            
            # Determine thresholds based on forecast and confidence interval
            # with sanity checks to ensure they make sense
            if is_length_measure:
                # For length measures, adjust bounds
                lower_threshold = max(0, math.floor(lower))
                upper_threshold = math.ceil(upper)
                
                # For length, make sure max threshold is at least max observed + 1
                # to allow for reasonable growth
                if upper_threshold <= max_value:
                    upper_threshold = max_value + 1
            else:
                # For regular values
                lower_threshold = max(0, math.floor(lower))
                upper_threshold = math.ceil(upper)
                
                # Ensure upper threshold is at least slightly higher than max observed
                if upper_threshold <= max_value:
                    upper_threshold = math.ceil(max_value * 1.05)  # Add at least 5%
            
            # Final validation to ensure min doesn't exceed max
            if lower_threshold > upper_threshold:
                # If bounds are inverted, set them to min and max of observed values
                lower_threshold = min_value
                upper_threshold = max_value
                # If they're still inverted (shouldn't happen), ensure separation
                if lower_threshold >= upper_threshold:
                    upper_threshold = lower_threshold + 1
            
            # Rescale back if we applied scaling
            if scale_factor != 1.0:
                lower_threshold /= scale_factor
                upper_threshold /= scale_factor
                log_info(f"Scaled thresholds back: bounds=[{lower_threshold}, {upper_threshold}]")
            
            log_info(f"Final threshold calculation: forecast={forecast}, bounds=[{lower_threshold}, {upper_threshold}]")
            return lower_threshold, upper_threshold
            
        except Exception as e:
            log_error(f"Error in EnhancedThresholdModel: {str(e)}")
            # Fallback to simple bounds based on min/max of values
            try:
                if not numeric_values:
                    return 0, 0
                min_val = min(numeric_values)
                max_val = max(numeric_values)
                
                # Special case: if all values are the same
                if min_val == max_val:
                    constant_value = min_val
                    # For zero, provide headroom
                    if constant_value == 0:
                        return 0, 1
                    # For non-zero, provide percentage margin
                    else:
                        # UPDATED: Align with forecast_utils.py logic
                        # Apply different margins based on value size
                        if constant_value > 10:
                            upper_margin = max(2, constant_value * 0.05)  # At least 2 or 5%
                        else:
                            upper_margin = max(1, constant_value * 0.05)  # At least 1 or 5%
                        
                        lower_margin = max(1, constant_value * 0.05)  # At least 1 or 5%
                        lower = max(0, constant_value - lower_margin)
                        upper = constant_value + upper_margin
                        return math.floor(lower), math.ceil(upper)
                        
                # Add margins to account for variability
                lower = max(0, min_val - (min_val * 0.05))
                upper = max_val + (max_val * 0.05) if max_val > 0 else 1
                return math.floor(lower), math.ceil(upper)
            except:
                # Last resort fallback
                return 0, 1