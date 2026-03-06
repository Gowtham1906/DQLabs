import pandas as pd
import numpy as np
from scipy import stats
from prophet import Prophet
import pmdarima as pmd
import math
from math import isnan
from copy import deepcopy

from dqlabs.utils.drift import (
    generate_prophet_dates,
    classify_list,
    is_seasonal_series,
)
from statsmodels.tsa.ar_model import AutoReg
from dqlabs.app_helper.log_helper import log_info

# install and import library for the facebook prophet library
# from prophet import Prophet

# install and import for SARIMAX

import warnings
from typing import List, Tuple
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.statespace.sarimax import SARIMAX
from scipy.stats import variation

warnings.filterwarnings("ignore")


class Evaluate_Performance(object):
    def __init__(self, y_true, y_pred):
        """Evaluate model performance based on predicted values
        and actual values

        Params:
            y_true (array): Actual array values of the test set
            y_pred (array): Predicted array values from the model
        """
        self.y_true = y_true
        self.y_pred = y_pred

    def mae(self):
        """
        MAE : Mean Absolute Error
        mean absolute error (MAE) is a measure of errors between
        predicted observations to the actual observations.
        """
        y_true, y_pred = np.array(self.y_true), np.array(self.y_pred)
        return np.mean(np.abs(y_true - y_pred))

    def mse(self):
        """
        MSE : Mean Squared Error
        The mean squared error or mean squared deviation of an estimator measures
        the average of the squares of the errors—that is,
        the average squared difference between the predicted values and the actual value.
        """
        y_true = np.array(self.y_true)
        y_pred = np.array(self.y_pred)
        differences = np.subtract(y_true, y_pred)
        squared_differences = np.square(differences)
        return squared_differences.mean()

    def rmse(self):
        """
        RMSE: Root Mean Square Error
        The root-mean-square deviation or root-mean-square error is a frequently used measure
        of the differences between values predicted by a model
        or an estimator and the values observed.
        """
        return math.sqrt(self.mse())

    def mape(self):
        """
        MAPE : Mean Absolute Percentage Error
        The MAPE — also called the mean absolute percentage deviation
        (MAPD) — measures accuracy of a forecast system.
        It measures this accuracy as a percentage, and can be calculated as the
        average absolute percent error for each time period minus actual
        values divided by actual values.
        """
        y_true, y_pred = np.array(self.y_true), np.array(self.y_pred)
        mape = np.mean(np.abs((y_true - y_pred) / y_true)) * 100
        return mape

    def run(self):
        return {
            "mae": self.mae(),
            "mse": self.mse(),
            "rmse": self.rmse(),
            "mape": self.mape(),
        }


class UnivariateModel:
    def __init__(self) -> None:
        """
        Params:
        timestamps: Created Date timestamps for time series analysis
        metric_values: Univariate values for which time series prediction is
        generated
        """
        pass

    def train_test_split(self, timestamps: list, metric_values: list):
        """Pre-Processing entire dataframe"""
        self.timesteps = np.array(timestamps)
        self.value = np.array(metric_values)

        """Create train and test splits the right way for time series data"""
        self.split_size = int(0.95 * len(self.value))  # 95% train, 5% test
        self.X_train, self.y_train = (
            self.timesteps[: self.split_size],
            self.value[: self.split_size],
        )
        self.X_test, self.y_test = (
            self.timesteps[self.split_size :],
            self.value[self.split_size :],
        )

        return self.X_train, self.y_train, self.X_test, self.y_test

    def get_threshold(self, pred: float, std: float) -> tuple:
        """
        Generate model threshold based on the forecasted value
        """
        lower_threshold, upper_threshold = 0, 0
        if 0 <= std < 100:
            lower_threshold, upper_threshold = pred - (0.1 * pred), pred + (0.1 * pred)
        elif 100 <= std < 500:
            lower_threshold, upper_threshold = (
                pred - (0.15 * pred),
                pred + (0.15 * pred),
            )
        elif 500 <= std < 1000:
            lower_threshold, upper_threshold = (
                pred - (0.20 * pred),
                pred + (0.20 * pred),
            )
        else:
            lower_threshold, upper_threshold = (
                pred - (0.25 * pred),
                pred + (0.25 * pred),
            )
        return lower_threshold, upper_threshold

    def prophet(self, sorted_values: list):
        """
        Prophet will run when we have 100-365 datapoints

        prophet Dataframe format:

        [index, ds, y]

        prophet requires the dataframe with index, ds and y as the columns.
        before passing the dataframe , it has to be cleaned to reach the above format.

        """

        model_fit = Prophet(
            growth="linear",
            interval_width=1.0,
            yearly_seasonality=False,  # or True if data spans over a year
            weekly_seasonality=False,  # set to True only if meaningful
            daily_seasonality=False,  # set to True only for high-frequency (hourly/minutely) data
            seasonality_mode="additive",
            changepoint_prior_scale=0.01,  # More flexibility for extreme deviations
            seasonality_prior_scale=0.01,  # Strengthen seasonal patterns
            changepoint_range=0.9,  # Allow trend shifts across 90% of data)
        )

        """ Fitting the model to the dataframe"""
        ds = generate_prophet_dates(len(sorted_values))
        df = pd.DataFrame({"ds": ds, "y": sorted_values})
        df.reset_index()
        prophet_model = model_fit.fit(df, algorithm="Newton")
        future_dates = prophet_model.make_future_dataframe(periods=1).iloc[-1:]
        prediction = prophet_model.predict(future_dates)
        lower_threshold, upper_threshold = (
            float(prediction["yhat_lower"]),
            float(prediction["yhat_upper"]),
        )
        log_info(("Prophet lower_threshold: ", lower_threshold))
        log_info(("Prophet upper_threshold: ", upper_threshold))
        # Calculate the forecasted value for prophet based on the prediction
        forecasted_value = float(prediction["yhat"])
        log_info(("Prophet forecasted_value: ", forecasted_value))
        return lower_threshold, upper_threshold

    def autoreg(self, sorted_values: list):
        # Fit the model with lag of 1
        model = AutoReg(sorted_values, lags=1).fit()

        # Forecast the next value
        forecast = model.predict(start=len(sorted_values), end=len(sorted_values))[0]

        # Estimate residual variance
        std_error = np.std(model.resid)

        # Calculate confidence interval (95% confidence)
        z_score = 1.96
        upper_bound = forecast + z_score * std_error
        lower_bound = max(
            0, forecast - z_score * std_error
        )  # Ensure lower bound is non-negative

        # Round bounds to two decimal places
        upper_bound = round(upper_bound, 2)
        lower_bound = round(lower_bound, 2)

        return lower_bound, upper_bound

    def autoarima(self, metric_values: np.array):
        """
        An autoregressive integrated moving average, or ARIMA, is a statistical analysis model that
        uses time series data to either better understand the data set or to predict future trends.
        A statistical model is autoregressive if it predicts future values based on past values.
        """
        autoarima_model = pmd.auto_arima(
            metric_values,
            start_p=0,
            start_q=0,
            test="kpss",
            trace=True,
            max_p=5,
            max_q=5,
            m=1,
            seasonal=False,
            error_action="ignore",
            stepwise=True,
        )
        return autoarima_model

    def ewma(self, metrics: list, alpha: float = 0.02) -> tuple:
        """
        EWMA will be run when we have 0-100 datapoints
        the value for ewma is always a series of data with the datetime removed
        datetime, y -> y is the value to be forecasted
        Note that loc is for population mean, and scale is for population standard deviation,
        and size is for number of samples to draw.
        """
        value_arr = np.array(metrics)

        """
        Compute the statisitical t-test to calculate threshold 
        """
        alpha = alpha  # significance level = 3%-7%
        df = len(value_arr) - 1  # degress of freedom = 20
        t_stat = stats.t.ppf(1 - alpha / 2, df)  # t-critical value for 95% CI = 2.093
        stdev = np.std(value_arr, ddof=1)  # sample standard deviation = 2.502
        n = len(value_arr)  # no of observations

        """
        Computing the threshold with 95% confidence
        """
        lower_threshold = np.mean(value_arr) - (t_stat * stdev / np.sqrt(n))
        upper_threshold = np.mean(value_arr) + (t_stat * stdev / np.sqrt(n))

        return lower_threshold, upper_threshold

    def sarimax(self, metrics: List[float], scale_factor: float = 2.5) -> Tuple[float, float]:
        """ SARIMAX model is used for forecasting time series data"""
        forecaster = AutoARIMAForecaster(metrics)
        forecaster.fit()
        forecast, lower_threshold, upper_threshold = forecaster.robust_forecast(
            scale_factor=scale_factor
        )
        return lower_threshold, upper_threshold

    def evaluate_performance(self, metric_values: list):
        """Pre-Processing the timestamp data to get the list of ordered timestamps"""
        timestamps = generate_prophet_dates(len(metric_values))
        self.X_train, self.y_train, self.X_test, self.y_test = self.train_test_split(
            timestamps, metric_values
        )

        """Prophet performance"""
        df = pd.DataFrame({"ds": self.X_train, "y": self.y_train})
        prophet_model = self.prophet(df)
        prediction = prophet_model.predict(pd.DataFrame({"ds": self.X_test}))
        prophet_y_pred = prediction["yhat"]
        prophet_overall_score = Evaluate_Performance(
            list(self.y_test), prophet_y_pred
        ).run()
        prophet_score = np.average(list(prophet_overall_score.values()))

        """ Arima Performance"""
        arima_model = self.autoarima(self.y_train)
        predictions = arima_model.predict(len(self.y_test))
        arima_y_pred = list(predictions)
        arima_overall_score = Evaluate_Performance(
            list(self.y_test), arima_y_pred
        ).run()
        arima_score = np.average(list(arima_overall_score.values()))

        """ Get the highest performance of two model scores"""
        total_score = {}
        total_score = {"prophet": prophet_score, "arima": arima_score}
        min_score = min(total_score.values())
        best_model = [
            key for key in total_score.keys() if total_score[key] == min_score
        ]

        return best_model[0]

    def best_model(self, metric_values: list) -> tuple:
        model_name = self.evaluate_performance(metric_values)
        stddev = np.std(metric_values)
        lower_threshold, upper_threshold = 0, 0
        ds = generate_prophet_dates(len(metric_values))
        if model_name == "prophet":
            df = pd.DataFrame({"ds": ds, "y": metric_values})
            df.reset_index()
            prophet_model = self.prophet(df)
            future_dates = prophet_model.make_future_dataframe(periods=1).iloc[-1:]
            prediction = prophet_model.predict(future_dates)
            forecast = float(prediction["yhat"])
            lower_threshold, upper_threshold = (
                float(prediction["yhat_lower"]),
                float(prediction["yhat_upper"]),
            )
        elif model_name == "arima":
            arima_model = self.autoarima(metric_values)
            """ 95% confidence interval to get the threshold values"""
            forecast, perf_int = arima_model.predict(
                1, return_conf_int=True, alpha=0.05
            )
            forecast = float(forecast[0])
            lower_threshold, upper_threshold = (
                float(perf_int[0][0]),
                float(perf_int[0][1]),
            )
        """ Updating threshold when negative threshold values are calculated"""
        if lower_threshold < 0:
            lower_threshold, upper_threshold = self.get_threshold(forecast, stddev)

        return lower_threshold, upper_threshold

    def calc_low_length(self, values: list) -> tuple:
        """
        Calculating the threshold for highly skewed data
        """
        values = np.array(values)
        lower_threshold = np.amin(values)
        upper_threshold = np.mean(values)

        return lower_threshold, upper_threshold

    @staticmethod
    def robust_hampel_filter(
        data: list, window_size=10, n_sigmas=3, cutoff_index=None
    ) -> list:
        """
        Applies the Hampel filter to detect and replace outliers in a time series.

        The Hampel filter identifies outliers based on the median absolute deviation (MAD)
        within a sliding window. Outliers are replaced with the mean of the values in the window.

        Args:
            data (list): The input time series data as a list of numerical values.
            window_size (int, optional): The size of the sliding window to compute the MAD.
                Defaults to 10.
            n_sigmas (int, optional): The number of standard deviations (scaled by a factor)
                to use as the threshold for detecting outliers. Defaults to 3.

        Returns:
            list: A new list with outliers replaced by the mean of their respective windows.

        Notes:
            - The scale factor `k` is set to 1.4826, which is appropriate for Gaussian distributions.
            - If the MAD within a window is zero (indicating no variation), the function skips
                processing for that window.
            - The function handles edge cases by adjusting the window size at the boundaries
                of the series.

        Example:
            >>> data = [1, 2, 2, 2, 100, 2, 2, 2, 1]
            >>> robust_hampel_filter(data, window_size=3, n_sigmas=3)
            [1, 2, 2, 2, 2, 2, 2, 2, 1]
        """
        # Convert the input data to a pandas Series for easier manipulation
        try:
            series = pd.Series(data)

            n = len(series)
            new_series = series.copy()
            k = 1.4826  # Scale factor for Gaussian distribution

            # If no cutoff_index provided, default to first 80% of the data as 'old'
            if cutoff_index is None:
                cutoff_index = int(0.8 * n)

            for i in range(n):
                if i >= cutoff_index:
                    continue  # Only process "old" values before the cutoff

                start = max(0, i - window_size)
                end = min(n, i + window_size + 1)
                window = series[start:end]
                mean = window.mean()
                mad = np.mean(np.abs(window - mean))

                if mad == 0:
                    continue  # No variation in the window → skip

                threshold = n_sigmas * k * mad
                if abs(series[i] - mean) > threshold:
                    new_series[i] = mean

            new_series = new_series.to_list()
        except Exception as e:
            log_info(("Error in robust_hampel_filter: ", str(e)))
            new_series = data
        return new_series

    def calc_thresh_med0(self, sorted_values: list) -> tuple:
        """
        Threshold is calculated when the median is 0 and for different length
        of the metrics is generated
        """
        prediction = np.array([0, 0])
        if len(sorted_values) == 1:
            log_info(
                ("Low length values model will be used for 1 value with 0 deviation")
            )
            lower_threshold, upper_threshold = sorted_values[0], sorted_values[0]
            prediction = [lower_threshold, upper_threshold]
        if len(sorted_values) == 2:
            log_info(
                ("Low length values model will be used for 2 value with 0 deviation")
            )
            lower_threshold, upper_threshold = self.calc_low_length(sorted_values)
            prediction = [lower_threshold, upper_threshold]
        if len(sorted_values) >= 3:
            stddev = np.std(sorted_values)
            log_info(
                ("Low length values model will be used for >=3 value with 0 deviation")
            )
            if 0 <= stddev < 2:
                log_info(("EWMA model will be used"))
                prediction = self.ewma(sorted_values)
                return prediction
            else:
                if stddev < 500 and len(sorted_values) < 4:
                    log_info(("Autoreg model will be used"))
                    prediction = self.autoreg(sorted_values)
                    return prediction
                
                log_info(("Prophet model will be used"))
                # smoothen data
                smoothed_sorted_values = self.smoothen_data(sorted_values)
                log_info(("smoothed_sorted_values: ", smoothed_sorted_values))

                prediction = self.prophet(smoothed_sorted_values)
        return prediction

    def calc_thresh_med1(
        self, sorted_values: list, mean: float, stddev: float, is_seasonal: bool = False
    ) -> tuple:
        """
        Threshold is calculated when the median is 1 and for different length
        of the metrics is generated
        """
        prediction = np.array([0, 0])

        if len(sorted_values) == 2:
            lower_threshold, upper_threshold = self.calc_low_length(sorted_values)
            log_info(("Low length values model will be used for 2 values"))
            prediction = [lower_threshold, upper_threshold]
        if len(sorted_values) == 3:
            lower_threshold, upper_threshold = self.calc_low_length(sorted_values)
            log_info(("Low length values model will be used for 3 values"))
            prediction = [lower_threshold, upper_threshold]
        if len(sorted_values) >= 4:
            if 0 <= stddev < 2:
                log_info(("EWMA model will be used"))
                prediction = self.ewma(sorted_values)
                return prediction
            else:
                if stddev < 500 and len(sorted_values) < 4:
                    log_info(("Autoreg model will be used"))
                    prediction = self.autoreg(sorted_values)
                    return prediction
                
                if is_seasonal and len(sorted_values) >= 60:
                    log_info(("SARIMAX model will be used"))
                    prediction = self.sarimax(sorted_values)
                    return prediction
                
                # smoothen data
                smoothed_sorted_values = self.smoothen_data(sorted_values)
                log_info(("smoothed_sorted_values: ", smoothed_sorted_values))
                log_info(("Prophet model will be used"))
                
                prediction = self.prophet(smoothed_sorted_values)

        return prediction

    @staticmethod
    def smoothen_data(data: list):
        """
        Smoothens the input data using a rolling mean with a window size of 3.

        This method applies a rolling mean to the input list of numerical data
        to smooth out fluctuations. It uses a centered window of size 3 and fills
        any NaN values introduced by the rolling operation using backward fill
        followed by forward fill.

        Args:
            data (list): A list of numerical values to be smoothed.

        Returns:
            list: A list of smoothed numerical values.
        """
        try:
            smoothed_values = pd.Series(data).rolling(window=3, center=True).mean()
            # # Fill NaNs introduced by rolling
            smoothed_values = smoothed_values.fillna(method="bfill").fillna(
                method="ffill"
            )
            values = list(smoothed_values)
        except Exception as e:
            log_info(("Error in smoothen_data: ", str(e)))
            values = data
        # Remove any whitespace values
        values = [value for value in values if not pd.isna(value)]
        return values

    def run(self, metric_values: list, job_type_seasonal:bool = False) -> tuple:
        # default value for lower and upper threshold
        metric_values = [value for value in metric_values if value is not None and value != '']  # Remove any whitespace values
        log_info(("Original Metric Values: ", metric_values))
        # check for seasonality in the data
        is_seasonal = False  # default value
        if job_type_seasonal and (len(metric_values) >= 60):
            # If the job type requires seasonal analysis,we first check if the data is seasonal or not
            is_seasonal = is_seasonal_series(metric_values)

        lower_threshold, upper_threshold = 0, 0
        metrics = metric_values

        # Handle Outliers in the data
        if len(metric_values) > 5:
            metrics = self.robust_hampel_filter(
                metric_values
            )  # default float list of values
            log_info(("Outlier Handled Metric Values: ", metric_values))

        metrics = [
            0 if np.isnan(value) else value for value in metrics
        ]  # handle nan values
        if not metrics:
            return lower_threshold, upper_threshold

        stddev = np.std(metrics)

        """ If all the values are same this will cumulatively save time"""
        metric_similar_value = metrics[
            0
        ]  # if all the values are equal to len of that list ex.[532.0,532.0,532.0]
        if metrics.count(metric_similar_value) == len(metrics):
            lower_threshold, upper_threshold = (
                metric_similar_value,
                metric_similar_value,
            )
            return int(lower_threshold), int(upper_threshold)
        else:
            stddev = 0
            median = 0
            mean = 0
            if len(metrics) > 1:
                stddev = np.std(metrics)
                mean = np.mean(metrics)
                median = np.median(metrics)

            prediction = np.array([0, 0])  # default prediction is 0,0 (lt,ut)

            """
            Filtering based on length of values and median (skewness and kurtosis)
            """

            if 0 <= stddev < 2:
                if median == 0:
                    prediction = self.calc_thresh_med0(metrics)
                else:
                    prediction = self.calc_thresh_med1(
                        metrics, mean=mean, stddev=stddev, is_seasonal=is_seasonal
                    )
            else:
                if median == 0:
                    prediction = self.calc_thresh_med0(metrics)
                else:
                    prediction = self.calc_thresh_med1(
                        metrics, mean=mean, stddev=stddev,is_seasonal=is_seasonal
                    )

            lower_threshold = np.amin(prediction)
            upper_threshold = np.amax(prediction)
        lower_threshold = lower_threshold if not isnan(lower_threshold) else 0
        upper_threshold = upper_threshold if not isnan(upper_threshold) else 0
        # weightage added to values for lower standard deviation

        if 0 <= stddev < 10:
            lower_threshold -= stddev
            upper_threshold += stddev

        lower_threshold = int(math.floor(lower_threshold))
        upper_threshold = int(math.ceil(upper_threshold))

        # condition when lower_threshold is greater than upper_threshold when negative is converted to positive values
        if lower_threshold > upper_threshold:
            lt_backup = deepcopy(lower_threshold)
            ut_backup = deepcopy(upper_threshold)
            lower_threshold = ut_backup
            upper_threshold = lt_backup

        # TE use case fix for data cardinality
        data_cardinality = classify_list(metrics)
        log_info(("data_cardinality: ", data_cardinality))
        if not data_cardinality == "mixed":
            if data_cardinality == "positive":
                if lower_threshold < 0:
                    # If the lower threshold is negative, we need to bootstrap
                    # to get a more accurate lower threshold
                    log_info(
                        (
                            "Lower_threshold is negative for positive values, bootstrapping for better estimate"
                        )
                    )
                    # Bootstrap the lower threshold
                    lower_threshold = 0  # clamp -ve lower threshold to 0
            elif data_cardinality == "negative":
                if upper_threshold > 0:
                    # If the lower threshold is negative, we need to bootstrap
                    # to get a more accurate lower threshold
                    log_info(
                        (
                            "Upper_threshold is positive for negative values, bootstrapping for better estimate"
                        )
                    )
                    # Bootstrap the lower threshold
                    upper_threshold = 0  # clamp -ve lower threshold to 0
        return lower_threshold, upper_threshold


# ========== Sarima Model ==========
class DataPreprocessor:
    def __init__(self):
        self.original_data = None
        self.processed_data = None
        self.outliers_removed = 0
        self.missing_values_filled = 0

    def preprocess_data(
        self,
        data: np.ndarray,
        remove_outliers: bool = True,
        fill_missing: bool = True,
        smooth_data: bool = True,
    ) -> np.ndarray:
        self.original_data = np.array(data)
        processed = self.original_data.copy()

        if fill_missing and np.any(np.isnan(processed)):
            processed = self._fill_missing_values(processed)

        if remove_outliers:
            processed, outliers_count = self._remove_outliers(processed)
            self.outliers_removed = outliers_count

        if smooth_data:
            processed = self._smooth_data(processed)

        self.processed_data = processed
        return processed

    def _fill_missing_values(self, data: np.ndarray) -> np.ndarray:
        data = pd.Series(data).fillna(method="ffill").fillna(method="bfill").values
        self.missing_values_filled = np.sum(np.isnan(self.original_data))
        return data

    def _remove_outliers(
        self, data: np.ndarray, threshold: float = 3.0
    ) -> Tuple[np.ndarray, int]:
        Q1 = np.percentile(data, 25)
        Q3 = np.percentile(data, 75)
        IQR = Q3 - Q1
        lower = Q1 - threshold * IQR
        upper = Q3 + threshold * IQR
        mask = (data < lower) | (data > upper)
        count = np.sum(mask)
        median_val = np.median(data[~mask])
        clean = data.copy()
        clean[mask] = median_val
        return clean, count

    def _smooth_data(self, data: np.ndarray, window_size: int = 3) -> np.ndarray:
        if len(data) < window_size:
            return data
        return (
            pd.Series(data)
            .rolling(window=window_size, center=True)
            .mean()
            .fillna(method="bfill")
            .fillna(method="ffill")
            .values
        )

    def get_quality_score(self) -> float:
        if self.original_data is None or self.processed_data is None:
            return 0.0
        cv = variation(self.processed_data)
        outlier_pct = self.outliers_removed / len(self.original_data)
        missing_pct = self.missing_values_filled / len(self.original_data)
        return max(0, 1 - (cv * 0.5 + outlier_pct + missing_pct))


# ========== Seasonality Detector ==========
class AutoSeasonalityDetector:
    def __init__(self, data: np.ndarray, max_seasonality: int = 24):
        self.data = data
        self.max_seasonality = max_seasonality
        self.preprocessor = DataPreprocessor()

    def detect(self) -> Tuple[bool, int]:
        data = self.preprocessor.preprocess_data(self.data)
        for method in [self._acf, self._decomposition, self._fft]:
            seasonal, period = method(data)
            if seasonal:
                return True, period
        return False, 0

    def _acf(self, data) -> Tuple[bool, int]:
        from statsmodels.tsa.stattools import acf

        acf_vals = acf(data, nlags=min(len(data) // 2, self.max_seasonality))
        for i in range(1, len(acf_vals) - 1):
            if (
                acf_vals[i] > 0.3
                and acf_vals[i] > acf_vals[i - 1]
                and acf_vals[i] > acf_vals[i + 1]
            ):
                return True, i
        return False, 0

    def _decomposition(self, data) -> Tuple[bool, int]:
        for period in range(2, self.max_seasonality):
            try:
                result = seasonal_decompose(data, period=period)
                strength = np.std(result.seasonal) / np.std(data)
                if strength > 0.1:
                    return True, period
            except:
                continue
        return False, 0

    def _fft(self, data) -> Tuple[bool, int]:
        power = np.abs(np.fft.fft(data)) ** 2
        freqs = np.fft.fftfreq(len(data))
        power[0] = 0
        idx = np.argmax(power[1 : len(power) // 2]) + 1
        freq = freqs[idx]
        if freq > 0:
            period = int(round(1 / freq))
            if 2 <= period <= self.max_seasonality:
                return True, period
        return False, 0

# ========== Forecaster ==========
class AutoARIMAForecaster:
    def __init__(self, data: List[float]):
        self.raw_data = np.array(data)
        self.detector = AutoSeasonalityDetector(self.raw_data)
        self.model = None

    def fit(self):
        seasonal, period = self.detector.detect()
        if seasonal:
            model = SARIMAX(
                self.raw_data, order=(1, 1, 1), seasonal_order=(1, 1, 1, period)
            )
        else:
            model = ARIMA(self.raw_data, order=(1, 1, 1))
        self.model = model.fit()

    def robust_forecast(self, scale_factor: float=2.5) -> Tuple[float, float, float]:
        forecast = self.model.forecast(steps=1)
        residuals = self.model.resid
        std = np.std(residuals)
        return (
            forecast[0],
            forecast[0] - scale_factor * std,
            forecast[0] + scale_factor * std,
        )