import pandas as pd
import numpy as np
from scipy import stats
from prophet import Prophet
import pmdarima as pmd
import math
from math import isnan
from copy import deepcopy
from dqlabs.app_helper.log_helper import log_error,log_info

from dqlabs.utils.drift import generate_prophet_dates

# install and import library for the facebook prophet library
# from prophet import Prophet

# install and import for SARIMAX
import statsmodels.api as sm

class Evaluate_Performance(object):
  def __init__(self,y_true,y_pred):
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
    mape = np.mean(np.abs((y_true- y_pred)/y_true))*100
    return mape
  
  def run(self):
    return {'mae':self.mae(),
            'mse':self.mse(),
            'rmse':self.rmse(),
            'mape':self.mape()}

class BehavioralModel:
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
    
    def get_threshold(self, pred: float, std:float)-> tuple:
        """
        Generate model threshold based on the forecasted value 
        """
        lower_threshold, upper_threshold = 0,0
        if 0 <= std < 100:
          lower_threshold, upper_threshold = pred - (0.1 * pred), pred + (0.1 * pred) 
        elif 100 <= std < 500:
          lower_threshold, upper_threshold = pred - (0.15 * pred), pred + (0.15 * pred) 
        elif 500 <= std < 1000:
          lower_threshold, upper_threshold = pred - (0.20 * pred), pred + (0.20 * pred) 
        else:
          lower_threshold, upper_threshold = pred - (0.25 * pred), pred + (0.25 * pred) 
        return lower_threshold, upper_threshold
    
    # Function to clean out-of-bounds dates
    def clean_dates(self,df, column):
        valid_dates = []
        for date in df[column]:
            try:
                valid_date = pd.to_datetime(date)
                valid_dates.append(valid_date)
            except (pd._libs.tslibs.np_datetime.OutOfBoundsDatetime, ValueError):
                valid_dates.append(pd.NaT)
        df[column] = valid_dates
        return df
    
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
                        interval_width=0.97,
                        yearly_seasonality="auto",
                        weekly_seasonality="auto",
                        daily_seasonality="auto",
                        seasonality_mode="additive",
                        changepoint_prior_scale=0.001,
                        seasonality_prior_scale=1.0,)
        
        """ Fitting the model to the dataframe"""
        ds = generate_prophet_dates(len(sorted_values))
        df = pd.DataFrame({"ds": ds, "y": sorted_values})
        df = self.clean_dates(df, 'ds') # Clean the 'ds' column
        df = df.dropna(subset=['ds']) # Filter out NaT values if needed
        
        df.reset_index()
        prophet_model = model_fit.fit(df, algorithm='Newton')
        future_dates = prophet_model.make_future_dataframe(periods=1).iloc[-1:]
        prediction = prophet_model.predict(future_dates)
        lower_threshold, upper_threshold = float(prediction['yhat_lower']), float(prediction['yhat_upper'])
        
        return lower_threshold, upper_threshold

    def autoarima(self,metric_values:np.array):
        """
        An autoregressive integrated moving average, or ARIMA, is a statistical analysis model that 
        uses time series data to either better understand the data set or to predict future trends. 
        A statistical model is autoregressive if it predicts future values based on past values. 
        """
        autoarima_model = pmd.auto_arima(metric_values, 
                              start_p=0, 
                              start_q=0,
                              test="kpss",
                              trace=True,
                              max_p=5,
                              max_q=5,
                              m=1,
                              seasonal=False,
                              error_action="ignore",
                              stepwise=True)
        return autoarima_model
    
    def ewma(self, metrics: list, alpha:float=0.02) -> tuple:
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
        alpha = alpha                            # significance level = 3%-7%
        df = len(value_arr) - 1                  # degress of freedom = 20
        t_stat = stats.t.ppf(1 - alpha/2, df)    # t-critical value for 95% CI = 2.093
        stdev = np.std(value_arr, ddof=1)        # sample standard deviation = 2.502
        n = len(value_arr)                       # no of observations

        """
        Computing the threshold with 95% confidence
        """
        lower_threshold = np.mean(value_arr) - (t_stat * stdev / np.sqrt(n))
        upper_threshold = np.mean(value_arr) + (t_stat * stdev / np.sqrt(n))   
        
        return lower_threshold, upper_threshold

    def sarimax(self, metrics: list) -> tuple:

        """
        SARIMAX will run for datapoints greater than 365+ datapoints
        """
        value_series = pd.Series(metrics)
        model = sm.tsa.statespace.SARIMAX(
            value_series, order=(1, 1, 1), seasonal_order=(1, 1, 0, 12)
        )
        result = model.fit()

        # forecasting the values

        prediction = result.get_forecast(step=1)
        pred_ci = prediction.conf_int()

        lower_threshold = pred_ci["lower y"][len(value_series)]
        upper_threshold = pred_ci["upper y"][len(value_series)]

        return lower_threshold, upper_threshold

    def evaluate_performance(self, metric_values: list):
        """ Pre-Processing the timestamp data to get the list of ordered timestamps"""
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
        arima_overall_score = Evaluate_Performance(list(self.y_test), arima_y_pred).run()
        arima_score = np.average(list(arima_overall_score.values()))

        """ Get the highest performance of two model scores"""
        total_score = {}
        total_score = {"prophet": prophet_score, "arima": arima_score}
        min_score = min(total_score.values())
        best_model = [
            key
            for key in total_score.keys()
            if total_score[key] == min_score
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
            forecast = float(prediction['yhat'])
            lower_threshold, upper_threshold = float(prediction['yhat_lower']), float(prediction['yhat_upper'])
        elif model_name == "arima":
            arima_model = self.autoarima(metric_values)
            """ 95% confidence interval to get the threshold values"""
            forecast, perf_int = arima_model.predict(1, return_conf_int=True, alpha=0.05)
            forecast = float(forecast[0])
            lower_threshold, upper_threshold = float(perf_int[0][0]), float(perf_int[0][1])
        """ Updating threshold when negative threshold values are calculated"""
        if lower_threshold < 0:
            lower_threshold, upper_threshold = self.get_threshold(forecast, stddev)

        return lower_threshold, upper_threshold

    
    def calc_low_length(self, values:list)-> tuple:
        """
        Calculating the threshold for highly skewed data 
        """
        values = np.array(values)
        lower_threshold = np.amin(values)
        upper_threshold = np.mean(values)
        
        return lower_threshold, upper_threshold
    
    def regenerate_values(self, values:list)->list:
        """
        Regenerate initial values and add more weights to latest values.
        This function is called when the deviation of the latest values is very low
        and prophet needs to be given updated values so that the threshold converges 
        towards latest metrics
        """
        last_val = values[-1] #the latest value of the current run
        final_values = []
        old_values = values[3:]
        for val in values[:3]:
            if val < last_val:
              val *= 1.02
              final_values.append(val)
            elif val > last_val:
              val = val - (val * 0.02)
              final_values.append(val)
            else:
                final_values.append(val)
        final_values.extend(old_values)

        return final_values
     
    def remove_outliers(self, data:list)-> list:
      """ Replace outliers with the median of the data"""
      threshold = 2
      z_scores = np.abs((data - np.mean(data)) / np.std(data))
      threshold_value = np.median(data)
      data_processed = [x if z <= threshold else threshold_value for x, z in zip(data, z_scores)]
      return data_processed
  
    def calc_thresh_med0(self,sorted_values:list)->tuple:
        """
        Threshold is calculated when the median is 0 and for different length
        of the metrics is generated 
        """
        prediction = np.array([0,0])
        if len(sorted_values) == 1:
            lower_threshold, upper_threshold = sorted_values[0], sorted_values[0] 
            prediction = [lower_threshold, upper_threshold]
        if len(sorted_values) == 2:
            lower_threshold, upper_threshold = self.calc_low_length(sorted_values)
            prediction = [lower_threshold, upper_threshold]
        if len(sorted_values) >= 3:
            lower_threshold, upper_threshold = self.calc_low_length(sorted_values)
            prediction = [lower_threshold, upper_threshold]
        return prediction
    
    def calc_thresh_med1(self,sorted_values:list, mean:float, stddev:float)->tuple:
        """
        Threshold is calculated when the median is 1 and for different length
        of the metrics is generated 
        """
        prediction = np.array([0,0])        
        if len(sorted_values) == 2:
          lower_threshold, upper_threshold = self.calc_low_length(sorted_values)
          prediction = [lower_threshold, upper_threshold]
        if len(sorted_values) ==3:
          lower_threshold, upper_threshold = self.calc_low_length(sorted_values)
          prediction = [lower_threshold, upper_threshold]
        if len(sorted_values) >=4:
          if 0 <= stddev < 2:  
            prediction = self.ewma(sorted_values)
          else:
            sorted_values = self.remove_outliers(sorted_values)
            prediction = self.prophet(sorted_values)

        return prediction
    
    def run(self, metric_values: list) -> tuple:
        #default value for lower and upper threshold
        lower_threshold, upper_threshold = 0,0
        metrics = [
                    float(metric.get("value"))
                    for metric in metric_values
                    if metric.get("value")
                ]
        if not metrics:
           return lower_threshold, upper_threshold
        stddev = np.std(metrics) 
        
        """ If all the values are same this will cumulatively save time"""
        metric_similar_value = metrics[0] #if all the values are equal to len of that list ex.[532.0,532.0,532.0]
        if metrics.count(metric_similar_value) == len(metrics):
            lower_threshold, upper_threshold = metric_similar_value, metric_similar_value
            return int(lower_threshold), int(upper_threshold)
        else:
            stddev = 0
            median = 0
            mean=0
            if len(metrics) > 1:
                stddev = np.std(metrics)
                mean = np.mean(metrics)
                median = np.median(metrics)

            prediction = np.array([0,0]) #default prediction is 0,0 (lt,ut)
            
            """
            Filtering based on length of values and median (skewness and kurtosis)
            """
            
            if 0 <= stddev < 2:
                start = pd.Timestamp.now()
                if median == 0:
                    prediction = self.calc_thresh_med0(metrics)
                else:
                    prediction = self.calc_thresh_med1(metrics, mean=mean, stddev=stddev)
            else:
                start = pd.Timestamp.now()
                if median == 0:
                    prediction = self.calc_thresh_med0(metrics)
                else:
                    prediction = self.calc_thresh_med1(metrics, mean=mean, stddev=stddev)
                
            if any( val < 0 for val in metrics) == True:
                prediction = self.ewma(metrics)
                lower_threshold, upper_threshold = (np.amin(prediction)), (np.amax(prediction))
            elif any(val == 0 for val in metrics) == True:
                lower_threshold, upper_threshold = (np.amin(prediction)), (np.amax(prediction))
            else:
                lower_threshold, upper_threshold = abs(np.amin(prediction)), abs(np.amax(prediction))
          
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
        
        #condition when lower threshold is negative and upper_threshold is positive
        lower_threshold = 0 if (lower_threshold < 0) and (upper_threshold >= 0) else lower_threshold    
            
        return lower_threshold, upper_threshold
