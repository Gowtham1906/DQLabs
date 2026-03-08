import warnings
import sys
import pandas as pd
import numpy as np
from dqlabs.utils.semantics.semantic_functions import (
    sorted_dict,
    get_dict_stat_range,
    enum_value_in_data,
)

warnings.filterwarnings(
    "ignore",
    ".*The frame.append method is deprecated and will be removed from pandas in a future version. Use pandas.concat instead.*",
)


class PreProcessing:
    def __init__(self, dataframe: pd.DataFrame) -> None:

        try:
            self.dataframe = dataframe
            columns = list(dataframe.columns)
            self.value = np.array(self.dataframe["value"]) if "value" in columns else np.array([])
            self.valid_count = np.array(self.dataframe["valid_count"]) if "valid_count" in columns else np.array([])
            self.invalid_count = np.array(self.dataframe["invalid_count"]) if "invalid_count" in columns else np.array([])

        except Exception as e:
            raise ValueError(
                f" The valid_count and invalid_count are not present in the dataframe {str(e)}"
            )

    def add_columns(self) -> pd.DataFrame:
        """
        Adding the total_count, percentage_valid to the dataframe
        This will help in filtering out different types of rules for different categorties
        from the rules class. (e.g. def_ssn(dict_rules): )

        """
        self.data = self.dataframe

        # generate total_count
        self.total_count = self.valid_count + self.invalid_count
        self.data["total_count"] = pd.Series(self.total_count)

        # generate percentage_change_valid
        try:
            np.seterr(divide="ignore", invalid="ignore")  # "dividebyzero defaulted"
            np.errstate(
                divide="ignore", invalid="ignore"
            )  # "ignoring null values for frequency and min_lenght and max_length"
            np.set_printoptions(
                suppress=True
            )  # Returns a round value instead of scientific exponential values
            self.percentage_valid = (self.valid_count / self.total_count) * 100
            self.data["percentage_valid"] = pd.Series(self.percentage_valid)
        except Exception:
            raise ZeroDivisionError("ZeroDivisionError")

        return self.data


class TypeFeatureModify(PreProcessing):

    default:int = 0    #default value for statistical measures
    thresh:int = 67    #default threshold of 67 percent

    def _boolean_valid_frequency(self,dataframe:pd.DataFrame)-> pd.DataFrame:
        """
        Filtering out frequency values above threshold value of 67%
        """
        pd.options.mode.chained_assignment = None #default = 'warn'
        self._df_freq = PreProcessing(dataframe).add_columns()
        try:
            if 'numeric' in self._df_freq['freq'].values:
                self._df_freq = self._df_freq[self._df_freq['freq']=='numeric']

                #defining frequency conditions
                self.conditions = [self._df_freq['percentage_valid'] > self.thresh,
                                    self._df_freq['percentage_valid'] < self.thresh]   

                #defining choices
                self.choices = [1,0] # 1 is True and 0 is False
                self._df_freq['boolean_valid'] = np.select(self.conditions, self.choices, default=0)

            else:
                self._df_freq = self._df_freq.head(0)
        except Exception as e:
            pass
            #logerror(f'Frequency value not present in dataframe',e)
        return self._df_freq
        
        
    
    def _boolean_valid_health(self, dataframe:pd.DataFrame)-> pd.DataFrame:
        """
        Filtering out primary health measures ['duplicate_count', 'blank_count', 'max_length', 'max_value', 'min_length', 'duplicate', 'distinct_count',
                                                'null_count', 'freshness', 'min_value', 'zero_values', 'row_count', 'column_count']
        """
        pd.options.mode.chained_assignment = None #default = 'warn'
        self._df_health = PreProcessing(dataframe).add_columns()

        try:
            if 'health' in self._df_health['type'].values:
                self._df_health = self._df_health[self._df_health['type']=='health']

                #defining health conditions
                self.conditions = [self._df_health['name'].eq('null_count') & self._df_health['percentage_valid']> self.default,
                                   self._df_health['name'].eq('blank_count') & self._df_health['percentage_valid']> self.default,
                                   self._df_health['name'].eq('distinct_count') & self._df_health['percentage_valid']> self.default,
                                   self._df_health['name'].eq('duplicate') & self._df_health['percentage_valid']> self.default ]

                #defining health choices
                self.choices = [1,1,1,1] # 1 is True and 0 is Falss
                self._df_health['boolean_valid'] = np.select(self.conditions, self.choices, default=0)
            else:
                self._df_health = self._df_health.head(0)
        
        except Exception as e:
            #logerror(f'Health value not present in dataframe',e)
            pass
            
        return self._df_health

    
    def _boolean_valid_pattern(self, dataframe:pd.DataFrame)-> pd.DataFrame:
        """
        Filtering out pattern measures[Empty, Text Only, Numeric Only, With Space, Alpha Numerics, With Special Characters]

        """
        pd.options.mode.chained_assignment = None #default = 'warn'
        self._df_pattern = PreProcessing(dataframe).add_columns()

        try:    
            if 'pattern' in self._df_pattern['category'].values:
                self._df_pattern = self._df_pattern[self._df_pattern['category']=='pattern']
                self._df_pattern['boolean_valid'] = np.where(self._df_pattern['valid_count']==0, 0, 1) # 1 is True and 0 is False
            else:
                self._df_pattern = self._df_pattern.head(0)
        
        except Exception as e:
            #logerror(f'Pattern value not present in dataframe',e)
            pass

        return self._df_pattern

    

    def _boolean_valid_distribution(self, dataframe:pd.DataFrame)-> pd.DataFrame:
        """
        Filtering out pattern measures[Empty, Text Only, Numeric Only, With Space, Alpha Numerics, With Special Characters]

        """
        pd.options.mode.chained_assignment = None #default = 'warn'
        self._df_distribution = PreProcessing(dataframe).add_columns()

        try:    
            if 'distribution' in self._df_distribution['type'].values:
                self._df_distribution = self._df_distribution[self._df_distribution['type']=='distribution']
                self._df_distribution['boolean_valid'] = np.where(self._df_distribution['valid_count']==0, False, True) # 1 is True and 0 is False

        
        except Exception as e:
            #logerror(f'Distribution value not present in dataframe',e)
            pass

        return self._df_distribution
    


    def _boolean_valid_enum(self, dataframe:pd.DataFrame)-> pd.DataFrame:
        """ Filtering out enum measures """

        pd.options.mode.chained_assignment = None #default = 'warn'
        self._df_enum = PreProcessing(dataframe).add_columns()
        if 'type' not in self._df_enum.columns:
            return self._df_enum
            
        if 'enum' in self._df_enum['type'].values:
            self._df_enum = self._df_enum.loc[self._df_enum['type'] == 'enum']
            self._df_enum = self._df_enum.loc[self._df_enum['freq'] == 'enum']
        else:
            self._df_enum = self._df_enum.head(0)
        if not self._df_enum.empty:    
            if 'enum' in self._df_enum['type'].values:
                self._df_enum['boolean_valid'] = np.where(self._df_enum['valid_count']>0, True, False) # 1 is True and 0 is False
        
        return self._df_enum
    
class GetMeasures(TypeFeatureModify):
    def __init__(self,dataframe:pd.DataFrame):
        self._df = dataframe
        self._df_health = TypeFeatureModify(self._df)._boolean_valid_health(dataframe)
        self._df_enum = TypeFeatureModify(self._df)._boolean_valid_enum(dataframe)
        self._df_pattern = TypeFeatureModify(self._df)._boolean_valid_pattern(dataframe)
        self._df_frequency = TypeFeatureModify(self._df)._boolean_valid_frequency(dataframe)

    @staticmethod
    def get_statistical_measures(dataframe: pd.DataFrame) -> dict:
        """
        Get all statistical measures from dataframe : [min_length, max_length, skewness, kurtosis, stddev, mean]

        """
        _df = dataframe
        try:
            if "min_length" or "max_length" in _df["name"].values():
                _min_length_df = (_df.loc[_df.name == "min_length", ["value"]])
                _min_length = 0
                if not _min_length_df.empty:
                    _min_length = _min_length_df.iloc[0]
                    _min_length = int(_min_length) if not _min_length.empty else 0

                _max_length_df = (_df.loc[_df.name == "max_length", ["value"]])
                _max_length = 0
                if not _max_length_df.empty:
                    _max_length = _max_length_df.iloc[0]
                    _max_length = int(_max_length) if not _max_length.empty else 0

                statistical_measures_dict = dict(
                    min_length=_min_length, max_length=_max_length
                )
                sorted_statistical_measures = sorted_dict(statistical_measures_dict)
        except Exception:
            raise ValueError("Statistical values not present in dataframe")

        return sorted_statistical_measures

    def get_health_measures(self) -> dict:
        """
        Get all health measures from dataframe : [null_count, blank_count, distinct_count, duplicate, zero_values,*max_value, *min_value]
        -- * used for text datatype

        """
        _df = self._df_health
        _null_count, _blank_count, _distinct_count, _duplicate,_zero_values= 0,0,0,0,0
        try:
            if 'null_count' in _df.name:
                _null_count = (
                    _df.loc[_df.name == "null_count", ["boolean_valid"]]
                ).iloc[0][0]
            if 'blank_count' in _df.name:
                _blank_count = (
                    _df.loc[_df.name == "blank_count", ["boolean_valid"]]
                ).iloc[0][0]
            if 'distinct_count' in _df.name:    
                _distinct_count = (
                    _df.loc[_df.name == "distinct_count", ["boolean_valid"]]
                ).iloc[0][0]
            if 'duplicate' in _df.name:
                _duplicate = (
                    _df.loc[_df.name == "duplicate", ["boolean_valid"]]
                ).iloc[0][0]
            if 'zero_values' in _df.name:
                _zero_values = (
                    _df.loc[_df.name == "zero_values", ["boolean_valid"]]
                ).iloc[0][0]
            _health_measures_dict = dict(
                is_null=_null_count,
                is_blank=_blank_count,
                is_distinct=_distinct_count,
                is_duplicate=_duplicate,
                zero_values=_zero_values
            )
            sorted_health_measures = sorted_dict(_health_measures_dict)
        except Exception:
            raise ValueError("Health values not present in dataframe")

        return sorted_health_measures

    def get_pattern_measures(self) -> dict:
        """
        Get all pattern measures from dataframe : [Alpha Numerics, Empty, Numeric Only, With Space, With Special Characters ]

        """
        _df = self._df_pattern
        _df['name'] = _df["name"].apply(lambda x:x.lower())
        try:
            if "alpha_numerics" in _df["name"].values:
                _alpha_numerics = int(
                    (
                        _df.loc[_df.name == "alpha_numerics", ["boolean_valid"]]
                    ).iloc[0][0]
                )
            else:
                _alpha_numerics = 0
            if "empty" in _df["name"].values:
                _empty = int(
                    (_df.loc[_df.name == "empty", ["boolean_valid"]]).iloc[0][0]
                )
            else:
                _empty = 0
            if "numeric_only" in _df["name"].values:
                _numeric_only = int(
                    (
                        _df.loc[_df.name == "numeric_only", ["boolean_valid"]]
                    ).iloc[0][0]
                )
            else:
                _numeric_only = 0
            if "text_only" in _df["name"].values:
                _text_only = int(
                    (_df.loc[_df.name == "text_only", ["boolean_valid"]]).iloc[
                        0
                    ][0]
                )
            else:
                _text_only = 0
            if "with_space" in _df["name"].values:
                _with_space = int(
                    (_df.loc[_df.name == "with_space", ["boolean_valid"]]).iloc[
                        0
                    ][0]
                )
            else:
                _with_space = 0
            if "with_special_characters" in _df["name"].values:
                _with_special_char = int(
                    (
                        _df.loc[
                            _df.name == "with_special_characters",
                            ["boolean_valid"],
                        ]
                    ).iloc[0][0]
                )
            else:
                _with_special_char = 0
            if "with_diacritics" in _df["name"].values:
                _with_diacritics = int(
                    (
                        _df.loc[
                            _df.name == "with_diacritics",
                            ["boolean_valid"],
                        ]
                    ).iloc[0][0]
                )
            else:
                _with_diacritics = 0
        
            if "valid" in _df["name"].values:
                _with_valid = int(
                    (
                        _df.loc[
                            _df.name == "valid",
                            ["boolean_valid"],
                        ]
                    ).iloc[0][0]
                )
            else:
                _with_valid = 0
                
            _pattern_dict = dict(
                alpha_numerics=_alpha_numerics,
                empty=_empty,
                numeric_only=_numeric_only,
                text_only=_text_only,
                with_space=_with_space,
                with_special_characters=_with_special_char,
                with_diacritics=_with_diacritics,
                with_valid=_with_valid
            )
            sorted_pattern_measures = sorted_dict(_pattern_dict)
        except Exception:
            raise ValueError("Pattern values not present in dataframe")

        return sorted_pattern_measures


    def get_enum_score(self, enum_list: list) -> float:
        """
        Get the enum score for categorical values that consist of enum values
        """
        _df = self._df_enum
        matching_score = 0
        _df["name"] = _df["name"].apply(lambda x: x.lower())
        _data_enum_list = _df["name"].to_list()
        _enum_list = enum_list
        final_list = enum_value_in_data(data_list=_data_enum_list, enum_list=_enum_list)
        total_enums = len(_enum_list)
        matching_score = len(final_list) / total_enums if total_enums else 0
        return matching_score

    def get_frequency_score(self, rules_dict: dict) -> float:
        """
        Get all frequency measures from dataframe

        """
        import sys
        _df = self._df_frequency
        
        """ Extracting only the integer values from the frequency dataframe"""
        integer_series = [int(value) for value in _df.name.values if (value)]
        freq_series = [value for value in integer_series if value <= sys.maxsize]
        len_integer, len_freq = len(integer_series), len(freq_series)
        if len_freq < len_integer:
            for value in range(0, len_integer - len_freq):
                freq_series.append(value)
        _df['updated_freq'] = freq_series
        
        matching_score = 0  # default
        _scoring_params = ["boolean_valid"]
        _range_measure = get_dict_stat_range(
        rules_dict
        )  # getting the range from min_length and max_length

        _df = _df.loc[:, ["updated_freq", "boolean_valid"]]
        _df.reset_index(inplace=True)
        if 0 in _df.updated_freq.values:
            _zero_val_index = _df.index[_df["updated_freq"] == 0].to_list()[
            0
            ]  # retreive the index of zero values

            if _df["boolean_valid"].iloc[_zero_val_index] == 0:
                matching_score += 1

            _drop_zero_df = _df.drop(
            [_zero_val_index]
            )  # dropping zero value from table so that we can calculate the remaining freq score
            _freq_measure = _drop_zero_df["updated_freq"].to_list()
            _bool_measure = _drop_zero_df["boolean_valid"].to_list()
        else:
            _freq_measure = _df.updated_freq.values
            _bool_measure = _df.boolean_valid.values
            
        for num in _freq_measure:
            if num in _range_measure:
                matching_score += 1
        
        matching_score = matching_score / len(_freq_measure)

        # computing the average score from zero_count, freq_count and boolean_count
        return matching_score