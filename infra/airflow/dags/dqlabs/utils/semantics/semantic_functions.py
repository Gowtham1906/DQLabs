"""
Migration Notes From V2 to V3:
Migrations Completed
"""

import numpy as np
import pandas as pd
import re
import os
import json  # Ensure json is imported if used elsewhere
import logging
from uuid import uuid4
from typing import Any, Dict, Optional, List, Tuple, Union

# DQLabs imports
from dqlabs.app_helper.db_helper import execute_query, fetchone, fetchall
from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.utils import get_last_runs
from dqlabs.utils.extract_workflow import get_queries
from dqlabs.enums.connection_types import ConnectionType
from dqlabs.app_helper.dag_helper import execute_native_query

logger = logging.getLogger(__name__)


def sorted_dict(rules: dict) -> dict:
    sorted_dict = dict(sorted(rules.items(), key=lambda item: item[0]))
    return sorted_dict


# cosine similarity for numericals


def cosine_numerical(dict1: dict, dict2: dict) -> float:
    from scipy.spatial.distance import cosine
    import pandas as pd

    _series1 = pd.Series(dict1)
    _series2 = pd.Series(dict2)
    cos_num = 1 - cosine(_series1, _series2)

    return cos_num


def convert_num_bool(list: list) -> list:
    """
    Converting numerical values in dict to boolean values(True, False)
    1 = True, 0 = False
    input = [0,1,1,0]
    output = [False, True, True, False]
    """
    converted_lst = [False if val == 0 else True for val in list]
    return converted_lst


def cosine_text(a: list, b: list) -> float:
    from collections import Counter

    # count word occurrences
    a_vals = Counter(a)
    b_vals = Counter(b)

    # convert to word-vectors
    words = list(a_vals.keys() | b_vals.keys())
    a_vect = [a_vals.get(word, 0) for word in words]  # [0, 0, 1, 1, 2, 1]
    b_vect = [b_vals.get(word, 0) for word in words]  # [1, 1, 1, 0, 1, 0]

    # find cosine
    len_a = sum(av * av for av in a_vect) ** 0.5  # sqrt(7)
    len_b = sum(bv * bv for bv in b_vect) ** 0.5  # sqrt(4)
    dot = sum(av * bv for av, bv in zip(a_vect, b_vect))  # 3
    cosine = dot / (len_a * len_b) if dot and len_a and len_b else 0

    return cosine


def get_dict_stat_range(rule_dict: dict, name: str = "statistical_measures") -> range:
    """
    Getting the range of min_length and max_length for all rules
    This will be used in the matching score calculation for frequency type
    """
    lst = sorted([item for item in rule_dict.get(name).values()]) if rule_dict else []
    converted_list = []
    for item in lst:
        try:
            if item:
                if "." in item:
                    item = float(item)
                else:
                    item = int(item)
        except:
            pass
        converted_list.append(item)
    lst = converted_list

    try:
        _range = range(0)
        if len(lst) > 1:
            if lst[0] == lst[1]:
                _range = (lst[0], lst[0])
            else:
                _range = range(lst[0], lst[1] + 1)
    except ValueError:
        print("Value not present in the list")

    return _range


def get_dict_rules(rule_dict: dict, name: str) -> dict:
    """
    Retrieving different rule measures as a dict
    """
    return rule_dict.get(name) if rule_dict else {}


def enum_value_in_data(data_list: list, enum_list: list) -> list:
    """
    Retreiving all matched enum values from dataframe and wordlist
    """
    result = np.unique(
        [np.array(enum) for item in data_list for enum in enum_list if item in enum]
    )
    return result


def get_semantic_terms(config: dict) -> str:
    """
    Returns the list of semantic terms to identify
    """
    try:
        connection = get_postgres_connection(config)
        semantic_terms = []
        with connection.cursor() as cursor:
            query_string = """
                select *
                from core.terms as t
                join core.domain as g on g.id = t.domain_id
                left join core.terms_mapping on terms_mapping.term_id = t.id
                left join core.attribute as p on p.id = terms_mapping.attribute_id
                where t.status = 'Verified'
            """
            cursor = execute_query(connection, cursor, query_string)
            semantic_terms = fetchall(cursor)
        return semantic_terms
    except Exception as e:
        raise e


def get_metrics(config: dict, attribute_id: str) -> str:
    """
    Returns the list of latest metrics of an asset for the semantic analysis
    """
    try:
        asset_id = config.get("asset_id")
        connection = get_postgres_connection(config)
        _dict_runids = get_last_runs(connection, asset_id)
        run_id = _dict_runids.get("run_id")

        semantic_metrics = {}
        with connection.cursor() as cursor:
            query_string = f"""
                select met.measure_id, met.measure_name as name,
                base.type, base.category, met.value, met.valid_count,
                met.invalid_count, met.total_count
                from core.metrics as met
                join core.measure as mes on mes.id = met.measure_id
                join core.base_measure as base ON base.id = mes.base_measure_id
                join core.asset as asset on asset.id=met.asset_id
                join core.attribute on attribute.id=met.attribute_id 
                left join core.terms_mapping on terms_mapping.attribute_id = attribute.id 
                where met.asset_id='{asset_id}' and met.run_id='{run_id}' and terms_mapping.term_id is null
                and met.attribute_id='{attribute_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            semantic_metrics = fetchall(cursor)
        return semantic_metrics
    except Exception as e:
        raise e


def get_semantic_metrics(config: dict, attribute_id: str) -> str:
    """
    Returns the list of latest semantics metrics of an asset for the semantic analysis
    """
    try:
        asset_id = config.get("asset_id")
        connection = get_postgres_connection(config)
        # _dict_runids = get_last_runs(connection, asset_id)
        # run_id = _dict_runids.get("run_id")

        semantic_metrics = {}
        with connection.cursor() as cursor:
            query_string = f"""
                with semantic_table as 
                (select met.run_id,met.attribute_id,met.measure_id,attribute.name as attribute_name,met.measure_name as "name",
                met.value,met.weightage,met.total_count,met.valid_count,met.invalid_count,
                met.valid_percentage,met.invalid_percentage,met.score,
                base.category,base.type,met.created_date,
                rank() over (partition by met.run_id order by met.created_date desc) as run_rank
                from core.metrics as met
                join core.measure as mes on mes.id = met.measure_id
                join core.base_measure as base ON base.id = mes.base_measure_id
                join core.asset as asset on asset.id=met.asset_id
                join core.attribute on attribute.id=met.attribute_id
                left join core.terms_mapping on terms_mapping.attribute_id = attribute.id 
                where terms_mapping.term_id is null
                and met.asset_id = '{asset_id}'
                and met.attribute_id = '{attribute_id}'
                order by created_date desc) 
                select sem.run_id, sem.measure_id, sem.attribute_name,sem.name,sem.value, sem.weightage,
                sem.total_count, sem.valid_count, sem.invalid_count,sem.valid_percentage,sem.invalid_percentage,
                sem.score,sem.category,sem.type
                from semantic_table as sem
                where sem.run_rank = 1
            """
            cursor = execute_query(connection, cursor, query_string)
            semantic_metrics = fetchall(cursor)
        return semantic_metrics
    except Exception as e:
        log_error("get_semantic_metrics", e)
        raise e


def get_semantic_rules(config: dict) -> str:
    """
    Returns the list of latest custom model rules of an asset for the semantic analysis
    """
    try:
        connection = get_postgres_connection(config)

        semantic_metrics = {}
        with connection.cursor() as cursor:
            query_string = f"""
                with semantic_model_tbl as 
                    (select glo.name as semantic_model_name,glo.organization_id,terms.*
                    from core.terms as terms
                    left join core.domain as glo on glo.id = terms.domain_id
                    where terms.status = 'Verified')
                select sem.semantic_model_name, sem.id, sem.name, sem.status,sem.derived_type,
                sem.threshold,sem.sensitivity, sem.min_length, sem.max_length, sem.is_null,sem.is_blank,
                sem.is_unique, sem.is_primary_key,sem.enum,
                bas_mes.id,bas_mes.technical_name, bas_mes.category,bas_mes.type,bas_mes.level,bas_mes.properties,bas_mes.is_positive,
                case when sem.semantic_model_name is not null then CONCAT(sem.semantic_model_name,'.',sem.name) else sem.name end "semantic_key"
                from semantic_model_tbl as sem
                left join core.base_measure as bas_mes on bas_mes.term_id = sem.id
            """
            cursor = execute_query(connection, cursor, query_string)
            semantic_metrics = fetchall(cursor)
        return semantic_metrics
    except Exception as e:
        raise e


def has_semantic_terms(config: dict) -> str:
    """
    Returns the list of semantic terms to identify
    """
    try:
        has_semantics = False
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = """
                select count(*) as terms_count
                from core.terms where status = 'Verified'
            """
            cursor = execute_query(connection, cursor, query_string)
            terms_count = fetchone(cursor)
            terms_count = terms_count.get("terms_count", 0) if terms_count else 0
            has_semantics = terms_count > 0
    except Exception:
        has_semantics = False
    finally:
        return has_semantics


def get_contains_terms(config: dict) -> str:
    """
    Returns the list of contains keywords for semantic analysis
    """
    try:
        connection = get_postgres_connection(config)

        contains_metrics = {}
        with connection.cursor() as cursor:
            query_string = f"""
                with contain_table as 
                (select glo.name as semantic_model_name,terms.id,terms.name,terms.derived_type,terms.contains
                from core.terms as terms
                left join core.domain as glo on glo.id = terms.domain_id
                where terms.status = 'Verified')
                select ctb.semantic_model_name,ctb.id,ctb.name,ctb.derived_type,ctb.contains,
                case when ctb.semantic_model_name is not null then CONCAT(ctb.semantic_model_name,'.',ctb.name) else ctb.name end "semantic_key"
                from contain_table as ctb
                where ctb.contains is not null
            """
            cursor = execute_query(connection, cursor, query_string)
            contains_metrics = fetchall(cursor)
        return contains_metrics
    except Exception as e:
        raise e


def get_synonyms_terms(config: dict) -> str:
    """
    Returns the list of contains keywords for semantic analysis
    """
    try:
        connection = get_postgres_connection(config)

        synonyms_metrics = {}
        with connection.cursor() as cursor:
            query_string = """
                with contain_table as 
                (select glo.name as semantic_model_name,terms.id,terms.name,
                terms.derived_type,terms.synonyms
                from core.terms as terms
                left join core.domain as glo on glo.id = terms.domain_id
                where terms.status = 'Verified')
                select ctb.semantic_model_name,ctb.id,ctb.name,
                ctb.synonyms,
                case when ctb.semantic_model_name is not null then CONCAT(ctb.semantic_model_name,'.',ctb.name) else ctb.name end "semantic_key"
                from contain_table as ctb
                where ctb.synonyms is not NULL
            """
            cursor = execute_query(connection, cursor, query_string)
            synonyms_metrics = fetchall(cursor)
        return synonyms_metrics
    except Exception as e:
        raise e


def get_threshold(config: dict, semantic_name: str) -> float:
    """
    Returns the threshold percentage set by user
    """
    if "." in semantic_name:
        semantic_name = semantic_name.split(".")[-1]
    name = semantic_name
    threshold = {}
    connection = get_postgres_connection(config)
    try:
        with connection.cursor() as cursor:
            query_string = f"""
                    select gt.id, gt.name, gt.domain_id, gt.threshold
                    from core.terms as gt
                    where gt.name ilike '{name}%'
                    and status = 'Verified'
                    order by name
                """
            cursor = execute_query(connection, cursor, query_string)
            threshold = fetchone(cursor)
        return threshold
    except Exception as e:
        raise e


def update_attribute_semantic_field(
    config: dict, attribute_id: str, term: dict
) -> None:
    """
    Updates the semantic field for an attribute by mapping it to a term.
    If asset has domain mapping, validates that the term belongs to that domain.
    Uses UPDATE-first pattern to avoid shared-lock escalation deadlocks.
    """
    asset_id = config.get("asset_id")
    term_id = term.get("id")
    filter_domain_ids = config.get("_filter_domain_ids")
    
    if not term_id:
        log_info(f"No term_id provided for attribute_id '{attribute_id}', skipping update")
        return
    
    connection = get_postgres_connection(config)
    try:
        with connection.cursor() as cursor:
            if filter_domain_ids:
                domain_ids_str = "', '".join(str(did) for did in filter_domain_ids if did)
                validation_query = f"""
                    SELECT id FROM core.terms 
                    WHERE id = '{term_id}' AND domain_id IN ('{domain_ids_str}')
                """
                cursor = execute_query(connection, cursor, validation_query)
                if not fetchone(cursor):
                    return
            
            update_query = f"""
                UPDATE core.terms_mapping SET term_id = '{term_id}'
                WHERE attribute_id = '{attribute_id}'
            """
            cursor = execute_query(connection, cursor, update_query)
            if cursor.rowcount == 0:
                query_input = (str(uuid4()), attribute_id, asset_id, term_id, "Approved")
                input_literals = ", ".join(["%s"] * len(query_input))
                query_param = cursor.mogrify(f"({input_literals}, CURRENT_TIMESTAMP)", query_input).decode("utf-8")
                insert_query = f"""
                    INSERT INTO core.terms_mapping (
                        id, attribute_id, asset_id, term_id, approval_status, created_date
                    ) VALUES {query_param}
                """
                cursor = execute_query(connection, cursor, insert_query)
    except Exception as e:
        log_error(f"Error updating attribute semantic field for attribute_id '{attribute_id}'", e)
        raise e


def get_training_data(config: dict, asset: str) -> str:
    """
    Returns the list of latest metrics of an asset for the semantic analysis
    """
    try:
        asset_id = asset.get("asset_id")
        connection = get_postgres_connection(config)

        training_metrics = {}
        with connection.cursor() as cursor:
            query_string = f"""
                select *
                from core.metrics
                where asset_id = {asset_id}
            """
            cursor = execute_query(connection, cursor, query_string)
            training_metrics = fetchall(cursor)
        return training_metrics
    except Exception as e:
        raise e


def padarray(array: np.array, pad: int = 30):
    """
    Padding the numpy array if it has missing values for model prediction
    Input Parameters:
    A = [1,2,3] :list
    size = 8 :size of the array
    pad = 30 (default no of features in training model)
    Ouput:
    [1,2,3,0,0,0,0,0]
    """
    split = pad - len(array)
    return np.pad(array, pad_width=(0, split), mode="constant")


def get_pred_array(dataframe: pd.DataFrame) -> np.array:
    """
    Get the pivot data of the dataframe
    """
    df_pivot = dataframe.pivot(index="attribute_name", columns="name", values="value")
    df_pivot = df_pivot.fillna(0)

    """
    Getting only the relevant features for prediction
    """
    col_list = [
        "sum",
        "standard_deviation",
        "median",
        "mean",
        "min_length",
        "max_length",
        "min_value",
        "max_value",
        "zero_values",
        "Empty",
        "Text Only",
        "Numeric Only",
        "With Space",
        "Alpha Numerics",
        "With Special Characters",
        "row_count",
        "column_count",
        "duplicate_count",
        "freshness",
        "null_count",
        "blank_count",
        "distinct_count",
        "duplicate",
        "special_char_count",
        "characters_count",
        "whitespace_count",
        "digits_count",
        "nan_count",
        "positive_count",
        "negative_count",
    ]
    _df = df_pivot[[val for val in df_pivot.columns if val in col_list]]
    _df = _df.replace(r"^\s*$", np.nan, regex=True)
    _df = _df.fillna(0)

    """
    Getting the values in the form of an array
    """
    X_pred = np.array(_df.iloc[:, :])
    return


""" Helper functions to get the rules dynamically for custom models"""


def merge_dict(d1: dict, d2: dict) -> dict:
    merged = {
        key: (value1, value2)
        for key, value1, value2 in zip(d1.keys(), d1.values(), d2.values())
    }
    return merged


def get_min_max_range(df_datatype: pd.DataFrame) -> dict:
    pass


def get_statistical_rules(df_datatype: pd.DataFrame) -> dict:
    df_datatype.drop_duplicates(subset="name", keep="first", inplace=True)
    df = df_datatype[["semantic_key", "min_length", "max_length"]]
    if "semantic_key" in df.columns:
        semantic_name = [name for name in df.semantic_key]
    if "min_length" in df.columns:
        min_length = [min for min in df.min_length]
    if "max_length" in df.columns:
        max_length = [max for max in df.max_length]
    rules_statistical_dict = {
        key: {
            "statistical_measures": {"min_length": min_value, "max_length": max_value}
        }
        for key, min_value, max_value in zip(semantic_name, min_length, max_length)
    }
    return rules_statistical_dict


def get_health_rules(df_datatype: pd.DataFrame) -> dict:
    df_datatype.drop_duplicates(subset="name", keep="first", inplace=True)
    """ Get only the health columns and renaming is_unique to is_duplicate"""

    df = df_datatype[["semantic_key", "is_null", "is_blank", "is_unique"]]
    df.rename(columns={"is_unique": "is_duplicate"}, inplace=True)

    """ Data Preprocessing for health rules"""
    df["is_duplicate"] = df["is_duplicate"].replace(
        ["False", "True"], ["True", "False"]
    )
    df["is_null"] = df["is_null"].astype(int)
    df["is_blank"] = df["is_blank"].astype(int)
    df["is_duplicate"] = df["is_duplicate"].astype(int)

    """ Get all the key-values pairs for health measures"""
    if "semantic_key" in df.columns:
        semantic_name = [name for name in df.semantic_key]
    if "is_null" in df.columns:
        semantic_null = [name for name in df.is_null]
    if "is_blank" in df.columns:
        semantic_blank = [name for name in df.is_blank]
    if "is_duplicate" in df.columns:
        semantic_duplicate = [name for name in df.is_duplicate]
    rules_health_dict = {
        key: {
            "health_measures": {
                "is_null": null_value,
                "is_blank": blank_value,
                "is_duplicate": duplicate_value,
            }
        }
        for key, null_value, blank_value, duplicate_value in zip(
            semantic_name, semantic_null, semantic_blank, semantic_duplicate
        )
    }
    return rules_health_dict


def get_pattern_rules(df_datatype: pd.DataFrame) -> dict:
    """Generate the pivot tables to get the pattern rules"""
    rules_pattern_dict = {}
    rules_pattern_dict_filtered = {}

    """ if pattern is null"""
    df_filter = df_datatype[
        df_datatype[["technical_name", "is_positive"]].notnull().all(1)
    ]
    index_list = [idx for idx in df_filter.index]
    df_unfiltered = df_datatype.loc[~df_datatype.index.isin(index_list)]

    """ if pattern is not null"""
    filtered_df = df_datatype[
        df_datatype[["technical_name", "is_positive"]].notnull().all(1)
    ]

    if not df_unfiltered.empty:
        for key in df_unfiltered["semantic_key"].drop_duplicates():
            rules_pattern_dict.update({key: [0 for val in range(0, 33)]})

    if not filtered_df.empty:
        df_pivot = filtered_df.pivot_table(
            values=["is_positive"],
            index="semantic_key",
            columns="technical_name",
            aggfunc="first",
        )
        df_pivot = df_pivot.fillna(0)
        df_pivot = df_pivot.replace(0, False)
        df_pivot.columns = df_pivot.columns.droplevel(0)
        df_pivot.columns.name = None
        df_pivot = df_pivot.reset_index()
        for column in df_pivot.columns[1:]:
            df_pivot[column] = df_pivot[column].astype(int)
            filtered_dict = df_pivot.set_index("semantic_key").T.to_dict("list")
            rules_pattern_dict_filtered.update(filtered_dict)

    rules_pattern_dict.update(rules_pattern_dict_filtered)
    return rules_pattern_dict


def get_enum_rules(df_datatype: pd.DataFrame) -> dict:
    df_datatype.drop_duplicates(subset="name", keep="first", inplace=True)
    df = df_datatype[["semantic_key", "enum"]]
    enum_values = [enum for enum in df.enum]
    semantic_name = [name for name in df.semantic_key]
    enum_values_dict = {
        key: {"enum": enum} for key, enum in zip(semantic_name, enum_values)
    }
    return enum_values_dict


def update_semantic_job_status(config: dict, status: str) -> float:
    """
    Updates the dag status
    """
    # Pull the latest metrics
    connection = get_postgres_connection(config)
    asset = config.get("asset", {})
    if not asset:
        return
    asset_id = asset.get("id")

    with connection.cursor() as cursor:
        query_string = f"""
            update core.data set semantic_run_status='{status}' where asset_id='{asset_id}'
        """
        execute_query(connection, cursor, query_string)


def remove_special_characters(input_string):
    # Use regex to replace non-alphanumeric characters with an empty string
    input_string = input_string.lower()
    output_string = re.sub(r"[^a-zA-Z0-9]", "", input_string)
    return output_string


"""-------------------new deep profiling functions------------------"""


def get_term_attribute_profile_metrics(config: dict):
    try:
        asset_id = config.get("asset_id")
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            if asset_id:
                query_string = f"""
                    SELECT DISTINCT terms_mapping.term_id
                    FROM core.terms
                    JOIN core.terms_mapping ON terms_mapping.term_id = terms.id
                    WHERE terms.is_active = true AND terms.is_delete = false
                    AND terms_mapping.asset_id = '{asset_id}'
                """
            else:
                query_string = """
                    SELECT DISTINCT term_id FROM core.terms
                    JOIN core.terms_mapping ON terms_mapping.term_id = terms.id
                    WHERE terms.is_active = true AND terms.is_delete = false
                """
            cursor = execute_query(connection, cursor, query_string)
            terms = fetchall(cursor)
            if terms:
                terms = [term.get("term_id") for term in terms]
                for term in terms:
                    try:
                        attribute_profiling(term, connection)
                    except Exception as e:
                        log_error(f"Error profiling term {term}", e)

        return True
    except Exception as e:
        log_error("Error in get_term_attribute_profile_metrics", e)
        return False


def attribute_profiling(term_id, connection):
    try:
        aggregated_data = {}
        query = f"""
                with measure_stat as (
                    select mes.attribute_id as attr_id, sum(
                        case when mes.is_active=True and
                        mes.technical_name not in (
                            'min_value', 'max_value', 'min_length', 'max_length', 'length_range',
                            'distinct_count', 'null_count', 'zero_values', 'value_range', 'pattern',
                            'space_count', 'blank_count'
                        ) and base.type not in ('custom', 'semantic')
                        then 1 else 0 end
                    ) as active_advanced_measures
                    from core.measure as mes
                    join core.base_measure as base on base.id=mes.base_measure_id
                    group by mes.attribute_id
                )
                SELECT 
                    attribute.completeness,
                    attribute.uniqueness,
                    attribute.character_distribution,
                    attribute.space_distribution,
                    attribute.numeric_distribution,
                    attribute.row_count as total_rows,
                    measure_stat.active_advanced_measures,
                    attribute.active_measures as attribute_measures,
                    attribute.length_distribution,
                    attribute.short_universal_patterns,
                    attribute.universal_patterns,
                    attribute.derived_type,
                    attribute.value_distribution,
                    attribute.user_defined_patterns,
                    attribute.is_advanced_profiling,
                    attribute.min_value as attribute_min_value,
                    attribute.max_value as attribute_max_value,
                    attribute.min_length as attribute_min_length,
                    attribute.max_length as attribute_max_length,
                    attribute.basic_profile,
                    terms.min_length,
                    terms.max_length,
                    terms.min_value,
                    terms.max_value
                FROM core.terms_mapping
                LEFT JOIN core.terms ON terms.id = terms_mapping.term_id
                LEFT JOIN core.attribute ON attribute.id = terms_mapping.attribute_id
                LEFT JOIN measure_stat on measure_stat.attr_id = attribute.id
                WHERE terms_mapping.term_id = '{term_id}' 
                and attribute.datatype not in ('BINARY')
                and attribute.is_active = true and attribute.is_selected = true
            """

        with connection.cursor() as cursor:
            cursor = execute_query(connection, cursor, query)
            response = fetchall(cursor)
        if response:
            profiling_keys = [key for key in response[0].keys()]
            aggr_int_keys = ["total_rows", "active_advanced_measures"]
            list_based_data = []
            dict_based_data = {}
            int_based_data = {key: 0 for key in aggr_int_keys}
            length_distribution_data = []
            value_distribution_data = []
            is_numeric_column = False
            is_advanced_profiling = False

            for record in response:
                if not is_numeric_column and record.get("derived_type").lower() in [
                    "numeric",
                    "integer",
                ]:
                    is_numeric_column = True
                if not is_advanced_profiling and record.get("is_advanced_profiling"):
                    is_advanced_profiling = True
                for key in profiling_keys:
                    value = record.get(key, "")
                    if not value:
                        continue

                    try:
                        parsed_value = (
                            json.loads(value) if isinstance(value, str) else value
                        )

                        if isinstance(parsed_value, list):
                            if key == "length_distribution":
                                for entry in parsed_value:
                                    length_distribution_data.append(
                                        {
                                            "length_value": entry.get(
                                                "length_value", ""
                                            ),
                                            "value_count": entry.get("value_count", ""),
                                        }
                                    )
                            elif key == "value_distribution":
                                for entry in parsed_value:
                                    value_distribution_data.append(
                                        {
                                            "length_value": entry.get(
                                                "length_value", ""
                                            ),
                                            "value_count": entry.get("value_count", ""),
                                            "enum_value": entry.get("enum_value", ""),
                                            "is_valid": entry.get("is_valid", True),
                                            "is_user_defined": entry.get(
                                                "is_user_defined", False
                                            ),
                                            "is_term_level": entry.get(
                                                "is_term_level", False
                                            ),
                                        }
                                    )
                            else:
                                for entry in parsed_value:
                                    entry["category"] = key
                                    entry["is_valid"] = (
                                        entry["is_valid"]
                                        if "is_valid" in entry
                                        else True
                                    )
                                    entry["pattern"] = (
                                        entry["pattern"] if "pattern" in entry else ""
                                    )
                                    entry["is_user_defined"] = entry.get(
                                        "is_user_defined", False
                                    )
                                    entry["is_term_level"] = entry.get(
                                        "is_term_level", False
                                    )
                                    if key == "universal_patterns":
                                        entry["short_universal_pattern"] = entry.get(
                                            "short_universal_pattern", ""
                                        )
                                        entry["sample_value"] = entry.get(
                                            "sample_value", ""
                                        )
                                        entry["universal_pattern"] = entry.get(
                                            "universal_pattern", ""
                                        )
                                    list_based_data.append(entry)

                        elif isinstance(parsed_value, dict):
                            if key not in dict_based_data:
                                dict_based_data[key] = {}
                            for k, v in parsed_value.items():
                                if v and (str(v) != ""):
                                    v = (
                                        check_value_datatype(v)
                                        if isinstance(v, str)
                                        else v
                                    )
                                    dict_value = dict_based_data[key].get(k, 0)
                                    dict_value = (
                                        check_value_datatype(dict_value)
                                        if isinstance(dict_value, str)
                                        else dict_value
                                    )
                                    dict_based_data[key][k] = dict_value + v

                    except json.JSONDecodeError:
                        pass

                # Sum integer fields
                for key in aggr_int_keys:
                    int_based_data[key] += check_value_datatype(record.get(key, 0))

            aggregated_list_data = {}
            if list_based_data:
                df = pd.DataFrame(list_based_data)

                aggregated_list_data = {}
                for category in df["category"].unique():
                    group = df[df["category"] == category]
                    if (
                        category == "user_defined_patterns"
                        or category == "universal_patterns"
                    ):
                        agg_dict = {}

                        if "is_user_defined" in group.columns:
                            agg_dict["is_user_defined"] = "max"
                        if "is_valid" in group.columns:
                            agg_dict["is_valid"] = "min"
                        if "value_count" in group.columns:
                            agg_dict["value_count"] = "sum"
                        if "pattern" in group.columns:
                            agg_dict["pattern"] = lambda x: next(
                                (i for i in x if i), ""
                            )
                        if "is_default" in group.columns:
                            agg_dict["is_default"] = "max"
                        if "is_term_level" in group.columns:
                            agg_dict["is_term_level"] = "max"
                        if category == "universal_patterns":
                            if "universal_pattern" in group.columns:
                                agg_dict["universal_pattern"] = "first"
                            if "short_universal_pattern" in group.columns:
                                agg_dict["short_universal_pattern"] = "first"
                            if "sample_value" in group.columns:
                                agg_dict["sample_value"] = "first"

                        grouped = group.groupby("enum_value", as_index=False).agg(
                            agg_dict
                        )

                        grouped["category"] = category
                        aggregated_list_data[category] = grouped.to_dict(
                            orient="records"
                        )
                    else:
                        grouped = group.groupby("enum_value", as_index=False).agg(
                            {"value_count": "sum", "is_valid": "min"}
                        )
                        grouped["category"] = category
                        aggregated_list_data[category] = grouped.to_dict(
                            orient="records"
                        )

            # Aggregate length_distribution
            if length_distribution_data:
                df_length = pd.DataFrame(length_distribution_data)
                aggregated_length_distribution = (
                    df_length.groupby("length_value", as_index=False)["value_count"]
                    .sum()
                    .to_dict(orient="records")
                )
                aggregated_list_data["length_distribution"] = (
                    aggregated_length_distribution
                )

            # Aggregate length_distribution
            if value_distribution_data:
                df_val_dist = pd.DataFrame(value_distribution_data)
                agg_dict = {"value_count": "sum"}

                if "is_valid" in df_val_dist.columns:
                    agg_dict["is_valid"] = "min"
                if "is_user_defined" in df_val_dist.columns:
                    agg_dict["is_user_defined"] = "max"
                if "pattern" in df_val_dist.columns:
                    agg_dict["pattern"] = "first"
                if "name" in df_val_dist.columns:
                    agg_dict["name"] = "first"
                if "is_term_level" in df_val_dist.columns:
                    agg_dict["is_term_level"] = "max"

                grouped = df_val_dist.groupby("enum_value", as_index=False).agg(
                    agg_dict
                )
                aggregated_value_distribution = grouped.to_dict(orient="records")
                aggregated_list_data["value_distribution"] = (
                    aggregated_value_distribution
                )

            aggregated_data = {
                **aggregated_list_data,
                **dict_based_data,
                **int_based_data,
                "derived_type": "Numeric" if is_numeric_column else "Text",
                "is_advanced_profiling": is_advanced_profiling,
                "term_id": term_id,
                "min_value": get_min_max_values(response, "min_value")
                if (not response[0].get("min_value", ""))
                else response[0].get("min_value", ""),
                "max_value": get_min_max_values(response, "max_value")
                if (not response[0].get("max_value", ""))
                else response[0].get("max_value", ""),
                "min_length": get_min_max_values(response, "min_length")
                if (not response[0].get("min_length", ""))
                else response[0].get("min_length", ""),
                "max_length": get_min_max_values(response, "max_length")
                if (not response[0].get("max_length", ""))
                else response[0].get("max_length", ""),
            }

        response = prepare_distributions(aggregated_data)
        if response:
            update_query = f"""
                UPDATE core.terms SET advanced_profiling_metrics = '{json.dumps(response, default=str).replace("'", "''")}' WHERE id = '{term_id}'    
            """
            with connection.cursor() as update_cursor:
                execute_query(connection, update_cursor, update_query)
        return response
    except Exception as e:
        log_error(f"Error in attribute_profiling for term_id {term_id}", e)
        return None


def prepare_distributions(metadata: dict):
    """
    Prepares the distribution data for the given attribute metadata
    """
    total_rows = metadata.get("total_rows")
    total_rows = int(total_rows) if total_rows else 0
    basic_profile = metadata.get("basic_profile")
    basic_profile = (
        json.loads(basic_profile) if isinstance(basic_profile, str) else basic_profile
    )
    basic_profile = basic_profile if basic_profile else {}
    length_distribution = __serialize_distribution(
        metadata.get("length_distribution"),
        total_rows,
        parse=True,
        key="length_value",
    )
    space_distribution = __serialize_distribution(
        metadata.get("space_distribution"), total_rows, True
    )
    universal_patterns = __serialize_distribution(
        metadata.get("universal_patterns"),
        total_rows,
        include_sample=True,
        metadata=metadata,
        pattern_type="universal_pattern",
    )
    short_universal_patterns = __serialize_distribution(
        metadata.get("short_universal_patterns"),
        total_rows,
        include_sample=True,
        metadata=metadata,
        pattern_type="short_universal_pattern",
    )
    value_distribution = __serialize_distribution(
        metadata.get("value_distribution"), total_rows
    )

    completeness = __serialize_distribution(
        metadata.get("completeness"), total_rows, True, True
    )
    if not completeness:
        null_count = 0
        blank_count = 0
        space_count = 0
        if basic_profile:
            null_count = basic_profile.get("null_count")
            blank_count = basic_profile.get("blank_count")
            space_count = basic_profile.get("space_count")

        null_count = int(null_count) if null_count else 0
        null_percentage = (
            round(((null_count / total_rows) * 100), 2) if total_rows else 0
        )

        blank_count = int(blank_count) if blank_count else 0
        blank_percentage = (
            round(((blank_count / total_rows) * 100), 2) if total_rows else 0
        )

        space_count = int(space_count) if space_count else 0
        space_percentage = (
            round(((space_count / total_rows) * 100), 2) if total_rows else 0
        )

        non_empty = total_rows - (null_count + blank_count + space_count)
        non_empty = non_empty if non_empty > 0 else 0
        non_empty_percentage = (
            round(((non_empty / total_rows) * 100), 2) if total_rows else 0
        )

        completeness = [
            {
                "enum_value": "null",
                "value_count": null_count,
                "name": "NULL",
                "count": null_count,
                "percentage": null_percentage,
                "is_default": True,
                "is_valid": True,
            },
            {
                "enum_value": "space",
                "value_count": space_count,
                "name": "SPACE",
                "count": space_count,
                "percentage": space_percentage,
                "is_default": True,
                "is_valid": True,
            },
            {
                "enum_value": "empty",
                "value_count": blank_count,
                "name": "EMPTY",
                "count": blank_count,
                "percentage": blank_percentage,
                "is_default": True,
                "is_valid": True,
            },
            {
                "enum_value": "non_empty",
                "value_count": non_empty,
                "name": "NON EMPTY",
                "count": non_empty,
                "percentage": non_empty_percentage,
                "is_default": True,
                "is_valid": True,
            },
        ]
    uniqueness = __serialize_distribution(metadata.get("uniqueness"), total_rows, True)
    if not uniqueness:
        distinct_count = 0
        if basic_profile:
            distinct_count = basic_profile.get("distinct_count")
        distinct_count = int(distinct_count) if distinct_count else 0
        distinct_percentage = (
            round(((distinct_count / total_rows) * 100), 2) if total_rows else 0
        )
        repeating_count = (total_rows - distinct_count) if total_rows else 0
        repeating_percentage = (
            round(((repeating_count / total_rows) * 100), 2) if total_rows else 0
        )
        uniqueness = [
            {
                "enum_value": "unique",
                "value_count": distinct_count,
                "name": "UNIQUE",
                "count": distinct_count,
                "percentage": distinct_percentage,
                "is_default": True,
                "is_valid": True,
            },
            {
                "enum_value": "repeating",
                "value_count": repeating_count,
                "name": "REPEATING",
                "count": repeating_count,
                "percentage": repeating_percentage,
                "is_default": True,
                "is_valid": True,
            },
        ]
    character_distribution = __serialize_distribution(
        metadata.get("character_distribution"), total_rows, True, True
    )
    if not character_distribution:
        character_distribution = [
            {
                "enum_value": "alphabet",
                "value_count": 0,
                "name": "ALPHABET",
                "count": 0,
                "percentage": 0,
                "is_default": True,
                "is_valid": True,
            },
            {
                "enum_value": "digits",
                "value_count": 0,
                "name": "DIGITS",
                "count": 0,
                "percentage": 0,
                "is_default": True,
                "is_valid": True,
            },
            {
                "enum_value": "alpha_numeric",
                "value_count": 0,
                "name": "ALPHA NUMERIC",
                "count": 0,
                "percentage": 0,
                "is_default": True,
                "is_valid": True,
            },
            {
                "enum_value": "special",
                "value_count": 0,
                "name": "SPECIAL",
                "count": 0,
                "percentage": 0,
                "is_default": True,
                "is_valid": True,
            },
            {
                "enum_value": "space",
                "value_count": 0,
                "name": "SPACE",
                "count": 0,
                "percentage": 0,
                "is_default": True,
                "is_valid": True,
            },
        ]
    if not space_distribution:
        space_distribution = [
            {
                "enum_value": "leading",
                "value_count": 0,
                "name": "LEADING",
                "count": 0,
                "percentage": 0,
                "is_default": True,
                "is_valid": True,
            },
            {
                "enum_value": "trailing",
                "value_count": 0,
                "name": "TRAILING",
                "count": 0,
                "percentage": 0,
                "is_default": True,
                "is_valid": True,
            },
            {
                "enum_value": "inner",
                "value_count": 0,
                "name": "INNER",
                "count": 0,
                "percentage": 0,
                "is_default": True,
                "is_valid": True,
            },
            {
                "enum_value": "outer",
                "value_count": 0,
                "name": "OUTER",
                "count": 0,
                "percentage": 0,
                "is_default": True,
                "is_valid": True,
            },
        ]
    numeric_distribution = __serialize_distribution(
        metadata.get("numeric_distribution"), total_rows, True
    )
    if not numeric_distribution:
        zero_values_count = 0
        if basic_profile:
            zero_values_count = basic_profile.get("zero_values")
        zero_values_count = int(zero_values_count) if zero_values_count else 0
        zero_values_percentage = (
            round(((zero_values_count / total_rows) * 100), 2) if total_rows else 0
        )
        numeric_distribution = [
            {
                "enum_value": "positive",
                "value_count": 0,
                "name": "POSITIVE",
                "count": 0,
                "percentage": 0,
                "is_default": True,
                "is_valid": True,
            },
            {
                "enum_value": "negative",
                "value_count": 0,
                "name": "NEGATIVE",
                "count": 0,
                "percentage": 0,
                "is_default": True,
                "is_valid": True,
            },
            {
                "enum_value": "zero",
                "value_count": zero_values_count,
                "name": "ZERO",
                "count": zero_values_count,
                "percentage": zero_values_percentage,
                "is_default": True,
                "is_valid": True,
            },
        ]
    user_defined_patterns = __serialize_distribution(
        metadata.get("user_defined_patterns"), total_rows, False, True
    )

    statistics = metadata.get("statistics")
    statistics = json.loads(statistics) if isinstance(statistics, str) else statistics
    statistics = statistics if statistics else {}

    attribute_measures = metadata.get("attribute_measures")
    attribute_measures = (
        json.loads(attribute_measures)
        if isinstance(attribute_measures, str)
        else attribute_measures
    )
    attribute_measures = attribute_measures if attribute_measures else {}

    metadata.update(
        {
            "basic_profile": basic_profile,
            "length_distribution": length_distribution,
            "space_distribution": space_distribution,
            "universal_patterns": universal_patterns,
            "short_universal_patterns": short_universal_patterns,
            "user_defined_patterns": user_defined_patterns,
            "value_distribution": value_distribution,
            "completeness": completeness,
            "uniqueness": uniqueness,
            "character_distribution": character_distribution,
            "numeric_distribution": numeric_distribution,
            "statistics": statistics,
            "attribute_measures": attribute_measures,
        }
    )
    return metadata


def __serialize_distribution(
    list_value,
    total_records,
    is_upper: bool = False,
    replace_special_chars: bool = False,
    parse: bool = False,
    key: str = "",
    include_sample: bool = False,
    metadata: dict = None,
    pattern_type: str = "",
):
    value_distribution = []
    if metadata:
        value_distribution = metadata.get("value_distribution")
        value_distribution = (
            json.loads(value_distribution)
            if isinstance(value_distribution, str)
            else value_distribution
        )
        value_distribution = value_distribution if value_distribution else []

    values = []
    if not list_value:
        return values
    values = json.loads(list_value) if isinstance(list_value, str) else list_value
    values = values if values else []
    for value in values:
        key = key if key else "enum_value"
        enum_value = str(value.get(key))
        if replace_special_chars:
            enum_value = enum_value.replace("_", " ")
        value_count = value.get("value_count")
        value_percentage = 0
        if total_records and value_count:
            try:
                value_percentage = round(((value_count / total_records) * 100), 2)
            except Exception as e:
                value_percentage = 0
        name = str(enum_value).upper() if is_upper else enum_value
        if parse and (name is None or name == "None"):
            name = "0"
        try:
            name = int(name) if parse else name
        except ValueError:
            try:
                name = float(name) if parse else name
            except ValueError:
                pass
        value_count = int(float(value_count)) if value_count else 0
        value.update(
            {"name": name, "count": value_count, "percentage": value_percentage}
        )
        if "is_default" not in value:
            value.update({"is_default": True})
        if "is_valid" not in value:
            value.update({"is_valid": True})
        if include_sample and metadata:
            sample_value = value.get("sample_value")
            if not sample_value and value_distribution:
                sample_value = next(
                    (
                        item.get("enum_value")
                        for item in value_distribution
                        if item.get(pattern_type) == enum_value
                    ),
                    None,
                )
            if sample_value:
                value.update({"sample_value": sample_value})
    return values


def get_asset_domain_ids(config: dict, asset_id: Optional[str] = None) -> List[str]:
    """Fetches domain IDs associated with an asset from domain_mapping table."""
    try:
        asset_id = asset_id or config.get("asset_id")
        if not asset_id:
            return []
        
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                SELECT DISTINCT domain_id 
                FROM core.domain_mapping 
                WHERE asset_id = '{asset_id}' AND domain_id IS NOT NULL
            """
            cursor = execute_query(connection, cursor, query_string)
            domain_mappings = fetchall(cursor)
            print(f"domain_mappings: {domain_mappings}")
        
        return [str(m.get("domain_id")) for m in domain_mappings if m.get("domain_id")]
    except Exception as e:
        log_error("Error fetching asset domain IDs", e)
        return []


def get_terms_data(config: dict, domain_ids: Optional[List[str]] = None):
    """Fetches terms data, optionally filtered by domain_ids."""
    try:
        connection = get_postgres_connection(config)
        valid_domain_ids = [str(did) for did in (domain_ids or []) if did]
        
        if valid_domain_ids:
            placeholders = ','.join(['%s'] * len(valid_domain_ids))
            query = f"SELECT * FROM core.terms WHERE domain_id IN ({placeholders})"
            with connection.cursor() as cursor:
                cursor.execute(query, tuple(valid_domain_ids))
                columns = [desc[0] for desc in cursor.description]
                dataframe = pd.DataFrame(cursor.fetchall(), columns=columns)
        else:
            dataframe = pd.read_sql("SELECT * FROM core.terms", connection)
        
        return dataframe
    except Exception as e:
        log_error("Error fetching terms data", e)
        return pd.DataFrame()


def get_min_max_values(response, category):
    name = f"attribute_{category}"
    func = min if "min" in category else max
    return func(
        (v.get(name) for v in response if v.get(name) is not None), default=None
    )


def check_value_datatype(value):
    if value is None or str(value).strip() == "":
        return 0
    value = str(value).strip()
    try:
        if "." not in value:
            return int(value)
        return float(value)
    except ValueError:
        return value


def get_fully_qualified_table_name(config: Dict[str, Any]) -> str:
    """
    Constructs and returns the fully qualified table name based on the provided configuration.

    The function determines the table name format based on the connection type and other
    configuration details such as database name, schema, and table technical name. It supports
    various connection types including Snowflake, Athena, Redshift, MSSQL, Oracle, MySQL,
    BigQuery, and more.

    Args:
        config (str): A dictionary containing configuration details. Expected keys include:
            - "asset": A dictionary containing asset details.
                - "properties": A dictionary containing asset properties, including:
                    - "database" (str): The name of the database (optional).
            - "connection": A dictionary containing connection details.
                - "type" (str): The type of the connection (e.g., Snowflake, MSSQL, etc.).
            - "table_technical_name" (str): The technical name of the table.
            - "schema" (str): The schema name (optional).

    Returns:
        str: The fully qualified table name formatted according to the connection type.
    """
    asset = config.get("asset")
    asset_properties = asset.get("properties", {})
    db_name = asset_properties.get("database", "")

    connection = config.get("connection", "")
    connection_type = connection.get("type", "")
    table_name = ""
    if connection_type in [
        ConnectionType.Snowflake.value,
        ConnectionType.Athena.value,
        ConnectionType.EmrSpark.value,
        ConnectionType.Redshift.value,
        ConnectionType.Redshift_Spectrum.value,
        ConnectionType.Postgres.value,
        ConnectionType.AlloyDB.value,
        ConnectionType.Denodo.value,
        ConnectionType.Db2.value,
        ConnectionType.DB2IBM.value,
        ConnectionType.SapHana.value,
        ConnectionType.SapEcc.value,
    ]:
        table = config.get("table_technical_name")
        schema = config.get("schema", "")
        table_name = f'"{schema}"."{table}"'
        if db_name:
            table_name = f'"{db_name}"."{schema}"."{table}"'
    elif connection_type in [ConnectionType.MSSQL.value, ConnectionType.Synapse.value]:
        table = config.get("table_technical_name")
        schema = config.get("schema", "")
        table_name = f"[{schema}].[{table}]"
        if db_name:
            table_name = f"[{db_name}].[{schema}].[{table}]"

    elif connection_type == ConnectionType.Oracle.value:
        table = config.get("table_technical_name")
        schema = config.get("schema", "")
        table_name = f'"{schema}"."{table}"'

    elif connection_type == ConnectionType.MySql.value:
        table = config.get("table_technical_name")
        schema = config.get("schema", "")
        table_name = f"{schema}.{table}"

    elif connection_type == ConnectionType.BigQuery.value:
        table = config.get("table_technical_name")
        schema = config.get("schema", "")
        table_name = f"`{schema}`.`{table}`"
    elif connection_type == ConnectionType.Databricks.value:
        table = config.get("table_technical_name")
        schema = config.get("schema", "")
        table_name = f"`{schema}`.`{table}`"
        if db_name:
            table_name = f"`{db_name}`.`{schema}`.`{table}`"

    return table_name


def fetch_asset_enum_data(config: Dict[str, Any]) -> Dict[Any, str]:
    """
    Fetches enumeration data for the given asset configuration.

    This function retrieves queries from the configuration, constructs a query
    to fetch data from the specified table, executes the query, and processes
    the results. The data is converted to lowercase for consistency.

    Args:
        config (dict): The configuration dictionary containing asset and query details.

    Returns:
        dict: The processed query data in lowercase format.
    """
    query_data = {}
    try:
        # Retrieve default queries and table name
        default_queries = get_queries(config)
        table_name = get_fully_qualified_table_name(config)

        # Get the attributes from the attributes parameter

        # Construct the asset query with a limit
        default_query_with_limit = default_queries.get("default_query_limit")
        asset_query = (
            default_query_with_limit.replace("<count>", "10")
            .replace("<query>", "*")
            .replace("<table_name>", table_name)
        )
        # Execute the query
        source_connection = ""
        query_data, native_connection = execute_native_query(
            config, asset_query, source_connection, is_list=True, convert_lower=False
        )
        # Update source connection if not already set
        if not source_connection and native_connection:
            source_connection = native_connection

    except Exception as e:
        log_error(f"Error occurred - {str(e)}", asset_query)
        return query_data

    return query_data if query_data else {}


def remove_by_key_inplace(
    data_list: List[Dict[str, Any]], target_value: str, target_key: str = "id"
) -> None:
    """
    Removes elements from a list of dictionaries in-place based on a specified target key and value.

    This function iterates through a list of dictionaries and removes any dictionary
    where the value associated with the specified key matches the given target_value.
    The operation modifies the input list directly.

    Args:
        data_list (List[Dict[str, Any]]): A list of dictionaries.
        target_value (str): The value to match for removal.
        target_key (str): The key to check in each dictionary. Defaults to 'id'.

    Returns:
        None
    """

    if (not isinstance(data_list, list)) or (not data_list):
        return data_list

    i = 0
    while i < len(data_list):
        if data_list[i].get(target_key) == target_value:
            del data_list[i]
        else:
            i += 1


# --- Data Validation Functions ---

def get_attribute_sample_values(
    config: dict, 
    attribute_id: str, 
    attribute_name: str, 
    limit: int = 20,
    asset_enum_data: Optional[List[Dict[str, Any]]] = None
) -> List[str]:
    """
    Fetches sample data values for a specific attribute.
    
    Args:
        config: Configuration dictionary
        attribute_id: ID of the attribute
        attribute_name: Name of the attribute
        limit: Maximum number of sample values to fetch
        asset_enum_data: Optional pre-fetched asset enum data to avoid repeated DB calls
        
    Returns:
        List of sample values as strings
    """
    sample_values = []
    try:
        # Use pre-fetched asset enum data if provided, otherwise fetch it
        if asset_enum_data is None:
            asset_enum_data = fetch_asset_enum_data(config)
        if not asset_enum_data:
            return sample_values
        
        # Extract values for this specific attribute
        attribute_name_lower = attribute_name.lower() if attribute_name else ""
        for record in asset_enum_data:
            if not isinstance(record, dict):
                continue
            # Try to find the attribute in the record (case-insensitive)
            for key, value in record.items():
                if key.lower() == attribute_name_lower:
                    if value is not None and str(value).strip():
                        sample_values.append(str(value).strip())
                        if len(sample_values) >= limit:
                            return sample_values
                    break
            
    except Exception as e:
        log_error(f"Error fetching sample values for attribute {attribute_id}", e)
    
    return sample_values[:limit]


def validate_data_matches_term_type(
    config: dict, 
    attribute_id: str, 
    attribute_name: str, 
    term_name: str,
    term_id: Optional[str] = None,
    term_derived_type: Optional[str] = None,
    attr_derived_type: Optional[str] = None,
    asset_enum_data: Optional[List[Dict[str, Any]]] = None,
    term_is_null: Optional[bool] = None
) -> bool:
    """
    Validates that actual attribute data values match the expected term type.
    Prevents mismatches like emails in FirstName fields or names in Email fields.
    Also validates that the derived_type (data type) of the term matches the attribute's derived_type.
    Also validates is_null constraint: if term.is_null = False, attribute data must not contain nulls.
    
    Args:
        config: Configuration dictionary
        attribute_id: ID of the attribute
        attribute_name: Name of the attribute
        term_name: Name of the semantic term being mapped
        term_id: ID of the semantic term (optional, used for derived_type validation)
        term_derived_type: Optional pre-fetched derived_type for the term
        attr_derived_type: Optional pre-fetched derived_type for the attribute
        asset_enum_data: Optional pre-fetched asset enum data to avoid repeated DB calls
        term_is_null: Optional pre-fetched is_null value for the term (False = NOT NULL constraint)
        
    Returns:
        True if data matches term type, False if there's a mismatch
    """
    if not term_name or not attribute_name:
        return True  # Skip validation if missing info
    
    term_name_lower = term_name.lower()
    attribute_name_lower = attribute_name.lower()
    
    # Validate derived_type compatibility if term_id is provided
    # Use pre-fetched derived_types if provided, otherwise fetch from database
    if term_id and (term_derived_type is None or attr_derived_type is None):
        try:
            connection = get_postgres_connection(config)
            with connection.cursor() as cursor:
                # Get term's derived_type if not provided
                if term_derived_type is None:
                    term_query = f"SELECT derived_type FROM core.terms WHERE id = '{term_id}'"
                    cursor = execute_query(connection, cursor, term_query)
                    term_result = fetchone(cursor)
                    if term_result:
                        term_derived_type = (term_result.get("derived_type") if isinstance(term_result, dict) 
                                            else term_result[0] if isinstance(term_result, tuple) else None)
                        if term_derived_type:
                            term_derived_type = str(term_derived_type).lower().strip()
                
                # Get attribute's derived_type if not provided
                if attr_derived_type is None:
                    attr_query = f"SELECT derived_type FROM core.attribute WHERE id = '{attribute_id}'"
                    cursor = execute_query(connection, cursor, attr_query)
                    attr_result = fetchone(cursor)
                    if attr_result:
                        attr_derived_type = (attr_result.get("derived_type") if isinstance(attr_result, dict) 
                                            else attr_result[0] if isinstance(attr_result, tuple) else None)
                        if attr_derived_type:
                            attr_derived_type = str(attr_derived_type).lower().strip()
        
        
        except Exception as e:
            log_error(f"Error validating derived_type for attribute {attribute_id} and term {term_id}", e)
            
    
    # Validate derived_type compatibility
    if term_derived_type and attr_derived_type:
        try:
            # Define compatible type mappings
            compatible_types = {
                "numeric": ["numeric", "integer", "int", "float", "double", "decimal", "number"],
                "integer": ["numeric", "integer", "int", "number"],
                "int": ["numeric", "integer", "int", "number"],
                "float": ["numeric", "float", "double", "decimal", "number"],
                "double": ["numeric", "float", "double", "decimal", "number"],
                "decimal": ["numeric", "float", "double", "decimal", "number"],
                "number": ["numeric", "integer", "int", "float", "double", "decimal", "number"],
                "text": ["text", "string", "varchar", "char", "nvarchar", "nvarchar2"],
                "string": ["text", "string", "varchar", "char", "nvarchar", "nvarchar2"],
                "varchar": ["text", "string", "varchar", "char", "nvarchar", "nvarchar2"],
                "char": ["text", "string", "varchar", "char", "nvarchar", "nvarchar2"],
                "nvarchar": ["text", "string", "varchar", "char", "nvarchar", "nvarchar2"],
                "nvarchar2": ["text", "string", "varchar", "char", "nvarchar", "nvarchar2"],
                "date": ["date", "datetime", "timestamp", "time", "date32", "date64"],
                "datetime": ["date", "datetime", "timestamp", "time", "date32", "date64"],
                "datetimeoffset": ["date", "datetime", "timestamp", "timestampoffset", "timestampntz", "time", "date32", "date64"],
                "timestamp": ["date", "datetime", "timestamp", "time", "date32", "date64"],
                "time": ["date", "datetime", "timestamp", "time", "date32", "date64"],
                "boolean": ["boolean", "bool", "bit"],
                "bool": ["boolean", "bool", "bit"],
                "bit": ["boolean", "bool", "bit"],
                "binary": ["binary", "blob", "varbinary"],
                "money": ["money", "decimal", "numeric", "float", "double"],
            }
            
            # Check if types are compatible
            # First check for exact match
            if term_derived_type == attr_derived_type:
                # Types match exactly, allow it
                print(f"Types match exactly, allow it: {term_derived_type} == {attr_derived_type}")
            else:
                # Check if attribute type is in the compatible list for term type
                term_compatible = compatible_types.get(term_derived_type, [term_derived_type])
                if attr_derived_type not in term_compatible:
                    log_info(f"Data type validation failed for attribute '{attribute_name}' (ID: {attribute_id}) "
                            f"mapped to term '{term_name}': "
                            f"Term derived_type is '{term_derived_type}' but attribute derived_type is '{attr_derived_type}'")
                    return False
        except Exception as e:
            log_error(f"Error validating derived_type for attribute {attribute_id} and term {term_id}", e)
            # Continue with other validations even if derived_type check fails
    
    # Get sample values (pass pre-fetched asset_enum_data if available)
    sample_values = get_attribute_sample_values(
        config, attribute_id, attribute_name, limit=20, asset_enum_data=asset_enum_data
    )
    print(f"sample_values for attribute: {attribute_name} and term: {term_name}: {sample_values}")
    
    if not sample_values:
        if term_is_null is False:
            log_info(f"Term has NOT NULL constraint (is_null=False) but no sample values available for attribute {attribute_name}, skipping data validation")
            return False
        # If no sample values available, skip validation (assume valid)
        log_info(f"No sample values available for attribute {attribute_name}, skipping data validation")
        return True
    
    # Email pattern detection
    email_pattern = r"[^@]+@[^@]+\.[^@]+"
    has_email_pattern = any(re.match(email_pattern, str(val)) for val in sample_values if val)
    
    # IP address pattern detection
    ip_pattern = r"^((?:[0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])\.){3}(?:[0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])$"
    has_ip_pattern = any(re.match(ip_pattern, str(val)) for val in sample_values if val)
    
    # Phone number pattern (basic)
    phone_pattern = r"[\d\s\-\(\)\+]{10,}"
    has_phone_pattern = any(re.match(phone_pattern, str(val)) and len(re.sub(r'[\s\-\(\)\+]', '', str(val))) >= 10 
                           for val in sample_values if val)
    
    # Name pattern (typically alphabetic, may have spaces, hyphens, apostrophes)
    name_pattern = r"^[A-Za-z\s\-\']+$"
    has_name_pattern = any(re.match(name_pattern, str(val)) and len(str(val).split()) <= 5 
                          for val in sample_values if val)
    
    # Validation rules based on term type
    validation_failed = False
    failure_reason = ""
    
    # Check for email data in non-email terms
    if has_email_pattern:
        non_email_terms = ["firstname", "lastname", "middlename", "name", "fullname", 
                          "first_name", "last_name", "middle_name", "full_name",
                          "city", "state", "country", "address", "addressline1", "addressline2",
                          "phone", "phonenumber", "ssn", "zip", "zipcode", "postalcode"]
        if any(term in term_name_lower for term in non_email_terms):
            validation_failed = True
            failure_reason = f"Data contains email patterns but term is '{term_name}' (non-email term)"
    
    # Check for IP data in non-IP terms
    if has_ip_pattern:
        non_ip_terms = ["firstname", "lastname", "name", "email", "city", "state", "country",
                       "address", "phone", "phonenumber", "ssn"]
        if any(term in term_name_lower for term in non_ip_terms):
            validation_failed = True
            failure_reason = f"Data contains IP address patterns but term is '{term_name}' (non-IP term)"
    
    # If term is "email", data MUST contain email patterns
    if "email" in term_name_lower:
        if not has_email_pattern:
            # Count how many values actually look like emails vs other patterns
            email_like_count = sum(1 for val in sample_values if '@' in str(val) and '.' in str(val))
            name_like_count = sum(1 for val in sample_values 
                                if re.match(name_pattern, str(val)) and '@' not in str(val))
            phone_like_count = sum(1 for val in sample_values 
                                 if re.match(phone_pattern, str(val)) and len(re.sub(r'[\s\-\(\)\+]', '', str(val))) >= 10)
            
            # If no email patterns at all, fail validation
            if email_like_count == 0:
                validation_failed = True
                if name_like_count > len(sample_values) * 0.3:  # More than 30% are names
                    failure_reason = f"Term is '{term_name}' but data contains name patterns (no email patterns found)"
                elif phone_like_count > len(sample_values) * 0.3:  # More than 30% are phone numbers
                    failure_reason = f"Term is '{term_name}' but data contains phone patterns (no email patterns found)"
                else:
                    failure_reason = f"Term is '{term_name}' but data contains no email patterns (found {len(sample_values)} sample values)"
            # If very few email patterns compared to other patterns, also fail
            elif email_like_count < len(sample_values) * 0.2:  # Less than 20% are email-like
                if name_like_count > email_like_count * 2:  # Names are more than 2x emails
                    validation_failed = True
                    failure_reason = f"Term is '{term_name}' but data contains mostly name patterns ({name_like_count} names vs {email_like_count} email-like values)"
                elif phone_like_count > email_like_count * 2:  # Phones are more than 2x emails
                    validation_failed = True
                    failure_reason = f"Term is '{term_name}' but data contains mostly phone patterns ({phone_like_count} phones vs {email_like_count} email-like values)"
    
    # Check for name terms but data contains emails
    name_terms = ["firstname", "lastname", "middlename", "name", "fullname"]
    if any(term in term_name_lower for term in name_terms) and has_email_pattern:
        email_count = sum(1 for val in sample_values if re.match(email_pattern, str(val)))
        if email_count > len(sample_values) * 0.3:  # More than 30% are emails
            validation_failed = True
            failure_reason = f"Term is '{term_name}' but data contains email patterns"
    
    if validation_failed:
        log_info(f"Data validation failed for attribute '{attribute_name}' (ID: {attribute_id}) "
                f"mapped to term '{term_name}': {failure_reason}")
        log_info(f"Sample values: {sample_values[:5]}")
        return False
    
    return True


def validate_and_update_semantic_field(
    config: dict,
    attribute_id: str,
    attribute_name: str,
    semantic_tag: dict,
    terms_data: Optional[Dict[str, Dict[str, Any]]] = None,
    attr_derived_types: Optional[Dict[str, str]] = None,
    asset_enum_data: Optional[List[Dict[str, Any]]] = None
) -> bool:
    """
    Validates attribute data matches term type and updates semantic field if valid.
    This is a wrapper function that combines validation and update operations.
    
    Args:
        config: Configuration dictionary
        attribute_id: ID of the attribute
        attribute_name: Name of the attribute
        semantic_tag: Dictionary containing term information (name, id, etc.)
        terms_data: Optional pre-fetched dictionary of term_id -> {"derived_type": str, "is_null": bool}
        attr_derived_types: Optional pre-fetched dictionary of attribute_id -> derived_type
        asset_enum_data: Optional pre-fetched asset enum data to avoid repeated DB calls
        
    Returns:
        True if update was successful, False if validation failed
    """
    term_id = semantic_tag.get("id")
    term_name = semantic_tag.get("name")
    
    if not term_id or not term_name:
        log_info(f"Skipping validation for attribute '{attribute_name}' - missing term_id or term_name")
        return False
    
    # Get pre-fetched term data if available
    term_data = terms_data.get(str(term_id)) if terms_data else {}
    term_derived_type = term_data.get("derived_type") if term_data else None
    term_is_null = term_data.get("is_null") if term_data else None
    
    # Get pre-fetched attribute derived_type if available
    attr_derived_type = attr_derived_types.get(str(attribute_id)) if attr_derived_types else None
    
    
    # Validate data matches term type (pass pre-fetched asset_enum_data and term_is_null if available)
    is_valid = validate_data_matches_term_type(
        config,
        attribute_id,
        attribute_name,
        term_name,
        term_id,
        term_derived_type,
        attr_derived_type,
        asset_enum_data,
        term_is_null
    )
    
    if not is_valid:
        log_info(
            f"Skipping mapping for attribute '{attribute_name}' (ID: {attribute_id}) "
            f"to term '{term_name}' - data validation failed"
        )
        return False
    
    # Validation passed, proceed with update
    try:
        update_attribute_semantic_field(config, attribute_id, semantic_tag)
        return True
    except Exception as e:
        log_error(
            f"Failed to update attribute {attribute_id} with term {term_id}",
            e,
        )
        return False
