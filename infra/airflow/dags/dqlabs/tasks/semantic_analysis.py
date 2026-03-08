# -*- coding: utf-8 -*-
"""
Migrated dqlabs semantic analysis code to a new module.
This module is responsible for mapping attributes to semantic tags using various NLP techniques.
It includes preprocessing of attributes and terms, computing scores, and applying thresholds.
It also handles multiprocessing for efficiency and provides a function to run the entire mapping process.
"""

# Standard Library Imports
import os
import re
import json
import time
import warnings
from collections import defaultdict
from functools import partial
from multiprocessing import Pool, cpu_count, set_start_method
from typing import Any, Dict, Iterator, List, Optional, Union

# Third-Party Libraries
import pandas as pd
import numpy as np
import torch
from tqdm import tqdm
from scipy import stats
import spacy
from sklearn.preprocessing import LabelEncoder
from sklearn.feature_extraction.text import TfidfVectorizer
from sentence_transformers import SentenceTransformer

# DQLabs Internal Imports

## Logging
from dqlabs.app_helper.log_helper import log_info, log_error

## Enums
from dqlabs.enums.schedule_types import ScheduleStatus

## Workflow and Tasks
from dqlabs.utils.extract_workflow import get_selected_attributes
from dqlabs.utils.tasks import get_task_config, check_task_status
from dqlabs.utils.request_queue import update_queue_detail_status, update_queue_status
from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.db_helper import execute_query, fetchall

## Semantic Preprocessing
from dqlabs.utils.semantics.semantic_preprocessing import (
    TypeFeatureModify,
    GetMeasures,
)

## Semantic Functions
from dqlabs.utils.semantics.semantic_functions import (
    cosine_text,
    get_dict_rules,
    get_semantic_metrics,
    get_semantic_rules,
    get_contains_terms,
    get_synonyms_terms,
    get_threshold,
    get_pattern_rules,
    get_health_rules,
    get_statistical_rules,
    get_enum_rules,
    merge_dict,
    update_semantic_job_status,
    update_attribute_semantic_field,
    remove_special_characters,
    get_term_attribute_profile_metrics,
    get_terms_data,
    get_asset_domain_ids,
    fetch_asset_enum_data,
    remove_by_key_inplace,
    validate_and_update_semantic_field,
)

from dqlabs.app_helper.dq_helper import (
    get_max_workers,
)

# Import semantic metrics functions
from dqlabs.utils.semantics.semantic_metrics import (
    initialize_models,
    process_attribute,
    preprocess_term,
    _calculate_weights,
    compute_attribute_mappings,
    validate_geographic_mappings,
)

# Import ZeroShotClassifier
from dqlabs.models.semantic_model import ZeroShotClassifier
from concurrent.futures import ThreadPoolExecutor, as_completed

# Suppress warnings
warnings.filterwarnings("ignore")

# --- Multiprocessing Start Method ---
try:
    current_method = torch.multiprocessing.get_start_method(allow_none=True)
    if current_method != "spawn":
        set_start_method("spawn", force=True)
except Exception as e:
    log_error("Could not set multiprocessing start method to 'spawn'", e)

# --- Global Variables & Constants ---
NLP_MODEL = None
SENTENCE_MODEL = None
MIN_SCORE_FLOOR = 0.4  # Minimum score threshold for a match
DIRECT_MATCH_BASE_SCORE = 0.98  # Base score for high-confidence direct fuzzy match
DIRECT_MATCH_METADATA_BOOST = 0.1  # Metadata similarity boost
MAX_DIRECT_MATCH_SCORE = 1.05  # Cap for direct match scores
FUZZY_EARLY_EXIT_THRESHOLD = 95.0  # Threshold (0-100) for fuzzy match early exit


# --- AttributeMapper Class ---
class AttributeMapper:
    """Orchestrates the attribute-to-term mapping process.

    Initializes models, preprocesses data, computes scores, and applies thresholds.
    """

    def __init__(
        self,
        attributes_df,
        terms_df,
        parallel_threshold=100,
        use_dynamic_threshold=True,
        threshold_method="otsu",
        min_confidence_threshold=0.5,
    ):
        if (
            not isinstance(attributes_df, pd.DataFrame)
            or "name" not in attributes_df.columns
        ):
            raise ValueError("Attributes DF missing 'name' column.")
        if not isinstance(terms_df, pd.DataFrame) or "name" not in terms_df.columns:
            raise ValueError("Terms DF missing 'name' column.")
        self.attributes_df = attributes_df.copy().reset_index(drop=True)
        self.terms_df = terms_df.copy().reset_index(drop=True)
        self.preprocessed_attrs = []
        self.preprocessed_terms = []
        self.tfidf_vectorizer = None
        self.tfidf_matrix = None
        self.attr_sentence_embeddings_all = np.array([])
        self.parallel_threshold = parallel_threshold
        self.use_dynamic_threshold = use_dynamic_threshold
        self.threshold_method = threshold_method
        self.min_confidence_threshold = min_confidence_threshold
        self.feedback_log = {}
        self.effective_threshold = None
        self._initialize_main_process_models()

    def _initialize_main_process_models(self):
        """Loads models in the main process with a fallback for spaCy."""
        global NLP_MODEL, SENTENCE_MODEL
        if NLP_MODEL is not None and SENTENCE_MODEL is not None:
            return
        if NLP_MODEL is None:
            try:
                NLP_MODEL = spacy.load("en_core_web_lg")
            except Exception as e:
                try:
                    NLP_MODEL = spacy.load("en_core_web_sm")
                except Exception as e2:
                    log_error("Main fallback spaCy load failed", e2)
        if SENTENCE_MODEL is None:
            try:
                device = "cuda" if torch.cuda.is_available() else "cpu"
                SENTENCE_MODEL = SentenceTransformer("all-MiniLM-L6-v2", device=device)
            except Exception as e:
                log_error("Main SBERT load error", e)

    def _preprocess_data(self, df, process_func, item_type="items", use_parallel=False):
        """Preprocesses data using the specified function."""
        start_time = time.time()
        if NLP_MODEL is None or SENTENCE_MODEL is None:
            self._initialize_main_process_models()
        if NLP_MODEL is None or SENTENCE_MODEL is None:
            log_info(f"Required models unavailable for {item_type}. Using fallbacks.")

        items_to_process = list(df.iterrows())
        processed_data = []
        actual_use_parallel = (
            use_parallel and len(items_to_process) > 1 and cpu_count() > 1
        )

        if actual_use_parallel:
            num_processes = min(
                cpu_count(), len(items_to_process), max(1, cpu_count() // 2)
            )
            try:
                process_func_partial = partial(
                    process_func, nlp_model=None, sentence_model=None
                )
                with Pool(
                    processes=num_processes, initializer=initialize_models
                ) as pool:
                    processed_data = pool.map(process_func_partial, items_to_process)
            except Exception as e:
                log_error(f"MP pool error ({item_type})", e)
                actual_use_parallel = False

        if not actual_use_parallel:
            try:
                processed_data = [
                    process_func(
                        item, nlp_model=NLP_MODEL, sentence_model=SENTENCE_MODEL
                    )
                    for item in tqdm(
                        items_to_process, desc=f"Preprocessing {item_type}"
                    )
                ]
            except ImportError:
                processed_data = [
                    process_func(
                        item, nlp_model=NLP_MODEL, sentence_model=SENTENCE_MODEL
                    )
                    for item in items_to_process
                ]
            except Exception as e:
                log_error(f"Error during sequential preprocessing of {item_type}", e)

        processed_data = [item for item in processed_data if item is not None]
        processed_data.sort(key=lambda x: x.get("idx", -1))
        return processed_data

    def _preprocess_attributes(self, use_parallel=False):
        """Preprocesses attributes and builds TF-IDF matrix."""
        self.preprocessed_attrs = self._preprocess_data(
            self.attributes_df, process_attribute, "attributes", use_parallel
        )
        start_time = time.time()
        all_names = []
        for attr in self.preprocessed_attrs:
            start = len(all_names)
            names = attr.get("names", [])
            all_names.extend(names)
            attr["name_indices"] = list(range(start, len(all_names)))

        self.attr_sentence_embeddings_all = np.array([])
        if all_names:
            unique_names = sorted(list(set(all_names)))
            if SENTENCE_MODEL is None:
                log_error(
                    "SBERT model not loaded for attr encoding",
                    Exception("Model not loaded"),
                )
            else:
                try:
                    embeds = SENTENCE_MODEL.encode(
                        unique_names, batch_size=128, show_progress_bar=False
                    )
                    map_ne = {n: e for n, e in zip(unique_names, embeds)}
                    dim = SENTENCE_MODEL.get_sentence_embedding_dimension()
                    num = len(all_names)
                    self.attr_sentence_embeddings_all = np.zeros(
                        (num, dim), dtype=np.float32
                    )
                    for i, n in enumerate(all_names):
                        self.attr_sentence_embeddings_all[i] = map_ne.get(
                            n, np.zeros(dim)
                        )
                except Exception as e:
                    log_error("Attr sentence encoding error", e)
                    self.attr_sentence_embeddings_all = np.array([])

        for attr in self.preprocessed_attrs:
            indices = attr.get("name_indices", [])
            attr["sentence_embeddings"] = np.array([])
            if indices and self.attr_sentence_embeddings_all.ndim == 2:
                start, end = indices[0], indices[-1] + 1
                if 0 <= start < end <= self.attr_sentence_embeddings_all.shape[0]:
                    attr["sentence_embeddings"] = self.attr_sentence_embeddings_all[
                        start:end
                    ]

        self.tfidf_vectorizer, self.tfidf_matrix = None, None
        if all_names:
            self.tfidf_vectorizer = TfidfVectorizer(
                ngram_range=(2, 5),
                sublinear_tf=True,
                max_df=0.90,
                min_df=2,
                analyzer="char_wb",
                stop_words=None,
            )
            try:
                valid_names = [n for n in all_names if n]
                if valid_names:
                    self.tfidf_matrix = self.tfidf_vectorizer.fit_transform(valid_names)
            except ValueError as ve:
                log_error(
                    "TF-IDF Fitting Error (ValueError - likely empty vocab). TF-IDF disabled.",
                    ve,
                )
                self.tfidf_vectorizer = self.tfidf_matrix = None
            except Exception as e:
                log_error("TF-IDF Fitting Error. TF-IDF disabled.", e)
                self.tfidf_vectorizer = self.tfidf_matrix = None

    def _preprocess_terms(self, use_parallel=False):
        """Preprocesses terms and encodes primary names."""
        self.preprocessed_terms = self._preprocess_data(
            self.terms_df, preprocess_term, "terms", use_parallel
        )
        start_time = time.time()
        primary_names = [term.get("name", "") for term in self.preprocessed_terms]
        map_ne = {}
        term_embeds = np.array([])
        if primary_names:
            valid_names = sorted(list(set(n for n in primary_names if n)))
            if valid_names:
                if SENTENCE_MODEL is None:
                    log_error(
                        "SBERT model not loaded for term encoding",
                        Exception("Model not loaded"),
                    )
                else:
                    try:
                        embeds = SENTENCE_MODEL.encode(
                            valid_names, batch_size=128, show_progress_bar=False
                        )
                        map_ne = {n: i for i, n in enumerate(valid_names)}
                        term_embeds = embeds
                    except Exception as e:
                        log_error("Term sentence encoding error", e)
                        term_embeds = np.array([])

        dim_to_use = 384  # Default MiniLM dimension
        if SENTENCE_MODEL and hasattr(
            SENTENCE_MODEL, "get_sentence_embedding_dimension"
        ):
            try:
                dim_to_use = SENTENCE_MODEL.get_sentence_embedding_dimension()
            except Exception:
                pass
        zero_vec = np.zeros(dim_to_use, dtype=np.float32)

        if term_embeds.ndim == 2 and term_embeds.shape[0] > 0:
            dim = term_embeds.shape[1]
            zero_vec = np.zeros(dim, dtype=np.float32)
            for term in self.preprocessed_terms:
                idx = map_ne.get(term.get("name", ""))
                term["sentence_embedding"] = (
                    term_embeds[idx] if idx is not None else zero_vec
                )
        else:
            for term in self.preprocessed_terms:
                term["sentence_embedding"] = zero_vec

    def map_attributes_to_terms(self, confidence_threshold=0.8):
        """Maps attributes to terms based on computed scores and term-specific thresholds,
        ensuring terms matched via fuzzy direct match aren't reused.
        Uses improved metadata scoring for better accuracy.
        """
        overall_start_time = time.time()

        use_parallel = (
            len(self.attributes_df) >= self.parallel_threshold
            and len(self.terms_df) >= self.parallel_threshold
            and len(self.attributes_df) > 1
            and len(self.terms_df) > 1
            and cpu_count() > 1
        )

        self._preprocess_attributes(use_parallel=use_parallel)
        self._preprocess_terms(use_parallel=use_parallel)

        if not self.preprocessed_attrs:
            return {}
        if not self.preprocessed_terms:
            return {}

        # Create a set to track terms that have been used
        used_term_ids = set()

        # Initial mapping results dictionary
        final_single_term_results = {}
        final_feedback_log = defaultdict(list)

        # Sort attributes by some priority (optional)
        # Prioritize attributes with meaningful names and longer names
        sorted_attrs = sorted(
            self.preprocessed_attrs,
            key=lambda a: (
                not a.get("is_non_meaningful", True),
                len(a.get("name", "")),
            ),
        )

        # First pass: Process direct fuzzy matches
        for attr in sorted_attrs:
            attr_name = attr.get("name")
            if not attr_name:
                continue

            # Skip any attribute that's already been mapped
            if attr_name in final_single_term_results:
                continue

            # Filter out terms that have already been used
            available_terms = [
                term
                for term in self.preprocessed_terms
                if term.get("idx") not in used_term_ids
            ]

            # Skip if no terms are available
            if not available_terms:
                break

            # Ensure each term has all_match_names including name, technical_name, synonyms, contains
            for term in available_terms:
                all_match_names = (
                    {term.get("name", "").lower()} if term.get("name") else set()
                )

                # Add technical_name
                technical_name = term.get("technical_name", "")
                if technical_name:
                    all_match_names.add(technical_name.lower())

                # Add synonyms
                synonyms = term.get("synonyms", [])
                if synonyms:
                    all_match_names.update(s.lower() for s in synonyms if s)

                # Add contains values
                contains = term.get("contains", [])
                if contains:
                    all_match_names.update(c.lower() for c in contains if c)

                # Remove empty strings
                all_match_names.discard("")

                # Update the term's all_match_names set
                term["all_match_names"] = all_match_names

            # Map this attribute using the current available terms
            result_tuple = compute_attribute_mappings(
                attr,
                available_terms,
                nlp_model=NLP_MODEL,
                sentence_model=SENTENCE_MODEL,
                tfidf_vectorizer=self.tfidf_vectorizer,
                tfidf_matrix=self.tfidf_matrix,
            )

            if (
                result_tuple
                and isinstance(result_tuple, tuple)
                and len(result_tuple) >= 2
            ):
                _, potential_list = result_tuple

                if potential_list:
                    # Store feedback info
                    final_feedback_log[attr_name] = [
                        {
                            "term": m.get("term"),
                            "score": round(m.get("score", 0.0), 4),
                            "match_type": m.get("match_type", "?"),
                            "threshold": m.get(
                                "threshold", self.min_confidence_threshold
                            ),
                        }
                        for m in potential_list
                        if m.get("term") is not None
                    ]

                    # Check for direct fuzzy match
                    direct_matches = [
                        m
                        for m in potential_list
                        if m.get("match_type") == "fuzzy_direct"
                        and m.get("score")
                        >= m.get("threshold", self.min_confidence_threshold)
                    ]

                    if direct_matches:
                        best_match = direct_matches[0]
                        term_name = best_match.get("term")

                        # Find the term data for this match
                        matched_term = next(
                            (t for t in available_terms if t.get("name") == term_name),
                            None,
                        )

                        if matched_term:
                            # Add this to our results
                            final_single_term_results[attr_name] = term_name

                            # Mark this term as used
                            used_term_ids.add(matched_term.get("idx"))

                            # Determine which match identifier was used (for logging)
                            matched_via = "primary name"
                            attr_name_lower = attr_name.lower()

                            # Check if it was a technical name, synonym or contains value that matched
                            tech_name = matched_term.get("technical_name", "").lower()
                            if tech_name and attr_name_lower == tech_name:
                                matched_via = f"technical name '{tech_name}'"
                            else:
                                for syn in matched_term.get("synonyms", []):
                                    if syn and attr_name_lower == syn.lower():
                                        matched_via = f"synonym '{syn}'"
                                        break

                                for contains in matched_term.get("contains", []):
                                    if contains and contains.lower() in attr_name_lower:
                                        matched_via = f"contains value '{contains}'"
                                        break


        # Second pass: Process remaining attributes with non-direct matching
        for attr in sorted_attrs:
            attr_name = attr.get("name")
            if not attr_name or attr_name in final_single_term_results:
                continue

            # Filter out terms that have already been used
            available_terms = [
                term
                for term in self.preprocessed_terms
                if term.get("idx") not in used_term_ids
            ]

            # Skip if no terms are available
            if not available_terms:
                break

            # Map this attribute
            result_tuple = compute_attribute_mappings(
                attr,
                available_terms,
                nlp_model=NLP_MODEL,
                sentence_model=SENTENCE_MODEL,
                tfidf_vectorizer=self.tfidf_vectorizer,
                tfidf_matrix=self.tfidf_matrix,
            )

            if (
                result_tuple
                and isinstance(result_tuple, tuple)
                and len(result_tuple) >= 2
            ):
                _, potential_list = result_tuple

                if potential_list:
                    # Store feedback info if not already stored
                    if attr_name not in final_feedback_log:
                        final_feedback_log[attr_name] = [
                            {
                                "term": m.get("term"),
                                "score": round(m.get("score", 0.0), 4),
                                "match_type": m.get("match_type", "?"),
                                "threshold": m.get(
                                    "threshold", self.min_confidence_threshold
                                ),
                            }
                            for m in potential_list
                            if m.get("term") is not None
                        ]

                    # Find best match by checking against term-specific thresholds
                    best_match = None
                    for mapping in potential_list:
                        score = mapping.get("score")
                        term = mapping.get("term")
                        term_threshold = mapping.get(
                            "threshold", self.min_confidence_threshold
                        )

                        if (
                            score is not None
                            and term is not None
                            and score >= term_threshold
                        ):
                            best_match = mapping
                            break

                    if best_match:
                        term_name = best_match.get("term")

                        # Find the term data for this match
                        matched_term = next(
                            (t for t in available_terms if t.get("name") == term_name),
                            None,
                        )

                        if matched_term:
                            # Add to results
                            final_single_term_results[attr_name] = term_name

                            # Mark this term as used
                            used_term_ids.add(matched_term.get("idx"))

                            # Determine which match identifier was used (for logging)
                            matched_via = "primary name"
                            attr_name_lower = attr_name.lower()

                            # Check if it was a technical name, synonym or contains value that matched
                            tech_name = matched_term.get("technical_name", "").lower()
                            if tech_name and attr_name_lower == tech_name:
                                matched_via = f"technical name '{tech_name}'"
                            else:
                                for syn in matched_term.get("synonyms", []):
                                    if syn and attr_name_lower == syn.lower():
                                        matched_via = f"synonym '{syn}'"
                                        break

                                for contains in matched_term.get("contains", []):
                                    if contains and contains.lower() in attr_name_lower:
                                        matched_via = f"contains value '{contains}'"
                                        break


        self.feedback_log = dict(final_feedback_log)
        final_results_sorted_single_term = dict(
            sorted(final_single_term_results.items())
        )

        return final_results_sorted_single_term


# --- Preliminary Checks --- #


def get_primary_measures_dict(dataframe: pd.DataFrame):
    """
    Get the primary measures from the dataframe
    """
    _df = dataframe
    _primary_measures_dict = {
        "statistical_measures_dict": list(
            GetMeasures(_df).get_statistical_measures(_df).values()
        ),
        "health_measures_dict": list(GetMeasures(_df).get_health_measures().values()),
        "pattern_measures_values": (
            list(GetMeasures(_df).get_pattern_measures().values())
        ),
    }
    return _primary_measures_dict


def get_min_max_range(df_datatype: pd.DataFrame) -> dict:
    """Get the min/max range for every rule"""

    df_datatype.drop_duplicates(subset="name", keep="first", inplace=True)
    df = df_datatype[["semantic_key", "min_length", "max_length"]]
    df = df.dropna()
    if "semantic_key" in df.columns:
        semantic_name = [name for name in df.semantic_key]
    if "min_length" in df.columns:
        min_length = [min for min in df.min_length]
    if "max_length" in df.columns:
        max_length = [max for max in df.max_length]
    min_max_dict = {
        key: (min_value, max_value)
        for key, min_value, max_value in zip(semantic_name, min_length, max_length)
    }
    return min_max_dict


def get_range_dataframe(dataframe: pd.DataFrame):
    df = dataframe
    df.drop_duplicates(subset="name", keep="first", inplace=True)
    min_length, max_length = 0, 0
    if "name" in df.columns:
        min_length = df[df["name"] == "min_length"].get("value")
        max_length = df[df["name"] == "max_length"].get("value")
        min_length = int(min_length) if not min_length.empty else 0
        max_length = int(max_length) if not max_length.empty else 0
    return min_length, max_length


def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """remove outliers based on zscore values for numericals"""
    df["zscore"] = stats.zscore(df["name"])
    return df.loc[df["zscore"].abs() <= 3]


def returnMatches(a: list, b: list) -> list:
    """Returns number of identical matches from rules and metrics"""
    return list(set(a) & set(b))


""" Depreciated code: Dissipated check_subset"""


def check_subset(metrics_range: tuple, rule_range: tuple) -> bool:
    """
    Checks if the metrics range is withing rules range

    input parameters:
    Example parameters:

    metrics_range = (1,4) -> (min_length,max_length) (Snumber)
    rules_range = (4,4) ->(min_length,max_length)    (SSN4)

    Output:

    Checks for if metrics_range is a subset of rules range
    if True:
        return f"rule semantic id"

    """
    metrics_range = range(metrics_range[0], metrics_range[1])
    rule_range = range(rule_range[0], rule_range[1])
    return set((metrics_range)).issubset(rule_range)


def get_filtered_distribution(
    df: pd.DataFrame, total_count: float, rules_dict: dict, threshold: int = 67
) -> dict:
    """
    Functions filters out all the frequency/distribution score
    which should be greater then the threshold
    """
    """ Converting object to encoded data for outlier removal"""
    if isinstance(total_count, str):
        int(total_count)
    le = LabelEncoder()
    df["name"] = le.fit_transform(df["name"])
    df_filtered = remove_outliers(df)
    final_dict = {}
    for key in rules_dict.keys():
        range_tuple = rules_dict.get(key)
        output_df = df_filtered[
            df_filtered["name"].isin(range(range_tuple[0], range_tuple[1]))
        ]
        value_count = int(output_df["value"].sum())
        if isinstance(value_count, str):
            int(value_count)
        try:
            valid_percentage = (int(value_count) / int(total_count)) * 100
        except ZeroDivisionError:
            valid_percentage = 0
        if valid_percentage >= threshold:
            final_dict.update({key: valid_percentage})
    return final_dict


def get_filtered_pattern(
    df: pd.DataFrame, df_rules: pd.DataFrame = None, threshold: int = 67
) -> dict:
    """Filtering out the matched pattern in the metrics data to the rule patterns"""
    """ Filtering metrics pattern"""
    df["name"] = df["name"].drop_duplicates(keep="last")
    df = df[df["name"].notna()]
    df = df.assign(value_percent=lambda x: ((x["value"] / x["value"].sum()) * 100))
    df = df.loc[df["value_percent"] > threshold]
    metrics_pattern = [pattern for pattern in df["name"].values]

    """ Filtering rules pattern"""
    key_list = list(set([key for key in df_rules["semantic_key"].values]))
    pattern_dict = dict()
    for key in key_list:
        temp_df = df_rules[df_rules["semantic_key"] == key]
        pattern_dict.update({key: list(temp_df["technical_name"].values)})
        del temp_df
    score_dict = dict()
    for key in pattern_dict.keys():
        total_length = len(pattern_dict.get(key))
        score = float(
            len(returnMatches(metrics_pattern, pattern_dict.get(key))) / total_length
        )
        score_dict.update({key: score})
    return score_dict


def get_rules(rules_dict: dict):
    """
    Get the rules as list for every class
    """
    _dict = {
        "statistical_rules": list(
            get_dict_rules(rules_dict[0][0][0], "statistical_measures").values()
        ),
        "health_rules": list(
            get_dict_rules(rules_dict[0][0][1], "health_measures").values()
        ),
        "pattern_rules": rules_dict[0][1],
    }
    return _dict


def get_enum_values(lst) -> list:
    res = json.loads(lst) if isinstance(lst, str) else lst
    enum_list = [res[idx].get("value") for idx in range(len(res))]
    return enum_list


def clean_table(df: pd.DataFrame) -> pd.DataFrame:
    """Pre Processing of the metics table to extract neccessary semantic measures"""
    if "type" not in df.columns:
        return df
    df["type"] = df["type"].fillna("enum")
    df["freq"] = np.where(df["name"].str.isnumeric(), "numeric", "enum")
    df.sort_values(by="type")
    return df


def strip_dict(dict_of_list: dict) -> dict:
    """Removes whitespace from dictionary of lists"""
    dd = dict()
    temp_lst = list()
    for key, values in dict_of_list.items():
        if len(values) == 1:
            dd[key] = values
        elif len(values) > 1:
            for lst in values:
                temp_lst.append(lst.strip())
            dd[key] = temp_lst
            temp_lst = []
    return dd


class GetRules:
    def __init__(self, dataframe: pd.DataFrame):
        self.dataframe = dataframe
        self.dataframe["derived_type"] = self.dataframe["derived_type"].apply(
            lambda x: x.lower()
        )

    def generate_rules(self, df: pd.DataFrame) -> dict:
        rules_pattern = get_pattern_rules(df)
        rules_health = get_health_rules(df)
        rules_statistical = get_statistical_rules(df)
        rules_enum = get_enum_rules(df)
        merge1 = merge_dict(rules_statistical, rules_health)
        merge2 = merge_dict(merge1, rules_pattern)
        final_merge = merge_dict(merge2, rules_enum)
        return final_merge

    def get_custom_rules(self, rule: str):
        df = self.dataframe
        df_data = df[df["derived_type"] == rule]
        rules = GetRules(df).generate_rules(df_data)
        return rules


class PersonModel:
    def __init__(self, input_metrics: dict) -> pd.DataFrame:
        """
        Getting all the measures from the dataframe and measuring it against the rules dictionary
        to generate a matching score.
        [statistical_measures, health_measures, pattern_measures, distribution_measures, frequency measures]
        All values in the dict are matched against the dataframe boolean functions
        Similarity score  = (A.B)/(||A||.||B||)
        """
        self.dataframe = pd.DataFrame.from_records(input_metrics)  # metrics table
        self.dataframe = clean_table(self.dataframe)
        self.attribute_name = (
            self.dataframe["attribute_name"].iloc[0]
            if "attribute_name" in self.dataframe.columns
            else "Empty"
        )
        self.matching_score = 0  # default
        self.error_rate = 0.0
        self.df_health = TypeFeatureModify(self.dataframe)._boolean_valid_health(
            self.dataframe
        )
        self.df_pattern = TypeFeatureModify(self.dataframe)._boolean_valid_pattern(
            self.dataframe
        )
        self.df_frequency = TypeFeatureModify(self.dataframe)._boolean_valid_frequency(
            self.dataframe
        )
        self.df_enum = TypeFeatureModify(self.dataframe)._boolean_valid_enum(
            self.dataframe
        )
        self.total_score = [count for count in self.dataframe["total_count"]]
        if self.total_score:
            self.total_score = int(max(self.total_score))
        else:
            self.total_score = 0

    def get_match_score(self, rules_metrics: dict, derived_type: str) -> tuple:
        semantic_score = 0  # default
        if self.dataframe.empty:
            return semantic_score
        """ Pre-Processing the rules and metrics data"""
        rules_df = pd.DataFrame.from_records(rules_metrics)
        rules_df = rules_df.replace(r"^\s*$", np.nan, regex=True)
        rules_df = rules_df.fillna(999)
        rules_df.min_length = rules_df.min_length.astype(int)
        rules_df.max_length = rules_df.max_length.astype(int)
        rules = GetRules(rules_df).get_custom_rules(derived_type)

        """ Declare dict variables"""
        dict_model_rules = {}
        pattern_scores_dict = {}
        rule_list = list()
        if derived_type == "text":
            """Get pattern scores"""
            self.df_pattern["value"] = pd.to_numeric(self.df_pattern["value"])
            if not self.df_pattern.empty:
                pattern_scores_dict = get_filtered_pattern(self.df_pattern, rules_df)

            df_data = rules_df[rules_df["derived_type"] == derived_type]
            df_data["derived_type"] = df_data["derived_type"].apply(lambda x: x.lower())
            min_max_range_dict = get_min_max_range(df_data)  # rules min-max
            """ Filter out rules to be scored"""
            self.df_frequency["value"] = pd.to_numeric(self.df_frequency["value"])
            self.df_frequency["name"] = pd.to_numeric(self.df_frequency["name"])
            if not self.df_frequency.empty:
                dict_model_rules = get_filtered_distribution(
                    self.df_frequency, self.total_score, min_max_range_dict
                )
            rule_list = list(set(dict_model_rules.keys()))
        else:
            if "date".lower() not in self.attribute_name.lower():
                min_length = int(
                    self.df_health["value"][
                        self.df_health["name"] == "min_length"
                    ].iloc[0]
                )
                max_length = int(
                    self.df_health["value"][
                        self.df_health["name"] == "max_length"
                    ].iloc[0]
                )
                for key in rules.keys():
                    statistical_measures = rules.get(key)[0][0][0]
                    rule_range = statistical_measures.get("statistical_measures")
                    min_rule, max_rule = (
                        int(rule_range.get("min_length")),
                        int(rule_range.get("max_length")),
                    )
                    if check_subset([min_length, max_length], [min_rule, max_rule]):
                        rule_list.append(key)
        """ Declaring empty list and dict to append scores for different keys"""
        final_score_list = []
        enum_score_list = []
        final_semantic_score = []
        if rule_list:
            for key in rule_list:
                model_rules = rules.get(key)
                enum_rule = model_rules[1]
                _get_primary_measures_dict = get_primary_measures_dict(self.dataframe)
                get_rules_dict = get_rules(model_rules)
                if derived_type == "text":
                    self.matching_score = {
                        "statistical_measure_score": cosine_text(
                            _get_primary_measures_dict["statistical_measures_dict"],
                            get_rules_dict["statistical_rules"],
                        ),
                        "health_measures_score": cosine_text(
                            _get_primary_measures_dict["health_measures_dict"],
                            get_rules_dict["health_rules"],
                        ),
                    }
                else:
                    self.matching_score = {
                        "statistical_measure_score": 1.0,
                        "health_measures_score": cosine_text(
                            _get_primary_measures_dict["health_measures_dict"],
                            get_rules_dict["health_rules"],
                        ),
                    }
                self.matching_score.update(
                    {"frequency_measure_score": float(dict_model_rules.get(key) / 100)}
                )
                if dict_model_rules:
                    if "pattern" in self.dataframe["category"].values:
                        self.matching_score.update(
                            {"pattern_match_score": pattern_scores_dict.get(key)}
                        )
                if not self.df_enum.empty:
                    if enum_rule.get("enum") == "[]":
                        self.matching_score = self.matching_score
                    else:
                        enum_lst = enum_rule.get("enum")
                        rule_enum_lst = get_enum_values(enum_lst)
                        self.matching_score.update(
                            {
                                "enum_measure_score": GetMeasures(
                                    self.df_enum
                                ).get_enum_score(rule_enum_lst),
                            }
                        )
                        enum_score = (
                            key,
                            self.matching_score.get("enum_measure_score"),
                        )
                        if enum_score[1] is not None and enum_score[1] > 0:
                            enum_score_list.append(enum_score)

                self.final_match_score = (
                    (sum(self.matching_score.values()) / len(self.matching_score))
                    if self.matching_score
                    else 0
                )
                total_score = self.final_match_score + self.error_rate
                if total_score >= 1:
                    total_score = 0.993
                key_score = (key, total_score)
                final_score_list.append(key_score)

            if enum_score_list:
                max_value = max([val[1] for val in enum_score_list])
                enum_idx = [val[0] for val in enum_score_list if val[1] == max_value][0]
                semantic_score = list(
                    [val for val in final_score_list if val[0] == enum_idx][0]
                )
                if semantic_score[1] >= 1:
                    semantic_score[1] = 0.993
                final_semantic_score = (semantic_score[1], semantic_score[0])
            else:
                if final_score_list:
                    max_value = max([val[1] for val in final_score_list])
                    semantic_score = [
                        val for val in final_score_list if val[1] == max_value
                    ][0]
                    final_semantic_score = (semantic_score[1], semantic_score[0])
            if "text" not in derived_type:
                if float(final_semantic_score[0]) >= 0.67:
                    final_semantic_score = list(final_semantic_score)
                    final_semantic_score[0] = 0.8
                    final_semantic_score = tuple(final_semantic_score)

            return final_semantic_score

    def get_contains_keyword(
        self, contains_metrics: dict, synonym_metrics: dict, attribute_name: str
    ) -> tuple:
        """Get the semantic keys which contain the keywords"""
        attribute_name = remove_special_characters(attribute_name)
        from collections import defaultdict

        key_list = list()
        contains = pd.DataFrame.from_records(contains_metrics)
        synonyms = pd.DataFrame.from_records(synonym_metrics)

        """ Latest change in updated contains and synonyms"""
        contains["contains"] = [",".join(map(str, val)) for val in contains["contains"]]
        synonyms["synonyms"] = [",".join(map(str, val)) for val in synonyms["synonyms"]]

        synonyms_dict, contains_dict = dict(), dict()
        dd, combined_dict = defaultdict(list), defaultdict(list)
        key_list = list()
        if not synonyms.empty:
            synonyms_items = [value for value in synonyms.synonyms.values]
            syn_key_items = [key for key in synonyms.semantic_key.values]
            synonyms_dict = {
                key: str(synonyms_items[idx]).split(",")
                for idx, key in enumerate(syn_key_items)
            }
            synonyms_dict = strip_dict(synonyms_dict)
        if not contains.empty:
            contains_items = [value for value in contains.contains.values]
            key_items = [key for key in contains.semantic_key.values]
            contains_dict = {
                key: str(contains_items[idx]).split(",")
                for idx, key in enumerate(key_items)
            }
            contains_dict = strip_dict(contains_dict)

        if synonyms_dict and contains_dict:
            dd = defaultdict(list)
            # you can list as many input dicts as you want here
            for d in (contains_dict, synonyms_dict):
                for key, value in d.items():
                    dd[key].extend(value)
            combined_dict = dd

        combined_dict = {k: list(set(v)) for k, v in combined_dict.items()}
        combined_dict = {
            key: [value.lower() for value in values]
            for key, values in combined_dict.items()
        }
        flag: int = 0

        """ Retrieving the highest score semantic term"""
        if combined_dict:
            for key, values in combined_dict.items():
                for val in values:
                    if val:
                        if attribute_name.lower() != "subscription_level":
                            val = remove_special_characters(val)
                            if str(val) in attribute_name.lower():
                                flag += 1
                    final_score = (flag / len(values), key) if flag else (0, key)
                    flag = 0
                    key_list.append(final_score)
            key_list = list(max(key_list))
        if synonyms_dict and not contains_dict:
            for key, values in synonyms_dict.items():
                for val in values:
                    if val:
                        if str(val).lower() in attribute_name.lower():
                            flag += 1
                final_score = (flag / len(values), key) if flag else (0, key)
                flag = 0
                key_list.append(final_score)
            key_list = list(max(key_list))
        if contains_dict and not synonyms_dict:
            for key, values in contains_dict.items():
                for val in values:
                    if val:
                        if str(val).lower() in attribute_name.lower():
                            flag += 1
                final_score = (flag / len(values), key) if flag else (0, key)
                flag = 0
                key_list.append(final_score)
            key_list = list(max(key_list))

        if key_list:
            if key_list[0] != 0:
                key_list[0] = 0.95
            key_list = tuple(key_list)

        return key_list


def primary_semantics_check(
    config: List[Dict[str, Any]], 
    attributes: List[Dict[str, Any]], 
    terms_data: Optional[Dict[str, Dict[str, Any]]] = None,
    attr_derived_types: Optional[Dict[str, str]] = None,
    asset_enum_data: Optional[List[Dict[str, Any]]] = None,
    **kwargs
) -> List[str]:
    """
    Run the main program which returns the highest matching score and semantic tag
    for the input data
    """
    semantic_rules = get_semantic_rules(config)
    contains_metrics = get_contains_terms(config)
    synonyms_metrics = get_synonyms_terms(config)
    for attribute in attributes:
        try:
            is_semantic_enabled = attribute.get("is_semantic_enabled")
            term_id = attribute.get("term_id")
            if term_id or (not is_semantic_enabled):
                continue
            attribute_name = attribute.get("name")
            derived_type = attribute.get("derived_type", "")
            attribute_id = attribute.get("id")
            attribute_metrics = get_semantic_metrics(config, attribute_id)

            match_score = []
            """
            Get the threshold for comparison
            """
            match_score = PersonModel(attribute_metrics).get_contains_keyword(
                contains_metrics, synonyms_metrics, attribute_name
            )

            if not match_score:
                match_score = PersonModel(attribute_metrics).get_match_score(
                    semantic_rules, derived_type.lower()
                )
            if not match_score:
                continue
            semantic_percentage, semantic_name = (
                float(match_score[0]),
                str(match_score[1]),
            )
            print(f"match_score 1157: {match_score}")
            print(f"semantic_name 1158: {semantic_name}")
            print(f"semantic_percentage 1159: {semantic_percentage}")
            threshold = get_threshold(config, semantic_name) or {}
            semantic_threshold = (
                float(threshold.get("threshold", 0)) if threshold else 0
            )
            print(f"semantic_threshold 1161: {semantic_threshold}")
            # semantic_threshold = 75.0
            semantic_tag = {}

            if threshold and ((semantic_percentage * 100) > semantic_threshold):
                semantic_tag = {"name": semantic_name, **threshold}
            if semantic_tag:
                # Validate and update using helper function
                if validate_and_update_semantic_field(
                    config, attribute_id, attribute_name, semantic_tag,
                    terms_data, attr_derived_types, asset_enum_data
                ):
                    attributes = remove_by_key_inplace(
                        data_list=attributes, target_value=attribute_id
                    )
        except Exception as e:
            log_error(f"run semantic analysis - {attribute_name} ", e)
    return attributes


def run_zeroshot_classification(
    config: List[Dict[str, Any]], 
    dq_attributes: List[Dict[str, Any]], 
    terms_data: Optional[Dict[str, Dict[str, Any]]] = None,
    attr_derived_types: Optional[Dict[str, str]] = None,
    asset_enum_data: Optional[List[Dict[str, Any]]] = None,
    **kwargs
) -> List[Dict[str, Any]]:
    """
    Executes zero-shot classification on the provided asset data and updates the semantic field
    of attributes based on the classification results.
    Args:
        config: Configuration object or dictionary containing necessary settings for the operation.
        dq_attributes (List[Dict[str, Any]]): A list of dictionaries representing data quality attributes.
            Each dictionary should contain an "id" and "attribute_name" key.
        **kwargs: Additional keyword arguments. Expected keys include:
            - "enum_asset_dataframe" (Dict[str, List[Any]]): A dictionary where keys are attribute names
                and values are lists of rows corresponding to the attribute.
    Returns:
        List[str]: A list of remaining attributes after processing and updating their semantic fields.
    Raises:
        KeyError: If required keys are missing in the input dictionaries.
        Exception: If the ZeroShotClassifier or update_attribute_semantic_field encounters an error.
    Notes:
        - The function initializes a ZeroShotClassifier instance to classify the semantic type of each
            attribute's data.
        - It updates the semantic field of attributes in the data quality system using the
            `update_attribute_semantic_field` function.
        - Processed attributes are removed from the `dq_attributes` list using the `remove_by_key_inplace` function.
    """
    attributes = dq_attributes.copy()
    attribute_names = [attr["name"].lower() for attr in attributes]

    # Use pre-fetched asset enum data if provided, otherwise fetch it
    if asset_enum_data is None:
        asset_enum_data = fetch_asset_enum_data(config)

    cleaned_data = []
    for record in asset_enum_data:
        cleaned_record = {
            key: value
            for key, value in record.items()
            if not isinstance(value, (bytearray, bytes))
            and key.lower() in attribute_names
        }
        cleaned_data.append(cleaned_record)

    asset_dataframe = pd.DataFrame(cleaned_data)

    zeroshotmodel = ZeroShotClassifier()

    def process_attribute(attribute: str, rows: List[str]):

        if not all(isinstance(row, str) for row in rows):
            return None

        semantic_term_classified = zeroshotmodel.classify_column_type(data=rows, column_name=attribute)
        if not semantic_term_classified:
            return None

        attribute_id = next(
            (
                attr["id"]
                for attr in dq_attributes
                if attr["name"].lower() == attribute.lower()
            ),
            None,
        )

        if not attribute_id:
            return None


        threshold = get_threshold(config, semantic_term_classified) or {}
        semantic_tag = {"name": semantic_term_classified, **threshold}
        
        # Ensure term_id exists before updating
        if not semantic_tag.get("id"):
            return None
        
        # Get attribute name for validation
        attribute_name = next(
            (
                attr["name"]
                for attr in dq_attributes
                if attr["id"] == attribute_id
            ),
            None,
        )
        
        if not attribute_name:
            return None
        
        # Validate and update using helper function
        if validate_and_update_semantic_field(
            config, attribute_id, attribute_name, semantic_tag,
            terms_data, attr_derived_types, asset_enum_data
        ):
            return attribute_id
        
        return None

    remaining_ids = set(attr["id"] for attr in dq_attributes)

    # Process each attribute in parallel
    max_workers = get_max_workers()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_attribute, attribute, rows.tolist() if hasattr(rows, 'tolist') else list(rows)): attribute
            for attribute, rows in asset_dataframe.items()
        }

        for future in as_completed(futures):
            attribute_id = future.result()
            if attribute_id:
                remaining_ids.discard(attribute_id)

    # Filter remaining attributes
    remaining_attributes = [
        attr for attr in dq_attributes if attr["id"] in remaining_ids
    ]
    return remaining_attributes


def run_semantic_analysis(config: dict, **kwargs):
    """Runs the main program to map attributes to semantic tags."""
    """Run preliminary Checks for attributes"""
    is_completed = check_task_status(config, kwargs)
    if is_completed:
        return
    asset = config.get("asset")
    asset = asset if asset else {}
    task_config = get_task_config(config, kwargs)
    update_queue_detail_status(
        config, ScheduleStatus.Running.value, task_config=task_config
    )
    update_queue_status(config, ScheduleStatus.Running.value, True)
    update_semantic_job_status(config, ScheduleStatus.Running.value)

    # Get domain IDs for filtering (no longer deleting existing mappings - will update instead)
    asset_id = config.get("asset_id")
    if asset_id:
        domain_ids = get_asset_domain_ids(config, asset_id)
        config["_filter_domain_ids"] = domain_ids if domain_ids else None
    else:
        config["_filter_domain_ids"] = None

    # Fetch the attributes to be processed
    attributes = get_selected_attributes(config)
    attributes = attributes if attributes else []
    
    # Fetch asset enum data once for all validation operations (reduces DB calls)
    asset_enum_data = None
    try:
        asset_enum_data = fetch_asset_enum_data(config)
    except Exception as e:
        log_error("Error fetching asset enum data for validation", e)
    
    """
    Primary semantic check
    """
    # Batch fetch term data (derived_type, is_null) and attribute derived_types once for all validation operations
    terms_data = {}  # term_id -> {"derived_type": str, "is_null": bool}
    attr_derived_types = {}
    try:
        connection = get_postgres_connection(config)
        try:
            with connection.cursor() as cursor:
                # Batch fetch all terms' data (derived_type and is_null) for primary/zeroshot validation
                # We fetch all terms since we don't know which ones will be used yet
                term_query = "SELECT id, derived_type, is_null FROM core.terms"
                cursor = execute_query(connection, cursor, term_query)
                term_results = fetchall(cursor)
                for term_result in term_results:
                    term_id = term_result.get("id") if isinstance(term_result, dict) else term_result[0]
                    derived_type = term_result.get("derived_type") if isinstance(term_result, dict) else term_result[1]
                    is_null = term_result.get("is_null") if isinstance(term_result, dict) else (term_result[2] if len(term_result) > 2 else None)
                    if term_id:
                        terms_data[str(term_id)] = {
                            "derived_type": str(derived_type).lower().strip() if derived_type else None,
                            "is_null": is_null
                        }
                
                # Get unique attribute IDs for derived_type only
                if attributes:
                    attr_ids = [str(attr.get("id")) for attr in attributes if attr.get("id")]
                    if attr_ids:
                        attr_ids_str = "', '".join(attr_ids)
                        attr_query = f"SELECT id, derived_type FROM core.attribute WHERE id IN ('{attr_ids_str}')"
                        cursor = execute_query(connection, cursor, attr_query)
                        attr_results = fetchall(cursor)
                        for attr_result in attr_results:
                            attr_id = attr_result.get("id") if isinstance(attr_result, dict) else attr_result[0]
                            derived_type = attr_result.get("derived_type") if isinstance(attr_result, dict) else attr_result[1]
                            if derived_type:
                                attr_derived_types[str(attr_id)] = str(derived_type).lower().strip()

        except Exception:
            pass
    except Exception as e:
        log_error("Error batch fetching terms data and attribute derived_types for primary/zeroshot validation", e)
    
    primary_unmapped_attributes = []
    if attributes:
        primary_unmapped_attributes = primary_semantics_check(
            config, attributes=attributes, 
            terms_data=terms_data,
            attr_derived_types=attr_derived_types,
            asset_enum_data=asset_enum_data,
            **kwargs
        )
        if not primary_unmapped_attributes:
            log_info("No attributes were mapped after primary_semantics_check.")

    # If no attributes were mapped, we can use the original attributes
    primary_unmapped_attributes = (
        primary_unmapped_attributes if primary_unmapped_attributes else attributes
    )
    """
    ZeroShot Classification Model
    """
    remaining_unmapped_attributes = []
    if primary_unmapped_attributes:
        try:
            remaining_unmapped_attributes = run_zeroshot_classification(
                config, dq_attributes=primary_unmapped_attributes,
                terms_data=terms_data,
                attr_derived_types=attr_derived_types,
                asset_enum_data=asset_enum_data
            )
        except Exception as e:
            log_error("Error in loading ZeroShotClassifier", e)

    remaining_unmapped_attributes = (
        remaining_unmapped_attributes
        if remaining_unmapped_attributes
        else primary_unmapped_attributes
    )

    if remaining_unmapped_attributes:
        # Get the remaining attributes to be processed
        if (
            isinstance(remaining_unmapped_attributes, list)
            and remaining_unmapped_attributes
        ):
            if isinstance(remaining_unmapped_attributes[0], dict):
                filtered_attributes = remaining_unmapped_attributes  # if list of dicts
            else:
                filtered_attributes = list(
                    filter(
                        lambda attr: attr["attribute_name"]
                        not in remaining_unmapped_attributes,
                        attributes,
                    )
                )  # if list

        terms_df = get_terms_data(config, domain_ids=config.get("_filter_domain_ids"))
        print(f"terms_df: {terms_df}")
        print(f"config.get('_filter_domain_ids'): {config.get('_filter_domain_ids')}")

        # Extract term data (derived_type, is_null) from terms_df if available
        if not terms_df.empty:
            for _, term_row in terms_df.iterrows():
                term_id = str(term_row.get("id", ""))
                if term_id:
                    if term_id not in terms_data:
                        terms_data[term_id] = {}
                    if "derived_type" in terms_df.columns:
                        derived_type = term_row.get("derived_type")
                        if derived_type:
                            terms_data[term_id]["derived_type"] = str(derived_type).lower().strip()
                    # if "is_null" in terms_df.columns:
                    #     is_null = term_row.get("is_null")
                    #     if is_null is not None:
                    #         terms_data[term_id]["is_null"] = is_null

        attributes_df = pd.DataFrame(filtered_attributes)
        
        # Also extract attribute derived_types from attributes_df if available
        if not attributes_df.empty:
            for _, attr_row in attributes_df.iterrows():
                attr_id = str(attr_row.get("id", ""))
                if attr_id:
                    if "derived_type" in attributes_df.columns:
                        derived_type = attr_row.get("derived_type")
                        if derived_type:
                            attr_derived_types[attr_id] = str(derived_type).lower().strip()

        # --- Run Mapping ---
        min_confidence_param = 0.6  # Used as floor/default in mapper
        parallel_thresh_test = 500  # Keep parallel threshold setting
        all_tests_successful = True  # Flag for overall success

        # Prepare ID lookup DataFrames once before the loop (if needed)
        attr_id_lookup_df = attributes_df[["name", "id"]].copy()
        attr_id_lookup_df.rename(
            columns={"name": "Attribute_Name", "id": "Attribute_ID"}, inplace=True
        )

        term_id_lookup_df = terms_df[["name", "id", "domain_id"]].copy()
        term_id_lookup_df.rename(
            columns={
                "name": "Mapped_Term_Name",
                "id": "Term_ID",
                "domain_id": "domain_id",
            },
            inplace=True,
        )

        # Simplified run - only one configuration now as dynamic threshold options removed
        test_start_time = time.time()
        final_results_df = pd.DataFrame()
        try:
            # Instantiate Mapper - Removed dynamic threshold parameters
            mapper = AttributeMapper(
                attributes_df,
                terms_df,
                min_confidence_threshold=min_confidence_param,  # Pass min confidence
                parallel_threshold=parallel_thresh_test,
            )
            # Perform mapping
            results_dict = (
                mapper.map_attributes_to_terms()
            )  # Pass confidence_param if still needed
            results_dict = validate_geographic_mappings(
                results_dict, attributes_df, terms_df
            )

            # Process results into DataFrame
            if results_dict:
                mapping_df = pd.DataFrame(
                    list(results_dict.items()),
                    columns=["Attribute_Name", "Mapped_Term_Name"],
                )
                # Merge with lookup tables to get IDs
                merged_attribute = pd.merge(
                    mapping_df, attr_id_lookup_df, on="Attribute_Name", how="left"
                )
                mergeed_att_term = pd.merge(
                    merged_attribute,
                    term_id_lookup_df,
                    on="Mapped_Term_Name",
                    how="left",
                )
                # Rename for clarity before final selection
                mergeed_att_term.rename(
                    columns={"Mapped_Term_Name": "Mapped_Term"}, inplace=True
                )
                # Select final columns
                final_results_df = mergeed_att_term[
                    [
                        "Attribute_Name",
                        "Attribute_ID",
                        "Term_ID",
                        "Mapped_Term",
                        "domain_id",
                    ]
                ]
            else:
                # Create empty DataFrame with correct columns if no results
                final_results_df = pd.DataFrame(
                    columns=[
                        "Attribute_Name",
                        "Attribute_ID",
                        "Term_ID",
                        "Mapped_Term",
                        "domain_id",
                    ]
                )

        except Exception as e:
            log_error("Critical error during semantic mapping", e)
            # Store error information if needed, e.g., results_store['mapping_error'] = str(e)
            all_tests_successful = False  # Mark as failed

            # Log head of results for verification

        update_count = 0
        skipped_count = 0
        validation_failed_count = 0
        for index, row in final_results_df.iterrows():
            semantic_tag = {}
            semantic_tag["name"] = row.get("Mapped_Term")
            semantic_tag["id"] = row.get("Term_ID")
            semantic_tag["domain_id"] = row.get("domain_id")
            attribute_id = row.get("Attribute_ID")
            attribute_name = row.get("Attribute_Name")

            # Check if both attribute ID and term ID are valid before updating
            if pd.notna(attribute_id) and pd.notna(semantic_tag["id"]):
                # Validate and update using helper function
                try:
                    if validate_and_update_semantic_field(
                        config, attribute_id, attribute_name, semantic_tag,
                        terms_data, attr_derived_types, asset_enum_data
                    ):
                        update_count += 1
                    else:
                        validation_failed_count += 1
                        skipped_count += 1
                        continue
                except Exception as e:
                    log_error(
                        f"Failed to validate/update attribute {attribute_id} with term {semantic_tag.get('id')}",
                        e,
                    )
                    # Potentially mark overall process as failed if updates fail?
                    all_tests_successful = False  # Example: Mark failure on DB error
            else:
                skipped_count += 1
            # print(semantic_tag) # Keep for debugging if needed
        
        if validation_failed_count > 0:
            log_info(f"Data validation prevented {validation_failed_count} attribute-term mappings due to data mismatch")

    # Optional: Run term attribute profile metrics update
    try:
        get_term_attribute_profile_metrics(config)
    except Exception as e:
        log_info(("Error updating term attribute profile metrics", e))

    # --- Final Status Update ---
    final_status = (
        ScheduleStatus.Completed.value
        if all_tests_successful
        else ScheduleStatus.Failed.value
    )
    update_queue_detail_status(config, final_status)
    update_queue_status(config, final_status)
    update_semantic_job_status(config, final_status)
    log_info(f"Semantic analysis process finished with status: {final_status}.")