# Import required logging functions
from dqlabs.app_helper.log_helper import log_error, log_info

# Import necessary libraries
import json
import logging
import os
import re
import sys
from collections import defaultdict

# Import required libraries for data processing and machine learning
import numpy as np
import pandas as pd
import spacy
import torch
from sentence_transformers import SentenceTransformer
from sklearn.cluster import KMeans
from sklearn.metrics.pairwise import cosine_similarity
from rapidfuzz import fuzz, process
import cv2

# --- Constants ---
MIN_SCORE_FLOOR = 0.4  # Minimum score threshold for a match to be considered
DIRECT_MATCH_BASE_SCORE = 0.98  # Base score for high-confidence direct fuzzy match
DIRECT_MATCH_METADATA_BOOST = 0.1  # Metadata similarity boost for direct match score
MAX_DIRECT_MATCH_SCORE = 1.05  # Cap for direct match scores (> 1 for distinction)
FUZZY_EARLY_EXIT_THRESHOLD = 95.0  # Threshold (0-100) for fuzzy match early exit

# --- Global Variables ---
NLP_MODEL = None
SENTENCE_MODEL = None

# --- Helper Functions ---
def initialize_models():
    """Initialize NLP and SBERT models in worker processes."""
    global NLP_MODEL, SENTENCE_MODEL
    pid = os.getpid()

    # Load spaCy model
    if NLP_MODEL is None:
        try:
            NLP_MODEL = spacy.load("en_core_web_lg", disable=["parser", "ner"])
        except OSError:
            try:
                NLP_MODEL = spacy.load("en_core_web_md", disable=["parser", "ner"])
            except Exception as e_md:
                log_error(f"Worker {pid}: Failed to load spaCy model (lg/md)", e_md)
                NLP_MODEL = None
        except Exception as e:
            log_error(f"Worker {pid}: spaCy load error", e)
            NLP_MODEL = None

    # Load Sentence Transformer model
    if SENTENCE_MODEL is None:
        try:
            device = 'cuda' if torch.cuda.is_available() else 'cpu'
            SENTENCE_MODEL = SentenceTransformer('all-MiniLM-L12-v2', device=device)
        except Exception as e:
            log_error(f"Worker {pid}: SBERT load error", e)
            SENTENCE_MODEL = None

def safe_load_json_list_or_dict(json_str):
    """Safely parse a JSON string into a list or dict, or return non-string input as-is.

    Args:
        json_str: Input to parse (string, dict, list, or other).

    Returns:
        Parsed JSON list/dict, input dict/list, or None if parsing fails or input is invalid.
    """
    try:
        # Check for scalar NA values
        is_scalar_na = pd.isna(json_str)
        if np.isscalar(is_scalar_na) and is_scalar_na:
            return None

        # If input is already a dict or list, return it as-is
        if isinstance(json_str, (dict, list)):
            return json_str

        # Check if input is a string
        if isinstance(json_str, str):
            # Check for empty string
            if not json_str.strip():
                return None
            # Attempt to load string as JSON
            json_str_stripped = json_str.strip()
            return json.loads(json_str_stripped)

        # Handle unexpected types (e.g., slice)
        return None

    except json.JSONDecodeError as e:
        log_error(f"JSON decode failed: {e} - Input: {str(json_str)[:100]}...")
        return None
    except Exception as e:
        log_error(f"Unexpected error: {e} - Input: {str(json_str)[:100]}...")
        return None
    finally:
        log_info("safe_load_json_list_or_dict completed")

def safe_get_distribution_count(data, target_enum_value):
    """Sum 'value_count' for a specific 'enum_value' in a list of distribution dicts.

    Args:
        data: List of distribution dictionaries.
        target_enum_value: Enum value to match.

    Returns:
        Total count as an integer.
    """
    if not isinstance(data, list):
        return 0
    count = 0
    target_enum_lower = target_enum_value.lower()
    possible_targets = {target_enum_lower}
    if target_enum_lower == 'no_space':
        possible_targets.add('no space')
    elif target_enum_lower == 'non_empty':
        possible_targets.add('non empty')

    for item in data:
        if (isinstance(item, dict) and 'enum_value' in item and
                isinstance(item.get('enum_value'), str)):
            enum_val_lower = item['enum_value'].lower()
            if enum_val_lower in possible_targets:
                value_count = item.get('value_count', 0)
                try:
                    count += float(value_count) if value_count is not None else 0
                except (ValueError, TypeError):
                    log_info(f"Could not convert value_count '{value_count}' to float "
                             f"for '{enum_val_lower}'.")
    return int(count)

def safe_get_basic_profile_value(data, key_name):
    """Extract a numeric value from basic profile data safely.

    Args:
        data: Dictionary containing profile data.
        key_name: Key to extract value from.

    Returns:
        Integer value or 0 if extraction fails.
    """
    if not isinstance(data, dict):
        return 0
    value = data.get(key_name, 0)
    try:
        return int(value) if value is not None else 0
    except (ValueError, TypeError):
        return 0

def determine_char_type(char_dist_data):
    """Determine the primary character type from distribution data.

    Args:
        char_dist_data: List of character distribution dictionaries.

    Returns:
        String indicating character type (e.g., 'alpha_only', 'alnum').
    """
    if not isinstance(char_dist_data, list):
        return 'unknown'
    counts = defaultdict(int)
    total_meaningful_chars = 0
    for item in char_dist_data:
        if (isinstance(item, dict) and 'enum_value' in item and
                isinstance(item.get('enum_value'), str)):
            enum_val = item['enum_value'].lower()
            if enum_val != 'space':
                count = safe_get_distribution_count([item], enum_val)
                counts[enum_val] += count
                total_meaningful_chars += count

    if total_meaningful_chars == 0:
        return 'other'

    has_alpha = counts.get('alphabet', 0) > 0
    has_digit = counts.get('digits', 0) > 0
    has_alnum = counts.get('alpha_numeric', 0) > 0
    has_special = counts.get('special', 0) > 0

    if has_special:
        return 'mixed_special'
    if has_alnum and not has_alpha and not has_digit:
        return 'alnum'
    if has_alpha and has_digit:
        return 'alnum'
    if has_alpha:
        return 'alpha_only'
    if has_digit:
        return 'digit_only'
    if has_alnum:
        return 'alnum'
    return 'other'

def determine_space_presence(space_dist_data):
    """Determine if spaces are present based on distribution data.

    Args:
        space_dist_data: List of space distribution dictionaries.

    Returns:
        String indicating space presence ('has_spaces', 'no_spaces', 'unknown').
    """
    if not isinstance(space_dist_data, list):
        return 'unknown'
    no_space_count = safe_get_distribution_count(space_dist_data, 'no_space')
    has_other_spaces = False
    total_count = 0
    for item in space_dist_data:
        if (isinstance(item, dict) and 'enum_value' in item and
                isinstance(item.get('enum_value'), str)):
            count = safe_get_distribution_count([item], item['enum_value'])
            total_count += count
            if (item['enum_value'].lower() not in ['no_space', 'no space'] and
                    count > 0):
                has_other_spaces = True
                break

    if has_other_spaces:
        return 'has_spaces'
    elif no_space_count > 0 and total_count > 0:
        return 'no_spaces'
    else:
        return 'unknown'

def determine_numeric_sign(numeric_dist_data):
    """Determine the sign characteristic of numeric data.

    Args:
        numeric_dist_data: List of numeric distribution dictionaries.

    Returns:
        String indicating numeric sign (e.g., 'pos_only', 'mixed_sign').
    """
    if not isinstance(numeric_dist_data, list):
        return 'not_numeric'
    counts = defaultdict(int)
    total_numeric_values = 0
    for item in numeric_dist_data:
        if (isinstance(item, dict) and 'enum_value' in item and
                isinstance(item.get('enum_value'), str)):
            count = safe_get_distribution_count([item], item['enum_value'])
            counts[item['enum_value'].lower()] += count
            total_numeric_values += count

    if total_numeric_values == 0:
        return 'not_numeric'

    has_pos = counts.get('positive', 0) > 0
    has_neg = counts.get('negative', 0) > 0
    has_zero = counts.get('zero', 0) > 0

    if has_neg and has_pos:
        return 'mixed_sign'
    if has_neg and not has_pos:
        return 'neg_only'
    if has_pos and has_zero and not has_neg:
        return 'non_negative'
    if has_pos and not has_zero and not has_neg:
        return 'pos_only'
    if has_zero and not has_pos and not has_neg:
        return 'zero_only'
    if has_pos:
        return 'pos_only'
    if has_neg:
        return 'neg_only'
    if has_zero:
        return 'zero_only'
    return 'unknown'

def safe_get_completeness(item_dict):
    """Get non-empty count from direct column or nested metrics.

    Args:
        item_dict: Dictionary containing completeness data.

    Returns:
        Integer non-empty count.
    """
    completeness_data = item_dict.get('completeness')
    if completeness_data is None:
        advanced_metrics = safe_load_json_list_or_dict(
            item_dict.get('advanced_profiling_metrics'))
        if isinstance(advanced_metrics, dict):
            completeness_data = advanced_metrics.get('completeness')

    if isinstance(completeness_data, str):
        data = safe_load_json_list_or_dict(completeness_data)
    elif isinstance(completeness_data, list):
        data = completeness_data
    else:
        data = None
    return safe_get_distribution_count(data, 'non_empty')

def safe_get_uniqueness(item_dict):
    """Get unique count from direct column or nested metrics.

    Args:
        item_dict: Dictionary containing uniqueness data.

    Returns:
        Integer unique count.
    """
    uniqueness_data = item_dict.get('uniqueness')
    if uniqueness_data is None:
        advanced_metrics = safe_load_json_list_or_dict(
            item_dict.get('advanced_profiling_metrics'))
        if isinstance(advanced_metrics, dict):
            uniqueness_data = advanced_metrics.get('uniqueness')

    if isinstance(uniqueness_data, str):
        data = safe_load_json_list_or_dict(uniqueness_data)
    elif isinstance(uniqueness_data, list):
        data = uniqueness_data
    else:
        data = None
    return safe_get_distribution_count(data, 'unique')

def ensure_list_from_json_string(data_str_or_list, lower=False, strip=True):
    """Convert JSON string, list, or single item into a clean list of strings.

    Args:
        data_str_or_list: Input data to process.
        lower: Whether to lowercase the strings (default: False).
        strip: Whether to strip whitespace from strings (default: True).

    Returns:
        List of processed strings.
    """
    if data_str_or_list is None:
        return []

    items_to_process = []
    if isinstance(data_str_or_list, str):
        loaded_data = safe_load_json_list_or_dict(data_str_or_list)
        if isinstance(loaded_data, list):
            items_to_process = loaded_data
        elif loaded_data is not None:
            items_to_process = [data_str_or_list]
        else:
            items_to_process = [data_str_or_list]
    elif isinstance(data_str_or_list, list):
        items_to_process = data_str_or_list
    elif (hasattr(data_str_or_list, '__iter__') and
          not isinstance(data_str_or_list, (str, bytes, dict))):
        items_to_process = list(data_str_or_list)
    else:
        items_to_process = [str(data_str_or_list)]

    processed_items = []
    for item in items_to_process:
        if item is None:
            continue
        s_item = str(item)
        if strip:
            s_item = s_item.strip()
        if lower:
            s_item = s_item.lower()
        if s_item:
            processed_items.append(s_item)
    return processed_items

def ensure_pattern_list(patterns_input):
    """Extract pattern strings from list, JSON list, or list of dicts.

    Args:
        patterns_input: Input containing pattern data.

    Returns:
        Sorted list of unique pattern strings.
    """
    raw_list = ensure_list_from_json_string(patterns_input, lower=False, strip=False)
    patterns = []
    for item in raw_list:
        pattern_val = None
        if isinstance(item, str):
            pattern_val = item
        elif isinstance(item, dict):
            pattern_val = (item.get('enum_value') or item.get('pattern') or
                           item.get('name'))
        if pattern_val and isinstance(pattern_val, str):
            pattern_val_stripped = pattern_val.strip()
            if pattern_val_stripped:
                patterns.append(pattern_val_stripped)
    return sorted(list(set(patterns)))

def ensure_enum_list(enums_input):
    """Extract enum strings from list, JSON list, or list of dicts, lowercased.

    Args:
        enums_input: Input containing enum data.

    Returns:
        Sorted list of unique, lowercased enum strings.
    """
    raw_list = ensure_list_from_json_string(enums_input, lower=False, strip=False)
    enums = []
    for item in raw_list:
        enum_val = None
        if isinstance(item, str):
            enum_val = item
        elif isinstance(item, dict):
            enum_val = item.get('enum_value') or item.get('name')

        if enum_val and isinstance(enum_val, str):
            enum_val_stripped = enum_val.strip()
            if enum_val_stripped:
                enums.append(enum_val_stripped)
    return sorted(list(set(e.lower() for e in enums)))

def ensure_contains_list(contains_input):
    """Ensure 'contains' field is a clean list of lowercased strings.

    Args:
        contains_input: Input containing 'contains' data.

    Returns:
        List of lowercased, stripped strings.
    """
    return ensure_list_from_json_string(contains_input, lower=True, strip=True)

def ensure_synonym_list(synonyms_input):
    """Ensure 'synonyms' field is a clean list of lowercased strings.

    Args:
        synonyms_input: Input containing synonym data.

    Returns:
        List of lowercased, stripped strings.
    """
    return ensure_list_from_json_string(synonyms_input, lower=True, strip=True)

def advanced_clean(text, nlp_model):
    """Clean text using regex and spaCy for tokenization/lemmatization.

    Args:
        text: Input text to clean.
        nlp_model: spaCy model for processing (or None).

    Returns:
        Dict with 'cleaned' text and 'tokens' set.
    """
    if not isinstance(text, str) or not text:
        return {'cleaned': '', 'tokens': set()}

    text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)
    text = text.replace('_', '').replace('-', '')
    text = re.sub(r'[^\w\s]', '', text, flags=re.UNICODE)
    text = re.sub(r'\s+', '', text).strip()
    if not text:
        return {'cleaned': '', 'tokens': set()}

    if nlp_model is None:
        lower_text = text.lower()
        tokens = set(t for t in lower_text.split() if t)
        return {'cleaned': lower_text, 'tokens': tokens}

    try:
        doc = nlp_model(text.lower())
        lemma_tokens = set()
        for token in doc:
            if (not token.is_stop and not token.is_punct and not token.is_space and
                    token.text.strip()):
                if token.is_alpha:
                    lemma_tokens.add(token.lemma_)
                else:
                    lemma_tokens.add(token.text)

        raw_tokens = set(t for t in text.lower().split() if t.strip())
        tokens = (lemma_tokens | raw_tokens) - {''}
        cleaned = ' '.join(sorted(list(tokens))) if tokens else text.lower()
    except Exception as e:
        log_error(f"NLP Error processing '{text[:50]}...'", e)
        cleaned = text.lower()
        tokens = set(t for t in cleaned.split() if t)

    return {'cleaned': cleaned, 'tokens': tokens}

def is_non_meaningful_name(name):
    """Check if an attribute name is generic or non-descriptive.

    Args:
        name: Attribute name to evaluate.

    Returns:
        Boolean indicating if the name is non-meaningful.
    """
    if not isinstance(name, str) or not name:
        return True
    name_lower = name.lower().strip()
    patterns = [
        r'^(col|column|attr|attribute|field|val|value)[_\s]?\d+$',
        r'^unnamed[:_\s]?\d*$',
        r'^field\d+$',
        r'^f\d+$'
    ]
    common_names = {'key', 'id', 'pk', 'fk', 'value', 'val', 'data', 'field',
                    'column', 'attribute', 'index', 'row', 'unnamed'}
    if (any(re.match(p, name_lower) for p in patterns) or
            name_lower in common_names or len(name_lower) < 3):
        return True
    return False

# --- Enhanced Metadata Processing Functions ---

def enhanced_metadata_preprocessing(term_data, attr_data):
    """Normalize metadata for better comparison by standardizing case and formats.
    
    Args:
        term_data: Dictionary containing term data and metadata
        attr_data: Dictionary containing attribute data and metadata
        
    Returns:
        tuple: (preprocessed_term_metadata, preprocessed_attr_metadata)
    """
    # Clone the metadata to avoid modifying originals
    term_meta = term_data.get('metadata', {}).copy() if isinstance(term_data, dict) else {}
    attr_meta = attr_data.get('metadata', {}).copy() if isinstance(attr_data, dict) else {}
    
    # Normalize patterns by standardizing case
    if 'patterns' in term_meta:
        term_meta['patterns'] = [p.lower() if isinstance(p, str) else p 
                                for p in term_meta.get('patterns', [])]
    if 'patterns' in attr_meta:
        attr_meta['patterns'] = [p.lower() if isinstance(p, str) else p 
                                for p in attr_meta.get('patterns', [])]
    
    # Normalize enum values
    if 'enums' in term_meta:
        term_meta['enums'] = [e.lower() if isinstance(e, str) else e 
                            for e in term_meta.get('enums', [])]
    if 'enums' in attr_meta:
        attr_meta['enums'] = [e.lower() if isinstance(e, str) else e 
                            for e in attr_meta.get('enums', [])]
    
    # Normalize name/technical name for better comparison
    term_name = term_data.get('name', '').lower() if isinstance(term_data, dict) else ''
    attr_name = attr_data.get('name', '').lower() if isinstance(attr_data, dict) else ''
    
    # Add special field-type detection
    term_meta['is_name_field'] = any(name_indicator in term_name 
                                    for name_indicator in ['name', 'first', 'last', 'middle'])
    term_meta['is_date_field'] = any(date_indicator in term_name 
                                    for date_indicator in ['date', 'time', 'day', 'month', 'year'])
    term_meta['is_id_field'] = any(id_indicator in term_name 
                                  for id_indicator in ['id', 'code', 'key', 'identifier'])
    
    # Apply same detection to attribute metadata
    attr_meta['is_name_field'] = any(name_indicator in attr_name 
                                   for name_indicator in ['name', 'first', 'last', 'middle'])
    attr_meta['is_date_field'] = any(date_indicator in attr_name 
                                   for date_indicator in ['date', 'time', 'day', 'month', 'year'])
    attr_meta['is_id_field'] = any(id_indicator in attr_name 
                                 for id_indicator in ['id', 'code', 'key', 'identifier'])
    
    # Normalize character type for text fields (especially names)
    if term_meta.get('is_name_field') and attr_meta.get('is_name_field'):
        equivalent_types = ['alpha_only', 'alnum', 'mixed_special']
        term_char = term_meta.get('char_type', 'unknown')
        attr_char = attr_meta.get('char_type', 'unknown')
        
        if term_char in equivalent_types and attr_char in equivalent_types:
            # Use the same normalized character type for both
            term_meta['char_type'] = 'normalized_text'
            attr_meta['char_type'] = 'normalized_text'
    
    return term_meta, attr_meta


def pattern_similarity_score(term_patterns, attr_patterns, fuzzy_matcher=None):
    """Calculate pattern similarity with partial matching support.
    
    Args:
        term_patterns: List of patterns from term
        attr_patterns: List of patterns from attribute
        fuzzy_matcher: Optional fuzzy matching function (defaults to using WRatio)
        
    Returns:
        float: Similarity score between 0 and 1
    """
    
    if not term_patterns and not attr_patterns:
        return 0.0
    
    if not term_patterns or not attr_patterns:
        return 0.0
        
    # Try exact overlap first
    term_pattern_set = set(str(p).lower() for p in term_patterns if p)
    attr_pattern_set = set(str(p).lower() for p in attr_patterns if p)
    
    overlap = len(term_pattern_set.intersection(attr_pattern_set))
    union = len(term_pattern_set.union(attr_pattern_set))
    exact_match_score = float(overlap) / union if union > 0 else 0.0
    
    # If poor exact match, try partial matching
    if exact_match_score < 0.3:
        partial_matches = 0
        for t_pattern in term_patterns:
            t_pattern_str = str(t_pattern).lower()
            for a_pattern in attr_patterns:
                a_pattern_str = str(a_pattern).lower()
                
                # Calculate string similarity
                if fuzzy_matcher:
                    similarity = fuzzy_matcher(t_pattern_str, a_pattern_str) / 100.0
                else:
                    similarity = fuzz.WRatio(t_pattern_str, a_pattern_str) / 100.0
                    
                if similarity > 0.7:  # Consider patterns similar if >70% match
                    partial_matches += similarity
        
        if partial_matches > 0:
            max_possible = max(len(term_patterns), len(attr_patterns))
            partial_score = min(partial_matches / max_possible, 1.0)
            return max(exact_match_score, partial_score * 0.8)  # Cap partial matches at 80%
    
    return exact_match_score


def enhanced_histogram_intersection(hist1, hist2):
    """Improved histogram comparison that's more robust to small shifts.
    
    Args:
        hist1: Dictionary mapping lengths to counts for term
        hist2: Dictionary mapping lengths to counts for attribute
        
    Returns:
        float: Intersection score between 0 and 1
    """
    if not hist1 or not hist2:
        return 0.0
    
    # Extract total counts, excluding special keys
    total1 = float(hist1.get('__total__', sum(v for k, v in hist1.items() 
                                          if k != '__total__' and not isinstance(k, str))))
    total2 = float(hist2.get('__total__', sum(v for k, v in hist2.items() 
                                          if k != '__total__' and not isinstance(k, str))))
    
    if total1 == 0 or total2 == 0:
        return 0.0

    # Convert to probability distributions with smoothing
    smoothed_hist1 = {k: v/total1 + 0.01 
                    for k, v in hist1.items() 
                    if k != '__total__' and not isinstance(k, str)}
    smoothed_hist2 = {k: v/total2 + 0.01 
                    for k, v in hist2.items() 
                    if k != '__total__' and not isinstance(k, str)}
    
    # Normalize after smoothing
    s1_total = sum(smoothed_hist1.values())
    s2_total = sum(smoothed_hist2.values())
    
    if s1_total == 0 or s2_total == 0:
        return 0.0
        
    smoothed_hist1 = {k: v/s1_total for k, v in smoothed_hist1.items()}
    smoothed_hist2 = {k: v/s2_total for k, v in smoothed_hist2.items()}
    
    # Regular histogram intersection (direct bin matching)
    all_keys = set(smoothed_hist1.keys()) | set(smoothed_hist2.keys())
    direct_intersection = sum(min(smoothed_hist1.get(k, 0), smoothed_hist2.get(k, 0)) 
                             for k in all_keys)
    
    # Add similarity for adjacent length bins (useful for fields like names where length can vary slightly)
    adjacent_intersection = 0
    for k1 in smoothed_hist1:
        if isinstance(k1, (int, float)):
            for offset in [-1, 1]:  # Check adjacent bins
                adjacent_key = k1 + offset
                if adjacent_key in smoothed_hist2:
                    # Count partial match with adjacent bin (with 70% value)
                    adjacent_intersection += min(smoothed_hist1[k1], 
                                               smoothed_hist2[adjacent_key]) * 0.7
    
    # Combined score (cap at 1.0)
    return min(direct_intersection + 0.5 * adjacent_intersection, 1.0)


def get_adaptive_metadata_weights(term_meta, attr_meta):
    """Determine weights based on the type of fields being compared.
    
    Args:
        term_meta: Dictionary containing term metadata
        attr_meta: Dictionary containing attribute metadata
        
    Returns:
        dict: Dictionary of weight factors for different metadata components
    """
    # Base weights
    weights = {
        'type': 1.5,              # Data type compatibility
        'char_type': 1.0,         # Character type
        'distinct_ratio': 0.75,   # Uniqueness characteristics
        'pattern': 0.5,           # Pattern matching
        'enum': 0.8,              # Enumerated values
        'space': 0.25,            # Space presence
        'numeric_sign': 0.25,     # Numeric sign patterns
        'length_histogram': 0.5   # Value length distribution
    }
    
    # Check if both are the same field type
    is_name_field = term_meta.get('is_name_field', False) and attr_meta.get('is_name_field', False)
    is_date_field = term_meta.get('is_date_field', False) and attr_meta.get('is_date_field', False)
    is_id_field = term_meta.get('is_id_field', False) and attr_meta.get('is_id_field', False)
    
    # Adjust weights for name fields
    if is_name_field:
        weights['pattern'] = 1.2      # Increase pattern importance
        weights['char_type'] = 1.5    # Character type matters more
        weights['distinct_ratio'] = 0.4  # Reduce distinct ratio importance
        weights['length_histogram'] = 0.8  # Increase length histogram importance
    
    # Adjust weights for date fields
    if is_date_field:
        weights['pattern'] = 1.5      # Pattern is critical for dates
        weights['enum'] = 0.3         # Enum values less important
        weights['length_histogram'] = 1.0  # Length is important for dates
        
    # Adjust weights for ID fields
    if is_id_field:
        weights['distinct_ratio'] = 1.2  # Uniqueness is critical for IDs
        weights['pattern'] = 1.0      # Pattern matching is important
        
    return weights


def improved_compute_metadata_score(term_data, attr_data):
    """Compute enhanced metadata similarity score between term and attribute.
    
    Args:
        term_data: Dictionary containing term data and metadata
        attr_data: Dictionary containing attribute data and metadata
        
    Returns:
        float: Enhanced metadata similarity score between 0 and 1
    """
    # Normalize metadata for better comparison
    term_meta, attr_meta = enhanced_metadata_preprocessing(term_data, attr_data)
    
    # Get adaptive weights based on field type
    weights = get_adaptive_metadata_weights(term_meta, attr_meta)
    
    score = 0.0
    max_score = 0.0
    
    # Track individual component scores for debugging
    component_scores = {}
    
    # --- Data Type Compatibility ---
    type_weight = weights['type']
    term_type = term_meta.get('type', 'unknown')
    attr_type = attr_meta.get('type', 'unknown')
    
    if term_type != 'unknown' and attr_type != 'unknown':
        max_score += type_weight
        
        # Define type families
        numeric_types = ['int', 'integer', 'decimal', 'numeric', 'float', 'double', 'number']
        date_types = ['date', 'datetime', 'timestamp', 'time']
        text_types = ['varchar', 'string', 'text', 'char', 'character']
        bool_types = ['bool', 'boolean', 'bit']
        
        # Check if both are in the same family
        term_family = next((f for f in [numeric_types, date_types, text_types, bool_types] 
                          if any(t in term_type.lower() for t in f)), None)
        attr_family = next((f for f in [numeric_types, date_types, text_types, bool_types] 
                          if any(t in attr_type.lower() for t in f)), None)
        
        type_sim = 0.0
        if term_family is not None and term_family == attr_family:
            # Within the same family, give nuanced scores
            if term_type == attr_type:
                type_sim = 1.0  # Exact match
            elif term_family == numeric_types:
                # Higher score for more compatible numeric types
                if ('int' in term_type and 'int' in attr_type) or \
                   ('float' in term_type and 'float' in attr_type):
                    type_sim = 0.9
                else:
                    type_sim = 0.8
            elif term_family == date_types:
                # Datetime types are highly compatible
                type_sim = 0.9
            else:
                type_sim = 0.8  # Text types are generally compatible
        elif term_family is not None and attr_family is not None:
            # Different families, but check for text representation of other types
            if (term_family == text_types and attr_family != text_types) or \
               (attr_family == text_types and term_family != text_types):
                type_sim = 0.5  # Text can represent other types
            else:
                type_sim = 0.0  # Incompatible families
        
        score += type_sim * type_weight
        component_scores['type_similarity'] = type_sim
    
    # --- Character Type Similarity ---
    char_weight = weights['char_type']
    term_char = term_meta.get('char_type', 'unknown')
    attr_char = attr_meta.get('char_type', 'unknown')
    
    if term_char != 'unknown' and attr_char != 'unknown':
        max_score += char_weight
        
        if term_char == attr_char:
            char_sim = 1.0
        elif term_char == 'normalized_text' and attr_char == 'normalized_text':
            # Special case for normalized text fields
            char_sim = 1.0
        elif ((term_char == 'alnum' and attr_char in ['alpha_only', 'digit_only']) or
              (attr_char == 'alnum' and term_char in ['alpha_only', 'digit_only'])):
            char_sim = 0.7
        elif ((term_char in ['alpha_only', 'digit_only', 'alnum'] and
               attr_char == 'mixed_special') or
              (attr_char in ['alpha_only', 'digit_only', 'alnum'] and
               term_char == 'mixed_special')):
            char_sim = 0.3
        else:
            char_sim = 0.1
            
        score += char_sim * char_weight
        component_scores['char_type_similarity'] = char_sim
    
    # --- Distinct Ratio Similarity ---
    ratio_weight = weights['distinct_ratio']
    term_ratio = term_meta.get('distinct_ratio', -1.0)
    attr_ratio = attr_meta.get('distinct_ratio', -1.0)
    
    if term_ratio >= 0.0 and attr_ratio >= 0.0:
        max_score += ratio_weight
        ratio_diff = abs(term_ratio - attr_ratio)
        
        # More nuanced ratio comparison
        if ratio_diff <= 0.05:  # Very close
            ratio_sim = 1.0
        elif ratio_diff <= 0.2:  # Moderately close
            ratio_sim = 0.8
        elif ratio_diff <= 0.4:  # Somewhat different
            ratio_sim = 0.5
        else:  # Very different
            ratio_sim = 0.2
            
        score += ratio_sim * ratio_weight
        component_scores['distinct_ratio_similarity'] = ratio_sim
    
    # --- Pattern Overlap with Enhanced Matching ---
    pattern_weight = weights['pattern']
    term_patterns = term_meta.get('patterns', [])
    attr_patterns = attr_meta.get('patterns', [])
    
    if term_patterns or attr_patterns:
        max_score += pattern_weight
        if term_patterns and attr_patterns:
            pattern_sim = pattern_similarity_score(term_patterns, attr_patterns, fuzz.WRatio)
            score += pattern_sim * pattern_weight
            component_scores['pattern_similarity'] = pattern_sim
    
    # --- Enum Overlap ---
    enum_weight = weights['enum']
    term_enums = [str(e).lower() for e in term_meta.get('enums', []) if e]
    attr_enums = [str(e).lower() for e in attr_meta.get('enums', []) if e]
    
    if term_enums or attr_enums:
        max_score += enum_weight
        if term_enums and attr_enums:
            # Try exact set overlap first
            overlap = len(set(term_enums).intersection(set(attr_enums)))
            union = len(set(term_enums).union(set(attr_enums)))
            enum_sim = float(overlap) / union if union > 0 else 0.0
            
            # If poor match, try fuzzy matching enum values
            if enum_sim < 0.3 and len(term_enums) > 0 and len(attr_enums) > 0:
                fuzzy_matches = 0
                for t_enum in term_enums:
                    best_match, score = process.extractOne(t_enum, attr_enums, scorer=fuzz.WRatio)
                    if score > 80:  # Only count good matches
                        fuzzy_matches += 1
                
                fuzzy_sim = fuzzy_matches / max(len(term_enums), len(attr_enums))
                enum_sim = max(enum_sim, fuzzy_sim * 0.8)  # Cap fuzzy at 80% of exact
                
            score += enum_sim * enum_weight
            component_scores['enum_similarity'] = enum_sim
    
    # --- Space Presence ---
    space_weight = weights['space']
    term_space = term_meta.get('space_presence', 'unknown')
    attr_space = attr_meta.get('space_presence', 'unknown')
    
    if term_space != 'unknown' and attr_space != 'unknown':
        max_score += space_weight
        
        if term_space == attr_space:
            space_sim = 1.0
        # Give partial credit for related space patterns
        elif (term_space == 'has_spaces' and attr_space == 'no_spaces') or \
             (attr_space == 'has_spaces' and term_space == 'no_spaces'):
            # These are different but not completely incompatible
            # For name fields, first/last name might have different space needs
            if term_meta.get('is_name_field') and attr_meta.get('is_name_field'):
                space_sim = 0.5
            else:
                space_sim = 0.2
        else:
            space_sim = 0.0
            
        score += space_sim * space_weight
        component_scores['space_similarity'] = space_sim
    
    # --- Numeric Sign Compatibility ---
    sign_weight = weights['numeric_sign']
    term_sign = term_meta.get('numeric_sign', 'unknown')
    attr_sign = attr_meta.get('numeric_sign', 'unknown')
    
    term_is_numeric = term_sign not in ['unknown', 'not_numeric']
    attr_is_numeric = attr_sign not in ['unknown', 'not_numeric']
    
    if term_is_numeric and attr_is_numeric:
        max_score += sign_weight
        
        if term_sign == attr_sign:
            sign_sim = 1.0
        elif ((term_sign == 'pos_only' and attr_sign == 'non_negative') or
              (attr_sign == 'pos_only' and term_sign == 'non_negative')):
            sign_sim = 0.7
        elif ((term_sign == 'pos_only' and attr_sign in ['mixed_sign', 'neg_only']) or
              (attr_sign == 'pos_only' and term_sign in ['mixed_sign', 'neg_only'])):
            sign_sim = 0.3
        else:
            sign_sim = 0.0
            
        score += sign_sim * sign_weight
        component_scores['numeric_sign_similarity'] = sign_sim
    elif term_is_numeric != attr_is_numeric:
        # One is numeric, one is not - significant mismatch
        max_score += sign_weight
        # No points added
        component_scores['numeric_sign_similarity'] = 0.0
    
    # --- Length Distribution Similarity ---
    length_weight = weights['length_histogram']
    term_hist = term_meta.get('length_histogram', {})
    attr_hist = attr_meta.get('length_histogram', {})
    
    if term_hist and attr_hist and term_hist.get('__total__', 0) > 0 and attr_hist.get('__total__', 0) > 0:
        max_score += length_weight
        
        length_sim = enhanced_histogram_intersection(term_hist, attr_hist)
        score += length_sim * length_weight
        component_scores['length_histogram_similarity'] = length_sim
    
    # --- Apply Name Field Special Handling ---
    if term_meta.get('is_name_field', False) and attr_meta.get('is_name_field', False):
        term_name = term_data.get('name', '').lower() if isinstance(term_data, dict) else ''
        attr_name = attr_data.get('name', '').lower() if isinstance(attr_data, dict) else ''
        
        # Clean names to compare core parts
        term_name_core = ''.join(c for c in term_name if c.isalnum()).lower()
        attr_name_core = ''.join(c for c in attr_name if c.isalnum()).lower()
        
        # Special case for name fields with high similarity despite case/formatting differences
        if term_name_core == attr_name_core:
            # Names match exactly when normalized - boost score
            name_bonus = 0.2
            component_scores['name_field_bonus'] = name_bonus
            
            # Add bonus to the total score
            score += name_bonus * max(1.0, max_score * 0.2)
    
    # --- Calculate Final Score ---
    if max_score <= 0:
        return 0.0
    
    final_metadata_score = score / max_score
    
    # Ensure score is within valid range
    return max(0.0, min(final_metadata_score, 1.0))


def process_attribute(idx_attr, nlp_model=None, sentence_model=None):
    """Preprocess an attribute row, extracting metadata and tokens.

    Args:
        idx_attr: Tuple of index and attribute data.
        nlp_model: spaCy model (optional).
        sentence_model: SentenceTransformer model (optional).

    Returns:
        Dict with processed attribute details.
    """
    idx, attr = idx_attr
    attr_dict = (attr.to_dict() if isinstance(attr, pd.Series) else
                 (attr if isinstance(attr, dict) else {}))
    original_name = str(attr_dict.get('name', '') or '').strip()
    if nlp_model is None:
        completeness_count = safe_get_completeness(attr_dict)
        uniqueness_count = safe_get_uniqueness(attr_dict)
        basic_profile_data = safe_load_json_list_or_dict(
            attr_dict.get('basic_profile', '{}'))
        distinct_count = safe_get_basic_profile_value(basic_profile_data,
                                                      'distinct_count')
        distinct_ratio = (float(distinct_count) / completeness_count
                          if completeness_count > 0 else 0.0)
        derived_type = str(attr_dict.get('derived_type', 'unknown') or
                           'unknown').lower()
        patterns = ensure_pattern_list(attr_dict.get('patterns', []))
        enums = ensure_enum_list(attr_dict.get('enums', []))
        length_dist_raw = safe_load_json_list_or_dict(
            attr_dict.get('length_distribution', '[]'))
        length_histogram = _parse_length_distribution(length_dist_raw)
        metadata = {
            'type': derived_type,
            'uniqueness_count': uniqueness_count,
            'completeness_count': completeness_count,
            'distinct_ratio': round(distinct_ratio, 4),
            'patterns': patterns,
            'enums': enums,
            'char_type': 'unknown',
            'space_presence': 'unknown',
            'numeric_sign': 'unknown',
            'length_histogram': length_histogram
        }
        return {
            'idx': idx,
            'original': attr_dict,
            'name': original_name,
            'names': [original_name] if original_name else [],
            'names_lower_set': {original_name.lower()} if original_name else set(),
            'tokens': set(),
            'is_non_meaningful': is_non_meaningful_name(original_name),
            'embeddings': {'spacy': None},
            'sentence_embeddings': np.array([]),
            'name_indices': [],
            'metadata': metadata
        }

    technical_name = str(attr_dict.get('technical_name', '') or '').strip()
    description = str(attr_dict.get('description', '') or '').strip()
    cleaned_original = advanced_clean(original_name, nlp_model)
    cleaned_technical = advanced_clean(technical_name, nlp_model)
    cleaned_description = advanced_clean(description, nlp_model)
    combined_tokens = (cleaned_original['tokens'] | cleaned_technical['tokens'] |
                       cleaned_description['tokens'])
    names_to_process = [name for name in [original_name, technical_name,
                                          description] if name]
    unique_names_for_encoding = sorted(list(set(names_to_process)))
    names_lower_set = {n.lower() for n in unique_names_for_encoding}
    cleaned_names_str = ' '.join(unique_names_for_encoding)
    spacy_embedding = (nlp_model(cleaned_names_str) if cleaned_names_str else
                       nlp_model(''))
    if not (spacy_embedding and spacy_embedding.has_vector and
            spacy_embedding.vector_norm > 0):
        spacy_embedding = nlp_model('')

    completeness_count = safe_get_completeness(attr_dict)
    uniqueness_count = safe_get_uniqueness(attr_dict)
    basic_profile_data = safe_load_json_list_or_dict(
        attr_dict.get('basic_profile', '{}'))
    char_dist_data = safe_load_json_list_or_dict(
        attr_dict.get('character_distribution', '[]'))
    space_dist_data = safe_load_json_list_or_dict(
        attr_dict.get('space_distribution', '[]'))
    numeric_dist_data = safe_load_json_list_or_dict(
        attr_dict.get('numeric_distribution', '[]'))
    length_dist_raw = safe_load_json_list_or_dict(
        attr_dict.get('length_distribution', '[]'))
    distinct_count = safe_get_basic_profile_value(basic_profile_data,
                                                  'distinct_count')
    distinct_ratio = (float(distinct_count) / completeness_count
                      if completeness_count > 0 else 0.0)
    char_type = determine_char_type(char_dist_data)
    space_presence = determine_space_presence(space_dist_data)
    numeric_sign = determine_numeric_sign(numeric_dist_data)
    derived_type = str(attr_dict.get('derived_type', 'unknown') or
                       'unknown').lower()
    is_numeric_type = any(t in derived_type for t in ['int', 'decimal', 'numeric',
                                                      'float', 'double', 'number'])
    if numeric_sign in ['unknown', 'not_numeric'] and is_numeric_type:
        numeric_sign = 'unknown'
    elif not is_numeric_type:
        numeric_sign = 'not_numeric'
    patterns = ensure_pattern_list(attr_dict.get('patterns', []))
    enums = ensure_enum_list(attr_dict.get('enums', []))
    length_histogram = _parse_length_distribution(length_dist_raw)

    metadata = {
        'type': derived_type,
        'uniqueness_count': uniqueness_count,
        'completeness_count': completeness_count,
        'distinct_ratio': round(distinct_ratio, 4),
        'patterns': patterns,
        'enums': enums,
        'char_type': char_type,
        'space_presence': space_presence,
        'numeric_sign': numeric_sign,
        'length_histogram': length_histogram
    }
    return {
        'idx': idx,
        'original': attr_dict,
        'name': original_name,
        'names': unique_names_for_encoding,
        'names_lower_set': names_lower_set,
        'tokens': combined_tokens,
        'is_non_meaningful': is_non_meaningful_name(original_name) or not original_name,
        'embeddings': {'spacy': spacy_embedding},
        'metadata': metadata
    }


def preprocess_term(idx_term, nlp_model=None, sentence_model=None):
    """Preprocess a term, extracting metadata, tokens, and the predefined threshold
       (scaled from percentage to 0-1 range).
       Now also includes technical_name in all_match_names for fuzzy matching.
    """
    idx, term = idx_term
    term_dict = (term.to_dict() if isinstance(term, pd.Series) else
                 (term if isinstance(term, dict) else {}))
    
    # Extract all possible naming identifiers
    original_name = str(term_dict.get('name', '') or '').strip()
    technical_name = str(term_dict.get('technical_name', '') or '').strip()
    synonyms = ensure_list_from_json_string(term_dict.get('synonyms', []),
                                            lower=True, strip=True)
    contains = ensure_contains_list(term_dict.get('contains', []))
    
    # Build a comprehensive set of all possible matching strings
    all_match_names = set()
    if original_name:
        all_match_names.add(original_name.lower())
    if technical_name:  # Add technical_name to matching set
        all_match_names.add(technical_name.lower())
    all_match_names.update(synonyms)
    all_match_names.update(contains)
    all_match_names.discard('')  # Remove empty strings
    
    
    # --- Extract and scale predefined threshold ---
    raw_threshold = term_dict.get('threshold') # Get the raw value (e.g., 67)
    scaled_predefined_threshold = 0.5 # Initialize with fallback value
    
    
    try:
        # Attempt to convert raw value to float
        numeric_threshold = float(raw_threshold)
        # Scale the numeric value by dividing by 100.0
        scaled_value = numeric_threshold / 100.0

        # Check if the scaled value is within the valid range (0.0 to 1.0)
        if 0.0 <= scaled_value <= 1.0:
            scaled_predefined_threshold = scaled_value
            scaled_predefined_threshold = scaled_predefined_threshold+0.05 # Add small boost
    except (ValueError, TypeError, pd.isna):
        # Keep the fallback value (0.5) if raw value is missing, None, NaN,
        # or cannot be converted to float
        pass
    
    
    # --- Existing metadata extraction ---
    advanced_metrics_json_str = term_dict.get('advanced_profiling_metrics', '')
    advanced_metrics_data = safe_load_json_list_or_dict(advanced_metrics_json_str)
    if not isinstance(advanced_metrics_data, dict):
        advanced_metrics_data = {}

    if nlp_model is None: # Minimal processing if NLP model is unavailable
        completeness_count = safe_get_completeness(term_dict)
        uniqueness_count = safe_get_uniqueness(term_dict)
        basic_profile_data = advanced_metrics_data.get('basic_profile', {})
        distinct_count = safe_get_basic_profile_value(basic_profile_data,
                                                      'distinct_count')
        distinct_ratio = (float(distinct_count) / completeness_count
                          if completeness_count > 0 else 0.0)
        derived_type = str(term_dict.get('derived_type', 'unknown') or
                           'unknown').lower()
        patterns_raw = advanced_metrics_data.get('short_universal_patterns', [])
        enums_raw = advanced_metrics_data.get('value_distribution', [])
        length_dist_raw = advanced_metrics_data.get('length_distribution', [])
        patterns = ensure_pattern_list(patterns_raw)
        enums = ensure_enum_list(enums_raw)
        length_histogram = _parse_length_distribution(length_dist_raw)
        metadata = {
            'type': derived_type,
            'uniqueness_count': uniqueness_count,
            'completeness_count': completeness_count,
            'distinct_ratio': round(distinct_ratio, 4),
            'patterns': patterns,
            'enums': enums,
            'char_type': 'unknown',
            'space_presence': 'unknown',
            'numeric_sign': 'unknown',
            'length_histogram': length_histogram
        }
        return {
            'idx': idx,
            'original': term_dict,
            'name': original_name,
            'technical_name': technical_name,  # Include technical_name in the result
            'tokens': set(),
            'sentence_embedding': np.array([]),
            'metadata': metadata,
            'synonyms': synonyms,
            'contains': contains,
            'all_match_names': all_match_names,
            'threshold': scaled_predefined_threshold
        }

    # --- Full processing with NLP model ---
    description = str(term_dict.get('description', '') or '').strip()
    tags_str = term_dict.get('tags', '')
    tags_list = ensure_list_from_json_string(tags_str, lower=True, strip=True)
    tags_text = ' '.join(tags_list)
    
    # Process all identifiers with NLP
    cleaned_name = advanced_clean(original_name, nlp_model)
    cleaned_technical_name = advanced_clean(technical_name, nlp_model)
    cleaned_description = advanced_clean(description, nlp_model)
    cleaned_tags = advanced_clean(tags_text, nlp_model)
    
    # Combine tokens from all sources
    tokens = (cleaned_name['tokens'] | cleaned_technical_name['tokens'] |
              cleaned_description['tokens'] | cleaned_tags['tokens'])

    # Extract metadata from advanced profiling metrics
    completeness_count = safe_get_completeness(term_dict)
    uniqueness_count = safe_get_uniqueness(term_dict)
    basic_profile_data = advanced_metrics_data.get('basic_profile', {})
    char_dist_data = advanced_metrics_data.get('character_distribution', [])
    space_dist_data = advanced_metrics_data.get('space_distribution', [])
    numeric_dist_data = advanced_metrics_data.get('numeric_distribution', [])
    patterns_raw = advanced_metrics_data.get('short_universal_patterns', [])
    enums_raw = advanced_metrics_data.get('value_distribution', [])
    length_dist_raw = advanced_metrics_data.get('length_distribution', [])
    
    distinct_count = safe_get_basic_profile_value(basic_profile_data, 'distinct_count')
    distinct_ratio = (float(distinct_count) / completeness_count
                      if completeness_count > 0 else 0.0)
    
    # Determine characteristic types
    char_type = determine_char_type(char_dist_data)
    space_presence = determine_space_presence(space_dist_data)
    numeric_sign = determine_numeric_sign(numeric_dist_data)
    derived_type = str(term_dict.get('derived_type', 'unknown') or 'unknown').lower()
    
    # Adjust numeric sign based on derived type
    is_numeric_type = any(t in derived_type for t in ['int', 'decimal', 'numeric',
                                                     'float', 'double', 'number'])
    if numeric_sign in ['unknown', 'not_numeric'] and is_numeric_type:
        numeric_sign = 'unknown'
    elif not is_numeric_type:
        numeric_sign = 'not_numeric'
    
    # Process patterns and enums
    patterns = ensure_pattern_list(patterns_raw)
    enums = ensure_enum_list(enums_raw)
    length_histogram = _parse_length_distribution(length_dist_raw)

    # Create metadata dictionary
    metadata = {
        'type': derived_type,
        'uniqueness_count': uniqueness_count,
        'completeness_count': completeness_count,
        'distinct_ratio': round(distinct_ratio, 4),
        'patterns': patterns,
        'enums': enums,
        'char_type': char_type,
        'space_presence': space_presence,
        'numeric_sign': numeric_sign,
        'length_histogram': length_histogram
    }
    
    # Process term through sentence transformer if available
    term_sentence_embedding = np.array([])
    if sentence_model is not None:
        try:
            # Encode term name and technical name for better similarity matching
            text_to_encode = original_name
            if technical_name:
                text_to_encode = f"{original_name} {technical_name}" 
            term_sentence_embedding = sentence_model.encode(text_to_encode)
        except Exception as e:
            log_error(f"Error encoding term '{original_name}' with SBERT", e)
    
    return {
        'idx': idx,
        'original': term_dict,
        'name': original_name,
        'technical_name': technical_name,
        'tokens': tokens,
        'sentence_embedding': term_sentence_embedding,
        'metadata': metadata,
        'synonyms': synonyms,
        'contains': contains,
        'all_match_names': all_match_names,
        'threshold': scaled_predefined_threshold
    }


def _calculate_weights(term_name, attr_details, fuzzy_score, jaccard_score,
                       semantic_score):
    """Calculate dynamic weights for combining similarity scores.

    Args:
        term_name: Name of the term.
        attr_details: Attribute details dictionary.
        fuzzy_score: Fuzzy matching score.
        jaccard_score: Jaccard similarity score.
        semantic_score: Semantic similarity score.

    Returns:
        Dict of normalized weights.
    """
    weights = {'semantic': 0.4, 'tfidf': 0.2, 'fuzzy': 0.3, 'jaccard': 0.1}
    fuzzy_score = (float(fuzzy_score) if fuzzy_score is not None and
                   not np.isnan(fuzzy_score) else 0.0)
    jaccard_score = (float(jaccard_score) if jaccard_score is not None and
                     not np.isnan(jaccard_score) else 0.0)
    semantic_score = (float(semantic_score) if semantic_score is not None and
                      not np.isnan(semantic_score) else 0.0)
    if attr_details.get('is_non_meaningful', False):
        weights = {'semantic': 0.5, 'tfidf': 0.3, 'fuzzy': 0.1, 'jaccard': 0.1}
        if semantic_score < 0.5:
            weights = {'semantic': 0.25, 'tfidf': 0.25, 'fuzzy': 0.25,
                       'jaccard': 0.25}
    else:
        if fuzzy_score > 0.95:
            weights['fuzzy'] = min(weights.get('fuzzy', 0) + 0.3, 0.7)
        elif fuzzy_score > 0.85:
            weights['fuzzy'] = min(weights.get('fuzzy', 0) + 0.15, 0.5)
        if semantic_score > 0.85:
            weights['semantic'] = min(weights.get('semantic', 0) + 0.2, 0.6)
        elif semantic_score < 0.4:
            weights['semantic'] = max(weights.get('semantic', 0) - 0.15, 0.1)
        if jaccard_score > 0.8:
            weights['jaccard'] = min(weights.get('jaccard', 0) + 0.1, 0.3)
        elif jaccard_score < 0.1:
            weights['jaccard'] = max(weights.get('jaccard', 0) - 0.05, 0.05)

    total_weight = sum(weights.values())
    return ({key: weight / total_weight for key, weight in weights.items()}
            if total_weight > 0 else
            {'semantic': 0.25, 'tfidf': 0.25, 'fuzzy': 0.25, 'jaccard': 0.25})


def compute_attribute_mappings(attr, preprocessed_terms, nlp_model=None,
                              sentence_model=None, tfidf_vectorizer=None,
                              tfidf_matrix=None):
    """Compute potential term mappings for an attribute with boosting.
    Uses improved metadata scoring for better accuracy, especially with name fields.
    """
    global NLP_MODEL, SENTENCE_MODEL
    if nlp_model is None:
        nlp_model = NLP_MODEL
    if sentence_model is None:
        sentence_model = SENTENCE_MODEL

    attr_idx = attr.get('idx', -1)
    attr_name = attr.get('name', f"attr_idx_{attr_idx}")
    attr_tokens = attr.get('tokens', set())
    attr_primary_name_lower = attr.get('name', '').lower()
    attr_sentence_embeddings = attr.get('sentence_embeddings', np.array([]))
    attr_spacy_embedding = attr.get('embeddings', {}).get('spacy')
    attr_metadata = attr.get('metadata', {})
    attr_is_non_meaningful = attr.get('is_non_meaningful', False)
    attr_name_indices = attr.get('name_indices', [])

    models_ok = nlp_model is not None and sentence_model is not None
    tfidf_ok = (tfidf_vectorizer is not None and tfidf_matrix is not None and
                attr_name_indices is not None and len(attr_name_indices) > 0)
    has_spacy_vector = (attr_spacy_embedding is not None and
                        hasattr(attr_spacy_embedding, 'has_vector') and
                        attr_spacy_embedding.has_vector and
                        attr_spacy_embedding.vector_norm > 0)

    potential_mappings = []
    # No longer collecting scores for dynamic threshold

    for term_data in preprocessed_terms:
        term_name = term_data.get('name')
        if not term_name:
            continue
        term_metadata = term_data.get('metadata', {})
        term_tokens = term_data.get('tokens', set())
        term_all_match_names_set = term_data.get('all_match_names', set())
        term_sentence_embedding = term_data.get('sentence_embedding', np.array([]))
        term_synonyms = term_data.get('synonyms', [])
        term_threshold = term_data.get('threshold', 0.5) # Get term's threshold
        all_term_names_for_spacy = [term_name] + term_synonyms

        # --- Direct Fuzzy Match Check (Early Exit) ---
        if attr_primary_name_lower and term_all_match_names_set:
            term_match_list_for_fuzz = list(term_all_match_names_set)
            best_fuzzy_match = process.extractOne(
                attr_primary_name_lower, term_match_list_for_fuzz,
                scorer=fuzz.WRatio, score_cutoff=FUZZY_EARLY_EXIT_THRESHOLD - 5)

            if best_fuzzy_match and best_fuzzy_match[1] >= FUZZY_EARLY_EXIT_THRESHOLD:
                # Use the IMPROVED metadata score calculation here
                metadata_score = improved_compute_metadata_score(term_data, attr)
                final_score = min(DIRECT_MATCH_BASE_SCORE +
                                 (metadata_score * DIRECT_MATCH_METADATA_BOOST),
                                 MAX_DIRECT_MATCH_SCORE)
                final_score = float(final_score)
                # Apply MIN_SCORE_FLOOR here too, although direct matches usually exceed it
                if final_score >= MIN_SCORE_FLOOR:
                    potential_mappings.append({
                        'term': term_name,
                        'score': final_score,
                        'match_type': 'fuzzy_direct',
                        'threshold': term_threshold # Include threshold
                    })
                break # Early exit on high confidence fuzzy match

        # --- Compute Individual Similarity Scores ---
        semantic_score, tfidf_sim, fuzzy_score, jaccard_sim = 0.0, 0.0, 0.0, 0.0

        # Semantic Similarity (SpaCy + SentenceBERT)
        sentence_sim = 0.0
        if (models_ok and
                term_sentence_embedding.ndim == 1 and
                term_sentence_embedding.size > 0 and
                attr_sentence_embeddings.ndim == 2 and
                attr_sentence_embeddings.shape[0] > 0 and
                term_sentence_embedding.shape[0] == attr_sentence_embeddings.shape[1]):
            try:
                sims = cosine_similarity(term_sentence_embedding.reshape(1, -1),
                                        attr_sentence_embeddings)
                sentence_sim = max(0.0, min(1.0, float(np.max(sims)
                                                        if sims.size > 0 else 0.0)))
            except Exception as e:
                log_info(f"Sentence sim error '{attr_name}' vs '{term_name}': {e}")
        spacy_sim = 0.0
        if models_ok and has_spacy_vector:
            max_spacy_sim = 0.0
            valid_term_names_for_spacy = [t for t in all_term_names_for_spacy if t]
            if valid_term_names_for_spacy:
                try:
                    term_docs = list(nlp_model.pipe([t.lower() for t in
                                                    valid_term_names_for_spacy]))
                    for term_doc in term_docs:
                        if (term_doc.has_vector and term_doc.vector_norm > 0 and
                                # Fix for 'is_oov' attribute error:
                                hasattr(term_doc, 'is_oov') and hasattr(attr_spacy_embedding, 'is_oov') and
                                not term_doc.is_oov and not attr_spacy_embedding.is_oov):
                            max_spacy_sim = max(max_spacy_sim,
                                              attr_spacy_embedding.similarity(term_doc))
                except Exception as e:
                    log_info(f"SpaCy sim error '{attr_name}' vs '{term_name}': {e}")
            spacy_sim = max(0.0, min(1.0, float(max_spacy_sim)))
        semantic_score = float(0.6 * sentence_sim + 0.4 * spacy_sim)

        # TF-IDF Similarity
        if tfidf_ok:
            try:
                max_tfidf_sim = 0.0
                term_match_strs_for_tfidf = list(term_all_match_names_set)
                valid_term_strs = [s for s in term_match_strs_for_tfidf if s]
                if valid_term_strs:
                    tfidf_term_vecs = tfidf_vectorizer.transform(valid_term_strs)
                    max_idx = max(attr_name_indices)
                    if max_idx < tfidf_matrix.shape[0]:
                        attr_tfidf_vectors = tfidf_matrix[attr_name_indices]
                        sims = cosine_similarity(tfidf_term_vecs, attr_tfidf_vectors)
                        if sims.size > 0:
                            max_tfidf_sim = sims.max()
                tfidf_sim = max(0.0, min(1.0, float(max_tfidf_sim)))
            except Exception as e:
                tfidf_sim = 0.0

        # Fuzzy Matching Score (if not direct matched earlier)
        if attr_primary_name_lower and term_all_match_names_set:
            try:
                term_match_list_for_fuzz = list(term_all_match_names_set)
                best_fuzzy_match_complex = process.extractOne(
                    attr_primary_name_lower, term_match_list_for_fuzz,
                    scorer=fuzz.WRatio, score_cutoff=50) # Use a lower cutoff for regular calculation
                fuzzy_score = (max(0.0, min(1.0, float(best_fuzzy_match_complex[1] / 100.0)))
                              if best_fuzzy_match_complex else 0.0)
            except Exception as e:
                fuzzy_score = 0.0
        else:
            fuzzy_score = 0.0

        # Jaccard Similarity
        if attr_tokens and term_tokens:
            intersection_size = len(attr_tokens.intersection(term_tokens))
            union_size = len(attr_tokens.union(term_tokens))
            jaccard_sim = float(intersection_size) / union_size if union_size > 0 else 0.0
        else:
            jaccard_sim = 0.0

        # Combine Textual Scores using Weights
        weights = _calculate_weights(term_name, attr, fuzzy_score, jaccard_sim,semantic_score)
        combined_score = float(weights['semantic'] * semantic_score +
                              weights['tfidf'] * tfidf_sim +
                              weights['fuzzy'] * fuzzy_score +
                              weights['jaccard'] * jaccard_sim)

        # Calculate Metadata Score using IMPROVED version
        metadata_score = improved_compute_metadata_score(term_data, attr)
        
        # Final Score Calculation with Conditional Boost/Penalty
        final_score = combined_score
        HIGH_METADATA_THRESHOLD = 0.85
        MIN_TEXTUAL_FOR_BOOST = 0.30
        LOW_METADATA_THRESHOLD = 0.4
        METADATA_PENALTY_FACTOR = 0.65
        METADATA_BOOST_FACTOR = 0.4

        if attr_is_non_meaningful: # Different logic for non-meaningful names
            if metadata_score < LOW_METADATA_THRESHOLD and combined_score > 0.1:
                # Penalize more heavily if metadata mismatch for generic attributes
                final_score = combined_score * METADATA_PENALTY_FACTOR * 0.8
            else:
                # Rely more on metadata score for generic attributes
                final_score = (combined_score * 0.5) + (metadata_score * 0.5)
        else: # Logic for meaningful attribute names
            if metadata_score < LOW_METADATA_THRESHOLD and combined_score > 0.1:
                # Apply penalty if metadata doesn't match well
                final_score = combined_score * METADATA_PENALTY_FACTOR
            elif (metadata_score >= HIGH_METADATA_THRESHOLD and
                 combined_score >= MIN_TEXTUAL_FOR_BOOST):
                # Apply boost if metadata matches well AND textual similarity is reasonable
                final_score = combined_score * (1.0 + metadata_score *
                                               METADATA_BOOST_FACTOR)

        # Clamp final score between 0 and 1.1 (allow > 1 for boosted scores)
        final_score = float(max(0.0, min(final_score, 1.1)))
        
        # Add to potential mappings if score is above the minimum floor
        if final_score >= MIN_SCORE_FLOOR:
            potential_mappings.append({
                'term': term_name,
                'score': final_score,
                'match_type': 'computed',
                'threshold': term_threshold # Include threshold
            })

    # Sort mappings by score descendingly
    potential_mappings.sort(key=lambda x: x['score'], reverse=True)

    # Return attribute name and the list of potential mappings
    return (attr_name, potential_mappings)


def _parse_length_distribution(dist_data):
    """Parse length distribution data into a {length: count} dict.

    Args:
        dist_data: List of length distribution dictionaries.

    Returns:
        Dict mapping lengths to counts.
    """
    if not isinstance(dist_data, list):
        return {}
    length_counts = defaultdict(int)
    total_count = 0
    for item in dist_data:
        if isinstance(item, dict):
            length_val_str = str(item.get('length_value', item.get('name', '')))
            count = item.get('value_count', 0)
            try:
                length = int(length_val_str)
                current_count = float(count) if count is not None else 0
                if length >= 0 and current_count > 0:
                    length_counts[length] += int(current_count)
                    total_count += int(current_count)
            except (ValueError, TypeError):
                continue
    if total_count > 0:
        length_counts['__total__'] = total_count
    return dict(length_counts)


def _calculate_histogram_intersection(hist1, hist2):
    """Calculate histogram intersection similarity between two length histograms.

    Args:
        hist1: First histogram dictionary.
        hist2: Second histogram dictionary.

    Returns:
        Float similarity score (0-1).
    """
    if not hist1 or not hist2:
        return 0.0
    total1 = float(hist1.get('__total__', sum(v for k, v in hist1.items()
                                              if k != '__total__')))
    total2 = float(hist2.get('__total__', sum(v for k, v in hist2.items()
                                              if k != '__total__')))
    if total1 == 0 or total2 == 0:
        return 0.0

    intersection_score = 0.0
    all_keys = set(hist1.keys()) | set(hist2.keys())
    all_keys.discard('__total__')

    for length in all_keys:
        prob1 = hist1.get(length, 0) / total1
        prob2 = hist2.get(length, 0) / total2
        intersection_score += min(prob1, prob2)
    return max(0.0, min(intersection_score, 1.0))

def validate_geographic_mappings(mapping_results, attributes_df, terms_df):
    """
    Aggressively validates geographic mappings to prevent misclassification.
    Shows exact values that triggered remapping for debugging.
    Handles proper value distribution structure.
    Handles case variations in term names (CITY, city, City, etc.)
    """
    # Comprehensive country list (ISO codes + full names)
    country_indicators = {
        # ISO codes
        "USA", "US", "UK", "UAE", "CA", "AU", "DE", "FR", "JP", "CN", "RU", "BR", "MX", "IN", 
        # Full names (lowercase for case-insensitive matching)
        "united states", "united states of america", "america", "canada", "mexico", 
        "united kingdom", "england", "france", "germany", "australia", "china", "japan", 
        "india", "brazil", "russia", "spain", "italy", "netherlands", "sweden", "norway",
        # Add all other countries as needed
    }
    
    # State/province indicators - both abbreviations and full names
    state_indicators = {
        # US states - abbreviations
        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN",
        "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", 
        "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", 
        "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
        # Canadian provinces - abbreviations
        "AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE", "QC", "SK", "YT",
        # State names (full list)
        "alabama", "alaska", "arizona", "california", "texas", "florida", "new york",
        "colorado", "connecticut", "delaware", "georgia", "hawaii", "idaho", "illinois",
        "indiana", "iowa", "kansas", "kentucky", "louisiana", "maine", "maryland",
        "massachusetts", "michigan", "minnesota", "mississippi", "missouri", "montana",
        "nebraska", "nevada", "new hampshire", "new jersey", "new mexico", "north carolina",
        "north dakota", "ohio", "oklahoma", "oregon", "pennsylvania", "rhode island",
        "south carolina", "south dakota", "tennessee", "utah", "vermont", "virginia",
        "washington", "west virginia", "wisconsin", "wyoming"
    }
    
    # Pattern-based checks
    country_patterns = [
        r"\b[A-Z]{2,3}\b",  # ISO codes like US, UK, UAE
        r"\bunited\s+states\b",  # Specific country patterns
        r"\bunited\s+kingdom\b",
        r"\b(north|south)\s+[a-z]+\b"  # North/South patterns often used for countries
    ]
    
    revised_mappings = mapping_results.copy()
    
    # Get term IDs and names dictionary for lookups
    term_id_to_name = {}
    term_name_to_id = {}
    
    # Build comprehensive term mappings from the terms_df dataframe
    for _, term_row in terms_df.iterrows():
        term_id = term_row.get("id")
        term_name = term_row.get("name", "")
        
        # Store original term name keyed by ID for reverse lookup
        term_id_to_name[term_id] = term_name
        
        # Use lowercase version for case-insensitive lookups
        term_name_to_id[term_name.lower()] = term_id
        
        # Also add plural forms for common geographic terms
        if term_name.lower() == "city":
            term_name_to_id["cities"] = term_id
        elif term_name.lower() == "country":
            term_name_to_id["countries"] = term_id
        elif term_name.lower() == "state":
            term_name_to_id["states"] = term_id
        elif term_name.lower() == "province":
            term_name_to_id["provinces"] = term_id
    
    # Find term IDs by looking them up in the system
    city_term_id = None
    country_term_id = None  
    state_term_id = None
    
    # Find geographic term IDs using case-insensitive lookup
    for term_name_lower, term_id in term_name_to_id.items():
        if term_name_lower in ["city", "cities"]:
            city_term_id = term_id
        elif term_name_lower in ["country", "countries"]:
            country_term_id = term_id
        elif term_name_lower in ["state", "states", "province", "provinces"]:
            state_term_id = term_id
    
    if city_term_id is None:
        return mapping_results
    
    remapping_details = {}  # Store details about remapping decisions for logging
    
    for attr_id, mapped_term_id in mapping_results.items():
        # Skip attributes not mapped to city
        if mapped_term_id != city_term_id:
            continue
            
        # Get the attribute data
        attr_row = attributes_df[attributes_df["id"] == attr_id].iloc[0] if not attributes_df[attributes_df["id"] == attr_id].empty else None
        
        if attr_row is None:
            continue
            
        attr_name = attr_row.get("name", "").lower()
        
        # Initialize remapping info
        remapping_info = {
            "attr_name": attr_row.get("name"),
            "original_term": term_id_to_name.get(city_term_id, "city"),
            "trigger_values": [],
            "new_term": None
        }
        
        # Check if attribute name itself contains country or state indicators
        attr_name_words = attr_name.lower().split()
        country_words = [word for word in attr_name_words if word in country_indicators]
        state_words = [word for word in attr_name_words if word in state_indicators]
        
        if country_words:
            remapping_info["trigger_values"].append(f"Column name contains country indicator: {', '.join(country_words)}")
            if country_term_id:
                revised_mappings[attr_id] = country_term_id
                remapping_info["new_term"] = term_id_to_name.get(country_term_id, "country")
            else:
                revised_mappings.pop(attr_id, None)
                remapping_info["new_term"] = "REMOVED"
            remapping_details[attr_id] = remapping_info
            continue
            
        elif state_words and state_term_id:
            remapping_info["trigger_values"].append(f"Column name contains state indicator: {', '.join(state_words)}")
            revised_mappings[attr_id] = state_term_id
            remapping_info["new_term"] = term_id_to_name.get(state_term_id, "state")
            remapping_details[attr_id] = remapping_info
            continue
            
        # Check sample values in the format you provided
        value_dist_raw = attr_row.get("value_distribution", "[]")
        
        try:
            value_dist = json.loads(value_dist_raw) if isinstance(value_dist_raw, str) else value_dist_raw
            
            # Handle empty or non-list value distributions
            if not isinstance(value_dist, list) or not value_dist:
                continue
                
            # Extract values in the format from your example
            sample_values = []
            for item in value_dist:
                # Extract both name and enum_value with count
                enum_value = item.get("enum_value", "")
                name_value = item.get("name", "")
                count = item.get("value_count", 0) or item.get("count", 0)
                
                # Use both name and enum_value for matching
                if enum_value:
                    sample_values.append((str(enum_value), count))
                if name_value and name_value != enum_value:
                    sample_values.append((str(name_value), count))
            
            # Ensure we have values to check
            if not sample_values:
                continue
                
            # Track specific values that triggered remapping
            country_matches = []
            state_matches = []
            pattern_matches = []
            
            # Get top values by frequency
            top_values = sorted(sample_values, key=lambda x: x[1], reverse=True)[:20]
            
            # Check for states (US state codes are often misclassified as cities)
            state_match_count = 0
            total_samples = len(top_values)
            
            for val, count in top_values:
                val_upper = val.upper()
                val_lower = val.lower()
                
                # Look for exact matches against state indicators
                if val_upper in state_indicators or val_lower in state_indicators:
                    state_match_count += 1
                    state_matches.append(f"{val} (count: {count}) - matches state abbreviation/name")
            
            # Check for countries
            country_match_count = 0
            for val, count in top_values:
                val_lower = val.lower()
                country_match = next((country for country in country_indicators 
                                    if country.lower() == val_lower or val_lower == country.lower()), None)
                if country_match:
                    country_match_count += 1
                    country_matches.append(f"{val} (count: {count}) - matches country '{country_match}'")
            
            # HARDCORE CHECK: 
            # Conditions:
            # 1. If ANY country values are found, remap to country
            # 2. If more than 30% of values are states, remap to state
            if country_match_count > 0:
                remapping_info["trigger_values"].extend(country_matches)
                if country_term_id:
                    revised_mappings[attr_id] = country_term_id
                    remapping_info["new_term"] = term_id_to_name.get(country_term_id, "country")
                else:
                    revised_mappings.pop(attr_id, None)
                    remapping_info["new_term"] = "REMOVED"
                remapping_details[attr_id] = remapping_info
            
            # If significant number of values match state codes, remap to state
            elif state_match_count > 0 and (state_match_count / total_samples) > 0.3 and state_term_id:
                remapping_info["trigger_values"].extend(state_matches)
                revised_mappings[attr_id] = state_term_id
                remapping_info["new_term"] = term_id_to_name.get(state_term_id, "state")
                remapping_details[attr_id] = remapping_info
                
        except Exception as e:
            log_error(f"Error processing value distribution for {attr_name}", e)
            continue
    
    # Generate a detailed report of all remappings
    if remapping_details:
        for attr_id, info in remapping_details.items():
            for val in info['trigger_values']:
                log_info(f"  - {val}")
            log_info("-" * 50)
        log_info("===============================================")
    
    return revised_mappings