import os
import sys
import json
import re
import io
import tempfile
import datetime
import logging
import traceback
from copy import deepcopy
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import dedupe
from dedupe import variables
from rapidfuzz import fuzz
from uuid import uuid4
from functools import reduce
from pyspark import StorageLevel

# Configure logging for production-level logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
logger = logging.getLogger(__name__)


class DedupeOptimizer:
    def __init__(self, config):
        self.spark_conf = config.get('spark_conf', {})
        self.job_config = config.get('job_config', {})

        logger.info("Initializing DedupeOptimizer")

        self._validate_config()
        self.spark = self._init_spark()
        self.fields = self.job_config.get('fields', [])
        self.settings = self.job_config.get('settings', {})
        self.external_credentials = self.job_config.get('external_credentials', {})
        self.df_spark = None
        # Detect source-to-asset mode
        self.is_source_asset = self._is_source_asset_mode()

    def _is_source_asset_mode(self):
        """
        Detect if this is source-to-asset deduplication mode
        
        Returns:
            bool: True if source-to-asset mode, False if single asset mode
        """
        return "source_jdbc_options" in self.job_config

    def _validate_config(self):
        # Validate critical configuration parts before processing
        if not self.job_config:
            raise ValueError("Missing job_config section in configuration")
        
        is_source_asset = self._is_source_asset_mode()
        
        if is_source_asset:
            # Source-to-asset mode validation
            logger.info("[Source-to-Asset] Validating source-to-asset mode configuration...")
            
            # Validate target JDBC options
            required_jdbc = ['url', 'user', 'password', 'driver']
            target_jdbc_opts = self.job_config.get('jdbc_options', {})
            if not target_jdbc_opts:
                raise ValueError("[Source-to-Asset] Missing required JDBC options (jdbc_options) for target in job_config")
            missing = [opt for opt in required_jdbc if opt not in target_jdbc_opts]
            if missing:
                raise ValueError(f"[Source-to-Asset] Missing required JDBC options for target: {', '.join(missing)}")
            
            # Validate source JDBC options
            source_jdbc_opts = self.job_config.get('source_jdbc_options', {})
            if not source_jdbc_opts:
                raise ValueError("[Source-to-Asset] Missing required JDBC options (source_jdbc_options) for source in job_config")
            missing = [opt for opt in required_jdbc if opt not in source_jdbc_opts]
            if missing:
                raise ValueError(f"[Source-to-Asset] Missing required JDBC options for source: {', '.join(missing)}")
            
            # Validate field mapping
            field_mapping = self.job_config.get('field_mapping', {})
            if not field_mapping:
                raise ValueError("[Source-to-Asset] field_mapping is required for source-to-asset mode")
            
            # Validate source fields
            source_fields = self.job_config.get('source_fields', [])
            if not source_fields:
                raise ValueError("[Source-to-Asset] source_fields are required for source-to-asset mode")
            
            # Validate fields have both source and target attributes
            if not self.job_config.get('fields'):
                raise ValueError("[Source-to-Asset] No fields defined in job_config")
            
            logger.info("[Source-to-Asset] Configuration validated successfully")
        else:
            # Single asset mode validation (existing logic)
            required_jdbc = ['url', 'user', 'password', 'driver']
            jdbc_opts = self.job_config.get('jdbc_options', {})
            if not jdbc_opts:
                raise ValueError("Missing required JDBC options in job_config")
            missing = [opt for opt in required_jdbc if opt not in jdbc_opts]
            if missing:
                raise ValueError(f"Missing required JDBC options: {', '.join(missing)}")
            if not self.job_config.get('fields'):
                raise ValueError("No fields defined in job_config")
        for field in self.job_config['fields']:
            if 'field' not in field or 'type' not in field:
                raise ValueError("Field definition requires 'field' and 'type' keys")

    def _init_spark(self):
        logger.info("Initializing Spark session...")
        try:
            spark_builder = SparkSession.builder.appName("DedupeProcessor")
            for k, v in self.spark_conf.items():
                spark_builder = spark_builder.config(k, v)
            if "spark.local.dir" not in self.spark_conf:
                spark_builder = spark_builder.config("spark.local.dir", "/tmp")
            spark = spark_builder.getOrCreate()
            logger.info("Spark session created successfully")
            return spark
        except Exception as e:
            logger.error("Spark initialization failed: %s", str(e))
            raise

    def _optimized_read(self):
        """
        Load data using an optimized JDBC strategy, adjusting partitions if needed.
        
        Returns:
            - Single asset mode: DataFrame
            - Source-to-asset mode: tuple (target_df, source_df)
        """
        try:
            if self.is_source_asset:
                # Source-to-asset mode: Read 2 DataFrames
                logger.info("[Source-to-Asset] Reading target DataFrame...")
                target_jdbc_options = self.job_config.get("jdbc_options", {})
                if not target_jdbc_options:
                    raise ValueError("[Source-to-Asset] Missing jdbc_options for target")
                target_df = self._read_dataframe(target_jdbc_options)
                
                logger.info("[Source-to-Asset] Reading source DataFrame...")
                source_jdbc_options = self.job_config.get("source_jdbc_options", {})
                if not source_jdbc_options:
                    raise ValueError("[Source-to-Asset] Missing source_jdbc_options")
                source_df = self._read_dataframe(source_jdbc_options)
                
                target_count = target_df.count()
                source_count = source_df.count()
                logger.info("[Source-to-Asset] Target DataFrame: %d rows, Source DataFrame: %d rows",
                           target_count, source_count)
                
                return target_df, source_df
            else:
                # Single asset mode: Read 1 DataFrame (matches backup exactly)
                jdbc_options = self.job_config.get("jdbc_options", {})
                if not jdbc_options:
                    raise ValueError("Missing required JDBC options in job_config")
                df = self._read_dataframe(jdbc_options)
                return df
        except Exception as e:
            logger.error("Data loading failed: %s", str(e))
            raise

    def _read_dataframe(self, jdbc_options):
        """
        Read DataFrame from JDBC and optimize partitions (matches backup logic exactly).
        No retry logic, no SSL error handling - simple and proven approach.
        """
        df = self.spark.read.format("jdbc").options(**jdbc_options).load()
        num_partitions = df.rdd.getNumPartitions()
        logger.info("DataFrame has %d partitions after load.", num_partitions)

        sc = self.spark.sparkContext
        default_parallelism = sc.defaultParallelism
        executor_memory = sc._conf.get("spark.executor.memory")
        executor_cores = sc._conf.get("spark.executor.cores")
        logger.info("Spark defaultParallelism: %d, executor_memory: %s, executor_cores: %s",
                    default_parallelism, executor_memory, executor_cores)

        min_partitions = max(10, default_parallelism * 2)
        max_partitions = 50
        if num_partitions < min_partitions:
            target_partitions = min(max(min_partitions * 2, num_partitions), max_partitions)
            df = df.coalesce(target_partitions)
            logger.info("Data repartitioned to %d partitions for improved parallelism.", target_partitions)
        else:
            logger.info("Partition count is sufficient; no repartitioning needed.")

        try:
            sample_row = df.limit(1).collect()
            logger.info("Sample row: %s", sample_row)
        except Exception:
            logger.warning("Could not fetch a sample row.")

        return df

    def apply_field_mapping(self, source_record, field_mapping):
        """
        Map source field names to target field names for comparison.
        Ensures all target fields are present (RecordLink requirement).
        
        Args:
            source_record: Dict with source field names
            field_mapping: Dict mapping source→target field names
        
        Returns:
            dict: Record with target field names, all target fields present
        """
        mapped_record = {
            target_field: source_record.get(source_field, "")
            for source_field, target_field in field_mapping.items()
        }
        return mapped_record

    def _check_file_exists(self, file_path):
        """
        Check if file exists in storage (S3/Azure/GCP)
        
        Args:
            file_path: Path to file in storage
        
        Returns:
            bool: True if file exists, False otherwise
        """
        if not file_path:
            return False
        try:
            # Use FileStorage to check file existence
            file_storage = FileStorage(self.external_credentials, self.spark)
            # Try to read the file - if it exists, this will succeed
            # If file doesn't exist, FileStorage.read() will raise FileNotFoundError
            file_storage.read(file_path)
            return True
        except (FileNotFoundError, RuntimeError):
            return False
        except Exception as e:
            logger.warning("Error checking file existence for %s: %s", file_path, str(e))
            return False

    def _load_training_model(self, training_model_path):
        """
        Load training model from storage
        
        Args:
            training_model_path: Path to training model file
        
        Returns:
            dict: Training model data
        """
        file_storage = FileStorage(self.external_credentials, self.spark)
        training_model = file_storage.read(training_model_path)
        # Convert to format expected by dedupe
        training_model = {
            key: [item["pair"] for item in training_model.get(key, [])]
            for key in ["match", "distinct", "unsure"]
        }
        return training_model

    def _load_clusters(self, clusters_path):
        """
        Load clusters from storage
        
        Args:
            clusters_path: Path to clusters file
        
        Returns:
            list: Clusters data
        """
        file_storage = FileStorage(self.external_credentials, self.spark)
        clusters = file_storage.read(clusters_path)
        return clusters

    def _is_valid_email(self, email):
        return bool(re.match(r"^[^@]+@[^@]+\.[^@]+$", str(email).strip()))

    def _is_valid_phone(self, phone):
        return bool(re.match(r"^\+?[\d\s\-]{10,15}$", str(phone).strip()))

    def _merge_cluster_records(self, cluster_records):
        """
        Merges a list of cluster records into a single representative record.
        Uses robust methods for merging each field.
        """
        if not cluster_records:
            raise ValueError("No records provided for merging.")
        merged_record = {}
        field_values = {}
        try:
            # Gather non-empty values per field
            for record in cluster_records:
                for key, value in record.items():
                    if pd.notna(value) and value != "":
                        field_values.setdefault(key, []).append(value)
            for key, values in field_values.items():
                try:
                    if "email" in key.lower():
                        valid_emails = [v for v in values if self._is_valid_email(v)]
                        if valid_emails:
                            counts = pd.Series(valid_emails).value_counts()
                            merged_record[key] = counts.idxmax()
                            continue
                    if any(x in key.lower() for x in ["phone", "mobile"]):
                        valid_phones = [v for v in values if self._is_valid_phone(v)]
                        if valid_phones:
                            counts = pd.Series(valid_phones).value_counts()
                            merged_record[key] = counts.idxmax()
                            continue
                    if "date" in key.lower():
                        try:
                            dates = pd.to_datetime(values, errors='coerce').dropna()
                            if not dates.empty:
                                merged_record[key] = str(dates.max())
                                continue
                        except Exception:
                            pass
                    counts = pd.Series(values).value_counts()
                    merged_record[key] = sorted(counts.index, key=lambda x: (-counts[x], -len(str(x))))[0]
                except Exception as e:
                    logger.warning("Error processing field '%s': %s", key, str(e))
                    merged_record[key] = None
            # Ensure all keys are present
            all_keys = {key for record in cluster_records for key in record.keys()}
            for key in all_keys:
                if key not in merged_record:
                    merged_record[key] = None
            return merged_record
        except Exception as e:
            logger.error("Failed to merge cluster records: %s", str(e))
            raise

    def _concat_fields(self, df_spark):
        """
        Concatenates specified fields based on configuration.
        Drops original fields after concatenation.
        """
        logger.info("Concatenating fields based on configuration...")
        try:
            concat_fields = self.settings.get("concat_fields", [])
            if not concat_fields:
                logger.info("No fields specified for concatenation. Skipping...")
                return df_spark
            for concat_config in concat_fields:
                output_field = concat_config.get("output_field")
                input_fields = concat_config.get("input_fields", [])
                separator = concat_config.get("separator", " ")
                if not output_field or not input_fields:
                    raise ValueError(f"Invalid concat configuration: {concat_config}")
                concat_expr = F.concat_ws(separator, *[F.col(field) for field in input_fields])
                df_spark = df_spark.withColumn(output_field, concat_expr)
                df_spark = df_spark.drop(*input_fields)
                logger.info("Concatenated fields %s into %s", input_fields, output_field)
            return df_spark
        except Exception as e:
            logger.error("Field concatenation failed: %s", str(e))
            raise

    def _prepare_dedupe_id(self, df_spark):
        # Prepare a unique dedupe_id by hashing configured fields.
        try:
            selfFields = [field['field'] for field in self.fields]
            hash_expr = F.sha2(F.concat_ws("||", *[F.col(field) for field in selfFields]), 256)
            primary_key = self.settings.get("primary_key")
            if primary_key and primary_key in df_spark.columns:
                return df_spark.withColumn("dedupe_id", F.col(primary_key).cast("string"))
            else:
                return df_spark.withColumn("dedupe_id", hash_expr.cast("string"))
        except Exception as e:
            logger.error("Failed to prepare dedupe_id: %s", str(e))
            raise

    def _build_dedupe_fields(self):
        # Build field objects for dedupe based on job configuration.
        field_objects = []
        field_type_mapping = {
            "String": variables.String,
            "ShortString": variables.ShortString,
            "Exact": variables.Exact,
            "Set": variables.Set,
            "Text": variables.Text,
            "Price": variables.Price,
            "LatLong": variables.LatLong,
            "Categorical": variables.Categorical
        }
        try:
            for field in self.fields:
                field_name = field.get('field')
                field_types = field.get('type', [])
                if not field_name or not field_types:
                    logger.warning("Skipping invalid field configuration: %s", field)
                    continue
                if isinstance(field_types, str):
                    field_types = [field_types]
                for field_type in field_types:
                    field_class = field_type_mapping.get(field_type)
                    if not field_class:
                        logger.warning("Unsupported field type '%s' for field '%s'. Skipping...", field_type, field_name)
                        continue
                    has_missing = bool(field.get('has_missing', False))
                    if field_type in ["Text", "String", "ShortString", "LatLong", "Categorical"]:
                        field_obj = field_class(field_name, has_missing=has_missing)
                    elif field_type in ["Exact", "Set", "Price"]:
                        field_obj = field_class(field_name)
                    else:
                        raise ValueError(f"Unsupported field type '{field_type}' for field '{field_name}'")
                    field_objects.append(field_obj)
            logger.info("Successfully created %d field objects.", len(field_objects))
            return field_objects
        except Exception as e:
            logger.error("Field creation failed: %s", str(e))
            raise

    def _build_recordlink_fields(self):
        """
        Build RecordLink field objects using TARGET field names (after field mapping)
        RecordLink requires both datasets to have the SAME field names
        
        Returns:
            list: RecordLink field objects with target field names
        """
        source_fields = self.job_config.get("source_fields", [])
        field_mapping = self.job_config.get("field_mapping", {})
        
        if not source_fields:
            raise ValueError("[Source-to-Asset] source_fields are required")
        if not field_mapping:
            raise ValueError("[Source-to-Asset] field_mapping is required")
        
        field_objects = []
        field_type_mapping = {
            "String": variables.String,
            "ShortString": variables.ShortString,
            "Exact": variables.Exact,
            "Set": variables.Set,
            "Text": variables.Text,
            "Price": variables.Price,
            "LatLong": variables.LatLong,
            "Categorical": variables.Categorical
        }
        
        try:
            for source_field_config in source_fields:
                source_field_name = source_field_config.get("field")
                field_types = source_field_config.get("type", [])
                
                if not source_field_name or not field_types:
                    logger.warning("[Source-to-Asset] Skipping invalid source field configuration: %s", source_field_config)
                    continue
                
                # Verify field is in mapping
                if source_field_name not in field_mapping:
                    logger.warning("[Source-to-Asset] Source field '%s' not in field_mapping. Skipping...", source_field_name)
                    continue
                
                target_field_name = field_mapping[source_field_name]
                
                if isinstance(field_types, str):
                    field_types = [field_types]
                
                field_type = field_types[0]  # Use first type
                field_class = field_type_mapping.get(field_type)
                if not field_class:
                    logger.warning("[Source-to-Asset] Unsupported field type '%s' for source field '%s'. Skipping...",
                                 field_type, source_field_name)
                    continue
                
                has_missing = source_field_config.get("has_missing", False)
                
                if field_type in ["Text", "String", "ShortString", "LatLong", "Categorical"]:
                    field_obj = field_class(target_field_name, has_missing=has_missing)
                elif field_type in ["Exact", "Set", "Price"]:
                    field_obj = field_class(target_field_name)
                else:
                    raise ValueError(f"[Source-to-Asset] Unsupported field type '{field_type}' for source field '{source_field_name}'")
                
                field_objects.append(field_obj)
            
            logger.info("[Source-to-Asset] Created %d RecordLink field objects with target field names.", len(field_objects))
            return field_objects
        except Exception as e:
            logger.error("[Source-to-Asset] RecordLink field creation failed: %s", str(e))
            raise

    def _merge_similar_clusters(self, clusters: list):
        logger.info("Merging similar clusters...")
        file_storage_service = FileStorage(self.external_credentials, self.spark)
        similar_clusters_path = self.external_credentials.get("similar_clusters_path")
        similar_clusters = file_storage_service.read(similar_clusters_path)
        cluster_map = {c["cluster_id"]: c for c in clusters}
        merged_clusters = []
        merged_clusters_ids = []
        for group in similar_clusters:
            merged_records = []
            merged_cluster_ids = []
            for record in group:
                cid = record.get("cluster_id")
                cluster = cluster_map.get(cid)
                if cluster:
                    merged_records.extend(cluster["records"])
                    merged_cluster_ids.extend(cluster.get("cluster_ids", []))
                else:
                    logger.warning("Cluster ID %s not found in cluster map. Skipping...", cid)
                merged_clusters_ids.append(cid)
            new_cluster_id = str(uuid4())
            merged_clusters.append({
                "cluster_id": new_cluster_id,
                "records": merged_records,
                "cluster_ids": merged_cluster_ids
            })
            logger.info("Created new merged cluster with ID %s containing %d records.",
                        new_cluster_id, len(merged_records))
        remaining_clusters = [cluster for cluster in clusters if cluster.get("cluster_id") not in merged_clusters_ids]
        logger.info("Identified %d remaining clusters not part of any similar group.", len(remaining_clusters))
        final_clusters = merged_clusters + remaining_clusters
        logger.info("Final clusters count after merging: %d", len(final_clusters))
        serialized_clusters = [self._serialize_data(cluster) for cluster in final_clusters]
        file_storage_service.save(serialized_clusters, self.external_credentials.get("clusters_path"))
        return final_clusters

    @staticmethod
    def _field_similarity(field, value1, value2):
        """Compute similarity for a field between two values."""
        weight = field.get('weight', 1)
        value1 = str(value1).strip() if value1 is not None else ""
        value2 = str(value2).strip() if value2 is not None else ""
        if not value1 and not value2:
            return 100 * weight
        field_types = field.get('type', [])
        if not isinstance(field_types, list):
            field_types = [field_types]
        field_similarity = 0
        for field_type in field_types:
            if field_type == "ShortString":
                sim = fuzz.ratio(value1, value2) * weight
            elif field_type in ["Text", "String"]:
                sim = fuzz.partial_ratio(value1, value2) * weight
            elif field_type == "Exact":
                sim = 100 * weight if value1 == value2 else 0
            elif field_type == "Set":
                set1 = set(value1.split())
                set2 = set(value2.split())
                sim = fuzz.token_set_ratio(set1, set2) * weight
            elif field_type == "Price":
                try:
                    price1 = float(value1)
                    price2 = float(value2)
                    sim = 100 * weight if abs(price1 - price2) < 0.01 else 0
                except ValueError:
                    sim = 0
            elif field_type == "LatLong":
                try:
                    lat1, lon1 = map(float, value1.split(","))
                    lat2, lon2 = map(float, value2.split(","))
                    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
                    dlat = lat2 - lat1
                    dlon = lon2 - lon1
                    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2)*np.sin(dlon/2)**2
                    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
                    R = 6371.0
                    sim = 100 * weight if (R * c) <= 1 else 0
                except Exception:
                    sim = 0
            elif field_type == "Categorical":
                sim = 100 * weight if value1 == value2 else 0
            else:
                sim = 0
            field_similarity = max(field_similarity, sim)
        return field_similarity

    @staticmethod
    def compute_similarity_static(record1, record2, fields):
        similarities = []
        total_weight = 0
        for field in fields:
            weight = field.get('weight', 1)
            total_weight += weight
            value1 = record1.get(field['field'], "")
            value2 = record2.get(field['field'], "")
            sim = DedupeOptimizer._field_similarity(field, value1, value2)
            similarities.append(sim)
        return sum(similarities) / total_weight if total_weight > 0 else 0

    def _compute_similarity(self, record1, record2, fields):
        similarities = []
        total_weight = 0
        for field in fields:
            weight = field.get('weight', 1)
            total_weight += weight
            value1 = record1.get(field['field'], "")
            value2 = record2.get(field['field'], "")
            sim = DedupeOptimizer._field_similarity(field, value1, value2)
            similarities.append(sim)
        return sum(similarities) / total_weight if total_weight > 0 else 0

    def _calculate_similarity(self, record1, record2):
        try:
            return self._compute_similarity(record1, record2, self.fields)
        except Exception as e:
            logger.error("Error calculating similarity: %s", str(e))
            return 0

    def _serialize_data(self, data):
        """
        Converts various data types to serializable format.
        """
        if isinstance(data, (float, int, str, bool, type(None))):
            return data
        elif isinstance(data, (pd.Timestamp, np.datetime64, datetime.date, datetime.datetime)):
            return str(data)
        elif isinstance(data, (np.float32, np.float64, np.int32, np.int64)):
            return data.item()
        elif hasattr(data, '__class__') and data.__class__.__name__ == 'Decimal':
            # Handle Decimal type (from Snowflake/PostgreSQL)
            return float(data)
        elif isinstance(data, dict):
            return {key: self._serialize_data(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self._serialize_data(item) for item in data]
        elif isinstance(data, tuple):
            return tuple(self._serialize_data(item) for item in data)
        elif isinstance(data, pd.DataFrame):
            return data.to_dict(orient="records")
        elif isinstance(data, pd.Series):
            return data.to_list()
        elif isinstance(data, np.ndarray):
            return data.tolist()
        elif isinstance(data, set):
            return list(data)
        elif isinstance(data, bytes):
            return data.decode("utf-8", errors="replace")
        elif hasattr(data, "__dict__"):
            return {key: self._serialize_data(value) for key, value in vars(data).items()}
        else:
            return str(data)

    def _to_dedupe_dict(self, df):
        return {
            str(row["dedupe_id"]): {
                key: str(value) if value is not None else ""
                for key, value in row.asDict().items()
                if key != "dedupe_id"
            }
            for row in df.collect()
        }

    def _sample_dataframe_spark(self) -> dict:
        # Sample the Spark DataFrame for generating training/validation sets.
        logger.info("Sampling dataframe for training/validation...")
        if self.df_spark is None or self.df_spark.rdd.isEmpty():
            logger.error("No data available to sample from.")
            raise ValueError("No data available to sample from.")
        sample_size = self.settings.get("sample_size", 1000)
        conditions = [(F.col(field).isNotNull()) & (F.length(F.trim(F.col(field))) > 0)
                      for field in self.df_spark.columns]
        combined_condition = reduce(lambda a, b: a & b, conditions)
        selected_columns = [F.col(field['field']).cast("string").alias(field['field']) for field in self.fields] + [F.col("dedupe_id")]
        df_complete = self.df_spark.select(*selected_columns).filter(combined_condition)\
            .persist(StorageLevel.DISK_ONLY)  # fixed: use valid StorageLevel constant
        total = df_complete.count()  # materialize cache
        logger.info("Found %d records with all non-empty fields.", total)
        
        if total > sample_size:
            # Order randomly and limit to sample_size for deterministic sampling.
            sample_df = df_complete.orderBy(F.rand(seed=42)).limit(sample_size)\
                .persist(StorageLevel.DISK_ONLY)  # fixed: use valid StorageLevel constant
        else:
            sample_df = df_complete.persist(StorageLevel.DISK_ONLY)  # fixed: use valid StorageLevel constant
        
        final_count = sample_df.count()  # materialize sample cache
        logger.info("Sampled %d records for training/validation.", final_count)
 
        sample_dict = self._to_dedupe_dict(sample_df)
        file_storage_service = FileStorage(self.external_credentials, self.spark)
        sample_data_path = self.external_credentials.get("sample_data_path")
        file_storage_service.save(sample_dict, sample_data_path)
        logger.info("Sample data saved to %s", sample_data_path)
        
        df_complete.unpersist()  # release cache
        sample_df.unpersist()
        return sample_dict

    def _calculate_num_partitions(self):
        # Ensure df_spark is persisted if not already cached
        if not self.df_spark.rdd.getStorageLevel().useMemory:
            self.df_spark.persist(StorageLevel.MEMORY_AND_DISK)
        user_defined_partitions = self.settings.get("desired_num_partitions")
        if user_defined_partitions is not None:
            num_partitions = int(user_defined_partitions)
        else:
            total_rows = self.df_spark.count()
            rows_per_partition = self.settings.get("rows_per_partition", 5000)
            estimated_partitions = max(int(total_rows / rows_per_partition), 1)
            default_parallelism = self.spark.sparkContext.defaultParallelism
            # Increase partitions to reduce load per partition
            num_partitions = max(estimated_partitions, default_parallelism * 2)
            logger.info("Calculated %d partitions based on %d rows and default parallelism %d.",
                        num_partitions, total_rows, default_parallelism)
        return num_partitions

    def run_training_data(self):
        # Generate training data for deduplication.
        try:
            logger.info("Generating training data...")
            match_threshold = self.settings.get("match_threshold", 90)
            distinct_threshold = self.settings.get("distinct_threshold", 30)
            max_pairs = self.settings.get("max_pairs", 10)
            min_pairs = self.settings.get("min_pairs", 5)
            match_dict = {}
            distinct_dict = {}
            file_storage_service = FileStorage(self.external_credentials, self.spark)
            sample_data_path = self.external_credentials.get("sample_data_path")
            
            try:
                sample = file_storage_service.read(sample_data_path)
            except (FileNotFoundError, Exception) as e:
                sample = self._sample_dataframe_spark()
            
            sample_items = list(sample.items())
            total_records = len(sample_items)
            if total_records == 0:
                raise ValueError("No records available for training data generation")
            used_match_ids = set()
            used_distinct_ids = set()
            for i in range(total_records):
                id1, record1 = sample_items[i]
                for j in range(i + 1, total_records):
                    id2, record2 = sample_items[j]
                    avg_similarity = self._calculate_similarity(record1, record2)
                    pair = (id1, id2)
                    if avg_similarity >= match_threshold:
                        if id1 not in used_match_ids and id2 not in used_match_ids:
                            match_dict[pair] = avg_similarity
                            used_match_ids.update([id1, id2])
                    elif avg_similarity < distinct_threshold:
                        if id1 not in used_distinct_ids and id2 not in used_distinct_ids:
                            distinct_dict[pair] = avg_similarity
                            used_distinct_ids.update([id1, id2])
            sorted_match = sorted(list(match_dict.items()), key=lambda x: x[1], reverse=True)[:max_pairs]
            sorted_distinct = sorted(list(distinct_dict.items()), key=lambda x: x[1])[:max_pairs]
            if len(sorted_match) < min_pairs or len(sorted_distinct) < min_pairs:
                logger.warning("Insufficient candidate pairs to guarantee the minimum pairs requirement.")
            match_pairs = [{"pair": (sample[id1], sample[id2]), "similarity": sim} for (id1, id2), sim in sorted_match]
            distinct_pairs = [{"pair": (sample[id1], sample[id2]), "similarity": sim} for (id1, id2), sim in sorted_distinct]
            logger.info("Generated %d match pairs, %d distinct pairs", len(match_pairs), len(distinct_pairs))
            result = {"match": match_pairs, "distinct": distinct_pairs, "unsure": []}
            file_storage_service = FileStorage(self.external_credentials, self.spark)
            destination_path = self.external_credentials.get("training_model_path")
            file_storage_service.save(result, destination_path)
            file_storage_service.delete(self.external_credentials.get("clusters_path"))
            file_storage_service.delete(self.external_credentials.get("extend_clusters_path"))
            file_storage_service.delete(self.external_credentials.get("similar_clusters_path"))
            logger.info("Training model saved successfully at %s", destination_path)
            return result
        except Exception as e:
            logger.error("Training data generation failed: %s", str(e))
            raise

    def run_clusters(self):
        # Process clustering using dedupe training results.
        logger.info("Starting distributed clusters process...")
        try:
            file_storage_service = FileStorage(self.external_credentials, self.spark)
            training_model_path = self.external_credentials.get("training_model_path")
            training_model = file_storage_service.read(training_model_path)
            training_model = {
                key: [item["pair"] for item in training_model.get(key, [])]
                for key in ["match", "distinct", "unsure"]
            }
            file_storage_service = FileStorage(self.external_credentials, self.spark)
            sample_data_path = self.external_credentials.get("sample_data_path")
            
            try:
                sample = file_storage_service.read(sample_data_path)
            except (FileNotFoundError, Exception) as e:
                sample = self._sample_dataframe_spark()
            
            num_cores = self.job_config.get("num_cores") or self.spark.sparkContext.defaultParallelism
            deduper = dedupe.Dedupe(self._build_dedupe_fields(), num_cores=num_cores)
            logger.info("Preparing deduper with training data...")
            deduper.prepare_training(sample)
            deduper.mark_pairs(training_model)
            deduper.train()
            deduper.cleanup_training()
            logger.info("Dedupe training completed successfully.")

            # Cast all selected columns to string to avoid type errors in fuzz functions
            selected_cols = [F.col(field['field']).cast("string").alias(field['field']) for field in self.fields] + [F.col("dedupe_id").cast("string").alias("dedupe_id")]
            data_rdd = self.df_spark.select(*selected_cols)\
                       .rdd.map(lambda row: (row["dedupe_id"], row.asDict()))
            num_partitions = self._calculate_num_partitions()
            data_rdd = data_rdd.repartition(num_partitions)\
                       .persist(StorageLevel.MEMORY_AND_DISK)
            logger.info("Data RDD repartitioned to %d partitions.", num_partitions)
            deduper_broadcast = self.spark.sparkContext.broadcast(deduper)
            threshold = self.settings.get("threshold", 0.5)

            def cluster_partition(partition):
                local_deduper = deduper_broadcast.value
                try:
                    partition_list = list(partition)
                    logger.info(f"Processing partition with {len(partition_list)} records. Keys: {[k for k, _ in partition_list[:5]]} ...")
                    partition_data = dict(partition_list)
                    if not partition_data:
                        logger.info("Empty partition encountered.")
                        return iter([])
                    # Optionally, limit records per partition for debugging
                    # partition_data = dict(list(partition_data.items())[:1000])
                    clusters_result = local_deduper.partition(partition_data, threshold=threshold)
                    if clusters_result is None:
                        logger.info("deduper.partition returned None, skipping partition.")
                        return iter([])
                    clusters = list(clusters_result)
                    logger.info(f"Partitioned into {len(clusters)} clusters.")
                    return (
                        {
                            "cluster_id": str(uuid4()),
                            "cluster_ids": cluster_ids,
                            "records": [partition_data[rid] for rid in cluster_ids],
                            "confidence": list(confidences)
                        }
                        for cluster_ids, confidences in clusters if len(cluster_ids) > 1
                    )
                except Exception as e:
                    logger.error(f"Error in cluster_partition: {type(e).__name__}: {e}\n{traceback.format_exc()}")
                    return iter([])

            start_time = datetime.datetime.now()
            # Cache the resulting RDD to avoid re-computation during collection
            clustered_dupes_with_records = data_rdd.mapPartitions(cluster_partition).cache().collect()
            duration = (datetime.datetime.now() - start_time).total_seconds()
            logger.info("Found %d clusters in %.2f seconds", len(clustered_dupes_with_records), duration)
            data_rdd.unpersist()  # release the cached RDD
            deduper_broadcast.destroy()  # destroy broadcast variable
            destination_path = self.external_credentials.get("clusters_path")
            serialized_clusters = [self._serialize_data(cluster) for cluster in clustered_dupes_with_records]
            file_storage_service.save(serialized_clusters, destination_path)
            file_storage_service.delete(self.external_credentials.get("extend_clusters_path"))
            file_storage_service.delete(self.external_credentials.get("similar_clusters_path"))

            iceberg_table_service = IcebergTableWriter(self.external_credentials, self.spark)
            iceberg_clusters_table_identifier = self.external_credentials.get("iceberg_clusters_table_identifier")
            iceberg_table_service.write(clustered_dupes_with_records, iceberg_clusters_table_identifier)
            return serialized_clusters

        except Exception as e:
            logger.error("Distributed clusters failed: %s", str(e))
            raise

    def run_extend_clusters(self, mode=None):
        # Execute hierarchical, incremental cluster extensions.
        logger.info("Starting hierarchical, incremental clusters (extend clusters)...")
        file_storage_service = FileStorage(self.external_credentials, self.spark)

        # Read existing clusters from storage.
        clusters = file_storage_service.read(self.external_credentials.get("clusters_path"))
        logger.info("Downloaded %d clusters from storage.", len(clusters))

        # Build a dictionary of dedupe_id -> record from Spark DataFrame.
        selected_columns = [field["field"] for field in self.fields] + ["dedupe_id"]
        data_d = {str(row["dedupe_id"]): row.asDict()
                  for row in self.df_spark.select(*selected_columns).collect()}

        # Build extended clusters with representative records.
        cluster_reps = {}
        extended_clusters = []
        clustered_ids = set()
        for cluster in clusters:
            cluster_ids = cluster.get("cluster_ids") or []
            if not cluster_ids:
                continue
            valid_records = [data_d.get(rec_id) for rec_id in cluster_ids if rec_id in data_d]
            if not valid_records:
                continue
            rep_record = self._merge_cluster_records(valid_records)
            cluster_id = cluster.get("cluster_id")
            rep_record["cluster_id"] = cluster_id
            extended_cluster = {
                "cluster_id": cluster_id,
                "cluster_ids": list(cluster_ids),
                "merged_record": rep_record,
                "records": valid_records,
                "confidence": cluster.get("confidence", []),
            }
            extended_clusters.append(extended_cluster)
            clustered_ids.update(cluster_ids)
            cluster_reps[cluster_id] = rep_record

        logger.info("Generated %d initial clusters with representatives.", len(extended_clusters))

        # Identify new records not present in any existing cluster.
        new_records = {rec_id: rec for rec_id, rec in data_d.items() if rec_id not in clustered_ids}
        logger.info("Identified %d new records not in any cluster.", len(new_records))

        local_fields = self.fields

        def local_calculate_similarity(record1, record2):
            try:
                return DedupeOptimizer.compute_similarity_static(record1, record2, local_fields)
            except Exception as e:
                logger.warning("Local similarity calculation error: %s", str(e))
                return 0

        extension_threshold = self.settings.get("extension_threshold", 90)

        def process_new_records_partition(partition):
            local_cluster_reps = cluster_reps_broadcast.value
            extended = []
            for new_id, new_record in partition:
                best_similarity = 0
                best_cluster_id = None
                for cid, rep in local_cluster_reps.items():
                    sim = local_calculate_similarity(new_record, rep)
                    if sim > best_similarity:
                        best_similarity = sim
                        best_cluster_id = cid
                if best_similarity >= extension_threshold and best_cluster_id:
                    extended.append((best_cluster_id, new_id, new_record, best_similarity))
            return iter(extended)

        cluster_reps_broadcast = self.spark.sparkContext.broadcast(cluster_reps)
        num_partitions = self._calculate_num_partitions()
        new_records_rdd = self.spark.sparkContext.parallelize(list(new_records.items()))\
                             .repartition(num_partitions)\
                             .persist(StorageLevel.MEMORY_AND_DISK)
        extended_matches = new_records_rdd.mapPartitions(process_new_records_partition).collect()
        new_records_rdd.unpersist()  # release after use

        # Extend clusters with new records if similarity criteria is met.
        for best_cluster_id, new_id, new_record, sim in extended_matches:
            for cluster in extended_clusters:
                if cluster.get("cluster_id") == best_cluster_id:
                    if new_id not in cluster.get("cluster_ids", []):
                        cluster["cluster_ids"].append(new_id)
                        cluster.setdefault("records", []).append({**new_record, "extended": True})
                        cluster.setdefault("confidence", []).append(sim)
                        cluster["extended"] = True
                    break

        logger.info("Total clusters after extension: %d", len(extended_clusters))
        dest_path = self.external_credentials.get("extend_clusters_path")
        if mode == "dedupe":
            dest_path = self.external_credentials.get("clusters_path")
        serialized_clusters = [self._serialize_data(cluster) for cluster in extended_clusters]
        file_storage_service.save(serialized_clusters, dest_path)
        file_storage_service.delete(self.external_credentials.get("similar_clusters_path"))

    def run_similar_clusters(self):
        # Identify groups of similar clusters based on representative record similarity
        logger.info("Starting similar clusters identification process with Spark parallelization...")
        file_storage_service = FileStorage(self.external_credentials, self.spark)
        clusters_path = self.external_credentials.get("clusters_path")
        clusters = file_storage_service.read(clusters_path)
        logger.info("Loaded %d clusters for processing.", len(clusters))
        similarity_threshold = self.settings.get("similarity_threshold", 90)

        # Precompute representative records
        cluster_reps = [
            (
                cluster.get("cluster_id"),
                {**self._merge_cluster_records(cluster.get("records", [])), "cluster_id": cluster.get("cluster_id")}
            )
            for cluster in clusters
        ]
        logger.info("Generated representative records for clusters.")

        # Create an RDD of the cluster representatives and perform a cartesian self-join.
        reps_rdd = self.spark.sparkContext.parallelize(cluster_reps)
        # Only compare pairs once (keep only pairs where cid1 < cid2)
        candidate_pairs = reps_rdd.cartesian(reps_rdd).filter(lambda pair: pair[0][0] < pair[1][0])
        fields_broadcast = self.spark.sparkContext.broadcast(self.fields)
        similar_pairs = candidate_pairs.filter(
            lambda pair: DedupeOptimizer.compute_similarity_static(pair[0][1], pair[1][1], fields_broadcast.value) >= similarity_threshold
        ).map(lambda pair: (pair[0][0], pair[1][0])).collect()
        logger.info("Found %d similar cluster pairs.", len(similar_pairs))
        
        # Use union-find (disjoint set) to group similar clusters
        parent = {}
        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x
        def union(x, y):
            rootx = find(x)
            rooty = find(y)
            if rootx != rooty:
                parent[rooty] = rootx

        all_ids = [cid for cid, _ in cluster_reps]
        for cid in all_ids:
            parent[cid] = cid
        for cid1, cid2 in similar_pairs:
            union(cid1, cid2)
        groups_dict = {}
        for cid in all_ids:
            root = find(cid)
            groups_dict.setdefault(root, []).append(cid)

        # Construct groups using the representative records
        rep_dict = dict(cluster_reps)
        groups = [ [rep_dict[cid] for cid in group] for group in groups_dict.values() if len(group) > 1 ]
        logger.info("Grouped similar clusters into %d groups.", len(groups))

        serialized_groups = [self._serialize_data(group) for group in groups]
        similar_clusters_path = self.external_credentials.get("similar_clusters_path")
        file_storage_service.save(serialized_groups, similar_clusters_path)

    def run_merge_clusters(self):
        # Execute the final merging and assignment of clusters.
        try:
            logger.info("Starting assign and merge process with %d records...", self.df_spark.count())
            file_storage_service = FileStorage(self.external_credentials, self.spark)
            file_storage_service.delete(self.external_credentials.get("extend_clusters_path"))
            file_storage_service.delete(self.external_credentials.get("output_with_cluster_path"))
            file_storage_service.delete(self.external_credentials.get("output_with_merged_path"))
            destination_path = self.external_credentials.get("clusters_path")
            clusters = file_storage_service.read(destination_path)
            clusters = self._merge_similar_clusters(clusters)
            logger.info("Writing clusters to Iceberg table...")
            iceberg_table_service = IcebergTableWriter(self.external_credentials, self.spark)
            iceberg_clusters_table_identifier = self.external_credentials.get("iceberg_clusters_table_identifier")
            iceberg_table_service.write(clusters, iceberg_clusters_table_identifier)
            if not clusters:
                raise ValueError("No clustered duplicates found")
            logger.info("Assigning cluster IDs to records...")
            data_list = self.df_spark.select(*([field['field'] for field in self.fields] + ['dedupe_id'])).toPandas().to_dict(orient="records")
            logger.info("Loaded %d records for assignment.", len(data_list))
            for cluster in clusters:
                cluster_id = cluster.get("cluster_id", "unique")
                cluster_ids_set = set(cluster.get("cluster_ids", []))
                for record in data_list:
                    if record.get("dedupe_id") in cluster_ids_set:
                        record["cluster_id"] = cluster_id
            logger.info("Cluster IDs assigned. Proceeding to merge records...")
            data_d = {row["dedupe_id"]: row.asDict() for row in self.df_spark.collect()}
            logger.info("Loaded %d records for merging.", len(data_d))
            merged_records = []
            duplicate_ids = set()
            for cluster in clusters:
                cluster_ids = cluster.get("cluster_ids", [])
                duplicate_ids.update(cluster_ids)
                valid_cluster_ids = [record_id for record_id in cluster_ids if record_id in data_d]
                if not valid_cluster_ids:
                    logger.warning("Skipping cluster %s due to missing records", cluster.get('cluster_id', 'unknown'))
                    continue
                representative_record = self._merge_cluster_records([data_d[record_id] for record_id in valid_cluster_ids])
                representative_record["cluster_id"] = cluster.get("cluster_id", "unknown")
                merged_records.append(representative_record)
                for record_id in valid_cluster_ids:
                    data_d.pop(record_id, None)
            distinct_records = [record for record_id, record in data_d.items() if record_id not in duplicate_ids]
            final_records = merged_records + distinct_records
            logger.info("Total unique records after deduplication: %d", len(final_records))

            def sort_and_format(records):
                sorted_records = sorted(records, key=lambda x: x.get("cluster_id", ""), reverse=True)
                return [{key: record.get(key, None) for key in (["cluster_id"] + [k for k in record if k != "cluster_id"])}
                        for record in sorted_records]

            data_list_formatted = sort_and_format(data_list)
            final_records_formatted = sort_and_format(final_records)
            logger.info("Saving deduplication records to cloud storage...")
            output_with_cluster_path = self.external_credentials.get("output_with_cluster_path")
            output_with_merged_path = self.external_credentials.get("output_with_merged_path")
            file_storage_service.save([self._serialize_data(cluster) for cluster in data_list_formatted], output_with_cluster_path)
            file_storage_service.save([self._serialize_data(cluster) for cluster in final_records_formatted], output_with_merged_path)
            file_storage_service.delete(self.external_credentials.get("similar_clusters_path"))
            logger.info("Assign and merge process completed successfully.")
            return {
                "status": "success",
                "total_records": len(data_list),
                "total_clusters": len(clusters),
                "total_distinct_records": len(distinct_records),
                "total_final_records": len(final_records),
                "total_duplicate_records": len(data_list) - len(final_records)
            }
        except Exception as e:
            logger.error("Assign and merge process failed: %s", str(e))
            raise

    def _sample_source_target_dataframes(self, source_df, target_df):
        """
        Sample source and target DataFrames (ALIGNED with single asset _sample_dataframe_spark)
        
        Args:
            source_df: Source DataFrame
            target_df: Target DataFrame
        
        Returns:
            tuple: (source_sample_dict, target_sample_dict) - dicts with record_id -> record
        """
        logger.info("[Source-to-Asset] Sampling source and target DataFrames...")
        sample_size = self.settings.get("sample_size", 1000)
        field_mapping = self.job_config.get("field_mapping", {})
        source_fields = self.job_config.get("source_fields", [])
        
        def filter_non_empty(df, columns):
            conditions = [(F.col(col).isNotNull() & (F.length(F.trim(F.col(col))) > 0)) 
                         for col in columns if col in df.columns]
            if not conditions:
                return df
            combined_condition = reduce(lambda a, b: a & b, conditions)
            return df.filter(combined_condition)
        
        # Filter source
        source_columns = [sf.get("field") for sf in source_fields if sf.get("field") in source_df.columns]
        source_filtered = filter_non_empty(source_df, source_columns)
        source_total = source_filtered.count()
        logger.info("[Source-to-Asset] Found %d source records with all non-empty fields", source_total)
        
        # Filter target  
        target_columns = [field['field'] for field in self.fields if field['field'] in target_df.columns]
        target_filtered = filter_non_empty(target_df, target_columns)
        target_total = target_filtered.count()
        logger.info("[Source-to-Asset] Found %d target records with all non-empty fields", target_total)
        
        if source_total > sample_size:
            source_sample_df = source_filtered.orderBy(F.rand(seed=42)).limit(sample_size)
        else:
            source_sample_df = source_filtered
        
        if target_total > sample_size:
            target_sample_df = target_filtered.orderBy(F.rand(seed=43)).limit(sample_size)
        else:
            target_sample_df = target_filtered
        
        source_sample_dict = {}
        for row in source_sample_df.collect():
            record = row.asDict()
            source_id = str(record.get("source_id") or 
                          record.get("dedupe_id") or 
                          record.get("id") or 
                          str(uuid4()))
            source_sample_dict[source_id] = {
                key: str(value) if value is not None else ""
                for key, value in record.items()
                if key in source_columns
            }
        
        target_sample_dict = {}
        for row in target_sample_df.collect():
            record = row.asDict()
            target_id = str(record.get("dedupe_id") or 
                          record.get("id") or 
                          str(uuid4()))
            target_sample_dict[target_id] = {
                key: str(value) if value is not None else ""
                for key, value in record.items()
                if key in target_columns
            }
        
        logger.info("[Source-to-Asset] Sampled %d source records, %d target records", 
                   len(source_sample_dict), len(target_sample_dict))
        
        # Apply field mapping and create final mapped versions (stored as sample)
        field_mapping = self.job_config.get("field_mapping", {})
        mapped_source_sample = {}
        for source_id, source_record in source_sample_dict.items():
            mapped_source_sample[source_id] = self.apply_field_mapping(source_record, field_mapping)
        
        target_field_names = set(field_mapping.values())
        filtered_target_sample = {}
        for target_id, target_record in target_sample_dict.items():
            filtered_record = {
                key: value
                for key, value in target_record.items()
                if key in target_field_names
            }
            for target_field in target_field_names:
                if target_field not in filtered_record:
                    filtered_record[target_field] = ""
            filtered_target_sample[target_id] = filtered_record
        
        # Save mapped versions as the sample (used in all steps)
        sample_data_path = self.external_credentials.get("source_asset_sample_data_path")
        if sample_data_path:
            file_storage_service = FileStorage(self.external_credentials, self.spark)
            sample_data = {
                "source_sample": mapped_source_sample,
                "target_sample": filtered_target_sample
            }
            file_storage_service.save(sample_data, sample_data_path)
            logger.info("[Source-to-Asset] Mapped sample data saved to %s", sample_data_path)
        
        return mapped_source_sample, filtered_target_sample

    def run_source_asset_training(self, source_df, target_df):
        """
        Exactly like single asset run_training_data()
        Generate training data for source-to-target deduplication.
        
        Args:
            source_df: Source DataFrame
            target_df: Target DataFrame
        
        Returns:
            dict: Training data for UI (same format as single asset mode)
        """
        logger.info("[Source-to-Asset] Generating training data...")
        try:
            match_threshold = self.settings.get("match_threshold", 70)
            distinct_threshold = self.settings.get("distinct_threshold", 30)
            max_pairs = self.settings.get("max_pairs", 10)
            min_pairs = self.settings.get("min_pairs", 5)
            field_mapping = self.job_config.get("field_mapping", {})
            
            match_dict = {}
            distinct_dict = {}
            file_storage_service = FileStorage(self.external_credentials, self.spark)
            sample_data_path = self.external_credentials.get("source_asset_sample_data_path")
            
            # Load or generate mapped samples (aligned with single-asset run_training_data)
            try:
                if sample_data_path:
                    sample_data = file_storage_service.read(sample_data_path)
                    mapped_source_sample = sample_data.get("source_sample", {})
                    filtered_target_sample = sample_data.get("target_sample", {})
                    logger.info("[Source-to-Asset] Loaded saved mapped samples: %d source, %d target", 
                               len(mapped_source_sample), len(filtered_target_sample))
                else:
                    raise FileNotFoundError("source_asset_sample_data_path not configured")
            except (FileNotFoundError, Exception) as e:
                logger.info("[Source-to-Asset] Sample data not found or error loading, generating new mapped samples: %s", str(e))
                mapped_source_sample, filtered_target_sample = self._sample_source_target_dataframes(source_df, target_df)
            
            source_items = list(mapped_source_sample.items())
            target_items = list(filtered_target_sample.items())
            source_total = len(source_items)
            target_total = len(target_items)
            
            if source_total == 0 or target_total == 0:
                raise ValueError("No records available for training data generation")
            
            match_dict = {}
            distinct_dict = {}
            
            max_comparisons = 500000
            total_possible = source_total * target_total
            
            if total_possible > max_comparisons:
                import math
                sample_per_side = int(math.sqrt(max_comparisons))
                source_items = source_items[:min(sample_per_side, source_total)]
                target_items = target_items[:min(sample_per_side, target_total)]
                logger.info("[Source-to-Asset] Limited to %d source × %d target = %d pairs", 
                           len(source_items), len(target_items), len(source_items) * len(target_items))
            
            similarity_scores = []
            for source_id, source_record in source_items:
                for target_id, target_record in target_items:
                    avg_similarity = self._calculate_similarity(source_record, target_record)
                    pair = (source_id, target_id)
                    similarity_scores.append(avg_similarity)
                    
                    if avg_similarity >= 99.9:
                        if pair not in match_dict:
                            match_dict[pair] = avg_similarity
                    elif avg_similarity >= distinct_threshold:
                        if pair not in distinct_dict:
                            distinct_dict[pair] = avg_similarity
            
            
            sorted_match = sorted(list(match_dict.items()), key=lambda x: x[1], reverse=True)[:max_pairs]
            sorted_distinct = sorted(list(distinct_dict.items()), key=lambda x: x[1])[:max_pairs]
            
            if len(sorted_match) < min_pairs or len(sorted_distinct) < min_pairs:
                logger.warning("[Source-to-Asset] Insufficient candidate pairs: match=%d (need %d), distinct=%d (need %d)", 
                             len(sorted_match), min_pairs, len(sorted_distinct), min_pairs)
            
            # Store pairs with actual dict objects from mapped samples (like single-asset)
            match_pairs = [
                {
                    "pair": (mapped_source_sample[source_id], filtered_target_sample[target_id]),
                    "similarity": sim,
                    "source_id": source_id,
                    "target_id": target_id
                }
                for (source_id, target_id), sim in sorted_match
            ]
            
            distinct_pairs = [
                {
                    "pair": (mapped_source_sample[source_id], filtered_target_sample[target_id]),
                    "similarity": sim,
                    "source_id": source_id,
                    "target_id": target_id
                }
                for (source_id, target_id), sim in sorted_distinct
            ]
            
            logger.info("[Source-to-Asset] Generated %d match pairs, %d distinct pairs", 
                       len(match_pairs), len(distinct_pairs))
            
            result = {"match": match_pairs, "distinct": distinct_pairs, "unsure": []}
            file_storage_service = FileStorage(self.external_credentials, self.spark)
            destination_path = self.external_credentials.get("source_asset_training_model_path")
            file_storage_service.save(result, destination_path)
            file_storage_service.delete(self.external_credentials.get("source_asset_clusters_path"))
            file_storage_service.delete(self.external_credentials.get("source_asset_extend_clusters_path"))
            file_storage_service.delete(self.external_credentials.get("source_asset_similar_clusters_path"))
            logger.info("[Source-to-Asset] Training model saved successfully at %s", destination_path)
            return result
        except Exception as e:
            logger.error("[Source-to-Asset] Training data generation failed: %s", str(e))
            raise

    def run_source_asset_clusters(self, source_df, target_df):
        """
        Exactly like single asset run_clusters()
        Process clustering using RecordLink training results.
        
        Args:
            source_df: Source DataFrame
            target_df: Target DataFrame
        
        Returns:
            list: Source-centric structure with matched target records
        """
        logger.info("[Source-to-Asset] Starting distributed clusters process (aligned with single asset mode)...")
        try:
            file_storage_service = FileStorage(self.external_credentials, self.spark)
            training_model_path = self.external_credentials.get("source_asset_training_model_path")
            training_model = file_storage_service.read(training_model_path)
            
            sample_data_path = self.external_credentials.get("source_asset_sample_data_path")
            
            if not sample_data_path:
                raise ValueError("[Source-to-Asset] source_asset_sample_data_path not configured. Cannot proceed without saved sample.")
            
            try:
                sample_data = file_storage_service.read(sample_data_path)
                mapped_source_sample = sample_data.get("source_sample", {})
                filtered_target_sample = sample_data.get("target_sample", {})
                if not mapped_source_sample or not filtered_target_sample:
                    raise ValueError("[Source-to-Asset] Loaded sample is empty. Cannot proceed.")
                logger.info("[Source-to-Asset] Loaded saved mapped samples: %d source, %d target", 
                           len(mapped_source_sample), len(filtered_target_sample))
            except (FileNotFoundError, Exception) as e:
                raise ValueError(f"[Source-to-Asset] Cannot load saved sample from {sample_data_path}. "
                              f"Sample is required for clusters step. Error: {str(e)}. "
                              f"Please rerun training step to generate the sample.") from e
            
            # Extract pairs directly from training model
            reconstructed_training_model = {
                key: [item["pair"] for item in training_model.get(key, [])]
                for key in ["match", "distinct", "unsure"]
            }
            
            logger.info("[Source-to-Asset] Extracted %d match, %d distinct, %d unsure pairs from training model",
                       len(reconstructed_training_model["match"]),
                       len(reconstructed_training_model["distinct"]),
                       len(reconstructed_training_model["unsure"]))
            
            # Extract ALL unique records from training pairs and use ONLY those for prepare_training()
            # CRITICAL: Use the EXACT same dict objects from training pairs (not copies, not from sample)
            # dedupe.io requires records in mark_pairs() to be the exact same objects passed to prepare_training()
            source_data_for_training = {}
            target_data_for_training = {}
            seen_source = set()
            seen_target = set()
            
            for key in ["match", "distinct", "unsure"]:
                for src_rec, tgt_rec in reconstructed_training_model[key]:
                    # Use content as key to deduplicate (same content = same record)
                    # But store the EXACT dict object (not a copy)
                    src_content = tuple(sorted(src_rec.items()))
                    tgt_content = tuple(sorted(tgt_rec.items()))
                    
                    # Store the actual dict object with a consistent ID
                    if src_content not in seen_source:
                        src_id = f"src_{abs(hash(src_content))}"
                        source_data_for_training[src_id] = src_rec  # Same object, not a copy
                        seen_source.add(src_content)
                    
                    if tgt_content not in seen_target:
                        tgt_id = f"tgt_{abs(hash(tgt_content))}"
                        target_data_for_training[tgt_id] = tgt_rec  # Same object, not a copy
                        seen_target.add(tgt_content)
            
            logger.info("[Source-to-Asset] Extracted %d unique source records, %d unique target records from training pairs", 
                       len(source_data_for_training), len(target_data_for_training))
            logger.info("[Source-to-Asset] Using training pair records for prepare_training()")
            
            num_cores = self.job_config.get("num_cores") or self.spark.sparkContext.defaultParallelism
            record_linker = dedupe.RecordLink(self._build_recordlink_fields(), num_cores=num_cores)
            logger.info("[Source-to-Asset] Preparing RecordLink with training data...")
            
            record_linker.prepare_training(source_data_for_training, target_data_for_training)
            record_linker.mark_pairs(reconstructed_training_model)
            record_linker.train()
            record_linker.cleanup_training()
            logger.info("[Source-to-Asset] RecordLink training completed successfully.")
            
            source_fields = self.job_config.get("source_fields", [])
            source_selected_cols = [F.col(sf.get("field")).cast("string").alias(sf.get("field")) 
                                   for sf in source_fields if sf.get("field") in source_df.columns]
            
            # Find source_id column (case-insensitive)
            source_id_column_name = None
            available_cols = source_df.columns
            for col in available_cols:
                col_upper = col.upper().replace("_", "")
                col_lower = col.lower().strip()
                if col_lower in ["source_id", "id"] or "SOURCE_ID" in col.upper() or "SOURCEID" in col_upper:
                    source_id_column_name = col
                    break
            
            if source_id_column_name:
                source_id_col = F.col(source_id_column_name).cast("string").alias("source_id")
            else:
                logger.warning("[Source-to-Asset] No source_id column found, using row number as fallback")
                source_id_col = F.monotonically_increasing_id().cast("string").alias("source_id")
            
            source_selected_cols.append(source_id_col)
            
            target_selected_cols = [F.col(field['field']).cast("string").alias(field['field']) 
                                   for field in self.fields if field['field'] in target_df.columns]
            target_selected_cols.append(F.col("dedupe_id").cast("string").alias("dedupe_id"))
            
            target_field_name_list = [field['field'] for field in self.fields if field['field'] in target_df.columns]
            target_field_name_list.append("dedupe_id")
            
            field_mapping = self.job_config.get("field_mapping", {})
            target_field_names = set(field_mapping.values())
            target_pandas_df = target_df.select(*target_selected_cols).toPandas()
            target_records = target_pandas_df.to_dict(orient="records")
            target_dict = {}
            for record in target_records:
                raw_id = record.get("dedupe_id") or record.get("id") or str(uuid4())
                target_id = str(raw_id) if raw_id is not None else str(uuid4())
                target_record = {
                    key: str(value) if value is not None else ""
                    for key, value in record.items()
                    if key in target_field_name_list
                }
                for target_field in target_field_names:
                    if target_field not in target_record:
                        target_record[target_field] = ""
                target_dict[target_id] = target_record
            
            filtered_target_dict = {}
            for target_id, target_record in target_dict.items():
                if target_id in filtered_target_sample:
                    filtered_target_dict[target_id] = target_record
            
            source_pandas_df = source_df.select(*source_selected_cols).toPandas()
            source_records = source_pandas_df.to_dict(orient="records")
            filtered_source_ids = set()
            for record in source_records:
                raw_id = record.get("source_id") or record.get("id")
                if raw_id is None:
                    raw_id = str(uuid4())
                source_id = str(raw_id) if raw_id is not None else str(uuid4())
                
                source_record = {
                    key: str(value) if value is not None else ""
                    for key, value in record.items()
                }
                mapped_source = self.apply_field_mapping(source_record, field_mapping)
                
                # Check if this source record is in the original sample
                if source_id in mapped_source_sample:
                    filtered_source_ids.add(source_id)
            
            
            threshold = self.settings.get("threshold", 0.5)
            
            logger.info("[Source-to-Asset] Converting filtered source data to RDD for distributed processing...")
            start_time = datetime.datetime.now()
            
            if source_id_column_name and filtered_source_ids:
                source_df_filtered = source_df.filter(
                    F.col(source_id_column_name).cast("string").isin(list(filtered_source_ids))
                )
            else:
                source_df_filtered = source_df
            
            source_rdd = source_df_filtered.select(*source_selected_cols).rdd.map(
                lambda row: (row.asDict(),)
            )
            
            num_partitions = self._calculate_num_partitions()
            source_rdd = source_rdd.repartition(num_partitions).persist(StorageLevel.MEMORY_AND_DISK)
            logger.info("[Source-to-Asset] Source RDD repartitioned to %d partitions", num_partitions)
            
            record_linker_broadcast = self.spark.sparkContext.broadcast(record_linker)
            target_dict_broadcast = self.spark.sparkContext.broadcast(filtered_target_dict)
            field_mapping_broadcast = self.spark.sparkContext.broadcast(field_mapping)
            
            def join_partition(partition):
                local_record_linker = record_linker_broadcast.value
                local_target_dict = target_dict_broadcast.value
                local_field_mapping = field_mapping_broadcast.value
                
                try:
                    partition_list = list(partition)
                    if not partition_list:
                        return iter([])
                    
                    partition_source_data = {}
                    
                    for (record_dict,) in partition_list:
                        raw_id = record_dict.get("source_id") or record_dict.get("id")
                        if raw_id is None:
                            raw_id = str(uuid4())
                        source_id = str(raw_id) if raw_id is not None else str(uuid4())
                        
                        source_record = {
                            key: str(value) if value is not None else ""
                            for key, value in record_dict.items()
                        }
                        
                        mapped_source = {
                            target_field: source_record.get(source_field, "")
                            for source_field, target_field in local_field_mapping.items()
                        }
                        partition_source_data[source_id] = mapped_source
                    
                    if not partition_source_data:
                        return iter([])
                    
                    partition_matches = local_record_linker.join(partition_source_data, local_target_dict, threshold=threshold)
                    
                    if partition_matches is None:
                        return iter([])
                    
                    return iter(partition_matches)
                except Exception as e:
                    logger.error(f"[Source-to-Asset] Error in join_partition: {type(e).__name__}: {e}\n{traceback.format_exc()}")
                    return iter([])
            
            matches = source_rdd.mapPartitions(join_partition).collect()
            source_rdd.unpersist()
            record_linker_broadcast.destroy()
            target_dict_broadcast.destroy()
            field_mapping_broadcast.destroy()
            
            if matches is None:
                logger.warning("[Source-to-Asset] RecordLink.join() returned None")
                matches = []
            
            duration = (datetime.datetime.now() - start_time).total_seconds()
            logger.info("[Source-to-Asset] Distributed RecordLink.join() completed in %.2f seconds, found %d matches", 
                       duration, len(matches))
            
            logger.info("[Source-to-Asset] Rebuilding source_original and source_data for match processing (filtered to sample)...")
            source_pandas_df = source_df_filtered.select(*source_selected_cols).toPandas()
            source_records = source_pandas_df.to_dict(orient="records")
            
            source_data = {}
            source_original = {}
            source_data_by_content = {}
            
            for record in source_records:
                raw_id = record.get("source_id") or record.get("id")
                if raw_id is None:
                    raw_id = str(uuid4())
                source_id = str(raw_id) if raw_id is not None else str(uuid4())
                
                if source_id not in filtered_source_ids:
                    continue
                
                source_record = {
                    key: str(value) if value is not None else ""
                    for key, value in record.items()
                }
                source_original[source_id] = source_record
                mapped_source = self.apply_field_mapping(source_record, field_mapping)
                source_data[source_id] = mapped_source
                source_data_by_content[frozenset(mapped_source.items())] = source_id
            
            source_matches = {}
            for source_id, source_record in source_original.items():
                source_matches[source_id] = {
                    "source_ids": [source_id],
                    "source_record": source_record,
                    "matched_target_records": [],
                    "match_count": 0
                }
            
            logger.info("[Source-to-Asset] Initialized %d source records in output", len(source_matches))
            
            for match in matches:
                try:
                    def is_score(value):
                        """Check if value is a score (numeric)"""
                        if isinstance(value, (float, int)):
                            return True
                        if hasattr(value, 'dtype'):
                            return np.issubdtype(value.dtype, np.number) and not np.issubdtype(value.dtype, np.integer)
                        return False
                    
                    if len(match) == 2:
                        elem1, elem2 = match
                        if is_score(elem2):
                            ids_array = elem1
                            match_score = elem2
                            
                            # Extract source_id and target_id from the array
                            if isinstance(ids_array, (list, tuple, np.ndarray)):
                                if len(ids_array) >= 2:
                                    source_id = ids_array[0]
                                    target_id = ids_array[1]
                                else:
                                    logger.warning("[Source-to-Asset] IDs array has less than 2 elements: %s", ids_array)
                                    continue
                            else:
                                logger.warning("[Source-to-Asset] First element is not a list/array: %s (type: %s)", ids_array, type(ids_array))
                                continue
                            
                            if hasattr(match_score, 'item'):
                                match_score = float(match_score.item())
                            else:
                                match_score = float(match_score) if match_score is not None else 1.0
                        else:
                            source_id, target_id = elem1, elem2
                            match_score = 1.0
                    elif len(match) == 3:
                        elem1, elem2, elem3 = match
                        # Identify which element is the score
                        score_idx = None
                        if is_score(elem1):
                            score_idx = 0
                        elif is_score(elem2):
                            score_idx = 1
                        elif is_score(elem3):
                            score_idx = 2
                        
                        if score_idx == 0:
                            match_score, source_id, target_id = elem1, elem2, elem3
                        elif score_idx == 1:
                            source_id, match_score, target_id = elem1, elem2, elem3
                        elif score_idx == 2:
                            source_id, target_id, match_score = elem1, elem2, elem3
                        else:
                            logger.warning("[Source-to-Asset] 3-tuple with no score detected, using first two as IDs: %s", match)
                            source_id, target_id = elem1, elem2
                            match_score = 1.0
                    else:
                        logger.warning("[Source-to-Asset] Unexpected match format (length %d): %s", len(match), match)
                        continue
                    
                    original_source_id = source_id
                    original_target_id = target_id
                    
                    if isinstance(source_id, np.ndarray):
                        if source_id.size > 0:
                            first_elem = source_id.flat[0]
                            if isinstance(first_elem, (np.str_, np.unicode_)):
                                source_id = str(first_elem)
                            elif hasattr(first_elem, 'item'):
                                source_id = str(first_elem.item())
                            else:
                                source_id = str(first_elem)
                        else:
                            source_id = ""
                    elif hasattr(source_id, '__iter__') and not isinstance(source_id, (str, bytes)):
                        source_id_list = list(source_id)
                        source_id = str(source_id_list[0]) if len(source_id_list) > 0 else ""
                    else:
                        source_id = str(source_id) if source_id is not None else ""
                    
                    if isinstance(target_id, np.ndarray):
                        if target_id.size > 0:
                            first_elem = target_id.flat[0]
                            if isinstance(first_elem, (np.str_, np.unicode_)):
                                target_id = str(first_elem)
                            elif hasattr(first_elem, 'item'):
                                target_id = str(first_elem.item())
                            else:
                                target_id = str(first_elem)
                        else:
                            target_id = ""
                    elif hasattr(target_id, '__iter__') and not isinstance(target_id, (str, bytes)):
                        target_id_list = list(target_id)
                        target_id = str(target_id_list[0]) if len(target_id_list) > 0 else ""
                    else:
                        target_id = str(target_id) if target_id is not None else ""
                    
                    if hasattr(match_score, 'item'):
                        match_score = float(match_score.item())
                    else:
                        match_score = float(match_score) if match_score is not None else 1.0
                    
                    if not source_id or not target_id:
                        logger.warning("[Source-to-Asset] Skipping match with empty ID: source_id=%s, target_id=%s", source_id, target_id)
                        continue
                    
                    matched_source_id = None
                    
                    if source_id in source_matches:
                        matched_source_id = source_id
                    else:
                        if source_id in source_data:
                            mapped_record = source_data[source_id]
                            record_key = frozenset(mapped_record.items())
                            if record_key in source_data_by_content:
                                matched_source_id = source_data_by_content[record_key]
                        
                        if matched_source_id is None and source_id in source_data:
                            mapped_record = source_data[source_id]
                            for orig_id, orig_record in source_original.items():
                                mapped_orig = self.apply_field_mapping(orig_record, field_mapping)
                                if mapped_orig == mapped_record:
                                    matched_source_id = orig_id
                                    break
                        
                        if matched_source_id is None:
                            logger.warning("[Source-to-Asset] Source ID %s (from %s) not found in source_matches. Tried: direct lookup, reverse lookup, content search. Available keys (first 10): %s", 
                                         source_id, original_source_id, list(source_matches.keys())[:10])
                            continue
                    
                    source_id = matched_source_id
                    
                    if source_id not in source_matches:
                        logger.warning("[Source-to-Asset] Source ID %s not in source_matches, creating entry", source_id)
                        source_matches[source_id] = {
                            "source_ids": [source_id],
                            "source_record": source_original.get(source_id, {}),
                            "matched_target_records": [],
                            "match_count": 0
                        }
                    
                    # Get target record
                    if target_id not in target_dict:
                        logger.warning("[Source-to-Asset] Target ID %s not found in target_dict, skipping", target_id)
                        continue
                    
                    target_record = target_dict.get(target_id, {})
                    source_matches[source_id]["matched_target_records"].append({
                        "target_id": target_id,
                        "match_score": match_score,
                        "target_record": target_record
                    })
                    source_matches[source_id]["match_count"] += 1
                except Exception as e:
                    logger.error("[Source-to-Asset] Error processing match %s: %s", match, str(e))
                    continue
            
            matched_results = list(source_matches.values())
            logger.info("[Source-to-Asset] Found matches for %d source records", len(matched_results))
            
            destination_path = self.external_credentials.get("source_asset_clusters_path")
            serialized_matches = [self._serialize_data(match) for match in matched_results]
            file_storage_service.save(serialized_matches, destination_path)
            
            file_storage_service.delete(self.external_credentials.get("source_asset_similar_clusters_path"))
            
            return serialized_matches
        except Exception as e:
            logger.error("[Source-to-Asset] Distributed clusters failed: %s", str(e))
            raise

    def run_source_asset_extend_clusters(self, source_df, target_df):
        """
        Extend existing source-to-target matches by finding additional target records
        that match existing source records (similar to single asset extend_clusters).
        
        Args:
            source_df: Source DataFrame
            target_df: Target DataFrame
        
        Returns:
            list: Extended source-centric structure with additional matched target records
        """
        logger.info("[Source-to-Asset] Starting extend clusters process...")
        try:
            file_storage_service = FileStorage(self.external_credentials, self.spark)
            clusters_path = self.external_credentials.get("source_asset_clusters_path")
            existing_matches = file_storage_service.read(clusters_path)
            logger.info("[Source-to-Asset] Loaded %d existing source matches", len(existing_matches))
            
            # Build a set of already matched target IDs
            matched_target_ids = set()
            source_matches_dict = {}
            
            for match in existing_matches:
                # Backward compatibility: handle old format with source_id (singular)
                source_ids = match.get("source_ids", [])
                if not source_ids:
                    old_source_id = match.get("source_id")
                    if old_source_id:
                        source_ids = [old_source_id]
                        match["source_ids"] = source_ids  # Update to new format
                source_id = source_ids[0] if source_ids else None
                
                match_count = match.get("match_count", 0)
                if source_id:
                    source_matches_dict[source_id] = match
                    if match_count > 0:
                        for target_match in match.get("matched_target_records", []):
                            target_id = target_match.get("target_id")
                            if not target_id:
                                target_record = target_match.get("target_record", {})
                                if isinstance(target_record, dict):
                                    target_id = target_record.get("dedupe_id")
                            if target_id:
                                matched_target_ids.add(str(target_id))
            
            # Get ALL source IDs from clusters (for ID mapping), but only extend those with matches
            source_ids_with_matches = set(source_matches_dict.keys())
            source_ids_to_extend = {sid for sid, m in source_matches_dict.items() if m.get("match_count", 0) > 0}
            
            # Prepare target data: get all target records not yet matched
            target_selected_cols = [F.col(field['field']).cast("string").alias(field['field']) 
                                   for field in self.fields if field['field'] in target_df.columns]
            target_selected_cols.append(F.col("dedupe_id").cast("string").alias("dedupe_id"))
            
            target_field_name_list = [field['field'] for field in self.fields if field['field'] in target_df.columns]
            target_field_name_list.append("dedupe_id")
            
            field_mapping = self.job_config.get("field_mapping", {})
            target_field_names = set(field_mapping.values())
            
            target_pandas_df = target_df.select(*target_selected_cols).toPandas()
            target_records = target_pandas_df.to_dict(orient="records")
            
            # Build target dict (excluding already matched targets)
            target_dict = {}
            for record in target_records:
                raw_id = record.get("dedupe_id") or record.get("id") or str(uuid4())
                target_id = str(raw_id) if raw_id is not None else str(uuid4())
                
                # Skip if already matched
                if target_id in matched_target_ids:
                    continue
                
                target_record = {
                    key: str(value) if value is not None else ""
                    for key, value in record.items()
                    if key in target_field_name_list
                }
                # Ensure all RecordLink fields are present
                for target_field in target_field_names:
                    if target_field not in target_record:
                        target_record[target_field] = ""
                target_dict[target_id] = target_record
            
            logger.info("[Source-to-Asset] Prepared %d unmatched target records for extension", len(target_dict))
            
            # Prepare source data for matching (only sources that already have matches)
            source_fields = self.job_config.get("source_fields", [])
            source_selected_cols = [F.col(sf.get("field")).cast("string").alias(sf.get("field")) 
                                   for sf in source_fields if sf.get("field") in source_df.columns]
            source_id_col = F.col("source_id").cast("string").alias("source_id") if "source_id" in source_df.columns else F.col("id").cast("string").alias("source_id")
            source_selected_cols.append(source_id_col)
            
            source_pandas_df = source_df.select(*source_selected_cols).toPandas()
            source_records = source_pandas_df.to_dict(orient="records")
            
            source_data = {}
            source_original = {}
            
            
            for record in source_records:
                raw_id = record.get("source_id") or record.get("id") or str(uuid4())
                source_id = str(raw_id) if raw_id is not None else str(uuid4())
                
                # Only process sources that should be extended (have matches)
                if source_id not in source_ids_to_extend:
                    continue
                
                source_record = {
                    key: str(value) if value is not None else ""
                    for key, value in record.items()
                }
                source_original[source_id] = source_record
                mapped_source = self.apply_field_mapping(source_record, field_mapping)
                source_data[source_id] = mapped_source
            
            logger.info("[Source-to-Asset] Prepared %d source records for extension", len(source_data))
            
            # Load trained RecordLink model
            training_model_path = self.external_credentials.get("source_asset_training_model_path")
            training_model = file_storage_service.read(training_model_path)
            
            # Sample source and target for training model reconstruction
            sample_data_path = self.external_credentials.get("source_asset_sample_data_path")
            
            if not sample_data_path:
                raise ValueError("[Source-to-Asset] source_asset_sample_data_path not configured. Cannot proceed without saved sample.")
            
            try:
                sample_data = file_storage_service.read(sample_data_path)
                mapped_source_sample = sample_data.get("source_sample", {})
                filtered_target_sample = sample_data.get("target_sample", {})
                if not mapped_source_sample or not filtered_target_sample:
                    raise ValueError("[Source-to-Asset] Loaded sample is empty. Cannot proceed.")
            except (FileNotFoundError, Exception) as e:
                raise ValueError(f"[Source-to-Asset] Cannot load saved sample from {sample_data_path}. "
                              f"Sample is required for extend_clusters step. Error: {str(e)}. "
                              f"Please rerun training step to generate the sample.") from e
            
            source_key_mapping = {}
            target_key_mapping = {}
            
            for source_id, source_record in mapped_source_sample.items():
                source_id_str = str(source_id)
                source_key_mapping[source_id] = source_id_str
            
            for target_id, target_record in filtered_target_sample.items():
                target_id_str = str(target_id)
                target_key_mapping[target_id] = target_id_str
            
            # Create ID mapping: database_id (from clusters) -> sample_key (from sample)
            id_mapping = {}
            field_mapping = self.job_config.get("field_mapping", {})
            
            # Strategy 1: Fast path - Check if clusters file already has sample_key
            for source_id in source_ids_with_matches:
                match = source_matches_dict.get(source_id)
                if not match:
                    continue
                
                sample_key = match.get("sample_key")
                if sample_key and sample_key in mapped_source_sample:
                    id_mapping[source_id] = sample_key
            
            # Strategy 2: Fallback - Match by content using source_record from clusters file
            unmatched_ids = [sid for sid in source_ids_with_matches if sid not in id_mapping]
            
            if unmatched_ids:
                for source_id in unmatched_ids:
                    match = source_matches_dict.get(source_id)
                    if not match:
                        continue
                    
                    source_record = match.get("source_record", {})
                    if not source_record:
                        continue
                    
                    mapped_record = self.apply_field_mapping(source_record, field_mapping)
                    sample_key = self._find_matching_sample_key(mapped_record, mapped_source_sample)
                    if sample_key:
                        id_mapping[source_id] = sample_key
            
            # Use mapping to filter sample data - only extend sources with matches
            filtered_source_data = {}
            for source_id in source_ids_to_extend:
                sample_key = id_mapping.get(source_id)
                if sample_key and sample_key in mapped_source_sample:
                    string_key = source_key_mapping.get(sample_key, str(sample_key))
                    filtered_source_data[string_key] = mapped_source_sample[sample_key]
                else:
                    logger.warning("[Source-to-Asset] Source record %s not found in sample after ID mapping", source_id)
            
            if not filtered_source_data:
                dest_path = self.external_credentials.get("source_asset_extend_clusters_path")
                if dest_path:
                    serialized_matches = [self._serialize_data(match) for match in existing_matches]
                    file_storage_service.save(serialized_matches, dest_path)
                
                return existing_matches
            
            filtered_target_dict = {}
            
            for sample_target_id, sample_target_record in filtered_target_sample.items():
                string_key = target_key_mapping.get(sample_target_id, str(sample_target_id))
                filtered_target_dict[string_key] = sample_target_record
            
            
            if not filtered_source_data:
                raise ValueError(
                    "[Source-to-Asset] No source records found in sample for extend_clusters. "
                    "This may happen if:\n"
                    "1. Clusters file uses database IDs that don't match sample indices\n"
                    "2. Sample file is missing or corrupted\n"
                    "3. Field mapping is incorrect\n"
                    "Please ensure training step has run and sample file exists."
                )
            
            if not filtered_target_dict:
                dest_path = self.external_credentials.get("source_asset_extend_clusters_path")
                if dest_path:
                    serialized_matches = [self._serialize_data(match) for match in existing_matches]
                    file_storage_service.save(serialized_matches, dest_path)
                
                return existing_matches
            
            # Extract pairs directly from training model
            reconstructed_training_model = {
                key: [item["pair"] for item in training_model.get(key, [])]
                for key in ["match", "distinct", "unsure"]
            }
            
            logger.info("[Source-to-Asset] Extracted %d match, %d distinct, %d unsure pairs from training model",
                       len(reconstructed_training_model["match"]),
                       len(reconstructed_training_model["distinct"]),
                       len(reconstructed_training_model["unsure"]))
            
            source_data_for_training = {}
            target_data_for_training = {}
            
            for source_id, source_record in mapped_source_sample.items():
                source_id_str = source_key_mapping[source_id]
                source_data_for_training[source_id_str] = source_record
            
            for target_id, target_record in filtered_target_sample.items():
                target_id_str = target_key_mapping[target_id]
                target_data_for_training[target_id_str] = target_record
            
            
            num_cores = self.job_config.get("num_cores") or self.spark.sparkContext.defaultParallelism
            recordlink_fields = self._build_recordlink_fields()
            record_linker = dedupe.RecordLink(recordlink_fields, num_cores=num_cores)
            
            record_linker.prepare_training(source_data_for_training, target_data_for_training)
            record_linker.mark_pairs(reconstructed_training_model)
            
            record_linker.train()
            record_linker.cleanup_training()
            
            extension_threshold = self.settings.get("extension_threshold", 0.7)
            
            matches = record_linker.join(filtered_source_data, filtered_target_dict, threshold=extension_threshold, constraint="many-to-one")
            
            if matches is None:
                logger.warning("[Source-to-Asset] RecordLink.join() returned None - this usually means no records were blocked together")
                matches = []
            elif len(matches) == 0:
                logger.warning("[Source-to-Asset] RecordLink.join() returned empty list - no matches found above threshold %.2f", extension_threshold)
            
            logger.info("[Source-to-Asset] Found %d new matches for extension (will add to %d existing source matches)", 
                       len(matches), len(source_matches_dict))
            
            # Process new matches and add to existing matches
            new_matches_count = 0
            for match in matches:
                try:
                    # Parse match (same logic as run_source_asset_clusters)
                    def is_score(value):
                        if isinstance(value, (float, int)):
                            return True
                        if hasattr(value, 'dtype'):
                            return np.issubdtype(value.dtype, np.number) and not np.issubdtype(value.dtype, np.integer)
                        return False
                    
                    if len(match) == 2:
                        elem1, elem2 = match
                        if is_score(elem2):
                            ids_array = elem1
                            match_score = elem2
                            
                            if isinstance(ids_array, (list, tuple, np.ndarray)):
                                if len(ids_array) >= 2:
                                    source_id = ids_array[0]
                                    target_id = ids_array[1]
                                else:
                                    continue
                            else:
                                continue
                            
                            if hasattr(match_score, 'item'):
                                match_score = float(match_score.item())
                            else:
                                match_score = float(match_score) if match_score is not None else 1.0
                        else:
                            source_id, target_id = elem1, elem2
                            match_score = 1.0
                    elif len(match) == 3:
                        elem1, elem2, elem3 = match
                        score_idx = None
                        if is_score(elem1):
                            score_idx = 0
                        elif is_score(elem2):
                            score_idx = 1
                        elif is_score(elem3):
                            score_idx = 2
                        
                        if score_idx == 0:
                            match_score, source_id, target_id = elem1, elem2, elem3
                        elif score_idx == 1:
                            source_id, match_score, target_id = elem1, elem2, elem3
                        elif score_idx == 2:
                            source_id, target_id, match_score = elem1, elem2, elem3
                        else:
                            source_id, target_id = elem1, elem2
                            match_score = 1.0
                    else:
                        continue
                    
                    # Convert IDs to strings
                    if isinstance(source_id, np.ndarray):
                        if source_id.size > 0:
                            first_elem = source_id.flat[0]
                            if isinstance(first_elem, (np.str_, np.unicode_)):
                                source_id = str(first_elem)
                            elif hasattr(first_elem, 'item'):
                                source_id = str(first_elem.item())
                            else:
                                source_id = str(first_elem)
                        else:
                            source_id = ""
                    elif hasattr(source_id, '__iter__') and not isinstance(source_id, (str, bytes)):
                        source_id_list = list(source_id)
                        source_id = str(source_id_list[0]) if len(source_id_list) > 0 else ""
                    else:
                        source_id = str(source_id) if source_id is not None else ""
                    
                    if isinstance(target_id, np.ndarray):
                        if target_id.size > 0:
                            first_elem = target_id.flat[0]
                            if isinstance(first_elem, (np.str_, np.unicode_)):
                                target_id = str(first_elem)
                            elif hasattr(first_elem, 'item'):
                                target_id = str(first_elem.item())
                            else:
                                target_id = str(first_elem)
                        else:
                            target_id = ""
                    elif hasattr(target_id, '__iter__') and not isinstance(target_id, (str, bytes)):
                        target_id_list = list(target_id)
                        target_id = str(target_id_list[0]) if len(target_id_list) > 0 else ""
                    else:
                        target_id = str(target_id) if target_id is not None else ""
                    
                    if not source_id or not target_id or source_id not in source_matches_dict:
                        continue
                    
                    if target_id not in target_dict:
                        continue
                    
                    target_record = target_dict.get(target_id, {})
                    
                    # Add new match to existing source match
                    existing_match = source_matches_dict[source_id]
                    existing_match["matched_target_records"].append({
                        "target_id": target_id,
                        "match_score": float(match_score) if hasattr(match_score, 'item') else float(match_score),
                        "target_record": target_record,
                        "extended": True
                    })
                    existing_match["match_count"] = len(existing_match["matched_target_records"])
                    new_matches_count += 1
                    
                except Exception as e:
                    logger.error("[Source-to-Asset] Error processing extension match %s: %s", match, str(e))
                    continue
            
            logger.info("[Source-to-Asset] Successfully added %d new matches to existing source matches", new_matches_count)
            
            
            extended_results = list(source_matches_dict.values())
            logger.info("[Source-to-Asset] Saving extended matches: %d source records (original: %d, new matches added: %d)", 
                       len(extended_results), len(existing_matches), new_matches_count)
            
            dest_path = self.external_credentials.get("source_asset_extend_clusters_path")
            if not dest_path:
                raise ValueError("[Source-to-Asset] source_asset_extend_clusters_path not found in external_credentials")
            
            logger.info("[Source-to-Asset] Saving to path: %s", dest_path)
            serialized_matches = [self._serialize_data(match) for match in extended_results]
            file_storage_service.save(serialized_matches, dest_path)
            logger.info("[Source-to-Asset] Successfully saved extended matches to %s", dest_path)
            
            file_storage_service.delete(self.external_credentials.get("source_asset_similar_clusters_path"))
            
            return serialized_matches
        except Exception as e:
            logger.error("[Source-to-Asset] Extend clusters failed: %s", str(e))
            raise

    def run_source_asset_similar_clusters(self, source_df, target_df):
        """
        Find groups of similar source-to-target matches (similar to single asset similar_clusters).
        Groups source records that have similar matched target records.
        
        Args:
            source_df: Source DataFrame
            target_df: Target DataFrame
        
        Returns:
            list: Groups of similar source matches
        """
        logger.info("[Source-to-Asset] Starting similar clusters identification process...")
        try:
            file_storage_service = FileStorage(self.external_credentials, self.spark)
            # Try extend_clusters first, fallback to initial clusters if not available
            extend_clusters_path = self.external_credentials.get("source_asset_extend_clusters_path")
            clusters_path = self.external_credentials.get("source_asset_clusters_path")
            
            source_matches = None
            if extend_clusters_path:
                try:
                    source_matches = file_storage_service.read(extend_clusters_path)
                    if source_matches and len(source_matches) > 0:
                        logger.info("[Source-to-Asset] Loaded %d source matches from extend_clusters", len(source_matches))
                    else:
                        logger.info("[Source-to-Asset] Extend clusters file exists but is empty, falling back to initial clusters")
                        source_matches = None
                except (FileNotFoundError, Exception) as e:
                    logger.info("[Source-to-Asset] Extend clusters not found, falling back to initial clusters: %s", str(e))
                    source_matches = None
            
            if source_matches is None and clusters_path:
                try:
                    source_matches = file_storage_service.read(clusters_path)
                    logger.info("[Source-to-Asset] Loaded %d source matches from initial clusters", len(source_matches))
                except (FileNotFoundError, Exception) as e:
                    logger.error("[Source-to-Asset] Initial clusters file not found: %s", str(e))
                    raise ValueError("[Source-to-Asset] No source matches found. Please run clusters first.")
            
            if not source_matches or len(source_matches) == 0:
                raise ValueError("[Source-to-Asset] No source matches found. Please run clusters or extend_clusters first.")
            
            logger.info("[Source-to-Asset] Loaded %d source matches for processing", len(source_matches))
            
            similarity_threshold = self.settings.get("similarity_threshold", 90)
            
            # Build representative records for each source match
            # Representative = merged source record + merged target records
            source_reps = []
            for match in source_matches:
                
                # Backward compatibility: handle old format with source_id (singular)
                source_ids = match.get("source_ids", [])
                if not source_ids:
                    old_source_id = match.get("source_id")
                    if old_source_id:
                        source_ids = [old_source_id]
                        match["source_ids"] = source_ids  # Update to new format
                source_id = source_ids[0] if source_ids else None
                if not source_id:
                    continue
                source_record = match.get("source_record", {})
                matched_targets = match.get("matched_target_records", [])
                
                # Merge target records to create representative
                if matched_targets:
                    target_records = [t.get("target_record", {}) for t in matched_targets]
                    merged_target = self._merge_cluster_records(target_records)
                else:
                    merged_target = {}
                
                # Combine source and merged target as representative
                rep_record = {**source_record, **merged_target, "source_id": source_id}
                source_reps.append((source_id, rep_record))
            
            logger.info("[Source-to-Asset] Generated representative records for %d source matches", len(source_reps))
            
            # Compare all pairs of source matches
            reps_rdd = self.spark.sparkContext.parallelize(source_reps)
            candidate_pairs = reps_rdd.cartesian(reps_rdd).filter(lambda pair: pair[0][0] < pair[1][0])
            fields_broadcast = self.spark.sparkContext.broadcast(self.fields)
            
            similar_pairs = candidate_pairs.filter(
                lambda pair: DedupeOptimizer.compute_similarity_static(pair[0][1], pair[1][1], fields_broadcast.value) >= similarity_threshold
            ).map(lambda pair: (pair[0][0], pair[1][0])).collect()
            
            logger.info("[Source-to-Asset] Found %d similar source match pairs", len(similar_pairs))
            
            # Use union-find to group similar source matches
            parent = {}
            def find(x):
                while parent[x] != x:
                    parent[x] = parent[parent[x]]
                    x = parent[x]
                return x
            def union(x, y):
                rootx = find(x)
                rooty = find(y)
                if rootx != rooty:
                    parent[rooty] = rootx
            
            all_ids = [sid for sid, _ in source_reps]
            for sid in all_ids:
                parent[sid] = sid
            for sid1, sid2 in similar_pairs:
                union(sid1, sid2)
            
            groups_dict = {}
            for sid in all_ids:
                root = find(sid)
                groups_dict.setdefault(root, []).append(sid)
            
            # Construct groups using the representative records
            rep_dict = dict(source_reps)
            groups = [[rep_dict[sid] for sid in group] for group in groups_dict.values() if len(group) > 1]
            
            logger.info("[Source-to-Asset] Grouped similar source matches into %d groups", len(groups))
            
            serialized_groups = [self._serialize_data(group) for group in groups]
            similar_clusters_path = self.external_credentials.get("source_asset_similar_clusters_path")
            file_storage_service.save(serialized_groups, similar_clusters_path)
            
            file_storage_service.delete(self.external_credentials.get("source_asset_extend_clusters_path"))
            
            return serialized_groups
        except Exception as e:
            logger.error("[Source-to-Asset] Similar clusters failed: %s", str(e))
            raise

    def run_source_asset_merge_clusters(self, source_df, target_df):
        """
        Merge similar source-to-target matches and prepare final output (similar to single asset merge_clusters).
        
        Args:
            source_df: Source DataFrame
            target_df: Target DataFrame
        
        Returns:
            dict: Statistics about merged matches
        """
        logger.info("[Source-to-Asset] Starting merge clusters process...")
        try:
            file_storage_service = FileStorage(self.external_credentials, self.spark)
            
            # Delete intermediate files
            file_storage_service.delete(self.external_credentials.get("source_asset_extend_clusters_path"))
            
            # Load existing matches
            clusters_path = self.external_credentials.get("source_asset_clusters_path")
            try:
                source_matches = file_storage_service.read(clusters_path)
            except Exception as e:
                logger.error("[Source-to-Asset] Failed to read clusters file: %s", str(e))
                raise ValueError("[Source-to-Asset] No source matches found. Cannot proceed with merge.")
            
            if source_matches is None:
                raise ValueError("[Source-to-Asset] No source matches found. Cannot proceed with merge.")
            if not isinstance(source_matches, (list, dict)):
                raise ValueError(f"[Source-to-Asset] Invalid source_matches format: {type(source_matches)}. Expected list or dict.")
            if isinstance(source_matches, dict):
                source_matches = list(source_matches.values())
            if len(source_matches) == 0:
                raise ValueError("[Source-to-Asset] No source matches found. Cannot proceed with merge.")
            
            # Load similar clusters if they exist
            similar_clusters_path = self.external_credentials.get("source_asset_similar_clusters_path")
            similar_groups = []
            try:
                similar_groups = file_storage_service.read(similar_clusters_path)
                if similar_groups is None:
                    similar_groups = []
                elif not isinstance(similar_groups, list):
                    similar_groups = []
                elif len(similar_groups) == 0:
                    similar_groups = []
            except Exception:
                logger.info("[Source-to-Asset] No similar clusters found, skipping merge")
                similar_groups = []
            
            # Merge similar groups
            if similar_groups and len(similar_groups) > 0:
                # Build mapping: source_id -> group
                source_to_group = {}
                for group in similar_groups:
                    if not isinstance(group, list):
                        continue
                    for rep_record in group:
                        if not isinstance(rep_record, dict):
                            continue
                        source_id = rep_record.get("source_id")
                        if source_id:
                            source_to_group[source_id] = group
                
                # Merge matches in the same group
                merged_matches = {}
                for match in source_matches:
                    
                    # Backward compatibility: handle old format with source_id (singular)
                    source_ids = match.get("source_ids", [])
                    if not source_ids:
                        old_source_id = match.get("source_id")
                        if old_source_id:
                            source_ids = [old_source_id]
                            match["source_ids"] = source_ids  # Update to new format
                    source_id = source_ids[0] if source_ids else None
                    if not source_id:
                        continue
                    if source_id in source_to_group:
                        # Find or create merged group
                        group_key = tuple(sorted([r.get("source_id") for r in source_to_group[source_id] if r.get("source_id")]))
                        if group_key not in merged_matches:
                            merged_matches[group_key] = {
                                "source_ids": list(group_key),
                                "source_records": [],
                                "matched_target_records": [],
                                "match_count": 0
                            }
                        
                        # Add source record
                        merged_matches[group_key]["source_records"].append(match.get("source_record", {}))
                        
                        # Merge target records (deduplicate by target_id)
                        existing_target_ids = {t.get("target_id") for t in merged_matches[group_key]["matched_target_records"]}
                        for target_match in match.get("matched_target_records", []):
                            target_id = target_match.get("target_id")
                            if target_id not in existing_target_ids:
                                merged_matches[group_key]["matched_target_records"].append(target_match)
                                existing_target_ids.add(target_id)
                        
                        merged_matches[group_key]["match_count"] = len(merged_matches[group_key]["matched_target_records"])
                    else:
                        # Not in a group, keep as-is
                        merged_matches[(source_id,)] = {
                            "source_ids": [source_id],
                            "source_records": [match.get("source_record", {})],
                            "matched_target_records": match.get("matched_target_records", []),
                            "match_count": match.get("match_count", 0)
                        }
                
                # Convert to list format
                final_matches = []
                for group_key, merged_match in merged_matches.items():
                    # Create merged source record
                    merged_source_record = self._merge_cluster_records(merged_match["source_records"])
                    
                    final_matches.append({
                        "source_ids": merged_match["source_ids"],
                        "source_record": merged_source_record,
                        "matched_target_records": merged_match["matched_target_records"],
                        "match_count": merged_match["match_count"],
                        "merged": len(merged_match["source_ids"]) > 1
                    })
                
                source_matches = final_matches
                logger.info("[Source-to-Asset] Merged %d similar groups into final matches", len(final_matches))
            else:
                # No similar groups - ensure all matches have proper structure (source_ids array and merged=False)
                logger.info("[Source-to-Asset] No similar groups found, ensuring all matches have proper structure")
                normalized_matches = []
                for match in source_matches:
                    # Ensure source_ids is always an array
                    source_ids = match.get("source_ids", [])
                    if not source_ids:
                        old_source_id = match.get("source_id")
                        if old_source_id:
                            source_ids = [old_source_id]
                        else:
                            logger.warning("[Source-to-Asset] Match has no source_id or source_ids, skipping: %s", match)
                            continue
                    
                    # Ensure merged field is set
                    normalized_match = {
                        "source_ids": source_ids if isinstance(source_ids, list) else [source_ids],
                        "source_record": match.get("source_record", {}),
                        "matched_target_records": match.get("matched_target_records", []),
                        "match_count": match.get("match_count", 0),
                        "merged": False  # No similar groups means no merges
                    }
                    normalized_matches.append(normalized_match)
                
                source_matches = normalized_matches
                logger.info("[Source-to-Asset] Normalized %d matches (no merges)", len(normalized_matches))
            
            # Validate source_matches is not empty
            if not source_matches or len(source_matches) == 0:
                raise ValueError("[Source-to-Asset] No source matches found after merge. Cannot create output files.")
            
            # Save final matches
            serialized_matches = [self._serialize_data(match) for match in source_matches]
            file_storage_service.save(serialized_matches, clusters_path)
            
            # Write to Iceberg table if configured
            try:
                iceberg_table_service = IcebergTableWriter(self.external_credentials, self.spark)
                iceberg_clusters_table_identifier = self.external_credentials.get("iceberg_clusters_table_identifier")
                if iceberg_clusters_table_identifier:
                    iceberg_table_service.write(source_matches, iceberg_clusters_table_identifier)
                    logger.info("[Source-to-Asset] Wrote clusters to Iceberg table")
            except Exception as e:
                logger.warning("[Source-to-Asset] Failed to write to Iceberg table: %s", str(e))
            
            
            logger.info("[Source-to-Asset] Creating output files...")
            
            source_pandas_df = source_df.toPandas()
            source_records = source_pandas_df.to_dict(orient="records")
            
            # Find source_id column
            source_id_col = None
            for col in source_pandas_df.columns:
                if col.lower() in ["source_id", "id"] or "SOURCE_ID" in col.upper():
                    source_id_col = col
                    break
            
            if source_id_col:
                source_full_data = {str(record[source_id_col]): record for record in source_records}
            else:
                source_full_data = {str(i): record for i, record in enumerate(source_records)}
            
            target_pandas_df = target_df.toPandas()
            target_records = target_pandas_df.to_dict(orient="records")
            
            # Find target_id column
            target_id_col = None
            for col in target_pandas_df.columns:
                if col.lower() in ["dedupe_id", "id"]:
                    target_id_col = col
                    break
            
            if target_id_col:
                target_full_data = {str(record[target_id_col]): record for record in target_records}
            else:
                target_full_data = {str(i): record for i, record in enumerate(target_records)}
            
            logger.info("[Source-to-Asset] Collected %d source records, %d target records",
                       len(source_full_data), len(target_full_data))
            
            source_selected_fields = [field['field'] for field in self.job_config.get("source_fields", [])]
            target_selected_fields = [field['field'] for field in self.fields]
            
            file_storage_service.delete(self.external_credentials.get("source_asset_output_with_cluster_path"))
            file_storage_service.delete(self.external_credentials.get("source_asset_output_with_merged_path"))
            
            output_with_cluster = []
            review_data_output = []
            
            for match in source_matches:
                source_ids = match.get("source_ids", [])
                if not source_ids:
                    old_source_id = match.get("source_id")
                    if old_source_id:
                        source_ids = [old_source_id]
                    else:
                        continue
                
                source_id = source_ids[0] if isinstance(source_ids, list) else source_ids
                matched_targets = match.get("matched_target_records", [])
                
                full_source_record = None
                for sid in source_ids:
                    full_source_record = source_full_data.get(str(sid)) or source_full_data.get(sid)
                    if full_source_record:
                        break
                if not full_source_record:
                    full_source_record = match.get("source_record", {})
                selected_source_record = {k: v for k, v in full_source_record.items() if k in source_selected_fields}
                
                for target_match in matched_targets:
                    # target_id can be at top level or inside target_record as dedupe_id
                    target_id = target_match.get("target_id")
                    if not target_id:
                        target_record = target_match.get("target_record", {})
                        target_id = target_record.get("dedupe_id") if isinstance(target_record, dict) else None
                    
                    if not target_id:
                        logger.warning("[Source-to-Asset] Skipping target match with no target_id: %s", target_match)
                        continue
                    
                    full_target_record = target_full_data.get(str(target_id)) or target_full_data.get(target_id) or target_match.get("target_record", {})
                    selected_target_record = {k: v for k, v in full_target_record.items() if k in target_selected_fields}
                    
                    cluster_record = {"source_id": source_id, "target_id": target_id}
                    for key, value in selected_source_record.items():
                        if isinstance(value, (dict, list)):
                            value = str(value)
                        cluster_record[f"source_{key}"] = value
                    for key, value in selected_target_record.items():
                        if isinstance(value, (dict, list)):
                            value = str(value)
                        cluster_record[f"target_{key}"] = value
                    output_with_cluster.append(cluster_record)
                    
                    review_record = {"source_id": source_id, "target_id": target_id}
                    for key, value in full_source_record.items():
                        if isinstance(value, (dict, list)):
                            value = str(value)
                        review_record[f"source_{key}"] = value
                    for key, value in full_target_record.items():
                        if isinstance(value, (dict, list)):
                            value = str(value)
                        review_record[f"target_{key}"] = value
                    
                    review_data_output.append(review_record)
            
            # Collect matched target IDs to identify distinct targets
            matched_target_ids = set()
            for match in source_matches:
                for target_match in match.get("matched_target_records", []):
                    target_id = target_match.get("target_id")
                    if not target_id:
                        target_record = target_match.get("target_record", {})
                        target_id = target_record.get("dedupe_id") if isinstance(target_record, dict) else None
                    if target_id:
                        matched_target_ids.add(str(target_id))
            
            distinct_target_count = 0
            for target_id, target_record in target_full_data.items():
                target_id_str = str(target_id)
                if target_id_str not in matched_target_ids:
                    distinct_target_count += 1
                    selected_target_record = {k: v for k, v in target_record.items() if k in target_selected_fields}
                    
                    distinct_cluster_record = {"source_id": None, "target_id": target_id_str}
                    for key, value in selected_target_record.items():
                        if isinstance(value, (dict, list)):
                            value = str(value)
                        distinct_cluster_record[f"target_{key}"] = value
                    output_with_cluster.append(distinct_cluster_record)
                    
                    distinct_review_record = {"source_id": None, "target_id": target_id_str}
                    for key, value in target_record.items():
                        if isinstance(value, (dict, list)):
                            value = str(value)
                        distinct_review_record[f"target_{key}"] = value
                    review_data_output.append(distinct_review_record)
            
            output_with_merged = review_data_output
            
            logger.info("[Source-to-Asset] Generated output: %d cluster records, %d review records", 
                       len(output_with_cluster), len(output_with_merged))
            
            if len(output_with_cluster) == 0 and len(output_with_merged) == 0:
                logger.warning("[Source-to-Asset] WARNING: No output records generated! This might indicate a data structure mismatch.")
                logger.warning("[Source-to-Asset] Source matches count: %d", len(source_matches))
                logger.warning("[Source-to-Asset] Source full data count: %d, Target full data count: %d", 
                             len(source_full_data), len(target_full_data))
                if source_matches:
                    logger.warning("[Source-to-Asset] Sample match structure: %s", str(source_matches[0])[:500])
            
            output_with_cluster_path = self.external_credentials.get("source_asset_output_with_cluster_path")
            output_with_merged_path = self.external_credentials.get("source_asset_output_with_merged_path")
            
            if output_with_cluster_path:
                serialized_cluster_output = [self._serialize_data(record) for record in output_with_cluster]
                file_storage_service.save(serialized_cluster_output, output_with_cluster_path)
                logger.info("[Source-to-Asset] Saved %d records to output_with_cluster at %s", 
                           len(serialized_cluster_output), output_with_cluster_path)
            
            if output_with_merged_path:
                serialized_merged_output = [self._serialize_data(record) for record in output_with_merged]
                file_storage_service.save(serialized_merged_output, output_with_merged_path)
                logger.info("[Source-to-Asset] Saved %d records to output_with_merged at %s", 
                           len(serialized_merged_output), output_with_merged_path)
            
            file_storage_service.delete(self.external_credentials.get("source_asset_similar_clusters_path"))
            
            stats = {
                "total_source_matches": len(source_matches),
                "total_matched_targets": sum(m.get("match_count", 0) for m in source_matches),
                "sources_with_matches": sum(1 for m in source_matches if m.get("match_count", 0) > 0),
                "sources_without_matches": sum(1 for m in source_matches if m.get("match_count", 0) == 0),
                "distinct_target_records": distinct_target_count,
                "total_output_records": len(output_with_cluster),
                "total_merged_output_records": len(output_with_merged)
            }
            
            logger.info("[Source-to-Asset] Merge completed: %s", stats)
            return stats
        except Exception as e:
            logger.error("[Source-to-Asset] Merge clusters failed: %s", str(e))
            raise

    def _find_matching_sample_key(self, source_record, sample_data):
        """
        Find matching sample key by comparing record values.
        
        Args:
            source_record: dict - Source record (already field-mapped)
            sample_data: dict - Sample data {sample_key: record, ...}
        
        Returns:
            str: Sample key if match found, None otherwise
        """
        if not source_record or not sample_data:
            return None
        
        # Get field names from source record (excluding metadata and ID fields)
        # Exclude ID fields like 'id', 'source_id', 'dedupe_id', etc. as they won't match
        excluded_fields = {'id', 'source_id', 'dedupe_id', 'target_id', '_id', 'row_id', 'record_id'}
        source_fields = set(k for k in source_record.keys() 
                           if not k.startswith('_') 
                           and k.lower() not in excluded_fields
                           and source_record[k] is not None 
                           and str(source_record[k]).strip() != "")
        
        if not source_fields:
            return None
        
        # Compare with each sample record
        for sample_key, sample_record in sample_data.items():
            if self._records_match(source_record, sample_record, source_fields):
                return sample_key
        
        return None

    def _records_match(self, record1, record2, fields_to_check=None):
        """
        Compare two records by comparing field values.
        
        Args:
            record1: dict - First record
            record2: dict - Second record
            fields_to_check: set - Fields to compare (if None, compare all)
        
        Returns:
            bool: True if records match
        """
        if not record1 or not record2:
            return False
        
        # Get fields to check
        if fields_to_check is None:
            fields1 = set(k for k in record1.keys() if not k.startswith('_'))
            fields2 = set(k for k in record2.keys() if not k.startswith('_'))
            fields_to_check = fields1 & fields2  # Intersection
        
        if not fields_to_check:
            return False
        
        # Compare all field values
        for field in fields_to_check:
            val1 = record1.get(field)
            val2 = record2.get(field)
            
            # Handle None/empty values
            if val1 is None and val2 is None:
                continue
            if val1 is None or val2 is None:
                return False
            
            # Normalize and compare strings
            str_val1 = str(val1).strip().lower()
            str_val2 = str(val2).strip().lower()
            
            if str_val1 != str_val2:
                return False
        
        # All fields matched
        return True

    def run(self):
        # Main entry point to execute the deduplication process.
        try:
            logger.info("Starting dedupe processing")
            mode = self.settings.get("mode", "dedupe").lower()
            
            # Read data based on mode
            if self.is_source_asset:
                # Source-to-asset mode: Read 2 DataFrames
                target_df, source_df = self._optimized_read()
                # Prepare target DataFrame
                target_df = self._concat_fields(target_df)
                target_df = self._prepare_dedupe_id(target_df)
                # Prepare source DataFrame - ensure it has an ID column
                if "dedupe_id" not in source_df.columns and "id" not in source_df.columns:
                    # Create a simple ID from first column or row number
                    source_df = source_df.withColumn("source_id", F.monotonically_increasing_id().cast("string"))
                elif "id" in source_df.columns and "dedupe_id" not in source_df.columns:
                    source_df = source_df.withColumn("dedupe_id", F.col("id").cast("string"))
                # Set target_df as df_spark for backward compatibility (single-asset functions still use self.df_spark)
                self.df_spark = target_df
            else:
                # Single asset mode: Read 1 DataFrame (existing logic)
                if mode not in ["similar_clusters"]:
                    self.df_spark = self._optimized_read()
                    self.df_spark = self._concat_fields(self.df_spark)
                    self.df_spark = self._prepare_dedupe_id(self.df_spark)
            
            # Route based on run_type
            if mode == "training":
                logger.info("Running in 'training' mode: Generating training data only.")
                self.run_training_data()
                return {"status": "success", "message": "Training data generated and saved successfully."}
            
            elif mode == "source_asset_training":
                logger.info("[Source-to-Asset] Running in 'source_asset_training' mode: Source-to-target training.")
                if not self.is_source_asset:
                    raise ValueError("source_asset_training requires source-to-asset mode")
                result = self.run_source_asset_training(source_df, target_df)
                return {"status": "success", "message": "Source-to-target training pairs generated successfully.", "data": result}
            
            elif mode == "clusters":
                logger.info("Running in 'clusters' mode: Performing clusters only.")
                self.run_clusters()
                return {"status": "success", "message": "Clusters data generated successfully."}
            
            elif mode == "source_asset_clusters":
                logger.info("[Source-to-Asset] Running in 'source_asset_clusters' mode: Source-to-target clustering.")
                if not self.is_source_asset:
                    raise ValueError("source_asset_clusters requires source-to-asset mode")
                result = self.run_source_asset_clusters(source_df, target_df)
                return {"status": "success", "message": "Source-to-target matches generated successfully.", "data": result}
            
            elif mode == "extend_clusters" or mode == "source_asset_extend_clusters":
                logger.info("Running in '%s' mode: Performing extended clusters.", mode)
                if self.is_source_asset:
                    result = self.run_source_asset_extend_clusters(source_df, target_df)
                    return {"status": "success", "message": "Extended source-to-target matches generated successfully.", "data": result}
                else:
                    self.run_extend_clusters()
                    return {"status": "success", "message": "Extended clusters generated successfully."}
            
            elif mode == "similar_clusters" or mode == "source_asset_similar_clusters":
                logger.info("Running in '%s' mode: Finding similar clusters.", mode)
                if self.is_source_asset:
                    result = self.run_source_asset_similar_clusters(source_df, target_df)
                    return {"status": "success", "message": "Similar source-to-target matches found successfully.", "data": result}
                else:
                    self.run_similar_clusters()
                    return {"status": "success", "message": "Similar clusters found successfully."}
            
            elif mode == "merge":
                logger.info("Running in 'merge' mode: Performing merge process.")
                if self.is_source_asset:
                    stats = self.run_source_asset_merge_clusters(source_df, target_df)
                    return {"status": "success", "data": stats, "message": "Source-to-target matches merged successfully."}
                else:
                    stats = self.run_merge_clusters()
                    return {"status": "success", "data": stats, "message": "Prepared data for merge successfully."}
            
            else:
                logger.info("Running complete dedupe process")
                self.run_clusters()
                self.run_extend_clusters(mode)
                self.run_similar_clusters()
                stats = self.run_merge_clusters()
                return {"status": "success", "data": stats, "message": "Successfully completed the deduplication process."}
        except Exception as e:
            logger.error("Processing failed: %s", str(e))
            return json.dumps({"status": "error", "message": str(e), "error_type": type(e).__name__})
        finally:
            try:
                self.spark.stop()
                logger.info("Spark session stopped")
            except Exception as e:
                logger.error("Spark shutdown failed: %s", str(e))


class FileStorage:
    """
    Handles saving and reading a file from cloud storage using Spark’s Hadoop FileSystem APIs.
    """

    def __init__(self, external_credentials: dict, spark: SparkSession) -> None:
        self.credentials = deepcopy(external_credentials)
        self.spark = spark
        self.provider = self.credentials.get("provider", "aws").lower()
        if self.provider == "aws":
            logger.info("Using Spark configuration for AWS S3 operations.")
        elif self.provider == "azure":
            logger.info("Using Spark configuration for Azure Blob operations.")
        elif self.provider == "gcp":
            logger.info("Using Spark configuration for GCP Cloud Storage operations.")
        elif self.provider == "disc":
            logger.info("Using local disk for file operations.")
        else:
            raise ValueError("Unsupported provider. Use 'aws', 'azure', 'gcp', or 'disc'.")

    def _prepare_temp_file(self, model_data, extension=".json") -> str:
        """
        Prepares a temporary file for model data.
        Writes as CSV if extension is '.csv', otherwise as JSON.
        Returns the path to the temporary file.
        """
        logger.info("Preparing temporary file for model data...")
        try:
            if extension.lower() == ".csv":
                if isinstance(model_data, pd.DataFrame):
                    df = model_data
                elif isinstance(model_data, list):
                    df = pd.DataFrame(model_data)
                elif isinstance(model_data, dict):
                    df = pd.DataFrame([model_data])
                else:
                    raise ValueError("Unsupported data type for CSV export")
                with tempfile.NamedTemporaryFile(delete=False, mode="w", suffix=".csv") as tmp_file:
                    df.to_csv(tmp_file, index=False)
                    tmp_path = tmp_file.name
            else:
                with tempfile.NamedTemporaryFile(delete=False, mode="w", suffix=".json") as tmp_file:
                    json.dump(model_data, tmp_file)
                    tmp_path = tmp_file.name
            logger.info("Temporary file created at %s", tmp_path)
            return tmp_path
        except Exception as e:
            logger.error("Failed to prepare temporary file: %s", str(e))
            raise RuntimeError(f"Failed to prepare temporary file: {str(e)}")

    def save(self, data, destination_path: str) -> str:
        logger.info("Saving file to %s using provider '%s'...", destination_path, self.provider)
        tmp_path = None
        try:
            ext = os.path.splitext(destination_path)[1].lower()
            tmp_path = self._prepare_temp_file(data, extension=ext)
            if self.provider == "disc":
                # Ensure parent directory exists
                parent_dir = os.path.dirname(destination_path)
                if parent_dir and not os.path.exists(parent_dir):
                    os.makedirs(parent_dir, exist_ok=True)
                import shutil
                shutil.copy(tmp_path, destination_path)
                file_url = f"file://{destination_path}"
                logger.info("File saved locally at %s", file_url)
                return file_url
            else:
                sc = self.spark.sparkContext
                hadoop_conf = sc._jsc.hadoopConfiguration()
                fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                    self.spark._jvm.org.apache.hadoop.fs.Path(destination_path).toUri(), hadoop_conf)
                src_path = self.spark._jvm.org.apache.hadoop.fs.Path("file://" + tmp_path)
                dst_path = self.spark._jvm.org.apache.hadoop.fs.Path(destination_path)
                logger.debug("Source: %s, Destination: %s", src_path, dst_path)
                try:
                    fs.copyFromLocalFile(False, True, src_path, dst_path)
                    if not fs.exists(dst_path):
                        raise IOError(f"File not saved at destination: {destination_path}")
                    file_url = f"{self.credentials.get('base_url', '')}{destination_path}"
                    logger.info("File saved successfully at %s", file_url)
                    return file_url
                except Exception as e:
                    logger.error("Failed to save file: %s", str(e))
                    raise RuntimeError(f"Failed to save file: {str(e)}")
        except Exception as e:
            logger.error("An error occurred during the save process: %s", str(e))
            raise RuntimeError(f"An error occurred during the save process: {str(e)}")
        finally:
            try:
                if tmp_path and os.path.exists(tmp_path):
                    os.remove(tmp_path)
                    logger.debug("Temporary file deleted.")
            except Exception as e:
                logger.warning("Failed to delete temporary file: %s", str(e))

    def read(self, destination_path: str) -> dict:
        logger.info("Reading file from %s using provider '%s'...", destination_path, self.provider)
        if self.provider == "disc":
            try:
                with open(destination_path, "r", encoding="utf-8") as f:
                    content = f.read().lstrip('\ufeff').strip().strip("\x00")
                    if not content.strip():
                        logger.error("JSON content is empty, returning empty dict.")
                        raise RuntimeError(f"File is empty or contains only whitespace: {destination_path}")
                    try:
                        return json.loads(content)
                    except json.JSONDecodeError as e:
                        logger.error("JSON decode error: %s; returning empty dict.", str(e))
                        raise RuntimeError(f"JSON decode error: {str(e)}")
            except FileNotFoundError:
                logger.error("File not found at %s", destination_path)
                raise FileNotFoundError(f"File not found at {destination_path}")
            except Exception as e:
                logger.error("An error occurred while reading the file: %s", str(e))
                raise RuntimeError(f"An error occurred while reading the file: {str(e)}")
        else:
            sc = self.spark.sparkContext
            hadoop_conf = sc._jsc.hadoopConfiguration()
            jvm = self.spark._jvm
            dst_path = jvm.org.apache.hadoop.fs.Path(destination_path)
            fs = dst_path.getFileSystem(hadoop_conf)
            stream = None
            try:
                stream = fs.open(dst_path)
                model_str = self.spark._jvm.org.apache.commons.io.IOUtils.toString(stream, "utf-8")
                model_str = model_str.lstrip('\ufeff').strip().strip("\x00")
                if not model_str.strip():
                    logger.error("JSON content is empty, returning empty dict.")
                    raise RuntimeError(f"File is empty or contains only whitespace: {destination_path}")
                try:
                    return json.loads(model_str)
                except json.JSONDecodeError as e:
                    logger.error("JSON decode error: %s; returning empty dict.", str(e))
                    raise RuntimeError(f"JSON decode error: {str(e)}")
            except FileNotFoundError:
                logger.error("File not found at %s", destination_path)
                raise FileNotFoundError(f"File not found at {destination_path}")
            except json.JSONDecodeError as e:
                logger.error("Failed to decode JSON: %s", str(e))
                raise ValueError(f"Failed to decode JSON: {str(e)}")
            except Exception as e:
                logger.error("An error occurred while reading the file: %s", str(e))
                raise RuntimeError(f"An error occurred while reading the file: {str(e)}")
            finally:
                if stream is not None:
                    try:
                        stream.close()
                    except Exception as e:
                        logger.warning("Failed to close the stream: %s", str(e))

    def delete(self, destination_path: str) -> None:
        logger.info("Deleting file at %s using provider '%s'...", destination_path, self.provider)
        if self.provider == "disc":
            try:
                if os.path.exists(destination_path):
                    os.remove(destination_path)
                    logger.info("File deleted successfully at %s", destination_path)
                else:
                    logger.warning("File does not exist at %s", destination_path)
            except Exception as e:
                logger.error("Failed to delete file: %s", str(e))
                raise RuntimeError(f"Failed to delete file: {str(e)}")
        else:
            sc = self.spark.sparkContext
            hadoop_conf = sc._jsc.hadoopConfiguration()
            jvm = self.spark._jvm
            dst_path = jvm.org.apache.hadoop.fs.Path(destination_path)
            fs = dst_path.getFileSystem(hadoop_conf)
            try:
                if fs.exists(dst_path):
                    fs.delete(dst_path, True)
                    logger.info("File deleted successfully at %s", destination_path)
                else:
                    logger.warning("File does not exist at %s", destination_path)
            except Exception as e:
                logger.error("Failed to delete file: %s", str(e))
                raise RuntimeError(f"Failed to delete file: {str(e)}")


class IcebergTableWriter:
    def __init__(self, external_credentials: dict, spark: SparkSession, num_output_partitions: int = None) -> None:
        self.spark = spark
        self.num_output_partitions = num_output_partitions
        self.external_credentials = deepcopy(external_credentials)
        self.ensure_iceberg_catalog_and_schema()

    def ensure_iceberg_catalog_and_schema(self):
        catalog_name = self.external_credentials.get('iceberg_catalog')
        schema_name = self.external_credentials.get('iceberg_schema')
        try:
            logger.info("Ensuring Iceberg catalog '%s' and schema '%s' exist...", catalog_name, schema_name)
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{schema_name}")
            logger.info("Iceberg catalog '%s' and schema '%s' are ready.", catalog_name, schema_name)
        except Exception as e:
            logger.error("Failed to ensure Iceberg catalog and schema: %s", str(e))

    def write(self, clusters_data, table_identifier: str, mode: str = "overwrite") -> None:
        try:
            logger.info("Writing clusters to Iceberg table: %s with mode: %s", table_identifier, mode)
            flat_records = []
            for cluster in clusters_data:
                if "cluster_id" in cluster and "records" in cluster:
                    cluster_id = cluster.get("cluster_id")
                    for record in cluster.get("records", []):
                        flat_record = {"cluster_id": cluster_id, **record}
                        flat_records.append(flat_record)
                elif "source_id" in cluster or "source_ids" in cluster:
                    source_ids = cluster.get("source_ids", [cluster.get("source_id")])
                    source_record = cluster.get("source_record", {})
                    matched_targets = cluster.get("matched_target_records", [])
                    
                    # Create one record per matched target
                    for target_match in matched_targets:
                        target_record = target_match.get("target_record", {})
                        match_score = target_match.get("match_score", 0.0)
                        target_id = target_match.get("target_id", "")
                        
                        flat_record = {
                            "source_ids": ",".join(str(sid) for sid in source_ids) if isinstance(source_ids, list) else str(source_ids),
                            "source_record": str(source_record),  # Convert dict to string for storage
                            "target_id": str(target_id),
                            "target_record": str(target_record),  # Convert dict to string for storage
                            "match_score": float(match_score),
                            "match_count": cluster.get("match_count", 0),
                            "merged": cluster.get("merged", False)
                        }
                        flat_records.append(flat_record)
                    
                    # If no matches, still create one record with source info
                    if not matched_targets:
                        flat_record = {
                            "source_ids": ",".join(str(sid) for sid in source_ids) if isinstance(source_ids, list) else str(source_ids),
                            "source_record": str(source_record),
                            "target_id": None,
                            "target_record": None,
                            "match_score": 0.0,
                            "match_count": 0,
                            "merged": cluster.get("merged", False)
                        }
                    flat_records.append(flat_record)
            if not hasattr(clusters_data, "write"):
                clusters_df = self.spark.createDataFrame(flat_records)
                logger.info("Clusters data converted to Spark DataFrame.")
            else:
                clusters_df = clusters_data
            if self.num_output_partitions is not None:
                clusters_df = clusters_df.coalesce(self.num_output_partitions)
                logger.info("Coalesced DataFrame to %d partitions.", self.num_output_partitions)
            else:
                num_partitions = clusters_df.rdd.getNumPartitions()
                if num_partitions > 50:
                    clusters_df = clusters_df.coalesce(50)
                    logger.info("Coalesced DataFrame to 50 partitions for optimization.")
            self.spark.sql(f"DROP TABLE IF EXISTS {table_identifier}")
            clusters_df.write.format("iceberg").mode(mode).saveAsTable(table_identifier)
            logger.info("Clusters written successfully to Iceberg table: %s", table_identifier)
        except Exception as e:
            logger.error("Failed to write clusters to Iceberg table: %s", str(e))

    def read(self, table_identifier: str):
        try:
            logger.info("Reading Iceberg table: %s", table_identifier)
            df = self.spark.read.format("iceberg").load(table_identifier)
            logger.info("Iceberg table %s read successfully.", table_identifier)
            return df
        except Exception as e:
            logger.error("Failed to read Iceberg table: %s", str(e))
            raise RuntimeError(f"Failed to read Iceberg table: {str(e)}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("Usage: python script.py '{\"config\":\"json\"}'")
        print(json.dumps({"status": "error", "message": "Usage: python script.py '{\"config\":\"json\"}'"}))
        sys.exit(1)
    try:
        raw_json = sys.argv[1]
        config = json.loads(raw_json)
        processor = DedupeOptimizer(config)
        result = processor.run()
        print("<-dq-result-> ", json.dumps(result, default=str))
    except json.JSONDecodeError as e:
        response = {"response": {"status": "failure", "message": str(e)}}
        print("<-dq-result-> ", json.dumps(response))
        sys.exit(1)
    except Exception as e:
        response = {"response": {"status": "failure", "message": str(e)}}
        print("<-dq-result-> ", json.dumps(response))
        sys.exit(1)