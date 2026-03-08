"""
File Validation Measures Implementation

This module implements S3 file validation measures including:
1. File structure validation (connection level)
2. Schema checking (asset level) 
3. Content hash verification (connection level)
4. Duplicate file detection (connection level)

The measures follow the same pattern as other measures in the codebase.
"""

import json
import hashlib
import boto3
import zlib
from typing import Dict, Tuple, Optional
from uuid import uuid4
import csv
from io import TextIOWrapper
import re
import base64
import xml.etree.ElementTree as ET
import os
from datetime import timedelta, datetime
from collections import Counter
from statistics import median
import ijson
from azure.storage.blob import BlobServiceClient

try:
    from awscrt import checksums as _awscrt_checksums  # crc32c, crc64nvme, crc32
except Exception:
    _awscrt_checksums = None

# ---- optional fallbacks ----
try:
    import google_crc32c  # crc32c
except Exception:
    google_crc32c = None

try:
    from crccheck.crc import Crc64Nvme  # CRC-64/NVME
except Exception:
    Crc64Nvme = None

from dqlabs.app_constants.dq_constants import PASSED, FAILED
from dqlabs.app_helper.dag_helper import (
    execute_native_query, 
    delete_metrics, 
    get_postgres_connection, 
    execute_query
)
from dqlabs.app_helper.dq_helper import check_measure_result
from dqlabs.app_helper.db_helper import execute_query, split_queries, fetchall
from dqlabs.app_helper.log_helper import log_error
from dqlabs.app_helper.connection_helper import decrypt_connection_config
from dqlabs.enums.connection_types import ConnectionType


def execute_file_validation_measure(measure: dict, config: dict, **kwargs) -> None:
    """
    Executes S3, ADLS file validation measures based on the measure type.
    
    Args:
        measure (dict): The measure configuration
        config (dict): The execution configuration
        **kwargs: Additional keyword arguments
    """
    try:
        # Initialize Measure Variables
        measure_name = measure.get("name")
        connection = config.get("connection")
        credentials = connection.get("credentials")
        advanced_options = credentials.get("advancedOption", [])
        advanced_config_id = measure.get("advanced_config_id")
        connection_type = connection.get("type").lower()
        connection = get_connection(connection_type, credentials)
        asset = config.get("asset")
        asset_id = asset.get("id")

        # Get Files from Connection
        files = []
        if not advanced_config_id and not asset_id:
            files = get_files(config, credentials, connection, connection_type)

        if asset_id:
            asset_properties = asset.get("properties")
            directory = asset_properties.get("full_path")
            files = get_files(config, credentials, connection, connection_type, directory=directory)
        
        # Filter Files based on Advanced Config
        if advanced_options and advanced_config_id:
            advanced_config = (next((option for option in advanced_options if option.get("id") == advanced_config_id), None))
            if advanced_config:
                files = get_files(config, credentials, connection, connection_type, directory=advanced_config.get("file_path"))
                pattern = re.compile(r"{}{}".format(advanced_config.get("file_path"), advanced_config.get("file_pattern")))
                files = [file for file in files if pattern.match(file["full_path"])]


        # Execute Measure
        metrics = {}
        status = PASSED
        if measure_name == "file_structure_validation":
            metrics, result, status = validate_file_structure(config, files, connection, connection_type)
        elif measure_name == "content_hash_verification":
            metrics, result, status = validate_content_hash(config, files, connection, connection_type)
        elif measure_name == "duplicate_file_detection":
            metrics, result, status = validate_duplicate_files(config, files, connection, connection_type)
        elif measure_name == "file_on_time_arrival":
            metrics, result, status = validate_file_on_time_arrival(config, measure, files, connection_type)

        # Save measure using the existing pattern
        measure = {**measure, **metrics, "status": status}
        if metrics:
            save_measure_detail(config, measure)
            delete_failed_rows(config, measure)

        # Save failed rows
        if result:
            save_failed_rows(config, measure, result)
            
    except Exception as e:
        log_error(f"Error executing file validation measure: {str(e)}", e)
        raise e

def validate_file_structure(config: dict, files: list, connection: any, connection_type: str) -> Tuple[Dict, str]:
    """
    Validates file structure (connection level measure) safely for large files.

    Checks:
    - File naming conventions
    - Directory structure
    - File extensions
    - File sizes
    """
    try:
        validation_results = []
        for file in files:
            file_type = file.get("file_type", "").lower()
            stream = None

            if connection_type == ConnectionType.S3.value:
                bucket_name = file.get("bucket", "")
            else:
                bucket_name = file.get("container", "")

            try:
                response = read_file(config, file, connection, connection_type)
                if connection_type == ConnectionType.S3.value:
                    stream = response.get("Body")
                else:
                    stream = ADLSStreamWrapper(response)

                # Normalize and check for empty/whitespace-only content
                chunk = stream.read(10)
                if not chunk or chunk.strip() == b'':
                    validation_results.append({
                        "is_valid": False,
                        "error": "File is empty",
                        "file_name": file.get("name"),
                        "bucket_name": bucket_name,
                        "file_path": file.get("file_path")
                    })
                    continue

                # JSON streaming validation
                if file_type == "json":
                    try:
                        for _ in ijson.items(stream, "item"):
                            break 
                    except Exception as e:
                        raise ValueError(str(e))

                # CSV streaming validation
                elif file_type == "csv":
                    wrapper = TextIOWrapper(stream, encoding="utf-8")
                    reader = csv.reader(wrapper)
                    try:
                        next(reader)
                    except Exception as e:
                        raise ValueError(str(e))

                # XML streaming validation
                elif file_type == "xml":
                    wrapper = TextIOWrapper(stream, encoding="utf-8")
                    try:
                        for _ in ET.iterparse(wrapper, events=("start", "end")):
                            break 
                    except Exception as e:
                        raise ValueError(str(e))

                # Parquet validation (magic number)
                elif file_type == "parquet":
                    content = stream.read()
                    try:
                        if isinstance(content, str):
                            content_bytes = content.encode('utf-8')
                        else:
                            content_bytes = content
                        
                        # Check for Parquet magic number (PAR1 at start and end)
                        if len(content_bytes) < 8:
                            validation_results.append({
                                "is_valid": False,
                                "error": "File too small to be valid Parquet",
                                "file_name": file.get("file_name"),
                                "file_name": file.get("name"),
                                "bucket_name": bucket_name,
                            })
                            continue
                        
                        # Check start magic number
                        if content_bytes[:4] != b'PAR1':
                            validation_results.append({
                                "is_valid": False,
                                "error": "Invalid Parquet magic number at start",
                                "file_name": file.get("file_name"),
                                "bucket_name": bucket_name,
                                "file_path": file.get("file_path")
                            })
                            continue
                        
                        # Check end magic number
                        if content_bytes[-4:] != b'PAR1':
                            validation_results.append({
                                "is_valid": False,
                                "error": "Invalid Parquet magic number at end",
                                "file_name": file.get("name"),
                                "bucket_name": bucket_name,
                                "file_path": file.get("file_path")
                            })
                            continue
                    except Exception as e:
                        validation_results.append({
                            "is_valid": False,
                            "error": f"File content is not valid Parquet: {str(e)}",
                            "file_name": file.get("name"),
                            "bucket_name": bucket_name,
                            "file_path": file.get("file_path")
                        })
                        continue
            except Exception as e:
                validation_results.append({
                    "is_valid": False,
                    "error": str(e),
                    "file_name": file.get("name"),
                    "bucket_name": bucket_name,
                    "file_path": file.get("file_path")
                })
                continue
            finally:
                if stream and connection_type == ConnectionType.S3.value:
                    stream.close()

        total_files = len(files)
        valid_files = len(validation_results) if validation_results else 0

        metrics = {
            "total_records": total_files,
            "valid_count": valid_files,
            "level": "measure"
        }
        status = PASSED
    except Exception as e:
        metrics = {}
        status = FAILED
        log_error(f"Failed on execute measure : {str(e)}", e)
    return metrics, validation_results, status


def validate_content_hash(config: dict, files: list, connection: any, connection_type: str) -> Tuple[Dict, str]:
    """
    Validates the content hash of all files in the S3 bucket using AWS S3 integrity checking.
    Connection level measure - validates all files in the connection.
    
    Based on AWS S3 object integrity checking documentation:
    https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html
    
    Args:
        measure (dict): The measure configuration
        config (dict): The execution configuration
        
    Returns:
        Tuple[Dict, str]: Metrics and status
    """
    try:
        validation_results = []
        content_hashes = []
        chunk_size = 1024 * 1024 * 5 # 5MB
        
        measure_id = config.get("measure_id", "")
        existing_metadata = get_measure_metadata(config, measure_id)
        for file in files:
            checksum_algorithm = file.get("ChecksumAlgorithm", [])
            checksum_type = file.get("ChecksumType", "FULL_OBJECT")
            container_name = file.get("bucket", "")
            if connection_type == ConnectionType.ADLS.value:
                container_name = file.get("container", "")
            try:
                response = read_file(config, file, connection, connection_type)
            except Exception as e:
                validation_results.append({
                    "is_valid": False,
                    "error": f"{str(e)}",
                    "file_name": file.get("name"),
                    "bucket_name": container_name,
                    "file_path": file.get("file_path")
                })
                continue
            if checksum_type == "COMPOSITE":
                validation_results.append({
                    "is_valid": False,
                    "error": "Composite checksum type is not supported",
                    "file_name": file.get("name"),
                    "bucket_name": container_name,
                    "file_path": file.get("file_path")
                })
                continue

            # Existing Hash
            existing_hash = next((metadata for metadata in existing_metadata if metadata.get("file_path") == file.get("file_path") and 
                    metadata.get("file_name") == file.get("name") and 
                    metadata.get("bucket_name") == container_name), None)
            if checksum_algorithm:
                algorithm = checksum_algorithm[0]
                algorithm_name = f"Checksum{algorithm}"
                actual_value = response.get(algorithm_name, "")
                
                if existing_hash:
                    expected_checksum = existing_hash.get("checksum")
                else:
                    body = response.get("Body")
                    body_response = body.iter_chunks(chunk_size=chunk_size)
                    expected_checksum = ""
                    if algorithm == "SHA256":
                        expected_checksum = _compute_streaming_sha256(body_response)
                    if algorithm == "CRC64NVME":
                        expected_checksum = _compute_streaming_crc64nvme(body_response)
                    elif algorithm == "CRC32C":
                        expected_checksum = _compute_streaming_crc32c(body_response)
                    elif algorithm == "CRC32":
                        expected_checksum = _compute_streaming_crc32(body_response)
                    elif algorithm == "SHA1":
                        expected_checksum = _compute_streaming_sha1(body_response)

                # Add to content_hashes
                content_hashes.append({
                    "file_name": file.get("name"),
                    "bucket_name": file.get("bucket"),
                    "file_path": file.get("file_path"),
                    "checksum": expected_checksum,
                    "algorithm": algorithm
                })
                # Check if the hash is valid
                if actual_value != expected_checksum:
                    validation_results.append({
                        "is_valid": False,
                        "error": "Content Hash verification failed",
                        "file_name": file.get("name"),
                        "bucket_name": container_name,
                        "file_path": file.get("file_path")
                    })
                    continue
            else:
                if connection_type == ConnectionType.ADLS.value:
                    actual_value = file.get("md5", "")
                else:
                    actual_value = file.get("etag", "")
                if existing_hash:
                    expected_checksum = existing_hash.get("checksum")
                else:
                    if connection_type == ConnectionType.ADLS.value:
                        body = response
                        body_response = body.chunks()
                    else:
                        body = response.get("Body")
                        body_response = body.iter_chunks(chunk_size=chunk_size)
                    h = hashlib.md5()
                    for chunk in body_response:
                        if not chunk:
                            continue
                        h.update(chunk)
                    expected_checksum = _b64_from_bytes(h.digest())
                content_hashes.append({
                    "file_name": file.get("name"),
                    "bucket_name": container_name,
                    "file_path": file.get("file_path"),
                    "checksum": expected_checksum,
                    "algorithm": "MD5"
                })
                if actual_value != expected_checksum:
                    validation_results.append({
                        "is_valid": False,
                        "error": "Content Hash verification failed",
                        "file_name": file.get("name"),
                        "bucket_name": container_name,
                        "file_path": file.get("file_path")
                    })
                    continue
                
        total_files = len(files)
        valid_files = sum(1 for result in validation_results if result["is_valid"]==False)

        # Create metrics following the existing pattern
        status = PASSED
        metrics = {
            "total_records": total_files,
            "valid_count": valid_files if valid_files else 0
        }
        save_measure_metadata(config, content_hashes)
    except Exception as e:
        metrics = {}
        status = FAILED
        log_error(f"Failed on execute measure : {str(e)}", e)
    return metrics, validation_results, status


def validate_duplicate_files(config: dict, files: list, connection: any, connection_type: str) -> Tuple[Dict, list, str]:
    """
    Detects duplicate files based on content hash comparison.
    Connection level measure - compares all files in the connection.

    This measure identifies files with identical content by using existing checksums
    when available (SHA256, CRC64NVME, CRC32C, CRC32, SHA1, ETag) or computing MD5
    from file content. Files with the same hash are considered duplicates.

    Performance optimization:
    - Uses existing S3 checksums when available (no need to read file content)
    - Falls back to ETag for simple uploads (multipart uploads excluded)
    - Only computes MD5 from content as last resort

    Args:
        config (dict): The execution configuration
        files (list): List of files to check for duplicates
        connection (any): The connection object
        connection_type (str): Type of connection (e.g., 's3')

    Returns:
        Tuple[Dict, list, str]: Metrics, validation results, and status
    """
    try:
        results = []
        file_hashes = {}  # hash -> list of files with that hash
        validation_results = []

        # Compute hash for each file
        for file in files:
            try:
                file_hash = None

                container_name = file.get("bucket", "")
                if connection_type == ConnectionType.ADLS.value:
                    container_name = file.get("container", "")

                # Check for existing checksum algorithms
                checksum_algorithm = file.get("ChecksumAlgorithm", [])
                if checksum_algorithm:
                    algorithm = checksum_algorithm[0]
                    if algorithm == "SHA256":
                        file_hash = file.get("ChecksumSHA256", "")
                    elif algorithm == "CRC64NVME":
                        file_hash = file.get("ChecksumCRC64NVME", "")
                    elif algorithm == "CRC32C":
                        file_hash = file.get("ChecksumCRC32C", "")
                    elif algorithm == "CRC32":
                        file_hash = file.get("ChecksumCRC32", "")
                    elif algorithm == "SHA1":
                        file_hash = file.get("ChecksumSHA1", "")

                # Fallback to ETag if available (for multipart uploads, ETag includes part count)
                if not file_hash and file.get("etag"):
                    etag = file.get("etag", "").strip('"')
                    if etag and not etag.endswith('-1'):
                        file_hash = etag

                # If no existing checksum available, compute MD5 from content
                if not file_hash:
                    response = read_file(config, file, connection, connection_type)
                    if connection_type == ConnectionType.ADLS.value:
                        body = response
                    else:
                        body = response.get("Body")
                    h = hashlib.md5()
                    chunk_size = 1024 * 1024 * 5  # 5MB chunks
                    if connection_type == ConnectionType.ADLS.value:
                        body_response = body.chunks()
                    else:
                        body_response = body.iter_chunks(chunk_size=chunk_size)
                    for chunk in body_response:
                        if not chunk:
                            continue
                        h.update(chunk)
                    file_hash = h.hexdigest()

                # Group files by hash
                if file_hash not in file_hashes:
                    file_hashes[file_hash] = []
                file_hashes[file_hash].append({
                    "file_name": file.get("name"),
                    "bucket_name": container_name,
                    "file_path": file.get("file_path"),
                    "full_path": file.get("full_path"),
                    "size": file.get("size"),
                    "hash": file_hash
                })

            except Exception as e:
                validation_results.append({
                    "is_valid": False,
                    "error": f"Failed to read file for hash computation: {str(e)}",
                    "file_names": [file.get("name")],
                    "bucket_names": [container_name],
                    "file_paths": [file.get("file_path")]
                })
                continue

        # Identify duplicate groups (files with same hash)
        for file_hash, files_with_hash in file_hashes.items():
            if len(files_with_hash) > 1:
                file_names = [f.get("file_name") for f in files_with_hash]
                file_paths = [f'{f.get("bucket_name")}/{f.get("file_path")}' for f in files_with_hash]
                bucket_names = [f.get("bucket_name") for f in files_with_hash]
                validation_results.append({
                    "is_valid": False,
                    "error": f"Duplicate files detected. {len(files_with_hash)} files have identical content (hash: {file_hash})",
                    "file_names": file_names,
                    "bucket_names": bucket_names,
                    "file_paths": file_paths,
                })

        # Calculate metrics
        total_files = len(files)
        metrics = {
            "total_records": total_files,
            "valid_count": len(validation_results) if validation_results else 0,
            "status": PASSED 
        }
        results = []
        for result in validation_results:
            bucket_name = list(set(result.get("bucket_names")))
            results.append({
                **result,
                "file_name": ",".join(result.get("file_names")),
                "bucket_name": ",".join(bucket_name),
                "file_path": ",".join(result.get("file_paths"))
            })

        status = PASSED

    except Exception as e:
        metrics = {}
        status = FAILED
        log_error(f"Failed to execute duplicate file detection: {str(e)}", e)

    return metrics, results, status

def _parse_datetime(dt_value):
    """
    Parse datetime value from string or return datetime object if already parsed.
    Handles ISO format strings with 'Z' suffix (UTC).
    """
    if isinstance(dt_value, datetime):
        return dt_value
    if isinstance(dt_value, str):
        # Handle ISO format with Z suffix (UTC)
        if dt_value.endswith('Z'):
            return datetime.fromisoformat(dt_value.replace('Z', '+00:00')).replace(tzinfo=None)
        else:
            return datetime.fromisoformat(dt_value)
    return dt_value

def validate_file_on_time_arrival(config: dict, measure: dict, files: list, connection_type: str) -> Tuple[Dict, list, str]:
    """
    Validates the file on time arrival.
    """
    try:
        
        files = sorted(files, key=lambda x: x.get("modified_at"))
        if len(files) < 2:
            return {}, [], PASSED
            

        measure_id = measure.get("id")
        properties = measure.get("properties", {})
        tolerance = properties.get("tolerance", {})
        tolerance = tolerance.get("value", 10) if tolerance.get("is_enabled", False) else None
        latest_file = files[-1]
        latest_file_path = latest_file.get("file_path")
        container_name = latest_file.get("bucket")
        if connection_type == ConnectionType.ADLS.value:
            container_name = latest_file.get("container")


        # Existing Arrival File
        existing_arrival_file = get_measure_metadata(config, measure_id)
        existing_file_path = None
        if existing_arrival_file:
            existing_file_path = existing_arrival_file[0].get("file_path")
            existing_bucket_name = existing_arrival_file[0].get("bucket_name")
            if existing_file_path == latest_file_path and existing_bucket_name == container_name:
                return {}, [], PASSED

        previous_file = files[-2]
        latest_file_time = _parse_datetime(latest_file.get("modified_at"))
        previous_file_time = _parse_datetime(previous_file.get("modified_at"))

        # Get Seasonality
        seasonality = detect_seasonality(files[:-1])
        if not seasonality:
            # Initial Comparison
            initial_upload_file = files[0]
            compare_uploaded_file = files[1]
            initial_upload_time = _parse_datetime(initial_upload_file.get("modified_at"))
            compare_uploaded_time = _parse_datetime(compare_uploaded_file.get("modified_at"))

            # Caulculate Frequency for first two files
            frequency = compare_uploaded_time - initial_upload_time
            expected_time = previous_file_time + frequency
        else:
            frequency = seasonality.get("frequency")
            active_days = seasonality.get("active_days")
            expected_time = previous_file_time
            while True:
                expected_time += frequency
                if expected_time.weekday() in active_days and active_days[expected_time.weekday()]:
                    break

        # Calculate Expected Time for latest file
        frequency_mins = frequency.total_seconds() / 60
        if tolerance:
            tolerance_percentage = tolerance / 100.0
            tolerance_mins = frequency_mins * tolerance_percentage
        else:
            tolerance_mins = frequency_mins
        diff = (latest_file_time - expected_time).total_seconds() / 60
        duration = (latest_file_time - expected_time).total_seconds()
        value_duration = (latest_file_time - previous_file_time).total_seconds()


        # Check File Late Arrival
        validation_results = []
        if diff > tolerance_mins:
            difference_duration = format_duration(duration)
            error = f"File arrived LATE by {difference_duration}. Expected: {expected_time} UTC  Arrived: {latest_file_time} UTC"
            validation_results.append({
                "is_valid": False,
                "error": error,
                "file_name": latest_file.get("name"),
                "bucket_name": container_name,
                "file_path": latest_file.get("file_path")
            })
        else:
            value_duration = frequency.total_seconds()

        metrics = {
            "valid_count": value_duration
        }
        save_measure_metadata(config, [latest_file])
        return metrics, validation_results, PASSED
        
    except Exception as e:
        log_error(f"Failed to execute file on time arrival: {str(e)}", e)
        return {}, [], FAILED

def detect_seasonality(files: list) -> dict:
    """
    Detect seasonality pattern from historical files.
    Returns:
        {
          "active_days": {0: True, ..., 6: False},
          "frequency": timedelta
        }
    """
    if len(files) < 30:  # require at least ~1 month of data
        return {}

    weekday_counts = Counter()
    deltas = []

    for i in range(1, len(files)):
        previous_file = files[i-1]
        current_file = files[i]
        prev_time = _parse_datetime(previous_file.get("modified_at"))
        curr_time = _parse_datetime(current_file.get("modified_at"))
        delta = (curr_time - prev_time).total_seconds()
        if delta > 0:
            deltas.append(delta)
        weekday_counts[curr_time.weekday()] += 1

    if not deltas:
        return {}

    # Median gap between files (robust to outliers)
    avg_frequency_seconds = median(deltas)
    frequency = timedelta(seconds=avg_frequency_seconds)

    # Detect active vs skip days
    active_days = {d: (weekday_counts[d] > 0) for d in range(7)}

    return {
        "active_days": active_days,
        "frequency": frequency
    }
                
def _b64_from_int(value: int, num_bytes: int) -> str:
    return base64.b64encode(value.to_bytes(num_bytes, "big")).decode("ascii")

def _b64_from_bytes(d: bytes) -> str:
    return base64.b64encode(d).decode("ascii")

def _compute_streaming_crc32(body_iter) -> str:
    if _awscrt_checksums:
        acc = 0
        for chunk in body_iter:
            if not chunk:
                continue
            acc = _awscrt_checksums.crc32(chunk, previous_crc32=acc)
        return _b64_from_int(acc & 0xFFFFFFFF, 4)
    # zlib fallback
    acc = 0
    for chunk in body_iter:
        if not chunk:
            continue
        acc = zlib.crc32(chunk, acc)
    return _b64_from_int(acc & 0xFFFFFFFF, 4)

def _compute_streaming_crc32c(body_iter) -> Optional[str]:
    if _awscrt_checksums:
        acc = 0
        for chunk in body_iter:
            if not chunk:
                continue
            acc = _awscrt_checksums.crc32c(chunk, previous_crc32c=acc)
        return _b64_from_int(acc & 0xFFFFFFFF, 4)
    if google_crc32c:
        hasher = google_crc32c.Checksum()
        for chunk in body_iter:
            if not chunk:
                continue
            hasher.update(chunk)
        return _b64_from_int(int(hasher.hexdigest(), 16), 4)
    return None  # no backend

def _compute_streaming_crc64nvme(body_iter) -> Optional[str]:
    if _awscrt_checksums:
        acc = 0
        for chunk in body_iter:
            if not chunk:
                continue
            acc = _awscrt_checksums.crc64nvme(chunk, previous_crc64nvme=acc)
        return _b64_from_int(acc & 0xFFFFFFFFFFFFFFFF, 8)
    if Crc64Nvme:
        crc = Crc64Nvme()
        for chunk in body_iter:
            if not chunk:
                continue
            crc.process(chunk)
        return _b64_from_int(crc.final(), 8)
    return None  # no backend

def _compute_streaming_sha1(body_iter) -> str:
    h = hashlib.sha1()
    for chunk in body_iter:
        if not chunk:
            continue
        h.update(chunk)
    return _b64_from_bytes(h.digest())

def _compute_streaming_sha256(body_iter) -> str:
    h = hashlib.sha256()
    for chunk in body_iter:
        if not chunk:
            continue
        h.update(chunk)
    return _b64_from_bytes(h.digest())

def save_measure_detail(config: dict, measure: dict):
    """
    Save metrics to the database
    """
    measure_id = measure.get("id", "")
    connection_id = config.get("connection_id")
    organization_id = config.get("organization_id")
    run_id = config.get("run_id", "")
    airflow_run_id = config.get("airflow_run_id")
    weightage = measure.get("weightage", 100)
    weightage = int(weightage) if weightage else 100
    is_archived = measure.get("is_archived", False)
    allow_score = measure.get("allow_score", True)
    is_drift_enabled = measure.get("is_drift_enabled", False)
    measure_name = measure.get("name", "")
    value = measure.get("valid_count", "")
    status = measure.get("status", PASSED)
    total_records = measure.get("total_records", 0)
    valid_count = measure.get("valid_count", 0)
    invalid_count = measure.get("invalid_count", 0)
    valid_percentage = measure.get("valid_percentage", 0)
    invalid_percentage = measure.get("invalid_percentage", 0)
    asset_id = measure.get("asset_id", "")
    asset_id = asset_id if asset_id else None
    
   
    score = None
    processed_query = measure.get("processed_query", "")
    delete_metrics(
        config,
        run_id=run_id,
        measure_id=measure_id,
        level="measure"
    )
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            measure_results = []
            query_input = (
                str(uuid4()),
                organization_id,
                connection_id,
                asset_id,
                None,
                measure_id,
                run_id,
                airflow_run_id,
                None,
                measure_name,
                "measure",
                str(value),
                weightage,
                total_records,
                valid_count,
                invalid_count,
                valid_percentage,
                invalid_percentage,
                score,
                status,
                is_archived,
                processed_query,
                allow_score,
                is_drift_enabled,
                True,
                True,
                False,
            )
            input_literals = ", ".join(["%s"] * len(query_input))
            query_param = cursor.mogrify(
                f"({input_literals},CURRENT_TIMESTAMP)",
                query_input,
            ).decode("utf-8")
            measure_results.append(query_param)

            try:
                pass_criteria_result = check_measure_result(measure, score)
                score = "null" if score is None else score
                update_measure_score_query = f"""
                    update core.measure set last_run_id='{run_id}', last_run_date=CURRENT_TIMESTAMP, score={score}, failed_rows=null, row_count = {total_records}, valid_rows = {valid_count}, invalid_rows = {invalid_count} , result = '{pass_criteria_result}'
                    where id='{measure_id}'
                """
                cursor = execute_query(
                    connection, cursor, update_measure_score_query)

            except Exception as e:
                pass

            measures_input = split_queries(measure_results)
            for input_values in measures_input:
                try:
                    input_value = ",".join(input_values)
                    attribute_insert_query = f"""
                        insert into core.metrics (id, organization_id, connection_id, asset_id, attribute_id,
                        measure_id, run_id, airflow_run_id,attribute_name, measure_name, level, value, weightage, total_count,
                        valid_count, invalid_count, valid_percentage, invalid_percentage, score, status, is_archived,
                        query, allow_score, is_drift_enabled, is_measure,is_active,is_delete, created_date)
                        values {input_value}
                    """
                    cursor = execute_query(
                        connection, cursor, attribute_insert_query)
                except Exception as e:
                    log_error("Save Measures: inserting new metric", e)
            
    except Exception as e:
        log_error(f"Failed to save metrics: {str(e)}", e)
        raise e


def get_connection(connection_type: str, credentials: dict):
    """
    Get the connection object from the config
    """
    if connection_type == ConnectionType.S3.value:
        credentials = decrypt_connection_config(credentials, connection_type)
        connection = boto3.client(
            's3',
            aws_access_key_id=credentials.get("aws_access_key"),
            aws_secret_access_key=credentials.get("aws_secret_access_key"),
            region_name=credentials.get("region")
        )
        return connection
    elif connection_type == ConnectionType.ADLS.value:
        credentials = decrypt_connection_config(credentials, connection_type)
        connection = BlobServiceClient(
            account_url=f"https://{credentials.get('storage_account_name')}.blob.core.windows.net",
            credential=credentials.get('storage_account_key')
        )
        return connection
    return None

def get_files(config: dict, credentials: dict, connection: any, connection_type: str, directory: str = None):
    """
    Get the files from the config
    """
    try:
        response = []
        if connection and connection_type == ConnectionType.S3.value:
            buckets = credentials.get("buckets", [])
            check_dir = True if directory else False
            filetype = credentials.get("filetype", [])
            for bucket_name in buckets:
                paginator = connection.get_paginator('list_objects_v2')
                page_iterator = paginator.paginate(Bucket=bucket_name)

                for page in page_iterator:
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            for ext in filetype:
                                if obj.get("Key").endswith(f".{ext}"):
                                    file_name = os.path.basename(obj.get("Key"))
                                    file_name = file_name.split(".")[0] if file_name else ""
                                    full_path = f"{bucket_name}/{obj.get('Key')}"
                                    if check_dir:
                                        if directory in obj.get("Key") or directory in full_path:
                                            response.append({
                                                "name": file_name,
                                                "asset_type": "file",
                                                "file_type": ext,
                                                "file_path": obj.get("Key"),
                                                "full_path": f"{bucket_name}/{obj.get('Key')}",
                                                "bucket": bucket_name,
                                                "created_at": obj.get("LastModified", ""),
                                                "modified_at": obj.get("LastModified", ""),
                                                "size": obj.get("Size"),
                                                "etag": obj.get("ETag", ""),
                                                "ChecksumAlgorithm": obj.get("ChecksumAlgorithm", []),
                                                "ChecksumType": obj.get("ChecksumType", ""),
                                                "ChecksumSHA256": obj.get("ChecksumSHA256", ""),
                                                "ChecksumCRC64NVME": obj.get("ChecksumCRC64NVME", ""),
                                                "ChecksumCRC32C": obj.get("ChecksumCRC32C", ""),
                                                "ChecksumCRC32": obj.get("ChecksumCRC32", ""),
                                                "ChecksumSHA1": obj.get("ChecksumSHA1", ""),
                                            })
                                    else:
                                        response.append({
                                            "name": file_name,
                                            "asset_type": "file",
                                            "file_type": ext,
                                            "file_path": obj.get("Key"),
                                            "full_path": f"{bucket_name}/{obj.get('Key')}",
                                            "bucket": bucket_name,
                                            "created_at": obj.get("LastModified", ""),
                                            "modified_at": obj.get("LastModified", ""),
                                            "size": obj.get("Size"),
                                            "etag": obj.get("ETag", ""),
                                            "ChecksumAlgorithm": obj.get("ChecksumAlgorithm", []),
                                            "ChecksumType": obj.get("ChecksumType", ""),
                                            "ChecksumSHA256": obj.get("ChecksumSHA256", ""),
                                            "ChecksumCRC64NVME": obj.get("ChecksumCRC64NVME", ""),
                                            "ChecksumCRC32C": obj.get("ChecksumCRC32C", ""),
                                            "ChecksumCRC32": obj.get("ChecksumCRC32", ""),
                                            "ChecksumSHA1": obj.get("ChecksumSHA1", ""),
                                        })
        else:
            params = {"directory": directory, "measure": True} if directory else {"measure": True}
            response, _ = execute_native_query(config, "", None, method_name="assets", is_list=True, parameters={"request_type": "assets","request_params": params})
    except Exception as e:
        log_error(f"Failed to get files: {str(e)}", e)
        
    finally:
        return response

def read_file(config: dict, file: dict, connection: any, connection_type: str):
    """
    Read the file content from the config
    """
    response = None
    try:
        if connection_type == ConnectionType.S3.value:
            bucket = file.get("bucket", "")
            file_path = file.get("file_path", "")
            response = connection.get_object(Bucket=bucket, Key=file_path)
        elif connection_type == ConnectionType.ADLS.value:
            container = file.get("container", "")
            file_path = file.get("file_path", "")
            container_client = connection.get_container_client(container)
            blob_client = container_client.get_blob_client(file_path)
            response = blob_client.download_blob()
        else:
            response, _ = execute_native_query(config, "", None, method_name="read_file", parameters={"request_type": "read_file","request_params":{**file}})
    except Exception as e:
        log_error(f"Failed to read file: {str(e)}", e)
    finally:
        return response

def delete_failed_rows(config: dict, measure: dict):
    """
    Delete failed rows from the database
    """
    measure_id = measure.get("id", "")
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            delete_query = f"""
                delete from core.file_measure_preview where measure_id = '{measure_id}'
            """
            cursor = execute_query(connection, cursor, delete_query)
    except Exception as e:
        log_error(f"Failed to delete failed rows: {str(e)}", e)


def save_failed_rows(config: dict, measure: dict, failed_rows: list):
    """
    Save Failed rows to the database
    """
    measure_id = measure.get("id", "")
    connection_id = config.get("connection_id")
    run_id = config.get("run_id", "")

    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            measure_results = []
            for failed_row in failed_rows:
                bucket_name = failed_row.get("bucket_name", "")
                file_path = failed_row.get("file_path", "")
                error = failed_row.get("error", "")
                query_input = (
                    str(uuid4()),
                    bucket_name,
                    file_path,
                    error,
                    connection_id,
                    measure_id,
                    run_id,
                )
                input_literals = ", ".join(["%s"] * len(query_input))
                query_param = cursor.mogrify(
                    f"({input_literals},CURRENT_TIMESTAMP)",
                    query_input,
                ).decode("utf-8")
                measure_results.append(query_param)

            measures_input = split_queries(measure_results)
            for input_values in measures_input:
                try:
                    input_value = ",".join(input_values)
                    preview_insert_query = f"""
                        insert into core.file_measure_preview (id, bucket_name, file_path, error, connection_id, 
                        measure_id, last_run_id, created_date)
                        values {input_value}
                    """
                    cursor = execute_query(
                        connection, cursor, preview_insert_query)
                except Exception as e:
                    log_error("Save Failed Rows: inserting new failed row", e)
            
    except Exception as e:
        log_error(f"Failed to save failed rows: {str(e)}", e)
        raise e

def get_measure_metadata(config: dict, measure_id: str):
    """
    Get File measure metadata from the database
    """
    metadata = []
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select * from core.file_measure_metadata where measure_id = '{measure_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            metadata = fetchall(cursor)
    except Exception as e:
        log_error(f"Failed to get measure metadata: {str(e)}", e)
        raise e
    finally:
        return metadata

def save_measure_metadata(config: dict, results: list):
    """
    Save measure metadata to the database
    """
    measure_id = config.get("measure_id", "")
    connection_id = config.get("connection_id")
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            delete_query = f"""
                delete from core.file_measure_metadata where measure_id = '{measure_id}'
            """
            cursor = execute_query(connection, cursor, delete_query)
            measure_results = []
            for result in results:
                query_input = (
                    str(uuid4()),
                    connection_id,
                    measure_id,
                    result.get("file_name") or result.get("name"),
                    result.get("bucket_name") or result.get("bucket"),
                    result.get("file_path"),
                    result.get("checksum"),
                    result.get("algorithm"),
                )
                input_literals = ", ".join(["%s"] * len(query_input))
                query_param = cursor.mogrify(
                    f"({input_literals},CURRENT_TIMESTAMP)",
                    query_input,
                ).decode("utf-8")
                measure_results.append(query_param)
                   
            measures_input = split_queries(measure_results)
            for input_values in measures_input:
                try:
                    input_value = ",".join(input_values)
                    measure_metadata_insert_query = f"""
                        insert into core.file_measure_metadata (id, connection_id, measure_id, file_name, bucket_name, file_path, 
                        checksum, algorithm, created_date)
                        values {input_value}
                    """
                    cursor = execute_query(
                        connection, cursor, measure_metadata_insert_query)
                except Exception as e:
                    log_error("Save Measure Metadata: inserting new measure metadata", e)
    except Exception as e:
        log_error(f"Failed to save measure metadata: {str(e)}", e)
        raise e


def format_duration(duration, omit_milliseconds=True):
    """
    Returns a short text representation of a duration in seconds.
    - duration: duration in seconds (can be float)
    - default_na: if True, returns "NA" for None/0 (unless show_zero)
    - omit_milliseconds: if True, omits ms unless duration < 1s
    - show_zero: if True, returns 0 if duration is 0
    """
    duration_text = ""
    if not duration:
        return duration_text

    duration_values = []
    total_seconds = float(duration)
    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, rem = divmod(rem, 60)
    seconds = int(rem)
    milliseconds = int(round((rem - seconds) * 1000))

    if int(days):
        duration_values.append(f"{int(days)} d")
    if int(hours):
        duration_values.append(f"{int(hours)} h")
    if int(minutes):
        duration_values.append(f"{int(minutes)} m")
    if seconds:
        duration_values.append(f"{seconds} s")
    if (total_seconds < 1 or not omit_milliseconds) and milliseconds > 0:
        duration_values.append(f"{milliseconds} ms")

    duration_text = " ".join(duration_values) if duration_values else "NA"
    return duration_text

class ADLSStreamWrapper:
    """
    Wrapper for Azure StorageStreamDownloader to make it compatible with TextIOWrapper
    and other file-like operations. Provides all methods and properties required by TextIOWrapper.
    """
    def __init__(self, stream):
        self.stream = stream
        self._closed = False
    
    def read(self, size=-1):
        """Read data from the stream."""
        if self._closed:
            raise ValueError("I/O operation on closed file")
        if size == -1:
            return self.stream.readall()
        return self.stream.read(size)
    
    def readable(self):
        """Return True as this is a readable stream."""
        return True
    
    def writable(self):
        """Return False as this is a read-only stream."""
        return False
    
    def seekable(self):
        """Return False as Azure streams are not seekable."""
        return False
    
    @property
    def closed(self):
        """Return True if the stream is closed."""
        return self._closed
    
    def close(self):
        """Close the stream if it has a close method."""
        if not self._closed:
            self._closed = True
            if hasattr(self.stream, 'close'):
                self.stream.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()