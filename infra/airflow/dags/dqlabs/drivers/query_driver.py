import sys
import json
import multiprocessing
import time
try:
    import psutil
except ImportError:
    psutil = None
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format
import logging

# Configure logging for production level observability.
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DQQueryDriver")


def parse_arguments(argv):
    args = argv[1:]
    input_config = args[0]
    input_config = (
        json.loads(input_config)
        if input_config and isinstance(input_config, str)
        else input_config
    )
    input_config = input_config if input_config else {}
    logger.info(f"Input Configuration: {input_config}")
    return input_config


def calculate_spark_resources(min_driver_gb=2, max_driver_gb=16):
    """
    Calculate Spark master and driver memory for local mode.
    In production, these should be set via config/environment.
    """
    cores = max(1, multiprocessing.cpu_count())
    master = f"local[{cores}]"
    if psutil:
        total_memory_gb = int(psutil.virtual_memory().total / (1024 ** 3))
        driver_memory_gb = max(min_driver_gb, min(max_driver_gb, int(total_memory_gb * 0.5)))
        driver_memory = f"{driver_memory_gb}g"
    else:
        driver_memory = f"{min_driver_gb}g"
    logger.info(f"Calculated Spark master: {master}, driver memory: {driver_memory}")
    return master, driver_memory


def set_aws_properties(spark: SparkSession, spark_conf: dict):
    """Set AWS properties as JVM system properties for Spark session"""
    jvm = spark._jvm
    aws_settings = {
        "aws.region": "aws.region",
        "aws.accessKeyId": "aws.accessKeyId",
        "aws.secretAccessKey": "aws.secretAccessKey",
    }
    for prop, key in aws_settings.items():
        value = spark_conf.get(key)
        if value:
            jvm.java.lang.System.setProperty(prop, value)
            logger.info(f"Set JVM property {prop}")


def create_spark_session(spark_conf: dict) -> SparkSession:
    # In production, set spark.master and spark.driver.memory via config or spark-submit
    master, driver_memory = calculate_spark_resources()
    spark_builder = SparkSession.builder.appName("DQQueryDriver")

    # Iceberg & performance tuning defaults.
    default_conf = {
        "spark.sql.shuffle.partitions": "64",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.kryoserializer.buffer.max": "64m",
        "spark.sql.parquet.enableVectorizedReader": "true",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
    }
    # Set default properties if not already provided.
    for key, value in default_conf.items():
        if key not in spark_conf:
            spark_builder = spark_builder.config(key, value)

    # Use calculated master if not provided in config (for local/dev only)
    if "spark.master" not in spark_conf:
        spark_builder = spark_builder.master(master)
        logger.info(f"Setting spark.master to {master}")

    # Set driver memory if not provided.
    if "spark.driver.memory" not in spark_conf:
        spark_builder = spark_builder.config("spark.driver.memory", driver_memory)
        logger.info(f"Setting spark.driver.memory to {driver_memory}")

    # Apply user provided configuration.
    for key, value in spark_conf.items():
        spark_builder = spark_builder.config(key, value)

    spark = spark_builder.getOrCreate()
    set_aws_properties(spark, spark_conf)

    logger.info("Spark session created successfully with the following configurations:")
    for conf_item in spark.sparkContext.getConf().getAll():
        logger.info(conf_item)

    return spark


def run_query(spark: SparkSession, query_string: str):
    logger.info(f"Executing query: {query_string}")
    df = spark.sql(query_string)

    # Check if this is a DESCRIBE TABLE query
    is_describe_query = query_string.strip().upper().startswith("DESCRIBE TABLE")
    logger.info(f"Is DESCRIBE TABLE query: {is_describe_query}")
    
    # For DESCRIBE TABLE queries, filter out rows with specific col_name values
    if is_describe_query:
        # Filter out rows where col_name is upload_timestamp or prev_upload_timestamp
        df = df.filter(~col("col_name").isin(["upload_timestamp", "prev_upload_timestamp"]))
        logger.info("Filtered out upload_timestamp and prev_upload_timestamp rows from DESCRIBE TABLE result")
    
    # Format timestamp/date columns for consistent output
    new_cols = []
    for field in df.schema.fields:
        f_name = field.name
        dt_string = field.dataType.simpleString()
        
        if dt_string == "timestamp":
            new_cols.append(date_format(col(f_name), "yyyy-MM-dd HH:mm:ss").alias(f_name))
        elif dt_string == "date":
            new_cols.append(date_format(col(f_name), "yyyy-MM-dd").alias(f_name))
        else:
            new_cols.append(col(f_name))
    
    # Only apply column filtering if we have columns to select
    if new_cols:
        df = df.select(*new_cols)
    
    return df


def main(argv):
    spark = None
    start_time = time.time()
    try:
        input_config = parse_arguments(argv)
        input_spark_conf = input_config.get("spark_conf", {})
        job_config = input_config.get("job_config", {})
        query_string = job_config.get("query_string", "")
        is_list = job_config.get("is_list", False)
        is_count_only = job_config.get("is_count_only", False)

        if not query_string:
            raise ValueError("Query string is not provided in configuration.")

        spark = create_spark_session(input_spark_conf)
        df = run_query(spark, query_string)
         
        if is_count_only:
            result = df.count()
        else:
            result = df.collect()
            result = [row.asDict() for row in result]
            result = result if is_list else (result[0] if result else {})
        print("<-dq-result-> ", json.dumps({"response": {"status": "success", "data": result}}, default=str))

    except Exception as e:
        logger.exception("An error occurred:")
        print("<-dq-result-> ", json.dumps({"response": {"status": "failure", "message": str(e)}}, default=str))
        sys.exit(1)
    finally:
        end_time = time.time()
        total_time = end_time - start_time
        logger.info(f"Total batch execution time: {total_time:.2f} seconds")
        if spark:
            spark.stop()


if __name__ == "__main__":
    main(sys.argv)