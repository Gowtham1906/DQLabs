import sys
import json
import platform
import multiprocessing
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


def calculate_spark_resources():
    """
    Calculate the default Spark master and driver memory based on system resources.

    Returns:
        tuple: (master, driver_memory)
          - master: e.g. "local[4]" based on available cores
          - driver_memory: e.g. "4g" based on half of system memory (at least 1g)
    """
    cores = multiprocessing.cpu_count()
    master = f"local[{cores}]"
    if psutil:
        total_memory = psutil.virtual_memory().total  # in bytes
        driver_memory_gb = max(1, int((total_memory / (1024 ** 3)) * 0.5))
        driver_memory = f"{driver_memory_gb}g"
    else:
        driver_memory = "2g"
    return master, driver_memory


def create_spark_session(spark_conf: dict) -> SparkSession:
    master, driver_memory = calculate_spark_resources()
    spark_builder = SparkSession.builder.appName("DQQueryDriver")

    # Production-level tuning defaults.
    default_conf = {
        # Tune based on your cluster size.
        "spark.sql.shuffle.partitions": "200",
        "spark.dynamicAllocation.enabled": "true",
        "spark.dynamicAllocation.minExecutors": "1",
        "spark.dynamicAllocation.maxExecutors": "10",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.kryoserializer.buffer.max": "64m"
    }
    # Set default properties if they are not already provided.
    for key, value in default_conf.items():
        if key not in spark_conf:
            spark_builder = spark_builder.config(key, value)

    # Use calculated master if not provided in config.
    if "spark.master" not in spark_conf:
        spark_builder = spark_builder.master(master)
        logger.info(f"Setting spark.master to {master}")

    # Set driver memory if not provided.
    if "spark.driver.memory" not in spark_conf:
        spark_builder = spark_builder.config(
            "spark.driver.memory", driver_memory)
        logger.info(f"Setting spark.driver.memory to {driver_memory}")

    # Apply user provided configuration.
    for key, value in spark_conf.items():
        spark_builder = spark_builder.config(key, value)

    spark = spark_builder.getOrCreate()
    set_aws_properties(spark, spark_conf)

    logger.info(
        "Spark session created successfully with the following configurations:")
    for conf_item in spark.sparkContext.getConf().getAll():
        logger.info(conf_item)

    return spark


def set_aws_properties(spark: SparkSession, spark_conf: dict):
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


def run_query(spark: SparkSession, tables: list):
    schema = []
    for table in tables:
        try:
            query_string = f"DESCRIBE {table}"
            columns = spark.sql(query_string).collect()
            column_names = [{"column_name": row.col_name, "datatype": row.data_type} for row in columns if row.col_name and not row.col_name.startswith('#')]
            schema.extend(column_names)
        except Exception as e:
            logger.info(f"Get Schema Error to {table}")
    return schema


def main(argv):
    try:
        input_config = parse_arguments(argv)
        input_spark_conf = input_config.get("spark_conf", {})
        job_config = input_config.get("job_config", {})
        tables = job_config.get("tables", [])

        spark = create_spark_session(input_spark_conf)
        result = run_query(spark, tables)

        print("<-dq-result-> ",
              json.dumps({"response": {"status": "success", "data": result}}, default=str))

    except Exception as e:
        logger.exception("An error occurred:")
        print("<-dq-result-> ",
              json.dumps({"response": {"status": "failure", "message": str(e)}}, default=str))
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv)
