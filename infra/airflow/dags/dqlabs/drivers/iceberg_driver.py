import json
import sys
import re
import multiprocessing
import logging
try:
    import psutil
except ImportError:
    psutil = None
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Setup production-level logging.
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(name)s: %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("DQIcebergDriver")

def parse_arguments(argv):
    args = argv[1:]
    input_config = args[0]
    input_config = (
        json.loads(input_config)
        if input_config and isinstance(input_config, str)
        else input_config
    )
    input_config = input_config if input_config else {}
    logger.info("Input Configuration: %s", input_config)
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
    spark_builder = SparkSession.builder.appName("DQAssetDriver")

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
    # Set default properties if not already provided.
    for key, value in default_conf.items():
        if key not in spark_conf:
            spark_builder = spark_builder.config(key, value)

    # Use calculated master if not provided in config.
    if "spark.master" not in spark_conf:
        spark_builder = spark_builder.master(master)
        logger.info("Setting spark.master to %s", master)

    # Set driver memory if not provided.
    if "spark.driver.memory" not in spark_conf:
        spark_builder = spark_builder.config(
            "spark.driver.memory", driver_memory)
        logger.info("Setting spark.driver.memory to %s", driver_memory)

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
    try:
        jvm = spark._jvm
        aws_settings = {
            "aws.region": "aws.region",
            "aws.accessKeyId": "aws.accessKeyId",
            "aws.secretAccessKey": "aws.secretAccessKey",
        }
        for prop, key in aws_settings.items():
            value = spark_conf.get(key)
            if value:
                # Use Java System.setProperty through JVM
                jvm.java.lang.System.setProperty(prop, value)
                logger.info("Set AWS JVM property %s", prop)
    except Exception as e:
        logger.warning("Failed to set AWS properties: %s", str(e))



def get_iceberg_table_location(spark, qualified_table_name):
    """
    Get the physical location of an Iceberg table.
    
    Args:
        spark: SparkSession
        qualified_table_name: Full table name (catalog.namespace.table)
        
    Returns:
        Physical location path of the table
    """
    try:
        result = spark.sql(f"DESCRIBE EXTENDED {qualified_table_name}").collect()
        for row in result:
            if row.col_name and "Location" in row.col_name:
                return row.data_type
        return None
    except Exception as e:
        logger.warning("Could not get table location: %s", str(e))
        return None

def execute_connection_query(spark, job_config: dict, query: str):
    try:
        jdbc_options = job_config.get("jdbc_options", {})
        if "dbtable" in jdbc_options:
            del jdbc_options["dbtable"]
        jdbc_options.update({"query": query, "fetchsize": 25000})
        response = spark.read.format("jdbc").options(**jdbc_options).load()
        return response
    except Exception as e:
        logger.error(f"Failed to execute connection query: {str(e)}")
        return None

def create_catalog_schema(spark, catalog_name: str, schema_name: str):
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{schema_name}")
        logger.info("Created namespace: %s", f"{catalog_name}.{schema_name}")
    except Exception as e:
        logger.error("Failed to create catalog and schema: %s", str(e))


def write_to_iceberg(spark, df, job_config):
    iceberg_table = job_config.get("external_credentials", {})
    catalog = iceberg_table.get("iceberg_catalog", "")
    namespace = iceberg_table.get("iceberg_schema", "")
    table_name = iceberg_table.get("table_name", "")
    dq_temp_table = f"dq_temp_{table_name}"
    
    # Ensure we have a valid table name
    if not table_name:
        raise ValueError("table_name is required in iceberg_table configuration")
    
    qualified_table_name = f"{catalog}.{namespace}.{table_name}".lower()
    

    df.createOrReplaceTempView(dq_temp_table)

    if not spark.catalog.tableExists(qualified_table_name):
        # Create new table - DataFrame already has both timestamp columns
        create_sql = f"""
            CREATE TABLE {qualified_table_name}
            USING iceberg
            TBLPROPERTIES ('format-version'='2')
            AS SELECT * FROM {dq_temp_table}
        """
        
        spark.sql(create_sql)
        logger.info("✅ Created Iceberg table: %s", qualified_table_name)
        
        # Get and log the actual table location
        table_location = get_iceberg_table_location(spark, qualified_table_name)
    else:
        # For existing tables, use INSERT OVERWRITE instead of DataFrame write
        logger.info("♻️ Table %s exists. Overwriting data...", qualified_table_name)
        try:
            # First try to truncate and insert
            spark.sql(f"TRUNCATE TABLE {qualified_table_name}")
            spark.sql(f"INSERT INTO {qualified_table_name} SELECT * FROM {dq_temp_table}")
            logger.info("✅ Successfully overwrote data using TRUNCATE + INSERT")
            
            # Get and log the actual table location
            table_location = get_iceberg_table_location(spark, qualified_table_name)
            if table_location:
                logger.info("📁 Table files stored at: %s", table_location)
        except Exception as e:
            logger.warning("TRUNCATE failed, trying DROP and CREATE: %s", str(e))
            try:
                # If truncate fails, drop and recreate the table
                spark.sql(f"DROP TABLE IF EXISTS {qualified_table_name}")
                
                create_sql = f"""
                    CREATE TABLE {qualified_table_name}
                    USING iceberg
                    TBLPROPERTIES ('format-version'='2')
                    AS SELECT * FROM {dq_temp_table}
                """
                
                spark.sql(create_sql)
                logger.info("✅ Successfully recreated table: %s", qualified_table_name)
            except Exception as drop_error:
                logger.error("Failed to recreate table: %s", str(drop_error))
                raise

def main(argv):
    spark = None
    try:
        # Parse input configuration
        input_config = parse_arguments(argv)

        input_spark_conf = input_config.get("spark_conf", {})
        job_config = input_config.get("job_config", {})
        query = job_config.get("query", "")
        is_table_exists = job_config.get("is_table_exists", False)
        external_credentials = job_config.get("external_credentials", {})
        catalog_name = external_credentials.get("iceberg_catalog", "")
        schema_name = external_credentials.get("iceberg_schema", "")
        table_name = external_credentials.get("table_name", "")

        # Initialize Spark session
        spark = create_spark_session(input_spark_conf)

        create_catalog_schema(spark, catalog_name, schema_name)

        if is_table_exists:
            is_table_exists = spark.catalog.tableExists(f"{catalog_name}.{schema_name}.{table_name}")
            if is_table_exists:
                logger.info("Table %s already exists. Skipping creation.", table_name)
                return {"response": {"status": "success"}}

        # Execute connection query
        df = execute_connection_query(spark, job_config, query)
        
        logger.info("Preview data:")
        df.show(truncate=False)
        logger.info("Columns: %s", df.columns)

        # Normalize column names to lowercase
        if df.columns:
            df = df.toDF(*[c for c in df.columns])

        # Write DataFrame to Iceberg table
        write_to_iceberg(spark, df, job_config)

        response = {"response": {"status": "success"}}
        print("<-dq-result->", json.dumps(response))

    except Exception as e:
        logger.exception("Error: %s", str(e))
        response = {"response": {"status": "failure", "message": str(e)}}
        print("<-dq-result->", json.dumps(response))
        raise

    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main(sys.argv)
