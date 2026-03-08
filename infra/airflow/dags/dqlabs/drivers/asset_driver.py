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
from pyspark.sql.functions import (col, to_timestamp, to_date,
                                   coalesce, date_sub, current_date,
                                   expr, to_utc_timestamp, current_timestamp,
                                   when, trim, isnull)
from py4j.java_gateway import java_import

# Setup production-level logging.
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(name)s: %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("DQAssetDriver")

# Map from simple type names to PySpark SQL types
TYPE_MAP = {
    "string": StringType(),
    "text": StringType(),
    "integer": IntegerType(),
    "int": IntegerType(),
    "bigint": LongType(),
    "long": LongType(),
    "float": FloatType(),
    "numeric": DoubleType(),
    "double": DoubleType(),
    "bool": BooleanType(),
    "boolean": BooleanType(),
    "binary": StringType(),
    "date": DateType(),
    "datetime": TimestampType(),
    "timestamp": TimestampType(),
    "time": StringType()
}

# Known date and timestamp formats
TIMESTAMP_FORMATS = [
    "yyyy-MM-dd HH:mm:ss.SSSSSS",
    "yyyy-MM-dd HH:mm:ss.SSS",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy/MM/dd HH:mm:ss",
    "MM-dd-yyyy HH:mm:ss",
    "dd-MM-yyyy HH:mm:ss",
    "MM/dd/yyyy HH:mm:ss",
    "dd/MM/yyyy HH:mm:ss"
]
DATE_FORMATS = [
    "yyyy-MM-dd",
    "yyyy/MM/dd",
    "MM-dd-yyyy",
    "dd-MM-yyyy",
    "MM/dd/yyyy",
    "dd/MM/yyyy"
]


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


def build_schema(attributes):
    default_type = StringType()
    schema = StructType([
        StructField(
            attr["name"],
            StringType() if attr["datatype"].lower() in ["timestamp", "datetime", "date"]
            else TYPE_MAP.get(attr["datatype"].lower(), default_type),
            True
        ) for attr in (attributes if attributes else [])
    ])
    return schema


def load_data(spark, file_config, job_config, schema):
    file_type = file_config.get("file_type", "")
    file_pattern = file_config.get("file_pattern", "")
    file_location = file_config.get("file_location", "")
    file_location = windows_to_wsl_path(file_location)
    options = file_config.get("options", {})

    logger.info("Loading data from: %s", file_location)

    # For CSV files, read as strings first (like upload code with dtype=str), then cast to proper types
    # This prevents empty values from causing entire columns to appear empty
    if file_type.lower() == "csv":
        attributes = job_config.get("attributes", [])
        # Create string schema to read all columns as strings first
        string_schema = StructType([
            StructField(attr["name"], StringType(), True)
            for attr in attributes
        ])
        
        # Set CSV options
        csv_options = options.copy()
        csv_options.setdefault("mode", "PERMISSIVE")
        if "inferSchema" in csv_options:
            csv_options.pop("inferSchema")
        
        # Read CSV files as strings
        if file_pattern:
            compiled_pattern = re.compile(file_pattern)
            jvm = spark._jvm
            java_import(jvm, 'org.apache.hadoop.fs.Path')
            java_import(jvm, 'org.apache.hadoop.fs.FileSystem')
            hadoop_conf = spark._jsc.hadoopConfiguration()
            fs = jvm.org.apache.hadoop.fs.FileSystem.get(
                jvm.java.net.URI(file_location), hadoop_conf)
            
            base_path = jvm.Path(file_location)
            iterator = fs.listFiles(base_path, True)
            matching_files = []
            
            while iterator.hasNext():
                file = iterator.next()
                file_path = jvm.Path(str(file.getPath().toString()))
                relative_path = str(file_path.toString()).replace(
                    str(base_path.toString()), "").lstrip('/')
                
                if compiled_pattern.match(relative_path):
                    matching_files.append(str(file.getPath().toString()))
            
            df = spark.read.schema(string_schema).format(file_type).options(**csv_options).load(matching_files)
        else:
            df = spark.read.schema(string_schema).format(file_type).options(**csv_options).load(file_location)
        
        # Cast columns to proper types, converting empty strings to null
        for attr in attributes:
            col_name = attr["name"]
            dtype = attr.get("datatype", "").lower()
            
            if col_name not in df.columns:
                continue
            
            # Convert empty/whitespace strings to null before casting
            empty_to_null = when((trim(col(col_name)) == "") | isnull(col(col_name)), None).otherwise(col(col_name))
            
            if dtype in ["string", "text"]:
                df = df.withColumn(col_name, trim(col(col_name)))
            elif dtype in ["date", "timestamp", "datetime"]:
                # Keep as string for apply_dynamic_parsing to handle later
                df = df.withColumn(col_name, empty_to_null)
            elif dtype in ["integer", "int"]:
                df = df.withColumn(col_name, empty_to_null.cast(IntegerType()))
            elif dtype in ["bigint", "long"]:
                df = df.withColumn(col_name, empty_to_null.cast(LongType()))
            elif dtype == "float":
                df = df.withColumn(col_name, empty_to_null.cast(FloatType()))
            elif dtype in ["numeric", "double"]:
                df = df.withColumn(col_name, empty_to_null.cast(DoubleType()))
            elif dtype in ["bool", "boolean"]:
                df = df.withColumn(col_name, empty_to_null.cast(BooleanType()))
            else:
                df = df.withColumn(col_name, empty_to_null)
        
        return df


    if file_pattern:
        compiled_pattern = re.compile(file_pattern)
        jvm = spark._jvm
        java_import(jvm, 'org.apache.hadoop.fs.Path')
        java_import(jvm, 'org.apache.hadoop.fs.FileSystem')
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(
            jvm.java.net.URI(file_location), hadoop_conf)
        
        base_path = jvm.Path(file_location)
        iterator = fs.listFiles(base_path, True)
        matching_files = []
        
        while iterator.hasNext():
            file = iterator.next()
            file_path = jvm.Path(str(file.getPath().toString()))
            
            # Get the relative path from base directory
            relative_path = str(file_path.toString()).replace(
                str(base_path.toString()), "").lstrip('/')
            
            logger.info("Checking relative path: %s", relative_path)
            
            if compiled_pattern.match(relative_path):
                matching_files.append(str(file.getPath().toString()))
        
        logger.info("matching_files: %s", matching_files)
        if file_type == "xml":
            row_tag = options.get("rowTag", "row")
            df = spark.read.format("xml") \
                .options(**options) \
                .option("rowTag", row_tag) \
                .schema(schema) \
                .load(",".join(matching_files))
        else:
            df = spark.read.schema(schema).format(
                file_type).options(**options).load(matching_files)
    else:
        df = spark.read.schema(schema).format(
            file_type).options(**options).load(file_location)
    return df


def apply_dynamic_parsing(df, attributes):
    for attr in attributes:
        col_name = attr["name"]
        dtype = attr["datatype"].lower()
        if dtype in ["timestamp", "datetime"]:
            parsed_exprs = [to_timestamp(col(col_name), fmt)
                            for fmt in TIMESTAMP_FORMATS]
            df = df.withColumn(col_name, coalesce(*parsed_exprs))
        elif dtype == "date":
            parsed_exprs = [to_date(col(col_name), fmt)
                            for fmt in DATE_FORMATS]
            df = df.withColumn(col_name, coalesce(*parsed_exprs))
    return df


def apply_incremental_filtering(df, incremental_config, attributes):
    depth_config = incremental_config.get("depth", {})
    depth = int(depth_config.get("value", 1))
    watermark_key = incremental_config.get("watermark")

    if watermark_key:
        col_map = {c.lower(): c for c in df.columns}
        watermark_col = col_map.get(watermark_key.lower())
        if watermark_col:
            watermark_dtype = None
            for attr in attributes:
                if attr["name"].lower() == watermark_key.lower():
                    watermark_dtype = attr["datatype"].lower()
                    break

            if watermark_dtype == "date":
                parsed_exprs = [to_date(col(watermark_col), fmt)
                                for fmt in DATE_FORMATS]
                df = df.withColumn("parsed_watermark", coalesce(*parsed_exprs))
                df = df.filter(col("parsed_watermark") >=
                               date_sub(current_date(), depth))
            elif watermark_dtype in ("timestamp", "datetime"):
                parsed_exprs = [to_utc_timestamp(to_timestamp(col(watermark_col), fmt), "UTC")
                                for fmt in TIMESTAMP_FORMATS]
                df = df.withColumn("parsed_watermark", coalesce(*parsed_exprs))
                df = df.filter(col("parsed_watermark") >= expr(
                    f"to_utc_timestamp(current_timestamp(), 'UTC') - INTERVAL {depth} DAYS"))
            else:
                raise Exception(
                    f"Unsupported watermark type '{watermark_dtype}' for column '{watermark_key}'")
    return df


def add_timestamp_column(df):
    """
    Add a timestamp column to the DataFrame with the current timestamp.
    This column can be used later to calculate freshness.
    
    Args:
        df: Spark DataFrame
        
    Returns:
        DataFrame with timestamp column added
    """
    # Check if upload_timestamp column already exists
    if "upload_timestamp" not in df.columns:
        df = df.withColumn("upload_timestamp", current_timestamp())
        logger.info("Added timestamp column 'upload_timestamp' with current timestamp")
    else:
        logger.info("Timestamp column 'upload_timestamp' already exists")
    
    return df


def get_previous_max_timestamp(spark, qualified_table_name):
    """
    Get the maximum upload_timestamp from the existing table.
    
    Args:
        spark: SparkSession
        qualified_table_name: Full table name
        
    Returns:
        Previous max timestamp or None if table doesn't exist or no data
    """
    try:
        if spark.catalog.tableExists(qualified_table_name):
            # Check if upload_timestamp column exists
            columns = spark.sql(f"DESCRIBE TABLE {qualified_table_name}").collect()
            column_names = [row.col_name for row in columns if row.col_name is not None]
            
            if "upload_timestamp" in column_names:
                result = spark.sql(f"SELECT MAX(upload_timestamp) as max_timestamp FROM {qualified_table_name}").collect()
                if result and result[0].max_timestamp is not None:
                    prev_max_timestamp = result[0].max_timestamp
                    logger.info(f"Found previous max timestamp: {prev_max_timestamp}")
                    return prev_max_timestamp
                else:
                    logger.info("No previous upload_timestamp found in table")
                    return None
            else:
                logger.info("upload_timestamp column doesn't exist in table")
                return None
        else:
            logger.info("Table doesn't exist, no previous timestamp")
            return None
    except Exception as e:
        logger.warning(f"Error getting previous max timestamp: {str(e)}")
        return None


def add_prev_upload_timestamp_column(df, prev_max_timestamp):
    """
    Add prev_upload_timestamp column to DataFrame.
    
    Args:
        df: Spark DataFrame
        prev_max_timestamp: Previous max timestamp value
        
    Returns:
        DataFrame with prev_upload_timestamp column added
    """
    if prev_max_timestamp is not None:
        df = df.withColumn("prev_upload_timestamp", expr(f"CAST('{prev_max_timestamp}' AS TIMESTAMP)"))
        logger.info("Added prev_upload_timestamp column with previous max timestamp")
    else:
        df = df.withColumn("prev_upload_timestamp", expr("CAST(NULL AS TIMESTAMP)"))
        logger.info("Added prev_upload_timestamp column with NULL value")
    
    return df


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


def write_to_iceberg(spark, df, job_config):
    iceberg_table = job_config.get("iceberg_table", {})
    catalog = iceberg_table.get("catalog", "")
    namespace = iceberg_table.get("namespace", "")
    table_name = iceberg_table.get("table_name", "")
    dq_temp_table = f"dq_temp_{table_name}"
    
    # Ensure we have a valid table name
    if not table_name:
        raise ValueError("table_name is required in iceberg_table configuration")
    
    qualified_table_name = f"{catalog}.{namespace}.{table_name}".lower()
    
    # Get previous max timestamp before any operations
    prev_max_timestamp = get_previous_max_timestamp(spark, qualified_table_name)
    
    # Add prev_upload_timestamp column to DataFrame
    df = add_prev_upload_timestamp_column(df, prev_max_timestamp)
    
    df.createOrReplaceTempView(dq_temp_table)

    namespace_name = f"{catalog}.{namespace}"
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace_name}")
        logger.info("Created namespace: %s", namespace_name)
    except Exception as e:
        logger.warning("Namespace creation failed (may already exist): %s", str(e))

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
        if table_location:
            logger.info("📁 Table files stored at: %s", table_location)
        else:
            logger.info("📁 Table files stored at: %s/%s/%s", warehouse_path, namespace, table_name)
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


def windows_to_wsl_path(path):
    """
    Convert a Windows path (e.g. "C:\\Users\\user\\file.txt" or file://f:/tmp/raj_file.csv)
    to a WSL path (e.g. "file:///mnt/c/Users/user/file.txt").
    Preserves the "file://" prefix if present.
    If the path is not a Windows-style path, returns the original path.
    Handles both backslash and forward-slash Windows drive letter paths.
    """
    if not path:
        return path

    prefix = ""
    original = path

    # Preserve file:// prefix if present.
    if path.startswith("file://"):
        prefix = "file://"
        path = path[len("file://"):]

    # If already a WSL path, return as is.
    if path.startswith("/mnt/"):
        return prefix + path

    # Replace backslashes with forward slashes for uniformity.
    fixed = path.replace("\\", "/")

    # Match drive letter paths like "f:/tmp/raj_file.csv" or "F:/tmp/raj_file.csv".
    match = re.match(r"^([a-zA-Z]):/(.*)", fixed)
    if match:
        drive = match.group(1).lower()
        rest = match.group(2)
        return prefix + f"/mnt/{drive}/{rest}"

    # If the path does not appear to be Windows (no drive letter), return original.
    return original


def main(argv):
    spark = None
    try:
        # Parse input configuration
        input_config = parse_arguments(argv)
        logger.info("Input Configuration: %s", input_config)
        input_spark_conf = input_config.get("spark_conf", {})
        job_config = input_config.get("job_config", {})
        file_config = job_config.get("file_config", {})
        attributes = job_config.get("attributes", [])
        incremental_config = job_config.get("incremental_config", {})

        # Initialize Spark session
        spark = create_spark_session(input_spark_conf)

        # Build and log schema
        schema = build_schema(attributes)
        logger.info("Configured Schema: %s", schema)

        # Load data
        df = load_data(spark, file_config, job_config, schema)

        # Apply dynamic parsing for dates and timestamps
        df = apply_dynamic_parsing(df, attributes)

        # Apply incremental filtering if configured
        if incremental_config:
            df = apply_incremental_filtering(
                df, incremental_config, attributes)

        # Add timestamp column for freshness calculation
        df = add_timestamp_column(df)

        logger.info("Preview data:")
        df.show(truncate=False)
        logger.info("Columns: %s", df.columns)

        # Normalize column names to lowercase
        if df.columns:
            df = df.toDF(*[c.lower() for c in df.columns])

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
