from pyspark.sql import SparkSession, DataFrame
import os
import sys
import requests
import logging
import json
from copy import deepcopy
from typing import Union, List, Dict, Any, Optional
from pyspark.sql.functions import to_timestamp, when, col, isnan, isnull, lit, broadcast
from pyspark.sql.types import (TimestampType, IntegerType, DecimalType, BooleanType, StringType, LongType, FloatType, DoubleType,
DateType, ShortType, BinaryType)
from py4j.java_gateway import java_import
from datetime import datetime

# Configure logging for production-level logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
logger = logging.getLogger(__name__)

queries = {
    "create_connection_metadata": """
        CREATE TABLE IF NOT EXISTS <catalog>.<schema>.connection_metadata (
            CONNECTION_ID STRING,
            CONNECTION_NAME STRING,
            DATASOURCE STRING,
            STATUS STRING,
            RUN_ID STRING,
            CREATED_DATE TIMESTAMP
        )
        """,
    "create_asset_metadata": """
            CREATE TABLE IF NOT EXISTS <catalog>.<schema>.asset_metadata (
                CONNECTION_ID STRING,
                DATABASE_NAME STRING,
                SCHEMA_NAME STRING,
                ASSET_ID STRING,
                TABLE_NAME STRING,
                ALIAS_NAME STRING,
                DQ_SCORE DECIMAL(18,6),
                TOTAL_RECORDS BIGINT,
                PASSED_RECORDS BIGINT,
                FAILED_RECORDS BIGINT,
                ISSUE_COUNT BIGINT,
                ALERT_COUNT BIGINT,
                RUN_ID STRING,
                STATUS STRING,
                IDENTIFIER_KEY STRING,
                CONVERSATIONS STRING,
                DESCRIPTION STRING,
                STEWARD_USER_ID STRING,
                STEWARD_USERS STRING,
                DOMAIN STRING,
                PRODUCT STRING,
                LAST_RUN_TIME TIMESTAMP,
                APPLICATIONS STRING,
                TERMS STRING,
                CREATED_DATE TIMESTAMP
        )
    """,
    "create_attribute_metadata": """
        CREATE TABLE IF NOT EXISTS <catalog>.<schema>.attribute_metadata (
            ASSET_ID STRING,
            ATTRIBUTE_ID STRING,
            ATTRIBUTE_NAME STRING,
            DQ_SCORE DECIMAL(18,6),
            TOTAL_RECORDS BIGINT,
            PASSED_RECORDS BIGINT,
            FAILED_RECORDS BIGINT,
            ISSUE_COUNT BIGINT,
            ALERT_COUNT BIGINT,
            STATUS STRING,
            RUN_ID STRING,
            DESCRIPTION STRING,
            TERM STRING,
            MIN_LENGTH STRING,
            MAX_LENGTH STRING,
            MIN_VALUE STRING,
            MAX_VALUE STRING,
            DATATYPE STRING,
            IS_PRIMARY BOOLEAN,
            TAGS STRING,
            LAST_RUN_TIME TIMESTAMP,
            JOB_STATUS STRING,
            CREATED_DATE TIMESTAMP
        )
    """,
    "create_measure_metadata": """
        CREATE TABLE IF NOT EXISTS <catalog>.<schema>.measure_metadata (
            CONNECTION_ID STRING,
            ASSET_ID STRING,
            ATTRIBUTE_ID STRING,
            MEASURE_ID STRING,
            MEASURE_NAME STRING,
            MEASURE_DIMENSION STRING,
            MEASURE_WEIGHTAGE DECIMAL(18,6),
            MEASURE_TYPE STRING,
            MEASURE_THRESHOLD STRING,
            MEASURE_QUERY STRING,
            TOTAL_RECORDS_SCOPE BOOLEAN,
            TOTAL_RECORDS_SCOPE_QUERY STRING,
            DQ_SCORE DECIMAL(18,6),
            TOTAL_RECORDS BIGINT,
            PASSED_RECORDS BIGINT,
            FAILED_RECORDS BIGINT,
            STATUS STRING,
            RUN_ID STRING,
            DOMAIN STRING,
            PRODUCT STRING,
            APPLICATION STRING,
            RUN_STATUS STRING,
            RUN_DATE TIMESTAMP,
            MEASURE_STATUS STRING,
            IS_ACTIVE BOOLEAN,
            PROFILE STRING,
            COMMENT STRING,
            CREATED_DATE TIMESTAMP
        )
    """,
    "create_user_metadata": """
        CREATE TABLE IF NOT EXISTS <catalog>.<schema>.user_session (
            USER_ID STRING,
            LAST_LOGGED_IN TIMESTAMP,
            USERNAME STRING,
            USERROLE STRING,
            USERMAILID STRING,
            TOTAL_LOGIN_COUNT BIGINT,
            AVG_SESSION_TIME STRING,
            MIN_SESSION_TIME STRING,
            MAX_SESSION_TIME STRING,
            AUDITS_COUNT BIGINT
        )
    """,
    "create_user_activity": """
        CREATE TABLE IF NOT EXISTS <catalog>.<schema>.user_activity (
            USER_ID STRING,
            USERNAME STRING,
            USERROLE STRING,
            ATTRIBUTE_ID STRING,
            PAGE STRING,
            MODULE STRING,
            SUBMODULE STRING,
            ASSETS STRING,
            ATTRIBUTE STRING,
            NOTIFICATION_TEXT STRING,
            CREATED_DATE TIMESTAMP
        )
    """,
    "failed_rows": {
        "drop": """DROP TABLE IF EXISTS <catalog>.<schema>.<table_name>""",
        "delete": "DELETE FROM <catalog>.<schema>.<table_name> WHERE RUN_ID = '<run_id>' and MEASURE_ID IN <measure_id>",
        "delete_summarized": "DELETE FROM <catalog>.<schema>.<table_name> WHERE RUN_ID = '<run_id>'",
        "create": """
            CREATE TABLE IF NOT EXISTS <catalog>.<schema>.<failed_rows_table> (
                CONNECTION_ID STRING,
                CONNECTION_NAME STRING,
                ASSET_ID STRING,
                ASSET_NAME STRING,
                ATTRIBUTE_ID STRING,
                ATTRIBUTE_NAME STRING,
                MEASURE_ID STRING,
                MEASURE_NAME STRING,
                RUN_ID STRING,
                MEASURE_CONDITION STRING,
                DOMAINS STRING,
                PRODUCTS STRING,
                APPLICATIONS STRING,
                TERMS STRING,
                TAGS STRING,
                IDENTIFIER_KEY STRING,
                EXPORTROW_ID STRING,
                MEASURE_DATA STRING,
                FAILED_ROW_DATA STRING,
                IS_SUMMARISED BOOLEAN,
                CREATED_DATE TIMESTAMP
            )
        """
    },
    "delete_existing_run": """
        DELETE FROM <catalog>.<schema>.<table_name> where run_id = '<run_id>'
    """
}


class ExportMetrics:
    def __init__(self, config):
        logger.info("Initializing ExportMetrics class...")
        logger.info(config)
        self.spark_conf = config.get("spark_conf", {})
        self.job_config = config.get("job_config", {})
        self.external_credentials = self.job_config.get('external_credentials', {})
        self.spark = self.__init_spark()
        self.catalog_name = self.external_credentials.get("iceberg_catalog")
        self.schema_name = self.external_credentials.get("iceberg_schema")
        self.input_metadata = self.get_failed_rows_metadata()
        self.connection_type = self.job_config.get("connection_type", "").lower()


    def __init_spark(self):
        # Initialize the Spark session with provided configuration.
        logger.info("Spark Conf", self.spark_conf)
        logger.info("Initializing Spark session...")
        try:
            spark_builder = SparkSession.builder.appName("ExportMetrics")
            for k, v in self.spark_conf.items():
                spark_builder = spark_builder.config(k, v)
            if "spark.local.dir" not in self.spark_conf:
                # Set temporary directory to a location with adequate disk space
                spark_builder = spark_builder.config("spark.local.dir", "/tmp")
            
            spark = spark_builder.getOrCreate()
            self.set_aws_properties(spark, self.spark_conf)
            logger.info(
                "Spark session created successfully with the following configurations:")
            for conf_item in spark.sparkContext.getConf().getAll():
                logger.info(conf_item)
            return spark
        except Exception as e:
            logger.error("Spark initialization failed: %s", str(e))
            raise

    def set_aws_properties(self, spark: SparkSession, spark_conf: dict) -> None:
        try:
            jvm = spark._jvm  # type: ignore
            aws_settings = {
                "aws.region": "aws.region",
                "aws.accessKeyId": "aws.accessKeyId",
                "aws.secretAccessKey": "aws.secretAccessKey",
            }
            for prop, key in aws_settings.items():
                value = spark_conf.get(key)
                if value:
                    jvm.java.lang.System.setProperty(prop, value)  # type: ignore
                    logger.info("Set AWS JVM property %s", prop)
            java_gw = spark.sparkContext._gateway  # type: ignore
            java_import(java_gw.jvm, "com.dqlabs.dialects.DeltaTableDialect")  # type: ignore
            java_gw.jvm.org.apache.spark.sql.jdbc.JdbcDialects.registerDialect(java_gw.jvm.DeltaTableDialect())  # type: ignore
        except Exception as e:
            logger.error(f"Failed to set AWS properties: {str(e)}")

    def get_failed_rows_metadata(self):
        """
        Get Failed Rows Metadata
        """
        failed_rows_metadata_url = self.job_config.get("failed_rows_file_url")
        response = requests.get(failed_rows_metadata_url)
        response.raise_for_status()
        response = response.json()
        if not response:
            raise ValueError("No Metadata Found")
        return response


    def create_catalog_schema(self):
        try:
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.catalog_name}.{self.schema_name}")
        except Exception as e:
            logger.error("Failed to ensure Iceberg catalog and schema: %s", str(e))

    def create_table(self, query: str, table_name: str):
        """
        Create an Iceberg table with the specified schema.
        """
        try:
            self.spark.sql(f"""
                {query}
                USING iceberg
                TBLPROPERTIES (
                    'format-version'='2',
                    'write.parquet.row-group-size-bytes'='134217728',
                    'write.parquet.page-size-bytes'='1048576',
                    'write.metadata.delete-after-commit.enabled'='true',
                    'write.metadata.previous-versions-max'='1',
                    'write.upsert.enabled'='true',
                    'write.delete.enabled'='true',
                    'write.merge.enabled'='true',
                    'write.fanout.enabled'='true',
                    'write.parquet.compression-codec'='snappy'
                )
            """)
        except Exception as e:
            logger.error(f"Failed to create Iceberg table: {table_name}", str(e))

    def spark_type_to_sql_type(self, spark_type):
        """
        Maps Spark data types to SQL-compatible types.
        Extend this mapping as needed.
        """
        if isinstance(spark_type, StringType):
            return "STRING"
        elif isinstance(spark_type, IntegerType):
            return "INT"
        elif isinstance(spark_type, LongType):
            return "BIGINT"
        elif isinstance(spark_type, ShortType):
            return "SMALLINT"
        elif isinstance(spark_type, FloatType):
            return "FLOAT"
        elif isinstance(spark_type, DoubleType):
            return "DOUBLE"
        elif isinstance(spark_type, BooleanType):
            return "BOOLEAN"
        elif isinstance(spark_type, TimestampType):
            return "TIMESTAMP"
        elif isinstance(spark_type, DateType):
            return "DATE"
        elif isinstance(spark_type, DecimalType):
            return "DECIMAL(18,6)"
        elif isinstance(spark_type, BinaryType):
            return "BINARY"
        else:
            return "STRING"

    def calculate_optimal_partitions(self, df: DataFrame, target_size_mb: int = 128) -> int:
        """
        Calculate optimal number of partitions based on data size and target partition size.
        
        Args:
            df: DataFrame to partition
            target_size_mb: Target partition size in MB (default 128MB)
            
        Returns:
            Optimal number of partitions
        """
        try:
            # Get DataFrame size in bytes - improved estimation
            row_count = df.count()
            if row_count == 0:
                return 2
            
            # Sample data to estimate size more accurately
            sample_size = min(1000, row_count)
            sample_df = df.limit(sample_size)
            
            # Estimate size based on sample
            sample_bytes = sample_df.select("*").rdd.map(lambda x: len(str(x))).sum()
            estimated_bytes_per_row = sample_bytes / sample_size if sample_size > 0 else 100
            
            total_estimated_bytes = row_count * estimated_bytes_per_row
            target_size_bytes = target_size_mb * 1024 * 1024
            
            # Calculate optimal partitions
            optimal_partitions = max(1, min(50, int(total_estimated_bytes / target_size_bytes)))
            optimal_partitions = max(optimal_partitions, 2)
            
            logger.info(f"Row count: {row_count}, Estimated size: ~{total_estimated_bytes / (1024*1024):.2f}MB, Optimal partitions: {optimal_partitions}")
            return optimal_partitions
        except Exception as e:
            logger.warning(f"Failed to calculate optimal partitions: {str(e)}, using default")
            return 10

    def execute_connection_query(self, query: str, is_list: bool = False):
        try:
            jdbc_options = self.job_config.get("jdbc_options", {})
            if "dbtable" in jdbc_options:
                del jdbc_options["dbtable"]
            jdbc_options.update({"query": query, "fetchsize": 25000})
            response = self.spark.read.format("jdbc").options(**jdbc_options).load()
            if is_list:
                response = [row.asDict() for row in response.collect()]
            elif response:
                if "row_number" in response.columns:
                    response = response.drop("row_number")
            return response
        except Exception as e:
            logger.error(f"Failed to execute connection query: {str(e)}")
            return None

    def execute_query(self, query: str):
        try:
            response = self.spark.sql(query)
            return response
        except Exception as e:
            logger.error(f"Failed to execute query: {str(e)}")
            return None


    def update_custom_field_columns(self, metadata_tables: list = []):
        custom_fields = self.input_metadata.get("custom_fields", [])
        if not custom_fields:
            return
        
        # Get all existing columns from all tables first
        all_existing_columns = []
        for table in metadata_tables:
            columns_query = f"DESCRIBE {self.catalog_name}.{self.schema_name}.{table}"
            existing_columns = self.spark.sql(columns_query).collect()
            column_names = [{"column_name": row.col_name, "table_name": table} for row in existing_columns if row.col_name and not row.col_name.startswith('#')]
            all_existing_columns.extend(column_names)
        
        # Process each custom field
        for field in custom_fields:
            field_level = field['level']
            table_name = f"{field_level}_metadata"
            field_names = field['names']
            if field_names and isinstance(field_names, str):
                field_names = json.loads(field_names)

            table_existing_columns = [column.get("column_name") for column in all_existing_columns if column.get("table_name").lower() == table_name]
            
            # Check if any of the field names already exist as columns in any table
            missing_columns = []
            for field_name in field_names:
                if field_name not in table_existing_columns:
                    missing_columns.append(field_name)
            
            # Add missing columns to the specific table based on field_level
            if missing_columns:
                table_name = f"{field_level}_metadata"
                if table_name.lower() not in metadata_tables:
                    continue
                # Create single ALTER query for all missing columns
                columns_to_add = ", ".join([f"`{column_name}` STRING" for column_name in missing_columns])
                alter_query = f"ALTER TABLE {self.catalog_name}.{self.schema_name}.{table_name} ADD COLUMNS ({columns_to_add})"
                try:
                    self.spark.sql(alter_query)
                    logger.info(f"Added columns {missing_columns} to table {table_name}")
                except Exception as e:
                    logger.error(f"Failed to add columns {missing_columns} to table {table_name}: {str(e)}")

    def write_table(self, df, table_name: str, is_metadata: bool = False):
        try:
            # Define data type mappings for conversion with null handling
            type_conversions = {
                'TimestampType()': (lambda col_name: to_timestamp(col(col_name)), 'timestamp'),
                'IntegerType()': (lambda col_name: when((col(col_name) == 'null') | isnull(col(col_name)) | isnan(col(col_name)), None).otherwise(col(col_name).cast('int')), 'integer'),
                'LongType()': (lambda col_name: when((col(col_name) == 'null') | isnull(col(col_name)) | isnan(col(col_name)), None).otherwise(col(col_name).cast('bigint')), 'long'),
                'DecimalType(18,6)': (lambda col_name: when((col(col_name) == 'null') | isnull(col(col_name)) | isnan(col(col_name)), None).otherwise(col(col_name).cast('decimal(18,6)')), 'decimal'),
                'BooleanType()': (lambda col_name: when((col(col_name) == 'null') | isnull(col(col_name)) | (col(col_name) == '') | (col(col_name) == '0') | (col(col_name) == 'false'), None).otherwise(when((col(col_name) == '1') | (col(col_name) == 'true'), lit(True)).otherwise(lit(False))), 'boolean')
            }

            # Get table schema using cached method
            table_schema = self.spark.table(f"{self.catalog_name}.{self.schema_name}.{table_name}").schema
            target_columns = [field.name for field in table_schema.fields]

            if not is_metadata:
                metadata_columns = ["CONNECTION_ID",
                    "CONNECTION_NAME",
                    "ASSET_ID",
                    "ASSET_NAME",
                    "ATTRIBUTE_ID",
                    "ATTRIBUTE_NAME",
                    "MEASURE_ID",
                    "MEASURE_NAME",
                    "RUN_ID",
                    "MEASURE_CONDITION",
                    "DOMAINS",
                    "PRODUCTS",
                    "APPLICATIONS",
                    "TERMS",
                    "TAGS",
                    "IDENTIFIER_KEY",
                    "EXPORTROW_ID",
                    "CREATED_DATE"]
                if self.connection_type in ["redshift", "redshift_spectrum"]:
                    target_columns = [column.lower() if column in metadata_columns else column for column in target_columns]

            # Remove 'row_number' column if it exists in the DataFrame
            if "row_number" in df.columns:
                df = df.drop("row_number")

            # Process each column based on its target data type
            for field in table_schema.fields:
                col_name = field.name
                data_type = str(field.dataType)
                if self.connection_type in ["redshift", "redshift_spectrum"] and not is_metadata:
                    col_name = col_name.lower()
                    
                if col_name in df.columns and data_type in type_conversions:
                    conversion_func, type_name = type_conversions[data_type]
                    try:
                        df = df.withColumn(col_name, conversion_func(col_name))
                        logger.debug(f"Converted column {col_name} to {type_name}")
                    except Exception as e:
                        logger.warning(f"Failed to convert column {col_name} to {type_name}: {str(e)}")
            
            # Add null columns for missing target columns efficiently
            missing_columns = set(target_columns) - set(df.columns)
            for col_name in missing_columns:
                df = df.withColumn(col_name, lit(None))
            
            # Select columns in the same order as target table
            target_columns = [f'`{column}`' for column in target_columns]
            final_df = df.select(*target_columns)
            
            # Calculate optimal partitions dynamically
            if not is_metadata:
                optimal_partitions = self.calculate_optimal_partitions(final_df)
                final_df = final_df.repartition(optimal_partitions)
            
            # Cache the DataFrame for better performance
            final_df.cache()

            if "row_number" in final_df.columns:
                final_df = final_df.drop("row_number")
            
            # Write to table with optimized settings
            final_df.write \
                .format("iceberg") \
                .mode("append") \
                .option("write.parquet.row-group-size-bytes", "134217728") \
                .option("write.parquet.page-size-bytes", "1048576") \
                .option("write.metadata.delete-after-commit.enabled", "true") \
                .option("write.metadata.previous-versions-max", "1") \
                .option("write.fanout.enabled", "true") \
                .option("write.parquet.compression-codec", "snappy") \
                .saveAsTable(f"{self.catalog_name}.{self.schema_name}.{table_name}")
            
            # Unpersist the cached DataFrame
            final_df.unpersist()
        except Exception as e:
            logger.error(f"Insert failed {table_name}", str(e))
    
    def insert_metadata(self, job_type: str, metadata_types: list = []):
        try:
            if job_type == "metadata":
                metadata_tables = metadata_types if 'All' not in metadata_types else ["connection_metadata", "asset_metadata", "attribute_metadata", "measure_metadata", "user_metadata", "user_activity"]
                metadata_tables = [t.replace(" ", "_").lower() for t in set(metadata_tables)]
                if "User Metadata" in metadata_types:
                    metadata_tables.append("user_activity")
            else:
                metadata_tables = ["connection_metadata", "asset_metadata", "attribute_metadata", "measure_metadata"]
            run_id = self.job_config.get("queue_id")
            
            # Batch process metadata for better performance
            for table in metadata_tables:
                if job_type != "metadata":
                    try:
                        # Delete existing run data
                        delete_query = queries.get("delete_existing_run")
                        if delete_query:
                            delete_query = (delete_query.replace("<catalog>", self.catalog_name)
                                            .replace("<schema>", self.schema_name)
                                            .replace("<table_name>", table)
                                            .replace("<run_id>", run_id))
                            self.spark.sql(delete_query)
                    except Exception as e:
                        logger.error(f"Failed to delete existing run for table {table}: {str(e)}")

                table_data = self.input_metadata.get("metadata", {}).get(table, [])
                if not table_data:
                    continue
                    
                # Convert list to DataFrame
                df = self.spark.createDataFrame(table_data)
                self.write_table(df, table, is_metadata=True)
                logger.info(f"Successfully inserted {len(table_data)} records into {table}")
        except Exception as e:
            logger.error(f"Insert metadata failed", str(e))

    def format_identifier_columns(self, identifier_columns: List[str]):
        if self.connection_type in ['bigquery', 'databricks', 'mysql']:
            return [f"`{column}`" for column in identifier_columns]
        elif self.connection_type in ['mssql', 'sql', 'synapse']:
            return [f"[{column}]" for column in identifier_columns]
        elif self.connection_type in ['oracle', 'snowflake', 'redshift', 'redshift_spectrum', 'denodo', 'hive', 'saphana']:
            return [f'"{column}"' for column in identifier_columns]
        return identifier_columns

    def prepare_identifier_query(self, identifier_columns: List[str]):
        delimeter = '|'
        concat_query_string = ""
        if self.connection_type == "bigquery":
            concat_query_string = f"ARRAY_TO_STRING({identifier_columns}, '{delimeter}')"
        elif self.connection_type in ['db2', 'db2ibm', 'oracle']:
            concat_query_string = f" || '{delimeter}' || ".join(identifier_columns)
        elif self.connection_type in ['redshift', 'redshift_spectrum']:
            identifier_columns = [f"{column}::text" for column in identifier_columns]
            concat_query_string = f" || '{delimeter}' || ".join(identifier_columns)
            concat_query_string = f"concat(({concat_query_string}), '')"
        elif self.connection_type == "saphana":
            concat_query_string = f""""""
            temp_index = 1
            for identifier_key_column in identifier_columns:
                concat_query_string = f"""{concat_query_string} case when {identifier_key_column} is not null then CAST({identifier_key_column} AS VARCHAR) else '' end"""
                if temp_index != len(identifier_columns):
                    concat_query_string = (
                        f"""{concat_query_string} || '{delimeter}' ||"""
                    )
                temp_index = temp_index + 1
        elif self.connection_type == "teradata":
            temp_index = 1
            for identifier_key_column in identifier_columns:
                concat_query_string += f"COALESCE(CAST({identifier_key_column} AS VARCHAR(255)), '')"
                if temp_index != len(identifier_columns):
                    concat_query_string = (
                        f"""{concat_query_string}, ' {delimeter} ',"""
                    )
                temp_index = temp_index + 1
        elif self.connection_type == "athena":
            identifier_columns = [f"cast({column} as varchar)" for column in identifier_columns]
            identifier_columns_str = ",".join(identifier_columns)
            concat_query_string = f"concat_ws('{delimeter}', {identifier_columns_str})"
        else:
            identifier_columns_str = ",".join(identifier_columns)
            concat_query_string = f"concat_ws('{delimeter}', {identifier_columns_str})"
        concat_query_string = f"{concat_query_string} AS IDENTIFIER_KEY"
        return concat_query_string

    def get_insert_source_columns(self, columns: List[Dict[str, str]]):
        """
        Get Insert Columns
        """
        unsupport_attributes = self.input_metadata.get("unsupported_attributes", [])
        unsupport_attributes = [column.lower() for column in unsupport_attributes] if unsupport_attributes else []
        unsupported_datatypes = ["variant","object","geography","geometry","varbinary","sql_variant","json","struct","map","array","blob"]
        if self.connection_type in ["redshift", "redshift_spectrum"]:
            unsupported_datatypes.append("boolean")
        insert_columns = [column.get("name") for column in columns if column.get("datatype") not in unsupported_datatypes and column.get("name").lower() not in unsupport_attributes]
        return insert_columns

    
    def insert_summarized_failed_rows(self, failed_rows_table):
        """
        Insert Summarized Failed rows records with optimized Spark operations
        - Uses Spark DataFrame operations to avoid memory issues
        - Groups failed rows by identifier key
        - Stores measure metadata in measure_data column
        - Stores failed row data in failed_row_data column
        """
        from datetime import datetime
        from decimal import Decimal
        import uuid
        from pyspark.sql.functions import (
            struct, to_json, collect_list, first, lit as spark_lit, 
            col as spark_col, concat_ws, coalesce, udf
        )
        from pyspark.sql.types import StringType
        
        try:
            failed_row_queries = queries.get("failed_rows", {})
            if not failed_row_queries:
                logger.error("Failed rows queries not found")
                return
                
            measures = self.input_metadata.get("measures", [])
            if not measures:
                logger.info("No measures found for summarized export")
                return
            
            # Get measure metadata for all measures
            measure_metadata_list = self.input_metadata.get("metadata", {}).get("measure_metadata", [])
            measure_metadata_dict = {m.get("MEASURE_ID"): m for m in measure_metadata_list}

            # Delete existing run records
            run_id = self.job_config.get("queue_id")
            delete_query = failed_row_queries.get("delete_summarized")
            if delete_query:
                delete_query = (delete_query.replace("<catalog>", self.catalog_name)
                                .replace("<schema>", self.schema_name)
                                .replace("<table_name>", failed_rows_table)
                                .replace("<run_id>", run_id))
                try:
                    self.spark.sql(delete_query)
                    logger.info(f"Deleted existing records for run_id: {run_id}")
                except Exception as e:
                    logger.warning(f"Failed to delete existing records: {str(e)}")
            
            primary_attributes = deepcopy(self.job_config.get("primary_attributes", []))
            
            # Process all measures and collect DataFrames
            all_measure_dfs = []
            
            for measure in measures:
                try:
                    measure_id = measure.get("measure_id")
                    measure_name = measure.get("measure_name")
                    measure_level = measure.get("level")
                    measure_category = measure.get("category")
                    column_limit = measure.get("column_limit", 50)
                    invalid_select_query = measure.get("invalid_select_query", "")
                    select_query = measure.get("select_query", "")
                    select_query = select_query.replace("<count>", "1")

                    if not select_query.strip().lower().startswith("select") and not invalid_select_query.strip().lower().startswith("with"):
                        continue
                    if (
                        measure_level == "asset"
                        and str(measure_name).lower() == "duplicates"
                        and not primary_attributes
                    ):
                        continue

                    # Get Measure Data as DataFrame (don't collect yet)
                    if measure_category == "cross_source":
                        measure_df = self.execute_query(invalid_select_query)
                    else:
                        measure_df = self.execute_connection_query(invalid_select_query)
                    if not isinstance(measure_df, DataFrame):
                        continue
                    
                    # Check if DataFrame is empty using limit(1) instead of count()
                    if measure_df.limit(1).count() == 0:
                        logger.info(f"No failed rows for measure {measure_name}")
                        continue

                    # Get measure metadata
                    metadata = measure_metadata_dict.get(measure_id, {})
                    
                    # Get Insert Columns and determine identifier columns
                    source_columns_with_datatype = [
                        {"name": field.name, "datatype": field.dataType.simpleString()} 
                        for field in measure_df.schema.fields
                    ]
                    insert_source_columns = self.get_insert_source_columns(source_columns_with_datatype)
                    insert_columns = deepcopy(insert_source_columns)
                    
                    if primary_attributes:
                        insert_columns = [col for col in primary_attributes if col in insert_source_columns]
                    if measure_category in ["query", "parameter"]:
                        insert_columns = insert_source_columns
                    
                    insert_columns.sort()
                    insert_columns = insert_columns[:column_limit]
                    
                    if not insert_columns:
                        logger.warning(f"No valid columns for identifier key in measure {measure_name}")
                        continue
                    
                    # Extract metadata values from failed_rows_metadata_columns
                    failed_rows_metadata_columns = measure.get("failed_rows_metadata_columns", [])
                    metadata_dict = {}
                    for col_expr in failed_rows_metadata_columns:
                        # Parse expressions like "'value' as COLUMN_NAME"
                        if " as " in col_expr.lower():
                            parts = col_expr.lower().split(" as ")
                            if len(parts) >= 2:
                                value_part = col_expr[:col_expr.lower().rfind(" as ")].strip().strip("'\"")
                                column_name = parts[-1].strip().upper()
                                # Convert 'None' string to empty string
                                if value_part in ("None", "null", "NULL", "'None'", "'null'"):
                                    value_part = ""
                                metadata_dict[column_name] = value_part
                    
                    connection_id = metadata_dict.get("CONNECTION_ID", "")
                    connection_name = metadata_dict.get("CONNECTION_NAME", "")
                    asset_id = metadata_dict.get("ASSET_ID", "")
                    asset_name = metadata_dict.get("ASSET_NAME", "")
                    attribute_id = metadata_dict.get("ATTRIBUTE_ID", "")
                    attribute_name = metadata_dict.get("ATTRIBUTE_NAME", "")
                    measure_condition = metadata_dict.get("MEASURE_CONDITION", "")
                    domains = metadata_dict.get("DOMAINS", "")
                    products = metadata_dict.get("PRODUCTS", "")
                    applications = metadata_dict.get("APPLICATIONS", "")
                    terms = metadata_dict.get("TERMS", "")
                    tags = metadata_dict.get("TAGS", "")

                    # Prepare measure_data JSON structure (as dictionary)
                    measure_data = {
                        "MEASURE_ID": metadata.get("MEASURE_ID", measure_id),
                        "MEASURE_NAME": metadata.get("MEASURE_NAME", measure_name),
                        "weightage": float(metadata.get("MEASURE_WEIGHTAGE", 0)) if metadata.get("MEASURE_WEIGHTAGE") else None,
                        "category": measure_category,
                        "created_date": str(metadata.get("CREATED_DATE", "")),
                        "total_count": int(metadata.get("TOTAL_RECORDS", 0)) if metadata.get("TOTAL_RECORDS") else 0,
                        "valid_count": int(metadata.get("PASSED_RECORDS", 0)) if metadata.get("PASSED_RECORDS") else 0,
                        "invalid_count": int(metadata.get("FAILED_RECORDS", 0)) if metadata.get("FAILED_RECORDS") else 0,
                        "score": float(metadata.get("DQ_SCORE", 0)) if metadata.get("DQ_SCORE") and metadata.get("DQ_SCORE") != "null" and metadata.get("DQ_SCORE") != "None" else None,
                        "dimension_name": str(metadata.get("MEASURE_DIMENSION", "")) if metadata.get("MEASURE_DIMENSION") else "",
                        "is_positive": True,
                        "last_run_status": str(metadata.get("RUN_STATUS", "")),
                        "run_date": str(metadata.get("RUN_DATE", "")),
                        "measure_status": str(metadata.get("MEASURE_STATUS", "")),
                        "description": str(metadata.get("COMMENT", "")) if metadata.get("COMMENT") else "",
                        "is_active": bool(metadata.get("IS_ACTIVE", True)),
                        "type": str(metadata.get("MEASURE_TYPE", "")),
                        "ATTRIBUTE_ID": attribute_id,
                        "ATTRIBUTE_NAME": attribute_name,
                        "metric_query": str(metadata.get("MEASURE_QUERY", "")),
                        "message": ""
                    }
                    measure_data_json = json.dumps(measure_data, default=str)

                    # Generate identifier key using Spark functions
                    identifier_cols = [coalesce(spark_col(f"`{col}`").cast("string"), spark_lit("")) for col in insert_columns]
                    identifier_expr = concat_ws("|", *identifier_cols)
                    
                    # Add metadata columns
                    enhanced_df = measure_df.withColumn("IDENTIFIER_KEY", identifier_expr) \
                        .withColumn("CONNECTION_ID", spark_lit(connection_id)) \
                        .withColumn("CONNECTION_NAME", spark_lit(connection_name)) \
                        .withColumn("ASSET_ID", spark_lit(asset_id)) \
                        .withColumn("ASSET_NAME", spark_lit(asset_name)) \
                        .withColumn("ATTRIBUTE_ID", spark_lit(attribute_id)) \
                        .withColumn("ATTRIBUTE_NAME", spark_lit(attribute_name)) \
                        .withColumn("MEASURE_ID_META", spark_lit(measure_id)) \
                        .withColumn("MEASURE_NAME_META", spark_lit(measure_name)) \
                        .withColumn("RUN_ID", spark_lit(run_id)) \
                        .withColumn("MEASURE_CONDITION", spark_lit(measure_condition)) \
                        .withColumn("DOMAINS", spark_lit(domains)) \
                        .withColumn("PRODUCTS", spark_lit(products)) \
                        .withColumn("APPLICATIONS", spark_lit(applications)) \
                        .withColumn("TERMS", spark_lit(terms)) \
                        .withColumn("TAGS", spark_lit(tags)) \
                        .withColumn("MEASURE_DATA_SINGLE", spark_lit(measure_data_json))
                    
                    # Convert all original columns to a JSON string for FAILED_ROW_DATA
                    # Select only the original source columns (exclude metadata columns we just added)
                    original_cols = [f"`{col}`" for col in measure_df.columns]
                    row_data_struct = struct(*[spark_col(col) for col in original_cols])
                    enhanced_df = enhanced_df.withColumn("FAILED_ROW_DATA", to_json(row_data_struct))
                    
                    # Filter out empty identifier keys
                    enhanced_df = enhanced_df.filter(spark_col("IDENTIFIER_KEY") != "")
                    
                    # Select only the columns we need for aggregation
                    select_cols = [
                        "IDENTIFIER_KEY", "CONNECTION_ID", "CONNECTION_NAME", 
                        "ASSET_ID", "ASSET_NAME", "ATTRIBUTE_ID", "ATTRIBUTE_NAME",
                        "RUN_ID", "MEASURE_CONDITION", "DOMAINS", "PRODUCTS", 
                        "APPLICATIONS", "TERMS", "TAGS", "MEASURE_DATA_SINGLE", "FAILED_ROW_DATA"
                    ]
                    enhanced_df = enhanced_df.select(*select_cols)
                    
                    # Cache this DataFrame if it's small enough
                    row_count = enhanced_df.count()
                    logger.info(f"Measure {measure_name}: {row_count} failed rows")
                    
                    if row_count > 0:
                        if row_count < 10000:  # Cache small DataFrames
                            enhanced_df = enhanced_df.cache()
                        all_measure_dfs.append(enhanced_df)
                    
                except Exception as e:
                    logger.error(f"Error processing measure {measure_id}: {str(e)}")
                    continue
            
            if not all_measure_dfs:
                logger.info("No failed rows to insert in summarized mode")
                return
            
            # Union all measure DataFrames
            logger.info(f"Combining {len(all_measure_dfs)} measure DataFrames...")
            combined_df = all_measure_dfs[0]
            for df in all_measure_dfs[1:]:
                combined_df = combined_df.union(df)
            
            # Group by identifier key and aggregate measures
            logger.info("Aggregating by identifier key...")
            
            # Group ONLY by IDENTIFIER_KEY to avoid duplicates
            # Take first value for all metadata columns since they should be consistent per identifier
            aggregated_df = combined_df.groupBy("IDENTIFIER_KEY").agg(
                first("CONNECTION_ID").alias("CONNECTION_ID"),
                first("CONNECTION_NAME").alias("CONNECTION_NAME"),
                first("ASSET_ID").alias("ASSET_ID"),
                first("ASSET_NAME").alias("ASSET_NAME"),
                first("ATTRIBUTE_ID").alias("ATTRIBUTE_ID"),
                first("ATTRIBUTE_NAME").alias("ATTRIBUTE_NAME"),
                first("RUN_ID").alias("RUN_ID"),
                first("MEASURE_CONDITION").alias("MEASURE_CONDITION"),
                first("DOMAINS").alias("DOMAINS"),
                first("PRODUCTS").alias("PRODUCTS"),
                first("APPLICATIONS").alias("APPLICATIONS"),
                first("TERMS").alias("TERMS"),
                first("TAGS").alias("TAGS"),
                collect_list("MEASURE_DATA_SINGLE").alias("MEASURE_DATA_LIST"),
                first("FAILED_ROW_DATA").alias("FAILED_ROW_DATA")
            )
            
            # Convert measure_data list to JSON array string
            # UDF to merge JSON strings into array
            def merge_json_array(json_list):
                if not json_list:
                    return "[]"
                try:
                    result = []
                    for json_str in json_list:
                        if json_str:
                            obj = json.loads(json_str)
                            result.append(obj)
                    return json.dumps(result, default=str)
                except Exception as e:
                    logger.error(f"Error merging JSON array: {str(e)}")
                    return "[]"
            
            merge_json_udf = udf(merge_json_array, StringType())
            
            final_df = aggregated_df.withColumn("MEASURE_DATA", merge_json_udf(spark_col("MEASURE_DATA_LIST"))) \
                .withColumn("MEASURE_ID", spark_lit("")) \
                .withColumn("MEASURE_NAME", spark_lit("")) \
                .withColumn("EXPORTROW_ID", spark_lit("")) \
                .withColumn("IS_SUMMARISED", spark_lit(True)) \
                .withColumn("CREATED_DATE", spark_lit(datetime.now())) \
                .drop("MEASURE_DATA_LIST")
            
            # Add summarized columns to table if they don't exist
            try:
                failed_row_columns = self.spark.table(f"{self.catalog_name}.{self.schema_name}.{failed_rows_table}").schema
                failed_row_column_names = [field.name for field in failed_row_columns.fields]
                
                alter_columns = []
                if "MEASURE_DATA" not in failed_row_column_names:
                    alter_columns.append("`MEASURE_DATA` STRING")
                if "FAILED_ROW_DATA" not in failed_row_column_names:
                    alter_columns.append("`FAILED_ROW_DATA` STRING")
                if "IS_SUMMARISED" not in failed_row_column_names:
                    alter_columns.append("`IS_SUMMARISED` BOOLEAN")
                
                if alter_columns:
                    alter_query = f"ALTER TABLE {self.catalog_name}.{self.schema_name}.{failed_rows_table} ADD COLUMNS ({', '.join(alter_columns)})"
                    self.spark.sql(alter_query)
                    logger.info(f"Added summarized columns to table {failed_rows_table}")
            except Exception as e:
                logger.error(f"Failed to add summarized columns: {str(e)}")
            
            # Write the aggregated DataFrame
            record_count = final_df.count()
            logger.info(f"Writing {record_count} summarized records to {failed_rows_table}")
            
            self.write_table(final_df, failed_rows_table, is_metadata=False)
            logger.info(f"Successfully inserted {record_count} summarized records into {failed_rows_table}")
            
        except Exception as e:
            logger.error(f"Insert summarized failed rows failed: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            raise

    def insert_failed_rows(self, failed_rows_table):
        """
        Insert Failed rows records with optimized batch processing
        """
        failed_row_queries = queries.get("failed_rows", {})
        if not failed_row_queries:
            logger.error("Failed rows queries not found")
            return
            
        measures = self.input_metadata.get("measures", [])
        if not measures:
            return

        # Delete existing run records
        run_id = self.job_config.get("queue_id")
        measure_list = [measure.get("measure_id") for measure in measures]
        measure_list = f"""({','.join(f"'{i}'" for i in measure_list)})"""
        delete_query = failed_row_queries.get("delete")
        if delete_query:
            delete_query = (delete_query.replace("<catalog>", self.catalog_name)
                            .replace("<schema>", self.schema_name)
                            .replace("<table_name>", failed_rows_table)
                            .replace("<measure_id>", measure_list)
                            .replace("<run_id>", run_id))
            self.spark.sql(delete_query)
        
        # Process measures in batches - single pass optimization
        logger.info("Processing measures in batches with single-pass optimization...")
        
        # Configurable batch size - process N measures at a time
        batch_size = 10
        total_measures_processed = 0
        total_rows_written = 0
        primary_attributes = deepcopy(self.job_config.get("primary_attributes", []))
        
        # Get current table schema once
        failed_row_columns = self.spark.table(f"{self.catalog_name}.{self.schema_name}.{failed_rows_table}").schema
        failed_row_column_names = set([field.name for field in failed_row_columns.fields])
        
        # Process measures in batches
        for batch_start in range(0, len(measures), batch_size):
            batch_end = min(batch_start + batch_size, len(measures))
            batch_measures = measures[batch_start:batch_end]
            batch_dataframes = []
            batch_columns_to_add = {}  # Collect new columns for this batch
            
            logger.info(f"Processing batch {batch_start // batch_size + 1}: measures {batch_start + 1} to {batch_end} of {len(measures)}")
            
            for measure in batch_measures:
                measure_name = measure.get("measure_name")
                measure_level = measure.get("level")
                measure_category = measure.get("category")
                column_limit = measure.get("column_limit")
                failed_rows_metadata_columns = measure.get("failed_rows_metadata_columns", [])
                invalid_select_query = measure.get("invalid_select_query", "")
                select_query = measure.get("select_query", "")
                select_query = select_query.replace("<count>","1")

                if not select_query.strip().lower().startswith("select") and not invalid_select_query.strip().lower().startswith("with"):
                    continue
                if (
                    measure_level == "asset"
                    and str(measure_name).lower() == "duplicates"
                    and not primary_attributes
                ):
                    continue

                try:
                    # Get Measure Data
                    if measure_category == "cross_source":
                        measure_df = self.execute_query(select_query)
                    else:
                        measure_df = self.execute_connection_query(select_query)
                    if not isinstance(measure_df, DataFrame):
                        continue
                    
                    # Check if dataframe has data using limit(1) for efficiency
                    if measure_df.limit(1).count() == 0:
                        logger.info(f"No failed rows for measure {measure_name}")
                        continue

                    source_columns = measure_df.columns
                    source_columns.sort()
                    source_columns = source_columns[:column_limit]
                    source_columns_with_datatype = [{"name": field.name, "datatype": field.dataType.simpleString()} for field in measure_df.schema.fields]
                    
                    # Collect new columns for this batch
                    for column in source_columns:
                        if column not in failed_row_column_names and column not in batch_columns_to_add:
                            source_column_type = measure_df.select(f"`{column}`").schema.fields[0].dataType
                            data_type = self.spark_type_to_sql_type(source_column_type)
                            batch_columns_to_add[column] = data_type

                    # Get Insert Columns
                    insert_source_colums = self.get_insert_source_columns(source_columns_with_datatype)
                    insert_columns = deepcopy(insert_source_colums)
                    if primary_attributes:
                        insert_columns = primary_attributes
                    if measure_category in ["query", "parameter"]:
                        insert_columns = insert_source_colums
                    insert_columns.sort()
                    insert_columns = insert_columns[:column_limit]
                    
                    # Prepare SubQuery
                    select_columns = deepcopy(source_columns)
                    select_columns.sort()
                    select_columns = select_columns[:column_limit]
                    select_columns = self.format_identifier_columns(select_columns)
                    select_columns = [f"subquery.{column}" for column in select_columns]
                    select_column_query = ",".join(select_columns)

                    # Prepare Identifier Columns
                    identifier_columns = deepcopy(insert_columns)
                    identifier_columns = self.format_identifier_columns(identifier_columns)
                    identifier_query = "'' AS IDENTIFIER_KEY"
                    if len(identifier_columns) > 1:
                        identifier_columns.sort()
                        identifier_query = self.prepare_identifier_query(identifier_columns)
                    else:
                        identifier_query = f"{insert_columns[0]} AS IDENTIFIER_KEY"
                    failed_rows_metadata_columns.append(identifier_query)

                    # Prepare Failed Row Query
                    failed_rows_metadata_query = ",".join(failed_rows_metadata_columns)
                    alias_query = "subquery" if self.connection_type == "oracle" else "AS subquery"
                    failed_row_query = f"select {failed_rows_metadata_query}, {select_column_query} from ({invalid_select_query}) {alias_query}"

                    logger.info(f"Executing failed_row_query for measure: {measure_name}")
                    logger.info(failed_row_query)
                    
                    if measure_category == "cross_source":
                        df = self.execute_query(failed_row_query)
                    else:
                        df = self.execute_connection_query(failed_row_query)
                    if df is not None and isinstance(df, DataFrame):
                        df = df.withColumn("CREATED_DATE", lit(datetime.now()))
                        batch_dataframes.append(df)
                except Exception as e:
                    logger.error(f"Error processing measure {measure_name}: {str(e)}")
                    continue
            
            # Alter table schema for new columns in this batch
            if batch_columns_to_add:
                columns_to_alter = [f"`{col_name}` {data_type}" for col_name, data_type in batch_columns_to_add.items()]
                alter_query = f"ALTER TABLE {self.catalog_name}.{self.schema_name}.{failed_rows_table} ADD COLUMNS ({', '.join(columns_to_alter)})"
                try:
                    self.spark.sql(alter_query)
                    logger.info(f"Added {len(columns_to_alter)} new columns to {failed_rows_table} in batch {batch_start // batch_size + 1}")
                    # Update the set of known column names
                    failed_row_column_names.update(batch_columns_to_add.keys())
                except Exception as e:
                    logger.error(f"Failed to alter table in batch {batch_start // batch_size + 1}: {str(e)}")
            
            # Write batch if we have dataframes
            if batch_dataframes:
                logger.info(f"Combining {len(batch_dataframes)} dataframes in current batch...")
                try:
                    # Union all dataframes in this batch
                    if len(batch_dataframes) == 1:
                        batch_combined_df = batch_dataframes[0]
                    else:
                        batch_combined_df = batch_dataframes[0]
                        for df in batch_dataframes[1:]:
                            batch_combined_df = batch_combined_df.unionByName(df, allowMissingColumns=True)
                    
                    batch_rows = batch_combined_df.count()
                    logger.info(f"Writing {batch_rows} rows from batch {batch_start // batch_size + 1} to {failed_rows_table}...")
                    
                    # Write batch to table
                    self.write_table(batch_combined_df, failed_rows_table)
                    
                    total_rows_written += batch_rows
                    total_measures_processed += len(batch_dataframes)
                    logger.info(f"Successfully wrote batch {batch_start // batch_size + 1}: {batch_rows} rows")
                    
                    # Clean up batch dataframes to free memory
                    for df in batch_dataframes:
                        try:
                            df.unpersist()
                        except:
                            pass
                    batch_combined_df.unpersist()
                    batch_dataframes.clear()
                    
                    # Force garbage collection to free memory
                    import gc
                    gc.collect()
                    
                except Exception as e:
                    logger.error(f"Error combining and writing batch {batch_start // batch_size + 1}: {str(e)}")
                    # Continue processing next batch even if this one fails
                    continue
            else:
                logger.info(f"No data to write in batch {batch_start // batch_size + 1}")

    def create_base_tables(self, job_type: str, metadata_types: list = []):
        print('metadata_types-----', metadata_types)
        if job_type == "metadata":
            metadata_tables = metadata_types if 'All' not in metadata_types else ["connection_metadata", "asset_metadata", "attribute_metadata", "measure_metadata", "user_metadata", "user_activity"]
            metadata_tables = [t.replace(" ", "_").lower() for t in set(metadata_tables)]
            if "User Metadata" in metadata_types:
                metadata_tables.append("user_activity")
        elif self.job_config.get("is_metadata"):
            metadata_tables = ["connection_metadata", "asset_metadata", "attribute_metadata", "measure_metadata"]
        else:
            metadata_tables = []
        for table in metadata_tables:
            metadata_query = f"create_{table}"
            create_table_query = queries.get(metadata_query)
            if create_table_query:
                create_table_query = (create_table_query.replace("<catalog>", self.catalog_name)
                                      .replace("<schema>", self.schema_name))
                self.create_table(create_table_query, table)

        if job_type != "metadata" and self.job_config.get("is_failed_rows"):
            failed_rows_table = self.job_config.get("failed_rows_table")
            create_failed_rows_query = queries.get("failed_rows")
            if create_failed_rows_query:
                create_failed_rows_query = create_failed_rows_query.get("create")
                if create_failed_rows_query and failed_rows_table:
                    create_failed_rows_query = (create_failed_rows_query.replace("<catalog>", self.catalog_name)
                                                .replace("<schema>", self.schema_name)
                                                .replace("<failed_rows_table>", failed_rows_table))
                    self.create_table(create_failed_rows_query, failed_rows_table)

        # Update Custom Fields
        if metadata_tables:
            self.update_custom_field_columns(metadata_tables)


    def delete_retention_period_data(self):
        """
        Delete Retention Period Data
        """
        try:
            queue_id = self.job_config.get("queue_id")
            last_runs =  self.input_metadata.get("retention_runs", [])
            if not last_runs:
                return
            if queue_id:
                last_runs.append(queue_id)
            last_runs = (f"""({','.join(f"'{w}'" for w in last_runs)})""")
            failed_rows_table = self.job_config.get("failed_rows_table")
            level = self.job_config.get("level")
            asset_id = self.job_config.get("asset_id")
            attribute_id = self.job_config.get("attribute_id")
            measure_id = self.job_config.get("measure_id")
            filters = []
            if level == "measure" and measure_id:
                filters.append(f"MEASURE_ID = '{measure_id}'")
            elif asset_id:
                filters.append(f"ASSET_ID = '{asset_id}'")
            elif attribute_id:
                filters.append(f"ATTRIBUTE_ID = '{attribute_id}'")
            
            filters = " and ".join(filters) if filters else ""
            filters = f" and {filters}" if filters else ""
            delete_query = f"""
                delete from {self.catalog_name}.{self.schema_name}.{failed_rows_table}
                where RUN_ID not in {last_runs} {filters}
            """
            self.spark.sql(delete_query)
        except Exception as e:
            logger.error("Delete Retention Data", str(e))

    def reset_metadata_table_data(self):
        try:
            metadata_tables = ["connection_metadata", "asset_metadata", "attribute_metadata", "measure_metadata", "user_metadata", "user_activity"]
            for table in metadata_tables:
                try:
                    # Delete existing run data
                    delete_query = f"delete from {self.catalog_name}.{self.schema_name}.{table}"
                    self.spark.sql(delete_query)
                except Exception as e:
                    logger.error("Reset Table Data", str(e))
        except Exception as e:
            logger.error("Reset Table Data", str(e))

    def get_jdbc_support(self):
        connection = self.job_config.get("connection", {})
        credentials = connection.get("credentials")
        credentials_json = credentials.get("keyjson", {})
        temp_file = None
        if credentials_json:
            try:
                import tempfile
                temp = tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.json')
                json.dump(credentials_json, temp)
                temp.close()
                temp_file = temp.name

                # Replace <key_json> in jdbc_url only (string) in jdbc_options with temp file path
                jdbc_options = self.job_config.get("jdbc_options", {})
                jdbc_url = jdbc_options.get("url")
                if isinstance(jdbc_url, str) and "<key_file>" in jdbc_url:
                    jdbc_url = jdbc_url.replace("<key_file>", temp_file)
                    jdbc_options.update({"url": jdbc_url})
                self.job_config.update({"jdbc_options": jdbc_options})

                # Store temp file path for later cleanup
                self._temp_keyjson_file = temp_file
            except Exception as e:
                logger.error("Failed to write keyjson to temp file: %s", str(e))
                self._temp_keyjson_file = None
        else:
            self._temp_keyjson_file = None

    def cleanup_temp_files(self):
        temp_file = getattr(self, "_temp_keyjson_file", None)
        if temp_file:
            try:
                os.remove(temp_file)
                logger.info(f"Deleted temp keyjson file: {temp_file}")
            except Exception as e:
                logger.error(f"Failed to delete temp keyjson file {temp_file}: {str(e)}")
            self._temp_keyjson_file = None

    def run(self):
        try:
            failed_rows_table = self.job_config.get("failed_rows_table")
            is_reset = self.job_config.get("is_reset")
            is_summarized = self.job_config.get("is_summarized")
            spark_failed_row_queries = queries.get("failed_rows", {})
            job_type = self.job_config.get("job_type", "failed_rows")
            metadata_types = self.job_config.get("metadata_types", [])

            self.create_catalog_schema()
            
            # Drop The table if reset
            if is_reset and spark_failed_row_queries:
                drop_query = spark_failed_row_queries.get("drop")
                if drop_query and failed_rows_table:
                    drop_query = (drop_query.replace("<catalog>", self.catalog_name)
                                    .replace("<schema>", self.schema_name)
                                    .replace("<table_name>", failed_rows_table))
                    self.spark.sql(drop_query)

            # Create Base Table
            self.create_base_tables(job_type, metadata_types)

            # Reset Table Data for metadata
            if job_type == "metadata":
                self.reset_metadata_table_data()

            # Insert Metadata and Failed Rows
            if self.job_config.get("is_metadata") or job_type == "metadata":
                self.insert_metadata(job_type, metadata_types)

            if job_type != "metadata" and self.job_config.get("is_failed_rows"):
                if self.connection_type == "bigquery":
                    self.get_jdbc_support()
                if is_summarized:
                    self.insert_summarized_failed_rows(failed_rows_table)
                else:
                    self.insert_failed_rows(failed_rows_table)
                self.delete_retention_period_data()
            return {"status": "success",  "message": "Successfully completed the export failed rows."}
        except Exception as e:
            logger.error("Processing failed: %s", str(e))
            return json.dumps({"status": "error", "message": str(e), "error_type": type(e).__name__})
        finally:
            try:
                if self.connection_type == "bigquery":
                    self.cleanup_temp_files()
                self.spark.stop()
                logger.info("Spark session stopped")
            except Exception as e:
                logger.error("Spark shutdown failed: %s", str(e))
    
    
if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("Usage: python script.py '{\"config\":\"json\"}'")
        print(json.dumps({"status": "error", "message": "Usage: python script.py '{\"config\":\"json\"}'"}))
        sys.exit(1)
    try:
        raw_json = sys.argv[1]
        config = json.loads(raw_json)
        processor = ExportMetrics(config)
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