from enum import Enum


class ConnectionType(Enum):
    Postgres = "postgresql"
    Snowflake = "snowflake"
    MSSQL = "mssql"
    S3 = "s3"
    AWS = "aws"
    Redshift = "redshift"
    Oracle = "oracle"
    MySql = "mysql"
    MongoDB = "mongo"
    BigQuery = "bigquery"
    Tableau = "tableau"
    Denodo = "denodo"
    Dbt = "dbt"
    Databricks = "databricks"
    Redshift_Spectrum = "redshift_spectrum"
    Db2 = 'db2'
    Fivetran = "fivetran"
    SapHana = "saphana"
    Athena = "athena"
    Teradata = "teradata"
    Synapse = "synapse"
    Airflow = "airflow"
    Talend = "talend"
    EmrSpark = "emr_spark"
    ADF = "adf"
    Hive = "hive"
    PowerBI = "powerbi"
    DB2IBM = 'db2ibm'
    ADLS = "adls"
    S3Select = "s3select"
    Wherescape = "wherescape"
    AlloyDB = "alloydb"
    Spark= "spark"
    File = "file"
    SapEcc = "sapecc"
    Salesforce = "salesforce"
    Coalesce = "coalesce_pipeline"
    SalesforceMarketing = "salesforce_marketing"
    SalesforceDataCloud = "salesforce_data_cloud"
    Sigma = 'sigma'
    Airbyte = 'airbyte'

#For Purview Integration
class EntityType(Enum):  

    Postgres = ["postgresql_table", "postgresql_view", "postgresql_view_column", "postgresql_table_column", "postgresql_schema"]
    Snowflake = ["snowflake_table", "snowflake_view", "snowflake_table_column","snowflake_view_column", "snowflake_schema"]
    MySql = ["mysql_table", "mysql_view", "mysql_table_column", "mysql_view_column", "mysql_schema"]
    Oracle = ["oracle_table", "oracle_view", "oracle_table_column", "oracle_view_column", "oracle_schema"]
    SapHana = ["sap_hana_table", "sap_hana_view", "sap_hana_table_column", "sap_hana_view_column", "sap_hana_schema"]
    MSSQL = ["mssql_table", "mssql_view", "mssql_column", "mssql_view_column","mssql_schema", "sql_table", "sql_view", "sql_column", "sql_view_column","sql_schema", "ms_sql_column", "ms_sql_view_column" ]
    Databricks = ["azure_datbricks_table", "azure_datbricks_view", "azure_datbricks_table_column", "azure_datbricks_view_column", "azure_datbricks_schema"]
    ADLS = ["azure_datalake_gen2_path", "azure_datalake_gen2_filesystem", "azure_datalake_gen2_resource_set", "azure_datalake_gen2_object", "azure_datalake_gen2_service"]




