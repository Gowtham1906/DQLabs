AIRFLOW_OWNER = ""
AIRFLOW_EMAIL = ""
AIRFLOW_EMAIL_ON_FAILURE = False
AIRFLOW_EMAIL_ON_RETRY = False
AIRFLOW_RETRIES = 0
AIRFLOW_DEPENDS_ON_PAST = False
MAX_DICTINCT_VALUES = 25
MAX_ACTIVE_TASKS = 5
DRIFT_DAYS = 7
DRIFT_PERIOD = 3
DEFAULT_MAX_RUNS_TO_STORE = 7
DEFAULT_MAX_RUNS_TO_STORE_TYPE = "runs"
DEFAULT_SLOTS = 2
DEFAULT_CHUNK_LIMIT = 10000
MAX_LOG_LENGTH = 50000
MAX_TASKS_TO_SUBMIT = 10
MAX_RETRY_DELAY = 15
DEFAULT_SEMANTIC_THRESHOLD = 67

DEFAULT_POSTGRES_CONN = "dq_postgres_conn"
DEFAULT_POSTGRES_PORT = 5432
APP_NAME = "DQLABS"

SPARK = "Spark"

# purview constant
PURVIEW = "purview"
COALESCE = "coalesce"

# Notification Variable
NOTIFICATION = "notification"
EMAIL_NOTIFICATION = "email"
SLACK_NOTIFICATION = "slack"
TEAMS_NOTIFICATION = "teams"
GOOGLE_CHAT_NOTIFICATION = "google_chat"
MANAGE_NEW_ALERTS_NOTIFICATION = "manage_new_alerts"
MANAGE_NEW_ISSUES_NOTIFICATION = "manage_new_issues"
NEW_ALERT_NOTIFICATION = "new_alert"
NEW_ALERT_PIPELINE_NOTIFICATION = "new_alert_pipeline"
SCHEDULE = "schedule"

# Alert Status
ALERT_OK = "Ok"
ALERT_LOW = "Low"
ALERT_MEDIUM = "Medium"
ALERT_HIGH = "High"


# Measure Status
PENDING = "pending"
PASSED = "passed"
FAILED = "failed"
SUCCESS = "success"


# Measure Types
RELIABILITY = "reliability"
HEALTH = "health"
PROFILE = "profile"
BASIC = "basic"
STATISTICS = "statistics"
FREQUENCY = "frequency"
DISTRIBUTION = "distribution"
BUSINESS_RULES = "business"
BEHAVIORAL = "behavioral"
LOOKUP = "lookup"
CUSTOM = "custom"
SEMANTIC_MEASURE = "semantic"
CONDITIONAL = "conditional"
PARAMETER = "parameter"
QUERY = "query"
LENGTH = "length"
PATTERN = "pattern"
ENUM = "enum"
RANGE = "range"
SEMANTICS = "semantics"
GROUPING = "grouping"
CROSS_SOURCE = "cross_source"
EXPORT_FAILED_ROWS = "export_failed_rows"
CATALOG_UPDATE = "catalog_update"
COMPARISON = "comparison"
USERACTIVITY = "user_activity"
METADATA = "metadata"
OBSERVE = "observe"
CATALOG_SCHEDULE = "catalog"
SYNCASSET = "sync"
USAGE_QUERY = "usage_query"
WORKFLOW = "workflow"
PROCESS = "process"
EXTRACT = "extract"
FILE_VALIDATION = "file_validation"
COST = "cost"
PERFORMANCE = "performance"
ADVANCED_MEASURES = [STATISTICS, FREQUENCY, DISTRIBUTION, BUSINESS_RULES, BEHAVIORAL, GROUPING, CROSS_SOURCE]
CUSTOM_MEASURES = [CUSTOM, SEMANTIC_MEASURE, BEHAVIORAL, BUSINESS_RULES, GROUPING, CROSS_SOURCE]
CUSTOM_MEASURE_CATEGORIES = [CONDITIONAL, QUERY, COMPARISON, PARAMETER, GROUPING, CROSS_SOURCE]

# Measure Levels
ASSET = "asset"
ATTRIBUTE = "attribute"

# Depth Types
DAYS = "days"
PERCENTAGE = "percentage"

# Notification Priority
HIGH = "high"
MEDIUM = "medium"
LOW = "low"

# Datatype Categories
DATE = "date"
NUMERIC = "numeric"
TEXT = "text"
BIT = "bit"
JSON = "json"
VARIANT = "variant"
ARRAY = "array"
OBJECT = "object"

# Schedule Types
MINUTES = "minutes"
HOURS = "hours"
DAYS = "days"
WEEKS = "weeks"
MONTHS = "months"

# attribute metadata columns
ATTRIBUTE_RANGE_COLUMNS = ["min_value", "max_value"]
ATTRIBUTE_LENGTH_RANGE_COLUMNS = ["min_length", "max_length"]
FAILED_ROWS_METADATA_TABLES = [
    "CONNECTION_METADATA",
    "ASSET_METADATA",
    "ATTRIBUTE_METADATA",
    "MEASURE_METADATA",
]
MEASURE_METADATA = "MEASURE_METADATA"
ASSET_METADATA = "ASSET_METADATA"
ATTRIBUTE_METADATA = "ATTRIBUTE_METADATA"
CONNECTION_METADATA = "CONNECTION_METADATA"

# Tracking Attribute Metadata Properties
PROPERTIES = {
    "length": ["min_length", "max_length"],
    "range": ["min_value", "max_value"],
    "constraints": ["is_null", "is_blank", "is_unique", "is_primary_key"],
}

# Collibra Constants
collibra_dq_score_type_id = "00000000-0000-0000-0000-090000010027"

task_categories = [
    SEMANTICS,
    RELIABILITY,
    PROFILE,
    STATISTICS,
    CUSTOM,
    BEHAVIORAL,
    EXPORT_FAILED_ROWS,
    CATALOG_UPDATE,
    NOTIFICATION,
    USERACTIVITY,
    CATALOG_SCHEDULE,
    METADATA,
    SYNCASSET,
    USAGE_QUERY,
    OBSERVE,
    WORKFLOW,
    PROCESS,
    EXTRACT
]

# Tableau Connection Type Mapping
TABLEAU_CONNECTION_TYPE_MAPPING = {
    "sqlserver": "mssql",
    "snowflake": "snowflake",
    "redshift": "redshift",
    "denodo": "denodo",
    "excel-direct": "excel-direct",
}

# Tableau Connection Type Mapping
POWERBI_CONNECTION_TYPE_MAPPING = {
    "sql": "mssql",
    "snowflake": "snowflake",
    # "redshift": "redshift",
    # "denodo": "denodo"
}

SIGMA_CONNECTION_TYPE_MAPPING = {
    "sqlserver": "mssql",
    "snowflake": "snowflake",
    "redshift": "redshift",
    "databricks": "databricks",
    "mysql": "mysql",
    "postgres": "postgresql",
    "bigquery": "bigquery",
    "alloydb": "alloydb"
}

SIGMA_CHART_BASED_COLUMN_KEYS = {
    "bar": ["axis", "series", "colorScale", "tooltip", "row", "column", "label", "color"],
    "line": ["color", "axis", "series", "tooltip", "row", "column", "label"],
    "combo": ["axis", "color", "column", "row", "series", "tooltip", "label"],
    "area": ["column", "row", "color", "axis", "series", "label", "tooltip"],
    "kpi": ["axis", "value", "tooltip", "comparison"],
    "donut": ["color", "row", "column", "series", "tooltip"],
    "scatter": ["label", "color", "column", "row", "series", "tooltip", "colorScale"],
    "box": ["axis", "series", "tooltip", "splitby"],
    "waterfall": ["axis", "series", "tooltip"],
    "sankey": ["tooltip", "stages", "value"],
    "funnel": ["stage", "colorScale", "tooltip", "series", "color"],
    "gauge": ["target", "maximum", "minimum", "value"],
    "region": ["color", "label", "tooltip", "region", "colorScale"],
    "latLong": ["latitude", "color", "size", "label", "tooltip", "longitude", "colorScale"],
    "geography": ["color", "tooltip", "geography", "colorScale"],
    "levelTable": ["base", "levels"],
    "pivotTable": ["row", "column", "value"],
}

# SMTP TYPES
SEND_GRID = "send_grid"
AWS_SES = "aws_ses"
GMAIL = "gmail"
OUTLOOK = "outlook"
PROOFPOINT = "proofpoint"

DEFAULT_AIRFLOW_HOSTNAME = "http://localhost:8080"

# Custom Comparison Measures as stored in backend
FRESHNESS = "freshness"
VOLUME = "row_count"
SCHEMA = "column_count"
DUPLICATES = "duplicate_count"
MEASURE_CATEGORIES = {
    "length": ["min_length", "max_length", "length_range"],
    "value": ["min_value", "max_value", "value_range"],
    "value_frequency": ["enum"],
    "length_frequency": ["length"],
    "health": [
        "distinct_count",
        "null_count",
        "zero_values",
        "space_count",
        "blank_count",
    ],
    "completeness": ["null_count", "non_empty", "space_count", "blank_count"],
    "uniqueness": ["distinct_count", "duplicate"],
    "character": [
        "digits_count",
        "characters_count",
        "special_char_count",
        "whitespace_count",
        "alpha_numeric_count",
    ],
    "space": ["leading_space", "trailing_space", "outer_space", "inner_space", "no_space"],
    "numeric": ["positive_count", "negative_count", "zero_values"],
    "short_universal_patterns": ["long_pattern", "short_pattern"],
    "user_defined_patterns": ["pattern"],
    "statistics": [
        "range",
        "margin_of_error",
        "q1",
        "q3",
        "sum",
        "mean",
        "median",
        "standard_deviation",
        "variance",
        "skewness",
        "kurtosis",
    ],
}

# Reporting Export Settings Variables
EXPORT_GROUP_FAILED_ROWS_TABLES = ["MEASURE_DATA", "ASSET_DATA"]
REPORT_EXPORT_GROUP_INDIVIDUAL = "individual"
REPORT_EXPORT_GROUP_MEASURES = "measures"
REPORT_EXPORT_GROUP_ASSET_MEASURES = "assets_and_measures"
REPORT_EXPORT_GROUP_DOMAINS = "domains"

IDENTIFIER_KEY_COLUMN_NAME = "IDENTIFIER_KEY"

# Error messages
CATALOG_DISABLED_ERROR = "Please configure a channel in the settings/integration to run catalog update tasks."
REPORTING_DISABLED_ERROR = "Please enable push down metrics in the settings -> platform -> configuration -> remediate section to run export failed row tasks."
SEMANTICS_DISABLED_ERROR = "Please enable semantic discovery in the settings -> platform -> configuration -> discover section to run semantics tasks."
DEPRECATED_ASSET_ERROR = "Could not be able to run the jobs for the deprecated data."
WATERMARK_COLUMN_MISSING_ERROR = "The watermark column '{column_name}' is updated or deleted for the asset '{asset_id}'. Job execution stopped."

DIRECT_QUERY_BASE_TABLE_LABEL = "<base_table>"

# Constants types for adf datatsets
# Reference : https://learn.microsoft.com/en-us/rest/api/datafactory/datasets/get?view=rest-datafactory-2018-06-01&tabs=Python#definitions
ADF_DATASETS_TYPES = {
    "SnowflakeV2Table": "snowflake",
    "PostgreSqlV2Table": "postgresql",
    "Db2TableDataset": "db2",
    "OracleTableDataset": "oracle",
    "SnowflakeDataset": "snowflake",
    "AmazonS3Dataset": "s3select",
    "AzureDatabricksDeltaLakeDataset": "databricks",
    "PostgreSqlTableDataset": "postgresql",
    "PostgreSqlV2TableDataset": "postgresql",
    "SapHanaTableDataset": "saphana",
    "MySqlTableDataset": "mysql",
    "SnowflakeV2Dataset": "snowflake",
    "SqlServerTable": "mssql",
    "AzureSqlDWTable": "synapse",
}
STOPPED_OR_STALLED_STATES = [
    "stopped_stage_altered",
    "stopped_stage_dropped",
    "stopped_file_format_dropped",
    "stopped_notification_integration_dropped",
    "stopped_missing_pipe",
    "stopped_missing_table",
    "stalled_compilation_error",
    "stalled_initialization_error",
    "stalled_execution_error",
    "stalled_internal_error",
    "stalled_stage_permission_error",
]

# Report Notification
SHARE_WIDGET = "share_widget"
SHARE_DASHBOARD = "share_dashboard"
REPORT = "report"
DASHBOARD = "dashboard"

# ASSET GROUP
ASSET_GROUP_DATA = "data"
ASSET_GROUP_REPORT = "report"
ASSET_GROUP_PIPELINE = "pipeline"

# ASSET TYPE
ASSET_TYPE_VIEW = "view"
ASSET_TYPE_TASK = "task"
ASSET_TYPE_PIPE = "pipe"
ASSET_TYPE_PROCEDURE = "stored procedure"


# WORKFLOW
CREATE_ISSUE = "create_issue"
CREATE_ALERT = "create_alert"
CREATE_ASSET = "create_asset"
UPDATE_ASSET = "update_asset"
JOB_EXECUTION = "job_execution"

# Atlan Custom Metadata Properties
ATLAN_CUSTOM_METADATA_PROPERTIES = ["DQ Score", "Total Rows", "Freshness", "Tables", "Views", "Attributes", "Pipelines", "Reports", "Users", "Active Rules", "Observed Rules", "Scoring Rules", "Duplicate Rows", "Alerts", "Issues", "Last Updated Date", "DqLabs Alerts Url", "DqLabs Issues Url", "DqLabs Measures Url"]
NEW_ATLAN_CUSTOM_METADATA_PROPERTIES = ["Tables", "Views", "Attributes", "Pipelines", "Reports", "Users"]

MAX_RETRY_DURATION = 1800  # 30 minutes

# Dashboard IDs
EXCEPTION_DASHBOARD_ID = "a62065e6-3790-4e74-875a-631737a37a49"