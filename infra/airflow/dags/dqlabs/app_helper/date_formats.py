from dqlabs.enums.connection_types import ConnectionType

DATE_FORMATS = {
    ConnectionType.BigQuery.value: {
        "YYYYMMDD": "%Y%m%d",
        "DDMMYYYY": "%d%m%Y",
        "YYYY-MM-DD": "%Y-%m-%d",
        "DD-MM-YYYY": "%d-%m-%Y",
        "YYYY-MM-DD HH:MI:SS": "%Y-%m-%d %H:%M:%S",
    },
    ConnectionType.MSSQL.value: {
        "DDMMYYYY": "105",
        "YYYYMMDD": "112",
        "YYYY-MM-DD": "23",
        "DD-MM-YYYY": "105",
        "YYYY-MM-DD HH:MI:SS": "120",
    },
    ConnectionType.Postgres.value: {
        "DDMMYYYY": "DD-MM-YYYY",
        "YYYYMMDD": "YYYYMMDD",
        "YYYY-MM-DD": "YYYY-MM-DD",
        "DD-MM-YYYY": "DD-MM-YYYY",
        "YYYY-MM-DD HH:MI:SS": "YYYY-MM-DD HH24:MI:SS",
    },
    ConnectionType.MySql.value: {
        "YYYYMMDD": "%Y%m%d",
        "YYYY-MM-DD": "%Y-%m-%d",
        "DD-MM-YYYY": "%d-%m-%Y",
        "YYYY-MM-DD HH:MI:SS": "%Y-%m-%d %H:%i:%s",
        "DDMMYYYY": "%d%m%Y",
    },
    ConnectionType.AlloyDB.value: {
       "YYYYMMDD": "YYYYMMDD",
       "YYYY-MM-DD": "YYYY-MM-DD",
       "DD-MM-YYYY": "DD-MM-YYYY",
       "YYYY-MM-DD HH:MI:SS": "YYYY-MM-DD HH24:MI:SS",
       "DDMMYYYY": "DDMMYYYY",
    },
    ConnectionType.Teradata.value: {
       "YYYYMMDD": "YYYYMMDD",
       "YYYY-MM-DD": "YYYY-MM-DD",
       "DD-MM-YYYY": "DD-MM-YYYY",
       "YYYY-MM-DD HH:MI:SS": "YYYY-MM-DD HH24:MI:SS",
       "DDMMYYYY": "DDMMYYYY",
    },
    ConnectionType.MongoDB.value: {
        "YYYYMMDD": "%Y%m%d",
        "DDMMYYYY": "%d%m%Y",
        "YYYY-MM-DD": "%Y-%m-%d",
        "DD-MM-YYYY": "%d-%m-%Y",
        "YYYY-MM-DD HH:MI:SS": "%Y-%m-%d %H:%M:%S",
    },
}
