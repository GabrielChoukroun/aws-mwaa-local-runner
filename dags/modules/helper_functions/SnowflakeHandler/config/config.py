def readFile(flag, filePath):
    queryPath = "modules/helper_functions/SnowflakeHandler/config/snowsql/"
    queryPath = "dags/" + queryPath if flag=="PRODUCTION" else queryPath
    return open(queryPath + filePath).read()

def getSnowSQLQueries(flag="PRODUCTION"):
    SNOWSQL_QUERIES = {
        "CREATE_TEMP_TABLE": readFile(flag, "CREATE_TEMP_TABLE.sql"),
        "PUT_FILE": readFile(flag, "PUT_FILE.sql"),
        "COPY_PARQUET": readFile(flag, "COPY_PARQUET.sql"),
        "MERGE_DMS_CDC": readFile(flag, "MERGE_DMS_CDC.sql"),
        "DROP_TABLE": readFile(flag, "DROP_TABLE.sql"),
        "REMOVE_FILE": readFile(flag, "REMOVE_FILE.sql"),
        "SHOW_TABLES": readFile(flag, "SHOW_TABLES.sql"),
        "SHOW_DATABASES": readFile(flag, "SHOW_DATABASES.sql"),
        "MERGE": readFile(flag, "MERGE.sql"),
        "CREATE_DATABASE": readFile(flag, "CREATE_DATABASE.sql"),
        "CREATE_TEMP_TABLE_FROM_TARGET": readFile(flag, "CREATE_TEMP_TABLE_FROM_TARGET.sql"),
        "MERGE_TEMP_INTO_TARGET": readFile(flag, "MERGE_TEMP_INTO_TARGET.sql"),
        "GET_COLUMNS": readFile(flag, "GET_COLUMNS.sql"),
        "GET_PK": readFile(flag, "GET_PK.sql"),
        "CREATE_NEW_TABLE_FROM_SOURCE": readFile(flag, "CREATE_NEW_TABLE_FROM_SOURCE.sql"),
        "TRUNCATE_TABLE": readFile(flag, "TRUNCATE_TABLE.sql"),
        "SELECT_WHERE_VALUE_EQUALS": readFile(flag, "SELECT_WHERE_VALUE_EQUALS.sql"),
        "INSERT_NEW_ROWS": readFile(flag, "INSERT_NEW_ROWS.sql"),
        "SELECT_COLUMN": readFile(flag, "SELECT_COLUMN.sql"),
        "ALTER_SESSION": readFile(flag, "ALTER_SESSION.sql"),
        "CREATE_TABLE":  readFile(flag, "CREATE_TABLE.sql")
    }
    return SNOWSQL_QUERIES