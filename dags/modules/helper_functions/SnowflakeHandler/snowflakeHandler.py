from modules.helper_functions.SnowflakeHandler.config import getSnowSQLQueries
from snowflake import connector as snowflake_connector
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
# from snowflake.sqlalchemy import URL
# from sqlalchemy import create_engine
from sqlalchemy.exc import DBAPIError, ResourceClosedError
from airflow.models import Variable
import traceback
import pandas as pd
import time
from airflow.models import Variable
import base64


import logging

class Snowflake:
    def __init__(self, flag="PRODUCTION"):
        self.logger = logging.getLogger("airflow-task")
        self.logger.setLevel(logging.INFO)
        self.SNOWSQL_QUERIES = getSnowSQLQueries()
        self.CREDS = Variable.get
        self.flag = flag

    def createConnection(self, warehouse, database, schema, connectorType="Snowflake"):
        """
        Create an engine and connect to Snowflake using SQLAlchemy
        """
        
        connectors = {
            "Snowflake": self._initSnowflakeConnector,
        }
        return connectors[connectorType](warehouse, database, schema)

    def _initSnowflakeConnector(self, warehouse, database, schema):
        """
        Creates a connection using Snowflake Python connector and returns the connection
        """
        if self.flag=="PRODUCTION":
            print("hello production")
            ctx = snowflake_connector.connect(
                account = self.CREDS("SNOWFLAKE_ACCOUNT"),
                user = self.CREDS("SNOWFLAKE_USER"),
                private_key = self._getKey(),
                host = self.CREDS("SNOWFLAKE_HOST"),
                warehouse = warehouse,
                database = database,
                schema = schema
            )
        else:
            ctx = snowflake_connector.connect(
                account = self.CREDS("SNOWFLAKE_ACCOUNT"),
                user = self.CREDS("SNOWFLAKE_USER"),
                password = self.CREDS("SNOWFLAKE_PASSWORD"),
                host = self.CREDS("SNOWFLAKE_HOST"),
                warehouse = warehouse,
                database = database,
                schema = schema
            )


        return ctx

    def _getKey(self):
        """
        Get Private key to authenticate Snowflake requests
        """
        _key = serialization.load_pem_private_key(base64.b64decode(Variable.get("rsa_key.p8")), password = self.CREDS("SNOWFLAKE_RSA").encode(), backend = default_backend())
        _pubKey = _key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        return _pubKey

    def _executeSnowSQL(self, listOfStatements, connector, connectorType):
        STATUS = False
        if connectorType == "SQLAlchemy":
            with connector.connect().execute_options(autocommit=False) as connection:
                try:
                    connection.execute("BEGIN")
                    # Execute all SnowSQL statements in order from the provided list
                    for statement in listOfStatements:
                        connection.execute(statement = statement)
                    connection.execute("COMMIT")
                    STATUS = True
                except Exception as err:
                    if type(err) == ResourceClosedError:
                        self.logger.warn("Resource to Snowflake was closed unexpectedly, retrying to connect...", extras={"processLevel": "_executeSnowSQL"})
                        # If the connection resource was closed for some reason, reconnect and execute the request
                        self._createConnection()
                        return self._executeSnowSQL(statement)
                    elif type(err) == DBAPIError and err.connection_invalidated:
                        self.logger.warn("DBAPIError or Connection was invalidated, retrying to connect...", extras={"processLevel": "_executeSnowSQL"})
                        # If the connection weas invalidated due to time out or other reasons, reconnect and execute the request
                        self._createConnection()
                        return self._executeSnowSQL(statement)
                    # If the execution fails for any other reason, raise the error and log to Logz.io
                    else:
                        #Rollback transaction on error
                        connection.execute("ROLLBACK")
                        STATUS = False
                        self.logger.error(err, extra = {"traceback": traceback.format_exc(), "processLevel": "_executeSnowSQL"})
                finally:
                    connection.close()
                    connector.dispose()
        elif connectorType == "Snowflake":
            with connector.cursor() as connection:
                try:
                    # Begin a transaction
                    connection.execute("BEGIN")
                    # Execute all SnowSQL statements in order from the provided list
                    for statement in listOfStatements:
                        if type(statement) == list:
                            # statements of type list are internal stage PUT statements along with file buffer that needs to be uploaded
                            connection.execute(statement[0], file_stream = statement[1])
                        else: connection.execute(statement = statement)
                    # Commit changes finally
                    connection.execute("COMMIT")
                except Exception as err:
                    connection.execute("ROLLBACK")
                    self.logger.error(err, extra = {"traceback": traceback.format_exc(), "processLevel": "_executeSnowSQL"})
                finally:
                    # Close the connection and connector
                    connection.close()
                    connector.close()
        return STATUS
            

    def PutOnInternalStage(self, fileName, stagePath, fileStream, warehouse, database, schema):
        """
        PUT provided {fileName} buffer into respective {stagePath} internal stage
        Parameters
        ----------
        fileName : str
            Name of the file that needs to be uploaded
        stagePath: str
            Path of the internal stage where the file needs to be uploaded
        fileStream: io.BytesIO
            io.BytesIO object which contains the buffer for the file that needs to be uploaded
        warehouse: str
            Name of the warehouse to use
        database: str
            Name of the database to use
        schema: str
            Name of the schema to use
        """
        putQuery = self.SNOWSQL_QUERIES['PUT_FILE'].format(fileName, stagePath)

        connector = self.createConnection(warehouse, database, schema, "Snowflake")
        with connector.cursor() as connection:
            try:
                connection.execute("BEGIN")
                result = connection.execute(putQuery, file_stream=fileStream).fetchone()
                connection.execute("COMMIT")
                return result
            except Exception as err:
                connection.execute("ROLLBACK")
                self.logger.error(err, extra = {"traceback": traceback.format_exc(), "processLevel": "_executeSnowSQL"})
            finally:
                connection.close()
                connector.close()

    def executeSnowSQL(self, listOfStatements, connector):
        """
        Execute a SnowSQL query on Snowflake and return sqlalchemy.engine.cursor
        Parameters
        ----------
        listOfStatements : list
            SnowSQL compatible queries which needs to be executed on Snowflake.
            For PUT statements to upload a file to a stage, structure a list containing the query and the file buffer and include it in the listOfStatements, for example:
                ['CREATE DATABASE test_database;', ['PUT file://test.csv @TEST_STAGE;', io.BytesIO object]]
        """
        return self._executeSnowSQL(listOfStatements, connector)

    def insertFromPandasDF(self, df, target_table, connector, 
                            database_name="RECON", schema_name="PUBLIC"):
        """
        Insert a pandas dataframe into a given target table, 
        by means of loading into a temp table, then merging into the target
        
        Parameters
        ----------
        df: pandas.DataFrame
            dataframe object containing the rows which need to be inserted
        target_table: String
            string containing the name of the table in Snowflake into which you insert data
        connector: Snowflake().connector.connect() object
            active connection to the database
        """

        connection = connector.cursor()

        # Create the temp table
        temporary_table_name = '_'.join([target_table, "TEMP"])

        tempTableCreate = self.SNOWSQL_QUERIES["CREATE_TEMP_TABLE_FROM_TARGET"].format(temporary_table_name, target_table)
        print(f"temp table statement: {tempTableCreate}")
        connection.execute(tempTableCreate)
        print(f'df columns: {df.columns}')
        print(f'length table: {len(df)}')
        df.columns = [str(col).upper() for col in df.columns]
        if len(df) == 0: # If no output was returned, break
            success = False
            nchunks = 0
            nrows = 0
            output = False
            return success, nchunks, nrows, output
        # Load pandas dataframe into the temp table
        success, nchunks, nrows, output = write_pandas(
                                            conn=connector, 
                                            df=df, 
                                            table_name=temporary_table_name)
        # merge the temp table into the target_table
        table_columns = pd.read_sql(self.SNOWSQL_QUERIES['GET_COLUMNS'].format(target_table), con = connector)
        column_list = table_columns['column_name'].tolist()
        primary_keys = pd.read_sql(self.SNOWSQL_QUERIES['GET_PK'].format(database_name, schema_name, target_table), con = connector)
        key_list = primary_keys['column_name'].tolist()
        constraints = ' AND '.join([f"{target_table}.{column} = {temporary_table_name}.{column}"
                                    for column in key_list])
        whenMatchedSet = ', '.join([f"{target_table}.{column} = {temporary_table_name}.{column}" for column in column_list])
        insertColumnNames = ', '.join(column_list)
        insertValues = ', '.join([temporary_table_name + f'.{column}' for column in column_list])
        print(f"column list: {column_list}")
        print(f'insertColumnNames: {insertColumnNames}')
        print(f"constraints: {constraints}")
        print(f"insertValues: {insertValues}")
        print(f"whenMatchedSet: {whenMatchedSet}")
        tempTargetMerge = self.SNOWSQL_QUERIES['MERGE_TEMP_INTO_TARGET'].format(target_table, temporary_table_name, 
                                                constraints, whenMatchedSet, insertColumnNames, 
                                                insertValues)
        print(f"tempTargetMerge: {tempTargetMerge}")
        connection.execute(tempTargetMerge)
        # Drop the temp table
        dropTemp = self.SNOWSQL_QUERIES["DROP_TABLE"].format(temporary_table_name)
        connection.execute(dropTemp)
        # return the success status
        return success, nchunks, nrows, output

    def replaceFromPandasDF(self, df, target_table, connector, 
                            database_name="RECON", schema_name="PUBLIC"):
        """
        Insert a pandas dataframe into a given target table, 
        by means of loading into a temp table, then merging into the target
        
        Parameters
        ----------
        df: pandas.DataFrame
            dataframe object containing the rows which need to be inserted
        target_table: String
            string containing the name of the table in Snowflake into which you insert data
        connector: Snowflake().connector.connect() object
            active connection to the database
        """

        connection = connector.cursor()

        df.columns = [str(col).upper() for col in df.columns]
        if len(df) == 0: # If no output was returned, break
            success = False
            nchunks = 0
            nrows = 0
            output = False
            return success, nchunks, nrows, output
        # drop current values
        self.truncateTable(target_table, connector)
        # insert new values
        success, nchunks, nrows, output = write_pandas(
                                            conn=connector, 
                                            df=df, 
                                            table_name=target_table)
        # return the success status
        return success, nchunks, nrows, output

    def createNewTableFromSource(self, new_table_name, source_table_name, connector):
        createNewTable = self.SNOWSQL_QUERIES['CREATE_NEW_TABLE_FROM_SOURCE'].format(new_table_name, source_table_name)
        print(f"new table creation: {createNewTable}")
        connection = connector.cursor()
        connection.execute(createNewTable)

    def truncateTable(self, target_table, connector):
        truncateTable = self.SNOWSQL_QUERIES['TRUNCATE_TABLE'].format(target_table)
        connection = connector.cursor()
        connection.execute(truncateTable)

    def getOrCreateMultipleIDs(self, df, subAccountColName, exchangeName, connector):
        quoted_exchange_name = f'\'{exchangeName}\''
        connection = connector.cursor()
        # Get all subaccounts and find the ones which aren't in the sub accounts table
        getAllSubs = self.SNOWSQL_QUERIES['SELECT_COLUMN'].format("*", exchangeName)
        subAccountsdf = pd.read_sql(getAllSubs, connector)
        newAccounts = set(df[subAccountColName]).difference(set(subAccountsdf['ADDRESS']))
        # If there are any accounts not in the subaccounts table, insert them
        if newAccounts:
            valueList = [f'(\'{acc_name}\', {quoted_exchange_name})' for acc_name in newAccounts]
            columns = ', '.join(['NAME', 'EXCHANGE'])
            values = ', '.join(valueList)
            newAccs = self.SNOWSQL_QUERIES['INSERT_NEW_ROWS'].format("SUBACCOUNTS", columns, values)
            print(newAccs)
            connection.execute(newAccs)
            time.sleep(3)
        # join the subAccounts to the main DF
        getNamesAndID = self.SNOWSQL_QUERIES['SELECT_COLUMN'].format("NAME, ID", "SUBACCOUNTS")
        print(f'names and ID query: {getNamesAndID}')
        IDdf = pd.read_sql(getNamesAndID, connector)
        print(f'IDdf columns:\n {IDdf.head(1)}')
        nameToIDDict = dict(zip(IDdf['NAME'], IDdf['ID']))
        df['SUBACCOUNT_ID'] = df['ADDRESS'].apply(lambda x: nameToIDDict[x])
        print(f'final df head before returning:\n{df.head(2)}')
        
        return df

    def getOrCreateID(self, acc_name, table_name, column_name, exchange_name, connector):
        quoted_acc_name = f'\'{acc_name}\''
        quoted_exchange_name = f'\'{exchange_name}\''
        getAccName = self.SNOWSQL_QUERIES['SELECT_WHERE_VALUE_EQUALS'].format(table_name, column_name, quoted_acc_name)
        print(f'getAccName: {getAccName}')
        connection = connector.cursor()
        df = pd.read_sql(getAccName, con = connector)
        if len(df) == 0:
            columns = ', '.join(['NAME', 'EXCHANGE'])
            value_list = ', '.join([quoted_acc_name, quoted_exchange_name])
            values = f'({value_list})'
            createNewAccount = self.SNOWSQL_QUERIES['INSERT_NEW_ROWS'].format(table_name, columns, values)
            print(f'new account insert: {createNewAccount}')
            connection.execute(createNewAccount)
            time.sleep(3)
            df = pd.read_sql(getAccName, con = connector)
        return df.at[0,'ID']

    def getOrCreateExplorerID(self, table_name, column_name, explorer_name, blockchain_name, connector):
        quoted_explorer_name = f'\'{explorer_name}\''
        quoted_blockchain_name=f'\'{blockchain_name}\''
        getExplorerName = self.SNOWSQL_QUERIES['SELECT_WHERE_VALUE_EQUALS'].format(table_name, column_name, quoted_explorer_name)
        print(f'getExplorerName: {getExplorerName}')

        df = pd.read_sql(getExplorerName, con = connector)
        if len(df) == 0:
            columns = ', '.join(['NAME', 'BLOCKCHAIN'])
            value_list = ', '.join([quoted_explorer_name, quoted_blockchain_name])
            values = f'({value_list})'
            createNewExplorer = self.SNOWSQL_QUERIES['INSERT_NEW_ROWS'].format(table_name, columns, values)
            print(f'new explorer insert: {createNewExplorer}')
            connector.execute(createNewExplorer)
            time.sleep(3)
            df = pd.read_sql(getExplorerName, con = connector)
        return df.at[0,'ID']

    def insertValues(self, tableName, columnList, valueList, connection):
        columns = ', '.join(columnList)
        valueList = [f'\'{val}\'' for val in valueList]
        values = f'({valueList})'
        createNewAccount = self.SNOWSQL_QUERIES['INSERT_NEW_ROWS'].format(tableName, columns, values)
        print(f'new account insert: {createNewAccount}')
        connection.execute(createNewAccount)
        time.sleep(3)

    def getAllColumnValues(self, table_name, column_name, connector, filter_value=None):
        if filter_value is None:
            getColumn = self.SNOWSQL_QUERIES['SELECT_COLUMN'].format(column_name, table_name)
        else:
            getColumn = self.SNOWSQL_QUERIES['SELECT_WHERE_VALUE_EQUALS'].format(table_name, column_name, filter_value)
        print(f'getColumn: {getColumn}')
        df = pd.read_sql(getColumn, con = connector)
        return df[column_name].tolist()

    def getFB_ID(self, table_name, column_name, workspace_name, connector):
        get_id_query = self.SNOWSQL_QUERIES['SELECT_WHERE_VALUE_EQUALS'].format(table_name, column_name, f"'{workspace_name}'")
        print(f'get_id_query: {get_id_query}')
        id = pd.read_sql(get_id_query, con = connector)
        if id.empty:
            add_id_query = self.SNOWSQL_QUERIES['INSERT_NEW_ROWS'].format(table_name, column_name, f"('{workspace_name}')")
            print(f'new workspace insert: {add_id_query}')
            with connector.cursor() as connection:
                connection.execute(add_id_query)
            id = pd.read_sql(get_id_query, con = connector)

        return id.at[0, 'ID']

    def alterSession(self, connector, setOrUnset, parameter, value=""):
        alterSessionQuery = self.SNOWSQL_QUERIES['ALTER_SESSION'].format(setOrUnset, parameter, value)
        with connector.cursor() as connection:
            connection.execute(alterSessionQuery)
        return

    def createTableFromDF(self, connector, table_name, df, primary_key_name = None, constraint = None):
        dic = {'int64':'int', 'float64':'float', 'bool': 'boolean', 
                'object': 'string'}
        mapColToType = []
        print(f'create table df head:\n{df.head(1)}')
        for col in df.columns:
            # print(f'createTableFromDF col type: {type(df[col])}')
            print(f'col: {col}')
            # print(f'df col\n{df[col]}')
            dtype = str(df[col].dtype)
            mapColToType.append(f'{col} {dic[dtype]}')
            if col == primary_key_name:
                mapColToType[-1] += " PRIMARY KEY"
        colTypeString = ',\n'.join(mapColToType)
        if constraint: 
            colTypeString += f',\n{constraint}'
        createTableQuery = self.SNOWSQL_QUERIES['CREATE_TABLE'].format("IF NOT EXISTS", table_name, colTypeString, constraint)
        print(f'mapColToType: {mapColToType}\ncolTypeString: {colTypeString}\n cols: {df.columns}')
        print(f'createTableQuery in createTableFromDF: {createTableQuery}')
        with connector.cursor() as connection:
            connection.execute(createTableQuery)
        return