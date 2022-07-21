import time

import pandas as pd
import requests
from modules.helper_functions.SnowflakeHandler import Snowflake
from modules.helper_functions.snowtrace import SnowTrace
from airflow.models import Variable
from airflow.decorators import dag, task
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime as dt
from modules.helper_functions.google_conn import GoogleSheets

@dag(
    schedule_interval="@hourly",
    start_date=dt(2022, 1, 1),
    catchup=False,
    tags=['price_export', 'coingecko', 'paprika'],
    default_args = {
        'owner': 'Gabriel Choukroun',
        'retries': 0,
    },
    max_active_runs = 1,
)
def Coinstat_Push():

    def get_coinstat():
        recon_id = '1Sd_2S_W4XszBXexj-L0mcLPkgDuzQwB3_mczReDg-iI'
        gSheets = GoogleSheets(recon_id)
        # Call the Drive v3 API
        ranges = 'Coin Stats!A1:AV150'
        response = gSheets.batchGet(ranges=ranges)
        df = pd.DataFrame(response[0]['values'])
        title0 = df.loc[0].fillna('').tolist()
        title1 = df.loc[1].fillna('').tolist()
        title2 = df.loc[2].fillna('').tolist()
        titles = [title0, title1, title2]
        for title in titles:
            temp = ' '
            for i in range(len(title)):
                if title[i] != '':
                    temp = title[i]
                else:
                    title[i] = temp
        title0[0] = 'Coin'

        final_list = list(map(lambda x0, x1, x2: x0.upper() + '-' + x1.upper() + '-' + x2.upper(), title0, title1, title2))
        final_list = list(map(lambda col: col.replace('- - ', ''), final_list))
        final_list = list(map(lambda col: col.replace(' - ', '-'), final_list))
        final_list = list(
            map(lambda col: col.replace('ASSETS-EXCHANGES-FIREBLOCKS-CUSTODY!', 'ASSETS-FIREBLOCKS-CUSTODY'), final_list))
        final_list = list(map(lambda col: col.replace('ASSETS-DEFI BORROWS-ASSETS-DEFI BORROWS-COLLATERAL',
                                                      'ASSETS-DEFI BORROWS-COLLATERAL'), final_list))
        final_list = list(
            map(lambda col: col.replace('ASSETS-DEFI BORROWS-ASSETS-DEFI BORROWS-TOKENS', 'ASSETS-DEFI BORROWS-TOKENS'),
                final_list))
        final_list = list(
            map(lambda col: col.replace('ASSETS-DEFI ASSETS-ASSETS-DEFI ASSETS-ASSETS', 'ASSETS-DEFI ASSETS'), final_list))
        final_list
        df = df.loc[3:]
        df.columns = final_list
        df.at[3, 'COIN'] = 'TOTAL'
        df['TIMESTAMPUTC'] = int(dt.utcnow().timestamp())
        return df

    @task()
    def write_to_snowflake():
        snowflake = Snowflake()
        # Params: Warehouse, database, schema, connectorType: Snowflake/SQLAlchemy
        connector = snowflake.createConnection("RECON_WAREHOUSE", "RECON", "PUBLIC", "Snowflake")

        df = get_coinstat()
        if len(df.index) != 0:
            try:
                # write data into snowflake
                success, nchunks, nrows, output = write_pandas(conn=connector, df=df,
                                                               table_name="COINSTATS_HISTORY")
                print(success, nchunks, nrows, output)
            except Exception as err:
                raise err
    write_to_snowflake()

coinstat = Coinstat_Push()