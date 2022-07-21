import ast
import json

import numpy as np
import pandas as pd
from fireblocks_sdk import FireblocksSDK
from fireblocks_sdk.api_types import FireblocksApiException
from airflow.models import Variable

from .models import TRANSACTION_MODEL


class FireblocksWrapper:
    def __init__(self, API_KEY, WORKSPACE_NAME):
        self.fb = FireblocksSDK(Variable.get("fireblocks_secret.key"), API_KEY)
        self.workspace_name = WORKSPACE_NAME

    def _remove_pii(self, x):
        source = ast.literal_eval(x['source']) if type(x['source'])==str else x['source']
        if source['type'] in ['INTERNAL_WALLET', 'EXTERNAL_WALLET']:
            source['name']=''
            
        destination = ast.literal_eval(x['destination']) if type(x['destination'])==str else x['destination']
        if destination['type'] in ['INTERNAL_WALLET', 'EXTERNAL_WALLET']:
            destination['name']=''
            
        return source, destination

    def _parse_transactions(self, transactions, clean):
        df = pd.DataFrame(transactions)

        df = df.replace({np.nan: None})
        columns = df.columns.tolist()

        for x in ['sourceType', 'sourceName', 'destinationType', 'destinationName']:
            try:
                columns.remove(x)
            except ValueError as e:
                pass
            
        df = df[columns]

        if clean:
            df['source'], df['destination'] = zip(*df.apply(self._remove_pii, axis=1))

        for key, value in TRANSACTION_MODEL.items():
            if key not in df.columns:
                df.loc[:, key] = pd.Series([value()] * len(df)) if value in [dict, list] else None
            else:
                df[key] = df[key].astype(str)
                if value in [dict, list]: df[key] = df[key].apply(ast.literal_eval)
                elif value=='datetime': df[key] = pd.to_datetime(df[key], unit='ms', utc=True)
                elif value in [int, float]: df[key] = pd.to_numeric(df[key], errors='coerce')
                else: df[key] = df[key].apply(value)

                if key=='extraParameters':
                    df[key] = df[key].apply(lambda x: {k:str(v) for k,v in x.items()} if x is not None else x)
                        
        df = df[TRANSACTION_MODEL.keys()]

        # Convert empty dict to None
        df = df.replace([{}], [None])

        # Convert empty list to NaN
        df = df.mask(df.applymap(str).eq('[]'))

        # Convert any None strings to NaN
        df = df.mask(df.applymap(str).eq('None'))

        # Convert np.nan to None
        df = df.replace({np.nan: None})

        df.columns = list(map(lambda x: x.upper(), df.columns))

        return df

    def _get_addresses(self, x):
        retry = 0
        while retry<3:
            #Retry thrice before raising Exception and exiting
            try:
                #Attempt to get addresses
                return self.fb.get_deposit_addresses(int(x['id']), x['assetid'])
            except FireblocksApiException as e:
                #If the request has expired, initialize the object token again and retry
                error = e.args[0].split("server: ")[1]
                error = json.loads(error.replace("'", '"'))
                if type(error)!=int:
                    if error["message"] == "The request has expired":
                        retry+=1
                elif error==500:
                    retry+=1
                else: raise FireblocksApiException(e.args[0])

    def get_historical_transactions(self, after, before, status, parse=False, clean=False):
        response = self.fb.get_transactions_with_page_info(
                after=after,
                before=before,
                limit=500, 
                status=status
            )

        transactions = []
        transactions += response['transactions']

        prevPage=response['pageDetails']['nextPage']
        while True:
            try:
                response = self.fb.get_transactions_with_page_info(next_or_previous_path=prevPage)
                transactions += response['transactions']
                if response['pageDetails']['nextPage']=='':
                    break
                else:
                    prevPage=response['pageDetails']['nextPage']
            except Exception as e:
                print(e)
        
        if len(transactions)==0:
            return None

        if parse:
            return self._parse_transactions(transactions, clean)
        return transactions

    def get_balances(self):
        return self.fb.get_vault_accounts(min_amount_threshold=0)
        
    def get_exchange_balances(self):
        return self.fb.get_exchange_accounts()

    def get_all_addresses(self):
        balances = self.get_balances()

        df = pd.DataFrame(balances)

        df = df.loc[~((df.name.str.contains('User')) & (df.name.str.contains('Deposit'))) & ~(df.name=='_Users')]

        df = df.set_index(['id', 'name'])['assets'].explode().apply(pd.Series)[['id']]

        df = df.rename(columns={
            'id': 'assetid'
        }).reset_index()

        df['addresses'] = df.apply(self._get_addresses, axis=1)

        df = df.mask(df.applymap(str).eq('[]'))
        df = df.replace({np.nan: None})

        df = df.rename(columns={
            'id': 'vault_id',
            'name': 'vault_name'
        })
        
        df.columns = list(map(lambda x: x.upper(), df.columns))

        return df
