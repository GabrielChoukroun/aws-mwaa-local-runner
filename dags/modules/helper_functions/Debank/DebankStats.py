from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import json
from .Debank import Debank

class DebankStats:
    def __init__(self):
        self.Debank = Debank()

    def getAllAccountBalances(self, addresses):
        pass
        with ThreadPoolExecutor() as executor:
            threads = [executor.submit(self.getAllChainBalances, acc_name) 
                        for acc_name in addresses]
            response = [f.result() for f in threads]
        if any([type(r) == pd.DataFrame for r in response]):
            df = pd.concat(response, axis=0).reset_index(drop=True)
            df.columns = ['ADDRESS', 'CURRENCY', 'USD_VALUE']
            if len(df) > 0: print(f'first row: {df.head(1)}')
            return df
        else: return pd.DataFrame()

    def getAllChainBalances(self, acc_name):
        """Since we cannot use the get_used_chains endpoint, I searched for 
        currencies which are used by any accounts, which are these. There are 
        30 potential currencies which could be used, but most of those calls will 
        give a value of 0"""
        currencies = ['heco', 'matic', 'eth', 'avax', 'ftm', 'bsc']
        response = [self.getOneChainBalance(acc_name, curr) for curr in currencies]
        df = pd.concat(response, axis=0).reset_index(drop=True)
        return df

    def getOneChainBalance(self, acc_name, currency):
        endpoint = f'v1/user/chain_balance?id={acc_name}&chain_id={currency}'
        response = self.Debank.executeRequest(endpoint)
        if response.status_code == 200:
            chain_balance = response.json()['usd_value']
            data = [[acc_name, currency, chain_balance]]
            df = pd.DataFrame(data)
        else:
            df = pd.DataFrame()
        return df
    
    def getCurrenciesForAccount(self, acc_name):
        endpoint = f'v1/user/used_chain_list?id={acc_name}'
        response = self.Debank.executeRequest(endpoint)
        return [responseDict['id'] for responseDict in response.json()['result']]


    def get_addresses(self, tableName, connector, offset, parallelOffset):
        """the statement gets all CeFi addresses from fireblocks using the
        workspace ID for CeFi addresses"""
        if tableName == "EXTERNAL_MANAGED_ADDRESSES":
            statement = """
                        SELECT ADDRESS::varchar AS ADDRESS FROM EXTERNAL_MANAGED_ADDRESSES
                        """
        else:
            statement = f"""
            SELECT ADDRESS::varchar AS ADDRESS FROM FIREBLOCKS_ADDRESSES
            WHERE WORKSPACE_ID = 'f5133331-05bd-4d37-8bd2-7ae6b45b264f'
            LIMIT {parallelOffset}
            OFFSET {offset}
            """
        print(f'addresses Statement: {statement}')
        addresses = pd.read_sql(statement, connector)['ADDRESS']
        return addresses.tolist()