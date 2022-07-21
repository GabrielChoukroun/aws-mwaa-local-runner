from .Kraken import Kraken
import math
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

class KrakenStats():
    def __init__(self, API_KEY, API_SECRET):
        self.kraken = Kraken(API_KEY, API_SECRET)
    
    def getBalance(self):
        response = self.kraken.processRequest('/0/private/Balance', {})
        if response.status_code==200:
            return response.json()
        else:
            return None

    def processBalance(self):
        data = self.getBalance()
        df = pd.DataFrame.from_dict(data['result'], orient='index', columns=['balance']).reset_index()
        df['balance'] = df['balance'].astype(float)
        df = df.loc[data['balance']>0]
        df = df.rename(columns={'index': 'asset'})

        return df
    
    def getLedgerInfo(self, endpoint="/0/private/Ledgers"):
        
        key = 'ledger'
        
        ledgers = pd.DataFrame()

        response = self.kraken.processRequest(endpoint, {})
        count = int(response.json()['result']['count'])
        
        print(f"Available Txs: {count}")

        result = pd.DataFrame(response.json()['result'])[key].apply(pd.Series).reset_index()

        if len(result)>0:
            endTimestamp = math.ceil(result['time'].min())
            ledgers = pd.concat([ledgers, result])

            while True:
                response = self.kraken.processRequest(endpoint, {"end":endTimestamp})

                if not response.json()['result'][key]:
                    break

                result = pd.DataFrame(response.json()['result'])[key].apply(pd.Series).reset_index()

                if math.ceil(result['time'].min()) == endTimestamp:
                    break

                endTimestamp = math.ceil(result['time'].min())

                ledgers = pd.concat([ledgers, result])
                ledgers = ledgers.drop_duplicates('index', keep='first')
        ledgers.rename(columns = {'index': 'ID'}, inplace=True)
        ledgers.columns = [col.upper() for col in ledgers.columns]
        return ledgers

    
    def getAddressInfo(self):
        assetsAndMethodsDF = self.getDepositMethods()[['asset','method']]
        # with ThreadPoolExecutor() as executor:
        #     threads = [executor.submit(self.getOneAddress, 
        #     {'asset':row['asset'], 'method':row['method']}) 
        #     for index, row in assetsAndMethodsDF.iterrows()]
        #     response = [f.result() for f in threads]
        response = []
        for index, row in assetsAndMethodsDF.iterrows():
            argDict = {'asset':row['asset'], 'method':row['method']}
            res = self.getOneAddress(argDict)
            response.append(res)
        if any([type(r) == pd.DataFrame for r in response]):
            df = pd.concat(response, axis=0).reset_index(drop=True)
            return df
        else: 
            print(f'no response from getAddressInfo')
            return pd.DataFrame()

    def getOneAddress(self, AssetDict):
        endpoint = '/0/private/DepositAddresses'
        response = self.kraken.processRequest(endpoint, AssetDict)
        print(f'getOneAddress response: {response.json()}')
        return pd.DataFrame(response.json().get('result', {}))

    def getDepositMethods(self):
        assets = self.getAssetInfo()
        with ThreadPoolExecutor() as executor:
            threads = [executor.submit(self.getOneDepositMethod, 
            {'asset':asset}) for asset in assets]
            response = [f.result() for f in threads]
        if any([type(r) == pd.DataFrame for r in response]):
            df = pd.concat(response, axis=0).reset_index(drop=True)
            return df
        else: 
            print(f'no response from getDepositMethods')
            return pd.DataFrame()

    def getOneDepositMethod(self, AssetDict):
        endpoint="/0/private/DepositMethods"
        response = self.kraken.processRequest(endpoint, AssetDict)
        print(f'getOneDepositMethod response: {response.json()}')
        jsonResponse = response.json()
        if jsonResponse['error'] == []:
            df = pd.DataFrame(response.json()['result'])
            df['asset'] = AssetDict['asset']
            return df
        else: return pd.DataFrame()

    def getAssetInfo(self, endpoint="/0/public/Assets"):
        response = self.kraken.processRequest(endpoint, AUTH=False)
        print(f'getAssetInfo response json: {response}')
        return list(response['result'].keys())