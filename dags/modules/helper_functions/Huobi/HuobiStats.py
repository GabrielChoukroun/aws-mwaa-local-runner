from .Huobi import Huobi
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import time

class HuobiStats():
    def __init__(self, ACCESS_KEY, SECRET_KEY):
        self.Huobi = Huobi(ACCESS_KEY, SECRET_KEY)

    def getAccountsInfo(self):
        endpoint = 'v1/account/accounts'
        response = self.Huobi.processRequest(endpoint).json()
        print(f'getAccountsInfo response:\n{response}')
        if 'data' in response:
            df = pd.DataFrame(response['data'])
        else: df = pd.DataFrame()
        return df

    def getAllAccountsHistory(self, accountIDList, currency = None,
                            transact_types = None, start_time = None, end_time = None,
                            sort = None, size = None):
        '''
        Executes this endpoint
        https://huobiapi.github.io/docs/spot/v1/en/#get-account-history
        '''
        with ThreadPoolExecutor() as executor:
            threads = [executor.submit(self.getOneAccountHistory, accID) 
                        for accID in accountIDList]
            response = [f.result() for f in threads]
        if any([type(r) == pd.DataFrame for r in response]):
            df = pd.concat(response, axis=0).reset_index(drop=True)
            df.columns = [col.replace('-', '_') for col in df.columns]
            return df
        else: return pd.DataFrame()

    def getOneAccountHistory(self, accID, currency = None, transact_types = None, 
                            start_time = None, end_time = None, sort = None, 
                            size = 500):
        params = {
            "account-id": accID,
            "currency": currency,
            "transact-types": transact_types,
            "start-time": start_time,
            "end-time": end_time,
            "sort": sort,
            "size": size
        }
        endpoint = "v1/account/history"
        response = self.Huobi.processRequest(endpoint, params).json()
        print(f'getAccountsHistory response:\n{response}')
        if 'data' in response:
            df = pd.DataFrame(response['data'])
            return df
        else: return pd.DataFrame()

    def getAllAccountLedgers(self, accountIDList):
        '''
        Executes this endpoint
        https://huobiapi.github.io/docs/spot/v1/en/#get-account-ledger
        '''
        with ThreadPoolExecutor() as executor:
            threads = [executor.submit(self.getOneAccountLedger, accID) 
                        for accID in accountIDList]
            response = [f.result() for f in threads]
        if any([type(r) == pd.DataFrame for r in response]):
            df = pd.concat(response, axis=0).reset_index(drop=True)
            df.columns = [col.replace('-', '_') for col in df.columns]
            return df
        else: return pd.DataFrame()

    def getOneAccountLedger(self, account_id, currency = None, transact_types = None,
                           start_time = None, end_time = None, sort = None, limit = 500,
                           from_id = None):
        params = {
            "accountId": account_id,
            "currency": currency,
            "transactTypes": transact_types,
            "startTime": start_time,
            "endTime": end_time,
            "sort": sort,
            "limit": limit,
            "fromId": from_id
        }

        endpoint = "v2/account/ledger"
        response = self.Huobi.processRequest(endpoint, params).json()
        print(f'getAccountsLedger response:\n{response}')
        if 'data' in response:
            df = pd.DataFrame(response['data'])
            return df
        else: return pd.DataFrame()

    def getAllAccountBalances(self, accountIDList):
        '''
        Executes this endpoint
        https://huobiapi.github.io/docs/spot/v1/en/#get-account-ledger
        '''
        with ThreadPoolExecutor() as executor:
            threads = [executor.submit(self.getOneAccountBalance, accID) 
                        for accID in accountIDList]
            response = [f.result() for f in threads]
        if any([type(r) == pd.DataFrame for r in response]):
            df = pd.concat(response, axis=0).reset_index(drop=True)
            df.columns = [col.replace('-', '_') for col in df.columns]
            return df
        else: return pd.DataFrame()

    def getOneAccountBalance(self, account_id):

        endpoint = f"v1/account/accounts/{account_id}/balance"
        response = self.Huobi.processRequest(endpoint).json()
        print(f'getAccountsBalance response:\n{response}')
        if 'data' in response:
            df = pd.DataFrame(response['data'])
            df.rename(columns={'type': 'account_type'}, inplace=True)
            df = pd.concat([df, pd.json_normalize(df['list'])], axis = 1)
            df.drop(columns=['list'], inplace=True)
            df.rename(columns={'type': 'balance_type'}, inplace=True)
            print(f'getOneAcc df\n{df.head(1)}')
            return df
        else: return pd.DataFrame()

    def getCurrencyList(self):
        endpoint = 'v1/common/currencys'
        return self.Huobi.processRequest(endpoint)

    def getAllAccountDeposits(self):
        '''
        Executes this endpoint
        https://huobiapi.github.io/docs/spot/v1/en/#query-deposit-address
        '''
        cryptoEndpoint = "v1/common/currencys"
        cryptoList = self.Huobi.processRequest(cryptoEndpoint, AUTH=False).json()['data']
        print(f'getAllAccountDeposits partial cryptoList: {cryptoList[:5]}')
        with ThreadPoolExecutor() as executor:
            threads = [executor.submit(self.getOneAccountDeposit, accID) 
                        for accID in cryptoList]
            response = [f.result() for f in threads]
        if any([type(r) == pd.DataFrame for r in response]):
            df = pd.concat(response, axis=0).reset_index(drop=True)
            df.columns = [col.replace('-', '_') for col in df.columns]
            return df
        else: return pd.DataFrame()

    def getOneAccountDeposit(self, currency):

        params = {
            "currency": currency
        }
        endpoint = "v2/account/deposit/address"
        response = self.Huobi.processRequest(endpoint, params).json()
        retries = 0
        while ('message' in response and 
                response['message'] == 'exceeded rate limit' and retries < 5):
                time.sleep(30)
                response = self.Huobi.processRequest(endpoint, params).json()
                retries += 1
            
        print(f'getAccountsDeposit response:\n{response}')
        if 'data' in response:
            df = pd.DataFrame(response['data'])
            return df
        
        else: return pd.DataFrame()

    def getAllAccountWithdrawals(self):
        '''
        Executes this endpoint
        https://huobiapi.github.io/docs/spot/v1/en/#query-withdraw-address
        '''
        cryptoEndpoint = "v1/common/currencys"
        cryptoList = self.Huobi.processRequest(cryptoEndpoint, AUTH=False).json()['data']
        print(f'getAllAccountWithdrawals partial cryptoList: {cryptoList[:5]}')
        with ThreadPoolExecutor() as executor:
            threads = [executor.submit(self.getOneAccountWithdrawal, accID) 
                        for accID in cryptoList]
            response = [f.result() for f in threads]
        if any([type(r) == pd.DataFrame for r in response]):
            df = pd.concat(response, axis=0).reset_index(drop=True)
            df.columns = [col.replace('-', '_') for col in df.columns]
            return df
        else: return pd.DataFrame()

    def getOneAccountWithdrawal(self, currency):

        params = {
            "currency": currency
        }
        endpoint = "v2/account/withdraw/address"
        response = self.Huobi.processRequest(endpoint, params)
        if response.status_code == 200:
            response = response.json()
            print(f'getAccountsWithdrawal response:\n{response}')
            if 'data' in response:
                df = pd.DataFrame(response['data'])
                return df
            else: return pd.DataFrame()
        else:
            print(f'getOneAccountWithdrawal status code: {response.status_code}')
            retries = 0
            while ('message' in response and 
                    response['message'] == 'exceeded rate limit' and 
                    retries < 5 and response.status_code == 200):
                    time.sleep(30)
                    response = self.Huobi.processRequest(endpoint, params)
                    retries += 1
            if response.status_code == 200:
                response = response.json()
                if 'data' in response:
                    df = pd.DataFrame(response['data'])
                    return df
            else: return pd.DataFrame()

    def getAllTradeOrders(self):
        '''
        Executes this endpoint
        https://huobiapi.github.io/docs/spot/v1/en/#search-past-orders
        '''
        tradeSymbols = self.getTradingSymbols()
        # print(f'trade Symbols: {tradeSymbols[:5]}')
        with ThreadPoolExecutor() as executor:
            threads = [executor.submit(self.getOneTradeOrder, symbol) 
                        for symbol in tradeSymbols]
            response = [f.result() for f in threads]
        if any([type(r) == pd.DataFrame for r in response]):
            df = pd.concat(response, axis=0).reset_index(drop=True)
            df.columns = [col.replace('-', '_') for col in df.columns]
            return df
        else: return pd.DataFrame()

    def getOneTradeOrder(self, symbol):
        params = {"symbol": symbol,
                "states": "filled,partial-canceled,canceled"}
        endpoint = "v1/order/orders"
        response = self.Huobi.processRequest(endpoint, params).json()
        # print(f'getOneTradeOrder response:\n{response}')
        if 'data' in response and response['data'] and len(response['data']) > 0:
            df = pd.DataFrame(response['data'])
            print(f'getOneTradeOrder df\n{df.head(1)}')
            return df
        else: return pd.DataFrame()

    def getTradingSymbols(self):
        symbolResponse = self.Huobi.processRequest("v2/settings/common/symbols", AUTH=False).json()
        tradingSymbols = []
        if 'data' in symbolResponse:
            for responseDic in symbolResponse['data']:
                tradingSymbols.append(responseDic['sc'])
        return tradingSymbols

    def getAllMarginOrdersIsolated(self):
        '''
        Executes this endpoint
        https://huobiapi.github.io/docs/spot/v1/en/#search-past-orders
        '''
        tradeSymbols = self.getTradingSymbols()
        with ThreadPoolExecutor() as executor:
            threads = [executor.submit(self.getOneMarginOrderIsolated, symbol) 
                        for symbol in tradeSymbols]
            response = [f.result() for f in threads]
        if any([type(r) == pd.DataFrame for r in response]):
            df = pd.concat(response, axis=0).reset_index(drop=True)
            df.columns = [col.replace('-', '_') for col in df.columns]
            return df
        else: return pd.DataFrame()

    def getOneMarginOrderIsolated(self, symbol):
        params = {"symbol": symbol}
        endpoint = "v1/margin/loan-orders"
        response = self.Huobi.processRequest(endpoint, params).json()
        if 'data' in response and response['data'] and len(response['data']) > 0:
            df = pd.DataFrame(response['data'])
            print(f'getOneMarginOrderIsolated df\n{df.head(1)}')
            return df
        else: return pd.DataFrame()

    def getAllMarginOrdersCross(self):
        '''
        Executes this endpoint
        https://huobiapi.github.io/docs/spot/v1/en/#search-past-margin-orders-cross
        '''
        tradeSymbols = self.getTradingSymbols()
        with ThreadPoolExecutor() as executor:
            threads = [executor.submit(self.getOneMarginOrderCross, symbol) 
                        for symbol in tradeSymbols]
            response = [f.result() for f in threads]
        if any([type(r) == pd.DataFrame for r in response]):
            df = pd.concat(response, axis=0).reset_index(drop=True)
            df.columns = [col.replace('-', '_') for col in df.columns]
            return df
        else: return pd.DataFrame()

    def getOneMarginOrderCross(self, symbol):
        params = {"symbol": symbol}
        endpoint = "v1/cross-margin/loan-orders"
        response = self.Huobi.processRequest(endpoint, params).json()
        retries = 0
        while response['status'] == 'error' and retries < 5:
            time.sleep(2)
            response = self.Huobi.processRequest(endpoint, params).json()
            retries += 1
        print(f'getOneMarginOrderCross response: {response}')
        if 'data' in response and response['data']:
            df = pd.DataFrame(response['data'])
            print(f'getOneMarginOrderCross df\n{df.head(1)}')
            return df
        else: return pd.DataFrame()
