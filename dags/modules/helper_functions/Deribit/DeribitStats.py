import ast
from datetime import datetime as dt
from datetime import timedelta

import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from .Deribit import Deribit

class DeribitStats:
    def __init__(self, CLIENT_ID, CLIENT_SIGNATURE):
        self.deribit = Deribit(CLIENT_ID, CLIENT_SIGNATURE)

    def getTransactionInfo(self):
        '''
        Formerly deribitTxLogDump in cns-recon
        '''
        # startTime = int((dt.utcnow() - timedelta(days=365 * 3)).timestamp() * 1000)
        # endTime = int((dt.utcnow() - timedelta(days=365 * 1)).timestamp() * 1000)
        
        startTime = int((dt.utcnow() - timedelta(days=1)).timestamp() * 1000)
        endTime = int((dt.utcnow()).timestamp() * 1000)

        columns = ['username', 'user_seq', 'user_role', 'user_id', 'type', 'trade_id', 
        'timestamp', 'side', 'profit_as_cashflow', 'price_currency', 'price', 'position', 
        'order_id', 'mark_price', 'interest_pl', 'instrument_name', 'info', 'id', 'fee_balance', 
        'equity', 'currency', 'commission', 'change', 'cashflow', 'balance', 'amount', 
        'session_upl', 'session_rpl', 'total_interest_pl']

        df = pd.DataFrame(columns=columns)
        for currency in ['USDC', 'BTC', 'ETH']:
            result = self.deribit.getTxLogAtTime(currency, startTime, endTime)
            if result:
                temp = pd.DataFrame(result)
                temp = temp.where(pd.notnull(temp), None)
                temp['info'] = temp['info'].astype(str)
                df = pd.concat([df, temp])

        
        df = df.where(pd.notnull(df), None)
        df.columns = [col.upper() for col in df.columns]
        return df

    def getAccSummary(self, currency):
        """
        https://docs.deribit.com/?shell#private-get_account_summary
        Parameters
        ------------------------
        obj: Deribit object
        """
        params = (
            ('currency', currency),
            ('extended', 'true'),
        )
        endpoint = '/private/get_account_summary'

        response = self.deribit.executeRequest(params, endpoint)
        if response['result']!={}:
            df = pd.json_normalize(response['result'], sep = '_')
            return df
        else:
            return None

    def processAccSummary(self):
        df = pd.DataFrame()
        for currency in ['USDC', 'BTC', 'ETH']:
            response = self.getAccSummary(currency)
            if response is not None:
                df = pd.concat([df, response])
        df.columns = [col.upper() for col in df.columns]
        df = df.astype('object')
        df = df.where(pd.notna(df), None)
        print(f'processAccSummary df cols: {df.columns}')
        print(f'first row: {df.iloc[0,:]}')
        print(f'length :{len(df)}')
        return df

    def getPositions(self, currency):
        """
        https://docs.deribit.com/?shell#private-get_positions
        Parameters
        ------------------------
        obj: Deribit object
        """
        params = (
            ('currency', currency),
        )
        endpoint = '/private/get_positions'

        response = self.deribit.executeRequest(params, endpoint)

        if response['result']!=[]:
            df =  pd.DataFrame(response['result'])
            return df
        else:
            return None

    def processPositions(self):
        df = pd.DataFrame()
        for currency in ['USDC', 'BTC', 'ETH']:
            response = self.getPositions(currency)
            if response is not None:
                if currency == 'USDC':
                    df.drop(columns=['estimated_liquidation_ratio', 'estimated_liquidation_ratio_map'])
                df = pd.concat([df, response])
        df.columns = [col.upper() for col in df.columns]
        return df

    def getTransfers(self):
        currencies = ['BTC', 'ETH', 'SOL', 'USDC']
        with ThreadPoolExecutor() as executor:
            threads = [executor.submit(self.getOneTransfer, 
            {'currency':currency}) for currency in currencies]
            response = [f.result() for f in threads]
        if any([type(r) == pd.DataFrame for r in response]):
            df = pd.concat(response, axis=0).reset_index(drop=True)
            return df
        else: 
            print(f'no response from getDepositMethods')
            return pd.DataFrame()

    def getOneTransfer(self, currency):
        endpoint = '/private/get_transfers'
        response = self.deribit.executeRequest(currency, endpoint)

        try:
            df =  pd.DataFrame(response['result']['data'])
            return df
        except:
            return pd.DataFrame()

    def getWithdrawals(self):
        currencies = ['BTC', 'ETH', 'SOL', 'USDC']
        with ThreadPoolExecutor() as executor:
            threads = [executor.submit(self.getOneWithdrawal, 
            {'currency':currency}) for currency in currencies]
            response = [f.result() for f in threads]
        if any([type(r) == pd.DataFrame for r in response]):
            df = pd.concat(response, axis=0).reset_index(drop=True)
            return df
        else: 
            print(f'no response from getDepositMethods')
            return pd.DataFrame()

    def getOneWithdrawal(self, currency):
        endpoint = '/private/get_withdrawals'
        response = self.deribit.executeRequest(currency, endpoint)
        try:
            df =  pd.DataFrame(response['result']['data'])
            return df
        except:
            return pd.DataFrame()