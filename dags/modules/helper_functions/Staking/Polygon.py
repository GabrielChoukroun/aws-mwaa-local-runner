import math
import requests
import pandas as pd
import numpy as np
from airflow.models import Variable
from modules.helper_functions.etherscan import Etherscan


class stakingPolygon:

    def processValue(self, x):
        """
        Modify the value of the transaction based on the decimal range of the respective {token}
        Parameters
        ------------------------
        x: Pandas Series
            contains information for transaction value and tokenDecimal
        Returns
        ------------------------
        float
            modified value of the transaction
        """
        if (math.isnan(float(x['tokenDecimal']))) or (float(x['tokenDecimal']) is np.nan):
            return int(x['value']) / (10 ** 18)
        else:
            return int(x['value']) / (10 ** int(x['tokenDecimal']))

    def getTotal(self):
        response = requests.get('https://www.polygon.stakin.com/')

        dfTotals = pd.DataFrame(columns = ['ID', 'stakedAmount', 'restakedAmount', 'withdrawnRewards',
                                           'pendingRewards', 'stakedAmountUSD', 'restakedAmountUSD',
                                           'withdrawnRewardsUSD', 'pendingRewardsUSD', 'lastUpdate'])

        dfTotals = dfTotals.append({'ID' : '1',
                              'stakedAmount' : response.json()[0]['stakedAmount'],
                              'restakedAmount' : response.json()[0]['restakedAmount'],
                              'withdrawnRewards' : response.json()[0]['withdrawnRewards'],
                              'pendingRewards' : response.json()[0]['pendingRewards'],
                              'stakedAmountUSD' : response.json()[0]['stakedAmountUSD'],
                              'restakedAmountUSD' : response.json()[0]['restakedAmountUSD'],
                              'withdrawnRewardsUSD' : response.json()[0]['withdrawnRewardsUSD'],
                              'pendingRewardsUSD' : response.json()[0]['pendingRewardsUSD'],
                              'lastUpdate' : response.json()[0]['lastUpdate']},
                ignore_index = True)

        return dfTotals

    def getStats(self):
        response = requests.get('https://www.polygon.stakin.com/')

        data = pd.DataFrame(response.json()).transpose().reset_index()
        df = data.loc[~(data['index'] == 'maticPriceUSD') & (data['index'].str.contains("0x", regex=False))]
        df = df.set_index('index')[0].apply(pd.Series).reset_index()
        df = df.rename(columns={'index': 'address'})

        return df

    def getFunding(self):
        response = requests.get('https://www.polygon.stakin.com/')
        addresses = [x for x in response.json()[0].keys() if '0x' in x]

        eth = Etherscan(Variable.get('etherscan_api_key'))
        transactions = []
        for address in addresses: transactions.extend(eth.fetchAllTxs(address))

        TXLOG = pd.DataFrame(transactions, columns = ['blockHash', 'blockNumber', 'confirmations', 'contractAddress', 'cumulativeGasUsed', 'from', 'gas', 'gasPrice', 'gasUsed', 'hash','input', 'nonce', 'timeStamp', 'to', 'tokenDecimal', 'tokenName', 'tokenSymbol', 'transactionIndex', 'value', 'isError','txreceipt_status'])
        TXLOG['value'] = TXLOG.apply(self.processValue, axis=1)
        TXLOG = TXLOG.loc[(TXLOG['tokenSymbol']=='MATIC') & (TXLOG['from'].isin(addresses)) & (TXLOG['to']=='0x5e3ef299fddf15eaa0432e6e66473ace8c13d908')][['timeStamp', 'hash', 'from', 'to', 'value', 'tokenSymbol']]

        TXLOG['timeStamp'] = pd.to_datetime(pd.to_numeric(TXLOG['timeStamp']), unit='s').dt.strftime("%Y-%m-%d %H:%M:%S")

        TXLOG = TXLOG.where(pd.notnull(TXLOG), None)
        TXLOG.rename(columns={"hash": "POLYGON_HASH", "from": "FROM_ADDRESS", "to": "TO_ADDRESS", "value": "AMOUNT"}, inplace = True)

        return TXLOG
