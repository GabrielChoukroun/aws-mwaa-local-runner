import hmac
import time
import urllib.parse
from typing import Any, Dict, List, Optional
from requests import Request, Response, Session
from ciso8601 import parse_datetime
from datetime import datetime as dt
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import numpy as np

class FtxClient:

    def __init__(self, api_key=None, api_secret=None, subaccount_name=None, endpoint_type='private') -> None:

        self._session = Session()
        self._api_key = api_key
        self._api_secret = api_secret
        self._subaccount_name = subaccount_name
        self._endpoint_type = endpoint_type
        self._ENDPOINT = 'https://ftx.com/api/'

    def _request(self, method: str, path: str, **kwargs) -> Any:
        request = Request(method, self._ENDPOINT + path, **kwargs)
        if self._endpoint_type == 'private':
            self._sign_request(request)
        response = self._session.send(request.prepare())
        return self._process_response(response)

    def _sign_request(self, request: Request) -> None:
        ts = int(time.time() * 1000)
        prepared = request.prepare()
        signature_payload = f'{ts}{prepared.method}{prepared.path_url}'.encode()
        if prepared.body:
            signature_payload += prepared.body
        signature = hmac.new(self._api_secret.encode(), signature_payload, 'sha256').hexdigest()
        request.headers['FTX-KEY'] = self._api_key
        request.headers['FTX-SIGN'] = signature
        request.headers['FTX-TS'] = str(ts)
        if self._subaccount_name:
            request.headers['FTX-SUBACCOUNT'] = urllib.parse.quote(self._subaccount_name)

    def _process_response(self, response: Response) -> Any:
        try:
            data = response.json()
        except ValueError:
            response.raise_for_status()
            raise
        else:
            if not data['success']:
                raise Exception(data['error'])
            return data['result']

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> list:
        return self._request('GET', path, params=params)

    def _post(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request('POST', path, json=params)

    def _delete(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request('DELETE', path, json=params)

    # -------------------------------------------------------------------------------------------------

    def getAllDeposits(self) -> List:
        print("deposits:")
        start_time = (dt.utcnow() - timedelta(days=2)).timestamp()
        end_time = dt.utcnow().timestamp()
        ids = set()
        limit = 200
        results = []
        while True:
            response = self._get(f'wallet/deposits', {
                'end_time': end_time,
                'start_time': start_time,
            })
            deduped_trades = [r for r in response if r['id'] not in ids]
            results.extend(deduped_trades)
            ids |= {r['id'] for r in deduped_trades}
            if len(response) == 0:
                break
            end_time = min(parse_datetime(t['time']) for t in response).timestamp()
            if len(response) < limit:
                break

        df = pd.DataFrame(results)
        df.columns = df.columns.str.upper()

        if not df.empty:
            df = df.reindex(['ID', 'COIN', 'SIZE', 'TIME', 'NOTES', 'STATUS', 'CONFIRMATIONS', 'FEE', 'METHOD', 'SENTTIME', 'TXID', 'ADDRESS', 'CONFIRMEDTIME'], axis=1)
            df = df.replace({np.nan: None})

        return df

    def getAllWithdrawals(self) -> List:
        print("Withdrawals:")
        start_time = (dt.utcnow() - timedelta(days=2)).timestamp()
        end_time = dt.utcnow().timestamp()
        ids = set()
        limit = 200
        results = []
        while True:
            response = self._get(f'wallet/withdrawals', {
                'end_time': end_time,
                'start_time': start_time,
            })
            deduped_trades = [r for r in response if r['id'] not in ids]
            results.extend(deduped_trades)
            ids |= {r['id'] for r in deduped_trades}
            if len(response) == 0:
                break
            end_time = min(parse_datetime(t['time']) for t in response).timestamp()
            if len(response) < limit:
                break
        df = pd.DataFrame(results)
        df.columns = df.columns.str.upper()

        if not df.empty:
            df = df.reindex(['ID', 'COIN', 'SIZE', 'TIME', 'NOTES', 'STATUS', 'ADDRESS', 'TAG', 'METHOD', 'TXID', 'FEE', 'DESTINATIONNAME'], axis=1)
            df = df.replace({np.nan: None})

        return df

    def getAllStakingTransactions(self) -> List:
        start_time = (dt.utcnow() - timedelta(days=2)).timestamp()
        end_time = dt.utcnow().timestamp()
        limit = 200
        results = []
        while True:
            response = self._get(f'staking/stakes', {
                'end_time': end_time,
                'start_time': start_time,
            })
            deduped_trades = [r for r in response]
            results.extend(deduped_trades)
            if len(response) == 0:
                break
            if end_time == min(parse_datetime(t['createdAt']) for t in response).timestamp():
                break
            end_time = min(parse_datetime(t['createdAt']) for t in response).timestamp()
            if len(response) < limit:
                break
        df = pd.DataFrame(results)
        df.columns = df.columns.str.upper()
        return df

    def getAllStakingRewards(self) -> List:
        start_time = (dt.utcnow() - timedelta(days=2)).timestamp()
        end_time = dt.utcnow().timestamp()
        limit = 200
        results = []
        while True:
            response = self._get(f'staking/staking_rewards', {
                'end_time': end_time,
                'start_time': start_time,
            })
            deduped_trades = [r for r in response]
            results.extend(deduped_trades)
            if len(response) == 0:
                break
            if end_time == min(parse_datetime(t['time']) for t in response).timestamp():
                break
            end_time = min(parse_datetime(t['time']) for t in response).timestamp()
            if len(response) < limit:
                break
        df = pd.DataFrame(results)
        df.columns = df.columns.str.upper()
        return df

    def getLendingRates(self):
        limit = 200
        results = []
        while True:
            response = self._get(f'spot_margin/lending_rates')
            deduped_trades = [r for r in response]
            results.extend(deduped_trades)
            if len(response) == 0:
                break
            if len(response) < limit:
                break
        df = pd.DataFrame(results)
        df.columns = df.columns.str.upper()
        df['ID'] = df.apply(lambda row: row['COIN'] + "_" + str(row['PREVIOUS']) + "_" + str(row['ESTIMATE']), axis=1)
        df['UPLOAD_DATE'] = time.time()
        return df

    def getBorrowRates(self):
        limit = 200
        results = []
        while True:
            response = self._get('spot_margin/borrow_rates')
            deduped_trades = [r for r in response]
            results.extend(deduped_trades)
            if len(response) == 0:
                break
            if len(response) < limit:
                break
        df = pd.DataFrame(response)
        df.columns = df.columns.str.upper()
        df['ID'] = df.apply(lambda row: row['COIN'] + "_" + str(row['PREVIOUS']) + "_" + str(row['ESTIMATE']), axis=1)
        df['UPLOAD_DATE'] = time.time()
        return df

    def getAllOTCFills(self) -> List:
        print("otc--fills:")
        start_time = (dt.utcnow() - timedelta(days=2)).timestamp()
        end_time = dt.utcnow().timestamp()
        ids = set()
        limit = 200
        results = []
        while True:
            response = self._get(f'otc/history', {
                'end_time': end_time,
                'start_time': start_time,
            })
            deduped_trades = [r for r in response if r['id'] not in ids]
            results.extend(deduped_trades)
            ids |= {r['id'] for r in deduped_trades}
            if len(response) == 0:
                break
            if end_time == min(parse_datetime(t['time']) for t in response).timestamp():
                break
            end_time = min(parse_datetime(t['time']) for t in response).timestamp()
            if len(response) < limit:
                break
        for fill in results:
            if "from" in fill:
                fill["_from"] = fill.pop("from")
            if "to" in fill:
                fill["_to"] = fill.pop("to")
        df = pd.DataFrame(results)
        df.columns = df.columns.str.upper()
        return df

    def getFills(self):
        res = []
        # gets all markets for the sub-account
        markets = self._get('markets')
        # gets all fills for all markets
        with ThreadPoolExecutor() as executor:
            fThreads = [executor.submit(self.getFillsForMarket, market.get('name'))
                        for market in markets]
            for f in fThreads: res += f.result()
        df = pd.DataFrame(res)
        df.columns = df.columns.str.upper()
        return df

    def getFillsForMarket(self, market: str) -> List:
        start_time = (dt.utcnow() - timedelta(days=2)).timestamp()
        end_time = dt.utcnow().timestamp()
        ids = set()
        limit = 200
        results = []
        while True:
            response = self._get(f'fills?market={market}', {
                'end_time': end_time,
                'start_time': start_time,
            })
            deduped_trades = [r for r in response if r['id'] not in ids]
            results.extend(deduped_trades)
            ids |= {r['id'] for r in deduped_trades}
            if len(response) == 0:
                break
            if end_time == min(parse_datetime(t['time']) for t in response).timestamp():
                break
            end_time = min(parse_datetime(t['time']) for t in response).timestamp()
            if len(response) < limit:
                break
        return results

    def getDepositAdresses(self):
        res = []
        coins_list = self._get('wallet/coins')
        # id is the name of coin
        name_of_coins = [dict_['id'] for dict_ in coins_list]
        for coin in name_of_coins:
            address = self._get(f'wallet/deposit_address/{coin}?')
            if address != False:
                address['coin'] = coin
                res.append(address)
        df = pd.DataFrame(res)
        df.columns = df.columns.str.upper()
        df['ID'] = df.apply(lambda row: row['ADDRESS'] + "_" + str(row['METHOD']) + "_" + str(row['COIN']) + "_" + str(
            self._subaccount_name) + "_" + str(row['TAG']), axis=1)
        return df

    def getWithdrawalAdresses(self):
        results = []
        limit = 200
        ids = set()
        while True:
            response = self._get(f'wallet/saved_addresses')
            deduped_trades = [r for r in response if r['id'] not in ids]
            results.extend(deduped_trades)
            ids |= {r['id'] for r in deduped_trades}
            if len(response) == 0:
                break
            if len(response) < limit:
                break
        df = pd.DataFrame(results)
        df.columns = df.columns.str.upper()
        return df

    def getBorrowSummary(self):
        limit = 200
        results = []
        while True:
            response = self._get('spot_margin/borrow_summary')
            results.extend(response)
            if len(response) == 0:
                break
            if len(response) < limit:
                break
        df = pd.DataFrame(response)
        df.columns = df.columns.str.upper()
        df['ID'] = df.apply(lambda row: row['COIN'] + "_" + str(row['SIZE']) + "_" + str(self._subaccount_name), axis=1)
        return df

    def getFundingPayments(self) -> List:
        start_time = (dt.utcnow() - timedelta(days=2)).timestamp()
        end_time = dt.utcnow().timestamp()
        ids = set()
        limit = 200
        results = []
        while True:
            response = self._get(f'funding_payments', {
                'end_time': end_time,
                'start_time': start_time,
            })
            deduped_trades = [r for r in response if r['id'] not in ids]
            results.extend(deduped_trades)
            ids |= {r['id'] for r in deduped_trades}
            if len(response) == 0:
                break
            if end_time == min(parse_datetime(t['time']) for t in response).timestamp():
                break
            end_time = min(parse_datetime(t['time']) for t in response).timestamp()
            if len(response) < limit:
                break
        df = pd.DataFrame(results)
        df.columns = df.columns.str.upper()
        return df

    def getLendingHistory(self) -> List:
        start_time = (dt.utcnow() - timedelta(days=2)).timestamp()
        end_time = dt.utcnow().timestamp()
        limit = 200
        results = []
        while True:
            response = self._get(f'spot_margin/history', {
                'end_time': end_time,
                'start_time': start_time,
            })
            results.extend(response)
            if len(response) == 0:
                break
            if end_time == min(parse_datetime(t['time']) for t in response).timestamp():
                break
            end_time = min(parse_datetime(t['time']) for t in response).timestamp()
            if len(response) < limit:
                break
        df = pd.DataFrame(results)
        df.columns = df.columns.str.upper()
        if not df.empty:
            df['ID'] = df.apply(lambda row: row['COIN'] + "_" + str(row['TIME']) + "_" + str(row['SIZE']), axis=1)
            df.drop_duplicates(subset=None, keep="first", inplace=True)
        return df

    def getBorrowHistory(self) -> List:
        start_time = (dt.utcnow() - timedelta(days=2)).timestamp()
        end_time = dt.utcnow().timestamp()
        limit = 200
        results = []
        while True:
            response = self._get(f'spot_margin/borrow_history', {
                'end_time': end_time,
                'start_time': start_time,
            })
            results.extend(response)
            if len(response) == 0:
                break
            if end_time == min(parse_datetime(t['time']) for t in response).timestamp():
                break
            end_time = min(parse_datetime(t['time']) for t in response).timestamp()
            if len(response) < limit:
                break
        df = pd.DataFrame(results)
        df.columns = df.columns.str.upper()
        if not df.empty:
            df['ID'] = df.apply(lambda row: row['COIN'] + "_" + str(row['TIME']) + "_" + str(row['SIZE']), axis=1)
            df.drop_duplicates(subset=None, keep="first", inplace=True)
        return df

    def getMarkets(self) -> List:
        executor = ThreadPoolExecutor(max_workers=10)
        response = executor.submit(self._get, 'markets')
        df = pd.DataFrame(response.result())
        df.columns = df.columns.str.upper()
        df['ID'] = df.apply(lambda row:  str(row['NAME']) + "_" + str(row['PRICE'])+ "_" + str(row['CHANGE1H']), axis=1)
        return df

    def getMarketCandles(self) -> List:
        res = []
        # gets all markets for the sub-account
        markets = self._get('markets')
        with ThreadPoolExecutor() as executor:
            fThreads = [executor.submit(self.getMarketHistory, market.get('name'))
                        for market in markets]
            for f in fThreads: res += f.result()
        df = pd.DataFrame(res)
        df.columns = df.columns.str.upper()
        df['ID'] = df.apply(
            lambda row: row['STARTTIME'] + "_" + str(row['TIME']) + "_" + str(row['VOLUME']) + "_" + str(
                row['CLOSE']) + "_" + str(self._subaccount_name) + "_" + str(row['OPEN']) + "_" + str(row['HIGH']) + str(row['LOW']), axis=1)
        df.drop_duplicates(subset=None, keep="first", inplace=True)
        return df

    def getMarketHistory(self, market_name):
        start_time = (dt.utcnow() - timedelta(days=2)).timestamp()
        end_time = dt.utcnow().timestamp()
        resolution = 3600
        limit = 200
        results = []
        while True:
            response = self._get(
                f'markets/{market_name}/candles?resolution={resolution}&start_time={start_time}&end_time={end_time}')
            results.extend(response)
            if len(response) == 0:
                break
            if len(response) < limit:
                break
        return results

    def getFundingRates(self) -> List:
        start_time = (dt.utcnow() - timedelta(days=2)).timestamp()
        end_time = dt.utcnow().timestamp()
        limit = 200
        results = []
        while True:
            response = self._get(f'funding_rates', {
                'end_time': end_time,
                'start_time': start_time,
            })
            results.extend(response)
            if len(response) == 0:
                break
            if end_time == min(parse_datetime(t['time']) for t in response).timestamp():
                break
            end_time = min(parse_datetime(t['time']) for t in response).timestamp()
            if len(response) < limit:
                break
        df = pd.DataFrame(results)
        df.columns = df.columns.str.upper()
        df['ID'] = df.apply(lambda row: row['FUTURE'] + "_" + str(row['RATE']) + "_" + str(row['TIME']) + str(
            self._subaccount_name), axis=1)
        df['UPLOAD_DATE'] = time.time()
        df.drop_duplicates(subset=None, keep="first", inplace=True)
        return df



