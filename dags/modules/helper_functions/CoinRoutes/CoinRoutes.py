import pandas as pd
import datetime as dt
from typing import Any, Dict, List, Optional
from requests import Response, request


class CoinRoutes:
    def __init__(self, token, limit=1000):
        self.url = "https://coinrts.cerberuscel.com/api"
        self.headers = {'Authorization': f'Token {token}'}
        self.limit = limit

    def _process_response(self, response: Response) -> Any:
        try:
            data = response.json()
        except ValueError as v:
            print(v)
            response.raise_for_status()
            raise
        else:
            if 'results' not in data.keys():
                print(data)
            else:
                return data['results']

    def get_all_client_orders(self, created_at_after: dt.datetime = None, created_at_before: dt.datetime = None,
                              order_type: str = None) -> pd.DataFrame:
        offset = 0
        results = []
        while True:
            params = {
                'limit': self.limit,
                'offset': offset,
                'created_at_after': created_at_after,
                'created_at_before': created_at_before,
                'order_type': order_type,
            }
            response = request("GET", self.url + '/client_orders', headers=self.headers, params=params)
            tmp = pd.DataFrame(self._process_response(response))
            results.append(tmp)
            offset += self.limit
            print(len(tmp))
            if len(tmp) != self.limit:
                break
        return pd.concat(results)

    def get_sor_orders(self, client_order_id: str) -> pd.DataFrame:
        print(client_order_id)
        offset = 0
        results = []
        while True:
            params = {
                'client_order_id': client_order_id,
                'limit': self.limit,
                'offset': offset
            }
            response = request("GET", self.url + '/sor_orders', headers=self.headers, params=params)
            tmp = pd.DataFrame(self._process_response(response))
            results.append(tmp)
            offset += self.limit
            print(len(tmp))
            if len(tmp) != self.limit:
                break
        return pd.concat(results)

    def get_all_currency_pairs(self) -> pd.DataFrame:
        response = request("GET", self.url + '/currency_pairs', headers=self.headers)
        return pd.json_normalize(response.json())

    def get_currency_pair_info(self, currency_pair: str) -> pd.DataFrame:
        response = request("GET", self.url + f'/currency_pairs/{currency_pair}', headers=self.headers())
        return pd.json_normalize(response.json())

    def get_positions(self) -> pd.DataFrame:
        response = request("GET", self.url + f'/positions', headers=self.headers)
        return pd.json_normalize(response.json())

    def get_currency_balances(self, current_balances: bool = False) -> pd.DataFrame:
        response = request("GET", self.url + f'/currency_balances', headers=self.headers)
        balances = pd.json_normalize(response.json())
        balances['amount'] = balances.amount.astype('float')
        if current_balances:
            return balances.query("amount != 0")
        return balances

    def get_all_funding_transactions(self) -> pd.DataFrame:
        offset = 0
        results = []
        while True:
            params = {
                'limit': self.limit,
                'offset': offset
            }
            response = request("GET", self.url + '/account_funding_history', headers=self.headers, params=params)
            tmp = pd.DataFrame(self._process_response(response))
            results.append(tmp)
            offset += self.limit
            print(len(tmp))
            if len(tmp) != self.limit:
                break
        return pd.concat(results)
