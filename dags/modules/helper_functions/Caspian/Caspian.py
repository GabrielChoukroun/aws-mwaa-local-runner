import hmac
import time
import urllib.parse
import hashlib
import requests
import pandas as pd


class Caspian:

    def __init__(self, api_key=None, secret_key=None):
        self.api_url = "https://api.caspian.tech/"
        self.api_key = api_key
        self.secret_key = secret_key

    def compute_signature(self, method, endpoint, nonce, params):
        data = '&'.join('{!s}={!s}'.format(key, urllib.parse.quote_plus(str(val))) for key, val in params.items())
        body = method + endpoint + nonce + data
        secret = bytes(self.secret_key, 'utf-8')
        message = bytes(body, 'utf-8')
        return hmac.new(secret, msg=message, digestmod=hashlib.sha256).hexdigest()

    def _get(self, endpoint, data, organization=''):
        nonce = str(int(round(time.time() * 1000)))
        signature = self.compute_signature('GET', endpoint, nonce, data)
        headers = {'api-nonce': nonce, 'api-key': self.api_key, 'api-signature': signature, 'api-organization': organization}
        return (requests.get(self.api_url + endpoint, headers=headers, params=data, verify=False)).json()

    def get_transactions(self):
        endpoint = 'v1/pms/rest/transactions/get'
        # for startDay and endDay: T- Today, T-1 means Yesterday, T+1 means Tomorrow etc.
        data = {"startDay": "T-1", "endDay": "T+1", "properties": "rate,status,broker,detailedCommission"}
        response = self._get(endpoint, data)
        df = pd.DataFrame(response.get("transactions"))
        df.columns = df.columns.str.upper()
        return df

    def get_positions(self):
        endpoint = 'v1/pms/rest/positions/get'
        data = {"requestIdentifier": "00000000-0000-0000-0000-000000000001", "day": "REALTIME", "aggregation": "custodian,portfolio,strategy,acquirer"}
        response = self._get(endpoint, data)
        df = pd.DataFrame(response.get("positions"))
        df.columns = df.columns.str.upper()
        df['ID'] = df.apply(lambda row: row['FUND'] + "_" + str(row['POSITION']) + "_" + str(row['INSTRUMENT']), axis=1)
        df.drop_duplicates(subset=None, keep="first", inplace=True)
        return df
