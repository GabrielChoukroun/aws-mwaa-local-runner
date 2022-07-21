from concurrent.futures import ThreadPoolExecutor
import urllib.parse
import hashlib
import hmac
import base64
import requests
import time
import json
import pandas as pd
import numpy as np

class Binance:
    def __init__(self, api_key=None, secret_key=None):
        self.api_url = "https://api.binance.us"
        self.api_key = api_key
        self.secret_key = secret_key

    # get binanceus signature
    def get_binanceus_signature(self, data, secret):
        postdata = urllib.parse.urlencode(data)
        message = postdata.encode()
        byte_key = bytes(secret, 'UTF-8')
        mac = hmac.new(byte_key, message, hashlib.sha256).hexdigest()
        return mac

    # Attaches auth headers and returns results of a POST request
    def binanceus_request(self, uri_path, data):
        api_key= self.api_key
        api_sec= self.secret_key
        headers = {}
        headers['X-MBX-APIKEY'] = api_key
        signature = self.get_binanceus_signature(data, api_sec)
        params={
            **data,
            "signature": signature,
            }
        req = requests.get((self.api_url + uri_path), params=params, headers=headers)
        return req.text

    def getDepositAddresses(self):
        name_of_coins = []
        res=[]
        uri_path = "/sapi/v1/capital/config/getall"
        data = {"timestamp": int(round(time.time() * 1000))}
        result = json.loads(self.binanceus_request(uri_path, data))
        name_of_coins = [dict_.get('coin') for dict_ in result]
        with ThreadPoolExecutor() as executor:
            fThreads = [executor.submit(self.getDepositsForCoin, coin)
                        for coin in name_of_coins]
            for f in fThreads: res.append(f.result())
        df = pd.DataFrame(res)
        df.columns = df.columns.str.upper()

        if not df.empty:
            df = df.reindex(['COIN', 'ADDRESS', 'TAG', 'URL'], axis=1)
            df['ID'] = df.apply(lambda row: str(row['COIN']) + "_" + str(row['ADDRESS']), axis=1)
            df = df.replace({np.nan: None})
            df.drop_duplicates(subset=None, keep="first", inplace=True)
        return df

    def getDepositsForCoin(self, coin):
        uri_path = "/sapi/v1/capital/deposit/address"
        data = {
            "coin": coin,
            "timestamp": int(round(time.time() * 1000))
        }
        result = json.loads(self.binanceus_request(uri_path, data))
        print("GET {}: {}".format(uri_path, result))
        return result
