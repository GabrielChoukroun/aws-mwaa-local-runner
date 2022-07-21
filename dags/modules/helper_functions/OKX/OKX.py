import base64
from datetime import datetime
import datetime as dt
import hmac
import requests
import pandas as pd


class OKX_client:
    def __init__(self, APIKEY=None, APISECRET=None, PASS=None):
        self.APIKEY = APIKEY
        self.APISECRET = APISECRET
        self.PASS = PASS
        self.BASE_URL = 'https://www.okx.com'

    def signature(self, timestamp, method, request_path, body, secret_key):
        if str(body) == '{}' or str(body) == 'None':
            body = ''
        message = str(timestamp) + str.upper(method) + str(request_path) + str(body)
        mac = hmac.new(bytes(secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
        d = mac.digest()
        return base64.b64encode(d)

    def get_header(self, request='GET', endpoint='', body: dict = dict()):
        cur_time = dt.datetime.utcnow().isoformat()[:-3] + 'Z'
        header = dict()
        header['CONTENT-TYPE'] = 'application/json'
        header['OK-ACCESS-KEY'] = self.APIKEY
        header['OK-ACCESS-SIGN'] = header['OK-ACCESS-SIGN'] = self.signature(cur_time, request, endpoint, body, self.APISECRET).decode()
        header['OK-ACCESS-TIMESTAMP'] = str(cur_time)
        header['OK-ACCESS-PASSPHRASE'] = self.PASS
        header['CONTENT-TYPE'] = 'application/json'
        return header

    def _get(self, url_path, payload={}):
        try:
            if str(payload) == '{}' or str(payload) == 'None':
                full_url = self.BASE_URL + url_path
                url = url_path
            else:
                url = '?'
                for key, value in payload.items():
                    url = url + str(key) + '=' + str(value) + '&'
                url = url_path + str(url[0:-1])
                full_url = self.BASE_URL + url
            header = self.get_header('GET', url, {})
            response = requests.get(full_url, params={}, headers=header)
            return response.json()
        except Exception:
            return Exception

    def json_to_df(self, rs):
        df = pd.DataFrame(rs)
        df.columns = df.columns.str.upper()
        return df

    def get_deposits(self):
        rs = self._get("/api/v5/asset/deposit-history")
        df = self.json_to_df(rs)
        if not df.empty:
            df['TS'] = df.apply(lambda row: datetime.utcfromtimestamp(int(row['TS']).strftime('%Y-%m-%d %H:%M:%S')), axis=1)
            df.rename(columns={'FROM': '_FROM', 'TO': '_TO'}, inplace=True)
        return df


    def get_withdrawals(self):
        rs = self._get("/api/v5/asset/withdrawal-history")
        df = self.json_to_df(rs)
        if not df.empty:
            df['TS'] = df.apply(lambda row: datetime.utcfromtimestamp(int(row['TS']).strftime('%Y-%m-%d %H:%M:%S')),axis=1)
        return df

    def get_balances(self):
        rs = self._get("/api/v5/asset/balances")
        df = self.json_to_df(rs)
        if not df.empty:
            df.rename(columns={'BAL': 'BALANCE', 'FROZENBAL': 'FROZEN_BAL', 'AVAILBAL': 'AVAILABLE'}, inplace=True)
        return self.json_to_df(rs)

    def get_fills(self):
        rs = self._get("/api/v5/trade/fills")
        df = self.json_to_df(rs)
        if not df.empty:
            df['TS'] = df.apply(lambda row: datetime.utcfromtimestamp(int(row['TS']).strftime('%Y-%m-%d %H:%M:%S')),axis=1)
            df.rename(columns={'INSTTYPE': 'INST_TYPE', 'INSTID': 'INST_ID', 'TRADEID': 'TRADE_ID', 'CLORDID': 'CL_ORD_ID', 'BILLID': 'BILL_ID', 'FILLPX': 'FILL_PRICE', 'FILLSZ': 'FILL_QUANT', 'POSSIDE': 'POS_SIDE', 'EXECTYPE': 'EXEC_TYPE', 'FEECCY': 'FEE_CCY'}, inplace=True)
            df['ID'] = df.apply(lambda row: row['INST_ID'] + "_" + str(row['TRADE_ID']) + "_" + str(row['ORDID']) + "_"+ str(row['BILL_ID']), axis=1)
        return df