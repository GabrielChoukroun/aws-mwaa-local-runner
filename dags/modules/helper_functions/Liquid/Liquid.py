from typing import List
from datetime import datetime as dt
from datetime import timedelta
import pandas as pd
import numpy as np
import requests
import jwt
import logging, json

class LiquidClient:

    def __init__(self, api_key=None, api_secret=None) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.session = requests.Session()
        self._ENDPOINT = 'https://api.liquid.com'

    def _create_auth_headers(self, path: str) -> dict:
        '''
        _create_auth_headers creates authentication header to call private API
        Parameters
        ----------
        path: str
            API path included in URI
        Returns
        -------
        dict
            Authentication headers to use private API
        '''
        payload = {
            'path': path,
            'nonce': int(dt.now().timestamp() * 1000),
            'token_id': self.api_key
        }
        return {
            'X-Quoine-Auth': jwt.encode(payload, self.api_secret, algorithm='HS256'),
            'X-Quoine-API-Version': '2',
            'Content-Type': 'application/json'
        }

    # -------------------------------------------------------------------------------------------------

    def get_transactions(self) -> List:
        print("Retrieving liquid transactions:")
        start_time = (dt.utcnow() - timedelta(days=2)).timestamp()
        end_time = dt.utcnow().timestamp()
        ids = set()
        limit = 500
        results = []
        path = f'/balance_history?timestamp_lte={end_time}&timestamp_gte={start_time}&limit={limit}'
        while True:
            api_response = self.session.get(self._ENDPOINT + path, headers=self._create_auth_headers(path))
            if not api_response.ok:
                logging.error(f'Failed to get transactions history....')
                raise Exception(f'status: {api_response.status_code}, text: {api_response.text}')
            response = json.loads(api_response.text)
            models = response['models']
            if not models:
                break
            deduped_models = [model for model in models if model['id'] not in ids]
            results.extend(deduped_models)
            ids |= {r['id'] for r in deduped_models}
            if response['next']:
                path = response['next']
            else:
                break
        df = pd.DataFrame(results)
        df.columns = df.columns.str.upper()
        if not df.empty:
            df = df.reindex(['ID', 'CURRENCY', 'AMOUNT', 'SOURCE_ACTION', 'SOURCE_OBJECT_TYPE', 'SOURCE_OBJECT_ID', 'TIMESTAMP', 'BALANCE'], axis=1)
            df = df.replace({np.nan: None})
        return df
