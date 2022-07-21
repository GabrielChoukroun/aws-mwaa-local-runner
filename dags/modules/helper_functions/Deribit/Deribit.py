import hashlib
import hmac
import secrets
from datetime import datetime as dt
from datetime import timedelta

import requests


class Deribit:
    def __init__(self, CLIENT_ID, CLIENT_SIGNATURE, ENDPOINT_TYPE='private'):
        self.CLIENT_ID = CLIENT_ID
        self.CLIENT_SIGNATURE = CLIENT_SIGNATURE
        self.BASE_URL = "https://www.deribit.com/api/v2"
        self.AUTH_FAIL = False
        self.ENDPOINT_TYPE = ENDPOINT_TYPE.lower()
        if self.ENDPOINT_TYPE == 'private':
            self._requestAuth()

    def _generate_signature(self):
        clientSecret = self.CLIENT_SIGNATURE
        timestamp = round(dt.now().timestamp() * 1000)
        nonce = secrets.token_urlsafe()
        data = ""
        signature = hmac.new(
            bytes(clientSecret, "latin-1"),
            msg=bytes('{}\n{}\n{}'.format(timestamp, nonce, data), "latin-1"),
            digestmod=hashlib.sha256
        ).hexdigest().lower()
        return timestamp, nonce, data, signature

    def _requestAuth(self):
        timestamp, nonce, data, signature = self._generate_signature()

        headers = {
            'Content-Type': 'application/json',
        }

        params = (
            ('client_id', self.CLIENT_ID),
            ('timestamp', timestamp),
            ('signature', signature),
            ('nonce', nonce),
            ('data', data),
            ('grant_type', 'client_signature'),
        )

        response = requests.get(self.BASE_URL + '/public/auth', headers=headers, params=params)
        print(f"deribit access token: {response.json()}")
        self.ACCESS_TOKEN = response.json()['result']['access_token']

    def executeRequest(self, params = {}, endpoint=None):
        headers = {
            'Content-Type': 'application/json',
        }

        if self.ENDPOINT_TYPE == 'private':
            headers['Authorization'] = f'Bearer {self.ACCESS_TOKEN}'

        response = requests.get(self.BASE_URL + endpoint, headers=headers, params=params)

        if response.status_code == 401:
            if self.AUTH_FAIL:
                self.AUTH_FAIL = True
                raise Exception("Deribit wrapper failed to Authenticate.")
            else:
                self._requestAuth()
                return self.executeRequest(params, endpoint)

        elif response.status_code == 200:
            return response.json()

    def getTxLog(self, currency):
        result = []

        # Parameters for the API call, 
        # Currency: BTC, ETH, USDC
        # Start Timestamp: Last 5 years
        # End Timestamp: Now
        # Deribit's maximum response count is 1000 log entries
        # TODO: flatten list of dictionaries according to the schema
        params = (
            ('currency', currency),
            ('start_timestamp', 1000 * int((dt.utcnow() - timedelta(days=365 * 5)).timestamp())),
            ('end_timestamp', 1000 * int(dt.utcnow().timestamp())),
            ('count', 1000)
        )
        endpoint = '/private/get_transaction_log'

        # Get initial response from the API endpoint
        response = self.executeRequest(params, endpoint)
        result += response['result']['logs']

        while True:
            # IF the result contains a continuation token which indicates there are more transactions that can be fetched from the log,
            # Execute another API call with the continuation token in params
            # This process is repeated until no continuation token is provided in the response
            if response['result']['continuation'] is not None:
                params = (
                    ('currency', currency),
                    ('start_timestamp', 1000 * int((dt.utcnow() - timedelta(days=365)).timestamp())),
                    ('end_timestamp', 1000 * int(dt.utcnow().timestamp())),
                    ('continuation', response['result']['continuation']),
                    ('count', 1000)
                )
                endpoint = '/private/get_transaction_log'

                response = self.executeRequest(params, endpoint)
                result += response['result']['logs']
            else:
                break

        return result

    def getTxLogAtTime(self, currency, start_timestamp, end_timestamp):
        result = []

        # Parameters for the API call, 
        # Currency: BTC, ETH, USDC
        # Start Timestamp: Last 5 years
        # End Timestamp: Now
        # Deribit's maximum response count is 1000 log entries
        # TODO: flatten list of dictionaries according to the schema
        params = (
            ('currency', currency),
            ('start_timestamp', start_timestamp),
            ('end_timestamp', end_timestamp),
            ('count', 1000)
        )
        endpoint = '/private/get_transaction_log'

        # Get initial response from the API endpoint
        response = self.executeRequest(params, endpoint)
        result += response['result']['logs']

        while True:
            # IF the result contains a continuation token which indicates there are more transactions that can be fetched from the log,
            # Execute another API call with the continuation token in params
            # This process is repeated until no continuation token is provided in the response
            if response['result']['continuation'] is not None:
                params = (
                    ('currency', currency),
                    ('start_timestamp', start_timestamp),
                    ('end_timestamp', end_timestamp),
                    ('continuation', response['result']['continuation']),
                    ('count', 1000)
                )
                endpoint = '/private/get_transaction_log'

                response = self.executeRequest(params, endpoint)
                result += response['result']['logs']
            else:
                break

        return result
