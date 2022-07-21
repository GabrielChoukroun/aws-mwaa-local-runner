import requests
import json
import hashlib
import hmac
from datetime import datetime as dt

class Bitfinex:
    def __init__(self, API_SECRET=None, API_KEY=None):
        self.BASE_URL = "https://api.bitfinex.com/"
        self.PUB_URL = "https://api-pub.bitfinex.com/"
        self.API_SECRET = API_SECRET
        self.API_KEY = API_KEY
        
    def generateHeader(self, endpoint, body):
        _apiPath = endpoint
        _nonce = str(int(dt.utcnow().timestamp()*100000))
        _body = json.dumps(body)

        _signature = f"/api/{_apiPath}{_nonce}{_body}"
        _sig = hmac.new(
            bytes(self.API_SECRET, "latin-1"),
            msg=bytes(_signature, "latin-1"),
            digestmod=hashlib.sha384
        ).hexdigest()
        
        return _nonce, _sig
    
    def executeRequest(self, endpoint, body={}, AUTH=True):
        if AUTH:
            body = body
            nonce, signature = self.generateHeader(endpoint, body)
            headers = {
                'Content-Type': 'application/json',
                'bfx-nonce': nonce,
                'bfx-apikey': self.API_KEY,
                'bfx-signature': signature
            }
            response = requests.post(self.BASE_URL + endpoint, headers=headers, json=body)
            return response
        else:
            response = requests.get(self.PUB_URL + endpoint, json=body)
            return response