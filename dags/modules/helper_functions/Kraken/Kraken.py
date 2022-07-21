import base64
import hashlib
import hmac
import time
import urllib.parse

import requests

class Kraken():
    def __init__(self, API_KEY, API_SEC):
        self.API_KEY = API_KEY
        self.API_SEC = API_SEC
        self.BASE_URL = "https://api.kraken.com"

    def getSignature(self, urlpath, data, secret):
        postdata = urllib.parse.urlencode(data)
        encoded = (str(data['nonce']) + postdata).encode()
        message = urlpath.encode() + hashlib.sha256(encoded).digest()

        mac = hmac.new(base64.b64decode(secret), message, hashlib.sha512)
        sigdigest = base64.b64encode(mac.digest())
        return sigdigest.decode()

    def processRequest(self, uri_path, data = {}, AUTH=True):
        
        if AUTH == False:
            resp = requests.get(self.BASE_URL + uri_path)
            return resp.json()
        data.update({"nonce": str(int(1000*time.time()))})
        
        headers = {}
        headers['API-Key'] = self.API_KEY
        headers['API-Sign'] = self.getSignature(uri_path, data, self.API_SEC)
        
        req = requests.post((self.BASE_URL + uri_path), headers=headers, data=data)
        if 'error' in req.json():
            if req.json()['error'] == ['EAPI:Rate limit exceeded']:
                time.sleep(30)
                return self.processRequest(uri_path, data)
        return req

    