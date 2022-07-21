import requests
import base64
import hashlib
import hmac
import datetime
from urllib import parse
import urllib.parse
import json

class UrlParamsBuilder(object):

    def __init__(self):
        self.param_map = dict()
        self.post_map = dict()
        self.post_list = list()

    def put_url(self, name, value):
        if value is not None:
            if isinstance(value, (list, dict)):
                self.param_map[name] = value
            else:
                self.param_map[name] = str(value)

    def put_post(self, name, value):
        if value is not None:
            if isinstance(value, (list, dict)):
                self.post_map[name] = value
            else:
                self.post_map[name] = str(value)

    def build_url(self):
        if len(self.param_map) == 0:
            return ""
        encoded_param = urllib.parse.urlencode(self.param_map)
        return "?" + encoded_param

    def build_url_to_json(self):
        return json.dumps(self.param_map)


class Huobi():
    def __init__(self, ACCESS_KEY, SECRET_KEY):
        self.ACCESS_KEY = ACCESS_KEY
        self.SECRET_KEY = SECRET_KEY
        self.BASE_URL = "https://api.huobi.pro/"

    def processRequest(self, endpoint, data = {}, AUTH = True):
        url = self.BASE_URL + endpoint
        if not AUTH:
            response = requests.get(url)
            # print(f'non auth response: {response.json()}')
            return response
        builder = UrlParamsBuilder()
        self.create_signature(self.ACCESS_KEY, self.SECRET_KEY, "GET", url, builder, data)
        url += builder.build_url()
        # print(f'processRequest url: {url}')
        response = requests.get(url)
        return response

    def create_signature(self, api_key, secret_key, method, url, builder, data={}):
        timestamp = self.utc_now()
        builder.put_url("AccessKeyId", api_key)
        builder.put_url("SignatureVersion", "2")
        builder.put_url("SignatureMethod", "HmacSHA256")
        builder.put_url("Timestamp", timestamp)
        for key, val in data.items():
            builder.put_url(key, val)
        host = urllib.parse.urlparse(url).hostname
        path = urllib.parse.urlparse(url).path

        keys = sorted(builder.param_map.keys())
        qs0 = '&'.join(['%s=%s' % (key, parse.quote(builder.param_map[key], safe='')) for key in keys])
        payload0 = '%s\n%s\n%s\n%s' % (method, host, path, qs0)
        dig = hmac.new(secret_key.encode('utf-8'), msg=payload0.encode('utf-8'), digestmod=hashlib.sha256).digest()
        s = base64.b64encode(dig).decode()
        builder.put_url("Signature", s)

    def utc_now(self):
        return datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
