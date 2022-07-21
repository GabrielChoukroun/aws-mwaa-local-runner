import requests

class Trovata():
    def __init__(self, APP_ID, SECRET_KEY, companyid):
        self.APP_ID = APP_ID
        self.SECRET_KEY = SECRET_KEY
        self.COMPANY_ID = companyid
        self.BASE_URL = "https://api.trovata.io/developer/"
        self.headers = {
            "Accept": "application/json",
            "appid": self.APP_ID,
            "secret": self.SECRET_KEY,
            "companyid": self.COMPANY_ID}

    def processRequest(self, endpoint, argDict = {}, AUTH = True):
        url = self.BASE_URL + endpoint
        if argDict:
            url += '?' + self.create_arg_list(argDict)
        print(f'url:\t{url}')
        response = requests.get(url, headers = self.headers)
        print(f'status code:\t{response.status_code}')
        print(f'text:\t{response.text}')
        if response.status_code < 400:
            return response.json()
        else:
            return {}

    def create_arg_list(self, argDict):
        res = []
        for key, value in argDict.items():
            if type(value) == bool:
                res.append('='.join([key, str(value).lower()]))
            else:
                res.append('='.join([key, str(value)]))
        return '&'.join(res)