import requests

class coinPaprika:

    URL = "https://api.coinpaprika.com/v1/"

    def __init__(self, coin_id='btc-bitcoin'):
        self.coin_id = coin_id

    def get_market(self):
        response = requests.get(self.URL+"coins/"+self.coin_id+"/markets")
        return response.json()

    def get_exchanges(self):
        response = requests.get(self.URL+"coins/"+self.coin_id+"/exchanges")
        return response.json()

    def get_market_depth(self):
        response = requests.get(self.URL+"coins/"+self.coin_id+"/market_depth")
        return response.json()

    def ohlc_latest(self):
        response = requests.get(self.URL+"coins/"+self.coin_id+"/ohlcv/latest")
        return response.json()

    def ohlc_today(self):
        response = requests.get(self.URL+self.coin_id+"/ohlcv/today")
        return response.json()

    def tickers(self):
        response = requests.get(self.URL+"tickers")
        return response.json()