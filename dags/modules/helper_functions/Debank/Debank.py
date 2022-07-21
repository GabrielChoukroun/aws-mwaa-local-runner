import requests
import json

class Debank:
    def __init__(self):
        self.BASE_URL = "https://openapi.debank.com/"
    
    def executeRequest(self, endpoint):
        response = requests.get(self.BASE_URL + endpoint)
        return response