from airflow.models import Variable
import requests

class BNB:
    
    def getStats():
        address = Variable.get('bnb_address')
        response = requests.get("https://dex.binance.org/api/v1/account/" + address).json()
        available = float(response['balances'][0]['free'])
        frozen = float(response['balances'][0]['frozen'])
        locked = float(response['balances'][0]['locked'])

        response = requests.get(f"https://api.binance.org/v1/staking/accounts/{address}/balance").json()
        delegated = response['delegated']
        unbonding = response['unbonding']

        data = [available, frozen, locked, delegated, unbonding]

        return data
