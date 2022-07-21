from concurrent.futures import ThreadPoolExecutor
from itertools import chain

import pandas as pd
import requests


class Beacon:
    def __init__(self, eth1Address, API_KEY=None):
        self._BASE_URL = "https://beaconcha.in/api/v1/"
        self._API_KEY = API_KEY
        self._ETH1_ADDRESS = eth1Address
        self._VALIDATORS = pd.DataFrame(self.getValidators().json()['data'])

    def _chunker(self, seq, size):
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))

    def executeRequest(self, URL):
        RETRY = 0
        URL += f"?apikey={self._API_KEY}" if self._API_KEY else URL
        while RETRY<5:
            response = requests.get(URL)
            if response.status_code==200: return response
            else: RETRY+=1
        raise Exception(f"Failed to fetch data from BeaconChain {URL}: {response.text}")

    def getValidators(self):
        return self.executeRequest(self._BASE_URL + f"validator/eth1/{self._ETH1_ADDRESS}")

    def getBalanceHistory(self):
        with ThreadPoolExecutor() as executor:
            threads = [executor.submit(self.executeRequest, self._BASE_URL + 'validator/' + ','.join(str(int(i)) for i in group['validatorindex']) + '/balancehistory') for group in self._chunker(self._VALIDATORS.dropna(), 100)]
            responses = [f.result().json()['data'][:len(group)] for f, group in zip(threads, self._chunker(self._VALIDATORS, 100))]
        
        return list(chain.from_iterable(responses))

    def getAttestationEfficiency(self):
        with ThreadPoolExecutor() as executor:
            threads = [executor.submit(self.executeRequest, self._BASE_URL + 'validator/' + ','.join(str(int(i)) for i in group['validatorindex']) + '/attestationefficiency') for group in self._chunker(self._VALIDATORS.dropna(), 100)]
            responses = [f.result().json()['data'][:len(group)] for f, group in zip(threads, self._chunker(self._VALIDATORS, 100))]
        
        return list(chain.from_iterable(responses))
