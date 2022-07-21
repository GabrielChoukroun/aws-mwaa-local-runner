import json
from web3 import Web3
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

from airflow.models import Variable
from modules.helper_functions.google_conn import GoogleSheets
from modules.helper_functions.time_functions import datetimezone


class stakingSGB:
    def __init__(self):
        self.sheetsAPI = GoogleSheets(Variable.get('google_recon_rs'))
        self.DateTime = datetimezone("US/Eastern")
        self.RPC = Web3(Web3.HTTPProvider('https://sgb.ftso.com.au/ext/bc/C/rpc'))
        self.ftso_address = '0xc5738334b972745067fFa666040fdeADc66Cb925'
        self.sgb_decimals = 18
        with open("dags/modules/helper_functions/Staking/data/sgb_abi.json") as f:
            self.abi = json.load(f)

    def getAddresses(self):
        addresses = self.sheetsAPI.batchGet(["[Pull] SGB Vault Addresses!A2:A"], valueRenderOption='FORMATTED_VALUE')[0]['values']
        
        return [x[0] for x in addresses]

    def getRewards(self, vault_address):
        contract_obj = self.RPC.eth.contract(address=self.ftso_address, abi=self.abi)

        unclaimed_epoches = contract_obj.functions.getEpochsWithUnclaimedRewards(
            vault_address).call()

        pending_rewards = 0

        for epoch in unclaimed_epoches:
            pending_rewards += int(contract_obj.functions.getStateOfRewards(
                vault_address, epoch).call()[1][0])

        return [[vault_address, pending_rewards / 10 ** self.sgb_decimals]]

    def processRewards(self):
        addresses = self.getAddresses()

        df = pd.DataFrame(columns=['Address', 'Rewards'])
        with ThreadPoolExecutor(max_workers=5) as executor:
            threads = [executor.submit(self.getRewards, address) for address in addresses]
            for f in threads:
                df = df.append({'Address': f.result()[0][0],
                                'Rewards': f.result()[0][1]},
                                ignore_index=True)

        return df


