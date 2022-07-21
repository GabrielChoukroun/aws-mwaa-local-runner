import ast
import pandas as pd

from airflow.models import Variable
from modules.helper_functions.beacon_chain import Beacon
from modules.helper_functions.google_conn import GoogleSheets
from modules.helper_functions.time_functions import datetimezone
from modules.helper_functions.etherscan import Etherscan

class stakingETH2:
    def __init__(self):
        self.address = ast.literal_eval(Variable.get('ETH2_addresslist'))
        self.DateTime = datetimezone('US/Eastern')
        self.ether = Etherscan(Variable.get('etherscan_api_key'))

    def processData(self):
        validators = self.getValidatorsFromAddresses(self.address)
        # eth_balances = self.getETHBalances(self.address)

    def getETHBalances(self):

        addressList = self.address
        resultList = []

        for address in addressList:
            resultList.append([address, int(self.ether.getETHBalance(address).json()['result']) / 10 ** 18])

        return pd.DataFrame(resultList, columns = ['ADDRESS', 'ETH_BALANCE'])

    def getSingleValidatorFromAddress(self, address):
        beacon = Beacon(eth1Address = address, API_KEY=Variable.get('beaconChain_API_KEY'))
        validators = beacon._VALIDATORS.copy()

        balances = beacon.getBalanceHistory()
        efficiency = beacon.getAttestationEfficiency()

        validators = validators.merge(pd.DataFrame(balances), on='validatorindex', how='left')
        validators = validators.merge(pd.DataFrame(efficiency), on='validatorindex', how='left')

        validators = validators[['publickey', 'validatorindex', 'valid_signature', 'balance', 'effectivebalance', 'attestation_efficiency']]
        validators['address'] = address

        validators = validators.where(pd.notnull(validators), None)

        return validators

    def getValidatorsFromAddresses(self):
        addressList = self.address
        resultList = []

        for address in addressList:
            resultList.append(self.getSingleValidatorFromAddress(address))

        return pd.concat(resultList)
