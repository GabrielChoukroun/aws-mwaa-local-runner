import ast
import pandas as pd
import json
import requests

from airflow.models import Variable
# from modules.helper_functions.beacon_chain import Beacon
# from modules.helper_functions.google_conn import GoogleSheets
from modules.helper_functions.time_functions import datetimezone
# from modules.helper_functions.etherscan import Etherscan

class stakingDOT:
    def __init__(self):
        # self.API_KEY = ENV['Subscan']['API_KEY']
        # self.address = ENV['Subscan']['address_dot']
        # self.sheetsAPI = GoogleSheets(CREDS['google']['recon_rs'])
        self.API_KEY = Variable.get('Subscan_API_KEY')
        self.address = Variable.get('Subscan_address_dot')
        self.DateTime = datetimezone('US/Eastern')

    def executeRequest(self, callType, endPoint):

        headers = {
            'Content-Type': 'application/json',
            'X-API-Key': self.API_KEY,
        }

        data = { "key": f"{self.address}", "row": 1, "page": 1 }
        data = json.dumps(data)
        # print('https://polkadot.api.subscan.io/api/scan/' + endPoint)

        response = requests.request(callType, 'https://polkadot.api.subscan.io/api/scan/' + endPoint, headers=headers, data=data)

        return response

    def getBalances(self):

        response = self.executeRequest("POST", "search")
        if response.status_code==200:
            data = response.json()['data']
            data = [[data['account']['balance'], data['account']['balance_lock']]]
            print(data)
            # df = pd.read_json(data['account'])
            # df = pd.json_normalize(data, record_path =['account'])
            df = pd.DataFrame(data, columns=['BALANCE', 'LOCKED_BALANCE'])
            updated_at = self.DateTime.getTimeStamp()
            df['UPDATED_AT'] = updated_at
            df['PK'] = 1
            df.columns = df.columns.str.upper()


            # self.sheetsAPI.batchClear([f'DOT Staking!B2:B3'])
            requestBody = {
                'valueInputOption': 'USER_ENTERED',
                'data':[
                    {
                        'range': f'DOT Staking!B2:B3',
                        'majorDimension': 'ROWS',
                        'values': data
                    }
                ]
            }
            # self.sheetsAPI.batchUpdate(requestBody)
            # print(data['account'])
            print(df)
            return df


            # updated_at = self.DateTime.getTimeStamp()
            # requestBody = {
            #             'valueInputOption': 'USER_ENTERED',
            #             'data': [
            #                 {
            #                     'range': 'DOT Staking!B1',
            #                     'majorDimension': 'ROWS',
            #                     'values': [[updated_at]]
            #                 }
            #             ]
            # }
            # self.sheetsAPI.batchUpdate(requestBody)

