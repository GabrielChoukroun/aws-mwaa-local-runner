import pandas as pd
from .Trovata import Trovata

class TrovataStats(Trovata):
    def __init__(self, APP_ID, SECRET_KEY, companyid):
        super().__init__(APP_ID, SECRET_KEY, companyid)

    def getAccounts(self, argDict = {}):
        endpoint = 'data/v1/accounts'
        responseJSON = self.processRequest(endpoint, argDict)
        if (responseJSON and 'accounts' in responseJSON and 
            responseJSON['accounts']):
            df = pd.DataFrame(responseJSON['accounts'])
            df.drop(columns=['tags'], inplace=True)
            return df
        return pd.DataFrame()