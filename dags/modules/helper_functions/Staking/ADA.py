from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from airflow.models import Variable
from modules.helper_functions.google_conn import GoogleSheets

from blockfrost import BlockFrostApi, ApiError

bfAPI = BlockFrostApi(
    project_id=Variable.get('blockfrost_project_id')
)

GAUTH_CREDS = None


class stakingADA:

    def getADAStakingKeys(self):
        sheetsAPI = GoogleSheets(Variable.get('google_recon'))
        addresses = sheetsAPI.batchGet(["[Pull] ADA Stake Keys!A3:A"], valueRenderOption='FORMATTED_VALUE')[0]['values']
        addresses = [x[0] for x in addresses]

        return addresses

    def getAccounts(self, stakeAddress):
        try:
            # https://github.com/blockfrost/blockfrost-python
            response = bfAPI.accounts(stake_address=stakeAddress, return_type='pandas')
            return response

        except ApiError as e:
            print(e)

    def getBFRewards(self, stakeAddress):
        try:
            # https://github.com/blockfrost/blockfrost-python
            response = bfAPI.account_rewards(stake_address=stakeAddress, return_type='pandas')
            return response

        except ApiError as e:
            print(e)

    def getBFWithdrawals(self, stakeAddress):
        try:
            # https://github.com/blockfrost/blockfrost-python
            response = bfAPI.account_withdrawals(stake_address=stakeAddress, return_type='pandas')
            return response

        except ApiError as e:
            print(e)

    def getBFAddresses(self, stakeAddress):
        try:
            # https://github.com/blockfrost/blockfrost-python
            response = bfAPI.account_addresses(stake_address=stakeAddress, return_type='pandas')
            response['STAKING_KEY'] = stakeAddress
            return response

        except ApiError as e:
            print(e)

    def getBFAddressTotal(self, address):
        try:
            # https://github.com/blockfrost/blockfrost-python
            response = bfAPI.address_total(address=address, return_type='pandas')
            return response

        except ApiError as e:
            print(e)

    def getStats(self):
        try:
            df = pd.DataFrame()
            with ThreadPoolExecutor(max_workers=2) as executor:
                threads = [executor.submit(self.getAccounts, address) for address in self.getADAStakingKeys()]
                for f in threads:
                    df = pd.concat([df, f.result()])
            
            return df
        except Exception as err:
            raise err

    def getRewards(self):
        try:
            df = pd.DataFrame()
            with ThreadPoolExecutor(max_workers=2) as executor:
                threads = [executor.submit(self.getBFRewards, address) for address in self.getADAStakingKeys()]
                for f in threads:
                    df = pd.concat([df, f.result()])

            df.columns = df.columns.str.upper()
            df['EPOCH_POOL_ID'] = df.apply(
                lambda row: str(row['EPOCH']) + "_" + row['POOL_ID'] + "_" + row['AMOUNT'], axis=1)
            df = df.convert_dtypes()
            df.drop_duplicates()

            return df
            
        except Exception as err:
            raise err

    def getWithdrawals(self):
        try:
            df = pd.DataFrame()
            with ThreadPoolExecutor(max_workers=2) as executor:
                threads = [executor.submit(self.getBFWithdrawals, address) for address in self.getADAStakingKeys()]
                for f in threads:
                    df = pd.concat([df, f.result()])
            
            return df
            
        except Exception as err:
            raise err

    def getAddressesTotal(self, addys):
        
        try:
            dfAddy = pd.DataFrame()
            with ThreadPoolExecutor(max_workers=2) as executor:
                threads = [executor.submit(self.getBFAddressTotal, address) for address in addys]
                for f in threads:
                    dfTemp = f.result()
                    received_sum = dfTemp['received_sum'][0]
                    if received_sum[0].get("unit") == "lovelace":
                        dfTemp['RECEIVED_SUM_LOVELACE'] = received_sum[0].get("quantity")
                        
                    received_sum2 = dfTemp['received_sum'].to_json()
                    dfTemp['received_sum'] = dfTemp.apply(
                        lambda row: str(row['received_sum'])[:254], axis=1)

                    sent_sum = dfTemp['sent_sum'][0]
                    if sent_sum[0].get("unit") == "lovelace":
                        dfTemp['SENT_SUM_LOVELACE'] = sent_sum[0].get("quantity")
                    dfTemp['sent_sum'] = dfTemp.apply(
                        lambda row: str(row['sent_sum']), axis=1)

                    dfAddy = pd.concat([dfAddy, dfTemp])
            
            return dfAddy
            
        except Exception as err:
            raise err

    def getAddresses(self):
        try:
            df = pd.DataFrame()
            with ThreadPoolExecutor(max_workers=2) as executor:
                threads = [executor.submit(self.getBFAddresses, address) for address in self.getADAStakingKeys()]
                for f in threads:
                    df = pd.concat([df, f.result()])
            
            addys = df['address'].tolist()
            
            return df
            
        except Exception as err:
            raise err


