import ast
import hashlib
import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime as dt
from datetime import timedelta

import numpy as np
import pandas as pd
import requests
# from cns_recon import CREDS, ENV, FBK, LOGGER
# from cns_recon.helper_funcs.db_connections import back_office, inst, veritas
# from cns_recon.helper_funcs.google_conn import GoogleSheets
# from cns_recon.helper_funcs.time_functions import datetimezone
from fireblocks_sdk import FireblocksSDK
from fireblocks_sdk.api_types import FireblocksApiException


class AUMFinance():
    def __init__(self, workspace_name, FBK):
        """
        Initialize AUM Recon Integration
        """
        # self.FBWorkspaces = ast.literal_eval(ENV['FireBlocks']['Vaults'])
        # self.FBObj = [FireblocksSDK(FBK, self.FBWorkspaces[x]) for x in self.FBWorkspaces]
        self.FBWorkspaces = [workspace_name]
        self.FBObj = [FireblocksSDK(FBK, workSpace) for workSpace in self.FBWorkspaces]
        # self.DateTime = datetimezone("US/Eastern")

    def fixTicker(self, df):
        """
        Fix tickers according to the database
        Parameters:
        ------------------------
        df: Pandas DataFrame
            DataFrame which needs to be modified
        Returns
        ------------------------
        Pandas DataFrame
        """
        df.loc[df.coin=='USDT', 'coin'] = 'USDT ERC20'
        df.loc[df.coin=='USDT_ERC20', 'coin'] = 'USDT ERC20'
        df.loc[df.coin=='DAI', 'coin'] = 'MCDAI'
        df.loc[df.coin=='KNC_OLD', 'coin'] = 'KNC'

        df.loc[df['coin'].str.endswith('_E_AVAX'), 'coin'] = df.loc[df['coin'].str.endswith('_E_AVAX'), 'coin'].str.replace('_E_AVAX', '')
        df.loc[df['coin'].str.endswith('_AVAX'), 'coin'] = df.loc[df['coin'].str.endswith('_AVAX'), 'coin'].str.replace('_AVAX', '')
        df.loc[df['coin'].str.endswith('_E'), 'coin'] = df.loc[df['coin'].str.endswith('_E'), 'coin'].str.replace('_E', '')
        df.loc[df['coin'].str.endswith('_POLYGON'), 'coin'] = df.loc[df['coin'].str.endswith('_POLYGON'), 'coin'].str.replace('_POLYGON', '')
        df.loc[df['coin'].str.endswith('_BSC'), 'coin'] = df.loc[df['coin'].str.endswith('_BSC'), 'coin'].str.replace('_BSC', '')
        df.loc[df['coin'].str.endswith('_ERC20'), 'coin'] = df.loc[df['coin'].str.endswith('_ERC20'), 'coin'].str.replace('_ERC20', '')

        return df

    def updateGSheet(self, sheetObj, sheetName, sheetRange, data):
        """
        Updates given {sheetObj}'s {sheetName} with provided {data} in specific {sheetRange}
        Parameters
        ------------------------
        sheetObj: <class> GoogleSheets Object
            Google Sheets Object for the provided sheet
        sheetName: str
            Name of the sheet where the data needs to be updated
        sheetRange: str
            Sheet Range for the data
        data: list
            properly formatted list for payload 'values'
        """

        sheetObj.batchClear([f'{sheetName}!{sheetRange}'])

        requestBody = {
                'valueInputOption': 'USER_ENTERED',
                'data':[
                    {
                        'range': f'{sheetName}!{sheetRange}',
                        'majorDimension': 'ROWS',
                        'values': data
                    }
                ]
        }
        sheetObj.batchUpdate(requestBody)
        updated_at = self.DateTime.getTimeStamp()
        requestBody = {
                'valueInputOption': 'USER_ENTERED',
                'data': [
                    {
                        'range': f'{sheetName}!B1',
                        'majorDimension': 'ROWS',
                        'values': [[updated_at]]
                    }
                ]
        }
        sheetObj.batchUpdate(requestBody)

    def reStackAddresses(self, df, indexes, obj):
        """
        Restack dataframe and fetch addresses for each vault id
        Parameters
        ------------------------
        df: Pandas DataFrame
            DataFrame that contains list of vault ids and assets
        indexes: list
            List of fields to retain
        obj: FireblocksSDK object
            obj for a particular workspace
        """
        def getAddresses(x):
            retry = 0
            while retry<3:
                #Retry thrice before raising Exception and exiting
                try:
                    #Attempt to get addresses
                    return obj.get_deposit_addresses(int(x['id']), x['coin'])
                except FireblocksApiException as e:
                    #If the request has expired, initialize the object token again and retry
                    error = e.args[0].split("server: ")[1]
                    error = json.loads(error.replace("'", '"'))
                    if type(error)!=int:
                        if error["message"] == "The request has expired":
                            retry+=1
                    elif error==500:
                        LOGGER.warning("Internal Fireblocks Error 500.. Retrying")
                        retry+=1
                    else: raise FireblocksApiException(e.args[0])

        temp = pd.DataFrame(df.set_index(indexes)['assets'].apply(pd.Series).stack()).rename(columns={0: 'assets'})
        temp = temp['assets'].apply(pd.Series).rename(columns={'id': 'coin'}).reset_index().drop(f'level_{len(indexes)}', axis=1)

        temp = temp.loc[((temp.hiddenOnUI==False) | (temp.name.str.contains('Frictional'))) | ((temp.hiddenOnUI==True) | (~temp.name.str.contains('User')))]
        print("Frictional vaults: ", len(temp.loc[(temp.name.str.contains('Frictional'))]))
        print("Hidden/Archived Vaults other than frictional and users: ", len(temp.loc[(temp.hiddenOnUI==True) & (~temp.name.str.contains('User'))]))
        temp['addresses'] = temp.apply(getAddresses, axis=1)

        temp = temp[['Origin', 'name', 'addresses']]
        temp = pd.DataFrame(temp.set_index(["Origin", "name"])['addresses'].apply(pd.Series).stack()).rename(columns={0: "addresses"})
        temp = temp['addresses'].apply(pd.Series)[['assetId', 'address', 'legacyAddress']].reset_index()
        temp = temp.drop('level_2', axis=1)

        return temp

    def processAddresses(self):
        """
        Integration to update Address YellowBook with addresses from Fireblocks
        """

        dataset = {}

        with ThreadPoolExecutor() as executor:
            threads = [executor.submit(vaultObj.get_vault_accounts, min_amount_threshold=0) for vaultObj, vaultName in zip(self.FBObj, self.FBWorkspaces)]
            [dataset.update({vaultName: pd.DataFrame(f.result())}) for vaultName, f in zip(self.FBWorkspaces, threads)]

        for frame in dataset:
            temp = dataset[frame]
            temp['Origin'] = frame
            temp = temp.loc[temp.assets.astype(str)!='[]']
            dataset[frame] = temp

        with ThreadPoolExecutor() as executor:
            threads = [executor.submit(self.reStackAddresses, dataset[frame], ['id', 'name', 'Origin', 'hiddenOnUI'], vaultObj) for frame, vaultObj in zip(dataset, self.FBObj)]
            [dataset.update({vaultName: pd.DataFrame(f.result())}) for vaultName, f in zip(self.FBWorkspaces, threads)]

        data = pd.DataFrame()

        for frame in dataset:
            data = pd.concat([data, dataset[frame]])

        data = data.rename(columns={
            'Origin': 'workspace',
            'assetId': 'asset',
            'legacyAddress': 'legacy_address',
            'name': 'vault_name'
        })

        veritas.insert_rows_multiple_conflict_nothing("recon.address_book", data, '(address, legacy_address, asset, workspace, vault_name)')

    def processVaultData(self, vaultObj, origin):
        """
        Fetch and Break down the vault asset data
        Parameters
        ------------------------
        vaultObj: FireblocksSDK object
            vaultObj for a particular workspace
        origin: str
            Label for the workspace
        Returns
        ------------------------
        Pandas DataFrame
        """
        start_time = time.time()
        df = pd.DataFrame()
        retry = 0
        while retry<3:
            try:
                print(f"Processing - {origin}")
                df = pd.DataFrame(vaultObj.get_vault_accounts(min_amount_threshold=0))
                break
            except FireblocksApiException as e:
                # LOGGER.warning(e)
                #If the request has expired, initialize the object token again and retry
                error = e.args[0].split("server: ")[1]
                error = json.loads(error.replace("'", '"'))
                if type(error)!=int:
                    if error["message"] == "The request has expired":
                        retry+=1
                elif error==500:
                    # LOGGER.warning("Internal Fireblocks Error 500.. Retrying")
                    retry+=1
                else: raise FireblocksApiException(e.args[0])

        #Return empty dataframe if no vaults
        if df.empty: return pd.DataFrame(columns = ['id', 'name', 'Origin', 'hiddenOnUI', 'coin', 'total', 'balance', 'lockedAmount', 'available', 'pending', 'frozen', 'staked', 'blockHeight'])

        INDEXES = ['id', 'name', 'Origin', 'hiddenOnUI']
        df['Origin'] = origin
        df = df.loc[df.assets.astype(str)!='[]']
        df = pd.DataFrame(df.set_index(INDEXES)['assets'].apply(pd.Series).stack()).rename(columns={0: 'assets'})

        df = df['assets'].apply(pd.Series).rename(columns={'id': 'coin'}).reset_index().drop('level_' + str(len(INDEXES)), axis=1)
        df['total'] = df['total'].astype(float)
        df['balance'] = df['balance'].astype(float)
        df['lockedAmount'] = df['lockedAmount'].astype(float)
        df['available'] = df['available'].astype(float)
        df['pending'] = df['pending'].astype(float)
        df['frozen'] = df['frozen'].astype(float)

        print(f"Finished processing for {origin} in {time.time() - start_time}s")
        return df

    def crunchVaultData(self):
        """
        Return vault accounts and their balances
        Returns
        ------------------------
        list
            Google Sheets API payload appropriate list
        """

        dataFrames = {}

        with ThreadPoolExecutor() as executor:
            threads = [executor.submit(self.processVaultData, vaultObj, vaultName) for vaultObj, vaultName in zip(self.FBObj, self.FBWorkspaces)]
            [dataFrames.update({vaultName: f.result()}) for vaultName, f in zip(self.FBWorkspaces, threads)]

        finalDF = pd.DataFrame()

        for vault in dataFrames:
            temp = dataFrames[vault]

            iwVault = temp.loc[temp.hiddenOnUI==False]
            iwUsers = temp.loc[temp.hiddenOnUI==True]

            iwVault = iwVault.drop(['hiddenOnUI'], axis=1)

            iwFrictional = iwUsers.loc[~iwUsers.name.str.contains('User')]
            iwUsers = iwUsers.loc[iwUsers.name.str.contains('User')]

            iwFrictional = iwFrictional[['coin', 'total', 'balance', 'lockedAmount', 'available', 'pending', 'frozen']].groupby('coin').sum()
            iwFrictional = iwFrictional.reset_index()
            iwFrictional['Origin'] = vault
            iwFrictional['name'] = "Frictional_Or_Hidden"

            iwUsers = iwUsers[['coin', 'total', 'balance', 'lockedAmount', 'available', 'pending', 'frozen']].groupby('coin').sum()
            iwUsers = iwUsers.reset_index()
            iwUsers['Origin'] = vault
            iwUsers['name'] = "User Wallets"

            iwVault = iwVault.groupby(['name', 'coin']).sum().reset_index()
            iwVault['Origin'] = vault

            iwVault = iwVault.loc[iwVault.name!=ENV['FireBlocks']['not_include']]

            finalDF = pd.concat([finalDF, iwVault, iwUsers, iwFrictional])

        finalDF = finalDF.fillna('')

        finalDF = self.fixTicker(finalDF.copy())

        self.updateAddSheet(finalDF[['Origin', 'name', 'coin']])

        del dataFrames
        return finalDF[['Origin', 'name', 'coin', 'total', 'balance', 'lockedAmount', 'available', 'pending', 'frozen']].values.tolist()

    def updateAddSheet(self, df):
        query = """
        select
            concat(workspace, vault_name, asset) as index,
            address,
            legacy_address
        from
            recon.address_book
        """
        addresses = veritas.create_pandas_table(query)

        df['index'] = df['Origin'] + df['name'] + df['coin']

        df = df.merge(addresses, on='index', how='left')
        df = df.fillna('')

        sheetsAPI = GoogleSheets(CREDS['google']['recon_fb'])
        self.updateGSheet(sheetsAPI, 'Addresses', 'A3:E', df[['Origin', 'name', 'coin', 'address', 'legacy_address']].values.tolist())



    def processExchangeData(self, exchanges, field):
        """
        Break down the exchange data for assets
        Parameters
        ------------------------
        exchanges: Pandas DataFrame
        field: str
            Field Name
        Returns
        ------------------------
        Pandas DataFrame
        """
        exchanges = exchanges.loc[~exchanges[field].isna()]
        df = pd.DataFrame(exchanges.rename(columns={'type':'exchange'}).set_index(['id', 'exchange'])[field].apply(pd.Series).stack()).rename(columns={0: 'assets'})
        df = df['assets'].apply(pd.Series).rename(columns={'id': 'coin'}).reset_index().drop('level_2', axis=1)
        
        if field == 'tradingAccounts':
            df = df.loc[df.assets.astype(str)!='[]']
            df = df.set_index(['id', 'exchange', 'type', 'name'])
            df = pd.DataFrame(df['assets'].apply(pd.Series).stack()).rename(columns={0: 'assets'})
            df = df['assets'].apply(pd.Series).rename(columns={'id': 'coin'}).reset_index().drop('level_4', axis=1)
        df = self.fixTicker(df)
        return df

    def crunchExchangeData(self):
        """
        Fetch and process exchange accounts from Fireblocks vaults
        Returns
        ------------------------
        list
            Google Sheets API payload appropriate list
        """
        exchanges = pd.DataFrame((self.FBObj)[0].get_exchange_accounts())

        with ThreadPoolExecutor() as executor:
            threads = [executor.submit(self.processExchangeData, exchanges, fieldName) for fieldName in ['assets', 'tradingAccounts']]
            results = [f.result() for f in threads]

        return results[0][['exchange', 'coin', 'balance', 'total', 'available', 'lockedAmount']].values.tolist(), results[1][['exchange', 'type', 'coin', 'balance', 'total', 'available', 'lockedAmount']].values.tolist()

    def initializeFBExchanges(self):
        """
        Integration for updating Fireblocks Exchange balances
        """
        dataAssets, dataTA = self.crunchExchangeData()
        
        sheetsAPI = GoogleSheets(CREDS['google']['recon_fb'])
        
        for data, sheetName, sheetRange in zip(
            [dataAssets, dataTA],
            ['Exchanges Assets', 'Exchanges TradingAccounts'],
            ['A3:F', 'A3:G']
        ):
            self.updateGSheet(sheetsAPI, sheetName, sheetRange, data)

    def initializeFB(self):
        """
        Integration for updating Fireblocks balances
        """
        data = self.crunchVaultData()

        sheetsAPI = GoogleSheets(CREDS['google']['recon_fb'])
        self.updateGSheet(sheetsAPI, "Wallets", "A3:I", data)

    def initializeBalances(self):
        """
        CoC Calculation
        """
        query= """
                select
                    loan_asset_short as coin,
                    sum(loan_amount) as "Retail Lent Out (Recievable)"
                from
                    loans
                where
                    deleted_at is NULL
                    and status = 'active'
                group by
                    loan_asset_short
                """
        loans_by_asset = back_office.create_pandas_table(query)

        query = """
                select
                    collateral_asset_short as coin,
                    sum(collateral_amount) as "retail_collateral"
                from
                    loans
                where
                    deleted_at is NULL
                    and status = 'active'
                group by
                    collateral_asset_short
                """
        colat = back_office.create_pandas_table(query)

        query = """
                select
                    loan_number,
                    loan_asset_short as coin,
                    loan_amount as "Retail Lent Out (Recievable)",
                    collateral_asset_short as coin,
                    collateral_amount as "retail_collateral"
                from
                    loans
                where
                    deleted_at is NULL
                    and status = 'active'
                """
        loan_book = back_office.create_pandas_table(query)

        query = """
                select
                    coin,
                    balance_entry_group_type_id,
                    sum(amount)
                from
                    balance_entries
                where
                    include_in_balance_calculation = True
                    and state not in  ('pending','pending_manual_approval','user_verified','signed')
                group by 
                    1,2
                """
        df = back_office.create_pandas_table(query)
        df.columns = ['coin','account_type', 'wallet_amount']
        df['account_type'] = df['account_type'].astype(str)
        type_map = {'2': 'wallet amount_yield',
                    '3':'wallet amount custody',
                    '4': 'wallet withheld (non-custody states)'}
        df['account_type']  = df['account_type'].map(type_map)
        df = df.pivot(index='coin',
                      columns='account_type',
                      values='wallet_amount').reset_index()

        df = colat.merge(df,on='coin',how='outer').fillna(0)

        query = """
                select
                    *
                from
                    balance_locked
                where 
                    include_in_balance_calculation = True
                """
        bl = back_office.create_pandas_table(query)

        bl = bl.groupby('coin').sum()['amount'].reset_index()
        bl.columns = ['coin','locked_balance']

        export = df.merge(bl,on='coin',how='left').fillna(0)
        export[['coin','retail_collateral','wallet amount_yield','locked_balance','wallet amount custody',
                'wallet withheld (non-custody states)']].set_index('coin')

        query  = """
                select
                    l.id,
                    l.borrowerid_fk,
                    bc.description as borrower_category,
                    l.start_date,
                    l.loan_number,
                    l.end_date,
                    l.open_tag,
                    l.closed_date,
                    l.note,
                    l.created,
                    l.updated,
                    l.prevloanid_fk,
                    l.terms, 
                    l.margin_call_level,
                    l.margin_refund_level,
                    l.collateral_perc,
                    l.refi,
                    l.loan_number_suffix,
                    ls.description as loan_status
                from 
                    instilend.loans l left join instilend.borrowers b on l.borrowerid_fk = b.id
                    left join instilend.borrower_categories bc on b.categoryid_fk = bc.id
                    left join instilend.loan_statustypes ls on l.statusid_fk = ls.id
                where
                    ls.description != 'Deleted'
                """
        loans = inst.create_pandas_table(query)
        loans = loans.rename(columns={'id':'loanid_fk'})
        loan_merge = loans[['loanid_fk','borrowerid_fk','open_tag','borrower_category','loan_status','loan_number','loan_number_suffix']]

        query = """
                select 
                    *
                from
                    instilend.collaterals
                """
        collat = inst.create_pandas_table(query)

        query = """
                select 
                    *
                from 
                    instilend.collateraladjs_statustypes
                """
        status = inst.create_pandas_table(query)

        query = """
                select 
                    collateraladjs.id,
                    loanid_fk,
                    origin_address,
                    destination_address,
                    collateraladjs.created,
                    collateraladjs.updated,
                    collateraladjs.statusid_fk,
                    quantity,
                    txid,
                    adjustment_date,
                    assetid_fk,
                    by_userid_fk,
                    symbol
                from 
                    instilend.collateraladjs
                left join instilend.assets al on collateraladjs.assetid_fk = al.id
                where
                    collateraladjs.statusid_fk != 999
                """
        adj = inst.create_pandas_table(query)
        adj = adj.merge(loan_merge,on='loanid_fk',how='left')
        adj.loc[(adj['statusid_fk'] == 2) & (adj['open_tag'] == False),['quantity']] = (adj['quantity'] * -1)
        adj.loc[(adj['statusid_fk'] == 1) & (adj['open_tag']),['quantity']] = (adj['quantity'] * -1)
        adjs = adj.groupby(['loanid_fk','assetid_fk']).sum()['quantity'].reset_index()

        query  = """
                select
                    *
                from 
                    instilend.assets
                """
        assets = inst.create_pandas_table(query)

        assets = assets[['id','symbol']]
        assets.columns = ['assetid_fk','symbol']
        collat = collat.merge(assets,on='assetid_fk',how='left')
        collat = collat.merge(loan_merge,on='loanid_fk',how='left')
        collat = collat.drop(collat.loc[collat['open_tag'].isna()].index)
        active_collat = collat.loc[(collat['loan_status'] == 'Active')]
        collat_summary = pd.DataFrame(active_collat.groupby(['open_tag','symbol']).sum()['quantity']).reset_index()
        collat_summary.columns = ['open_tag', 'symbol', 'collat_quantity']
        
        adjs = adjs.merge(loan_merge,on='loanid_fk',how='left')
        adjs = adjs.drop(adjs.loc[adjs['open_tag'].isna()].index)
        adjs = adjs.merge(assets,on='assetid_fk',how='left')
        active_adjs = adjs.loc[(adjs['loan_status'] == 'Active')]
        adj_summary = active_adjs.groupby(['symbol','open_tag']).sum().reset_index().drop(columns=['loan_number'])
        adj_summary.columns = ['symbol', 'open_tag', 'adj_quantity']
        query = """
                select 
                    *
                from
                    instilend.loanelements
                """
        le = inst.create_pandas_table(query)
        le = le.merge(assets,on='assetid_fk')
        le = le.merge(loan_merge,on='loanid_fk',how='left')
        active_le = le.loc[(le['loan_status'] == 'Active')]
        active_loans = pd.DataFrame(active_le.groupby(['open_tag','symbol']).sum()['quantity']).reset_index()
        active_loans.columns = ['open_tag', 'symbol', 'loan_quantity']
        inst_summary = active_loans.merge(adj_summary,on=['open_tag','symbol'],how='outer').merge(collat_summary,on=['open_tag','symbol'],how='outer')
        inst_summary.loc[inst_summary['open_tag'] == True,['open_tag']] = 'Celsius Loan'
        inst_summary.loc[inst_summary['open_tag'] == False,['open_tag']] = 'Celsius Borrow'
        inst_summary = inst_summary.fillna(0)
        inst_summary['net_collateral'] = inst_summary['adj_quantity'] + inst_summary['collat_quantity']
        inst_summary = inst_summary.set_index(['open_tag','symbol'])[['loan_quantity','net_collateral']].unstack('open_tag').fillna(0).reset_index()
        inst_summary.columns = ['coin','Celsius Loaned (Receivable)','Celsius Borrowed (Payable)','Collateral Recieved (Payable)','Collateral Posted (Receivable)']
        inst_summary.loc[inst_summary['coin'] == 'USDT',['coin']] = 'USDT ERC20'

        adj_mer = active_adjs[['loanid_fk','symbol','quantity']]

        le_mer = active_le[['loanid_fk','symbol','quantity','open_tag']]

        collat_mer = active_collat[['loanid_fk','symbol','quantity']]

        full_collat_mer = collat_mer.merge(adj_mer,on=['loanid_fk','symbol'],how='outer')

        full_collat_mer[['quantity_x','quantity_y']] = full_collat_mer[['quantity_x','quantity_y']].fillna(0)

        full_collat_mer['total_quantity'] = full_collat_mer['quantity_x'] + full_collat_mer['quantity_y']

        full_collat_mer = full_collat_mer[['loanid_fk','symbol','total_quantity']]

        full_collat_mer = full_collat_mer.loc[full_collat_mer['symbol'].isna() == False]

        cross_collat = le_mer.merge(full_collat_mer,on='loanid_fk',how='left')

        cross_collat.columns = ['loanid_fk', 'loan_symbol', 'loan_quantity', 'open_tag', 'collat_symbol', 'collat_total_quantity']

        cross_collat.loc[cross_collat['open_tag'] == True,['open_tag']] = 'Celsius Loan'

        cross_collat.loc[cross_collat['open_tag'] == False,['open_tag']] = 'Celsius Borrow'

        cross_collat = cross_collat.groupby(['loan_symbol','collat_symbol','open_tag'])['collat_total_quantity'].sum().reset_index()

        le_summary = le_mer.groupby('symbol').sum().reset_index()

        le_summary = le_summary[['symbol', 'quantity']]

        le_summary.columns = ['loan_symbol','loan_quantity']

        cross_collat = cross_collat.merge(le_summary,on='loan_symbol',how='left')

        query = """
                select
                    coin,
                    sum(amount) as "primetrust_balance (asset)"
                from
                    balance_entries be join custodians c on be.custodian_id = c.id
                where
                    include_in_balance_calculation = True
                    and c.adapter = 'primetrust'
                    and (transaction_id IS NOT NULL OR transaction_id NOT in ('', ' '))
                    and state not in  ('pending','pending_manual_approval')
                group by coin
                """
        pt = back_office.create_pandas_table(query)
        export = export[['coin','retail_collateral','wallet amount_yield','locked_balance','wallet amount custody',
                'wallet withheld (non-custody states)']]
        
        export = export.merge(inst_summary,on='coin',how='outer').fillna(0)
        export = export.merge(loans_by_asset,on='coin',how='outer').fillna(0)
        export = export.merge(pt,on='coin',how='outer').fillna(0)

        # url = 'https://api.celsius.network/priv/v1/internal_statistics/cost_of_capital'
        # r = requests.get(url)

        def flatten_json(y):
            out = {}

            def flatten(x, name=''):
                if type(x) is dict:
                    for a in x:
                        flatten(x[a], name + a + '_')
                elif type(x) is list:
                    i = 0
                    for a in x:
                        flatten(a, name + str(i) + '_')
                        i += 1
                else:
                    out[name[:-1]] = x

            flatten(y)
            return out

        # result = json.loads(r.text)['result']
        # for i,v in enumerate(result):
        #     result[i] = flatten_json(v)

        # result = pd.DataFrame(result)
        # result = result.set_index('coin').astype(float)
        
        # result['total_amount'] = result['in_kind_amount'] + result['none_amount'] + result['bronze_amount'] + result['silver_amount'] + result['gold_amount'] + result['platinum_amount'] + result['promo_none_amount'] + result['promo_bronze_amount'] + result['promo_silver_amount'] + result['promo_gold_amount'] + result['promo_platinum_amount']
        # result['earn_in_kind_total'] = result['in_kind_amount'] + result['none_amount'] + result['promo_none_amount'] + result['promo_bronze_amount'] + result['promo_silver_amount'] + result['promo_gold_amount'] + result['promo_platinum_amount']
        # result['earn_in_cel_total'] = result['bronze_amount'] + result['silver_amount'] + result['gold_amount'] + result['platinum_amount']

        # result['total_collateral'] = result['collateral_none_amount'] + result['collateral_bronze_amount'] + result['collateral_silver_amount'] + result['collateral_gold_amount'] + result['collateral_platinum_amount']

        # result['total_in_cel_collateral'] = result['collateral_bronze_amount'] + result['collateral_silver_amount'] + result['collateral_gold_amount'] + result['collateral_platinum_amount']

        # rate0 = (result['in_kind_amount'] / result['total_amount']) * result['in_kind_rate']
        # rate1 = (result['none_amount'] / result['total_amount']) * result['none_rate']
        # rate2 = (result['bronze_amount'] / result['total_amount']) * result['bronze_rate']
        # rate3 = (result['silver_amount'] / result['total_amount']) * result['silver_rate']
        # rate4 = (result['gold_amount'] / result['total_amount']) * result['gold_rate']
        # rate5 = (result['platinum_amount'] / result['total_amount']) * result['platinum_rate']
        # rate6 = (result['promo_none_amount'] / result['total_amount']) * result['promo_none_rate']
        # rate7 = (result['promo_bronze_amount'] / result['total_amount']) * result['promo_bronze_rate']
        # rate8 = (result['promo_silver_amount'] / result['total_amount']) * result['promo_silver_rate']
        # rate9 = (result['promo_gold_amount'] / result['total_amount']) * result['promo_gold_rate']
        # rate10 = (result['promo_platinum_amount'] / result['total_amount']) * result['promo_platinum_rate']

        # result['total_blended_rate'] = rate0 + rate1 + rate2 + rate3 + rate4 + rate5 + rate6 + rate7 + rate8 + rate9 + rate10

        # rate0 = (result['in_kind_amount'] / result['earn_in_kind_total']) * result['in_kind_rate']
        # rate1 = (result['none_amount'] / result['earn_in_kind_total']) * result['none_rate']
        # rate2 = (result['promo_none_amount'] / result['earn_in_kind_total']) * result['promo_none_rate']
        # rate7 = (result['promo_bronze_amount'] / result['earn_in_kind_total']) * result['promo_bronze_rate']
        # rate8 = (result['promo_silver_amount'] / result['earn_in_kind_total']) * result['promo_silver_rate']
        # rate9 = (result['promo_gold_amount'] / result['earn_in_kind_total']) * result['promo_gold_rate']
        # rate10 = (result['promo_platinum_amount'] / result['earn_in_kind_total']) * result['promo_platinum_rate']

        # result['blended_in_kind_rate'] = rate0 + rate1 + rate2 + rate7 + rate8 + rate9 + rate10

        # rate2 = (result['bronze_amount'] / result['earn_in_cel_total']) * result['bronze_rate']
        # rate3 = (result['silver_amount'] / result['earn_in_cel_total']) * result['silver_rate']
        # rate4 = (result['gold_amount'] / result['earn_in_cel_total']) * result['gold_rate']
        # rate5 = (result['platinum_amount'] / result['earn_in_cel_total']) * result['platinum_rate']

        # result['blended_earn_in_cel_rate'] = rate2 + rate3 + rate4 + rate5

        # rate0 = (result['collateral_none_amount'] / result['collateral_none_amount']) * result['collateral_none_rate']

        # result['collateral_in_kind_blended_rate'] = rate0

        # rate1 = (result['collateral_bronze_amount'] / result['total_in_cel_collateral']) * result['collateral_bronze_rate']
        # rate2 = (result['collateral_silver_amount'] / result['total_in_cel_collateral']) * result['collateral_silver_rate']
        # rate3 = (result['collateral_gold_amount'] / result['total_in_cel_collateral']) * result['collateral_gold_rate']
        # rate4 = (result['collateral_platinum_amount'] / result['total_in_cel_collateral']) * result['collateral_platinum_rate']

        # result['collateral_in_cel_blended_rate'] = rate1 + rate2 + rate3 + rate4

        # rate0 = (result['collateral_none_amount'] / result['total_collateral']) * result['collateral_none_rate']
        # rate1 = (result['collateral_bronze_amount'] / result['total_collateral']) * result['collateral_bronze_rate']
        # rate2 = (result['collateral_silver_amount'] / result['total_collateral']) * result['collateral_silver_rate']
        # rate3 = (result['collateral_gold_amount'] / result['total_collateral']) * result['collateral_gold_rate']
        # rate4 = (result['collateral_platinum_amount'] / result['total_collateral']) * result['collateral_platinum_rate']

        # result['total_collateral_blended_rate'] = rate0 + rate1 + rate2 + rate3 + rate4

        # result['earning_in_cel_asset_percent'] = result['earn_in_cel_total'] / result['total_amount']

        # result['earning_in_cel_collateral_asset_percent'] = result['total_in_cel_collateral'] / result['total_collateral']

        # result = result.reset_index()

        # retail_cost_capital = result[['coin','total_blended_rate','blended_in_kind_rate','blended_earn_in_cel_rate','total_collateral_blended_rate','collateral_in_kind_blended_rate','collateral_in_cel_blended_rate','earning_in_cel_asset_percent','earning_in_cel_collateral_asset_percent']].fillna(0)

        # export = export.merge(retail_cost_capital,on='coin',how='left').fillna(0)

        # export['wallet_assets_earning_in_cel'] = export['wallet_amount'] * export['earning_in_cel_asset_percent']

        active_loans.columns = ['open_tag','symbol','total_loan_sum']

        active_le = active_le.merge(active_loans,on= ['open_tag','symbol'],how='left')

        active_le['blended_rate'] = (active_le['quantity'] / active_le['total_loan_sum']) * active_le['interest_rate']

        inst_blended_rates = pd.DataFrame(active_le.groupby(['open_tag','symbol'])['blended_rate'].sum()).unstack('open_tag').fillna(0).reset_index()

        inst_blended_rates.columns = ['coin','Lend at APR %', 'Borrow at APR %']

        inst_blended_rates.loc[inst_blended_rates['coin'] == 'USDT',['coin']] = 'USDT ERC20'

        inst_blended_rates[['Lend at APR %', 'Borrow at APR %']] = inst_blended_rates[['Lend at APR %', 'Borrow at APR %']] / 100

        export = export.merge(inst_blended_rates,on='coin',how='left')

        query= """
                select
                    loan_asset_short as coin,
                    loan_amount,
                    interest_rate as interest_rate
                from
                    loans
                where
                    deleted_at is NULL
                    and status = 'active'
                """
        loan_income = back_office.create_pandas_table(query)

        loan_income = loan_income.merge(loans_by_asset,on='coin',how='left')

        loan_income['retail_loans_blended_rate'] = loan_income['loan_amount'] / loan_income['Retail Lent Out (Recievable)'] * loan_income['interest_rate']
        retail_loan_rates = (loan_income.groupby('coin')['retail_loans_blended_rate'].sum() / 100).reset_index()
        export = export.merge(retail_loan_rates,on='coin',how='left').fillna(0)

        #collat = collat.drop(columns=['margin_call_level','margin_refund_level'])
        collat = collat.merge(loans[['loanid_fk','collateral_perc','margin_call_level','margin_refund_level']],on='loanid_fk',how='left')
        collat_given = collat.loc[(collat['open_tag']) & (collat['loan_status'].isin(['Active','Pending']))][['loan_number','loan_number_suffix','borrowerid_fk','loan_status','symbol','quantity', 'margin_call_level','margin_refund_level','collateral_perc']]
        collat_recieved = collat.loc[(collat['open_tag'] == False) & (collat['loan_status'].isin(['Active','Pending']))][['loan_number','loan_number_suffix','borrowerid_fk','loan_status','symbol','quantity', 'margin_call_level','margin_refund_level','collateral_perc']]
        
        loans_recieved = le.loc[(le['open_tag']) & (le['loan_status'].isin(['Active','Pending']))][['loan_number','loan_number_suffix','borrowerid_fk','loan_status','symbol','quantity']]
        loans_given = le.loc[(le['open_tag'] == False) & (le['loan_status'].isin(['Active','Pending']))][['loan_number','loan_number_suffix','borrowerid_fk','loan_status','symbol','quantity']]
        
        adj['type'] = np.NaN
        adj.loc[adj['statusid_fk'] == 1,['type']] = 'Margin Call'
        adj.loc[adj['statusid_fk'] == 2,['type']] = 'Reverse Call'
        adj = adj.loc[adj['statusid_fk']!=3]
        adj_on_borrows = adj.loc[(adj['open_tag']) & (adj['loan_status'].isin(['Active','Pending']))][['adjustment_date','type','loan_number','loan_number_suffix','borrowerid_fk','loan_status','symbol','quantity']]
        adj_on_loans = adj.loc[(adj['open_tag'] == False) & (adj['loan_status'].isin(['Active','Pending']))][['adjustment_date','type','loan_number','loan_number_suffix','borrowerid_fk','loan_status','symbol','quantity']]
        adj_on_borrows = adj_on_borrows.sort_values('adjustment_date',ascending=False)
        adj_on_loans = adj_on_loans.sort_values('adjustment_date',ascending=False)

        adj_on_borrows['adjustment_date'] = pd.to_datetime(adj_on_borrows['adjustment_date']).dt.strftime("%Y-%m-%d")
        adj_on_loans['adjustment_date'] = pd.to_datetime(adj_on_loans['adjustment_date']).dt.strftime("%Y-%m-%d")


        #CN Transactions
        def returnTypeName(x):
            return x['type'], x['name']

        CN_txs = self.FBObj[0].get_transactions(status='COMPLETED', after=f'{self.DateTime.getEpoch(self.DateTime.getRelativeDate(days=30), format="%Y-%m-%d")}', limit=100000, order_by='createdAt')
        CN_txs = pd.DataFrame(CN_txs)
        CN_txs['createdAt'] = pd.to_datetime(CN_txs['createdAt'], unit='ms').dt.strftime("%Y-%m-%d")
        for x in ['source', 'destination']:
            CN_txs[f'{x}Type'], CN_txs[f'{x}Name'] = zip(*CN_txs[x].apply(returnTypeName))

        CN_txs = CN_txs[['createdAt', 'assetId', 'sourceType', 'sourceName', 'destinationType', 'destinationName', 'amount', 'destinationAddress']]
        CN_txs = CN_txs.rename(columns={'assetId': 'coin'})
        CN_txs = self.fixTicker(CN_txs)
        CN_txs = CN_txs.fillna('')

        export = export.fillna('')
        sheetsAPI = GoogleSheets(CREDS['google']['recon_retail'])
        cols_order = export.columns.tolist()
        cols_order.remove('wallet amount custody')
        cols_order.remove('wallet withheld (non-custody states)')
        cols_order.append('wallet amount custody')
        cols_order.append('wallet withheld (non-custody states)')

        self.updateGSheet(sheetsAPI, "Retail Balances", "A3:V", export[cols_order].values.tolist())

        for data, sheetName, sheetRange in zip(
            [collat_given, collat_recieved, loans_recieved, loans_given, adj_on_borrows, adj_on_loans, CN_txs], 
            ['Collateral Given', 'Collateral Received', 'Loans Received', 'Loans Given', 'Margin Calls on Borrows', 'Margin Calls on Loans', 'CN FB Transactions'],
            ["A3:I", "A3:I", "A3:F", "A3:F", "A3:H", "A3:H", "A3:H"]
        ):
            data = data.fillna('')
            if 'borrowerid_fk' in data:
                data['borrowerid_fk'] = data['borrowerid_fk'].apply(lambda x: hashlib.sha512(x.encode()).hexdigest())
            sheetsAPI = GoogleSheets(CREDS['google']['recon_inst'])
            self.updateGSheet(sheetsAPI, sheetName, sheetRange, data.values.tolist())

        sheetsAPI = GoogleSheets(CREDS['google']['retail_loan_book'])
        self.updateGSheet(sheetsAPI, "Retail_loan_book", "A4:E", loan_book.values.tolist())

        updated_at = self.DateTime.getTimeStamp()
        self.updateGSheet(sheetsAPI, "Retail_loan_book", "B1", [[updated_at]])

    def RewardsDeposits(self):
        today = dt.today()
        this_year = str(today.year) + '-01-01 00:00:00'

        this_year

        month = 0
        if today.month < 10:
            month = '0' + str(today.month)
        else:
            month = str(today.month)

        this_month = str(today.year) + '-' + month + '-01  00:00:00'

        friday = 4
        days_since_week_start = friday - today.weekday()
        week_start_date = today - timedelta(days=days_since_week_start)

        last_week_start = str((week_start_date - timedelta(days=7)).date()) +  ' 00:00:00'

        week_start_date = str(week_start_date.date()) +  ' 00:00:00'

        yesterday = str((today - timedelta(days=1)).date()) +  ' 00:00:00'

        query = f"""
                select
                    coin,
                    sum(amount) as rewards_since_inception
                from
                    balance_entries
                where
                    include_in_balance_calculation = True
                    and descriptive_purpose = 'interest'
                group by coin
                """
        df1 = back_office.create_pandas_table(query)

        query = f"""
                select
                    coin,
                    sum(amount) as rewards_this_year
                from
                    balance_entries
                where
                    include_in_balance_calculation = True
                    and descriptive_purpose = 'interest'
                    and date >= '{this_year}'
                group by coin
                """
        df2 = back_office.create_pandas_table(query)

        query = f"""
                select
                    coin,
                    sum(amount) as rewards_this_month
                from
                    balance_entries
                where
                    include_in_balance_calculation = True
                    and descriptive_purpose = 'interest'
                    and date >= '{this_month}'
                group by coin
                """
        df3 = back_office.create_pandas_table(query)

        query = f"""
                select
                    coin,
                    sum(amount) as rewards_last_week
                from
                    balance_entries
                where
                    include_in_balance_calculation = True
                    and descriptive_purpose = 'interest'
                    and date >= '{last_week_start}'
                    and date < '{week_start_date}'
                group by coin
                """
        df4 = back_office.create_pandas_table(query)

        query = f"""
                select
                    coin,
                    sum(amount) as rewards_this_week
                from
                    balance_entries
                where
                    include_in_balance_calculation = True
                    and descriptive_purpose = 'interest'
                    and date >= '{week_start_date}'
                    and date < '{str(today.date())}'
                group by coin
                """
        df5 = back_office.create_pandas_table(query)

        query = f"""
                select
                    coin,
                    sum(amount) as rewards_yesterday
                from
                    balance_entries
                where
                    include_in_balance_calculation = True
                    and descriptive_purpose = 'interest'
                    and date >= '{yesterday}'
                    and date < '{str(today.date())}'
                group by coin
                """
        df6 = back_office.create_pandas_table(query)

        df = df1.merge(df2,on='coin',how='left').merge(df3,on='coin',how='left').merge(df4,on='coin',how='left').merge(df5,on='coin',how='left').merge(df6,on='coin',how='left')

        df = df.fillna(0)

        query = f"""
                select
                    coin,
                    sum(amount) as balances_end_of_last_week
                from
                    balance_entries
                where
                    include_in_balance_calculation = True
                    and date < '{week_start_date}'
                group by coin
                """
        b = back_office.create_pandas_table(query)

        query = f"""
                select
                    coin,
                    sum((((DATE_PART('day', date - '{week_start_date}') * 86400) + (DATE_PART('hour', date - '{week_start_date}') * 1440) + (DATE_PART('minute', date - '{week_start_date}') * 60) + (DATE_PART('second', date - '{week_start_date}'))) / 604800) * amount) as proportion_week 
                from
                    balance_entries
                where
                    include_in_balance_calculation = True
                    and date > '{week_start_date}'
                group by coin
                """
        b1 = back_office.create_pandas_table(query)

        b = b.merge(b1,on='coin',how='outer').fillna(0)

        b['week_principal'] = b.sum(axis=1)
        b = b[['coin','week_principal']]

        url = 'https://api.celsius.network/priv/v1/internal_statistics/cost_of_capital'
        r = requests.get(url)

        def flatten_json(y):
            out = {}

            def flatten(x, name=''):
                if type(x) is dict:
                    for a in x:
                        flatten(x[a], name + a + '_')
                elif type(x) is list:
                    i = 0
                    for a in x:
                        flatten(a, name + str(i) + '_')
                        i += 1
                else:
                    out[name[:-1]] = x

            flatten(y)
            return out

        result = json.loads(r.text)['result']

        for i,v in enumerate(result):
            result[i] = flatten_json(v)

        result = pd.DataFrame(result)

        result = result.set_index('coin').astype(float)

        result['total_amount'] = result['in_kind_amount'] + result['none_amount'] + result['bronze_amount'] + result['silver_amount'] + result['gold_amount'] + result['platinum_amount'] + result['promo_none_amount'] + result['promo_bronze_amount'] + result['promo_silver_amount'] + result['promo_gold_amount'] + result['promo_platinum_amount']

        result['earn_in_kind_total'] = result['in_kind_amount'] + result['none_amount'] + result['promo_none_amount'] + result['promo_bronze_amount'] + result['promo_silver_amount'] + result['promo_gold_amount'] + result['promo_platinum_amount']

        result['earn_in_cel_total'] = result['bronze_amount'] + result['silver_amount'] + result['gold_amount'] + result['platinum_amount']

        rate0 = (result['in_kind_amount'] / result['earn_in_kind_total']) * result['in_kind_rate']
        rate1 = (result['none_amount'] / result['earn_in_kind_total']) * result['none_rate']
        rate2 = (result['promo_none_amount'] / result['earn_in_kind_total']) * result['promo_none_rate']
        rate7 = (result['promo_bronze_amount'] / result['earn_in_kind_total']) * result['promo_bronze_rate']
        rate8 = (result['promo_silver_amount'] / result['earn_in_kind_total']) * result['promo_silver_rate']
        rate9 = (result['promo_gold_amount'] / result['earn_in_kind_total']) * result['promo_gold_rate']
        rate10 = (result['promo_platinum_amount'] / result['earn_in_kind_total']) * result['promo_platinum_rate']

        result['blended_in_kind_rate'] = rate0 + rate1 + rate2 + rate7 + rate8 + rate9 + rate10

        rate2 = (result['bronze_amount'] / result['earn_in_cel_total']) * result['bronze_rate']
        rate3 = (result['silver_amount'] / result['earn_in_cel_total']) * result['silver_rate']
        rate4 = (result['gold_amount'] / result['earn_in_cel_total']) * result['gold_rate']
        rate5 = (result['platinum_amount'] / result['earn_in_cel_total']) * result['platinum_rate']

        result['blended_earn_in_cel_rate'] = rate2 + rate3 + rate4 + rate5
        result['earning_in_cel_asset_percent'] = result['earn_in_cel_total'] / result['total_amount']

        b = b.merge(result,on='coin',how='left')
        b['this_week_accrued_cel_interest'] = (b['week_principal'] * (1 - b['earning_in_cel_asset_percent']) * b['blended_in_kind_rate'] / 52)
        b['this_week_accrued_in_kind_interest'] = (b['week_principal'] * (b['earning_in_cel_asset_percent']) * b['blended_earn_in_cel_rate'])
        b = b[['coin', 'this_week_accrued_cel_interest','this_week_accrued_in_kind_interest']].reset_index()

        df = df.merge(b,on='coin',how='outer')
        df.fillna(0, inplace=True)
        data = df.values.tolist()

        """self.sheetsAPI.batchClear(['Data Pull - RewardsOnDeposits!A3:AA'])
        requestBody = {
                'valueInputOption': 'USER_ENTERED',
                'data':[
                    {
                        'range': 'Data Pull - RewardsOnDeposits!A3:AA',
                        'majorDimension': 'ROWS',
                        'values': data
                    }
                ]
        }
        self.sheetsAPI.batchUpdate(requestBody)
        updated_at = self.DateTime.getTimeStamp()
        requestBody = {
            'valueInputOption': 'USER_ENTERED',
            'data': [
                {
                    'range': 'Data Pull - RewardsOnDeposits!B1',
                    'majorDimension': 'ROWS',
                    'values': [[updated_at]]
                }
            ]
        }
        self.sheetsAPI.batchUpdate(requestBody)"""

