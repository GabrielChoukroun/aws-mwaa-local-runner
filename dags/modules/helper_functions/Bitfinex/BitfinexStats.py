from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import json

class BitfinexStats:
    def __init__(self):
        pass 

    def getLedgerInfo(self, bfxObj, category=None, startTime=None, endTime=None, limit=2500):
        """
        https://docs.bitfinex.com/reference#rest-auth-ledgers
        Parameters
        ------------------------
        obj: Bitfinex object
        """
        body = {
            "category": category,
            "start": startTime,
            "end": endTime,
            "limit": limit
        }
        
        body = {key: value for key, value in body.items() if value is not None}
        response = bfxObj.executeRequest('v2/auth/r/ledgers/hist', body=body)
        
        columns = [
            'ID',
            'CURRENCY',
            'PLACEHOLDER',
            'MTS',
            'PLACEHOLDER',
            'AMOUNT',
            'BALANCE',
            'PLACEHOLDER',
            'DESCRIPTION'
        ]
        print(f'response status code: {response.status_code}')
        print(f'response reason: {response.reason}')
        df = pd.DataFrame(response.json(), columns=columns)
        df.drop(['PLACEHOLDER'], axis=1, inplace=True)
        
        df.loc[df['CURRENCY']=='DSH', 'CURRENCY'] = 'DASH'
        df.loc[df['CURRENCY']=='UST', 'CURRENCY'] = 'USDt'
        df.loc[df['CURRENCY']=='ALG', 'CURRENCY'] = 'ALGO'
        df.loc[df['CURRENCY']=='UDC', 'CURRENCY'] = 'USDC'
            
        return df
    
    def getAllTickerPairs(self, bfxObj, columnInfoDF):
        """
        https://docs.bitfinex.com/reference#rest-public-tickers
        Parameters
        ------------------------
        obj: Bitfinex object
        """
        # Call all currencies
        endPoint = "v2/tickers?symbols=ALL"
        response = bfxObj.executeRequest(endPoint, body={}, AUTH=False).json()
        # split currencies into funding and trading
        trading = self.getTradingPairsFromResponse(response)
        # get column names
        tradingColumnTypeDF = self.getColumnsAndTypes(columnInfoDF)
        # Make final dataframe to insert
        tradingDF = pd.DataFrame(data = trading, columns = tradingColumnTypeDF.column_name)
        
        return tradingDF

    def getTradingPairsFromResponse(self, response):
        '''
        Gets trading pairs from an API call to 
        https://api-pub.bitfinex.com/v2/tickers?symbols=ALL
        response is the raw string returned by the call, and we filter for trading pairs
        '''
        return [pair for pair in response if pair[0].startswith('t')]

    def getColumnsAndTypes(self, columnInfoDF):
        columnTypeDF = columnInfoDF[['column_name', 'data_type']]
        columnTypeDF.data_type = columnInfoDF.data_type.apply(json.loads)
        typeDict = {"FIXED":int, "TEXT":str, "REAL":float}
        columnTypeDF['python_type'] = [typeDict[columnTypeDF.data_type[i]['type']] for i in range(len(columnTypeDF))]
        return columnTypeDF[['column_name', 'python_type']]

    

    def getAllFundingTrades(self, bfxObj, endPoint, APIFields, dropColumns):
        """
        https://api.bitfinex.com/v2/auth/r/funding/trades/Symbol/hist
        Parameters
        --------------
        bfxObj: An instance of Bitfinex class
        endPoint: A string representing the target endPoint, to be passed to the bfxObj
        APIFields: A list of strings representing field names in the API
        dropColumns: A list of strings representing fields to drop IE "PLACEHOLDER"
        """
        # Get the response from the endPoint
        response = bfxObj.executeRequest(endPoint)
        # Load 
        print(f'responseFundingTrades response: {response.json()}')
        print(f'columns used for DF: {APIFields}')
        df = pd.DataFrame(data=response.json(), columns=APIFields)
        print(f'first row: {df.head(1)}')
        df.drop(dropColumns, axis=1, inplace=True)
        return df

    def getAllFundingInfo(self, bfxObj, symbols, columns):
        with ThreadPoolExecutor() as executor:
            threads = [executor.submit(self.getOneFundingInfo, bfxObj, symbol) for symbol in symbols]
            response = [f.result() for f in threads]
        fundingInfo = pd.concat(response)
        print(f'before columns: {fundingInfo.columns}')
        print(f'columns to apply: {columns}')
        fundingInfo.columns = columns
        print(f'after columns: {fundingInfo.columns}')

        return fundingInfo

    def getOneFundingInfo(self, bfxObj, symbol):
        """
        https://docs.bitfinex.com/reference#rest-auth-info-funding
        Parameters
        ------------------------
        obj: Bitfinex object
        """
        fieldNames = ['symbol', 'info']
        response = bfxObj.executeRequest('v2/auth/r/info/funding/' + symbol)
        response = pd.DataFrame([response.json()[1:]], columns = fieldNames).set_index(['symbol'])['info'].apply(pd.Series).reset_index()
        return response