import requests

class Polygonscan:
    """
    Provides methods to fetch data from Polygonscan.io

    ...

    Methods
    -------
    fetchTokenTx()
        Returns last set of internal transactions for the provided address
        
    fetchTxList()
        Returns last set of MATIC transactions for the provided address
        
    fetchHistorical()
        Returns last set of transactions for provided action method | 'tokentx', 'tokenlist'
        
    fetchAllTxs()
        Returns all historical transactions for provided address
    """
    def __init__(self, API_KEY):
        self.API_KEY = API_KEY
        self.BASE_URL = "https://api.polygonscan.com/api"
        
    def _executeRequest(self, **kwargs):
        response = requests.get(self.BASE_URL, data=self._params(**kwargs))
        if response.status_code==200: 
            if response.json()['message']=='NOTOK': 
                if response.json()['result']=="Error! Invalid address format":
                    return None
                else:
                    return self._executeRequest(**kwargs)
            return response
        else: return self._executeRequest(**kwargs)
        
    def _params(self, **kwargs):
        data = dict(**kwargs)
        if 'apikey' in data: return data
        else: 
            data['apikey'] = self.API_KEY
            return data
    
    def fetchTokenTx(self, address, endBlock=None):
        """
        Returns last set of internal transactions for the provided address

        Parameters
        ----------
        address : str
            Polygon address
        
        endBlock : int (optional)
            Ending Block to provide on API call
            
        method: function
        
        Returns
        -------
        <response object>
        """
        if endBlock is not None: return self._executeRequest(module='account', action='tokentx', address=address, endblock=endBlock, sort='desc')
        else: return self._executeRequest(module='account', action='tokentx', address=address, sort='desc')
        
    def fetchTxList(self, address, endBlock=None):
        """
        Returns last set of normal transactions for the provided address

        Parameters
        ----------
        address : str
            Polygon address
        
        endBlock : int (optional)
            Ending Block to provide on API call
            
        method: function
        
        Returns
        -------
        <response object>
        """
        if endBlock is not None: return self._executeRequest(module='account', action='txlist', address=address, endblock=endBlock, sort='desc')
        else: return self._executeRequest(module='account', action='txlist', address=address, sort='desc')
    
    def fetchTransactions(self, address, action, startBlock):
        """
        Returns last set of all Normal|Internal for the provided address from a startBlock

        Parameters
        ----------
        address : str
            Polygon address
        
        action : str
            Determines which set of transactions: Normal or Internal

        endBlock : int
            Starting Block to provide on API call
            
        method: function
        
        Returns
        -------
        <response object>
        """
        result = []
        while True:

            print('Entering poly_obj method')
            response = self._executeRequest(module='account', action=action, address=address, startBlock=startBlock)

            if response is None: return result
            if response.json()['message']=='No transactions found': return result
            
            result += response.json()['result']

            temp = int(max(response.json()['result'], key=lambda x:x['blockNumber'])['blockNumber'])
            
            print(temp)
            startBlock=temp+1

    def fetchHistorical(self, address, method):
        """
        Returns last set of transactions for provided action method | 'tokentx', 'tokenlist'

        Parameters
        ----------
        address : str
            Polygon address
            
        method: function
        
        Returns
        -------
        list
        """
        response = method(address)
        if response is None: return []
        if response.json()['message']=='No transactions found': return []
        
        endBlock = min(response.json()['result'], key=lambda x:x['blockNumber'])['blockNumber']
        
        result = response.json()['result']
        while True:
            response = method(address, endBlock)

            temp = min(response.json()['result'], key=lambda x:x['blockNumber'])['blockNumber']
            if temp == endBlock: break
            endBlock = temp

            result += response.json()['result']
        
        #Remove any duplicates and return the set of transactions
        return [dict(t) for t in {tuple(sorted(d.items())) for d in result}]                                
        
    def fetchAllTxs(self, address):
        """
        Returns all historical transactions for provided address

        Parameters
        ----------
        address : str
            Polygon address
        
        Returns
        -------
        list
        """
        tokentx = self.fetchHistorical(address, self.fetchTokenTx)
        txlist = self.fetchHistorical(address, self.fetchTxList)
        
        return tokentx + txlist
