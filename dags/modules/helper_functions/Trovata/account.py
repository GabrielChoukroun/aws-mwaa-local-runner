class AccountClient(object):

    def __init__(self, **kwargs):
        """
        Create the request client instance.
        :param kwargs: The option of request connection.
            api_key: The public key applied from Huobi.
            secret_key: The private key applied from Huobi.
            url: The URL name like "https://api.huobi.pro".
            init_log: Init logger, default is False, True will init logger handler
        """
        self.__kwargs = kwargs

    def get_accounts(self):
        """
        Get the account list.
        :return: The list of accounts data.
        """

        from huobi.service.account.get_accounts import GetAccountsService
        return GetAccountsService({}).request(**self.__kwargs)

class GetAccountsService:

    def __init__(self, params):
        self.params = params

    def request(self, **kwargs):
        channel = "/v1/account/accounts"

        def parse(dict_data):
            data_list = dict_data.get("data", [])
            return default_parse_list_dict(data_list, Account, [])

        return RestApiSyncClient(**kwargs).request_process(HttpMethod.GET_SIGN, 
                                            channel, self.params, parse)