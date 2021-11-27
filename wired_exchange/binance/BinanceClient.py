import pandas as pd
from binance.client import Client

from wired_exchange.core.ExchangeClient import ExchangeClient


class BinanceClient(ExchangeClient):
    def __init__(self, api_key=None, api_secret=None):
        super().__init__('binance', api_key, api_secret, None)

    def open(self):
        if self._httpClient is None:
            self._httpClient = Client(self._api_key, self._api_secret)
            self._logger.debug(f'http client for {self} instantiated')
        return self

    def close(self):
        if self._httpClient is not None:
            self._httpClient.close_connection()
            self._logger.debug('http client closed')
        self._httpClient = None

    def get_balances(self):
        self.open()
        wallet = pd.DataFrame(self._httpClient.get_account()['balances'])
        wallet['free'] = pd.to_numeric(wallet['free'])
        wallet['locked'] = pd.to_numeric(wallet['locked'])
        wallet = wallet[(wallet['free'] > 0) | (wallet['locked'] > 0)]
        wallet.set_index('asset')
        return wallet
