import numpy as np
import pandas as pd
from binance.client import Client

from wired_exchange.core import to_transactions
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
        balances = pd.DataFrame(self._httpClient.get_account()['balances'])
        balances['free'] = pd.to_numeric(balances['free'])
        balances['total'] = pd.to_numeric(balances['locked']) + balances['free']
        balances = balances[(balances['total'] > 0)]
        balances.drop(columns=['locked'], inplace=True)
        balances.rename(columns={'asset': 'currency', 'free': 'available'}, inplace=True)
        balances['platform'] = self.platform
        # try:
        #     tickers = self._httpClient.get_all_tickers()
        #     balances = balances.merge(tickers, left_index=True,
        #                               right_on='currency', how='left')
        #     balances.drop(columns=['symbol', 'symbolName'], inplace=True)
        # except:
        #     self._logger.warning('cannot retrieve current tickers', exc_info=True)
        balances.set_index('currency', inplace=True)
        return balances

    def get_transactions(self, symbol: str = None):
        self.open()
        if symbol is not None:
            transactions = self._httpClient.get_all_orders(symbol=symbol)
        else:
            transactions = None
            for currency in self.get_balances().index:
                if currency != 'USDT':
                    try:
                        current_transactions = pd.DataFrame(self._httpClient.get_all_orders(symbol=f'{currency}USDT'))
                        current_transactions['base_currency'] = currency
                        current_transactions['quote_currency'] = 'USDT'
                        if transactions is None:
                            transactions = current_transactions
                        else:
                            transactions = transactions.append(current_transactions, ignore_index=True)
                    except:
                        self._logger.error(f'{currency}: cannot retrieve orders for currency', exc_info=True)
        return self._to_transactions(transactions)

    def _to_transactions(self, orders: dict) -> pd.DataFrame:
        tr = pd.DataFrame(orders)
        if tr.size == 0:
            return tr
        tr['time'] = pd.to_datetime(tr['time'], unit='ms', utc=True)
        tr['fee'] = np.NAN
        tr['fee_currency'] = None
        tr.rename(
            columns=dict(orderId='order_id', cummulativeQuoteQty='amount', origQty='size'), inplace=True)
        tr.drop(['symbol', 'orderListId', 'clientOrderId', 'executedQty', 'timeInForce',
                 'type', 'stopPrice', 'icebergQty', 'updateTime', 'isWorking', 'origQuoteOrderQty'],
                axis='columns', inplace=True)
        tr.astype(dict(order_id='string'))
        tr['platform'] = self.platform
        tr['id'] = tr['order_id'].apply(lambda id: f'{self.platform}_{id}')
        return to_transactions(tr[tr['status'] != 'CANCELED'])