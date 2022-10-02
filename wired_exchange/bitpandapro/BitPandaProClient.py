from datetime import datetime

import numpy as np
import pandas as pd

from wired_exchange.core import to_transactions, to_isoformat, merge
from wired_exchange.core.ExchangeClient import ExchangeClient

from typing import Union, Literal

from wired_exchange.exchange_rates.ExchangeRatesClient import ExchangeRatesClient


class BitPandaProClient(ExchangeClient):
    """BitPanda Pro API client"""

    def __init__(self, api_secret=None, host_url=None):
        super().__init__('bitpanda_pro', None, api_secret, host_url, always_authenticate=False)

    def _authenticate(self, request):
        request.headers["Authorization"] = "Bearer " + self._api_key
        self._logger.debug('authentication headers added')

    def get_orders(self, start_time: Union[datetime, int, float, None] = None,
                   end_time: Union[datetime, int, float, None] = None,
                   include_filled: bool = True):
        self.open()
        params = {'with_just_orders': True, 'with_just_filled_inactive': False}
        self._set_date_range_params(params, start_time, end_time)
        try:
            response = self._send_get(f'/v1/account/orders', params=params, authenticated=True)
            orders = list(map(lambda x: x['order'], response['order_history']))
            if include_filled:
                params['with_just_filled_inactive'] = True
                response = self._send_get(f'/account/orders', params=params, authenticated=True)
                orders += list(map(lambda x: x['order'], response['order_history']))
            return self._to_orders(orders)

        except BaseException as ex:
            raise Exception('cannot retrieve orders from BitPanda Pro') from ex

    def get_transactions(self, start_time: Union[datetime, int, float, None] = None,
                         end_time: Union[datetime, int, float, None] = None):
        self.open()
        params = {}
        self._set_date_range_params(params, start_time, end_time)
        try:
            response = self._send_get(f'/v1/account/trades', params=params, authenticated=True)
            transactions = list(map(lambda x: merge(x['trade'], x['fee']), response['trade_history']))
            return self._to_transactions(transactions)

        except BaseException as ex:
            raise Exception('cannot retrieve orders from BitPanda Pro') from ex

    def get_balances(self):
        self.open()
        try:
            response = self._send_get('/v1/account/balances', authenticated=True)
            return self._to_balances(response['balances'])
        except BaseException as ex:
            raise Exception('cannot retrieve positions from BitPanda Pro') from ex

    def _send_get(self, path: str, params: dict = None, authenticated: bool = False):
        request = self._httpClient.build_request('GET', path, params=params)
        if authenticated:
            self._authenticate(request)
        response = self._httpClient.send(request).json()
        return response

    # {'order_history': [
    #   {'order':  {
    #       'trigger_price': '1501.0',
    #       'time_in_force': 'GOOD_TILL_CANCELLED',
    #       'is_post_only': False,
    #       'order_id': 'e9f463b8-c7e7-4f4f-ac39-b8934d138bb2',
    #       'account_holder': '419f1a1c-aadf-4fca-a335-ef183a62c608',
    #       'account_id': '419f1a1c-aadf-4fca-a335-ef183a62c608',
    #       'instrument_code': 'ETH_CHF',
    #       'time': '2022-06-10T17:32:50.889892Z',
    #       'side': 'BUY',
    #       'price': '1500.0',
    #       'amount': '1.3334',
    #       'filled_amount': '0.0',
    #       'type': 'STOP',
    #       'sequence': 7068502923,
    #       'status': 'OPEN'},
    #       'trades': []},
    #  ], 'max_page_size': 100}}

    def _to_orders(self, orders: dict):
        frame = self.to_response_dataframe(orders)
        if frame.size == 0:
            return frame
        frame = frame.loc[:, ('order_id', 'base_currency', 'quote_currency', 'type', 'side', 'price',
                              'amount', 'status', 'time', 'platform', 'trigger_price')]
        # frame['status'].replace('closed', 'FILLED', inplace=True)
        frame.loc['id'] = frame['order_id'].apply(lambda o_id: f'{self.platform}_{o_id}')
        frame.rename(columns={'amount': 'size'}, inplace=True)
        frame.astype(dict(price='float', size='float', trigger_price='float'))
        frame.set_index('id', inplace=True)
        return frame

    def _to_transactions(self, transactions: dict):
        frame = self.to_response_dataframe(transactions)
        if frame.size == 0:
            return frame
        frame = frame.loc[:, ('trade_id', 'order_id', 'base_currency', 'quote_currency', 'side', 'price',
                              'amount', 'time', 'platform', 'fee_amount', 'fee_percentage', 'fee_currency')]
        frame.rename(columns={'amount': 'size', 'fee_amount': 'fee',
                              'fee_percentage': 'fee_rate'},
                     inplace=True)
        frame['id'] = frame['trade_id'].apply(lambda t_id: f'{self.platform}_{t_id}')
        frame['side'] = frame['side'].apply(lambda s: s.lower())
        frame.astype(dict(fee_rate='float'))
        return to_transactions(frame)

    def to_response_dataframe(self, transactions):
        frame = pd.DataFrame(transactions)
        if frame.size == 0:
            return frame
        frame.loc[:, 'base_currency'] = frame['instrument_code'].apply(lambda s: s.split('_')[0])
        frame.loc[:, 'quote_currency'] = frame['instrument_code'].apply(lambda s: s.split('_')[1])
        frame['platform'] = self.platform
        frame['time'] = pd.to_datetime(frame['time'])
        return frame

    def _to_balances(self, balances: dict):
        frame = pd.DataFrame(balances)
        if frame.size == 0:
            return frame
        frame.rename(columns=dict(currency_code='currency'), inplace=True)
        frame = frame.loc[:, ('currency', 'available', 'locked', 'time')]
        frame['platform'] = self.platform
        frame['available'] = pd.to_numeric(frame['available'])
        frame['locked'] = pd.to_numeric(frame['locked'])
        frame['total'] = frame['available'] + frame['locked']
        frame['time'] = pd.to_datetime(frame['time'])
        # evaluate current price
        with ExchangeRatesClient() as change:
            frame['price'] = frame['currency'].apply(lambda c: self.get_rate(change, c, 'USDT'))
            frame['price_usd'] = frame['currency'].apply(lambda c: self.get_rate(change, c, 'USD'))
        frame.set_index('currency', inplace=True)
        return frame

    @staticmethod
    def _set_date_range_params(params: dict, start_time, end_time, precision: Literal['s', 'ms'] = None) -> dict:
        if start_time is not None:
            params['from'] = to_isoformat(start_time, precision)
        if end_time is not None:
            params['to'] = to_isoformat(end_time, precision)
        return params

    def get_rate(self, change: ExchangeRatesClient, base_currency: str, quote_currency: str):
        self.open()
        try:
            response = self._send_get(f'/v1/market-ticker/{base_currency}_{quote_currency}', authenticated=True)
            if 'error' in response:
                return change.get_live_rate(base_currency, quote_currency)[quote_currency]
            else:
                float(response['last_price'])
        except:
            try:
                self._logger.debug(f'cannot resolve {quote_currency} rate for {base_currency}', exc_info=True)
                return change.get_live_rate(base_currency, quote_currency)[quote_currency]
            except:
                self._logger.warning(f'cannot resolve {quote_currency} rate for {base_currency}')
                return np.NAN
