import base64
import functools
import hashlib
import hmac
import time
from enum import Enum
from typing import Literal
from datetime import datetime

import pandas as pd
from pandas import DataFrame

import httpx

from wired_exchange.core import to_timestamp, to_transactions, to_klines
from wired_exchange.core.ExchangeClient import ExchangeClient


class CandleStickResolution(Enum):
    _1min = '1min'
    _3min = '3min'
    _5min = '5min'
    _15min = '15min'
    _30min = '30min'
    _1hour = '1hour'
    _2hour = '2hour'
    _4hour = '4hour'
    _6hour = '6hour'
    _8hour = '8hour'
    _12hour = '12hour'
    _1day = '1day'
    _1week = '1week'


class KucoinClient(ExchangeClient):

    def __init__(self, api_key: str = None, api_passphrase: str = None,
                 api_secret: str = None, host_url: str = None):
        super().__init__('kucoin', api_key, api_secret, host_url, always_authenticate=False)
        self._api_passphrase = api_passphrase if api_passphrase is not None else self._get_exchange_env_value(
            'api_passphrase')

    def _authenticate(self, request):
        ts = int(round(time.time())) * 1000
        self._logger.debug(f'timestamp {ts}')
        signature_payload = f'{str(ts)}{request.method.upper()}{request.url.raw_path.decode("utf-8")}'
        if request.content:
            signature_payload += request.content.decode('utf-8')
        signature = base64.b64encode(
            hmac.new(self._api_secret.encode('utf-8'), signature_payload.encode('utf-8'), hashlib.sha256).digest())
        self._logger.debug(f'payload {signature_payload}')
        passphrase = base64.b64encode(
            hmac.new(self._api_secret.encode('utf-8'), self._api_passphrase.encode('utf-8'), hashlib.sha256).digest())
        request.headers['KC-API-SIGN'] = signature.decode('utf-8')
        request.headers['KC-API-TIMESTAMP'] = str(ts)
        request.headers['KC-API-KEY'] = self._api_key
        request.headers['KC-API-PASSPHRASE'] = passphrase.decode('utf-8')
        request.headers['KC-API-KEY-VERSION'] = "2"

    def get_transactions(self, start_time, end_time=None,
                         trade_type: Literal['spot', 'margin'] = 'spot') -> pd.DataFrame:
        self.open()
        params = {'tradeType': 'MARGIN_TRADE' if trade_type.lower() == 'margin' else 'TRADE'}
        self._add_date_range_params(params, start_time, end_time, 'ms')
        try:
            request = self._httpClient.build_request('GET', '/v1/fills', params=params)
            self._authenticate(request)
            response = self._read_pages('/v1/fills', params, authenticated=True)
            return self._to_transactions(response)
        except httpx.HTTPStatusError as ex:
            self._logger.error('cannot retrieve transactions from Kucoin', ex)

    def get_orders(self, start_time=None, end_time=None, trade_type: Literal['spot', 'margin'] = 'spot',
                   status: Literal['done', 'active'] = None):
        self.open()
        params = {'tradeType': 'MARGIN_TRADE' if trade_type.lower() == 'margin' else 'TRADE'}
        if status is not None:
            params['status'] = status
        self._add_date_range_params(params, start_time, end_time)
        try:
            request = self._httpClient.build_request('GET', '/v1/orders', params=params)
            self._authenticate(request)
            response = self._httpClient.send(request)
            response.json()
        except httpx.HTTPStatusError as ex:
            self._logger.error('cannot retrieve transactions from Kucoin', ex)

    def get_prices_history(self, base: str, quote: str,
                           resolution: CandleStickResolution, start_time=None, end_time=None) -> pd.DataFrame:
        self.open()
        # For each query, the system would return at most **1500** pieces of data. To obtain more data, please page
        # the data by time.
        params = dict(symbol=f'{base}-{quote}', type=resolution.value)
        self._add_date_range_params(params, start_time, end_time, 's')
        try:
            request = self._httpClient.build_request('GET', '/v1/market/candles', params=params)
            response = self._httpClient.send(request).json()
            if not response['code'].startswith('200'):
                raise RuntimeError(f'{response["code"]}: response code does not indicate a success')
            return self._to_klines(response['data'], base, quote)
        except httpx.HTTPStatusError as ex:
            self._logger.error('cannot retrieve transactions from Kucoin', ex)

    # {
    #     "symbol": "CAKE-USDT",
    #     "tradeId": "61913fb6674fa9563b3c76fe",
    #     "orderId": "6190e3cbfe8b030001429310",
    #     "counterOrderId": "61913fb6f4f1fd0001c2e6df",
    #     "side": "buy",
    #     "liquidity": "maker",
    #     "forceTaker": false,
    #     "price": "18.086",
    #     "size": "0.615",
    #     "funds": "11.12289",
    #     "fee": "0.01112289",
    #     "feeRate": "0.001",
    #     "feeCurrency": "USDT",
    #     "stop": "",
    #     "tradeType": "TRADE",
    #     "type": "limit",
    #     "createdAt": 1636908982000
    # }

    def _to_transactions(self, fills: dict) -> pd.DataFrame:
        tr = DataFrame(fills)
        if tr.size == 0:
            return tr
        tr['time'] = pd.to_datetime(tr['createdAt'], unit='ms', utc=True)
        tr['base_currency'] = tr['symbol'].apply(lambda s: s.split('-')[0])
        tr['quote_currency'] = tr['symbol'].apply(lambda s: s.split('-')[1])
        tr.rename(
            columns=dict(feeCurrency='fee_currency', tradeId='trade_id', orderId='order_id',
                         counterOrderId='counter_order_id', feeRate='fee_rate'), inplace=True)
        tr.drop(['createdAt', 'symbol', 'forceTaker', 'stop', 'tradeType',
                 'funds', 'liquidity', 'counter_order_id'],
                axis='columns', inplace=True)
        tr.astype(dict(order_id='string', trade_id='string'))
        tr['platform'] = self.platform
        tr['id'] = tr['trade_id'].apply(lambda id: f'{self.platform}_{id}')
        return to_transactions(tr)

    # [
    #     [
    #         "1545904980",             //Start time of the candle cycle
    #         "0.058",                  //opening price
    #         "0.049",                  //closing price
    #         "0.058",                  //highest price
    #         "0.049",                  //lowest price
    #         "0.018",                  //Transaction volume
    #         "0.000945"                //Transaction amount
    #     ]
    # ]
    def _to_klines(self, candles: dict, base: str, quote: str) -> pd.DataFrame:
        tr = pd.DataFrame(candles, columns=['time', 'open', 'close', 'high', 'low', 'volume', 'amount'])
        if tr.size == 0:
            return tr
        tr.drop(['amount'], inplace=True, axis='columns')
        tr['time'] = pd.to_numeric(tr['time']) * 1000
        return to_klines(tr, base, quote)

    def get_balances(self):
        self.open()
        try:
            request = self._httpClient.build_request('GET', '/v1/accounts')
            self._authenticate(request)
            response = self._httpClient.send(request).json()
            if not response['code'].startswith('200'):
                raise RuntimeError(f'{response["code"]}: response code does not indicate a success')
            return self._to_balances(response['data'])
        except httpx.HTTPStatusError as ex:
            self._logger.error('cannot retrieve accounts list from Kucoin', ex)

    def _to_balances(self, balances_json: dict):
        balances = pd.DataFrame(balances_json).groupby('currency').sum()
        if balances.size == 0:
            return balances
        balances.rename(columns=dict(balance='total'), inplace=True)
        balances.drop(columns=['id', 'type', 'holds'], inplace=True)
        balances['total'] = pd.to_numeric(balances['total'])
        balances['available'] = pd.to_numeric(balances['available'])
        balances['platform'] = self.platform
        return balances


    def get_account_operations(self, start_time=None, end_time=None) -> pd.DataFrame:
        self.open()
        params = {}
        self._add_date_range_params(params, start_time, end_time, 'ms')
        try:
            columns = ['amount', 'currency',
                   'status', 'createdAt', 'updatedAt', 'walletTxId']
            result = self._read_pages('/v1/deposits', params, authenticated=True)
            deposits = pd.DataFrame(result, columns=columns)
            deposits['type'] = 'deposit'
            result = self._read_pages('/v1/withdrawals', params, authenticated=True)
            withdrawals = pd.DataFrame(result, columns=columns)
            withdrawals['type'] = 'withdrawal'
            return self._to_account_operations(deposits, withdrawals)
        except httpx.HTTPStatusError as ex:
            self._logger.error('cannot retrieve transactions from Kucoin', ex)

    def _to_account_operations(self, deposits, withdrawals):
        operations = deposits.append(withdrawals, ignore_index=True)
        operations['updatedAt'] = pd.to_datetime(operations['updatedAt'], unit='ms', utc=True)
        operations['platform'] = self.platform
        return operations


    def get_orders_v1(self, symbol=None, start_time=None, end_time=None) -> pd.DataFrame:
        self.open()
        params = dict(pageSize=200)
        if symbol is not None:
            params['symbol'] = symbol
        self._add_date_range_params(params, start_time, end_time, 'ms')
        try:
            return self._read_pages('/v1/hist-orders', params, authenticated=True)
        except httpx.HTTPStatusError as ex:
            self._logger.error('cannot retrieve transactions from Kucoin', ex)

    def _read_pages(self, path, params, authenticated, aggregated=True):
        pages = self._get_pages(path, params, authenticated)
        if aggregated:
            return self._aggregate_pages(pages)
        else:
            return pages

    def _get_pages(self, path: str, params: dict, authenticated: bool = False):
        remaining_pages = True
        params['current_page'] = 0
        while remaining_pages:
            params['current_page'] += 1
            request = self._httpClient.build_request('GET', path, params=params)
            if authenticated:
                self._authenticate(request)
            response = self._httpClient.send(request)
            json = response.json()
            total_pages = json['data']['totalPage']
            remaining_pages = params['current_page'] < total_pages
            if json['data']['totalNum'] > 0:
                yield json['data']['items']

    @staticmethod
    def _add_date_range_params(params: dict, start_time, end_time, precision) -> dict:
        if start_time is not None:
            params['startAt'] = to_timestamp(start_time, precision) if isinstance(start_time, datetime) else int(
                round(start_time))
        if end_time is not None:
            params['endAt'] = to_timestamp(end_time, precision) if isinstance(end_time, datetime) else int(
                round(end_time))
        return params

    @staticmethod
    def _aggregate_pages(iterable):
        read_orders = [json for json in iterable]
        if len(read_orders) > 0:
            return functools.reduce(lambda x, y: x + y, read_orders)
        else:
            return {}
