import base64
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

    def get_transactions(self, start_time, end_time=None, trade_type: Literal['spot', 'margin'] = 'spot'):
        self.open()
        params = {'tradeType': 'MARGIN_TRADE' if trade_type.lower() == 'margin' else 'TRADE'}
        self.add_date_range_params(params, start_time, end_time, 'ms')
        try:
            request = self._httpClient.build_request('GET', '/v1/fills', params=params)
            self._authenticate(request)
            response = self._httpClient.send(request)
            return self._to_transactions(response.json())
        except httpx.HTTPStatusError as ex:
            self._logger.error('cannot retrieve transactions from Kucoin', ex)

    def get_orders(self, start_time=None, end_time=None, trade_type: Literal['spot', 'margin'] = 'spot',
                   status: Literal['done', 'active'] = None):
        self.open()
        params = {'tradeType': 'MARGIN_TRADE' if trade_type.lower() == 'margin' else 'TRADE'}
        if status is not None:
            params['status'] = status
        self.add_date_range_params(params, start_time, end_time)
        try:
            request = self._httpClient.build_request('GET', '/v1/orders', params=params)
            self._authenticate(request)
            response = self._httpClient.send(request)
            response.json()
        except httpx.HTTPStatusError as ex:
            self._logger.error('cannot retrieve transactions from Kucoin', ex)

    def add_date_range_params(self, params, start_time, end_time, precision):
        if start_time is not None:
            params['startAt'] = to_timestamp(start_time, precision) if isinstance(start_time, datetime) else int(
                round(start_time))
        if end_time is not None:
            params['endAt'] = to_timestamp(end_time, precision) if isinstance(end_time, datetime) else int(
                round(end_time))
        return params

    def get_prices(self, base: str, quote: str, resolution: CandleStickResolution, start_time=None, end_time=None):
        self.open()
        # For each query, the system would return at most **1500** pieces of data. To obtain more data, please page
        # the data by time.
        params = dict(symbol=f'{base}-{quote}', type=resolution.value)
        self.add_date_range_params(params, start_time, end_time, 's')
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

    def _to_transactions(self, fills: dict):
        tr = DataFrame(fills['data']['items'])
        tr['time'] = pd.to_datetime(tr['createdAt'], unit='ms', utc=True)
        tr['base_currency'] = tr['symbol'].apply(lambda s: s.split('-')[0])
        tr['quote_currency'] = tr['symbol'].apply(lambda s: s.split('-')[1])
        tr.rename(
            columns=dict(feeCurrency='fee_currency', tradeId='trade_id', orderId='order_id',
                         counterOrderId='counter_order_id', feeRate='fee_rate'), inplace=True)
        tr.drop(['createdAt', 'symbol', 'forceTaker', 'stop', 'tradeType', 'funds', 'liquidity'],
                axis='columns', inplace=True)
        tr['platform'] = self.platform
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
    def _to_klines(self, candles: dict, base: str, quote: str):
        tr = pd.DataFrame(candles, columns=['time', 'open', 'close', 'high', 'low', 'volume', 'amount'])
        tr.drop(['amount'], inplace=True, axis='columns')
        tr['time'] = pd.to_numeric(tr['time']) * 1000
        return to_klines(tr, base, quote)
